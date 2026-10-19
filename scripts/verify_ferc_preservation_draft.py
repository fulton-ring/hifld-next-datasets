"""Actual isolated local FERC preservation/gate/history replay; no acceptance.

Requires the matching complete historical archive and its original receipts.
The two invalid originals must fail both full source and normal catalog quality.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--capture-dir',type=Path,required=True)
parser.add_argument('--runtime-dir',type=Path,required=True)
args=parser.parse_args()
runtime=args.runtime_dir.absolute();capture=args.capture_dir.absolute()
if runtime.exists()and any(runtime.iterdir()):raise ValueError('Use an empty isolated runtime directory.')
for name in ['dagster','staging','published']:(runtime/name).mkdir(parents=True,exist_ok=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for name in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(name,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),
    HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_FERC_CAPTURE_DIR=str(capture),HIFLD_SHADOW_MANUAL_ONLY='1')

from dagster import AssetKey,AssetSelection,DagsterInstance,Definitions,define_asset_job
from dagster_hifld.assets.ferc_regions import ferc_historical_preservation as source,ferc_historical_full_quality
from dagster_hifld.checks import publish_catalog_quality_manifest
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.sources import ferc_regions as c

stage=c.StagingStorageResource(local_dir=str(runtime/'staging'));publish=c.PublishedStorageResource(local_dir=str(runtime/'published'))
publish.write_key('synthetic-prior.bin',b'Synthetic prior output: preserve through every unapproved FERC gate/fault. No real dataset accepted.')
prior={k:publish.read_key(k)for k in publish.list_prefix()}
source_job=normal.resolve_job_def('ferc_historical_reconstruction')
jobs=[source_job,
    define_asset_job('ferc_actual_full_source_quality',selection=AssetSelection.assets(source)|AssetSelection.checks(ferc_historical_full_quality)),
    define_asset_job('ferc_normal_catalog_and_quality',selection=AssetSelection.assets(*[k for k in normal.resolve_asset_graph().get_all_asset_keys()if k.path[0]=='publish'])|AssetSelection.checks(publish_catalog_quality_manifest)),
    define_asset_job('ferc_missing_registry_mechanical_negative',selection=AssetSelection.assets(AssetKey(['publish','verify']),AssetKey(['publish','promote'])).without_checks())]
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,
    resources={'staging_storage':stage,'published_storage':publish},jobs=jobs)
report={'schema_version':1,'scope':'Explicit complete unapproved historical-as-provided preservation; real2-invalid source/catalog failures, no release approval.',
    'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(c.__file__).parents[3],text=True).strip(),
    'module':c.__file__,'runs':[]}

def execute(label,job,*,partition=None,success,config=None):
    result=defs.resolve_job_def(job).execute_in_process(instance=instance,partition_key=partition,run_config=config,raise_on_error=False)
    errors=[e.event_specific_data.error.to_string()for e in result.get_step_failure_events()]
    checks=[{'name':e.check_name,'passed':e.passed,'metadata':{k:str(v)for k,v in e.metadata.items()}}for e in result.get_asset_check_evaluations()]
    report['runs'].append({'label':label,'run_id':result.run_id,'success':result.success,'errors':errors,'checks':checks})
    (runtime/'execution-progress.json').write_text(json.dumps(report,indent=2)+'\n')
    assert result.success is success,(label,errors)
    assert prior=={k:publish.read_key(k)for k in publish.list_prefix()}
    return result

source_config={'ops':{source.op.name:{'config':{'capture_directory':str(capture)}}}}
with patch('dagster_hifld.assets.publish.process_layer_partitioned_geoparquet',side_effect=AssertionError('Forbidden generic native GeoParquet regeneration'))as converter,DagsterInstance.get()as instance:
    result=execute('complete_original_preservation_only',source_job.name,success=True,config=source_config)
    output=result.output_for_node(source.op.name);assert output['feature_count']==5 and output['source_invalid_geometry_count']==2
    version=output['version'];prefix=f'{c.SLUG}/{c.SLUG}/{version}';root=runtime/'staging'/prefix
    assert(c.SLUG,c.SLUG,version)in set(_iter_staged_version_paths(stage))
    result=execute('source_refresh_idempotent',source_job.name,success=True,config=source_config);assert result.output_for_node(source.op.name)['reused']is True
    execute('real_full_source_quality_rejects_two_invalid_originals','ferc_actual_full_source_quality',success=False,config=source_config)
    assert report['runs'][-1]['checks']and any(not v['passed']for v in report['runs'][-1]['checks'])
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
    execute('normal_catalog_quality_blocks_downstream','ferc_normal_catalog_and_quality',partition=prefix,success=False)
    assert any(not v['passed']for v in report['runs'][-1]['checks'])
    result=execute('post_catalog_source_refresh',source_job.name,success=True,config=source_config);assert result.output_for_node(source.op.name)['reused']is True
    execute('missing_registry_separate_mechanical_negative_no_quality_waiver','ferc_missing_registry_mechanical_negative',partition=prefix,success=False)
    assert any('missing_independent_release_approval_registry'in e for e in report['runs'][-1]['errors'])
    manifest=c.strict_json((root/'metadata/source_manifest.json').read_bytes());c.validate_release_provenance(manifest)
    dictionary=c.strict_json((root/'metadata/data_dictionary.json').read_bytes())
    assert c.encode(dictionary['inputs'])==c.encode(manifest['inputs'])and len(manifest['inputs'])==3
    native=c.pq.read_table(root/'geoparquet'/f'{c.SLUG}.parquet')
    table,audit=c.inspect_source(capture/'pasda_original.body');assert native.equals(c.expected_parquet_table(table),check_metadata=True)
    assert not(root/'geopackage').exists()and converter.call_count==0
    report.update(version=version,normal_quality=c.strict_json((root/'metadata/quality_manifest.json').read_bytes()),
        full_original_values_IDS_decimal_literals_native_WKB_exact=True,proposed_decimal_mapping_approved=False,
        all_three_input_provenance_records_preserved=True,generic_converter_calls=converter.call_count,prior_preserved=True)
    state_dir=runtime/'staging/_construction-state'/c.SLUG
    outside={p.relative_to(state_dir):p.read_bytes()for p in state_dir.rglob('*')if p.is_file()}
    original_manifest=(root/'metadata/source_manifest.json').read_bytes();ready=c.strict_json((root/c.READY).read_bytes())
    original_raw={k:stage.sha256_key(k)for k in stage.list_prefix('_raw-captures')}
    shutil.rmtree(state_dir)
    execute('observed_whole_witness_loss',source_job.name,success=False,config=source_config);assert not root.exists()
    incident=runtime/'staging'/f'_construction-incidents/{c.SLUG}/{version}/incident.json';incident_body=incident.read_bytes()
    for n in range(2):
        execute(f'permanent_refusal_retry_{n+1}',source_job.name,success=False,config=source_config)
        assert not root.exists()and not state_dir.exists()and incident.read_bytes()==incident_body
    for name,body in outside.items():path=state_dir/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
    # The intact first candidate is quarantined as evidence. Exact trusted
    # original witnesses permit cold reconstruction, never fabricated receipts.
    execute('exact_original_operator_restoration_cold_rebuild',source_job.name,success=True,config=source_config)
    assert(root/'metadata/source_manifest.json').read_bytes()==original_manifest
    assert all(c.sha256_file(root/name)==sha for name,sha in ready['objects'].items())
    execute('trusted_restoration_reuse',source_job.name,success=True,config=source_config)
    incident.unlink();incident.parent.rmdir();shutil.rmtree(state_dir)
    old_pins={str(p.relative_to(root)):c.sha256_file(p)for p in root.rglob('*')if p.is_file()};real=c.immutable
    def interrupt(storage,key,body):
        if key.startswith('_construction-incidents/'):raise OSError('SYNTHETIC_incident_body_interruption')
        return real(storage,key,body)
    with patch.object(c,'immutable',side_effect=interrupt):execute('incident_partial_write_preserves_observed_candidate',source_job.name,success=False,config=source_config)
    assert root.exists()and incident.parent.is_dir()and not incident.exists()
    for n in range(2):
        execute(f'partial_journal_retry_{n+1}',source_job.name,success=False,config=source_config)
        assert old_pins=={str(p.relative_to(root)):c.sha256_file(p)for p in root.rglob('*')if p.is_file()}
    for name,body in outside.items():path=state_dir/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
    incident.write_bytes(incident_body)
    execute('exact_original_and_journal_operator_restore',source_job.name,success=True,config=source_config)
    assert(root/'metadata/source_manifest.json').read_bytes()==original_manifest
    assert original_raw=={k:stage.sha256_key(k)for k in stage.list_prefix('_raw-captures')}
    report.update(permanent_observed_loss_refusal=True,partial_journal_preserves_candidate=True,
        trusted_original_restoration_only=True,raw_originals_and_first_receipts_preserved=True,prior_preserved=True)
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'run_count':len(report['runs']),'prior_preserved':True,'release_approved':False}))
