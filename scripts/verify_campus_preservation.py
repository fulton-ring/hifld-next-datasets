"""Actual isolated complete campus preservation and failed-release graph replay.

Source-only success excludes checks and means preservation, never acceptance.
"""
from __future__ import annotations
import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser()
parser.add_argument('--capture-dir',type=Path,default=Path(__file__).parents[1]/'docs/dataset-sources/colleges-and-universities-campuses/evidence/captures')
parser.add_argument('--runtime-dir',type=Path,required=True)
args=parser.parse_args();runtime=args.runtime_dir.resolve();capture=args.capture_dir.resolve()
if runtime.exists() and any(runtime.iterdir()):raise ValueError('Use an empty isolated runtime directory.')
for name in ['dagster','staging','published']:(runtime/name).mkdir(parents=True,exist_ok=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for name in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(name,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1',HIFLD_CAMPUS_CAPTURE_DIR=str(capture))
from dagster import AssetKey,AssetSelection,DagsterInstance,Definitions,define_asset_job
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.campus_preservation import ornl_campus_historical_preservation as source,campus_historical_full_quality
from dagster_hifld.sources import campus_preservation as c
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
stage=c.StagingStorageResource(local_dir=str(runtime/'staging'));publish=c.PublishedStorageResource(local_dir=str(runtime/'published'))
publish.write_key('synthetic-accepted-fixture.bin',b'Synthetic prior local accepted fixture; no real campus release approved.')
prior={k:publish.read_key(k) for k in publish.list_prefix()}
jobs=[normal.resolve_job_def('ornl_campus_historical_reconstruction'),
    define_asset_job('campus_full_source_quality',selection=AssetSelection.assets(source)),
    define_asset_job('campus_normal_catalog_formats',selection=AssetSelection.assets(*[k for k in normal.resolve_asset_graph().get_all_asset_keys() if k.path[0]=='publish'])),
    define_asset_job('campus_approval_only',selection=AssetSelection.assets(AssetKey(['publish','verify']),AssetKey(['publish','promote'])).without_checks())]
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':stage,'published_storage':publish},jobs=jobs)
# Job names identify exact explicit selections above; source-only excludes checks.
source_config={'ops':{source.op.name:{'config':{'capture_directory':str(capture)}}}}
report={'classification':'2022 HIFLD/ORNL campus historical preservation candidate; NOT accepted source/release/contract','code_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True,cwd=Path(__file__).parents[1]).strip(),'runs':[]}
def save():(runtime/'execution.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
def run(label,job,instance,**kwargs):
    result=defs.resolve_job_def(job).execute_in_process(instance=instance,raise_on_error=False,**kwargs)
    errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
    report['runs'].append({'label':label,'job':job,'run_id':result.run_id,'success':result.success,'errors':errors,'materialized_assets':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()], 'checks':[{'name':v.check_name,'passed':v.passed,'metadata':str(v.metadata)} for v in result.get_asset_check_evaluations()]})
    if result.success and job=='ornl_campus_historical_reconstruction':report['runs'][-1]['output']=result.output_for_node(source.op.name)
    save();return result,errors
with patch('dagster_hifld.assets.publish.process_layer_partitioned_geoparquet',side_effect=AssertionError('Do not regenerate native prebuilt Parquet')) as converter,DagsterInstance.get() as instance:
    result,errors=run('source_stage_without_checks','ornl_campus_historical_reconstruction',instance,run_config=source_config)
    assert result.success,errors
    output=result.output_for_node(source.op.name);assert output['feature_count']==5898 and output['source_invalid_geometry_count']==15 and not output['reused']
    version=output['version'];prefix=f'{c.SLUG}/{c.SLUG}/{version}';root=runtime/'staging'/prefix
    report.update(version=version,source_stage_excludes_quality_check=True)
    assert (c.SLUG,c.SLUG,version) in set(_iter_staged_version_paths(stage))
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
    original_manifest=(root/'metadata/source_manifest.json').read_bytes();owned=c.strict_json((root/c.READY).read_bytes())['objects']
    raw_before={k:stage.sha256_key(k) for k in stage.list_prefix('_raw-captures')}
    result,errors=run('unchanged_source_reuse','ornl_campus_historical_reconstruction',instance,run_config=source_config)
    assert result.success and result.output_for_node(source.op.name)['reused'],errors
    result,errors=run('full_5898_source_quality_blocks_15_invalid','campus_full_source_quality',instance,run_config=source_config)
    assert not result.success and any(not v.passed and v.check_name=='campus_historical_full_quality' for v in result.get_asset_check_evaluations())
    for cycle in [1,2]:
        result,errors=run(f'normal_catalog_formats_cycle_{cycle}_release_blocked','campus_normal_catalog_formats',instance,partition_key=prefix)
        assert not result.success,errors
        assert not any(e.is_step_success and e.step_key=='publish__promote' for e in result.all_events)
        result,errors=run(f'post_normal_cycle_{cycle}_source_reuse','ornl_campus_historical_reconstruction',instance,run_config=source_config)
        assert result.success and result.output_for_node(source.op.name)['reused'],errors
    result,errors=run('missing_approval_blocks_promotion_without_quality_check_claim','campus_approval_only',instance,partition_key=prefix)
    assert not result.success and any('missing_independent_release_approval_registry' in error for error in errors),errors
    assert not any(e.is_step_success and e.step_key=='publish__promote' for e in result.all_events)
    outside=runtime/'staging/_construction-state'/c.SLUG;trusted=runtime/'trusted-original-construction';shutil.copytree(outside,trusted);shutil.rmtree(outside)
    result,errors=run('observed_whole_witness_loss_rejected','ornl_campus_historical_reconstruction',instance,run_config=source_config)
    assert not result.success and not root.exists(),errors
    incident=runtime/'staging/_construction-incidents'/c.SLUG/version/'incident.json';incident_body=incident.read_bytes()
    later=runtime/'synthetic-later-receipts';shutil.copytree(capture,later)
    for name in c.INPUTS:
        receipt=c.strict_json((later/(name+'.receipt.json')).read_bytes());receipt.update(started_at='2026-10-20T01:02:03+00:00',observed_at='2026-10-20T01:02:04+00:00');(later/(name+'.receipt.json')).write_bytes(c.encode(receipt))
    later_config={'ops':{source.op.name:{'config':{'capture_directory':str(later)}}}}
    for retry in [1,2]:
        result,errors=run(f'later_receipts_retry_{retry}_no_bootstrap','ornl_campus_historical_reconstruction',instance,run_config=later_config)
        assert not result.success and any('construction_missing_original_state' in error for error in errors),errors
        assert not root.exists() and incident.read_bytes()==incident_body and not outside.exists()
    shutil.copytree(trusted,outside)
    result,errors=run('explicit_exact_original_state_restore_cold_rebuild','ornl_campus_historical_reconstruction',instance,run_config=later_config)
    assert result.success and not result.output_for_node(source.op.name)['reused'],errors
    assert (root/'metadata/source_manifest.json').read_bytes()==original_manifest
    assert all(c.sha256_file(root/name)==pin['sha256'] for name,pin in owned.items())
    assert incident.read_bytes()==incident_body
    assert all(stage.sha256_key(k)==v for k,v in raw_before.items())
    assert prior=={k:publish.read_key(k) for k in publish.list_prefix()}
    captures=c.acquire(stage,capture);tables={name:c.decode_archive(name,captures[name][0])[0] for name in c.ARCHIVES}
    terminal=c.pq.read_table(root/'geoparquet'/f'{c.SLUG}.parquet');expected=c.geotable(tables[c.PRIMARY],3857)
    assert terminal.equals(expected,check_metadata=True)
    other=c.pq.read_table(root/'candidate/native_wgs84.parquet');assert other.equals(c.geotable(tables['pasda-original-zip'],4326),check_metadata=True)
    c.reconcile_native(root,tables[c.PRIMARY]);assert converter.call_count==0
    report.update(all_5898_primary_and_secondary_native_rows_types_dates_nulls_ids_wkb_exact=True,source_invalid_geometry_count=15,
        all_source_owned_objects_unchanged=True,raw_original_objects_preserved=True,original_construction_receipts_preserved_after_later_observations=True,
        prior_synthetic_accepted_fixture_unchanged=True,release_promoted=False,generic_converter_calls=converter.call_count,source_owned_pins=owned)
    save()
print(json.dumps({'runs':[(r['label'],r['run_id'],r['success']) for r in report['runs']],'version':version}))
