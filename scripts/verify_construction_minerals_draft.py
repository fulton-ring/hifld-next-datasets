"""Actual registered complete preservation reconstruction and fail-closed gates.

The prior release marker and injected quality/storage failures are local fixtures,
not approval of this historical input, a baseline, contract or a real release.
"""
from __future__ import annotations
import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);parser.add_argument('--capture-dir',type=Path,required=True);args=parser.parse_args()
runtime=args.runtime.absolute();captures=args.capture_dir.absolute();assert not runtime.exists() and captures.is_dir()
for name in ('dagster','staging','published'):(runtime/name).mkdir(parents=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS'):os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_CONSTRUCTION_MINERALS_CAPTURE_DIR=str(captures),HIFLD_CONSTRUCTION_MINERALS_PACKAGE_ID='stanford-ds401wt4784-preservation-as-provided-v1',HIFLD_SHADOW_MANUAL_ONLY='1')
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld import construction_state
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.construction_minerals import construction_minerals_preservation_candidate as asset
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import construction_minerals as source
from dagster_hifld.release_approval import validate_release_provenance

staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
published.write_key('synthetic-prior-release-fixture.bin',b'local-preservation-fixture-only-not-accepted-real-data')
def snap(storage):return {k:storage.sha256_key(k) for k in storage.list_prefix()}
def files(root):return {str(p.relative_to(root)):source.sha256_file(p) for p in root.rglob('*') if p.is_file()}
prior=snap(published);keys=[k for k in normal.resolve_asset_graph().get_all_asset_keys() if k.path[0]=='publish']
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('construction_minerals_normal_graph',selection=AssetSelection.assets(*keys))])
report={'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(source.__file__).parents[3],text=True).strip(),'module':source.__file__,'runtime':str(runtime),'package_id':source.PACKAGE,'source_accepted':False,'release_accepted':False,'prior_release':'synthetic local fixture only, not a real accepted release','runs':[]}
def record(kind,result):
    errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
    report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,'materializations':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()],'checks':[{'name':e.check_name,'passed':e.passed} for e in result.get_asset_check_evaluations()]})
    (runtime/'execution-progress.json').write_text(json.dumps(report,indent=2)+'\n');return errors
with DagsterInstance.get() as instance:
    job=normal.resolve_job_def('construction_minerals_preservation_reconstruction');graph=defs.resolve_job_def('construction_minerals_normal_graph')
    def run(label,expected=None):
        result=job.execute_in_process(instance=instance,raise_on_error=False);errors=record(label,result)
        assert result.success if expected is None else (not result.success and any(expected in e for e in errors)),errors
        assert snap(published)==prior
        return result
    install=construction_state._install_once
    def fail_marker(base,key,body):
        if key.endswith('.first-construction.json'):raise OSError('synthetic first-marker fault after CAS installation')
        install(base,key,body)
    with patch.object(construction_state,'_install_once',fail_marker):run('synthetic_CAS_only_first_marker_failure','construction_state_io_failed')
    evidence=list((runtime/'staging/_construction-state'/source.SLUG).glob('*.json.evidence/*.json'));assert len(evidence)==1
    evidence=evidence[0];original=evidence.read_bytes();envelope=json.loads(original);version=envelope['version'];alias=runtime/'staging'/source.construction_record_key(version)
    state_before=files(runtime/'staging/_construction-state')
    for i in (1,2):run('CAS_only_cold_retry_'+str(i),'construction_missing_original_state')
    assert files(runtime/'staging/_construction-state')==state_before and not alias.exists()
    marker={'schema_version':1,'version':version,'processing_fingerprint':envelope['processing_fingerprint'],'original_bundle_sha256':source.digest(original),'original_bundle_evidence_key':str(evidence.relative_to(runtime/'staging'))}
    Path(str(alias)+'.first-construction.json').write_bytes(json.dumps(marker,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode());alias.write_bytes(original)
    first=run('complete766_after_explicit_trusted_state_restore');output=first.output_for_node(asset.op.name);assert output['feature_count']==766 and not output['accepted'];assert output['version']==version
    prefix=build_publish_partition_key(source.SLUG,source.SLUG,version);root=runtime/'staging'/prefix
    assert (source.SLUG,source.SLUG,version) in list(_iter_staged_version_paths(staging))
    candidate_before=files(root);raw_before={k:v for k,v in snap(staging).items() if k.startswith('_raw-captures/')}
    again=run('complete_source_byte_idempotent');assert again.output_for_node(asset.op.name)['reused'] and files(root)==candidate_before
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
    for cycle in (1,2):
        result=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);errors=record('normal_catalog_'+str(cycle)+'_missing_real_approval_registry',result)
        assert not result.success and errors and all('missing_independent_release_approval_registry' in e for e in errors),errors
        assert snap(published)==prior
        run('source_after_catalog_'+str(cycle))
    full=source.acquire(staging,str(captures),source.PACKAGE);table,published_ids,audit=source.inspect_captures(full);source.verify_outputs(root,table,published_ids)
    native=root/f'geoparquet/{source.SLUG}.parquet';manifest=json.loads((root/'metadata/source_manifest.json').read_text());validate_release_provenance(manifest)
    assert manifest['approved']is False and len(manifest['inputs'])==14
    report['native_candidate']={'rows':766,'physical_schema':str(table.schema),'native_parquet_sha256':source.sha256_file(native),'native_parquet_bytes':native.stat().st_size,'all_source_fields_IDs_exact_decimals_native_geometry_verified':True,'published_identity_columns':published_ids.column_names,'all6_complete_published_identifier_columns_separate':True,'own_PRJ_and_conflicting_FGDC_datum_retained':True}
    report['quality']=json.loads((root/'metadata/quality_manifest.json').read_text())
    # Explicit synthetic clone: make one geometry null, then accurately re-pin its
    # proposed native capability. Normal quality must reject; source is untouched.
    badprefix=prefix+'-synthetic-null-geometry';badroot=runtime/'staging'/badprefix;shutil.copytree(root,badroot);bad=badroot/f'geoparquet/{source.SLUG}.parquet';badtable=pq.read_table(bad);values=badtable['geometry'].to_pylist();values[0]=None
    index=badtable.schema.get_field_index('geometry');badtable=badtable.set_column(index,pa.field('geometry',pa.binary(),nullable=True),pa.array(values,pa.binary()));bad.write_bytes(source.parquet_bytes(badtable))
    bm=json.loads((badroot/'metadata/source_manifest.json').read_text());pin=bm['prebuilt_output_policy']['formats']['geoparquet']['objects'][0];pin.update(sha256=source.sha256_file(bad),bytes=bad.stat().st_size);(badroot/'metadata/source_manifest.json').write_bytes(source.encode(bm))
    # Remove catalog-derived authored snapshot so the clone's synthetic manifest
    # is resolved honestly for its own synthetic partition, not a stale original.
    shutil.rmtree(badroot/'metadata/source');(badroot/'metadata/resolved_provenance.json').unlink()
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix]);result=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);record('synthetic_null_geometry_failed_quality_gate',result)
    assert not result.success and any(not check.passed for check in result.get_asset_check_evaluations()) and snap(published)==prior
    for label in ('coforged_original_receipt','missing_original_CAS','coforged_native_parquet','unknown_metadata'):
        restore=None;withheld=None;ready=json.loads((root/source.READY).read_text())
        if label=='coforged_original_receipt':
            restore=alias.read_bytes();b=json.loads(restore);r=b['payload']['captures']['minop2x020.dbf']['receipt'];r['observed_at']='2026-10-19T23:59:59+00:00'
            next(i for i in b['payload']['manifest']['inputs'] if i['input_id'].startswith('minop2x020.dbf:'))['retrieved_at']=r['observed_at']
            alias.write_bytes(source.canonical_construction_bundle(version,b['processing_fingerprint'],b['payload']))
            for relative,body in [('raw/captures/minop2x020.dbf.receipt.json',source.encode(r)),('metadata/source_manifest.json',source.encode(b['payload']['manifest']))]:
                (root/relative).write_bytes(body);ready['objects'][relative]=source.digest(body)
            expected='construction_marker_mismatch'
        elif label=='missing_original_CAS':withheld=evidence;restore=evidence.read_bytes();evidence.unlink();expected='construction_original_evidence_mismatch'
        elif label=='coforged_native_parquet':
            changed=pq.read_table(native).replace_schema_metadata({b'geo':b'{}'});native.write_bytes(source.parquet_bytes(changed));ready['objects'][str(native.relative_to(root))]=source.sha256_file(native);expected='full_native_source_values'
        else:(root/'metadata/unexpected.json').write_bytes(b'{}');expected='unknown_missing_or_linked_candidate_object'
        (root/source.READY).write_bytes(source.encode(ready));altered=files(root);old={q for q in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-candidate-*') if q.is_dir()}
        failed=run('reject_'+label,expected);assert not root.exists();new={q for q in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-candidate-*') if q.is_dir()}-old;assert len(new)==1 and files(next(iter(new)))==altered
        if label in ('coforged_original_receipt','missing_original_CAS'):
            run('repeat_cold_reject_'+label,expected)
            (withheld if withheld else alias).write_bytes(restore) # Explicit exact trusted operator restore only.
        restored=run('rebuild_after_explicit_restore_'+label);assert not restored.output_for_node(asset.op.name)['reused'];source.verify_outputs(root,table,published_ids)
        assert json.loads((root/'metadata/source_manifest.json').read_text())==manifest and raw_before=={k:v for k,v in snap(staging).items() if k.startswith('_raw-captures/')}
    report.update(output=output,all_original_capture_bytes_preserved=True,source_byte_idempotent=True,two_native_prebuilt_catalog_cycles_passed_to_real_missing_approval_gate=True,failed_quality_and_approval_preserve_prior_local_fixture=True,complete_rejected_versions_detached_intact=True,no_source_baseline_contract_or_datum_acceptance=True)
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'code_head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
