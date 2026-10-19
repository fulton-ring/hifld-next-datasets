"""Bounded registered receipt-I/O repair and full native parity proof.

The prior release marker and injected quality/storage failures are local fixtures,
not approval of this historical input, a baseline, contract or a real release.
"""
from __future__ import annotations
import argparse
import json
import os
import shutil
import subprocess
import struct
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
    assert os.geteuid()!=0,'Real unreadable-receipt proof requires ordinary POSIX user'
    job=normal.resolve_job_def('construction_minerals_preservation_reconstruction');graph=defs.resolve_job_def('construction_minerals_normal_graph')
    invalid=runtime/'unreadable-receipt-input';shutil.copytree(captures,invalid);name='legacy_metadata';receipt=invalid/(name+'.receipt.json');original_receipt=receipt.read_bytes();body=(invalid/(name+'.body')).read_bytes();receipt.chmod(0)
    os.environ['HIFLD_CONSTRUCTION_MINERALS_CAPTURE_DIR']=str(invalid)
    try:
        failed=job.execute_in_process(instance=instance,raise_on_error=False);errors=record('actual_POSIX_unreadable_first_receipt_body_retained',failed)
        assert not failed.success and any('Permission denied' in e for e in errors)
    finally:
        receipt.chmod(0o600);os.environ['HIFLD_CONSTRUCTION_MINERALS_CAPTURE_DIR']=str(captures)
    assert receipt.read_bytes()==original_receipt and (invalid/(name+'.body')).read_bytes()==body
    key=f'_raw-captures/{source.SLUG}/{source.digest(body)}/{name}.body';assert staging.read_key(key)==body
    attempt=next((runtime/'staging/_quarantine').rglob('attempted-inputs.json'));obj=json.loads(attempt.read_text())['objects'][name]
    assert obj['body_key']==key and obj['raw_receipt_read'] is False and 'raw_receipt_key' not in obj and not (runtime/'staging/_construction-state').exists()
    first=job.execute_in_process(instance=instance,raise_on_error=False);record('fresh_complete766_native_gold_parity',first);assert first.success
    output=first.output_for_node(asset.op.name);assert output['feature_count']==766 and not output['reused'] and not output['accepted'];prefix=build_publish_partition_key(source.SLUG,source.SLUG,output['version']);root=runtime/'staging'/prefix
    full=source.acquire(staging,str(captures),source.PACKAGE);table,published_ids,audit=source.inspect_captures(full);source.verify_outputs(root,table,published_ids)
    native=root/f'geoparquet/{source.SLUG}.parquet';assert source.sha256_file(native)=='422f2e291993922e37a4008ad03461e631eefc7bc86fe562569ee5e99bd18b7d' and native.stat().st_size==48722
    before=files(root);raw_before={k:v for k,v in snap(staging).items() if k.startswith('_raw-captures/')}
    again=job.execute_in_process(instance=instance,raise_on_error=False);record('idempotent_complete_source_reuse',again);assert again.success and again.output_for_node(asset.op.name)['reused'] and files(root)==before
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
    for cycle in (1,2):
        result=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);errors=record('normal_catalog_'+str(cycle)+'_real_missing_approval_gate',result)
        assert not result.success and errors and all('missing_independent_release_approval_registry' in e for e in errors) and snap(published)==prior
        result=job.execute_in_process(instance=instance,raise_on_error=False);record('source_refresh_after_catalog_'+str(cycle),result);assert result.success
        source.verify_outputs(root,table,published_ids)
    assert raw_before=={k:v for k,v in snap(staging).items() if k.startswith('_raw-captures/')} and snap(published)==prior
    report.update(output=output,source_rows=766,source_schema=str(table.schema),native_parquet_bytes=48722,native_parquet_sha256=source.sha256_file(native),native_gold_parity=True,complete_published_ID_columns=published_ids.column_names,unreadable_receipt_body_retained=True,unread_receipt_bytes_not_invented=True,external_input_unchanged_after_permission_restore=True,all_raw_objects_and_prior_fixture_preserved=True,normal_catalog_cycles=2,release_approved=False,historical_unchanged_gate_scope='21 registered c947 runs retained separately; no current repeated quality/state/cache negatives claimed here')
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
