"""Actual registered metadata JSON-type repair proof; no release acceptance."""
from __future__ import annotations
import argparse
import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);parser.add_argument('--capture-dir',type=Path,required=True);args=parser.parse_args()
runtime=args.runtime.absolute();captures=args.capture_dir.absolute();assert not runtime.exists() and captures.is_dir()
for name in ('dagster','staging','published'):(runtime/name).mkdir(parents=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS'):os.environ.pop(key,None)
inputs=runtime/'inputs';shutil.copytree(captures,inputs)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_FEMA_OFFICES_CAPTURE_DIR=str(inputs),HIFLD_SHADOW_MANUAL_ONLY='1')
import pyarrow.parquet as pq
from dagster import AssetSelection, DagsterInstance, Definitions, define_asset_job
from dagster_hifld.definitions import defs as normal, _iter_staged_version_paths
from dagster_hifld.assets.fema_regional_offices import fema_regional_offices
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource, PublishedStorageResource
from dagster_hifld.sources import fema_regional_offices as s
from dagster_hifld.release_approval import validate_release_provenance

staging=StagingStorageResource(local_dir=str(runtime/'staging'));published=PublishedStorageResource(local_dir=str(runtime/'published'))
published.write_key('synthetic-prior-fixture.bin',b'local-preservation-fixture-only-no-real-accepted-release')
def snapshot(path):return {str(p.relative_to(path)):s.sha256_file(p) for p in path.rglob('*') if p.is_file()}
prior=snapshot(runtime/'published');original_inputs=snapshot(inputs)
keys=[key for key in normal.resolve_asset_graph().get_all_asset_keys() if key.path[0]=='publish']
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('fema_offices_local_normal_graph',selection=AssetSelection.assets(*keys))])
report={'approved':False,'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(s.__file__).parents[3],text=True).strip(),'module':s.__file__,'runtime':str(runtime),'runs':[],'scope':'Complete real captured14-office input, unpatched source/candidate/Git/state/catalog/gates; explicitly labelled fixture corruption/fault probes. Live acquisition tested separately.'}
def record(kind,result):
    errors=[event.event_specific_data.error.to_string() for event in result.get_step_failure_events()]
    report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,'materializations':[event.asset_key.to_user_string() for event in result.get_asset_materialization_events()],'checks':[{'name':event.check_name,'passed':event.passed} for event in result.get_asset_check_evaluations()]})
    (runtime/'execution-progress.json').write_bytes(s.encode(report));assert snapshot(runtime/'published')==prior
    return errors

def move_incident(version,label):
    # Test operator explicitly retains the negative journal OUTSIDE staging;
    # restoring exact trusted originals alone never silently clears an incident.
    directory=runtime/'staging'/s.incident_directory(version)
    if directory.exists():directory.rename(runtime/('operator-reviewed-fixture-incident-'+label))

with DagsterInstance.get() as instance:
    job=normal.resolve_job_def('fema_regional_offices_draft_refresh');graph=defs.resolve_job_def('fema_offices_local_normal_graph')
    def refresh(label,expected=None):
        result=job.execute_in_process(instance=instance,raise_on_error=False);errors=record(label,result)
        if expected is None:assert result.success,errors
        else:assert not result.success and any(expected in error for error in errors),errors
        return result
    input_bytes={p:p.read_bytes() for p in inputs.iterdir()}
    metadata=inputs/'metadata_after.body';value=json.loads(metadata.read_bytes());assert value['fields'][0]['nullable'] is False;value['fields'][0]['nullable']=0;body=s.encode(value);metadata.write_bytes(body)
    p=inputs/'metadata_after.receipt.json';receipt=json.loads(p.read_bytes());receipt.update(sha256=s.digest(body),bytes=len(body));p.write_bytes(s.encode(receipt))
    for number in (1,2):
        refresh('synthetic_metadata_nullable_false_to_numeric_zero_'+str(number),'source_schema_physical_types_geometry_or_layer_identity_drift')
        assert not (runtime/'staging'/'_construction-state').exists() and not (runtime/'staging'/s.SLUG).exists()
    assert (runtime/'staging'/f'_raw-captures/{s.SLUG}/{s.digest(body)}/metadata_after.body').read_bytes()==body
    report['malformed_json_type_fails_before_state_and_candidate']=True
    for p,body in input_bytes.items():p.write_bytes(body)
    first=refresh('complete_real_fourteen_office_after_type_repair');output=first.output_for_node(fema_regional_offices.op.name);assert output['feature_count']==14
    version=output['version'];prefix=build_publish_partition_key(s.SLUG,s.SLUG,version);root=runtime/'staging'/prefix;native=root/'geoparquet'/f'{s.SLUG}.parquet';gold=native.read_bytes();manifest=(root/'metadata/source_manifest.json').read_bytes()
    assert s.digest(gold)=='dd521c10f9efc35fbb1c8dcf71fcc346ddaa649d7d69bd1d9797144154291fa9'
    expected,source_audit=s.inspect_captures({name:((inputs/(name+'.body')).read_bytes(),json.loads((inputs/(name+'.receipt.json')).read_bytes())) for name in s.request_plan(list(range(1,15)))})
    assert pq.read_table(native).equals(s.expected_parquet(expected),check_metadata=True);s.verify_outputs(root,expected);validate_release_provenance(json.loads(manifest))
    before=snapshot(root);again=refresh('complete_source_idempotent_after_repair');assert again.output_for_node(fema_regional_offices.op.name)['reused'] is True and snapshot(root)==before
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
    for cycle in (1,2):
        result=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);errors=record('normal_catalog_'+str(cycle)+'_real_missing_release_registry',result)
        assert not result.success and errors and all('missing_independent_release_approval_registry' in error for error in errors),errors
        refresh('source_after_catalog_'+str(cycle));assert native.read_bytes()==gold and (root/'metadata/source_manifest.json').read_bytes()==manifest
    report.update(output=output,native_sha256=s.digest(gold),native_bytes=len(gold),native_all_original_rows_fields_nulls_IDs_and_WKB_equal=True,prior_fixture_preserved=snapshot(runtime/'published')==prior,all_original_inputs_preserved=snapshot(inputs)==original_inputs,source_audit=source_audit,quality=json.loads((root/'metadata/quality_manifest.json').read_bytes()),actual_run_count=len(report['runs']))
(runtime/'execution.json').write_bytes(s.encode(report));print(json.dumps({'head':report['code_head'],'runs':[(run['kind'],run['run_id'],run['success']) for run in report['runs']]}))
