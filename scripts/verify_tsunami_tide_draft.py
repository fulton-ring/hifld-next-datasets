"""Actual isolated Dagster replay of every original actual official NOAA archival tide station.

No real dataset is accepted. Synthetic prior output tests failed-gate preservation.
"""
from __future__ import annotations
import argparse
import json
import os
import subprocess
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser()
parser.add_argument('--capture-dir',type=Path,default=Path(__file__).parents[1]/'docs/dataset-sources/tsunami-capable-tide-stations/evidence/inputs')
parser.add_argument('--runtime-dir',type=Path,required=True)
args=parser.parse_args()
runtime=args.runtime_dir.resolve();capture=args.capture_dir.resolve()
if runtime.exists() and any(runtime.iterdir()): raise ValueError('Use an empty isolated runtime directory.')
for directory in ['dagster','published','staging']:(runtime/directory).mkdir(parents=True,exist_ok=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_TSUNAMI_TIDE_CAPTURE_DIR=str(capture),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import Definitions,define_asset_job,AssetSelection,DagsterInstance,AssetKey
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.checks import publish_catalog_quality_manifest
from dagster_hifld.assets.noaa_tsunami_tide_stations import noaa_tsunami_tide_stations as source
from dagster_hifld.sources import tsunami_tide_stations as c
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
stage=c.StagingStorageResource(local_dir=str(runtime/'staging'));publish=c.PublishedStorageResource(local_dir=str(runtime/'published'))
publish.write_key('synthetic-accepted-fixture.bin',b'Keep this synthetic prior accepted fixture; no real NOAA source accepted.')
prior={k:publish.read_key(k) for k in publish.list_prefix()}
jobs=[normal.resolve_job_def('tsunami_tide_source_integrity'),define_asset_job('tsunami_tide_stations_formats',selection=AssetSelection.assets(*[k for k in normal.resolve_asset_graph().get_all_asset_keys() if k.path[0]=='publish'])),define_asset_job('tsunami_tide_stations_bad_quality',selection=AssetSelection.assets(AssetKey(['publish','verify']),AssetKey(['publish','promote']))|AssetSelection.checks(publish_catalog_quality_manifest))]
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':stage,'published_storage':publish},jobs=jobs)
report={'classification':'Actual complete local draft, NOT real source/baseline/contract acceptance','code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[1],text=True).strip(),'module':c.__file__,'runs':[]}
def record(label,result):
    errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
    report['runs'].append({'label':label,'run_id':result.run_id,'success':result.success,'errors':errors})
    return errors
with patch('dagster_hifld.assets.publish.process_layer_partitioned_geoparquet',side_effect=AssertionError('Forbidden generic regeneration of native prebuilt GeoParquet')) as converter,DagsterInstance.get() as instance:
    for label in ['source','refresh']:
        result=defs.resolve_job_def('tsunami_tide_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
        errors=record(label,result);assert result.success,errors
        output=result.output_for_node(source.op.name);assert output['feature_count']==374
        report['runs'][-1]['output']=output
    version=output['version'];prefix=f'{c.SLUG}/{c.SLUG}/{version}';root=runtime/'staging'/prefix
    assert (c.SLUG,c.SLUG,version) in set(_iter_staged_version_paths(stage))
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
    before={key:stage.sha256_key(key) for key in stage.list_prefix('_raw-captures')}
    source_pins=c.strict_json((root/c.READY).read_bytes())['objects']
    for cycle in [1,2]:
        result=defs.resolve_job_def('tsunami_tide_stations_formats').execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
        errors=record(f'normal_catalog_formats_cycle_{cycle}_unapproved_promotion',result)
        assert not result.success and errors and all('missing_independent_release_approval_registry' in error for error in errors),errors
        result=defs.resolve_job_def('tsunami_tide_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
        errors=record(f'post_catalog_cycle_{cycle}_source_refresh',result);assert result.success,errors
        assert result.output_for_node(source.op.name)['reused'] is True
    quality_key=prefix+'/metadata/quality_manifest.json';saved=stage.read_key(quality_key)
    injected=json.loads(saved);injected['quality_check_passed']=False
    injected['test_only_injected_failure']='Synthetic false quality flag on full valid real candidate; not a finding that producer geometry is invalid.'
    stage.write_key(quality_key,c.encode(injected))
    try:
        result=defs.resolve_job_def('tsunami_tide_stations_bad_quality').execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
        record('synthetic_failed_quality_blocks_promote',result)
        assert not result.success and any(not check.passed for check in result.get_asset_check_evaluations())
        assert not any(e.is_step_success and e.step_key=='publish__promote' for e in result.all_events)
    finally:stage.write_key(quality_key,saved)
    result=defs.resolve_job_def('tsunami_tide_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
    errors=record('post_gate_source_refresh',result);assert result.success,errors
    manifest=c.strict_json((root/'metadata/source_manifest.json').read_bytes());c.validate_release_provenance(manifest)
    dictionary=c.strict_json((root/'metadata/data_dictionary.json').read_bytes())
    assert dictionary['inputs']==manifest['inputs'] and len(manifest['inputs'])==18
    missing_ready=(root/c.READY).read_bytes()
    (root/c.READY).unlink()
    result=defs.resolve_job_def('tsunami_tide_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
    errors=record('missing_ready_marker_cache_rejected_and_wholly_quarantined',result)
    assert not result.success and any('invalid_cache_moved_intact_out_of_discovery' in error for error in errors)
    assert not root.exists() and (c.SLUG,c.SLUG,version) not in set(_iter_staged_version_paths(stage))
    result=defs.resolve_job_def('tsunami_tide_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
    errors=record('original_source_rebuild_after_quarantine',result);assert result.success,errors
    assert result.output_for_node(source.op.name)['reused'] is False
    result=defs.resolve_job_def('tsunami_tide_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
    errors=record('rebuilt_cache_refresh',result);assert result.success,errors
    assert result.output_for_node(source.op.name)['reused'] is True
    current={key:stage.sha256_key(key) for key in stage.list_prefix('_raw-captures')}
    assert current==before
    assert all(c.sha256_file(root/name)==pin for name,pin in source_pins.items())
    assert prior=={k:publish.read_key(k) for k in publish.list_prefix()}
    raw={n:((capture/(n+'.body')).read_bytes(),c.strict_json((capture/(n+'.receipt.json')).read_bytes())) for n in c.URLS}
    table,audit=c.inspect_originals(raw)
    c.reconcile_outputs(root,table)
    terminal=c.pq.read_table(root/'geoparquet'/f'{c.SLUG}.parquet')
    assert terminal.equals(c.expected_parquet_table(table),check_metadata=True)
    assert terminal.num_rows==374 and terminal['OBJECTID'].to_pylist()==list(range(1,965))
    geo=json.loads(terminal.schema.metadata[b'geo'])
    assert c.pyproj.CRS(geo['columns']['wkb_geometry']['crs']).equals(c.pyproj.CRS(3857))
    assert converter.call_count==0
    report.update(version=version,discoverable_without_runner_copies=True,original_capture_objects_unchanged=True,source_owned_objects_unchanged=True,prior_accepted_fixture_unchanged=True,
        full_374_rows_ids_attrs_types_native_wkb_and_coverage_exact=True,native_points_preserved=True,all_eighteen_input_provenance_records_retained=True,generic_converter_calls=converter.call_count,
        terminal_extra_columns_unapproved=sorted(set(terminal.column_names)-set(c.FIELDS)),source_objects=source_pins,
        quality_manifest=json.loads(saved),source_manifest=manifest,actual_source_OBJECTID_count=len(set(table['OBJECTID'].to_pylist())))
(runtime/'materialization.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'runs':[(x['label'],x['run_id'],x['success']) for x in report['runs']],'prior_unchanged':True}))
