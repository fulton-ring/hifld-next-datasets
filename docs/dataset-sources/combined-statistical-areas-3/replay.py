import os,json
from pathlib import Path
import argparse
parser=argparse.ArgumentParser();parser.add_argument('--capture-dir',type=Path,required=True);parser.add_argument('--runtime-dir',type=Path,required=True);args=parser.parse_args()
p=args.capture_dir.resolve();runtime=args.runtime_dir.resolve()
assert not (runtime/'dagster').exists(), 'Use a fresh isolated runtime directory'
for d in ['dagster','published','staging']:(runtime/d).mkdir(parents=True,exist_ok=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_CSA_CAPTURE_DIR=str(p),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import Definitions,define_asset_job,AssetSelection,DagsterInstance,AssetKey
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.combined_statistical_areas import census_combined_statistical_areas as source
from dagster_hifld.sources.combined_statistical_areas import *
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
stage=StagingStorageResource(local_dir=str(runtime/'staging'));publish=PublishedStorageResource(local_dir=str(runtime/'published'))
publish.write_key('synthetic-accepted-fixture.bin',b'Keep this synthetic prior accepted fixture; no real source accepted.')
old={k:publish.read_key(k) for k in publish.list_prefix()}
from dagster_hifld.checks import publish_catalog_quality_manifest
jobs=[define_asset_job('csa_quality_negative',selection=AssetSelection.assets(AssetKey(['publish','verify']),AssetKey(['publish','promote'])) | AssetSelection.checks(publish_catalog_quality_manifest)),define_asset_job('csa_source_test',selection=AssetSelection.assets(source)),define_asset_job('csa_format_test',selection=AssetSelection.assets(*[k for k in normal.resolve_asset_graph().get_all_asset_keys() if k.path[0]=='publish']))]
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':stage,'published_storage':publish},jobs=jobs)
report={'classification':'Actual complete local draft pipeline; real dataset NOT approved','module':__import__('dagster_hifld').__file__,'runs':[]}
with DagsterInstance.get() as instance:
 for label in ['source','refresh']:
  result=defs.resolve_job_def('csa_source_test').execute_in_process(instance=instance,raise_on_error=False)
  assert result.success,[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
  output=result.output_for_node(source.op.name);assert output['feature_count']==184
  report['runs'].append({'label':label,'run_id':result.run_id,'success':result.success,'output':output})
 version=output['version'];prefix=f'{SLUG}/{SLUG}/{version}'
 assert (SLUG,SLUG,version) in set(_iter_staged_version_paths(stage))
 report['discoverable_without_runner_copies']=True
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
 result=defs.resolve_job_def('csa_format_test').execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'label':'normal_catalog_formats_quality_and_gate','run_id':result.run_id,'success':result.success,'errors':errors})
 assert not result.success and errors and all('missing_independent_release_approval_registry' in e for e in errors),errors
 quality_key=prefix+'/metadata/quality_manifest.json'
 original_quality=stage.read_key(quality_key)
 synthetic_negative=json.loads(original_quality);synthetic_negative['quality_check_passed']=False
 synthetic_negative['test_only_injected_failure']='Synthetic false quality flag on the full real candidate, not a claim that real geometry failed.'
 stage.write_key(quality_key,encode(synthetic_negative))
 try:
  negative=defs.resolve_job_def('csa_quality_negative').execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
  checks=negative.get_asset_check_evaluations()
  assert not negative.success and any(not check.passed for check in checks)
  assert not any(e.is_step_success and e.step_key=='publish__promote' for e in negative.all_events)
  report['runs'].append({'label':'synthetic_failed_quality_flag_on_full_candidate','run_id':negative.run_id,'success':negative.success,'promote_executed':False})
 finally:
  stage.write_key(quality_key,original_quality)
 refreshed=defs.resolve_job_def('csa_source_test').execute_in_process(instance=instance,raise_on_error=False)
 assert refreshed.success,[e.event_specific_data.error.to_string() for e in refreshed.get_step_failure_events()]
 assert refreshed.output_for_node(source.op.name)['version']==version and refreshed.output_for_node(source.op.name)['reused'] is True
 report['runs'].append({'label':'post_formats_refresh_fresh_original_reconciliation','run_id':refreshed.run_id,'success':refreshed.success})
 report['prior_fixture_unchanged']=old=={k:publish.read_key(k) for k in publish.list_prefix()}
 assert report['prior_fixture_unchanged']
 source_table=pq.read_table(runtime/'staging'/prefix/'candidate/typed-source.parquet')
 terminal_table=pq.read_table(runtime/'staging'/prefix/'geoparquet'/f'{SLUG}.parquet')
 terminal_rows={row['OBJECTID']:row for row in terminal_table.to_pylist()}
 assert len(terminal_rows)==184 and terminal_table.num_rows==184
 for row in source_table.to_pylist():
  actual=terminal_rows[row['OBJECTID']]
  assert all(type(row[key]) is type(actual[key]) and row[key]==actual[key] for key in FIELDS if key!='SHAPE')
  assert row['SHAPE']==actual['geometry']
 assert pyproj.CRS(json.loads(terminal_table.schema.metadata[b'geo'])['columns']['geometry']['crs']).equals(pyproj.CRS(4269))
 for field in source_table.schema:
  if field.name!='SHAPE': assert terminal_table.schema.field(field.name).type==field.type
 report['terminal_all184_rows_all_original_ids_attrs_values_types_and_native_wkb_exact']=True
 report['terminal_extra_columns_unapproved']=list(set(terminal_table.column_names)-set(FIELDS))
 report['version']=version
 report['quality_manifest']=json.loads(stage.read_key(prefix+'/metadata/quality_manifest.json'))
 report['source_manifest_validates']=validate_release_provenance(json.loads(stage.read_key(prefix+'/metadata/source_manifest.json'))) is None
 report['candidate_ready']=json.loads(stage.read_key(prefix+'/metadata/csa_candidate_ready.json'))
 (runtime/'materialization.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
 print(json.dumps({'runs':[(x['label'],x['run_id'],x['success']) for x in report['runs']],'prior_unchanged':report['prior_fixture_unchanged']}))
