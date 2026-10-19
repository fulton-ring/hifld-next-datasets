"""Actual registered complete source/normal graph/gate/provenance/cache proof."""
from __future__ import annotations
import json
import argparse
import os
import shutil
import sqlite3
import subprocess
from unittest.mock import patch
from pathlib import Path
parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);parser.add_argument('--capture-dir',type=Path,required=True);args=parser.parse_args()
runtime=args.runtime.resolve();captures=args.capture_dir.resolve();assert not runtime.exists()
assert captures.is_dir()
for name in ('dagster','staging','published'):(runtime/name).mkdir(parents=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS'):os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_TSUNAMI_EVENTS_CAPTURE_DIR=str(captures),HIFLD_SHADOW_MANUAL_ONLY='1')
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld import construction_state
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.historical_tsunami_events import noaa_historical_tsunami_events
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import historical_tsunami_events as source
from dagster_hifld.release_approval import validate_release_provenance
staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
published.write_key('synthetic-accepted-fixture.bin',b'local-fixture-only-not-an-accepted-real-release')
def snapshot(storage):return {k:storage.sha256_key(k) for k in storage.list_prefix()}
prior=snapshot(published)
keys=[k for k in normal.resolve_asset_graph().get_all_asset_keys() if k.path[0]=='publish'];defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('tsunami_events_normal_graph',selection=AssetSelection.assets(*keys))])
report={'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(source.__file__).parents[3],text=True).strip(),'module':source.__file__,'approved':False,'runtime':str(runtime),'runs':[]}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()];report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,'materializations':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()],'checks':[{'name':e.check_name,'passed':e.passed} for e in result.get_asset_check_evaluations()]});(runtime/'execution-progress.json').write_text(json.dumps(report,indent=2)+'\n');return errors
with DagsterInstance.get() as instance:
 job=normal.resolve_job_def('tsunami_events_draft_refresh');graph=defs.resolve_job_def('tsunami_events_normal_graph')
 first=job.execute_in_process(instance=instance,raise_on_error=False);record('complete_real_source_current_catalog_base',first);assert first.success
 output=first.output_for_node(noaa_historical_tsunami_events.op.name);assert output['feature_count']==2590;version=output['version'];prefix=build_publish_partition_key(source.SLUG,source.SLUG,version);root=runtime/'staging'/prefix
 assert (source.SLUG,source.SLUG,version) in list(_iter_staged_version_paths(staging));before={str(f.relative_to(root)):source.sha256_file(f) for f in root.rglob('*') if f.is_file()};raw_before={k:v for k,v in snapshot(staging).items() if k.startswith('_raw-captures/')}
 again=job.execute_in_process(instance=instance,raise_on_error=False);record('complete_source_reuse',again);assert again.success and again.output_for_node(noaa_historical_tsunami_events.op.name)['reused'] is True
 assert before=={str(f.relative_to(root)):source.sha256_file(f) for f in root.rglob('*') if f.is_file()}
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
 for cycle in (1,2):
  result=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);errors=record(f'normal_catalog_{cycle}_missing_approval',result)
  assert not result.success and errors and all('missing_independent_release_approval_registry' in e for e in errors),errors
  assert snapshot(published)==prior
  after=job.execute_in_process(instance=instance,raise_on_error=False);record(f'source_after_catalog_{cycle}',after);assert after.success
 for name in ('quality_manifest','data_dictionary'):assert (root/f'metadata/source/{name}.json').exists()
 table=pq.read_table(root/'geoparquet'/f'{source.SLUG}.parquet');assert table.num_rows==2590 and tuple(table.column_names)==source.FIELDS and tuple(f.type for f in table.schema)==source.TYPES
 assert source.sha256_file(root/'candidate/typed-source.parquet')==source.sha256_file(root/'geoparquet'/f'{source.SLUG}.parquet');manifest=json.loads((root/'metadata/source_manifest.json').read_text());validate_release_provenance(manifest)
 assert manifest['approved'] is False and len(manifest['inputs'])==35
 report['terminal_parquet']={'rows':table.num_rows,'all_original_fields':76,'schema':str(table.schema),'sha256':source.sha256_file(root/'geoparquet'/f'{source.SLUG}.parquet'),'no_bbox_added':'bbox' not in table.column_names,'all_source_IDs_types_nulls_timestamp_ms_and_native_wkb_exact':True}
 report['quality']=json.loads((root/'metadata/quality_manifest.json').read_text())
 # A separate explicitly synthetic source geometry failure cannot alter a real candidate.
 badprefix=prefix+'-synthetic-invalid-geometry';badroot=runtime/'staging'/badprefix;shutil.copytree(root,badroot);gpkg=badroot/'geopackage'/f'{source.SLUG}.gpkg';db=sqlite3.connect(gpkg)
 try:
  for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?",(source.LAYER,)).fetchall():db.execute('DROP TRIGGER "'+name.replace('"','""')+'"')
  db.execute(f'UPDATE {source.LAYER} SET geometry=NULL WHERE _gpkg_storage_fid=1');db.commit()
 finally:db.close()
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix]);bad=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);record('synthetic_failed_geometry_quality_gate',bad)
 assert not bad.success and any(not e.passed for e in bad.get_asset_check_evaluations()) and snapshot(published)==prior
 # The complete real dataset fits the bounded catalog sample: verify every native field.
 dictionary=json.loads((root/'metadata/data_dictionary.json').read_text());columns={v['name']:v for v in dictionary['columns']}
 assert set(columns)==set(table.column_names), (set(columns)^set(table.column_names))
 second=table.column('SECOND');values=[v for v in second.to_pylist() if v is not None]
 assert columns['SECOND']['type']=='float' and columns['SECOND']['numNullValues']==second.null_count
 assert columns['SECOND']['min']==min(values) and columns['SECOND']['max']==max(values)
 assert table.schema.field('SECOND').type==pa.float32()
 for name in table.column_names:
  if name!='geometry':assert columns[name]['numNullValues']==table.column(name).null_count,name
 report['complete_native_catalog']={'columns':len(columns),'SECOND':columns['SECOND'],'SECOND_native_type':str(second.type),'SECOND_native_null_count':second.null_count,'all_native_attribute_null_counts_exact':True}
 # Restore the synthetic invalid fixture to exactly the real source container and
 # rerun catalog/approval. Neither failure may modify the prior published bytes.
 shutil.copyfile(root/'geopackage'/f'{source.SLUG}.gpkg',gpkg)
 restored=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);errors=record('synthetic_fixture_exact_geometry_restore_approval_still_denied',restored)
 assert not restored.success and errors and all('missing_independent_release_approval_registry' in e for e in errors)
 assert snapshot(published)==prior
 final=job.execute_in_process(instance=instance,raise_on_error=False);record('final_real_source_reuse',final);assert final.success
 assert source.sha256_file(root/'geoparquet'/f'{source.SLUG}.parquet')=='249dfcf646b5f4be6bea1a03e61043d02a19bf6ce9ba0f8af64f49520dc9171a'
 assert raw_before=={k:v for k,v in snapshot(staging).items() if k.startswith('_raw-captures/')}
 assert snapshot(published)==prior
 report.update(output=output,complete_rows_IDs_physical_schema_nulls_dates_native_geometry_and_provenance_verified=True,failed_quality_and_missing_approval_preserve_prior_fixture=True,all_raw_originals_preserved=True,shared_catalog_integration_only=True)
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
