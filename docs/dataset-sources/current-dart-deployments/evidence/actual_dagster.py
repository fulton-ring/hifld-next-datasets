"""Actual isolated Dagster source and normal format/gate runs on all62real DART sites."""
from __future__ import annotations
import argparse
import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);args=parser.parse_args()
repo=Path(__file__).resolve().parents[4];capture=repo/'docs/dataset-sources/current-dart-deployments/evidence/captures'
runtime=args.runtime.resolve();assert not runtime.exists()
for name in ['dagster','staging','published']:(runtime/name).mkdir(parents=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
import fiona
import pyarrow.parquet as pq
import shapely
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.noaa_current_dart import noaa_current_dart
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import current_dart as source

staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True)
published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
published.write_key('synthetic-accepted-fixture.bin',b'local-fixture-only-not-an-accepted-real-release')
def snapshot(storage):return {key:source.sha(storage.read_key(key)) for key in storage.list_prefix()}
prior=snapshot(published)
normalkeys=[key for key in normal.resolve_asset_graph().get_all_asset_keys() if key.path[0]=='publish']
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},
 jobs=[define_asset_job('dart_source',selection=AssetSelection.assets(noaa_current_dart)),define_asset_job('dart_graph',selection=AssetSelection.assets(*normalkeys))])
report={'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'module':source.__file__,'runtime':str(runtime),'approved':False,'runs':[]}
config={'ops':{noaa_current_dart.op.name:{'config':{'capture_dir':str(capture)}}}}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,
  'materializations':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()],
  'checks':[{'name':e.check_name,'passed':e.passed} for e in result.get_asset_check_evaluations()]})
 (runtime/'execution-progress.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
 return errors
with DagsterInstance.get() as instance:
 sourcejob=normal.resolve_job_def('noaa_dart_draft_refresh');graph=defs.resolve_job_def('dart_graph')
 first=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('real_complete_source',first);assert first.success
 output=first.output_for_node(noaa_current_dart.op.name);assert output['feature_count']==62
 prefix=build_publish_partition_key(source.SLUG,source.SLUG,output['version']);root=runtime/'staging'/prefix
 assert (source.SLUG,source.SLUG,output['version']) in list(_iter_staged_version_paths(staging))
 assert source.sha((root/'raw'/source.ARCHIVE_NAME).read_bytes())==source.PIN
 before={str(p.relative_to(root)):source.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
 again=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('real_complete_refresh',again);assert again.success and output==again.output_for_node(noaa_current_dart.op.name)
 assert before=={str(p.relative_to(root)):source.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
 formats=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
 errors=record('normal_formats_missing_approval',formats)
 assert not formats.success and errors and all('missing_independent_release_approval_registry' in error for error in errors),errors
 assert snapshot(published)==prior
 post=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('real_complete_post_formats_refresh',post);assert post.success and post.output_for_node(noaa_current_dart.op.name)==output
 assert snapshot(published)==prior
 second=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
 errors=record('second_normal_catalog_missing_approval',second)
 assert not second.success and errors and all('missing_independent_release_approval_registry' in error for error in errors),errors
 for name in ['data_dictionary','quality_manifest']:
  assert (root/f'metadata/source/{name}.json').exists()
 secondpost=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('source_after_second_catalog',secondpost);assert secondpost.success
 assert snapshot(published)==prior
 rows=pq.read_table(root/'candidate/typed-source.parquet').to_pylist();assert len(rows)==62
 with fiona.open(root/'geopackage'/f'{source.SLUG}.gpkg') as f:gp={r['properties']['OBJECTID']:r for r in f}
 for row in rows:
  expected=gp[row['OBJECTID']];assert dict(expected['properties'])=={key:row[key] for key in source.SCHEMA}
  assert shapely.geometry.shape(expected.geometry).wkb==row['geometry']
 terminal=pq.read_table(root/'geoparquet'/f'{source.SLUG}.parquet')
 assert len(terminal)==62 and str(terminal.schema.field('OBJECTID').type)=='int32'
 bysection={row['OBJECTID']:row for row in rows}
 for row in terminal.to_pylist():
  expected=bysection[row['OBJECTID']]
  assert {key:row[key] for key in source.SCHEMA}=={key:expected[key] for key in source.SCHEMA}
  assert row['geometry']==expected['geometry']
 report['terminal_parquet']={'rows':62,'all_source_values_types_wkb_exact':True,'prebuilt_parquet_bytes_equal':(root/'candidate/typed-source.parquet').read_bytes()==(root/'geoparquet'/f'{source.SLUG}.parquet').read_bytes(),'source_nullable_declarations_preserved':True}
 # Corrupt a separate explicitly synthetic test partition, leaving the real candidate unchanged.
 badprefix=prefix+'-synthetic-invalid-geometry';badroot=runtime/'staging'/badprefix;shutil.copytree(root,badroot)
 gpfile=badroot/'geopackage'/f'{source.SLUG}.gpkg'
 with fiona.open(gpfile) as f:schema=f.schema;crs=f.crs;features=[{'properties':dict(r['properties']),'geometry':r.geometry} for r in f]
 features[0]['geometry']=None;gpfile.unlink()
 with fiona.open(gpfile,'w',driver='GPKG',layer=source.SLUG,schema=schema,crs=crs) as f:f.writerecords(features)
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix])
 bad=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False)
 record('synthetic_failed_geometry_quality_gate',bad);assert not bad.success
 assert any(not e.passed for e in bad.get_asset_check_evaluations())
 assert snapshot(published)==prior
 real_quality=json.loads((root/'metadata/quality_manifest.json').read_text())
 report['cache_metadata_probes']=[]
 for label,sql in [('contents_crs','UPDATE gpkg_contents SET srs_id=4326'),
   ('authority','UPDATE gpkg_spatial_ref_sys SET organization_coordsys_id=4326 WHERE srs_id=3857'),
   ('ancillary_table','CREATE TABLE unexpected_metadata (value TEXT)')]:
  gp=root/'geopackage'/f'{source.SLUG}.gpkg';marker=root/'metadata/current_dart_candidate_ready.json'
  with sqlite3.connect(gp) as db:db.execute(sql)
  ready=json.loads(marker.read_text());ready['objects'][f'geopackage/{source.SLUG}.gpkg']=source.sha(gp.read_bytes());marker.write_text(json.dumps(ready))
  altered={str(p.relative_to(root)):source.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
  negative=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
  errors=record('metadata_self_repin_'+label,negative)
  assert not negative.success and errors and any('cached_geopackage_canonical_container_differs' in e for e in errors),errors
  assert not root.exists()
  retained=[q for q in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-cache-*/candidate') if {str(p.relative_to(q)):source.sha(p.read_bytes()) for p in q.rglob('*') if p.is_file()}==altered]
  assert len(retained)==1 and snapshot(published)==prior
  healthy=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
  record('fresh_source_after_quarantine_'+label,healthy);assert healthy.success
  assert snapshot(published)==prior
  report['cache_metadata_probes'].append({'kind':label,'failure_run_id':negative.run_id,'fresh_source_run_id':healthy.run_id,'quarantine':str(retained[0]),'all_failed_cache_bytes_preserved':True})
 report.update(output=output,all_62_ids_values_types_wkb_preserved=True,source_discoverable_without_manual_copy=True,refresh_byte_idempotent=True,prior_published_fixture_preserved=True,
  quality=real_quality,provenance_input_count=len(json.loads((root/'metadata/source_manifest.json').read_text())['inputs']))
(runtime/'execution.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'code_head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
