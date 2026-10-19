"""Actual isolated Dagster source and normal format/gate runs on all942real CBRS units."""
from __future__ import annotations
import argparse
import json
import os
import shutil
import subprocess
import sqlite3
import pyarrow as pa
from pathlib import Path

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);args=parser.parse_args()
repo=Path(__file__).resolve().parents[1];capture=repo/'docs/dataset-sources/cbrs-units-2/evidence/captures'
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
from dagster_hifld.assets.fws_units import fws_cbrs_units
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import cbrs_units as source

staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True)
published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
published.write_key('synthetic-accepted-fixture.bin',b'local-fixture-only-not-an-accepted-real-release')
def snapshot(storage):return {key:source.sha(storage.read_key(key)) for key in storage.list_prefix()}
prior=snapshot(published)
normalkeys=[key for key in normal.resolve_asset_graph().get_all_asset_keys() if key.path[0]=='publish']
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},
 jobs=[define_asset_job('cbrs_source',selection=AssetSelection.assets(fws_cbrs_units)),define_asset_job('cbrs_graph',selection=AssetSelection.assets(*normalkeys))])
report={'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'module':source.__file__,'runtime':str(runtime),'approved':False,'runs':[]}
config={'ops':{fws_cbrs_units.op.name:{'config':{'capture_dir':str(capture)}}}}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,
  'materializations':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()],
  'checks':[{'name':e.check_name,'passed':e.passed} for e in result.get_asset_check_evaluations()]})
 return errors
with DagsterInstance.get() as instance:
 sourcejob=defs.resolve_job_def('cbrs_source');graph=defs.resolve_job_def('cbrs_graph')
 first=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('real_complete_source',first);assert first.success
 output=first.output_for_node(fws_cbrs_units.op.name);assert output['feature_count']==942
 prefix=build_publish_partition_key(source.SLUG,source.SLUG,output['version']);root=runtime/'staging'/prefix
 assert (source.SLUG,source.SLUG,output['version']) in list(_iter_staged_version_paths(staging))
 assert source.sha((root/'raw'/source.ARCHIVE_NAME).read_bytes())==source.PIN
 before={str(p.relative_to(root)):source.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
 again=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('real_complete_refresh',again);assert again.success and output==again.output_for_node(fws_cbrs_units.op.name)
 assert before=={str(p.relative_to(root)):source.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
 formats=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
 errors=record('normal_formats_missing_approval',formats)
 assert not formats.success and errors and all('missing_independent_release_approval_registry' in error for error in errors),errors
 assert snapshot(published)==prior
 post=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('real_complete_post_formats_refresh',post);assert post.success and post.output_for_node(fws_cbrs_units.op.name)==output
 assert snapshot(published)==prior
 # The second normal catalog snapshots its previously generated dictionary/quality.
 second_formats=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
 second_errors=record('second_normal_formats_missing_approval',second_formats)
 assert not second_formats.success and second_errors and all('missing_independent_release_approval_registry' in e for e in second_errors)
 assert (root/'metadata/source/data_dictionary.json').exists() and (root/'metadata/source/quality_manifest.json').exists()
 second_refresh=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('real_complete_post_second_catalog_refresh',second_refresh)
 assert second_refresh.success and second_refresh.output_for_node(fws_cbrs_units.op.name)==output
 assert snapshot(published)==prior
 rows=pq.read_table(root/'candidate/typed-source.parquet').to_pylist();assert len(rows)==942
 with fiona.open(root/'geopackage'/f'{source.SLUG}.gpkg') as f:gp={r['properties']['__source_fid']:r for r in f}
 for row in rows:
  expected=gp[row['__source_fid']];assert dict(expected['properties'])=={key:row[key].isoformat() if hasattr(row[key],'isoformat') else row[key] for key in source.SCHEMA}
  assert shapely.geometry.shape(expected.geometry).wkb==row['geometry']
 terminal=pq.read_table(root/'geoparquet'/f'{source.SLUG}.parquet')
 assert len(terminal)==942
 for name,kind in source.SCHEMA.items():
  assert str(terminal.schema.field(name).type)==('double' if kind.startswith('float') else 'date32[day]' if kind=='date' else 'string')
 assert set(terminal.column_names)-set(source.SCHEMA)-{'geometry'}==set()
 bysection={row['__source_fid']:row for row in rows}
 for row in terminal.to_pylist():
  expected=bysection[row['__source_fid']]
  assert {key:row[key] for key in source.SCHEMA}=={key:expected[key] for key in source.SCHEMA}
  assert row['geometry']==expected['geometry']
 report['terminal_parquet']={'rows':942,'all_source_values_types_wkb_exact':True,'extra_columns':[],'nullable_inference_requires_review':True}
 # Authentic complete-candidate self-repin attacks exercise the actual registered Dagster asset.
 readyfile=root/'metadata/cbrs_units_candidate_ready.json';ready_original=readyfile.read_bytes()
 gpfile=root/'geopackage'/f'{source.SLUG}.gpkg';gp_original=gpfile.read_bytes()
 parquet=root/'candidate/typed-source.parquet';parquet_original=parquet.read_bytes()
 report['self_repin_probes']=[]
 for attack in ['gpkg_contents_srs','gpkg_spatial_ref_definition','parquet_geo_crs']:
  try:
   if attack.startswith('gpkg'):
    with sqlite3.connect(gpfile) as db:
     db.execute("UPDATE gpkg_contents SET srs_id=4326" if attack=='gpkg_contents_srs' else "UPDATE gpkg_spatial_ref_sys SET definition='undefined' WHERE srs_id=4269")
    target=gpfile
   else:
    table=pq.read_table(parquet);metadata=dict(table.schema.metadata);geo=json.loads(metadata[b'geo']);geo['columns']['geometry']['crs']={'type':'name','properties':{'name':'EPSG:3857'}}
    metadata[b'geo']=source.encode(geo);pq.write_table(table.replace_schema_metadata(metadata),parquet,compression='zstd');target=parquet
   ready=json.loads(ready_original);ready['objects'][str(target.relative_to(root))]=source.sha(target.read_bytes());readyfile.write_bytes(source.encode(ready))
   negative=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
   errors=record('complete_source_cache_self_repin_'+attack,negative)
   assert not negative.success and errors
   assert snapshot(published)==prior
   report['self_repin_probes'].append({'attack':attack,'run_id':negative.run_id,'rejected':True,'prior_published_preserved':True})
  finally:
   gpfile.write_bytes(gp_original);parquet.write_bytes(parquet_original);readyfile.write_bytes(ready_original)
 assert all((root/key).read_bytes() and source.sha((root/key).read_bytes())==digest for key,digest in json.loads(ready_original)['objects'].items())
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
 assert (root/'candidate/typed-source.parquet').read_bytes()==(root/'geoparquet'/f'{source.SLUG}.parquet').read_bytes()
 report.update(output=output,all_942_ids_values_types_wkb_preserved=True,source_discoverable_without_manual_copy=True,refresh_byte_idempotent=True,prior_published_fixture_preserved=True,
  quality=json.loads((root/'metadata/quality_manifest.json').read_text()),provenance_input_count=len(json.loads((root/'metadata/source_manifest.json').read_text())['inputs']))
(runtime/'execution.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'code_head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
