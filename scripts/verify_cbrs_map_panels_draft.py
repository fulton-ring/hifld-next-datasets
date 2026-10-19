"""Actual isolated Dagster source and normal format/gate runs on all574real map panels."""
from __future__ import annotations
import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);args=parser.parse_args()
repo=Path(__file__).resolve().parents[1];capture=repo/'docs/dataset-sources/cbrs-map-panels-1/evidence/captures'
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
from dagster_hifld.assets.fws_map_panels import fws_cbrs_map_panels
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import cbrs_map_panels as source

staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True)
published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
published.write_key('synthetic-accepted-fixture.bin',b'local-fixture-only-not-an-accepted-real-release')
def snapshot(storage):return {key:source.sha(storage.read_key(key)) for key in storage.list_prefix()}
prior=snapshot(published)
normalkeys=[key for key in normal.resolve_asset_graph().get_all_asset_keys() if key.path[0]=='publish']
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},
 jobs=[define_asset_job('cbrs_source',selection=AssetSelection.assets(fws_cbrs_map_panels)),define_asset_job('cbrs_graph',selection=AssetSelection.assets(*normalkeys))])
report={'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'module':source.__file__,'runtime':str(runtime),'approved':False,'runs':[]}
config={'ops':{fws_cbrs_map_panels.op.name:{'config':{'capture_dir':str(capture)}}}}
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
 output=first.output_for_node(fws_cbrs_map_panels.op.name);assert output['feature_count']==574
 prefix=build_publish_partition_key(source.SLUG,source.SLUG,output['version']);root=runtime/'staging'/prefix
 assert (source.SLUG,source.SLUG,output['version']) in list(_iter_staged_version_paths(staging))
 assert source.sha((root/'raw'/source.ARCHIVE_NAME).read_bytes())==source.PIN
 before={str(p.relative_to(root)):source.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
 again=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('real_complete_refresh',again);assert again.success and output==again.output_for_node(fws_cbrs_map_panels.op.name)
 assert before=={str(p.relative_to(root)):source.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
 formats=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
 errors=record('normal_formats_missing_approval',formats)
 assert not formats.success and errors and all('missing_independent_release_approval_registry' in error for error in errors),errors
 assert snapshot(published)==prior
 post=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('real_complete_post_formats_refresh',post);assert post.success and post.output_for_node(fws_cbrs_map_panels.op.name)==output
 repeated=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
 errors=record('second_normal_catalog_missing_approval',repeated)
 assert not repeated.success and errors and all('missing_independent_release_approval_registry' in error for error in errors),errors
 aftersecond=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('source_refresh_after_second_catalog',aftersecond);assert aftersecond.success and aftersecond.output_for_node(fws_cbrs_map_panels.op.name)==output
 assert snapshot(published)==prior
 rows=pq.read_table(root/'candidate/typed-source.parquet').to_pylist();assert len(rows)==574
 with fiona.open(root/'geopackage'/f'{source.SLUG}.gpkg') as f:gp={r['properties']['OBJECTID']:r for r in f}
 for row in rows:
  expected=gp[row['OBJECTID']];assert dict(expected['properties'])=={key:row[key] for key in source.SCHEMA}
  assert shapely.geometry.shape(expected.geometry).wkb==row['geometry']
 terminal=pq.read_table(root/'geoparquet'/f'{source.SLUG}.parquet')
 assert len(terminal)==574
 for name,kind in source.SCHEMA.items():
  assert str(terminal.schema.field(name).type)==('double' if kind.startswith('float') else 'int64' if kind.startswith('int') else 'string')
 assert set(terminal.column_names)-set(source.SCHEMA)-{'geometry'}=={'bbox'}
 byoid={row['OBJECTID']:row for row in rows}
 for row in terminal.to_pylist():
  expected=byoid[row['OBJECTID']]
  assert {key:row[key] for key in source.SCHEMA}=={key:expected[key] for key in source.SCHEMA}
  assert row['geometry']==expected['geometry']
 report['terminal_parquet']={'rows':574,'all_source_values_types_wkb_exact':True,'extra_columns':['bbox'],'nullable_inference_requires_review':True}
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
 report.update(output=output,all_574_ids_values_types_wkb_preserved=True,source_discoverable_without_manual_copy=True,refresh_byte_idempotent=True,prior_published_fixture_preserved=True,
  quality=json.loads((root/'metadata/quality_manifest.json').read_text()),provenance_input_count=len(json.loads((root/'metadata/source_manifest.json').read_text())['inputs']))
(runtime/'execution.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'code_head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
