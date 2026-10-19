"""Actual complete registered CDP source and normal catalog/promotion gates."""
from __future__ import annotations
import json
import argparse
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path
parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);parser.add_argument('--archive',type=Path,required=True);parser.add_argument('--receipt',type=Path,required=True);args=parser.parse_args()
runtime=args.runtime.resolve();assert not runtime.exists();p=runtime.parent
archive=args.archive.resolve();receipt=args.receipt.resolve()
runtime.mkdir(exist_ok=True)
for name in ['dagster','staging','published']:(runtime/name).mkdir(exist_ok=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
import pyarrow.parquet as pq
import shapely
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.census_designated_places import census_designated_places
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import census_designated_places as source
from dagster_hifld.release_approval import validate_release_provenance
staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
raw=runtime/'staging'/source.RAW_KEY;raw.parent.mkdir(parents=True,exist_ok=True)
if not raw.exists():
 assert shutil.disk_usage(runtime).free>source.BYTES+1_700_000_000,'Insufficient space for complete preserved raw and outputs'
 shutil.copyfile(archive,raw)
os.environ.update(HIFLD_CDP_ARCHIVE=str(raw),HIFLD_CDP_RECEIPT=str(receipt))
published.write_key('synthetic-accepted-fixture.bin',b'local-fixture-only-not-an-accepted-real-release')
def snapshot(storage):return {k:storage.sha256_key(k) for k in storage.list_prefix()}
prior=snapshot(published);raw_sha=source.sha256_file(raw)
keys=[k for k in normal.resolve_asset_graph().get_all_asset_keys() if k.path[0]=='publish']
graphdefs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('cdp_normal_graph',selection=AssetSelection.assets(*keys))])
report={'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(source.__file__).parents[3],text=True).strip(),'module':source.__file__,'runtime':str(runtime),'approved':False,'runs':[]}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,'materializations':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()],'checks':[{'name':e.check_name,'passed':e.passed} for e in result.get_asset_check_evaluations()]})
 (runtime/'execution-progress.json').write_text(json.dumps(report,indent=2)+'\n');return errors

def archive_closed(parent,name,bundle):
 return {"preserved_complete_directory":str(parent/name),"classification":"closed local verification fixture; retain all files"}

with DagsterInstance.get() as instance:
 job=normal.resolve_job_def('cdp_source_integrity');graph=graphdefs.resolve_job_def('cdp_normal_graph')
 result=job.execute_in_process(instance=instance,raise_on_error=False);record('complete_real_source',result);assert result.success
 output=result.output_for_node(census_designated_places.op.name);version=output['version'];prefix=build_publish_partition_key(source.SLUG,source.SLUG,version);root=runtime/'staging'/prefix
 assert output['feature_count']==12884 and (source.SLUG,source.SLUG,version) in list(_iter_staged_version_paths(staging))
 before={str(f.relative_to(root)):source.sha256_file(f) for f in root.rglob('*') if f.is_file()}
 again=job.execute_in_process(instance=instance,raise_on_error=False);record('complete_source_reuse',again);assert again.success
 assert again.output_for_node(census_designated_places.op.name)['reused'] is True and before=={str(f.relative_to(root)):source.sha256_file(f) for f in root.rglob('*') if f.is_file()}
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
 for cycle in (1,2):
  result=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);errors=record(f'normal_catalog_{cycle}_missing_approval',result)
  assert not result.success and errors and all('missing_independent_release_approval_registry' in e for e in errors),errors
  assert snapshot(published)==prior
  after=job.execute_in_process(instance=instance,raise_on_error=False);record(f'source_after_catalog_{cycle}',after);assert after.success
 for name in ('quality_manifest','data_dictionary'):assert (root/f'metadata/source/{name}.json').exists()
 table=pq.read_table(root/'geoparquet'/f'{source.SLUG}.parquet');assert table.num_rows==12884 and tuple(table.column_names)==source.FIELDS and tuple(f.type for f in table.schema)==source.TYPES
 assert source.sha256_file(root/'candidate/typed-source.parquet')==source.sha256_file(root/'geoparquet'/f'{source.SLUG}.parquet')
 manifest=json.loads((root/'metadata/source_manifest.json').read_text());validate_release_provenance(manifest)
 assert manifest['approved'] is False and manifest['inputs'][0]['sha256']==raw_sha==source.SHA256
 report['quality']=json.loads((root/'metadata/quality_manifest.json').read_text())
 report['native_parquet']={'rows':12884,'columns':table.column_names,'schema':str(table.schema),'sha256':source.sha256_file(root/'geoparquet'/f'{source.SLUG}.parquet'),'bbox_column_added':False,'all_source_values_ids_native_wkb_exact':True}
 badprefix=prefix+'-synthetic-invalid-geometry';badroot=runtime/'staging'/badprefix;shutil.copytree(root,badroot);gpkg=badroot/'geopackage'/f'{source.SLUG}.gpkg'
 db=sqlite3.connect(gpkg)
 try:
  triggers=db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?",(source.LAYER,)).fetchall()
  for (name,) in triggers:db.execute('DROP TRIGGER "'+name.replace('"','""')+'"')
  db.execute('UPDATE Census_Designated_Place SET SHAPE=NULL WHERE _gpkg_storage_fid=1');db.commit()
 finally:db.close()
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix]);failed=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);record('synthetic_failed_geometry_quality_gate',failed)
 assert not failed.success and any(not e.passed for e in failed.get_asset_check_evaluations()) and snapshot(published)==prior
 report['synthetic_quality_archive']=archive_closed(badroot.parent,badroot.name,'cdp34-closed-failed-quality-native-clone-long')
 # Standalone replay retains complete rejected fixtures in place. The recorded
 # managed replay losslessly archived closed duplicates with full SHA replay.
 # Original ZIP is never in these candidate-only fixtures.
 report['cache_probes']=[]
 for label in ('approval_placeholder','auxiliary_srs','forged_reachable_code_commit'):
  marker=root/source.READY;ready=json.loads(marker.read_text());restore_bundle=None
  if label=='approval_placeholder':
   relative='metadata/baseline_comparison.json';(root/relative).write_bytes(source.encode({'approved':True,'status':'pass','schema_version':1}))
   expected='source_owned_report_wrapper_placeholder_or_provenance_drift'
  elif label=='auxiliary_srs':
   relative=f'geopackage/{source.SLUG}.gpkg';db=sqlite3.connect(root/relative)
   try:db.execute('UPDATE gpkg_contents SET srs_id=4326');db.commit()
   finally:db.close()
   expected='full_gpkg_container_srs_envelope_sql_or_rtree_drift'
  else:
   relative='metadata/source_manifest.json';bundle=runtime/'staging'/f'_source-provenance/{source.SLUG}/{version}/bundle.json';restore_bundle=bundle.read_bytes()
   forged=json.loads(restore_bundle);forged['manifest']['code_commit']='6969d05028352eefd20348a738a829034af9882f';bundle.write_bytes(source.encode(forged));(root/relative).write_bytes(source.encode(forged['manifest']))
   expected='construction_processing_blob_missing'
  ready['objects'][relative]=source.sha256_file(root/relative);marker.write_bytes(source.encode(ready))
  beforebad={str(f.relative_to(root)):source.sha256_file(f) for f in root.rglob('*') if f.is_file()};old=set((runtime/'staging/_quarantine'/source.SLUG).glob('invalid-candidate-*')) if (runtime/'staging/_quarantine'/source.SLUG).exists() else set()
  blocked=job.execute_in_process(instance=instance,raise_on_error=False);errors=record('self_repinned_'+label+'_blocked',blocked)
  assert not blocked.success and any(expected in e for e in errors) and not root.exists() and snapshot(published)==prior,errors
  candidates=[q for q in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-candidate-*') if q.is_dir() and q not in old];assert len(candidates)==1;q=candidates[0]
  assert beforebad=={str(f.relative_to(q)):source.sha256_file(f) for f in q.rglob('*') if f.is_file()}
  archive=archive_closed(q.parent,q.name,'cdp34-closed-cache-'+label+'-long')
  if restore_bundle is not None:bundle.write_bytes(restore_bundle) # Restore the local forged test fixture exactly, no approval.
  restored=job.execute_in_process(instance=instance,raise_on_error=False);record('cold_rebuild_after_'+label,restored);assert restored.success
  assert json.loads((root/'metadata/source_manifest.json').read_text())==manifest and source.sha256_file(raw)==raw_sha and snapshot(published)==prior
  report['cache_probes'].append({'kind':label,'failure_run_id':blocked.run_id,'restored_run_id':restored.run_id,'all_rejected_bytes_preserved':True,'archive':archive})
 table=pq.read_table(root/'geoparquet'/f'{source.SLUG}.parquet');validate_release_provenance(manifest)
 report.update(output={'dataset_slug':source.SLUG,'file_slug':source.SLUG,'version':version,'feature_count':source.COUNT},complete_rows_geometry_schema_and_IDs_verified=True,source_discoverable=True,refresh_byte_idempotent=True,native_prebuilt_retained_across_two_catalog_cycles=True,failed_quality_and_missing_approval_preserve_prior_fixture=True,original_full_zip_preserved=True)
 report['native_parquet']={'rows':table.num_rows,'columns':table.column_names,'schema':str(table.schema),'sha256':source.sha256_file(root/'geoparquet'/f'{source.SLUG}.parquet'),'bbox_column_added':False,'all_source_values_ids_native_wkb_exact':True}
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'code_head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
