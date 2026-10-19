"""Continue six successful runs after repairing only a synthetic test driver."""
from __future__ import annotations
import json
import os
import sqlite3
import subprocess
from pathlib import Path
p=Path('/workspace/work/hifld-coordinator/34');runtime=p/'runtime'
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
import pyarrow.parquet as pq
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld.definitions import defs as normal
from dagster_hifld.assets.census_designated_places import census_designated_places
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import census_designated_places as source
from dagster_hifld.release_approval import validate_release_provenance
staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
raw=runtime/'staging'/source.RAW_KEY;os.environ.update(HIFLD_CDP_ARCHIVE=str(raw),HIFLD_CDP_RECEIPT=str(p/'official2024_substategeo.receipt.json'))
report=json.loads((runtime/'execution-progress.json').read_text());version=next((runtime/'staging'/source.SLUG/source.SLUG).glob('*')) .name
version=version.removesuffix('-synthetic-invalid-geometry');prefix=build_publish_partition_key(source.SLUG,source.SLUG,version);root=runtime/'staging'/prefix
manifest=json.loads((root/'metadata/source_manifest.json').read_text());raw_sha=source.sha256_file(raw)
report['driver_repair']={'prior_log':'materialization.log','failure':'Synthetic geometry mutation left additional GPKG triggers invoking ST_IsEmpty, unavailable in stdlib sqlite. No source run failed. Continuation drops ALL triggers only in the explicitly synthetic clone before mutation. Six prior actual runs preserved.'}
def snapshot(storage):return {k:storage.sha256_key(k) for k in storage.list_prefix()}
prior=snapshot(published)
keys=[k for k in normal.resolve_asset_graph().get_all_asset_keys() if k.path[0]=='publish'];defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('cdp_normal_graph',selection=AssetSelection.assets(*keys))])
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,'materializations':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()],'checks':[{'name':e.check_name,'passed':e.passed} for e in result.get_asset_check_evaluations()]});(runtime/'execution-progress.json').write_text(json.dumps(report,indent=2)+'\n');return errors

def archive_closed(parent,name,bundle):
 subprocess.run(['/workspace/hifld-worker-worktrees/19/.venv/bin/python','/workspace/work/hifld-coordinator/scripts/archive_closed_fuel.py','--parent',str(parent),'--bundle',bundle,name],check=True)
 return json.loads((parent/(name+'.archived.json')).read_text())

with DagsterInstance.get() as instance:
 job=normal.resolve_job_def('cdp_source_integrity');graph=defs.resolve_job_def('cdp_normal_graph')
 badprefix=prefix+'-synthetic-invalid-geometry';badroot=runtime/'staging'/badprefix;gpkg=badroot/'geopackage'/f'{source.SLUG}.gpkg'
 db=sqlite3.connect(gpkg)
 try:
  triggers=db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?",(source.LAYER,)).fetchall()
  for (name,) in triggers:db.execute('DROP TRIGGER "'+name.replace('"','""')+'"')
  db.execute('UPDATE Census_Designated_Place SET SHAPE=NULL WHERE _gpkg_storage_fid=1');db.commit()
 finally:db.close()
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix]);failed=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);record('synthetic_failed_geometry_quality_gate',failed)
 assert not failed.success and any(not e.passed for e in failed.get_asset_check_evaluations()) and snapshot(published)==prior
 report['synthetic_quality_archive']=archive_closed(badroot.parent,badroot.name,'cdp34-closed-failed-quality-native-clone-long')
 # Cache metadata attacks retain every rejected byte, archived only after full
 # member-by-member SHA verification. Original ZIP is never in these duplicates.
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
