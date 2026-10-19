"""Actual complete registered CDP source and normal catalog/promotion gates."""
from __future__ import annotations
import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path
p=Path('/workspace/work/hifld-coordinator/34');runtime=p/'runtime'
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
if not raw.exists():os.rename(p/'official2024_substategeo.zip',raw)
os.environ.update(HIFLD_CDP_ARCHIVE=str(raw),HIFLD_CDP_RECEIPT=str(p/'official2024_substategeo.receipt.json'))
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
 # Corrupt one geometry in a separate explicitly synthetic partition. The real
 # candidate and its first-construction provenance stay byte-identical.
 badprefix=prefix+'-synthetic-invalid-geometry';badroot=runtime/'staging'/badprefix;shutil.copytree(root,badroot)
 gpkg=badroot/'geopackage'/f'{source.SLUG}.gpkg'
 with sqlite3.connect(gpkg) as db:
  db.execute('DROP TRIGGER IF EXISTS rtree_Census_Designated_Place_SHAPE_update1');db.execute('DROP TRIGGER IF EXISTS rtree_Census_Designated_Place_SHAPE_update2');db.execute('DROP TRIGGER IF EXISTS rtree_Census_Designated_Place_SHAPE_update3');db.execute('DROP TRIGGER IF EXISTS rtree_Census_Designated_Place_SHAPE_update4')
  db.execute('UPDATE Census_Designated_Place SET SHAPE=NULL WHERE _gpkg_storage_fid=1')
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix]);failed=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);record('synthetic_failed_geometry_quality_gate',failed)
 assert not failed.success and any(not e.passed for e in failed.get_asset_check_evaluations()) and snapshot(published)==prior
 # Actual source-owned placeholder forgery, despite a matching ready self-pin,
 # must detach the complete bad version and retain raw/prior published data.
 marker=root/source.READY;placeholder=root/'metadata/baseline_comparison.json';forged=source.encode({'approved':True,'status':'pass','schema_version':1});placeholder.write_bytes(forged)
 ready=json.loads(marker.read_text());ready['objects']['metadata/baseline_comparison.json']=source.digest(forged);marker.write_bytes(source.encode(ready))
 beforebad={str(f.relative_to(root)):source.sha256_file(f) for f in root.rglob('*') if f.is_file()}
 blocked=job.execute_in_process(instance=instance,raise_on_error=False);errors=record('self_repinned_approval_placeholder_blocked',blocked)
 assert not blocked.success and any('source_owned_report_wrapper_placeholder_or_provenance_drift' in e for e in errors) and not root.exists() and snapshot(published)==prior
 quarantines=list((runtime/'staging/_quarantine'/source.SLUG).glob('invalid-candidate-*'));assert len(quarantines)==1
 assert beforebad=={str(f.relative_to(quarantines[0])):source.sha256_file(f) for f in quarantines[0].rglob('*') if f.is_file()}
 restored=job.execute_in_process(instance=instance,raise_on_error=False);record('cold_rebuild_preserves_original_provenance',restored);assert restored.success
 assert json.loads((root/'metadata/source_manifest.json').read_text())==manifest and source.sha256_file(raw)==raw_sha and snapshot(published)==prior
 report.update(output=output,complete_rows_geometry_schema_and_IDs_verified=True,source_discoverable=True,refresh_byte_idempotent=True,native_prebuilt_retained_across_two_catalog_cycles=True,failed_quality_and_missing_approval_preserve_prior_fixture=True,whole_invalid_cache_preserved=str(quarantines[0]),original_full_zip_preserved=True)
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'code_head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
