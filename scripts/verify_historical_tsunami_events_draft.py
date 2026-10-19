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
 # Synthetic local storage failure after complete native/private validation and
 # CAS install. Repeated refresh must not bootstrap another original bundle.
 install=construction_state._install_once
 def fail_marker(base,key,body):
  if key.endswith('.first-construction.json'):raise OSError('synthetic marker-write fault after CAS')
  install(base,key,body)
 with patch.object(construction_state,'_install_once',fail_marker):
  partial=job.execute_in_process(instance=instance,raise_on_error=False);errors=record('synthetic_CAS_only_first_marker_failure',partial)
 assert not partial.success and any('construction_state_io_failed' in e for e in errors)
 states=list((runtime/'staging/_construction-state'/source.SLUG).glob('*.json.evidence/*.json'));assert len(states)==1
 evidence=states[0];original_envelope=evidence.read_bytes();envelope=json.loads(original_envelope);state_version=envelope['version'];binding=runtime/'staging'/source.construction_record_key(state_version)
 state_before={str(f.relative_to(runtime/'staging/_construction-state')):source.sha256_file(f) for f in (runtime/'staging/_construction-state').rglob('*') if f.is_file()}
 for attempt in (1,2):
  result=job.execute_in_process(instance=instance,raise_on_error=False);errors=record('CAS_only_refresh_'+str(attempt)+'_blocked',result)
  assert not result.success and any('construction_missing_original_state' in e for e in errors) and not binding.exists()
 assert state_before=={str(f.relative_to(runtime/'staging/_construction-state')):source.sha256_file(f) for f in (runtime/'staging/_construction-state').rglob('*') if f.is_file()}
 # Explicit exact trusted operator restoration, not an automatic production path.
 marker={'schema_version':1,'version':state_version,'processing_fingerprint':envelope['processing_fingerprint'],'original_bundle_sha256':source.digest(original_envelope),'original_bundle_evidence_key':str(evidence.relative_to(runtime/'staging'))}
 Path(str(binding)+'.first-construction.json').write_bytes(json.dumps(marker,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())
 binding.write_bytes(original_envelope)
 first=job.execute_in_process(instance=instance,raise_on_error=False);record('complete_real_source_after_exact_operator_restore',first);assert first.success
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
 # Self-repinned cache reports/container and reachable-but-wrong-code claims
 # cannot become authority. Every rejected version remains complete outside discovery.
 report['cache_probes']=[]
 for label in ('approval_placeholder','auxiliary_srs','forged_reachable_code_commit','forged_original_receipt','missing_original_binding','missing_CAS','missing_marker'):
  marker=root/source.READY;ready=json.loads(marker.read_text());binding=runtime/'staging'/source.construction_record_key(version);restore_binding=None;withheld=None;restore_extra=None
  if label=='approval_placeholder':relative='metadata/baseline_comparison.json';(root/relative).write_bytes(source.encode({'approved':True,'status':'pass','schema_version':1}));expected='source_owned_report_wrapper_placeholder_or_raw_provenance_drift'
  elif label=='auxiliary_srs':
   relative=f'geopackage/{source.SLUG}.gpkg';db=sqlite3.connect(root/relative)
   try:db.execute('UPDATE gpkg_contents SET srs_id=4326');db.commit()
   finally:db.close()
   expected='gpkg_full_container_srs_headers_sql_or_index_drift'
  elif label in ('forged_reachable_code_commit','forged_original_receipt'):
   relative='metadata/source_manifest.json';restore_binding=binding.read_bytes();forged=json.loads(restore_binding);payload=forged['payload']
   if label=='forged_reachable_code_commit':payload['manifest']['code_commit']='6969d05028352eefd20348a738a829034af9882f'
   else:
    receipt=payload['captures']['page_00000']['receipt'];receipt['observed_at']='2026-10-19T23:04:58+00:00'
    next(i for i in payload['manifest']['inputs'] if i['input_id']=='page_00000:'+receipt['sha256'])['retrieved_at']=receipt['observed_at']
    receipt_path=root/'raw/captures/page_00000.receipt.json';receipt_path.write_bytes(source.encode(receipt));ready['objects'][str(receipt_path.relative_to(root))]=source.sha256_file(receipt_path)
   binding.write_bytes(construction_state.canonical_construction_bundle(version,forged['processing_fingerprint'],payload));(root/relative).write_bytes(source.encode(payload['manifest']));expected='construction_marker_mismatch'
  else:
   relative=None
   if label=='missing_original_binding':withheld=binding;expected='construction_missing_original_state'
   elif label=='missing_marker':withheld=Path(str(binding)+'.first-construction.json');expected='construction_missing_original_state'
   else:withheld=Path(str(binding)+'.evidence')/(source.digest(binding.read_bytes())+'.json');expected='construction_original_evidence_mismatch'
   restore_extra=withheld.read_bytes();withheld.rename(runtime/('operator-withheld-'+label+'.body'))
  if relative:ready['objects'][relative]=source.sha256_file(root/relative);marker.write_bytes(source.encode(ready))
  altered={str(f.relative_to(root)):source.sha256_file(f) for f in root.rglob('*') if f.is_file()};old=set((runtime/'staging/_quarantine'/source.SLUG).glob('invalid-candidate-*')) if (runtime/'staging/_quarantine'/source.SLUG).exists() else set()
  failed=job.execute_in_process(instance=instance,raise_on_error=False);errors=record('self_repinned_'+label+'_blocked',failed)
  assert not failed.success and any(expected in e for e in errors) and not root.exists() and snapshot(published)==prior,errors
  found=[q for q in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-candidate-*') if q.is_dir() and q not in old];assert len(found)==1
  assert altered=={str(f.relative_to(found[0])):source.sha256_file(f) for f in found[0].rglob('*') if f.is_file()}
  if label in ('missing_original_binding','missing_CAS','missing_marker'):
   for repeat in (1,2):
    retry=job.execute_in_process(instance=instance,raise_on_error=False);retry_errors=record(label+'_second_refresh_'+str(repeat)+'_blocked',retry)
    assert not retry.success and any(expected in e for e in retry_errors) and not root.exists() and not withheld.exists()
   withheld.write_bytes(restore_extra) # Explicit exact trusted restoration.
  elif restore_binding is not None:binding.write_bytes(restore_binding)
  restored=job.execute_in_process(instance=instance,raise_on_error=False);record('cold_rebuild_after_'+label,restored);assert restored.success
  assert json.loads((root/'metadata/source_manifest.json').read_text())==manifest and snapshot(published)==prior and raw_before=={k:v for k,v in snapshot(staging).items() if k.startswith('_raw-captures/')}
  report['cache_probes'].append({'kind':label,'failure_run_id':failed.run_id,'restored_run_id':restored.run_id,'quarantine':str(found[0]),'all_rejected_bytes_preserved':True})
 report.update(output=output,complete_rows_IDs_physical_schema_nulls_dates_native_geometry_and_provenance_verified=True,source_discoverable_without_copy=True,source_byte_idempotent=True,native_prebuilt_retained_across_two_catalog_cycles=True,failed_quality_and_missing_approval_preserve_prior_fixture=True,all_raw_originals_preserved=True)
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
