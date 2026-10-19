"""Registered full-source recovery; synthetic newer receipts never claim LIVE reads."""
from __future__ import annotations
import argparse
from datetime import datetime,timedelta
import io,json,os,subprocess,zipfile
from pathlib import Path

parser=argparse.ArgumentParser();parser.add_argument('--resume',action='store_true');parser.add_argument('--bounded-history',action='store_true');parser.add_argument('--runtime',type=Path,required=True);parser.add_argument('--repo',type=Path,required=True);args=parser.parse_args()
repo=args.repo.resolve();runtime=args.runtime.resolve();captures=repo/'docs/dataset-sources/historical-tsunami-observations/evidence/captures'
seed=json.loads((runtime/'execution.json').read_bytes());original_commit=seed['code_head'];current_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert current_commit!=original_commit
subprocess.run(['git','merge-base','--is-ancestor',original_commit,current_commit],cwd=repo,check=True)
changed=subprocess.check_output(['git','diff','--name-only',original_commit,current_commit],cwd=repo,text=True).splitlines();assert changed and all(p.startswith('docs/') for p in changed),changed
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance,Definitions,AssetSelection,define_asset_job
from dagster_hifld.definitions import defs
from dagster_hifld.assets.noaa_tsunami_observations import noaa_tsunami_observations as asset
from dagster_hifld.sources import tsunami_observations as source
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME

staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
output=seed['output'];prefix=f"{source.SLUG}/{source.SLUG}/{output['version']}";root=runtime/'staging'/prefix
bundle_key,first_key,incident_key=source.trust_keys(output['version']);binding=runtime/'staging'/bundle_key;first=runtime/'staging'/first_key
original_binding=binding.read_bytes();original_first=first.read_bytes();original_manifest=(root/'metadata/source_manifest.json').read_bytes();assert json.loads(original_manifest)['code_commit']==original_commit

def snapshot(directory):return {str(p.relative_to(directory)):source.sha(p.read_bytes()) for p in directory.rglob('*') if p.is_file()}
def published_snapshot():return {k:source.sha(published.read_key(k)) for k in published.list_prefix()}
prior=published_snapshot();original_objects=snapshot(root);original_raw=snapshot(captures);prior_report=None
if args.resume:
 prior_report=json.loads((runtime/'provenance-recovery-progress.json').read_bytes())
 assert len(prior_report['runs'])==3 and prior_report['runs'][-1]['success']
 failed=[q for q in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-cache-*/candidate') if not (q/'metadata/observations_candidate_ready.json').exists()]
 assert len(failed)==1;original_objects=snapshot(failed[0])
 # The initial quality-negative fixture retained the byte-original ready record
 # before its geometry was changed. This is an expectation only, never a repair.
 ready_copy=runtime/'staging'/(prefix+'-synthetic-invalid-geometry')/'metadata/observations_candidate_ready.json'
 ready_body=ready_copy.read_bytes();ready=json.loads(ready_body)
 assert ready['objects']=={name:original_objects[name] for name in source.expected_object_names()}
 assert (root/'metadata/observations_candidate_ready.json').read_bytes()==ready_body
 original_objects['metadata/observations_candidate_ready.json']=source.sha(ready_body)
newer=runtime/'synthetic-newer-valid-receipts';newer.mkdir(exist_ok=args.resume)
for name in ([] if args.resume else source.ARCHIVE_NAMES):
 with zipfile.ZipFile(captures/(name+'.zip')) as zin,zipfile.ZipFile(newer/(name+'.zip'),'w',compression=zipfile.ZIP_DEFLATED) as zout:
  for n in zin.namelist():
   body=zin.read(n)
   if n.endswith('.receipt.json'):
    receipt=json.loads(body);receipt['fetched_at']=(datetime.fromisoformat(receipt['fetched_at'])+timedelta(hours=2)).isoformat();body=source.encode(receipt)
   zout.writestr(n,body)
original_http=source.unpack_captures({n:(captures/(n+'.zip')).read_bytes() for n in source.ARCHIVE_NAMES});new_http=source.unpack_captures({n:(newer/(n+'.zip')).read_bytes() for n in source.ARCHIVE_NAMES})
assert all(original_http[n][0]==new_http[n][0] for n in original_http)
report=prior_report or {'approved':False,'fixture_scope':'SYNTHETIC newer receipt timestamps on all82complete REAL bodies; NOT new LIVE retrievals.','original_construction_commit':original_commit,'genuine_docs_only_current_head':current_commit,'changed_git_paths':changed,'full_source_history_mode':'bounded_core' if args.bounded_history else 'all_cases','runs':[]}
config={'ops':{asset.op.name:{'config':{'capture_dir':str(newer)}}}}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors});(runtime/'provenance-recovery-progress.json').write_bytes(source.encode(report));assert published_snapshot()==prior
 return errors
with DagsterInstance.get() as instance:
 job=defs.resolve_job_def('noaa_tsunami_observations_draft_refresh')
 publishkeys=[key for key in defs.resolve_asset_graph().get_all_asset_keys() if key.path[0]=='publish']
 recovery_defs=Definitions(assets=defs.assets,asset_checks=defs.asset_checks,resources={'staging_storage':staging,'published_storage':published},
  jobs=[define_asset_job('observations_recovery_catalog',selection=AssetSelection.assets(*publishkeys))])
 graph=recovery_defs.resolve_job_def('observations_recovery_catalog');instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
 def run(kind):
  result=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);return result,record(kind,result)
 def quarantine_preserved(objects):
  assert not root.exists();assert any(snapshot(q)==objects for q in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-cache-*/candidate'))
 def restored_exact():
  actual=snapshot(root);canonical=source.expected_object_names()|{'metadata/observations_candidate_ready.json'}
  assert {n:actual[n] for n in canonical}=={n:original_objects[n] for n in canonical}
  assert binding.read_bytes()==original_binding and first.read_bytes()==original_first
  assert (root/'metadata/source_manifest.json').read_bytes()==original_manifest
  if actual!=original_objects:
   # Source rebuild owns canonical data. Re-run the REAL normal catalog to
   # regenerate its derived dictionary/provenance/snapshots, never copy them.
   for index in range(2):
    result=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
    errors=record('normal_catalog_reconstruction_after_cold_'+str(index+1),result)
    assert not result.success and errors and all('missing_independent_release_approval_registry' in error for error in errors),errors
   assert snapshot(root)==original_objects
 if not args.resume:
  healthy,_=run('genuine_docs_only_head_newer_receipt_reuse');assert healthy.success;restored_exact()
  ready_path=root/'metadata/observations_candidate_ready.json';ready_path.unlink();missing_objects=snapshot(root)
  failed,errors=run('missing_candidate_ready_whole_quarantine');assert not failed.success and any('missing_candidate_ready' in e for e in errors);quarantine_preserved(missing_objects)
  rebuilt,_=run('cold_reconstruction_retains_original_manifest_82receipts');assert rebuilt.success;restored_exact()
 else:
  report['harness_resume']='Three completed source runs retained; canonical cold output verified first, generated catalog metadata regenerated through actual normal assets.'
  restored_exact()
 ready_path=root/'metadata/observations_candidate_ready.json'
 for label in (['bundle_missing'] if args.bounded_history else ['bundle_missing','first_marker_missing','both_records_missing']):
  before=snapshot(root)
  if label in ['bundle_missing','both_records_missing']:binding.unlink()
  if label in ['first_marker_missing','both_records_missing']:first.unlink()
  failed,errors=run(label+'_whole_quarantine');assert not failed.success and any('missing_original_construction' in e for e in errors);quarantine_preserved(before)
  failed,errors=run(label+'_second_attempt_no_bootstrap');assert not failed.success;assert not root.exists()
  assert binding.exists()==(label=='first_marker_missing');assert first.exists()==(label=='bundle_missing')
  # Explicit operator-fixture restoration, not source code healing.
  binding.write_bytes(original_binding);first.write_bytes(original_first)
  rebuilt,_=run(label+'_after_explicit_trusted_record_restore');assert rebuilt.success;restored_exact()
 for label,claimed in ([] if args.bounded_history else [('zero_commit','0'*40),('real_reachable_wrong_processing_source','6f79b762f02839f00a51a75a1e002f864d0146ec')]):
  if label.startswith('real'):
   files=json.loads((root/'metadata/processing_identity.json').read_bytes())['files']
   for path in files:subprocess.run(['git','cat-file','-e',claimed+':'+path],cwd=repo,check=True)
  manifest=json.loads(original_manifest);manifest['code_commit']=claimed;forged_manifest=source.encode(manifest)
  (root/'metadata/source_manifest.json').write_bytes(forged_manifest)
  for p in [root/'metadata/source/source_manifest.json',*list((root/'metadata/source').glob('version_*_source_manifest.json'))]:
   if p.exists():p.write_bytes(forged_manifest)
  bundle=json.loads(original_binding);bundle['manifest']=manifest;binding.write_bytes(source.encode(bundle));marker=json.loads(original_first);marker['original_bundle_sha256']=source.sha(binding.read_bytes());first.write_bytes(source.encode(marker))
  ready=json.loads(ready_path.read_bytes());ready['objects']['metadata/source_manifest.json']=source.sha(forged_manifest);ready_path.write_bytes(source.encode(ready));before=snapshot(root)
  failed,errors=run('coforged_mutable_candidate_bundle_marker_'+label);assert not failed.success and any('construction_' in e for e in errors);quarantine_preserved(before)
  if label.startswith('real'):assert any('construction_processing_blob_hash_mismatch' in e for e in errors),errors
  binding.write_bytes(original_binding);first.write_bytes(original_first);rebuilt,_=run('source_after_explicit_restore_'+label);assert rebuilt.success;restored_exact()
 bundle=json.loads(original_binding)
 for n in source.ARCHIVE_NAMES:
  original=(captures/(n+'.zip')).read_bytes();later=(newer/(n+'.zip')).read_bytes()
  assert staging.read_key(source.archive_key(n,later))==later;assert staging.read_key(bundle['archives'][n]['key'])==original and later!=original
 assert snapshot(captures)==original_raw and published_snapshot()==prior
 report.update(status='PASS',all_original_candidate_bytes_and_full_first_manifest_restored=True,all82original_and_synthetic_newer_receipts_retained=True,all_original_inputs_and_prior_local_publication_unchanged=True,tested_missing_record_second_attempts_never_bootstrapped=True)
(runtime/'provenance-recovery.json').write_bytes(source.encode(report));print(source.encode(report).decode())
