"""Actual registered recovery on real captures; synthetic later receipt fixture disclosed.

First run actual_dagster.py on the frozen code. Make a genuine documentation-only
checkpoint commit, then run this script with that existing isolated runtime.
This script does not change Git history or approve any dataset contract.
"""
from __future__ import annotations
import argparse
from datetime import datetime,timedelta
import json
import os
from pathlib import Path
import subprocess

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);parser.add_argument('--repo',type=Path,required=True);args=parser.parse_args()
repo=args.repo.resolve();runtime=args.runtime.resolve();captures=repo/'docs/dataset-sources/current-dart-deployments/evidence/captures'
seed=json.loads((runtime/'execution.json').read_bytes());original_commit=seed['code_head'];current_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert current_commit!=original_commit,'Create the genuine docs-only checkpoint after the seed run first'
subprocess.run(['git','merge-base','--is-ancestor',original_commit,current_commit],cwd=repo,check=True)
changed=subprocess.check_output(['git','diff','--name-only',original_commit,current_commit],cwd=repo,text=True).splitlines();assert changed and all(p.startswith('docs/') for p in changed),changed
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.assets.noaa_current_dart import noaa_current_dart
from dagster_hifld.sources import current_dart as source
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
output=seed['output'];prefix=f"{source.SLUG}/{source.SLUG}/{output['version']}";root=runtime/'staging'/prefix
binding=runtime/'staging/_candidate-construction'/source.SLUG/(output['version']+'.json');original_binding=binding.read_bytes();original_manifest=(root/'metadata/source_manifest.json').read_bytes()
assert json.loads(original_manifest)['code_commit']==original_commit
report={'approved':False,'fixture_scope':'Explicit synthetic later valid receipt timestamps applied to complete unchanged real captured bodies; NOT new LIVE retrieval evidence.','source_construction_commit':original_commit,'genuine_docs_only_current_head':current_commit,'changed_git_paths':changed,'runs':[],'assertions':[]}
def snapshot(directory):return {str(p.relative_to(directory)):source.sha(p.read_bytes()) for p in directory.rglob('*') if p.is_file()}
def published_snapshot():return {k:source.sha(published.read_key(k)) for k in published.list_prefix()}
prior=published_snapshot();original_objects=snapshot(root);original_raw=snapshot(captures)
newer=runtime/'synthetic-newer-valid-receipts';newer.mkdir(exist_ok=False)
for name in source.URLS:
 body=(captures/(name+'.body')).read_bytes();receipt=json.loads((captures/(name+'.receipt.json')).read_bytes())
 receipt['fetched_at']=(datetime.fromisoformat(receipt['fetched_at'])+timedelta(hours=2)).isoformat()
 (newer/(name+'.body')).write_bytes(body);(newer/(name+'.receipt.json')).write_bytes(source.encode(receipt))
config={'ops':{noaa_current_dart.op.name:{'config':{'capture_dir':str(newer)}}}}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors})
 (runtime/'provenance-recovery-progress.json').write_bytes(source.encode(report));assert published_snapshot()==prior
 return errors
with DagsterInstance.get() as instance:
 job=defs.resolve_job_def('noaa_dart_draft_refresh')
 def run(kind):
  result=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);return result,record(kind,result)
 healthy,_=run('genuine_docs_head_healthy_newer_receipts');assert healthy.success;assert snapshot(root)==original_objects;assert binding.read_bytes()==original_binding
 marker=root/'metadata/current_dart_candidate_ready.json';marker.unlink();missing_objects=snapshot(root)
 failed,errors=run('missing_marker_newer_receipts_whole_quarantine');assert not failed.success and any('missing_candidate_ready' in e for e in errors)
 assert not root.exists();assert binding.read_bytes()==original_binding
 assert any(snapshot(q)==missing_objects for q in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-cache-*/candidate'))
 rebuilt,_=run('cold_rebuild_original_provenance_after_docs_head_newer_receipts');assert rebuilt.success
 assert snapshot(root)==original_objects and binding.read_bytes()==original_binding and (root/'metadata/source_manifest.json').read_bytes()==original_manifest
 report['assertions'].append('Healthy refresh and cold reconstruction after whole-cache quarantine retain original manifest and all12original receipts across genuine docs-only HEAD; newer receipts retained separately.')
 wrong_source_commit=subprocess.check_output(['git','rev-parse','d4d8aa5d1fb480c3087bd1a58b0594c8ab4ba713'],cwd=repo,text=True).strip()
 for label,claimed in [('unresolvable_all_zero_commit','0'*40),('authentic_commit_with_wrong_processing_source',wrong_source_commit)]:
  manifest=json.loads(original_manifest);manifest['code_commit']=claimed;(root/'metadata/source_manifest.json').write_bytes(source.encode(manifest))
  # Coforge ALL authored snapshots if present, the outside bundle and ready digest.
  for p in [root/'metadata/source/source_manifest.json',*list((root/'metadata/source').glob('version_*_source_manifest.json'))]:
   if p.exists():p.write_bytes(source.encode(manifest))
  bundle=json.loads(original_binding);bundle['manifest']=manifest;binding.write_bytes(source.encode(bundle))
  ready=json.loads(marker.read_bytes());ready['objects']['metadata/source_manifest.json']=source.sha(source.encode(manifest));marker.write_bytes(source.encode(ready));altered=snapshot(root);forged_binding=binding.read_bytes()
  negative,errors=run('coforged_bundle_and_cache_'+label);assert not negative.success and any('construction_' in e for e in errors),errors
  if label=='authentic_commit_with_wrong_processing_source':
   assert any('construction_processing_blob_hash_mismatch' in e for e in errors),errors
  assert not root.exists() and binding.read_bytes()==forged_binding
  assert any(snapshot(q)==altered for q in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-cache-*/candidate'))
  # Explicit operator-fixture restoration of original binding, not adapter healing.
  binding.write_bytes(original_binding);restored,_=run('source_after_explicit_original_bundle_restore_'+label);assert restored.success and snapshot(root)==original_objects
 for label,invalid in [('missing',None),('invalid',b'{}\n')]:
  altered=snapshot(root)
  if invalid is None:binding.unlink()
  else:binding.write_bytes(invalid)
  negative,errors=run('outside_bundle_'+label+'_whole_cache_quarantine');assert not negative.success and not root.exists()
  assert any(snapshot(q)==altered for q in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-cache-*/candidate'))
  if invalid is None:assert not binding.exists()
  else:assert binding.read_bytes()==invalid
  binding.write_bytes(original_binding);restored,_=run('source_after_operator_fixture_restore_'+label);assert restored.success and snapshot(root)==original_objects
 bundle=json.loads(original_binding)
 for name in source.URLS:
  new_receipt=(newer/(name+'.receipt.json')).read_bytes();body=(newer/(name+'.body')).read_bytes();key=f'_raw-captures/{source.SLUG}/{source.sha(body)}/{source.sha(new_receipt)}.receipt.json'
  assert staging.read_key(key)==new_receipt;assert staging.read_key(bundle['captures'][name]['receipt_key'])!=new_receipt
 assert snapshot(captures)==original_raw and published_snapshot()==prior
 report.update(status='PASS',all_original_candidate_objects_restored_byte_exact=True,original_binding_preserved_byte_exact=True,all_original_and_newer_receipts_retained=True,published_fixture_unchanged=True,all_original_input_files_unchanged=True)
(runtime/'provenance-recovery.json').write_bytes(source.encode(report));print(source.encode(report).decode())
