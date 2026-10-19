"""Actual registered full-source provenance repair probes after a docs-only commit.

First run verify_cbrs_buffer_draft.py on the source checkpoint. Commit only docs
or scripts, then run this script against that isolated runtime. New observation
receipts are explicit simulated later observations of the complete real bodies;
no additional live fetch or acceptance is claimed.
"""
import argparse,json,os,pathlib,shutil,subprocess
parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=pathlib.Path,required=True);args=parser.parse_args()
runtime=args.runtime.resolve();repo=pathlib.Path(__file__).resolve().parents[1]
original=json.loads((runtime/'execution.json').read_bytes())
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head!=original['code_head'],'Commit only docs/scripts after initial source materialization.'
for k in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(k,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import Definitions,AssetSelection,AssetKey,define_asset_job,DagsterInstance
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.fws import fws_cbrs_buffer_zone as asset
from dagster_hifld.sources import cbrs_buffer as s
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
stage=StagingStorageResource(local_dir=str(runtime/'staging'));pub=PublishedStorageResource(local_dir=str(runtime/'published'))
assert pathlib.Path(s.__file__).resolve().is_relative_to(repo/'src')
output=original['output'];prefix=f'{s.SLUG}/{s.SLUG}/{output["version"]}';root=runtime/'staging'/prefix
pin=runtime/'staging/_source-provenance'/s.SLUG/output['version']/'bundle.json';pinbytes=pin.read_bytes()
manifest=(root/'metadata/source_manifest.json').read_bytes()
def inventory(path):return {str(p.relative_to(path)):s.sha256_file(p) for p in path.rglob('*') if p.is_file()}
rawbefore=inventory(runtime/'staging/_raw-captures');prior=inventory(runtime/'published')
original_candidate=inventory(root)
assert json.loads(manifest)['code_commit']==original['code_head']
assert s.encode(s.processing_identity())==s.encode(json.loads((root/'metadata/processing_identity.json').read_bytes()))
capture=runtime/'later-observation-fixture';capture.mkdir()
old=repo/'docs/dataset-sources/cbrs-buffer-zone-1/evidence/captures'
for name in s.URLS:
 os.link(old/(name+'.body'),capture/(name+'.body'))
 receipt=json.loads((old/(name+'.receipt.json')).read_bytes());receipt['observed_at']='2026-10-20T01:02:03+00:00'
 (capture/(name+'.receipt.json')).write_bytes(s.encode(receipt))
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':stage,'published_storage':pub},jobs=[define_asset_job('construction_bound_source',selection=AssetSelection.assets(asset)),define_asset_job('construction_repeat_catalog',selection=AssetSelection.keys(AssetKey(['publish','catalog'])))])
report={'code_head':head,'original_construction_head':original['code_head'],'real_docs_only_head_changed':True,'effective_processing_identity_unchanged':True,'source_rows':1145,'later_receipts':'explicit simulated later observations of unchanged complete real captured bodies; no new live claim','runs':[],'rejected':[]}
config={'ops':{asset.op.name:{'config':{'capture_dir':str(capture)}}}}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors})
 (runtime/'construction-in-progress.json').write_text(json.dumps(report,indent=2)+'\n');return errors
with DagsterInstance.get() as instance:
 job=defs.resolve_job_def('construction_bound_source');cat=defs.resolve_job_def('construction_repeat_catalog')
 result=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('actual_docs_only_HEAD_and_later_receipt_refresh',result);assert result.success and result.output_for_node(asset.op.name)==output
 assert inventory(root)==original_candidate and pin.read_bytes()==pinbytes
 for i in [1,2]:
  result=cat.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);record('actual_second_catalog_cycle_'+str(i),result);assert result.success
  result=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('actual_reuse_after_catalog_cycle_'+str(i),result);assert result.success
 backup=runtime/'clean-provenance-bound-candidate';shutil.copytree(root,backup)
 for mode in ['forged_original_commit','forged_original_receipts','missing_external_binding']:
  if not root.exists():shutil.copytree(backup,root)
  ready=root/'metadata/cbrs_buffer_candidate_ready.json';marker=json.loads(ready.read_bytes())
  if mode=='missing_external_binding':
   saved=runtime/'original-binding-preserved.json';pin.rename(saved)
  else:
   for name in ['metadata/source_manifest.json','metadata/source/source_manifest.json']:
    p=root/name;x=json.loads(p.read_bytes())
    if mode=='forged_original_commit':x['code_commit']='0'*40
    else:
     for item in x['inputs']:item['retrieved_at']='2026-10-20T01:02:03+00:00'
    p.write_bytes(s.encode(x))
    if name in marker['objects']:marker['objects'][name]=s.sha256_file(p)
   if mode=='forged_original_receipts':
    for name in s.URLS:
     p=root/'raw/captures'/f'{name}.receipt.json';x=json.loads(p.read_bytes());x['observed_at']='2026-10-20T01:02:03+00:00';p.write_bytes(s.encode(x));marker['objects'][str(p.relative_to(root))]=s.sha256_file(p)
    p=root/'raw/retrieval.json';p.write_bytes((root/'raw/captures/official_zip.receipt.json').read_bytes());marker['objects']['raw/retrieval.json']=s.sha256_file(p)
   ready.write_bytes(s.encode(marker))
  bad=inventory(root);qroot=runtime/'staging/_quarantine'/s.SLUG/'rejected-cache';before={p for p in qroot.glob('*') if p.is_dir()}
  result=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);errors=record(mode,result);assert not result.success
  reason={'forged_original_commit':'cached_original_construction_manifest_changed','forged_original_receipts':'cached_capture_differs_from_original_construction_bundle','missing_external_binding':'missing_original_construction_provenance_bundle'}[mode]
  assert any(reason in e for e in errors),errors
  added={p for p in qroot.glob('*') if p.is_dir()}-before;assert len(added)==1;retained=next(iter(added))/root.name
  assert inventory(retained)==bad and s.sha256_file(retained/'raw'/s.ARCHIVE_NAME)==s.PIN
  assert not root.exists() and (s.SLUG,s.SLUG,output['version']) not in list(_iter_staged_version_paths(stage))
  assert inventory(runtime/'published')==prior
  report['rejected'].append({'mode':mode,'reason':reason,'retained':str(retained),'complete_corrupt_tree_pins_preserved':True,'not_discoverable':True,'raw_preserved':True})
  if mode=='missing_external_binding':pin.parent.mkdir(parents=True,exist_ok=True);saved.rename(pin)
 # Rebuild from fresh complete source observations, using the outside original pin.
 result=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('actual_rebuild_retains_first_commit_and_receipts',result);assert result.success and result.output_for_node(asset.op.name)==output
 assert (root/'metadata/source_manifest.json').read_bytes()==manifest and pin.read_bytes()==pinbytes
 for name in s.URLS:assert (root/f'raw/captures/{name}.receipt.json').read_bytes()==(backup/f'raw/captures/{name}.receipt.json').read_bytes()
 assert s.sha256_file(root/'geopackage'/f'{s.SLUG}.gpkg')=='29edab0b125fc438930422b0f5ff395869d25fa4854077222632515423c79a56'
 assert s.sha256_file(root/'candidate/typed-source.parquet')=='54b294ce12e616ade4b8fa8b00bd27cab9ce320a7ef4af7ce24807a7d4e1480e'
 now=inventory(runtime/'staging/_raw-captures');assert all(now.get(k)==v for k,v in rawbefore.items()) and inventory(runtime/'published')==prior
 report.update(original_provenance_bundle_unchanged=True,original_raw_captures_preserved=True,prior_published_fixture_preserved=True,all_native_payload_hashes_unchanged=True,original_manifest_and_receipts_preserved_on_rebuild=True)
(runtime/'construction-execution.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'code_head':head,'runs':[(x['kind'],x['run_id'],x['success']) for x in report['runs']]}))
