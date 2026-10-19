"""Replay real later ZIP/metadata receipts after a documentation-only commit."""
import json,os,subprocess,sys
from pathlib import Path
repo=Path('/workspace/hifld-worker-worktrees/30');runtime=Path('/workspace/work/hifld-coordinator/30/repairs/construction-graph');old=json.loads((runtime/'execution.json').read_text());assert old['two_normal_catalog_cycles_and_refresh_pass']
for k in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(k,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld.definitions import defs as normal
from dagster_hifld.assets.fws_prohibitions import fws_cbrs_prohibitions
from dagster_hifld.sources import cbrs_prohibitions as s
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
o=old['output'];root=runtime/'staging'/o['dataset_slug']/o['file_slug']/o['version'];before=s.tree_inventory(root);raw_before=s.tree_inventory(runtime/'staging/_raw-captures');prior=s.tree_inventory(runtime/'published');bundle=runtime/'staging'/s.construction_bundle_key(o['version']);original_bundle=bundle.read_bytes();head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();construction=json.loads(original_bundle)['manifest']['code_commit'];assert head!=construction
assert subprocess.check_output(['git','diff',construction,head,'--','src','pyproject.toml','uv.lock'],cwd=repo)==b''
report={'documentation_head':head,'original_construction_commit':construction,'original_manifest_sha256':s.sha((root/'metadata/source_manifest.json').read_bytes()),'original_bundle_sha256':s.sha(original_bundle),'processing_files_unchanged_from_construction':True,'runs':[],'approved':False}
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('construction_docs_refresh',selection=AssetSelection.assets(fws_cbrs_prohibitions))]);job=defs.resolve_job_def('construction_docs_refresh');capture=repo/'docs/dataset-sources/cbrs-prohibitions-1/evidence/captures'
later=runtime/'later-real-receipt-replay';later.mkdir()
for n in s.URLS:
 (later/(n+'.body')).symlink_to(capture/(n+'.body'))
 receipt=(Path('/workspace/work/hifld-coordinator/30/repairs/live-captures') if n in {'official_zip','fws_prohibitions_metadata'} else capture)/(n+'.receipt.json')
 (later/(n+'.receipt.json')).write_bytes(receipt.read_bytes())
with DagsterInstance.get() as instance:
 for kind,inputs in [('documentation_only_HEAD_refresh',capture),('later_real_ZIP_and_metadata_receipts_same_complete_inputs',later)]:
  result=job.execute_in_process(instance=instance,run_config={'ops':{fws_cbrs_prohibitions.op.name:{'config':{'capture_dir':str(inputs)}}}},raise_on_error=False)
  errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()];report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors});assert result.success and result.output_for_node(fws_cbrs_prohibitions.op.name)==o
  assert s.tree_inventory(root)==before and bundle.read_bytes()==original_bundle and s.tree_inventory(runtime/'published')==prior
 saved=runtime/'closed-docs-original-for-rebuild';os.rename(root,saved)
 result=job.execute_in_process(instance=instance,run_config={'ops':{fws_cbrs_prohibitions.op.name:{'config':{'capture_dir':str(later)}}}},raise_on_error=False)
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()];report['runs'].append({'kind':'documentation_HEAD_fresh_rebuild_later_real_receipts','run_id':result.run_id,'success':result.success,'errors':errors});assert result.success and result.output_for_node(fws_cbrs_prohibitions.op.name)==o
 ready=json.loads((saved/'metadata/cbrs_prohibitions_candidate_ready.json').read_text());assert json.loads((root/'metadata/cbrs_prohibitions_candidate_ready.json').read_text())==ready
 assert all((root/name).read_bytes()==(saved/name).read_bytes() for name in ready['objects']) and bundle.read_bytes()==original_bundle and s.tree_inventory(runtime/'published')==prior
 report['docs_HEAD_fresh_rebuild_complete_source_owned_objects']=s.tree_inventory(root)
 import shutil
 shutil.rmtree(root);os.rename(saved,root)
for path,metadata in raw_before.items():assert s.tree_inventory(runtime/'staging/_raw-captures')[path]==metadata
for n in {'official_zip','fws_prohibitions_metadata'}:
 receipt=s.strict_json((later/(n+'.receipt.json')).read_bytes());key=f"_raw-captures/{s.SLUG}/{receipt['sha256']}/{s.sha(s.encode(receipt))}.receipt.json";assert staging.read_key(key)==s.encode(receipt)
report.update(original_candidate_manifest_bundle_raw_and_prior_preserved=True,later_real_observation_receipts_retained=True,docs_HEAD_fresh_rebuild_original_manifest_receipts_native_bytes_preserved=True,all_1693_source_rows_unchanged=True);(runtime/'docs-only-reuse.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
