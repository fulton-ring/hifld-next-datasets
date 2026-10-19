"""Actual docs-only historical commit and post-rebuild normal graph followup."""
from pathlib import Path
import os,json,subprocess
from datetime import datetime,timezone
from unittest.mock import patch
runtime=Path('/workspace/work/hifld-coordinator/31/graph-construction-auth');report=json.loads((runtime/'execution.json').read_bytes());out=report['output'];repo=Path('/workspace/hifld-worker-worktrees/31');head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head!=report['code_head'];assert not subprocess.check_output(['git','diff',report['code_head'],head,'--','src','tests','scripts'],cwd=repo)
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance,Definitions,define_asset_job,AssetSelection
from dagster_hifld.definitions import defs as normal
from dagster_hifld.assets.fws_units import fws_cbrs_units
from dagster_hifld.sources import cbrs_units as s
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.partitions import build_publish_partition_key
stage=StagingStorageResource(local_dir=str(runtime/'staging'));published=PublishedStorageResource(local_dir=str(runtime/'published'));prefix=build_publish_partition_key(out['dataset_slug'],out['file_slug'],out['version']);root=runtime/'staging'/prefix;binding=runtime/'staging/_source-provenance'/out['dataset_slug']/out['version']/'bundle.json';bound=binding.read_bytes();prior=s.candidate_inventory(runtime/'published');original=json.loads(bound);assert original['manifest']['code_commit']==report['code_head']
keys=[key for key in normal.resolve_asset_graph().get_all_asset_keys() if key.path[0]=='publish'];defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':stage,'published_storage':published},jobs=[define_asset_job('cbrs_post_rebuild_normal_graph',selection=AssetSelection.assets(*keys))])
job=normal.resolve_job_def('fws_cbrs_units_draft_refresh');capture=repo/'docs/dataset-sources/cbrs-units-2/evidence/captures';runs=[]
with patch('dagster_hifld.assets.publish.process_layer_partitioned_geoparquet',side_effect=AssertionError('native artifact cannot fall back to generic converter')) as converter,DagsterInstance.get() as instance:
 # A complete rebuild has source metadata only. Re-run the actual normal graph
 # to prove recovered native artifacts re-enter catalog/formatter/release gates.
 result=defs.resolve_job_def('cbrs_post_rebuild_normal_graph').execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 assert not result.success and errors and all('missing_independent_release_approval_registry' in e for e in errors)
 runs.append({'kind':'post_rebuild_normal_missing_real_registry','run_id':result.run_id,'success':False,'errors':errors,'expected_denial':True,'actual_head':head,'original_construction_head':report['code_head'],'prior_published_preserved':s.candidate_inventory(runtime/'published')==prior})
 before=s.candidate_inventory(root)
 for kind,path in [('actual_docs_only_historical_commit_reuse',capture),('same_content_synthetic_later_observations',runtime/'later-observations')]:
  if kind=='same_content_synthetic_later_observations':
   path.mkdir()
   for name in s.URLS:
    (path/(name+'.body')).write_bytes((capture/(name+'.body')).read_bytes())
    receipt=json.loads((capture/(name+'.receipt.json')).read_bytes());receipt['observed_at']=datetime.now(timezone.utc).isoformat();receipt['synthetic_receipt_observation_fixture']=True;(path/(name+'.receipt.json')).write_bytes(s.encode(receipt))
  result=job.execute_in_process(instance=instance,run_config={'ops':{fws_cbrs_units.op.name:{'config':{'capture_dir':str(path)}}}},raise_on_error=False);errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
  assert result.success,errors;assert result.output_for_node(fws_cbrs_units.op.name)==out
  assert s.candidate_inventory(root)==before and binding.read_bytes()==bound and s.candidate_inventory(runtime/'published')==prior
  runs.append({'kind':kind,'run_id':result.run_id,'success':True,'actual_head':head,'original_construction_head':report['code_head'],'candidate_and_binding_unchanged':True,'prior_published_preserved':True})
  if kind=='same_content_synthetic_later_observations':
   for name in s.URLS:
    receipt=json.loads((path/(name+'.receipt.json')).read_bytes());key=f'_raw-captures/{s.SLUG}/{receipt["sha256"]}/{s.sha(s.encode(receipt))}.receipt.json';assert stage.read_key(key)==s.encode(receipt)
 assert converter.call_count==0
 followup={'approved':False,'runs':runs,'new_synthetic_observations_losslessly_retained_separately':True,'original_construction_history_retained':True,'generic_converter_calls':converter.call_count,'real_live_source_acquisition_evidence':'historical4cdb complete actual24run proof; current followup uses complete preserved real inputs'}
(runtime/'construction-history.json').write_text(json.dumps(followup,indent=2)+'\n');print(json.dumps([(v['kind'],v['run_id'],v['success']) for v in runs]))
