from pathlib import Path
import os,json,subprocess
from datetime import datetime,timezone
r=Path('/workspace/work/hifld-coordinator/31/graph-review-repair');report=json.loads((r/'execution.json').read_bytes());out=report['output'];repo=Path('/workspace/hifld-worker-worktrees/31');head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head!=report['code_head'];assert not subprocess.check_output(['git','diff',report['code_head'],head,'--','src','tests','scripts'],cwd=repo)
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(r/'dagster'),HIFLD_STAGING_DIR=str(r/'staging'),HIFLD_DATASETS_DIR=str(r/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.assets.fws_units import fws_cbrs_units
from dagster_hifld.sources import cbrs_units as s
from dagster_hifld.resources import StagingStorageResource
stage=StagingStorageResource(local_dir=str(r/'staging'));root=r/'staging'/out['dataset_slug']/out['file_slug']/out['version'];binding=r/'staging/_source-provenance'/out['dataset_slug']/out['version']/'bundle.json';bound=binding.read_bytes();before=s.candidate_inventory(root);published=s.candidate_inventory(r/'published');original=json.loads(bound);assert original['manifest']['code_commit']==report['code_head']
job=defs.resolve_job_def('fws_cbrs_units_draft_refresh');capture=repo/'docs/dataset-sources/cbrs-units-2/evidence/captures';runs=[]
with DagsterInstance.get() as instance:
 for kind,path in [('actual_docs_only_head_reuse',capture),('same_content_synthetic_later_observations',r/'later-observations')]:
  if kind=='same_content_synthetic_later_observations':
   path.mkdir()
   for name in s.URLS:
    # New fetch-observation receipts with unchanged complete captures. Preserve
    # bodies as a separate bounded input capture; no real acceptance implied.
    (path/(name+'.body')).write_bytes((capture/(name+'.body')).read_bytes())
    receipt=json.loads((capture/(name+'.receipt.json')).read_bytes());receipt['observed_at']=datetime.now(timezone.utc).isoformat();receipt['synthetic_receipt_observation_fixture']=True;(path/(name+'.receipt.json')).write_bytes(s.encode(receipt))
  result=job.execute_in_process(instance=instance,run_config={'ops':{fws_cbrs_units.op.name:{'config':{'capture_dir':str(path)}}}},raise_on_error=False)
  errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
  assert result.success,errors;assert result.output_for_node(fws_cbrs_units.op.name)==out
  assert s.candidate_inventory(root)==before and binding.read_bytes()==bound and s.candidate_inventory(r/'published')==published
  runs.append({'kind':kind,'run_id':result.run_id,'success':True,'actual_head':head,'original_construction_head':report['code_head'],'candidate_and_binding_unchanged':True,'prior_published_preserved':True})
  if kind=='same_content_synthetic_later_observations':
   for name in s.URLS:
    receipt=json.loads((path/(name+'.receipt.json')).read_bytes());key=f'_raw-captures/{s.SLUG}/{receipt["sha256"]}/{s.sha(s.encode(receipt))}.receipt.json';assert stage.read_key(key)==s.encode(receipt)
(r/'docs-and-observations-refresh.json').write_text(json.dumps({'approved':False,'runs':runs,'new_observations_losslessly_retained_separately':True,'original_construction_history_retained':True},indent=2)+'\n');print(json.dumps(runs))
