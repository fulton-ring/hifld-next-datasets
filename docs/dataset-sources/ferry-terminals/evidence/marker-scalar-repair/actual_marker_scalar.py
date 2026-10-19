import json,os,subprocess
from pathlib import Path
from unittest.mock import patch
rt=Path('/workspace/work/hifld-coordinator/89/marker-scalar-actual');assert not rt.exists()
for name in ['dagster','staging','published']:(rt/name).mkdir(parents=True)
(rt/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for k in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(k,None)
os.environ.update(DAGSTER_HOME=str(rt/'dagster'),HIFLD_STAGING_DIR=str(rt/'staging'),HIFLD_DATASETS_DIR=str(rt/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.assets.bts_ferry_terminals import bts_ferry_terminals
from dagster_hifld.sources import ferry_terminals as s
report={'source_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'approved':False,'scope':'Actual complete845-row registered graph; scalar marker corruption fixtures, no release','runs':[]}
capture=Path('/workspace/hifld-worker-worktrees/89/docs/dataset-sources/ferry-terminals/evidence/captures');config={'ops':{bts_ferry_terminals.op.name:{'config':{'capture_dir':str(capture)}}}}
prior=rt/'published/prior.bin';prior.write_bytes(b'Synthetic preserved prior fixture; no dataset approval.')
def snap(root):return {str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
prior_before=snap(rt/'published')
def record(label,r):
 errors=[e.event_specific_data.error.to_string() for e in r.get_step_failure_events()];report['runs'].append({'kind':label,'run_id':r.run_id,'success':r.success,'errors':errors});(rt/'execution-progress.json').write_bytes(s.encode(report));assert snap(rt/'published')==prior_before;return errors
with DagsterInstance.get() as inst:
 job=defs.resolve_job_def('bts_ferry_terminals_draft_refresh');r=job.execute_in_process(instance=inst,run_config=config,raise_on_error=False);record('complete_real_source',r);assert r.success
 output=r.output_for_node(bts_ferry_terminals.op.name);root=rt/'staging'/s.SLUG/s.SLUG/output['version'];before=snap(root);raw_before=snap(rt/'staging/_raw-captures')
 base=rt/'staging/_candidate-construction'/s.SLUG;alias=base/(output['version']+'.json');marker=alias.with_name(alias.name+'.first-construction.json');saved=marker.read_bytes();state_before=snap(base)
 for value,label in [(True,'boolean'),(1.0,'float')]:
  changed=json.loads(saved);changed['schema_version']=value;marker.write_bytes(s.encode(changed))
  for attempt in [1,2]:
   with patch.object(s,'write_canonical_geopackage',wraps=s.write_canonical_geopackage) as writer:r=job.execute_in_process(instance=inst,run_config=config,raise_on_error=False)
   errors=record(label+'_invalid_marker_attempt_'+str(attempt),r);assert not r.success and writer.call_count==0 and any('missing_or_invalid_first_construction_marker' in e for e in errors);assert not root.exists();assert snap(rt/'staging/_raw-captures')==raw_before
  marker.write_bytes(saved);r=job.execute_in_process(instance=inst,run_config=config,raise_on_error=False);record(label+'_exact_saved_marker_operator_restore_and_rebuild',r);assert r.success and snap(root)==before and snap(base)==state_before
 r=job.execute_in_process(instance=inst,run_config=config,raise_on_error=False);record('healthy_cache_refresh',r);assert r.success and snap(root)==before
 reference=json.loads(Path('/workspace/work/hifld-coordinator/89/complete-construction-actual/execution.json').read_text())['native_byte_parity'];assert {p:s.sha((root/p).read_bytes()) for p in reference}==reference
 report.update(output=output,native_byte_parity=reference,all845_native_rows_values_types_geometry_unchanged=True,all9original_receipts_and_state_raw_prior_preserved=True,explicit_original_marker_operator_restore_only=True)
(rt/'execution.json').write_bytes(s.encode(report));print(json.dumps({'source':report['source_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
