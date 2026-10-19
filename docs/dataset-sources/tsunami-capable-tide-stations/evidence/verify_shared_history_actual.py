"""Actual registered source adoption of shared first-construction missing-state gate."""
import json,os,subprocess
from pathlib import Path
R=Path('/workspace/work/hifld-coordinator/334');r=R/'actual-verified-dagster';C=R/'capture'
for k in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(k,None)
os.environ.update(DAGSTER_HOME=str(r/'dagster'),HIFLD_TSUNAMI_TIDE_CAPTURE_DIR=str(C),HIFLD_STAGING_DIR=str(r/'staging'),HIFLD_DATASETS_DIR=str(r/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.sources import tsunami_tide_stations as f
s=r/'staging';root=next((s/f.SLUG/f.SLUG).glob('v*'));record=next(p for p in(s/'_construction-state'/f.SLUG).glob('*.json')if not p.name.endswith('.first-construction.json'));original=record.read_bytes();marker=Path(str(record)+'.first-construction.json');mark=marker.read_bytes();state=json.loads(mark);cas=s/state['original_bundle_evidence_key'];evidence=cas.read_bytes();assert original==evidence
raw={str(p.relative_to(s)):f.sha256_file(p)for p in(s/'_raw-captures').rglob('*')if p.is_file()};prior={str(p.relative_to(r/'published')):f.sha256_file(p)for p in(r/'published').rglob('*')if p.is_file()};before={str(p.relative_to(root)):f.sha256_file(p)for p in root.rglob('*')if p.is_file()};existing=set((s/'_quarantine'/f.SLUG).glob('invalid-candidate-*'));report={'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(f.__file__).parents[3],text=True).strip(),'scope':'Actual registered complete374 source, synthetic missing first alias; no real release accepted','runs':[]}
with DagsterInstance.get()as instance:
 record.unlink()
 for i in range(3):
  result=defs.resolve_job_def('tsunami_tide_source_integrity').execute_in_process(instance=instance,raise_on_error=False);errors=[e.event_specific_data.error.to_string()for e in result.get_step_failure_events()];report['runs'].append({'label':'missing_original_alias_retry_'+str(i+1),'run_id':result.run_id,'success':result.success,'errors':errors});assert not result.success and any('construction_missing_original_state'in e for e in errors);assert not root.exists()and not record.exists();assert marker.read_bytes()==mark and cas.read_bytes()==evidence
  if i==0:
   q=set((s/'_quarantine'/f.SLUG).glob('invalid-candidate-*'))-existing;assert len(q)==1;kept=next(iter(q));assert before=={str(p.relative_to(kept)):f.sha256_file(p)for p in kept.rglob('*')if p.is_file()}
 record.write_bytes(original)
 for label in ['exact_trusted_original_alias_restored_rebuild','valid_original_reuse']:
  result=defs.resolve_job_def('tsunami_tide_source_integrity').execute_in_process(instance=instance,raise_on_error=False);assert result.success;out=result.output_for_node('ingest__tsunami_capable_tide_stations__tsunami_capable_tide_stations');assert out['feature_count']==374;report['runs'].append({'label':label,'run_id':result.run_id,'success':True,'output':out})
assert raw=={str(p.relative_to(s)):f.sha256_file(p)for p in(s/'_raw-captures').rglob('*')if p.is_file()};assert prior=={str(p.relative_to(r/'published')):f.sha256_file(p)for p in(r/'published').rglob('*')if p.is_file()};assert marker.read_bytes()==mark and cas.read_bytes()==evidence and record.read_bytes()==original
report.update(whole_invalid_candidate_retained=True,three_retries_fail_without_bootstrap=True,exact_trusted_original_restoration_required=True,raw_marker_fullCAS_prior_fixture_unchanged=True,all374_source_rows_reconstructed=True);(r/'shared-history-actual.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
