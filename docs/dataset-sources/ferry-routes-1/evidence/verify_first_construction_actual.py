"""Actual registered full-source retry protocol; requires completed normal runner runtime."""
import argparse,json,os,subprocess
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--runtime-dir',type=Path,required=True);p.add_argument('--capture-dir',type=Path,required=True);a=p.parse_args();r=a.runtime_dir.resolve()
for k in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(k,None)
os.environ.update(DAGSTER_HOME=str(r/'dagster'),HIFLD_FERRY_ROUTES_CAPTURE_DIR=str(a.capture_dir.resolve()),HIFLD_STAGING_DIR=str(r/'staging'),HIFLD_DATASETS_DIR=str(r/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.sources import ferry_routes as f
s=r/'staging';root=next((s/f.SLUG/f.SLUG).glob('v*'));primary=next((s/'_source-provenance').rglob('bundle.json'));marker=next((s/'_construction-state').rglob('*.json'));markerbytes=marker.read_bytes();state=json.loads(markerbytes);evidence=s/state['original_bundle_evidence_key'];original=evidence.read_bytes();assert original==primary.read_bytes();assert f.digest(original)==state['original_bundle_sha256']
raw={str(p.relative_to(s)):f.sha256_file(p)for p in(s/'_raw-captures').rglob('*')if p.is_file()};prior={str(p.relative_to(r/'published')):f.sha256_file(p)for p in(r/'published').rglob('*')if p.is_file()}
candidate={str(p.relative_to(root)):f.sha256_file(p)for p in root.rglob('*')if p.is_file()};existing_q=set((s/'_quarantine'/f.SLUG).glob('invalid-candidate-*'))
report={'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(f.__file__).parents[3],text=True).strip(),'classification':'Actual registered source lifecycle; trusted original restoration is explicit test action, no real release accepted','runs':[]}
with DagsterInstance.get()as instance:
    primary.unlink()
    for i in range(3):
        result=defs.resolve_job_def('ferry_routes_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
        errors=[e.event_specific_data.error.to_string()for e in result.get_step_failure_events()];report['runs'].append({'label':'missing_primary_retry_'+str(i+1),'run_id':result.run_id,'success':result.success,'errors':errors})
        assert not result.success and any('after_first_construction'in e for e in errors)
        assert not primary.exists()and not root.exists();assert marker.read_bytes()==markerbytes and evidence.read_bytes()==original
        if i==0:
            fresh_q=set((s/'_quarantine'/f.SLUG).glob('invalid-candidate-*'))-existing_q;assert len(fresh_q)==1
            preserved=next(iter(fresh_q));assert candidate=={str(p.relative_to(preserved)):f.sha256_file(p)for p in preserved.rglob('*')if p.is_file()}
    primary.parent.mkdir(parents=True,exist_ok=True);primary.write_bytes(original)
    for label in ['exact_trusted_original_restored_rebuild','original_binding_reuse']:
        result=defs.resolve_job_def('ferry_routes_source_integrity').execute_in_process(instance=instance,raise_on_error=False);assert result.success
        output=result.output_for_node('ingest__ferry_routes_1__ferry_routes_1');assert output['feature_count']==964
        report['runs'].append({'label':label,'run_id':result.run_id,'success':result.success,'output':output})
assert raw=={str(p.relative_to(s)):f.sha256_file(p)for p in(s/'_raw-captures').rglob('*')if p.is_file()}
assert prior=={str(p.relative_to(r/'published')):f.sha256_file(p)for p in(r/'published').rglob('*')if p.is_file()}
assert marker.read_bytes()==markerbytes and evidence.read_bytes()==original
q=list((s/'_quarantine'/f.SLUG).glob('invalid-candidate-*'));assert q
report.update(raw_attempts_unchanged=True,prior_synthetic_accepted_fixture_unchanged=True,permanent_state_and_full_original_CAS_unchanged=True,whole_invalid_candidate_preserved=True,repeated_retry_cannot_rebase_original_construction=True,all_964_real_rows_rebuilt=True)
(r/'first-construction-actual.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
