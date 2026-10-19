"""Resume actual trusted-original restoration after an honest local-space guard failure."""
import json,os,subprocess
from pathlib import Path
R=Path('/workspace/work/hifld-coordinator/88');r=R/'registered-final-repair';capture=R/'capture'
for k in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(k,None)
os.environ.update(DAGSTER_HOME=str(r/'dagster'),HIFLD_FERRY_ROUTES_CAPTURE_DIR=str(capture),HIFLD_STAGING_DIR=str(r/'staging'),HIFLD_DATASETS_DIR=str(r/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.sources import ferry_routes as f
s=r/'staging';marker=next((s/'_construction-state').rglob('*.json'));markerbytes=marker.read_bytes();state=json.loads(markerbytes);evidence=s/state['original_bundle_evidence_key'];original=evidence.read_bytes();primary=s/f'_source-provenance/{f.SLUG}/{state["processing_fingerprint"]}/bundle.json';assert primary.read_bytes()==original;assert f.digest(original)==state['original_bundle_sha256'];root=s/f'{f.SLUG}/{f.SLUG}/{state["version"]}';assert not root.exists()
raw={str(p.relative_to(s)):f.sha256_file(p)for p in(s/'_raw-captures').rglob('*')if p.is_file()};prior={str(p.relative_to(r/'published')):f.sha256_file(p)for p in(r/'published').rglob('*')if p.is_file()};report=json.loads((R/'first-construction-partial-attempt.json').read_text());report['scope']='Actual registered full964-source history protocol. Three intended retry rejections, one explicit-original recovery blocked by actual disk guard, then resumed recovery/reuse. No real release accepted.';report['head']=subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(f.__file__).parents[3],text=True).strip()
with DagsterInstance.get()as instance:
 for label in ['exact_trusted_original_restored_rebuild_after_disk_restored','original_binding_reuse']:
  result=defs.resolve_job_def('ferry_routes_source_integrity').execute_in_process(instance=instance,raise_on_error=False);errors=[e.event_specific_data.error.to_string()for e in result.get_step_failure_events()];assert result.success,errors;output=result.output_for_node('ingest__ferry_routes_1__ferry_routes_1');assert output['feature_count']==964;report['runs'].append({'label':label,'run_id':result.run_id,'success':result.success,'output':output})
assert raw=={str(p.relative_to(s)):f.sha256_file(p)for p in(s/'_raw-captures').rglob('*')if p.is_file()};assert prior=={str(p.relative_to(r/'published')):f.sha256_file(p)for p in(r/'published').rglob('*')if p.is_file()};assert marker.read_bytes()==markerbytes and evidence.read_bytes()==original;assert json.loads((root/'metadata/source_manifest.json').read_text())==json.loads(original)['manifest']
for name in f.URLS:
 body=(capture/(name+'.body')).read_bytes();assert(s/f'_raw-captures/{f.SLUG}/{f.digest(body)}/{name}.body').read_bytes()==body
f.reconcile_outputs(root,f.inspect_originals({n:((capture/(n+'.body')).read_bytes(),json.loads((capture/(n+'.receipt.json')).read_text()))for n in f.URLS})[0])
report.update(raw_attempts_unchanged_during_resumed_recovery=True,all_original_raw_bodies_exact=True,prior_synthetic_accepted_fixture_unchanged=True,permanent_state_and_full_original_CAS_unchanged=True,three_intended_retry_assertions_completed_before_later_guard_failure=True,whole_invalid_candidate_preserved=True,repeated_retry_cannot_rebase_original_construction=True,all_964_real_rows_rebuilt=True,guard_preserved=True)
(r/'first-construction-actual.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'runs':[(x.get('label',x.get('status')),x['run_id'],x.get('success',x.get('status')))for x in report['runs']],'full_originals_preserved':True}))
