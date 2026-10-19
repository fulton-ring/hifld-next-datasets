"""Supplemental actual registered first-claim refusal or LIVE refresh.

Use source code published BEFORE construction. Explicit bad code claims are
fixture faults; no real source/contract/baseline/release is accepted here.
"""
import argparse,json,os,subprocess
from pathlib import Path
from unittest.mock import patch
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--mode',choices=['first-claim','live'],required=True);p.add_argument('--runtime-dir',type=Path,required=True);p.add_argument('--capture-dir',type=Path);a=p.parse_args();r=a.runtime_dir.absolute()
if a.mode=='first-claim':
    assert a.capture_dir and not r.exists()
    for part in('dagster','staging','published'):(r/part).mkdir(parents=True)
    (r/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
else:assert(r/'materialization.json').is_file()
for key in('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST','HIFLD_COLLEGES_CAPTURE_DIR'):os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(r/'dagster'),HIFLD_STAGING_DIR=str(r/'staging'),HIFLD_DATASETS_DIR=str(r/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
if a.mode=='first-claim':os.environ['HIFLD_COLLEGES_CAPTURE_DIR']=str(a.capture_dir.absolute())
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.sources import colleges_universities as c

def pins(root):return {str(p.relative_to(root)):c.sha256_file(p)for p in root.rglob('*')if p.is_file()}
report={'approved':False,'mode':a.mode,'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[1],text=True).strip()}
if a.mode=='first-claim':
    published=c.PublishedStorageResource(local_dir=str(r/'published'));published.write_key('prior-fixture.bin',b'Prior fixture only; no real acceptance.');prior=pins(r/'published');source=c.manifest_for
    def bad_claim(*args,**kwargs):
        value=source(*args,**kwargs);value['code_commit']='0'*40;return value
    with DagsterInstance.get()as instance,patch.object(c,'manifest_for',side_effect=bad_claim):result=defs.resolve_job_def('colleges_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
    errors=[e.event_specific_data.error.to_string()for e in result.get_step_failure_events()];report.update(run_id=result.run_id,success=result.success,errors=errors)
    (r/'result-progress.json').write_bytes(c.encode(report));assert not result.success and any('construction_invalid_commit'in e for e in errors);assert not(r/'staging/_construction-state').exists()and not list((r/'staging'/c.SLUG/c.SLUG).glob('v*'));assert pins(r/'published')==prior
    for name in c.URLS:
        body=(a.capture_dir/(name+'.body')).read_bytes();assert(r/'staging/_raw-captures'/c.SLUG/c.digest(body)/(name+'.body')).read_bytes()==body
    report.update(actual_full6438_source_native_build_then_real_Git_guard_rejection=True,raw_and_prior_preserved=True,no_shared_state_or_canonical_candidate=True,fixture_scope='Only returned manifest code_commit is set to zero; actual Git validator/source/native/shared state remain unpatched.')
    out=r/'result.json'
else:
    main=json.loads((r/'materialization.json').read_bytes());root=r/'staging'/c.SLUG/c.SLUG/main['version'];before=pins(root);raw=pins(r/'staging/_raw-captures');state=pins(r/'staging/_construction-state');prior=pins(r/'published');attempts=set((r/'staging/_acquisition-attempts'/c.SLUG).glob('*.json'))
    with DagsterInstance.get()as instance:result=defs.resolve_job_def('colleges_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
    errors=[e.event_specific_data.error.to_string()for e in result.get_step_failure_events()];report.update(run_id=result.run_id,success=result.success,errors=errors);out=r.parent/'live-registered-refresh.json';out.write_bytes(c.encode(report));assert result.success,errors
    output=result.output_for_node('ingest__colleges_and_universities__colleges_and_universities');assert output['reused']is True and output['feature_count']==6438 and output['version']==main['version'];assert pins(root)==before and pins(r/'published')==prior and pins(r/'staging/_construction-state')==state;assert raw.items()<=pins(r/'staging/_raw-captures').items()
    added=set((r/'staging/_acquisition-attempts'/c.SLUG).glob('*.json'))-attempts;assert len(added)==1;attempt=json.loads(next(iter(added)).read_bytes());assert set(attempt['objects'])==set(c.URLS);receipts={}
    for name,item in attempt['objects'].items():
        body=(r/'staging'/item['body_key']).read_bytes();receipt=json.loads((r/'staging'/item['raw_receipt_key']).read_bytes());c.validate_receipt(name,body,receipt);receipts[name]=receipt
    report.update(complete_LIVE_HTTP_responses=3,output=output,receipts=receipts,all_original_candidate_first_context_raw_and_prior_preserved=True)
out.write_bytes(c.encode(report));print(json.dumps({'mode':a.mode,'run_id':result.run_id,'expected_result_verified':True}))
