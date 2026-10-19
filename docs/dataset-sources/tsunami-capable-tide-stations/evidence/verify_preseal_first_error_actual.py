"""Actual registered invalid first code-claim fixture leaves trust empty; later valid build works."""
import json,os,subprocess
from pathlib import Path
from unittest.mock import patch
R=Path('/workspace/work/hifld-coordinator/334');r=R/'actual-preseal-first-error';C=R/'capture'
if r.exists()and any(r.iterdir()):raise ValueError('empty isolated runtime required')
for n in ['dagster','staging','published']:(r/n).mkdir(parents=True,exist_ok=True)
(r/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n');(r/'published/synthetic-accepted-fixture.bin').write_bytes(b'Keep synthetic prior fixture; no NOAA source accepted.')
for k in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(k,None)
os.environ.update(DAGSTER_HOME=str(r/'dagster'),HIFLD_TSUNAMI_TIDE_CAPTURE_DIR=str(C),HIFLD_STAGING_DIR=str(r/'staging'),HIFLD_DATASETS_DIR=str(r/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.sources import tsunami_tide_stations as f
original=f.manifest_for
report={'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(f.__file__).parents[3],text=True).strip(),'scope':'Actual registered full374 source with synthetic invalid first code-claim fixture, not malformed producer evidence or release acceptance','runs':[]};prior={str(p.relative_to(r/'published')):f.sha256_file(p)for p in(r/'published').rglob('*')if p.is_file()}
def invalid(captures,identity):
 m=original(captures,identity);m['code_commit']='0'*40;return m
with DagsterInstance.get()as instance:
 with patch.object(f,'manifest_for',side_effect=invalid):result=defs.resolve_job_def('tsunami_tide_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
 errors=[e.event_specific_data.error.to_string()for e in result.get_step_failure_events()];assert not result.success and any('construction_invalid_commit'in e for e in errors);assert not[p for p in(r/'staging').glob('_construction-state/**/*')if p.is_file()];assert not list((r/'staging').glob(f'{f.SLUG}/{f.SLUG}/v*'));report['runs'].append({'label':'invalid_first_code_claim_rejected_before_seal','run_id':result.run_id,'success':False,'errors':errors});report['zero_trust_records_after_invalid_first_claim']=True
 for label in ['valid_first_claim_after_failed_attempt','valid_original_reuse']:
  result=defs.resolve_job_def('tsunami_tide_source_integrity').execute_in_process(instance=instance,raise_on_error=False);assert result.success;out=result.output_for_node('ingest__tsunami_capable_tide_stations__tsunami_capable_tide_stations');assert out['feature_count']==374;report['runs'].append({'label':label,'run_id':result.run_id,'success':True,'output':out})
 assert prior=={str(p.relative_to(r/'published')):f.sha256_file(p)for p in(r/'published').rglob('*')if p.is_file()};report['prior_synthetic_accepted_fixture_unchanged']=True;report['all_original_source_raw_bodies_preserved']=all((r/'staging'/f'_raw-captures/{f.SLUG}/{f.digest((C/(n+".body")).read_bytes())}/{n}.body').read_bytes()==(C/(n+'.body')).read_bytes()for n in f.URLS)
(r/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'source_commit':report['source_commit'],'runs':[(x['label'],x['run_id'],x['success'])for x in report['runs']]}))
