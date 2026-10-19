"""Registered actual8-row provenance denials and trusted original reconstruction."""
import argparse,json,os,subprocess
from pathlib import Path
parser=argparse.ArgumentParser();parser.add_argument('--runtime-dir',type=Path,required=True);parser.add_argument('--repo',type=Path,default=Path.cwd());args=parser.parse_args();repo=args.repo.resolve();rt=args.runtime_dir.resolve();data=json.loads((rt/'materialization.json').read_text())
for n in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(n,None)
os.environ.update(DAGSTER_HOME=str(rt/'dagster'),HIFLD_STAGING_DIR=str(rt/'staging'),HIFLD_DATASETS_DIR=str(rt/'published'),HIFLD_SHADOW_MANUAL_ONLY='1',HIFLD_CONCITY_CAPTURE_DIR=str(repo/'docs/dataset-sources/consolidated-cities/evidence/inputs'))
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.sources import consolidated_cities as s
from dagster_hifld.assets.census_consolidated_cities import census_consolidated_cities
root=rt/'staging'/s.SLUG/s.SLUG/data['version'];bundle=next((rt/'staging/_source-provenance'/s.SLUG).glob('*/bundle.json'));original_bundle=bundle.read_bytes();manifest=root/'metadata/source_manifest.json';original_manifest=manifest.read_bytes()
source_objects=json.loads((root/s.READY).read_bytes())['objects'];raw={str(p.relative_to(rt/'staging')):s.sha256_file(p) for p in (rt/'staging/_raw-captures').rglob('*') if p.is_file()};prior={str(p.relative_to(rt/'published')):s.sha256_file(p) for p in (rt/'published').rglob('*') if p.is_file()}
report={'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'approved':False,'runs':[]}
with DagsterInstance.get() as inst:
 for case in ['coforged_zero','coforged_reachable_wrong_source','missing_anchor','malformed_anchor']:
  if case.startswith('coforged'):
   m=json.loads(original_manifest);m['code_commit']='0'*40 if case=='coforged_zero' else '6969d05028352eefd20348a738a829034af9882f';manifest.write_bytes(s.encode(m));b=json.loads(original_bundle);b['manifest']=m;bundle.write_bytes(s.encode(b));v=json.loads((root/s.READY).read_bytes());v['objects']['metadata/source_manifest.json']=s.sha256_file(manifest);(root/s.READY).write_bytes(s.encode(v))
  elif case=='missing_anchor':bundle.unlink()
  else:bundle.write_bytes(b'{}')
  r=defs.resolve_job_def('concity_source_integrity').execute_in_process(instance=inst,raise_on_error=False);err=[e.event_specific_data.error.to_string() for e in r.get_step_failure_events()];assert not r.success and not root.exists(),err
  report['runs'].append({'case':case,'run_id':r.run_id,'success':r.success,'errors':err,'whole_candidate_quarantined':True})
  bundle.write_bytes(original_bundle)
  r=defs.resolve_job_def('concity_source_integrity').execute_in_process(instance=inst,raise_on_error=False);assert r.success
  report['runs'].append({'case':'trusted_original_rebuild_after_'+case,'run_id':r.run_id,'success':True})
  assert manifest.read_bytes()==original_manifest and bundle.read_bytes()==original_bundle
  assert all(s.sha256_file(root/name)==pin for name,pin in source_objects.items())
 assert raw=={str(p.relative_to(rt/'staging')):s.sha256_file(p) for p in (rt/'staging/_raw-captures').rglob('*') if p.is_file()}
 assert prior=={str(p.relative_to(rt/'published')):s.sha256_file(p) for p in (rt/'published').rglob('*') if p.is_file()}
report.update(original_full_manifest_bundle_preserved=True,original_raw_and_prior_fixture_preserved=True,all_original_native_bytes_exact_after_each_rebuild=True)
(rt/'authenticated-provenance.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'runs':[(r['case'],r['run_id'],r['success']) for r in report['runs']],'all_assertions_pass':True}))
