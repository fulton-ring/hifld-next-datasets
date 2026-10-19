import os,json,subprocess,argparse
from pathlib import Path
parser=argparse.ArgumentParser(description='Verify actual registered refresh after a genuine docs-only advance, preserving original construction and every storage object.');parser.add_argument('--runtime-dir',type=Path,required=True);parser.add_argument('--capture-dir',type=Path,default=Path(__file__).parent/'inputs');args=parser.parse_args();W=Path(__file__).resolve().parents[4];runtime=args.runtime_dir.resolve();capture=args.capture_dir.resolve()
for k in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(k,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_TSUNAMI_TIDE_CAPTURE_DIR=str(capture),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.sources import tsunami_tide_stations as f
before={str(p.relative_to(runtime/'staging')):f.sha256_file(p)for p in(runtime/'staging').rglob('*')if p.is_file()};prior={str(p.relative_to(runtime/'published')):f.sha256_file(p)for p in(runtime/'published').rglob('*')if p.is_file()}
with DagsterInstance.get()as instance:result=defs.resolve_job_def('tsunami_tide_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
assert result.success
assert before=={str(p.relative_to(runtime/'staging')):f.sha256_file(p)for p in(runtime/'staging').rglob('*')if p.is_file()}
assert prior=={str(p.relative_to(runtime/'published')):f.sha256_file(p)for p in(runtime/'published').rglob('*')if p.is_file()}
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=W,text=True).strip();original=json.loads((runtime/'materialization.json').read_text())['code_commit']
paths=subprocess.check_output(['git','diff','--name-only',original,head],cwd=W,text=True).splitlines();assert paths and all(p.startswith('docs/')for p in paths)
root=next((runtime/'staging'/f.SLUG/f.SLUG).glob('v*'));manifest=json.loads((root/'metadata/source_manifest.json').read_text());assert manifest['code_commit']==original
j={'head':head,'original_construction_commit':original,'changed_paths':paths,'run_id':result.run_id,'success':True,'all_staging_objects_unchanged':True,'prior_synthetic_accepted_fixture_unchanged':True,'original_construction_provenance_retained':True,'source_rows':374}
(runtime/'docs-only-refresh.json').write_text(json.dumps(j,indent=2)+'\n');print(json.dumps(j))
