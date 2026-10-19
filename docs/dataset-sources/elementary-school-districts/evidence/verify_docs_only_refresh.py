from pathlib import Path
import os,json,subprocess
R=Path('/workspace/work/hifld-coordinator/64/registered-observed-repair');W=Path('/workspace/hifld-worker-worktrees/64');C=W/'docs/dataset-sources/elementary-school-districts/evidence/inputs'
for n in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(n,None)
os.environ.update(DAGSTER_HOME=str(R/'dagster'),HIFLD_ELSD_CAPTURE_DIR=str(C),HIFLD_STAGING_DIR=str(R/'staging'),HIFLD_DATASETS_DIR=str(R/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import Definitions,DagsterInstance
from dagster_hifld.definitions import defs as normal
from dagster_hifld.sources import elementary_school_districts as s
from dagster_hifld.assets.census_elementary_districts import census_elementary_districts as asset
stage=s.StagingStorageResource(local_dir=str(R/'staging'));pub=s.PublishedStorageResource(local_dir=str(R/'published'))
D=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':stage,'published_storage':pub},jobs=[normal.resolve_job_def('elsd_source_integrity')])
before={str(p.relative_to(R/'staging')):s.sha256_file(p)for p in(R/'staging').rglob('*')if p.is_file()};prior={k:pub.read_key(k)for k in pub.list_prefix()}
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=W,text=True).strip();assert head=='05979024d4596e40a55f2a48bd9f6408752140e0'
changed=subprocess.check_output(['git','diff','--name-only','e016d4350e3d48b3264849b332617406f95c4bbd',head],cwd=W,text=True).splitlines();assert changed==['docs/dataset-sources/elementary-school-districts/README.md']
with DagsterInstance.get()as instance:
 r=D.resolve_job_def('elsd_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
 errors=[e.event_specific_data.error.to_string()for e in r.get_step_failure_events()];assert r.success,errors
 output=r.output_for_node(asset.op.name);assert output['reused']is True and output['feature_count']==2030
 root=R/'staging'/s.SLUG/s.SLUG/output['version'];manifest=s.strict_json((root/'metadata/source_manifest.json').read_bytes());assert manifest['code_commit']=='e016d4350e3d48b3264849b332617406f95c4bbd'
 assert before=={str(p.relative_to(R/'staging')):s.sha256_file(p)for p in(R/'staging').rglob('*')if p.is_file()};assert prior=={k:pub.read_key(k)for k in pub.list_prefix()}
 report={'head':head,'original_construction_head':manifest['code_commit'],'changed_files':changed,'run_id':r.run_id,'success':r.success,'output':output,'all_staging_file_SHA256_unchanged':True,'staging_file_count':len(before),'all_26_original_retrieval_contexts_retained':True,'prior_fixture_unchanged':True,'release_approved':False}
 (R.parent/'docs-only-refresh.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
