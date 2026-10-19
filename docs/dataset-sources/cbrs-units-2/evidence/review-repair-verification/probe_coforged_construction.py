import os,json,hashlib
from pathlib import Path
runtime=Path('/workspace/work/hifld-coordinator/31/graph-review-repair');repo=Path('/workspace/hifld-worker-worktrees/31')
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.sources import cbrs_units as s
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.provenance import provenance_projection
execution=json.loads((runtime/'execution.json').read_bytes());version=execution['output']['version'];root=runtime/'staging'/s.SLUG/s.SLUG/version
metadata={str(p.relative_to(root)) :p.read_bytes() for p in (root/'metadata').rglob('*') if p.is_file()}
raw={str(p.relative_to(root)):s.sha(p.read_bytes()) for p in (root/'raw').rglob('*') if p.is_file()}
binding=runtime/'staging/_source-provenance'/s.SLUG/version/'bundle.json';binding_original=binding.read_bytes()
pub={str(p.relative_to(runtime/'published')):s.sha(p.read_bytes()) for p in (runtime/'published').rglob('*') if p.is_file()}
try:
 original=(root/'metadata/source_manifest.json').read_bytes();manifest=json.loads(original);original_commit=manifest['code_commit'];manifest['code_commit']='0'*40;forged=s.encode(manifest)
 bundle=json.loads(binding_original);bundle['manifest']=manifest;binding.write_bytes(s.encode(bundle))
 (root/'metadata/source_manifest.json').write_bytes(forged)
 ready=json.loads(metadata['metadata/cbrs_units_candidate_ready.json']);ready['objects']['metadata/source_manifest.json']=s.sha(forged);(root/'metadata/cbrs_units_candidate_ready.json').write_bytes(s.encode(ready))
 for path in (root/'metadata/source').glob('*source_manifest.json'):
  if path.name=='source_manifest.json':path.write_bytes(forged)
  else:path.unlink()
 (root/'metadata/source'/('version_'+s.sha(forged)+'_source_manifest.json')).write_bytes(forged)
 storage=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True)
 resolved=provenance_projection(load_resolved_source_manifest(storage,s.SLUG,s.SLUG,version).metadata)
 (root/'metadata/resolved_provenance.json').write_bytes(s.encode(resolved))
 with DagsterInstance.get() as instance:
  result=defs.resolve_job_def('fws_cbrs_units_draft_refresh').execute_in_process(instance=instance,run_config={'ops':{'ingest__cbrs_units_2__cbrs_units_2':{'config':{'capture_dir':str(repo/'docs/dataset-sources/cbrs-units-2/evidence/captures')}}}},raise_on_error=False)
  errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report={'source_checkpoint':'4cdb6bcb80b6fb66ff8fffeb86ae760fa544de19','probe':'canonical and external first-construction bundle co-forged original code_commit, all candidate snapshots/resolved/ready self-repinned; implementation behavior only, never source approval','original_commit':original_commit,'forged_commit':'0'*40,'run_id':result.run_id,'success':result.success,'errors':errors,'forged_manifest_persisted':(root/'metadata/source_manifest.json').exists() and json.loads((root/'metadata/source_manifest.json').read_bytes())['code_commit']=='0'*40,'raw_preserved':raw=={str(p.relative_to(root)):s.sha(p.read_bytes()) for p in (root/'raw').rglob('*') if p.is_file()},'published_preserved':pub=={str(p.relative_to(runtime/'published')):s.sha(p.read_bytes()) for p in (runtime/'published').rglob('*') if p.is_file()}}
 Path('/workspace/work/hifld-coordinator/31/graph-review-repair/coforged-provenance-probe.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
finally:
 binding.write_bytes(binding_original)
 if not root.exists():
  retained=[p for p in (runtime/'staging/_quarantine'/s.SLUG/'versions').glob('*/candidate') if p.exists()]
  assert len(retained)==1;os.rename(retained[0],root)
 if root.exists():
  for p in list((root/'metadata').rglob('*')):
   if p.is_file() and str(p.relative_to(root)) not in metadata:p.unlink()
  for name,body in metadata.items():
   p=root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
