"""Actual source-job repair replay using complete574 source records; no terminal rerun."""
from __future__ import annotations
import argparse
import ast
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);parser.add_argument('--prior-runtime',type=Path,required=True);args=parser.parse_args()
runtime=args.runtime.resolve();runtime.mkdir(parents=True,exist_ok=False)
for n in ['dagster','staging','published']:(runtime/n).mkdir()
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for n in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(n,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import Definitions,DagsterInstance
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.fws_map_panels import fws_cbrs_map_panels,fws_cbrs_map_panels_draft_refresh_job
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import cbrs_map_panels as s
repo=Path(__file__).resolve().parents[1];capture=repo/'docs/dataset-sources/cbrs-map-panels-1/evidence/captures'
staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
published.write_key('prior-synthetic-accepted-fixture.bin',b'fixture-only-not-real-accepted-data')
def files(root):return {str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
def published_snapshot():return {n:s.sha(published.read_key(n)) for n in published.list_prefix()}
def raw_snapshot():return files(runtime/'staging/_raw-captures')
priorpub=published_snapshot();report={'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'approved':False,'source_module':s.__file__,'runs':[]}
def record(kind,result):
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]})
defs=Definitions(assets=normal.assets,resources={'staging_storage':staging,'published_storage':published},jobs=[fws_cbrs_map_panels_draft_refresh_job])
config={'ops':{fws_cbrs_map_panels.op.name:{'config':{'capture_dir':str(capture)}}}}
with DagsterInstance.get() as instance:
 job=defs.resolve_job_def('fws_cbrs_map_panels_draft_refresh')
 first=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('complete_real_source',first);assert first.success
 output=first.output_for_node(fws_cbrs_map_panels.op.name);assert output['feature_count']==574
 prefix=f"{s.SLUG}/{s.SLUG}/{output['version']}";root=runtime/'staging'/prefix;pristine=runtime/'pristine';shutil.copytree(root,pristine);before_raw=raw_snapshot()
 again=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('complete_refresh',again);assert again.success and files(root)==files(pristine)
 priorreport=json.loads((args.prior_runtime/'execution.json').read_bytes());priorroot=args.prior_runtime/'staging'/s.SLUG/s.SLUG/priorreport['output']['version']
 report['unchanged_payloads']={}
 for name in [f'geopackage/{s.SLUG}.gpkg','candidate/typed-source.parquet','metadata/candidate.records.jsonl',f'raw/{s.ARCHIVE_NAME}']:
  assert (root/name).read_bytes()==(priorroot/name).read_bytes();report['unchanged_payloads'][name]=s.sha((root/name).read_bytes())
 # Data constructors are identical to the previously fully catalogued/formatted proof.
 path='src/dagster_hifld/sources/cbrs_map_panels.py';old=ast.parse(subprocess.check_output(['git','show','814cc2b:'+path],cwd=repo,text=True));new=ast.parse((repo/path).read_text())
 report['unchanged_data_constructors']={}
 for name in ['inspect_source','typed_table','write_geopackage']:
  a=ast.dump(next(n for n in old.body if isinstance(n,ast.FunctionDef) and n.name==name));b=ast.dump(next(n for n in new.body if isinstance(n,ast.FunctionDef) and n.name==name));assert a==b;report['unchanged_data_constructors'][name]=s.sha(a.encode())
 for mode in ['missing_ready','source_fidelity_population','blocked_baseline_claim']:
  if not root.exists():shutil.copytree(pristine,root)
  marker=root/'metadata/cbrs_map_panels_candidate_ready.json';ready=json.loads(marker.read_bytes())
  if mode=='missing_ready':marker.unlink()
  else:
   relative='metadata/source-fidelity/baseline_comparison.json' if mode=='source_fidelity_population' else 'metadata/baseline_comparison.json';path=root/relative;doc=json.loads(path.read_bytes())
   if mode=='source_fidelity_population':doc['populations']['baseline']['rows']=1;doc['populations']['candidate']['rows']=1;doc['retained_rows_compared']=1;doc['baseline']['approved']=True
   else:doc.update(status='pass',approved=True,acceptance='approved')
   path.write_bytes(s.encode(doc));ready['objects'][relative]=s.sha(path.read_bytes());marker.write_bytes(s.encode(ready))
  before=files(root);oldq=set((runtime/'staging/_quarantine'/s.SLUG).glob('invalid-cache-*'))
  bad=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record(mode,bad);assert not bad.success
  assert not root.exists() and (s.SLUG,s.SLUG,output['version']) not in list(_iter_staged_version_paths(staging))
  added=set((runtime/'staging/_quarantine'/s.SLUG).glob('invalid-cache-*'))-oldq;assert len(added)==1
  retained=added.pop()/'candidate';assert files(retained)==before
  assert published_snapshot()==priorpub and raw_snapshot()==before_raw
  report['runs'][-1].update(quarantined_every_byte_exact=True,canonical_undiscoverable=True,prior_published_and_raw_unchanged=True)
 shutil.copytree(pristine,root)
 restored=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('restored_source_refresh',restored);assert restored.success and files(root)==files(pristine)
 assert published_snapshot()==priorpub and raw_snapshot()==before_raw
report.update(output=output,all574_data_payloads_byte_identical_to_prior_catalog_format_proof=True,prior_terminal_proof='b0b1de8aec3a14026a9e0f8bfdec023e74281cb1',no_terminal_graph_repeated=True,prior_published_and_raw_unchanged=True)
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps([(r['kind'],r['run_id'],r['success']) for r in report['runs']]))
