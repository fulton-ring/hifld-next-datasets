import os,json,pathlib,subprocess,threading,argparse
from unittest.mock import patch
parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=pathlib.Path,required=True);args=parser.parse_args();r=args.runtime.resolve();assert not r.exists()
for n in ['dagster','staging','published']:(r/n).mkdir(parents=True)
(r/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for n in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(n,None)
os.environ.update(DAGSTER_HOME=str(r/'dagster'),HIFLD_STAGING_DIR=str(r/'staging'),HIFLD_DATASETS_DIR=str(r/'published'))
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.fws import fws_cbrs_buffer_zone as asset
from dagster_hifld.sources import cbrs_buffer as s
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
repo=pathlib.Path(__file__).resolve().parents[1];capture=repo/'docs/dataset-sources/cbrs-buffer-zone-1/evidence/captures'
stage=StagingStorageResource(local_dir=str(r/'staging'));pub=PublishedStorageResource(local_dir=str(r/'published'));pub.write_key('prior-local-fixture.bin',b'local fixture, no accepted real release')
def inventory(p):return {str(q.relative_to(p)):s.sha256_file(q) for q in p.rglob('*') if q.is_file()}
prior=inventory(r/'published')
defs=Definitions(assets=normal.assets,resources={'staging_storage':stage,'published_storage':pub},jobs=[define_asset_job('actual_registered_cbrs_source',selection=AssetSelection.assets(asset))])
config={'ops':{asset.op.name:{'config':{'capture_dir':str(capture)}}}};report={'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'approved':False,'runs':[]}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()];report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors});return errors
with DagsterInstance.get() as instance:
 job=defs.resolve_job_def('actual_registered_cbrs_source')
 first=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('fresh_complete_real1145_source',first);assert first.success
 o=first.output_for_node(asset.op.name);assert o['feature_count']==1145;prefix=f'{s.SLUG}/{s.SLUG}/{o["version"]}';root=r/'staging'/prefix;before=inventory(root)
 assert s.sha256_file(root/'geopackage'/f'{s.SLUG}.gpkg')=='29edab0b125fc438930422b0f5ff395869d25fa4854077222632515423c79a56'
 assert s.sha256_file(root/'candidate/typed-source.parquet')=='54b294ce12e616ade4b8fa8b00bd27cab9ce320a7ef4af7ce24807a7d4e1480e'
 marker=root/'metadata/cbrs_buffer_candidate_ready.json';ready=json.loads(marker.read_text());payloads={n:(root/n).read_bytes() for n in ready['objects']};marker.unlink();bad=inventory(root)
 started=threading.Event();winner=threading.Event();threads=[];thread_errors=[];original_q=s.quarantine_rejected_cache_held;original_diag=s.quarantine
 def install():
  try:started.set();s.commit_complete_candidate(stage,prefix,payloads,ready);winner.set()
  except Exception as e:thread_errors.append(str(e))
 def invalid_held(*args):
  b=threading.Thread(target=install);threads.append(b);b.start();assert started.wait(10)
  assert not winner.wait(.15),'installer bypassed per-version invalidation lock'
  return original_q(*args)
 def diagnostic(*args):
  assert winner.wait(30),'competing installer failed after invalidation lock release'
  return original_diag(*args)
 with patch.object(s,'quarantine_rejected_cache_held',side_effect=invalid_held),patch.object(s,'quarantine',side_effect=diagnostic):
  failed=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);errors=record('actual_registered_missing_ready_race',failed)
 for t in threads:t.join(10)
 assert not failed.success and any('incomplete_existing_candidate_missing_ready_receipt' in e for e in errors),errors
 assert not thread_errors and winner.is_set() and inventory(root)==before
 q=next((r/'staging/_quarantine'/s.SLUG/'rejected-cache').glob('*/'+root.name));assert inventory(q)==bad
 assert s.sha256_file(q/'raw'/s.ARCHIVE_NAME)==s.PIN and inventory(r/'published')==prior
 assert (s.SLUG,s.SLUG,o['version']) in list(_iter_staged_version_paths(stage))
 reuse=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('winning_complete_real_cache_reuse',reuse);assert reuse.success and inventory(root)==before
 report.update(output=o,real_rows=1145,original_GP_and_typedParquet_payloads_unchanged=True,installer_blocked_under_invalid_lookup_lock=True,winner_installed_before_outer_diagnostic=True,winning_valid_cache_preserved_and_reused=True,invalid_cache_moved_intact=str(q),invalid_rawZIP_preserved=True,prior_local_published_fixture_preserved=inventory(r/'published')==prior)
(r/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
