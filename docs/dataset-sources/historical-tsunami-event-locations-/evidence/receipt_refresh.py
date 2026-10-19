"""Explicit synthetic receipt-only refresh, never a claim of a new HTTP fetch."""
import pathlib,json,os,shutil,datetime
p=pathlib.Path('/workspace/work/hifld-coordinator/113');runtime=p/'runtime';fixture=p/'synthetic-receipt-only-refresh';assert not fixture.exists();shutil.copytree(p/'captures',fixture)
for f in fixture.glob('*.receipt.json'):
 receipt=json.loads(f.read_text());receipt.update(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),replay_classification='Synthetic receipt-only refresh fixture; body replays retained original HTTP capture, not a new network observation.');f.write_text(json.dumps(receipt,sort_keys=True))
for key in ('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS'):os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_TSUNAMI_EVENTS_CAPTURE_DIR=str(fixture),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.assets.historical_tsunami_events import noaa_historical_tsunami_events
from dagster_hifld.sources import historical_tsunami_events as s
report=json.loads((runtime/'execution.json').read_text());version=report['output']['version'];root=runtime/'staging'/s.SLUG/s.SLUG/version
before={str(f.relative_to(root)):s.sha256_file(f) for f in root.rglob('*') if f.is_file()};prior={str(f.relative_to(runtime/'published')):s.sha256_file(f) for f in (runtime/'published').rglob('*') if f.is_file()}
with DagsterInstance.get() as instance:
 result=defs.resolve_job_def('tsunami_events_draft_refresh').execute_in_process(instance=instance,raise_on_error=False);assert result.success
 output=result.output_for_node(noaa_historical_tsunami_events.op.name);assert output['reused'] and output['version']==version
assert before=={str(f.relative_to(root)):s.sha256_file(f) for f in root.rglob('*') if f.is_file()} and prior=={str(f.relative_to(runtime/'published')):s.sha256_file(f) for f in (runtime/'published').rglob('*') if f.is_file()}
proof={'classification':'Explicit synthetic receipt-only replay, not actual new upstream observation','run_id':result.run_id,'success':result.success,'version':version,'original_manifest_receipts_candidate_and_prior_fixture_byte_identical':True,'fresh_synthetic_receipts_separately_preserved':True};p.joinpath('receipt-refresh.json').write_text(json.dumps(proof,indent=2));report['synthetic_receipt_only_refresh']=proof;(runtime/'execution.json').write_text(json.dumps(report,indent=2));print(proof)
