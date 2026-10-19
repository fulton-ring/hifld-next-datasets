"""Synthetic receipt context + real docs-only Git history on preserved local draft."""
import copy,json,os,subprocess
from pathlib import Path
from unittest.mock import patch
base=Path('/workspace/work/hifld-coordinator/113');runtime=base/'construction-state-runtime'
execution=json.loads((runtime/'execution.json').read_text());version=execution['output']['version']
for key in ('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS'):os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_TSUNAMI_EVENTS_CAPTURE_DIR=str(base/'captures'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import historical_tsunami_events as source
storage=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True)
root=runtime/'staging'/source.SLUG/source.SLUG/version
state=runtime/'staging/_construction-state'/source.SLUG

def snap(path):return {str(p.relative_to(path)):source.sha256_file(p) for p in path.rglob('*') if p.is_file()}
before=snap(root);state_before=snap(state);raw_before=snap(runtime/'staging/_raw-captures');prior=snap(runtime/'published')
original=json.loads((root/'metadata/source_manifest.json').read_bytes())
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(source.__file__).parents[3],text=True).strip();assert head!=original['code_commit']
report={'classification':'actual registered refresh after docs-only history + synthetic receipt-only context, no new HTTP observation/acceptance','head':head,'source_code_commit':original['code_commit'],'version':version,'runs':[]}
with DagsterInstance.get() as instance:
 job=defs.resolve_job_def('tsunami_events_draft_refresh')
 first=job.execute_in_process(instance=instance,raise_on_error=False)
 assert first.success and first.output_for_node('ingest__historical_tsunami_event_locations___historical_tsunami_event_locations_')['reused']
 report['runs'].append({'kind':'docs_only_HEAD_original_code_and_receipts_reused','run_id':first.run_id,'success':first.success})
 captured=source.acquire(storage,base/'captures');synthetic={name:(body,copy.deepcopy(receipt)|{'observed_at':'2026-10-19T23:59:00+00:00','synthetic_receipt_context':True}) for name,(body,receipt) in captured.items()}
 with patch.object(source,'acquire',return_value=synthetic):second=job.execute_in_process(instance=instance,raise_on_error=False)
 assert second.success and second.output_for_node('ingest__historical_tsunami_event_locations___historical_tsunami_event_locations_')['reused']
 report['runs'].append({'kind':'synthetic_new_receipts_do_not_rebind_original','run_id':second.run_id,'success':second.success})
assert before==snap(root) and state_before==snap(state) and prior==snap(runtime/'published')
raw_after=snap(runtime/'staging/_raw-captures');assert all(raw_after[k]==value for k,value in raw_before.items())
report.update(candidate_bytes_unchanged=True,original_state_bytes_unchanged=True,original_raw_depot_objects_preserved=len(raw_before),new_synthetic_receipt_evidence_retained=len(raw_after)-len(raw_before),prior_fixture_preserved=True)
(runtime/'receipt-docs-history.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
