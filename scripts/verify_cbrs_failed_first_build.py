import argparse,os,json,pathlib,subprocess
from unittest.mock import patch
parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=pathlib.Path,required=True);args=parser.parse_args()
repo=pathlib.Path(__file__).resolve().parents[1];r=args.runtime.resolve();assert not r.exists()
for n in ['dagster','staging','published']:(r/n).mkdir(parents=True)
(r/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for k in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(k,None)
os.environ.update(DAGSTER_HOME=str(r/'dagster'),HIFLD_STAGING_DIR=str(r/'staging'),HIFLD_DATASETS_DIR=str(r/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs,_iter_staged_version_paths
from dagster_hifld.sources import cbrs_buffer as s
from dagster_hifld.assets.fws import fws_cbrs_buffer_zone as asset
from dagster_hifld.resources import StagingStorageResource
assert pathlib.Path(s.__file__).resolve().is_relative_to(repo/'src')
store=StagingStorageResource(local_dir=str(r/'staging'));(r/'published/prior-local-fixture').write_bytes(b'local-only failure preservation fixture, not a real accepted source');before=(r/'published/prior-local-fixture').read_bytes()
with DagsterInstance.get() as instance,patch.object(s,'write_native_geopackage',side_effect=OSError('first native writer interruption regression')) as writer:
 result=defs.resolve_job_def('fws_cbrs_buffer_draft_refresh').execute_in_process(instance=instance,run_config={'ops':{asset.op.name:{'config':{'capture_dir':str(repo/'docs/dataset-sources/cbrs-buffer-zone-1/evidence/captures')}}}},raise_on_error=False)
 assert not result.success and writer.call_count==1 and len(writer.call_args.args[0])==1145
 bundles=list((r/'staging/_source-provenance').rglob('bundle.json'));markers=list((r/'staging'/s.SLUG).rglob('cbrs_buffer_candidate_ready.json'));discovered=list(_iter_staged_version_paths(store))
 report={'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'scope':'Authentic complete1145 input, actual registered source, injected first native writer failure; no Git/row/fingerprint/receipt mock','run_id':result.run_id,'expected_failure':not result.success,'rows_validated_before_injected_failure':len(writer.call_args.args[0]),'first_binding_files_after_failed_private_construction':[str(p) for p in bundles],'ready_receipts_after_failure':[str(p) for p in markers],'discovered_versions_after_failure':discovered,'raw_original_ZIP_preserved':any(s.sha256_file(p)==s.PIN for p in (r/'staging/_raw-captures').rglob('official_zip.body')),'prior_local_fixture_preserved':(r/'published/prior-local-fixture').read_bytes()==before,'errors':[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()],'approval_invented':False}
 assert not bundles and not markers and not discovered and report['raw_original_ZIP_preserved'] and report['prior_local_fixture_preserved']
(r/'result.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k!='errors'}))
