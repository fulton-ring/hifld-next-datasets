"""Actual complete-source state refusal/recovery; fixture faults grant no acceptance.

Run normal11 lifecycle first at a public processing commit. This harness uses
independent copies and the actual Git/shared-state/native/source code. --docs-only
requires a genuine later docs-only HEAD and preserves original first contexts.
"""
import argparse,json,os,shutil,subprocess
from pathlib import Path
from unittest.mock import patch
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--baseline-runtime',type=Path,required=True);p.add_argument('--runtime-dir',type=Path,required=True);p.add_argument('--capture-dir',type=Path,required=True);p.add_argument('--docs-only',action='store_true');a=p.parse_args()
baseline=a.baseline_runtime.absolute();runtime=a.runtime_dir.absolute();captures=a.capture_dir.absolute();assert not runtime.exists();runtime.mkdir(parents=True)
for key in('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST'):os.environ.pop(key,None)
os.environ.update(HIFLD_COLLEGES_CAPTURE_DIR=str(captures),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import Definitions,DagsterInstance
from dagster_hifld.assets.colleges_universities import nces_colleges_universities,colleges_source_integrity_job
from dagster_hifld.sources import colleges_universities as c
report={'approved':False,'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[1],text=True).strip(),'source_rows':6438,'cases':[]}
def hashes(root):return {str(p.relative_to(root)):c.sha256_file(p)for p in root.rglob('*')if p.is_file()}
def save():(runtime/'provenance-repair.json').write_bytes(c.encode(report))
for case in(['genuine_docs_only']if a.docs_only else['primary_receipt_manifest_coforge','all_original_witnesses_missing','failed_and_partial_negative_journal']):
    if shutil.disk_usage(runtime).free<400_000_000:raise RuntimeError('global disk floor')
    work=runtime/case;shutil.copytree(baseline/'staging',work/'staging',ignore=shutil.ignore_patterns('_quarantine','_building'))
    (work/'dagster').mkdir();(work/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n');os.environ['DAGSTER_HOME']=str(work/'dagster')
    stage=c.StagingStorageResource(local_dir=str(work/'staging'));published=c.PublishedStorageResource(local_dir=str(work/'published'));published.write_key('prior-synthetic-accepted.bin',b'Prior fixture only; no real release acceptance.');prior=hashes(work/'published');raw=hashes(work/'staging/_raw-captures');root=next((work/'staging'/c.SLUG/c.SLUG).glob('v*'));version=root.name;alias=work/'staging'/c.construction_record_key(version)
    state={p:p.read_bytes()for p in(work/'staging/_construction-state').rglob('*')if p.is_file()};first=alias.read_bytes();original_candidate=hashes(root);original_manifest=(root/'metadata/source_manifest.json').read_bytes();original_code=json.loads(original_manifest)['code_commit'];before=hashes(work/'staging')
    item={'case':case,'original_commit':original_code,'current_head':report['head'],'runs':[]};report['cases'].append(item)
    definitions=Definitions(assets=[nces_colleges_universities],jobs=[colleges_source_integrity_job],resources={'staging_storage':stage,'published_storage':published})
    def refresh(label,expected=None):
        result=definitions.resolve_job_def('colleges_source_integrity').execute_in_process(instance=instance,raise_on_error=False);errors=[e.event_specific_data.error.to_string()for e in result.get_step_failure_events()];item['runs'].append({'label':label,'run_id':result.run_id,'success':result.success,'errors':errors});save();assert hashes(work/'published')==prior
        if expected is None:assert result.success,errors
        else:assert not result.success and any(expected in error for error in errors),errors
        return result
    def restore_fixture(label):
        for p,body in state.items():p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
        incident=work/'staging'/c.incident_directory(version)
        if incident.exists():incident.rename(work/('operator-retained-fixture-incident-'+label))
    with DagsterInstance.get()as instance:
        if case=='genuine_docs_only':
            assert original_code!=report['head'];changed=subprocess.check_output(['git','diff','--name-only',original_code,report['head']],cwd=Path(__file__).parents[1],text=True).splitlines();assert changed and all(not x.startswith('src/')and x not in('pyproject.toml','uv.lock')for x in changed)
            result=refresh('genuine_docs_only_preserves_first_context');assert result.output_for_node(nces_colleges_universities.op.name)['reused']is True;assert before.items()<=hashes(work/'staging').items();assert all(k.startswith('_acquisition-attempts/')for k in hashes(work/'staging').keys()-before.keys());item['actual_git_changed_paths']=changed
        elif case=='primary_receipt_manifest_coforge':
            value=json.loads(first);receipt=value['payload']['captures']['nces_geocode']['receipt'];receipt['observed_at']='2026-10-19T23:20:00+00:00';next(x for x in value['payload']['manifest']['inputs']if x['input_id'].startswith('nces_geocode:'))['retrieved_at']=receipt['observed_at'];alias.write_bytes(c.canonical_construction_bundle(value['version'],value['processing_fingerprint'],value['payload']));bad=alias.read_bytes();refresh('coforged_first_context_rejected','construction_marker_mismatch');assert not root.exists()and alias.read_bytes()==bad
            for count in(1,2):refresh('repeated_cold_refusal_'+str(count),'permanent_observed_original_trust_loss')
            restore_fixture('coforge');refresh('explicit_exact_original_restore');refresh('reused_after_trusted_restore')
        elif case=='all_original_witnesses_missing':
            for p in state:p.unlink()
            refresh('observed_whole_original_state_loss','observed_original_trust_loss');assert not root.exists()
            for count in(1,2):refresh('repeated_cold_refusal_'+str(count),'permanent_observed_original_trust_loss')
            for p,body in state.items():p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
            refresh('trusted_state_restore_alone_cannot_clear_negative_incident','permanent_observed_original_trust_loss');restore_fixture('all-witnesses');refresh('explicit_reviewed_original_restore');refresh('reuse_after_original_restore')
        else:
            alias.unlink()
            with patch.object(c,'record_observed_loss',side_effect=OSError('synthetic negative journal IO failure')):refresh('journal_IO_failure_preserves_candidate','incident_write_failed_candidate_preserved')
            assert hashes(root)==original_candidate;alias.write_bytes(first);incident=work/'staging'/c.incident_directory(version);incident.mkdir()
            for count in(1,2):refresh('empty_partial_journal_refusal_'+str(count),'permanent_observed_original_trust_loss');assert hashes(root)==original_candidate
            restore_fixture('partial-journal');refresh('explicit_operator_retains_partial_journal_then_reuses')
        assert hashes(root)==original_candidate and(root/'metadata/source_manifest.json').read_bytes()==original_manifest;assert hashes(work/'staging/_raw-captures')==raw and hashes(work/'published')==prior
        assert {p: p.read_bytes()for p in state}==state
        table,audit=c.inspect_archives({}, {n:((captures/(n+'.body')).read_bytes(),json.loads((captures/(n+'.receipt.json')).read_bytes()))for n in c.URLS});c.reconcile_outputs(root,table)
        item.update(all6438_native_rows_exact=True,original_context_and_raw_and_prior_preserved=True);save()
report.update(status='PASS',actual_run_count=sum(len(x['runs'])for x in report['cases']));save();print(json.dumps({'status':'PASS','cases':len(report['cases']),'actual_runs':report['actual_run_count']}))
