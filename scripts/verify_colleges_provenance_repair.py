"""Actual registered full-source provenance denials/recovery and docs-only reuse.

The baseline must come from verify_colleges_draft.py on the same committed
processing files. The current HEAD may advance only documentation afterward.
No source authority, contract, baseline, execution time or release is accepted.
"""
import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline-runtime',type=Path,required=True)
parser.add_argument('--runtime-dir',type=Path,required=True)
parser.add_argument('--capture-dir',type=Path,required=True)
args=parser.parse_args()
baseline=args.baseline_runtime.resolve();runtime=args.runtime_dir.resolve();captures=args.capture_dir.resolve()
if runtime.exists() and any(runtime.iterdir()):raise ValueError('Use an empty isolated repair runtime directory')
runtime.mkdir(parents=True,exist_ok=True)
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(key,None)
os.environ.update(HIFLD_COLLEGES_CAPTURE_DIR=str(captures),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance,Definitions
from dagster_hifld.definitions import _iter_staged_version_paths
from dagster_hifld.assets.colleges_universities import nces_colleges_universities,colleges_source_integrity_job
from dagster_hifld.sources import colleges_universities as c

report={'approved':False,'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[1],text=True).strip(),'source_rows':6438,'cases':[]}

def hashes(directory):
    return {str(p.relative_to(directory)):c.sha256_file(p)for p in directory.rglob('*')if p.is_file()}

def save():
    (runtime/'provenance-repair.json').write_text(json.dumps(report,indent=2)+'\n')

for case in ['genuine_later_docs_only_HEAD','missing_external_anchor','malformed_external_anchor','zero_commit_coforgery','existing_wrong_code_coforgery','candidate_only_forgery']:
    if shutil.disk_usage(runtime).free<500_000_000:raise RuntimeError('Bounded local free-space guard')
    work=runtime/case
    shutil.copytree(baseline/'staging',work/'staging',ignore=shutil.ignore_patterns('_quarantine','_building'))
    (work/'dagster').mkdir();(work/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
    os.environ['DAGSTER_HOME']=str(work/'dagster')
    stage=c.StagingStorageResource(use_local=True,local_dir=str(work/'staging'))
    published=c.PublishedStorageResource(use_local=True,local_dir=str(work/'published'))
    published.write_key('prior-synthetic-accepted.bin',b'Preserve prior synthetic accepted data; no real acceptance.')
    prior=hashes(work/'published');raw=hashes(work/'staging/_raw-captures')
    root=next((work/'staging'/c.SLUG/c.SLUG).glob('v*'));prefix=str(root.relative_to(work/'staging'))
    manifest=root/'metadata/source_manifest.json';ready=root/c.READY
    bundle=next((work/'staging/_source-provenance').rglob('bundle.json'))
    original_code=json.loads(manifest.read_text())['code_commit'];before_all=hashes(work/'staging')
    if case=='missing_external_anchor':bundle.unlink()
    elif case=='malformed_external_anchor':bundle.write_bytes(b'{"manifest":{}}\n')
    elif case!='genuine_later_docs_only_HEAD':
        metadata=json.loads(manifest.read_text())
        metadata['code_commit']='4800e4f5df550bfadd3631585268ce68a0fa25d9' if case=='existing_wrong_code_coforgery' else '0'*40
        manifest.write_bytes(c.encode(metadata));pins=json.loads(ready.read_text());pins['objects']['metadata/source_manifest.json']=c.sha256_file(manifest);ready.write_bytes(c.encode(pins))
        if case!='candidate_only_forgery':
            anchor=json.loads(bundle.read_text());anchor['manifest']=metadata;bundle.write_bytes(c.encode(anchor))
    bad_version=hashes(root);bad_anchor=bundle.read_bytes()if bundle.exists()else None
    defs=Definitions(assets=[nces_colleges_universities],jobs=[colleges_source_integrity_job],resources={'staging_storage':stage,'published_storage':published})
    item={'case':case,'original_construction_commit':original_code,'current_head':report['head'],'runs':[]}
    report['cases'].append(item)
    def record(label,result):
        entry={'label':label,'run_id':result.run_id,'success':result.success,'errors':[e.event_specific_data.error.to_string()for e in result.get_step_failure_events()]}
        item['runs'].append(entry);save();return entry
    with DagsterInstance.get()as instance:
        result=defs.resolve_job_def('colleges_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
        entry=record(case,result)
        if case=='genuine_later_docs_only_HEAD':
            assert original_code!=report['head'],'Advance a genuine docs-only commit before this proof'
            assert result.success and result.output_for_node(nces_colleges_universities.op.name)['reused'],entry
            assert before_all==hashes(work/'staging')
            assert json.loads(manifest.read_text())['code_commit']==original_code
            item.update(all_staging_objects_unchanged=True,original_construction_commit_retained=True)
        else:
            assert not result.success and any('invalid_cache_moved_intact_out_of_discovery' in error for error in entry['errors']),entry
            if case=='zero_commit_coforgery':assert any('construction_invalid_commit'in error for error in entry['errors'])
            if case=='existing_wrong_code_coforgery':assert any('construction_processing_blob_missing'in error or 'construction_processing_blob_hash_mismatch'in error for error in entry['errors'])
            assert not root.exists() and (c.SLUG,c.SLUG,prefix.rsplit('/',1)[1])not in set(_iter_staged_version_paths(stage))
            rejected=next((work/'staging/_quarantine'/c.SLUG).glob('invalid-candidate-*'))
            assert bad_version==hashes(rejected)
            if case!='candidate_only_forgery':
                assert not bundle.exists()
                if bad_anchor is not None:
                    preserved=next((work/'staging/_quarantine'/c.SLUG).glob('invalid-provenance-*/bundle.json'))
                    assert preserved.read_bytes()==bad_anchor
            else:assert bundle.exists() and bundle.read_bytes()==bad_anchor
            item.update(canonical_undiscoverable_after_rejection=True,whole_invalid_version_preserved=True,invalid_anchor_preserved=bad_anchor is not None)
            for label in ['verified_original_reconstruction','reconstructed_cache_reuse']:
                result=defs.resolve_job_def('colleges_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
                entry=record(label,result);assert result.success,entry
                output=result.output_for_node(nces_colleges_universities.op.name)
                assert output['feature_count']==6438 and output['reused']==(label=='reconstructed_cache_reuse')
            item['valid_anchor_established_after_verified_reconstruction']=bundle.exists()
        assert raw==hashes(work/'staging/_raw-captures') and prior==hashes(work/'published')
        actual={name:((captures/(name+'.body')).read_bytes(),c.strict_json((captures/(name+'.receipt.json')).read_bytes()))for name in c.URLS}
        table,audit=c.inspect_archives({},actual);c.reconcile_outputs(root,table)
        item.update(original_raw_unchanged=True,prior_accepted_fixture_unchanged=True,all_6438_native_rows_IDs_types_WKB_unchanged=True)
        save()
report['status']='PASS';save();print(json.dumps({'status':'PASS','cases':len(report['cases']),'runs':sum(len(x['runs'])for x in report['cases'])}))
