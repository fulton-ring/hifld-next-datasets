"""Narrow actual normal-Dagster replay for provenance/error/identity review repairs.

Uses complete real captures. Prior full terminal proof is reusable only when the
canonical data bytes and named transformation ASTs remain identical. No acceptance.
"""
import argparse
import ast
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def files(root):
    return {str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--capture',type=Path,required=True)
    parser.add_argument('--prior-full-run',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    root=args.output.resolve();root.mkdir(parents=True,exist_ok=True)
    if args.capture.resolve().is_relative_to(root):raise ValueError('Capture must remain outside run output')
    for key in ('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS'):os.environ.pop(key,None)
    os.environ.update(DAGSTER_HOME=str(root/'dagster'),HIFLD_STAGING_DIR=str(root/'staging'),
        HIFLD_DATASETS_DIR=str(root/'published'),HIFLD_STAGING_PREFIX='',HIFLD_DATASETS_PREFIX='')
    (root/'dagster').mkdir(exist_ok=True);(root/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
    from dagster import AssetKey,DagsterInstance
    from dagster_hifld.definitions import defs
    import dagster_hifld.amtrak_extract as adapter
    from dagster_hifld.partitions import PUBLISH_PARTITIONS
    from dagster_hifld.release_approval import validate_release_provenance,verifier_implementation_sha256
    prior=json.loads((args.prior_full_run/'execution.json').read_bytes())
    implementation=verifier_implementation_sha256()
    version='v0.0.0-reviewfix-'+implementation[:12];prefix=f'{adapter.SLUG}/{adapter.SLUG}/{version}'
    staged=root/'staging'/prefix;published=root/'published';published.mkdir(exist_ok=True)
    fixture=published/'accepted-fixture.bin'
    if not fixture.exists():fixture.write_bytes(b'Synthetic local acceptance sentinel; no real acceptance')
    accepted_before=files(published)
    repo=defs.get_repository_def();source_key=AssetKey(['ingest',adapter.SLUG,adapter.SLUG])
    source_job=repo.get_implicit_job_def_for_assets([source_key])
    source_config=lambda capture,version:{'ops':{adapter.bts_amtrak_stations.op.name:{'config':{'capture_dir':str(capture.resolve()),'version':version}}}}
    record={'classification':'Unapproved real-source draft; narrow independent-review repair validation',
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'implementation_sha256':implementation,'module':adapter.__file__,'version':version,
        'published_before':accepted_before,'no_dataset_acceptance':True}
    def save():(root/'execution.json').write_text(json.dumps(record,indent=2)+'\n')
    with DagsterInstance.get() as instance:
        result=source_job.execute_in_process(instance=instance,asset_selection=[source_key],run_config=source_config(args.capture,version),raise_on_error=False)
        record['ingest']={'run_id':result.run_id,'success':result.success};save();assert result.success
        manifest=json.loads((staged/'metadata/source_manifest.json').read_bytes())
        validate_release_provenance(manifest)
        assert manifest['schema_version']==1 and manifest['processor']=='HIFLD Next' and manifest['complete_rows']==1036
        bodies={str(p.relative_to(staged)):p for p in (staged/'raw_arcgis').rglob('*.body')}
        assert set(bodies)=={i['input_id'] for i in manifest['inputs']} and len(bodies)==18
        for item in manifest['inputs']:
            body=bodies[item['input_id']];receipt=json.loads((staged/item['receipt_path']).read_bytes())
            assert item['sha256']==sha(body) and item['bytes']==body.stat().st_size
            assert item['canonical_url']==receipt['url'] and item['retrieval_url']==receipt['final_url']
            assert item['retrieved_at']==receipt['observed_at'] and item['role']=='official_distributor'
        before=files(staged)
        result=source_job.execute_in_process(instance=instance,asset_selection=[source_key],run_config=source_config(args.capture,version),raise_on_error=False)
        record['refresh']={'run_id':result.run_id,'success':result.success,'all_version_bytes_identical':before==files(staged)}
        assert result.success and before==files(staged)
        instance.add_dynamic_partitions(PUBLISH_PARTITIONS.name,[prefix])
        catalog_key=AssetKey(['publish','catalog']);catalog_job=repo.get_implicit_job_def_for_assets([catalog_key])
        result=catalog_job.execute_in_process(instance=instance,asset_selection=[catalog_key],partition_key=prefix,raise_on_error=False)
        record['catalog']={'run_id':result.run_id,'success':result.success};save();assert result.success
        dictionary=json.loads((staged/'metadata/data_dictionary.json').read_bytes())
        validate_release_provenance(dictionary)
        assert dictionary['inputs']==manifest['inputs']
        record['provenance']={'schema_version':1,'processor':'HIFLD Next','observed_body_inputs':len(manifest['inputs']),
            'every_retained_observed_body_hash_length_url_time_verified':True,'shared_validator_manifest_and_dictionary_pass':True,
            'catalog_inputs_equal':True,'promotion_allowed':manifest['promotion_allowed']}
        prior_stage=args.prior_full_run/'staging'/adapter.SLUG/adapter.SLUG/prior['version']
        unchanged={}
        for relative in ('draft_candidate/candidate.parquet',f'geopackage/{adapter.SLUG}.gpkg',f'geojson/{adapter.SLUG}.geojson'):
            unchanged[relative]={'prior_sha256':sha(prior_stage/relative),'current_sha256':sha(staged/relative)}
            assert unchanged[relative]['prior_sha256']==unchanged[relative]['current_sha256']
        repo_root=Path(adapter.__file__).resolve().parents[2]
        old_source=subprocess.check_output(['git','show','bcf6cc143a8b95185f53289f3a30d378b577b443:src/dagster_hifld/amtrak_extract.py'],cwd=repo_root,text=True)
        def functions(source):return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(source).body if isinstance(n,ast.FunctionDef)}
        old,new=functions(old_source),functions(Path(adapter.__file__).read_text())
        names=('write_typed_geopackage','verify_typed_geopackage')
        assert all(old[n]==new[n] for n in names)
        def candidate_writer(source):
            function=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='build_candidate')
            node=next(n for n in ast.walk(function) if isinstance(n,ast.With) and any(isinstance(i.context_expr,ast.Call) and isinstance(i.context_expr.func,ast.Attribute) and i.context_expr.func.attr=='ParquetWriter' for i in n.items))
            return ast.dump(node,include_attributes=False)
        assert candidate_writer(old_source)==candidate_writer(Path(adapter.__file__).read_text())
        record['prior_full_terminal_proof']={'prior_source_checkpoint':'be49bcb93f1901b408e709b6df33bb58dcc98681',
            'prior_full_graph_run_id':prior['normal_graph']['run_id'],'prior_fidelity':prior['terminal_parquet_audit'],
            'canonical_bytes_unchanged':unchanged,'named_transformation_ASTs_unchanged':['build_candidate::ParquetWriter',*names],
            'meaning':'Prior full terminal proof retained; this repair changes error/provenance/identity handling, not source data transformations. No full conversion repeat claimed.'}
        gate_keys=[AssetKey(['publish','verify']),AssetKey(['publish','promote'])]
        gate_job=repo.get_implicit_job_def_for_assets(gate_keys)
        result=gate_job.execute_in_process(instance=instance,asset_selection=gate_keys,partition_key=prefix,raise_on_error=False)
        failures={e.step_key:e.event_specific_data.error.to_string() for e in result.get_step_failure_events()}
        assert set(failures)=={'publish__verify'} and 'missing_independent_release_approval_registry' in failures['publish__verify']
        record['narrow_unapproved_gate']={'run_id':result.run_id,'success':result.success,'failed_steps':failures,
            'promote_never_ran':not any(e.step_key=='publish__promote' and e.event_type_value=='STEP_START' for e in result.all_events),
            'selection':'verify/promote only against canonical staged outputs, no full conversion repeat'}
        record['synthetic_replay_failures']=[]
        for name,kind in [('bts_count','error'),('bts_ids_after','array')]:
            bad=root/f'corrupt-copy-{kind}';shutil.copytree(args.capture,bad)
            path=bad/(name+'.body');value=json.loads(path.read_bytes())
            if kind=='error':value['error']={'code':500,'message':'Synthetic review regression; authentic fields retained'}
            else:value=[value]
            path.write_text(json.dumps(value));receipt_path=bad/(name+'.receipt.json');receipt=json.loads(receipt_path.read_bytes())
            receipt.update(bytes=path.stat().st_size,sha256=sha(path));receipt_path.write_text(json.dumps(receipt))
            bad_version=version+'-'+kind
            result=source_job.execute_in_process(instance=instance,asset_selection=[source_key],run_config=source_config(bad,bad_version),raise_on_error=False)
            failures={e.step_key:e.event_specific_data.error.to_string() for e in result.get_step_failure_events()}
            assert not result.success and any('ArcGIS application error or nonobject' in reason for reason in failures.values())
            quarantined=list((root/'staging/quarantine'/adapter.SLUG/bad_version).rglob(name+'.body'))
            assert len(quarantined)==1 and quarantined[0].read_bytes()==path.read_bytes()
            assert not (root/'staging'/adapter.SLUG/adapter.SLUG/bad_version).exists()
            record['synthetic_replay_failures'].append({'kind':kind,'body':name,'run_id':result.run_id,
                'success':result.success,'exact_raw_quarantined':True,'no_canonical_candidate':True,
                'meaning':'Deliberately corrupted local copies, not observed producer errors'})
        record['synthetic_attempt_provenance_failures']=[]
        for kind,timestamp,relative in [('invalid','not-a-date','attempts/page-000/000.receipt.json'),('naive','2026-10-19T00:00:00','attempts/page-000/000.receipt.json'),('invalid-offset-attempt','2026-10-19T00:00:00+00:99','attempts/page-000/000.receipt.json'),('invalid-offset-terminal','2026-10-19T00:00:00+00:99','page-000.receipt.json')]:
            bad=root/f'corrupt-attempt-{kind}';shutil.copytree(args.capture,bad)
            receipt_path=bad/relative
            receipt=json.loads(receipt_path.read_bytes());receipt['observed_at']=timestamp
            receipt_path.write_text(json.dumps(receipt))
            bad_version=version+'-attempt-'+kind
            result=source_job.execute_in_process(instance=instance,asset_selection=[source_key],run_config=source_config(bad,bad_version),raise_on_error=False)
            failures={e.step_key:e.event_specific_data.error.to_string() for e in result.get_step_failure_events()}
            assert not result.success and any('isoformat' in reason or 'timezone' in reason for reason in failures.values())
            canonical=root/'staging'/adapter.SLUG/adapter.SLUG/bad_version
            assert not canonical.exists()
            statuses=list((root/'staging/quarantine'/adapter.SLUG/bad_version).rglob('status.json'))
            assert len(statuses)==1
            status=json.loads(statuses[0].read_bytes());assert status['state']=='blocked' and status['raw_preserved']
            retained=statuses[0].parent/'raw'
            assert all((retained/p.relative_to(bad)).read_bytes()==p.read_bytes() for p in bad.rglob('*') if p.is_file())
            record['synthetic_attempt_provenance_failures'].append({'kind':kind,'run_id':result.run_id,'success':result.success,
                'exact_raw_and_receipts_quarantined':True,'no_canonical_version':True,
                'receipt':relative,'meaning':'Deliberately corrupted local receipt timestamp; not an observed source defect'})
        extra=root/'extra-diagnostic-copy';shutil.copytree(args.capture,extra)
        diagnostic=extra/'page-retained-diagnostic.receipt.json'
        diagnostic.write_text(json.dumps({'url':'https://untrusted.invalid/forged','final_url':'https://untrusted.invalid/forged','observed_at':'not-a-date','headers':None}))
        extra_version=version+'-untrusted-diagnostic'
        result=source_job.execute_in_process(instance=instance,asset_selection=[source_key],run_config=source_config(extra,extra_version),raise_on_error=False)
        assert result.success
        extra_staged=root/'staging'/adapter.SLUG/adapter.SLUG/extra_version
        extra_manifest=json.loads((extra_staged/'metadata/source_manifest.json').read_bytes());validate_release_provenance(extra_manifest)
        assert len(extra_manifest['inputs'])==18 and len(extra_manifest['original_retrieval_receipts'])==9
        assert diagnostic.name not in {item['name'] for item in extra_manifest['original_retrieval_receipts']}
        untrusted=extra_manifest['lineage']['untrusted_diagnostics'];assert len(untrusted)==1
        assert untrusted[0]['sha256']==sha(diagnostic) and untrusted[0]['used_for_source_or_retrieval_claims'] is False
        assert (extra_staged/'raw_arcgis'/diagnostic.name).read_bytes()==diagnostic.read_bytes()
        record['untrusted_diagnostic_receipt']={'run_id':result.run_id,'success':True,'byte_hash_pinned':True,
            'preserved':True,'retrieval_claims_exclude_diagnostic':True,'source_inputs_unchanged':18}
    record['published_after']=files(published);assert record['published_after']==accepted_before
    assert all((staged/'raw_arcgis'/r['name']).read_bytes()==(args.capture/r['name']).read_bytes() for r in adapter.inspect_capture(args.capture)['raw_files'])
    record['accepted_fixture_and_real_raw_preserved']=True;save()
    print(json.dumps({k:record[k] for k in ('code_commit','ingest','refresh','catalog','provenance','synthetic_replay_failures','synthetic_attempt_provenance_failures','accepted_fixture_and_real_raw_preserved')}))


if __name__=='__main__':main()
