"""Actual registered complete preservation reconstruction and fail-closed gates.

The prior release marker and injected quality/storage failures are local fixtures,
not approval of this historical input, a baseline, contract or a real release.
"""
from __future__ import annotations
import argparse
import json
import os
import shutil
import struct
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);parser.add_argument('--capture-dir',type=Path,required=True);args=parser.parse_args()
runtime=args.runtime.absolute();captures=args.capture_dir.absolute();assert not runtime.exists() and captures.is_dir()
for name in ('dagster','staging','published'):(runtime/name).mkdir(parents=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS'):os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_CRUSHED_STONE_CAPTURE_DIR=str(captures),HIFLD_CRUSHED_STONE_PACKAGE_ID='stanford-cs268nn2622-preservation-as-provided-v1',HIFLD_SHADOW_MANUAL_ONLY='1')
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld import construction_state
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.crushed_stone import usgs_crushed_stone_preservation as asset
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import crushed_stone as source
from dagster_hifld.release_approval import validate_release_provenance

staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
for index in range(14):
    published.write_key(f'synthetic-prior-release-fixture/{index:02d}.bin',f'isolated synthetic fixture object {index:02d}; not accepted production data'.encode())
def snap(storage):return {k:storage.sha256_key(k) for k in storage.list_prefix()}
def files(root):return {str(p.relative_to(root)):source.sha256_file(p) for p in root.rglob('*') if p.is_file()}
prior=snap(published);keys=[k for k in normal.resolve_asset_graph().get_all_asset_keys() if k.path[0]=='publish']
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('crushed_stone_normal_graph',selection=AssetSelection.assets(*keys))])
report={'execution_code_identity':source.processing_identity(),'module':source.__file__,'runtime':str(runtime),'package_id':source.PACKAGE,'source_accepted':False,'release_accepted':False,'prior_release':'14-object synthetic local fixture only, not a real accepted release','runs':[]}
def record(kind,result):
    errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
    report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,'materializations':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()],'checks':[{'name':e.check_name,'passed':e.passed} for e in result.get_asset_check_evaluations()]})
    (runtime/'execution-progress.json').write_text(json.dumps(report,indent=2)+'\n');return errors
def move_incident(version,label):
    # Explicit test operator recovery: retain the incident outside discovery;
    # restoring trusted bytes NEVER automatically clears the negative journal.
    path=runtime/'staging'/source.incident_directory(version)
    if path.exists():path.rename(runtime/('operator-fixture-retained-incident-'+label))

with DagsterInstance.get() as instance:
    job=normal.resolve_job_def('crushed_stone_preservation_reconstruct');graph=defs.resolve_job_def('crushed_stone_normal_graph')
    def run(label,expected=None):
        result=job.execute_in_process(instance=instance,raise_on_error=False);errors=record(label,result)
        assert result.success if expected is None else (not result.success and any(expected in error for error in errors)),errors
        assert snap(published)==prior
        return result
    invalid=runtime/'synthetic-metadata-input';shutil.copytree(captures,invalid);name='legacy_metadata';receipt=invalid/(name+'.receipt.json');original_receipt=receipt.read_bytes();bad_receipt=b'{ malformed first receipt'
    receipt.write_bytes(bad_receipt);os.environ['HIFLD_CRUSHED_STONE_CAPTURE_DIR']=str(invalid)
    try:run('synthetic_malformed_first_receipt_full_body_and_raw_metadata_retained','Expecting property name')
    finally:receipt.write_bytes(original_receipt);os.environ['HIFLD_CRUSHED_STONE_CAPTURE_DIR']=str(captures)
    body=(captures/(name+'.body')).read_bytes();bodykey=f'_raw-captures/{source.SLUG}/{source.digest(body)}/{name}.body';receiptkey=f'_raw-captures/{source.SLUG}/{source.digest(body)}/{name}.raw-receipt-{source.digest(bad_receipt)}.body'
    assert staging.read_key(bodykey)==body and staging.read_key(receiptkey)==bad_receipt and not (runtime/'staging/_construction-state').exists()
    receipt.chmod(0);os.environ['HIFLD_CRUSHED_STONE_CAPTURE_DIR']=str(invalid)
    try:run('genuine_chmod000_first_receipt_body_only_retained','Permission denied')
    finally:receipt.chmod(0o600);os.environ['HIFLD_CRUSHED_STONE_CAPTURE_DIR']=str(captures)
    attempts=[json.loads(p.read_bytes()) for p in (runtime/'staging'/'_quarantine').rglob('attempted-inputs.json')]
    assert any(a['objects'][name]['raw_receipt_read'] is False and 'raw_receipt_key' not in a['objects'][name] for a in attempts)
    assert not (runtime/'staging/_construction-state').exists()
    install=construction_state._install_once
    def fail_marker(base,key,body):
        if key.endswith('.first-construction.json'):raise OSError('synthetic marker-write fault after CAS')
        install(base,key,body)
    with patch.object(construction_state,'_install_once',fail_marker):run('CAS_only_first_marker_fault_after_complete_private_validation','construction_state_io_failed')
    evidences=list((runtime/'staging/_construction-state'/source.SLUG).glob('*.json.evidence/*.json'));assert len(evidences)==1
    evidence=evidences[0];original=evidence.read_bytes();envelope=json.loads(original);version=envelope['version'];alias=runtime/'staging'/source.construction_record_key(version)
    run('partial_original_cold_failure_records_permanent_incident','construction_missing_original_state');run('partial_original_repeated_cold_denied','permanent_observed_original_trust_loss');assert not alias.exists()
    marker={'schema_version':1,'version':version,'processing_fingerprint':envelope['processing_fingerprint'],'original_bundle_sha256':source.digest(original),'original_bundle_evidence_key':str(evidence.relative_to(runtime/'staging'))}
    Path(str(alias)+'.first-construction.json').write_bytes(json.dumps(marker,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode());alias.write_bytes(original);move_incident(version,'first-marker-fault')
    first=run('complete_real2324_after_explicit_exact_state_restoration');output=first.output_for_node(asset.op.name);assert output['feature_count']==2324 and output['accepted'] is False and output['version']==version
    prefix=build_publish_partition_key(source.SLUG,source.SLUG,version);root=runtime/'staging'/prefix;native=root/f'geoparquet/{source.SLUG}.parquet';gold=native.read_bytes();original_manifest=(root/'metadata/source_manifest.json').read_bytes();state_files={p:p.read_bytes() for p in alias.parent.rglob('*') if p.is_file()}
    assert (source.SLUG,source.SLUG,version) in list(_iter_staged_version_paths(staging));before=files(root);raw_before={key:sha for key,sha in snap(staging).items() if key.startswith('_raw-captures/')}
    again=run('complete_native_byte_idempotent_reuse');assert again.output_for_node(asset.op.name)['reused'] and files(root)==before
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
    for cycle in (1,2):
        result=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);errors=record('normal_catalog_'+str(cycle)+'_real_missing_independent_approval',result)
        assert not result.success and errors and all('missing_independent_release_approval_registry' in error for error in errors),errors
        assert snap(published)==prior;run('source_after_catalog_'+str(cycle));assert native.read_bytes()==gold
    full=source.acquire(staging,str(captures),source.PACKAGE);table,published_ids,audit=source.inspect_captures(full);source.verify_outputs(root,table,published_ids);assert pq.read_table(native).equals(source.expected_parquet(table),check_metadata=True)
    assert table.column_names==source.SOURCE_NAMES and table.num_rows==2324 and 'source_fid' not in table.column_names and 'id' not in table.column_names
    assert json.loads((root/'metadata/published-identifiers.json').read_bytes())==published_ids
    manifest=json.loads(original_manifest);validate_release_provenance(manifest);assert manifest['approved'] is False and len(manifest['inputs'])==15
    report['native_candidate']={'rows':2324,'attributes':11,'physical_schema':str(table.schema),'native_parquet_sha256':source.digest(gold),'native_parquet_bytes':len(gold),'all_source_attributes_IDs_decimal_types_FIPS_nativeWKB_equal':True,'source_record_lineage_separate':True,'native_CRS':'EPSG:4269','published_baseline_rows':766,'published_family':'MINOP construction-minerals, not accepted crushed-stone continuation','full_published_formats':list(published_ids['complete_formats'])};report['quality']=json.loads((root/'metadata/quality_manifest.json').read_bytes());report['source_audit']=audit
    # Labelled synthetic clone, not an accepted source: null one native WKB
    # and honestly re-pin its proposed capability; raw masters stay unchanged.
    badprefix=prefix+'-synthetic-null-geometry';badroot=runtime/'staging'/badprefix;shutil.copytree(root,badroot);badnative=badroot/f'geoparquet/{source.SLUG}.parquet';badtable=pq.read_table(badnative);values=badtable['geometry'].to_pylist();values[0]=None
    index=badtable.schema.get_field_index('geometry');badtable=badtable.set_column(index,pa.field('geometry',pa.binary(),nullable=True),pa.array(values,pa.binary()));badnative.write_bytes(source.parquet_bytes(badtable))
    badmanifest=json.loads((badroot/'metadata/source_manifest.json').read_bytes());pin=badmanifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0];pin.update(sha256=source.sha256_file(badnative),bytes=badnative.stat().st_size);(badroot/'metadata/source_manifest.json').write_bytes(source.encode(badmanifest));shutil.rmtree(badroot/'metadata/source');(badroot/'metadata/resolved_provenance.json').unlink()
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix]);result=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);record('synthetic_null_geometry_quality_gate_preserves_prior',result);assert not result.success and any(not check.passed for check in result.get_asset_check_evaluations()) and snap(published)==prior
    for label in ('unknown_metadata','self_repinned_native_parquet','coforged_original_receipt','missing_original_CAS','all_original_witnesses_missing'):
        ready=json.loads((root/source.READY).read_bytes());restore=None
        if label=='unknown_metadata':(root/'metadata/unexpected.json').write_bytes(b'{"approved":true}');expected='unknown_missing_or_linked_candidate_object'
        elif label=='self_repinned_native_parquet':
            changed=pq.read_table(native).replace_schema_metadata({b'geo':b'{}'});native.write_bytes(source.parquet_bytes(changed));ready['objects'][str(native.relative_to(root))]=source.sha256_file(native);(root/source.READY).write_bytes(source.encode(ready));expected='full_native_source_values'
        elif label=='coforged_original_receipt':
            value=json.loads(original);receipt_value=value['payload']['captures']['crstonx020.dbf']['receipt'];receipt_value['observed_at']='2026-10-19T23:04:58+00:00';next(item for item in value['payload']['manifest']['inputs'] if item['input_id'].startswith('crstonx020.dbf:'))['retrieved_at']=receipt_value['observed_at'];alias.write_bytes(source.canonical_construction_bundle(version,value['processing_fingerprint'],value['payload']));expected='construction_marker_mismatch'
        elif label=='missing_original_CAS':evidence.unlink();expected='construction_original_evidence_mismatch'
        else:
            for p in state_files:p.unlink()
            expected='construction_missing_original_state'
        altered=files(root);old={p for p in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-candidate-*') if p.is_dir()}
        run('reject_'+label,expected);assert not root.exists();new={p for p in (runtime/'staging/_quarantine'/source.SLUG).glob('invalid-candidate-*') if p.is_dir()}-old;assert len(new)==1 and files(next(iter(new)))==altered
        if label in ('coforged_original_receipt','missing_original_CAS','all_original_witnesses_missing'):
            for repeat in (1,2):run(label+'_repeated_cold_'+str(repeat),'permanent_observed_original_trust_loss')
            for p,body in state_files.items():p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
            move_incident(version,label)
        run('cold_rebuild_exact_original_after_'+label);source.verify_outputs(root,table,published_ids);assert native.read_bytes()==gold and (root/'metadata/source_manifest.json').read_bytes()==original_manifest and snap(published)==prior
    # A partial negative incident cannot be ignored even when originals restore.
    alias.unlink();before=files(root)
    with patch.object(source,'record_observed_loss',side_effect=OSError('synthetic journal persistence fault')):run('negative_incident_write_fault_preserves_candidate','incident_write_failed_candidate_preserved')
    assert files(root)==before;directory=runtime/'staging'/source.incident_directory(version);directory.mkdir();(directory/'.pending-fixture-interruption').write_bytes(b'partial');alias.write_bytes(original)
    run('partial_negative_incident_blocks_after_exact_original_restore','permanent_observed_original_trust_loss');assert files(root)==before;move_incident(version,'partial-journal');run('explicit_operator_fixture_recovery_after_partial_journal');assert native.read_bytes()==gold
    assert raw_before.items() <= snap(staging).items() and snap(published)==prior
    report.update(output=output,all_original_capture_bytes_preserved=True,source_byte_idempotent=True,two_normal_native_prebuilt_catalog_cycles_reach_real_missing_approval=True,failed_quality_and_approval_preserve_prior_local_fixture=True,all_rejected_candidate_bytes_detached_intact=True,permanent_original_loss_no_cold_rebootstrap=True,no_source_baseline_contract_identity_or_release_acceptance=True,actual_run_count=len(report['runs']))
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'code_head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
