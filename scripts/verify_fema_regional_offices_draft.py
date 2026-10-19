"""Actual registered office-point graph/gates; fixtures do not grant acceptance."""
from __future__ import annotations
import argparse
import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);parser.add_argument('--capture-dir',type=Path,required=True);args=parser.parse_args()
runtime=args.runtime.absolute();captures=args.capture_dir.absolute();assert not runtime.exists() and captures.is_dir()
for name in ('dagster','staging','published'):(runtime/name).mkdir(parents=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS'):os.environ.pop(key,None)
inputs=runtime/'inputs';shutil.copytree(captures,inputs)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_FEMA_OFFICES_CAPTURE_DIR=str(inputs),HIFLD_SHADOW_MANUAL_ONLY='1')
import pyarrow.parquet as pq
from dagster import AssetSelection, DagsterInstance, Definitions, define_asset_job
from dagster_hifld.definitions import defs as normal, _iter_staged_version_paths
from dagster_hifld.assets.fema_regional_offices import fema_regional_offices
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource, PublishedStorageResource
from dagster_hifld.sources import fema_regional_offices as s
from dagster_hifld.release_approval import validate_release_provenance

staging=StagingStorageResource(local_dir=str(runtime/'staging'));published=PublishedStorageResource(local_dir=str(runtime/'published'))
published.write_key('synthetic-prior-fixture.bin',b'local-preservation-fixture-only-no-real-accepted-release')
def snapshot(path):return {str(p.relative_to(path)):s.sha256_file(p) for p in path.rglob('*') if p.is_file()}
prior=snapshot(runtime/'published');original_inputs=snapshot(inputs)
keys=[key for key in normal.resolve_asset_graph().get_all_asset_keys() if key.path[0]=='publish']
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('fema_offices_local_normal_graph',selection=AssetSelection.assets(*keys))])
report={'approved':False,'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(s.__file__).parents[3],text=True).strip(),'module':s.__file__,'runtime':str(runtime),'runs':[],'scope':'Complete real captured14-office input, unpatched source/candidate/Git/state/catalog/gates; explicitly labelled fixture corruption/fault probes. Live acquisition tested separately.'}
def record(kind,result):
    errors=[event.event_specific_data.error.to_string() for event in result.get_step_failure_events()]
    report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,'materializations':[event.asset_key.to_user_string() for event in result.get_asset_materialization_events()],'checks':[{'name':event.check_name,'passed':event.passed} for event in result.get_asset_check_evaluations()]})
    (runtime/'execution-progress.json').write_bytes(s.encode(report));assert snapshot(runtime/'published')==prior
    return errors

def move_incident(version,label):
    # Test operator explicitly retains the negative journal OUTSIDE staging;
    # restoring exact trusted originals alone never silently clears an incident.
    directory=runtime/'staging'/s.incident_directory(version)
    if directory.exists():directory.rename(runtime/('operator-reviewed-fixture-incident-'+label))

with DagsterInstance.get() as instance:
    job=normal.resolve_job_def('fema_regional_offices_draft_refresh');graph=defs.resolve_job_def('fema_offices_local_normal_graph')
    def refresh(label,expected=None):
        result=job.execute_in_process(instance=instance,raise_on_error=False);errors=record(label,result)
        if expected is None:assert result.success,errors
        else:assert not result.success and any(expected in error for error in errors),errors
        return result
    receipt=inputs/'authority_root.receipt.json';receipt.chmod(0)
    try:refresh('genuine_unreadable_first_receipt_body_only_diagnostic','Permission denied')
    finally:receipt.chmod(0o600)
    body=(inputs/'authority_root.body').read_bytes();assert (runtime/'staging'/f'_raw-captures/{s.SLUG}/{s.digest(body)}/authority_root.body').read_bytes()==body
    attempts=list((runtime/'staging'/'_quarantine').rglob('attempted-inputs.json'));assert len(attempts)==1
    attempted=json.loads(attempts[0].read_bytes())['objects']['authority_root'];assert attempted['raw_receipt_read'] is False and 'raw_receipt_key' not in attempted
    first=refresh('complete_real_fourteen_office_source');output=first.output_for_node(fema_regional_offices.op.name);assert output['feature_count']==14
    version=output['version'];prefix=build_publish_partition_key(s.SLUG,s.SLUG,version);root=runtime/'staging'/prefix
    assert (s.SLUG,s.SLUG,version) in list(_iter_staged_version_paths(staging));native=root/'geoparquet'/f'{s.SLUG}.parquet';gold=native.read_bytes();original_manifest=(root/'metadata/source_manifest.json').read_bytes()
    original_candidate=snapshot(root);original_raw={k:v for k,v in snapshot(runtime/'staging').items() if k.startswith('_raw-captures/')};alias=runtime/'staging'/s.construction_record_key(version);bundle=alias.read_bytes();state_files={p:p.read_bytes() for p in alias.parent.rglob('*') if p.is_file()}
    again=refresh('source_idempotent_full_byte_reuse');assert again.output_for_node(fema_regional_offices.op.name)['reused'] is True and snapshot(root)==original_candidate
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
    for cycle in (1,2):
        result=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);errors=record('normal_catalog_'+str(cycle)+'_real_missing_release_registry',result)
        assert not result.success and errors and all('missing_independent_release_approval_registry' in error for error in errors),errors
        refresh('source_refresh_after_catalog_'+str(cycle));assert native.read_bytes()==gold and (root/'metadata/source_manifest.json').read_bytes()==original_manifest
    source_table,source_audit=s.inspect_captures({name:((inputs/(name+'.body')).read_bytes(),json.loads((inputs/(name+'.receipt.json')).read_bytes())) for name in s.request_plan(list(range(1,15)))})
    actual=pq.read_table(native);assert actual.equals(s.expected_parquet(source_table),check_metadata=True);s.verify_outputs(root,source_table);validate_release_provenance(json.loads(original_manifest))
    report['native']={'rows':actual.num_rows,'attributes':14,'schema':str(actual.schema),'parquet_bytes':len(gold),'parquet_sha256':s.digest(gold),'all_original_values_IDs_physical_types_nulls_whitespace_native_points_exact':True,'baseline_comparison':json.loads((root/'metadata/baseline_comparison.json').read_bytes()),'source_audit':source_audit}
    report['quality']=json.loads((root/'metadata/quality_manifest.json').read_bytes())
    # Independently labelled geometry corruption on a complete isolated clone.
    badprefix=prefix+'-synthetic-null-geometry';badroot=runtime/'staging'/badprefix;shutil.copytree(root,badroot)
    with sqlite3.connect(badroot/'geopackage'/f'{s.SLUG}.gpkg') as db:
        for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?",(s.LAYER,)).fetchall():db.execute('DROP TRIGGER "'+name.replace('"','""')+'"')
        db.execute('UPDATE '+s.LAYER+' SET geometry=NULL WHERE _gpkg_storage_fid=1')
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix]);result=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);record('synthetic_null_geometry_quality_gate',result);assert not result.success and any(not event.passed for event in result.get_asset_check_evaluations())
    # Full unchanged source with a new display-only response/observation context.
    before_renderer=snapshot(root);original_input_files={p:p.read_bytes() for p in inputs.iterdir()}
    for name in ('metadata_before','metadata_after'):
        p=inputs/(name+'.body');value=json.loads(p.read_bytes());value['drawingInfo']={'synthetic_display_only':True};body=s.encode(value);p.write_bytes(body)
        p=inputs/(name+'.receipt.json');receipt_value=json.loads(p.read_bytes());receipt_value.update(sha256=s.digest(body),bytes=len(body),observed_at='2026-10-19T17:00:00+00:00');p.write_bytes(s.encode(receipt_value))
    result=refresh('display_only_new_receipt_context_reuses_original');assert result.output_for_node(fema_regional_offices.op.name)['reused'] is True and snapshot(root)==before_renderer and alias.read_bytes()==bundle
    for p,body in original_input_files.items():p.write_bytes(body)
    # Closed-roster rejection preserves the whole candidate before cold recovery.
    (root/'metadata/unknown-forged.json').write_bytes(b'{"approved":true}');altered=snapshot(root)
    refresh('unknown_metadata_whole_candidate_rejected','unknown_missing_or_linked_candidate_object');assert not root.exists()
    quarantines=[p for p in (runtime/'staging'/'_quarantine'/s.SLUG).glob('invalid-candidate-*') if p.is_dir()];assert len(quarantines)==1 and snapshot(quarantines[0])==altered
    refresh('cold_rebuild_exact_original_after_cache_corruption');assert native.read_bytes()==gold and (root/'metadata/source_manifest.json').read_bytes()==original_manifest
    # Semantically consistent original receipt+manifest coforge still fails its
    # independent outside marker, then permanent negative incident stops retries.
    forged=json.loads(bundle);receipt_value=forged['payload']['captures']['page_00000']['receipt'];receipt_value['observed_at']='2026-10-19T23:04:58+00:00'
    next(item for item in forged['payload']['manifest']['inputs'] if item['input_id']=='page_00000:'+receipt_value['sha256'])['retrieved_at']=receipt_value['observed_at']
    alias.write_bytes(s.canonical_construction_bundle(version,forged['processing_fingerprint'],forged['payload']))
    refresh('coforged_original_receipt_and_manifest_denied','construction_marker_mismatch');assert not root.exists()
    for count in (1,2):refresh('forged_original_repeated_cold_'+str(count),'permanent_observed_original_trust_loss')
    alias.write_bytes(bundle);move_incident(version,'receipt-forgery');refresh('explicit_exact_original_restore_after_forgery');assert native.read_bytes()==gold and (root/'metadata/source_manifest.json').read_bytes()==original_manifest
    # Lose ALL original outside witnesses; journal must be installed before
    # whole quarantine so absent-candidate retries cannot bootstrap new history.
    for path in state_files:path.unlink()
    refresh('all_outside_original_witnesses_missing','observed_original_trust_loss');assert not root.exists() and not alias.exists()
    for count in (1,2):refresh('all_witnesses_missing_cold_'+str(count),'permanent_observed_original_trust_loss')
    for p,body in state_files.items():p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
    move_incident(version,'all-witnesses-missing');refresh('explicit_exact_all_original_witness_restore');assert native.read_bytes()==gold
    # Storage failure of the negative journal must leave the candidate intact.
    alias.unlink();before=snapshot(root)
    with patch.object(s,'record_observed_loss',side_effect=OSError('synthetic incident persistence failure')):refresh('incident_write_fault_preserves_remaining_candidate','incident_write_failed_candidate_preserved')
    assert snapshot(root)==before
    directory=runtime/'staging'/s.incident_directory(version);directory.mkdir();(directory/'.pending-interrupted-fixture').write_bytes(b'partial incident')
    alias.write_bytes(bundle);refresh('partial_incident_blocks_even_after_original_restore','permanent_observed_original_trust_loss');assert snapshot(root)==before
    move_incident(version,'partial-journal');refresh('explicit_reviewed_fixture_incident_restore');assert native.read_bytes()==gold and (root/'metadata/source_manifest.json').read_bytes()==original_manifest
    assert original_raw.items() <= snapshot(runtime/'staging').items() and snapshot(inputs)==original_inputs and snapshot(runtime/'published')==prior
    report.update(output=output,original_input_objects_preserved=True,original_raw_objects_preserved=True,original_first_construction_receipts_and_native_bytes_preserved=True,failed_quality_and_approval_preserve_prior_fixture=True,permanent_observed_loss_and_partial_incident_no_rebootstrap=True,actual_run_count=len(report['runs']))
(runtime/'execution.json').write_bytes(s.encode(report));print(json.dumps({'head':report['code_head'],'runs':[(run['kind'],run['run_id'],run['success']) for run in report['runs']]}))
