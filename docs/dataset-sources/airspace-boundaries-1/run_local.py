"""Real isolated Dagster replay and failed-promotion preservation, no approvals."""
import hashlib
import json
import os
import pathlib
import sys

from audit_formats import audit_formats

from dagster import AssetKey, AssetSelection, DagsterInstance, define_asset_job, Definitions

import dagster_hifld.faa_airspace as adapter
from dagster_hifld.assets import hifld_dataset_assets
from dagster_hifld.assets.faa import faa_airspace_boundaries
from dagster_hifld.checks import hifld_asset_checks
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.promotion import promote_immutable_objects
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

ROOT=pathlib.Path(__file__).parent
capture=pathlib.Path(sys.argv[1]);runtime=pathlib.Path(sys.argv[2]);runtime.mkdir(parents=True,exist_ok=True)
assert 'HIFLD_RELEASE_APPROVALS' not in os.environ
stage=StagingStorageResource(local_dir=str(runtime/'staging'));published=PublishedStorageResource(local_dir=str(runtime/'published'))
resources={'staging_storage':stage,'published_storage':published}
state=runtime/'dagster';state.mkdir(exist_ok=True);(state/'dagster.yaml').write_text('telemetry:\n  enabled: false\n');os.environ['DAGSTER_HOME']=str(state)
result={'adapter_module_path':str(pathlib.Path(adapter.__file__).resolve()),'capture_dir':str(capture.resolve()),'local_storage':str(runtime.resolve()),'approval_registry_present':False,'shared_verifier_dependency':'15a515241e16e5cda8c8b30d9599f321dc9aabcd'}
with DagsterInstance.from_config(str(state)) as instance:
    ingest_job=define_asset_job('faa_ingest_replay',selection=AssetSelection.assets(faa_airspace_boundaries))
    defs=Definitions(assets=hifld_dataset_assets,asset_checks=hifld_asset_checks,resources=resources,jobs=[ingest_job])
    r=defs.resolve_job_def('faa_ingest_replay').execute_in_process(instance=instance,run_config={'ops':{'ingest__airspace_boundaries_1__airspace_boundaries_1':{'config':{'capture_dir':str(capture)}}}})
    assert r.success
    event=next(e for e in r.get_asset_materialization_events() if e.asset_key==AssetKey(['ingest',adapter.SLUG,adapter.SLUG]));out=r.output_for_node(event.step_key)
    result['ingest']={'run_id':r.run_id,'success':r.success,'output':out}
    candidate=stage.read_key(out['quarantine_prefix']+'/candidate/airspace.parquet')
    import io
    import pyarrow.parquet as pq
    t=pq.read_table(io.BytesIO(candidate));assert t.num_rows==477
    assert t['GLOBAL_ID'].null_count==0 and len(set(t['GLOBAL_ID'].to_pylist()))==477
    assert len(t.column_names)==40
    manifest=json.loads(stage.read_key(out['quarantine_prefix']+'/metadata/provenance.json'));assert manifest['invalid_geometry_count']==0 and manifest['diagnostic_server_geojson_invalid_geometry_count']==22 and manifest['promotion_allowed'] is False
    result['assertions']={'rows':t.num_rows,'columns':t.column_names,'unique_GLOBAL_ID':477,'invalid_geometry_count':0,'diagnostic_server_geojson_invalid_geometry_count':22,'raw_source_native_sha256':hashlib.sha256(stage.read_key(out['quarantine_prefix']+'/raw/source-native.json')).hexdigest(),'candidate_sha256':hashlib.sha256(candidate).hexdigest()}
    # Deliberately isolated conversion sandbox; this copy is not acceptance.
    version='v0.0.10-draft';prefix=f'{adapter.SLUG}/{adapter.SLUG}/{version}'
    stage.write_key(prefix+'/geopackage/airspace.gpkg',stage.read_key(out['quarantine_prefix']+'/candidate/airspace-native.gpkg'))
    stage.write_key(prefix+'/metadata/source_manifest.json',json.dumps({'publisher':manifest['publisher'],'source_url':adapter.LAYER_URL,'transformations':manifest['transformations'],'lineage':{'quarantine_only':True,'approval':None},'unsupported_output_formats':adapter.UNSUPPORTED_OUTPUT_FORMATS}).encode())
    key=build_publish_partition_key(adapter.SLUG,adapter.SLUG,version);instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[key])
    sentinel='accepted/sentinel.json';published.write_key(sentinel,b'{"historical":"preserve"}')
    before={k:hashlib.sha256(published.read_key(k)).hexdigest() for k in published.list_prefix()}
    selection=AssetSelection.keys(['publish','catalog'],['publish','formats','geopackage'],['publish','formats','geoparquet'],['publish','formats','pmtiles'],['publish','formats','shapefile_zip'],['publish','verify'],['publish','promote'])
    defs=Definitions(assets=hifld_dataset_assets,asset_checks=hifld_asset_checks,resources=resources,jobs=[define_asset_job('faa_draft_graph',selection=selection),define_asset_job('faa_verify_probe',selection=AssetSelection.keys(['publish','verify']))])
    r=defs.resolve_job_def('faa_draft_graph').execute_in_process(instance=instance,partition_key=key,raise_on_error=False)
    result['normal_graph']={'run_id':r.run_id,'success':r.success,'checks':[{'name':e.event_specific_data.check_name,'passed':e.event_specific_data.passed} for e in r.all_events if e.event_type_value=='ASSET_CHECK_EVALUATION']}
    assert not r.success
    quality=json.loads(stage.read_key(prefix+'/metadata/quality_manifest.json'));result['normal_graph']['quality_manifest']=quality
    assert quality['feature_count']==477 and quality['invalid_geometry_count']==0
    assert any(e.asset_key==AssetKey(['publish','formats','geopackage']) for e in r.get_asset_materialization_events())
    assert any(e.asset_key==AssetKey(['publish','formats','geoparquet']) for e in r.get_asset_materialization_events())
    assert any(e.asset_key==AssetKey(['publish','formats','pmtiles']) for e in r.get_asset_materialization_events())
    assert not any(k.endswith('.zip') for k in stage.list_prefix(prefix+'/shapefile'))
    result['terminal_roundtrip']=audit_formats(pathlib.Path(stage.local_dir),out['quarantine_prefix'],prefix)
    assert not any(e.asset_key==AssetKey(['publish','promote']) for e in r.get_asset_materialization_events())
    r=defs.resolve_job_def('faa_verify_probe').execute_in_process(instance=instance,partition_key=key,raise_on_error=False)
    assert not r.success;result['verify_probe']={'run_id':r.run_id,'success':False,'reason':'missing_independent_release_approval_registry'}
    try:promote_immutable_objects(stage,published,[(prefix+'/geopackage/airspace.gpkg',prefix+'/geopackage/airspace.gpkg')])
    except ReleaseApprovalError as exc:result['direct_promotion_error']=str(exc)
    else:raise AssertionError('Unapproved promotion unexpectedly passed')
    assert result['direct_promotion_error']=='missing_independent_release_approval_registry'
    after={k:hashlib.sha256(published.read_key(k)).hexdigest() for k in published.list_prefix()}
    assert before==after;result['accepted_inventory_unchanged']=before==after
    result['staged_candidate_preserved']=stage.object_exists(out['quarantine_prefix']+'/candidate/airspace.parquet')
(ROOT/'evidence/local-materialization.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
