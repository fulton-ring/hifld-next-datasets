"""Complete real-input replay through normal assets and fail-closed release gates."""
import hashlib
import json
import os
from pathlib import Path
import sys
import subprocess
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetKey, AssetSelection, DagsterInstance, Definitions, define_asset_job, build_sensor_context
from dagster_hifld import aviation_extract as a
from dagster_hifld.assets import hifld_dataset_assets
from dagster_hifld.assets.faa import faa_aviation_facilities
from dagster_hifld.checks import hifld_asset_checks, publish_catalog_quality_manifest
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.definitions import _iter_staged_version_paths, version_discovery_sensor
from dagster_hifld.promotion import promote_immutable_objects
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

capture=Path(sys.argv[1]);runtime=Path(sys.argv[2]);runtime.mkdir(parents=True,exist_ok=True)
assert 'HIFLD_RELEASE_APPROVALS' not in os.environ
stage=StagingStorageResource(local_dir=str(runtime/'staging'));published=PublishedStorageResource(local_dir=str(runtime/'published'))
state=runtime/'dagster';state.mkdir(exist_ok=True);(state/'dagster.yaml').write_text('telemetry:\n  enabled: false\n');os.environ['DAGSTER_HOME']=str(state)
resources={'staging_storage':stage,'published_storage':published}
result={'adapter_module':a.__file__,'capture':str(capture),'runtime':str(runtime),'approval_registry_present':False}
with DagsterInstance.from_config(str(state)) as instance:
    job=define_asset_job('aviation_real_ingest',selection=AssetSelection.assets(faa_aviation_facilities))
    defs=Definitions(assets=hifld_dataset_assets,asset_checks=hifld_asset_checks,resources=resources,jobs=[job])
    config={'ops':{faa_aviation_facilities.op.name:{'config':{'capture_dir':str(capture)}}}}
    r=defs.resolve_job_def('aviation_real_ingest').execute_in_process(instance=instance,run_config=config)
    assert r.success;out=r.output_for_node(faa_aviation_facilities.op.name)
    result['ingest']={'run_id':r.run_id,'success':True,'output':out}
    assert (a.SLUG,a.SLUG,out['version']) in list(_iter_staged_version_paths(stage))
    with mock.patch('dagster_hifld.definitions.StagingStorageResource.from_env',return_value=stage):
        discovery=version_discovery_sensor(build_sensor_context(instance=instance))
    discovered=[]
    for request in discovery.dynamic_partitions_requests:
        discovered.extend(request.partition_keys)
        instance.add_dynamic_partitions(request.partitions_def_name,request.partition_keys)
    assert build_publish_partition_key(a.SLUG,a.SLUG,out['version']) in discovered
    result['normal_discovery']={'ingest_alone_discoverable':True,'sensor_partition_requests':discovered,'manual_data_or_manifest_copies':False}
    r=defs.resolve_job_def('aviation_real_ingest').execute_in_process(instance=instance,run_config=config)
    assert r.success and r.output_for_node(faa_aviation_facilities.op.name)==out
    result['idempotent_replay']={'run_id':r.run_id,'success':True,'output_identical':True}
    prefix=f"{a.SLUG}/{a.SLUG}/{out['version']}";root=Path(stage.local_dir)/prefix
    table=pq.read_table(root/'draft_candidate/candidate.parquet');assert table.num_rows==19427
    result['candidate_audit']=a.verify_typed_geopackage(root/'draft_candidate/candidate.parquet',root/'geopackage'/f'{a.SLUG}.gpkg')
    source_manifest=json.loads((root/'metadata/source_manifest.json').read_bytes())
    a.validate_release_provenance(source_manifest)
    assert len(source_manifest['inputs'])==17
    for item in source_manifest['inputs']:
        assert stage.sha256_key(item['raw_key'])==item['sha256']
    result['source_manifest_structurally_valid_without_approval']=True
    provenance=json.loads((root/'draft_candidate/provenance.json').read_bytes())
    assert provenance['processing']['code_commit']==subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    assert provenance['processing']['working_tree_modified'] is False
    result['processing_provenance']=provenance['processing']
    audit=a.inspect_capture(capture)
    originals=[f for p in sorted(capture.glob('page-*.body')) for f in a._json(p)['features']]
    assert table.column_names==[f['name'] for f in audit['fields']]+['geometry']
    for field in audit['fields']:
        assert table[field['name']].to_pylist()==[f['attributes'][field['name']] for f in originals]
        assert table.schema.field(field['name']).type==a._TYPES[field['type']]
        assert table.schema.field(field['name']).nullable==field['nullable']
    assert table['geometry'].to_pylist()==[a.to_wkb(a._exact_point(f['geometry']),byte_order=1) for f in originals]
    result['source_assertions']={'rows':table.num_rows,'source_columns':len(audit['fields']),
        'all_original_values_physical_types_nullability_geometry_exact':True,
        'unique_OBJECTID':len(set(table['OBJECTID'].to_pylist())),
        'unique_SITE_NO':audit['unique_SITE_NO'],'duplicate_SITE_NO_rows':audit['duplicate_SITE_NO_rows'],
        'effective_dates':sorted(set(table['EFF_DATE'].to_pylist())),'native_crs':'EPSG:3857',
        'bounds':audit['bounds'],'all_raw_receipts_and_bytes_verified':True}
    if '--ingest-only' in sys.argv[3:]:
        (runtime/'execution.json').write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps(result,indent=2));sys.exit(0)
    # This sentinel models existing accepted data for preservation behavior only.
    published.write_key('accepted/preservation-fixture.json',b'{"release":"fixture-only-not-a-real-approval"}')
    inventory=lambda storage:{k:hashlib.sha256(storage.read_key(k)).hexdigest() for k in storage.list_prefix()}
    accepted_before=inventory(published)
    key=build_publish_partition_key(a.SLUG,a.SLUG,out['version']);instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[key])
    downstream=AssetSelection.keys(['publish','formats','geopackage'],['publish','formats','geoparquet'],['publish','formats','pmtiles'],['publish','formats','shapefile_zip'],['publish','verify'],['publish','promote'])
    graph=define_asset_job('aviation_real_graph',selection=downstream|AssetSelection.keys(['publish','catalog']))
    quality_job=define_asset_job('aviation_quality_failure',selection=downstream|AssetSelection.checks(publish_catalog_quality_manifest))
    defs=Definitions(assets=hifld_dataset_assets,asset_checks=hifld_asset_checks,resources=resources,jobs=[graph,quality_job])
    r=defs.resolve_job_def('aviation_real_graph').execute_in_process(instance=instance,partition_key=key,raise_on_error=False)
    failures=[{'step':e.step_key,'reason':e.event_specific_data.error.cause.message.strip()} for e in r.get_step_failure_events()]
    result['normal_graph']={'run_id':r.run_id,'success':r.success,'failures':failures,
        'checks':[{'name':e.event_specific_data.check_name,'passed':e.event_specific_data.passed} for e in r.all_events if e.event_type_value=='ASSET_CHECK_EVALUATION']}
    assert not r.success and len(failures)==1 and failures[0]['step']=='publish__verify' and 'missing_independent_release_approval_registry' in failures[0]['reason']
    assert not any(e.asset_key==AssetKey(['publish','promote']) for e in r.get_asset_materialization_events())
    quality=json.loads(stage.read_key(prefix+'/metadata/quality_manifest.json'));assert quality['feature_count']==19427 and quality['quality_check_passed'] is True
    result['normal_graph']['quality_manifest']=quality
    # Full terminal value/type/geometry audit, including declared nullable changes.
    paths=sorted((root/'geoparquet').glob('**/*.parquet'));assert paths
    terminal=pq.read_table(paths[0]);source_ids=table['OBJECTID'].to_pylist();terminal_ids=terminal['OBJECTID'].to_pylist()
    assert len(terminal_ids)==len(set(terminal_ids))==len(source_ids) and set(source_ids)==set(terminal_ids)
    # Generic terminal writer deliberately reorders spatially; exact original
    # OBJECTID values establish correspondence without inventing row keys.
    positions={oid:index for index,oid in enumerate(terminal_ids)}
    terminal=terminal.take(pa.array([positions[oid] for oid in source_ids],type=pa.int64()))
    schema_differences=[]
    for field in table.schema:
        assert table[field.name].to_pylist()==terminal[field.name].to_pylist()
        assert field.type==terminal.schema.field(field.name).type
        if field.nullable!=terminal.schema.field(field.name).nullable:schema_differences.append({'name':field.name,'source_nullable':field.nullable,'terminal_nullable':terminal.schema.field(field.name).nullable})
    result['terminal_parquet']={'rows':terminal.num_rows,'all_source_columns_values_types_native_wkb_equal':True,'derived_extra_columns':sorted(set(terminal.column_names)-set(table.column_names)),'unapproved_nullable_metadata_differences':schema_differences}
    assert accepted_before==inventory(published)
    raw_candidate_before={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for d in ['raw_arcgis','draft_candidate'] for p in (root/d).rglob('*') if p.is_file()}
    # Deliberate failed quality witness probes implementation behavior only.
    bad=dict(quality,quality_check_passed=False,invalid_geometry_count=1);stage.write_key(prefix+'/metadata/quality_manifest.json',json.dumps(bad).encode())
    r=defs.resolve_job_def('aviation_quality_failure').execute_in_process(instance=instance,partition_key=key,raise_on_error=False)
    assert not r.success
    assert any(e.event_type_value=='ASSET_CHECK_EVALUATION' and e.event_specific_data.check_name=='quality_manifest' and not e.event_specific_data.passed for e in r.all_events)
    assert not any(e.asset_key==AssetKey(['publish','promote']) for e in r.get_asset_materialization_events())
    result['failed_quality_gate']={'run_id':r.run_id,'success':False,'promotion_materialized':False,'synthetic_invalid_manifest_for_behavior_only':True}
    try:promote_immutable_objects(stage,published,[(prefix+'/geopackage/'+a.SLUG+'.gpkg',prefix+'/geopackage/'+a.SLUG+'.gpkg')])
    except ReleaseApprovalError as exc:result['direct_promotion_error']=str(exc)
    else:raise AssertionError('Unapproved direct promotion passed')
    assert result['direct_promotion_error']=='missing_independent_release_approval_registry'
    assert accepted_before==inventory(published)
    assert raw_candidate_before=={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for d in ['raw_arcgis','draft_candidate'] for p in (root/d).rglob('*') if p.is_file()}
    result['preservation']={'accepted_inventory_unchanged':True,'all_raw_and_candidate_bytes_unchanged':True,'fixture_is_not_real_acceptance':True}
(runtime/'execution.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
