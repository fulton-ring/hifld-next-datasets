"""Replay the normal registered source + catalog + format graph, without approval.

Usage: uv run python .../run_normal_graph.py CAPTURE_ROOT NEW_PRIVATE_ROOT VERSION
Requires tippecanoe on PATH. The complete official capture is replayed; no live
retrieval is implied. A new private directory is required, with cloud buckets
and release-approval environment variables removed before Definitions imports.
"""
import hashlib
import json
import os
import pathlib
import sys


def main(capture, root, version, source_only=False):
    root.mkdir(parents=True, exist_ok=False)
    home=root/'dagster';home.mkdir()
    (home/'dagster.yaml').write_text('telemetry:\n  enabled: false\n')
    for key in ('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS'):
        os.environ.pop(key,None)
    os.environ.update(DAGSTER_HOME=str(home),HIFLD_STAGING_DIR=str(root/'staging'),
        HIFLD_DATASETS_DIR=str(root/'published'),HIFLD_STAGING_PREFIX='',HIFLD_DATASETS_PREFIX='')
    # Import after private local resources are configured; no user/cloud state.
    from dagster import AssetKey, DagsterInstance, DefaultScheduleStatus
    from dagster_hifld.definitions import defs
    from dagster_hifld.assets.fuel_stations import SLUG, bts_alternative_fueling_stations, processing_identity
    from dagster_hifld.partitions import PUBLISH_PARTITIONS
    from dagster_hifld.promotion import promote_immutable_objects
    from dagster_hifld.release_approval import ReleaseApprovalError, validate_release_provenance
    from dagster_hifld.source_manifest import load_resolved_source_manifest
    from dagster_hifld.resources import StagingStorageResource, PublishedStorageResource

    prefix=f'{SLUG}/{SLUG}/{version}'
    staging=StagingStorageResource(local_dir=str(root/'staging'),use_local=True)
    published=PublishedStorageResource(local_dir=str(root/'published'),use_local=True)
    published.write_key('previous-local-fixture/keep.bin',b'preserve previous local fixture')
    before={k:published.sha256_key(k) for k in published.list_prefix('')}
    repository=defs.get_repository_def()
    refresh=repository.get_job('fuel_stations_draft_refresh_job')
    schedule=repository.get_schedule_def('fuel_stations_draft_refresh_schedule')
    assert schedule.default_status==DefaultScheduleStatus.STOPPED
    assert set(refresh.asset_layer.executable_asset_keys)=={next(iter(bts_alternative_fueling_stations.keys))}
    keys=[AssetKey(['publish','catalog']),AssetKey(['publish','verify']),AssetKey(['publish','promote'])]+[
        AssetKey(['publish','formats',x]) for x in ('geopackage','geoparquet','pmtiles','shapefile_zip')]
    if source_only: keys=[AssetKey(['publish','catalog'])]
    job=repository.get_implicit_job_def_for_assets(keys)
    record={'local_only':True,'source_catalog_only':source_only,'version':version,'processing':processing_identity(),
        'normal_definitions_module':sys.modules['dagster_hifld.definitions'].__file__,
        'normal_asset_count':len(repository.asset_graph.get_all_asset_keys()),
        'schedule_default_status':str(schedule.default_status),'schedule_activated':False,
        'schedule_cron':schedule.cron_schedule,'schedule_execution_timezone':schedule.execution_timezone,
        'refresh_asset_selection':[k.path for k in refresh.asset_layer.executable_asset_keys]}
    with DagsterInstance.get() as instance:
        source=refresh.execute_in_process(instance=instance,run_config={'ops':{
            bts_alternative_fueling_stations.op.name:{'config':{'capture_dir':str(capture),'version':version}}}},
            raise_on_error=False)
        record['source']={'run_id':source.run_id,'success':source.success}
        (root/'validation.json').write_text(json.dumps(record,indent=2)+'\n')
        assert source.success,'Source failed; retained raw/quarantine and Dagster logs remain under private root'
        manifest=json.loads((root/'staging'/prefix/'metadata/source_manifest.json').read_bytes())
        validate_release_provenance(manifest)
        validate_release_provenance(load_resolved_source_manifest(staging,SLUG,SLUG,version).metadata)
        record.update(direct_provenance_validation=True,input_count=len(manifest['inputs']))
        instance.add_dynamic_partitions(PUBLISH_PARTITIONS.name,[prefix])
        result=job.execute_in_process(asset_selection=keys,partition_key=prefix,instance=instance,raise_on_error=False)
        record.update(run_id=result.run_id,success=result.success,
            successful_steps=[e.step_key for e in result.all_events if e.event_type_value=='STEP_SUCCESS'],
            failed_steps={e.step_key:e.event_specific_data.error.to_string() for e in result.get_step_failure_events()},
            shapefile_outputs=[])
        for event in result.all_events:
            if event.event_type_value=='ASSET_MATERIALIZATION' and event.step_key=='publish__formats__shapefile_zip':
                record['shapefile_outputs'].append({k:str(v.value) for k,v in event.event_specific_data.materialization.metadata.items()})
        candidate=f'{prefix}/draft_candidate/candidate.parquet'
        try:
            promote_immutable_objects(staging,published,[(candidate,f'{prefix}/geoparquet/{SLUG}.parquet')],version_prefix=prefix)
        except ReleaseApprovalError as error:
            record['direct_promotion_rejection']=str(error)
        else:
            raise AssertionError('Unapproved direct promotion unexpectedly succeeded')
    after={k:published.sha256_key(k) for k in published.list_prefix('')}
    record.update(previous_release_before=before,previous_release_after=after,
        previous_release_preserved=before==after,
        candidate_and_raw_preserved=(root/'staging'/prefix/'raw_arcgis/page-000.body').is_file() and (root/'staging'/candidate).is_file())
    (root/'validation.json').write_text(json.dumps(record,indent=2)+'\n')
    assert before==after and record['candidate_and_raw_preserved']
    if source_only:
        assert result.success and not record['failed_steps']
    else:
        assert set(record['failed_steps'])=={'publish__verify'}
        assert 'missing_independent_release_approval_registry' in record['failed_steps']['publish__verify']
    assert 'publish__promote' not in record['successful_steps']
    print(json.dumps({k:v for k,v in record.items() if k!='failed_steps'},indent=2))


if __name__=='__main__':
    main(pathlib.Path(sys.argv[1]).resolve(),pathlib.Path(sys.argv[2]).resolve(),sys.argv[3], source_only=len(sys.argv)>4 and sys.argv[4]=='--source-catalog-only')
