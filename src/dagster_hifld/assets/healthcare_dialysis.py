"""Healthcare Ready's complete inactive Rx Open dialysis category draft."""
import os
from dagster import AssetCheckResult, AssetKey, AssetSelection, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job
from dagster_hifld.partitions import parse_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.dialysis_centers import SLUG, stage_dialysis_centers, location_quality

@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='healthcare_ready',compute_kind='http',description='Complete producer inactive Rx Open dialysis category; original IDs/types/native points/status retained, including zero coordinates.')
def healthcare_ready_dialysis_centers(context,staging_storage:StagingStorageResource):
    result=stage_dialysis_centers(staging_storage,capture_dir=os.environ.get('HIFLD_DIALYSIS_CAPTURE_DIR'))
    context.add_output_metadata(result)
    return result

@asset_check(asset=['publish','catalog'],name='dialysis_source_locations',blocking=True,description='Refuse source zero/unknown points; preserve those records in draft without geocoding or filtering.')
def dialysis_source_locations(context,staging_storage:StagingStorageResource):
    if not context.has_partition_key:return AssetCheckResult(passed=False,metadata={'reason':'no_partition_selected'})
    dataset,file,version=parse_publish_partition_key(context.partition_key)
    if(dataset,file)!=(SLUG,SLUG):return AssetCheckResult(passed=True,metadata={'reason':'different_dataset'})
    passed,metadata=location_quality(staging_storage,version)
    return AssetCheckResult(passed=passed,metadata=metadata)

dialysis_source_integrity_job=define_asset_job('dialysis_source_integrity',selection=AssetSelection.assets(healthcare_ready_dialysis_centers))
dialysis_source_integrity_schedule=ScheduleDefinition(name='dialysis_source_integrity_monthly',job=dialysis_source_integrity_job,cron_schedule='0 9 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
