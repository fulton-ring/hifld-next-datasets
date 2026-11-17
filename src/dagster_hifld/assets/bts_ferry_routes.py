"""Complete actual official BTS NCFO2020 route draft; scheduled refresh is stopped."""

from dagster import AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, define_asset_job

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.ferry_routes import SLUG, stage_ferry_routes


class BtsFerryRoutesConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(['ingest',SLUG,SLUG]),group_name='bureau_of_transportation_statistics',compute_kind='http',
       description='Complete actual official BTS NCFO2020 routes; preserves all original IDs,55 fields and native paths. Producer item/layer vintage conflict and all release compatibility decisions remain unapproved.')
def bts_ferry_routes(context,config:BtsFerryRoutesConfig,staging_storage:StagingStorageResource):
    result=stage_ferry_routes(staging_storage,capture_dir=config.capture_directory or None)
    context.add_output_metadata(result)
    return result


ferry_routes_source_integrity_job=define_asset_job('ferry_routes_source_integrity',selection=AssetSelection.assets(bts_ferry_routes))
ferry_routes_source_integrity_schedule=ScheduleDefinition(name='ferry_routes_source_integrity_monthly',job=ferry_routes_source_integrity_job,
    cron_schedule='0 11 1 * *',execution_timezone='America/New_York',default_status=DefaultScheduleStatus.STOPPED)
