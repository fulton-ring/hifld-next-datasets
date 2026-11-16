"""Explicit original-state electric territory pilots; all schedules stopped."""
from dagster import (
    AssetCheckResult, AssetIn, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.state_electric_territories import IOWA, WISCONSIN, stage_state_territories


class StateTerritoriesConfig(Config):
    capture_directory: str = ''


@asset(key=AssetKey(['ingest', IOWA, 'electrical-service-boundaries']), group_name='energy', compute_kind='http',
    description='Native Iowa Utilities Commission electrical service polygons; ESRI102004 and all native fields retained; unaccepted state pilot.')
def iowa_electric_service_boundaries(context, config: StateTerritoriesConfig, staging_storage: StagingStorageResource):
    results = stage_state_territories(staging_storage, 'iowa', config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [r['staging_prefix'] for r in results])
    context.add_output_metadata(results[0])
    return results


@asset(key=AssetKey(['ingest', WISCONSIN, 'cooperative']), group_name='energy', compute_kind='http',
    description='PSC Wisconsin original ZIP: separate cooperative, investor-owned and municipal layers; EPSG3071 including municipal Z; approximate boundaries, no legal accuracy guarantee.')
def wisconsin_psc_electric_cooperative(context, config: StateTerritoriesConfig, staging_storage: StagingStorageResource):
    results = stage_state_territories(staging_storage, 'wisconsin', config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [r['staging_prefix'] for r in results])
    context.add_output_metadata({'files': len(results), 'feature_count': sum(r['feature_count'] for r in results)})
    return results


@asset_check(asset=iowa_electric_service_boundaries, blocking=True)
def iowa_electric_boundaries_quality(iowa_electric_service_boundaries):
    return AssetCheckResult(passed=all(r['source_population_complete'] and r['null_geometry_count'] == 0
        and r['invalid_geometry_count'] == 0 for r in iowa_electric_service_boundaries))


@asset_check(asset=wisconsin_psc_electric_cooperative, blocking=True)
def wisconsin_electric_territories_quality(wisconsin_psc_electric_cooperative):
    return AssetCheckResult(passed=all(r['source_population_complete'] and r['null_geometry_count'] == 0
        and r['invalid_geometry_count'] == 0 for r in wisconsin_psc_electric_cooperative))


@asset(key=AssetKey(['ingest', WISCONSIN, 'investor-owned']), ins={'wisconsin_psc_electric_cooperative': AssetIn(key=AssetKey(['ingest', WISCONSIN, 'cooperative']))}, group_name='energy', compute_kind='local',
    description='PSC Wisconsin native investor-owned layer from the same validated original ZIP capture.')
def wisconsin_psc_electric_investor_owned(wisconsin_psc_electric_cooperative):
    return next(r for r in wisconsin_psc_electric_cooperative if r['file_slug'] == 'investor-owned')


@asset(key=AssetKey(['ingest', WISCONSIN, 'municipal']), ins={'wisconsin_psc_electric_cooperative': AssetIn(key=AssetKey(['ingest', WISCONSIN, 'cooperative']))}, group_name='energy', compute_kind='local',
    description='PSC Wisconsin native municipal layer, including original Z coordinates, from the same ZIP capture.')
def wisconsin_psc_electric_municipal(wisconsin_psc_electric_cooperative):
    return next(r for r in wisconsin_psc_electric_cooperative if r['file_slug'] == 'municipal')


INGEST_ASSETS = [iowa_electric_service_boundaries, wisconsin_psc_electric_cooperative,
    wisconsin_psc_electric_investor_owned, wisconsin_psc_electric_municipal]
ASSET_CHECKS = [iowa_electric_boundaries_quality, wisconsin_electric_territories_quality]
JOBS = [define_asset_job('original_state_electric_territories_draft', selection=AssetSelection.assets(*INGEST_ASSETS))]
SCHEDULES = [ScheduleDefinition(name='original_state_electric_territories_draft_monthly', job=JOBS[0],
    cron_schedule='0 10 1 * *', default_status=DefaultScheduleStatus.STOPPED)]
