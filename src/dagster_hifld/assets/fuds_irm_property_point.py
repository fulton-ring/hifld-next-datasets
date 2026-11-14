"""Dagster source and quality checks for the IRM property point draft."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fuds_irm_property_point import DATASET_SLUG, FILE_SLUG, capture_fuds_irm_property_point


class FudsIrmPropertyPointConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="usace", compute_kind="http",
       description="USACE Military/FUDS_Data MapServer layer 6, IRM Property Point, under group layer 5. Draft only; source release date, freshness, rights, and public contract remain unapproved.")
def fuds_irm_property_point_ingest(context, config: FudsIrmPropertyPointConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_fuds_irm_property_point(staging_storage, version=build_version_id(context), capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=fuds_irm_property_point_ingest, blocking=True)
def fuds_irm_property_point_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") is not None and candidate.get("candidate_ready") is not None and candidate.get("source_null_geometry_count") == 0
    return AssetCheckResult(passed=passed, metadata={"complete_source_population":candidate.get("feature_count"), "null_geometry_count":candidate.get("source_null_geometry_count"), "source_release_accepted":False, "freshness_accepted":False, "schema_contract_accepted":False, "id_contract_accepted":False, "rights_accepted":False, "release_accepted":False})


@asset_check(asset=fuds_irm_property_point_ingest, blocking=True)
def fuds_irm_property_point_geometry_quality(candidate: dict) -> AssetCheckResult:
    invalid = candidate.get("source_invalid_geometry_count")
    return AssetCheckResult(passed=invalid == 0, metadata={"source_invalid_geometry_count":invalid, "geometry_repaired":False, "source_release_accepted":False})


fuds_irm_property_point_draft_job = define_asset_job("fuds_irm_property_point_draft_refresh", selection=AssetSelection.assets(fuds_irm_property_point_ingest))
fuds_irm_property_point_draft_schedule = ScheduleDefinition(name="fuds_irm_property_point_draft_refresh_schedule", job=fuds_irm_property_point_draft_job, cron_schedule="0 8 1 * *", execution_timezone="America/New_York", default_status=DefaultScheduleStatus.STOPPED)
