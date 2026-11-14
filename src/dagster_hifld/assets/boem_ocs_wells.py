"""Draft BOEM/BSEE OCS well-point acquisition assets."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.boem_ocs_wells import (
    DATASET_SLUG, FILE_SLUG, capture_boem_ocs_wells,
)


class BoemOcsWellsConfig(Config):
    capture_directory: str = ""
    offline_capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="boem",
    compute_kind="http",
    description=(
        "BOEM/BSEE national Marine Mineral Cadastre OCS Oil and Natural Gas "
        "Wells layer on BOEM's own gis.boem.gov server; captures the native "
        "Esri JSON pages and stages an EPSG:4269 GeoParquet point candidate. "
        "Source release date, schema/ID contracts, rights and release are "
        "unapproved."
    ),
)
def boem_ocs_wells(context, config: BoemOcsWellsConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_boem_ocs_wells(
        staging_storage,
        capture_directory=config.capture_directory or None,
        offline_capture_directory=config.offline_capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=boem_ocs_wells, blocking=True)
def boem_ocs_wells_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("feature_count", 0)
    passed = (isinstance(count, int) and count > 0 and bool(candidate.get("candidate_ready"))
              and not candidate.get("release_accepted", False))
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": count,
            "unique_api_number_count": candidate.get("unique_api_number_count"),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "source_authority_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


boem_ocs_wells_draft_job = define_asset_job(
    "boem_ocs_wells_draft_refresh",
    selection=AssetSelection.assets(boem_ocs_wells),
)
boem_ocs_wells_draft_schedule = ScheduleDefinition(
    name="boem_ocs_wells_draft_refresh_schedule",
    job=boem_ocs_wells_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [boem_ocs_wells]
ASSET_CHECKS = [boem_ocs_wells_complete]
JOBS = [boem_ocs_wells_draft_job]
SCHEDULES = [boem_ocs_wells_draft_schedule]
