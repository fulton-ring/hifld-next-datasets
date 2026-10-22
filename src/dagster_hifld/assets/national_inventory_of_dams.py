"""Draft ingest and explicit source-quality gates for the USACE NID."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.national_inventory_of_dams import (
    DATASET_SLUG, FILE_SLUG, capture_national_inventory_of_dams,
)


class NationalInventoryOfDamsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Direct USACE National Inventory of Dams producer layer 0 draft. Preserves all "
        "81 producer fields, source OBJECTID and native EPSG:4269 point geometry. NID "
        "contains only qualifying inventory records and USACE says the inventory remains incomplete."
    ),
)
def national_inventory_of_dams_ingest(context, config: NationalInventoryOfDamsConfig,
                                      staging_storage: StagingStorageResource) -> dict:
    result = capture_national_inventory_of_dams(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=national_inventory_of_dams_ingest, blocking=True)
def national_inventory_of_dams_population_complete(candidate: dict) -> AssetCheckResult:
    complete = (type(candidate.get("feature_count")) is int and candidate["feature_count"] > 0
                and candidate.get("candidate_ready") is not None)
    return AssetCheckResult(passed=complete, metadata={
        "captured_feature_count": candidate.get("feature_count", 0),
        "source_population_complete": complete,
        "nid_scope": "USACE inclusion criteria only; not all dams",
        "historical_crosswalk_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "rights_accepted": False,
        "release_accepted": False,
    })


@asset_check(asset=national_inventory_of_dams_ingest, blocking=True)
def national_inventory_of_dams_geometry_quality(candidate: dict) -> AssetCheckResult:
    invalid = candidate.get("source_invalid_geometry_count")
    nulls = candidate.get("source_null_geometry_count")
    return AssetCheckResult(
        passed=type(invalid) is int and invalid == 0 and type(nulls) is int and nulls == 0,
        metadata={
            "invalid_geometry_count": invalid,
            "null_geometry_count": nulls,
            "geometry_repaired": False,
            "source_release_accepted": False,
        },
    )


national_inventory_of_dams_draft_job = define_asset_job(
    "national_inventory_of_dams_draft_refresh",
    selection=AssetSelection.assets(national_inventory_of_dams_ingest),
)
national_inventory_of_dams_draft_schedule = ScheduleDefinition(
    name="national_inventory_of_dams_draft_refresh_schedule",
    job=national_inventory_of_dams_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
