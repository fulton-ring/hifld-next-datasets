"""FUDS property-boundary native candidate assets."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fuds_property_boundaries import (
    DATASET_SLUG, FILE_SLUG, capture_fuds_property_boundaries,
)


class FudsPropertyBoundariesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "USACE Military/FUDS_Data MapServer layer 4, FUDS Property Polygon. "
        "Candidate only: source layer is FY24 compilation context; precise "
        "release date, schema/ID contract, rights, and mapping are unapproved."
    ),
)
def fuds_property_boundaries_ingest(
    context, config: FudsPropertyBoundariesConfig, staging_storage: StagingStorageResource
) -> dict:
    version = build_version_id(context)
    result = capture_fuds_property_boundaries(
        staging_storage, version=version,
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=fuds_property_boundaries_ingest, blocking=True)
def fuds_property_boundaries_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 3022 and candidate.get("source_null_geometry_count") == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_release_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "freshness_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


@asset_check(asset=fuds_property_boundaries_ingest, blocking=True)
def fuds_property_boundaries_geometry_quality(candidate: dict) -> AssetCheckResult:
    invalid_count = candidate.get("source_invalid_geometry_count")
    return AssetCheckResult(
        passed=invalid_count == 0,
        metadata={
            "source_invalid_geometry_count": invalid_count,
            "geometry_repaired": False,
            "source_release_accepted": False,
        },
    )


fuds_property_boundaries_draft_job = define_asset_job(
    "fuds_property_boundaries_draft_refresh",
    selection=AssetSelection.assets(fuds_property_boundaries_ingest),
)
fuds_property_boundaries_draft_schedule = ScheduleDefinition(
    name="fuds_property_boundaries_draft_refresh_schedule",
    job=fuds_property_boundaries_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
