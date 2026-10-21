"""USACE dataset assets."""

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    Config,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.sources.usace_navigation_marks_areas import (
    DATASET_SLUG as NAVIGATION_MARKS_DATASET_SLUG,
    FILE_SLUG as NAVIGATION_MARKS_FILE_SLUG,
    capture_usace_navigation_marks_areas,
)

_DATASET_SLUG = "administrative-areas-usace-ienc"
_FILE_SLUG = "administrative-areas-usace-ienc"
_DESCRIPTION = (
    "Administrative Areas (USACE IENC) (HIFLD inventory). Publisher: "
    "Army Geospatial Center / US Army Corps of Engineers. Inland navigation "
    "system (rivers, lock chambers) in 22 states; IENC S-57 data in Esri "
    "File Geodatabase. Source: PASDA HIFLD/FEMA conus_clipped mirror."
)


@asset(
    key=AssetKey(["ingest", _DATASET_SLUG, _FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=_DESCRIPTION,
)
def usace_administrative_areas_usace_ienc(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [("https://www.pasda.psu.edu/download/hifld_fema/conus_clipped/Administrative_Areas__USACE_IENC_.zip", "Administrative_Areas__USACE_IENC_.zip")],
        _DATASET_SLUG,
        _FILE_SLUG,
        version,
        staging_storage,
    )


class UsaceNavigationMarksAreasConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", NAVIGATION_MARKS_DATASET_SLUG, NAVIGATION_MARKS_FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Official USACE IENC NAVIGATIONAL_SYSTEM_OF_MARKS_AREA layer (109 rows observed). "
        "Faithful candidate only; BTS item succession, baseline, schema/ID, coverage, rights, and release acceptance remain pending."
    ),
)
def usace_navigation_marks_areas_usace_ienc(
    context, config: UsaceNavigationMarksAreasConfig, staging_storage: StagingStorageResource
) -> dict:
    result = capture_usace_navigation_marks_areas(
        staging_storage,
        version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(NAVIGATION_MARKS_DATASET_SLUG, NAVIGATION_MARKS_FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usace_navigation_marks_areas_usace_ienc, blocking=True)
def usace_navigation_marks_areas_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 109 and candidate.get("source_population_complete") is True
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
            "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
            "drift_guard": candidate.get("drift_guard", "missing"),
            "bts_item_succession_accepted": False,
            "baseline_comparison_accepted": False,
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "coverage_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


usace_navigation_marks_areas_draft_job = define_asset_job(
    "usace_navigation_marks_areas_draft_refresh",
    selection=AssetSelection.assets(usace_navigation_marks_areas_usace_ienc),
)
usace_navigation_marks_areas_draft_schedule = ScheduleDefinition(
    name="usace_navigation_marks_areas_draft_refresh_schedule",
    job=usace_navigation_marks_areas_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
