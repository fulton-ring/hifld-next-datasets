"""Stopped draft refresh for the official USACE IENC MOORING_FACILITY_AREA layer."""

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

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.mooring_facility_areas_usace_ienc import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    FILE_SLUG,
    capture_mooring_facility_areas_usace_ienc,
)


class MooringFacilityAreasUsaceIencConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Official USACE/Army Geospatial Center USACE_IENC_Master_Service FeatureServer MOORING_FACILITY_AREA "
        "layer 79 (ArcGIS item 6ee858a220a143a48c8d40a6cf0d3680, owner usace_agc_dkd). IENC coverage is "
        "7,260 miles across 21 rivers, primarily in the central United States; this is not national mooring "
        "coverage and not asserted equivalent to the historical HIFLD/DataLumos archive. Unapproved faithful "
        "candidate; baseline, contract, coverage, rights, and release decisions remain pending."
    ),
)
def mooring_facility_areas_usace_ienc(
    context,
    config: MooringFacilityAreasUsaceIencConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_mooring_facility_areas_usace_ienc(
        staging_storage,
        version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=mooring_facility_areas_usace_ienc, blocking=True)
def mooring_facility_areas_usace_ienc_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == EXPECTED_COUNT
        and candidate.get("source_population_complete") is True
        and candidate.get("geometry_valid_count") == EXPECTED_COUNT
        and candidate.get("geometry_invalid_count") == 0
        and candidate.get("geometry_empty_count") == 0
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "geometry_valid_count": candidate.get("geometry_valid_count", "missing"),
            "geometry_invalid_count": candidate.get("geometry_invalid_count", "missing"),
            "geometry_empty_count": candidate.get("geometry_empty_count", "missing"),
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
            "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
            "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
            "drift_guard": candidate.get("drift_guard", "missing"),
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


mooring_facility_areas_usace_ienc_draft_job = define_asset_job(
    "mooring_facility_areas_usace_ienc_draft_refresh",
    selection=AssetSelection.assets(mooring_facility_areas_usace_ienc),
)
mooring_facility_areas_usace_ienc_draft_schedule = ScheduleDefinition(
    name="mooring_facility_areas_usace_ienc_draft_refresh_schedule",
    job=mooring_facility_areas_usace_ienc_draft_job,
    cron_schedule="0 8 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
