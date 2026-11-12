"""Stopped draft refresh for the official USACE IENC LAND_AREA layer."""

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
from dagster_hifld.sources.land_surface_areas_usace_ienc import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_EMPTY_GEOMETRY_COUNT,
    EXPECTED_FEATURE_ID_NULL_COUNT,
    EXPECTED_INVALID_GEOMETRY_COUNT,
    FILE_SLUG,
    capture_land_surface_areas_usace_ienc,
)


class LandSurfaceAreasUsaceIencConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Official USACE/Army Geospatial Center IENC LAND_AREA MapServer layer 97. "
        "Complete feature-level capture pinned to the full OBJECTID roster with "
        "value/geometry baselines. The layer currently publishes one "
        "self-intersecting ring (OBJECTID 12570) that is preserved unmodified and "
        "reported. Any candidate is limited to inland-chart coverage, not "
        "nationwide land-surface coverage. Historical HIFLD continuity, vintage, "
        "rights and schema acceptance remain pending."
    ),
)
def land_surface_areas_usace_ienc_candidate(
    context,
    config: LandSurfaceAreasUsaceIencConfig,
    staging_storage: StagingStorageResource,
) -> dict:
    result = capture_land_surface_areas_usace_ienc(
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


@asset_check(asset=land_surface_areas_usace_ienc_candidate, blocking=True)
def land_surface_areas_usace_ienc_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == EXPECTED_COUNT
        and candidate.get("source_population_complete") is True
        and candidate.get("geometry_valid_count") == EXPECTED_COUNT - EXPECTED_INVALID_GEOMETRY_COUNT
        and candidate.get("geometry_invalid_count") == EXPECTED_INVALID_GEOMETRY_COUNT
        and candidate.get("geometry_empty_count") == EXPECTED_EMPTY_GEOMETRY_COUNT
        and candidate.get("feature_id_null_count") == EXPECTED_FEATURE_ID_NULL_COUNT
        and candidate.get("geometry_repaired") is False
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "geometry_valid_count": candidate.get("geometry_valid_count", "missing"),
            "geometry_invalid_count": candidate.get("geometry_invalid_count", "missing"),
            "geometry_empty_count": candidate.get("geometry_empty_count", "missing"),
            "geometry_repaired": candidate.get("geometry_repaired", "missing"),
            "feature_id_null_count": candidate.get("feature_id_null_count", "missing"),
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
            "source_record_sha256": candidate.get("source_record_sha256", "missing"),
            "source_geometry_sha256": candidate.get("source_geometry_sha256", "missing"),
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


@asset_check(asset=land_surface_areas_usace_ienc_candidate, blocking=False)
def land_surface_areas_usace_ienc_geometry_quality(candidate: dict) -> AssetCheckResult:
    """Source geometry quality is not accepted while the publisher's layer is invalid.

    The captured source currently contains one self-intersecting ring
    (OBJECTID 12570). It is preserved byte-for-byte and reported here; it is
    never repaired. Geometry-quality acceptance, like every other release
    decision, stays false until an explicit acceptance process grants it.
    """
    invalid = candidate.get("geometry_invalid_count")
    return AssetCheckResult(
        passed=False,
        description=(
            "Source geometry quality acceptance is pending: the live IENC "
            "LAND_AREA layer publishes invalid topology (self-intersecting ring) "
            "that this candidate preserves unmodified."
        ),
        metadata={
            "geometry_invalid_count": invalid if type(invalid) is int else -1,
            "invalid_geometry_object_ids": candidate.get("invalid_geometry_object_ids", []),
            "geometry_repaired": False,
            "geometry_quality_accepted": False,
            "rights_accepted": False,
            "release_accepted": False,
        },
    )


land_surface_areas_usace_ienc_draft_job = define_asset_job(
    "land_surface_areas_usace_ienc_draft_refresh",
    selection=AssetSelection.assets(land_surface_areas_usace_ienc_candidate),
)
land_surface_areas_usace_ienc_draft_schedule = ScheduleDefinition(
    name="land_surface_areas_usace_ienc_draft_refresh_schedule",
    job=land_surface_areas_usace_ienc_draft_job,
    cron_schedule="0 8 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)


INGEST_ASSETS = [land_surface_areas_usace_ienc_candidate]
ASSET_CHECKS = [
    land_surface_areas_usace_ienc_complete,
    land_surface_areas_usace_ienc_geometry_quality,
]
JOBS = [land_surface_areas_usace_ienc_draft_job]
SCHEDULES = [land_surface_areas_usace_ienc_draft_schedule]
