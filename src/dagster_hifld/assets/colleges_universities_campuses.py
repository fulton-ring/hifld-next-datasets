"""NCES reporting-institution points as the maintained Colleges and Universities
Campuses successor.

The retired 2022 ORNL product carried campus polygons. NCES EDGE publishes
current postsecondary reporting-institution points, so this asset derives a
byte-faithful alias of the existing Colleges and Universities NCES candidate
instead of fetching the same release again. It carries points only and no campus
outlines.
"""
from dagster import (
    AssetCheckResult,
    AssetIn,
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.candidate_alias import stage_candidate_alias
from dagster_hifld.sources.colleges_universities import SLUG as SOURCE_SLUG

DATASET_SLUG = "colleges-and-universities-campuses"
FILE_SLUG = DATASET_SLUG
SOURCE_READY_MARKER = "metadata/colleges_candidate_ready.json"
SOURCE_AUDIT_MARKER = "metadata/colleges_source_audit.json"
TITLE = (
    "Colleges and Universities Campuses - NCES EDGE postsecondary "
    "reporting-institution points (successor to the 2022 ORNL campus polygons; "
    "no campus outlines)"
)
DESCRIPTION = (
    "Unapproved NCES EDGE postsecondary reporting-institution points, derived "
    "from the Colleges and Universities ingest instead of a second fetch. "
    "Successor to the retired 2022 ORNL campus polygons; points only, no campus "
    "outlines, no ORNL enrichment, and no approved historical ID mapping."
)


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    ins={
        "nces_colleges_universities": AssetIn(
            key=AssetKey(["ingest", SOURCE_SLUG, SOURCE_SLUG])
        )
    },
    group_name="department_of_education",
    compute_kind="local",
    description=DESCRIPTION,
)
def nces_colleges_universities_campuses(
    context,
    nces_colleges_universities: dict,
    staging_storage: StagingStorageResource,
) -> dict:
    version = nces_colleges_universities["version"]
    result = stage_candidate_alias(
        staging_storage,
        source_dataset_slug=SOURCE_SLUG,
        source_file_slug=SOURCE_SLUG,
        source_version=version,
        source_ready_marker=SOURCE_READY_MARKER,
        target_dataset_slug=DATASET_SLUG,
        target_file_slug=FILE_SLUG,
        target_version=version,
        title=TITLE,
        transformation={
            "operation": "derive_dataset_alias",
            "parameters": {
                "source_dataset_slug": SOURCE_SLUG,
                "source_file_slug": SOURCE_SLUG,
                "source_version": version,
                "note": (
                    "Same staged NCES reporting-institution points; no second "
                    "source fetch. The successor is points only and carries no "
                    "campus outlines."
                ),
            },
        },
        source_audit_marker=SOURCE_AUDIT_MARKER,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    output = {**nces_colleges_universities, **result}
    context.add_output_metadata({
        "dataset_slug": result["dataset_slug"],
        "file_slug": result["file_slug"],
        "version": result["version"],
        "feature_count": output.get("feature_count"),
        "reused": result["reused"],
        "source_dataset_slug": result["source_dataset_slug"],
        "source_version": result["source_version"],
        "prebuilt_parquet_sha256": result["prebuilt_parquet_sha256"],
    })
    return output


@asset_check(asset=nces_colleges_universities_campuses, blocking=True)
def nces_colleges_universities_campuses_complete(candidate: dict) -> AssetCheckResult:
    audit = candidate.get("source_audit") or {}
    count = candidate.get("feature_count")
    passed = (
        isinstance(count, int)
        and count > 0
        and audit.get("feature_count") == count
        and audit.get("points") == count
        and audit.get("crs") == "EPSG:4326"
        and audit.get("invalid_geometries") == 0
        and candidate.get("prebuilt_parquet_sha256")
        == candidate.get("source_prebuilt_parquet_sha256")
        == candidate.get("manifest_prebuilt_parquet_sha256")
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "feature_count": count if isinstance(count, int) else 0,
            "source_feature_count": audit.get("feature_count", 0),
            "geometry_type": "Point" if audit.get("points") == count else "unexpected",
            "crs": audit.get("crs", "missing"),
            "invalid_geometry_count": audit.get("invalid_geometries", -1),
            "source_relationship": "changed successor: NCES points, no ORNL campus outlines",
        },
    )


colleges_universities_campuses_job = define_asset_job(
    "colleges_universities_campuses_source_integrity",
    selection=AssetSelection.assets(nces_colleges_universities_campuses).upstream(),
)
colleges_universities_campuses_schedule = ScheduleDefinition(
    name="colleges_universities_campuses_source_integrity_monthly",
    job=colleges_universities_campuses_job,
    cron_schedule="0 9 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [nces_colleges_universities_campuses]
ASSET_CHECKS = [nces_colleges_universities_campuses_complete]
JOBS = [colleges_universities_campuses_job]
SCHEDULES = [colleges_universities_campuses_schedule]
