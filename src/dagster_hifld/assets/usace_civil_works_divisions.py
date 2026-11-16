"""Explicit stopped-refresh Dagster asset for the USACE Civil Works Divisions."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.usace_civil_works_divisions import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_usace_civil_works_divisions,
)


class UsaceCivilWorksDivisionsConfig(Config):
    capture_directory: str = ""
    offline_capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "USACE-hosted ArcGIS Online usace_cw_divisions FeatureServer layer 0 'USACE "
        "Divisions' (8 division polygons observed 2026-11-14, untagged geographic "
        "wkid 4140): the AGOL item 4cdab8820d7a4f58aaa003be63f059ac owned by USACE "
        "account usace_crrel_als in organization n1YM8pTrFmm7L4hs, whose item "
        "description matches the HIFLD inventory entry verbatim and which the HIFLD "
        "Open Data portal item c9055810a0ed453eb8f1b3f87122d2d3 registers as its "
        "source service. Unapproved candidate; freshness, baseline, schema/ID "
        "contract, coverage, rights and release decisions remain pending; the "
        "producer can republish the hosted service and resequence OBJECTIDs, which "
        "the bracketed capture refuses."
    ),
)
def usace_civil_works_divisions(context, config: UsaceCivilWorksDivisionsConfig,
                                staging_storage: StagingStorageResource) -> dict:
    result = capture_usace_civil_works_divisions(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
        offline_capture_directory=config.offline_capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usace_civil_works_divisions, blocking=True)
def usace_civil_works_divisions_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("feature_count", 0)
    passed = (isinstance(count, int) and count > 0
              and (EXPECTED_COUNT is None or count == EXPECTED_COUNT)
              and bool(candidate.get("candidate_ready"))
              and not candidate.get("release_accepted", False))
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": count,
        "null_geometry_count": candidate.get("null_geometry_count"),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_release_accepted": False,
        "source_freshness_accepted": False,
        "baseline_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "coverage_accepted": False,
        "rights_accepted": False,
        "source_authority_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    })


usace_civil_works_divisions_draft_job = define_asset_job(
    "usace_civil_works_divisions_draft_refresh",
    selection=AssetSelection.assets(usace_civil_works_divisions),
)
usace_civil_works_divisions_draft_schedule = ScheduleDefinition(
    name="usace_civil_works_divisions_draft_refresh_schedule",
    job=usace_civil_works_divisions_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [usace_civil_works_divisions]
ASSET_CHECKS = [usace_civil_works_divisions_complete]
JOBS = [usace_civil_works_divisions_draft_job]
SCHEDULES = [usace_civil_works_divisions_draft_schedule]
