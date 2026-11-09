"""Explicit stopped-refresh Dagster asset for the USFWS Corporate Master Table locations."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fws_cmt_locations import (
    EXPECTED_COUNT, EXPECTED_GEOMETRY_ATTRIBUTE_DISAGREEMENT_COUNT,
    EXPECTED_NULL_COORDINATE_COUNT, DATASET_SLUG, FILE_SLUG,
    capture_fws_cmt_locations,
)


class FwsCmtLocationsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="fws",
    compute_kind="http",
    description=(
        "Official FWS_HQ_BA_CMT_Locations FeatureServer layer 0 (1,816 rows observed). "
        "Preserves native Web Mercator point geometry and CMT latitude/longitude attributes "
        "without reconciliation. Unapproved faithful candidate; the trusted source registry has "
        "no USFWS entry; source, baseline, contract, coverage, rights, and release decisions "
        "remain pending."
    ),
)
def fws_cmt_locations(context, config: FwsCmtLocationsConfig,
                      staging_storage: StagingStorageResource) -> dict:
    result = capture_fws_cmt_locations(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=fws_cmt_locations, blocking=True)
def fws_cmt_locations_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("source_population_complete") is True
              and candidate.get("null_coordinate_count") == EXPECTED_NULL_COORDINATE_COUNT
              and candidate.get("geometry_attribute_disagreement_count")
              == EXPECTED_GEOMETRY_ATTRIBUTE_DISAGREEMENT_COUNT)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "null_coordinate_count": candidate.get("null_coordinate_count", "missing"),
        "geometry_attribute_disagreement_count":
            candidate.get("geometry_attribute_disagreement_count", "missing"),
        "org_code_leading_zero_count": candidate.get("org_code_leading_zero_count", "missing"),
        "source_registry_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "baseline_accepted": False,
    })


fws_cmt_locations_draft_job = define_asset_job(
    "fws_cmt_locations_draft_refresh",
    selection=AssetSelection.assets(fws_cmt_locations),
)
fws_cmt_locations_draft_schedule = ScheduleDefinition(
    name="fws_cmt_locations_draft_refresh_schedule",
    job=fws_cmt_locations_draft_job,
    cron_schedule="0 8 1,15 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
