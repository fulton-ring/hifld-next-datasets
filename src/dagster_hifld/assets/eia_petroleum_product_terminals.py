"""Explicit stopped-refresh asset for the pinned historical EIA petroleum product terminals layer."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.eia_petroleum_product_terminals import (
    DATASET_SLUG, EXPECTED_COUNT, EXPECTED_ZIP_BYTES, EXPECTED_ZIP_SHA256, FILE_SLUG, SOURCE_RELEASE,
    capture_eia_petroleum_product_terminals,
)


class EiaPetroleumProductTerminalsConfig(Config):
    capture_directory: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="energy", compute_kind="http",
       description="Pinned historical EIA 'Petroleum Product Terminals' shapefile (1,474 operable bulk "
                   "terminals as of September 2021, attributes from EIA-815) from the producer's own "
                   "eia.gov distribution. Historical unapproved candidate; inventory equivalence, "
                   "currentness and rights remain unresolved.")
def eia_petroleum_product_terminals(context, config: EiaPetroleumProductTerminalsConfig,
                                   staging_storage: StagingStorageResource) -> dict:
    result = capture_eia_petroleum_product_terminals(
        staging_storage, version=f"v{SOURCE_RELEASE.replace('-', '')}",
        capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=eia_petroleum_product_terminals, blocking=True)
def eia_petroleum_product_terminals_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("source_population_complete") is True
              and candidate.get("capture_directory") is not None)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_record_sha256": candidate.get("source_record_sha256", "missing"),
        "source_geometry_sha256": candidate.get("source_geometry_sha256", "missing"),
        "source_archive_bytes": EXPECTED_ZIP_BYTES,
        "source_archive_sha256": EXPECTED_ZIP_SHA256,
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "release_accepted": False,
    })


eia_petroleum_product_terminals_draft_job = define_asset_job("eia_petroleum_product_terminals_draft_refresh",
                                                             selection=AssetSelection.assets(eia_petroleum_product_terminals))
eia_petroleum_product_terminals_draft_schedule = ScheduleDefinition(name="eia_petroleum_product_terminals_draft_refresh_schedule",
                                                                    job=eia_petroleum_product_terminals_draft_job,
                                                                    cron_schedule="0 8 1 * *",
                                                                    default_status=DefaultScheduleStatus.STOPPED)

INGEST_ASSETS = [eia_petroleum_product_terminals]
ASSET_CHECKS = [eia_petroleum_product_terminals_complete]
JOBS = [eia_petroleum_product_terminals_draft_job]
SCHEDULES = [eia_petroleum_product_terminals_draft_schedule]
