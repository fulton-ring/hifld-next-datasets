"""Explicit unapproved ingest asset for the FHWA National Tunnel Inventory."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fhwa_tunnel_inventory import (
    DATASET_SLUG, FILE_SLUG, capture_fhwa_tunnel_inventory,
)


class FhwaTunnelInventoryConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="fhwa",
    compute_kind="http",
    description=(
        "Newest FHWA National Tunnel Inventory annual XML release. "
        "Draft only: source, schema, ID, rights, mapping and release acceptance remain pending."
    ),
)
def fhwa_national_tunnel_inventory(context, config: FhwaTunnelInventoryConfig,
                                   staging_storage: StagingStorageResource) -> dict:
    result = capture_fhwa_tunnel_inventory(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=fhwa_national_tunnel_inventory, blocking=True)
def fhwa_national_tunnel_inventory_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("feature_count")
    passed = (type(count) is int and count > 0
              and candidate.get("source_population_complete") is True)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": count if count is not None else "missing",
        "source_population_complete": candidate.get("source_population_complete", False),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_release_year": candidate.get("source_release_year", "missing"),
        "source_data_url": candidate.get("source_data_url", "missing"),
        "source_release_accepted": False,
        "source_edition_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "rights_accepted": False,
        "mapping_approved": False,
        "quality_check_passed": False,
        "release_accepted": False,
    })


@asset_check(asset=fhwa_national_tunnel_inventory, blocking=False)
def fhwa_national_tunnel_inventory_drift(candidate: dict) -> AssetCheckResult:
    # No independently approved release is wired for this source. Development
    # captures and removed code pins must never masquerade as approved baselines.
    return approved_baseline_drift(candidate)


fhwa_tunnel_inventory_draft_job = define_asset_job(
    "fhwa_tunnel_inventory_draft_refresh",
    selection=AssetSelection.assets(fhwa_national_tunnel_inventory),
)
fhwa_tunnel_inventory_draft_schedule = ScheduleDefinition(
    name="fhwa_tunnel_inventory_draft_refresh_schedule",
    job=fhwa_tunnel_inventory_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [fhwa_national_tunnel_inventory]
ASSET_CHECKS = [fhwa_national_tunnel_inventory_complete, fhwa_national_tunnel_inventory_drift]
JOBS = [fhwa_tunnel_inventory_draft_job]
SCHEDULES = [fhwa_tunnel_inventory_draft_schedule]
