"""Explicit unapproved ingest asset for the FHWA National Tunnel Inventory."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fhwa_tunnel_inventory import (
    DATASET_SLUG, FILE_SLUG, EXPECTED_COUNT, capture_fhwa_tunnel_inventory,
)


class FhwaTunnelInventoryConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="fhwa",
    compute_kind="http",
    description=(
        "Exact FHWA National Tunnel Inventory point layer distributed by BTS NTAD. "
        "Draft only: source publication and compilation dates conflict; release remains blocked."
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
def fhwa_national_tunnel_inventory_release_gate(candidate: dict) -> AssetCheckResult:
    return AssetCheckResult(
        passed=False,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0) == EXPECTED_COUNT,
            "source_release_accepted": False,
            "source_edition_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "quality_check_passed": False,
            "release_accepted": False,
        },
        description="Blocked pending FHWA confirmation of source edition chronology and explicit release approvals.",
    )


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
