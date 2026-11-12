"""Official USACE Principal Ports successor-layer draft asset."""

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
from dagster_hifld.sources.principal_ports import (
    DATASET_SLUG,
    FILE_SLUG,
    capture_principal_ports,
)


class PrincipalPortsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=(
        "Complete observed 150-feature official USACE Principal Ports CY2023 polygon layer. "
        "Candidate successor only; historical HIFLD CY2022 geometry and schema continuity, "
        "release date, freshness, rights, and contract remain unapproved."
    ),
)
def principal_ports_usace(context, config: PrincipalPortsConfig,
                          staging_storage: StagingStorageResource) -> dict:
    result = capture_principal_ports(
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


@asset_check(asset=principal_ports_usace, blocking=True)
def principal_ports_source_complete(candidate: dict) -> AssetCheckResult:
    passed = (
        candidate.get("feature_count") == 150
        and candidate.get("source_population_complete") is True
    )
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_port_roster_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_coverage_extent": str(candidate.get("source_coverage_extent", "missing")),
        "unrepaired_invalid_source_geometries": candidate.get("source_invalid_geometry_count_without_repair", "unknown"),
        "baseline_comparison_accepted": False,
        "source_release_accepted": False,
        "source_freshness_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "coverage_accepted": False,
        "rights_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    })


principal_ports_draft_job = define_asset_job(
    "principal_ports_draft_refresh",
    selection=AssetSelection.assets(principal_ports_usace),
)
principal_ports_draft_schedule = ScheduleDefinition(
    name="principal_ports_draft_refresh_schedule",
    job=principal_ports_draft_job,
    cron_schedule="0 8 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
