"""Explicit stopped-refresh Dagster asset for FCC ULS BRS/EBS transmitters."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.brs_ebs_transmitters import (
    DATASET_SLUG, EXPECTED_ACTIVE_LOCATIONS, FILE_SLUG, capture_brs_ebs_transmitters,
)


class BrsEbsTransmittersConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="fcc",
    compute_kind="http",
    description=(
        "FCC ULS weekly public access complete MDS/ITFS licenses file l_mdsitfs.zip: location records of active "
        "BRS (BR) and EBS (ED) licenses under the ULS-derived tower column contract (4669 active locations "
        "observed). BRS/EBS licensing is geographic (47 CFR 27.1208), so this is the FCC's filed-site population "
        "for the 2.5 GHz band, not a census of physical transmitters. Unapproved candidate; baseline, contract, "
        "coverage, rights, and release decisions remain pending."
    ),
)
def brs_ebs_transmitters(context, config: BrsEbsTransmittersConfig,
                         staging_storage: StagingStorageResource) -> dict:
    result = capture_brs_ebs_transmitters(
        staging_storage, version=build_version_id(context),
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=brs_ebs_transmitters, blocking=True)
def brs_ebs_transmitters_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_ACTIVE_LOCATIONS
              and candidate.get("source_population_complete") is True)
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "source_id_sha256": candidate.get("source_id_sha256", "missing"),
        "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
        "source_capture_evidence_sha256": candidate.get("source_capture_evidence_sha256", "missing"),
        "source_coverage_scope": candidate.get("source_coverage_scope", "missing"),
        "baseline_comparison_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    })


brs_ebs_transmitters_draft_job = define_asset_job(
    "brs_ebs_transmitters_draft_refresh",
    selection=AssetSelection.assets(brs_ebs_transmitters),
)
brs_ebs_transmitters_draft_schedule = ScheduleDefinition(
    name="brs_ebs_transmitters_draft_refresh_schedule",
    job=brs_ebs_transmitters_draft_job,
    cron_schedule="0 9 * * 1",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [brs_ebs_transmitters]
ASSET_CHECKS = [brs_ebs_transmitters_complete]
JOBS = [brs_ebs_transmitters_draft_job]
SCHEDULES = [brs_ebs_transmitters_draft_schedule]
