"""EPA FRS TSCA layer draft assets."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_tsca import DATASET_SLUG, FILE_SLUG, capture_epa_tsca


class EpaTscaConfig(Config):
    capture_directory: str = ""
    replay_capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="epa",
    compute_kind="http",
    description=(
        "Exact EPA_GEO FRS TSCA-associated point layer. Candidate only; source release date and currentness remain unresolved."
    ),
)
def epa_frs_toxic_substances_control_act_tsca(
    context, config: EpaTscaConfig, staging_storage: StagingStorageResource
) -> dict:
    version = build_version_id(context)
    result = capture_epa_tsca(
        staging_storage, version=version,
        capture_directory=config.capture_directory or None,
        replay_capture_directory=config.replay_capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_frs_toxic_substances_control_act_tsca, blocking=True)
def epa_frs_tsca_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 26454 and candidate.get("source_null_geometry_count") == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


epa_tsca_draft_job = define_asset_job(
    "epa_tsca_draft_refresh",
    selection=AssetSelection.assets(epa_frs_toxic_substances_control_act_tsca),
)
epa_tsca_draft_schedule = ScheduleDefinition(
    name="epa_tsca_draft_refresh_schedule",
    job=epa_tsca_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
