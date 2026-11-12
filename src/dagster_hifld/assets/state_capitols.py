"""Draft-only USGS State Capitols source asset with explicit quality gates."""
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset, asset_check,
                     define_asset_job)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.state_capitols import (
    COUNT,
    capture_state_capitols,
)

DATASET_SLUG = FILE_SLUG = "state-capitols"


class StateCapitolsConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "Complete native EPSG:3857 capture of the official USGS National Map Structures "
        "State Capitols layer, staged in the standard versioned namespace. HIFLD archive "
        "equivalence, source edition, schema, rights, coverage, and release decisions remain pending."
    ),
)
def state_capitols_usgs(context, config: StateCapitolsConfig,
                        staging_storage: StagingStorageResource) -> dict:
    result = capture_state_capitols(staging_storage, version=build_version_id(context),
                                    capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=state_capitols_usgs, blocking=True)
def state_capitols_population_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("source_population_complete") is True
              and candidate.get("feature_count") == COUNT)
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": candidate.get("feature_count", 0),
        "id_sha256": candidate.get("source_id_sha256", "missing"),
        "record_sha256": candidate.get("source_record_sha256", "missing"),
    })


state_capitols_draft_job = define_asset_job(
    "state_capitols_draft_refresh",
    selection=AssetSelection.assets(state_capitols_usgs),
)
state_capitols_draft_schedule = ScheduleDefinition(
    name="state_capitols_draft_refresh_schedule",
    job=state_capitols_draft_job,
    cron_schedule="0 10 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)