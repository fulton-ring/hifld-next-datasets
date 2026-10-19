"""Dagster draft assets for the static EIA pipeline archive."""
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
from dagster_hifld.sources.eia_natural_gas_pipelines import (
    DATASET_SLUG,
    FILE_SLUG,
    capture_eia_natural_gas_pipelines,
)


class EiaNaturalGasPipelinesConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="eia",
    compute_kind="http",
    description=(
        "Captures EIA's January 2020 native shapefile ZIP and stages a full, "
        "unapproved GeoParquet candidate. The static 2020 archive does not "
        "establish currentness; its refresh schedule is stopped."
    ),
)
def eia_natural_gas_interstate_intrastate_pipelines(
    context, config: EiaNaturalGasPipelinesConfig, staging_storage: StagingStorageResource
) -> dict:
    result = capture_eia_natural_gas_pipelines(
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


@asset_check(asset=eia_natural_gas_interstate_intrastate_pipelines, blocking=True)
def eia_natural_gas_interstate_intrastate_pipelines_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 32961
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_id_field": "none declared in source shapefile",
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


eia_natural_gas_pipelines_draft_job = define_asset_job(
    "eia_natural_gas_pipelines_draft_refresh",
    selection=AssetSelection.assets(eia_natural_gas_interstate_intrastate_pipelines),
)
eia_natural_gas_pipelines_draft_schedule = ScheduleDefinition(
    name="eia_natural_gas_pipelines_draft_refresh_schedule",
    job=eia_natural_gas_pipelines_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
