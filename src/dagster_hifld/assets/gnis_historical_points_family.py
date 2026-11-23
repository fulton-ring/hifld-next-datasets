"""Shared Dagster definitions for the USGS GNIS historical point layers.

The three historical point layers share one ingest asset shape: an http
capture of the layer, a blocking completeness check, the approved-baseline
drift check, a draft job and a stopped draft schedule. ``build_gnis_historical_
points_assets`` builds them for one layer module; the per-layer module keeps
the public names, the INGEST_ASSETS/ASSET_CHECKS/JOBS/SCHEDULES registration
lists and the layer-specific description.
"""

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

from dagster_hifld.capture_completeness import register_approved_baseline_drift
from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource


class GnisHistoricalPointsConfig(Config):
    capture_directory: str = ""


def build_gnis_historical_points_assets(source_module, description: str):
    """Build (asset, complete check, drift check, job, schedule) for one layer.

    The asset name is the source module stem; the asset key is
    ``ingest/<dataset_slug>/<file_slug>`` from the layer module.
    """
    name = source_module.__name__.rsplit(".", 1)[-1]
    capture = getattr(source_module, f"capture_{name}")

    def ingest_asset(
            context,
            config: GnisHistoricalPointsConfig,
            staging_storage: StagingStorageResource,
    ) -> dict:
        result = capture(
            staging_storage,
            version=build_version_id(context),
            capture_directory=config.capture_directory or None,
        )
        context.instance.add_dynamic_partitions(
            PUBLISH_PARTITION_NAME,
            [build_publish_partition_key(source_module.DATASET_SLUG,
                                         source_module.FILE_SLUG,
                                         result["version"])],
        )
        context.add_output_metadata(result)
        return result

    ingest_asset.__name__ = name
    ingest_asset = asset(
        key=AssetKey(["ingest", source_module.DATASET_SLUG, source_module.FILE_SLUG]),
        group_name="usgs",
        compute_kind="http",
        description=description,
    )(ingest_asset)

    def complete_check(candidate: dict) -> AssetCheckResult:
        passed = (
            candidate.get("capture_complete") is True
            and type(candidate.get("feature_count")) is int
            and candidate["feature_count"] > 0
        )
        return AssetCheckResult(
            passed=passed,
            metadata={
                "complete_source_population": candidate.get("feature_count", 0),
                "source_id_sha256": candidate.get("source_id_sha256", "missing"),
                "source_schema_sha256": candidate.get("source_schema_sha256", "missing"),
                "source_release_accepted": False,
                "source_freshness_accepted": False,
                "succession_accepted": False,
                "schema_contract_accepted": False,
                "id_contract_accepted": False,
                "coverage_accepted": False,
                "rights_accepted": False,
                "mapping_approved": False,
                "release_accepted": False,
            },
        )

    complete_check.__name__ = f"{name}_complete"
    complete_check = asset_check(asset=ingest_asset, blocking=True)(complete_check)
    drift_check = register_approved_baseline_drift(
        ingest_asset, f"{name}_approved_baseline_drift")
    job = define_asset_job(
        f"{name}_draft_refresh",
        selection=AssetSelection.assets(ingest_asset),
    )
    schedule = ScheduleDefinition(
        name=f"{name}_draft_refresh_schedule",
        job=job,
        cron_schedule="0 9 15 2,4,6,8,10,12 *",
        execution_timezone="America/New_York",
        default_status=DefaultScheduleStatus.STOPPED,
    )
    return ingest_asset, complete_check, drift_check, job, schedule
