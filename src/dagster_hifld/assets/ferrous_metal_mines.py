"""Draft-only USGS National Atlas Ferrous Metal Mines source asset with explicit gates."""
from pathlib import Path

from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset, asset_check,
                     define_asset_job)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.ferrous_metal_mines import stage_ferrous_metal_mines


class FerrousMetalMinesConfig(Config):
    archive_path: str = ""
    archive_receipt_path: str = ""
    metadata_path: str = ""
    metadata_receipt_path: str = ""


@asset(
    key=AssetKey(["ingest", "ferrous-metal-mines", "ferrous-metal-mines"]),
    group_name="usgs",
    compute_kind="local",
    description=(
        "Lossless decode of an explicitly supplied preserved capture of the retired USGS National "
        "Atlas Ferrous Metal Mines producer archive (minop3x020, December 2005), with the surviving "
        "producer FGDC metadata captured from the USGS legacy WAF. No default fetch, substitute IDs, "
        "or release acceptance; schema, baseline, coverage, rights, and release decisions pending."
    ),
)
def ferrous_metal_mines_candidate(context, config: FerrousMetalMinesConfig,
                                  staging_storage: StagingStorageResource) -> dict:
    result = stage_ferrous_metal_mines(
        staging_storage,
        archive_path=Path(config.archive_path) if config.archive_path else None,
        archive_receipt_path=Path(config.archive_receipt_path) if config.archive_receipt_path else None,
        metadata_path=Path(config.metadata_path) if config.metadata_path else None,
        metadata_receipt_path=Path(config.metadata_receipt_path) if config.metadata_receipt_path else None,
    )
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(result["dataset_slug"], result["file_slug"], result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=ferrous_metal_mines_candidate, blocking=True)
def ferrous_metal_mines_population_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("source_population_complete") is True
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": candidate.get("feature_count", 0),
        "source_record_sha256": candidate.get("source_record_sha256", "missing"),
        "accepted": candidate.get("accepted", True),
    })


ferrous_metal_mines_draft_job = define_asset_job(
    "ferrous_metal_mines_draft_refresh",
    selection=AssetSelection.assets(ferrous_metal_mines_candidate),
)
ferrous_metal_mines_draft_schedule = ScheduleDefinition(
    name="ferrous_metal_mines_draft_refresh_schedule",
    job=ferrous_metal_mines_draft_job,
    cron_schedule="0 9 1,15 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
