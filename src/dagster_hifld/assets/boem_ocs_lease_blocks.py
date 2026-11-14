"""Draft BOEM/BSEE OCS lease-block acquisition assets."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.boem_ocs_lease_blocks import (
    DATASET_SLUG, EXPECTED_COUNT, FILE_SLUG, capture_boem_ocs_lease_blocks,
)


class BoemOcsLeaseBlocksConfig(Config):
    capture_directory: str = ""
    offline_capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="boem",
    compute_kind="http",
    description=(
        "BOEM/BSEE national Marine Mineral Cadastre 'BOEM OCS Lease Blocks' "
        "layer (id 11, 278,438 source features observed 2026-11-13) on BOEM's "
        "own gis.boem.gov server as the maintained first-party publication for "
        "the HIFLD inventory entry outer-continental-shelf-ocs-lease-blocks-1 "
        "(the crosswalk's GeoPlatform GeoSeas host no longer resolves). "
        "Captures native Esri JSON pages and stages an EPSG:4269 GeoParquet "
        "polygon candidate; source release date, schema/ID contracts, rights "
        "and release are unapproved."
    ),
)
def boem_ocs_lease_blocks(context, config: BoemOcsLeaseBlocksConfig,
                          staging_storage: StagingStorageResource) -> dict:
    result = capture_boem_ocs_lease_blocks(
        staging_storage,
        capture_directory=config.capture_directory or None,
        offline_capture_directory=config.offline_capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=boem_ocs_lease_blocks, blocking=True)
def boem_ocs_lease_blocks_complete(candidate: dict) -> AssetCheckResult:
    count = candidate.get("feature_count", 0)
    passed = (isinstance(count, int) and count > 0
              and (EXPECTED_COUNT is None or count == EXPECTED_COUNT)
              and bool(candidate.get("candidate_ready"))
              and not candidate.get("release_accepted", False))
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": count,
            "unique_block_key_count": candidate.get("unique_block_key_count"),
            "null_geometry_count": candidate.get("null_geometry_count"),
            "source_id_sha256": candidate.get("source_id_sha256", "missing"),
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "source_authority_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


boem_ocs_lease_blocks_draft_job = define_asset_job(
    "boem_ocs_lease_blocks_draft_refresh",
    selection=AssetSelection.assets(boem_ocs_lease_blocks),
)
boem_ocs_lease_blocks_draft_schedule = ScheduleDefinition(
    name="boem_ocs_lease_blocks_draft_refresh_schedule",
    job=boem_ocs_lease_blocks_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [boem_ocs_lease_blocks]
ASSET_CHECKS = [boem_ocs_lease_blocks_complete]
JOBS = [boem_ocs_lease_blocks_draft_job]
SCHEDULES = [boem_ocs_lease_blocks_draft_schedule]
