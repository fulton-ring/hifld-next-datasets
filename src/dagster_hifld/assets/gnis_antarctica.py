"""Draft Dagster assets for the USGS GNIS Antarctica named-features capture."""
from dagster import AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus, ScheduleDefinition, asset, asset_check, define_asset_job

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.gnis_antarctica import (
    DATASET_SLUG,
    EXPECTED_ALLNAMES_COUNT,
    EXPECTED_COUNT,
    FILE_SLUG,
    capture_gnis_antarctica,
)


class GnisAntarcticaConfig(Config):
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http",
    description=(
        "GNIS Full Model Antarctica named-features point layer from the producer's own The "
        "National Map staged product, cross-verified against the producer's pipe-delimited "
        "product. Candidate extraction only; release date, freshness, rights, and "
        "schema/ID acceptance remain pending."
    ),
)
def usgs_gnis_antarctica(context, config: GnisAntarcticaConfig,
                         staging_storage: StagingStorageResource) -> dict:
    result = capture_gnis_antarctica(
        staging_storage,
        version="v20261102",
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=usgs_gnis_antarctica, blocking=True)
def usgs_gnis_antarctica_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("feature_count") == EXPECTED_COUNT
              and candidate.get("variant_name_count") == EXPECTED_ALLNAMES_COUNT
              and candidate.get("candidate_ready") is not None)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": candidate.get("feature_count", 0),
            "source_variant_name_count": candidate.get("variant_name_count", 0),
            "cross_format_verified": True,
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


gnis_antarctica_draft_job = define_asset_job(
    "gnis_antarctica_draft_refresh",
    selection=AssetSelection.assets(usgs_gnis_antarctica),
)
gnis_antarctica_draft_schedule = ScheduleDefinition(
    name="gnis_antarctica_draft_refresh_schedule",
    job=gnis_antarctica_draft_job,
    cron_schedule="0 6 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [usgs_gnis_antarctica]
ASSET_CHECKS = [usgs_gnis_antarctica_complete]
JOBS = [gnis_antarctica_draft_job]
SCHEDULES = [gnis_antarctica_draft_schedule]
