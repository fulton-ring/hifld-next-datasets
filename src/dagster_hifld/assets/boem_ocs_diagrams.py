"""Draft BOEM OCS protraction footprint acquisition assets."""
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.boem_ocs_diagrams import (
    DATASET_SLUG, FILE_SLUG, REGIONS, capture_boem_ocs_diagrams,
)


class BoemOcsDiagramsConfig(Config):
    capture_directory: str = ""
    offline_capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="boem",
    compute_kind="http",
    description=(
        "BOEM_GeoSEAS national OPD footprint layer; captures the native Esri "
        "JSON pages and stages an EPSG:3857 GeoParquet candidate. Gulf-only "
        "Data Center downloads are not used. Candidate source dates, schema, "
        "ID mapping and release are unapproved."
    ),
)
def boem_ocs_protraction_diagrams(context, config: BoemOcsDiagramsConfig,
                                  staging_storage: StagingStorageResource) -> dict:
    result = capture_boem_ocs_diagrams(
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


@asset_check(asset=boem_ocs_protraction_diagrams, blocking=True)
def boem_ocs_protraction_diagrams_complete(candidate: dict) -> AssetCheckResult:
    regions = candidate.get("region_counts", {})
    count = candidate.get("feature_count", 0)
    passed = (isinstance(count, int) and count > 0 and isinstance(regions, dict)
              and set(regions) == set(REGIONS) and sum(regions.values()) == count
              and all(isinstance(value, int) and value > 0 for value in regions.values()))
    return AssetCheckResult(
        passed=passed,
        metadata={
            "complete_source_population": count,
            "region_counts": regions,
            "source_release_accepted": False,
            "source_freshness_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "rights_accepted": False,
            "mapping_approved": False,
            "release_accepted": False,
        },
    )


boem_ocs_diagrams_draft_job = define_asset_job(
    "boem_ocs_diagrams_draft_refresh",
    selection=AssetSelection.assets(boem_ocs_protraction_diagrams),
)
boem_ocs_diagrams_draft_schedule = ScheduleDefinition(
    name="boem_ocs_diagrams_draft_refresh_schedule",
    job=boem_ocs_diagrams_draft_job,
    cron_schedule="0 8 1 * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)
