"""Shared asset, check, job and schedule construction for the BTS NTAD
Intermodal Freight Facilities ingest layers.

The per-layer thin modules call ``build_bts_intermodal_layer`` and register the
returned definitions in their module-level lists; the checks read the layer's
expected count from its frozen spec at call time.
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

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource


class BtsIntermodalFamilyConfig(Config):
    capture_directory: str = ""


def build_bts_intermodal_layer(source_module, capture, *, asset_name, check_name,
                               job_name, schedule_name, expected_count):
    """Build one layer's ingest asset, blocking check, draft job and stopped schedule.

    ``expected_count`` is a zero-argument callable resolved at check time so the
    thin module's patchable ``EXPECTED_SOURCE_COUNT`` global stays the source of
    truth, as in the pre-family asset modules.
    """
    spec = source_module.SPEC

    def compute(context, config: BtsIntermodalFamilyConfig,
                staging_storage: StagingStorageResource) -> dict:
        result = capture(staging_storage, capture_directory=config.capture_directory or None)
        context.instance.add_dynamic_partitions(
            PUBLISH_PARTITION_NAME,
            [build_publish_partition_key(
                source_module.SPEC.dataset_slug, source_module.SPEC.file_slug,
                result["version"])],
        )
        context.add_output_metadata(result)
        return result

    compute.__name__ = asset_name
    asset_def = asset(
        key=AssetKey(["ingest", spec.dataset_slug, spec.file_slug]),
        group_name="bts",
        compute_kind="http",
        description=(
            f"Exact BTS-maintained NTAD {spec.description_token} point layer. "
            "The current schema, ID, source vintage, and publication approvals "
            "remain pending."
        ),
    )(compute)

    def check(candidate: dict) -> AssetCheckResult:
        passed = (
            candidate.get("source_population_complete") is True
            and             candidate.get("feature_count") == expected_count()
            and candidate.get("source_null_geometry_count") == 0
            and candidate.get("source_invalid_geometry_count") == 0
        )
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

    check.__name__ = check_name
    check_def = asset_check(asset=asset_def, blocking=True)(check)

    job = define_asset_job(job_name, selection=AssetSelection.assets(asset_def))
    schedule = ScheduleDefinition(
        name=schedule_name,
        job=job,
        cron_schedule="0 8 1 * *",
        execution_timezone="America/New_York",
        default_status=DefaultScheduleStatus.STOPPED,
    )
    return asset_def, check_def, job, schedule
