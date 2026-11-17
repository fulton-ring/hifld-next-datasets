"""Draft Dagster assets for every FEMA NFHL layer in ``sources.nfhl.layers.LAYERS``.

Each layer gets an ingest asset keyed ``ingest/<slug>/<slug>``, a blocking
completeness check (staged features equal the producer's capture-time count),
a non-blocking WARN check for count drift since the previous candidate, and a
stopped monthly schedule.
"""

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
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
from dagster_hifld.sources.nfhl import LAYERS, NfhlLayer, capture_nfhl_layer
from dagster_hifld.sources.nfhl.candidate import APPROVAL_FLAGS


class NfhlCaptureConfig(Config):
    # Resumable capture root; each layer captures under <capture_directory>/<slug>.
    capture_directory: str = ""


def _previous_feature_count(context, key: AssetKey) -> int | None:
    event = context.instance.get_latest_materialization_event(key)
    materialization = event.asset_materialization if event is not None else None
    value = materialization.metadata.get("feature_count") if materialization is not None else None
    count = getattr(value, "value", value)
    return count if type(count) is int else None


def _layer_definitions(layer: NfhlLayer):
    key = AssetKey(["ingest", layer.slug, layer.slug])
    kind = "table" if layer.is_table else "layer"

    @asset(key=key, group_name="fema", compute_kind="http",
           description=(f"FEMA NFHL MapServer {kind} {layer.layer_id} '{layer.name}' from the producer's own REST "
                        "publication, complete against the producer's count at capture time. Candidate extraction "
                        "only; release date, freshness, rights, and schema/ID acceptance remain pending."))
    def ingest(context, config: NfhlCaptureConfig, staging_storage: StagingStorageResource) -> dict:
        previous = _previous_feature_count(context, key)
        result = capture_nfhl_layer(staging_storage, layer, capture_directory=config.capture_directory or None)
        result = {**result, "previous_feature_count": previous}
        context.instance.add_dynamic_partitions(
            PUBLISH_PARTITION_NAME, [build_publish_partition_key(layer.slug, layer.slug, result["version"])])
        context.add_output_metadata({k: v for k, v in result.items() if v is not None})
        return result

    @asset_check(asset=ingest, name=f"fema_{layer.stem}_complete", blocking=True)
    def complete(candidate: dict) -> AssetCheckResult:
        passed = (type(candidate.get("feature_count")) is int
                  and candidate.get("feature_count") == candidate.get("source_count")
                  and candidate.get("candidate_ready") is not None)
        return AssetCheckResult(passed=passed, metadata={
            "feature_count": candidate.get("feature_count", 0),
            "source_count_at_capture": candidate.get("source_count", 0),
            **{k: v for k, v in candidate.items() if k.endswith("_geometry_count")},
            "technical_geometry_quality_accepted": False, **APPROVAL_FLAGS})

    @asset_check(asset=ingest, name=f"fema_{layer.stem}_count_drift", blocking=False)
    def count_drift(candidate: dict) -> AssetCheckResult:
        previous, current = candidate.get("previous_feature_count"), candidate.get("feature_count")
        return AssetCheckResult(
            passed=previous is None or previous == current, severity=AssetCheckSeverity.WARN,
            metadata={"feature_count": current,
                      **({} if previous is None else {"previous_feature_count": previous,
                                                      "change": current - previous})})

    job = define_asset_job(f"{layer.stem}_draft_refresh", selection=AssetSelection.assets(ingest))
    schedule = ScheduleDefinition(name=f"{layer.stem}_draft_refresh_schedule", job=job, cron_schedule="0 8 1 * *",
                                  execution_timezone="America/New_York",
                                  default_status=DefaultScheduleStatus.STOPPED)
    return ingest, [complete, count_drift], job, schedule


INGEST_ASSETS, ASSET_CHECKS, JOBS, SCHEDULES = [], [], [], []
for _layer in LAYERS:
    _ingest, _checks, _job, _schedule = _layer_definitions(_layer)
    INGEST_ASSETS.append(_ingest)
    ASSET_CHECKS.extend(_checks)
    JOBS.append(_job)
    SCHEDULES.append(_schedule)
