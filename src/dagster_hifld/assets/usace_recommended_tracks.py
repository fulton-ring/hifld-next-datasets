"""Proposal-only Dagster registration for USACE IENC recommended tracks."""
from __future__ import annotations

import json

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    MetadataValue,
    RunRequest,
    SkipReason,
    asset,
    asset_check,
    define_asset_job,
    schedule,
)

from dagster_hifld.download import build_version_id
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.usace_recommended_tracks import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_ID_SHA256,
    FILE_SLUG,
    LAYER_URL,
    capture,
)

_ASSET_KEY = AssetKey(["ingest", DATASET_SLUG, FILE_SLUG])
_DESCRIPTION = (
    "Unapproved native draft for IENC RECOMMENDED_TRACK_LINE MapServer layer 46. "
    "Coverage is limited to the IENC chart feature class; BTS item succession, "
    "historical continuity, rights, and release acceptance remain unresolved."
)


@asset(key=_ASSET_KEY, group_name="usace", compute_kind="http", description=_DESCRIPTION)
def usace_recommended_tracks_ienc(context, staging_storage: StagingStorageResource) -> dict:
    if (isinstance(staging_storage, PublishedStorageResource) or not staging_storage.use_local
            or staging_storage.bucket or not staging_storage.local_dir or staging_storage.prefix):
        raise ValueError("recommended_tracks_draft_requires_isolated_local_staging")
    result = capture(staging_storage)
    partition_key = build_publish_partition_key(
        DATASET_SLUG, FILE_SLUG, str(result["version"]),
    )
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition_key])
    context.add_output_metadata({
        "feature_count": MetadataValue.int(int(result["feature_count"])),
        "source_ids_sha256": MetadataValue.text(str(result["source_id_sha256"])),
        "source_records_sha256": MetadataValue.text(str(result["source_record_sha256"])),
        "source_schema_sha256": MetadataValue.text(str(result["source_schema_sha256"])),
        "candidate_ready": MetadataValue.text(str(result["candidate_ready"])),
        "source_population_complete": MetadataValue.bool(True),
        "release_accepted": MetadataValue.bool(False),
    })
    return {**result, "partition_key": partition_key}


@asset_check(asset=usace_recommended_tracks_ienc, name="recommended_tracks_complete", blocking=True)
def usace_recommended_tracks_complete(
    context,
    candidate: dict,
    staging_storage: StagingStorageResource,
) -> AssetCheckResult:
    version = candidate.get("version")
    if not isinstance(version, str) or candidate.get("source_population_complete") is not True:
        return AssetCheckResult(passed=False, metadata={"reason": "incomplete_source_candidate"})
    prefix = staging_storage.build_target_location(DATASET_SLUG, FILE_SLUG, version, "")
    try:
        audit = json.loads(staging_storage.read_key(prefix + "metadata/source_audit.json"))
        ready = json.loads(staging_storage.read_key(prefix + "metadata/candidate_ready.json"))
    except (OSError, ValueError, KeyError) as exc:
        return AssetCheckResult(passed=False, metadata={"reason": "source_evidence_unreadable",
                                                        "error_type": type(exc).__name__})
    passed = (
        candidate.get("feature_count") == EXPECTED_COUNT
        and candidate.get("source_id_sha256") == EXPECTED_ID_SHA256
        and audit.get("feature_count") == EXPECTED_COUNT
        and audit.get("objectid_sha256") == EXPECTED_ID_SHA256
        and audit.get("count_before") == audit.get("count_after") == EXPECTED_COUNT
        and ready.get("source_population_complete") is True
        and ready.get("source_id_sha256") == EXPECTED_ID_SHA256
        and ready.get("release_accepted") is False
    )
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": audit.get("feature_count"),
        "objectid_sha256": audit.get("objectid_sha256"),
        "source_scope": audit.get("source_scope"),
        "bts_item_succession_accepted": False,
        "release_accepted": False,
        "reason": "complete_pinned_source_population" if passed else "source_population_gate_failed",
    })


recommended_tracks_draft_job = define_asset_job(
    "recommended_tracks_usace_ienc_draft_refresh",
    selection=AssetSelection.assets(usace_recommended_tracks_ienc),
    tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
)


@schedule(
    job=recommended_tracks_draft_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    required_resource_keys={"staging_storage"},
)
def recommended_tracks_draft_schedule(context):
    storage = context.resources.staging_storage
    if (isinstance(storage, PublishedStorageResource) or not storage.use_local
            or storage.bucket or not storage.local_dir or storage.prefix):
        return SkipReason("Recommended Tracks draft refresh only runs against isolated local staging")
    if context.scheduled_execution_time is None:
        return SkipReason("Draft refresh requires an explicit scheduled day")
    day = context.scheduled_execution_time.date().isoformat()
    return RunRequest(
        run_key=f"recommended-tracks-ienc-draft-{day}",
        tags={"hifld/draft": "true", "hifld/promotion_allowed": "false"},
    )
