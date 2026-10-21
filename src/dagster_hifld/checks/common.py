"""Shared helpers for manifest-backed asset checks."""

from __future__ import annotations

import json

from dagster import AssetCheckResult

from dagster_hifld.release_approval import ReleaseApprovalError, require_release_approval
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

# Retained only for compatibility with callers; no acceptance semantics remain.
BASELINE_VERSION = "v20260214"


def read_quality_check_result(
    staging_storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> AssetCheckResult:
    key = f"{dataset_slug}/{file_slug}/{version}/metadata/quality_manifest.json"
    if not staging_storage.object_exists(key):
        return AssetCheckResult(
            passed=False,
            metadata={
                "reason": "catalog_not_run",
                "dataset_slug": dataset_slug,
                "file_slug": file_slug,
                "version": version,
            },
        )

    data = json.loads(
        staging_storage.read_bytes(
            dataset_slug,
            file_slug,
            version,
            "metadata/quality_manifest.json",
        ).decode("utf-8")
    )
    return AssetCheckResult(
        passed=data.get("quality_check_passed") is True,
        metadata=data,
    )


def read_baseline_comparison_result(
    staging_storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    destination: PublishedStorageResource | None = None,
) -> AssetCheckResult:
    """Require an exact independently accepted report, never sampled count parity."""
    key = f"{dataset_slug}/{file_slug}/{version}/metadata/baseline_comparison.json"
    if not staging_storage.object_exists(key):
        # Leave durable, explicit evidence that the normal catalog graph reached
        # the comparison gate but had no independently pinned baseline/config.
        # Never overwrite an existing report or infer acceptance from catalog
        # quality metadata.
        blocked = {
            "schema_version": 1,
            "status": "blocked",
            "reason_codes": ["missing_comparison_configuration"],
            "acceptance": "not_established_by_comparison",
            "dataset_slug": dataset_slug,
            "file_slug": file_slug,
            "version": version,
        }
        staging_storage.write_key_if_unchanged(
            key, json.dumps(blocked, sort_keys=True).encode("utf-8"), expected_snapshot=None
        )
        return AssetCheckResult(passed=False, metadata={"reason": "missing_full_data_comparison"})
    try:
        # Use configured destination solely to match the independent decision.
        # No destination writes occur during acceptance checking.
        destination = destination or PublishedStorageResource.from_env()
        from dagster_hifld.portolan.release import read_staged_release
        _, target_prefix = read_staged_release(staging_storage, f"{dataset_slug}/{file_slug}/{version}")
        require_release_approval(staging_storage, destination, [(key, f"{target_prefix}/metadata/baseline_comparison.json")])
    except (ReleaseApprovalError, ValueError, OSError, KeyError, TypeError) as exc:
        return AssetCheckResult(passed=False, metadata={"reason": str(exc)})
    return AssetCheckResult(passed=True, metadata={"reason": "exact_manual_release_decision", "report": key})
