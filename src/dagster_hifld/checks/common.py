"""Shared helpers for manifest-backed asset checks."""

from __future__ import annotations

import json

from dagster import AssetCheckResult, MetadataValue

from dagster_hifld.resources import StagingStorageResource

BASELINE_VERSION = "v20260214"
ROW_COUNT_TOLERANCE = 0.10


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
        passed=bool(data.get("quality_check_passed")),
        metadata=data,
    )


def read_baseline_comparison_result(
    staging_storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> AssetCheckResult:
    """Compare current partition to BASELINE_VERSION for column superset and row count (±10%)."""
    if version == BASELINE_VERSION:
        return AssetCheckResult(
            passed=True,
            metadata={
                "reason": "is_baseline",
                "baseline_version": BASELINE_VERSION,
            },
        )

    baseline_quality_key = (
        f"{dataset_slug}/{file_slug}/{BASELINE_VERSION}/metadata/quality_manifest.json"
    )
    if not staging_storage.object_exists(baseline_quality_key):
        return AssetCheckResult(
            passed=True,
            metadata={
                "reason": "no_baseline",
                "baseline_version": BASELINE_VERSION,
            },
        )

    current_quality_key = (
        f"{dataset_slug}/{file_slug}/{version}/metadata/quality_manifest.json"
    )
    current_dict_key = (
        f"{dataset_slug}/{file_slug}/{version}/metadata/data_dictionary.json"
    )
    if not staging_storage.object_exists(current_quality_key):
        return AssetCheckResult(
            passed=False,
            metadata={
                "reason": "current_catalog_incomplete",
                "missing": "quality_manifest.json",
                "dataset_slug": dataset_slug,
                "file_slug": file_slug,
                "version": version,
            },
        )
    if not staging_storage.object_exists(current_dict_key):
        return AssetCheckResult(
            passed=False,
            metadata={
                "reason": "current_catalog_incomplete",
                "missing": "data_dictionary.json",
                "dataset_slug": dataset_slug,
                "file_slug": file_slug,
                "version": version,
            },
        )

    baseline_quality = json.loads(
        staging_storage.read_bytes(
            dataset_slug,
            file_slug,
            BASELINE_VERSION,
            "metadata/quality_manifest.json",
        ).decode("utf-8")
    )
    current_quality = json.loads(
        staging_storage.read_bytes(
            dataset_slug,
            file_slug,
            version,
            "metadata/quality_manifest.json",
        ).decode("utf-8")
    )
    baseline_dict = json.loads(
        staging_storage.read_bytes(
            dataset_slug,
            file_slug,
            BASELINE_VERSION,
            "metadata/data_dictionary.json",
        ).decode("utf-8")
    )
    current_dict = json.loads(
        staging_storage.read_bytes(
            dataset_slug,
            file_slug,
            version,
            "metadata/data_dictionary.json",
        ).decode("utf-8")
    )

    baseline_cols = {c["name"] for c in baseline_dict.get("columns", []) if "name" in c}
    current_cols = {c["name"] for c in current_dict.get("columns", []) if "name" in c}
    dropped_cols = sorted(baseline_cols - current_cols)
    added_cols = sorted(current_cols - baseline_cols)

    baseline_rows = int(baseline_quality.get("feature_count") or 0)
    current_rows = int(current_quality.get("feature_count") or 0)
    if baseline_rows > 0:
        row_delta_pct = abs(current_rows - baseline_rows) / baseline_rows
    else:
        row_delta_pct = 0.0 if current_rows == 0 else 1.0

    columns_ok = not dropped_cols
    rows_ok = row_delta_pct <= ROW_COUNT_TOLERANCE
    passed = columns_ok and rows_ok

    return AssetCheckResult(
        passed=passed,
        metadata={
            "baseline_version": BASELINE_VERSION,
            "baseline_row_count": baseline_rows,
            "current_row_count": current_rows,
            "row_delta_pct": round(row_delta_pct * 100, 2),
            "row_tolerance_pct": ROW_COUNT_TOLERANCE * 100,
            "dropped_columns": MetadataValue.json(dropped_cols),
            "added_columns": MetadataValue.json(added_cols),
            "columns_ok": columns_ok,
            "rows_ok": rows_ok,
        },
    )
