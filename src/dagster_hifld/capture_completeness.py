"""Capture-time ArcGIS population integrity and separate approved-release drift.

No source, identifier contract, baseline or release is approved by these helpers.
Callers still validate metadata, page boundaries, schema, geometry and transport.
"""
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check


def validate_capture_witness(count_before: int, ids_before: list[int],
                             count_after: int, ids_after: list[int],
                             captured_ids: list[int] | None = None) -> list[int]:
    """Require stable counts/unique native ID sets, plus exact captured coverage.

    ID endpoint order is immaterial. Page ordering/boundaries remain the caller's
    responsibility. Passing no captured IDs validates only the source witnesses.
    """
    for count, ids in ((count_before, ids_before), (count_after, ids_after)):
        if (type(count) is not int or count < 0 or not isinstance(ids, list)
                or any(type(oid) is not int or oid <= 0 for oid in ids)
                or len(ids) != count or len(set(ids)) != count):
            raise ValueError("source_count_or_id_roster_drift")
    if count_before != count_after or set(ids_before) != set(ids_after):
        raise ValueError("source_count_or_id_roster_drift")
    ordered = sorted(ids_before)
    if captured_ids is not None and (not isinstance(captured_ids, list)
            or any(type(oid) is not int for oid in captured_ids)
            or len(captured_ids) != count_before or sorted(captured_ids) != ordered):
        raise ValueError("source_captured_id_roster_incomplete")
    return ordered


def approved_baseline_drift(candidate: dict, approved_baseline: dict | None = None,
                            *, count_tolerance: int = 0) -> AssetCheckResult:
    """Warn on count/ID drift from an independently approved same-file release.

    The caller must obtain that reference from its release-approval workflow;
    code pins and development captures are never default baselines. A tolerance
    affects count drift only: changed native IDs still warn, even at equal count.
    """
    if type(count_tolerance) is not int or count_tolerance < 0:
        raise ValueError("baseline_count_tolerance_invalid")
    if approved_baseline is None:
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.WARN, metadata={
            "comparison_status": "skipped_no_approved_baseline",
            "baseline_available": False,
            "comparison_scope": "feature count and native ID roster only",
        })
    if (approved_baseline.get("release_accepted") is not True
            or not approved_baseline.get("version")
            or any(not candidate.get(key) or candidate.get(key) != approved_baseline.get(key)
                   for key in ("dataset_slug", "file_slug"))
            or any(type(value.get("feature_count")) is not int or value["feature_count"] < 0
                   or not isinstance(value.get("source_id_sha256"), str)
                   or not value["source_id_sha256"] for value in (candidate, approved_baseline))):
        raise ValueError("approved_baseline_reference_invalid")
    delta = candidate["feature_count"] - approved_baseline["feature_count"]
    ids_changed = candidate["source_id_sha256"] != approved_baseline["source_id_sha256"]
    return AssetCheckResult(passed=abs(delta) <= count_tolerance and not ids_changed,
        severity=AssetCheckSeverity.WARN, metadata={
            "comparison_status": "compared_approved_release", "baseline_available": True,
            "baseline_version": approved_baseline["version"],
            "dataset_slug": candidate["dataset_slug"], "file_slug": candidate["file_slug"],
            "comparison_scope": "feature count and native ID roster only",
            "feature_count_delta": delta, "count_tolerance": count_tolerance,
            "native_id_roster_changed": ids_changed,
        })


def register_approved_baseline_drift(asset, name: str):
    """Register the non-blocking approved-baseline drift WARN check for one asset.

    Completeness is enforced by :func:`validate_capture_witness` at capture
    time. This check only compares a candidate against an independently
    approved same-file release when the caller supplies one; with no approved
    baseline it reports an explicit skip and never turns a code pin or a
    development capture into a release baseline.
    """
    if not isinstance(name, str) or not name or not name.isidentifier():
        raise ValueError("baseline_drift_check_name_invalid")

    def check(candidate: dict) -> AssetCheckResult:
        return approved_baseline_drift(candidate)

    check.__name__ = name
    return asset_check(asset=asset, blocking=False)(check)
