"""Fail-closed manual acceptance boundary for immutable release objects.

This is deliberately not automatic acceptance. Deployment-owned registry entries
are exact maintainer decisions; staging manifests/reports cannot approve themselves.
The registry is runtime configuration with the same trust as storage credentials.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import unsupported_output_reason
from dagster_hifld.native_verification import ADAPTER as NATIVE_ADAPTER, _code_digest, _digest, reader_versions


class ReleaseApprovalError(ValueError):
    """Candidate lacks an exact, independent release acceptance decision."""


def verifier_implementation_sha256() -> str:
    """Bind decisions to installed pipeline code, including direct call paths."""
    root = Path(__file__).parent
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        name = path.relative_to(root).as_posix().encode()
        body = path.read_bytes()
        digest.update(len(name).to_bytes(8, "big") + name)
        digest.update(len(body).to_bytes(8, "big") + body)
    return digest.hexdigest()


def _storage_identity(storage: StagingStorageResource) -> dict:
    return {
        "backend": "local" if storage.use_local or not storage.bucket else "s3" if storage._uses_s3() else "gcs",
        "location": str(Path(storage.local_dir).resolve()) if storage.use_local or not storage.bucket else storage.bucket,
        "prefix": storage.prefix,
    }


def _registry(source: StagingStorageResource | None, destination: StagingStorageResource) -> dict:
    registry_path = os.environ.get("HIFLD_RELEASE_APPROVALS")
    if not registry_path:
        raise ReleaseApprovalError("missing_independent_release_approval_registry")
    path = Path(registry_path).resolve()
    for storage in (source, destination):
        if storage is not None and (storage.use_local or not storage.bucket):
            if path.is_relative_to(Path(storage.local_dir).resolve()):
                raise ReleaseApprovalError("acceptance_registry_must_be_outside_candidate_and_release_storage")
    try:
        registry = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise ReleaseApprovalError("unreadable_release_approval_registry") from exc
    if registry.get("schema_version") != 1 or not isinstance(registry.get("releases"), list):
        raise ReleaseApprovalError("unsupported_release_approval_registry")
    return registry


def _entry(registry: dict, destination: StagingStorageResource, key: str) -> dict:
    entries = [entry for entry in registry["releases"]
               if entry.get("destination") == _storage_identity(destination)
               and key in entry.get("published_objects", {})]
    if len(entries) != 1:
        raise ReleaseApprovalError(f"missing_or_ambiguous_exact_object_approval:{key}")
    entry = entries[0]
    decision = entry.get("decision", {})
    if any(not decision.get(field) for field in ("id", "approved_by", "approved_at")):
        raise ReleaseApprovalError("missing_maintainer_decision_identity")
    if not entry.get("baseline") or not entry.get("contract_sha256"):
        raise ReleaseApprovalError("missing_accepted_baseline_or_contract")
    baseline = entry["baseline"]
    if any(not baseline.get(field) for field in ("dataset_slug", "file_slug", "version", "catalog_url", "catalog_sha256", "data_objects", "mapping_evidence", "decision")):
        raise ReleaseApprovalError("incomplete_accepted_baseline_reference")
    if any(not baseline["decision"].get(field) for field in ("id", "approved_by", "approved_at")):
        raise ReleaseApprovalError("missing_baseline_acceptance_decision")
    for obj in baseline["data_objects"]:
        if not obj.get("url") or not re.fullmatch(r"[0-9a-f]{64}", obj.get("sha256", "")) or not isinstance(obj.get("bytes"), int):
            raise ReleaseApprovalError("incomplete_pinned_baseline_object")
    baseline_input = baseline.get("comparison_input", {})
    if (baseline_input.get("adapter") != "typed-geojson-feature-jsonl-v1"
            or not re.fullmatch(r"[0-9a-f]{64}", baseline_input.get("sha256", ""))
            or not isinstance(baseline_input.get("bytes"), int)):
        raise ReleaseApprovalError("missing_accepted_baseline_comparison_input_pin")
    derivation = baseline_input.get("derivation", {})
    if (derivation.get("source_sha256") != sorted(obj["sha256"] for obj in baseline["data_objects"])
            or not derivation.get("code_sha256") or not isinstance(derivation.get("parameters"), dict)):
        raise ReleaseApprovalError("missing_reviewed_baseline_normalization_derivation")
    if not entry.get("report_sha256") or not entry.get("provenance_sha256"):
        raise ReleaseApprovalError("missing_reviewed_report_or_provenance")
    if entry.get("implementation_sha256") != verifier_implementation_sha256():
        raise ReleaseApprovalError("reviewed_pipeline_implementation_changed")
    return entry


def require_generated_object_approval(storage: StagingStorageResource, key: str, contents: bytes) -> None:
    """Generated published metadata/styles are also subject to exact acceptance."""
    logical = storage._ensure_prefixed(key).removeprefix(storage.prefix.rstrip("/") + "/") if storage.prefix else key
    entry = _entry(_registry(None, storage), storage, logical)
    if hashlib.sha256(contents).hexdigest() != entry["published_objects"][logical]:
        raise ReleaseApprovalError(f"generated_object_differs_from_accepted_bytes:{logical}")


def require_release_approval(
    source: StagingStorageResource,
    destination: StagingStorageResource,
    pairs: list[tuple[str, str]],
) -> dict[str, object]:
    """Recheck complete staged inventories, reviewed metadata and copy byte pins.

    Returns source snapshots for the conditional copy, closing the hash/copy race.
    This boundary cannot attest to transformations; their review is explicitly part
    of the exact manual release decision until source-specific verifiers exist.
    """
    if not pairs:
        raise ReleaseApprovalError("empty_release_inventory")
    registry = _registry(source, destination)
    snapshots = {}
    checked = set()
    for source_key, destination_key in pairs:
        logical = destination._ensure_prefixed(destination_key)
        if destination.prefix:
            logical = logical.removeprefix(destination.prefix.rstrip("/") + "/")
        entry = _entry(registry, destination, logical)
        entry_id = id(entry)
        if entry_id not in checked:
            prefix = entry.get("staged_version_prefix", "")
            if not prefix or len(prefix.split("/")) != 3:
                raise ReleaseApprovalError("invalid_staged_version_identity")
            expected = entry.get("staged_objects", {})
            actual_keys = source.list_prefix(prefix)
            normalized = {key.removeprefix(source.prefix.rstrip("/") + "/") if source.prefix else key: key for key in actual_keys}
            if set(normalized) != set(expected):
                raise ReleaseApprovalError("staged_object_inventory_changed")
            for name, key in normalized.items():
                before = source.object_snapshot(key)
                if before is None or source.sha256_key(key) != expected[name]:
                    raise ReleaseApprovalError(f"staged_object_bytes_changed:{name}")
                if source.object_snapshot(key) != before:
                    raise ReleaseApprovalError(f"staged_object_changed_during_verification:{name}")
                snapshots[key] = before
            for filename, digest in (("baseline_comparison.json", entry["report_sha256"]), ("source_manifest.json", entry["provenance_sha256"])):
                key = f"{prefix}/metadata/{filename}"
                if expected.get(key) != digest:
                    raise ReleaseApprovalError(f"reviewed_metadata_pin_missing:{filename}")
            report = json.loads(source.read_key(f"{prefix}/metadata/baseline_comparison.json"))
            if report.get("status") not in {"pass", "needs_decision"} or report.get("baseline") != entry["baseline"] or report.get("contract_sha256") != entry["contract_sha256"]:
                raise ReleaseApprovalError("reviewed_comparison_incomplete_or_unmatched")
            if any(counts.get("null_keys") or counts.get("duplicate_keys") or counts.get("invalid_records") for counts in report.get("populations", {}).values()):
                raise ReleaseApprovalError("invalid_comparison_population")
            if set(report.get("populations", {})) != {"baseline", "candidate"}:
                raise ReleaseApprovalError("missing_full_comparison_populations")
            if report.get("adapter") != "typed-geojson-feature-jsonl-v1":
                raise ReleaseApprovalError("unsupported_comparison_adapter")
            baseline_input = entry["baseline"]["comparison_input"]
            if report.get("baseline_sha256") != baseline_input["sha256"] or report.get("baseline_input_bytes") != baseline_input["bytes"]:
                raise ReleaseApprovalError("report_baseline_input_does_not_match_accepted_reference")
            candidate_input = entry.get("candidate_comparison_input", {})
            candidate_key = candidate_input.get("key")
            if (not candidate_key or candidate_key not in expected
                    or expected[candidate_key] != candidate_input.get("sha256")
                    or report.get("candidate_sha256") != candidate_input.get("sha256")
                    or report.get("candidate_input_bytes") != candidate_input.get("bytes")
                    or source.key_size(candidate_key) != candidate_input.get("bytes")
                    or candidate_input.get("adapter") != report["adapter"]):
                raise ReleaseApprovalError("report_candidate_input_does_not_match_accepted_inventory")
            candidate_derivation = candidate_input.get("derivation", {})
            source_objects = candidate_derivation.get("source_objects", {})
            if (not source_objects or not candidate_derivation.get("code_sha256")
                    or not isinstance(candidate_derivation.get("parameters"), dict)
                    or any(expected.get(key) != value or entry["published_objects"].get(key) != value for key, value in source_objects.items())):
                raise ReleaseApprovalError("missing_reviewed_candidate_normalization_derivation")
            from_manifest = json.loads(source.read_key(f"{prefix}/metadata/source_manifest.json"))
            validate_release_provenance(from_manifest)
            for format_name, directory in (("shapefile_zip", "shapefile"), ("geopackage", "geopackage"),
                                           ("geoparquet", "geoparquet"), ("pmtiles", "pmtiles")):
                if unsupported_output_reason(from_manifest, format_name) and any(name.startswith(f"{prefix}/{directory}/") for name in expected):
                    raise ReleaseApprovalError(f"unsupported_output_present:{format_name}")
            quality = json.loads(source.read_key(f"{prefix}/metadata/quality_manifest.json"))
            if quality.get("quality_check_passed") is not True:
                raise ReleaseApprovalError("catalog_quality_failed")
            evidence_keys = [f"{prefix}/metadata/{filename}" for filename in
                             ("source_manifest.json", "baseline_comparison.json", "baseline_comparison.md", "quality_manifest.json", "data_dictionary.json")]
            sidecar = report.get("sidecar", {})
            sidecar_name = sidecar.get("path", "")
            if not sidecar_name or Path(sidecar_name).name != sidecar_name:
                raise ReleaseApprovalError("missing_complete_difference_sidecar")
            sidecar_key = f"{prefix}/metadata/{sidecar_name}"
            if expected.get(sidecar_key) != sidecar.get("sha256"):
                raise ReleaseApprovalError("comparison_sidecar_pin_mismatch")
            evidence_keys.append(sidecar_key)
            if any(name not in entry["published_objects"] or entry["published_objects"][name] != expected.get(name) for name in evidence_keys):
                raise ReleaseApprovalError("acceptance_evidence_must_be_downloadable")
            field_sidecar = report.get("attribute_field_changes")
            if field_sidecar:
                _require_downloadable_evidence(entry, prefix, field_sidecar)
            if "native_inputs" in report:
                _require_native_evidence(report, entry, source, prefix)
            checked.add(entry_id)
        normalized_source = source._ensure_prefixed(source_key)
        if normalized_source not in snapshots:
            raise ReleaseApprovalError("source_object_outside_reviewed_inventory")
        name = normalized_source.removeprefix(source.prefix.rstrip("/") + "/") if source.prefix else normalized_source
        if entry["published_objects"][logical] != entry["staged_objects"][name]:
            raise ReleaseApprovalError(f"copy_bytes_differ_from_accepted_output:{logical}")
    return snapshots


def _require_downloadable_evidence(entry: dict, prefix: str, descriptor: dict, *, parent: str = "") -> str:
    if not isinstance(descriptor, dict):
        raise ReleaseApprovalError("missing_native_evidence_descriptor")
    name = descriptor.get("path", "")
    path = Path(name)
    if not name or path.is_absolute() or any(part in {".", ".."} for part in path.parts) or path.as_posix() != name:
        raise ReleaseApprovalError("unsafe_or_missing_native_evidence_path")
    key = f"{prefix}/metadata/" + (parent.rstrip("/") + "/" if parent else "") + name
    if (entry["staged_objects"].get(key) != descriptor.get("sha256")
            or entry["published_objects"].get(key) != descriptor.get("sha256")):
        raise ReleaseApprovalError("native_evidence_not_pinned_and_downloadable")
    return key


def _require_native_evidence(report: dict, entry: dict, source: StagingStorageResource, prefix: str) -> None:
    descriptors = report["native_inputs"]
    if not isinstance(descriptors, dict) or set(descriptors) != {"baseline", "candidate"}:
        raise ReleaseApprovalError("missing_both_native_receipts")
    if not report.get("attribute_field_changes"):
        raise ReleaseApprovalError("missing_native_per_field_comparison")
    for side, descriptor in descriptors.items():
        receipt_key = _require_downloadable_evidence(entry, prefix, descriptor)
        receipt = json.loads(source.read_key(receipt_key))
        parent = Path(descriptor["path"]).parent.as_posix()
        if (not isinstance(receipt, dict) or receipt != descriptor.get("receipt") or receipt.get("schema_version") != 1
                or receipt.get("adapter") != NATIVE_ADAPTER or receipt.get("status") != "complete"
                or receipt.get("code_sha256") != _code_digest()
                or receipt.get("reader_versions") != reader_versions()
                or _digest({key: value for key, value in receipt.items() if key != "receipt_id"}) != receipt.get("receipt_id")):
            raise ReleaseApprovalError("native_receipt_incomplete_forged_or_stale")
        policy = receipt.get("policy", {})
        if (policy.get("status") != "reviewed" or _digest(policy) != receipt.get("policy_sha256")
                or entry.get("native_policy_sha256") != receipt.get("policy_sha256")):
            raise ReleaseApprovalError("native_comparison_policy_not_independently_accepted")
        normalized = receipt.get("normalized", {})
        normalized_key = _require_downloadable_evidence(entry, prefix, normalized, parent=parent)
        if (normalized.get("sha256") != report.get(f"{side}_sha256")
                or normalized.get("bytes") != report.get(f"{side}_input_bytes")
                or source.key_size(normalized_key) != normalized.get("bytes")):
            raise ReleaseApprovalError("native_normalized_input_does_not_match_comparison")
        lineage = receipt.get("lineage", {})
        lineage_key = _require_downloadable_evidence(entry, prefix, lineage, parent=parent)
        if (source.key_size(lineage_key) != lineage.get("bytes") or lineage.get("rows") != receipt.get("rows")
                or receipt.get("rows") != report["populations"][side].get("rows")):
            raise ReleaseApprovalError("native_lineage_or_population_count_unmatched")
        objects = receipt.get("objects", [])
        if not objects or sum(obj["rows_observed"] for obj in objects) != receipt["rows"]:
            raise ReleaseApprovalError("native_object_population_unmatched")
        schema_digest = _digest(receipt.get("physical_schema"))
        if any(obj.get("physical_schema_sha256") != schema_digest for obj in objects):
            raise ReleaseApprovalError("native_partition_physical_schema_unmatched")
        if side == "baseline":
            accepted = sorted((obj["sha256"], obj["bytes"]) for obj in entry["baseline"]["data_objects"])
            if sorted((obj["sha256"], obj["bytes"]) for obj in objects) != accepted:
                raise ReleaseApprovalError("native_baseline_original_objects_unmatched")
            pin = entry["baseline"]["comparison_input"]
            if pin.get("derivation", {}).get("parameters", {}).get("receipt_sha256") != descriptor["sha256"]:
                raise ReleaseApprovalError("native_baseline_derivation_receipt_unmatched")
        else:
            if normalized_key != entry["candidate_comparison_input"].get("key"):
                raise ReleaseApprovalError("native_candidate_input_key_unmatched")
            approved_sources = entry["candidate_comparison_input"]["derivation"]["source_objects"]
            storage_prefix = receipt.get("parameters", {}).get("storage_prefix", "")
            native_sources = {f"{prefix}/" + (storage_prefix.rstrip("/") + "/" if storage_prefix else "") + obj["key"]: obj["sha256"] for obj in objects}
            if (native_sources != approved_sources or any(entry["staged_objects"].get(key) != digest for key, digest in native_sources.items())
                    or any(source.key_size(f"{prefix}/" + (storage_prefix.rstrip("/") + "/" if storage_prefix else "") + obj["key"]) != obj["bytes"] for obj in objects)):
                raise ReleaseApprovalError("native_candidate_original_objects_unmatched")
            derivation = entry["candidate_comparison_input"]["derivation"]
            if derivation.get("code_sha256") != receipt["code_sha256"] or derivation.get("parameters", {}).get("receipt_sha256") != descriptor["sha256"]:
                raise ReleaseApprovalError("native_candidate_derivation_receipt_unmatched")
    physical = report.get("physical_schema", {})
    baseline_schema, candidate_schema = (descriptors[side]["receipt"]["physical_schema"] for side in ("baseline", "candidate"))
    equal = baseline_schema == candidate_schema
    if (physical.get("baseline") != baseline_schema or physical.get("candidate") != candidate_schema
            or physical.get("equal") is not equal or (not equal and report.get("status") == "pass")):
        raise ReleaseApprovalError("native_report_physical_schema_unmatched")


def validate_release_provenance(manifest: dict) -> None:
    """Minimum multi-input published provenance shape; unknown authority blocks."""
    if manifest.get("schema_version") != 1 or manifest.get("processor") != "HIFLD Next" or not manifest.get("inputs"):
        raise ReleaseApprovalError("missing_versioned_multi_input_provenance")
    seen = set()
    for item in manifest["inputs"]:
        fields = ("input_id", "producer", "host", "canonical_url", "retrieval_url", "retrieved_at", "sha256", "license", "attribution")
        if any(not item.get(field) for field in fields) or not isinstance(item.get("bytes"), int):
            raise ReleaseApprovalError("incomplete_input_provenance")
        if not re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) or item["bytes"] < 0:
            raise ReleaseApprovalError("invalid_input_checksum_or_length")
        for name in ("canonical_url", "retrieval_url"):
            parsed = urlsplit(item[name])
            if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
                raise ReleaseApprovalError("unsafe_provenance_url")
            if any(re.search(r"token|secret|signature|credential|password|api.?key|^sig$|^key$", key, re.IGNORECASE)
                   for key, _ in parse_qsl(parsed.query)):
                raise ReleaseApprovalError("provenance_url_contains_sensitive_query")
        if item["input_id"] in seen:
            raise ReleaseApprovalError("conflicting_input_identity")
        seen.add(item["input_id"])
        if item.get("role") not in {"producer_publication", "official_distributor", "mirror", "archive"}:
            raise ReleaseApprovalError("unresolved_source_authority")
        if item["role"] == "official_distributor" and not item.get("delegation_evidence"):
            raise ReleaseApprovalError("unverified_official_distribution")
        if not item.get("release_date") and not item.get("release_date_unknown_reason"):
            raise ReleaseApprovalError("unexplained_unknown_release_date")
    if not manifest.get("code_commit") or not isinstance(manifest.get("transformations"), list):
        raise ReleaseApprovalError("missing_transformation_provenance")
