"""Pure provenance shape validation; captured claims are never acceptance."""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import PurePosixPath
from urllib.parse import parse_qsl, urlsplit
from dagster_hifld.output_policy import validate_output_policy

_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})")
INPUT_URL_FIELDS = ("canonical_url", "retrieval_url", "requested_url", "request_url", "observed_final_url", "final_url")
_ROLES = {"producer_publication", "official_distributor", "mirror", "archive"}


def _text(value: object, reason: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(reason)
    return value


def validate_provenance_url(value: object) -> str:
    """Require a public HTTP URL without embedded credentials or sensitive keys."""
    value = _text(value, "unsafe_provenance_url")
    if any(char.isspace() or ord(char) < 32 for char in value) or "\\" in value:
        raise ValueError("unsafe_provenance_url")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("unsafe_provenance_url") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise ValueError("unsafe_provenance_url")
    if port is not None and not 0 < port < 65536:
        raise ValueError("unsafe_provenance_url")
    if any(re.search(r"token|secret|signature|credential|password|api.?key|^sig$|^key$", key, re.IGNORECASE)
           for key, _ in parse_qsl(parsed.query)):
        raise ValueError("provenance_url_contains_sensitive_query")
    return parsed.hostname


def _retrieved_at(value: object) -> None:
    if not isinstance(value, str) or not _TIMESTAMP.fullmatch(value):
        raise ValueError("retrieved_at_requires_timezone_aware_iso_timestamp")
    if not value.endswith("Z") and (int(value[-5:-3]) > 23 or int(value[-2:]) > 59):
        raise ValueError("invalid_retrieved_at_timestamp_offset")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("missing timezone")
    except ValueError as exc:
        raise ValueError("invalid_retrieved_at_timestamp") from exc


def validate_provenance(manifest: object) -> None:
    """Validate complete captured multi-input metadata without certifying claims.

    Retrieval timestamps remain distinct from producer release/effective dates.
    Unknown producer release dates require an honest explanation; no date,
    license, attribution, delegation or authority is fabricated by validation.
    """
    if (not isinstance(manifest, dict) or type(manifest.get("schema_version")) is not int or manifest["schema_version"] != 1
            or manifest.get("processor") != "HIFLD Next" or not isinstance(manifest.get("inputs"), list) or not manifest["inputs"]):
        raise ValueError("missing_versioned_multi_input_provenance")
    validate_output_policy(manifest)
    try:
        json.dumps(manifest, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError("provenance_must_contain_finite_json_values") from exc
    _text(manifest.get("code_commit"), "missing_transformation_provenance")
    transforms = manifest.get("transformations")
    if not isinstance(transforms, list):
        raise ValueError("missing_transformation_provenance")
    for item in transforms:
        if isinstance(item, str):
            _text(item, "incomplete_transformation_provenance")
        elif isinstance(item, dict):
            _text(item.get("name") or item.get("operation"), "incomplete_transformation_provenance")
            if "parameters" in item and not isinstance(item["parameters"], dict):
                raise ValueError("invalid_transformation_parameters")
        else:
            raise ValueError("incomplete_transformation_provenance")
    if "manifest_layers" in manifest:
        layers = manifest["manifest_layers"]
        if not isinstance(layers, list) or not 0 < len(layers) <= 3:
            raise ValueError("invalid_authored_manifest_layers")
        layer_names = set()
        for layer in layers:
            if not isinstance(layer, dict) or not isinstance(layer.get("layer"), str) or layer["layer"] not in {"dataset", "file", "version"} or layer["layer"] in layer_names:
                raise ValueError("invalid_authored_manifest_layer_identity")
            layer_names.add(layer["layer"])
            if not isinstance(layer.get("sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", layer["sha256"]):
                raise ValueError("invalid_authored_manifest_layer_checksum")
            for name in ("snapshot", "manifest_key"):
                value = layer.get(name)
                if (not isinstance(value, str) or not value or PurePosixPath(value).is_absolute() or ".." in PurePosixPath(value).parts
                        or PurePosixPath(value).as_posix() != value):
                    raise ValueError("unsafe_authored_manifest_layer_path")
            if not layer["snapshot"].startswith("metadata/source/"):
                raise ValueError("unsafe_authored_manifest_layer_snapshot")
            if not isinstance(layer.get("input_ids"), list) or any(not isinstance(identity, str) for identity in layer["input_ids"]):
                raise ValueError("invalid_authored_manifest_layer_inputs")
    seen = set()
    for item in manifest["inputs"]:
        if not isinstance(item, dict):
            raise ValueError("incomplete_input_provenance")
        fields = ("input_id", "producer", "host", "canonical_url", "retrieval_url", "retrieved_at", "sha256", "license", "attribution")
        for field in fields:
            _text(item.get(field), "incomplete_input_provenance")
        identity = item["input_id"]
        if identity != identity.strip() or identity in seen:
            raise ValueError("conflicting_input_identity")
        seen.add(identity)
        if item["producer"].strip().casefold() == "hifld next":
            raise ValueError("processor_cannot_replace_original_input_producer")
        if not re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) or type(item.get("bytes")) is not int or item["bytes"] < 0:
            raise ValueError("invalid_input_checksum_or_length")
        validate_provenance_url(item["canonical_url"])
        retrieval_host = validate_provenance_url(item["retrieval_url"])
        host = item["host"]
        if host != host.strip() or host.casefold() != retrieval_host.casefold():
            raise ValueError("retrieval_host_does_not_match_input_host")
        if urlsplit(item["retrieval_url"]).fragment:
            raise ValueError("retrieval_url_cannot_contain_fragment")
        for field in INPUT_URL_FIELDS[2:]:
            if item.get(field) is not None:
                validate_provenance_url(item[field])
        _retrieved_at(item["retrieved_at"])
        if "http_status" in item and (type(item["http_status"]) is not int or not 200 <= item["http_status"] < 300):
            raise ValueError("failed_http_attempt_cannot_be_a_successful_source_input")
        if not isinstance(item.get("role"), str) or item["role"] not in _ROLES:
            raise ValueError("unresolved_source_authority")
        if "distributor" in item:
            _text(item["distributor"], "invalid_distributor_identity")
        delegation = item.get("delegation_evidence")
        if item["role"] == "official_distributor":
            if not isinstance(delegation, (dict, str)) or not delegation or (isinstance(delegation, str) and not delegation.strip()):
                raise ValueError("unverified_official_distribution")
        if isinstance(delegation, dict):
            for name, value in delegation.items():
                if not isinstance(name, str):
                    raise ValueError("invalid_delegation_evidence_fields")
                if name.endswith(("url", "href", "link")) and value is not None:
                    validate_provenance_url(value)
        release = item.get("release_date")
        if release is not None:
            _text(release, "invalid_producer_release_date")
            try:
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", release):
                    date.fromisoformat(release)
                else:
                    _retrieved_at(release)
            except ValueError as exc:
                raise ValueError("invalid_producer_release_date") from exc
        elif not isinstance(item.get("release_date_unknown_reason"), str) or not item["release_date_unknown_reason"].strip():
            raise ValueError("unexplained_unknown_release_date")
        if "release_date_unknown_reason" in item and item["release_date_unknown_reason"] is not None:
            _text(item["release_date_unknown_reason"], "invalid_unknown_release_date_explanation")
    if any(identity not in seen for layer in manifest.get("manifest_layers", []) for identity in layer["input_ids"]):
        raise ValueError("authored_manifest_layer_has_unresolved_input_identity")


PROVENANCE_FIELDS = ("schema_version", "processor", "inputs", "transformations", "code_commit", "lineage", "manifest_layers",
                     "unsupported_output_formats", "prebuilt_output_policy")


def provenance_projection(metadata: dict) -> dict | None:
    """Copy complete authored provenance without importing legacy fallback claims."""
    if "inputs" not in metadata and "processor" not in metadata:
        return None
    value = {key: metadata[key] for key in PROVENANCE_FIELDS if key in metadata}
    validate_provenance(value)
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
