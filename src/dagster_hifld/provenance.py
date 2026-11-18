"""Pure provenance shape validation; captured claims are never acceptance."""
from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import PurePosixPath
from urllib.parse import parse_qsl, urlsplit
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.literal_geojson_policy import literal_geojson_spec

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


# Optional evidence fields are additive to schema v1; absent legacy claims stay absent.
REFRESH_METADATA_FIELDS = ("coverage", "source_relationship", "documentation", "field_definitions", "format_transformations")
_INPUT_PURPOSES = {"data", "schema", "methodology", "license", "geocoding"}


def _input_refs(value: object, identities: set[str]) -> None:
    if not isinstance(value, list) or any(not isinstance(ref, str) or ref not in identities for ref in value):
        raise ValueError("unresolved_metadata_input_reference")


def validate_refresh_metadata(manifest: dict) -> None:
    """Validate optional claims only after manifest layers have been resolved."""
    identities = {item["input_id"] for item in manifest["inputs"]}
    for item in manifest["inputs"]:
        if "purpose" in item and (not isinstance(item["purpose"], str) or item["purpose"] not in _INPUT_PURPOSES):
            raise ValueError("invalid_input_purpose")
        if "refresh" not in item:
            continue
        refresh = item["refresh"]
        if not isinstance(refresh, dict):
            raise ValueError("invalid_input_refresh")
        status = refresh.get("maintenance_status")
        if not isinstance(status, str) or status not in {"maintained", "unverified", "retired"}:
            raise ValueError("invalid_maintenance_status")
        for field in ("maintenance_evidence_input_id", "latest_evidence_input_id"):
            if field in refresh:
                _input_refs([refresh[field]], identities)
        if status == "maintained" and "maintenance_evidence_input_id" not in refresh:
            raise ValueError("maintained_source_requires_evidence")
        if "latest_checked_at" in refresh:
            _retrieved_at(refresh["latest_checked_at"])
            if "latest_evidence_input_id" not in refresh:
                raise ValueError("latest_check_requires_evidence")
        for field in ("source_release_id", "data_period", "update_cadence", "selection_rule", "source_state_fingerprint"):
            if field in refresh:
                if field == "source_release_id" and refresh[field] is None:
                    reasons = refresh.get("unknown_reasons")
                    _text(reasons.get(field) if isinstance(reasons, dict) else None,
                          "unknown_source_release_id_requires_reason")
                    continue
                _text(refresh[field], "invalid_refresh_text")
        reasons = refresh.get("unknown_reasons", {})
        if not isinstance(reasons, dict):
            raise ValueError("invalid_refresh_unknown_reasons")
        for field, reason in reasons.items():
            _text(field, "invalid_refresh_unknown_reason")
            _text(reason, "invalid_refresh_unknown_reason")
        if status == "unverified" and not reasons:
            raise ValueError("unverified_maintenance_requires_unknown_reason")
    if "coverage" in manifest and not isinstance(manifest["coverage"], dict):
        raise ValueError("invalid_source_coverage")
    if "source_relationship" in manifest:
        relation = manifest["source_relationship"]
        if not isinstance(relation, dict):
            raise ValueError("invalid_source_relationship")
        name = relation.get("relationship")
        if not isinstance(name, str) or name not in {"equivalent", "changed_successor", "partial_successor", "unverified"}:
            raise ValueError("invalid_source_relationship")
        _input_refs(relation.get("evidence_input_ids"), identities)
        if name != "unverified" and not relation["evidence_input_ids"]:
            raise ValueError("source_relationship_requires_evidence")
        if name == "unverified":
            _text(relation.get("unknown_reason"), "unverified_relationship_requires_reason")
    if "documentation" in manifest:
        documents = manifest["documentation"]
        if not isinstance(documents, list):
            raise ValueError("invalid_documentation")
        paths = set()
        for doc in documents:
            if not isinstance(doc, dict):
                raise ValueError("invalid_documentation")
            path = doc.get("path")
            if (not isinstance(path, str) or not path or path in paths or "\\" in path
                    or any(c in path for c in "?#%") or PurePosixPath(path).is_absolute()
                    or ".." in PurePosixPath(path).parts or PurePosixPath(path).as_posix() != path
                    or not path.startswith(("metadata/", "raw/"))):
                raise ValueError("unsafe_documentation_path")
            paths.add(path)
            _input_refs([doc.get("input_id")], identities)
            if not isinstance(doc.get("purpose"), str) or doc["purpose"] not in _INPUT_PURPOSES:
                raise ValueError("invalid_documentation_purpose")
            media_type = _text(doc.get("media_type"), "invalid_documentation_media_type")
            if not re.fullmatch(r"[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+(?:;[^\r\n]+)?", media_type):
                raise ValueError("invalid_documentation_media_type")
            if (type(doc.get("bytes")) is not int or doc["bytes"] < 0 or not isinstance(doc.get("sha256"), str)
                    or not re.fullmatch(r"[0-9a-f]{64}", doc["sha256"])):
                raise ValueError("invalid_documentation_checksum_or_length")
    if "field_definitions" in manifest:
        definitions = manifest["field_definitions"]
        if not isinstance(definitions, dict):
            raise ValueError("invalid_field_definitions")
        for name, definition in definitions.items():
            _text(name, "invalid_field_definition_name")
            if not isinstance(definition, dict):
                raise ValueError("invalid_field_definition")
            basis = definition.get("basis")
            if not isinstance(basis, str) or basis not in {"publisher", "derived", "unknown"}:
                raise ValueError("invalid_field_definition_basis")
            _input_refs(definition.get("input_ids"), identities)
            if basis != "unknown" and not definition["input_ids"]:
                raise ValueError("field_definition_requires_evidence")
            fields = definition.get("source_fields")
            if not isinstance(fields, list) or any(not isinstance(field, str) or not field.strip() for field in fields):
                raise ValueError("invalid_field_definition_source_fields")
            for field in ("description", "units", "null_meaning", "identifier_meaning"):
                if field in definition:
                    _text(definition[field], "invalid_field_definition_text")
            for field in ("codes", "sentinel_values"):
                if field in definition and not isinstance(definition[field], dict):
                    raise ValueError("invalid_field_definition_codes")
    if "format_transformations" in manifest:
        formats = manifest["format_transformations"]
        if not isinstance(formats, dict):
            raise ValueError("invalid_format_transformations")
        for name, operations in formats.items():
            _text(name, "invalid_transformation_format")
            if not isinstance(operations, list):
                raise ValueError("invalid_format_transformations")
            for operation in operations:
                if not isinstance(operation, dict):
                    raise ValueError("invalid_format_transformation")
                _text(operation.get("name"), "invalid_format_transformation")
                _input_refs(operation.get("input_ids"), identities)


def enrich_dictionary_columns(columns: list[dict], metadata: dict | None) -> list[dict]:
    """Attach evidenced semantics without touching native types or statistics."""
    definitions = (metadata or {}).get("field_definitions", {})
    for column in columns:
        definition = definitions.get(column["name"])
        if definition is not None:
            column["semantic_definition"] = json.loads(json.dumps(definition))
            if "description" in definition:
                column["description"] = definition["description"]
        # Observed examples/frequencies are not publisher-declared value domains.
        if "possibleValues" in column:
            column["valuesBasis"] = "observed_values_not_publisher_domain"
    return columns


def validate_dictionary_semantics(dictionary: dict) -> None:
    """Reject column projections that lose or change authored semantic evidence."""
    definitions = dictionary.get("field_definitions", {})
    for column in dictionary.get("columns", []):
        expected = definitions.get(column["name"])
        if expected is not None and (column.get("semantic_definition") != expected
                or ("description" in expected and column.get("description") != expected["description"])):
            raise ValueError("dictionary_column_semantics_differ_from_evidenced_definition")


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
    literal_geojson_spec(manifest)
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
    validate_refresh_metadata(manifest)
    if any(identity not in seen for layer in manifest.get("manifest_layers", []) for identity in layer["input_ids"]):
        raise ValueError("authored_manifest_layer_has_unresolved_input_identity")


PROVENANCE_FIELDS = ("schema_version", "processor", "inputs", "transformations", "code_commit", "processing_code", "code_identity_scheme", "lineage", "documentation_url", "manifest_layers",
                     "unsupported_output_formats", "prebuilt_output_policy", "literal_geojson_catalog", *REFRESH_METADATA_FIELDS)


def provenance_projection(metadata: dict) -> dict | None:
    """Copy complete authored provenance without importing legacy fallback claims."""
    if "inputs" not in metadata and "processor" not in metadata:
        if any(field in metadata for field in REFRESH_METADATA_FIELDS):
            raise ValueError("refresh_metadata_requires_input_provenance")
        return None
    value = {key: metadata[key] for key in PROVENANCE_FIELDS if key in metadata}
    validate_provenance(value)
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
