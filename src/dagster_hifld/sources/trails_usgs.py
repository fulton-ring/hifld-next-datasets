"""Draft-only capture of the complete observed USGS The National Map Trails layer.

The USGS Transportation service (carto.nationalmap.gov) is the USGS source
candidate for the HIFLD inventory entry ``trails-2``.  Equivalence, producer
release date, rights, coverage, and migration decisions remain unapproved.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import re
import shutil
import stat
import struct
from datetime import datetime, timezone
from pathlib import Path
from time import time_ns
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.capture_completeness import validate_capture_witness
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.sources.arcgis_layer import _validate_offset_replay_refusal, load_replay, write_outputs
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.capture_common import (
    capture_get, processing_identity, candidate_prefix,
    content_derived_version, seal_ready_candidate, verify_cached_candidate,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.sources.arcgis_capture_sequence import capture_request_steps, retry_capture_request

DATASET_SLUG = FILE_SLUG = "trails-2"
PUBLISHER = "U.S. Geological Survey, The National Map Transportation"
SERVICE_URL = "https://carto.nationalmap.gov/arcgis/rest/services/transportation/MapServer"
LAYER_ID = 37
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
BATCH_SIZE = 2000
MAX_RESPONSE_BYTES = 64_000_000
MAX_CAPTURE_BYTES = 8_000_000_000
DISK_FLOOR_BYTES = 8_000_000_000
EXPECTED_FIELD_NAMES = [
    "ohvisorunder50inches", "osvm", "icewatercraft", "ebike", "electricwatercraft",
    "livestock", "maplabel", "pets", "publisheddate", "routetype", "seasonopen",
    "sourceeditdate", "trailsurface", "objectid", "permanentidentifier", "name",
    "namealternate", "trailnumber", "trailnumberalternate", "sourcefeatureid",
    "sourcedatasetid", "sourcedatadecscription", "sourceoriginator", "loaddate",
    "trailtype", "hikerpedestrian", "bicycle", "packsaddle", "motorcycle",
    "ohvover50inches", "snowshoe", "crosscountryski", "dogsled",
    "nonmotorizedwatercraft", "primarytrailmaintainer", "nationaltraildesignation",
    "lengthmiles", "networklength", "globalid",
]
EXPECTED_CRS = CRS.from_epsg(3857)
STRING_TYPES = {"esriFieldTypeString", "esriFieldTypeGUID", "esriFieldTypeGlobalID"}
ARROW_TYPES = {
    "esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int64(),
    "esriFieldTypeSmallInteger": pa.int32(), "esriFieldTypeString": pa.string(),
    "esriFieldTypeGUID": pa.string(), "esriFieldTypeGlobalID": pa.string(),
    "esriFieldTypeDate": pa.timestamp("ms", tz="UTC"),
    "esriFieldTypeDouble": pa.float64(), "esriFieldTypeSingle": pa.float32(),
}
ESRI_TO_ARROW_TYPE = {
    "esriFieldTypeOID": "int64", "esriFieldTypeInteger": "int64",
    "esriFieldTypeSmallInteger": "int32", "esriFieldTypeString": "string",
    "esriFieldTypeGUID": "string", "esriFieldTypeGlobalID": "string",
    "esriFieldTypeDate": "timestamp[ms, UTC]", "esriFieldTypeDouble": "float64",
    "esriFieldTypeSingle": "float32",
}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _processing_identity(repo_root: Path | None = None) -> dict[str, object]:
    root = Path(repo_root) if repo_root is not None else Path(__file__).parents[3]
    identity = processing_identity(root)
    validate_construction_code_identity(root, identity["code_commit"], identity["files"], identity=identity)
    return identity


_RETRY_ATTEMPTS = 4
_RETRY_BASE_DELAY_SECONDS = 5.0

# Refused-attempt evidence lives under ``refused/`` inside the capture root:
# the successful capture roster must contain only the completed artifacts and
# the manifest, so refused attempts are moved into a validated separate
# directory (whose naming, membership and hash parity ``_load`` enforces) and
# their bytes are counted against the capture budget.
_REFUSED_DIR = "refused"
_REFUSED_ENTRY = re.compile(
    r"(?P<name>.+)\.error-(?P<stamp>[0-9]+)\.(?P<suffix>body|wire|receipt\.json)\Z")


def _witness_refused(root: Path, name: str) -> int:
    """Move a retained refused attempt aside so the slot can be refetched.

    Returns the number of retained refused bytes, which the caller counts
    against the capture budget so retained evidence is never free.
    """
    stamp = time_ns()
    directory = root / _REFUSED_DIR
    directory.mkdir(exist_ok=True)
    moved = 0
    for suffix in ("body", "wire", "receipt.json"):
        source = root / f"{name}.{suffix}"
        if source.exists():
            moved += source.stat().st_size
            source.replace(directory / f"{name}.error-{stamp}.{suffix}")
    return moved


def _capture(client: httpx.Client, url: str, root: Path, name: str,
             retained: int) -> tuple[bytes, dict, int]:
    if type(retained) is not int or retained < 0 or retained > MAX_CAPTURE_BYTES:
        raise ValueError("trails_capture_budget_invalid")
    refused_bytes = [0]

    def reserve(progress) -> None:
        prospective = (retained + refused_bytes[0] + progress.wire_bytes_retained
                       + progress.decoded_bytes_retained + progress.reserve_bytes)
        if prospective > MAX_CAPTURE_BYTES:
            raise OSError("trails_capture_budget_exceeded")

    # The producer intermittently drops mid-page transports (observed on a
    # full-width 2,000-record page); incomplete captures are witnessed aside
    # under refused/ and retried a bounded number of times before the capture
    # fails closed, so a capture that succeeds on a later attempt stays
    # replayable.
    def preserve_failed(exc, attempt):
        refused_bytes[0] += _witness_refused(root, name)

    body, receipt = retry_capture_request(
        lambda attempt: capture_get(client, url, root, name,
                                    max_bytes=MAX_RESPONSE_BYTES, reserve=reserve),
        attempts=_RETRY_ATTEMPTS,
        schedule=tuple(_RETRY_BASE_DELAY_SECONDS * attempt for attempt in range(_RETRY_ATTEMPTS - 1)),
        retryable=lambda exc, attempt: isinstance(exc, ValueError)
            and str(exc).startswith("incomplete_source_capture:"),
        preserve_failed=preserve_failed,
    )
    return body, receipt, refused_bytes[0]


def _capture_into(captures: dict, client: httpx.Client, url: str, root: Path, name: str,
                  retained: int) -> int:
    """Run one capture, retain it, and return the bytes to add to the budget.

    The returned total includes the artifact's own refused-attempt evidence so
    retained retry evidence is never free.
    """
    body, receipt, refused = _capture(client, url, root, name, retained)
    captures[name] = (body, receipt)
    return refused + receipt["bytes"] + receipt["wire_bytes"]


def _validate_refused_evidence(directory: Path, accepted: dict[str, tuple[bytes, dict]]) -> dict:
    """Bind each refused triple to a failed attempt of its accepted GET."""
    groups: dict[tuple[str, str], dict[str, Path]] = {}
    for entry in directory.iterdir():
        match = _REFUSED_ENTRY.fullmatch(entry.name)
        if not match or match.group("name") not in accepted:
            raise ValueError("trails_refused_evidence_invalid")
        slot = groups.setdefault((match.group("name"), match.group("stamp")), {})
        slot[match.group("suffix")] = entry
    if not groups:
        raise ValueError("trails_refused_evidence_invalid")
    roster = {}
    for (name, stamp), slot in groups.items():
        if set(slot) != {"body", "wire", "receipt.json"}:
            raise ValueError("trails_refused_evidence_invalid")
        receipt_body = slot["receipt.json"].read_bytes()
        try:
            receipt = json.loads(receipt_body)
            if (not isinstance(receipt, dict) or type(receipt.get("bytes")) is not int
                    or type(receipt.get("wire_bytes")) is not int
                    or (receipt.get("complete_body") is True and receipt.get("status") == 200)):
                raise ValueError("trails_refused_evidence_invalid")
            _validate_offset_replay_refusal(directory, f"{name}.error-{stamp}",
                accepted[name][1]["url"], max_capture_bytes=MAX_RESPONSE_BYTES)
        except ValueError as exc:
            raise ValueError("trails_refused_evidence_invalid") from exc
        roster.update({slot["body"].name: {"bytes": receipt["bytes"], "sha256": receipt["sha256"]},
                       slot["wire"].name: {"bytes": receipt["wire_bytes"], "sha256": receipt["wire_sha256"]},
                       slot["receipt.json"].name: {"bytes": len(receipt_body), "sha256": _sha(receipt_body)}})
    return roster


def _artifact_rosters(root: Path) -> dict:
    refused = root / _REFUSED_DIR
    return {"artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json", _REFUSED_DIR}),
            "refused_roster": capture_roster_digests(refused) if refused.is_dir() else {}}


def _load(root: Path) -> dict[str, tuple[bytes, dict]]:
    """Replay bounded captures, including original pre-roster manifests.

    Legacy manifests retain receipt/hash and wire-decoding checks; only newer
    manifests carry separate accepted/refused artifact-roster digests.
    """
    if any(path.is_symlink() for path in (root, *root.parents)) or not root.is_dir():
        raise ValueError("trails_capture_roster_invalid")
    files = []
    actual = set()
    refused_dir = root / _REFUSED_DIR
    for entry in root.iterdir():
        actual.add(entry.name)
        if entry.name == _REFUSED_DIR:
            if entry.is_symlink() or not entry.is_dir() or not any(entry.iterdir()):
                raise ValueError("trails_capture_roster_invalid")
            for refused in entry.iterdir():
                if not stat.S_ISREG(refused.lstat().st_mode):
                    raise ValueError("trails_refused_evidence_invalid")
                files.append(refused)
        else:
            if not stat.S_ISREG(entry.lstat().st_mode):
                raise ValueError("trails_capture_roster_invalid")
            files.append(entry)
    sizes = {path: path.stat().st_size for path in files}
    if sum(sizes.values()) > MAX_CAPTURE_BYTES:
        raise ValueError("trails_capture_budget_exceeded")
    if any(size > MAX_RESPONSE_BYTES for path, size in sizes.items()
           if path.parent == root or not path.name.endswith(".wire")):
        raise ValueError("trails_response_budget_exceeded")
    manifest = json.loads((root / "capture_manifest.json").read_bytes())
    if (manifest.get("complete") is not True or not isinstance(manifest.get("artifact_names"), list)
            or not isinstance(manifest.get("receipts"), dict)):
        raise ValueError("trails_capture_manifest_invalid")
    names = manifest["artifact_names"]
    if (any(not isinstance(name, str) or Path(name).name != name or name in {".", ".."} for name in names)
            or len(set(names)) != len(names) or set(manifest["receipts"]) != set(names)):
        raise ValueError("trails_capture_roster_invalid")
    expected = {"capture_manifest.json"} | {
        f"{name}.{suffix}" for name in names for suffix in ("body", "wire", "receipt.json")}
    has_refused = _REFUSED_DIR in actual
    if has_refused:
        expected.add(_REFUSED_DIR)
    if actual != expected:
        raise ValueError("trails_capture_roster_invalid")
    result, roster = {}, {}
    for name in names:
        body, receipt = load_replay(root, name)
        if receipt != manifest["receipts"][name]:
            raise ValueError("trails_capture_receipt_mismatch:" + name)
        result[name] = (body, receipt)
        receipt_body = (root / f"{name}.receipt.json").read_bytes()
        roster.update({f"{name}.body": {"bytes": len(body), "sha256": _sha(body)},
                       f"{name}.wire": {"bytes": receipt["wire_bytes"], "sha256": receipt["wire_sha256"]},
                       f"{name}.receipt.json": {"bytes": len(receipt_body), "sha256": _sha(receipt_body)}})
    refused_roster = _validate_refused_evidence(refused_dir, result) if has_refused else {}
    if "artifact_roster" in manifest and (manifest["artifact_roster"] != roster
            or manifest.get("refused_roster") != refused_roster):
        raise ValueError("trails_capture_artifact_roster_mismatch")
    return result


def _sr(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError("trails_spatial_reference_missing:" + label)
    wkids = {v for k, v in value.items() if k in {"wkid", "latestWkid"}}
    if not wkids or not wkids.issubset({3857, 102100}):
        raise ValueError("trails_spatial_reference_drift:" + label)


def _validate_page_metadata(page: dict, label: str) -> None:
    if page.get("geometryType") != "esriGeometryPolyline":
        raise ValueError("trails_page_geometry_type_drift:" + label)
    _sr(page.get("spatialReference"), "page:" + label)


def _validate_polyline_geometry(geometry: object) -> None:
    if not isinstance(geometry, dict):
        raise ValueError("trails_polyline_geometry_drift")
    if "z" in geometry or "m" in geometry:
        raise ValueError("trails_unhandled_z_or_m_ordinate")
    if not set(geometry) <= {"paths", "hasM", "hasZ", "spatialReference"} or "paths" not in geometry:
        raise ValueError("trails_polyline_geometry_drift")
    paths = geometry["paths"]
    if not isinstance(paths, list) or not paths:
        raise ValueError("trails_polyline_geometry_drift")
    for path in paths:
        if not isinstance(path, list) or not path:
            raise ValueError("trails_polyline_geometry_drift")
        for point in path:
            if not isinstance(point, list) or len(point) not in (2, 3):
                raise ValueError("trails_polyline_geometry_drift")
            if len(point) == 3 and point[2] is not None:
                # The layer declares hasM: true (observed 2026-11-14), so a
                # populated served m ordinate is within the producer's own
                # declared schema and is retained verbatim; it must still be a
                # finite number. Any served z fails closed.
                if type(point[2]) not in (int, float):
                    raise ValueError("trails_polyline_geometry_drift")
                if not math.isfinite(point[2]):
                    raise ValueError("trails_nonfinite_geometry")
            if type(point[0]) not in (int, float) or type(point[1]) not in (int, float):
                raise ValueError("trails_polyline_geometry_drift")
            if not math.isfinite(point[0]) or not math.isfinite(point[1]):
                raise ValueError("trails_nonfinite_geometry")
    if geometry.get("hasZ"):
        raise ValueError("trails_unhandled_z_or_m_ordinate")
    if geometry.get("spatialReference") is not None:
        _sr(geometry["spatialReference"], "feature")


def _validate_dimensions_probe(probe: dict, expected_oid: int) -> None:
    _validate_page_metadata(probe, "dimensions_probe")
    features = probe.get("features")
    if not isinstance(features, list) or len(features) != 1:
        raise ValueError("trails_dimensions_probe_population_drift")
    attributes = features[0].get("attributes")
    if (not isinstance(attributes, dict)
            or attributes.get("objectid") != expected_oid):
        raise ValueError("trails_dimensions_probe_population_drift")
    _validate_polyline_geometry(features[0].get("geometry"))


# The before/after layer gate pins the data-contract projection of the layer
# document. The producer rotates the document's drawingInfo colorRamp
# presentation within a capture window (observed 2026-11-14: colorRamp colors
# changed across a 34-minute live capture while every data-contract member and
# the whole service document stayed byte-identical), so whole-document
# canonical equality can never hold reliably for this continuously-updated
# layer; presentation metadata stays retained verbatim in the capture but is
# excluded from the drift gate.
_LAYER_CONTRACT_MEMBERS = ("id", "name", "type", "geometryType", "hasM", "hasZ",
                           "extent", "fields", "geometryField", "maxRecordCount",
                           "supportedQueryFormats")


def _layer_contract(layer: dict) -> dict:
    return {member: layer.get(member) for member in _LAYER_CONTRACT_MEMBERS}


def _layer_contract_unchanged(layer_before: dict, layer_after: dict) -> None:
    if _canonical(_layer_contract(layer_before)) != _canonical(_layer_contract(layer_after)):
        raise ValueError("trails_layer_changed_during_capture")


def _fields(layer: dict) -> list[dict]:
    fields = layer.get("fields")
    if (layer.get("id") != LAYER_ID or layer.get("name") != "Trails"
            or layer.get("geometryType") != "esriGeometryPolyline"
            or layer.get("hasM") is not True
            or not isinstance(fields, list)
            or [field.get("name") for field in fields] != [*EXPECTED_FIELD_NAMES, "shape", "shape_Length"]
            or layer.get("geometryField", {}).get("name") != "shape"):
        raise ValueError("trails_layer_schema_drift")
    extent = layer.get("extent")
    if not isinstance(extent, dict):
        raise ValueError("trails_layer_extent_missing")
    _sr(extent.get("spatialReference"), "layer_extent")
    attribute_fields = [field for field in fields if field.get("type") != "esriFieldTypeGeometry"]
    for field in attribute_fields:
        if field.get("type") not in ARROW_TYPES:
            raise ValueError("trails_unsupported_field_type:" + str(field.get("name")))
    return attribute_fields


def _validate_request_scope(name: str, receipt: dict, ids: list[int]) -> None:
    request_url = receipt.get("url")
    final_url = receipt.get("final_url", request_url)
    if final_url != request_url:
        raise ValueError("trails_capture_request_scope_drift:" + name)
    for url in (request_url, final_url):
        if not isinstance(url, str):
            raise ValueError("trails_capture_url_missing:" + name)
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != "carto.nationalmap.gov":
            raise ValueError("trails_capture_endpoint_drift:" + name)
    parsed = urlsplit(request_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    if name.startswith("service_"):
        expected_path, expected = urlsplit(SERVICE_URL).path, {"f": ["pjson"]}
    elif name.startswith("layer_"):
        expected_path, expected = urlsplit(LAYER_URL).path, {"f": ["pjson"]}
    elif name.startswith("count_"):
        expected_path, expected = urlsplit(LAYER_URL).path + "/query", {
            "where": ["1=1"], "returnCountOnly": ["true"], "f": ["json"]}
    elif name.startswith("ids_"):
        expected_path, expected = urlsplit(LAYER_URL).path + "/query", {
            "where": ["1=1"], "returnIdsOnly": ["true"], "f": ["json"]}
    elif name == "dimensions_probe":
        if not ids:
            raise ValueError("trails_dimensions_probe_without_ids")
        expected_path, expected = urlsplit(LAYER_URL).path + "/query", {
            "objectIds": [str(ids[0])], "outFields": ["OBJECTID"], "returnGeometry": ["true"],
            "returnZ": ["true"], "returnM": ["true"], "f": ["json"]}
    elif name.startswith("page_"):
        start = int(name.removeprefix("page_"))
        page_ids = ids[start:start + BATCH_SIZE]
        if not page_ids:
            raise ValueError("trails_unexpected_page:" + name)
        # Pages walk the sorted roster with resultOffset/resultRecordCount
        # pagination in producer order: a 2,000-id objectIds list is a ~20k
        # character URI and the server answers it with HTTP 414 (observed
        # 2026-11-14), while the equivalent resultOffset page serves the same
        # ordered slice.
        expected_path, expected = urlsplit(LAYER_URL).path + "/query", {
            "where": ["1=1"], "outFields": ["*"], "returnGeometry": ["true"],
            "returnZ": ["true"], "returnM": ["true"],
            "orderByFields": ["objectid ASC"],
            "resultOffset": [str(start)], "resultRecordCount": [str(BATCH_SIZE)],
            "f": ["json"]}
    else:
        raise ValueError("trails_unexpected_capture_name:" + name)
    if parsed.path != expected_path or params != expected:
        raise ValueError("trails_capture_request_scope_drift:" + name)


def _population(captures: dict[str, tuple[bytes, dict]]) -> tuple[dict, dict]:
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    layer = docs["layer_before"]
    fields = _fields(layer)
    _layer_contract_unchanged(layer, docs["layer_after"])
    if docs["service_before"] != docs["service_after"]:
        raise ValueError("trails_service_changed_during_capture")
    ids_before = docs["ids_before"].get("objectIds")
    ids_after = docs["ids_after"].get("objectIds")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    ids_before = validate_capture_witness(count_before, ids_before, count_after, ids_after)
    if not ids_before:
        raise ValueError("trails_dimensions_probe_without_ids")
    expected_page_names = {f"page_{start:05d}" for start in range(0, len(ids_before), BATCH_SIZE)}
    expected_names = expected_page_names | {"service_before", "service_after", "layer_before", "layer_after",
        "count_before", "count_after", "ids_before", "ids_after", "dimensions_probe"}
    if set(docs) != expected_names:
        raise ValueError("trails_page_receipt_roster_drift")
    if "dimensions_probe" not in docs:
        raise ValueError("trails_dimensions_probe_missing")
    for name, (_body, receipt) in captures.items():
        _validate_request_scope(name, receipt, ids_before)
    _validate_dimensions_probe(docs["dimensions_probe"], ids_before[0])
    names = [f["name"] for f in fields]
    names_lower = [name.lower() for name in names]
    record_sha = hashlib.sha256()
    geometry_sha = hashlib.sha256()
    bounds = [math.inf, math.inf, -math.inf, -math.inf]
    missing_geometry = []
    populated_m_features = 0
    null_counts = {name: 0 for name in names}
    seen = set()
    page_names = sorted(n for n in docs if n.startswith("page_"))
    for name in page_names:
        page = docs[name]
        _validate_page_metadata(page, name)
        rows = page.get("features")
        start = int(name.rsplit("_", 1)[1])
        expected = ids_before[start:start + BATCH_SIZE]
        # A resultOffset page at the producer's record cap is exactly full and
        # the server flags more rows with exceededTransferLimit; only a page
        # below the cap claiming more rows is a truncation.
        if (not isinstance(rows, list) or len(rows) != len(expected)
                or (docs[name].get("exceededTransferLimit") and len(rows) != BATCH_SIZE)):
            raise ValueError("trails_page_population_incomplete:" + name)
        for feature in rows:
            raw_attrs = feature.get("attributes")
            # Esri field names are case-insensitive and the producer serves
            # 'shape_length' for the declared 'shape_Length'; compare the
            # served key order case-insensitively and re-key each feature by
            # the declared name.
            raw_lower = ({str(key).lower(): value for key, value in raw_attrs.items()}
                         if isinstance(raw_attrs, dict) else {})
            if list(raw_lower) != names_lower:
                raise ValueError("trails_attribute_roster_drift")
            attrs = {name: raw_lower[name.lower()] for name in names}
            feature["attributes"] = attrs
            oid = attrs["objectid"]
            if type(oid) is not int or oid not in expected or oid in seen:
                raise ValueError("trails_native_id_drift")
            seen.add(oid)
            for field in fields:
                key, value, kind = field["name"], attrs[field["name"]], field["type"]
                if value is None:
                    if field.get("nullable") is False:
                        raise ValueError("trails_nonnullable_null:" + key)
                    null_counts[key] += 1
                elif kind in STRING_TYPES and not isinstance(value, str):
                    raise ValueError("trails_attribute_type_drift:" + key)
                elif kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"} and type(value) is not int:
                    raise ValueError("trails_attribute_type_drift:" + key)
                elif kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"} and (type(value) not in (int, float) or not math.isfinite(value)):
                    raise ValueError("trails_attribute_type_drift:" + key)
                elif kind == "esriFieldTypeDate" and type(value) is not int:
                    raise ValueError("trails_attribute_type_drift:" + key)
            record_sha.update(_canonical(attrs) + b"\n")
            geometry = feature.get("geometry")
            if geometry is None:
                missing_geometry.append(oid)
            else:
                _validate_polyline_geometry(geometry)
                if _populated_m(geometry["paths"]):
                    populated_m_features += 1
                for path in geometry["paths"]:
                    for point in path:
                        bounds[0] = min(bounds[0], point[0]); bounds[1] = min(bounds[1], point[1])
                        bounds[2] = max(bounds[2], point[0]); bounds[3] = max(bounds[3], point[1])
            feature["geometry"] = geometry
            geometry_sha.update(_canonical(geometry) + b"\n")
    if seen != set(ids_before) or len(seen) != len(ids_before):
        raise ValueError("trails_feature_roster_mismatch")
    audit = {
        "feature_count": len(seen), "missing_geometry_objectids": missing_geometry,
        "populated_m_feature_count": populated_m_features,
        "id_sha256": _sha(_canonical(sorted(seen))), "schema_sha256": _sha(_canonical(fields)),
        "record_sha256": record_sha.hexdigest(), "geometry_sha256": geometry_sha.hexdigest(),
        "native_crs": "EPSG:3857", "native_bounds": bounds,
        "field_names": names, "source_field_declarations": fields,
        "null_counts": null_counts,
    }
    return layer, audit


# ISO WKB dimension offsets: base geometry type + 1000 (Z) / 2000 (M).
_WKB_MULTILINESTRING = 5
_WKB_LINESTRING = 2
_WKB_M_OFFSET = 2000


def _populated_m(paths: list) -> bool:
    """True when any served vertex of the feature carries a populated m ordinate."""
    return any(len(point) == 3 and point[2] is not None for path in paths for point in path)


def _wkb_polyline(paths: list) -> bytes:
    # Served third ordinates are retained verbatim: a feature whose served m
    # ordinates are all null is emitted as plain 2D WKB (the served nulls
    # carry no value), while a feature with any populated m ordinate is
    # emitted as WKB MultiLineString M with the served values verbatim (a
    # served null inside such a feature encodes as NaN, the WKB null-m
    # convention).
    has_m = _populated_m(paths)
    out = bytearray()
    out += struct.pack("<BI", 1, _WKB_MULTILINESTRING + (_WKB_M_OFFSET if has_m else 0))
    out += struct.pack("<I", len(paths))
    for path in paths:
        out += struct.pack("<BI", 1, _WKB_LINESTRING + (_WKB_M_OFFSET if has_m else 0))
        out += struct.pack("<I", len(path))
        for point in path:
            if has_m:
                m = float(point[2]) if len(point) == 3 and point[2] is not None else math.nan
                out += struct.pack("<ddd", float(point[0]), float(point[1]), m)
            else:
                out += struct.pack("<dd", float(point[0]), float(point[1]))
    return bytes(out)


def _geometry_types(features: list[dict]) -> list[str]:
    types = set()
    for feature in features:
        geometry = feature.get("geometry")
        if geometry is None:
            continue
        types.add("MultiLineString M" if _populated_m(geometry["paths"]) else "MultiLineString")
    return sorted(types)


def _parquet(features: list[dict], fields: list[dict], count: int) -> bytes:
    arrays, schema = [], []
    for field in fields:
        name, kind = field["name"], field["type"]
        vals = [feature["attributes"][name] for feature in features]
        dtype = ARROW_TYPES[kind]
        if kind == "esriFieldTypeDate":
            vals = [None if value is None else datetime.fromtimestamp(value / 1000, tz=timezone.utc) for value in vals]
        if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
            vals = [None if value is None else float(value) for value in vals]
        arrays.append(pa.array(vals, type=dtype, safe=True))
        schema.append(pa.field(name, dtype, nullable=field.get("nullable") is not False))
    geom = []
    for feature in features:
        polyline = feature.get("geometry")
        geom.append(None if polyline is None else _wkb_polyline(polyline["paths"]))
    arrays.append(pa.array(geom, type=pa.binary()))
    schema.append(pa.field("geometry", pa.binary(), nullable=True))
    coords = [p[:2] for f in features if f.get("geometry") is not None
              for path in f["geometry"]["paths"] for p in path]
    bbox = [min(p[0] for p in coords), min(p[1] for p in coords), max(p[0] for p in coords), max(p[1] for p in coords)]
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": _geometry_types(features), "crs": EXPECTED_CRS.to_json_dict(), "bbox": bbox}}}
    table = pa.Table.from_arrays(arrays, schema=pa.schema(schema, metadata={b"geo": _canonical(geo)}))
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    body = sink.getvalue()
    readback = pq.read_table(pa.BufferReader(body))
    if readback.num_rows != count or readback.schema.names != [f["name"] for f in fields] + ["geometry"]:
        raise ValueError("trails_parquet_readback_schema_drift")
    if readback.column("objectid").to_pylist() != [f["attributes"]["objectid"] for f in features]:
        raise ValueError("trails_parquet_oid_parity_failure")
    for field in fields:
        name, kind = field["name"], field["type"]
        expected = [feature["attributes"][name] for feature in features]
        if kind == "esriFieldTypeDate":
            expected = [None if value is None else datetime.fromtimestamp(value / 1000, tz=timezone.utc)
                        for value in expected]
        elif kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
            expected = [None if value is None else float(value) for value in expected]
        if readback.column(name).to_pylist() != expected:
            raise ValueError("trails_parquet_attribute_parity_failure:" + name)
    expected_wkb = [None if feature.get("geometry") is None else _wkb_polyline(feature["geometry"]["paths"])
                    for feature in features]
    if readback.column("geometry").to_pylist() != expected_wkb:
        raise ValueError("trails_parquet_native_wkb_parity_failure")
    metadata = json.loads(readback.schema.metadata[b"geo"])
    declared_crs = CRS.from_json_dict(metadata["columns"]["geometry"]["crs"])
    if (metadata["columns"]["geometry"].get("encoding") != "WKB"
            or metadata["columns"]["geometry"].get("geometry_types") != _geometry_types(features)
            or not declared_crs.equals(EXPECTED_CRS, ignore_axis_order=True)):
        raise ValueError("trails_parquet_crs_or_encoding_drift")
    return body


def capture_trails(storage: StagingStorageResource, *, version: str,
                   capture_directory: str | None = None) -> dict[str, object]:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("trails_isolated_local_staging_required")
    if not isinstance(version, str) or not version or Path(version).name != version or version in {".", ".."}:
        raise ValueError("trails_unsafe_version")
    staging = Path(storage.local_dir).absolute()
    if any(p.is_symlink() for p in (staging, *staging.parents)):
        raise ValueError("trails_unsafe_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < DISK_FLOOR_BYTES:
        raise ValueError("trails_disk_floor_before_capture")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        captures = _load(root)
    else:
        root = ((requested / version) if requested else default_capture_dir("trails")) / uuid4().hex
        if any(p.is_symlink() for p in (root, *root.parents)):
            raise ValueError("trails_unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        retained = 0
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(120, connect=20),
                              headers={"User-Agent": "hifld-next-datasets/usgs-tnm-trails-draft"}) as client:
                def request_steps(retained):
                    requests = [("service_before", SERVICE_URL + "?f=pjson"), ("layer_before", LAYER_URL + "?f=pjson"),
                        ("count_before", _query({"where":"1=1","returnCountOnly":"true","f":"json"})),
                        ("ids_before", _query({"where":"1=1","returnIdsOnly":"true","f":"json"}))]
                    for name, url in requests:
                        retained += (yield (name, url, retained,))
                    ids = json.loads(captures["ids_before"][0]).get("objectIds")
                    ids = validate_capture_witness(json.loads(captures["count_before"][0]).get("count"), ids,
                        json.loads(captures["count_before"][0]).get("count"), ids)
                    if not ids:
                        raise ValueError("trails_dimensions_probe_without_ids")
                    probe_url = _query({"objectIds": str(ids[0]), "outFields": "OBJECTID", "returnGeometry": "true",
                                        "returnZ": "true", "returnM": "true", "f": "json"})
                    retained += (yield ('dimensions_probe', probe_url, retained,))
                    for start in range(0, len(ids), BATCH_SIZE):
                        name = f"page_{start:05d}"
                        url = _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                                      "returnZ": "true", "returnM": "true",
                                      "orderByFields": "objectid ASC", "resultOffset": start,
                                      "resultRecordCount": BATCH_SIZE, "f": "json"})
                        retained += (yield (name, url, retained,))
                    for name, url in [("count_after", _query({"where":"1=1","returnCountOnly":"true","f":"json"})),
                        ("ids_after", _query({"where":"1=1","returnIdsOnly":"true","f":"json"})),
                        ("layer_after", LAYER_URL + "?f=pjson"), ("service_after", SERVICE_URL + "?f=pjson")]:
                        retained += (yield (name, url, retained,))

                    return retained
                retained = capture_request_steps(request_steps(retained), lambda name, url, used: _capture_into(captures, client, url, root, name, used))
            layer, audit = _population(captures)
            manifest = {"complete": True, "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        **_artifact_rosters(root),
                        "artifact_names": sorted(captures), "receipts": {n:r for n,(_b,r) in sorted(captures.items())}}
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
            _load(root)
        except Exception as exc:
            receipts = {}
            for p in root.glob("*.receipt.json"):
                try: receipts[p.name.removesuffix(".receipt.json")] = json.loads(p.read_bytes())
                except (OSError, ValueError): receipts[p.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
            refused = {}
            refused_dir = root / _REFUSED_DIR
            if refused_dir.is_dir() and not refused_dir.is_symlink():
                for p in sorted(refused_dir.glob("*.receipt.json")):
                    key = p.name.removesuffix(".receipt.json")
                    try: refused[key] = json.loads(p.read_bytes())
                    except (OSError, ValueError): refused[key] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({"complete":False,"error_code":type(exc).__name__,
                "captured_at":datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),"artifact_names":sorted(receipts),
                "receipts":receipts,"refused_receipts":refused})+b"\n")
            raise
    if replay:
        layer, audit = _population(captures)
    fields = _fields(layer)
    service_info = json.loads(captures["service_before"][0])
    identity = _processing_identity()
    version = content_derived_version(audit["record_sha256"], audit["geometry_sha256"], audit["schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    parquet_path = f"geoparquet/{FILE_SLUG}.parquet"
    result = {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["feature_count"],
        "source_population_complete":True,"source_id_sha256":audit["id_sha256"],"source_schema_sha256":audit["schema_sha256"],
        "source_record_sha256":audit["record_sha256"],"source_geometry_sha256":audit["geometry_sha256"],
        "candidate_ready":f"{prefix}/metadata/candidate_ready.json","capture_directory":str(root),"reused":False,
        "source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"baseline_accepted":False,"coverage_accepted":False,"rights_accepted":False,
        "mapping_approved":False,"release_accepted":False}
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        payload = json.loads(load_first_construction(storage, state_key, version, fingerprint))["payload"]
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
            fingerprint, identity, audit["record_sha256"], audit["schema_sha256"],
            payload["objects"], payload["capture_receipts"])
        return {**result, "reused": True}
    parquet = _parquet_pages(root, fields, audit["feature_count"])
    inputs = [{"input_id":name,"producer":PUBLISHER,"host":urlsplit(receipt["url"]).hostname,
        "role":"producer_publication","canonical_url":LAYER_URL,"retrieval_url":receipt["url"],
        "retrieved_at":receipt["observed_at"],"http_status":receipt["status"],"bytes":receipt["bytes"],
        "sha256":receipt["sha256"],"wire_bytes":receipt["wire_bytes"],"wire_sha256":receipt["wire_sha256"],
        "release_date":None,"release_date_unknown_reason":"The service declares no per-snapshot release date (no editingInfo); the observed layer state is dated evidence, not a release date.",
        "license":"Rights review pending.","attribution":"USGS attribution review pending."}
        for name,(_body,receipt) in sorted(captures.items())]
    manifest = {"schema_version":1,"approved":False,"title":"Trails","description":"Complete observed USGS The National Map Transportation Trails layer candidate; unapproved draft.",
        "publisher":PUBLISHER,"agency":"U.S. Geological Survey","source_url":LAYER_URL,"license":None,
        "source_modified":None,"date_issued":None,"date_modified":None,
        "processor":"HIFLD Next","code_commit":identity["code_commit"],"code_identity_scheme":identity["code_identity_scheme"],
        "processing_identity":identity,"inputs":inputs,
        "transformations":[{"operation":"Native ArcGIS polyline feature capture to typed GeoParquet WKB","parameters":{
            "attributes":"All 39 declared layer attributes retained verbatim, including the served virtual shape_Length and the producer's 'sourcedatadecscription' spelling. Source ESRI declarations are recorded verbatim in source_audit; output Arrow types use the explicit esri_to_arrow_type_mapping below. No schema conversion contract is approved.",
            "geometry":"Native EPSG:3857 polylines preserved; no reprojection, geocoding, repair, or row dropping. The layer declares hasM: true and serves [x, y, m] vertices; served m ordinates are retained verbatim, not repaired: a feature whose served m ordinates are all null is emitted as 2D WKB MultiLineString, while a feature with any served m ordinate is emitted as WKB MultiLineString M with the served values verbatim (a served null inside such a feature encodes as NaN, the WKB null-m convention). Any served z ordinate, non-finite ordinate, or unexpected geometry member fails closed.",
            "source_population":"Exact objectid roster and count captured before and after all pages; complete current layer only."}}],
        "lineage":{"layer_id":LAYER_ID,"layer_name":"Trails","native_crs":"EPSG:3857","feature_count":audit["feature_count"],
            "source_id_sha256":audit["id_sha256"],"source_schema_sha256":audit["schema_sha256"],"source_record_sha256":audit["record_sha256"],
            "source_geometry_sha256":audit["geometry_sha256"],"service_refresh_observed":service_info.get("copyrightText"),"release_date":None,
            "hifld_2024_equivalence":"unverified","observed_archive_scope":"HIFLD Trails inventory entry trails-2 and DataLumos project 240112 copy retained as identification context only; never fetched"},
        "esri_to_arrow_type_mapping":{field["type"]:ESRI_TO_ARROW_TYPE[field["type"]] for field in fields},
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Retains native attributes and EPSG:3857 polyline geometry.",
            "objects":[{"path":parquet_path,"bytes":len(parquet),"sha256":_sha(parquet)}]}}},
        "unsupported_output_formats":{"geopackage":"No conversion contract approved.","shapefile_zip":"No field conversion contract approved.","pmtiles":"No tiling or release policy approved."},
        "promotion_allowed":False}
    validate_output_policy(manifest); provenance_projection(manifest)
    outputs = {parquet_path:parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n",
               "metadata/source_audit.json":_canonical(audit)+b"\n"}
    ready = {"approved":False,"version":version,"objects":{parquet_path:{"bytes":len(parquet),"sha256":_sha(parquet)}},
        "source_population_complete":True,"source_id_sha256":audit["id_sha256"],"source_record_sha256":audit["record_sha256"],
        "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    descriptors = write_outputs(storage, prefix, outputs)
    for name, (body, receipt) in captures.items():
        descriptors.update(write_outputs(storage, prefix, {
            f"raw/{name}.body": body, f"raw/{name}.wire": (root / f"{name}.wire").read_bytes(),
            f"raw/{name}.receipt.json": _canonical(receipt) + b"\n"}))
    refused_dir = root / _REFUSED_DIR
    if refused_dir.is_dir():
        for path in sorted(refused_dir.iterdir()):
            descriptors.update(write_outputs(storage, prefix, {f"evidence/refused/{path.name}": path.read_bytes()}))
    descriptors.update(write_outputs(storage, prefix, {"metadata/candidate_ready.json": _canonical(ready)+b"\n"}))
    seal_ready_candidate(storage, state_key, version, fingerprint, {
        "processing_identity": identity, "source_record_sha256": audit["record_sha256"],
        "source_schema_sha256": audit["schema_sha256"], "objects": descriptors,
        "capture_receipts": {name: receipt for name, (_body, receipt) in captures.items()}})
    return result


def _ids_view(captures: dict[str, tuple[bytes, dict]]) -> list[int]:
    return sorted(json.loads(captures["ids_before"][0])["objectIds"])


def _parquet_pages(root: Path, fields: list[dict], count: int) -> bytes:
    """Build the candidate GeoParquet from the raw page bodies in objectid order."""
    names = [f["name"] for f in fields]
    arrays = {name: [] for name in names}
    wkb = []
    geometry_types = set()
    page_names = sorted(f"page_{start:05d}" for start in range(0, count, BATCH_SIZE))
    for name in page_names:
        body = (root / f"{name}.body").read_bytes()
        page = json.loads(body)
        for feature in page.get("features", []):
            attrs = {str(key).lower(): value for key, value in feature["attributes"].items()}
            for field in fields:
                kind = field["type"]
                # Esri field names are case-insensitive on the wire (the
                # producer serves the declared 'shape_Length' as
                # 'shape_length'); arrays are keyed by the declared name and
                # values are read through the lowercased served key.
                value = attrs[field["name"].lower()]
                if kind == "esriFieldTypeDate":
                    value = None if value is None else datetime.fromtimestamp(value / 1000, tz=timezone.utc)
                elif kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
                    value = None if value is None else float(value)
                arrays[field["name"]].append(value)
            geometry = feature.get("geometry")
            if geometry is not None:
                geometry_types.add("MultiLineString M" if _populated_m(geometry["paths"])
                                   else "MultiLineString")
            wkb.append(None if geometry is None else _wkb_polyline(geometry["paths"]))
    schema_fields = [pa.field(f["name"], ARROW_TYPES[f["type"]], nullable=f.get("nullable") is not False) for f in fields]
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    arrays["geometry"] = wkb
    table = pa.Table.from_pydict(arrays, schema=pa.schema(schema_fields, metadata=None))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": sorted(geometry_types), "crs": EXPECTED_CRS.to_json_dict()}}}
    table = table.replace_schema_metadata({b"geo": _canonical(geo)})
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()
