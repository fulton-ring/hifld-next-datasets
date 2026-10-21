"""Draft-only native capture of the official USGS National Map State Capitols layer.

The USGS Structures service exposes the complete national State Capitols
point layer in EPSG:3857.  This module resolves the live layer declaration,
captures the complete roster by explicit object ID, preserves every response
body and checksum, and writes a typed GeoParquet candidate.  It does not
approve the source, schema, baseline, coverage, rights, or release.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import shutil
import struct
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = FILE_SLUG = "state-capitols"
PUBLISHER = "U.S. Geological Survey National Geospatial Program"
SERVICE_URL = "https://carto.nationalmap.gov/arcgis/rest/services/structures/MapServer"
LAYER_ID = 6
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "State Capitols"
SERVICE_MAP_NAME = "structures"
SERVICE_DESCRIPTION_PREFIX = "USGS Structures"
COUNT = 55
EXPECTED_ID_SHA = "f760211171a6ee10347ae7a6e612a6d42a19c104217fbdf959223abcc8963770"
EXPECTED_RECORD_SHA = "fe5b62591cf0364d4f9b4c5fc42ac9d07f209178c52432a1ca4dd7ac230e2322"
EXPECTED_GEOMETRY_SHA = "f8351597c3cac78a4c6456eaee5fbc4034eb4a9999764cb9061875fcb239a0d3"
EXPECTED_SCHEMA_SHA = "4ef3e6b267361951434b64e7d08cc839033707b6f288c1c1445545c6142c8abb"
EXPECTED_EXTENT = {
    "xmin": -19746524.6974,
    "ymin": -11197179.371800002,
    "xmax": 19282803.9185,
    "ymax": 11514242.744599998,
}
EXPECTED_BOUNDS = (-19000413.5635, -1606583.511599999, 16225222.911899999, 8031042.3895)
EXPECTED_STATE_ROSTER = [
    "AK", "AL", "AR", "AS", "AZ", "CA", "CO", "CT", "DE", "FL", "GA", "GU", "HI",
    "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO",
    "MP", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK",
    "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VI", "VT", "WA",
    "WI", "WV", "WY",
]
EXPECTED_FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    {"name": "PERMANENT_IDENTIFIER", "type": "esriFieldTypeString", "alias": "PERMANENT_IDENTIFIER", "length": 40, "domain": None},
    {"name": "SOURCE_FEATUREID", "type": "esriFieldTypeString", "alias": "SOURCE_FEATUREID", "length": 40, "domain": None},
    {"name": "SOURCE_DATASETID", "type": "esriFieldTypeString", "alias": "SOURCE_DATASETID", "length": 40, "domain": None},
    {"name": "SOURCE_DATADESC", "type": "esriFieldTypeString", "alias": "SOURCE_DATADESC", "length": 100, "domain": None},
    {"name": "SOURCE_ORIGINATOR", "type": "esriFieldTypeString", "alias": "SOURCE_ORIGINATOR", "length": 130, "domain": None},
    {"name": "LOADDATE", "type": "esriFieldTypeDate", "alias": "LOADDATE", "length": 8, "domain": None},
    {"name": "FTYPE", "type": "esriFieldTypeInteger", "alias": "FType", "domain": None},
    {"name": "FCODE", "type": "esriFieldTypeInteger", "alias": "FCode", "domain": {"type": "codedValue", "name": "Education FCode", "description": "", "codedValues": [{"name": "School", "code": 73002}, {"name": "School: Elementary", "code": 73003}, {"name": "School: Middle School", "code": 73004}, {"name": "School: High School", "code": 73005}, {"name": "College / University", "code": 73006}, {"name": "Technical / Trade School", "code": 73007}], "mergePolicy": "esriMPTDefaultValue", "splitPolicy": "esriSPTDefaultValue"}},
    {"name": "NAME", "type": "esriFieldTypeString", "alias": "NAME", "length": 100, "domain": None},
    {"name": "POINTLOCATIONTYPE", "type": "esriFieldTypeInteger", "alias": "PointLocationType", "domain": {"type": "codedValue", "name": "PointLocationType Domain", "description": "", "codedValues": [{"name": "Unknown", "code": 0}, {"name": "Centroid", "code": 1}, {"name": "Egress or Entrance", "code": 2}, {"name": "Turn-off location", "code": 3}, {"name": "Approximate", "code": 4}], "mergePolicy": "esriMPTDefaultValue", "splitPolicy": "esriSPTDefaultValue"}},
    {"name": "ADMINTYPE", "type": "esriFieldTypeInteger", "alias": "AdminType", "domain": {"type": "codedValue", "name": "OwnerClass Domain", "description": "", "codedValues": [{"name": "Unknown", "code": 0}, {"name": "Federal", "code": 1}, {"name": "Tribal", "code": 2}, {"name": "State", "code": 3}, {"name": "Regional", "code": 4}, {"name": "County", "code": 5}, {"name": "Municipal", "code": 6}, {"name": "Private", "code": 7}], "mergePolicy": "esriMPTDefaultValue", "splitPolicy": "esriSPTDefaultValue"}},
    {"name": "ADDRESSBUILDINGNAME", "type": "esriFieldTypeString", "alias": "ADDRESSBUILDINGNAME", "length": 60, "domain": None},
    {"name": "ADDRESS", "type": "esriFieldTypeString", "alias": "ADDRESS", "length": 75, "domain": None},
    {"name": "CITY", "type": "esriFieldTypeString", "alias": "CITY", "length": 40, "domain": None},
    {"name": "STATE", "type": "esriFieldTypeString", "alias": "STATE", "length": 2, "domain": None},
    {"name": "ZIPCODE", "type": "esriFieldTypeString", "alias": "ZIPCODE", "length": 10, "domain": None},
    {"name": "GNIS_ID", "type": "esriFieldTypeString", "alias": "GNIS_ID", "length": 10, "domain": None},
    {"name": "GLOBALID", "type": "esriFieldTypeGlobalID", "alias": "GLOBALID", "length": 38, "domain": None},
]
DECLARED_NAMES = [field["name"] for field in EXPECTED_FIELDS if field["type"] != "esriFieldTypeGeometry"]
RESPONSE_KEYS = [name.lower() for name in DECLARED_NAMES]
EXPECTED_RESPONSE_FIELDS = [
    {"name": "objectid", "type": "esriFieldTypeOID", "alias": "OBJECTID"},
    {"name": "permanent_identifier", "type": "esriFieldTypeString", "alias": "PERMANENT_IDENTIFIER", "length": 40},
    {"name": "source_featureid", "type": "esriFieldTypeString", "alias": "SOURCE_FEATUREID", "length": 40},
    {"name": "source_datasetid", "type": "esriFieldTypeString", "alias": "SOURCE_DATASETID", "length": 40},
    {"name": "source_datadesc", "type": "esriFieldTypeString", "alias": "SOURCE_DATADESC", "length": 100},
    {"name": "source_originator", "type": "esriFieldTypeString", "alias": "SOURCE_ORIGINATOR", "length": 130},
    {"name": "loaddate", "type": "esriFieldTypeDate", "alias": "LOADDATE", "length": 8},
    {"name": "ftype", "type": "esriFieldTypeInteger", "alias": "FType"},
    {"name": "fcode", "type": "esriFieldTypeInteger", "alias": "FCode"},
    {"name": "name", "type": "esriFieldTypeString", "alias": "NAME", "length": 100},
    {"name": "pointlocationtype", "type": "esriFieldTypeInteger", "alias": "PointLocationType"},
    {"name": "admintype", "type": "esriFieldTypeInteger", "alias": "AdminType"},
    {"name": "addressbuildingname", "type": "esriFieldTypeString", "alias": "ADDRESSBUILDINGNAME", "length": 60},
    {"name": "address", "type": "esriFieldTypeString", "alias": "ADDRESS", "length": 75},
    {"name": "city", "type": "esriFieldTypeString", "alias": "CITY", "length": 40},
    {"name": "state", "type": "esriFieldTypeString", "alias": "STATE", "length": 2},
    {"name": "zipcode", "type": "esriFieldTypeString", "alias": "ZIPCODE", "length": 10},
    {"name": "gnis_id", "type": "esriFieldTypeString", "alias": "GNIS_ID", "length": 10},
    {"name": "globalid", "type": "esriFieldTypeGlobalID", "alias": "GLOBALID", "length": 38},
]
EXPECTED_FIELD_ALIASES = {field["name"]: field["alias"] for field in EXPECTED_RESPONSE_FIELDS}
EXPECTED_NULL_COUNTS = {
    "objectid": 0, "permanent_identifier": 0, "source_featureid": 55,
    "source_datasetid": 0, "source_datadesc": 0, "source_originator": 0,
    "loaddate": 0, "ftype": 0, "fcode": 0, "name": 0, "pointlocationtype": 0,
    "admintype": 55, "addressbuildingname": 55, "address": 0, "city": 0,
    "state": 0, "zipcode": 0, "gnis_id": 0, "globalid": 0,
}
ATTRIBUTE_TYPES = {field["name"].lower(): field["type"]
                   for field in EXPECTED_FIELDS if field["type"] != "esriFieldTypeGeometry"}
BATCH_SIZE = 2000
MAX_RESPONSE_BYTES = 64_000_000
MAX_CAPTURE_BYTES = 64_000_000
EXPECTED_CRS = CRS.from_epsg(3857)
_ARROW_TYPES = {
    "esriFieldTypeOID": pa.int64(),
    "esriFieldTypeString": pa.string(),
    "esriFieldTypeInteger": pa.int64(),
    "esriFieldTypeDate": pa.int64(),
    "esriFieldTypeGlobalID": pa.string(),
    "esriFieldTypeDouble": pa.float64(),
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _validate_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_drift")
    found = set()
    for key in ("wkid", "latestWkid"):
        if key in value:
            number = value[key]
            if type(number) is not int or number not in {3857, 102100}:
                raise ValueError(label + "_crs_drift")
            found.add(3857)
    for key in ("wkt", "wkt2"):
        if key in value:
            raw = value[key]
            if not isinstance(raw, str) or not raw.strip():
                raise ValueError(label + "_crs_drift")
            try:
                crs = CRS.from_wkt(raw)
            except Exception as exc:
                raise ValueError(label + "_crs_drift") from exc
            if not crs.equals(EXPECTED_CRS, ignore_axis_order=True):
                raise ValueError(label + "_crs_drift")
            found.add(3857)
    if not found or len(found) != 1:
        raise ValueError(label + "_crs_drift")


def _capture(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    retained = sum(path.stat().st_size for pattern in ("*.body", "*.wire")
                   for path in root.glob(pattern) if path.is_file())
    if retained + 2 * 4_000_000 > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_exceeded")
    return capture_get(client, url, root, name, max_bytes=MAX_RESPONSE_BYTES)


def _load_capture(root: Path, name: str) -> tuple[bytes, bytes, dict]:
    body_path, wire_path, receipt_path = (root / f"{name}.body", root / f"{name}.wire",
                                          root / f"{name}.receipt.json")
    if any(path.is_symlink() for path in (body_path, wire_path, receipt_path)):
        raise ValueError("capture_symlink_rejected")
    body, wire, receipt = body_path.read_bytes(), wire_path.read_bytes(), json.loads(receipt_path.read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("capture_receipt_invalid:" + name)
    return body, wire, receipt


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest = json.loads((root / "capture_manifest.json").read_bytes())
    receipts = manifest.get("receipts", {})
    if manifest.get("complete") is not True or not isinstance(receipts, dict) or not receipts:
        raise ValueError("capture_manifest_invalid")
    captures = {}
    for name in sorted(receipts):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != receipts.get(name):
            raise ValueError("capture_receipt_manifest_mismatch:" + name)
        captures[name] = (body, receipt)
    return captures


def _validate_metadata(service: dict, layer: dict) -> None:
    if (not str(service.get("serviceDescription", "")).startswith(SERVICE_DESCRIPTION_PREFIX)
            or service.get("mapName") != SERVICE_MAP_NAME
            or not (isinstance(service.get("currentVersion"), (int, float))
                    and abs(service.get("currentVersion") - 11.3) < 1e-9)):
        raise ValueError("official_usgs_service_identity_drift")
    advertised = {(entry.get("id"), entry.get("name")) for entry in service.get("layers", [])}
    if (LAYER_ID, LAYER_NAME) not in advertised:
        raise ValueError("official_usgs_service_identity_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer"
            or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("fields") != EXPECTED_FIELDS
            or layer.get("maxRecordCount") != 2000):
        raise ValueError("source_layer_schema_or_limit_drift")
    extent = layer.get("extent")
    if not isinstance(extent, dict):
        raise ValueError("source_extent_missing")
    for key, value in EXPECTED_EXTENT.items():
        actual = extent.get(key)
        if type(actual) not in (int, float) or abs(actual - value) > 1e-3:
            raise ValueError("source_extent_drift")
    if extent.get("spatialReference") is None:
        raise ValueError("source_extent_crs_drift")
    _validate_spatial_reference(extent["spatialReference"], "source_extent")


def _validate_population(response_fields: object, ids: object,
                         pages: dict[str, dict]) -> tuple[list[dict], dict]:
    if response_fields != EXPECTED_RESPONSE_FIELDS:
        raise ValueError("source_response_schema_drift")
    if (not isinstance(ids, list) or any(type(value) is not int for value in ids)
            or len(ids) != COUNT or len(set(ids)) != COUNT
            or _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA):
        raise ValueError("source_id_roster_drift")
    features = []
    seen = set()
    nulls = {key: 0 for key in RESPONSE_KEYS}
    bounds = [float("inf"), float("inf"), float("-inf"), float("-inf")]
    for name in sorted(pages):
        page = pages[name]
        if page.get("error") or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncation:" + name)
        if (page.get("fields") != EXPECTED_RESPONSE_FIELDS
                or page.get("fieldAliases") != EXPECTED_FIELD_ALIASES
                or page.get("displayFieldName") != "NAME"
                or page.get("geometryType") != "esriGeometryPoint"):
            raise ValueError("source_page_schema_drift:" + name)
        _validate_spatial_reference(page.get("spatialReference"), name + "_response")
        rows = page.get("features")
        index = int(name[-3:])
        expected = sorted(ids)[index * BATCH_SIZE:(index + 1) * BATCH_SIZE]
        if not isinstance(rows, list) or len(rows) != len(expected):
            raise ValueError("source_page_population_incomplete:" + name)
        page_ids = []
        for feature in rows:
            attributes, geometry = feature.get("attributes"), feature.get("geometry")
            if not isinstance(attributes, dict) or set(attributes) != set(RESPONSE_KEYS):
                raise ValueError("source_attribute_roster_drift")
            oid = attributes.get("objectid")
            if type(oid) is not int or oid not in expected or oid in seen:
                raise ValueError("source_native_id_drift")
            for key in RESPONSE_KEYS:
                value = attributes[key]
                kind = ATTRIBUTE_TYPES[key]
                if value is None:
                    nulls[key] += 1
                    continue
                if kind in ("esriFieldTypeOID", "esriFieldTypeInteger") and type(value) is not int:
                    raise ValueError("source_integer_type_drift:" + key)
                if kind == "esriFieldTypeDate" and type(value) is not int:
                    raise ValueError("source_date_type_drift:" + key)
                if kind in ("esriFieldTypeString", "esriFieldTypeGlobalID") and not isinstance(value, str):
                    raise ValueError("source_string_type_drift:" + key)
            if not isinstance(geometry, dict) or set(geometry) != {"x", "y"}:
                raise ValueError("source_geometry_structure_drift")
            for coordinate in (geometry["x"], geometry["y"]):
                if type(coordinate) not in (int, float) or not math.isfinite(coordinate):
                    raise ValueError("source_xy_drift")
            x, y = geometry["x"], geometry["y"]
            bounds = [min(bounds[0], x), min(bounds[1], y), max(bounds[2], x), max(bounds[3], y)]
            page_ids.append(oid)
            seen.add(oid)
            features.append(feature)
        if sorted(page_ids) != expected:
            raise ValueError("source_page_ids_drift:" + name)
    if seen != set(ids) or len(features) != COUNT:
        raise ValueError("source_population_incomplete")
    if nulls != EXPECTED_NULL_COUNTS:
        raise ValueError("source_null_count_drift")
    if any(abs(actual - expected) > 1e-3 for actual, expected in zip(bounds, EXPECTED_BOUNDS)):
        raise ValueError("source_geometry_coverage_drift")
    state_roster = sorted({feature["attributes"]["state"] for feature in features})
    if state_roster != EXPECTED_STATE_ROSTER:
        raise ValueError("source_state_roster_drift")
    features.sort(key=lambda feature: feature["attributes"]["objectid"])
    audit = {
        "source_feature_count": len(features),
        "source_id_sha256": _sha(_canonical(sorted(seen))),
        "source_id_min": min(seen),
        "source_id_max": max(seen),
        "source_schema_sha256": _sha(_canonical(EXPECTED_FIELDS)),
        "source_record_sha256": _sha(_canonical([feature["attributes"] for feature in features])),
        "source_geometry_sha256": _sha(_canonical([[feature["attributes"]["objectid"], feature["geometry"]]
                                                   for feature in features])),
        "source_geometry_type": "esriGeometryPoint",
        "source_wkid": 3857,
        "source_bounds": bounds,
        "source_extent": EXPECTED_EXTENT,
        "attribute_null_counts": nulls,
        "source_state_roster": state_roster,
        "source_coverage_scope": ("Complete current USGS National Map Structures State Capitols layer, "
                                  "native EPSG:3857; 50 states plus AS, GU, MP, PR, and VI; "
                                  "equivalence to the HIFLD 2024 archive is unverified."),
    }
    if audit["source_schema_sha256"] != EXPECTED_SCHEMA_SHA:
        raise ValueError("source_schema_sha_drift")
    if audit["source_record_sha256"] != EXPECTED_RECORD_SHA:
        raise ValueError("source_record_sha_drift")
    if audit["source_geometry_sha256"] != EXPECTED_GEOMETRY_SHA:
        raise ValueError("source_geometry_sha_drift")
    return features, audit


def _wkb(geometry: dict) -> bytes:
    return struct.pack("<BIdd", 1, 1, float(geometry["x"]), float(geometry["y"]))


def _parquet_bytes(features: list[dict]) -> bytes:
    arrays = []
    for name in DECLARED_NAMES:
        arrays.append(pa.array([row["attributes"][name.lower()] for row in features],
                               type=_ARROW_TYPES[ATTRIBUTE_TYPES[name.lower()]]))
    arrays.append(pa.array([_wkb(row["geometry"]) for row in features], type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=DECLARED_NAMES + ["geometry"])
    metadata = {
        b"geo": _canonical({
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": EXPECTED_CRS.to_json_dict(),
            }},
        })
    }
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> tuple[str, dict]:
    roster = {}
    for name in sorted(captures):
        _body, wire, _receipt = _load_capture(root, name)
        roster[name] = {"body_sha256": _sha((root / f"{name}.body").read_bytes()),
                        "wire_sha256": _sha(wire)}
    return _sha(_canonical(roster)), roster


def _processing_identity(repo_root: Path | None = None) -> dict[str, object]:
    root = Path(repo_root) if repo_root is not None else Path(__file__).parents[3]
    identity = processing_identity(root)
    validate_construction_code_identity(
        root, identity["code_commit"], identity["files"], identity=identity,
    )
    return identity


def _preserve_original_receipts(captures: dict[str, tuple[bytes, dict]],
                                original_receipts: object) -> dict[str, dict]:
    if not isinstance(original_receipts, dict) or set(original_receipts) != set(captures):
        raise ValueError("cached_original_receipt_roster_changed")
    for name, (_body, receipt) in captures.items():
        sealed = original_receipts[name]
        if any(receipt.get(field) != sealed.get(field) for field in
               ("complete_body", "status", "bytes", "sha256", "wire_bytes", "wire_sha256")):
            raise ValueError("cached_source_capture_content_changed:" + name)
    return original_receipts


def _stable_layer(layer: dict) -> dict:
    """Project the data-bearing layer metadata; server-drawn symbology is volatile."""
    return {key: layer.get(key) for key in
            ("id", "name", "type", "geometryType", "fields", "maxRecordCount", "extent")}


def capture_state_capitols(storage: StagingStorageResource, *, version: str,
                           capture_directory: str | None = None,
                           repo_root: Path | None = None) -> dict[str, object]:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_isolated_local_staging_required")
    if (not isinstance(version, str) or not version or Path(version).name != version
            or version in {".", ".."} or "/" in version or "\\" in version):
        raise ValueError("unsafe_capture_version")
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 100_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        captures = _read_replay(root)
    else:
        if requested and any(requested.glob("*.body")):
            raise ValueError("unsealed_capture_attempt_exists")
        root = ((requested / version) if requested
                else Path("/tmp/hifld-state-capitols-capture")) / uuid4().hex
        if any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures: dict[str, tuple[bytes, dict]] = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/usgs-structures-draft"}) as client:
                for name, url in (("service_before", SERVICE_URL + "?f=pjson"),
                                  ("layer_before", LAYER_URL + "?f=pjson"),
                                  ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                                  ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}))):
                    captures[name] = _capture(client, url, root, name)
                ids = json.loads(captures["ids_before"][0]).get("objectIds")
                if (not isinstance(ids, list) or len(ids) != COUNT
                        or _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA):
                    raise ValueError("source_seed_id_roster_drift")
                for index, start in enumerate(range(0, len(ids), BATCH_SIZE)):
                    page_ids = ids[start:start + BATCH_SIZE]
                    url = _query({"objectIds": ",".join(map(str, page_ids)), "outFields": "*",
                                  "returnGeometry": "true", "orderByFields": "OBJECTID ASC",
                                  "f": "json"})
                    captures[f"page_{index:03d}"] = _capture(client, url, root, f"page_{index:03d}")
                for name, url in (("layer_after", LAYER_URL + "?f=pjson"),
                                  ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                                  ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                                  ("service_after", SERVICE_URL + "?f=pjson")):
                    captures[name] = _capture(client, url, root, name)
            docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
            _validate_metadata(docs["service_before"], docs["layer_before"])
            _validate_metadata(docs["service_after"], docs["layer_after"])
            if (docs["service_after"] != docs["service_before"]
                    or _stable_layer(docs["layer_after"]) != _stable_layer(docs["layer_before"])):
                raise ValueError("source_changed_during_capture")
            if docs["count_before"].get("count") != COUNT or docs["count_after"].get("count") != COUNT:
                raise ValueError("source_count_drift")
            if docs["ids_after"].get("objectIds") != docs["ids_before"].get("objectIds"):
                raise ValueError("source_id_roster_changed_during_capture")
            pages = {name: payload for name, payload in docs.items() if name.startswith("page_")}
            _features, audit = _validate_population(docs["page_000"].get("fields")
                                                    if pages else None,
                                                    docs["ids_before"].get("objectIds"), pages)
            manifest = {
                "complete": True,
                "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "source_feature_count": audit["source_feature_count"],
                "source_record_sha256": audit["source_record_sha256"],
                "source_schema_sha256": audit["source_schema_sha256"],
                "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            }
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
            captures = _read_replay(root)
        except Exception as exc:
            names = sorted(path.name.removesuffix(".receipt.json") for path in root.glob("*.receipt.json"))
            receipts = {}
            for name in names:
                try:
                    receipts[name] = json.loads((root / f"{name}.receipt.json").read_bytes())
                except (OSError, ValueError):
                    receipts[name] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({
                "complete": False,
                "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "error_code": type(exc).__name__,
                "receipts": receipts,
            }) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version,
                                              captures, type(exc).__name__)
            raise
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    _validate_metadata(docs["service_before"], docs["layer_before"])
    _validate_metadata(docs["service_after"], docs["layer_after"])
    if (docs["service_after"] != docs["service_before"]
            or _stable_layer(docs["layer_after"]) != _stable_layer(docs["layer_before"])):
        raise ValueError("source_changed_during_capture")
    if docs["count_before"].get("count") != COUNT or docs["count_after"].get("count") != COUNT:
        raise ValueError("source_count_drift")
    if docs["ids_after"].get("objectIds") != docs["ids_before"].get("objectIds"):
        raise ValueError("source_id_roster_changed_during_capture")
    pages = {name: payload for name, payload in docs.items() if name.startswith("page_")}
    features, audit = _validate_population(docs["page_000"].get("fields") if pages else None,
                                           docs["ids_before"].get("objectIds"), pages)
    parquet = _parquet_bytes(features)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    repo = Path(repo_root) if repo_root is not None else Path(__file__).parents[3]
    identity = _processing_identity(repo)
    capture_hash, _roster = _identity_hash(captures, root)
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    fingerprint = _sha(_canonical({"identity": identity,
                                   "source_record_sha256": audit["source_record_sha256"],
                                   "source_schema_sha256": audit["source_schema_sha256"],
                                   "source_capture_sha256": capture_hash}))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    original_payload = None
    if storage.object_snapshot(state_key) is not None:
        original_payload = json.loads(load_first_construction(storage, state_key, version,
                                                              fingerprint)).get("payload")
        receipts = _preserve_original_receipts(
            captures, original_payload.get("capture_receipts") if isinstance(original_payload, dict) else None)
    audit.update({"source_capture_evidence_sha256": capture_hash, "source_population_complete": True,
                  "source_release_accepted": False, "source_freshness_accepted": False,
                  "schema_contract_accepted": False, "id_contract_accepted": False,
                  "coverage_accepted": False, "rights_accepted": False,
                  "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    if original_payload is None:
        receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    inputs = [{
        "input_id": name,
        "producer": PUBLISHER,
        "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": LAYER_URL,
        "retrieval_url": receipt["url"],
        "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"],
        "bytes": receipt["bytes"],
        "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"],
        "wire_sha256": receipt["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": ("The USGS Structures service does not declare a dated data release; "
                                        "LOADDATE attribute values are per-feature load times, not an edition date."),
        "license": "Rights review pending.",
        "attribution": "U.S. Geological Survey; final attribution review pending.",
    } for name, receipt in sorted(receipts.items())]
    manifest = {
        "schema_version": 1,
        "approved": False,
        "title": "State Capitols",
        "description": "Complete capture of the official USGS National Map Structures State Capitols layer; unapproved candidate only.",
        "publisher": PUBLISHER,
        "agency": "U.S. Geological Survey",
        "source_url": LAYER_URL,
        "license": None,
        "source_modified": None,
        "date_issued": None,
        "date_modified": None,
        "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"],
        "processing_identity": identity,
        "inputs": inputs,
        "transformations": [{
            "operation": "Native ESRI point feature capture to typed GeoParquet WKB",
            "parameters": {
                "source_fields": ("All declared attributes retained in declared types; the JSON query "
                                  "response lowercases attribute keys and this transform restores the "
                                  "declared source field names without changing any value."),
                "source_ids": ("OBJECTID, GLOBALID, PERMANENT_IDENTIFIER, and GNIS_ID retained unchanged; "
                               "no substitute row identifiers."),
                "source_crs": "EPSG:3857",
                "geometry": "Native XY points preserved without reprojection, repair, or simplification.",
                "raw_evidence": ("Original wire bytes, decoded response bodies, and hash-pinned receipts "
                                 "retained under raw/."),
                "candidate_status": ("unapproved; archive equivalence, current edition, schema, rights, "
                                     "coverage, and release decisions pending"),
            },
        }],
        "lineage": ("USGS National Map Structures service (layer 6 'State Capitols'; identical duplicate "
                    "exposure at layer 41). HIFLD 2024 DataLumos capture and the DHS crosswalk are "
                    "historical references; exact equivalence is unresolved."),
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {"geoparquet": {
                "reason": "Native attributes and EPSG:3857 points preserved as typed GeoParquet.",
                "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}],
            }},
        },
        "unsupported_output_formats": {
            "geopackage": "No format-specific mapping approved.",
            "shapefile_zip": "No format-specific mapping approved.",
            "pmtiles": "No display generalization or release policy approved.",
        },
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    provenance_projection(manifest)
    outputs = {
        output_path: parquet,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
    }
    for name, (body, _receipt) in captures.items():
        _b, wire, _r = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipts[name]) + b"\n"
    ready = {
        "approved": False,
        "version": version,
        "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_release_accepted": False,
        "freshness_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "coverage_accepted": False,
        "rights_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    }
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    if original_payload is not None:
        verify_cached_candidate(storage, repo, state_key, prefix, version, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"],
                                original_payload["objects"], receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": COUNT, "source_population_complete": True,
                "source_id_sha256": audit["source_id_sha256"],
                "source_record_sha256": audit["source_record_sha256"],
                "source_schema_sha256": audit["source_schema_sha256"],
                "source_capture_evidence_sha256": capture_hash, "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}
    descriptors = {}
    for name, body in sorted(outputs.items()):
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if snapshot is None or snapshot.size != len(body) or storage.sha256_key(key) != descriptor["sha256"]:
            raise ValueError("candidate_readback_failed:" + name)
        descriptors[name] = descriptor
    seal_ready_candidate(storage, state_key, version, fingerprint,
                         {"processing_identity": identity,
                          "source_record_sha256": audit["source_record_sha256"],
                          "source_schema_sha256": audit["source_schema_sha256"],
                          "capture_receipts": receipts, "objects": descriptors})
    validate_construction_code_identity(repo, identity["code_commit"], identity["files"],
                                        identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"],
            "source_record_sha256": audit["source_record_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_geometry_sha256": audit["source_geometry_sha256"],
            "source_capture_evidence_sha256": capture_hash,
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "capture_directory": str(root), "reused": replay and original_payload is not None,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}