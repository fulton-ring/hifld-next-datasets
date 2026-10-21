"""Draft-only native capture of the USGS Quaternary Fault and Fold Database.

The official USGS ArcGIS service exposes the national fault traces in
EPSG:3857.  This module captures the complete national roster by explicit
object ID, preserves every response body and checksum, and writes a typed
GeoParquet candidate.  It does not approve the source, schema, baseline,
coverage, rights, or release.
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

from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.sources.epa_frs_common import capture_get, processing_identity, write_immutable

DATASET_SLUG = FILE_SLUG = "quaternary-fault-lines"
PUBLISHER = "U.S. Geological Survey Earthquake Hazards Program"
SERVICE_URL = "https://earthquake.usgs.gov/arcgis/rest/services/haz/Qfaults/MapServer"
LAYER_ID = 21
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
SERVICE_MAP_NAME = "2020 Release"
LAYER_NAME = "National Database"
EXPECTED_COUNT = 112_809
EXPECTED_ID_SHA = "9918f4432ae51ff5a78ce8fde86976181f70c79106a4daa63f5b1e85f4b663d9"
EXPECTED_RECORD_SHA = "ce2b8535e87ee3a495735c404d1f9d2a5d7dc63c29789f5441ab12cea9e2ab8d"
EXPECTED_GEOMETRY_SHA = "a86afbdc44a9a138c007ca95a8f85020716055e7d2d8cb82f672a8ffac21ec93"
EXPECTED_SCHEMA_SHA = "e77beb61938c9b3ff2a3852a7437bd8079cf47bf0899f24b3be82e94d8c2d86b"
EXPECTED_EXTENT = {
    "xmin": -20037507.0672,
    "ymin": 2127432.182599999,
    "xmax": 20037507.067199998,
    "ymax": 11317901.5467,
}
EXPECTED_BOUNDS = tuple(EXPECTED_EXTENT[k] for k in ("xmin", "ymin", "xmax", "ymax"))
EXPECTED_NULL_COUNTS = {
    "OBJECTID": 0, "fault_name": 0, "section_name": 0, "fault_id": 0,
    "section_id": 0, "Location": 0, "linetype": 0, "age": 0,
    "dip_direction": 793, "slip_rate": 717, "slip_sense": 715,
    "mapped_scale": 0, "class": 434, "mapped_certainty": 0, "strike": 434,
    "total_fault_length": 21950, "cooperators": 0, "earthquake": 754,
    "last_review": 934, "fault_url": 310, "symbology": 0, "ref_id": 0,
    "Fault_ID_num": 0, "Shape_Length": 0, "archive_pdf": 0,
}
EXPECTED_FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "Shape", "type": "esriFieldTypeGeometry", "alias": "Shape", "domain": None},
    {"name": "fault_name", "type": "esriFieldTypeString", "alias": "Fault Name", "length": 100, "domain": None},
    {"name": "section_name", "type": "esriFieldTypeString", "alias": "Section Name", "length": 100, "domain": None},
    {"name": "fault_id", "type": "esriFieldTypeString", "alias": "Fault ID", "length": 10, "domain": None},
    {"name": "section_id", "type": "esriFieldTypeString", "alias": "Section ID", "length": 1, "domain": None},
    {"name": "Location", "type": "esriFieldTypeString", "alias": "Location", "length": 100, "domain": None},
    {"name": "linetype", "type": "esriFieldTypeString", "alias": "Linetype", "length": 50, "domain": None},
    {"name": "age", "type": "esriFieldTypeString", "alias": "Age", "length": 50, "domain": None},
    {"name": "dip_direction", "type": "esriFieldTypeString", "alias": "Dip Direction", "length": 50, "domain": None},
    {"name": "slip_rate", "type": "esriFieldTypeString", "alias": "Slip Rate(mm/year)", "length": 100, "domain": None},
    {"name": "slip_sense", "type": "esriFieldTypeString", "alias": "Slip Sense", "length": 50, "domain": None},
    {"name": "mapped_scale", "type": "esriFieldTypeString", "alias": "Mapped Scale", "length": 50, "domain": None},
    {"name": "class", "type": "esriFieldTypeString", "alias": "Fault Class", "length": 50, "domain": None},
    {"name": "mapped_certainty", "type": "esriFieldTypeString", "alias": "Mapping Certainty", "length": 200, "domain": None},
    {"name": "strike", "type": "esriFieldTypeString", "alias": "Average Strike", "length": 50, "domain": None},
    {"name": "total_fault_length", "type": "esriFieldTypeInteger", "alias": "Total Fault Length (km)", "domain": None},
    {"name": "cooperators", "type": "esriFieldTypeString", "alias": "Cooperator", "length": 200, "domain": None},
    {"name": "earthquake", "type": "esriFieldTypeString", "alias": "Associated Earthquake", "length": 200, "domain": None},
    {"name": "last_review", "type": "esriFieldTypeDate", "alias": "Date of Last Review", "length": 8, "domain": None},
    {"name": "fault_url", "type": "esriFieldTypeString", "alias": "Fault url", "length": 500, "domain": None},
    {"name": "symbology", "type": "esriFieldTypeString", "alias": "symbology", "length": 100, "domain": None},
    {"name": "ref_id", "type": "esriFieldTypeString", "alias": "Citation ID", "length": 50, "domain": None},
    {"name": "Fault_ID_num", "type": "esriFieldTypeInteger", "alias": "Fault ID Number", "domain": None},
    {"name": "Shape_Length", "type": "esriFieldTypeDouble", "alias": "Shape_Length", "domain": None},
    {"name": "archive_pdf", "type": "esriFieldTypeString", "alias": "archive report pdf", "length": 255, "domain": None},
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELDS if field["type"] != "esriFieldTypeGeometry"]
BATCH_SIZE = 200
MAX_RESPONSE_BYTES = 64_000_000
MAX_CAPTURE_BYTES = 2_000_000_000
EXPECTED_CRS = CRS.from_epsg(3857)
_ARROW_TYPES = {
    "esriFieldTypeOID": pa.int64(),
    "esriFieldTypeString": pa.string(),
    "esriFieldTypeInteger": pa.int64(),
    "esriFieldTypeDate": pa.int64(),
    "esriFieldTypeDouble": pa.float64(),
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


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


def _capture(client: httpx.Client, url: str, root: Path, name: str, retained: int) -> tuple[bytes, dict]:
    if type(retained) is not int or retained < 0 or retained > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_invalid")

    def reserve(progress) -> None:
        # Counts all persisted decoded and original wire bytes from earlier requests,
        # plus this response's acknowledged bytes and the next bounded read/decode.
        prospective = (retained + progress.wire_bytes_retained
                       + progress.decoded_bytes_retained + progress.reserve_bytes)
        if prospective > MAX_CAPTURE_BYTES:
            raise OSError("source_capture_budget_exceeded")

    return capture_get(client, url, root, name, max_bytes=MAX_RESPONSE_BYTES, reserve=reserve)


def _load_capture(root: Path, name: str) -> tuple[bytes, dict]:
    body = (root / f"{name}.body").read_bytes()
    receipt = json.loads((root / f"{name}.receipt.json").read_bytes())
    if receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body) or receipt.get("status") != 200:
        raise ValueError("capture_receipt_invalid:" + name)
    wire_path = root / f"{name}.wire"
    if "wire_bytes" in receipt:
        if (not wire_path.is_file() or wire_path.is_symlink()):
            raise ValueError("capture_wire_receipt_invalid:" + name)
        wire = wire_path.read_bytes()
        if receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire):
            raise ValueError("capture_wire_receipt_invalid:" + name)
    elif wire_path.exists():
        # Legacy decoded-only captures never had a wire-byte pin; do not infer one.
        raise ValueError("legacy_capture_has_unpinned_wire:" + name)
    return body, receipt


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest = json.loads((root / "capture_manifest.json").read_bytes())
    names = manifest.get("artifact_names")
    if manifest.get("complete") is not True or not isinstance(names, list) or not names:
        raise ValueError("capture_manifest_invalid")
    result = {}
    for name in names:
        body, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("capture_receipt_manifest_mismatch:" + name)
        result[name] = (body, receipt)
    return result


def _validate_metadata(service: dict, layer: dict) -> None:
    if service.get("mapName") != SERVICE_MAP_NAME:
        raise ValueError("official_usgs_service_identity_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("geometryType") != "esriGeometryPolyline"
            or layer.get("fields") != EXPECTED_FIELDS
            or layer.get("maxRecordCount") != 2000):
        raise ValueError("source_layer_schema_or_limit_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent = layer.get("extent")
    if not isinstance(extent, dict):
        raise ValueError("source_extent_missing")
    for key, value in EXPECTED_EXTENT.items():
        actual = extent.get(key)
        if type(actual) not in (int, float) or abs(actual - value) > 1e-3:
            raise ValueError("source_extent_drift")
    if extent.get("spatialReference") is not None:
        _validate_spatial_reference(extent["spatialReference"], "source_extent")


def _validate_population(layer: dict, ids: object, pages: dict[str, dict]) -> tuple[list[dict], dict]:
    if not isinstance(ids, list) or any(type(value) is not int for value in ids):
        raise ValueError("source_id_roster_drift")
    if len(ids) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT:
        raise ValueError("source_id_roster_drift")
    if _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA:
        raise ValueError("source_id_roster_drift")
    features = []
    seen = set()
    nulls = {field["name"]: 0 for field in ATTRIBUTE_FIELDS}
    vertices = 0
    bounds = [float("inf"), float("inf"), float("-inf"), float("-inf")]
    for name in sorted(pages):
        page = pages[name]
        if page.get("error") or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncation:" + name)
        _validate_spatial_reference(page.get("spatialReference"), name + "_response")
        rows = page.get("features")
        index = int(name[-3:])
        expected = ids[index * BATCH_SIZE:(index + 1) * BATCH_SIZE]
        if not isinstance(rows, list) or len(rows) != len(expected):
            raise ValueError("source_page_population_incomplete:" + name)
        page_ids = []
        for feature in rows:
            attributes, geometry = feature.get("attributes"), feature.get("geometry")
            if not isinstance(attributes, dict) or set(attributes) != {field["name"] for field in ATTRIBUTE_FIELDS}:
                raise ValueError("source_attribute_roster_drift")
            oid = attributes.get("OBJECTID")
            if type(oid) is not int or oid not in expected or oid in seen:
                raise ValueError("source_native_id_drift")
            for field in ATTRIBUTE_FIELDS:
                key, kind = field["name"], field["type"]
                value = attributes[key]
                if value is None:
                    nulls[key] += 1
                    continue
                if kind == "esriFieldTypeOID" and type(value) is not int:
                    raise ValueError("source_oid_type_drift")
                if kind == "esriFieldTypeString" and not isinstance(value, str):
                    raise ValueError("source_string_type_drift:" + key)
                if kind == "esriFieldTypeInteger" and type(value) is not int:
                    raise ValueError("source_integer_type_drift:" + key)
                if kind == "esriFieldTypeDate" and type(value) is not int:
                    raise ValueError("source_date_type_drift:" + key)
                if kind == "esriFieldTypeDouble" and type(value) not in (int, float):
                    raise ValueError("source_double_type_drift:" + key)
            if not isinstance(geometry, dict) or set(geometry) - {"paths", "hasZ", "hasM", "spatialReference"}:
                raise ValueError("source_geometry_structure_drift")
            if geometry.get("spatialReference") is not None:
                _validate_spatial_reference(geometry["spatialReference"], "feature_geometry")
            if geometry.get("hasZ", False) is not False or geometry.get("hasM", False) is not False:
                raise ValueError("source_zm_drift")
            paths = geometry.get("paths")
            if not isinstance(paths, list) or not paths:
                raise ValueError("source_paths_missing")
            for path in paths:
                if not isinstance(path, list) or len(path) < 2:
                    raise ValueError("source_path_invalid")
                for point in path:
                    if (not isinstance(point, list) or len(point) != 2
                            or any(type(value) not in (int, float) or not math.isfinite(value) for value in point)):
                        raise ValueError("source_xy_drift")
                    bounds = [min(bounds[0], point[0]), min(bounds[1], point[1]),
                              max(bounds[2], point[0]), max(bounds[3], point[1])]
                    vertices += 1
            page_ids.append(oid)
            seen.add(oid)
            features.append(feature)
        if sorted(page_ids) != expected:
            raise ValueError("source_page_ids_drift:" + name)
    if seen != set(ids) or len(features) != EXPECTED_COUNT:
        raise ValueError("source_population_incomplete")
    if nulls != EXPECTED_NULL_COUNTS:
        raise ValueError("source_null_count_drift")
    if any(abs(actual - expected) > 1e-3 for actual, expected in zip(bounds, EXPECTED_BOUNDS)):
        raise ValueError("source_geometry_coverage_drift")
    features.sort(key=lambda feature: feature["attributes"]["OBJECTID"])
    audit = {
        "source_feature_count": len(features),
        "source_id_sha256": _sha(_canonical(sorted(seen))),
        "source_id_min": min(seen),
        "source_id_max": max(seen),
        "source_schema_sha256": _sha(_canonical(layer["fields"])),
        "source_record_sha256": _sha(_canonical([feature["attributes"] for feature in features])),
        "source_geometry_sha256": _sha(_canonical([[feature["attributes"]["OBJECTID"], feature["geometry"]] for feature in features])),
        "source_geometry_type": "esriGeometryPolyline",
        "source_wkid": 3857,
        "source_vertex_count": vertices,
        "source_bounds": bounds,
        "source_extent": layer["extent"],
        "attribute_null_counts": nulls,
        "source_coverage_scope": "Complete current USGS Qfaults National Database layer, native EPSG:3857; equivalence to the HIFLD 2024 archive is unverified.",
    }
    if audit["source_schema_sha256"] != EXPECTED_SCHEMA_SHA:
        raise ValueError("source_schema_sha_drift")
    if audit["source_record_sha256"] != EXPECTED_RECORD_SHA:
        raise ValueError("source_record_sha_drift")
    if audit["source_geometry_sha256"] != EXPECTED_GEOMETRY_SHA:
        raise ValueError("source_geometry_sha_drift")
    return features, audit


def _wkb(geometry: dict) -> bytes:
    paths = geometry["paths"]
    out = bytearray(struct.pack("<BI", 1, 5))
    out.extend(struct.pack("<I", len(paths)))
    for path in paths:
        out.extend(struct.pack("<BI", 1, 2))
        out.extend(struct.pack("<I", len(path)))
        for x, y in path:
            out.extend(struct.pack("<dd", float(x), float(y)))
    return bytes(out)


def _parquet_bytes(features: list[dict]) -> bytes:
    arrays = []
    for field in ATTRIBUTE_FIELDS:
        arrays.append(pa.array([row["attributes"][field["name"]] for row in features],
                               type=_ARROW_TYPES[field["type"]]))
    arrays.append(pa.array([_wkb(row["geometry"]) for row in features], type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=[field["name"] for field in ATTRIBUTE_FIELDS] + ["geometry"])
    metadata = {
        b"geo": _canonical({
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {
                "encoding": "WKB",
                "geometry_types": ["MultiLineString"],
                "crs": EXPECTED_CRS.to_json_dict(),
            }},
        })
    }
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _processing_identity(repo_root: Path | None = None) -> dict[str, object]:
    root = Path(repo_root) if repo_root is not None else Path(__file__).parents[3]
    identity = processing_identity(root)
    validate_construction_code_identity(
        root, identity["code_commit"], identity["files"], identity=identity,
    )
    return identity


def capture_quaternary_fault_lines(storage: StagingStorageResource, *, version: str,
                                   capture_directory: str | None = None) -> dict[str, object]:
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
    if shutil.disk_usage(staging).free < 1_000_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        captures = _read_replay(root)
    else:
        root = ((requested / version) if requested else Path("/tmp/hifld-qfl-capture")) / uuid4().hex
        if any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures: dict[str, tuple[bytes, dict]] = {}
        retained = 0
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(90, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/usgs-qfaults-draft"}) as client:
                for name, url in (("service_before", SERVICE_URL + "?f=pjson"),
                                  ("layer_before", LAYER_URL + "?f=pjson"),
                                  ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                                  ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}))):
                    captures[name] = _capture(client, url, root, name, retained)
                    retained += captures[name][1]["bytes"] + captures[name][1].get("wire_bytes", 0)
                ids = json.loads(captures["ids_before"][0]).get("objectIds")
                if not isinstance(ids, list) or len(ids) != EXPECTED_COUNT or _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA:
                    raise ValueError("source_seed_id_roster_drift")
                for index, start in enumerate(range(0, len(ids), BATCH_SIZE)):
                    page_ids = ids[start:start + BATCH_SIZE]
                    url = _query({"objectIds": ",".join(map(str, page_ids)), "outFields": "*",
                                  "returnGeometry": "true", "returnZ": "true", "returnM": "true",
                                  "orderByFields": "OBJECTID ASC", "f": "json"})
                    name = f"page_{index:03d}"
                    captures[name] = _capture(client, url, root, name, retained)
                    retained += captures[name][1]["bytes"] + captures[name][1].get("wire_bytes", 0)
                for name, url in (("layer_after", LAYER_URL + "?f=pjson"),
                                  ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                                  ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                                  ("service_after", SERVICE_URL + "?f=pjson")):
                    captures[name] = _capture(client, url, root, name, retained)
                    retained += captures[name][1]["bytes"] + captures[name][1].get("wire_bytes", 0)
            docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
            _validate_metadata(docs["service_before"], docs["layer_before"])
            if docs["service_after"] != docs["service_before"] or docs["layer_after"] != docs["layer_before"]:
                raise ValueError("source_changed_during_capture")
            if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
                raise ValueError("source_count_drift")
            if docs["ids_after"].get("objectIds") != docs["ids_before"].get("objectIds"):
                raise ValueError("source_id_roster_changed_during_capture")
            pages = {name: payload for name, payload in docs.items() if name.startswith("page_")}
            _features, audit = _validate_population(docs["layer_before"], docs["ids_before"].get("objectIds"), pages)
            manifest = {
                "complete": True,
                "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "source_feature_count": audit["source_feature_count"],
                "source_record_sha256": audit["source_record_sha256"],
                "source_schema_sha256": audit["source_schema_sha256"],
                "artifact_names": sorted(captures),
                "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            }
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
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
                "artifact_names": names,
                "receipts": receipts,
            }) + b"\n")
            raise
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    _validate_metadata(docs["service_before"], docs["layer_before"])
    if docs["service_after"] != docs["service_before"] or docs["layer_after"] != docs["layer_before"]:
        raise ValueError("source_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    if docs["ids_after"].get("objectIds") != docs["ids_before"].get("objectIds"):
        raise ValueError("source_id_roster_changed_during_capture")
    pages = {name: payload for name, payload in docs.items() if name.startswith("page_")}
    features, audit = _validate_population(docs["layer_before"], docs["ids_before"].get("objectIds"), pages)
    parquet = _parquet_bytes(features)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    identity = _processing_identity()
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    inputs = [{
        "input_id": name,
        "producer": PUBLISHER,
        "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": LAYER_URL,
        "retrieval_url": receipt["url"],
        "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"],
        "bytes": receipt["bytes"],
        "sha256": receipt["sha256"],
        **({"wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"]}
           if "wire_bytes" in receipt else {"wire_capture": "legacy_decoded_only_not_available"}),
        "release_date": None,
        "release_date_unknown_reason": "The USGS Qfaults service declares a 2020 map release but no dated data-release or edition identifier.",
        "license": "Rights review pending.",
        "attribution": "U.S. Geological Survey; final attribution review pending.",
    } for name, receipt in sorted(receipts.items())]
    manifest = {
        "schema_version": 1,
        "approved": False,
        "title": "Quaternary Fault Lines",
        "description": "Complete capture of the official USGS Qfaults National Database layer; unapproved candidate only.",
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
            "operation": "Native ESRI polyline feature capture to typed GeoParquet WKB",
            "parameters": {
                "source_fields": "All declared attributes retained in declared types; last_review retained as epoch milliseconds.",
                "source_ids": "OBJECTID retained unchanged; no substitute row identifiers.",
                "source_crs": "EPSG:3857",
                "geometry": "All XY paths and vertex order preserved without reprojection, repair, or simplification.",
                "raw_evidence": ("New capture versions retain original wire bytes, decoded response bodies, and receipts under raw/. "
                                 "Legacy decoded-only replays retain their original bodies and receipts; no wire bytes are synthesized."),
                "candidate_status": "unapproved; archive equivalence, current edition, schema, rights, coverage, and release decisions pending",
            },
        }],
        "lineage": "USGS Quaternary Fault and Fold Database, Qfaults 2020 release service. HIFLD 2024 DataLumos capture and the DHS crosswalk are historical references; exact equivalence is unresolved.",
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {"geoparquet": {
                "reason": "Native attributes and EPSG:3857 XY paths preserved as typed GeoParquet.",
                "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}],
            }},
        },
        "unsupported_output_formats": {
            "geopackage": "No format-specific mapping approved.",
            "shapefile_zip": "No format-specific mapping approved.",
            "pmtiles": "No display generalization or release policy approved.",
        },
    }
    validate_output_policy(manifest)
    provenance_projection(manifest)
    outputs = {
        output_path: parquet,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
    }
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        wire_path = root / f"{name}.wire"
        if "wire_bytes" in receipt:
            outputs[f"raw/{name}.wire"] = wire_path.read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    for name, body in sorted(outputs.items()):
        write_immutable(storage, f"{prefix}/{name}", body)
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
    write_immutable(storage, f"{prefix}/metadata/candidate_ready.json", _canonical(ready) + b"\n")
    return {
        "dataset_slug": DATASET_SLUG,
        "file_slug": FILE_SLUG,
        "version": version,
        "feature_count": EXPECTED_COUNT,
        "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"],
        "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
        "capture_directory": str(root),
        "reused": replay,
        "source_release_accepted": False,
        "source_freshness_accepted": False,
        "schema_contract_accepted": False,
        "id_contract_accepted": False,
        "baseline_accepted": False,
        "coverage_accepted": False,
        "rights_accepted": False,
        "mapping_approved": False,
        "release_accepted": False,
    }
