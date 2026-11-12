"""Complete, source-faithful draft capture of the USACE IENC BERTHS_AREA layer."""

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
from shapely import from_wkb
from shapely.geometry import LinearRing

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    capture_get,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "berths-areas-usace-ienc"
FILE_SLUG = DATASET_SLUG
BTS_ITEM_ID = "3607d485a9854d6fa458bf507058ceae"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
SERVICE_DIRECTORY_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson"
SERVICE_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/BERTHS_AREA/MapServer"
LAYER_ID = 98
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; official IENC Feature Classes service"
EXPECTED_COUNT = 1108
EXPECTED_IDS = list(range(1, EXPECTED_COUNT + 1))
EXPECTED_ID_SHA256 = "601df26e076232404b0e22fd1afacd6ca6461064107ae1ce4b33d1f92b2cfbd5"
EXPECTED_RECORD_SHA256 = "a6437ee21d5a419653eb08ec59a6b938faf18fa5e05c803940e67b104f5381fb"
EXPECTED_GEOMETRY_SHA256 = "8a17537ea5be40535600a98fd1d9cedab85fac9a56ad57528e8e775ba3590539"
EXPECTED_SCHEMA_SHA256 = "55fe5000eb9aacd69e612211f48b5cf5532cee536f1080901d177e5842a0751c"
EXPECTED_EXTENT = {"xmin": -94.33752259999994, "ymin": 29.727434000000073,
                   "xmax": -79.83875189999998, "ymax": 45.02651950000006}
PAGE_SIZE = 500
PAGE_MAX_BYTES = 16_000_000
CAPTURE_MAX_BYTES = 96_000_000
SOURCE_CRS = CRS.from_epsg(4326)
EXPECTED_FIELD_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    *[
        {"name": name, "type": "esriFieldTypeString", "alias": name,
         "length": 254, "domain": None}
        for name in (
            "Category_of_Berth", "Class_of_Dangerous_Cargo", "Text_Description",
            "Depth_Range_Value_1", "Object_Name", "Status", "Information",
        )
    ],
    {"name": "Scale_Minimum", "type": "esriFieldTypeInteger", "alias": "Scale_Minimum", "domain": None},
    *[
        {"name": name, "type": "esriFieldTypeString", "alias": name,
         "length": 254, "domain": None}
        for name in ("Source_Date", "Source_Indication", "Source_Dataset", "Feature_ID")
    ],
    {"name": "SHAPE_Length", "type": "esriFieldTypeDouble", "alias": "SHAPE_Length", "domain": None},
    {"name": "SHAPE_Area", "type": "esriFieldTypeDouble", "alias": "SHAPE_Area", "domain": None},
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELD_DECLARATIONS if field["name"] != "SHAPE"]
EXPECTED_CAPTURE_NAMES = {
    "bts_item", "service_directory", "service_before", "layer_before", "count_before",
    "ids_before", "page_000", "page_001", "page_002", "layer_after", "count_after", "ids_after",
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_urls(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("bts_item", BTS_ITEM_URL),
        ("service_directory", SERVICE_DIRECTORY_URL),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        page_ids = ids[start:start + PAGE_SIZE]
        urls.append((f"page_{index:03d}", _query({
            "objectIds": ",".join(map(str, page_ids)), "outFields": "*",
            "returnGeometry": "true", "returnZ": "true", "returnM": "true",
            "outSR": "4326", "orderByFields": "OBJECTID ASC", "f": "json",
        })))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ])
    return urls


def _capture(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * PAGE_MAX_BYTES > CAPTURE_MAX_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    return capture_get(client, url, root, name, max_bytes=PAGE_MAX_BYTES)


def _load_capture(root: Path, name: str) -> tuple[bytes, bytes, dict]:
    body_path, wire_path, receipt_path = (
        root / f"{name}.body", root / f"{name}.wire", root / f"{name}.receipt.json"
    )
    if any(path.is_symlink() for path in (body_path, wire_path, receipt_path)):
        raise ValueError("retained_capture_symlink_rejected")
    body, wire = body_path.read_bytes(), wire_path.read_bytes()
    receipt = json.loads(receipt_path.read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("retained_capture_receipt_invalid:" + name)
    return body, wire, receipt


def _validate_request_identity(name: str, receipt: dict, expected_url: str) -> None:
    if (receipt.get("method") != "GET" or receipt.get("url") != expected_url
            or receipt.get("final_url") != expected_url):
        raise ValueError("retained_capture_request_identity_mismatch:" + name)


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    receipts = manifest.get("receipts")
    roster = manifest.get("artifact_roster")
    if manifest.get("complete") is not True or not isinstance(receipts, dict) or not isinstance(roster, dict):
        raise ValueError("retained_capture_manifest_invalid")
    if set(receipts) != EXPECTED_CAPTURE_NAMES:
        raise ValueError("retained_capture_name_roster_changed")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    expected_urls = dict(_capture_urls(EXPECTED_IDS))
    captures = {}
    for name in sorted(EXPECTED_CAPTURE_NAMES):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != receipts[name]:
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        _validate_request_identity(name, receipt, expected_urls[name])
        captures[name] = body, receipt
    return captures


def _validate_crs(value: object, label: str, *, required: bool = True) -> bool:
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            code = value[key]
            if type(code) is not int or code != 4326:
                raise ValueError(f"{label}_crs_drift")
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            text = value[key]
            if not isinstance(text, str) or not text.strip():
                raise ValueError(f"{label}_crs_drift")
            try:
                parsed = CRS.from_wkt(text)
            except Exception as exc:
                raise ValueError(f"{label}_crs_drift") from exc
            if not parsed.equals(SOURCE_CRS, ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
            found = True
    if required and not found:
        raise ValueError(f"{label}_crs_drift")
    return found


def _polygon_wkb(geometry: dict) -> bytes:
    """Encode native ArcGIS rings in order without reorientation or repair."""
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    for flag in ("hasZ", "hasM"):
        if flag in geometry and type(geometry[flag]) is not bool:
            raise ValueError("source_polygon_ordinate_flag_invalid")
        if geometry.get(flag, False):
            raise ValueError("source_polygon_not_xy")
    if "spatialReference" in geometry:
        _validate_crs(geometry["spatialReference"], "feature_geometry")
    source_rings = []
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
            raise ValueError("source_polygon_ring_invalid_or_open")
        if any(not isinstance(point, list) or len(point) != 2
               or any(isinstance(value, bool) or not isinstance(value, (int, float))
                      or not math.isfinite(float(value)) for value in point)
               for point in ring):
            raise ValueError("source_polygon_coordinate_not_finite_xy")
        source_rings.append(ring)
    # This layer's observed encoding is one clockwise exterior followed by
    # counter-clockwise interior rings. Any topology/layout change is quarantined.
    if LinearRing(source_rings[0]).is_ccw:
        raise ValueError("source_exterior_ring_winding_drift")
    if any(not LinearRing(ring).is_ccw for ring in source_rings[1:]):
        raise ValueError("source_interior_ring_winding_drift")
    body = bytearray(struct.pack("<BI", 1, 3))
    body.extend(struct.pack("<I", len(source_rings)))
    for ring in source_rings:
        body.extend(struct.pack("<I", len(ring)))
        for point in ring:
            body.extend(struct.pack("<dd", float(point[0]), float(point[1])))
    wkb = bytes(body)
    polygon = from_wkb(wkb)
    if polygon.geom_type != "Polygon" or polygon.is_empty or not polygon.is_valid:
        raise ValueError("source_polygon_invalid_without_repair")
    return wkb


def _attribute_type(field: dict) -> pa.DataType:
    types = {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
             "esriFieldTypeDouble": pa.float64(), "esriFieldTypeString": pa.string()}
    try:
        return types[field["type"]]
    except KeyError as exc:
        raise ValueError("source_attribute_type_drift:" + str(field.get("name"))) from exc


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    arrays = []
    schema_fields = []
    attributes = [field for field in fields if field["name"] != "SHAPE"]
    for field in attributes:
        name, dtype = field["name"], _attribute_type(field)
        values = [feature["attributes"][name] for feature in features]
        if pa.types.is_integer(dtype) and any(value is not None and type(value) is not int for value in values):
            raise ValueError("source_integer_attribute_type_drift:" + name)
        if pa.types.is_floating(dtype) and any(value is not None and (
            isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
        ) for value in values):
            raise ValueError("source_double_attribute_type_drift:" + name)
        if pa.types.is_string(dtype) and any(value is not None and not isinstance(value, str) for value in values):
            raise ValueError("source_string_attribute_type_drift:" + name)
        arrays.append(pa.array(values, type=dtype, safe=True))
        schema_fields.append(pa.field(name, dtype, nullable=True,
                                      metadata={b"esri_field": _canonical(field)}))
    wkb = [_polygon_wkb(feature["geometry"]) for feature in features]
    arrays.append(pa.array(wkb, type=pa.binary()))
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {
        "geometry": {"encoding": "WKB", "geometry_types": ["Polygon"],
                     "crs": SOURCE_CRS.to_json_dict()}}}
    schema = pa.schema(schema_fields, metadata={b"geo": _canonical(geo)})
    table = pa.Table.from_arrays(arrays, schema=schema)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    item = docs["bts_item"]
    if item.get("id") != BTS_ITEM_ID or item.get("owner") != "USDOT_BTS":
        raise ValueError("bts_ienc_item_identity_drift")
    item_text = " ".join(str(item.get(key, "")) for key in ("title", "description"))
    if ("Inland Electronic Navigational Charts" not in item_text
            or "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes" not in item_text
            or "USACE_IENC_Master_Service_gdb.zip" not in item_text):
        raise ValueError("bts_ienc_official_distribution_link_drift")
    if not any(service.get("name") == "IENC_Feature_Classes/BERTHS_AREA"
               and service.get("type") == "MapServer" for service in docs["service_directory"].get("services", [])):
        raise ValueError("official_ienc_service_directory_drift")
    service = docs["service_before"]
    if service.get("mapName") != "BERTHS_AREA" or service.get("copyrightText") != "USACE IENC":
        raise ValueError("source_service_identity_drift")
    _validate_crs(service.get("spatialReference"), "source_service")
    layer = docs["layer_before"]
    fields = layer.get("fields")
    projected = [{key: field[key] for key in ("name", "type", "alias", "length", "domain") if key in field}
                 for field in fields] if isinstance(fields, list) else None
    if projected != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("name") != "BERTHS_AREA" or layer.get("id") != LAYER_ID
            or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("maxRecordCount") != 2000
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError("source_layer_geometry_extent_or_page_limit_drift")
    _validate_crs(layer.get("spatialReference"), "source_layer")
    _validate_crs(layer.get("extent", {}).get("spatialReference"), "source_layer_extent")
    if docs["layer_after"] != layer:
        raise ValueError("source_layer_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (ids_before != EXPECTED_IDS or ids_after != ids_before
            or len(set(ids_before)) != EXPECTED_COUNT
            or docs["ids_before"].get("objectIdFieldName") != "OBJECTID"
            or docs["ids_after"].get("objectIdFieldName") != "OBJECTID"):
        raise ValueError("source_id_population_drift")
    if _sha(_canonical(sorted(ids_before))) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_hash_drift")
    features = []
    page_ids = []
    for name in ("page_000", "page_001", "page_002"):
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_transfer_limit")
        if page.get("geometryType") != "esriGeometryPolygon":
            raise ValueError("source_page_geometry_type_drift")
        _validate_crs(page.get("spatialReference"), name + "_spatial_reference")
        records = page.get("features")
        if not isinstance(records, list):
            raise ValueError("source_page_features_missing")
        features.extend(records)
        page_ids.extend(feature.get("attributes", {}).get("OBJECTID") for feature in records)
    if (len(features) != EXPECTED_COUNT or sorted(page_ids) != EXPECTED_IDS
            or len(set(page_ids)) != EXPECTED_COUNT):
        raise ValueError("source_feature_population_incomplete")
    expected_attributes = [field["name"] for field in ATTRIBUTE_FIELDS]
    for feature in features:
        attrs, geometry = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or list(attrs) != expected_attributes:
            raise ValueError("source_record_attribute_schema_drift")
        if not isinstance(geometry, dict) or not isinstance(geometry.get("rings"), list):
            raise ValueError("source_record_polygon_missing")
        _polygon_wkb(geometry)
    features.sort(key=lambda feature: feature["attributes"]["OBJECTID"])
    records_sha = _sha(_canonical([feature["attributes"] for feature in features]))
    geometry_sha = _sha(_canonical([[feature["attributes"]["OBJECTID"], feature["geometry"]]
                                    for feature in features]))
    schema_sha = _sha(_canonical(fields))
    if records_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_snapshot_drift")
    if geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_snapshot_drift")
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_snapshot_drift")
    xs, ys = [], []
    vertices = 0
    ring_count_histogram: dict[str, int] = {}
    source_dates = []
    for feature in features:
        geometry = feature["geometry"]
        ring_count_histogram[str(len(geometry["rings"]))] = ring_count_histogram.get(str(len(geometry["rings"])), 0) + 1
        for ring in geometry["rings"]:
            vertices += len(ring)
            xs.extend(point[0] for point in ring)
            ys.extend(point[1] for point in ring)
        source_dates.append(feature["attributes"]["Source_Date"])
    extent = {"xmin": min(xs), "ymin": min(ys), "xmax": max(xs), "ymax": max(ys)}
    if extent != EXPECTED_EXTENT:
        raise ValueError("source_geometry_coverage_extent_drift")
    feature_id_unique_count = len({feature["attributes"]["Feature_ID"] for feature in features})
    audit = {
        "source_feature_count": len(features), "source_population_complete": True,
        "source_id_sha256": EXPECTED_ID_SHA256, "source_id_min": ids_before[0],
        "source_id_max": ids_before[-1], "source_schema_sha256": schema_sha,
        "source_record_sha256": records_sha, "source_geometry_sha256": geometry_sha,
        "source_coverage_extent": extent, "source_polygon_vertex_count": vertices,
        "source_ring_count_histogram": ring_count_histogram,
        "source_feature_id_unique_count": feature_id_unique_count,
        "source_feature_id_duplicate_count": len(features) - feature_id_unique_count,
        "source_feature_id_note": "Feature_ID is preserved as published but is not unique; OBJECTID is the only pinned unique source key.",
        "source_feature_source_date_min": min(source_dates),
        "source_feature_source_date_max": max(source_dates),
        "source_wkid": 4326, "source_geometry_type": "esriGeometryPolygon",
        "source_coverage_scope": "Complete observed BERTHS_AREA MapServer layer; only the returned IENC chart coverage area is represented.",
        "drift_guard": "Exact producer service/layer, schema, count, complete ID roster, all native attributes, geometry content, CRS and extent are pinned.",
    }
    return fields, features, audit


def _read_capture(root: Path, storage: StagingStorageResource,
                  version: str) -> dict[str, tuple[bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_bytes())
        if manifest.get("complete") is not True:
            raise ValueError("retained_capture_manifest_not_complete")
        return _read_replay(root)
    if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")):
        raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
    root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                          headers={"User-Agent": "hifld-next-datasets/usace-ienc-draft"}) as client:
            for name, url in _capture_urls(EXPECTED_IDS):
                captures[name] = _capture(client, url, root, name)
            _validate(captures)
    except Exception as exc:
        retained = {}
        for path in sorted(root.glob("*.receipt.json")):
            try:
                name = path.name.removesuffix(".receipt.json")
                receipt = json.loads(path.read_bytes())
                retained[name] = receipt
                body_path, wire_path = root / f"{name}.body", root / f"{name}.wire"
                if body_path.is_file() and wire_path.is_file():
                    captures.setdefault(name, (body_path.read_bytes(), receipt))
            except (OSError, ValueError):
                retained[path.name] = {"receipt_readback_failed": True}
        (root / "capture_manifest.json").write_bytes(_canonical({
            "captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
            "error_code": type(exc).__name__, "receipts": retained,
        }) + b"\n")
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, version,
                                           captures, type(exc).__name__)
        raise
    (root / "capture_manifest.json").write_bytes(_canonical({
        "captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
        "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"}),
    }) + b"\n")
    return _read_replay(root)


def _write_outputs(storage: StagingStorageResource, prefix: str,
                   outputs: dict[str, bytes]) -> dict[str, dict[str, object]]:
    descriptors = {}
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
        snapshot = storage.object_snapshot(f"{prefix}/{name}")
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if (snapshot is None or snapshot.size != descriptor["bytes"]
                or storage.sha256_key(f"{prefix}/{name}") != descriptor["sha256"]
                or storage.object_snapshot(f"{prefix}/{name}") != snapshot):
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    return descriptors


def capture_usace_berths_areas(storage: StagingStorageResource, *, version: str,
                               capture_directory: str | None = None) -> dict:
    """Capture/replay one complete local-only source candidate with sealed caching."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not isinstance(version, str) or not version or Path(version).name != version or "/" in version or "\\" in version:
        raise ValueError("unsafe_capture_version")
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    if requested and (".." in requested.parts or any(path.is_symlink() for path in (requested, *requested.parents))):
        raise ValueError("unsafe_capture_directory")
    root = requested or Path("/tmp/hifld-native-reconstruction/21/captures") / (version + "-" + uuid4().hex)
    if not requested:
        root.mkdir(parents=True, exist_ok=False)
    captures = _read_capture(root, storage, version)
    fields, features, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    capture_evidence = {}
    for name in sorted(captures):
        _body, _receipt = captures[name]
        _body2, wire, _receipt2 = _load_capture(root, name)
        capture_evidence[name] = {"decoded_sha256": _sha(_body), "wire_sha256": _sha(wire)}
    capture_sha = _sha(_canonical(capture_evidence))
    audit.update({
        "source_capture_evidence_sha256": capture_sha, "source_capture_evidence": capture_evidence,
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False, "baseline_accepted": False,
    })
    record_sha = audit["source_record_sha256"]
    geometry_sha = audit["source_geometry_sha256"]
    schema_sha = audit["source_schema_sha256"]
    identity_sha = _sha(_canonical(identity))
    candidate_version = "v" + _sha(_canonical({
        "record_sha256": record_sha, "geometry_sha256": geometry_sha,
        "schema_sha256": schema_sha, "decoded_wire_evidence_sha256": capture_sha,
        "processing_identity": identity,
    }))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, candidate_version)
    parquet = _parquet_bytes(fields, features)
    inputs = []
    for name, (_body, receipt) in sorted(captures.items()):
        distributor = name == "bts_item"
        inputs.append({
            "input_id": name,
            "producer": "USDOT Bureau of Transportation Statistics (BTS) IENC metadata" if distributor else PUBLISHER,
            "host": urlsplit(receipt["url"]).hostname,
            "role": "official_distributor" if distributor else "producer_publication",
            "canonical_url": BTS_ITEM_URL if distributor else LAYER_URL,
            "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
            "release_date": None,
            "release_date_unknown_reason": "Neither the layer metadata nor BTS IENC item gives a release date for this exact extracted layer snapshot; per-feature Source_Date values are retained as attributes, not interpreted as dataset release dates.",
            "license": "Unresolved; rights review pending.",
            "attribution": "USACE IENC / BTS; attribution and rights review pending.",
            **({"distributor": "USACE IENC Feature Classes service", "delegation_evidence": {
                "bts_item_url": BTS_ITEM_URL, "bts_item_id": BTS_ITEM_ID,
                "bts_item_owner": "USDOT_BTS", "official_service_directory": SERVICE_DIRECTORY_URL,
                "linked_feature_classes_service": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes",
                "basis": "Captured BTS item identifies the IENC program and links the official service and native GDB; the service publishes copyright USACE IENC.",
            }} if distributor else {}),
        })
    manifest = {
        "schema_version": 1, "approved": False, "title": "Berths Areas (USACE IENC)",
        "description": "Complete observed official USACE IENC BERTHS_AREA feature class; faithful draft only. Coverage equals the source layer extent and does not assert equivalence to the historical HIFLD baseline.",
        "publisher": PUBLISHER, "agency": "US Army Corps of Engineers", "source_url": LAYER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Native Esri JSON polygon rings to GeoParquet WKB candidate",
            "parameters": {"source_fields": "all declared non-geometry fields retained in source order and types",
                           "source_ids": "OBJECTID and duplicate-capable Feature_ID both retained unchanged",
                           "geometry": "all XY ring coordinates, ring order, and winding retained without reprojection, reorientation, repair, or simplification",
                           "candidate_status": "unapproved; source release, baseline, ID/schema, coverage, and rights decisions remain pending"}}],
        "lineage": "BTS IENC item 3607d485a9854d6fa458bf507058ceae links the official USACE IENC Feature Classes service and master GDB; this candidate is exactly BERTHS_AREA MapServer layer 98.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "All native attributes, OBJECTID, duplicate-capable Feature_ID values, and source-order polygon rings are retained; release and compatibility decisions remain pending.",
            "objects": [{"path": f"geoparquet/{FILE_SLUG}.parquet", "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {
            "geopackage": "No accepted historical schema/ID contract or pinned native GDB byte comparison.",
            "pmtiles": "No projection, simplification, or display transformation approved.",
            "shapefile_zip": "No approved schema mapping or field-width and precision review.",
        },
        "promotion_allowed": False, "source_accepted": False, "approved_contract": None,
        "baseline": None,
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs = {
        f"geoparquet/{FILE_SLUG}.parquet": parquet,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
    }
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {
        "approved": False, "version": candidate_version,
        "objects": {f"geoparquet/{FILE_SLUG}.parquet": {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True, "source_record_sha256": record_sha,
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    }
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    state_key = f"_construction-state/{DATASET_SLUG}/{candidate_version}.json"
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, candidate_version, identity_sha))
        payload = bundle.get("payload")
        if not isinstance(payload, dict) or payload.get("objects") is None or payload.get("capture_receipts") is None:
            raise ValueError("cached_candidate_construction_state_missing")
        for name, (body, _receipt) in captures.items():
            if storage.read_key(f"{prefix}/raw/{name}.body") != body:
                raise ValueError("cached_source_bytes_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix,
                                candidate_version, identity_sha, identity, record_sha,
                                schema_sha, payload["objects"], payload["capture_receipts"])
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
                "feature_count": EXPECTED_COUNT, "source_population_complete": True,
                "source_id_sha256": EXPECTED_ID_SHA256, "source_id_min": 1, "source_id_max": EXPECTED_COUNT,
                "source_schema_sha256": schema_sha, "source_coverage_extent": EXPECTED_EXTENT,
                "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
                "source_feature_id_unique_count": audit["source_feature_id_unique_count"],
                "source_feature_id_duplicate_count": audit["source_feature_id_duplicate_count"],
                "source_coverage_scope": audit["source_coverage_scope"], "drift_guard": audit["drift_guard"],
                "source_capture_evidence_sha256": capture_sha,
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "release_accepted": False, "capture_directory": str(root)}
    descriptors = _write_outputs(storage, prefix, outputs)
    seal_ready_candidate(storage, state_key, candidate_version, identity_sha, {
        "processing_identity": identity, "source_record_sha256": record_sha,
        "source_schema_sha256": schema_sha,
        "capture_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "objects": descriptors,
    })
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
    )
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": EXPECTED_ID_SHA256, "source_id_min": 1, "source_id_max": EXPECTED_COUNT,
            "source_schema_sha256": schema_sha, "source_coverage_extent": EXPECTED_EXTENT,
            "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
            "source_feature_id_unique_count": audit["source_feature_id_unique_count"],
            "source_feature_id_duplicate_count": audit["source_feature_id_duplicate_count"],
            "source_coverage_scope": audit["source_coverage_scope"], "drift_guard": audit["drift_guard"],
            "source_capture_evidence_sha256": capture_sha,
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "capture_directory": str(root),
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False}
