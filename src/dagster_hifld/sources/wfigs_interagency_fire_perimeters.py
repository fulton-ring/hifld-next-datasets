"""Complete coherent-snapshot capture of the NIFC WFIGS Interagency Fire Perimeters.

The National Interagency Fire Center organization (ArcGIS Online org
``T4QMspbfLg3qTGWY``, item owner ``NIFC_Authoritative``) publishes the full
WFIGS Interagency Fire Perimeters feature service; the producer owns this
publication, so no mirror or archive is involved. The layer is live data
(service editing continues continuously), so the draft proves completeness for
one coherent snapshot: count/ids/object rosters and the layer ``editingInfo``
edit witness must match before and after every page. Everything is retained
unrepaired: rings are reassembled into shells/holes by strict whole-ring
containment with hole-aware nesting depth, without adding, removing or
reordering vertices, and invalid or degenerate producer polygons are preserved
and release-blocked. Source, schema/ID
contract, baseline, rights and release acceptance remain unapproved.
"""
from __future__ import annotations

import io
import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.esri_geometry import to_wkb, zero_area_rings
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    capture_get,
    content_derived_version,
    processing_identity,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = FILE_SLUG = "wfigs-interagency-fire-perimeters"
PUBLISHER = "National Interagency Fire Center (NIFC) WFIGS ArcGIS Online organization"
AGENCY = "National Wildfire Coordinating Group / National Interagency Fire Center"
ORG_ID = "T4QMspbfLg3qTGWY"
OWNER = "NIFC_Authoritative"
SERVICE_ITEM_ID = "5e72b1699bf74eefb3f3aff6f4ba5511"
ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{SERVICE_ITEM_ID}?f=pjson"
SERVICE_URL = (
    f"https://services3.arcgis.com/{ORG_ID}/arcgis/rest/services/"
    "WFIGS_Interagency_Perimeters/FeatureServer"
)
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LANDING_URL = "https://data-nifc.opendata.arcgis.com/datasets/nifc::wfigs-interagency-fire-perimeters/about"
LAYER_NAME = "Perimeters"
GEOMETRY_TYPE = "esriGeometryPolygon"
OBJECT_ID_FIELD = "OBJECTID"
GLOBAL_ID_FIELD = "GlobalID"
PAGE_SIZE = 2000
MAX_RESPONSE_BYTES = 256 * 1024 * 1024
MAX_CAPTURE_BYTES = 4_000_000_000
NATIVE_CRS = CRS.from_epsg(4326)
SERVICE_HOST = urlsplit(SERVICE_URL).hostname
SHARING_HOST = urlsplit(ITEM_URL).hostname
STRING_TYPES = {"esriFieldTypeString", "esriFieldTypeGUID", "esriFieldTypeGlobalID"}
INT_TYPES = {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"}
FLOAT_TYPES = {"esriFieldTypeDouble", "esriFieldTypeSingle"}
ARROW_TYPES = {
    "esriFieldTypeOID": pa.int64(),
    "esriFieldTypeInteger": pa.int64(),
    "esriFieldTypeSmallInteger": pa.int32(),
    "esriFieldTypeString": pa.string(),
    "esriFieldTypeGUID": pa.string(),
    "esriFieldTypeGlobalID": pa.string(),
    "esriFieldTypeDate": pa.timestamp("ms", tz="UTC"),
    "esriFieldTypeDouble": pa.float64(),
    "esriFieldTypeSingle": pa.float32(),
}
ESRI_TO_ARROW_TYPE = {
    "esriFieldTypeOID": "int64",
    "esriFieldTypeInteger": "int64",
    "esriFieldTypeSmallInteger": "int32",
    "esriFieldTypeString": "string",
    "esriFieldTypeGUID": "string",
    "esriFieldTypeGlobalID": "string",
    "esriFieldTypeDate": "timestamp[ms, UTC]",
    "esriFieldTypeDouble": "float64",
    "esriFieldTypeSingle": "float32",
}
LAYER_PIN_PROJECTION = (
    "id", "name", "geometryType", "objectIdField", "globalIdField",
    "hasZ", "hasM", "maxRecordCount", "spatialReference", "fields", "geometryField",
    "supportedQueryFormats", "advancedQueryCapabilities",
)
PINNED_SPATIAL_REFERENCE = {"wkid": 4326, "latestWkid": 4326}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _sr(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError("wfigs_spatial_reference_missing:" + label)
    observed = {key: value.get(key) for key in ("wkid", "latestWkid") if key in value}
    if not observed or any(observed.get(key) != PINNED_SPATIAL_REFERENCE[key] for key in observed):
        raise ValueError("wfigs_spatial_reference_drift:" + label)


def _validate_rings(rings: object) -> list[list[list[float]]] | None:
    """Validate one Esri JSON polygon geometry; empty rings means null geometry."""
    if not isinstance(rings, list):
        if rings is None:
            return None
        raise ValueError("wfigs_polygon_geometry_invalid")
    if not rings:
        return None
    for ring in rings:
        if not isinstance(ring, list) or not ring:
            raise ValueError("wfigs_polygon_ring_invalid")
        for vertex in ring:
            if not isinstance(vertex, list) or len(vertex) != 2:
                raise ValueError("wfigs_polygon_vertex_invalid")
            for value in vertex:
                if type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError("wfigs_polygon_vertex_invalid")
            if not (-180 <= vertex[0] <= 180 and -90 <= vertex[1] <= 90):
                raise ValueError("wfigs_polygon_coordinates_invalid")
        if ring[0] != ring[-1]:
            raise ValueError("wfigs_polygon_ring_unclosed")
    return rings


def _census(rings: list, geometry: object) -> dict[str, int]:
    """Per-feature conversion census over served rings and the GDAL result.

    Ring orientation is recorded against the Esri JSON convention (exterior
    clockwise, hole counterclockwise) but never decides nesting: membership is
    GDAL's through the shared native Esri polygon conversion, so a
    noncanonical producer ring cannot become an invented hole. The census is
    returned so invalid conversions stay visible and release-blocked.
    """
    parts = geometry.geoms if geometry.geom_type == "MultiPolygon" else [geometry]
    noncanonical = 0
    for polygon in parts:
        if polygon.exterior.is_ccw:
            noncanonical += 1
        noncanonical += sum(1 for hole in polygon.interiors if not hole.is_ccw)
    return {"rings": len(rings), "parts": len(parts),
            "holes": sum(len(polygon.interiors) for polygon in parts),
            "degenerate_rings": len(zero_area_rings(rings)),
            "invalid": 0 if geometry.is_valid else 1,
            "empty": 1 if geometry.is_empty else 0,
            "vertices": sum(len(ring) for ring in rings),
            "noncanonical_orientation": noncanonical}


def _validate_page_metadata(page: dict, label: str) -> None:
    if page.get("geometryType") != GEOMETRY_TYPE:
        raise ValueError("wfigs_page_geometry_type_drift:" + label)
    _sr(page.get("spatialReference"), "page:" + label)


def _fields(layer: dict) -> list[dict]:
    fields = layer.get("fields")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("geometryType") != GEOMETRY_TYPE
            or layer.get("objectIdField") != OBJECT_ID_FIELD
            or layer.get("globalIdField") != GLOBAL_ID_FIELD
            or layer.get("hasZ") is not False or layer.get("hasM") is not False
            or not isinstance(fields, list)
            or layer.get("maxRecordCount") != PAGE_SIZE
            or fields[0].get("name") != OBJECT_ID_FIELD):
        raise ValueError("wfigs_layer_identity_or_schema_drift")
    _sr(layer.get("spatialReference"), "layer")
    for field in fields:
        if field.get("type") not in ARROW_TYPES:
            raise ValueError("wfigs_unsupported_field_type:" + str(field.get("name")))
        if field.get("name") == OBJECT_ID_FIELD and field.get("type") != "esriFieldTypeOID":
            raise ValueError("wfigs_oid_field_type_drift")
    names = [field["name"] for field in fields]
    if len(names) != len(set(names)) or OBJECT_ID_FIELD not in names or GLOBAL_ID_FIELD not in names:
        raise ValueError("wfigs_field_roster_invalid")
    return fields


def _validate_request_scope(name: str, receipt: dict, page_size: int) -> None:
    url = receipt.get("url")
    if not isinstance(url, str):
        raise ValueError("wfigs_capture_url_missing:" + name)
    parsed = urlsplit(url)
    if name == "item":
        host, expected = SHARING_HOST, urlsplit(ITEM_URL)
    elif name.startswith(("service_", "layer_", "count_", "ids_", "page_")):
        host, expected = SERVICE_HOST, urlsplit(_scope_query(name, page_size))
    else:
        raise ValueError("wfigs_unexpected_capture_name:" + name)
    if parsed.scheme != "https" or parsed.hostname != host:
        raise ValueError("wfigs_capture_endpoint_drift:" + name)
    if (parsed.path, parsed.query) != (expected.path, expected.query):
        raise ValueError("wfigs_capture_request_scope_drift:" + name)
    if receipt.get("final_url") != url:
        raise ValueError("wfigs_capture_redirect_not_pinned:" + name)


def _scope_query(name: str, page_size: int = PAGE_SIZE) -> str:
    if name.startswith("service_"):
        return SERVICE_URL + "?f=pjson"
    if name.startswith("layer_"):
        return LAYER_URL + "?f=pjson"
    if name.startswith("count_"):
        return _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})
    if name.startswith("ids_"):
        return _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})
    if name.startswith("page_"):
        offset = int(name.removeprefix("page_"))
        return _query({
            "where": "1=1", "outFields": "*", "returnGeometry": "true",
            "orderByFields": f"{OBJECT_ID_FIELD} ASC",
            "resultOffset": str(offset), "resultRecordCount": str(page_size), "f": "json",
        })
    raise ValueError("wfigs_unexpected_capture_name:" + name)


def _validate_attributes(attributes: dict, fields: list[dict], label: str) -> None:
    names = [field["name"] for field in fields]
    if [str(key) for key in attributes] != names:
        raise ValueError("wfigs_attribute_roster_drift:" + label)
    for field in fields:
        name, kind = field["name"], field["type"]
        value = attributes[name]
        if value is None:
            if field.get("nullable") is False:
                raise ValueError("wfigs_nonnullable_null:" + name + ":" + label)
        elif kind in STRING_TYPES:
            length = field.get("length")
            if not isinstance(value, str) or (isinstance(length, int) and length > 0 and len(value) > length):
                raise ValueError("wfigs_attribute_type_drift:" + name + ":" + label)
        elif kind in INT_TYPES and type(value) is not int:
            raise ValueError("wfigs_attribute_type_drift:" + name + ":" + label)
        elif kind in FLOAT_TYPES and (type(value) not in (int, float) or not math.isfinite(value)):
            raise ValueError("wfigs_attribute_type_drift:" + name + ":" + label)
        elif kind == "esriFieldTypeDate" and type(value) is not int:
            raise ValueError("wfigs_attribute_type_drift:" + name + ":" + label)


def _population(captures: dict[str, tuple[bytes, dict]], page_size: int = PAGE_SIZE) -> tuple[dict, list[dict], dict]:
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    if _canonical(docs["service_before"]) != _canonical(docs["service_after"]):
        raise ValueError("wfigs_service_changed_during_capture")
    layer, layer_after = docs["layer_before"], docs["layer_after"]
    fields = _fields(layer)
    pinned = {key: layer.get(key) for key in LAYER_PIN_PROJECTION}
    if pinned != {key: layer_after.get(key) for key in LAYER_PIN_PROJECTION}:
        raise ValueError("wfigs_schema_changed_during_capture")
    witness = layer.get("editingInfo", {})
    if not isinstance(witness, dict) or witness != layer_after.get("editingInfo", {}):
        raise ValueError("wfigs_edit_witness_changed_during_capture")
    item = docs["item"]
    if (item.get("id") != SERVICE_ITEM_ID or item.get("owner") != OWNER
            or item.get("orgId") != ORG_ID or item.get("type") != "Feature Service"):
        raise ValueError("wfigs_producer_item_identity_drift")
    ids_before = docs["ids_before"].get("objectIds")
    ids_after = docs["ids_after"].get("objectIds")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    if (not isinstance(ids_before, list) or not ids_before
            or any(type(value) is not int for value in ids_before)
            or len(ids_before) != len(set(ids_before))
            or sorted(ids_before) != sorted(ids_after or [])
            or count_before != len(ids_before) or count_after != len(ids_before)):
        raise ValueError("wfigs_population_changed_or_incomplete")
    ordered_ids = sorted(ids_before)
    expected_pages = {f"page_{start:05d}" for start in range(0, len(ordered_ids), page_size)}
    if {name for name in docs if name.startswith("page_")} != expected_pages:
        raise ValueError("wfigs_page_roster_drift")
    for name in docs:
        _validate_request_scope(name, captures[name][1], page_size)
        if name.startswith("layer_"):
            continue
        if isinstance(docs[name], dict) and docs[name].get("error"):
            raise ValueError("wfigs_source_error_response:" + name)
    features: list[dict] = []
    ring_sets: list[list[list[list[float]]] | None] = []
    geometry_census = {"rings": 0, "parts": 0, "holes": 0, "degenerate_rings": 0,
                       "invalid": 0, "empty": 0, "vertices": 0}
    null_geometry_objectids = []
    records, geometries = [], []
    bounds = [math.inf, math.inf, -math.inf, -math.inf]
    for name in sorted(n for n in docs if n.startswith("page_")):
        page = docs[name]
        _validate_page_metadata(page, name)
        rows = page.get("features")
        offset = int(name.removeprefix("page_"))
        expected = ordered_ids[offset:offset + page_size]
        if not isinstance(rows, list) or len(rows) != len(expected):
            raise ValueError("wfigs_page_population_incomplete:" + name)
        if page.get("exceededTransferLimit") is True and len(rows) != page_size:
            raise ValueError("wfigs_page_error_or_truncation:" + name)
        for position, row in enumerate(rows):
            attributes = row.get("attributes")
            if not isinstance(attributes, dict):
                raise ValueError("wfigs_feature_attributes_missing:" + name)
            oid = attributes.get(OBJECT_ID_FIELD)
            if type(oid) is not int or expected[position] != oid:
                raise ValueError("wfigs_page_order_or_id_drift:" + name)
            _validate_attributes(attributes, fields, name)
            global_id = attributes.get(GLOBAL_ID_FIELD)
            if global_id is not None and not isinstance(global_id, str):
                raise ValueError("wfigs_attribute_type_drift:" + GLOBAL_ID_FIELD + ":" + name)
            raw_geometry = row.get("geometry")
            if raw_geometry is not None and not isinstance(raw_geometry, dict):
                raise ValueError("wfigs_polygon_geometry_invalid:" + name)
            if isinstance(raw_geometry, dict) and set(raw_geometry) - {"rings"}:
                raise ValueError("wfigs_geometry_member_drift:" + name)
            rings = _validate_rings(raw_geometry.get("rings") if isinstance(raw_geometry, dict) else None)
            ring_sets.append(rings)
            if rings is None:
                null_geometry_objectids.append(oid)
                geometry = None
            else:
                # Ring nesting is GDAL's through the shared native Esri
                # polygon conversion; a conversion failure refuses the
                # capture, and OGC-invalid producer topology is retained
                # verbatim and counted in the census, never repaired.
                geometry = shapely.from_wkb(to_wkb({"rings": rings}, GEOMETRY_TYPE,
                                                   has_z=False, has_m=False))
                feature_census = _census(rings, geometry)
                for key in geometry_census:
                    geometry_census[key] += feature_census[key]
                if not geometry.is_empty:
                    west, south, east, north = geometry.bounds
                    bounds = [min(bounds[0], west), min(bounds[1], south),
                              max(bounds[2], east), max(bounds[3], north)]
            records.append(attributes)
            geometries.append(geometry)
    if len(records) != len(ordered_ids):
        raise ValueError("wfigs_feature_roster_mismatch")
    global_ids = [record.get(GLOBAL_ID_FIELD) for record in records]
    observed_global_ids = [value for value in global_ids if value is not None]
    if len(observed_global_ids) != len(set(observed_global_ids)):
        raise ValueError("wfigs_global_id_duplicates")
    if bounds[0] is math.inf:
        bounds = [0.0, 0.0, 0.0, 0.0]
    null_counts = {field["name"]: sum(1 for record in records if record.get(field["name"]) is None)
                   for field in fields}
    audit = {
        "feature_count": len(records),
        "source_population_complete": True,
        "source_id_sha256": _sha(_canonical(ordered_ids)),
        "schema_sha256": _sha(_canonical(fields)),
        "record_sha256": _sha(_canonical(records)),
        "geometry_sha256": _sha(_canonical(ring_sets)),
        "ring_sha256": _sha(_canonical(ring_sets)),
        "native_crs": "EPSG:4326",
        "native_bounds": bounds,
        "field_names": [field["name"] for field in fields],
        "source_field_declarations": fields,
        "null_counts": null_counts,
        "null_geometry_objectids": null_geometry_objectids,
        "geometry_census": geometry_census,
        "global_id_unique_count": len(set(observed_global_ids)),
        "page_count": len(expected_pages),
        "edit_witness": witness,
        "item_owner": OWNER,
        "item_org_id": ORG_ID,
    }
    return layer, [{"attributes": record, "geometry": geometry}
                   for record, geometry in zip(records, geometries)], audit


def _parquet(features: list[dict], fields: list[dict], audit: dict) -> bytes:
    arrays, schema = [], []
    for field in fields:
        name, kind = field["name"], field["type"]
        values = [feature["attributes"][name] for feature in features]
        dtype = ARROW_TYPES[kind]
        if kind == "esriFieldTypeDate":
            values = [None if value is None else datetime.fromtimestamp(value / 1000, tz=timezone.utc)
                      for value in values]
        elif kind in FLOAT_TYPES:
            values = [None if value is None else float(value) for value in values]
        arrays.append(pa.array(values, type=dtype, safe=True))
        schema.append(pa.field(name, dtype, nullable=field.get("nullable") is not False))
    wkbs = [None if feature["geometry"] is None else feature["geometry"].wkb
            for feature in features]
    arrays.append(pa.array(wkbs, type=pa.binary()))
    schema.append(pa.field("geometry", pa.binary(), nullable=bool(audit["null_geometry_objectids"])))
    geometry_types = sorted({("Polygon" if isinstance(feature["geometry"], shapely.Polygon)
                              else "MultiPolygon") for feature in features
                             if feature["geometry"] is not None})
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": geometry_types,
        "crs": NATIVE_CRS.to_json_dict(), "bbox": audit["native_bounds"]}}}
    table = pa.Table.from_arrays(arrays, schema=pa.schema(schema, metadata={b"geo": _canonical(geo)}))
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    body = sink.getvalue()
    readback = pq.read_table(pa.BufferReader(pa.py_buffer(body)))
    if readback.num_rows != len(features) or readback.schema.names != audit["field_names"] + ["geometry"]:
        raise ValueError("wfigs_parquet_readback_schema_drift")
    if readback.column(OBJECT_ID_FIELD).to_pylist() != [f["attributes"][OBJECT_ID_FIELD] for f in features]:
        raise ValueError("wfigs_parquet_oid_parity_failure")
    for field in fields:
        name, kind = field["name"], field["type"]
        expected = [feature["attributes"][name] for feature in features]
        if kind == "esriFieldTypeDate":
            expected = [None if value is None else datetime.fromtimestamp(value / 1000, tz=timezone.utc)
                        for value in expected]
        elif kind in FLOAT_TYPES:
            expected = [None if value is None else float(value) for value in expected]
        if readback.column(name).to_pylist() != expected:
            raise ValueError("wfigs_parquet_attribute_parity_failure:" + name)
    if readback.column("geometry").to_pylist() != wkbs:
        raise ValueError("wfigs_parquet_native_wkb_parity_failure")
    metadata = json.loads(readback.schema.metadata[b"geo"])
    if (metadata["columns"]["geometry"].get("encoding") != "WKB"
            or not CRS.from_json_dict(metadata["columns"]["geometry"]["crs"]).equals(NATIVE_CRS)):
        raise ValueError("wfigs_parquet_crs_or_encoding_drift")
    return body


def _processing_identity(repo_root: Path | None = None) -> dict[str, object]:
    root = Path(repo_root) if repo_root is not None else Path(__file__).parents[3]
    identity = processing_identity(root)
    validate_construction_code_identity(root, identity["code_commit"], identity["files"], identity=identity)
    return identity


def _capture(client: httpx.Client, url: str, root: Path, name: str, retained: int) -> tuple[bytes, dict]:
    if type(retained) is not int or retained < 0 or retained > MAX_CAPTURE_BYTES:
        raise ValueError("wfigs_capture_budget_invalid")

    def reserve(progress) -> None:
        prospective = (retained + progress.wire_bytes_retained + progress.decoded_bytes_retained
                       + progress.reserve_bytes)
        if prospective > MAX_CAPTURE_BYTES:
            raise OSError("wfigs_capture_budget_exceeded")

    return capture_get(client, url, root, name, max_bytes=MAX_RESPONSE_BYTES, reserve=reserve)


def _load(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest = json.loads((root / "capture_manifest.json").read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("artifact_names"), list):
        raise ValueError("wfigs_capture_manifest_invalid")
    actual = {path.name for path in root.iterdir()}
    expected = {"capture_manifest.json"}
    for name in manifest["artifact_names"]:
        expected.update({f"{name}.body", f"{name}.receipt.json", f"{name}.wire"})
    if actual != expected or any((root / name).is_symlink() for name in actual):
        raise ValueError("wfigs_capture_roster_invalid")
    result = {}
    for name in manifest["artifact_names"]:
        body = (root / f"{name}.body").read_bytes()
        wire = (root / f"{name}.wire").read_bytes()
        receipt = json.loads((root / f"{name}.receipt.json").read_bytes())
        if (receipt != manifest["receipts"].get(name) or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
            raise ValueError("wfigs_capture_receipt_mismatch:" + name)
        if len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("wfigs_response_budget_exceeded")
        result[name] = (body, receipt)
    if sum(len(body) + int(receipt.get("wire_bytes", 0)) for body, receipt in result.values()) > MAX_CAPTURE_BYTES:
        raise ValueError("wfigs_capture_budget_exceeded")
    return result


def capture_wfigs_interagency_fire_perimeters(storage: StagingStorageResource, *, version: str | None = None,
                                              capture_directory: str | None = None,
                                              page_size: int = PAGE_SIZE) -> dict[str, object]:
    """Capture one coherent snapshot of the complete producer layer into local staging."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("wfigs_isolated_local_staging_required")
    staging = Path(storage.local_dir).absolute()
    if any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("wfigs_unsafe_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 12_000_000_000:
        raise ValueError("wfigs_disk_floor_before_capture")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        captures = _load(root)
    else:
        root = ((requested / (version or "draft")) if requested
                else Path("/tmp/hifld-wfigs-capture")) / uuid4().hex
        if any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("wfigs_unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        retained = 0
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(120, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/wfigs-fire-perimeters-draft"}) as client:
                static = [
                    ("item", ITEM_URL),
                    ("service_before", SERVICE_URL + "?f=pjson"),
                    ("layer_before", LAYER_URL + "?f=pjson"),
                    ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                    ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                ]
                for name, url in static:
                    captures[name] = _capture(client, url, root, name, retained)
                    retained += captures[name][1]["bytes"] + captures[name][1]["wire_bytes"]
                ids = json.loads(captures["ids_before"][0]).get("objectIds")
                if (not isinstance(ids, list) or not ids
                        or any(type(value) is not int for value in ids)
                        or len(ids) != len(set(ids))):
                    raise ValueError("wfigs_initial_id_population_drift")
                ordered_ids = sorted(ids)
                for start in range(0, len(ordered_ids), page_size):
                    name = f"page_{start:05d}"
                    captures[name] = _capture(client, _scope_query(name), root, name, retained)
                    retained += captures[name][1]["bytes"] + captures[name][1]["wire_bytes"]
                for name, url in [
                    ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                    ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                    ("layer_after", LAYER_URL + "?f=pjson"),
                    ("service_after", SERVICE_URL + "?f=pjson"),
                ]:
                    captures[name] = _capture(client, url, root, name, retained)
                    retained += captures[name][1]["bytes"] + captures[name][1]["wire_bytes"]
            manifest = {"complete": True, "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "artifact_names": sorted(captures),
                        "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())}}
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        except Exception as exc:
            receipts = {}
            for path in root.glob("*.receipt.json"):
                try:
                    receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical(
                {"complete": False, "error_code": type(exc).__name__,
                 "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                 "artifact_names": sorted(receipts), "receipts": receipts}) + b"\n")
            raise
    layer, features, audit = _population(captures, page_size)
    fields = _fields(layer)
    parquet = _parquet(features, fields, audit)
    identity = _processing_identity()
    resolved_version = version or content_derived_version(
        audit["record_sha256"], audit["ring_sha256"], audit["schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, resolved_version)
    parquet_path = f"geoparquet/{FILE_SLUG}.parquet"
    inputs = []
    for name, (_body, receipt) in sorted(captures.items()):
        inputs.append({
            "input_id": name + ":" + receipt["sha256"],
            "producer": PUBLISHER,
            "host": urlsplit(receipt["url"]).hostname,
            "role": "producer_publication",
            "canonical_url": ITEM_URL if name == "item" else SERVICE_URL,
            "retrieval_url": receipt["url"],
            "retrieved_at": receipt["observed_at"],
            "http_status": receipt["status"],
            "bytes": receipt["bytes"],
            "sha256": receipt["sha256"],
            "wire_bytes": receipt["wire_bytes"],
            "wire_sha256": receipt["wire_sha256"],
            "release_date": None,
            "release_date_unknown_reason": (
                "The WFIGS service publishes continuously refreshed live data with no dated "
                "release; the layer editingInfo edit witness is operational metadata, not a release date."),
            "license": "Rights review pending.",
            "attribution": "National Interagency Fire Center WFIGS Interagency Fire Perimeters.",
        })
    manifest = {
        "schema_version": 1,
        "approved": False,
        "title": "WFIGS Interagency Fire Perimeters",
        "description": ("Complete coherent-snapshot capture of the NIFC WFIGS Interagency Fire "
                        "Perimeters layer; unapproved draft with all acceptance gates false."),
        "publisher": PUBLISHER,
        "agency": AGENCY,
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
            "operation": "Native ArcGIS polygon feature capture to typed GeoParquet WKB",
            "parameters": {
                "attributes": ("All declared layer attributes are retained verbatim in producer order; "
                               "source Esri declarations are recorded in source_audit; no mapping is approved."),
                "geometry": ("Esri JSON rings reassembled into shells/holes by strict whole-ring "
                             "containment with hole-aware nesting depth; "
                             "vertices, ring membership and order preserved without reprojection, "
                             "simplification or repair; invalid/degenerate polygons retained and release-blocked."),
                "source_population": ("Count/ids/object rosters and the layer editingInfo edit witness "
                                      "match before and after all pages; one coherent live snapshot."),
                "historical_comparison": ("The deprecated HIFLD/GeoPlatform hosting (crosswalk DOI "
                                          "10.3886/E241367V1) is not used; the producer NIFC publication replaces it."),
            }}],
        "lineage": {
            "layer_id": LAYER_ID,
            "layer_name": LAYER_NAME,
            "native_crs": "EPSG:4326",
            "feature_count": audit["feature_count"],
            "source_id_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["schema_sha256"],
            "source_record_sha256": audit["record_sha256"],
            "source_geometry_sha256": audit["ring_sha256"],
            "service_item_id": SERVICE_ITEM_ID,
            "item_owner": OWNER,
            "item_org_id": ORG_ID,
            "edit_witness": audit["edit_witness"],
            "release_date": None,
            "esri_to_arrow_type_mapping": {field["type"]: ESRI_TO_ARROW_TYPE[field["type"]] for field in fields},
        },
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {"geoparquet": {
                "reason": "Retains native attributes and EPSG:4326 polygon geometry for the coherent captured snapshot.",
                "objects": [{"path": parquet_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {
            "geopackage": "No conversion contract approved.",
            "shapefile_zip": "No field conversion contract approved.",
            "pmtiles": "No tiling or release policy approved.",
        },
        "promotion_allowed": False,
    }
    validate_output_policy(manifest)
    provenance_projection(manifest)
    outputs = {parquet_path: parquet,
               "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
    ready = {"approved": False, "version": resolved_version,
             "objects": {parquet_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
             "source_population_complete": True, "source_id_sha256": audit["source_id_sha256"],
             "source_record_sha256": audit["record_sha256"],
             "source_release_accepted": False, "source_freshness_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False,
             "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
             "mapping_approved": False, "release_accepted": False}
    write_immutable(storage, f"{prefix}/metadata/candidate_ready.json", _canonical(ready) + b"\n")
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": resolved_version,
            "feature_count": len(features), "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["schema_sha256"],
            "source_record_sha256": audit["record_sha256"], "source_geometry_sha256": audit["ring_sha256"],
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "capture_directory": str(root), "reused": replay,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}
