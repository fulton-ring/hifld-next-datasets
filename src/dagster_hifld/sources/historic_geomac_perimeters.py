"""NIFC InterAgency Fire Perimeter History for the historic-perimeters dataset.

The National Interagency Fire Center organization (ArcGIS Online org
``T4QMspbfLg3qTGWY``, item owner ``WFMRDA_Authoritative``, the NIFC Wildland
Fire Management Research, Development & Application program) publishes the
complete InterAgency Fire Perimeter History as the all-years view layer. The
producer owns the publication; no mirror or archive is involved. The layer is
maintained (its ``editingInfo`` data edit date advances) and supersedes the
retired GeoMAC 2000-2018 File Geodatabase this dataset previously captured.

The layer is captured as ordered-offset pages of the sorted OBJECTID roster:
count/id rosters and the layer ``editingInfo`` edit witness must match before
and after every page, and the accepted pages must tile the complete roster
without gaps or overlaps. Every attribute is preserved verbatim in producer
order with its declared Esri type. Polygon rings are converted by the shared
GDAL ESRIJSON path (``sources.esri_geometry.to_wkb``); the service's native
EPSG:3857 coordinates are retained without reprojection, simplification or
repair. A source polygon that is null, empty or OGC-invalid fails the capture
closed. Source, schema/ID contract, baseline, rights and release acceptance
remain unapproved.
"""
from __future__ import annotations

import hashlib
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
import pyproj
import shapely

from dagster_hifld.http_capture import CaptureDocuments
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import (
    PublishedStorageResource,
    StagingStorageResource,
    default_capture_dir,
)
from dagster_hifld.sources.arcgis_layer import (
    capture_layer, capture_page, load_receipt, load_replay, RetainedCaptureView,
)
from dagster_hifld.sources.capture_common import (
    candidate_prefix,
    content_derived_version,
    processing_identity,
    write_immutable,
)
from dagster_hifld.sources.esri_common import arrow_type, canonical as _canonical, sha as _sha
from dagster_hifld.sources.esri_geometry import to_wkb

DATASET_SLUG = FILE_SLUG = "historic-perimeters-combined-2000-2018-geomac"
TITLE = "Historic Perimeters (NIFC InterAgency Fire Perimeter History)"
PUBLISHER = ("National Interagency Fire Center (NIFC), Wildland Fire Management Research, "
             "Development & Application program")
AGENCY = ("National Interagency Fire Center (NIFC); contributing USDA Forest Service, "
          "Department of the Interior BLM/BIA/FWS/NPS, Alaska Interagency Fire Center and CalFire")
ORG_ID = "T4QMspbfLg3qTGWY"
OWNER = "WFMRDA_Authoritative"
ITEM_ID = "e02b85c0ea784ce7bd8add7ae3d293d0"
ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=pjson"
SERVICE_URL = (
    f"https://services3.arcgis.com/{ORG_ID}/arcgis/rest/services/"
    "InterAgencyFirePerimeterHistory_All_Years_View/FeatureServer"
)
LANDING_URL = ("https://data-nifc.opendata.arcgis.com/datasets/nifc::"
               "interagency-fire-perimeter-history-about")
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "InteragencyFirePerimeterHistory"
GEOMETRY_TYPE = "esriGeometryPolygon"
OBJECT_ID_FIELD = "OBJECTID"
GLOBAL_ID_FIELD = "GlobalID"
PAGE_SIZE = 2000
MAX_PAGE_BYTES = 64 * 1024 * 1024
MAX_CAPTURE_BYTES = 4_000_000_000
DISK_FLOOR_BYTES = 8_000_000_000
NATIVE_WKID = 3857
NATIVE_CRS = pyproj.CRS.from_epsg(NATIVE_WKID)
SERVICE_HOST = urlsplit(SERVICE_URL).hostname
SHARING_HOST = urlsplit(ITEM_URL).hostname

# Producer field roster pinned by name, Esri type, declared length and nullability.
EXPECTED_FIELDS = (
    ("OBJECTID", "esriFieldTypeOID", None, False),
    ("IRWINID", "esriFieldTypeString", 50, True),
    ("FORID", "esriFieldTypeString", 50, True),
    ("INCIDENT", "esriFieldTypeString", 50, True),
    ("GIS_ACRES", "esriFieldTypeSingle", None, True),
    ("UNQE_FIRE_ID", "esriFieldTypeString", 22, True),
    ("DATE_CUR", "esriFieldTypeString", 14, True),
    ("FIRE_YEAR_INT", "esriFieldTypeSmallInteger", None, True),
    ("UNIT_ID", "esriFieldTypeString", 8, True),
    ("POO_RESP_I", "esriFieldTypeString", 8, True),
    ("LOCAL_NUM", "esriFieldTypeString", 10, True),
    ("FEATURE_CA", "esriFieldTypeString", 50, True),
    ("MAP_METHOD", "esriFieldTypeString", 80, True),
    ("COMMENTS", "esriFieldTypeString", 254, True),
    ("GEO_ID", "esriFieldTypeString", 50, True),
    ("SOURCE", "esriFieldTypeString", 20, True),
    ("AGENCY", "esriFieldTypeString", 10, True),
    ("FIRE_YEAR", "esriFieldTypeString", 4, True),
    ("GlobalID", "esriFieldTypeGlobalID", 38, False),
)
FIELD_NAMES = [name for name, _type, _length, _nullable in EXPECTED_FIELDS]
LAYER_PIN_PROJECTION = (
    "id", "name", "geometryType", "objectIdField", "globalIdField",
    "hasZ", "hasM", "maxRecordCount", "spatialReference", "geometryField",
    "supportedQueryFormats",
)
STRING_TYPES = {"esriFieldTypeString", "esriFieldTypeGUID", "esriFieldTypeGlobalID"}
INT_TYPES = {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"}
FLOAT_TYPES = {"esriFieldTypeDouble", "esriFieldTypeSingle"}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


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
            "orderByFields": f"{OBJECT_ID_FIELD} ASC", "outSR": str(NATIVE_WKID),
            "resultOffset": str(offset), "resultRecordCount": str(page_size), "f": "json",
        })
    raise ValueError("ifph_unexpected_capture_name:" + name)


def _spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError("ifph_spatial_reference_missing:" + label)
    observed = {key: value.get(key) for key in ("wkid", "latestWkid") if key in value}
    # ArcGIS advertises Web Mercator as wkid 102100 with latestWkid 3857.
    if (not observed
            or observed.get("latestWkid", NATIVE_WKID) != NATIVE_WKID
            or observed.get("wkid", NATIVE_WKID) not in (102100, NATIVE_WKID)):
        raise ValueError("ifph_spatial_reference_drift:" + label)


def _field_projection(fields: object) -> list[dict[str, object]]:
    if not isinstance(fields, list):
        raise ValueError("ifph_field_roster_missing")
    return [{key: field.get(key) for key in ("name", "type", "length", "nullable")}
            for field in fields]


def _expected_field_projection() -> list[dict[str, object]]:
    return [{"name": name, "type": kind, "length": length, "nullable": nullable}
            for name, kind, length, nullable in EXPECTED_FIELDS]


def _fields(layer: dict) -> list[dict]:
    fields = layer.get("fields")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("geometryType") != GEOMETRY_TYPE
            or layer.get("objectIdField") != OBJECT_ID_FIELD
            or layer.get("globalIdField") != GLOBAL_ID_FIELD
            or layer.get("hasZ") is not False or layer.get("hasM") is not False
            or not isinstance(fields, list)
            or layer.get("maxRecordCount") != PAGE_SIZE):
        raise ValueError("ifph_layer_identity_or_schema_drift")
    _spatial_reference(layer.get("spatialReference"), "layer")
    if _field_projection(fields) != _expected_field_projection():
        raise ValueError("ifph_field_roster_drift")
    return fields


def _validate_request_scope(name: str, receipt: dict, page_size: int) -> None:
    url = receipt.get("url")
    if not isinstance(url, str):
        raise ValueError("ifph_capture_url_missing:" + name)
    parsed = urlsplit(url)
    if name == "item":
        host, expected = SHARING_HOST, urlsplit(ITEM_URL)
    elif name.startswith(("service_", "layer_", "count_", "ids_", "page_")):
        host, expected = SERVICE_HOST, urlsplit(_scope_query(name, page_size))
    else:
        raise ValueError("ifph_unexpected_capture_name:" + name)
    if parsed.scheme != "https" or parsed.hostname != host:
        raise ValueError("ifph_capture_endpoint_drift:" + name)
    if (parsed.path, parsed.query) != (expected.path, expected.query):
        raise ValueError("ifph_capture_request_scope_drift:" + name)
    if receipt.get("final_url") != url:
        raise ValueError("ifph_capture_redirect_not_pinned:" + name)


def _validate_attributes(attributes: dict, fields: list[dict], label: str) -> None:
    names = [field["name"] for field in fields]
    if [str(key) for key in attributes] != names:
        raise ValueError("ifph_attribute_roster_drift:" + label)
    for field in fields:
        name, kind = field["name"], field["type"]
        value = attributes[name]
        if value is None:
            if field.get("nullable") is False:
                raise ValueError("ifph_nonnullable_null:" + name + ":" + label)
        elif kind in STRING_TYPES:
            length = field.get("length")
            if not isinstance(value, str) or (isinstance(length, int) and length > 0 and len(value) > length):
                raise ValueError("ifph_attribute_type_drift:" + name + ":" + label)
        elif kind in INT_TYPES and (type(value) is not int):
            raise ValueError("ifph_attribute_type_drift:" + name + ":" + label)
        elif kind in FLOAT_TYPES and (type(value) not in (int, float) or not math.isfinite(value)):
            raise ValueError("ifph_attribute_type_drift:" + name + ":" + label)


def _geometry(raw: object, label: str) -> tuple[bytes, shapely.Geometry]:
    if raw is None:
        raise ValueError("ifph_null_geometry:" + label)
    if not isinstance(raw, dict):
        raise ValueError("ifph_polygon_geometry_invalid:" + label)
    wkb = to_wkb(raw, GEOMETRY_TYPE, has_z=False, has_m=False)
    if wkb is None:
        raise ValueError("ifph_null_geometry:" + label)
    decoded = shapely.from_wkb(wkb)
    if decoded.is_empty:
        raise ValueError("ifph_empty_geometry:" + label)
    if not decoded.is_valid:
        raise ValueError("ifph_invalid_geometry:" + label + ":" + shapely.is_valid_reason(decoded))
    return wkb, decoded


def _page_body(root: Path, name: str, receipt: dict) -> bytes:
    body = (root / f"{name}.body").read_bytes()
    wire = (root / f"{name}.wire").read_bytes()
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("ifph_page_capture_receipt_invalid:" + name)
    return body


def _population(root: Path, static: dict[str, tuple[bytes, dict]],
                page_names: list[str], receipts: dict[str, dict],
                page_size: int = PAGE_SIZE) -> tuple[dict, list[dict], dict]:
    docs = CaptureDocuments(static)
    if _canonical(docs["service_before"]) != _canonical(docs["service_after"]):
        raise ValueError("ifph_service_changed_during_capture")
    layer, layer_after = docs["layer_before"], docs["layer_after"]
    fields = _fields(layer)
    pinned = {key: layer.get(key) for key in LAYER_PIN_PROJECTION}
    if pinned != {key: layer_after.get(key) for key in LAYER_PIN_PROJECTION}:
        raise ValueError("ifph_schema_changed_during_capture")
    if _field_projection(layer_after.get("fields")) != _field_projection(fields):
        raise ValueError("ifph_schema_changed_during_capture")
    witness = layer.get("editingInfo", {})
    if not isinstance(witness, dict) or witness != layer_after.get("editingInfo", {}):
        raise ValueError("ifph_edit_witness_changed_during_capture")
    item = docs["item"]
    if (item.get("id") != ITEM_ID or item.get("owner") != OWNER
            or item.get("orgId") != ORG_ID or item.get("type") != "Feature Service"
            or item.get("url") != SERVICE_URL):
        raise ValueError("ifph_producer_item_identity_drift")
    ids_before = docs["ids_before"].get("objectIds")
    ids_after = docs["ids_after"].get("objectIds")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    if (not isinstance(ids_before, list) or not ids_before
            or any(type(value) is not int for value in ids_before)
            or len(ids_before) != len(set(ids_before))
            or sorted(ids_before) != sorted(ids_after or [])
            or count_before != len(ids_before) or count_after != len(ids_before)):
        raise ValueError("ifph_population_changed_or_incomplete")
    ordered_ids = sorted(ids_before)
    expected_pages = {f"page_{start:05d}" for start in range(0, len(ordered_ids), page_size)}
    if set(page_names) != expected_pages:
        raise ValueError("ifph_page_roster_drift")
    for name in sorted(docs):
        _validate_request_scope(name, static[name][1], page_size)
        if name.startswith("layer_"):
            continue
        if isinstance(docs[name], dict) and docs[name].get("error"):
            raise ValueError("ifph_source_error_response:" + name)
    records: list[dict] = []
    geometries: list[bytes] = []
    null_counts = {name: 0 for name in FIELD_NAMES}
    geometry_types: set[str] = set()
    vertices = 0
    bounds = [math.inf, math.inf, -math.inf, -math.inf]
    year_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    agency_counts: dict[str, int] = {}
    for name in sorted(page_names):
        _validate_request_scope(name, receipts[name], page_size)
        page = json.loads(_page_body(root, name, receipts[name]))
        if isinstance(page, dict) and page.get("error"):
            raise ValueError("ifph_source_error_response:" + name)
        if page.get("geometryType") != GEOMETRY_TYPE:
            raise ValueError("ifph_page_geometry_type_drift:" + name)
        _spatial_reference(page.get("spatialReference"), "page:" + name)
        rows = page.get("features")
        offset = int(name.removeprefix("page_"))
        expected = ordered_ids[offset:offset + page_size]
        if not isinstance(rows, list) or len(rows) != len(expected):
            raise ValueError("ifph_page_population_incomplete:" + name)
        if page.get("exceededTransferLimit") is True and len(rows) != page_size:
            raise ValueError("ifph_page_error_or_truncation:" + name)
        for position, row in enumerate(rows):
            attributes = row.get("attributes")
            if not isinstance(attributes, dict):
                raise ValueError("ifph_feature_attributes_missing:" + name)
            oid = attributes.get(OBJECT_ID_FIELD)
            if type(oid) is not int or expected[position] != oid:
                raise ValueError("ifph_page_order_or_id_drift:" + name)
            _validate_attributes(attributes, fields, name)
            for field_name in FIELD_NAMES:
                if attributes[field_name] is None:
                    null_counts[field_name] += 1
            year = attributes.get("FIRE_YEAR_INT")
            year_counts[str(year)] = year_counts.get(str(year), 0) + 1
            source = attributes.get("SOURCE")
            source_counts[str(source)] = source_counts.get(str(source), 0) + 1
            agency = attributes.get("AGENCY")
            agency_counts[str(agency)] = agency_counts.get(str(agency), 0) + 1
            wkb, decoded = _geometry(row.get("geometry"), name + ":" + str(oid))
            geometry_types.add(decoded.geom_type)
            vertices += int(shapely.get_num_coordinates(decoded))
            west, south, east, north = decoded.bounds
            bounds = [min(bounds[0], west), min(bounds[1], south),
                      max(bounds[2], east), max(bounds[3], north)]
            records.append(attributes)
            geometries.append(wkb)
    if len(records) != len(ordered_ids):
        raise ValueError("ifph_feature_roster_mismatch")
    global_ids = [record.get(GLOBAL_ID_FIELD) for record in records]
    if len(global_ids) != len(set(global_ids)):
        raise ValueError("ifph_global_id_duplicates")
    record_digest = hashlib.sha256()
    geometry_digest = hashlib.sha256()
    for oid, record, wkb in zip(ordered_ids, records, geometries):
        body = _canonical(record)
        record_digest.update(len(body).to_bytes(8, "big"))
        record_digest.update(body)
        geometry_digest.update(oid.to_bytes(8, "big", signed=True))
        geometry_digest.update(len(wkb).to_bytes(8, "big"))
        geometry_digest.update(wkb)
    audit = {
        "feature_count": len(records),
        "source_population_complete": True,
        "source_id_sha256": _sha(_canonical(ordered_ids)),
        "source_record_sha256": record_digest.hexdigest(),
        "source_geometry_sha256": geometry_digest.hexdigest(),
        "source_schema_sha256": _sha(_canonical(_field_projection(fields))),
        "source_unique_id_count": len(set(ordered_ids)),
        "source_null_geometry_count": 0,
        "source_empty_geometry_count": 0,
        "source_invalid_geometry_count": 0,
        "source_vertices": vertices,
        "source_bounds": bounds,
        "geometry_types": sorted(geometry_types),
        "null_counts": null_counts,
        "fire_year_counts": dict(sorted(year_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "agency_counts": dict(sorted(agency_counts.items())),
        "global_id_unique_count": len(set(global_ids)),
        "page_count": len(expected_pages),
        "edit_witness": witness,
        "item_owner": OWNER,
        "item_org_id": ORG_ID,
        "native_crs": f"EPSG:{NATIVE_WKID}",
    }
    return layer, [{"attributes": record, "geometry": wkb}
                   for record, wkb in zip(records, geometries)], audit


def _parquet(features: list[dict], fields: list[dict], audit: dict) -> bytes:
    arrays, schema = [], []
    for field in fields:
        name, kind = field["name"], field["type"]
        values = [feature["attributes"][name] for feature in features]
        dtype = arrow_type(field)
        if kind in FLOAT_TYPES:
            values = [None if value is None else float(value) for value in values]
        arrays.append(pa.array(values, type=dtype, safe=True))
        schema.append(pa.field(name, dtype, nullable=field.get("nullable") is not False))
    wkbs = [feature["geometry"] for feature in features]
    arrays.append(pa.array(wkbs, type=pa.binary()))
    schema.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": audit["geometry_types"],
        "crs": NATIVE_CRS.to_json_dict(), "bbox": audit["source_bounds"]}}}
    table = pa.Table.from_arrays(arrays, schema=pa.schema(schema, metadata={b"geo": _canonical(geo)}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    body = sink.getvalue().to_pybytes()
    readback = pq.read_table(pa.BufferReader(pa.py_buffer(body)))
    if readback.num_rows != len(features) or readback.schema.names != FIELD_NAMES + ["geometry"]:
        raise ValueError("ifph_parquet_readback_schema_drift")
    if readback.column(OBJECT_ID_FIELD).to_pylist() != [f["attributes"][OBJECT_ID_FIELD] for f in features]:
        raise ValueError("ifph_parquet_oid_parity_failure")
    for field in fields:
        name = field["name"]
        expected = pa.array([feature["attributes"][name] for feature in features],
                            type=arrow_type(field), safe=True)
        if readback.column(name).to_pylist() != expected.to_pylist():
            raise ValueError("ifph_parquet_attribute_parity_failure:" + name)
    if readback.column("geometry").to_pylist() != wkbs:
        raise ValueError("ifph_parquet_native_wkb_parity_failure")
    metadata = json.loads(readback.schema.metadata[b"geo"])
    if (metadata["columns"]["geometry"].get("encoding") != "WKB"
            or not pyproj.CRS.from_json_dict(metadata["columns"]["geometry"]["crs"]).equals(NATIVE_CRS)):
        raise ValueError("ifph_parquet_crs_or_encoding_drift")
    return body


def _processing_identity(repo_root: Path | None = None) -> dict[str, object]:
    root = Path(repo_root) if repo_root is not None else Path(__file__).parents[3]
    identity = processing_identity(root)
    validate_construction_code_identity(root, identity["code_commit"], identity["files"], identity=identity)
    return identity


def _capture(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    return capture_page(client, url, root, name, page_max_bytes=MAX_PAGE_BYTES,
                        capture_max_bytes=MAX_CAPTURE_BYTES)


def _load(root: Path) -> tuple[dict[str, tuple[bytes, dict]], list[str], dict[str, dict]]:
    manifest = json.loads((root / "capture_manifest.json").read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("artifact_names"), list):
        raise ValueError("ifph_capture_manifest_invalid")
    actual = {path.name for path in root.iterdir()}
    expected = {"capture_manifest.json"}
    for name in manifest["artifact_names"]:
        expected.update({f"{name}.body", f"{name}.receipt.json", f"{name}.wire"})
    if actual != expected or any((root / name).is_symlink() for name in actual):
        raise ValueError("ifph_capture_roster_invalid")
    static: dict[str, tuple[bytes, dict]] = {}
    page_names: list[str] = []
    receipts: dict[str, dict] = {}
    for name in manifest["artifact_names"]:
        receipt = load_receipt(root, name, ValueError)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("ifph_capture_receipt_mismatch:" + name)
        receipts[name] = receipt
        if name.startswith("page_"):
            page_names.append(name)
        else:
            body, replay_receipt = load_replay(root, name)
            if replay_receipt != receipt:
                raise ValueError("ifph_capture_receipt_mismatch:" + name)
            static[name] = (body, receipt)
    if sum(int(receipt["bytes"]) + int(receipt["wire_bytes"]) for receipt in receipts.values()) > MAX_CAPTURE_BYTES:
        raise ValueError("ifph_capture_budget_exceeded")
    return static, sorted(page_names), receipts


def capture_historic_perimeters(storage: StagingStorageResource, *, version: str | None = None,
                                capture_directory: str | Path | None = None,
                                page_size: int = PAGE_SIZE) -> dict[str, object]:
    """Capture one coherent snapshot of the complete NIFC history layer into local staging."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("ifph_isolated_local_staging_required")
    staging = Path(storage.local_dir).absolute()
    if any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("ifph_unsafe_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < DISK_FLOOR_BYTES:
        raise ValueError("ifph_disk_floor_before_capture")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        static, page_names, receipts = _load(root)
    else:
        root = ((requested / (version or "draft")) if requested
                else default_capture_dir(DATASET_SLUG, FILE_SLUG, uuid4().hex))
        if any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("ifph_unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        static, page_names, receipts = {}, [], {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(120, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/historic-perimeters-draft"}) as client:
                documents = [
                    ("item", ITEM_URL),
                    ("service_before", SERVICE_URL + "?f=pjson"),
                    ("layer_before", LAYER_URL + "?f=pjson"),
                    ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                    ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                ]
                def fetch(client, url, root, name):
                    value = _capture(client, url, root, name)
                    receipts[name] = value[1]
                    if name.startswith("page_"):
                        page_names.append(name)
                    else:
                        static[name] = value
                    return value

                def validate_seed(body):
                    ids = json.loads(body).get("objectIds")
                    if (not isinstance(ids, list) or not ids
                            or any(type(value) is not int for value in ids)
                            or len(ids) != len(set(ids))):
                        raise ValueError("ifph_initial_id_population_drift")
                    return sorted(ids)

                def roster(ids):
                    return [(f"page_{start:05d}", _scope_query(f"page_{start:05d}", page_size))
                            for start in range(0, len(ids), page_size)] + [
                        (name, _scope_query(name, page_size))
                        for name in ("count_after", "ids_after", "layer_after", "service_after")]

                capture_layer(LAYER_URL, root, MAX_CAPTURE_BYTES, page_size, client=client,
                    captures=RetainedCaptureView(root), discovery_documents=documents,
                    validate_seed=validate_seed, roster=roster, fetch=fetch,
                    on_validation_error=lambda view: _population(root, static, page_names, receipts, page_size))
            manifest = {"complete": True,
                        "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "artifact_names": sorted(receipts), "receipts": receipts}
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        except Exception as exc:
            failed = {}
            for path in root.glob("*.receipt.json"):
                try:
                    failed[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    failed[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical(
                {"complete": False, "error_code": type(exc).__name__,
                 "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                 "artifact_names": sorted(failed), "receipts": failed}) + b"\n")
            raise
    layer, features, audit = _population(root, static, page_names, receipts, page_size)
    fields = _fields(layer)
    parquet = _parquet(features, fields, audit)
    identity = _processing_identity()
    resolved_version = version or content_derived_version(
        audit["source_record_sha256"], audit["source_geometry_sha256"], audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, resolved_version)
    parquet_path = f"geoparquet/{FILE_SLUG}.parquet"
    inputs = []
    for name, receipt in sorted(receipts.items()):
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
                "The NIFC history service publishes no dated release; the layer editingInfo "
                "data edit date is operational metadata, not a producer release date."),
            "license": "Rights review pending.",
            "attribution": "National Interagency Fire Center InterAgency Fire Perimeter History.",
        })
    manifest = {
        "schema_version": 1,
        "approved": False,
        "title": TITLE,
        "description": ("Complete coherent-snapshot capture of the NIFC InterAgency Fire Perimeter "
                        "History all-years layer, the maintained successor to the retired GeoMAC "
                        "2000-2018 perimeters; unapproved draft with all acceptance gates false."),
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
                "attributes": ("All 19 declared layer attributes are retained verbatim in producer "
                               "order with their declared Esri types; no mapping is approved."),
                "geometry": ("Esri JSON rings converted by the shared GDAL ESRIJSON path "
                             "(sources.esri_geometry.to_wkb); native EPSG:3857 coordinates retained "
                             "without reprojection, simplification, ring reorganization or repair; "
                             "null, empty and OGC-invalid source polygons fail the capture closed."),
                "source_population": ("Count/id rosters and the layer editingInfo edit witness match "
                                      "before and after all pages; one coherent snapshot."),
                "superseded_source": ("The retired GeoMAC 2000-2018 File Geodatabase capture is "
                                      "removed; this maintained NIFC layer is the producer publication."),
            }}],
        "lineage": {
            "layer_id": LAYER_ID,
            "layer_name": LAYER_NAME,
            "native_crs": f"EPSG:{NATIVE_WKID}",
            "feature_count": audit["feature_count"],
            "source_id_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_record_sha256": audit["source_record_sha256"],
            "source_geometry_sha256": audit["source_geometry_sha256"],
            "service_item_id": ITEM_ID,
            "landing_url": LANDING_URL,
            "item_owner": OWNER,
            "item_org_id": ORG_ID,
            "edit_witness": audit["edit_witness"],
            "release_date": None,
            "supersedes": "Historic Perimeters Combined 2000-2018 GeoMAC (retired)",
            "esri_to_arrow_type_mapping": {field["type"]: str(arrow_type(field)) for field in fields},
        },
        "prebuilt_output_policy": {
            "schema_version": 1,
            "formats": {"geoparquet": {
                "reason": "Retains native attributes and EPSG:3857 polygon geometry for the coherent captured snapshot.",
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
    feature_count = len(features)
    del features
    write_immutable(storage, f"{prefix}/{parquet_path}", parquet)
    write_immutable(storage, f"{prefix}/metadata/source_manifest.json", _canonical(manifest) + b"\n")
    write_immutable(storage, f"{prefix}/metadata/source_audit.json", _canonical(audit) + b"\n")
    for name, receipt in receipts.items():
        body = static[name][0] if name in static else _page_body(root, name, receipt)
        write_immutable(storage, f"{prefix}/raw/{name}.body", body)
        write_immutable(storage, f"{prefix}/raw/{name}.wire", (root / f"{name}.wire").read_bytes())
        write_immutable(storage, f"{prefix}/raw/{name}.receipt.json", _canonical(receipt) + b"\n")
    ready = {"approved": False, "version": resolved_version,
             "objects": {parquet_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
             "source_population_complete": True,
             "source_id_sha256": audit["source_id_sha256"],
             "source_record_sha256": audit["source_record_sha256"],
             "source_invalid_geometry_count": 0,
             "source_release_accepted": False, "source_freshness_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False,
             "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
             "mapping_approved": False, "release_accepted": False}
    write_immutable(storage, f"{prefix}/metadata/candidate_ready.json", _canonical(ready) + b"\n")
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": resolved_version,
            "feature_count": feature_count, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_record_sha256": audit["source_record_sha256"],
            "source_geometry_sha256": audit["source_geometry_sha256"],
            "source_invalid_geometry_count": 0,
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "capture_directory": str(root), "reused": replay,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "baseline_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}
