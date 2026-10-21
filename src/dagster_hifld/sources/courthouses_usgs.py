"""Draft-only capture of the complete observed USGS National Structures courthouse layer.

The current ArcGIS service is a USGS source candidate.  The 2024 HIFLD archive
is retained only as a historical reference; equivalence, release date, rights,
coverage, and migration decisions remain unapproved.
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
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import capture_get, processing_identity, write_immutable

DATASET_SLUG = FILE_SLUG = "courthouses-3"
PUBLISHER = "U.S. Geological Survey, National Structures Dataset"
SERVICE_URL = "https://carto.nationalmap.gov/arcgis/rest/services/structures/MapServer"
LAYER_ID = 40
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
EXPECTED_COUNT = 3713
BATCH_SIZE = 500
MAX_RESPONSE_BYTES = 16_000_000
MAX_CAPTURE_BYTES = 256_000_000
EXPECTED_FIELD_NAMES = [
    "OBJECTID", "PERMANENT_IDENTIFIER", "SOURCE_FEATUREID", "SOURCE_DATASETID",
    "SOURCE_DATADESC", "SOURCE_ORIGINATOR", "LOADDATE", "FTYPE", "FCODE",
    "NAME", "POINTLOCATIONTYPE", "ADMINTYPE", "ADDRESSBUILDINGNAME", "ADDRESS",
    "CITY", "STATE", "ZIPCODE", "GNIS_ID", "GLOBALID",
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


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _processing_identity(repo_root: Path | None = None) -> dict[str, object]:
    root = Path(repo_root) if repo_root is not None else Path(__file__).parents[3]
    identity = processing_identity(root)
    validate_construction_code_identity(root, identity["code_commit"], identity["files"], identity=identity)
    return identity


def _capture(client: httpx.Client, url: str, root: Path, name: str, retained: int) -> tuple[bytes, dict]:
    if type(retained) is not int or retained < 0 or retained > MAX_CAPTURE_BYTES:
        raise ValueError("courthouses_capture_budget_invalid")

    def reserve(progress) -> None:
        prospective = (retained + progress.wire_bytes_retained + progress.decoded_bytes_retained
                       + progress.reserve_bytes)
        if prospective > MAX_CAPTURE_BYTES:
            raise OSError("courthouses_capture_budget_exceeded")

    return capture_get(client, url, root, name, max_bytes=MAX_RESPONSE_BYTES, reserve=reserve)


def _load(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest = json.loads((root / "capture_manifest.json").read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("artifact_names"), list):
        raise ValueError("courthouses_capture_manifest_invalid")
    actual = {p.name for p in root.iterdir()}
    expected = {"capture_manifest.json"}
    for name in manifest["artifact_names"]:
        expected.update({f"{name}.body", f"{name}.receipt.json", f"{name}.wire"})
    if actual != expected or any((root / name).is_symlink() for name in actual):
        raise ValueError("courthouses_capture_roster_invalid")
    result = {}
    for name in manifest["artifact_names"]:
        body = (root / f"{name}.body").read_bytes()
        wire = (root / f"{name}.wire").read_bytes()
        receipt = json.loads((root / f"{name}.receipt.json").read_bytes())
        if (receipt != manifest["receipts"].get(name) or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
            raise ValueError("courthouses_capture_receipt_mismatch:" + name)
        if len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("courthouses_response_budget_exceeded")
        result[name] = (body, receipt)
    if sum(len(body) + int(receipt.get("wire_bytes", 0)) for body, receipt in result.values()) > MAX_CAPTURE_BYTES:
        raise ValueError("courthouses_capture_budget_exceeded")
    return result


def _sr(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError("courthouses_spatial_reference_missing:" + label)
    wkids = {v for k, v in value.items() if k in {"wkid", "latestWkid"}}
    if not wkids or not wkids.issubset({3857, 102100}):
        raise ValueError("courthouses_spatial_reference_drift:" + label)


def _validate_page_metadata(page: dict, label: str) -> None:
    if page.get("geometryType") != "esriGeometryPoint":
        raise ValueError("courthouses_page_geometry_type_drift:" + label)
    _sr(page.get("spatialReference"), "page:" + label)


def _validate_point_geometry(geometry: object) -> None:
    if not isinstance(geometry, dict):
        raise ValueError("courthouses_point_geometry_drift")
    if "z" in geometry or "m" in geometry:
        raise ValueError("courthouses_unhandled_z_or_m_ordinate")
    if set(geometry) - {"x", "y", "spatialReference"}:
        raise ValueError("courthouses_point_geometry_drift")
    if type(geometry.get("x")) not in (int, float) or type(geometry.get("y")) not in (int, float):
        raise ValueError("courthouses_point_geometry_drift")
    if not math.isfinite(geometry["x"]) or not math.isfinite(geometry["y"]):
        raise ValueError("courthouses_nonfinite_geometry")
    if geometry.get("spatialReference") is not None:
        _sr(geometry["spatialReference"], "feature")


def _validate_dimensions_probe(probe: dict, expected_oid: int) -> None:
    _validate_page_metadata(probe, "dimensions_probe")
    features = probe.get("features")
    if not isinstance(features, list) or len(features) != 1:
        raise ValueError("courthouses_dimensions_probe_population_drift")
    attributes = features[0].get("attributes")
    if (not isinstance(attributes, dict)
            or {str(key).upper(): value for key, value in attributes.items()}.get("OBJECTID") != expected_oid):
        raise ValueError("courthouses_dimensions_probe_population_drift")
    _validate_point_geometry(features[0].get("geometry"))


def _fields(layer: dict) -> list[dict]:
    fields = layer.get("fields")
    if (layer.get("id") != LAYER_ID or layer.get("name") != "Courthouses"
            or layer.get("geometryType") != "esriGeometryPoint"
            or not isinstance(fields, list)
            or [field.get("name") for field in fields] != ["OBJECTID", "SHAPE", *EXPECTED_FIELD_NAMES[1:]]
            or layer.get("geometryField", {}).get("name") != "SHAPE"):
        raise ValueError("courthouses_layer_schema_drift")
    extent = layer.get("extent")
    if not isinstance(extent, dict):
        raise ValueError("courthouses_layer_extent_missing")
    _sr(extent.get("spatialReference"), "layer_extent")
    attribute_fields = [field for field in fields if field.get("type") != "esriFieldTypeGeometry"]
    for field in attribute_fields:
        if field.get("type") not in ARROW_TYPES:
            raise ValueError("courthouses_unsupported_field_type:" + str(field.get("name")))
    return attribute_fields


def _validate_request_scope(name: str, receipt: dict, ids: list[int]) -> None:
    request_url = receipt.get("url")
    final_url = receipt.get("final_url", request_url)
    for url in (request_url, final_url):
        if not isinstance(url, str):
            raise ValueError("courthouses_capture_url_missing:" + name)
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != "carto.nationalmap.gov":
            raise ValueError("courthouses_capture_endpoint_drift:" + name)
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
            raise ValueError("courthouses_dimensions_probe_without_ids")
        expected_path, expected = urlsplit(LAYER_URL).path + "/query", {
            "objectIds": [str(ids[0])], "outFields": ["OBJECTID"], "returnGeometry": ["true"],
            "returnZ": ["true"], "returnM": ["true"], "f": ["json"]}
    elif name.startswith("page_"):
        start = int(name.removeprefix("page_"))
        page_ids = ids[start:start + BATCH_SIZE]
        if not page_ids:
            raise ValueError("courthouses_unexpected_page:" + name)
        expected_path, expected = urlsplit(LAYER_URL).path + "/query", {
            "objectIds": [",".join(map(str, page_ids))], "outFields": ["*"],
            "returnGeometry": ["true"], "returnZ": ["true"], "returnM": ["true"],
            "orderByFields": ["OBJECTID ASC"], "f": ["json"]}
    else:
        raise ValueError("courthouses_unexpected_capture_name:" + name)
    if parsed.path != expected_path or params != expected:
        raise ValueError("courthouses_capture_request_scope_drift:" + name)


def _population(captures: dict[str, tuple[bytes, dict]]) -> tuple[dict, list[dict], dict]:
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    layer = docs["layer_before"]
    fields = _fields(layer)
    if _canonical(layer) != _canonical(docs["layer_after"]):
        raise ValueError("courthouses_layer_changed_during_capture")
    if docs["service_before"] != docs["service_after"]:
        raise ValueError("courthouses_service_changed_during_capture")
    ids_before = docs["ids_before"].get("objectIds")
    ids_after = docs["ids_after"].get("objectIds")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    if (not isinstance(ids_before, list) or any(type(i) is not int for i in ids_before)
            or len(ids_before) != EXPECTED_COUNT or len(set(ids_before)) != EXPECTED_COUNT
            or not isinstance(ids_after, list) or sorted(ids_before) != sorted(ids_after)
            or count_before != EXPECTED_COUNT or count_after != EXPECTED_COUNT):
        raise ValueError("courthouses_population_changed_or_incomplete")
    ids_before = sorted(ids_before)
    expected_page_names = {f"page_{start:05d}" for start in range(0, EXPECTED_COUNT, BATCH_SIZE)}
    if {name for name in docs if name.startswith("page_")} != expected_page_names:
        raise ValueError("courthouses_page_receipt_roster_drift")
    if "dimensions_probe" not in docs:
        raise ValueError("courthouses_dimensions_probe_missing")
    for name, (_body, receipt) in captures.items():
        _validate_request_scope(name, receipt, ids_before)
    _validate_dimensions_probe(docs["dimensions_probe"], ids_before[0])
    features = []
    seen = set()
    names = [f["name"] for f in fields]
    bounds = [math.inf, math.inf, -math.inf, -math.inf]
    missing_geometry = []
    for name in sorted(n for n in docs if n.startswith("page_")):
        page = docs[name]
        _validate_page_metadata(page, name)
        rows = page.get("features")
        start = int(name.rsplit("_", 1)[1])
        expected = ids_before[start:start + BATCH_SIZE]
        if not isinstance(rows, list) or len(rows) != len(expected) or docs[name].get("exceededTransferLimit"):
            raise ValueError("courthouses_page_population_incomplete:" + name)
        for feature in rows:
            raw_attrs = feature.get("attributes")
            if not isinstance(raw_attrs, dict) or [str(k).upper() for k in raw_attrs] != names:
                raise ValueError("courthouses_attribute_roster_drift")
            # The service's native layer schema uses uppercase field names, while
            # FeatureServer query JSON lowercases those same member names. Retain
            # original response bytes in raw/ and key values by the declared names.
            attrs = {str(key).upper(): value for key, value in raw_attrs.items()}
            feature["attributes"] = attrs
            oid = attrs["OBJECTID"]
            if type(oid) is not int or oid not in expected or oid in seen:
                raise ValueError("courthouses_native_id_drift")
            seen.add(oid)
            for field in fields:
                key, value, kind = field["name"], attrs[field["name"]], field["type"]
                if value is None:
                    if field.get("nullable") is False:
                        raise ValueError("courthouses_nonnullable_null:" + key)
                elif kind in STRING_TYPES and not isinstance(value, str):
                    raise ValueError("courthouses_attribute_type_drift:" + key)
                elif kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"} and type(value) is not int:
                    raise ValueError("courthouses_attribute_type_drift:" + key)
                elif kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"} and (type(value) not in (int, float) or not math.isfinite(value)):
                    raise ValueError("courthouses_attribute_type_drift:" + key)
                elif kind == "esriFieldTypeDate" and type(value) is not int:
                    raise ValueError("courthouses_attribute_type_drift:" + key)
            geometry = feature.get("geometry")
            if geometry is None:
                missing_geometry.append(oid)
            else:
                _validate_point_geometry(geometry)
                bounds[0] = min(bounds[0], geometry["x"]); bounds[1] = min(bounds[1], geometry["y"])
                bounds[2] = max(bounds[2], geometry["x"]); bounds[3] = max(bounds[3], geometry["y"])
            features.append(feature)
    if seen != set(ids_before) or len(features) != EXPECTED_COUNT:
        raise ValueError("courthouses_feature_roster_mismatch")
    audit = {
        "feature_count": len(features), "missing_geometry_objectids": missing_geometry,
        "id_sha256": _sha(_canonical(sorted(seen))), "schema_sha256": _sha(_canonical(fields)),
        "record_sha256": _sha(_canonical([f["attributes"] for f in features])),
        "geometry_sha256": _sha(_canonical([f.get("geometry") for f in features])),
        "native_crs": "EPSG:3857", "native_bounds": bounds,
        "field_names": names, "source_field_declarations": fields,
        "null_counts": {field["name"]: sum(f["attributes"][field["name"]] is None for f in features) for field in fields},
    }
    return layer, features, audit


def _parquet(features: list[dict], fields: list[dict]) -> bytes:
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
        point = feature.get("geometry")
        geom.append(None if point is None else struct.pack("<BIdd", 1, 1, float(point["x"]), float(point["y"])))
    arrays.append(pa.array(geom, type=pa.binary()))
    schema.append(pa.field("geometry", pa.binary(), nullable=True))
    coords = [f["geometry"] for f in features if f.get("geometry") is not None]
    bbox = [min(g["x"] for g in coords), min(g["y"] for g in coords), max(g["x"] for g in coords), max(g["y"] for g in coords)]
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Point"], "crs": EXPECTED_CRS.to_json_dict(), "bbox": bbox}}}
    table = pa.Table.from_arrays(arrays, schema=pa.schema(schema, metadata={b"geo": _canonical(geo)}))
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    body = sink.getvalue()
    readback = pq.read_table(pa.BufferReader(body))
    if readback.num_rows != EXPECTED_COUNT or readback.schema.names != EXPECTED_FIELD_NAMES + ["geometry"]:
        raise ValueError("courthouses_parquet_readback_schema_drift")
    if readback.column("OBJECTID").to_pylist() != [f["attributes"]["OBJECTID"] for f in features]:
        raise ValueError("courthouses_parquet_oid_parity_failure")
    for field in fields:
        name, kind = field["name"], field["type"]
        expected = [feature["attributes"][name] for feature in features]
        if kind == "esriFieldTypeDate":
            expected = [None if value is None else datetime.fromtimestamp(value / 1000, tz=timezone.utc)
                        for value in expected]
        elif kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
            expected = [None if value is None else float(value) for value in expected]
        if readback.column(name).to_pylist() != expected:
            raise ValueError("courthouses_parquet_attribute_parity_failure:" + name)
    expected_wkb = [None if feature.get("geometry") is None else struct.pack(
        "<BIdd", 1, 1, float(feature["geometry"]["x"]), float(feature["geometry"]["y"])) for feature in features]
    if readback.column("geometry").to_pylist() != expected_wkb:
        raise ValueError("courthouses_parquet_native_wkb_parity_failure")
    metadata = json.loads(readback.schema.metadata[b"geo"])
    declared_crs = CRS.from_json_dict(metadata["columns"]["geometry"]["crs"])
    if metadata["columns"]["geometry"].get("encoding") != "WKB" or not declared_crs.equals(EXPECTED_CRS, ignore_axis_order=True):
        raise ValueError("courthouses_parquet_crs_or_encoding_drift")
    return body


def capture_courthouses(storage: StagingStorageResource, *, version: str,
                        capture_directory: str | None = None) -> dict[str, object]:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("courthouses_isolated_local_staging_required")
    if not isinstance(version, str) or not version or Path(version).name != version or version in {".", ".."}:
        raise ValueError("courthouses_unsafe_version")
    staging = Path(storage.local_dir).absolute()
    if any(p.is_symlink() for p in (staging, *staging.parents)):
        raise ValueError("courthouses_unsafe_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 1_000_000_000:
        raise ValueError("courthouses_disk_floor_before_capture")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        captures = _load(root)
    else:
        root = ((requested / version) if requested else Path("/tmp/hifld-courthouses-capture")) / uuid4().hex
        if any(p.is_symlink() for p in (root, *root.parents)):
            raise ValueError("courthouses_unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        retained = 0
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(60, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/usgs-courthouses-draft"}) as client:
                requests = [("service_before", SERVICE_URL + "?f=pjson"), ("layer_before", LAYER_URL + "?f=pjson"),
                    ("count_before", _query({"where":"1=1","returnCountOnly":"true","f":"json"})),
                    ("ids_before", _query({"where":"1=1","returnIdsOnly":"true","f":"json"}))]
                for name, url in requests:
                    captures[name] = _capture(client, url, root, name, retained)
                    retained += captures[name][1]["bytes"] + captures[name][1]["wire_bytes"]
                ids = json.loads(captures["ids_before"][0]).get("objectIds")
                if (not isinstance(ids, list) or any(type(value) is not int for value in ids)
                        or len(ids) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT):
                    raise ValueError("courthouses_initial_id_population_drift")
                ids = sorted(ids)
                probe_url = _query({"objectIds": str(ids[0]), "outFields": "OBJECTID", "returnGeometry": "true",
                                    "returnZ": "true", "returnM": "true", "f": "json"})
                captures["dimensions_probe"] = _capture(client, probe_url, root, "dimensions_probe", retained)
                retained += captures["dimensions_probe"][1]["bytes"] + captures["dimensions_probe"][1]["wire_bytes"]
                for start in range(0, len(ids), BATCH_SIZE):
                    page_ids = ids[start:start + BATCH_SIZE]
                    name = f"page_{start:05d}"
                    url = _query({"objectIds": ",".join(map(str, page_ids)), "outFields": "*", "returnGeometry": "true",
                                  "returnZ": "true", "returnM": "true", "orderByFields": "OBJECTID ASC", "f": "json"})
                    captures[name] = _capture(client, url, root, name, retained)
                    retained += captures[name][1]["bytes"] + captures[name][1]["wire_bytes"]
                for name, url in [("count_after", _query({"where":"1=1","returnCountOnly":"true","f":"json"})),
                    ("ids_after", _query({"where":"1=1","returnIdsOnly":"true","f":"json"})),
                    ("layer_after", LAYER_URL + "?f=pjson"), ("service_after", SERVICE_URL + "?f=pjson")]:
                    captures[name] = _capture(client, url, root, name, retained)
                    retained += captures[name][1]["bytes"] + captures[name][1]["wire_bytes"]
            manifest = {"complete": True, "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "artifact_names": sorted(captures), "receipts": {n:r for n,(_b,r) in sorted(captures.items())}}
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        except Exception as exc:
            receipts = {}
            for p in root.glob("*.receipt.json"):
                try: receipts[p.name.removesuffix(".receipt.json")] = json.loads(p.read_bytes())
                except (OSError, ValueError): receipts[p.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({"complete":False,"error_code":type(exc).__name__,
                "captured_at":datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),"artifact_names":sorted(receipts),"receipts":receipts})+b"\n")
            raise
    layer, features, audit = _population(captures)
    fields = _fields(layer)
    service_info = json.loads(captures["service_before"][0])
    parquet = _parquet(features, fields)
    identity = _processing_identity()
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    parquet_path = f"geoparquet/{FILE_SLUG}.parquet"
    inputs = [{"input_id":name,"producer":PUBLISHER,"host":urlsplit(receipt["url"]).hostname,
        "role":"producer_publication","canonical_url":LAYER_URL,"retrieval_url":receipt["url"],
        "retrieved_at":receipt["observed_at"],"http_status":receipt["status"],"bytes":receipt["bytes"],
        "sha256":receipt["sha256"],"wire_bytes":receipt["wire_bytes"],"wire_sha256":receipt["wire_sha256"],
        "release_date":None,"release_date_unknown_reason":"The service does not declare a producer release date; the October 2026 refresh marker is an observation, not a release date.",
        "license":"Rights review pending.","attribution":"USGS attribution review pending."}
        for name,(_body,receipt) in sorted(captures.items())]
    manifest = {"schema_version":1,"approved":False,"title":"Courthouses","description":"Complete observed USGS National Structures Courthouses layer candidate; unapproved draft.",
        "publisher":PUBLISHER,"agency":"U.S. Geological Survey","source_url":LAYER_URL,"license":None,
        "source_modified":None,"date_issued":None,"date_modified":None,
        "processor":"HIFLD Next","code_commit":identity["code_commit"],"code_identity_scheme":identity["code_identity_scheme"],
        "processing_identity":identity,"inputs":inputs,
        "transformations":[{"operation":"Native ArcGIS point feature capture to typed GeoParquet WKB","parameters":{
            "attributes":"All 19 declared layer attributes retained. Source ESRI declarations are recorded verbatim in source_audit; output Arrow types use the explicit esri_to_arrow_type_mapping below. No schema conversion contract is approved.",
            "geometry":"Native EPSG:3857 points preserved; no reprojection, geocoding, repair, or row dropping.",
            "source_population":"Exact OBJECTID roster and count captured before and after all pages; complete current layer only.",
            "historical_comparison":"DataLumos HIFLD OPEN 2024 archive retrieval returned HTTP 403; no equivalence asserted."}}],
        "lineage":{"layer_id":LAYER_ID,"layer_name":"Courthouses","native_crs":"EPSG:3857","feature_count":EXPECTED_COUNT,
            "source_id_sha256":audit["id_sha256"],"source_schema_sha256":audit["schema_sha256"],"source_record_sha256":audit["record_sha256"],
            "source_geometry_sha256":audit["geometry_sha256"],"service_refresh_observed":service_info.get("copyrightText"),"release_date":None,
            "hifld_2024_equivalence":"unverified","observed_archive_scope":"USGS and DHS HIFLD OPEN Courthouses, DataLumos V1 (2024 data period); archive access returned 403",
            "esri_to_arrow_type_mapping":{field["type"]:ESRI_TO_ARROW_TYPE[field["type"]] for field in fields}},
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Retains native attributes and EPSG:3857 point geometry.",
            "objects":[{"path":parquet_path,"bytes":len(parquet),"sha256":_sha(parquet)}]}}},
        "unsupported_output_formats":{"geopackage":"No conversion contract approved.","shapefile_zip":"No field conversion contract approved.","pmtiles":"No tiling or release policy approved."},
        "promotion_allowed":False}
    validate_output_policy(manifest); provenance_projection(manifest)
    outputs = {parquet_path:parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n",
               "metadata/source_audit.json":_canonical(audit)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (root/f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt)+b"\n"
    for name,body in outputs.items(): write_immutable(storage,f"{prefix}/{name}",body)
    ready = {"approved":False,"version":version,"objects":{parquet_path:{"bytes":len(parquet),"sha256":_sha(parquet)}},
        "source_population_complete":True,"source_id_sha256":audit["id_sha256"],"source_record_sha256":audit["record_sha256"],
        "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    write_immutable(storage,f"{prefix}/metadata/candidate_ready.json",_canonical(ready)+b"\n")
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":len(features),
        "source_population_complete":True,"source_id_sha256":audit["id_sha256"],"source_schema_sha256":audit["schema_sha256"],
        "source_record_sha256":audit["record_sha256"],"source_geometry_sha256":audit["geometry_sha256"],
        "candidate_ready":f"{prefix}/metadata/candidate_ready.json","capture_directory":str(root),"reused":replay,
        "source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"baseline_accepted":False,"coverage_accepted":False,"rights_accepted":False,
        "mapping_approved":False,"release_accepted":False}
