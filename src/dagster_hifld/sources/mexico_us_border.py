"""Bounded native capture for the official IBWC US–Mexico international boundary layer.

This creates an unapproved source-faithful candidate from the producing agency's
own published feature service. Each ArcGIS response is retained verbatim with its
wire receipt. The DHS HIFLD crosswalk points to the agency's own portal item and
REST service; no mirror, archive or third-party host is ever fetched.
"""
from __future__ import annotations

import hashlib
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
import shapely
from pyproj import CRS
from shapely.geometry import LineString, MultiLineString

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response, capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "mexico-and-us-border-1"
FILE_SLUG = DATASET_SLUG
LAYER_URL = "https://gisserver.ibwc.gov/agsserver/rest/services/USIBWCPublic/US_MexicoIB/FeatureServer/0"
SERVICE_URL = LAYER_URL.rsplit("/", 1)[0]
SERVICE_DIRECTORY_URL = "https://gisserver.ibwc.gov/agsserver/rest/services/USIBWCPublic?f=pjson"
PORTAL_ITEM_ID = "f4d6ea8085d142f9b71a2860fe40bb60"
PORTAL_ITEM_URL = f"https://gisportal.ibwc.gov/agsportal/sharing/rest/content/items/{PORTAL_ITEM_ID}?f=json"
PUBLISHER = "International Boundary and Water Commission, United States and Mexico"
CROSSWALK_NOTE = ("DHS HIFLD Open Data crosswalk (DOI 10.3886/E241367V1) deprecates this layer "
                  "and directs users to the IBWC portal item and REST service captured here.")
EXPECTED_COUNT = 14
EXPECTED_IDS = list(range(1, EXPECTED_COUNT + 1))
EXPECTED_ID_SHA256 = "397db166b17b198de79db0e6b31cf193aa4e77f9b695385b7f70e94455f8d01c"
EXPECTED_GLOBALID_SHA256 = "75eaee73cf7d5c69f9cd29cd679a85694167669bfc2e987e7999fb83e9e7797f"
EXPECTED_GLOBALIDS = [
    "{2E367C76-0FEE-4316-9140-804CD159957E}", "{3CA5AD51-ECC8-41FC-BDA4-82A5C220AAF3}",
    "{66A9F54A-4B41-4439-8945-2C34C048093F}", "{24116A62-D6FB-4A7B-83FA-8558F3970889}",
    "{1093EA66-C068-4E44-877F-FF84EC423B5F}", "{996B5DDE-A90B-480F-93A1-710174260EAD}",
    "{6FBF3B57-42D2-4159-8BE3-534D91E35B85}", "{8C94A81E-BDA2-44A8-BD5B-3730602BBECF}",
    "{E13A1DCA-0153-41B8-B619-6CE2C8291F23}", "{8558ACF7-E07D-41C0-91A8-9EA5420EFC58}",
    "{65F1D28C-EBD8-49CA-8790-B872FEA83DD6}", "{468B451C-A0F9-44BA-A09A-CF00F385CD05}",
    "{70DA3A9C-525E-4F85-BF91-45AD61A7552D}", "{44E95C90-091B-4731-927E-528EA3F340C2}",
]
PAGE_SIZE = 250
MAX_BODY_BYTES = 2_500_000
MAX_CAPTURE_BYTES = 6_000_000
EXPECTED_FIELDS = [
    ("Name", "esriFieldTypeString"), ("DistanceMeters", "esriFieldTypeDouble"),
    ("DistanceFeet", "esriFieldTypeDouble"), ("DistanceKilometers", "esriFieldTypeDouble"),
    ("DistanceMiles", "esriFieldTypeDouble"), ("DistanceNauticalMiles", "esriFieldTypeDouble"),
    ("BORDER_TYPE", "esriFieldTypeInteger"), ("DateUpdated", "esriFieldTypeDate"),
    ("GlobalID", "esriFieldTypeGlobalID"), ("OBJECTID", "esriFieldTypeOID"),
    ("Label", "esriFieldTypeString"), ("Shape__Length", "esriFieldTypeDouble"),
]
EXPECTED_BORDER_TYPE_CODES = {1: "Land Boundary", 2: "River Boundary", 3: "Maritime Boundary"}
EXPECTED_FIELD_DECLARATIONS = [
    {"name": name, "type": kind, "alias": alias, "domain": domain, "nullable": nullable,
     **({"length": length} if length is not None else {})}
    for (name, kind), (alias, length, domain, nullable) in zip(
        EXPECTED_FIELDS,
        [("Name", 50, None, True), ("Distance Meters", None, None, True),
         ("Distance Feet", None, None, True), ("Distance Kilometers", None, None, True),
         ("Distance Miles", None, None, True), ("Distance Nautical Miles", None, None, True),
         ("Border Type", None, {"type": "codedValue", "name": "BORDER_TYPE", "description": "Border Type",
          "codedValues": [{"name": label, "code": code} for code, label in EXPECTED_BORDER_TYPE_CODES.items()],
          "mergePolicy": "esriMPTDefaultValue", "splitPolicy": "esriSPTDefaultValue"}, True),
         ("Date Updated", 8, None, True), ("GlobalID", 38, None, False),
         ("OBJECTID", None, None, False), ("Label", 255, None, True),
         ("Shape.STLength()", None, None, True)],
        strict=True)
]
EXPECTED_LAYER_EXTENT = {
    "xmin": -13076099.8221,
    "ymin": 2978913.1922000013,
    "xmax": -10789586.927,
    "ymax": 3858031.197800003,
}
EXPECTED_LAYER_SR = {"wkid": 102100, "latestWkid": 3857}
EXPECTED_PAGE_SR = {"wkid": 4326, "latestWkid": 4326}
EXPECTED_MAX_RECORD_COUNT = 2000
EXPECTED_COPYRIGHT = ("International Boundary and Water Commission, United States and Mexico; "
                      "National Institute of Geography and Statistics, Mexico; United States Geological Survey")
EXPECTED_ITEM = {
    "id": PORTAL_ITEM_ID, "owner": "ibwcgis", "type": "Feature Service", "access": "public",
    "title": "US_MexicoIB", "created": 1706746235861, "url": SERVICE_URL,
}
EXPECTED_ITEM_TAGS = ["NGDA", "National Geospatial Data Asset", "NGDAID82",
                      "Governmental Units and Administrative and Statistical Boundaries Theme",
                      "United States Boundary", "Mexico Boundary", "International Boundary"]
EXPECTED_COVERAGE = {
    "xmin": -117.46460326864985,
    "ymin": 25.837052801802585,
    "xmax": -96.92450845860279,
    "ymax": 32.718744268381684,
}
COVERAGE_TOLERANCE = 1e-9
SOURCE_CRS = CRS.from_epsg(4326)


def _validate_spatial_reference(value: object, label: str, expected: dict[str, int]) -> None:
    """Require every advertised ArcGIS CRS representation to agree with the pinned EPSG pair."""
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    for key, code in expected.items():
        if key in value:
            wkid = value[key]
            if type(wkid) is not int or wkid != code:
                raise ValueError(f"{label}_crs_drift")
    for key in ("wkt", "wkt2"):
        if key in value:
            wkt = value[key]
            if not isinstance(wkt, str) or not wkt.strip():
                raise ValueError(f"{label}_crs_drift")
            try:
                parsed = CRS.from_wkt(wkt)
            except Exception as exc:
                raise ValueError(f"{label}_crs_drift") from exc
            if not parsed.equals(CRS.from_epsg(expected["latestWkid"]), ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
    if not {"wkid", "latestWkid"} & set(value):
        raise ValueError(f"{label}_crs_drift")


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _fetch(client: httpx.Client, url: str, root: Path, name: str, budget: list[int]):
    root.mkdir(parents=True, exist_ok=True)
    body_path, wire_path = root / f"{name}.body", root / f"{name}.wire"
    for path in (body_path, wire_path):
        path.unlink(missing_ok=True)
    hashes = {"body": hashlib.sha256(), "wire": hashlib.sha256()}
    sizes = {"body": 0, "wire": 0}
    budget_start = budget[0]
    observed = datetime.now(timezone.utc).isoformat()

    def retain(path, kind):
        def write(chunk):
            with path.open("ab") as stream:
                stream.write(chunk)
            hashes[kind].update(chunk); sizes[kind] += len(chunk)
            budget[0] = budget_start + sizes["wire"] + sizes["body"]
        return write

    def reserve(progress):
        if budget_start + progress.wire_bytes_retained + progress.decoded_bytes_retained + progress.reserve_bytes > MAX_CAPTURE_BYTES:
            raise ValueError("source_capture_budget_exceeded")

    response = None
    result = None
    final_url = url
    transport_error = None
    try:
        with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
            final_url = str(response.url)
            result = capture_http_response(response, max_wire_bytes=MAX_BODY_BYTES,
                max_decoded_bytes=MAX_BODY_BYTES, reserve=reserve,
                retain_wire=retain(wire_path, "wire"), retain_decoded=retain(body_path, "body"))
    except Exception as exc:
        transport_error = type(exc).__name__
    receipt = {"method": "GET", "url": url, "final_url": final_url, "observed_at": observed,
        "status": result.status_code if result is not None else (response.status_code if response is not None else None),
        "complete_body": bool(result is not None and result.complete and transport_error is None),
        "wire_bytes": sizes["wire"], "wire_sha256": hashes["wire"].hexdigest(),
        "bytes": sizes["body"], "sha256": hashes["body"].hexdigest(),
        "content_encoding": result.content_encoding if result is not None else None,
        "content_length": result.content_length if result is not None else None,
        "error_code": result.error_code if result is not None else ("transport_error" if transport_error else "capture_result_missing"),
        "error_type": result.error_type if result is not None else transport_error}
    (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    if transport_error:
        raise httpx.TransportError("capture_transport_error:" + transport_error)
    if result is None:
        raise ValueError("capture_result_missing")
    body, wire = body_path.read_bytes(), wire_path.read_bytes()
    if (not result.complete or result.status_code != 200 or final_url != url
        or len(body) != receipt["bytes"] or _sha(body) != receipt["sha256"]
        or len(wire) != receipt["wire_bytes"] or _sha(wire) != receipt["wire_sha256"]):
        raise ValueError("incomplete_source_capture:" + name)
    return body, receipt


def _fetch_retry(client: httpx.Client, url: str, root: Path, name: str, budget: list[int]):
    """Retry one transient transport/incomplete read, retaining every attempt."""
    last = None
    for attempt in range(2):
        attempt_name = f"{name}.attempt{attempt+1}"
        try:
            body, receipt = _fetch(client, url, root, attempt_name, budget)
            for suffix in ("body", "wire", "receipt.json"):
                (root / f"{attempt_name}.{suffix}").replace(root / f"{name}.{suffix}")
            return body, receipt
        except (httpx.TransportError, ValueError) as exc:
            last = exc
            if isinstance(exc, ValueError) and not str(exc).startswith("incomplete_source_capture:"):
                raise
    raise last


def _esri_polyline(geometry: dict) -> bytes:
    paths = geometry.get("paths")
    if not isinstance(paths, list) or not paths:
        raise ValueError("source_polyline_paths_missing")
    has_z, has_m = geometry.get("hasZ", False), geometry.get("hasM", False)
    if type(has_z) is not bool or type(has_m) is not bool:
        raise ValueError("source_ordinate_flags_invalid")
    if has_z or has_m:
        # The pinned layer contract is XY-only; correctly flagged dimensional
        # geometry is source drift and must fail closed, never be sealed.
        raise ValueError("source_polyline_dimensional_flags_rejected")
    decoded = []
    for path in paths:
        if not isinstance(path, list) or not path:
            raise ValueError("source_polyline_path_invalid")
        points = []
        for point in path:
            if not isinstance(point, list) or len(point) != 2:
                raise ValueError("source_polyline_zm_flags_do_not_match_coordinates")
            if any(type(value) not in (int, float) or not math.isfinite(value) for value in point):
                raise ValueError("source_polyline_coordinate_invalid")
            points.append([float(point[0]), float(point[1])])
        decoded.append(points)
    if len(decoded) == 1:
        wkb = bytearray(struct.pack("<BI", 1, 2))
        wkb.extend(struct.pack("<I", len(decoded[0])))
        for x, y in decoded[0]:
            wkb.extend(struct.pack("<dd", x, y))
        return bytes(wkb)
    wkb = bytearray(struct.pack("<BI", 1, 5))
    wkb.extend(struct.pack("<I", len(decoded)))
    for points in decoded:
        wkb.extend(struct.pack("<BI", 1, 2))
        wkb.extend(struct.pack("<I", len(points)))
        for x, y in points:
            wkb.extend(struct.pack("<dd", x, y))
    return bytes(wkb)


def _arrow_type(field: dict):
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeSingle": pa.float32(),
            "esriFieldTypeDouble": pa.float64(), "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64(), "esriFieldTypeGlobalID": pa.string()}.get(
        field.get("type"), pa.string())


def _parquet_bytes(fields: list[dict], features: list[dict], wkid: int) -> bytes:
    arrays = []
    for field in fields:
        name, dtype = field["name"], _arrow_type(field)
        arrays.append(pa.array([feature["attributes"][name] for feature in features], type=dtype))
    geometries = [_esri_polyline(feature["geometry"]) for feature in features]
    arrays.append(pa.array(geometries, type=pa.binary()))
    names = [field["name"] for field in fields] + ["geometry"]
    table = pa.Table.from_arrays(arrays, names=names)
    crs = {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS",
           "name": "WGS 84", "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
           "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
           "id": {"authority": "EPSG", "code": 6326}},
           "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"}, {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
           "id": {"authority": "EPSG", "code": wkid}}
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB",
        "geometry_types": ["LineString", "MultiLineString"], "crs": crs}}})}
    sink = __import__("io").BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_roster(ids: list[int]) -> list[tuple[str, str]]:
    urls = [("portal_item", PORTAL_ITEM_URL), ("service_directory", SERVICE_DIRECTORY_URL),
            ("service_before", SERVICE_URL + "?f=pjson"),
            ("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
            ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}))]
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        page_ids = ids[start:start + PAGE_SIZE]
        urls.append((f"page_{index:03d}", _query({"objectIds": ",".join(map(str, page_ids)),
            "outFields": "*", "returnGeometry": "true", "returnZ": "true", "returnM": "true",
            "outSR": "4326", "orderByFields": "OBJECTID ASC", "f": "json"})))
    urls.extend([("layer_after", LAYER_URL + "?f=pjson"),
                 ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                 ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                 ("service_after", SERVICE_URL + "?f=pjson")])
    return urls


def _load_replay(root: Path, name: str) -> tuple[bytes, dict]:
    paths = [root / f"{name}.body", root / f"{name}.wire", root / f"{name}.receipt.json"]
    if any(path.is_symlink() for path in paths):
        raise ValueError("retained_capture_symlink_rejected")
    body = paths[0].read_bytes(); wire = paths[1].read_bytes(); receipt = json.loads(paths[2].read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("retained_capture_receipt_invalid:" + name)
    return body, receipt


def _validate(captures: dict[str, tuple[bytes, dict]]):
    if not {"ids_seed", *[name for name, _url in _capture_roster(EXPECTED_IDS)]} <= set(captures):
        raise ValueError("source_capture_roster_incomplete")
    docs = {name: json.loads(body) for name, (body, _r) in captures.items()}
    item = docs["portal_item"]
    item_url = f"https://gisportal.ibwc.gov/agsportal/home/item.html?id={PORTAL_ITEM_ID}"
    if (item.get("id") != PORTAL_ITEM_ID or item.get("owner") != EXPECTED_ITEM["owner"]
            or item.get("type") != EXPECTED_ITEM["type"] or item.get("access") != EXPECTED_ITEM["access"]
            or item.get("title") != EXPECTED_ITEM["title"] or item.get("created") != EXPECTED_ITEM["created"]
            or item.get("url") != SERVICE_URL):
        raise ValueError("ibwc_portal_item_identity_drift")
    if item.get("tags") != EXPECTED_ITEM_TAGS or item.get("accessInformation") != EXPECTED_COPYRIGHT:
        raise ValueError("ibwc_portal_item_identity_drift")
    license_info = str(item.get("licenseInfo", ""))
    if "Public Domain" not in license_info or "Public Use" not in license_info:
        raise ValueError("ibwc_portal_item_rights_drift")
    description = " ".join(str(item.get(key, "")) for key in ("snippet", "description"))
    if "international boundary between the United States of America and the United States of Mexico" not in description:
        raise ValueError("ibwc_portal_item_description_drift")
    directory = docs["service_directory"]
    if not any(service.get("name") == "USIBWCPublic/US_MexicoIB"
               and service.get("type") == "FeatureServer" for service in directory.get("services", [])):
        raise ValueError("official_ibwc_service_directory_drift")
    service = docs["service_before"]
    if docs["service_after"] != service:
        raise ValueError("source_service_changed_during_capture")
    service_description = str(service.get("serviceDescription", ""))
    for marker in ("international boundary between the United States of America and the United States of Mexico",
                   "1970 Treaty"):
        if marker not in service_description:
            raise ValueError("official_us_mexico_boundary_service_description_drift")
    if (service.get("copyrightText") != EXPECTED_COPYRIGHT
            or [(layer.get("id"), layer.get("name"), layer.get("geometryType")) for layer in service.get("layers", [])]
            != [(0, "US_MexicoIB", "esriGeometryPolyline")]):
        raise ValueError("official_us_mexico_boundary_service_drift")
    if service.get("supportedQueryFormats") != "JSON":
        raise ValueError("official_us_mexico_boundary_service_drift")
    layer = docs["layer_before"]
    if docs["layer_after"] != layer:
        raise ValueError("source_layer_changed_during_capture")
    fields = layer.get("fields")
    projected = [{key: f[key] for key in ("name", "type", "alias", "length", "domain", "nullable") if key in f}
                 for f in fields] if isinstance(fields, list) else None
    if projected != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("name") != "US_MexicoIB" or layer.get("id") != 0
            or layer.get("geometryType") != "esriGeometryPolyline"):
        raise ValueError("source_layer_identity_drift")
    if layer.get("maxRecordCount") != EXPECTED_MAX_RECORD_COUNT:
        raise ValueError("source_page_limit_drift")
    if layer.get("hasZ") or layer.get("hasM"):
        raise ValueError("source_layer_dimensional_drift")
    extent = layer.get("extent", {})
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise ValueError("source_coverage_extent_drift")
    _validate_spatial_reference(extent.get("spatialReference"), "source_layer_extent", EXPECTED_LAYER_SR)
    # FeatureServer layer JSON may omit a layer-level spatialReference; when it is
    # declared it must agree with the native Web Mercator pair.
    if layer.get("spatialReference"):
        _validate_spatial_reference(layer.get("spatialReference"), "source_layer", EXPECTED_LAYER_SR)
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    before, after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (not isinstance(before, list) or before != after or before != EXPECTED_IDS
            or len(before) != EXPECTED_COUNT or len(set(before)) != EXPECTED_COUNT
            or _sha(_canonical(sorted(before))) != EXPECTED_ID_SHA256):
        raise ValueError("source_id_population_drift")
    if docs["ids_seed"].get("objectIds") != EXPECTED_IDS:
        raise ValueError("source_seed_id_population_drift")
    features = []
    pages = sorted(name for name in captures if name.startswith("page_"))
    if len(pages) != (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE:
        raise ValueError("source_page_roster_incomplete")
    for name in pages:
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncation")
        if page.get("hasZ") or page.get("hasM"):
            raise ValueError("source_page_dimensional_drift")
        _validate_spatial_reference(page.get("spatialReference"), "source_page", EXPECTED_PAGE_SR)
        rows = page.get("features")
        if not isinstance(rows, list):
            raise ValueError("source_page_features_missing")
        features.extend(rows)
    expected_attrs = [field["name"] for field in fields]
    got = []
    geometry_valid = geometry_empty = 0
    vertices = 0
    for feature in features:
        attrs, geom = feature.get("attributes"), feature.get("geometry")
        if (not isinstance(attrs, dict) or list(attrs) != expected_attrs
                or not isinstance(geom, dict) or "paths" not in geom):
            raise ValueError("source_record_schema_or_geometry_drift")
        if set(geom) - {"paths", "hasZ", "hasM", "spatialReference"}:
            raise ValueError("unexpected_source_geometry_ordinates")
        if geom.get("hasZ") or geom.get("hasM"):
            raise ValueError("source_geometry_dimensional_drift")
        reference = geom.get("spatialReference")
        if reference is not None:
            _validate_spatial_reference(reference, "source_feature", EXPECTED_PAGE_SR)
        for name, kind in EXPECTED_FIELDS:
            value = attrs.get(name)
            if value is None:
                if not next(f["nullable"] for f in fields if f["name"] == name):
                    raise ValueError("source_non_nullable_value_missing:" + name)
                continue
            if kind == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                raise ValueError("source_oid_value_type_drift")
            if kind == "esriFieldTypeInteger" and type(value) is not int:
                raise ValueError("source_integer_value_type_drift:" + name)
            if kind == "esriFieldTypeDouble" and (type(value) not in (int, float) or not math.isfinite(value)):
                raise ValueError("source_double_value_type_drift:" + name)
            if kind == "esriFieldTypeDate" and type(value) is not int:
                raise ValueError("source_date_value_type_drift:" + name)
            if kind in ("esriFieldTypeString", "esriFieldTypeGlobalID") and (not isinstance(value, str) or not value):
                raise ValueError("source_string_value_type_drift:" + name)
            if name == "BORDER_TYPE" and value not in EXPECTED_BORDER_TYPE_CODES:
                raise ValueError("source_coded_domain_value_drift:BORDER_TYPE")
        oid = attrs.get("OBJECTID")
        global_id = attrs.get("GlobalID")
        if type(oid) is not int or oid <= 0:
            raise ValueError("source_native_identifier_missing_or_type_drift")
        if not isinstance(global_id, str) or not global_id:
            raise ValueError("source_global_id_missing_or_type_drift")
        got.append(oid)
        for path in geom.get("paths", []):
            for point in path:
                if (not isinstance(point, list) or len(point) < 2
                        or any(type(value) not in (int, float) or not math.isfinite(value) for value in point[:2])):
                    raise ValueError("source_polyline_ordinate_contract_drift")
                if not (EXPECTED_COVERAGE["xmin"] - COVERAGE_TOLERANCE <= point[0] <= EXPECTED_COVERAGE["xmax"] + COVERAGE_TOLERANCE
                        and EXPECTED_COVERAGE["ymin"] - COVERAGE_TOLERANCE <= point[1] <= EXPECTED_COVERAGE["ymax"] + COVERAGE_TOLERANCE):
                    raise ValueError("source_geometry_outside_declared_coverage")
                vertices += 1
        geometry = shapely.from_wkb(_esri_polyline(geom))
        geometry_empty += int(geometry.is_empty)
        geometry_valid += int(geometry.is_valid and not geometry.is_empty)
    if len(features) != EXPECTED_COUNT or sorted(got) != sorted(before) or len(set(got)) != EXPECTED_COUNT:
        raise ValueError("source_feature_population_incomplete")
    features.sort(key=lambda feature: feature["attributes"]["OBJECTID"])
    global_ids = [feature["attributes"]["GlobalID"] for feature in features]
    record_sha = _sha(_canonical([feature["attributes"] for feature in features]))
    geometry_sha = _sha(_canonical([[feature["attributes"]["OBJECTID"], feature["geometry"]] for feature in features]))
    schema_sha = _sha(_canonical(fields))
    if _sha(_canonical(global_ids)) != EXPECTED_GLOBALID_SHA256:
        raise ValueError("source_global_id_population_drift")
    return fields, features, {"source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": schema_sha,
        "source_id_sha256": _sha(_canonical(sorted(got))),
        "source_global_id_sha256": _sha(_canonical(global_ids)),
        "source_feature_count": len(features), "source_id_min": min(got), "source_id_max": max(got),
        "global_id_distinct_count": len(set(global_ids)), "global_id_duplicate_count": len(global_ids) - len(set(global_ids)),
        "source_geometry_type": "esriGeometryPolyline", "source_wkid": EXPECTED_PAGE_SR["latestWkid"],
        "source_layer_wkid": EXPECTED_LAYER_SR["latestWkid"],
        "source_has_z": bool(layer.get("hasZ")), "source_has_m": bool(layer.get("hasM")),
        "source_vertex_count": vertices, "geometry_valid_count": geometry_valid,
        "geometry_empty_count": geometry_empty,
        "geometry_invalid_count": len(features) - geometry_valid - geometry_empty,
        "source_coverage_scope": "Complete observed current official IBWC US–Mexico international boundary feature layer; "
        "14 boundary segments including land, river and maritime portions of the 1970 Treaty boundary.",
        "drift_guard": "Current public IBWC portal item " + PORTAL_ITEM_ID + " (owner ibwcgis on gisportal.ibwc.gov) is the "
        "producing agency's own publication; the DHS HIFLD crosswalk directs this layer there. Official service-directory "
        "membership, service/layer identity, field names/types/domains, native Web Mercator layer CRS, pinned layer extent, "
        "page errors, and before/after exact OBJECTID and GlobalID rosters are checked; service coordinates are requested "
        "in WGS84 via the service's own outSR=4326 response."}


def _write_outputs(storage, prefix: str, outputs: dict[str, bytes]) -> dict[str, dict[str, object]]:
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


def capture_mexico_us_border(storage: StagingStorageResource, *, version: str,
                             capture_directory: str | None = None) -> dict[str, object]:
    """Acquire a complete bounded source snapshot, or replay an explicit capture directory."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(path.is_symlink() for path in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
    elif requested:
        if any(requested.glob("*.body")) or any(requested.glob("*.wire")) or any(requested.glob("*.receipt.json")):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root = requested / (version + "-" + uuid4().hex)
    else:
        root = Path("/tmp/hifld-native-reconstruction/153/captures") / (version + "-" + uuid4().hex)
    if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True, exist_ok=True)
    captures = {}
    try:
        if replay:
            retained_manifest = json.loads((root / "capture_manifest.json").read_bytes())
            if retained_manifest.get("complete") is not True or not isinstance(retained_manifest.get("receipts"), dict):
                raise ValueError("retained_capture_manifest_invalid")
            roster = retained_manifest.get("artifact_roster")
            if not isinstance(roster, dict):
                raise ValueError("retained_capture_roster_manifest_missing")
            validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
            if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
                raise ValueError("retained_capture_artifact_digest_mismatch")
            ids_body, ids_receipt = _load_replay(root, "ids_before")
            ids = json.loads(ids_body).get("objectIds")
            if not isinstance(ids, list):
                raise ValueError("retained_id_seed_invalid")
            names = ["ids_seed", *[name for name, _url in _capture_roster(ids)]]
            if set(retained_manifest["receipts"]) != set(names):
                raise ValueError("retained_capture_roster_changed")
            captures = {name: _load_replay(root, name) for name in names}
            if any(receipt != retained_manifest["receipts"][name] for name, (_body, receipt) in captures.items()):
                raise ValueError("retained_capture_manifest_receipt_mismatch")
            expected_urls = dict(_capture_roster(ids))
            expected_urls["ids_seed"] = _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})
            for name, (_body, receipt) in captures.items():
                if (receipt.get("method") != "GET" or receipt.get("url") != expected_urls[name]
                        or receipt.get("final_url") != expected_urls[name]):
                    raise ValueError("retained_capture_request_identity_mismatch:" + name)
            attempt_map = retained_manifest.get("attempts", {})
            if not isinstance(attempt_map, dict):
                raise ValueError("retained_capture_attempt_manifest_invalid")
            for name, receipt in attempt_map.items():
                if not isinstance(name, str) or Path(name).name != name or "/" in name or "\\" in name:
                    raise ValueError("retained_capture_attempt_name_invalid")
                attempt_paths = [root / f"{name}.body", root / f"{name}.wire"]
                if any(path.is_symlink() for path in attempt_paths):
                    raise ValueError("retained_capture_attempt_symlink_rejected")
                attempt_body, attempt_wire = attempt_paths[0].read_bytes(), attempt_paths[1].read_bytes()
                if (receipt.get("bytes") != len(attempt_body) or receipt.get("sha256") != _sha(attempt_body)
                        or receipt.get("wire_bytes") != len(attempt_wire) or receipt.get("wire_sha256") != _sha(attempt_wire)):
                    raise ValueError("retained_failed_attempt_bytes_changed:" + name)
        else:
            budget_root = requested if requested else root.parent
            existing_bytes = sum(path.stat().st_size for path in budget_root.rglob("*")
                                 if path.is_file() and path.suffix in {".body", ".wire"})
            capture_budget = [existing_bytes]
            if existing_bytes > MAX_CAPTURE_BYTES:
                raise ValueError("source_capture_budget_exceeded_before_request")
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/ibwc-us-mexico-draft"}) as client:
                seed_body, seed_receipt = _fetch_retry(client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                                                       root, "ids_seed", capture_budget)
                ids = json.loads(seed_body).get("objectIds")
                if ids != EXPECTED_IDS or _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA256:
                    raise ValueError("source_seed_id_count_or_roster_drift")
                captures["ids_seed"] = (seed_body, seed_receipt)
                for name, url in _capture_roster(ids):
                    captures[name] = _fetch_retry(client, url, root, name, capture_budget)
        fields, features, audit = _validate(captures)
    except Exception as exc:
        retained = {}
        for receipt_path in sorted(root.glob("*.receipt.json")):
            try:
                retained[receipt_path.name.removesuffix(".receipt.json")] = json.loads(receipt_path.read_bytes())
            except (OSError, ValueError):
                retained[receipt_path.name] = {"receipt_readback_failed": True}
        # An explicit replay is read-only evidence. A failed validation must not
        # rewrite a previously complete capture as a fresh, incomplete attempt.
        if not replay:
            (root / "capture_manifest.json").write_bytes(_canonical(
                {"captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
                 "error_code": type(exc).__name__, "receipts": retained}) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
        raise

    identity = processing_identity(Path(__file__).parents[3])
    wkid = EXPECTED_PAGE_SR["latestWkid"]
    parquet = _parquet_bytes(fields, features, wkid)
    capture_sha = _sha(_canonical({name: _sha(body) for name, (body, _receipt) in sorted(captures.items())}))
    audit["source_capture_sha256"] = capture_sha
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_sha, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    inputs = [{"input_id": name,
        "producer": PUBLISHER,
        "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": PORTAL_ITEM_URL if name == "portal_item" else (SERVICE_URL if name in ("service_before", "service_after", "service_directory") else LAYER_URL),
        "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
        "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
        "license": "IBWC portal item declares Access Constraint None (Public Domain Information), Use Constraint None (Public Use); rights acceptance pending.",
        "attribution": PUBLISHER + "; rights acceptance pending.",
        "release_date": None,
        "release_date_unknown_reason": "The portal item and service publish no dataset release date; portal item created/modified times and per-feature DateUpdated attributes are recorded in the audit but are not a release date.",
        **({"distributor": "IBWC own portal (gisportal.ibwc.gov), owner ibwcgis; DHS HIFLD crosswalk directs this layer here",
            "delegation_evidence": {"portal_item_url": PORTAL_ITEM_URL, "portal_item_id": PORTAL_ITEM_ID,
            "portal_item_owner": "ibwcgis", "crosswalk_doi": "10.3886/E241367V1"}}
           if name == "portal_item" else {})}
        for name, (_body, receipt) in sorted(captures.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "Mexico and US Border (IBWC US_MexicoIB)",
        "description": "Complete observed current official IBWC US–Mexico international boundary feature layer; "
        "unapproved faithful candidate. 14 polyline segments (land, river and maritime) per the 1970 Treaty boundary. "
        "Intended for mapping purposes only per the producer's own service description.",
        "publisher": PUBLISHER, "agency": PUBLISHER, "source_url": LAYER_URL,
        "license": "Public domain per IBWC portal item metadata; acceptance pending.",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate", "parameters": {
            "source_fields": "all declared attributes retained in source order and declared physical types, including the BORDER_TYPE coded-domain codes and per-feature DateUpdated epoch-milliseconds",
            "source_ids": "OBJECTID and GlobalID retained unchanged; no substitute row identifiers generated",
            "geometry": "Polyline paths retained as captured from the service's own outSR=4326 response; WKB LineString/MultiLineString candidate at EPSG:4326; no reprojection, repair or coordinate edit; native layer storage CRS (Web Mercator 102100/3857) recorded in the audit",
            "candidate_status": "unapproved; crosswalk succession, source release, historical baseline, ID/schema/coverage and rights decisions remain pending"}}],
        "lineage": CROSSWALK_NOTE + " The captured portal item (owner ibwcgis, org 0123456789ABCDEF on gisportal.ibwc.gov) "
        "declares NGDA ID 82 and points at the captured FeatureServer on gisserver.ibwc.gov. Relationship to the historical "
        "HIFLD-hosted copy (GeoPlatform item 1ceab4c3babb4f058ec6c773574a6ae5, archived inventory modification 2025-04-08) "
        "is unestablished; no archive, mirror or third-party host is fetched.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason":
            "Native layer attributes, identifiers and unmodified polyline paths retained as a candidate; "
            "release and compatibility decisions remain pending.",
            "objects": [{"path": f"geoparquet/{DATASET_SLUG}.parquet", "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No approved schema/compatibility contract.",
                                       "pmtiles": "No display transformation approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"source_fields": fields, "official_layer_name": "US_MexicoIB",
        "official_source_item": "IBWC portal item " + PORTAL_ITEM_ID, "prior_source_item": None,
        "source_item_succession_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
        "ibwc_portal_item": {key: item_metadata for key, item_metadata in (
            (key, json.loads(captures["portal_item"][0]).get(key)) for key in
            ("id", "owner", "created", "modified", "title", "type", "access", "url", "snippet"))}})
    attempts = {}
    for receipt_path in sorted(root.glob("*.attempt*.receipt.json")):
        try:
            attempts[receipt_path.name.removesuffix(".receipt.json")] = json.loads(receipt_path.read_bytes())
        except (OSError, ValueError):
            attempts[receipt_path.name] = {"receipt_readback_failed": True}
    if not replay:
        (root / "capture_manifest.json").write_bytes(_canonical(
            {"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
             "source_feature_count": audit["source_feature_count"],
             "source_record_sha256": audit["source_record_sha256"],
             "source_schema_sha256": audit["source_schema_sha256"],
             "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
             "attempts": attempts,
             "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
    outputs = {f"geoparquet/{DATASET_SLUG}.parquet": parquet,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version,
        "objects": {f"geoparquet/{DATASET_SLUG}.parquet": {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True,
        "source_record_sha256": audit["source_record_sha256"],
        "source_item_succession_accepted": False, "source_release_accepted": False,
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    fingerprint = _sha(_canonical(identity))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("capture_receipts"), dict):
            raise ValueError("cached_candidate_original_receipts_missing")
        original_receipts = payload["capture_receipts"]
        if set(original_receipts) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name, (fresh_body, _receipt) in captures.items():
            retained = storage.read_key(f"{prefix}/raw/{name}.body")
            if retained != fresh_body:
                raise ValueError("cached_source_bytes_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
            fingerprint, identity, audit["source_record_sha256"], audit["source_schema_sha256"],
            payload.get("objects", {}), original_receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["source_feature_count"], "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"],
            "source_id_max": audit["source_id_max"], "source_schema_sha256": audit["source_schema_sha256"],
            "source_coverage_scope": audit["source_coverage_scope"], "drift_guard": audit["drift_guard"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(root)}
    descriptors = _write_outputs(storage, prefix, outputs)
    seal_ready_candidate(storage, state_key, version, fingerprint,
        {"processing_identity": identity, "source_record_sha256": audit["source_record_sha256"],
         "source_schema_sha256": audit["source_schema_sha256"], "capture_receipts": receipts,
         "objects": descriptors})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["source_feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"],
        "source_id_max": audit["source_id_max"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_coverage_scope": audit["source_coverage_scope"], "drift_guard": audit["drift_guard"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json",
        "source_item_succession_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
