"""Source-faithful draft capture of the USACE/BTS Navigation Facilities layer."""
from __future__ import annotations

import hashlib
import json
import math
import struct
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj

from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.sources.epa_frs_common import processing_identity

DATASET_SLUG = "docks-usace-bts"
FILE_SLUG = DATASET_SLUG
LAYER_URL = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Docks/FeatureServer/0"
EXPECTED_COUNT = 20563
PAGE_SIZE = 2000
MAX_BYTES = 384_000_000
_STRINGS = {"esriFieldTypeString", "esriFieldTypeGUID", "esriFieldTypeGlobalID"}


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                       allow_nan=False) + "\n").encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _source_modified_date(value):
    if type(value) is not int or value < 0:
        raise ValueError("source_edit_timestamp_missing")
    return datetime.fromtimestamp(value / 1000, timezone.utc).date().isoformat()


def _request(client, staging, version, name, url, params, budget, replay_prefix=None):
    if replay_prefix is not None:
        body = staging.read_key(f"{replay_prefix}/raw/{name}.body")
        wire = staging.read_key(f"{replay_prefix}/raw/{name}.wire")
        receipt_raw = staging.read_key(f"{replay_prefix}/raw/{name}.receipt.json")
        receipt = json.loads(receipt_raw)
        if (receipt.get("complete") is not True or receipt.get("status") != 200
                or receipt.get("sha256") != _sha(body) or receipt.get("bytes") != len(body)
                or receipt.get("wire_sha256") != _sha(wire) or receipt.get("wire_bytes") != len(wire)
                or receipt.get("url", "").split("?", 1)[0] != url):
            raise ValueError("retained_capture_receipt_mismatch:" + name)
        request_body = b""
        if name.startswith("page_"):
            request_body = staging.read_key(f"{replay_prefix}/raw/{name}.request.body")
            if (receipt.get("method") != "POST" or receipt.get("request_body_sha256") != _sha(request_body)
                    or receipt.get("request_body_bytes") != len(request_body)):
                raise ValueError("retained_capture_request_mismatch:" + name)
        for suffix, data in (("body", body), ("wire", wire), ("receipt.json", receipt_raw)):
            staging.write(DATASET_SLUG, FILE_SLUG, version, f"raw/{name}.{suffix}", data)
        if request_body:
            staging.write(DATASET_SLUG, FILE_SLUG, version, f"raw/{name}.request.body", request_body)
        budget[0] += len(body) + len(wire)
        if budget[0] > MAX_BYTES:
            raise ValueError("source_total_capture_budget_exceeded")
        parsed = json.loads(body)
        if not isinstance(parsed, dict) or "error" in parsed:
            raise ValueError("invalid_arcgis_response:" + name)
        return parsed
    body_parts, wire_parts = [], []
    response_obj = None
    method = "POST" if name.startswith("page_") else "GET"
    request_body = urlencode(params).encode("ascii") if method == "POST" else b""
    try:
        with client.stream(method, url, params=params if method == "GET" else None,
                           data=params if method == "POST" else None,
                           headers={"Accept-Encoding": "identity"}) as response:
            response_obj = response
            final_url = str(response.url)
            capture = capture_http_response(
                response, max_wire_bytes=32_000_000, max_decoded_bytes=32_000_000,
                reserve=lambda progress: _reserve(budget, progress.wire_bytes_retained + progress.decoded_bytes_retained + progress.reserve_bytes),
                retain_wire=wire_parts.append, retain_decoded=body_parts.append,
            )
    except Exception as exc:
        raise ValueError("source_transport_error:" + type(exc).__name__) from exc
    body, wire = b"".join(body_parts), b"".join(wire_parts)
    receipt = {"method": method, "url": str(response_obj.request.url), "final_url": final_url,
               "request_body_bytes": len(request_body), "request_body_sha256": _sha(request_body) if request_body else None,
               "status": capture.status_code, "observed_at": datetime.now(timezone.utc).isoformat(),
               "bytes": len(body), "sha256": _sha(body), "wire_bytes": len(wire),
               "wire_sha256": _sha(wire), "content_type": response_obj.headers.get("content-type"),
               "content_encoding": capture.content_encoding, "complete": capture.complete,
               "error_code": capture.error_code}
    budget[0] += len(body) + len(wire)
    if budget[0] > MAX_BYTES:
        raise ValueError("source_total_capture_budget_exceeded")
    for suffix, data in (("body", body), ("wire", wire), ("receipt.json", _canonical(receipt))):
        staging.write(DATASET_SLUG, FILE_SLUG, version, f"raw/{name}.{suffix}", data)
    if request_body:
        staging.write(DATASET_SLUG, FILE_SLUG, version, f"raw/{name}.request.body", request_body)
    if not capture.complete or capture.status_code != 200 or final_url != str(response_obj.request.url):
        raise ValueError("incomplete_source_response:" + name)
    parsed = json.loads(body)
    if not isinstance(parsed, dict) or "error" in parsed:
        raise ValueError("invalid_arcgis_response:" + name)
    return parsed


def _reserve(budget, upcoming):
    if budget[0] + upcoming > MAX_BYTES:
        raise ValueError("source_total_capture_budget_exceeded")


def _schema(layer):
    fields = layer.get("fields")
    if not isinstance(fields, list) or not fields:
        raise ValueError("source_fields_missing")
    names = [field.get("name") for field in fields]
    if any(not isinstance(name, str) for name in names) or len(names) != len(set(names)):
        raise ValueError("source_field_roster_invalid")
    if [(f.get("name"), f.get("type")) for f in fields] != EXPECTED_SCHEMA:
        raise ValueError("source_schema_drift")
    return fields


def _arrow_type(kind):
    if kind in _STRINGS:
        return pa.string()
    if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger", "esriFieldTypeDate"}:
        return pa.int64()
    if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
        return pa.float64()
    raise ValueError("unsupported_esri_field_type:" + str(kind))


def _validate_crs(ref):
    if not isinstance(ref, dict):
        raise ValueError("source_crs_missing")
    codes = [ref[k] for k in ("wkid", "latestWkid") if k in ref]
    if not codes or any(type(code) is not int or code != 4269 for code in codes):
        raise ValueError("source_crs_drift")


def _coordinates_agree(attrs, geometry):
    # Native attributes are published to six decimal places; geometry retains
    # the unrounded NAD83 doubles. Preserve both and validate their declared
    # precision relation instead of coercing either representation.
    return (type(attrs.get("LONGITUDE")) in (int, float)
            and type(attrs.get("LATITUDE")) in (int, float)
            and round(geometry.get("x"), 6) == attrs["LONGITUDE"]
            and round(geometry.get("y"), 6) == attrs["LATITUDE"])
    if any(k in ref and not pyproj.CRS.from_wkt(ref[k]).equals(pyproj.CRS.from_epsg(4269), ignore_axis_order=True)
           for k in ("wkt", "wkt2")):
        raise ValueError("source_crs_drift")


def _parquet(fields, features):
    arrays = {}
    for field in fields:
        name, kind = field["name"], field["type"]
        if kind == "esriFieldTypeGeometry":
            continue
        vals = [f.get("attributes", {}).get(name) for f in features]
        for value in vals:
            if value is None:
                continue
            if kind in _STRINGS and not isinstance(value, str):
                raise ValueError("source_string_type_drift:" + name)
            if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger", "esriFieldTypeDate"} and type(value) is not int:
                raise ValueError("source_integer_type_drift:" + name)
            if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"} and type(value) not in (int, float):
                raise ValueError("source_float_type_drift:" + name)
        arrays[name] = pa.array(vals, type=_arrow_type(kind))
    xs, ys, geometries = [], [], []
    for feature in features:
        point = feature.get("geometry")
        if point is None:
            xs.append(None); ys.append(None); geometries.append(None)
            continue
        x, y = point.get("x"), point.get("y")
        if type(x) not in (int, float) or type(y) not in (int, float):
            raise ValueError("source_point_geometry_invalid")
        xs.append(float(x)); ys.append(float(y)); geometries.append(struct.pack("<BIdd", 1, 1, float(x), float(y)))
    arrays["native_x"] = pa.array(xs, type=pa.float64())
    arrays["native_y"] = pa.array(ys, type=pa.float64())
    arrays["geometry"] = pa.array(geometries, type=pa.binary())
    table = pa.table(arrays)
    metadata = dict(table.schema.metadata or {})
    metadata[b"geo"] = _canonical({"version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"],
                                   "crs": pyproj.CRS.from_epsg(4269).to_json_dict()}}}).strip()
    sink = pa.BufferOutputStream()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", version="2.6")
    return sink.getvalue().to_pybytes()


def capture(staging, version, client=None, replay_version=None):
    """Capture the entire observed layer; retain exact pages, wire bodies, and receipts."""
    replay_prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{replay_version}" if replay_version else None
    owned = client is None and replay_prefix is None
    client = client or (httpx.Client(timeout=90, follow_redirects=False) if replay_prefix is None else None)
    budget = [0]
    try:
        layer = _request(client, staging, version, "layer", LAYER_URL, {"f": "pjson"}, budget, replay_prefix)
        if layer.get("id") != 0 or layer.get("name") != "Navigation Facilities (Dock)" or layer.get("geometryType") != "esriGeometryPoint":
            raise ValueError("source_layer_identity_drift")
        advanced = layer.get("advancedQueryCapabilities")
        if (layer.get("maxRecordCount", 0) < 1 or not isinstance(advanced, dict)
                or advanced.get("supportsPagination") is not True):
            raise ValueError("source_pagination_contract_drift")
        for dim in ("hasZ", "hasM"):
            if type(layer.get(dim)) is not bool or layer[dim]:
                raise ValueError("source_dimensions_drift")
        _validate_crs(layer.get("spatialReference"))
        _validate_crs(layer.get("extent", {}).get("spatialReference"))
        fields = _schema(layer)
        if fields[0]["name"] != "OBJECTID" or fields[0]["type"] != "esriFieldTypeOID":
            raise ValueError("source_objectid_contract_drift")
        before_count = _request(client, staging, version, "count_before", LAYER_URL + "/query",
                                {"f":"json", "where":"1=1", "returnCountOnly":"true"}, budget, replay_prefix).get("count")
        before_result = _request(client, staging, version, "ids_before", LAYER_URL + "/query",
                                 {"f":"json", "where":"1=1", "returnIdsOnly":"true"}, budget, replay_prefix)
        ids = before_result.get("objectIds")
        if before_count != EXPECTED_COUNT or not isinstance(ids, list) or len(ids) != EXPECTED_COUNT or any(type(i) is not int for i in ids):
            raise ValueError("source_population_drift")
        ids = sorted(ids)
        if len(set(ids)) != len(ids):
            raise ValueError("source_duplicate_objectid")
        features, pages = [], []
        for start in range(0, len(ids), PAGE_SIZE):
            wanted = ids[start:start + PAGE_SIZE]
            result = _request(client, staging, version, f"page_{start:07d}", LAYER_URL + "/query", {
                "f":"json", "objectIds":",".join(map(str, wanted)), "outFields":"*",
                "returnGeometry":"true", "outSR":"4269", "orderByFields":"OBJECTID ASC"
            }, budget, replay_prefix)
            page = result.get("features")
            if (not isinstance(page, list) or len(page) != len(wanted) or result.get("exceededTransferLimit") is True
                    or result.get("geometryType") != "esriGeometryPoint"):
                raise ValueError("source_page_population_drift")
            _validate_crs(result.get("spatialReference"))
            if [(f.get("name"), f.get("type")) for f in result.get("fields", [])] != [(f["name"], f["type"]) for f in fields]:
                raise ValueError("source_page_schema_drift")
            returned = [f.get("attributes", {}).get("OBJECTID") for f in page]
            if returned != wanted:
                raise ValueError("source_page_objectid_roster_drift")
            for feature in page:
                attrs = feature.get("attributes")
                if not isinstance(attrs, dict) or set(attrs) != {f["name"] for f in fields if f["type"] != "esriFieldTypeGeometry"}:
                    raise ValueError("source_attribute_roster_drift")
                geom = feature.get("geometry")
                if geom is not None:
                    if "z" in geom or "m" in geom:
                        raise ValueError("source_dimension_drift")
                    if "spatialReference" in geom:
                        _validate_crs(geom["spatialReference"])
                    if "LATITUDE" in attrs and "LONGITUDE" in attrs and not _coordinates_agree(attrs, geom):
                        raise ValueError("source_coordinate_attribute_geometry_drift")
            features.extend(page)
            pages.append({"offset": start, "requested_ids": wanted, "returned_ids": returned, "count": len(page)})
        after_count = _request(client, staging, version, "count_after", LAYER_URL + "/query",
                               {"f":"json", "where":"1=1", "returnCountOnly":"true"}, budget, replay_prefix).get("count")
        after_result = _request(client, staging, version, "ids_after", LAYER_URL + "/query",
                                {"f":"json", "where":"1=1", "returnIdsOnly":"true"}, budget, replay_prefix)
        after_ids = after_result.get("objectIds")
        if after_count != before_count or not isinstance(after_ids, list) or sorted(after_ids) != ids:
            raise ValueError("source_population_changed_during_capture")
        source_capture_started_at = (
            json.loads(staging.read_key(f"{replay_prefix}/raw/layer.receipt.json")).get("observed_at")
            if replay_prefix else json.loads(staging.read_key(
                f"{DATASET_SLUG}/{FILE_SLUG}/{version}/raw/layer.receipt.json")).get("observed_at")
        )
        parquet = _parquet(fields, features)
        invalid_coordinate_count = sum(
            feature.get("geometry") is not None
            and (not math.isfinite(feature["geometry"].get("x", math.nan))
                 or not math.isfinite(feature["geometry"].get("y", math.nan))
                 or not (-180 <= feature["geometry"]["x"] <= 180)
                 or not (-90 <= feature["geometry"]["y"] <= 90))
            for feature in features)
        parquet_path = f"geoparquet/{FILE_SLUG}.parquet"
        staging.write(DATASET_SLUG, FILE_SLUG, version, parquet_path, parquet)
        ids_sha = _sha(_canonical(ids)); schema_sha = _sha(_canonical([(f["name"], f["type"]) for f in fields]))
        source_record_sha = _sha(_canonical([f["attributes"] for f in features]))
        identity = processing_identity(Path(__file__).parents[3])
        source_item = "23d91bd988ac4fc9943128965bddfa37"
        manifest = {"schema_version":1,"approved":False,"title":"Navigation Facilities (Dock) — current USACE/BTS ArcGIS layer",
            "description":"Current candidate from the USACE-copyrighted Navigation Facilities (Dock) layer distributed through BTS/NTAD. This is a changed-source candidate; historical HIFLD Docks equivalence, national coverage semantics, source acceptance, rights acceptance, and release approval remain pending.",
            "publisher":"USACE source represented by BTS/NTAD catalog; ArcGIS service copyright USACE","agency":"U.S. Army Corps of Engineers",
            "source_url":LAYER_URL,"source_modified":_source_modified_date(layer.get("editingInfo",{}).get("dataLastEditDate")),
            "license":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
            "code_identity_scheme":identity.get("code_identity_scheme"),"processing_code":identity,
            "inputs":[{"input_id":source_item,"source_url":LAYER_URL,"publisher":"USACE-copyrighted ArcGIS service; BTS/NTAD catalog distribution","sha256":None,"observed_feature_count":len(features)}],
            "transformations":[{"operation":"Esri FeatureServer complete ID roster to typed GeoJSON and native GeoParquet candidate","parameters":{"source_fields":"all 47 declared nongeometry fields retained in source order and source values","geometry":"point XY retained in EPSG:4269; WKB encoded without reprojection","source_population":"all exact ObjectIDs returned by before/after roster snapshots","acceptance":"draft only; historical HIFLD equivalence not asserted"}}],
            "source_coverage_scope":"All 20,563 feature IDs observed in the Navigation Facilities (Dock) ArcGIS layer at capture time; geographic and historical HIFLD scope equivalence unapproved.",
            "source_acceptance":False,"rights_accepted":False,"coverage_accepted":False,"baseline":None,
            "native_crs":"EPSG:4269 (NAD83)","source_id_field":"OBJECTID","source_id_sha256":ids_sha,
            "source_schema_sha256":schema_sha,"source_record_sha256":source_record_sha,"feature_count":len(features),
            "fields":[{"name":f["name"],"type":f["type"],"alias":f.get("alias"),"length":f.get("length"),"nullable":f.get("nullable")} for f in fields],
            "capture_pages":pages,"attribute_preservation":"Every ArcGIS attribute is retained with source name, order, type, and nulls; source JSON pages are retained verbatim.",
            "transformation":"All declared ArcGIS attributes retain native names and values. The canonical GeoPackage encodes source point XY with EPSG:4269 CRS; the sidecar native GeoParquet carries WKB and exact native_x/native_y. No reprojection, row repair, filtering, or HIFLD-equivalence claim.",
            "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Source-faithful typed attributes and original point XY are retained with explicit EPSG:4269 metadata; this is an unapproved candidate, not release evidence.","objects":[{"path":parquet_path,"sha256":_sha(parquet),"bytes":len(parquet)}]}}},
            "unsupported_output_formats":{"shapefile_zip":"Field-name/value coercion is unapproved.","geopackage":"No output field mapping or baseline compatibility approval.","pmtiles":"No display transformation is approved."},
            "release_accepted":False,"promotion_allowed":False}
        staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/source_manifest.json", _canonical(manifest))
        staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/native_capture.json", _canonical({
            "layer_url":LAYER_URL,"capture_started_at":datetime.now(timezone.utc).isoformat(),"feature_count":len(features),
            "count_before":before_count,"count_after":after_count,"ids_sha256":ids_sha,"schema_sha256":schema_sha,
            "record_attributes_sha256":source_record_sha,"native_parquet_sha256":_sha(parquet),"native_crs":"EPSG:4269",
            "pages":pages,"capture_started_at":source_capture_started_at,"processed_at":datetime.now(timezone.utc).isoformat(),
            "replayed_from_version":replay_version,"capture_budget_bytes":budget[0],"source_edit_timestamp_epoch_ms":layer.get("editingInfo",{}).get("dataLastEditDate"),"catalog_description_count":"nearly 12,000",
            "api_observed_count":len(features),"catalog_api_count_discrepancy":True,"geometries_outside_epsg4326_coordinate_domain_preserved":invalid_coordinate_count,
            "code_identity":identity,
            "historical_hifld_equivalence_accepted":False,"rights_accepted":False,"source_accepted":False,"release_accepted":False}))
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":len(features),
            "source_population_complete":True,"source_id_sha256":ids_sha,"source_schema_sha256":schema_sha,
            "source_record_sha256":source_record_sha,"native_parquet_sha256":_sha(parquet),
            "null_geometry_count":sum(feature.get("geometry") is None for feature in features),
            "invalid_coordinate_count":invalid_coordinate_count,
            "source_coverage_scope":manifest["source_coverage_scope"],"candidate_ready":"metadata/source_manifest.json",
            "source_accepted":False,"rights_accepted":False,"coverage_accepted":False,"release_accepted":False}
    finally:
        if owned:
            client.close()


EXPECTED_SCHEMA = [("OBJECTID", "esriFieldTypeOID"), ("LATITUDE", "esriFieldTypeDouble"),
    ("LONGITUDE", "esriFieldTypeDouble"), ("LOC_DOCK", "esriFieldTypeString"),
    ("NAV_UNIT_ID", "esriFieldTypeString"), ("NAV_UNIT_GUID", "esriFieldTypeDouble"),
    ("UNLOCODE", "esriFieldTypeString"), ("NAV_UNIT_NAME", "esriFieldTypeString"),
    ("FAC_TYPE", "esriFieldTypeString"), ("DATA_RECORD_STATUS", "esriFieldTypeString"),
    ("LOCATION_DESCRIPTION", "esriFieldTypeString"), ("STREET_ADDRESS", "esriFieldTypeString"),
    ("CITY_OR_TOWN", "esriFieldTypeString"), ("STATE", "esriFieldTypeString"),
    ("ZIPCODE", "esriFieldTypeString"), ("COUNTY_NAME", "esriFieldTypeString"),
    ("COUNTY_FIPS_CODE", "esriFieldTypeDouble"), ("CONGRESS", "esriFieldTypeString"),
    ("CONGRESS_FIPS", "esriFieldTypeString"), ("TOWS_LINK_NUM", "esriFieldTypeDouble"),
    ("TOWS_MILE", "esriFieldTypeDouble"), ("WTWY", "esriFieldTypeDouble"),
    ("WTWY_NAME", "esriFieldTypeString"), ("PORT", "esriFieldTypeDouble"),
    ("PORT_NAME", "esriFieldTypeString"), ("PSA", "esriFieldTypeString"),
    ("PSA_NAME", "esriFieldTypeString"), ("MILE", "esriFieldTypeDouble"),
    ("BANK", "esriFieldTypeString"), ("OPERATORS", "esriFieldTypeString"),
    ("OWNERS", "esriFieldTypeString"), ("PURPOSE", "esriFieldTypeString"),
    ("HIGHWAY_NOTE", "esriFieldTypeString"), ("RAILWAY_NOTE", "esriFieldTypeString"),
    ("COMMODITIES", "esriFieldTypeString"), ("CONSTRUCTION", "esriFieldTypeString"),
    ("MECHANICAL_HANDLING", "esriFieldTypeString"), ("REMARKS", "esriFieldTypeString"),
    ("VERTICAL_DATUM", "esriFieldTypeString"), ("DEPTH_MIN", "esriFieldTypeString"),
    ("DEPTH_MAX", "esriFieldTypeString"), ("BERTHING_LARGEST", "esriFieldTypeString"),
    ("BERTHING_TOTAL", "esriFieldTypeString"), ("DECK_HEIGHT_MIN", "esriFieldTypeString"),
    ("DECK_HEIGHT_MAX", "esriFieldTypeString"), ("PARENT_OR_CHILD", "esriFieldTypeString"),
    ("SERVICE_INITIATION_DATE", "esriFieldTypeString")]
