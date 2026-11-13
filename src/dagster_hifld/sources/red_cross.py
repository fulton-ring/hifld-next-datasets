"""Source-faithful, candidate-only capture of the Red Cross divisions layer.

The American Red Cross publishes this layer itself through its NHQRedCrossGIS
publisher account (item 8c6753c3c5814d05be65723cd4615d7c, organization
pGfbNJoYypmNq86F), per the repository source audit that retired the retired
PASDA mirror input. This adapter preserves the complete observed six-division
population and records the remaining limits; it does not resolve the FY26/FY27
effective edition, repair geometry, map the historical product, or approve a
release.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from datetime import datetime, timezone
from fiona.io import MemoryFile
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

import fiona
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import shape

from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.resources import StagingStorageResource

DATASET_SLUG = FILE_SLUG = "american-red-cross-divisions"
SERVICE = "https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Master_ARC_Geography_2022/FeatureServer"
SERVICE_URL = SERVICE + "/1"
ITEM_ID = "8c6753c3c5814d05be65723cd4615d7c"
PUBLISHER_ACCOUNT = "NHQRedCrossGIS"
ORGANIZATION_ID = "pGfbNJoYypmNq86F"
ITEM_API_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}"
OWNER_API_URL = f"https://www.arcgis.com/sharing/rest/community/users/{PUBLISHER_ACCOUNT}"
EXPECTED_LAYER = "Master_RC_Geo_Division_2027"
EXPECTED_COUNT = 6
EXPECTED_IDS = (1, 2, 3, 4, 5, 6)
# Frozen only after the complete original ring population was audited against
# the preserved native captures. Empty refuses staging.
GEOMETRY_SHA256 = "9f80eb2d1843f5a6b9414c349a6615ca1d762fd4eb06497c56b49477482c448c"
ATTRIBUTE_SHA256 = "30259c13416de0bb7c5b99bef310d9da0d1c3e2ed3e7351fab18285f3c84fa61"
FIELDS = [("OBJECTID", "esriFieldTypeOID", False, None),
          ("DCODE", "esriFieldTypeString", True, 255),
          ("Division", "esriFieldTypeString", True, 255),
          ("States", "esriFieldTypeString", True, 255),
          ("Acres", "esriFieldTypeDouble", True, None),
          ("SQ_MI", "esriFieldTypeDouble", True, None),
          ("Division_DCODE", "esriFieldTypeString", True, 60),
          ("Shape__Area", "esriFieldTypeDouble", True, None),
          ("Shape__Length", "esriFieldTypeDouble", True, None)]
MAX_FEATURE_BYTES = 32 * 1024 * 1024
MAX_META_BYTES = 1024 * 1024
MAX_TOTAL_BYTES = 48 * 1024 * 1024

CAPTURE_NAMES = (
    "layer_before", "item", "owner", "count_before", "ids_before", "page_0000",
    "tail", "count_after", "ids_after", "layer_after",
)
CAPTURE_FILES = {f"{name}.{suffix}" for name in CAPTURE_NAMES for suffix in ("body", "receipt.json")}
_CANONICAL = lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True,
                                       separators=(",", ":"), allow_nan=False).encode("utf-8")
_STRING_TYPES = {"esriFieldTypeString", "esriFieldTypeGUID", "esriFieldTypeGlobalID"}


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _strict_json(body: bytes):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("red_cross_divisions_duplicate_json_member")
            result[key] = value
        return result
    value = json.loads(body.decode("utf-8"), object_pairs_hook=pairs,
                       parse_constant=lambda value: (_ for _ in ()).throw(ValueError("red_cross_divisions_nonfinite_json")))
    if type(value) is not dict:
        raise ValueError("red_cross_divisions_nonobject_response")
    if "error" in value:
        code = value["error"].get("code") if isinstance(value["error"], dict) else None
        raise ValueError(f"red_cross_divisions_error_response:{code}")
    return value


def _limit(name: str) -> int:
    return MAX_FEATURE_BYTES if name.startswith("page_") else MAX_META_BYTES


def _validate_receipt(name: str, body: bytes, receipt: dict) -> dict:
    if not isinstance(receipt, dict) or receipt.get("status") != 200:
        raise ValueError("red_cross_divisions_http_status_drift:" + name)
    if not receipt.get("complete_body", receipt.get("complete", False)):
        raise ValueError("red_cross_divisions_incomplete_receipt:" + name)
    if receipt.get("sha256") != _sha(body) or type(receipt.get("bytes")) is not int or receipt["bytes"] != len(body):
        raise ValueError("red_cross_divisions_receipt_body_mismatch:" + name)
    observed = receipt.get("fetched_at", receipt.get("observed_at"))
    if not isinstance(observed, str):
        raise ValueError("red_cross_divisions_missing_original_retrieval_time:" + name)
    try:
        stamp = datetime.fromisoformat(observed.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("red_cross_divisions_invalid_original_retrieval_time:" + name) from exc
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("red_cross_divisions_naive_original_retrieval_time:" + name)
    request_url = receipt.get("url")
    final_url = receipt.get("final_url", request_url)
    if not isinstance(request_url, str) or not isinstance(final_url, str):
        raise ValueError("red_cross_divisions_receipt_url_missing:" + name)
    if name.startswith("layer_"):
        expected_host, expected_path = urlsplit(SERVICE_URL).hostname, urlsplit(SERVICE_URL).path
    elif name.startswith(("count_", "ids_", "page_", "tail")):
        expected_host, expected_path = urlsplit(SERVICE_URL).hostname, urlsplit(SERVICE_URL).path + "/query"
    elif name == "item":
        expected_host, expected_path = urlsplit(ITEM_API_URL).hostname, urlsplit(ITEM_API_URL).path
    elif name == "owner":
        expected_host, expected_path = urlsplit(OWNER_API_URL).hostname, urlsplit(OWNER_API_URL).path
    else:
        raise ValueError("red_cross_divisions_unknown_capture:" + name)
    for url in (request_url, final_url):
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != expected_host or parsed.path != expected_path:
            raise ValueError("red_cross_divisions_receipt_endpoint_drift:" + name)
    return {"body": body, "receipt": receipt, "retrieved_at": observed,
            "sha256": _sha(body), "bytes": len(body)}


def _load_capture(capture_dir: Path) -> dict[str, dict]:
    if capture_dir.is_symlink() or not capture_dir.is_dir():
        raise ValueError("red_cross_divisions_capture_directory_invalid")
    actual = {entry.name for entry in capture_dir.iterdir()}
    if actual != CAPTURE_FILES or any((capture_dir / name).is_symlink() or not (capture_dir / name).is_file() for name in actual):
        raise ValueError("red_cross_divisions_capture_roster_mismatch")
    captures = {}
    total = 0
    for name in CAPTURE_NAMES:
        body = (capture_dir / f"{name}.body").read_bytes()
        receipt_body = (capture_dir / f"{name}.receipt.json").read_bytes()
        receipt = _strict_json(receipt_body)
        if len(body) > _limit(name):
            raise ValueError("red_cross_divisions_response_byte_limit:" + name)
        total += len(body)
        captures[name] = {**_validate_receipt(name, body, receipt), "receipt_body": receipt_body}
    if total > MAX_TOTAL_BYTES:
        raise ValueError("red_cross_divisions_total_response_byte_limit")
    return captures


def _request(client: httpx.Client, url: str, params: dict[str, str], name: str) -> tuple[bytes, dict]:
    raw: list[bytes] = []
    wire: list[bytes] = []
    with client.stream("GET", url, params=params, headers={"Accept-Encoding": "identity"}) as response:
        captured = capture_http_response(response, max_wire_bytes=_limit(name),
            max_decoded_bytes=_limit(name), reserve=lambda progress: None,
            retain_wire=wire.append, retain_decoded=raw.append)
        receipt = {"url": url + "?" + urlencode(params), "final_url": str(response.url),
            "fetched_at": datetime.now(timezone.utc).isoformat(), "status": captured.status_code,
            "bytes": captured.decoded_bytes_retained, "sha256": captured.decoded_sha256,
            "wire_bytes": captured.wire_bytes_retained, "wire_sha256": captured.wire_sha256,
            "complete_body": captured.complete, "content_encoding": captured.content_encoding,
            "content_type": response.headers.get("content-type"), "error_code": captured.error_code}
    body = b"".join(raw)
    if not captured.complete or captured.status_code != 200 or len(body) > _limit(name):
        raise ValueError("red_cross_divisions_live_response_incomplete:" + name)
    return body, receipt


def capture_live(capture_dir: Path) -> None:
    """Capture item/account witnesses, metadata, count/ID witnesses and the complete layer page."""
    capture_dir.mkdir(parents=True, exist_ok=False)
    total = 0
    def save(name: str, body: bytes, receipt: dict):
        nonlocal total
        if len(body) > _limit(name):
            raise ValueError("red_cross_divisions_response_byte_limit:" + name)
        total += len(body)
        if total > MAX_TOTAL_BYTES:
            raise ValueError("red_cross_divisions_total_response_byte_limit")
        (capture_dir / f"{name}.body").write_bytes(body)
        (capture_dir / f"{name}.receipt.json").write_bytes(_CANONICAL(receipt) + b"\n")
    with httpx.Client(timeout=httpx.Timeout(45, connect=15), follow_redirects=False) as client:
        save("layer_before", *_request(client, SERVICE_URL, {"f": "pjson"}, "layer_before"))
        save("item", *_request(client, ITEM_API_URL, {"f": "json"}, "item"))
        save("owner", *_request(client, OWNER_API_URL, {"f": "json"}, "owner"))
        save("count_before", *_request(client, SERVICE_URL + "/query",
            {"f": "json", "where": "1=1", "returnCountOnly": "true"}, "count_before"))
        save("ids_before", *_request(client, SERVICE_URL + "/query",
            {"f": "json", "where": "1=1", "returnIdsOnly": "true"}, "ids_before"))
        save("page_0000", *_request(client, SERVICE_URL + "/query",
            {"f": "json", "where": "1=1", "outFields": "*", "returnGeometry": "true",
             "orderByFields": "OBJECTID ASC", "resultOffset": 0, "resultRecordCount": 2000}, "page_0000"))
        save("tail", *_request(client, SERVICE_URL + "/query",
            {"f": "json", "where": "1=1", "outFields": "OBJECTID", "returnGeometry": "false",
             "orderByFields": "OBJECTID ASC", "resultOffset": EXPECTED_COUNT, "resultRecordCount": 2000}, "tail"))
        save("count_after", *_request(client, SERVICE_URL + "/query",
            {"f": "json", "where": "1=1", "returnCountOnly": "true"}, "count_after"))
        save("ids_after", *_request(client, SERVICE_URL + "/query",
            {"f": "json", "where": "1=1", "returnIdsOnly": "true"}, "ids_after"))
        save("layer_after", *_request(client, SERVICE_URL, {"f": "pjson"}, "layer_after"))
    _load_capture(capture_dir)


def _ids(payload: dict) -> list[int]:
    if payload.get("objectIdFieldName") != "OBJECTID" or not isinstance(payload.get("objectIds"), list):
        raise ValueError("red_cross_divisions_id_witness_malformed")
    ids = payload["objectIds"]
    if any(type(identity) is not int for identity in ids) or len(set(ids)) != len(ids):
        raise ValueError("red_cross_divisions_id_roster_invalid")
    return sorted(ids)


def _validate_request_scope(name: str, receipt: dict) -> None:
    params = {key: values for key, values in parse_qs(urlsplit(receipt["url"]).query, keep_blank_values=True).items()}
    if name.startswith("layer_"):
        expected = {"f": ["pjson"]}
    elif name in ("item", "owner"):
        expected = {"f": ["json"]}
    elif name.startswith("count_"):
        expected = {"f": ["json"], "where": ["1=1"], "returnCountOnly": ["true"]}
    elif name.startswith("ids_"):
        expected = {"f": ["json"], "where": ["1=1"], "returnIdsOnly": ["true"]}
    elif name == "page_0000":
        expected = {"f": ["json"], "where": ["1=1"], "outFields": ["*"], "returnGeometry": ["true"],
            "orderByFields": ["OBJECTID ASC"], "resultOffset": ["0"], "resultRecordCount": ["2000"]}
    elif name == "tail":
        expected = {"f": ["json"], "where": ["1=1"], "outFields": ["OBJECTID"], "returnGeometry": ["false"],
            "orderByFields": ["OBJECTID ASC"], "resultOffset": [str(EXPECTED_COUNT)], "resultRecordCount": ["2000"]}
    else:
        raise ValueError("red_cross_divisions_unknown_capture:" + name)
    if params != expected:
        raise ValueError("red_cross_divisions_request_scope_drift:" + name)


def _schema(layer: dict) -> list[dict]:
    if (layer.get("id") != 1 or layer.get("name") != EXPECTED_LAYER
            or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("objectIdField") != "OBJECTID"
            or layer.get("serviceItemId") != ITEM_ID):
        raise ValueError("red_cross_divisions_layer_identity_drift")
    if layer.get("hasZ") is not False or layer.get("hasM") is not False:
        raise ValueError("red_cross_divisions_dimension_declaration_drift")
    sr = layer.get("spatialReference")
    if not isinstance(sr, dict) or sr.get("latestWkid") != 3857 or sr.get("wkid") not in {102100, 3857}:
        raise ValueError("red_cross_divisions_crs_declaration_drift")
    fields = layer.get("fields")
    if not isinstance(fields, list) or len(fields) != len(FIELDS):
        raise ValueError("red_cross_divisions_field_roster_drift")
    if any(type(field.get("nullable")) is not bool for field in fields):
        raise ValueError("red_cross_divisions_field_nullable_declaration_drift")
    declared = [(field.get("name"), field.get("type"), field.get("nullable"), field.get("length")) for field in fields]
    if _CANONICAL(declared) != _CANONICAL(list(FIELDS)):
        raise ValueError("red_cross_divisions_native_field_declaration_drift")
    return fields


def _witnesses(captures: dict[str, dict], item: dict, owner: dict) -> None:
    if item.get("id") != ITEM_ID or item.get("owner") != PUBLISHER_ACCOUNT or item.get("access") != "public" or item.get("url") != SERVICE:
        raise ValueError("red_cross_divisions_publisher_item_drift")
    if item.get("orgId") != ORGANIZATION_ID:
        raise ValueError("red_cross_divisions_publisher_organization_drift")
    if owner.get("username") != PUBLISHER_ACCOUNT:
        raise ValueError("red_cross_divisions_publisher_account_drift")


def _validate_population(captures: dict[str, dict]) -> tuple[dict, list[dict], list[int], list]:
    layer = _strict_json(captures["layer_before"]["body"])
    fields = _schema(layer)
    _witnesses(captures, _strict_json(captures["item"]["body"]), _strict_json(captures["owner"]["body"]))
    if _CANONICAL(_strict_json(captures["layer_after"]["body"])) != _CANONICAL(layer):
        raise ValueError("red_cross_divisions_layer_metadata_changed_during_capture")
    count_before = _strict_json(captures["count_before"]["body"]).get("count")
    count_after = _strict_json(captures["count_after"]["body"]).get("count")
    ids_before = _ids(_strict_json(captures["ids_before"]["body"]))
    ids_after = _ids(_strict_json(captures["ids_after"]["body"]))
    if (type(count_before) is not int or type(count_after) is not int
            or count_before != EXPECTED_COUNT or count_after != EXPECTED_COUNT
            or len(ids_before) != EXPECTED_COUNT or tuple(ids_before) != EXPECTED_IDS
            or ids_before != ids_after):
        raise ValueError("red_cross_divisions_population_changed_during_capture")
    for name in CAPTURE_NAMES:
        _validate_request_scope(name, captures[name]["receipt"])
    features = _strict_json(captures["page_0000"]["body"])
    tail = _strict_json(captures["tail"]["body"])
    if features.get("exceededTransferLimit") is True or tail.get("exceededTransferLimit") is True:
        raise ValueError("red_cross_divisions_incomplete_pagination")
    if tail.get("features") != []:
        raise ValueError("red_cross_divisions_incomplete_pagination")
    if (features.get("spatialReference") != layer["spatialReference"]
            or features.get("geometryType") != "esriGeometryPolygon"
            or features.get("objectIdFieldName") != "OBJECTID"):
        raise ValueError("red_cross_divisions_response_geometry_profile_drift")
    expected_query_fields = [{k: v for k, v in field.items() if k not in {"nullable", "editable"}} for field in layer["fields"]]
    if _CANONICAL(features.get("fields")) != _CANONICAL(expected_query_fields):
        raise ValueError("red_cross_divisions_response_schema_drift")
    rows = features.get("features")
    names = [field["name"] for field in fields]
    if not isinstance(rows, list) or len(rows) != EXPECTED_COUNT:
        raise ValueError("red_cross_divisions_incomplete_rows")
    ids = []
    raw_rings = []
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"attributes", "geometry"}:
            raise ValueError("red_cross_divisions_feature_member_drift")
        attrs = row["attributes"]
        if not isinstance(attrs, dict) or list(attrs) != names:
            raise ValueError("red_cross_divisions_attribute_roster_or_order_drift")
        for field in fields:
            value = attrs[field["name"]]
            kind = field["type"]
            if value is None:
                if field.get("nullable") is False:
                    raise ValueError("red_cross_divisions_nonnullable_attribute_null:" + field["name"])
                continue
            if kind in _STRING_TYPES:
                width = field.get("length")
                if not isinstance(value, str) or (isinstance(width, int) and len(value.encode("utf-16-le")) // 2 > width):
                    raise ValueError("red_cross_divisions_text_type_or_width:" + field["name"])
            elif kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"}:
                if type(value) is not int or not 0 <= value < 2**63:
                    raise ValueError("red_cross_divisions_integer_type_or_range:" + field["name"])
            elif kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
                if type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError("red_cross_divisions_double_type_or_nonfinite:" + field["name"])
            else:
                raise ValueError("red_cross_divisions_unsupported_field_type:" + field["name"])
        ids.append(attrs["OBJECTID"])
        geometry = row["geometry"]
        if not isinstance(geometry, dict) or set(geometry) != {"rings"}:
            raise ValueError("red_cross_divisions_native_polygon_geometry_drift")
        for ring in geometry["rings"]:
            if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
                raise ValueError("red_cross_divisions_unclosed_or_short_ring")
            if any(type(point) is not list or len(point) != 2
                   or any(type(value) not in (int, float) or not math.isfinite(value) for value in point) for point in ring):
                raise ValueError("red_cross_divisions_nonfinite_or_nonXY_coordinate")
        raw_rings.append(tuple(tuple(tuple(float(value) for value in point) for point in ring)
                               for ring in geometry["rings"]))
    if tuple(ids) != EXPECTED_IDS:
        raise ValueError("red_cross_divisions_ordered_identifier_census_drift")
    geometry_census = hashlib.sha256(_CANONICAL([[row["attributes"]["OBJECTID"], row["geometry"]] for row in rows])).hexdigest()
    if not GEOMETRY_SHA256 or geometry_census != GEOMETRY_SHA256:
        raise ValueError(f"red_cross_divisions_geometry_census_drift_requires_review:{geometry_census}")
    attribute_census = hashlib.sha256(_CANONICAL([[row["attributes"]["OBJECTID"], row["attributes"]] for row in rows])).hexdigest()
    if not ATTRIBUTE_SHA256 or attribute_census != ATTRIBUTE_SHA256:
        raise ValueError(f"red_cross_divisions_attribute_census_drift_requires_review:{attribute_census}")
    # GDAL identifies ESRIJSON from the leading metadata; valid JSON member order
    # must not affect decoding. Rewrap only for that parser, retain original raw
    # wire bytes and verify every resulting attribute and ordered ring below.
    decoder_input = {"objectIdFieldName": "OBJECTID", "geometryType": features["geometryType"],
        "spatialReference": features["spatialReference"], "fields": features["fields"], "features": rows}
    with MemoryFile(json.dumps(decoder_input, allow_nan=False).encode()) as memory, memory.open(driver="ESRIJSON") as stream:
        converted = [(dict(feature["properties"]), shape(feature["geometry"])) for feature in stream]
    if len(converted) != len(rows) or any(props != row["attributes"] for (props, geom), row in zip(converted, rows)):
        raise ValueError("red_cross_divisions_native_decoder_attribute_loss")
    geometries = [geom for props, geom in converted]
    if any(geom.is_empty or geom.geom_type not in {"Polygon", "MultiPolygon"} for geom in geometries):
        raise ValueError("red_cross_divisions_missing_or_nonpolygon_geometry")
    for row, geom in zip(rows, geometries):
        original = Counter(tuple(tuple(float(value) for value in point) for point in ring) for ring in row["geometry"]["rings"])
        if _ring_bag(geom) != original:
            raise ValueError("red_cross_divisions_geometry_association_or_ring_loss_for_source_ID")
    if _whole_population_ring_bag(geometries) != Counter(ring for row_rings in raw_rings for ring in row_rings):
        raise ValueError("red_cross_divisions_native_ring_coordinate_or_order_loss")
    if tuple(ids) != EXPECTED_IDS:
        raise ValueError("red_cross_divisions_ordered_identifier_census_drift")
    return layer, rows, ids_before, geometries


def _ring_bag(geom) -> Counter:
    polygons = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    return Counter(tuple(point for point in ring.coords) for polygon in polygons
                   for ring in [polygon.exterior, *polygon.interiors])


def _whole_population_ring_bag(geometries) -> Counter:
    result = Counter()
    for geom in geometries:
        result.update(_ring_bag(geom))
    return result


def _arrow_type(kind: str):
    if kind in _STRING_TYPES:
        return pa.string()
    if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"}:
        return pa.int64()
    if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
        return pa.float64()
    raise ValueError("red_cross_divisions_unsupported_arrow_field_type")


def _geoparquet(fields: list[dict], rows: list[dict], geometries: list) -> tuple[bytes, dict]:
    arrays = []
    schema_fields = []
    for field in fields:
        dtype = _arrow_type(field["type"])
        arrays.append(pa.array([row["attributes"][field["name"]] for row in rows], type=dtype, safe=True))
        schema_fields.append(pa.field(field["name"], dtype, nullable=field.get("nullable") is not False))
    wkb_values = [geom.wkb for geom in geometries]
    arrays.append(pa.array(wkb_values, type=pa.binary(), safe=True))
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=False))
    bounds = [min(geom.bounds[index] for geom in geometries) for index in (0, 1)] + \
             [max(geom.bounds[index] for geom in geometries) for index in (2, 3)]
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB",
               "geometry_types": sorted(set(geom.geom_type for geom in geometries)),
               "crs": pyproj.CRS.from_epsg(3857).to_json_dict(), "bbox": bounds}}}
    metadata = {b"geo": _CANONICAL(geo),
                b"arc_source_fields": _CANONICAL(fields),
                b"arc_encoding_gate": ("OID producer bit width unspecified by ArcGIS metadata; proposed int64 container "
                                       "encoding and every schema compatibility decision remain unapproved.").encode("utf-8")}
    schema = pa.schema(schema_fields, metadata=metadata)
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), sink, compression="zstd", use_dictionary=False)
    body = sink.getvalue().to_pybytes()
    table = pq.read_table(pa.BufferReader(body))
    if table.num_rows != EXPECTED_COUNT or table.schema.names != [field["name"] for field in fields] + ["geometry"]:
        raise ValueError("red_cross_divisions_geoparquet_readback_schema_drift")
    if table.column("OBJECTID").to_pylist() != [row["attributes"]["OBJECTID"] for row in rows]:
        raise ValueError("red_cross_divisions_geoparquet_id_parity_failure")
    if table.column("geometry").null_count != 0 or table.column("geometry").to_pylist() != wkb_values:
        raise ValueError("red_cross_divisions_geoparquet_geometry_parity_failure")
    if any(table.column(field["name"]).to_pylist() != [row["attributes"][field["name"]] for row in rows]
           for field in fields):
        raise ValueError("red_cross_divisions_geoparquet_attribute_parity_failure")
    return body, geo


def _identity_files() -> list[Path]:
    package_root = Path(__file__).resolve().parents[1]
    return [package_root / "sources" / "red_cross.py", package_root / "assets" / "red_cross.py",
            package_root / "http_capture.py", package_root / "provenance.py",
            package_root / "output_policy.py", package_root / "literal_geojson_policy.py",
            package_root / "resources.py"]


def _code_digest() -> str:
    package_root = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    for path in _identity_files():
        name = path.relative_to(package_root).as_posix().encode("utf-8")
        body = path.read_bytes()
        digest.update(len(name).to_bytes(8, "big") + name)
        digest.update(len(body).to_bytes(8, "big") + body)
    return digest.hexdigest()


_EDITION_REASON = ("Layer and item metadata claim FY27/July2026 while the active service notice says it was "
                   "temporarily reverted to FY26/January2026; the effective six-row edition is unresolved.")


def _manifest(captures: dict[str, dict], layer: dict, fields: list[dict], parquet: bytes, geo: dict,
              input_records: dict[str, dict], presence_bytes: bytes, invalid_geometry_ids: list[int]) -> dict:
    canonical_urls = {name: (SERVICE_URL if name.startswith("layer_") else
                             ITEM_API_URL if name == "item" else OWNER_API_URL if name == "owner"
                             else SERVICE_URL + "/query") for name in CAPTURE_NAMES}
    inputs = []
    for name in CAPTURE_NAMES:
        record = input_records[name]
        receipt = record["receipt"]
        request_url = receipt.get("url")
        observed_url = receipt.get("final_url", request_url)
        inputs.append({"input_id": f"ARC-divisions:{name}",
            "producer": "American Red Cross (publisher account NHQRedCrossGIS)",
            "host": urlsplit(observed_url).hostname, "role": "producer_publication",
            "canonical_url": canonical_urls[name], "request_url": request_url,
            "retrieval_url": observed_url, "retrieved_at": record["retrieved_at"],
            "sha256": record["sha256"], "bytes": record["bytes"],
            "raw_path": f"raw_arcgis/{name}.body", "receipt_path": f"raw_arcgis/{name}.receipt.json",
            "receipt_sha256": _sha(record["receipt_body"]), "receipt_bytes": len(record["receipt_body"]),
            "http_status": 200,
            "license": "Publisher item metadata claims no explicit constraints; rights are not independently verified.",
            "attribution": "American Red Cross NHQRedCrossGIS publisher account; attribution claim unaccepted.",
            "release_date_unknown_reason": _EDITION_REASON})
    raw_roster = []
    for name in CAPTURE_NAMES:
        raw_roster.extend([
            {"name": f"raw_arcgis/{name}.body", "bytes": captures[name]["bytes"], "sha256": captures[name]["sha256"]},
            {"name": f"raw_arcgis/{name}.receipt.json", "bytes": len(captures[name]["receipt_body"]),
             "sha256": _sha(captures[name]["receipt_body"])},
        ])
    raw_roster.append({"name": "metadata/source_feature_presence.json", "bytes": len(presence_bytes),
                       "sha256": _sha(presence_bytes)})
    manifest = {"title": "American Red Cross Divisions", "description":
        "Complete observed six-division ArcGIS layer candidate from the American Red Cross NHQRedCrossGIS publisher account. "
        "Rights, effective FY26/FY27 edition, historical baseline compatibility and release remain unapproved.",
        "publisher": "American Red Cross (NHQRedCrossGIS publisher account)", "source_url": SERVICE_URL,
        "agency": "American Red Cross", "schema_version": 1, "processor": "HIFLD Next",
        "code_commit": f"installed-processing-sha256:{_code_digest()}",
        "code_identity_scheme": "installed_source_bundle_sha256",
        "processing_code": {"installed_pipeline_sha256": _code_digest(),
            "identity_files": [path.relative_to(Path(__file__).resolve().parents[1]).as_posix()
                               for path in _identity_files()],
            "identity_scope": "Installed source adapter, asset wrapper, capture helper, provenance, output policy and staging resource code bytes; this fingerprint is not a Git commit."},
        "inputs": inputs,
        "transformations": [{"name": "ArcGIS FeatureServer ESRIJSON polygons to typed GeoParquet", "parameters": {
            "attributes": "identity names, order and typed values from the declared nine layer fields; nulls preserved",
            "geometry": "native EPSG:3857 rings preserved without reprojection, quantization or topology repair; Polygon/MultiPolygon grouping exactly as decoded from ESRIJSON",
            "identifiers": "original OBJECTID values 1-6 retained; no substitute row IDs",
            "row_order": "ascending OBJECTID as returned by the complete single page and empty tail query",
            "drift_census": "pinned whole-layer attribute and geometry censuses; any source change refuses staging with the observed digest",
            "geometry_quality": "known source geometry validity defects are reported, never repaired"}, "acceptance": "unapproved draft"}],
        "lineage": {"candidate_scope": "complete observed service layer only; no national or HIFLD coverage equivalence asserted",
            "item_id": ITEM_ID, "publisher_account": PUBLISHER_ACCOUNT,
            "organization_id": ORGANIZATION_ID, "layer_name": EXPECTED_LAYER,
            "item_url": ITEM_API_URL, "owner_url": OWNER_API_URL,
            "declared_crs": "EPSG:3857", "native_field_schema": fields,
            "geometry_census_sha256": GEOMETRY_SHA256, "attribute_census_sha256": ATTRIBUTE_SHA256,
            "stable_population_witnesses": ["count_before", "ids_before", "count_after", "ids_after", "tail"],
            "raw_capture_roster": raw_roster,
            "source_feature_presence_path": "metadata/source_feature_presence.json",
            "source_feature_presence_sha256": _sha(presence_bytes),
            "authority_status": "American Red Cross publishes this layer itself through its NHQRedCrossGIS publisher account per the repository source audit; rights and formal acceptance remain pending.",
            "effective_release_status": "unresolved FY26/January2026 reversion notice versus FY27/July2026 claims",
            "historical_baseline": "retired PASDA HIFLD mirror input never used by this adapter; no historical mapping or migration applied",
            "crosswalk_status": "unverified; DHS crosswalk provider returned HTTP 403",
            "invalid_geometry_ids": invalid_geometry_ids},
        "promotion_allowed": False,
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Native typed candidate preserves the observed nine source attributes and EPSG:3857 polygon geometry without reprojection or repair.",
            "objects": [{"path": f"geoparquet/{FILE_SLUG}.parquet", "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "OID width, Double precision and shape-field-name mapping require reviewed decisions.",
            "shapefile_zip": "Long original attribute names exceed DBF limits; no destructive field mapping approved.",
            "pmtiles": "Reprojection or quantization would alter native geometry; no visualization mapping approved."}}
    validate_provenance(manifest)
    return manifest


def _invalid_geometry_ids(rows: list[dict], geometries: list) -> list[int]:
    return [row["attributes"]["OBJECTID"] for row, geom in zip(rows, geometries) if not geom.is_valid]


def capture_and_stage(staging: StagingStorageResource, version: str, capture_directory: str | None = None) -> dict:
    if not staging.use_local or staging.bucket:
        raise ValueError("red_cross_divisions_draft_requires_isolated_local_staging")
    if not version.startswith("v") or "/" in version or ".." in version:
        raise ValueError("red_cross_divisions_immutable_version_required")
    if capture_directory:
        captures = _load_capture(Path(capture_directory))
    else:
        destination = Path(staging.local_dir).resolve() / staging.prefix / "quarantine" / DATASET_SLUG / version / "capture"
        if destination.exists():
            raise ValueError("red_cross_divisions_capture_directory_already_exists")
        capture_live(destination)
        captures = _load_capture(destination)
    layer, rows, ids, geometries = _validate_population(captures)
    fields = _schema(layer)
    invalid_ids = _invalid_geometry_ids(rows, geometries)
    parquet, geo = _geoparquet(fields, rows, geometries)
    presence = [{"OBJECTID": row["attributes"]["OBJECTID"],
                 "attribute_members": list(row["attributes"]),
                 "geometry_member_present": "geometry" in row,
                 "ring_count": len(row["geometry"]["rings"])} for row in rows]
    presence_bytes = _CANONICAL({"schema_version": 1, "rows": presence}) + b"\n"
    prefix = staging.build_target_location(DATASET_SLUG, FILE_SLUG, version, "").rstrip("/")
    if staging.list_prefix(prefix):
        raise ValueError("red_cross_divisions_version_prefix_already_exists")
    records = {}
    for name in CAPTURE_NAMES:
        record = captures[name]
        staging.write(DATASET_SLUG, FILE_SLUG, version, f"raw_arcgis/{name}.body", record["body"])
        staging.write(DATASET_SLUG, FILE_SLUG, version, f"raw_arcgis/{name}.receipt.json", record["receipt_body"])
        records[name] = record
    staging.write(DATASET_SLUG, FILE_SLUG, version, f"geoparquet/{FILE_SLUG}.parquet", parquet)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/source_feature_presence.json", presence_bytes)
    manifest = _manifest(captures, layer, fields, parquet, geo, records, presence_bytes, invalid_ids)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/source_manifest.json", _CANONICAL(manifest) + b"\n")
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": len(rows), "source_population_complete": True,
        "source_id_sha256": _sha(_CANONICAL(ids)), "source_schema_sha256": _sha(_CANONICAL(fields)),
        "source_geometry_census_sha256": GEOMETRY_SHA256, "source_attribute_census_sha256": ATTRIBUTE_SHA256,
        "native_geoparquet_sha256": _sha(parquet), "native_crs": "EPSG:3857",
        "source_invalid_geometry_ids": invalid_ids,
        "observed_capture_retrieved_at": min(captures[name]["retrieved_at"] for name in CAPTURE_NAMES),
        "source_authority_accepted": False, "source_release_accepted": False,
        "baseline_accepted": False, "contract_accepted": False, "rights_accepted": False,
        "promotion_allowed": False}
