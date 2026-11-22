"""Bounded, source-faithful capture of the official USACE IENC pylon points."""
from __future__ import annotations

import hashlib
import json
import platform
import struct
from datetime import datetime, timezone
from importlib.metadata import version as package_version
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely import wkb
from dagster_hifld.sources.arcgis_raw_capture import (
    RawCapturePolicy, request_raw, capture_exact_id_population, population_snapshot,
)

from dagster_hifld.sources.arcgis_layer import validate_crs as _validate_crs, validate_dimensions as _validate_dimensions

DATASET_SLUG = "pylon-locations-usace-ienc"
FILE_SLUG = DATASET_SLUG
LAYER_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/PYLONS_POINT/MapServer/21"
PAGE_SIZE = 200
FIELDS = [
    ("OBJECTID", "esriFieldTypeOID"),
    ("Category_of_Pylon", "esriFieldTypeString"),
    ("Water_Level", "esriFieldTypeString"),
    ("Condition", "esriFieldTypeString"),
    ("Information", "esriFieldTypeString"),
    ("Scale_Minimum", "esriFieldTypeInteger"),
    ("Source_Date", "esriFieldTypeString"),
    ("Source_Indication", "esriFieldTypeString"),
    ("Source_Dataset", "esriFieldTypeString"),
    ("Feature_ID", "esriFieldTypeString"),
]
SOURCE_SCHEMA = [("OBJECTID", "esriFieldTypeOID"), ("SHAPE", "esriFieldTypeGeometry"), *FIELDS[1:]]
_STRING = {"esriFieldTypeString", "esriFieldTypeDate", "esriFieldTypeGUID", "esriFieldTypeGlobalID"}


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _request(client: httpx.Client, params: dict[str, object], staging, version_key: str, name: str, budget: list[int]) -> dict:
    retained = None
    return request_raw(client, LAYER_URL if name == "layer" else LAYER_URL + "/query", params, name, budget,
        write=lambda relative, data: staging.write(DATASET_SLUG, FILE_SLUG, version_key, relative, data),
        encode=_json_bytes, retained=retained.read_response if retained is not None else None,
        policy=RawCapturePolicy(8_000_000, 128_000_000, attempts=1))


def _metadata(client: httpx.Client, staging, version_key: str, budget: list[int]) -> dict:
    value = _request(client, {"f": "json"}, staging, version_key, "layer", budget)
    if value.get("id") != 21 or value.get("name") != "PYLONS_POINT" or value.get("geometryType") != "esriGeometryPoint":
        raise ValueError("source_layer_identity_drift")
    if value.get("maxRecordCount", 0) < 1:
        raise ValueError("source_transfer_limit_invalid")
    refs = [value.get("spatialReference")]
    if isinstance(value.get("extent"), dict):
        refs.append(value["extent"].get("spatialReference"))
    for ref in refs:
        _validate_crs(ref)
    _validate_dimensions(value)
    fields = value.get("fields")
    expected = [{"name": name, "type": kind} for name, kind in SOURCE_SCHEMA]
    actual = [{"name": field.get("name"), "type": field.get("type")} for field in fields or []]
    if actual != expected:
        raise ValueError("source_schema_drift")
    return value


def _id_snapshot(client, staging, version_key: str, name: str, budget: list[int]) -> list[int]:
    result = _request(client, {"f": "json", "where": "1=1", "returnIdsOnly": "true"}, staging, version_key, name, budget)
    if result.get("objectIdFieldName") != "OBJECTID":
        raise ValueError("source_object_id_field_drift")
    ids = result.get("objectIds")
    if (not isinstance(ids, list) or not ids
            or any(type(x) is not int or x <= 0 for x in ids)):
        raise ValueError("source_id_population_drift")
    if len(set(ids)) != len(ids):
        raise ValueError("source_duplicate_object_id")
    return sorted(ids)


def _count_snapshot(client, staging, version_key: str, name: str, budget: list[int]) -> int:
    result = _request(client, {"f": "json", "where": "1=1", "returnCountOnly": "true"}, staging, version_key, name, budget)
    count = result.get("count")
    if type(count) is not int or count < 0:
        raise ValueError("source_count_drift")
    return count


def _arrow_type(field_type: str):
    if field_type in _STRING:
        return pa.string()
    if field_type in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"}:
        return pa.int64()
    if field_type in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
        return pa.float64()
    raise ValueError("unsupported_source_field_type:" + field_type)


def _make_parquet(metadata: dict, features: list[dict]) -> bytes:
    fields = [field for field in metadata["fields"] if field["type"] != "esriFieldTypeGeometry"]
    arrays = {}
    for field in fields:
        name, kind = field["name"], field["type"]
        values = [feature["attributes"].get(name) for feature in features]
        if name == "OBJECTID" and any(type(x) is not int for x in values):
            raise ValueError("source_object_id_value_drift")
        if kind in {"esriFieldTypeInteger", "esriFieldTypeSmallInteger"} and any(x is not None and type(x) is not int for x in values):
            raise ValueError("source_integer_attribute_type_drift:" + name)
        if kind in _STRING and any(x is not None and not isinstance(x, str) for x in values):
            raise ValueError("source_string_attribute_type_drift:" + name)
        arrays[name] = pa.array(values, type=_arrow_type(kind))
    xs, ys, wkb = [], [], []
    for feature in features:
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict) or type(geometry.get("x")) not in (int, float) or type(geometry.get("y")) not in (int, float):
            raise ValueError("source_point_geometry_invalid")
        x, y = geometry["x"], geometry["y"]
        if not (-180 <= x <= 180 and -90 <= y <= 90):
            raise ValueError("source_point_out_of_epsg4326_bounds")
        xs.append(x)
        ys.append(y)
        wkb.append(struct.pack("<BIdd", 1, 1, float(x), float(y)))
    arrays["native_x"] = pa.array(xs, type=pa.float64())
    arrays["native_y"] = pa.array(ys, type=pa.float64())
    arrays["geometry"] = pa.array(wkb, type=pa.binary())
    table = pa.table(arrays)
    metadata_bytes = dict(table.schema.metadata or {})
    metadata_bytes[b"geo"] = _json_bytes({
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": pyproj.CRS.from_epsg(4326).to_json_dict()}},
    }).strip()
    table = table.replace_schema_metadata(metadata_bytes)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", version="2.6")
    return sink.getvalue().to_pybytes()


def compare_generated_geoparquet(staging, version_key: str) -> dict[str, object]:
    """Require the generic typed GeoParquet candidate to preserve every native row."""
    prefix = staging.build_target_location(DATASET_SLUG, FILE_SLUG, version_key, "")
    native_key = prefix + f"metadata/native_candidate/{FILE_SLUG}.parquet"
    candidate = pq.read_table(pa.BufferReader(staging.read_key(native_key)))
    generated_keys = [key for key in staging.list_keys(DATASET_SLUG, FILE_SLUG, version_key)
                      if key.startswith(prefix + "geoparquet/") and key.endswith(".parquet")]
    if not generated_keys:
        raise ValueError("native_parity_generated_geoparquet_missing")
    output_tables = [pq.read_table(pa.BufferReader(staging.read_key(key))) for key in generated_keys]
    output = pa.concat_tables(output_tables, promote_options="permissive") if len(output_tables) > 1 else output_tables[0]
    source_ids = candidate.column("OBJECTID").to_pylist()
    output_ids = output.column("OBJECTID").to_pylist() if "OBJECTID" in output.column_names else None
    if output_ids is None or len(source_ids) != len(output_ids):
        raise ValueError("native_parity_feature_count_or_id_missing")
    if sorted(source_ids) != sorted(output_ids) or len(set(output_ids)) != len(output_ids):
        raise ValueError("native_parity_object_id_roster_drift")
    source_by_id = {identity: index for index, identity in enumerate(source_ids)}
    output_by_id = {identity: index for index, identity in enumerate(output_ids)}
    attribute_names = [name for name, _ in FIELDS]
    for name, kind in FIELDS:
        if name not in output.column_names or candidate.schema.field(name).type != output.schema.field(name).type:
            raise ValueError("native_parity_attribute_schema_drift:" + name)
        left, right = candidate.column(name).to_pylist(), output.column(name).to_pylist()
        if any(left[source_by_id[identity]] != right[output_by_id[identity]] for identity in source_by_id):
            raise ValueError("native_parity_attribute_value_drift:" + name)
    if "geometry" not in output.column_names:
        raise ValueError("native_parity_geometry_missing")
    xs, ys = candidate.column("native_x").to_pylist(), candidate.column("native_y").to_pylist()
    geometries = output.column("geometry").to_pylist()
    for identity, source_index in source_by_id.items():
        point = wkb.loads(geometries[output_by_id[identity]])
        if point.geom_type != "Point" or point.x != xs[source_index] or point.y != ys[source_index]:
            raise ValueError("native_parity_exact_xy_drift")
    return {"feature_count": len(source_ids), "attribute_fields": attribute_names,
            "id_sha256": _digest(_json_bytes(sorted(source_ids))),
            "candidate_sha256": _digest(staging.read_key(native_key)),
            "geoparquet_sha256": [_digest(staging.read_key(key)) for key in generated_keys],
            "exact_xy_and_attributes": True}


def _capture(staging, version_key: str, client: httpx.Client | None = None) -> dict:
    """Fetch, validate, and stage one complete native point population."""
    owned = client is None
    client = client or httpx.Client(timeout=60, follow_redirects=False)
    try:
        budget = [0]
        metadata = _metadata(client, staging, version_key, budget)
        count_before, before = population_snapshot(
            lambda: _count_snapshot(client, staging, version_key, "count_before", budget),
            lambda: _id_snapshot(client, staging, version_key, "ids_before", budget))
        if count_before != len(before):
            raise ValueError("source_count_or_id_roster_drift")
        def fetch_page(offset, requested):
            result = _request(client, {
                "f": "json", "objectIds": ",".join(map(str, requested)), "outFields": "*",
                "returnGeometry": "true", "outSR": "4326",
            }, staging, version_key, f"page_{offset:07d}", budget)
            return result

        def validate_page(offset, requested, result):
            page = result.get("features")
            if not isinstance(page, list) or len(page) > metadata["maxRecordCount"] or result.get("exceededTransferLimit") is True:
                raise ValueError(f"source_incomplete_page:{offset}")
            if result.get("geometryType") != "esriGeometryPoint":
                raise ValueError("source_page_geometry_type_drift")
            _validate_dimensions(result)
            if not isinstance(result.get("fields"), list):
                raise ValueError("source_page_schema_drift")
            actual_fields = [{"name": field.get("name"), "type": field.get("type")} for field in result["fields"]]
            if actual_fields != [{"name": name, "type": kind} for name, kind in FIELDS]:
                raise ValueError("source_page_schema_drift")
            _validate_crs(result.get("spatialReference"))
            page_ids = [f.get("attributes", {}).get("OBJECTID") for f in page]
            if page_ids != requested:
                raise ValueError(f"source_page_id_or_order_drift:{offset}")
            for feature in page:
                attributes = feature.get("attributes")
                if not isinstance(attributes, dict) or set(attributes) != {name for name, _ in FIELDS}:
                    raise ValueError("source_page_attribute_roster_drift")
                geometry = feature.get("geometry")
                if not isinstance(geometry, dict):
                    raise ValueError("source_page_crs_drift")
                if "z" in geometry or "m" in geometry:
                    raise ValueError("source_dimension_drift")
                if "spatialReference" in geometry:
                    _validate_crs(geometry["spatialReference"])
            return page

        def close_witnesses():
            return population_snapshot(
                lambda: _count_snapshot(client, staging, version_key, "count_after", budget),
                lambda: _id_snapshot(client, staging, version_key, "ids_after", budget), ids_first=True)

        features, pages, (count_after, after) = capture_exact_id_population(
            before, PAGE_SIZE, fetch_page=fetch_page, validate_page=validate_page,
            close_witnesses=close_witnesses)
        fetched_ids = [feature["attributes"]["OBJECTID"] for feature in features]
        if count_before != count_after or before != after or fetched_ids != before or len(features) != len(before):
            raise ValueError("source_population_changed_during_capture")
        rows = [{"attributes": f["attributes"], "geometry": f["geometry"]} for f in features]
        row_digest = _digest(_json_bytes(rows))
        ids_digest = _digest(_json_bytes(before))
        parquet = _make_parquet(metadata, features)
        geojson = _json_bytes({"type": "FeatureCollection", "features": [
            {"type": "Feature", "id": f["attributes"]["OBJECTID"], "properties": f["attributes"],
             "geometry": {"type": "Point", "coordinates": [f["geometry"]["x"], f["geometry"]["y"]]}}
            for f in features
        ]})
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"geojson/{FILE_SLUG}.geojson", geojson)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"metadata/native_candidate/{FILE_SLUG}.parquet", parquet)
        processor_identity = {"package": "hifld-next-datasets", "version": package_version("hifld-next-datasets"),
                              "capture_module_sha256": _digest(Path(__file__).read_bytes()),
                              "runtime_package_source_sha256": _packaged_code_sha256(),
                              "python": platform.python_version(), "runtime": "Dagster packaged runtime"}
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/native_capture.json", _json_bytes({
            "source_layer": LAYER_URL, "service_layer_id": 21, "feature_count": len(features),
            "count_before": count_before, "count_after": count_after,
            "before_id_sha256": ids_digest, "after_id_sha256": _digest(_json_bytes(after)),
            "source_record_sha256": row_digest, "parquet_sha256": _digest(parquet), "geojson_sha256": _digest(geojson),
            "native_crs": "EPSG:4326", "field_schema": SOURCE_SCHEMA, "attribute_fields": FIELDS, "pages": pages,
            "code_identity": processor_identity, "coverage_statement": "Official IENC chart service only; nationwide coverage is not established.",
            "refresh_semantics": "Each run captures a fresh source snapshot; retained captures are evidence and are not replayed as a cache.",
            "release_accepted": False,
        }))
        return {"feature_count": len(features), "ids_sha256": ids_digest, "records_sha256": row_digest,
                "parquet_sha256": _digest(parquet), "release_accepted": False}
    finally:
        if owned:
            client.close()


def _packaged_code_sha256() -> str:
    root = Path(__file__).parents[1]
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        body = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big") + relative)
        digest.update(len(body).to_bytes(8, "big") + body)
    return digest.hexdigest()


def capture(staging, version_key: str, client: httpx.Client | None = None) -> dict:
    """Retain failed attempts as quarantine evidence before propagating failure."""
    try:
        return _capture(staging, version_key, client)
    except Exception as exc:
        error = str(exc).split(":", 1)[0]
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/quarantine.json", _json_bytes({
            "status": "quarantined", "source_layer": LAYER_URL,
            "observed_at": datetime.now(timezone.utc).isoformat(), "failure_code": error,
            "capture_module_sha256": _digest(Path(__file__).read_bytes()),
            "release_accepted": False,
        }))
        raise
