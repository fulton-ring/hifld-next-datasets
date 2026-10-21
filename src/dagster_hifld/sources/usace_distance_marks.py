"""Bounded, source-faithful capture of the official USACE IENC distance marks."""
from __future__ import annotations

import hashlib
import json
import struct
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.sources.epa_frs_common import processing_identity

DATASET_SLUG = "distance-marks-usace-ienc-2"
FILE_SLUG = DATASET_SLUG
LAYER_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/DISTANCE_MARK_POINT/MapServer/10"
EXPECTED_COUNT = 7453
PAGE_SIZE = 200
FIELDS = [
    ("OBJECTID", "esriFieldTypeOID"),
    ("Category_of_Distance_Mark", "esriFieldTypeString"),
    ("Horizontal_Units", "esriFieldTypeString"),
    ("Waterway_Distance", "esriFieldTypeDouble"),
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
    retained = getattr(client, "retained_capture", None)
    if retained is not None:
        body, wire, receipt = retained.read_response(name, params)
        budget[0] += len(body) + len(wire)
        if budget[0] > 128_000_000:
            raise ValueError("source_total_capture_budget_exceeded")
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}.json", body)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}.wire", wire)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}.receipt.json", _json_bytes(receipt))
        parsed = json.loads(body)
        if not isinstance(parsed, dict) or "error" in parsed:
            raise ValueError(f"invalid_arcgis_response:{name}")
        return parsed
    url = LAYER_URL if name == "layer" else LAYER_URL + "/query"
    for attempt in range(3):
        raw_parts: list[bytes] = []
        wire_parts: list[bytes] = []
        try:
            with client.stream("GET", url, params=params, headers={"Accept-Encoding": "identity"}) as response:
                capture = capture_http_response(
                    response, max_wire_bytes=8_000_000, max_decoded_bytes=8_000_000,
                    reserve=lambda progress: _reserve(budget, progress.wire_bytes_retained + progress.decoded_bytes_retained + progress.reserve_bytes),
                    retain_wire=wire_parts.append, retain_decoded=raw_parts.append,
                )
        except Exception as exc:
            raise ValueError("source_transport_error:" + type(exc).__name__) from exc
        body = b"".join(raw_parts)
        wire = b"".join(wire_parts)
        status_ok = capture.complete and 200 <= capture.status_code < 300
        # Preserve unsuccessful responses under attempt-specific names before retrying.
        artifact_name = name if status_ok else f"{name}.attempt_{attempt + 1}"
        receipt = {
            "method": "GET", "url": str(response.request.url), "status": capture.status_code,
            "attempt": attempt + 1, "observed_at": datetime.now(timezone.utc).isoformat(), "bytes": len(body),
            "sha256": _digest(body), "wire_bytes": len(wire), "wire_sha256": _digest(wire),
            "content_type": response.headers.get("content-type"), "content_encoding": capture.content_encoding,
            "complete": capture.complete, "error_code": capture.error_code,
        }
        budget[0] += len(body) + len(wire)
        if budget[0] > 128_000_000:
            raise ValueError("source_total_capture_budget_exceeded")
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}.json", body)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}.wire", wire)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}.receipt.json", _json_bytes(receipt))
        if status_ok:
            parsed = json.loads(body)
            if not isinstance(parsed, dict) or "error" in parsed:
                raise ValueError(f"invalid_arcgis_response:{name}")
            return parsed
        if capture.status_code in {429, 500, 502, 503, 504} and attempt < 2:
            time.sleep(0.25 * (attempt + 1))
            continue
        raise ValueError("incomplete_source_response:" + name)
    raise ValueError("source_retry_limit_exceeded:" + name)


def _reserve(budget: list[int], upcoming: int) -> None:
    if budget[0] + upcoming > 128_000_000:
        raise ValueError("source_total_capture_budget_exceeded")


def _metadata(client: httpx.Client, staging, version_key: str, budget: list[int]) -> dict:
    value = _request(client, {"f": "json"}, staging, version_key, "layer", budget)
    if value.get("id") != 10 or value.get("name") != "DISTANCE_MARK_POINT" or value.get("geometryType") != "esriGeometryPoint":
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


def _validate_crs(ref: object) -> None:
    if not isinstance(ref, dict):
        raise ValueError("source_crs_drift")
    codes = [ref[key] for key in ("wkid", "latestWkid") if key in ref]
    if not codes or any(type(code) is not int or code != 4326 for code in codes):
        raise ValueError("source_crs_drift")
    for key in ("wkt", "wkt2"):
        if key in ref:
            try:
                same_crs = pyproj.CRS.from_wkt(ref[key]).equals(pyproj.CRS.from_epsg(4326), ignore_axis_order=True)
            except Exception as exc:
                raise ValueError("source_crs_drift") from exc
            if not same_crs:
                raise ValueError("source_crs_drift")


def _validate_dimensions(declaration: dict) -> None:
    for name in ("hasZ", "hasM"):
        if name in declaration and (type(declaration[name]) is not bool or declaration[name]):
            raise ValueError("source_dimension_drift")


def _id_snapshot(client, staging, version_key: str, name: str, budget: list[int]) -> list[int]:
    result = _request(client, {"f": "json", "where": "1=1", "returnIdsOnly": "true"}, staging, version_key, name, budget)
    if result.get("objectIdFieldName") != "OBJECTID":
        raise ValueError("source_object_id_field_drift")
    ids = result.get("objectIds")
    if not isinstance(ids, list) or len(ids) != EXPECTED_COUNT or any(type(x) is not int for x in ids):
        raise ValueError("source_id_population_drift")
    if len(set(ids)) != len(ids):
        raise ValueError("source_duplicate_object_id")
    return sorted(ids)


def _count_snapshot(client, staging, version_key: str, name: str, budget: list[int]) -> int:
    result = _request(client, {"f": "json", "where": "1=1", "returnCountOnly": "true"}, staging, version_key, name, budget)
    count = result.get("count")
    if type(count) is not int or count != EXPECTED_COUNT:
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
        if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"} and any(
            x is not None and (type(x) not in (int, float) or not float("-inf") < float(x) < float("inf"))
            for x in values
        ):
            raise ValueError("source_floating_attribute_type_drift:" + name)
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


class _RetainedCaptureClient:
    """Read immutable successful responses from an earlier retained capture."""

    def __init__(self, staging, source_version: str):
        self.staging = staging
        self.source_version = source_version
        self.retained_capture = self

    def read_response(self, name: str, params: dict[str, object]):
        root = self.staging.build_target_location(DATASET_SLUG, FILE_SLUG, self.source_version, "")
        body_key = root + f"raw/{name}.json"
        wire_key = root + f"raw/{name}.wire"
        receipt_key = root + f"raw/{name}.receipt.json"
        try:
            body, wire = self.staging.read_key(body_key), self.staging.read_key(wire_key)
            receipt = json.loads(self.staging.read_key(receipt_key))
        except (OSError, ValueError) as exc:
            raise ValueError("retained_source_response_missing:" + name) from exc
        if (receipt.get("method") != "GET" or receipt.get("complete") is not True
                or type(receipt.get("status")) is not int or not 200 <= receipt["status"] < 300
                or receipt.get("bytes") != len(body) or receipt.get("wire_bytes") != len(wire)
                or receipt.get("sha256") != _digest(body) or receipt.get("wire_sha256") != _digest(wire)):
            raise ValueError("retained_source_receipt_mismatch:" + name)
        parsed_url = urlsplit(receipt.get("url", ""))
        if parsed_url.scheme != "https" or parsed_url.netloc != "ienccloud.us" or parsed_url.path != urlsplit(LAYER_URL if name == "layer" else LAYER_URL + "/query").path:
            raise ValueError("retained_source_request_identity_mismatch:" + name)
        query = parse_qs(parsed_url.query, keep_blank_values=True)
        for key, value in params.items():
            expected = str(value).lower() if isinstance(value, bool) else str(value)
            if query.get(key) != [expected]:
                raise ValueError("retained_source_request_parameters_mismatch:" + name)
        return body, wire, receipt


def _write_source_manifest(staging, version_key: str, metadata: dict, identity: dict,
                           source_record_sha256: str, raw_source_version: str | None) -> dict:
    prefix = staging.build_target_location(DATASET_SLUG, FILE_SLUG, version_key, "")
    inputs = []
    retry_receipts = []
    for key in staging.list_keys(DATASET_SLUG, FILE_SLUG, version_key):
        marker = "/raw/"
        if marker not in key or not key.endswith(".receipt.json"):
            continue
        name = key.split(marker, 1)[1][:-len(".receipt.json")]
        receipt = json.loads(staging.read_key(key))
        if ".attempt_" in name:
            retry_receipts.append({"artifact": f"raw/{name}.receipt.json", "status": receipt["status"],
                                   "sha256": receipt["sha256"], "wire_sha256": receipt["wire_sha256"]})
            continue
        if receipt.get("complete") is not True or not 200 <= receipt.get("status", 0) < 300:
            raise ValueError("manifest_contains_failed_source_input")
        request_url = receipt["url"]
        input_id = f"{name}:{receipt['sha256']}"
        inputs.append({
            "input_id": input_id, "producer": "USACE IENC (service metadata claim; authority acceptance pending)",
            "host": urlsplit(request_url).hostname, "role": "producer_publication",
            "canonical_url": LAYER_URL, "retrieval_url": request_url,
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"], "raw_path": f"raw/{name}.json",
            "license": "Unresolved; rights review pending.",
            "attribution": "USACE IENC (service copyright metadata; rights review pending).",
            "release_date": None,
            "release_date_unknown_reason": "The observed service layer does not identify an immutable producer release date.",
        })
    if raw_source_version:
        source_root = staging.build_target_location(DATASET_SLUG, FILE_SLUG, raw_source_version, "")
        for key in staging.list_keys(DATASET_SLUG, FILE_SLUG, raw_source_version):
            if "/raw/" not in key or ".attempt_" not in key or not key.endswith(".receipt.json"):
                continue
            receipt = json.loads(staging.read_key(key))
            retry_receipts.append({"artifact": key[len(source_root):], "status": receipt["status"],
                                   "sha256": receipt["sha256"], "wire_sha256": receipt["wire_sha256"]})
    if not inputs:
        raise ValueError("source_manifest_inputs_missing")
    manifest = {
        "schema_version": 1, "processor": "HIFLD Next", "approved": False,
        "title": "Distance Marks (USACE IENC) — unaccepted native snapshot draft",
        "description": "Complete observed DISTANCE_MARK_POINT service layer candidate. IENC chart-service coverage is not a nationwide distance-mark inventory. Source edition, HIFLD continuity, rights, schema/ID contract, baseline, and release decisions remain pending.",
        "agency": "US Army Corps of Engineers (IENC service claim; authority acceptance pending)",
        "publisher": "USACE IENC (service metadata claim)", "source_url": LAYER_URL,
        "documentation_url": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson",
        "license": "Rights review pending.", "date_issued": None, "date_modified": None,
        "code_commit": identity["code_commit"], "code_identity_scheme": identity["code_identity_scheme"],
        "processing_code": identity, "inputs": inputs,
        "transformations": [{"operation": "Complete ESRI JSON distance-mark point capture to typed GeoParquet evidence candidate",
                             "parameters": {"attributes": "All declared nongeometry fields and values retained in source order; source declared Waterway_Distance double represented as Arrow float64.",
                                            "identifiers": "OBJECTID retained exactly; Feature_ID retained as an original attribute; no synthetic identifier.",
                                            "geometry": "Source XY preserved as float64 coordinates and point WKB in EPSG:4326; no reprojection or repair.",
                                            "candidate_status": "Unaccepted draft; no output or release format approval implied."}}],
        "lineage": {"scope": "Complete observed DISTANCE_MARK_POINT layer only; no national or HIFLD archive coverage equivalence asserted.",
                    "layer_id": 10, "source_description": metadata.get("description"),
                    "source_copyright_text": metadata.get("copyrightText"),
                    "source_release_date_known": False, "source_record_sha256": source_record_sha256,
                    "raw_capture_mode": "retained_response_replay" if raw_source_version else "live_http_capture",
                    "retained_source_capture_version": raw_source_version,
                    "failed_retry_responses": retry_receipts,
                    "source_contract_accepted": False, "schema_contract_accepted": False,
                    "id_contract_accepted": False, "coverage_accepted": False,
                    "rights_accepted": False, "baseline_accepted": False, "release_accepted": False},
        "unsupported_output_formats": {
            "shapefile_zip": "No approved field-name, type, or geometry conversion mapping exists.",
            "geopackage": "No approved container-schema or release mapping exists.",
            "pmtiles": "No approved reprojection, quantization, or visualization mapping exists.",
        },
    }
    validate_provenance(manifest)
    staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/source_manifest.json", _json_bytes(manifest))
    return manifest


def _capture(staging, version_key: str, client: httpx.Client | None = None,
             raw_source_version: str | None = None) -> dict:
    """Fetch, validate, and stage one complete native point population."""
    owned = client is None
    client = client or httpx.Client(timeout=60, follow_redirects=False)
    try:
        budget = [0]
        metadata = _metadata(client, staging, version_key, budget)
        count_before = _count_snapshot(client, staging, version_key, "count_before", budget)
        before = _id_snapshot(client, staging, version_key, "ids_before", budget)
        if len(before) != EXPECTED_COUNT:
            raise ValueError("source_count_drift")
        features = []
        pages = []
        for offset in range(0, len(before), PAGE_SIZE):
            requested = before[offset:offset + PAGE_SIZE]
            result = _request(client, {
                "f": "json", "objectIds": ",".join(map(str, requested)), "outFields": "*",
                "returnGeometry": "true", "outSR": "4326",
            }, staging, version_key, f"page_{offset:07d}", budget)
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
            features.extend(page)
            pages.append({"offset": offset, "requested_ids": requested, "returned_ids": page_ids, "count": len(page)})
        after = _id_snapshot(client, staging, version_key, "ids_after", budget)
        count_after = _count_snapshot(client, staging, version_key, "count_after", budget)
        fetched_ids = [feature["attributes"]["OBJECTID"] for feature in features]
        if count_before != count_after or before != after or fetched_ids != before or len(features) != EXPECTED_COUNT:
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
        processor_identity = processing_identity(Path(__file__).parents[3])
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/native_capture.json", _json_bytes({
            "source_layer": LAYER_URL,
            "service_directory": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson",
            "service_layer_id": 10,
            "service_description": metadata.get("description"),
            "source_copyright_text": metadata.get("copyrightText"),
            "source_release_date": None,
            "source_release_date_known": False,
            "feature_count": len(features),
            "count_before": count_before, "count_after": count_after,
            "before_id_sha256": ids_digest, "after_id_sha256": _digest(_json_bytes(after)),
            "source_record_sha256": row_digest, "parquet_sha256": _digest(parquet), "geojson_sha256": _digest(geojson),
            "native_crs": "EPSG:4326", "field_schema": SOURCE_SCHEMA, "attribute_fields": FIELDS, "pages": pages,
            "code_identity": processor_identity, "coverage_statement": "Complete observed DISTANCE_MARK_POINT layer in the official IENC chart service; nationwide coverage is not established.",
            "capture_mode": "retained_response_replay" if raw_source_version else "live_http_capture",
            "retained_source_capture_version": raw_source_version,
            "refresh_semantics": "Default runs capture a fresh source snapshot. An explicit retained_response_replay is an evidence-only reprocessing operation, not a cache or currentness claim.",
            "historical_hifld_continuity_accepted": False,
            "source_contract_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "coverage_accepted": False,
            "rights_accepted": False,
            "release_accepted": False,
        }))
        _write_source_manifest(staging, version_key, metadata, processor_identity, row_digest, raw_source_version)
        return {"feature_count": len(features), "ids_sha256": ids_digest, "records_sha256": row_digest,
                "parquet_sha256": _digest(parquet), "release_accepted": False}
    finally:
        if owned:
            client.close()


def capture(staging, version_key: str, client: httpx.Client | None = None,
            retained_source_version: str | None = None) -> dict:
    """Retain failed attempts as quarantine evidence before propagating failure."""
    try:
        if retained_source_version is not None:
            if client is not None:
                raise ValueError("retained_replay_does_not_accept_http_client")
            client = _RetainedCaptureClient(staging, retained_source_version)
        return _capture(staging, version_key, client, retained_source_version)
    except Exception as exc:
        error = str(exc).split(":", 1)[0]
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/quarantine.json", _json_bytes({
            "status": "quarantined", "source_layer": LAYER_URL,
            "observed_at": datetime.now(timezone.utc).isoformat(), "failure_code": error,
            "capture_module_sha256": _digest(Path(__file__).read_bytes()),
            "release_accepted": False,
        }))
        raise
