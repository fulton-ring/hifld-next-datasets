"""Source-faithful, candidate-only capture of the Red Cross HQ ArcGIS layer.

The source account's authority and active edition are unresolved. This adapter
preserves the complete observed service population and records those limits; it
does not map the historical product, fill the missing Saipan geometry, or
approve a release.
"""
from __future__ import annotations

import hashlib
import json
import math
import struct
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj

from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.resources import StagingStorageResource

DATASET_SLUG = FILE_SLUG = "american-red-cross-headquarters"
SERVICE_URL = "https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Master_ARC_Geography_2022/FeatureServer/0"
ITEM_URL = "https://www.arcgis.com/home/item.html?id=8c6753c3c5814d05be65723cd4615d7c"
EXPECTED_LAYER = "Master_RC_Geo_HQ_2027"
EXPECTED_COUNT = 226
EXPECTED_MISSING_GEOMETRY_ID = 226
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_TOTAL_BYTES = 32 * 1024 * 1024

CAPTURE_NAMES = (
    "layer_before", "count_before", "ids_before", "page_0000", "page_0100",
    "page_0200", "count_after", "ids_after", "layer_after",
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
                raise ValueError("red_cross_hq_duplicate_json_member")
            result[key] = value
        return result
    return json.loads(body.decode("utf-8"), object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError("red_cross_hq_nonfinite_json")))


def _validate_receipt(name: str, body: bytes, receipt: dict) -> dict:
    if not isinstance(receipt, dict) or receipt.get("status") != 200:
        raise ValueError("red_cross_hq_http_status_drift:" + name)
    digest = receipt.get("sha256")
    size = receipt.get("bytes", receipt.get("wire_bytes"))
    if not receipt.get("complete_body", receipt.get("complete", False)):
        raise ValueError("red_cross_hq_incomplete_receipt:" + name)
    if digest != _sha(body) or type(size) is not int or size != len(body):
        raise ValueError("red_cross_hq_receipt_body_mismatch:" + name)
    observed = receipt.get("fetched_at", receipt.get("observed_at"))
    if not isinstance(observed, str):
        raise ValueError("red_cross_hq_missing_original_retrieval_time:" + name)
    try:
        stamp = datetime.fromisoformat(observed.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("red_cross_hq_invalid_original_retrieval_time:" + name) from exc
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("red_cross_hq_naive_original_retrieval_time:" + name)
    request_url = receipt.get("url")
    final_url = receipt.get("final_url", request_url)
    if not isinstance(request_url, str) or not isinstance(final_url, str):
        raise ValueError("red_cross_hq_receipt_url_missing:" + name)
    expected_path = urlsplit(SERVICE_URL).path if name.startswith("layer_") else urlsplit(SERVICE_URL).path + "/query"
    for url in (request_url, final_url):
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != "services.arcgis.com" or parsed.path != expected_path:
            raise ValueError("red_cross_hq_receipt_endpoint_drift:" + name)
    return {"body": body, "receipt": receipt, "retrieved_at": observed,
            "sha256": digest, "bytes": size}


def _load_capture(capture_dir: Path) -> dict[str, dict]:
    if capture_dir.is_symlink() or not capture_dir.is_dir():
        raise ValueError("red_cross_hq_capture_directory_invalid")
    actual = {entry.name for entry in capture_dir.iterdir()}
    if actual != CAPTURE_FILES or any((capture_dir / name).is_symlink() or not (capture_dir / name).is_file() for name in actual):
        raise ValueError("red_cross_hq_capture_roster_mismatch")
    captures = {}
    total = 0
    for name in CAPTURE_NAMES:
        body = (capture_dir / f"{name}.body").read_bytes()
        receipt_body = (capture_dir / f"{name}.receipt.json").read_bytes()
        receipt = _strict_json(receipt_body)
        if len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("red_cross_hq_response_byte_limit:" + name)
        total += len(body)
        captures[name] = {**_validate_receipt(name, body, receipt), "receipt_body": receipt_body}
    if total > MAX_TOTAL_BYTES:
        raise ValueError("red_cross_hq_total_response_byte_limit")
    return captures


def _request(client: httpx.Client, name: str, params: dict[str, str]) -> tuple[bytes, dict]:
    url = SERVICE_URL if name.startswith("layer_") else SERVICE_URL + "/query"
    raw: list[bytes] = []
    wire: list[bytes] = []
    with client.stream("GET", url, params=params, headers={"Accept-Encoding": "identity"}) as response:
        captured = capture_http_response(response, max_wire_bytes=MAX_RESPONSE_BYTES,
            max_decoded_bytes=MAX_RESPONSE_BYTES, reserve=lambda progress: None,
            retain_wire=wire.append, retain_decoded=raw.append)
        receipt = {"url": str(response.request.url), "final_url": str(response.url),
            "fetched_at": datetime.now(timezone.utc).isoformat(), "status": captured.status_code,
            "bytes": captured.decoded_bytes_retained, "sha256": captured.decoded_sha256,
            "wire_bytes": captured.wire_bytes_retained, "wire_sha256": captured.wire_sha256,
            "complete_body": captured.complete, "content_encoding": captured.content_encoding,
            "content_type": response.headers.get("content-type"), "error_code": captured.error_code}
    body = b"".join(raw)
    if not captured.complete or captured.status_code != 200 or len(body) > MAX_RESPONSE_BYTES:
        raise ValueError("red_cross_hq_live_response_incomplete:" + name)
    return body, receipt


def capture_live(capture_dir: Path) -> None:
    """Capture two metadata and count/ID witnesses around a bounded page read."""
    capture_dir.mkdir(parents=True, exist_ok=False)
    total = 0
    def save(name: str, body: bytes, receipt: dict):
        nonlocal total
        if len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("red_cross_hq_response_byte_limit:" + name)
        total += len(body)
        if total > MAX_TOTAL_BYTES:
            raise ValueError("red_cross_hq_total_response_byte_limit")
        (capture_dir / f"{name}.body").write_bytes(body)
        (capture_dir / f"{name}.receipt.json").write_bytes(_CANONICAL(receipt) + b"\n")
    with httpx.Client(timeout=httpx.Timeout(45, connect=15), follow_redirects=False) as client:
        for name, params in (("layer_before", {"f": "pjson"}),
                             ("count_before", {"f": "json", "where": "1=1", "returnCountOnly": "true"}),
                             ("ids_before", {"f": "json", "where": "1=1", "returnIdsOnly": "true"})):
            save(name, *_request(client, name, params))
        ids = _ids(_strict_json((capture_dir / "ids_before.body").read_bytes()))
        if len(ids) != EXPECTED_COUNT:
            raise ValueError("red_cross_hq_observed_id_population_changed")
        for offset in range(0, len(ids), 100):
            chunk = ids[offset:offset + 100]
            name = f"page_{offset:04d}"
            params = {"f": "json", "objectIds": ",".join(map(str, chunk)), "outFields": "*",
                "returnGeometry": "true", "returnZ": "true", "returnM": "true",
                "returnTrueCurves": "true", "orderByFields": "OBJECTID ASC"}
            save(name, *_request(client, name, params))
        for name, params in (("count_after", {"f": "json", "where": "1=1", "returnCountOnly": "true"}),
                             ("ids_after", {"f": "json", "where": "1=1", "returnIdsOnly": "true"}),
                             ("layer_after", {"f": "pjson"})):
            save(name, *_request(client, name, params))
    _load_capture(capture_dir)


def _ids(payload: dict) -> list[int]:
    if payload.get("objectIdFieldName") != "OBJECTID" or not isinstance(payload.get("objectIds"), list):
        raise ValueError("red_cross_hq_id_witness_malformed")
    ids = payload["objectIds"]
    if any(type(identity) is not int for identity in ids) or len(set(ids)) != len(ids):
        raise ValueError("red_cross_hq_id_roster_invalid")
    return sorted(ids)


def _validate_request_scope(name: str, receipt: dict, ids: list[int]) -> None:
    url = receipt.get("url")
    parsed = urlsplit(url)
    params = {key: values for key, values in parse_qs(parsed.query, keep_blank_values=True).items()}
    if name.startswith("layer_"):
        expected = {"f": ["pjson"]}
    elif name.startswith("count_"):
        expected = {"f": ["json"], "where": ["1=1"], "returnCountOnly": ["true"]}
    elif name.startswith("ids_"):
        expected = {"f": ["json"], "where": ["1=1"], "returnIdsOnly": ["true"]}
    else:
        offset = int(name.removeprefix("page_"))
        page_ids = ids[offset:offset + 100]
        if not page_ids:
            raise ValueError("red_cross_hq_unexpected_page_receipt")
        expected = {"f": ["json"], "objectIds": [",".join(map(str, page_ids))],
            "outFields": ["*"], "returnGeometry": ["true"], "returnZ": ["true"],
            "returnM": ["true"], "returnTrueCurves": ["true"],
            "orderByFields": ["OBJECTID ASC"]}
    if params != expected:
        raise ValueError("red_cross_hq_request_scope_drift:" + name)


def _schema(layer: dict) -> list[dict]:
    if (layer.get("id") != 0 or layer.get("name") != EXPECTED_LAYER
            or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("objectIdField") != "OBJECTID"):
        raise ValueError("red_cross_hq_layer_identity_drift")
    if layer.get("hasZ") is not False or layer.get("hasM") is not False:
        raise ValueError("red_cross_hq_dimension_declaration_drift")
    sr = layer.get("spatialReference")
    if not isinstance(sr, dict) or sr.get("latestWkid") != 3857 or sr.get("wkid") not in {102100, 3857}:
        raise ValueError("red_cross_hq_crs_declaration_drift")
    fields = layer.get("fields")
    if not isinstance(fields, list) or len(fields) != 19:
        raise ValueError("red_cross_hq_field_roster_drift")
    if fields[0].get("name") != "OBJECTID" or fields[0].get("type") != "esriFieldTypeOID":
        raise ValueError("red_cross_hq_identifier_schema_drift")
    for field in fields:
        if not isinstance(field.get("name"), str) or not isinstance(field.get("type"), str):
            raise ValueError("red_cross_hq_field_declaration_malformed")
        if field["type"] not in (_STRING_TYPES | {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger", "esriFieldTypeDouble", "esriFieldTypeSingle"}):
            raise ValueError("red_cross_hq_unsupported_field_type:" + field["name"])
    if [field["name"] for field in fields] != [
        "OBJECTID", "ECODE", "Chapter", "Region_Type", "RCODE", "Region", "DCODE", "Division",
        "Chapter_Address", "Chapter_Address_2", "Chapter_City", "Chapter_State", "Chapter_Zip",
        "Chapter_Phone", "Time_Zone", "FEMA_Region", "Status", "Score", "Match_addr"]:
        raise ValueError("red_cross_hq_field_roster_drift")
    return fields


def _arrow_type(kind: str):
    if kind in _STRING_TYPES:
        return pa.string()
    if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"}:
        return pa.int64()
    if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
        return pa.float64()
    raise ValueError("red_cross_hq_unsupported_arrow_field_type")


def _validate_population(captures: dict[str, dict]) -> tuple[dict, list[dict], list[int]]:
    layer_before = _strict_json(captures["layer_before"]["body"])
    layer_after = _strict_json(captures["layer_after"]["body"])
    fields = _schema(layer_before)
    if _CANONICAL(layer_before) != _CANONICAL(layer_after):
        raise ValueError("red_cross_hq_layer_metadata_changed_during_capture")
    count_before = _strict_json(captures["count_before"]["body"]).get("count")
    count_after = _strict_json(captures["count_after"]["body"]).get("count")
    ids_before = _ids(_strict_json(captures["ids_before"]["body"]))
    ids_after = _ids(_strict_json(captures["ids_after"]["body"]))
    if (type(count_before) is not int or type(count_after) is not int
            or count_before != EXPECTED_COUNT or count_after != EXPECTED_COUNT
            or len(ids_before) != EXPECTED_COUNT or ids_before != ids_after):
        raise ValueError("red_cross_hq_population_changed_during_capture")
    for name in CAPTURE_NAMES:
        _validate_request_scope(name, captures[name]["receipt"], ids_before)
    observed = []
    for name in ("page_0000", "page_0100", "page_0200"):
        body = _strict_json(captures[name]["body"])
        features = body.get("features") if isinstance(body, dict) else None
        if not isinstance(features, list) or body.get("exceededTransferLimit") is True:
            raise ValueError("red_cross_hq_feature_page_incomplete:" + name)
        observed.extend(features)
    observed_ids = [feature.get("attributes", {}).get("OBJECTID") for feature in observed]
    if (len(observed) != EXPECTED_COUNT or observed_ids != ids_before
            or any(type(value) is not int for value in observed_ids)):
        raise ValueError("red_cross_hq_feature_page_id_roster_drift")
    names = [field["name"] for field in fields]
    missing_geometry = []
    for feature in observed:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict) or list(attrs) != names:
            raise ValueError("red_cross_hq_attribute_roster_or_order_drift")
        for field in fields:
            value = attrs[field["name"]]
            kind = field["type"]
            if value is None:
                if field.get("nullable") is False:
                    raise ValueError("red_cross_hq_nonnullable_attribute_null:" + field["name"])
                continue
            if kind in _STRING_TYPES and not isinstance(value, str):
                raise ValueError("red_cross_hq_attribute_type_drift:" + field["name"])
            if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"} and type(value) is not int:
                raise ValueError("red_cross_hq_attribute_type_drift:" + field["name"])
            if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"} and (type(value) not in (int, float) or not math.isfinite(value)):
                raise ValueError("red_cross_hq_attribute_type_drift:" + field["name"])
        geometry = feature.get("geometry")
        if geometry is None:
            missing_geometry.append(attrs["OBJECTID"])
        elif (not isinstance(geometry, dict) or set(geometry) != {"x", "y"}
              or type(geometry["x"]) not in (int, float) or type(geometry["y"]) not in (int, float)
              or not math.isfinite(geometry["x"]) or not math.isfinite(geometry["y"])):
            raise ValueError("red_cross_hq_native_point_geometry_drift")
    if missing_geometry != [EXPECTED_MISSING_GEOMETRY_ID]:
        raise ValueError("red_cross_hq_missing_geometry_roster_changed")
    missing_attrs = observed[-1]["attributes"]
    if missing_attrs.get("ECODE") != "55004" or missing_attrs.get("Status") != "U":
        raise ValueError("red_cross_hq_missing_geometry_identity_changed")
    return layer_before, observed, ids_before


def _geoparquet(fields: list[dict], features: list[dict]) -> tuple[bytes, dict]:
    arrays = []
    schema_fields = []
    for field in fields:
        name, kind = field["name"], field["type"]
        dtype = _arrow_type(kind)
        values = [feature["attributes"][name] for feature in features]
        if name == "OBJECTID" and any(type(value) is not int for value in values):
            raise ValueError("red_cross_hq_objectid_type_drift")
        if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
            values = [None if value is None else float(value) for value in values]
        arrays.append(pa.array(values, type=dtype, safe=True))
        schema_fields.append(pa.field(name, dtype, nullable=field.get("nullable") is not False))
    geometry_values = []
    coordinates = []
    for feature in features:
        geometry = feature.get("geometry")
        if geometry is None:
            geometry_values.append(None)
        else:
            x, y = float(geometry["x"]), float(geometry["y"])
            coordinates.append((x, y))
            geometry_values.append(struct.pack("<BIdd", 1, 1, x, y))
    arrays.append(pa.array(geometry_values, type=pa.binary(), safe=True))
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    bbox = [min(x for x, _ in coordinates), min(y for _, y in coordinates),
            max(x for x, _ in coordinates), max(y for _, y in coordinates)]
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"],
               "crs": pyproj.CRS.from_epsg(3857).to_json_dict(), "bbox": bbox}}}
    schema = pa.schema(schema_fields, metadata={b"geo": _CANONICAL(geo)})
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), sink, compression="zstd", use_dictionary=False)
    body = sink.getvalue().to_pybytes()
    table = pq.read_table(pa.BufferReader(body))
    if table.num_rows != EXPECTED_COUNT or table.schema.names != [field["name"] for field in fields] + ["geometry"]:
        raise ValueError("red_cross_hq_geoparquet_readback_schema_drift")
    if table.column("OBJECTID").to_pylist() != [feature["attributes"]["OBJECTID"] for feature in features]:
        raise ValueError("red_cross_hq_geoparquet_id_parity_failure")
    if table.column("geometry").null_count != 1:
        raise ValueError("red_cross_hq_geoparquet_null_geometry_parity_failure")
    if any(table.column(field["name"]).to_pylist() != [feature["attributes"][field["name"]] for feature in features]
           for field in fields):
        raise ValueError("red_cross_hq_geoparquet_attribute_parity_failure")
    return body, geo


def _manifest(captures: dict[str, dict], layer: dict, fields: list[dict], parquet: bytes, geo: dict,
              input_records: dict[str, dict], presence_bytes: bytes) -> dict:
    package_root = Path(__file__).resolve().parents[1]
    identity_files = [Path(__file__), package_root / "assets" / "red_cross_headquarters.py",
                      package_root / "http_capture.py", package_root / "native_geoparquet_catalog.py",
                      package_root / "source_manifest.py", package_root / "provenance.py"]
    code_digest = hashlib.sha256()
    for path in identity_files:
        name = path.relative_to(package_root).as_posix().encode("utf-8")
        body = path.read_bytes()
        code_digest.update(len(name).to_bytes(8, "big") + name)
        code_digest.update(len(body).to_bytes(8, "big") + body)
    code_sha = code_digest.hexdigest()
    inputs = []
    for name in CAPTURE_NAMES:
        record = input_records[name]
        receipt = record["receipt"]
        request_url = receipt.get("url")
        observed_url = receipt.get("final_url", request_url)
        inputs.append({"input_id": f"ARC-HQ:{name}",
            "producer": "American Red Cross (publisher account claim; not independently confirmed)",
            "host": urlsplit(observed_url).hostname, "role": "producer_publication",
            "canonical_url": SERVICE_URL, "request_url": request_url, "retrieval_url": observed_url,
            "retrieved_at": record["retrieved_at"], "sha256": record["sha256"], "bytes": record["bytes"],
            "raw_path": f"raw_arcgis/{name}.body",
            "receipt_path": f"raw_arcgis/{name}.receipt.json",
            "receipt_sha256": _sha(record["receipt_body"]), "receipt_bytes": len(record["receipt_body"]),
            "http_status": 200, "license": "Publisher claims no constraints; rights are not independently verified.",
            "attribution": "Publisher account claims Red Cross Disaster Data and Systems Operations; authority is unresolved.",
            "release_date_unknown_reason": "Layer title claims FY27 while the active service notice says temporarily reverted to FY26; effective source edition is unresolved."})
    raw_roster = []
    for name in CAPTURE_NAMES:
        raw_roster.extend([
            {"name": f"raw_arcgis/{name}.body", "bytes": captures[name]["bytes"], "sha256": captures[name]["sha256"]},
            {"name": f"raw_arcgis/{name}.receipt.json", "bytes": len(captures[name]["receipt_body"]),
             "sha256": _sha(captures[name]["receipt_body"])},
        ])
    raw_roster.append({"name": "metadata/source_feature_presence.json", "bytes": len(presence_bytes),
                       "sha256": _sha(presence_bytes)})
    manifest = {"title": "American Red Cross Headquarters", "description":
        "Complete observed 226-record ArcGIS HQ layer candidate. Source authority, edition, baseline compatibility and release remain unapproved.",
        "publisher": "American Red Cross (source account claim; unverified)", "source_url": SERVICE_URL,
        "agency": "American Red Cross (claimed producer; exact publication delegation unverified)",
        "schema_version": 1, "processor": "HIFLD Next", "code_commit": f"installed-processing-sha256:{code_sha}",
        "code_identity_scheme": "installed_source_bundle_sha256", "processing_code": {"installed_pipeline_sha256": code_sha,
            "identity_files": [path.relative_to(package_root).as_posix() for path in identity_files],
            "identity_scope": "Installed source adapter, asset wrapper, capture helper, native catalog, manifest and provenance code bytes; this fingerprint is not a Git commit."},
        "inputs": inputs,
        "transformations": [{"name": "ArcGIS FeatureServer JSON to typed GeoParquet", "parameters": {
            "attributes": "identity names and typed values from declared layer fields; nulls preserved",
            "geometry": "x/y coordinates preserved as native EPSG:3857 Point WKB; no reprojection",
            "missing_geometry": "OBJECTID 226 is encoded as one null candidate geometry and its original absent geometry-member state is retained in a row-keyed sidecar; not geocoded, copied from history, or dropped",
            "row_order": "ascending OBJECTID as returned by complete page sequence"}, "acceptance": "unapproved draft"}],
        "lineage": {"candidate_scope": "complete observed service layer only; no national or HIFLD coverage equivalence asserted",
            "item_id": "8c6753c3c5814d05be65723cd4615d7c", "publisher_account": "NHQRedCrossGIS",
            "organization_id": "pGfbNJoYypmNq86F", "layer_name": EXPECTED_LAYER,
            "declared_crs": "EPSG:3857", "native_field_schema": fields,
            "stable_population_witnesses": ["count_before", "ids_before", "count_after", "ids_after"],
            "raw_capture_roster": raw_roster, "source_feature_presence_path": "metadata/source_feature_presence.json",
            "source_feature_presence_sha256": _sha(presence_bytes), "complete_wire_bytes": False,
            "wire_bytes_limit": "The retained 2026-10-19 evidence preserves full decoded response bodies and source receipts, but did not retain separate HTTP wire bodies.",
            "authority_status": "unresolved", "effective_release_status": "unresolved FY26/FY27 notice conflict",
            "historical_baseline": "unapproved; source IDs/geometries/schema differ; no mapping or migration applied",
            "crosswalk_status": "unverified; provider returned HTTP 403"},
        "promotion_allowed": False,
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Native typed candidate preserves the observed source attributes and EPSG:3857 geometry without reprojection.",
            "objects": [{"path": f"geoparquet/{FILE_SLUG}.parquet", "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No format or field-schema acceptance has been granted.",
            "shapefile_zip": "No field-name/type conversion has been reviewed.", "pmtiles": "No tiling or release decision has been accepted."}}
    validate_provenance(manifest)
    return manifest


def capture_and_stage(staging: StagingStorageResource, version: str, capture_directory: str | None = None) -> dict:
    if not staging.use_local or staging.bucket:
        raise ValueError("red_cross_hq_draft_requires_isolated_local_staging")
    if not version.startswith("v") or "/" in version or ".." in version:
        raise ValueError("red_cross_hq_immutable_version_required")
    if capture_directory:
        captures = _load_capture(Path(capture_directory))
    else:
        destination = Path(staging.local_dir).resolve() / staging.prefix / "quarantine" / DATASET_SLUG / version / "capture"
        if destination.exists():
            raise ValueError("red_cross_hq_capture_directory_already_exists")
        capture_live(destination)
        captures = _load_capture(destination)
    layer, features, ids = _validate_population(captures)
    fields = _schema(layer)
    parquet, geo = _geoparquet(fields, features)
    presence = [{"OBJECTID": feature["attributes"]["OBJECTID"],
                 "attribute_members": list(feature["attributes"]),
                 "geometry_member_present": "geometry" in feature}
                for name in ("page_0000", "page_0100", "page_0200")
                for feature in _strict_json(captures[name]["body"])["features"]]
    presence_bytes = _CANONICAL({"schema_version": 1, "rows": presence}) + b"\n"
    prefix = staging.build_target_location(DATASET_SLUG, FILE_SLUG, version, "").rstrip("/")
    if staging.list_prefix(prefix):
        raise ValueError("red_cross_hq_version_prefix_already_exists")
    records = {}
    for name in CAPTURE_NAMES:
        record = captures[name]
        body, receipt = record["body"], record["receipt"]
        staging.write(DATASET_SLUG, FILE_SLUG, version, f"raw_arcgis/{name}.body", body)
        staging.write(DATASET_SLUG, FILE_SLUG, version, f"raw_arcgis/{name}.receipt.json", record["receipt_body"])
        records[name] = record
    staging.write(DATASET_SLUG, FILE_SLUG, version, f"geoparquet/{FILE_SLUG}.parquet", parquet)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/source_feature_presence.json", presence_bytes)
    manifest = _manifest(captures, layer, fields, parquet, geo, records, presence_bytes)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/source_manifest.json", _CANONICAL(manifest) + b"\n")
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": len(features), "source_population_complete": True,
        "source_id_sha256": _sha(_CANONICAL(ids)), "source_schema_sha256": _sha(_CANONICAL(fields)),
        "native_geoparquet_sha256": _sha(parquet), "native_crs": "EPSG:3857",
        "native_null_geometry_count": 1, "observed_capture_retrieved_at": min(captures[name]["retrieved_at"] for name in CAPTURE_NAMES),
        "source_authority_accepted": False, "source_release_accepted": False,
        "baseline_accepted": False, "contract_accepted": False, "rights_accepted": False,
        "promotion_allowed": False}
