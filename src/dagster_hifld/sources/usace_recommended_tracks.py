"""Bounded native capture of USACE IENC recommended track lines.

The BTS item and IENC service are preserved as evidence, but item succession,
rights, release freshness, and HIFLD continuity remain maintainer decisions.
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
from shapely.geometry import LineString, MultiLineString
from shapely import to_wkb

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    capture_get,
    content_derived_version,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
    processing_identity,
)

DATASET_SLUG = "recommended-tracks-usace-ienc"
FILE_SLUG = DATASET_SLUG
SERVICE_DIRECTORY_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=json"
LAYER_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/RECOMMENDED_TRACK_LINE/MapServer/46"
LAYER_QUERY_URL = LAYER_URL + "/query"
BTS_ITEM_ID = "7ec34864c8804468a9cf52d437fe4b6c"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
PRIOR_BTS_ITEM_ID = "3607d485a9854d6fa458bf507058ceae"
PRIOR_BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{PRIOR_BTS_ITEM_ID}?f=json"
EXPECTED_COUNT = 666
EXPECTED_ID_SHA256 = "400349bf6a0c8f418cb845c1d18a9a50d6d70344d6627e7cc54406575f3b6823"
PAGE_SIZE = 200
MAX_CAPTURE_BYTES = 128_000_000
EXPECTED_EXTENT = {
    "xmin": -96.66790889999999,
    "ymin": 29.723524000000054,
    "xmax": -79.47402739999995,
    "ymax": 45.06155370000005,
}
EXPECTED_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    {"name": "Category_of_Recommended_Track", "type": "esriFieldTypeString", "alias": "Category_of_Recommended_Track", "domain": None, "length": 254},
    {"name": "Traffic_Flow", "type": "esriFieldTypeString", "alias": "Traffic_Flow", "domain": None, "length": 254},
    {"name": "Information", "type": "esriFieldTypeString", "alias": "Information", "domain": None, "length": 254},
    {"name": "Scale_Minimum", "type": "esriFieldTypeInteger", "alias": "Scale_Minimum", "domain": None},
    {"name": "Source_Date", "type": "esriFieldTypeString", "alias": "Source_Date", "domain": None, "length": 254},
    {"name": "Source_Indication", "type": "esriFieldTypeString", "alias": "Source_Indication", "domain": None, "length": 254},
    {"name": "Source_Dataset", "type": "esriFieldTypeString", "alias": "Source_Dataset", "domain": None, "length": 254},
    {"name": "Feature_ID", "type": "esriFieldTypeString", "alias": "Feature_ID", "domain": None, "length": 254},
]
ATTRIBUTE_FIELDS = [(field["name"], field["type"]) for field in EXPECTED_DECLARATIONS
                    if field["type"] != "esriFieldTypeGeometry"]
_STRING_TYPES = {"esriFieldTypeString"}


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json(value: object) -> bytes:
    return _canonical(value) + b"\n"


def _url(base: str, params: dict[str, object] | None = None) -> str:
    if not params:
        return base
    return base + "?" + urlencode(params)


def _read_capture(root: Path, name: str, url: str,
                  captures: dict[str, tuple[bytes, dict[str, object]]], budget: list[int],
                  client: httpx.Client, *, allow_error: bool = False) -> dict:
    try:
        body, receipt = capture_get(client, url, root, name, max_bytes=8_000_000)
    except Exception:
        receipt_path = root / f"{name}.receipt.json"
        body_path = root / f"{name}.body"
        if receipt_path.is_file() and body_path.is_file():
            try:
                captures[name] = (body_path.read_bytes(), json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                pass
        raise
    budget[0] += len(body) + int(receipt["wire_bytes"])
    if budget[0] > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_exceeded")
    captures[name] = (body, receipt)
    value = json.loads(body)
    if not isinstance(value, dict) or ("error" in value and not allow_error):
        raise ValueError("invalid_source_json:" + name)
    return value


def _validate_crs(value: object, reason: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(reason)
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            if type(value[key]) is not int or value[key] != 4326:
                raise ValueError(reason)
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            try:
                if not pyproj.CRS.from_wkt(value[key]).equals(
                        pyproj.CRS.from_epsg(4326), ignore_axis_order=True):
                    raise ValueError(reason)
            except Exception as exc:
                raise ValueError(reason) from exc
            found = True
    if not found:
        raise ValueError(reason)


def _validate_metadata(metadata: dict) -> None:
    if metadata.get("id") != 46 or metadata.get("name") != "RECOMMENDED_TRACK_LINE":
        raise ValueError("source_layer_identity_drift")
    if metadata.get("geometryType") != "esriGeometryPolyline":
        raise ValueError("source_geometry_type_drift")
    if type(metadata.get("maxRecordCount")) is not int or metadata["maxRecordCount"] < PAGE_SIZE:
        raise ValueError("source_transfer_limit_drift")
    if metadata.get("copyrightText") != "USACE IENC":
        raise ValueError("source_copyright_declaration_drift")
    for dim in ("hasZ", "hasM"):
        if dim in metadata and (type(metadata[dim]) is not bool or metadata[dim] is not False):
            raise ValueError("source_dimension_drift")
    declarations = metadata.get("fields")
    if not isinstance(declarations, list) or any(not isinstance(field, dict) for field in declarations):
        raise ValueError("source_schema_drift")
    actual = [{key: field.get(key) for key in ("name", "type", "alias", "domain", "length")
               if key in field or key != "length"} for field in declarations]
    if actual != EXPECTED_DECLARATIONS:
        raise ValueError("source_schema_drift")
    extent = metadata.get("extent")
    if not isinstance(extent, dict) or any(extent.get(k) != v for k, v in EXPECTED_EXTENT.items()):
        raise ValueError("source_layer_coverage_extent_drift")
    _validate_crs(metadata.get("spatialReference"), "source_layer_crs_drift")
    _validate_crs(extent.get("spatialReference"), "source_extent_crs_drift")


def _ids(value: dict, label: str) -> list[int]:
    if value.get("objectIdFieldName") != "OBJECTID":
        raise ValueError("source_object_id_field_drift:" + label)
    ids = value.get("objectIds")
    if not isinstance(ids, list) or any(type(item) is not int for item in ids):
        raise ValueError("source_id_roster_invalid:" + label)
    sorted_ids = sorted(ids)
    if (len(sorted_ids) != EXPECTED_COUNT or len(set(sorted_ids)) != EXPECTED_COUNT
            or _sha(_json(sorted_ids).rstrip(b"\n")) != EXPECTED_ID_SHA256):
        raise ValueError("source_id_population_drift:" + label)
    return sorted_ids


def _validate_feature(feature: object, expected_id: int, extent: dict) -> dict:
    if not isinstance(feature, dict):
        raise ValueError("source_feature_shape_invalid")
    attrs, geometry = feature.get("attributes"), feature.get("geometry")
    if not isinstance(attrs, dict) or set(attrs) != {name for name, _ in ATTRIBUTE_FIELDS}:
        raise ValueError("source_attribute_roster_drift")
    if type(attrs.get("OBJECTID")) is not int or attrs["OBJECTID"] != expected_id:
        raise ValueError("source_feature_id_or_order_drift")
    for name, kind in ATTRIBUTE_FIELDS:
        value = attrs[name]
        if kind in _STRING_TYPES and value is not None and not isinstance(value, str):
            raise ValueError("source_attribute_type_drift:" + name)
        if kind == "esriFieldTypeInteger" and value is not None and type(value) is not int:
            raise ValueError("source_attribute_type_drift:" + name)
    if not isinstance(geometry, dict) or "z" in geometry or "m" in geometry:
        raise ValueError("source_geometry_dimension_drift")
    if "spatialReference" in geometry:
        _validate_crs(geometry["spatialReference"], "source_geometry_crs_drift")
    paths = geometry.get("paths")
    if not isinstance(paths, list) or not paths:
        raise ValueError("source_polyline_paths_missing")
    for path in paths:
        if not isinstance(path, list) or len(path) < 2:
            raise ValueError("source_polyline_path_invalid")
        for coordinate in path:
            if (not isinstance(coordinate, list) or len(coordinate) != 2
                    or any(type(value) not in (int, float) or not math.isfinite(value)
                           for value in coordinate)):
                raise ValueError("source_coordinate_dimension_or_type_drift")
            x, y = coordinate
            if not (extent["xmin"] <= x <= extent["xmax"]
                    and extent["ymin"] <= y <= extent["ymax"]):
                raise ValueError("source_coordinate_outside_pinned_coverage")
    return {"attributes": attrs, "geometry": {"paths": paths}}


def _source_geometries(features: list[dict]) -> list[object]:
    geometries = []
    for feature in features:
        paths = feature["geometry"]["paths"]
        coords = [[(coordinate[0], coordinate[1]) for coordinate in path] for path in paths]
        geometries.append(LineString(coords[0]) if len(coords) == 1 else MultiLineString(coords))
    return geometries


def _make_parquet(features: list[dict]) -> bytes:
    arrays: dict[str, pa.Array] = {}
    for name, kind in ATTRIBUTE_FIELDS:
        values = [feature["attributes"][name] for feature in features]
        if kind == "esriFieldTypeOID":
            if any(type(value) is not int for value in values):
                raise ValueError("source_object_id_type_drift")
            arrow_type = pa.int64()
        elif kind == "esriFieldTypeInteger":
            arrow_type = pa.int32()
        elif kind in _STRING_TYPES:
            arrow_type = pa.string()
        else:
            raise ValueError("unsupported_source_field_type:" + kind)
        arrays[name] = pa.array(values, type=arrow_type)
    geometries = _source_geometries(features)
    arrays["geometry"] = pa.array([to_wkb(geometry, byte_order=1, output_dimension=2)
                                    for geometry in geometries], type=pa.binary())
    table = pa.table(arrays)
    metadata = dict(table.schema.metadata or {})
    metadata[b"geo"] = _json({
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB", "geometry_types": ["LineString", "MultiLineString"],
            "crs": pyproj.CRS.from_epsg(4326).to_json_dict(),
        }},
    }).strip()
    table = table.replace_schema_metadata(metadata)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", version="2.6")
    return sink.getvalue().to_pybytes()


def _make_geojson(features: list[dict]) -> bytes:
    collection = []
    for feature in features:
        paths = feature["geometry"]["paths"]
        if len(paths) == 1:
            geometry = {"type": "LineString", "coordinates": paths[0]}
        else:
            geometry = {"type": "MultiLineString", "coordinates": paths}
        collection.append({"type": "Feature", "id": feature["attributes"]["OBJECTID"],
                           "properties": feature["attributes"], "geometry": geometry})
    return _json({"type": "FeatureCollection", "features": collection})


def _object_descriptor(body: bytes) -> dict[str, object]:
    return {"bytes": len(body), "sha256": _sha(body)}


def _capture_manifest(root: Path, *, complete: bool, captures: dict[str, tuple[bytes, dict]],
                      error_code: str | None = None, count: int | None = None) -> None:
    roster = {}
    for path in sorted(root.iterdir()):
        if path.name == "capture_manifest.json":
            continue
        if path.is_symlink() or not path.is_file():
            raise ValueError("capture_artifact_not_regular")
        roster[path.name] = _sha(path.read_bytes())
    value = {"schema_version": 1, "complete": complete,
             "captured_at": datetime.now(timezone.utc).isoformat(),
             "source_layer": LAYER_URL, "expected_feature_count": EXPECTED_COUNT,
             "captured_request_count": len(captures), "receipts": {n: r for n, (_b, r) in sorted(captures.items())},
             "artifact_roster_sha256": _sha(_json(roster))}
    if count is not None:
        value["source_feature_count"] = count
    if error_code:
        value["error_code"] = error_code
    (root / "capture_manifest.json").write_bytes(_json(value))


def _persist_outputs(storage: StagingStorageResource, prefix: str,
                     outputs: dict[str, bytes]) -> dict[str, dict[str, object]]:
    descriptors = {}
    for name, body in outputs.items():
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        if (snapshot is None or snapshot.size != len(body) or storage.sha256_key(key) != _sha(body)
                or storage.object_snapshot(key) != snapshot):
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = _object_descriptor(body)
    return descriptors


def _assert_bts_item(item: dict) -> None:
    if (item.get("id") != BTS_ITEM_ID or item.get("owner") != "USDOT_BTS"
            or item.get("title") != "Inland Electronic Navigational Charts"
            or item.get("type") != "PDF" or item.get("access") != "public"):
        raise ValueError("bts_distribution_item_identity_drift")
    description = item.get("description")
    if not isinstance(description, str) or "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes" not in description:
        raise ValueError("bts_distribution_link_missing")


def capture(storage: StagingStorageResource, *, capture_directory: str | None = None,
            client: httpx.Client | None = None) -> dict[str, object]:
    """Acquire one complete source snapshot and write an immutable unapproved candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(path.is_symlink() for path in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 128_000_000:
        raise ValueError("disk_floor_before_acquisition")
    root = Path(capture_directory).absolute() if capture_directory else None
    if root is None:
        root = Path("/tmp/hifld-native-reconstruction/290/captures") / uuid4().hex
    if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    if root.exists() and any(root.iterdir()):
        raise ValueError("capture_directory_not_empty")
    root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    budget = [0]
    owned_client = client is None
    client = client or httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                                    headers={"User-Agent": "hifld-next-datasets/usace-ienc-draft"})
    try:
        bts_item = _read_capture(root, "bts_item", BTS_ITEM_URL, captures, budget, client)
        _assert_bts_item(bts_item)
        # The previously referenced item is retained for continuity review;
        # either a denial/error body or a changed item remains evidence, not authority.
        _read_capture(root, "prior_bts_item", PRIOR_BTS_ITEM_URL, captures, budget, client,
                      allow_error=True)
        directory = _read_capture(root, "service_directory", SERVICE_DIRECTORY_URL,
                                  captures, budget, client)
        services = directory.get("services")
        if not isinstance(services, list) or not any(
                row.get("name") == "IENC_Feature_Classes/RECOMMENDED_TRACK_LINE"
                and row.get("type") == "MapServer" for row in services if isinstance(row, dict)):
            raise ValueError("source_service_directory_identity_drift")
        metadata_before = _read_capture(root, "layer_before", _url(LAYER_URL, {"f": "json"}),
                                        captures, budget, client)
        _validate_metadata(metadata_before)
        count_before = _read_capture(root, "count_before", _url(LAYER_QUERY_URL, {
            "where": "1=1", "returnCountOnly": "true", "f": "json"}), captures, budget, client)
        ids_before_value = _read_capture(root, "ids_before", _url(LAYER_QUERY_URL, {
            "where": "1=1", "returnIdsOnly": "true", "f": "json"}), captures, budget, client)
        ids_before = _ids(ids_before_value, "before")
        if count_before.get("count") != EXPECTED_COUNT:
            raise ValueError("source_count_drift_before")
        features = []
        for offset in range(0, len(ids_before), PAGE_SIZE):
            requested = ids_before[offset:offset + PAGE_SIZE]
            name = f"page_{offset:06d}"
            page = _read_capture(root, name, _url(LAYER_QUERY_URL, {
                "objectIds": ",".join(map(str, requested)), "outFields": "*",
                "returnGeometry": "true", "outSR": "4326", "f": "json",
            }), captures, budget, client)
            if (page.get("geometryType") != "esriGeometryPolyline"
                    or page.get("exceededTransferLimit") is True
                    or not isinstance(page.get("features"), list)
                    or len(page["features"]) != len(requested)):
                raise ValueError("source_page_incomplete")
            for dim in ("hasZ", "hasM"):
                if dim in page and (type(page[dim]) is not bool or page[dim] is not False):
                    raise ValueError("source_page_dimension_drift")
            _validate_crs(page.get("spatialReference"), "source_page_crs_drift")
            page_fields = page.get("fields")
            expected_attributes = [{"name": name, "type": kind} for name, kind in ATTRIBUTE_FIELDS]
            if (not isinstance(page_fields, list)
                    or [{"name": f.get("name"), "type": f.get("type")} for f in page_fields]
                    != expected_attributes):
                raise ValueError("source_page_schema_drift")
            for feature, identity in zip(page["features"], requested, strict=True):
                features.append(_validate_feature(feature, identity, EXPECTED_EXTENT))
        ids_after_value = _read_capture(root, "ids_after", _url(LAYER_QUERY_URL, {
            "where": "1=1", "returnIdsOnly": "true", "f": "json"}), captures, budget, client)
        count_after = _read_capture(root, "count_after", _url(LAYER_QUERY_URL, {
            "where": "1=1", "returnCountOnly": "true", "f": "json"}), captures, budget, client)
        metadata_after = _read_capture(root, "layer_after", _url(LAYER_URL, {"f": "json"}),
                                       captures, budget, client)
        _validate_metadata(metadata_after)
        ids_after = _ids(ids_after_value, "after")
        if (ids_before != ids_after or count_after.get("count") != EXPECTED_COUNT
                or count_before != count_after or metadata_before != metadata_after
                or [f["attributes"]["OBJECTID"] for f in features] != ids_before):
            raise ValueError("source_population_or_metadata_changed_during_capture")
    except Exception as exc:
        try:
            _capture_manifest(root, complete=False, captures=captures, error_code=type(exc).__name__)
        finally:
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG,
                                              datetime.now(timezone.utc).strftime("v%Y%m%dT%H%M%SZ"),
                                              captures, type(exc).__name__)
        raise
    finally:
        if owned_client:
            client.close()

    row_objects = [{"attributes": feature["attributes"], "geometry": feature["geometry"]}
                   for feature in features]
    source_record_sha = _sha(_json(row_objects).rstrip(b"\n"))
    source_geometry_sha = _sha(_json([feature["geometry"] for feature in features]).rstrip(b"\n"))
    source_schema_sha = _sha(_json(EXPECTED_DECLARATIONS).rstrip(b"\n"))
    receipt_body_hashes = {name: {"body_sha256": receipt["sha256"],
                                  "wire_sha256": receipt["wire_sha256"]}
                           for name, (_body, receipt) in sorted(captures.items())}
    source_capture_sha = _sha(_json(receipt_body_hashes).rstrip(b"\n"))
    version_record_sha = _sha(_json({"source_record_sha256": source_record_sha,
                                     "source_capture_sha256": source_capture_sha}).rstrip(b"\n"))
    identity = processing_identity(Path(__file__).parents[3])
    version = "v1.0.0-" + content_derived_version(
        version_record_sha, source_geometry_sha, source_schema_sha, identity)[1:]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    parquet = _make_parquet(features)
    geojson = _make_geojson(features)
    observed_at = max(str(receipt["observed_at"]) for _body, receipt in captures.values())
    inputs = []
    for name, (_body, receipt) in sorted(captures.items()):
        if name == "prior_bts_item":
            continue
        inputs.append({
            "input_id": name, "producer": "USACE IENC" if name not in {"bts_item", "prior_bts_item"} else "USDOT BTS",
            "host": urlsplit(str(receipt["url"])).hostname, "role": "producer_publication",
            "canonical_url": receipt["url"], "retrieval_url": receipt["url"],
            "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
            "http_status": receipt["status"], "bytes": receipt["bytes"],
            "sha256": receipt["sha256"], "wire_bytes": receipt["wire_bytes"],
            "wire_sha256": receipt["wire_sha256"], "license": "Unresolved; rights review pending.",
            "attribution": "USACE IENC; rights review pending.", "release_date": None,
            "release_date_unknown_reason": "No release date was exposed for this extracted layer snapshot.",
        })
    source_manifest = {
        "schema_version": 1, "approved": False,
        "title": "Recommended Tracks (USACE IENC)",
        "description": "Native draft candidate for IENC RECOMMENDED_TRACK_LINE; coverage is limited to this chart feature class.",
        "publisher": "USACE IENC (service copyright declaration; publisher and continuity review pending)",
        "agency": "US Army Corps of Engineers (source service declaration)",
        "source_url": LAYER_URL, "license": None, "source_modified": None,
        "date_issued": None, "date_modified": None, "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "native_esri_polyline_to_geojson_and_geoparquet_candidate",
            "parameters": {"attributes": "all declared source attributes and IDs retained without mapping",
                "geometry": "source path coordinates retained at EPSG:4326; no reprojection, simplification, repair, or geocoding",
                "candidate_status": "unapproved; BTS item succession, release freshness, HIFLD continuity, schema/ID contract, coverage, rights, and release decisions remain pending"}}],
        "lineage": "The public BTS Inland Electronic Navigational Charts PDF item describes bi-monthly USACE/AGC updates and links the IENC Feature Classes service. A previously referenced BTS map-service item is retained as an unavailable/changed-item observation. BTS item succession, distribution authority, and continuity require maintainer review; no BTS item is treated as approval of this candidate.",
        "unsupported_output_formats": {"geopackage": "No approved schema/compatibility contract.",
                                       "pmtiles": "No display transformation approved."},
    }
    validate_release_provenance(source_manifest)
    validate_output_policy(source_manifest)
    id_list = [feature["attributes"]["OBJECTID"] for feature in features]
    audit = {
        "schema_version": 1, "source_layer_url": LAYER_URL, "source_layer_name": "RECOMMENDED_TRACK_LINE",
        "source_layer_id": 46, "service_directory_url": SERVICE_DIRECTORY_URL,
        "feature_count": len(features), "count_before": EXPECTED_COUNT, "count_after": EXPECTED_COUNT,
        "objectid_sha256": EXPECTED_ID_SHA256, "objectid_min": min(id_list), "objectid_max": max(id_list),
        "source_record_sha256": source_record_sha, "source_geometry_sha256": source_geometry_sha,
        "source_schema_sha256": source_schema_sha, "source_capture_sha256": source_capture_sha,
        "source_fields": EXPECTED_DECLARATIONS, "geometry_type": "esriGeometryPolyline",
        "native_crs": "EPSG:4326", "pinned_extent": EXPECTED_EXTENT,
        "source_scope": "IENC chart feature class only; nationwide coverage and HIFLD historical equivalence are not established.",
        "capture_request_count": len(captures), "capture_receipts": {n: r for n, (_b, r) in sorted(captures.items())},
        "capture_directory": str(root), "capture_completed_at": observed_at,
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "bts_item_succession_accepted": False, "mapping_approved": False,
        "release_accepted": False,
    }
    parquet_name = f"geoparquet/{FILE_SLUG}.parquet"
    manifest = {**source_manifest,
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Native line vertices and source fields retained in a typed unapproved candidate.",
            "objects": [{"path": parquet_name, "bytes": len(parquet), "sha256": _sha(parquet)}]}}}}
    validate_output_policy(manifest)
    ready = {
        "approved": False, "version": version,
        "objects": {parquet_name: _object_descriptor(parquet)},
        "source_population_complete": True, "source_record_sha256": source_record_sha,
        "source_id_sha256": EXPECTED_ID_SHA256, "source_schema_sha256": source_schema_sha,
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "bts_item_succession_accepted": False, "mapping_approved": False,
        "release_accepted": False,
    }
    _capture_manifest(root, complete=True, captures=captures, count=len(features))
    outputs = {
        f"geojson/{FILE_SLUG}.geojson": geojson,
        parquet_name: parquet,
        "metadata/source_manifest.json": _json(manifest),
        "metadata/source_audit.json": _json(audit),
        "metadata/processing_identity.json": _json(identity),
        "metadata/candidate_ready.json": _json(ready),
    }
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _json(receipt)
    state_fingerprint = _sha(_json(identity).rstrip(b"\n"))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        # Preserve first-construction receipts but require the live source bytes
        # to match the raw inputs bound to the candidate version.
        from dagster_hifld.construction_state import load_first_construction
        bundle = json.loads(load_first_construction(storage, state_key, version, state_fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("capture_receipts"), dict):
            raise ValueError("cached_candidate_original_receipts_missing")
        original_receipts = payload["capture_receipts"]
        if set(original_receipts) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name, (body, _receipt) in captures.items():
            if storage.read_key(f"{prefix}/raw/{name}.body") != body:
                raise ValueError("cached_source_bytes_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix,
            version, state_fingerprint, identity, source_record_sha, source_schema_sha,
            payload.get("objects", {}), original_receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": len(features), "source_population_complete": True,
                "source_id_sha256": EXPECTED_ID_SHA256, "source_record_sha256": source_record_sha,
                "source_schema_sha256": source_schema_sha, "candidate_ready": prefix + "/metadata/candidate_ready.json",
                "reused": True, "release_accepted": False, "capture_directory": str(root)}

    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    descriptors = _persist_outputs(storage, prefix, outputs)
    seal_ready_candidate(storage, state_key, version, state_fingerprint, {
        "processing_identity": identity, "source_record_sha256": source_record_sha,
        "source_schema_sha256": source_schema_sha,
        "capture_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "objects": descriptors,
    })
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": len(features), "source_population_complete": True,
            "source_id_sha256": EXPECTED_ID_SHA256, "source_record_sha256": source_record_sha,
            "source_schema_sha256": source_schema_sha, "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "reused": False, "release_accepted": False, "capture_directory": str(root)}
