"""Draft extractor for USACE Interim Risk Management property boundary layer.

The USACE hosted FeatureServer remains live, while its own description dates the
compiled FUDS content to FY24. This module captures native Esri JSON and
stages only an unapproved candidate; it does not establish currentness or permit
publication.
"""
from __future__ import annotations

import hashlib
import base64
import json
import math
import platform
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import MultiPolygon, Polygon

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file

SERVICE_ITEM_ID = "df5f8412d041469692ff4846c7131a56"
SERVICE_URL = "https://geospatial.sec.usace.army.mil/server/rest/services/Military/FUDS_Data/MapServer"
LAYER_ID = 8
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "IRM Property Boundary"
DATASET_SLUG = "interim-risk-management-irm-property-boundary"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Army Corps of Engineers (USACE), Military/FUDS_Data MapServer"
SOURCE_VINTAGE = "unknown"
SOURCE_VINTAGE_NOTE = "The exact source release date for the IRM Property Boundary layer is unknown. The ArcGIS service item modified timestamp is not a source release date."
EXPECTED_SCHEMA_SHA256 = ""
EXPECTED_COUNT = 601
EXPECTED_ID_SHA256 = "3d5284da651fdf9442cba775fcf226141d2c4e81c385e1304720173754d1b25a"
EXPECTED_RECORD_SHA256 = ""
EXPECTED_GEOMETRY_SHA256 = ""
PAGE_SIZE = 100
MAX_BODY_BYTES = 32_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _fetch(client: httpx.Client, url: str, directory: Path, name: str, *, max_bytes: int = MAX_BODY_BYTES) -> tuple[bytes, dict[str, object]]:
    return capture_get(client, url, directory, name, max_bytes=max_bytes)


def _feature_urls(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
        ("ids_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})),
    ]
    for offset in range(0, len(ids), PAGE_SIZE):
        params = {
            "objectIds": ",".join(str(x) for x in ids[offset:offset + PAGE_SIZE]),
            "outFields": "*", "returnGeometry": "true", "returnZ": "true", "returnM": "true",
            "outSR": "4326", "orderByFields": "OBJECTID ASC", "f": "json",
        }
        urls.append((f"page_{offset // PAGE_SIZE:03d}", LAYER_URL + "/query?" + urlencode(params)))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
        ("ids_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})),
    ])
    return urls


def _profile_schema(layer: dict[str, object]) -> tuple[list[dict[str, object]], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise ValueError("source_field_roster_drift")
    plain = [_plain_field(x) for x in fields]
    return plain, _sha(_canonical(plain))


def _ring_signed_area(ring: list[list[object]]) -> float:
    return sum(float(ring[i][0]) * float(ring[i + 1][1]) - float(ring[i + 1][0]) * float(ring[i][1])
               for i in range(len(ring) - 1)) / 2.0


def _source_polygon_geometry(rings: list[list[list[object]]]):
    """Decode Esri clockwise shells/counter-clockwise holes without repair."""
    shells: list[list[list[object]]] = []
    holes: list[list[list[object]]] = []
    for ring in rings:
        if any(not isinstance(point, list) or len(point) != 2 for point in ring):
            raise ValueError("source_xy_coordinate_shape_drift")
        area = _ring_signed_area(ring)
        if area < 0:
            shells.append(ring)
        elif area > 0:
            holes.append(ring)
        else:
            # Zero-area rings cannot encode shell/hole orientation. Retain them
            # as shells for a deterministic invalid candidate and report them.
            shells.append(ring)
    shell_polygons = [Polygon([[*coordinate[:3]] for coordinate in ring]) for ring in shells]
    holes_by_shell: list[list[list[list[object]]]] = [[] for _ in shells]
    for ring in holes:
        polygon = Polygon([[*coordinate[:3]] for coordinate in ring])
        containing = [i for i, shell in enumerate(shell_polygons) if shell.covers(polygon.representative_point())]
        if not containing:
            raise ValueError("source_orphan_interior_ring")
        owner = max(containing, key=lambda i: shell_polygons[i].area)
        holes_by_shell[owner].append([[*coordinate[:3]] for coordinate in ring])
    polygons = [Polygon([[*coordinate[:3]] for coordinate in ring], holes_by_shell[i])
                for i, ring in enumerate(shells)]
    if len(polygons) == 1:
        return polygons[0]
    return MultiPolygon(polygons)


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]) -> tuple[dict[str, object], list[dict[str, object]], dict[str, object]]:
    docs = {name: json.loads(body) for name, (body, _) in captures.items()}
    expected_names = {
        "service_before", "layer_before", "count_before", "ids_before",
        "ids_seed", "layer_after", "service_after", "count_after", "ids_after",
        *(f"page_{i:03d}" for i in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE)),
    }
    if set(captures) != expected_names:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    service = docs["service_before"]; layer = docs["layer_before"]
    if (not isinstance(service, dict) or service.get("serviceItemId") != SERVICE_ITEM_ID
            or service.get("mapName") != "FUDS Data"
            or service.get("copyrightText") != "US Army Corps of Engineers"):
        raise ValueError("source_item_identity_or_authority_drift")
    if not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME and x.get("type") == "Feature Layer" and x.get("parentLayerId") == 5 for x in service.get("layers", []) if isinstance(x, dict)):
        raise ValueError("exact_layer_missing")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("parentLayer", {}).get("id") != 5
            or "Formerly Used Defense Sites" not in str(service.get("description", ""))):
        raise ValueError("exact_layer_metadata_drift")
    if not any(x.get("id") == 7 and x.get("name") == "IRM Project Boundary" for x in service.get("layers", []) if isinstance(x, dict)):
        raise ValueError("irm_project_boundary_layer_missing")
    spatial = layer.get("spatialReference", {})
    if spatial.get("latestWkid") != 4326:
        raise ValueError("source_crs_drift")
    fields, schema_sha = _profile_schema(layer)
    if not fields or fields[0].get("name") != "OBJECTID" or fields[0].get("type") != "esriFieldTypeOID":
        raise ValueError("source_object_id_field_drift")
    after_fields = docs["layer_after"].get("fields")
    if not isinstance(after_fields, list) or [_plain_field(x) for x in after_fields] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    if EXPECTED_SCHEMA_SHA256 and schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_drift")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    ids_before = docs["ids_before"].get("objectIds"); ids_after = docs["ids_after"].get("objectIds")
    ids_seed = docs["ids_seed"].get("objectIds")
    if not isinstance(ids_before, list) or ids_before != ids_after or len(ids_before) != EXPECTED_COUNT:
        raise ValueError("source_id_population_drift")
    if not isinstance(ids_seed, list) or sorted(ids_seed) != sorted(ids_before):
        raise ValueError("source_id_population_changed_before_capture")
    ids_before = sorted(ids_before)
    if ids_before != list(range(1, EXPECTED_COUNT + 1)):
        raise ValueError("source_id_roster_drift")
    if EXPECTED_ID_SHA256 and _sha(_canonical(ids_before)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if docs["layer_before"] != docs["layer_after"] or docs["service_before"] != docs["service_after"]:
        raise ValueError("source_metadata_changed_during_capture")
    page_names = [name for name in captures if name.startswith("page_")]
    page_names.sort()
    features = []
    for name in page_names:
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncated")
        if page.get("spatialReference", {}).get("latestWkid") != 4326:
            raise ValueError("source_page_spatial_reference_drift")
        if "hasZ" in page or "hasM" in page:
            raise ValueError("source_page_geometry_dimensions_drift")
        rows = page.get("features")
        if not isinstance(rows, list): raise ValueError("source_page_missing_features")
        features.extend(rows)
    source_spatial_reference = docs[page_names[0]].get("spatialReference") if page_names else None
    if not isinstance(source_spatial_reference, dict) or any(docs[name].get("spatialReference") != source_spatial_reference for name in page_names):
        raise ValueError("source_page_spatial_reference_drift")
    got_ids = [f.get("attributes", {}).get("OBJECTID") for f in features]
    if len(features) != EXPECTED_COUNT or sorted(got_ids) != ids_before or len(set(got_ids)) != EXPECTED_COUNT:
        raise ValueError("source_page_population_incomplete")
    expected_names = [x["name"] for x in fields if x["type"] != "esriFieldTypeGeometry"]
    geometries = []
    for feature in features:
        attrs = feature.get("attributes")
        geom = feature.get("geometry")
        if not isinstance(attrs, dict) or list(attrs) != expected_names:
            raise ValueError("source_record_schema_drift")
        if geom is None:
            geometries.append([attrs["OBJECTID"], None])
            continue
        if not isinstance(geom, dict) or not isinstance(geom.get("rings"), list):
            raise ValueError("source_geometry_schema_drift")
        if not geom["rings"]:
            raise ValueError("source_polygon_rings_invalid")
        for ring in geom["rings"]:
            if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
                raise ValueError("source_ring_closure_or_shape_drift")
            for coordinate in ring:
                if (not isinstance(coordinate, list) or len(coordinate) != 2
                        or any(type(value) not in (int, float) or not math.isfinite(value) for value in coordinate)):
                    raise ValueError("source_xy_coordinate_shape_drift")
        geometries.append([attrs["OBJECTID"], geom["rings"]])
    features.sort(key=lambda f: f["attributes"]["OBJECTID"])
    records_sha = _sha(_canonical(features))
    geometry_sha = _sha(_canonical(geometries))
    if EXPECTED_RECORD_SHA256 and records_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_value_or_geometry_baseline_drift")
    if EXPECTED_GEOMETRY_SHA256 and geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    output_geometries = [
        _source_polygon_geometry(feature["geometry"]["rings"])
        for feature in features if feature.get("geometry") is not None
    ]
    invalid_geometry_count = sum(not geometry.is_valid for geometry in output_geometries)
    geometry_bounds = [geometry.bounds for geometry in output_geometries]
    source_extent = layer.get("extent", {})
    expected_bounds = [source_extent.get(key) for key in ("xmin", "ymin", "xmax", "ymax")]
    actual_bounds = ([min(x[0] for x in geometry_bounds), min(x[1] for x in geometry_bounds),
                      max(x[2] for x in geometry_bounds), max(x[3] for x in geometry_bounds)]
                     if geometry_bounds else None)
    null_geometry_count = sum(feature.get("geometry") is None for feature in features)
    if null_geometry_count == 0 and actual_bounds != expected_bounds:
        raise ValueError("source_coverage_extent_drift")
    all_coordinates = [coordinate for feature in features if feature.get("geometry") is not None
                       for ring in feature["geometry"]["rings"] for coordinate in ring]
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids_before)),
        "source_record_sha256": records_sha, "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": schema_sha, "source_crs_epsg": 4326, "source_spatial_reference": source_spatial_reference,
        "source_height_model_info": layer.get("sourceHeightModelInfo"),
        "source_layer_hasZ": layer.get("hasZ"), "source_layer_hasM_field_present": "hasM" in layer,
        "geometry_type": "Polygon", "null_geometry_count": null_geometry_count, "invalid_geometry_count": invalid_geometry_count,
        "unique_source_id_count": len(set(got_ids)),
        "source_geometry_bounds": actual_bounds,
        "source_z_min": None, "source_z_max": None,
        "source_m_vertex_count": 0, "source_m_nonnull_vertex_count": 0,
        "source_zero_area_ring_count": sum(_ring_signed_area(ring) == 0 for feature in features for ring in feature["geometry"]["rings"]),
        "arcgis_service_item_id": service.get("serviceItemId"),
        "service_modified_is_source_release_date": False,
    }
    return layer, fields, {"features": features, "audit": audit, "ids": ids_before}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    kind = field["type"]
    if kind == "esriFieldTypeOID": return pa.int64()
    if kind == "esriFieldTypeInteger": return pa.int32()
    if kind == "esriFieldTypeSmallInteger": return pa.int16()
    if kind == "esriFieldTypeDouble": return pa.float64()
    if kind == "esriFieldTypeSingle": return pa.float32()
    if kind in ("esriFieldTypeString", "esriFieldTypeGUID"): return pa.string()
    if kind == "esriFieldTypeBlob": return pa.binary()
    if kind == "esriFieldTypeGeometry": raise ValueError("geometry_field_is_separate_from_attributes")
    raise ValueError("unsupported_source_physical_type:" + str(kind))


def _parquet_bytes(fields: list[dict[str, object]], features: list[dict[str, object]], spatial_reference: dict[str, object]) -> bytes:
    columns = {}
    for field in fields:
        name = field["name"]
        if field["type"] != "esriFieldTypeGeometry":
            values = [row["attributes"][name] for row in features]
            if field["type"] == "esriFieldTypeBlob":
                try:
                    values = [None if value is None else base64.b64decode(value, validate=True) for value in values]
                except (TypeError, ValueError) as exc:
                    raise ValueError("source_blob_encoding_drift") from exc
            columns[name] = pa.array(values, type=_arrow_type(field))
    geometries = [
        _source_polygon_geometry(row["geometry"]["rings"]) if row.get("geometry") is not None else None
        for row in features
    ]
    columns["geometry"] = pa.array([geometry.wkb if geometry is not None else None for geometry in geometries], type=pa.binary())
    if any(field["name"] in {"geometry", "source_geometry"} for field in fields):
        raise ValueError("proposed_native_geometry_column_collision")
    # Preserve producer geometry JSON byte-for-value (including coordinate
    # order and omitted dimensional flags) alongside the standards GeoParquet WKB.
    source_geometry_type = pa.string()
    source_geometry = [_canonical(row.get("geometry")).decode("utf-8") if row.get("geometry") is not None else None for row in features]
    columns["source_geometry"] = pa.array(source_geometry, type=source_geometry_type)
    schema = pa.schema([pa.field(field["name"], _arrow_type(field), nullable=field.get("nullable", True)) for field in fields if field["type"] != "esriFieldTypeGeometry"] + [pa.field("geometry", pa.binary(), nullable=True), pa.field("source_geometry", source_geometry_type, nullable=True)])
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{
        "encoding":"WKB", "geometry_types":["Polygon", "MultiPolygon"], "crs":pyproj.CRS.from_epsg(4326).to_json_dict(), "edges":"planar",
    }}}
    table = pa.Table.from_pydict(columns, schema=schema.with_metadata({b"geo":_canonical(geo)}))
    sink = pa.BufferOutputStream(); pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _write_immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    write_immutable(storage, key, body)


def _verify_first_candidate(storage: StagingStorageResource, prefix: str, state_key: str,
                           version: str, fingerprint: str, identity: Mapping[str, object],
                           audit: Mapping[str, object], expected_keys: set[str], current_parquet: bytes) -> dict[str, object]:
    """Validate an earlier candidate before treating a same-version run as reuse."""
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity", "source_record_sha256", "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise ValueError("cached_first_candidate_identity_or_source_changed")
    objects = payload["objects"]
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], objects, payload["capture_receipts"])
    if not isinstance(objects, dict) or set(objects) != expected_keys:
        raise ValueError("cached_candidate_roster_changed")
    for name, descriptor in objects.items():
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if (not isinstance(descriptor, dict) or snapshot is None
                or descriptor != {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
                or storage.object_snapshot(key) != snapshot):
            raise ValueError("cached_candidate_object_integrity_failed:" + name)
    parquet_key = f"{prefix}/geoparquet/{DATASET_SLUG}.parquet"
    if objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != {"bytes": len(current_parquet), "sha256": _sha(current_parquet)}:
        raise ValueError("cached_native_candidate_changed")
    receipt_map = payload["capture_receipts"]
    if not isinstance(receipt_map, dict): raise ValueError("cached_capture_receipts_missing")
    for name, receipt in receipt_map.items():
        raw_key = f"{prefix}/raw/{name}.body"; wire_key = f"{prefix}/raw/{name}.wire"
        body = storage.read_key(raw_key); wire = storage.read_key(wire_key)
        receipt_body = storage.read_key(f"{prefix}/raw/{name}.receipt.json")
        if (receipt.get("complete_body") is not True or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt_body != _canonical(receipt)+b"\n"):
            raise ValueError("cached_original_capture_changed:" + name)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,
            "source_null_geometry_count":audit["null_geometry_count"],"source_invalid_geometry_count":audit["invalid_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json",
            "reused":True,"source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
            "freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def capture_fuds_irm_property_boundary(storage: StagingStorageResource, *, version: str, capture_directory: str | None = None) -> dict[str, object]:
    """Capture and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000: raise ValueError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/124/captures")
    capture_root = attempt_root / f"{version}-{uuid4().hex}"
    capture_root = capture_root.absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=60, headers={"User-Agent":"hifld-next-datasets/fuds-irm-property-boundary-draft"}) as client:
            seed_body, seed_receipt = _fetch(client, LAYER_URL + "/query?" + urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"}), capture_root, "ids_seed")
            seed = json.loads(seed_body).get("objectIds")
            if not isinstance(seed, list) or len(seed) != EXPECTED_COUNT:
                raise ValueError("source_seed_id_count_drift")
            captures["ids_seed"] = (seed_body, seed_receipt)
            for name, url in _feature_urls(seed):
                body, receipt = _fetch(client, url, capture_root, name)
                captures[name] = (body, receipt)
        layer, fields, extracted = _validate_captures(captures)
    except Exception as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try: receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError): receipts[path.name] = {"receipt_readback_failed":True}
        (capture_root / "capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":False,"error_code":type(exc).__name__,"receipts":receipts})+b"\n")
        if captures:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          captures, type(exc).__name__)
        raise

    identity = _code_identity()
    parquet_body = _parquet_bytes(fields, extracted["features"], extracted["audit"]["source_spatial_reference"])
    source_objects = {f"geoparquet/{DATASET_SLUG}.parquet":{"bytes":len(parquet_body),"sha256":_sha(parquet_body)}}
    manifest = {
      "schema_version":1,"approved":False,"title":LAYER_NAME,"description":"Exact USACE Military/FUDS_Data MapServer layer 8 IRM property-boundary polygons. The layer metadata and query responses do not declare Z or M dimensions. Precise source release date is unknown, so currentness and successor decision are pending.",
      "publisher":PUBLISHER,"agency":"U.S. Army Corps of Engineers","source_url":LAYER_URL,"license":None,
      "source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
      "inputs":[{"input_id":name,"producer":PUBLISHER,"host":urlsplit(receipt["url"]).hostname,"role":"producer_publication","canonical_url":SERVICE_URL if name == "service_before" or name == "service_after" else LAYER_URL,"retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],"http_status":receipt["status"],"bytes":receipt["bytes"],"sha256":receipt["sha256"],"license":"unknown_pending_rights_review","attribution":"USACE service copyrightText is U.S. Army Corps of Engineers; downstream license and rights review remain pending.","release_date":None,"release_date_unknown_reason":SOURCE_VINTAGE_NOTE} for name,(_,receipt) in sorted(captures.items())],
      "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"feature_layer_id":LAYER_ID,"source_fields":"all source fields preserved in producer order and declared physical types","source_ids":"Preserve each OBJECTID, all source identifiers unchanged","geometry":"Primary GeoParquet XY WKB derives shell/hole grouping from Esri ring winding at source EPSG:4326. Auxiliary source_geometry retains exact Esri geometry JSON, including coordinate and ring order; raw response pages retain absent hasZ/hasM flags. No reprojection, closure, or geometry repair is performed.","candidate_status":"unapproved; production publication prohibited pending source freshness, schema/ID contract, rights and mapping acceptance"}}],
      "lineage":"The USACE Military/FUDS_Data MapServer exposes layer 8, IRM Property Boundary, under group layer 5 Interim Risk Management (IRM). Layer 7 IRM Project Boundary is a distinct source product. The precise source release date is unknown; service or portal modification timestamps are not treated as source release dates.",
      "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native Esri query values, all attributes, IDs and source XY geometry retained exactly in source_geometry; raw pages preserve undeclared hasZ/hasM flags. Source vintage and release approvals remain unresolved.","objects":[{"path":next(iter(source_objects)),**next(iter(source_objects.values()))}]}}},
      "unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {**extracted["audit"],"source_fields":fields,"official_layer_name":LAYER_NAME,"arcgis_service_item_id":SERVICE_ITEM_ID,"service_modified_is_not_release_date":True,"source_vintage":SOURCE_VINTAGE,"source_vintage_note":SOURCE_VINTAGE_NOTE,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    version = content_derived_version(audit["source_record_sha256"],
                                      audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    (capture_root / "capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,"error_code":None,"source_feature_count":EXPECTED_COUNT,"source_record_sha256":audit["source_record_sha256"],"source_geometry_sha256":audit["source_geometry_sha256"],"receipts":{name:receipt for name,(_,receipt) in sorted(captures.items())}})+b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs = {
        f"geoparquet/{DATASET_SLUG}.parquet":parquet_body,
        "metadata/source_manifest.json":_canonical(manifest)+b"\n",
        "metadata/source_audit.json":_canonical(audit)+b"\n",
        "metadata/processing_identity.json":_canonical(identity)+b"\n",
    }
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root/f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt)+b"\n"
    ready = {"approved":False,"version":version,"objects":source_objects,"source_record_sha256":audit["source_record_sha256"],"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready)+b"\n"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        return _verify_first_candidate(storage,prefix,state_key,version,fingerprint,identity,audit,set(outputs),parquet_body)
    for relative, body in outputs.items(): _write_immutable(storage,f"{prefix}/{relative}",body)
    descriptors = {name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    for name, descriptor in descriptors.items():
        key=f"{prefix}/{name}"; snapshot=storage.object_snapshot(key)
        if snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] or storage.object_snapshot(key) != snapshot:
            raise ValueError("candidate_output_physical_readback_failed:"+name)
    payload = {"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":{name:receipt for name,(_,receipt) in captures.items()},"objects":descriptors}
    seal_ready_candidate(storage,state_key,version,fingerprint,payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"source_null_geometry_count":audit["null_geometry_count"],"source_invalid_geometry_count":audit["invalid_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json","source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(capture_root)}
