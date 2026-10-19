"""Lossless native candidate capture for the official USACE IENC buoy layer.

The USDOT/BTS NTAD item explicitly links this USACE chart-server service and
describes the update cadence. This adapter captures the linked public service
as an unapproved candidate; a recent service/item edit is not a source release
date, and no output/schema/ID/rights decision is implied.
"""
from __future__ import annotations

import hashlib
import json
import math
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
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    capture_get,
    content_derived_version,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "isolated-danger-buoy-point-usace-ienc"
FILE_SLUG = DATASET_SLUG
BTS_ITEM_ID = "3607d485a9854d6fa458bf507058ceae"
BTS_ITEM_LANDING_URL = f"https://www.arcgis.com/home/item.html?id={BTS_ITEM_ID}"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
SERVICE_FOLDER_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes"
SERVICE_NAME = "IENC_Feature_Classes/ISOLATED_DANGER_BUOY_POINT"
SERVICE_URL = f"{SERVICE_FOLDER_URL}/ISOLATED_DANGER_BUOY_POINT/MapServer"
LAYER_ID = 2
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
MASTER_GEODATABASE_URL = "https://ienccloud.us/ienc/products/files/U37/ienc_master_dataset_gdb/USACE_IENC_Master_Service_gdb.zip"
EXPECTED_COUNT = 1
EXPECTED_FEATURE_IDS = {"U3 1617851130 00001"}
EXPECTED_SCHEMA_SHA256 = "0ddd1a4cc2ae65123922e664aa24c21fdcf99b6cefeb86a991e709b8c812e30b"
PAGE_SIZE = 2000
MAX_BODY_BYTES = 8 * 1024 * 1024

_QUERY_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID", None),
    ("Buoy_Shape", "esriFieldTypeString", 254),
    ("Color", "esriFieldTypeString", 254),
    ("Object_Name", "esriFieldTypeString", 254),
    ("Information", "esriFieldTypeString", 254),
    ("Scale_Minimum", "esriFieldTypeInteger", None),
    ("Source_Date", "esriFieldTypeString", 254),
    ("Source_Indication", "esriFieldTypeString", 254),
    ("Source_Dataset", "esriFieldTypeString", 254),
    ("Feature_ID", "esriFieldTypeString", 254),
]
_LAYER_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID", None),
    ("SHAPE", "esriFieldTypeGeometry", None),
    *_QUERY_FIELDS[1:],
]
_EXPECTED_CAPTURE_NAMES = {
    "bts_item_before", "service_folder", "service_before", "layer_before",
    "count_before", "ids_before", "ids_seed", "page_000", "bts_item_after",
    "service_after", "layer_after", "count_after", "ids_after",
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "domain") if key in field}


def _expected_profile(fields: list[dict[str, object]], names: list[str]) -> list[dict[str, object]]:
    if not isinstance(fields, list) or any(not isinstance(item, dict) for item in fields):
        raise ValueError("source_field_roster_missing")
    if [field.get("name") for field in fields] != names:
        raise ValueError("source_field_name_or_order_drift")
    profile = [_plain_field(field) for field in fields]
    expected = _LAYER_FIELDS if names[0] == "OBJECTID" and len(names) == 11 else _QUERY_FIELDS
    for field, (name, kind, length) in zip(profile, expected):
        if field.get("name") != name or field.get("type") != kind:
            raise ValueError("source_field_type_drift:" + name)
        if length is not None and field.get("length") != length:
            raise ValueError("source_field_length_drift:" + name)
    return profile


def _urls(ids: list[int]) -> list[tuple[str, str]]:
    result: list[tuple[str, str]] = []
    for offset in range(0, len(ids), PAGE_SIZE):
        params = {
            "objectIds": ",".join(str(item) for item in ids[offset:offset + PAGE_SIZE]),
            "outFields": "*", "returnGeometry": "true", "returnZ": "true", "returnM": "true", "f": "json",
        }
        result.append((f"page_{offset // PAGE_SIZE:03d}", LAYER_URL + "/query?" + urlencode(params)))
    return result


def _validate_captures(
    captures: dict[str, tuple[bytes, dict[str, object]]],
) -> tuple[list[dict[str, object]], dict[str, object], list[dict[str, object]]]:
    if set(captures) != _EXPECTED_CAPTURE_NAMES:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    documents = {name: json.loads(body) for name, (body, _) in captures.items()}

    item_before, item_after = documents["bts_item_before"], documents["bts_item_after"]
    item_description = str(item_before.get("description", ""))
    lower_description = item_description.casefold()
    if (item_before != item_after or item_before.get("id") != BTS_ITEM_ID
            or item_before.get("owner") != "USDOT_BTS" or not item_before.get("orgId")
            or item_before.get("title") != "Inland Electronic Navigational Charts"
            or "updated bi-monthly" not in lower_description
            or MASTER_GEODATABASE_URL not in item_description or SERVICE_FOLDER_URL not in item_description
            or "only charts downloaded from the usace chart server" not in lower_description):
        raise ValueError("official_bts_distribution_link_or_cadence_drift")

    services = documents["service_folder"].get("services")
    if (not isinstance(services, list)
            or not any(isinstance(item, dict) and item.get("name") == SERVICE_NAME and item.get("type") == "MapServer" for item in services)):
        raise ValueError("official_service_folder_missing_exact_layer_service")

    service_before, service_after = documents["service_before"], documents["service_after"]
    layer_before, layer_after = documents["layer_before"], documents["layer_after"]
    if service_before != service_after or layer_before != layer_after:
        raise ValueError("source_metadata_changed_during_capture")
    service_layers = service_before.get("layers")
    if (service_before.get("mapName") != "ISOLATED_DANGER_BUOY_POINT"
            or service_before.get("copyrightText") != "USACE IENC"
            or not isinstance(service_layers, list)
            or not any(isinstance(item, dict) and item.get("id") == LAYER_ID and item.get("name") == "ISOLATED_DANGER_BUOY_POINT"
                       and item.get("geometryType") == "esriGeometryPoint" for item in service_layers)):
        raise ValueError("exact_usace_map_service_layer_identity_drift")
    spatial = layer_before.get("sourceSpatialReference", layer_before.get("spatialReference", {}))
    if (layer_before.get("id") != LAYER_ID or layer_before.get("name") != "ISOLATED_DANGER_BUOY_POINT"
            or layer_before.get("geometryType") != "esriGeometryPoint"
            or not isinstance(spatial, dict) or spatial.get("latestWkid", spatial.get("wkid")) != 4326
            or not isinstance(layer_before.get("advancedQueryCapabilities"), dict)
            or layer_before["advancedQueryCapabilities"].get("supportsPagination") is not True
            or layer_before.get("maxRecordCount", 0) < 1):
        raise ValueError("source_geometry_crs_or_query_capability_drift")

    layer_field_names = [item[0] for item in _LAYER_FIELDS]
    query_field_names = [item[0] for item in _QUERY_FIELDS]
    layer_fields = _expected_profile(layer_before.get("fields"), layer_field_names)

    count_before = documents["count_before"].get("count")
    count_after = documents["count_after"].get("count")
    ids_before = documents["ids_before"].get("objectIds")
    ids_seed = documents["ids_seed"].get("objectIds")
    ids_after = documents["ids_after"].get("objectIds")
    if (count_before != EXPECTED_COUNT or count_after != EXPECTED_COUNT
            or not isinstance(ids_before, list) or not isinstance(ids_seed, list) or not isinstance(ids_after, list)
            or len(ids_before) != EXPECTED_COUNT or len(set(ids_before)) != len(ids_before)
            or sorted(ids_before) != sorted(ids_seed) or sorted(ids_before) != sorted(ids_after)):
        raise ValueError("source_count_or_objectid_roster_drift")

    page_names = sorted(name for name in captures if name.startswith("page_"))
    if page_names != ["page_000"]:
        raise ValueError("source_page_roster_unexpected")
    page = documents["page_000"]
    if "error" in page or page.get("exceededTransferLimit") is True:
        raise ValueError("source_page_error_or_transfer_limit")
    if page.get("spatialReference", {}).get("latestWkid", page.get("spatialReference", {}).get("wkid")) != 4326:
        raise ValueError("source_page_crs_drift")
    query_fields = _expected_profile(page.get("fields"), query_field_names)
    features = page.get("features")
    if not isinstance(features, list) or len(features) != EXPECTED_COUNT:
        raise ValueError("source_page_feature_count_mismatch")
    feature_ids: set[str] = set()
    object_ids: list[int] = []
    for feature in features:
        attributes, geometry = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attributes, dict) or list(attributes) != query_field_names:
            raise ValueError("source_record_attribute_roster_drift")
        object_id = attributes.get("OBJECTID")
        feature_id = attributes.get("Feature_ID")
        if type(object_id) is not int or object_id not in ids_before:
            raise ValueError("source_objectid_missing_or_unexpected")
        if not isinstance(feature_id, str) or not feature_id.strip():
            raise ValueError("source_feature_id_missing")
        feature_ids.add(feature_id)
        object_ids.append(object_id)
        if not isinstance(geometry, dict) or set(geometry) != {"x", "y"}:
            raise ValueError("source_point_dimension_or_geometry_drift")
        if any(type(geometry[key]) not in (int, float) or not math.isfinite(geometry[key]) for key in ("x", "y")):
            raise ValueError("source_point_coordinate_invalid")
        for field, (_name, kind, _length) in zip(query_fields, _QUERY_FIELDS):
            value = attributes[field["name"]]
            if value is None:
                continue
            if kind in {"esriFieldTypeOID", "esriFieldTypeInteger"} and type(value) is not int:
                raise ValueError("source_integer_physical_type_drift:" + field["name"])
            if kind == "esriFieldTypeString" and not isinstance(value, str):
                raise ValueError("source_string_physical_type_drift:" + field["name"])
    if (sorted(object_ids) != sorted(ids_before) or len(set(object_ids)) != EXPECTED_COUNT
            or feature_ids != EXPECTED_FEATURE_IDS or len(feature_ids) != EXPECTED_COUNT):
        raise ValueError("source_feature_id_or_objectid_population_drift")
    features.sort(key=lambda item: item["attributes"]["OBJECTID"])
    record_sha = _sha(_canonical(features))
    ids_sha = _sha(_canonical(sorted(feature_ids)))
    geometry_rows = [[f["attributes"]["Feature_ID"], f["attributes"]["OBJECTID"], f["geometry"]["x"], f["geometry"]["y"]] for f in features]
    geometry_sha = _sha(_canonical(geometry_rows))
    schema_sha = _sha(_canonical({"layer_fields": layer_fields, "query_fields": query_fields, "crs": 4326, "geometry_type": "Point", "dimensions": "XY"}))
    coverage = {
        "xmin":min(f["geometry"]["x"] for f in features), "ymin":min(f["geometry"]["y"] for f in features),
        "xmax":max(f["geometry"]["x"] for f in features), "ymax":max(f["geometry"]["y"] for f in features),
    }
    extent = layer_before.get("extent")
    if (schema_sha != EXPECTED_SCHEMA_SHA256 or not isinstance(extent, dict)
            or any(extent.get(key) != value for key,value in coverage.items())):
        raise ValueError("source_schema_or_complete_coverage_drift")
    audit = {
        "source_feature_count": len(features), "source_objectids": sorted(object_ids),
        "source_objectid_sha256": _sha(_canonical(sorted(object_ids))),
        "source_feature_ids": sorted(feature_ids), "source_feature_id_sha256": ids_sha,
        "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": schema_sha, "source_crs_epsg": 4326,
        "geometry_type": "Point", "geometry_dimensions": "XY", "null_geometry_count": 0,
        "source_extent_epsg4326":coverage,
        "unique_objectid_count": len(set(object_ids)), "unique_feature_id_count": len(feature_ids),
        "official_bts_item_id": BTS_ITEM_ID, "official_distribution_link_verified": True,
        "source_release_date": None,
        "source_release_date_unknown_reason": "The official BTS item describes a bi-monthly delivery cadence but the observed feature Source_Date is a feature survey date, not a release date; no dated producer extraction was exposed by the feature service.",
        "feature_source_dates": sorted({str(f["attributes"].get("Source_Date")) for f in features if f["attributes"].get("Source_Date") is not None}),
        "feature_source_dates_are_release_dates":False,
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    }
    return query_fields, audit, features


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    kind = field["type"]
    if kind == "esriFieldTypeOID": return pa.int64()
    if kind == "esriFieldTypeInteger": return pa.int32()
    if kind == "esriFieldTypeString": return pa.string()
    raise ValueError("unsupported_source_physical_type:" + str(kind))


def _parquet_bytes(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    names = [field["name"] for field in fields]
    columns = {name: pa.array([row["attributes"][name] for row in features], type=_arrow_type(field))
               for name, field in zip(names, fields)}
    columns["SHAPE"] = pa.array([Point(row["geometry"]["x"], row["geometry"]["y"]).wkb for row in features], type=pa.binary())
    schema = pa.schema([pa.field(field["name"], _arrow_type(field)) for field in fields]
                       + [pa.field("SHAPE", pa.binary(), nullable=False)])
    geo = {"version":"1.1.0", "primary_column":"SHAPE", "columns":{"SHAPE":{
        "encoding":"WKB", "geometry_types":["Point"], "crs":pyproj.CRS.from_epsg(4326).to_json_dict(), "edges":"planar",
    }}}
    table = pa.Table.from_pydict(columns, schema=schema.with_metadata({b"geo":_canonical(geo)}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _capture_names(ids: list[int]) -> list[tuple[str, str]]:
    return [
        ("bts_item_before", BTS_ITEM_URL),
        ("service_folder", SERVICE_FOLDER_URL + "?f=pjson"),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
        ("ids_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})),
        ("ids_seed", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})),
        *_urls(ids),
        ("bts_item_after", BTS_ITEM_URL),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("count_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
        ("ids_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})),
    ]


def _verify_first_candidate(storage: StagingStorageResource, state_key: str, prefix: str,
                            version: str, fingerprint: str, identity: Mapping[str, object],
                            audit: Mapping[str, object], parquet_body: bytes) -> dict[str, object]:
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity", "source_record_sha256", "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise ValueError("cached_first_candidate_identity_or_source_changed")
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
                            identity, audit["source_record_sha256"], audit["source_schema_sha256"],
                            payload["objects"], payload["capture_receipts"])
    expected_parquet = payload["objects"].get(f"geoparquet/{DATASET_SLUG}.parquet")
    if expected_parquet != {"bytes":len(parquet_body), "sha256":_sha(parquet_body)}:
        raise ValueError("cached_native_candidate_changed")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,
            "feature_count":EXPECTED_COUNT,"source_null_geometry_count":0,
            "source_feature_id_sha256":audit["source_feature_id_sha256"],
            "source_record_sha256":audit["source_record_sha256"],
            "candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,
            "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
            "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}


def capture_usace_ienc_isolated_danger(
    storage: StagingStorageResource, *, version: str, capture_directory: str | None = None,
) -> dict[str, object]:
    """Capture, validate, and immutably stage one full native layer candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for char in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000:
        raise ValueError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/130/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=60,
                          headers={"User-Agent":"hifld-next-datasets/usace-ienc-isolated-danger-draft"}) as client:
            seed_body, seed_receipt = capture_get(
                client,
                LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"}),
                capture_root, "ids_seed", max_bytes=MAX_BODY_BYTES,
            )
            seed = json.loads(seed_body).get("objectIds")
            captures["ids_seed"] = (seed_body, seed_receipt)
            if not isinstance(seed, list) or len(seed) != EXPECTED_COUNT or any(type(value) is not int for value in seed):
                raise ValueError("source_seed_id_count_drift")
            for name, url in _capture_names(seed):
                if name == "ids_seed":
                    continue
                body, receipt = capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES)
                captures[name] = (body, receipt)
        fields, audit, features = _validate_captures(captures)
    except Exception as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            name = path.name.removesuffix(".receipt.json")
            try:
                receipt = json.loads(path.read_bytes())
                receipts[name] = receipt
                body_path, wire_path = capture_root / f"{name}.body", capture_root / f"{name}.wire"
                if body_path.exists() and wire_path.exists():
                    captures.setdefault(name, (body_path.read_bytes(), receipt))
            except (ValueError, OSError): receipts[path.name] = {"receipt_readback_failed":True}
        (capture_root / "capture_manifest.json").write_bytes(_canonical({
            "captured_at":datetime.now(timezone.utc).isoformat(), "complete":False,
            "error_code":type(exc).__name__, "receipts":receipts,
        })+b"\n")
        if captures:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, captures, type(exc).__name__)
        raise

    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    parquet_body = _parquet_bytes(fields, features)
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    parquet_descriptor = {"bytes":len(parquet_body), "sha256":_sha(parquet_body)}
    manifest = {
        "schema_version":1, "approved":False,
        "title":"Isolated Danger Buoy Point (USACE IENC)",
        "description":"Exact ISOLATED_DANGER_BUOY_POINT layer from the USACE IENC chart server, exposed through the official USDOT/BTS NTAD distribution link. Candidate is unapproved; release/freshness, schema, ID, rights, and baseline migration decisions are pending.",
        "agency":"U.S. Army Corps of Engineers / Army Geospatial Center",
        "publisher":"U.S. Army Corps of Engineers / Army Geospatial Center; distributed through U.S. Department of Transportation / Bureau of Transportation Statistics NTAD",
        "source_url":LAYER_URL,
        "license":"BTS item supplies a use caution but no explicit license; rights review remains pending.",
        "source_modified":None, "date_issued":None, "date_modified":None,
        "processor":"HIFLD Next", "code_commit":identity["code_commit"],
        "inputs":[
            {
                "input_id":name,
                "producer":("U.S. Department of Transportation / Bureau of Transportation Statistics (NTAD)" if name.startswith("bts_item") else "U.S. Army Corps of Engineers / Army Geospatial Center"),
                **({} if name.startswith("bts_item") else {"distributor":"U.S. Department of Transportation / Bureau of Transportation Statistics (NTAD)"}),
                "host":urlsplit(receipt["url"]).hostname,
                "role":"producer_publication" if name.startswith("bts_item") else "official_distributor",
                "canonical_url":BTS_ITEM_LANDING_URL,
                "retrieval_url":receipt["url"], "observed_final_url":receipt["final_url"],
                "retrieved_at":receipt["observed_at"], "http_status":receipt["status"],
                "bytes":receipt["bytes"], "sha256":receipt["sha256"],
                "license":"BTS item supplies a use caution but no explicit license; rights review remains pending.",
                "attribution":"BTS IENC item identifies USACE/AGC as producer and links the USACE chart-server web services; BTS is listed as distributor. Attribution acceptance remains pending.",
                **({} if name.startswith("bts_item") else {"delegation_evidence":{
                    "official_bts_item_url":BTS_ITEM_LANDING_URL,
                    "bts_item_id":BTS_ITEM_ID,
                    "distribution_description":"The official USDOT/BTS NTAD IENC item links this USACE chart-server web map service and the USACE master geodatabase.",
                    "master_geodatabase_url":MASTER_GEODATABASE_URL,
                }}),
                "release_date":None,
                "release_date_unknown_reason":"Official BTS metadata describes a bi-monthly chart delivery cadence, but the service does not expose a dated source extraction. Feature Source_Date is a source feature survey date, not a release date.",
            }
            for name, (_, receipt) in sorted(captures.items())
        ],
        "transformations":[{
            "operation":"native_esri_mapserver_json_to_geoparquet_candidate",
            "parameters":{
                "source_layer_id":LAYER_ID, "source_layer_name":"ISOLATED_DANGER_BUOY_POINT",
                "source_fields":"All ten query attributes retained under exact producer names and source physical types, including OBJECTID and Feature_ID; the source SHAPE geometry field is written as WKB under its exact producer name.",
                "source_identifiers":"OBJECTID and Feature_ID are both preserved as distinct source values; no id column, surrogate key, or historical published mapping is created.",
                "geometry":"Original SHAPE source field and XY coordinates preserved as WKB Point in the source EPSG:4326 CRS; no reprojection or repair.",
                "candidate_status":"Unapproved; publication requires separate source, freshness, rights, baseline, field mapping, ID, and release acceptance.",
            },
        }],
        "lineage":"The official USDOT/BTS NTAD item 3607d485a9854d6fa458bf507058ceae states USACE/AGC updates IENC charts bi-monthly, links this chart-server service folder and the USACE master geodatabase, and describes BTS as distributor. This capture uses only the exact isolated danger buoy point MapServer layer. The item metadata and feature-level Source_Date are not treated as the producer release date.",
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Only native attribute names, source physical values and source XY point geometry are retained; no public schema mapping or ID migration is approved.","objects":[{"path":parquet_name, **parquet_descriptor}]}}},
        "unsupported_output_formats":{
            "shapefile_zip":"No reviewed mapping or complete S-57/GDB source packaging contract is approved for a derived ZIP.",
            "geopackage":"No approved public schema/identifier mapping.",
            "pmtiles":"No display transformation or generalized geometry is approved.",
        },
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit.update({
        "source_fields":fields,
        "official_layer_name":"ISOLATED_DANGER_BUOY_POINT",
        "official_bts_item_id":BTS_ITEM_ID,
        "feature_source_dates_are_release_dates":False,
        "source_vintage_note":"BTS states bi-monthly updates; observed feature Source_Date values are feature survey dates and no dated producer release was available.",
    })
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"], audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    receipts = {name:receipt for name,(_,receipt) in sorted(captures.items())}
    (capture_root / "capture_manifest.json").write_bytes(_canonical({
        "captured_at":datetime.now(timezone.utc).isoformat(), "complete":True,
        "source_feature_count":audit["source_feature_count"],
        "source_feature_id_sha256":audit["source_feature_id_sha256"],
        "source_record_sha256":audit["source_record_sha256"],
        "source_geometry_sha256":audit["source_geometry_sha256"],
        "source_schema_sha256":audit["source_schema_sha256"], "receipts":receipts,
    })+b"\n")
    outputs = {
        parquet_name:parquet_body,
        "metadata/source_manifest.json":_canonical(manifest)+b"\n",
        "metadata/source_audit.json":_canonical(audit)+b"\n",
        "metadata/processing_identity.json":_canonical(identity)+b"\n",
    }
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt)+b"\n"
    ready = {
        "approved":False,"version":version,"objects":{parquet_name:parquet_descriptor},
        "source_feature_count":audit["source_feature_count"],
        "source_record_sha256":audit["source_record_sha256"],
        "source_feature_id_sha256":audit["source_feature_id_sha256"],
        "source_schema_sha256":audit["source_schema_sha256"],
        "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,
    }
    outputs["metadata/candidate_ready.json"] = _canonical(ready)+b"\n"
    fingerprint = _sha(_canonical(identity))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        return _verify_first_candidate(storage, state_key, prefix, version, fingerprint, identity, audit, parquet_body)
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
    descriptors = {name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    for name, descriptor in descriptors.items():
        key=f"{prefix}/{name}"
        snapshot=storage.object_snapshot(key)
        if (snapshot is None or snapshot.size != descriptor["bytes"]
                or storage.sha256_key(key) != descriptor["sha256"] or storage.object_snapshot(key) != snapshot):
            raise ValueError("candidate_output_physical_readback_failed:"+name)
    payload={"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],
             "source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":receipts,"objects":descriptors}
    seal_ready_candidate(storage,state_key,version,fingerprint,payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {
        "dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,
        "feature_count":audit["source_feature_count"],"source_null_geometry_count":0,
        "source_invalid_geometry_count":0,"candidate_ready":prefix+"/metadata/candidate_ready.json",
        "source_feature_id_sha256":audit["source_feature_id_sha256"],
        "source_record_sha256":audit["source_record_sha256"],
        "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,
        "capture_directory":str(capture_root),
    }
