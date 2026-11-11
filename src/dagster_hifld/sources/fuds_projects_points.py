"""Lossless, approval-gated capture of the maintained USACE FUDS Projects point layer.

Layer 2 is the FUDS project inventory. Property points, MRS records, IRM
features, district/division boundaries, and the historical HIFLD archive are
separate products. Source freshness and public-schema compatibility are not
accepted by this draft.
"""
from __future__ import annotations

import hashlib
import json
import shutil
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
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "formerly-used-defense-sites-projects-points"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://geospatial.sec.usace.army.mil/server/rest/services/Military/FUDS_Data/MapServer"
LAYER_ID = 2
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "FUDS Project Point"
PUBLISHER = "U.S. Army Corps of Engineers (USACE)"
SOURCE_DESCRIPTION = "The physical location of a specific project."
EXPECTED_COUNT = 5584
EXPECTED_LAYER_ROSTER = [(0, "FUDS FY24", "Group Layer"), (1, "FUDS Property Points", "Feature Layer"),
                        (2, "FUDS Project Point", "Feature Layer"), (3, "FUDS Munitions Response Site", "Feature Layer"),
                        (4, "FUDS Property Polygon", "Feature Layer"), (5, "Interim Risk Management (IRM)", "Group Layer"),
                        (6, "IRM Property Point", "Feature Layer"), (7, "IRM Project Boundary", "Feature Layer"),
                        (8, "IRM Property Boundary", "Feature Layer"), (9, "FUDS Program District Boundaries", "Feature Layer"),
                        (10, "FUDS Program Division Boundaries", "Feature Layer")]
PAGE_SIZE = 500
EXPECTED_ID_SHA256 = "5359fde45cd47d6741072698008b46b051b06d7098b2c564ca0c7a45f5a13da5"
EXPECTED_RECORD_SHA256 = "f3b1eee2380b8d17f1df8862483ba1170a9d543aec8249e66ab341a025d028d7"
EXPECTED_GEOMETRY_SHA256 = "23d10ad3d4a16d3a6f7264ec504711f8dd0ccb28288e0e5d51b4dfad4422fcd1"
EXPECTED_SCHEMA_SHA256 = "1129e8cadaaf0387b1892fdad34fcf58fd87994c6090af4d7425e95d0b0d0061"
MAX_CAPTURE_BYTES = 32 * 1024 * 1024


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _feature_urls(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
        ("ids_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})),
    ]
    for offset in range(0, len(ids), PAGE_SIZE):
        params = {"where":"1=1", "outFields":"*", "returnGeometry":"true", "returnZ":"true", "returnM":"true",
                  "outSR":"4326", "orderByFields":"OBJECTID ASC", "resultOffset":offset,
                  "resultRecordCount":PAGE_SIZE, "f":"json"}
        urls.append((f"page_{offset // PAGE_SIZE:03d}", LAYER_URL + "/query?" + urlencode(params)))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
        ("ids_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})),
    ])
    return urls


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]):
    docs = {name: json.loads(body) for name, (body, _) in captures.items()}
    page_count = (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE
    expected_names = {"service_before", "layer_before", "count_before", "ids_before", "ids_seed", "layer_after", "service_after", "count_after", "ids_after", *(f"page_{i:03d}" for i in range(page_count))}
    if set(captures) != expected_names:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or service.get("mapName") != "FUDS Data" or service.get("copyrightText") != "US Army Corps of Engineers":
        raise ValueError("official_usace_service_identity_drift")
    service_layers = service.get("layers")
    roster = [(x.get("id"), x.get("name"), x.get("type")) for x in service_layers if isinstance(x, dict)] if isinstance(service_layers, list) else None
    if roster != EXPECTED_LAYER_ROSTER:
        raise ValueError("fuds_service_layer_roster_drift")
    if not any(x.get("id") == 2 and x.get("name") == LAYER_NAME and x.get("type") == "Feature Layer" for x in service_layers):
        raise ValueError("exact_fuds_project_layer_missing")
    if layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME or layer.get("description") != SOURCE_DESCRIPTION or layer.get("geometryType") != "esriGeometryPoint":
        raise ValueError("exact_fuds_projects_point_layer_metadata_drift")
    if layer.get("hasZ") is True or layer.get("hasM") is True:
        raise ValueError("source_z_or_m_geometry_unsupported")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2000 or layer.get("spatialReference", {}).get("latestWkid") != 4326:
        raise ValueError("source_record_limit_or_crs_drift")
    fields_value = layer.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(x, dict) for x in fields_value):
        raise ValueError("source_field_roster_drift")
    fields = [_plain_field(x) for x in fields_value]
    schema_sha = _sha(_canonical(fields))
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_contract_drift")
    if docs["layer_after"].get("fields") is None or [_plain_field(x) for x in docs["layer_after"]["fields"]] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    for name in ("layer_before", "layer_after"):
        if docs[name] != layer: raise ValueError("source_layer_metadata_changed_during_capture")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    rosters = [docs[name].get("objectIds") for name in ("ids_before", "ids_seed", "ids_after")]
    if any(not isinstance(roster, list) or len(roster) != EXPECTED_COUNT or
           any(type(value) is not int for value in roster) or len(set(roster)) != EXPECTED_COUNT
           for roster in rosters):
        raise ValueError("source_id_population_drift")
    if any(sorted(roster) != sorted(rosters[0]) for roster in rosters[1:]):
        raise ValueError("source_id_population_drift")
    ids = sorted(rosters[0])
    if _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if docs["ids_before"].get("objectIdFieldName") != "OBJECTID" or docs["ids_after"].get("objectIdFieldName") != "OBJECTID":
        raise ValueError("source_id_field_changed")
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")

    features = []
    for page_idx, offset in enumerate(range(0, EXPECTED_COUNT, PAGE_SIZE)):
        page = docs[f"page_{page_idx:03d}"]
        if not isinstance(page, dict) or "error" in page:
            raise ValueError("source_page_error")
        rows = page.get("features")
        if not isinstance(rows, list): raise ValueError("source_page_missing_features")
        expected = ids[offset:offset + PAGE_SIZE]
        actual = [r.get("attributes", {}).get("OBJECTID") for r in rows if isinstance(r, dict)]
        full_nonfinal = len(expected) == PAGE_SIZE and offset + PAGE_SIZE < EXPECTED_COUNT
        exceeded = page.get("exceededTransferLimit") is True
        if actual != expected or len(rows) != len(expected):
            raise ValueError("source_page_id_slice_incomplete_or_reordered")
        if exceeded and not full_nonfinal:
            raise ValueError("source_page_unexpected_exceeded_transfer_limit")
        spatial = page.get("spatialReference", {})
        if spatial and spatial.get("latestWkid") != 4326:
            raise ValueError("source_page_spatial_reference_drift")
        features.extend(rows)
    attr_names = [field["name"] for field in fields if field["name"] != "SHAPE"]
    null_geometry = 0
    geometry_records = []
    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict) or list(attrs) != attr_names:
            raise ValueError("source_record_attribute_schema_drift")
        geom = feature.get("geometry")
        if geom is None:
            null_geometry += 1
            geometry_records.append([attrs["OBJECTID"], None, None])
        elif isinstance(geom, dict) and set(geom) == {"x", "y"} and all(type(geom[k]) in (int, float) for k in ("x", "y")):
            geometry_records.append([attrs["OBJECTID"], geom["x"], geom["y"]])
        else:
            raise ValueError("source_point_geometry_invalid")
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    record_sha = _sha(_canonical(features))
    geometry_sha = _sha(_canonical(geometry_records))
    if record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    audit = {"source_feature_count":len(features), "source_id_sha256":_sha(_canonical(ids)),
             "source_record_sha256":record_sha, "source_geometry_sha256":geometry_sha,
             "source_schema_sha256":schema_sha, "source_crs_epsg":4326,
             "geometry_type":"Point", "null_geometry_count":null_geometry,
             "unique_source_id_count":len(ids), "source_compilation_context":"Layer is under the service group named FUDS FY24; this is a compilation label, not a dated release assertion.",
             "source_release_date":None,
             "source_release_date_unknown_reason":"The maintained service does not publish a dated release for this layer; fetch and portal modification times are not source release dates."}
    return layer, fields_value, {"features":features,"audit":audit,"ids":ids}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID":pa.int64(), "esriFieldTypeInteger":pa.int32(),
            "esriFieldTypeSmallInteger":pa.int16(), "esriFieldTypeDouble":pa.float64(),
            "esriFieldTypeSingle":pa.float32(), "esriFieldTypeString":pa.string(),
            "esriFieldTypeDate":pa.timestamp("ms", tz="UTC"), "esriFieldTypeBlob":pa.binary()}[str(field["type"])]


def _parquet_bytes(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    columns = {}
    schema_fields = []
    for field in fields:
        name = field["name"]
        typ = pa.binary() if name == "SHAPE" else _arrow_type(field)
        if name == "SHAPE":
            values = [None if row.get("geometry") is None else Point(row["geometry"]["x"], row["geometry"]["y"]).wkb for row in features]
            schema_fields.append(pa.field(name, typ, nullable=True))
        else:
            values = [row["attributes"].get(name) for row in features]
            schema_fields.append(pa.field(name, typ, nullable=True))
        columns[name] = pa.array(values, type=typ)
    geo = {"version":"1.1.0", "primary_column":"SHAPE", "columns":{"SHAPE":{
        "encoding":"WKB", "geometry_types":["Point"], "crs":pyproj.CRS.from_epsg(4326).to_json_dict(), "edges":"planar"}}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields).with_metadata({b"geo":_canonical(geo)}))
    sink = pa.BufferOutputStream(); pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def capture_fuds_projects_points(storage: StagingStorageResource, *, version: str, capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000: raise ValueError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/96/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=60, headers={"User-Agent":"hifld-next-datasets/fuds-projects-draft"}) as client:
            seed_url = LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})
            seed_body, seed_receipt = capture_get(client, seed_url, capture_root, "ids_seed", max_bytes=MAX_CAPTURE_BYTES)
            seed = json.loads(seed_body).get("objectIds")
            if (not isinstance(seed, list) or len(seed) != EXPECTED_COUNT or
                    any(type(value) is not int for value in seed) or len(set(seed)) != EXPECTED_COUNT):
                raise ValueError("source_seed_id_population_drift")
            captures["ids_seed"] = (seed_body, seed_receipt)
            for name, url in _feature_urls(sorted(seed)):
                body, receipt = capture_get(client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES)
                captures[name] = (body, receipt)
        layer, source_fields, extracted = _validate_captures(captures)
    except Exception as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try: receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError): receipts[path.name] = {"receipt_readback_failed":True}
        (capture_root / "capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":False,"error_code":type(exc).__name__,"receipts":receipts})+b"\n")
        retained = dict(captures)
        for receipt_path in capture_root.glob("*.receipt.json"):
            name = receipt_path.name.removesuffix(".receipt.json")
            if name in retained:
                continue
            try:
                retained[name] = ((capture_root / f"{name}.body").read_bytes(), json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            quarantine_incomplete_capture(storage,capture_root,DATASET_SLUG,version,retained,type(exc).__name__)
        raise
    identity = _code_identity()
    parquet_body = _parquet_bytes(source_fields, extracted["features"])
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {rel:{"bytes":len(parquet_body),"sha256":_sha(parquet_body)}}
    manifest = {
        "schema_version":1,"approved":False,"title":LAYER_NAME,
        "description":"USACE-maintained Formerly Used Defense Sites (FUDS) project point layer (MapServer layer 2). The service groups it under FUDS FY24; this is a compilation context, not a publication date. Complete source attributes and point geometry are represented in the candidate. Source release date, freshness, public schema contract, and rights/attribution review remain unresolved.",
        "publisher":PUBLISHER,"agency":"US Army Corps of Engineers","source_url":LAYER_URL,
        "license":None,"source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
        "inputs":[{"input_id":name,"producer":PUBLISHER,"host":urlsplit(receipt["url"]).hostname,"role":"producer_publication","canonical_url":LAYER_URL,"retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],"http_status":receipt["status"],"bytes":receipt["bytes"],"sha256":receipt["sha256"],"license":"Not stated by the source; rights review pending.","attribution":"USACE service copyright text; formal license and attribution terms require maintainer review.","release_date":None,"release_date_unknown_reason":"Layer release date is not stated by the service."} for name,(_,receipt) in sorted(captures.items())],
        "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"layer_id":LAYER_ID,"source_fields":"all producer fields retained in producer order and physical types; SHAPE is the original geometry field represented as WKB","source_ids":"Preserve producer OBJECTID and all project identifiers/values; no substitute IDs, casts, trimming, or joins","geometry":"Source point x/y retained as WKB in EPSG:4326; null source geometry retained as null","candidate_status":"unapproved; publication blocked pending freshness, schema/ID contract, attribution and release acceptance"}}],
        "lineage":"USACE Military/FUDS_Data MapServer layer 2 FUDS Project Point. Layer 1 is FUDS Property Points, layer 3 is FUDS Munitions Response Site, layers 6–8 are IRM products, and layers 9–10 are FUDS district/division boundaries. The DataLumos HIFLD archive and existing HIFLD candidate are historical comparison artifacts, not this maintained source publication.",
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Complete native source fields, IDs, values, physical nulls, and geometry retained; all release and compatibility approvals remain pending.","objects":[{"path":rel,**source_objects[rel]}]}}},
        "unsupported_output_formats":{"shapefile_zip":"No approved source-to-public Shapefile field mapping.","geopackage":"No approved source-to-public schema mapping.","pmtiles":"No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit={**extracted["audit"],"source_fields":[_plain_field(f) for f in source_fields],"official_layer_name":LAYER_NAME,"layer_id":LAYER_ID,"service_url":SERVICE_URL,"release_date_unknown":True,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    version=content_derived_version(audit["source_record_sha256"],audit["source_geometry_sha256"],audit["source_schema_sha256"],identity)
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    (capture_root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,"source_feature_count":EXPECTED_COUNT,"source_record_sha256":audit["source_record_sha256"],"source_geometry_sha256":audit["source_geometry_sha256"],"receipts":{name:receipt for name,(_,receipt) in sorted(captures.items())}})+b"\n")
    state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs={rel:parquet_body,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"]=body
        outputs[f"raw/{name}.wire"]=(capture_root/f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"]=_canonical(receipt)+b"\n"
    outputs["metadata/candidate_ready.json"]=_canonical({"approved":False,"version":version,"objects":source_objects,"source_record_sha256":audit["source_record_sha256"],"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})+b"\n"
    fingerprint=_sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle=json.loads(load_first_construction(storage,state_key,version,fingerprint)); payload=bundle.get("payload")
        if not isinstance(payload,dict):raise ValueError("cached_first_candidate_invalid")
        objects=payload.get("objects",{})
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,identity,audit["source_record_sha256"],audit["source_schema_sha256"],objects,payload.get("capture_receipts",{}))
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"source_null_geometry_count":audit["null_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,"source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    for relative,body in outputs.items():write_immutable(storage,f"{prefix}/{relative}",body)
    descriptors={name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    for name,descriptor in descriptors.items():
        key=f"{prefix}/{name}";snap=storage.object_snapshot(key)
        if snap is None or snap.size!=descriptor["bytes"] or storage.sha256_key(key)!=descriptor["sha256"] or storage.object_snapshot(key)!=snap:
            raise ValueError("candidate_output_physical_readback_failed:"+name)
    payload={"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":{name:receipt for name,(_,receipt) in captures.items()},"objects":descriptors}
    seal_ready_candidate(storage,state_key,version,fingerprint,payload)
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"source_null_geometry_count":audit["null_geometry_count"],"source_invalid_geometry_count":0,"candidate_ready":prefix+"/metadata/candidate_ready.json","source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(capture_root)}
