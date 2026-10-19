"""Draft extractor for BTS's exact IPCD feature layer.

The BTS describes IPCD as a frozen publication whose latest information is from 2022. This module captures the BTS-owned layer and stages only an unapproved candidate; it does not imply future refreshes or permit publication.
"""
from __future__ import annotations

import hashlib
import json
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
from shapely.geometry import Point

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

ITEM_ID = "25d9314da4ec44088f1db9e9e11a19d6"
SERVICE_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Intermodal_Passenger_Connectivity_Database/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Intermodal Passenger Connectivity Database (IPCD)"
DATASET_SLUG = "intermodal-passenger-connectivity-database-ipcd"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Department of Transportation, Bureau of Transportation Statistics (BTS), USDOT_BTS ArcGIS publication"
SOURCE_VINTAGE = "2022-10-19"
SOURCE_VINTAGE_NOTE = "BTS ArcGIS service description says IPCD is no longer updated and latest information is from 2022; service/item modified timestamps are not release dates."
BTS_DICTIONARY_URL = "https://usdot.maps.arcgis.com/sharing/rest/content/items/adbb0f820467470f82b9c8c83dbedf78/data"
EXPECTED_COUNT = 15488
EXPECTED_ID_SHA256 = "d852f0b08f8045ece669f33f7e6d1c74c230c8aebcd4885e08f25cea6e4a7446"
EXPECTED_RECORD_SHA256 = "dee65248326386817db6de50d9074190427436dbb1bbe09381b921a61f21244e"
EXPECTED_GEOMETRY_SHA256 = "787e435912aa4eb66f960d15bf7ae6d3cf9627e03a17c81fbd501ba61e018c11"
EXPECTED_SCHEMA_SHA256 = "870edc2ecd6994fb4d18769a57fcf0da9deafd12e529dcaca5f1d98639074142"
PAGE_SIZE = 100
MAX_BODY_BYTES = 32_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return dict(field)


def _fetch(client: httpx.Client, url: str, directory: Path, name: str, *, max_bytes: int = MAX_BODY_BYTES) -> tuple[bytes, dict[str, object]]:
    return capture_get(client, url, directory, name, max_bytes=max_bytes)


def _feature_urls(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("portal_item", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"),
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


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]) -> tuple[dict[str, object], list[dict[str, object]], dict[str, object]]:
    docs = {name: json.loads(body) for name, (body, _) in captures.items()}
    service, layer, portal = docs["service_before"], docs["layer_before"], docs["portal_item"]
    if portal.get("id") != ITEM_ID or portal.get("owner") != "USDOT_BTS" or portal.get("title") != "Intermodal Passenger Connectivity Database (IPCD)" or portal.get("url") != SERVICE_URL or portal.get("access") != "public":
        raise ValueError("official_BTS_portal_identity_drift")
    if service.get("serviceItemId") != ITEM_ID or not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME for x in service.get("layers", []) if isinstance(x, dict)):
        raise ValueError("official_BTS_service_layer_drift")
    if layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPoint":
        raise ValueError("exact_layer_metadata_drift")
    if "no longer being updated" not in service.get("description", "") or "latest information is from 2022" not in service.get("description", ""):
        raise ValueError("BTS_freshness_statement_drift")
    fields, schema_sha = _profile_schema(layer)
    if [_plain_field(x) for x in docs["layer_after"].get("fields", [])] != fields or docs["layer_before"] != docs["layer_after"] or docs["service_before"] != docs["service_after"]:
        raise ValueError("source_metadata_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    ids = docs["ids_before"].get("objectIds"); ids_after = docs["ids_after"].get("objectIds"); seed = docs["ids_seed"].get("objectIds")
    if not isinstance(ids, list) or ids != ids_after or sorted(ids) != sorted(seed or []) or len(ids) != EXPECTED_COUNT or len(set(ids)) != len(ids):
        raise ValueError("source_id_population_drift")
    if _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA256:
        raise ValueError("frozen_source_id_roster_drift")
    if set(captures) != {"portal_item", "service_before", "layer_before", "count_before", "ids_before", "ids_seed", "layer_after", "service_after", "count_after", "ids_after", *(f"page_{i:03d}" for i in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE))}:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    features = []
    for name in sorted(n for n in captures if n.startswith("page_")):
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True or page.get("spatialReference", {}).get("latestWkid") != 4326:
            raise ValueError("source_page_error_or_spatial_drift")
        rows = page.get("features")
        if not isinstance(rows, list): raise ValueError("source_page_missing_features")
        features.extend(rows)
    features.sort(key=lambda f: f.get("attributes", {}).get("OBJECTID", -1))
    got_ids = [f.get("attributes", {}).get("OBJECTID") for f in features]
    expected_fields = [f["name"] for f in fields]
    geom_rows = []
    for f in features:
        attrs, geom = f.get("attributes"), f.get("geometry")
        if not isinstance(attrs, dict) or list(attrs) != expected_fields or not isinstance(geom, dict) or set(geom) != {"x", "y"}:
            raise ValueError("source_record_schema_or_geometry_drift")
        geom_rows.append([attrs["OBJECTID"], geom["x"], geom["y"]])
    if len(features) != EXPECTED_COUNT or sorted(got_ids) != sorted(ids) or len(set(got_ids)) != EXPECTED_COUNT:
        raise ValueError("source_page_population_incomplete")
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("frozen_source_schema_drift")
    if _sha(_canonical(features)) != EXPECTED_RECORD_SHA256:
        raise ValueError("frozen_source_record_drift")
    if _sha(_canonical(geom_rows)) != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("frozen_source_geometry_drift")
    audit = {"source_feature_count":len(features), "source_id_sha256":_sha(_canonical(sorted(ids))), "source_record_sha256":_sha(_canonical(features)), "source_geometry_sha256":_sha(_canonical(geom_rows)), "source_schema_sha256":schema_sha, "source_crs_epsg":4326, "geometry_type":"Point", "null_geometry_count":0, "unique_source_id_count":len(set(got_ids)), "portal_item_id":portal["id"], "portal_owner":portal["owner"], "portal_modified_epoch_ms":portal.get("modified"), "portal_modified_is_source_release_date":False}
    return layer, fields, {"features":features,"audit":audit,"ids":sorted(ids)}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    kind = field["type"]
    if kind == "esriFieldTypeOID": return pa.int64()
    if kind == "esriFieldTypeInteger": return pa.int32()
    if kind == "esriFieldTypeSmallInteger": return pa.int16()
    if kind == "esriFieldTypeDouble": return pa.float64()
    if kind == "esriFieldTypeSingle": return pa.float32()
    if kind == "esriFieldTypeString": return pa.string()
    if kind == "esriFieldTypeDate": return pa.int64()
    raise ValueError("unsupported_source_physical_type:" + str(kind))


def _parquet_bytes(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    columns = {}
    for field in fields:
        name = field["name"]
        columns[name] = pa.array([row["attributes"][name] for row in features], type=_arrow_type(field))
    columns["geometry"] = pa.array([Point(row["geometry"]["x"], row["geometry"]["y"]).wkb for row in features], type=pa.binary())
    schema = pa.schema([pa.field(field["name"], _arrow_type(field), nullable=field.get("nullable", True)) for field in fields] + [pa.field("geometry", pa.binary(), nullable=False)])
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{
        "encoding":"WKB", "geometry_types":["Point"], "crs":pyproj.CRS.from_epsg(4326).to_json_dict(), "edges":"planar",
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
            "source_null_geometry_count":0,"source_invalid_geometry_count":0,"candidate_ready":prefix+"/metadata/candidate_ready.json",
            "reused":True,"source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
            "freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def _load_retained_capture(capture_root: Path) -> tuple[dict[str, tuple[bytes, dict[str, object]]], str]:
    """Load an exact earlier source capture without changing its receipts or timestamps."""
    manifest_path = capture_root / "capture_manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    receipts = manifest.get("receipts")
    if manifest.get("complete") is not True or not isinstance(receipts, dict):
        raise ValueError("retained_capture_not_complete")
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    for name, original in receipts.items():
        if not isinstance(name, str) or not isinstance(original, dict):
            raise ValueError("retained_capture_receipt_invalid")
        body = (capture_root / f"{name}.body").read_bytes()
        wire = (capture_root / f"{name}.wire").read_bytes()
        receipt_bytes = (capture_root / f"{name}.receipt.json").read_bytes()
        receipt = json.loads(receipt_bytes)
        if (receipt != original or receipt_bytes != _canonical(receipt) + b"\n"
                or receipt.get("complete_body") is not True or receipt.get("status") != 200
                or receipt.get("final_url") != receipt.get("url") or receipt.get("error_code") is not None
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
            raise ValueError("retained_capture_integrity_failed:" + name)
        captures[name] = (body, receipt)
    return captures, str(manifest.get("captured_at") or "")


def capture_bts_ipcd(storage: StagingStorageResource, *, version: str, capture_directory: str | None = None, reuse_retained_capture: bool = False) -> dict[str, object]:
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
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/129/captures")
    capture_root = attempt_root.absolute() if reuse_retained_capture else (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    captured_at = ""
    if reuse_retained_capture:
        captures, captured_at = _load_retained_capture(capture_root)
        layer, fields, extracted = _validate_captures(captures)
    else:
        capture_root.mkdir(parents=True, exist_ok=True)
        try:
            with httpx.Client(follow_redirects=False, timeout=60, headers={"User-Agent":"hifld-next-datasets/bts-ipcd-frozen-snapshot-draft"}) as client:
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
        captured_at = datetime.now(timezone.utc).isoformat()

    identity = _code_identity()
    parquet_body = _parquet_bytes(fields, extracted["features"])
    source_objects = {"geoparquet/intermodal-passenger-connectivity-database-ipcd.parquet":{"bytes":len(parquet_body),"sha256":_sha(parquet_body)}}
    manifest = {
      "schema_version":1,"approved":False,"title":LAYER_NAME,"description":"BTS describes IPCD as no longer updated, with latest information from 2022. Adoption and dataset continuity remain release gates.",
      "publisher":PUBLISHER,"agency":"U.S. Department of Transportation, Bureau of Transportation Statistics","source_url":LAYER_URL,"license":"https://www.usa.gov/publicdomain/label/1.0/",
      "source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
      "inputs":[{"input_id":name,"producer":PUBLISHER,"host":urlsplit(receipt["url"]).hostname,"role":"producer_publication","canonical_url":f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json" if name == "portal_item" else LAYER_URL,"retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],"http_status":receipt["status"],"bytes":receipt["bytes"],"sha256":receipt["sha256"],"license":"https://www.usa.gov/publicdomain/label/1.0/","attribution":"BTS ArcGIS item states the NTAD dataset is a U.S. government work available for unrestricted public use; rights review remains pending.","release_date":None,"release_date_unknown_reason":SOURCE_VINTAGE_NOTE} for name,(_,receipt) in sorted(captures.items())],
      "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"feature_layer_id":LAYER_ID,"source_fields":"all source fields preserved in producer order and declared physical types","source_ids":"Preserve source OBJECTID and every source identifier unchanged","geometry":"Esri point x/y retained as WKB at EPSG:4326; no reprojection or repair","candidate_status":"unapproved; production publication prohibited pending source freshness, schema/ID contract, rights and mapping acceptance"}}],
      "lineage":"USDOT_BTS ArcGIS item 25d9314da4ec44088f1db9e9e11a19d6 serves exact layer 0. BTS says IPCD is no longer updated and latest information is from 2022. The ArcGIS item modification date is not a release date.",
      "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native Esri query values, all attributes, IDs and source point geometry retained; source vintage and release approvals remain unresolved.","objects":[{"path":next(iter(source_objects)),**next(iter(source_objects.values()))}]}}},
      "unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {**extracted["audit"],"source_fields":fields,"official_layer_name":LAYER_NAME,"arcgis_item_id":ITEM_ID,"service_modified_is_not_release_date":True,"source_vintage":SOURCE_VINTAGE,"source_vintage_note":SOURCE_VINTAGE_NOTE,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    version = content_derived_version(audit["source_record_sha256"],
                                      audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    if not reuse_retained_capture:
        (capture_root / "capture_manifest.json").write_bytes(_canonical({"captured_at":captured_at,"complete":True,"error_code":None,"source_feature_count":EXPECTED_COUNT,"source_record_sha256":audit["source_record_sha256"],"source_geometry_sha256":audit["source_geometry_sha256"],"receipts":{name:receipt for name,(_,receipt) in sorted(captures.items())}})+b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs = {
        "geoparquet/intermodal-passenger-connectivity-database-ipcd.parquet":parquet_body,
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
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"source_null_geometry_count":0,"source_invalid_geometry_count":0,"candidate_ready":prefix+"/metadata/candidate_ready.json","source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(capture_root)}
