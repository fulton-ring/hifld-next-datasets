"""EPA FRS RCRA TSD native point-layer draft capture.

The ArcGIS service is EPA hosted and EPA's item identifies RCRAInfo as the
program source.  The adapter retains every response and every source field;
release, field-ID, and date interpretation decisions remain pending.
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
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

ITEM_ID = "0b30c57359194330bb89c9b3cf9c0bd5"
SERVICE_URL = "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_INTERESTS_RCRA_TSD/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "FRS_INTERESTS_RCRA_TSD"
DATASET_SLUG = "epa-rcra-treatment-storage-and-disposal-facilities"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Environmental Protection Agency, Facility Registry Service Support Team"
PROGRAM_SOURCE = "Resource Conservation and Recovery Act (RCRA) via RCRAInfo"
EPA_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"
EPA_PUBLIC_DOMAIN = "https://www.usa.gov/publicdomain/label/1.0/"
FRS_URL = "https://www.epa.gov/frs"
PAGE_SIZE = 1000
MAX_BODY_BYTES = 32 * 1024 * 1024
EXPECTED_SCHEMA_SHA256 = "f0f50c2204dfd72bcc5df3b2faae12f45b9b9c0f02c059078a1bfc2d2054184c"


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _urls(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("epa_item", EPA_ITEM_URL), ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
        ("ids_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})),
    ]
    for offset in range(0, len(ids), PAGE_SIZE):
        params = {"where":"1=1", "outFields":"*", "returnGeometry":"true",
                  "outSR":"4269", "orderByFields":"OBJECTID ASC", "resultOffset":offset,
                  "resultRecordCount":min(PAGE_SIZE, len(ids) - offset), "f":"json"}
        urls.append((f"page_{offset // PAGE_SIZE:03d}", LAYER_URL + "/query?" + urlencode(params)))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"), ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
        ("ids_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})),
    ])
    return urls


def _fields(layer: dict) -> tuple[list[dict], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or not fields or any(not isinstance(f, dict) for f in fields):
        raise ValueError("source_field_roster_drift")
    plain = [_plain_field(f) for f in fields]
    names = [field.get("name") for field in plain]
    if len(names) != len(set(names)) or "OBJECTID" not in names:
        raise ValueError("source_field_identity_drift")
    return plain, _sha(_canonical(plain))


def _stable_item(item: dict) -> dict:
    # ArcGIS increments only this counter on GET. Keep both complete raw
    # snapshots but exclude this documented volatile observation from identity.
    stable = dict(item)
    stable.pop("numViews", None)
    return stable


def validate_capture_documents(captures: Mapping[str, bytes], *,
                               expected_schema_sha256: str | None = EXPECTED_SCHEMA_SHA256) -> tuple[dict, list[dict], dict]:
    """Validate exact item, source identity, schema and complete ordered feature roster."""
    docs = {name: json.loads(body) for name, body in captures.items()}
    required = {"epa_item", "service_before", "layer_before", "count_before", "ids_before",
                "ids_seed", "layer_after", "service_after", "count_after", "ids_after"}
    if not required.issubset(docs):
        raise ValueError("source_capture_roster_incomplete")
    item = docs["epa_item"]
    if (item.get("id") != ITEM_ID or item.get("owner") != "EPA_GEO"
            or item.get("title") != "EPA  Facility Registry Service - RCRA Hazardous Waste Treatment, Storage, and Disposal (RCRA TSD)"
            or item.get("url") != SERVICE_URL or item.get("access") != "public"):
        raise ValueError("official_epa_item_identity_drift")
    if "public domain" not in str(item.get("licenseInfo", "")).lower() or "usa.gov/publicdomain" not in str(item.get("licenseInfo", "")):
        raise ValueError("exact_item_public_license_declaration_missing")
    if "RCRA" not in str(item.get("accessInformation", "")) or "FRS Support" not in str(item.get("accessInformation", "")):
        raise ValueError("epa_producer_or_program_lineage_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if service.get("serviceItemId") != ITEM_ID or service.get("capabilities") != "Query,Extract":
        raise ValueError("official_service_identity_or_capability_drift")
    if not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME
               for x in service.get("layers", []) if isinstance(x, dict)):
        raise ValueError("exact_source_layer_missing")
    if layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPoint":
        raise ValueError("source_layer_name_or_geometry_drift")
    if layer.get("objectIdField") != "OBJECTID" or not isinstance(layer.get("maxRecordCount"), int) or layer["maxRecordCount"] < PAGE_SIZE:
        raise ValueError("source_oid_or_page_contract_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_missing")
    sr = layer.get("extent", {}).get("spatialReference", {})
    if (sr.get("latestWkid") or sr.get("wkid")) != 4269:
        raise ValueError("source_crs_drift")
    fields, schema_sha = _fields(layer)
    if expected_schema_sha256 is not None and schema_sha != expected_schema_sha256:
        raise ValueError("source_schema_drift")
    if docs["layer_after"].get("fields") is None or [_plain_field(x) for x in docs["layer_after"]["fields"]] != fields:
        raise ValueError("source_schema_changed_during_capture")
    if docs["service_before"] != docs["service_after"] or docs["layer_before"] != docs["layer_after"]:
        raise ValueError("source_metadata_changed_during_capture")
    if _stable_item(item) != _stable_item(docs.get("epa_item_after", item)):
        raise ValueError("source_item_identity_changed_during_capture")
    before_count, after_count = docs["count_before"].get("count"), docs["count_after"].get("count")
    ids = docs["ids_before"].get("objectIds")
    after_ids = docs["ids_after"].get("objectIds")
    seed_ids = docs["ids_seed"].get("objectIds")
    if (not isinstance(before_count, int) or before_count <= 0 or before_count != after_count
            or not isinstance(ids, list) or len(ids) != before_count or ids != after_ids
            or sorted(seed_ids or []) != sorted(ids) or len(set(ids)) != len(ids)):
        raise ValueError("source_count_or_id_roster_changed_during_capture")
    ids = sorted(ids)
    page_names = sorted(name for name in docs if name.startswith("page_"))
    expected_pages = (len(ids) + PAGE_SIZE - 1) // PAGE_SIZE
    if page_names != [f"page_{i:03d}" for i in range(expected_pages)]:
        raise ValueError("source_page_roster_incomplete")
    features = []
    for index, name in enumerate(page_names):
        page = docs[name]
        if "error" in page:
            raise ValueError("source_page_error")
        rows = page.get("features")
        page_ids = ids[index * PAGE_SIZE:(index + 1) * PAGE_SIZE]
        if (not isinstance(rows, list) or [row.get("attributes", {}).get("OBJECTID") for row in rows] != page_ids
                or (page.get("exceededTransferLimit") is True and len(rows) != len(page_ids))):
            raise ValueError("source_page_incomplete_or_unordered")
        if (page.get("spatialReference", {}).get("latestWkid") or page.get("spatialReference", {}).get("wkid")) != 4269:
            raise ValueError("source_page_crs_drift")
        features.extend(rows)
    expected_names = [field["name"] for field in fields]
    for row in features:
        attrs, geom = row.get("attributes"), row.get("geometry")
        if not isinstance(attrs, dict) or list(attrs) != expected_names or not isinstance(geom, dict) or set(geom) != {"x", "y"}:
            raise ValueError("source_attribute_or_geometry_shape_drift")
        if not all(isinstance(geom.get(coord), (int, float)) for coord in ("x", "y")):
            raise ValueError("source_geometry_coordinate_invalid")
        if any(field["type"] == "esriFieldTypeDate" and value is not None and not isinstance(value, int)
               for field in fields for value in [attrs[field["name"]]]):
            raise ValueError("source_date_value_not_native_epoch_milliseconds")
    if len(features) != before_count or [r["attributes"]["OBJECTID"] for r in features] != ids:
        raise ValueError("source_population_incomplete")
    records_sha = _sha(_canonical(features))
    geometry_sha = _sha(_canonical([[r["attributes"]["OBJECTID"], r["geometry"]["x"], r["geometry"]["y"]] for r in features]))
    audit = {"source_feature_count": before_count, "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": records_sha, "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha, "source_crs_epsg": 4269,
             "geometry_type": "Point", "null_geometry_count": 0, "unique_source_id_count": len(ids),
             "arcgis_item_id": ITEM_ID, "portal_owner": item["owner"],
             "portal_modified_epoch_ms": item.get("modified"),
             "portal_modified_is_source_release_date": False,
             "license_declaration": "EPA ArcGIS item declares EPA-produced data public domain and links usa.gov public-domain label; full terms retained in raw item snapshot."}
    return layer, fields, {"features": features, "audit": audit, "ids": ids}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    kind = field["type"]
    if kind == "esriFieldTypeOID": return pa.int64()
    if kind == "esriFieldTypeInteger": return pa.int32()
    if kind == "esriFieldTypeSmallInteger": return pa.int16()
    if kind == "esriFieldTypeDouble": return pa.float64()
    if kind == "esriFieldTypeSingle": return pa.float32()
    if kind == "esriFieldTypeString": return pa.string()
    if kind == "esriFieldTypeDate": return pa.int64()  # native epoch ms; timestamp conversion unapproved
    raise ValueError("unsupported_source_physical_type:" + str(kind))


def native_geoparquet(fields: list[dict], features: list[dict]) -> bytes:
    columns = {field["name"]: pa.array([row["attributes"][field["name"]] for row in features], type=_arrow_type(field))
               for field in fields}
    columns["geometry"] = pa.array([Point(row["geometry"]["x"], row["geometry"]["y"]).wkb for row in features], type=pa.binary())
    schema = pa.schema([pa.field(f["name"], _arrow_type(f), nullable=f.get("nullable", True)) for f in fields]
                       + [pa.field("geometry", pa.binary(), nullable=False)])
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{
        "encoding":"WKB", "geometry_types":["Point"], "crs":pyproj.CRS.from_epsg(4269).to_json_dict(), "edges":"planar"}}}
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(columns, schema=schema.with_metadata({b"geo":_canonical(geo)})), sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _identity() -> dict:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def _verify_first_candidate(storage: StagingStorageResource, prefix: str, state_key: str,
                            version: str, fingerprint: str, identity: Mapping[str, object],
                            audit: Mapping[str, object], expected_names: set[str],
                            current_parquet: bytes) -> dict[str, object]:
    """Verify and reuse the immutable first candidate for equal source/code identity."""
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict)
            or set(payload) != {"processing_identity", "source_record_sha256", "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise ValueError("cached_first_candidate_identity_or_source_changed")
    objects, receipts = payload["objects"], payload["capture_receipts"]
    if not isinstance(objects, dict) or set(objects) != expected_names or not isinstance(receipts, dict):
        raise ValueError("cached_candidate_roster_changed")
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
                            identity, audit["source_record_sha256"], audit["source_schema_sha256"],
                            objects, receipts)
    if objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != {"bytes":len(current_parquet),"sha256":_sha(current_parquet)}:
        raise ValueError("cached_native_candidate_changed")
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["source_feature_count"],
            "source_null_geometry_count":0,"candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,
            "source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
            "freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}


def capture_epa_rcra_tsd(storage: StagingStorageResource, *, version: str,
                         capture_directory: str | None = None) -> dict[str, object]:
    """Capture one complete native EPA layer to local-only unapproved staging."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(p.is_symlink() for p in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000:
        raise ValueError("disk_floor_before_acquisition")
    root = Path(capture_directory or "/tmp/hifld-native-reconstruction/74/captures").absolute()
    if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True, exist_ok=True)
    capture_root = root / f"{version}-{uuid4().hex}"
    captures: dict[str, tuple[bytes, dict]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=60, headers={"User-Agent":"hifld-next-datasets/epa-rcra-tsd-draft"}) as client:
            seed_body, seed_receipt = capture_get(client, LAYER_URL + "/query?" + urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"}), capture_root, "ids_seed", max_bytes=MAX_BODY_BYTES)
            seed = json.loads(seed_body).get("objectIds")
            if not isinstance(seed, list) or not seed:
                raise ValueError("source_seed_id_list_missing")
            captures["ids_seed"] = (seed_body, seed_receipt)
            for name, url in _urls(sorted(seed)):
                body, receipt = capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES)
                captures[name] = (body, receipt)
            item_after_body, item_after_receipt = capture_get(client, EPA_ITEM_URL, capture_root, "epa_item_after", max_bytes=MAX_BODY_BYTES)
            captures["epa_item_after"] = (item_after_body, item_after_receipt)
        layer, fields, extracted = validate_capture_documents({name: body for name, (body, _) in captures.items()})
    except Exception as exc:
        (capture_root / "capture_manifest.json").write_bytes(_canonical({"complete":False,"error_code":type(exc).__name__,"receipts":{n:r for n,(_,r) in captures.items()}})+b"\n")
        if captures:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, captures, type(exc).__name__)
        raise
    identity = _identity()
    parquet = native_geoparquet(fields, extracted["features"])
    relative_file = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {relative_file:{"bytes":len(parquet),"sha256":_sha(parquet)}}
    records = extracted["features"]
    license_html = json.loads(captures["epa_item"][0])["licenseInfo"]
    manifest = {
        "schema_version":1,"approved":False,"title":json.loads(captures["epa_item"][0])["title"],
        "description":json.loads(captures["epa_item"][0]).get("description"),"publisher":PUBLISHER,
        "agency":"Environmental Protection Agency","source_url":LAYER_URL,
        "license":EPA_PUBLIC_DOMAIN,"license_declaration_sha256":_sha(license_html.encode()),
        "source_modified":None,"date_issued":None,"date_modified":None,
        "processor":"HIFLD Next","code_commit":identity["code_commit"],
        "inputs":[{"input_id":name,"producer":PUBLISHER,"host":urlsplit(r["url"]).hostname,
          "role":"producer_publication","canonical_url":FRS_URL if name.startswith("epa_item") else LAYER_URL,
          "retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],
          "http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],
          "license":EPA_PUBLIC_DOMAIN,"attribution":"EPA item accessInformation identifies EPA FRS Support Team and RCRA; license and disclaimer are captured verbatim in the EPA item response.","release_date":None,
          "release_date_unknown_reason":"EPA item modification is an ArcGIS item timestamp, not a source release date; service exposes no verified release date."}
          for name,(_,r) in sorted(captures.items())],
        "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{
          "source_fields":"all published non-geometry attributes in producer order and declared types",
          "source_ids":"OBJECTID retained as int64 and REGISTRY_ID retained verbatim; OBJECTID documented nonpermanent",
          "geometry":"native point x/y retained as WKB in source NAD83 EPSG:4269, no reprojection or repair",
          "dates":"Esri epoch-millisecond values retained as int64; timestamp conversion unapproved",
          "source_count":len(records),"source_id_sha256":extracted["audit"]["source_id_sha256"],
          "candidate_status":"unapproved; release blocked pending accepted complete public baseline and field/ID/date contract"}}],
        "lineage":f"EPA_GEO item {ITEM_ID} identifies the EPA Facility Registry Service Support Team as producer and Resource Conservation and Recovery Act (RCRA) as program source; its description ties TSD facilities to RCRAInfo. EPA FRS: {FRS_URL}.",
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Preserves all native item attributes, identifiers and NAD83 point geometry. Baseline, date/ID contracts and release remain pending.","objects":[{"path":relative_file,**source_objects[relative_file]}]}}},
        "unsupported_output_formats":{"shapefile_zip":"No approved mapping for source text widths, null semantics, and date representation.","geopackage":"No approved source-to-public schema mapping.","pmtiles":"No approved display transformation."},
        "public_baseline":{"collection_url":f"https://storage.googleapis.com/hifld-next-datasets-prod/hifld/{DATASET_SLUG}/{DATASET_SLUG}/v1.0.0/collection.json",
          "complete_asset_roster_pinned":False,"provider_generation_and_md5_pinned":False,
          "full_ids_geometry_schema_coverage_value_comparison":False,
          "reason":"Public v1.0.0 collection and every linked asset must be generation/checksum pinned and compared before source/baseline acceptance."},
        "historical_archive":{"reference":"DataLumos 239683V1","used_as_source":False,
          "status":"historical_only; not substituted for the current official EPA service"},
        "source_license_declaration_html":license_html,
    }
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit = {**extracted["audit"],"source_fields":fields,"official_layer_name":LAYER_NAME,
             "feature_service_url":SERVICE_URL,"feature_layer_url":LAYER_URL,
             "source_program":PROGRAM_SOURCE,"source_dates_preserved_as_epoch_milliseconds":True,
             "date_conversion_approved":False,"objectid_permanence_accepted":False,
             "freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
             "rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"], audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    (capture_root / "capture_manifest.json").write_bytes(_canonical({"complete":True,"source_count":len(records),"source_record_sha256":audit["source_record_sha256"],"source_geometry_sha256":audit["source_geometry_sha256"],"source_id_sha256":audit["source_id_sha256"],"receipts":{n:r for n,(_,r) in sorted(captures.items())}})+b"\n")
    outputs = {relative_file:parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n",
               "metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root/f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt)+b"\n"
    ready = {"approved":False,"version":version,"objects":source_objects,"source_record_sha256":audit["source_record_sha256"],
             "source_count":len(records),"source_release_accepted":False,"schema_contract_accepted":False,
             "id_contract_accepted":False,"date_conversion_approved":False,"rights_accepted":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready)+b"\n"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        return _verify_first_candidate(storage,prefix,state_key,version,fingerprint,identity,audit,set(outputs),parquet)
    for name,body in outputs.items(): write_immutable(storage,f"{prefix}/{name}",body)
    descriptors = {name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    for name,desc in descriptors.items():
        key=f"{prefix}/{name}"; snap=storage.object_snapshot(key)
        if snap is None or snap.size != desc["bytes"] or storage.sha256_key(key) != desc["sha256"] or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_output_physical_readback_failed:"+name)
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,
        "source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],
        "capture_receipts":{n:r for n,(_,r) in captures.items()},"objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":len(records),
            "source_null_geometry_count":0,"candidate_ready":prefix+"/metadata/candidate_ready.json",
            "capture_directory":str(capture_root),"source_release_accepted":False,"schema_contract_accepted":False,
            "id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,
            "mapping_approved":False,"release_accepted":False}
