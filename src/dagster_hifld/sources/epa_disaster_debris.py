"""Native capture and unapproved candidate construction for EPA DDRT."""
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
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable,
)

EPA_PAGE = "https://www.epa.gov/disaster-debris/disaster-debris-recovery-tool"
DOC_URL = "https://www.epa.gov/system/files/documents/2026-09/usepa_ddrt_info.pdf"
ITEM_ID = "9bb31b863df0491aa209cc91d15defb1"
SERVICE_URL = "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/EPA_Disaster_Debris_Recovery_Data/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Disaster_Debris_Recovery_Data"
DATASET_SLUG = "epa-disaster-debris-recovery-data"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Environmental Protection Agency (EPA_GEO hosted FeatureServer)"
SOURCE_RELEASE = "2026-08"
SOURCE_RELEASE_NOTE = "EPA landing page states the DDRT national data layer and web map were updated in August 2026; ArcGIS item modified and layer edit timestamps are portal edit events, not source release dates."
PAGE_SIZE = 10000
MAX_BODY_BYTES = 64_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _field_profile(field: Mapping[str, object]) -> dict[str, object]:
    keys = ("name", "type", "alias", "length", "nullable", "domain", "defaultValue")
    return {k: field[k] for k in keys if k in field}


def _portal_publication_snapshot(item: Mapping[str, object]) -> dict[str, object]:
    """Exclude only ArcGIS's request-incremented view counter from item stability."""
    return {key: value for key, value in item.items() if key != "numViews"}


def _source_layer_stable(before: Mapping[str, object], after: Mapping[str, object]) -> bool:
    """Require the full layer/service metadata snapshot to remain unchanged."""
    return dict(before) == dict(after)


def _validated_page_rows(page: Mapping[str, object], expected_ids: list[int], *, has_following: bool) -> list[dict[str, object]]:
    """Accept ArcGIS continuation flags only when this full ordered ID slice is complete."""
    if "error" in page:
        raise ValueError("source_page_error_or_truncated")
    rows = page.get("features")
    if not isinstance(rows, list):
        raise ValueError("source_page_missing_features")
    page_ids = [row.get("attributes", {}).get("OBJECTID") if isinstance(row, dict) else None for row in rows]
    if (any(type(value) is not int for value in page_ids)
            or page_ids != expected_ids or len(set(page_ids)) != len(page_ids)):
        raise ValueError("source_page_id_slice_incomplete_or_duplicated")
    exceeded = page.get("exceededTransferLimit")
    if exceeded is not None and type(exceeded) is not bool:
        raise ValueError("source_page_transfer_limit_flag_invalid")
    if exceeded is True and (len(rows) != PAGE_SIZE or not has_following):
        raise ValueError("source_page_truncated_with_transfer_limit_flag")
    return rows


def _type(field: Mapping[str, object]) -> pa.DataType:
    kind = field["type"]
    if kind == "esriFieldTypeOID": return pa.int64()
    if kind == "esriFieldTypeInteger": return pa.int32()
    if kind == "esriFieldTypeSmallInteger": return pa.int16()
    if kind == "esriFieldTypeDouble": return pa.float64()
    if kind in ("esriFieldTypeString", "esriFieldTypeGlobalID"): return pa.string()
    raise ValueError("unsupported_source_physical_type:" + str(kind))


def _urls(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("epa_landing", EPA_PAGE), ("epa_document", DOC_URL),
        ("portal_item", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"),
        ("service_before", SERVICE_URL + "?f=pjson"), ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", LAYER_URL + "/query?" + urlencode({"where":"1=1","returnCountOnly":"true","f":"json"})),
        ("ids_before", LAYER_URL + "/query?" + urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"})),
    ]
    for offset in range(0, len(ids), PAGE_SIZE):
        params = {"where":"1=1", "resultOffset":offset, "resultRecordCount":PAGE_SIZE,
                  "orderByFields":"OBJECTID ASC", "outFields":"*",
                  "returnGeometry":"true", "returnZ":"true", "returnM":"true", "f":"json"}
        urls.append((f"page_{offset // PAGE_SIZE:03d}", LAYER_URL + "/query?" + urlencode(params)))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"), ("service_after", SERVICE_URL + "?f=pjson"),
        ("portal_item_after", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"),
        ("count_after", LAYER_URL + "/query?" + urlencode({"where":"1=1","returnCountOnly":"true","f":"json"})),
        ("ids_after", LAYER_URL + "/query?" + urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"})),
    ])
    return urls


def _validate(captures: dict[str, tuple[bytes, dict[str, object]]]) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    docs = {n: json.loads(body) for n, (body, _) in captures.items() if n not in ("epa_landing", "epa_document")}
    portal, service, layer = docs["portal_item"], docs["service_before"], docs["layer_before"]
    landing = captures["epa_landing"][0]
    if (b"updated in August 2026" not in landing and b"updated in August" not in landing) or ITEM_ID.encode() not in landing:
        raise ValueError("official_epa_publication_or_release_evidence_drift")
    if not captures["epa_document"][0].startswith(b"%PDF-") or b"EPA" not in captures["epa_document"][0]:
        raise ValueError("official_epa_document_identity_drift")
    if (portal.get("id") != ITEM_ID or portal.get("owner") != "EPA_GEO" or portal.get("access") != "public"
            or portal.get("type") != "Feature Service" or portal.get("url") != SERVICE_URL
            or portal.get("title") != "National Disaster Debris Recovery Facilities (EPA 2026)"):
        raise ValueError("epa_portal_identity_or_delegation_drift")
    if _portal_publication_snapshot(portal) != _portal_publication_snapshot(docs["portal_item_after"]):
        raise ValueError("epa_portal_metadata_changed_during_capture")
    if service.get("serviceItemId") != ITEM_ID or not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME for x in service.get("layers",[]) if isinstance(x,dict)):
        raise ValueError("epa_service_layer_identity_drift")
    if (layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("hasZ") is not False or layer.get("hasM") is not False):
        raise ValueError("epa_layer_schema_or_geometry_drift")
    if layer.get("objectIdField") != "OBJECTID":
        raise ValueError("source_object_id_field_drift")
    if layer.get("extent",{}).get("spatialReference",{}).get("latestWkid") != 3857:
        raise ValueError("source_crs_drift")
    fields = layer.get("fields")
    if not isinstance(fields,list) or not fields or any(not isinstance(f,dict) for f in fields): raise ValueError("source_field_roster_drift")
    profile = [_field_profile(f) for f in fields]
    if docs["layer_after"].get("name") != LAYER_NAME or [_field_profile(f) for f in docs["layer_after"].get("fields",[])] != profile:
        raise ValueError("source_schema_changed_during_capture")
    if (not _source_layer_stable(docs["layer_before"], docs["layer_after"])
            or not _source_layer_stable(docs["service_before"], docs["service_after"])):
        raise ValueError("source_metadata_changed_during_capture")
    before, after, seed = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds"), docs["ids_seed"].get("objectIds")
    count = docs["count_before"].get("count")
    if not isinstance(count,int) or count <= 0 or docs["count_after"].get("count") != count: raise ValueError("source_count_changed_during_capture")
    if not isinstance(before,list) or before != after or not isinstance(seed,list) or sorted(seed) != sorted(before) or len(set(before)) != count:
        raise ValueError("source_id_population_changed_or_invalid")
    features=[]
    names=[f["name"] for f in fields]
    source_ids=sorted(before)
    expected_pages=[f"page_{i:03d}" for i in range((count+PAGE_SIZE-1)//PAGE_SIZE)]
    page_names=sorted(x for x in captures if x.startswith("page_"))
    if page_names != expected_pages:
        raise ValueError("source_page_roster_incomplete_or_unexpected")
    for page_index,n in enumerate(page_names):
        page=docs[n]
        if page.get("spatialReference",{}).get("latestWkid") != 3857: raise ValueError("source_page_crs_drift")
        expected_ids=source_ids[page_index*PAGE_SIZE:(page_index+1)*PAGE_SIZE]
        rows=_validated_page_rows(page,expected_ids,has_following=page_index < len(page_names)-1)
        features.extend(rows)
    if len(features)!=count: raise ValueError("source_feature_count_incomplete")
    for feature in features:
        attrs=feature.get("attributes")
        geom=feature.get("geometry")
        if not isinstance(attrs,dict) or list(attrs)!=names:
            raise ValueError("source_property_roster_or_order_drift")
        if geom is not None and (not isinstance(geom,dict) or set(geom) != {"x","y"}):
            raise ValueError("source_native_geometry_unrepresentable")
    ids=[f["attributes"].get("OBJECTID") for f in features]
    if sorted(ids)!=sorted(before) or len(set(ids))!=count: raise ValueError("source_page_ids_incomplete_or_duplicated")
    features.sort(key=lambda f:f["attributes"]["OBJECTID"])
    schema_sha=_sha(_canonical(profile)); records_sha=_sha(_canonical(features))
    geom_audit=[]
    for f in features:
        g=f.get("geometry")
        geom_audit.append([f["attributes"]["OBJECTID"],None if g is None else g])
    geometry_sha=_sha(_canonical(geom_audit))
    nulls=sum(f.get("geometry") is None for f in features)
    invalid=sum(1 for f in features if isinstance(f.get("geometry"),dict) and not all(isinstance(f["geometry"].get(k),(int,float)) for k in ("x","y")))
    identifier_fields=[f["name"] for f in fields if f["name"] in {"OBJECTID","GlobalID","FRS_ID","SOR_ID"}]
    identifier_manifest={}
    for name in identifier_fields:
        values=[feature["attributes"][name] for feature in features]
        nonnull=[value for value in values if value is not None]
        identifier_manifest[name]={"count":len(values),"null_count":len(values)-len(nonnull),
                                   "distinct_nonnull_count":len(set(nonnull)),"sha256":_sha(_canonical(values))}
    audit={"source_feature_count":count,"source_id_count":len(set(ids)),"source_id_sha256":_sha(_canonical(sorted(ids))),
           "source_identifier_manifest":identifier_manifest,
           "source_record_sha256":records_sha,"source_geometry_sha256":geometry_sha,"source_schema_sha256":schema_sha,
           "source_fields":profile,"source_crs_epsg":3857,"source_geometry_type":"Point","source_null_geometry_count":nulls,
           "source_invalid_coordinate_count":invalid,"portal_item_id":ITEM_ID,"portal_owner":portal["owner"],
           "portal_modified_epoch_ms":portal.get("modified"),"portal_modified_is_source_release_date":False,
           "portal_volatile_fields_excluded_from_stability_check":["numViews"],
           "layer_editing_info":layer.get("editingInfo"),"layer_edit_timestamps_are_source_release_dates":False,
           "source_release_month":SOURCE_RELEASE,"source_release_evidence":EPA_PAGE}
    return fields,features,audit


def _native_parquet(fields: list[dict[str,object]], features: list[dict[str,object]]) -> bytes:
    columns={}
    schema_fields=[]
    for f in fields:
        typ=_type(f); name=f["name"]
        columns[name]=pa.array([r["attributes"][name] for r in features], type=typ)
        schema_fields.append(pa.field(name,typ,nullable=f.get("nullable",True)))
    geom_values=[]
    for r in features:
        g=r.get("geometry")
        geom_values.append(None if g is None else Point(g["x"],g["y"]).wkb)
    columns["geometry"]=pa.array(geom_values,type=pa.binary())
    schema_fields.append(pa.field("geometry",pa.binary(),nullable=True))
    geo={"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":["Point"],"crs":pyproj.CRS.from_epsg(3857).to_json_dict(),"edges":"planar"}}}
    table=pa.Table.from_pydict(columns,schema=pa.schema(schema_fields,metadata={b"geo":_canonical(geo)}))
    out=pa.BufferOutputStream(); pq.write_table(table,out,compression="zstd"); return out.getvalue().to_pybytes()


def capture_epa_disaster_debris(storage: StagingStorageResource, *, version: str, capture_directory: str | None = None) -> dict[str,object]:
    """Capture the live EPA layer, or refuse all outputs and retain partial receipts in quarantine."""
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version): raise ValueError("invalid_version")
    base=Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base,*base.parents)): raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(base).free<256_000_000: raise ValueError("disk_floor_before_acquisition")
    attempt=Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/65/captures")
    root=(attempt/f"{version}-{uuid4().hex}").absolute()
    if ".." in root.parts or any(parent.is_symlink() for parent in (root,*root.parents)): raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True,exist_ok=True); captures={}
    try:
        with httpx.Client(follow_redirects=False,timeout=90,headers={"User-Agent":"hifld-next-datasets/epa-ddrt-draft"}) as client:
            layer_url=LAYER_URL+"/query?"+urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"})
            body,receipt=capture_get(client,layer_url,root,"ids_seed",max_bytes=MAX_BODY_BYTES)
            ids=json.loads(body).get("objectIds")
            if not isinstance(ids,list) or not ids: raise ValueError("source_seed_ids_unavailable")
            captures["ids_seed"]=(body,receipt)
            for name,url in _urls(ids):
                body,receipt=capture_get(client,url,root,name,max_bytes=MAX_BODY_BYTES)
                captures[name]=(body,receipt)
        fields,features,audit=_validate(captures)
    except Exception as exc:
        receipts={}
        for path in sorted(root.glob("*.receipt.json")):
            try:
                name=path.name.removesuffix(".receipt.json")
                receipts[name]=json.loads(path.read_bytes())
                if name not in captures:
                    captures[name]=((root/f"{name}.body").read_bytes(),receipts[name])
            except (ValueError,OSError): receipts[path.name]={"receipt_readback_failed":True}
        (root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":False,"error_code":type(exc).__name__,"receipts":receipts})+b"\n")
        if captures: quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise
    return _build_candidate(storage,version,root,captures,fields,features,audit)


def replay_epa_disaster_debris(storage: StagingStorageResource, *, version: str, capture_directory: str) -> dict[str, object]:
    """Rebuild from a complete immutable raw capture after verifying each body and wire receipt."""
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    root=Path(capture_directory).absolute()
    if ".." in root.parts or any(parent.is_symlink() for parent in (root,*root.parents)):
        raise ValueError("unsafe_capture_directory")
    manifest=json.loads((root/"capture_manifest.json").read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"),dict):
        raise ValueError("replay_capture_not_complete")
    captures={}
    for name,receipt in manifest["receipts"].items():
        body=(root/f"{name}.body").read_bytes(); wire=(root/f"{name}.wire").read_bytes()
        if (receipt.get("complete_body") is not True or receipt.get("status") != 200
                or receipt.get("final_url") != receipt.get("url")
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
            raise ValueError("replay_capture_receipt_mismatch:"+name)
        if (root/f"{name}.receipt.json").read_bytes() != _canonical(receipt)+b"\n":
            raise ValueError("replay_capture_receipt_file_mismatch:"+name)
        captures[name]=(body,receipt)
    try:
        fields,features,audit=_validate(captures)
    except Exception as exc:
        quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise
    return _build_candidate(storage,version,root,captures,fields,features,audit)


def _build_candidate(storage: StagingStorageResource, version: str, root: Path, captures, fields, features, audit) -> dict[str, object]:
    identity=processing_identity(Path(__file__).parents[3])
    try:
        parquet=_native_parquet(fields,features)
    except Exception as exc:
        quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise
    objpath=f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects={objpath:{"bytes":len(parquet),"sha256":_sha(parquet)}}
    manifest={"schema_version":1,"approved":False,"title":"EPA Disaster Debris Recovery Data","description":"Exact native EPA_GEO national Disaster Debris Recovery Tool point layer. EPA landing page states August 2026 update; release approval remains pending.","publisher":PUBLISHER,"agency":"Environmental Protection Agency","source_url":LAYER_URL,"license":"https://edg.epa.gov/EPA_Data_License.html","source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
      "inputs":[{"input_id":name,"producer":PUBLISHER,"host":urlsplit(rc["url"]).hostname,"role":"producer_publication","canonical_url":EPA_PAGE if name.startswith("epa_") else LAYER_URL,"retrieval_url":rc["url"],"observed_final_url":rc["final_url"],"retrieved_at":rc["observed_at"],"http_status":rc["status"],"bytes":rc["bytes"],"sha256":rc["sha256"],"license":"https://edg.epa.gov/EPA_Data_License.html","attribution":"U.S. EPA Disaster Debris Recovery Tool; EPA_GEO publishes the linked public layer. Rights review remains pending.","release_date":None,"release_date_unknown_reason":SOURCE_RELEASE_NOTE} for name,(_,rc) in sorted(captures.items())],
      "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"source_fields":"all source fields and producer-declared physical types, order retained","source_ids":"OBJECTID and GlobalID retained unchanged","geometry":"native point x/y retained in source EPSG:3857; null/invalid geometries are reported and never filtered; no repair/reprojection","candidate_status":"unapproved; release prohibited pending quality, rights, and schema contract acceptance"}}],
      "lineage":f"EPA landing page {EPA_PAGE} links public EPA_GEO item {ITEM_ID}, which links exact FeatureServer layer 0. EPA landing records the August 2026 national update. ArcGIS modified/edit timestamps are preserved as portal edits and are not used as release dates.",
      "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native EPA point properties and IDs retained; approval remains pending.","objects":[{"path":objpath,**source_objects[objpath]}]}}},"unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No approved display transformation."}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    version=content_derived_version(audit["source_record_sha256"],audit["source_geometry_sha256"],audit["source_schema_sha256"],identity)
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version); state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    (root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,"error_code":None,"source_feature_count":audit["source_feature_count"],"source_record_sha256":audit["source_record_sha256"],"source_geometry_sha256":audit["source_geometry_sha256"],"receipts":{n:r for n,(_,r) in sorted(captures.items())}})+b"\n")
    outputs={objpath:parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"]=body; outputs[f"raw/{name}.wire"]=(root/f"{name}.wire").read_bytes(); outputs[f"raw/{name}.receipt.json"]=_canonical(receipt)+b"\n"
    ready={"approved":False,"version":version,"objects":source_objects,"source_record_sha256":audit["source_record_sha256"],"source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"]=_canonical(ready)+b"\n"
    fp=_sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle=json.loads(load_first_construction(storage,state_key,version,fp)); payload=bundle.get("payload")
        if not isinstance(payload,dict): raise ValueError("cached_first_candidate_payload_missing")
        if not isinstance(payload.get("objects"),dict) or set(payload["objects"]) != set(outputs):
            raise ValueError("cached_candidate_roster_changed")
        if payload["objects"].get(objpath) != {"bytes":len(parquet),"sha256":_sha(parquet)}:
            raise ValueError("cached_native_candidate_changed")
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fp,identity,audit["source_record_sha256"],audit["source_schema_sha256"],payload["objects"],payload["capture_receipts"])
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["source_feature_count"],"source_id_count":audit["source_id_count"],"source_null_geometry_count":audit["source_null_geometry_count"],"source_invalid_coordinate_count":audit["source_invalid_coordinate_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,"capture_directory":str(root),"source_release_accepted":False,"release_accepted":False}
    for name,body in outputs.items(): write_immutable(storage,f"{prefix}/{name}",body)
    descriptors={name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    for name,desc in descriptors.items():
        snap=storage.object_snapshot(f"{prefix}/{name}")
        if snap is None or snap.size!=desc["bytes"] or storage.sha256_key(f"{prefix}/{name}")!=desc["sha256"] or storage.object_snapshot(f"{prefix}/{name}")!=snap: raise ValueError("candidate_output_readback_failed:"+name)
    payload={"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":{n:r for n,(_,r) in captures.items()},"objects":descriptors}
    seal_ready_candidate(storage,state_key,version,fp,payload)
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["source_feature_count"],"source_id_count":audit["source_id_count"],"source_null_geometry_count":audit["source_null_geometry_count"],"source_invalid_coordinate_count":audit["source_invalid_coordinate_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json","source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(root)}
