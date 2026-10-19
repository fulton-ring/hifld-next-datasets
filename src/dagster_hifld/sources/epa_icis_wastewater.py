"""Native, unapproved capture of EPA's ICIS wastewater treatment plant layer."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from urllib.parse import urlencode
from urllib.parse import urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import Point

from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance

ITEM_ID = "0895b107f9184e7cb31707767b506a64"
PORTAL_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"
SERVICE_URL = "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_Wastewater/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Integrated Compliance Information System (ICIS) Wastewater Treatment Plants"
DATASET_SLUG = "icis-wastewater-treatment-plants"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Environmental Protection Agency (EPA_GEO hosted FeatureServer)"
SCHEMA_SHA256 = "db38c1a62fdf82b3bd94acc81d88e776fe1b94f533b2fe8a8f28875222bb0b34"
PAGE_SIZE = 250


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _field_contract(layer: dict) -> list[dict]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(f, dict) for f in fields):
        raise ValueError("source_fields_missing")
    plain = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    if _sha(_canonical(plain)) != SCHEMA_SHA256:
        raise ValueError("source_schema_drift")
    return fields


def _arrow_type(field: dict) -> pa.DataType:
    kind = field["type"]
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeSingle": pa.float32(), "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64()}[kind]


def _native_parquet(fields: list[dict], features: list[dict]) -> bytes:
    cols = {f["name"]: pa.array([x["attributes"][f["name"]] for x in features], type=_arrow_type(f)) for f in fields}
    cols["geometry"] = pa.array([None if x["geometry"] is None else Point(x["geometry"]["x"], x["geometry"]["y"]).wkb for x in features], type=pa.binary())
    schema = pa.schema([pa.field(f["name"], _arrow_type(f), nullable=f.get("nullable", True)) for f in fields] + [pa.field("geometry", pa.binary(), nullable=True)])
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{"encoding":"WKB", "geometry_types":["Point"], "crs":pyproj.CRS.from_epsg(4269).to_json_dict(), "edges":"planar"}}}
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(cols, schema=schema.with_metadata({b"geo":_canonical(geo)})), sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def capture_epa_icis_wastewater(storage: StagingStorageResource, *, version: str,
                                capture_directory: str | None = None) -> dict:
    """Bracket a complete ID-paged capture and retain every source response."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute(); base.mkdir(parents=True, exist_ok=True)
    capture_root = Path(capture_directory or "/tmp/hifld-native-reconstruction/118/captures") / version
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict]] = {}
    def get(client, name, url, params=None):
        target = url + ("?" + urlencode(params) if params else "")
        body, receipt = capture_get(client, target, capture_root, name)
        data = json.loads(body)
        if not isinstance(data, dict) or "error" in data: raise ValueError("source_response_error:" + name)
        captures[name] = (body, receipt)
        return data
    try:
        with httpx.Client(follow_redirects=False, timeout=90, headers={"User-Agent":"hifld-next-datasets/epa-icis-wastewater"}) as client:
            portal_item = get(client, "portal_item", PORTAL_ITEM_URL)
            if (portal_item.get("id") != ITEM_ID or portal_item.get("owner") != "EPA_GEO"
                    or portal_item.get("title") != "EPA Facility Registry Service - Integrated Compliance Information System (ICIS) Wastewater Treatment Plants"
                    or portal_item.get("url") != SERVICE_URL or portal_item.get("access") != "public"):
                raise ValueError("source_portal_authority_or_publication_drift")
            service = get(client, "service_before", SERVICE_URL, {"f":"pjson"})
            layer = get(client, "layer_before", LAYER_URL, {"f":"pjson"})
            if service.get("serviceItemId") != ITEM_ID or layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPoint": raise ValueError("source_layer_identity_drift")
            fields = _field_contract(layer)
            has_z, has_m = layer.get("hasZ", False), layer.get("hasM", False)
            if type(has_z) is not bool or type(has_m) is not bool: raise ValueError("source_geometry_dimensions_unpinned")
            if has_z or has_m: raise ValueError("source_geometry_dimensions_unsupported")
            query_caps=layer.get("advancedQueryCapabilities",{})
            if not isinstance(query_caps,dict) or query_caps.get("supportsPagination") is not True or query_caps.get("supportsOrderBy") is not True:
                raise ValueError("source_ordered_pagination_not_supported")
            sr = layer.get("extent", {}).get("spatialReference", {})
            if sr.get("latestWkid") != 4269: raise ValueError("source_crs_drift")
            count = get(client, "count_before", LAYER_URL+"/query", {"where":"1=1","returnCountOnly":"true","f":"json"}).get("count")
            ids = get(client, "ids_before", LAYER_URL+"/query", {"where":"1=1","returnIdsOnly":"true","f":"json"}).get("objectIds")
            if type(count) is not int or count < 1 or not isinstance(ids,list) or len(ids)!=count or any(type(i) is not int for i in ids) or len(set(ids))!=count: raise ValueError("source_initial_id_roster_invalid")
            seed = sorted(ids); features=[]
            for page_no, offset in enumerate(range(0,count,PAGE_SIZE)):
                page = get(client, f"page_{page_no:03d}", LAYER_URL+"/query", {"where":"1=1","outFields":"*","returnGeometry":"true","returnZ":"true","returnM":"true","outSR":"4269","orderByFields":"OBJECTID ASC","resultOffset":offset,"resultRecordCount":PAGE_SIZE,"f":"json"})
                rows=page.get("features"); expected=min(PAGE_SIZE,count-offset)
                if not isinstance(rows,list) or len(rows)!=expected: raise ValueError("source_page_incomplete")
                if page.get("exceededTransferLimit") is True and offset+expected>=count:
                    raise ValueError("source_final_page_exceeded_transfer_limit")
                page_sr=page.get("spatialReference",{})
                if not isinstance(page_sr,dict) or page_sr.get("latestWkid",page_sr.get("wkid"))!=4269: raise ValueError("source_page_crs_drift")
                page_ids=[row.get("attributes",{}).get("OBJECTID") if isinstance(row,dict) else None for row in rows]
                if page_ids!=seed[offset:offset+expected]: raise ValueError("source_page_id_slice_mismatch")
                features.extend(rows)
            layer_after=get(client,"layer_after",LAYER_URL,{"f":"pjson"})
            service_after=get(client,"service_after",SERVICE_URL,{"f":"pjson"})
            count_after=get(client,"count_after",LAYER_URL+"/query",{"where":"1=1","returnCountOnly":"true","f":"json"}).get("count")
            ids_after=get(client,"ids_after",LAYER_URL+"/query",{"where":"1=1","returnIdsOnly":"true","f":"json"}).get("objectIds")
        if layer_after != layer or service_after != service or count_after != count or sorted(ids_after or []) != seed: raise ValueError("source_changed_during_capture")
        names=[f["name"] for f in fields]; got=[]; expected_geometry_keys={"x","y"} | ({"z"} if has_z else set()) | ({"m"} if has_m else set())
        for item in features:
            a=item.get("attributes"); g=item.get("geometry")
            if not isinstance(a,dict) or list(a)!=names: raise ValueError("source_record_schema_drift")
            if g is not None:
                if not isinstance(g,dict) or set(g)!=expected_geometry_keys: raise ValueError("source_geometry_dimensions_drift")
                if not all(type(g[k]) in (int,float) and math.isfinite(g[k]) for k in expected_geometry_keys): raise ValueError("source_coordinate_invalid")
            got.append(a.get("OBJECTID"))
        if len(features)!=count or sorted(got)!=seed or len(set(got))!=count: raise ValueError("source_feature_population_incomplete")
        features.sort(key=lambda x:x["attributes"]["OBJECTID"])
    except Exception as exc:
        if captures: quarantine_incomplete_capture(storage,capture_root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise
    record_sha=_sha(_canonical(features)); geom_sha=_sha(_canonical([[x["attributes"]["OBJECTID"],None if x["geometry"] is None else x["geometry"]] for x in features]))
    identity=processing_identity(Path(__file__).parents[3]); schema_sha=SCHEMA_SHA256
    native=_native_parquet(fields,features); version=content_derived_version(record_sha,geom_sha,schema_sha,identity); prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    null_geometry_count=sum(x["geometry"] is None for x in features)
    native_name=f"geoparquet/{DATASET_SLUG}.parquet"; audit={"feature_count":count,"source_id_sha256":_sha(_canonical(seed)),"source_record_sha256":record_sha,"source_geometry_sha256":geom_sha,"source_schema_sha256":schema_sha,"source_crs_epsg":4269,"geometry_type":"Point","null_geometry_count":null_geometry_count,"all_ids_captured_once":True,"ids_equal_before_after":True,"portal_item_id":ITEM_ID,"source_vintage":None,"source_vintage_note":"No source release vintage was asserted; the item modified timestamp is not evidence of source vintage."}
    manifest={"schema_version":1,"approved":False,"title":LAYER_NAME,"publisher":PUBLISHER,"agency":"Environmental Protection Agency","source_url":LAYER_URL,"source_release_date":None,"source_modified":None,"portal_item_modified_epoch_ms":portal_item["modified"],"portal_item_modified_is_source_release_date":False,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],"source_item_id":ITEM_ID,"source_vintage":None,"inputs":[{"input_id":n,"producer":PUBLISHER,"host":urlsplit(r["url"]).hostname,"role":"producer_publication","canonical_url":PORTAL_ITEM_URL.split("?",1)[0] if n=="portal_item" else SERVICE_URL,"retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],"http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],"license":"Unreviewed; no license claim made.","attribution":"EPA_GEO ArcGIS item attribution has not been reviewed.","release_date":None,"release_date_unknown_reason":"The service and item metadata do not establish an EPA producer release date."} for n,(_,r) in sorted(captures.items())],"transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"layer_id":LAYER_ID,"fields":"all 63 fields retained in source order and declared physical types","geometry":"native point coordinates retained as WKB in EPSG:4269","source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}}],"prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native Esri attributes, source IDs, and point coordinates are preserved without public mapping; approval gates remain open.","objects":[{"path":native_name,"bytes":len(native),"sha256":_sha(native)}]}}},"unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No approved display transformation."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs={native_name:native,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,receipt) in captures.items(): outputs.update({f"raw/{name}.body":body,f"raw/{name}.wire":(capture_root/f"{name}.wire").read_bytes(),f"raw/{name}.receipt.json":_canonical(receipt)+b"\n"})
    ready={"approved":False,"version":version,"objects":{native_name:{"bytes":len(native),"sha256":_sha(native)},**{k:{"bytes":len(v),"sha256":_sha(v)} for k,v in outputs.items() if k.startswith("raw/")}},"source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"]=_canonical(ready)+b"\n"
    state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint=_sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        sealed=json.loads(load_first_construction(storage,state_key,version,fingerprint))
        payload=sealed.get("payload")
        if not isinstance(payload,dict): raise ValueError("cached_first_candidate_payload_missing")
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,
            identity,record_sha,schema_sha,payload.get("objects",{}),payload.get("capture_receipts",{}))
        if set(payload["objects"])!=set(outputs): raise ValueError("cached_candidate_roster_changed")
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":count,
                "source_null_geometry_count":null_geometry_count,"candidate_ready":f"{prefix}/metadata/candidate_ready.json",
                "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
                "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,
                "release_accepted":False,"reused":True,"capture_directory":str(capture_root)}
    for name,body in outputs.items():
        key=f"{prefix}/{name}"
        write_immutable(storage,key,body)
        snapshot=storage.object_snapshot(key)
        if snapshot is None or snapshot.size!=len(body) or storage.sha256_key(key)!=_sha(body) or storage.object_snapshot(key)!=snapshot:
            raise ValueError("candidate_output_physical_readback_failed:"+name)
    descriptors={name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,
                         "source_record_sha256":record_sha,"source_schema_sha256":schema_sha,
                         "capture_receipts":{name:receipt for name,(_body,receipt) in captures.items()},
                         "objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":count,"source_null_geometry_count":null_geometry_count,"candidate_ready":f"{prefix}/metadata/candidate_ready.json","source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(capture_root)}
