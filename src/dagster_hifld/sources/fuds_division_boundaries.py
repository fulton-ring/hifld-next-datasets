"""Unapproved native capture of the USACE FUDS Program Division polygons."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import MultiPolygon, Polygon

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

DATASET_SLUG = "formerly-used-defense-sites-division-boundaries"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://geospatial.sec.usace.army.mil/server/rest/services/Military/FUDS_Data/MapServer"
LAYER_ID = 10
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "FUDS Program Division Boundaries"
PUBLISHER = "U.S. Army Corps of Engineers (USACE), Military/FUDS_Data MapServer"
PAGE_SIZE = 50
SCHEMA_SHA256 = "f9e51a73d089f8d6ee32215fc1cb7c270eb87f80757da887c3ac8a2d0c948ee2"
MAX_RESPONSE_BYTES = 128 * 1024 * 1024


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _schema(layer: dict) -> list[dict]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or not fields or any(not isinstance(f, dict) for f in fields):
        raise ValueError("source_fields_missing")
    plain = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f}
             for f in fields]
    if _sha(_canonical(plain)) != SCHEMA_SHA256:
        raise ValueError("source_schema_drift")
    return plain


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeSingle": pa.float32(), "esriFieldTypeString": pa.string(),
            "esriFieldTypeGUID": pa.string(), "esriFieldTypeGlobalID": pa.string(),
            "esriFieldTypeDate": pa.int64(), "esriFieldTypeBlob": pa.binary()}[field["type"]]


def _ring_area(ring: list[list[float]]) -> float:
    x0, y0 = ring[0]
    return math.fsum((ring[i][0]-x0)*(ring[i+1][1]-y0) - (ring[i+1][0]-x0)*(ring[i][1]-y0)
                     for i in range(len(ring)-1)) / 2


def _esri_polygon(geometry: dict) -> MultiPolygon:
    """Interpret native Esri ring direction without repairing or simplifying."""
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    shells, holes = [], []
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
            raise ValueError("source_polygon_ring_invalid")
        if any(not isinstance(point, list) or len(point) != 2 or
               any(type(value) not in (int, float) or not math.isfinite(value) for value in point)
               for point in ring):
            raise ValueError("source_polygon_coordinate_dimensions_or_values_invalid")
        poly = Polygon(ring)
        if not poly.is_valid or poly.area == 0:
            raise ValueError("source_polygon_ring_invalid_geometry")
        (shells if _ring_area(ring) < 0 else holes).append((ring, poly))
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    assigned: dict[int, list] = {i: [] for i in range(len(shells))}
    for ring, hole in holes:
        parents = [(p.area, i) for i, (_shell, p) in enumerate(shells) if p.contains(hole)]
        if not parents:
            raise ValueError("source_polygon_hole_without_containing_shell")
        assigned[min(parents)[1]].append(ring)
    result = MultiPolygon([Polygon(shell, assigned[i]) for i, (shell, _p) in enumerate(shells)])
    if not result.is_valid:
        raise ValueError("source_polygon_invalid_no_repair_permitted")
    return result


def _native_parquet(fields: list[dict], features: list[dict]) -> bytes:
    # SHAPE is the geometry and ArcGIS does not return it as an attribute.
    # Blob values are absent from query responses. The schema declaration and
    # complete responses are retained; no unavailable value is invented.
    attr_fields = [f for f in fields if f["type"] not in {"esriFieldTypeGeometry", "esriFieldTypeBlob"}]
    names = [f["name"] for f in attr_fields]
    cols = {f["name"]: pa.array([x["attributes"][f["name"]] for x in features], type=_arrow_type(f))
            for f in attr_fields}
    geoms = [_esri_polygon(x["geometry"]) for x in features]
    cols["geometry"] = pa.array([g.wkb for g in geoms], type=pa.binary())
    schema = pa.schema([pa.field(f["name"], _arrow_type(f), nullable=f.get("nullable", True))
                        for f in attr_fields] + [pa.field("geometry", pa.binary(), nullable=False)])
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{
        "encoding":"WKB", "geometry_types":["MultiPolygon"], "crs":pyproj.CRS.from_epsg(4326).to_json_dict(), "edges":"planar"}}}
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(cols, schema=schema.with_metadata({b"geo":_canonical(geo)})), sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def capture_fuds_division_boundaries(storage: StagingStorageResource, *, version: str,
                                     capture_directory: str | None = None) -> dict:
    """Bracket the complete, ordered layer capture and seal an unapproved candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    root = Path(capture_directory or "/tmp/hifld-native-reconstruction/95/captures") / version
    root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict]] = {}
    def get(client, name, url, params=None):
        target = url + ("?" + urlencode(params) if params else "")
        body, receipt = capture_get(client, target, root, name, max_bytes=MAX_RESPONSE_BYTES)
        value = json.loads(body)
        if not isinstance(value, dict) or "error" in value: raise ValueError("source_response_error:" + name)
        captures[name] = body, receipt
        return value
    try:
        with httpx.Client(follow_redirects=False, timeout=90, headers={"User-Agent":"hifld-next-datasets/fuds-divisions"}) as client:
            service = get(client, "service_before", SERVICE_URL, {"f":"pjson"})
            layer = get(client, "layer_before", LAYER_URL, {"f":"pjson"})
            if (layer.get("name") != LAYER_NAME or layer.get("id") != LAYER_ID
                    or layer.get("geometryType") != "esriGeometryPolygon"):
                raise ValueError("source_layer_identity_drift")
            fields = layer["fields"]
            schema = _schema(layer)
            sr = layer.get("extent", {}).get("spatialReference", {})
            if sr.get("latestWkid", sr.get("wkid")) != 4326: raise ValueError("source_crs_drift")
            caps = layer.get("advancedQueryCapabilities", {})
            if caps.get("supportsPagination") is not True or caps.get("supportsOrderBy") is not True:
                raise ValueError("source_ordered_pagination_not_supported")
            count = get(client, "count_before", LAYER_URL+"/query", {"where":"1=1","returnCountOnly":"true","f":"json"}).get("count")
            ids = get(client, "ids_before", LAYER_URL+"/query", {"where":"1=1","returnIdsOnly":"true","f":"json"}).get("objectIds")
            if type(count) is not int or count < 1 or not isinstance(ids,list) or len(ids)!=count or any(type(i) is not int for i in ids) or len(set(ids))!=count:
                raise ValueError("source_initial_id_roster_invalid")
            seed = sorted(ids); features=[]
            for page_no, offset in enumerate(range(0, count, PAGE_SIZE)):
                page = get(client, f"page_{page_no:03d}", LAYER_URL+"/query", {
                    "where":"1=1", "outFields":"*", "returnGeometry":"true", "returnZ":"true", "returnM":"true",
                    "outSR":"4326", "orderByFields":"OBJECTID ASC", "resultOffset":offset,
                    "resultRecordCount":PAGE_SIZE, "f":"json"})
                rows = page.get("features"); expected = min(PAGE_SIZE, count-offset)
                if not isinstance(rows,list) or len(rows)!=expected: raise ValueError("source_page_incomplete")
                exceeded=page.get("exceededTransferLimit",False)
                if type(exceeded) is not bool: raise ValueError("source_page_transfer_limit_flag_invalid")
                if exceeded and (len(rows)!=PAGE_SIZE or offset+expected>=count):
                    raise ValueError("source_page_exceeded_transfer_limit")
                if page.get("spatialReference",{}).get("latestWkid",page.get("spatialReference",{}).get("wkid")) != 4326:
                    raise ValueError("source_page_crs_drift")
                if [r.get("attributes",{}).get("OBJECTID") for r in rows] != seed[offset:offset+expected]:
                    raise ValueError("source_page_id_slice_mismatch")
                features.extend(rows)
            after = get(client, "layer_after", LAYER_URL, {"f":"pjson"})
            service_after = get(client, "service_after", SERVICE_URL, {"f":"pjson"})
            count_after = get(client,"count_after",LAYER_URL+"/query",{"where":"1=1","returnCountOnly":"true","f":"json"}).get("count")
            ids_after = get(client,"ids_after",LAYER_URL+"/query",{"where":"1=1","returnIdsOnly":"true","f":"json"}).get("objectIds")
        if layer != after or service != service_after or count_after != count or sorted(ids_after or []) != seed:
            raise ValueError("source_changed_during_capture")
        field_names=[f["name"] for f in fields if f["type"] not in {"esriFieldTypeGeometry","esriFieldTypeBlob"}]
        if any(not isinstance(x.get("attributes"),dict) or list(x["attributes"]) != field_names for x in features):
            raise ValueError("source_record_schema_drift")
        if sorted(x["attributes"]["OBJECTID"] for x in features) != seed: raise ValueError("source_population_incomplete")
        features.sort(key=lambda x:x["attributes"]["OBJECTID"])
        geom_digest=_sha(_canonical([[x["attributes"]["OBJECTID"],x["geometry"]] for x in features]))
        record_digest=_sha(_canonical([x["attributes"] for x in features]))
        identity=processing_identity(Path(__file__).parents[3]); schema_digest=_sha(_canonical(schema))
        native=_native_parquet(fields,features); version=content_derived_version(record_digest,geom_digest,schema_digest,identity)
        prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version); native_name=f"geoparquet/{DATASET_SLUG}.parquet"
        audit={"feature_count":count,"source_id_sha256":_sha(_canonical(seed)),"source_record_sha256":record_digest,
            "source_geometry_sha256":geom_digest,"source_schema_sha256":schema_digest,"source_crs_epsg":4326,
            "geometry_type":"Polygon","all_ids_captured_once":True,"ids_equal_before_after":True,
            "source_release_date":None,"source_release_date_unknown_reason":"Service metadata provides no producer release date.",
            "declared_field_values_not_returned_by_feature_query":{"SE_ANNO_CAD_DATA":{
                "source_type":"esriFieldTypeBlob","value_status":"unknown; omitted from all captured feature attributes",
                "raw_response_retained":True,"substituted_null":False}}}
        manifest={"schema_version":1,"processor":"HIFLD Next","approved":False,"title":LAYER_NAME,"publisher":PUBLISHER,
            "agency":"U.S. Army Corps of Engineers","source_url":LAYER_URL,"source_release_date":None,
            "date_issued":None,"date_modified":None,"code_commit":identity["code_commit"],"inputs":[
                {"input_id":n,"producer":PUBLISHER,"host":urlsplit(r["url"]).hostname,"role":"producer_publication",
                 "canonical_url":LAYER_URL,"retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],
                 "http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],"license":"Unreviewed; no license claim made.",
                 "attribution":"USACE layer copyright/attribution has not been reviewed.","role":"producer_publication","release_date":None,
                 "release_date_unknown_reason":"USACE service responses do not provide a producer release date."}
                for n,(_b,r) in sorted(captures.items())],
            "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{
                "layer_id":LAYER_ID,"attributes":"all query-returned source attributes retained in source order/types/values",
                "geometry":"Esri rings interpreted as Polygon/MultiPolygon WKB in EPSG:4326; no repair or simplification",
                "blob_field":"SE_ANNO_CAD_DATA is not returned by Esri feature queries; verbatim raw responses retained",
                "approval_gates_open":True}}],
            "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Candidate preserves all query-returned FUDS fields and native vertices; the declared but unreturned BLOB value remains unknown and approval gates remain open.","objects":[{"path":native_name,"bytes":len(native),"sha256":_sha(native)}]}}}}
        validate_release_provenance(manifest); validate_output_policy(manifest)
        outputs={native_name:native,"metadata/source_manifest.json":_canonical(manifest)+b"\n",
            "metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
        for name,(body,receipt) in captures.items():
            outputs.update({f"raw/{name}.body":body,f"raw/{name}.wire":(root/f"{name}.wire").read_bytes(),f"raw/{name}.receipt.json":_canonical(receipt)+b"\n"})
        descriptors={name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
        ready={"approved":False,"version":version,"objects":descriptors,"source_release_accepted":False,
            "freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
            "rights_accepted":False,"mapping_approved":False,"release_accepted":False}
        outputs["metadata/candidate_ready.json"]=_canonical(ready)+b"\n"
        state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"; fingerprint=_sha(_canonical(identity))
        if storage.object_snapshot(state_key) is not None:
            sealed=json.loads(load_first_construction(storage,state_key,version,fingerprint)); payload=sealed.get("payload")
            if not isinstance(payload,dict): raise ValueError("cached_first_candidate_payload_missing")
            verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,
                identity,record_digest,schema_digest,payload.get("objects",{}),payload.get("capture_receipts",{}))
            if set(payload["objects"]) != set(outputs): raise ValueError("cached_candidate_roster_changed")
            return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":count,
                "candidate_ready":f"{prefix}/metadata/candidate_ready.json","release_accepted":False,"reused":True}
        for name,body in outputs.items():
            key=f"{prefix}/{name}"; write_immutable(storage,key,body); snap=storage.object_snapshot(key)
            if snap is None or snap.size != len(body) or storage.sha256_key(key) != _sha(body) or storage.object_snapshot(key) != snap:
                raise ValueError("candidate_output_physical_readback_failed:"+name)
        descriptors={name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
        seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,
            "source_record_sha256":record_digest,"source_schema_sha256":schema_digest,
            "capture_receipts":{n:r for n,(_b,r) in captures.items()},"objects":descriptors})
        validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":count,
            "candidate_ready":f"{prefix}/metadata/candidate_ready.json","release_accepted":False,"reused":False}
    except Exception as exc:
        if captures: quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise
