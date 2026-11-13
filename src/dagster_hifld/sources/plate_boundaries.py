"""Immutable native-CRS capture and typed GeoParquet candidate for official USGS plates."""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import struct
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from shapely import from_wkb

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.validation import sha256_file

DATASET_SLUG = FILE_SLUG = "plate-boundaries"
PUBLISHER = "U.S. Geological Survey Earthquake Hazards Program"
SERVICE_URL = "https://earthquake.usgs.gov/arcgis/rest/services/eq/map_plateboundaries/MapServer"
LAYER_ID = 1
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
COUNT = 1175
EXPECTED_ID_SHA = "16439bcc2d7b9a12e48447a351cd4d769a8e1ad0b4776ae59c2c4963b9b6b8e0"
BATCH_SIZE = 100
MAX_CAPTURE_BYTES = 64_000_000
EXPECTED_FIELDS = [
    {"name":"OBJECTID","type":"esriFieldTypeOID","alias":"OBJECTID","domain":None},
    {"name":"Shape","type":"esriFieldTypeGeometry","alias":"Shape","domain":None},
    {"name":"NAME","type":"esriFieldTypeString","alias":"NAME","length":200,"domain":None},
    {"name":"LABEL","type":"esriFieldTypeString","alias":"LABEL","length":50,"domain":None},
    {"name":"Shape_Length","type":"esriFieldTypeDouble","alias":"Shape_Length","domain":None},
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELDS if field["type"] != "esriFieldTypeGeometry"]
EXPECTED_NULL_COUNTS = {"NAME":3,"LABEL":0,"Shape_Length":0}
EXPECTED_EXTENT = {"xmin":-20037507.067161843,"ymin":-9921601.01373757,
                   "xmax":20037507.067161843,"ymax":22830719.838254996}
EXPECTED_BOUNDS = tuple(EXPECTED_EXTENT[k] for k in ("xmin","ymin","xmax","ymax"))
EXPECTED_CAPTURE_NAMES = {"service_before","layer_before","count_before","ids_before","service_after","layer_after","count_after","ids_after",
                          *{f"page_{n:03d}" for n in range((COUNT+BATCH_SIZE-1)//BATCH_SIZE)}}
EXPECTED_CRS = CRS.from_epsg(3857)


def _canonical(value: object) -> bytes:
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False).encode("utf-8")

def _sha(body: bytes) -> str: return hashlib.sha256(body).hexdigest()
def _query(params: dict[str,object]) -> str: return LAYER_URL+"/query?"+urlencode(params)


def _validate_spatial_reference(value: object,label: str,*,required: bool=True) -> bool:
    if not isinstance(value,dict): raise ValueError(label+"_crs_drift")
    found=[]
    for key in ("wkid","latestWkid"):
        if key in value:
            val=value[key]
            if type(val) is not int or val not in {3857,102100}: raise ValueError(label+"_crs_drift")
            found.append(3857 if val==102100 else val)
    for key in ("wkt","wkt2"):
        if key in value:
            raw=value[key]
            if not isinstance(raw,str) or not raw.strip(): raise ValueError(label+"_crs_drift")
            try: crs=CRS.from_wkt(raw)
            except Exception as exc: raise ValueError(label+"_crs_drift") from exc
            if not crs.equals(EXPECTED_CRS,ignore_axis_order=True): raise ValueError(label+"_crs_drift")
            found.append(3857)
    if (required and not found) or len(set(found))>1: raise ValueError(label+"_crs_drift")
    return bool(found)


def _capture(client: httpx.Client,url: str,root: Path,name: str):
    retained=sum(p.stat().st_size for pat in ("*.body","*.wire") for p in root.glob(pat) if p.is_file())
    if retained+2*4_000_000>MAX_CAPTURE_BYTES: raise ValueError("source_capture_budget_exceeded")
    return capture_get(client,url,root,name,max_bytes=4_000_000)


def _load_capture(root: Path,name: str):
    bodyp,wirep,receiptp=(root/f"{name}.body",root/f"{name}.wire",root/f"{name}.receipt.json")
    if any(p.is_symlink() for p in (bodyp,wirep,receiptp)): raise ValueError("capture_symlink_rejected")
    body,wire,receipt=bodyp.read_bytes(),wirep.read_bytes(),json.loads(receiptp.read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status")!=200 or receipt.get("bytes")!=len(body)
        or receipt.get("sha256")!=_sha(body) or receipt.get("wire_bytes")!=len(wire) or receipt.get("wire_sha256")!=_sha(wire)):
        raise ValueError("capture_receipt_invalid:"+name)
    return body,wire,receipt


def _read_replay(root: Path):
    manifest=json.loads((root/"capture_manifest.json").read_bytes())
    roster=manifest.get("artifact_roster")
    if manifest.get("complete") is not True or not isinstance(roster,dict) or set(manifest.get("receipts",{}))!=EXPECTED_CAPTURE_NAMES:
        raise ValueError("capture_manifest_invalid")
    validate_capture_roster(root,set(roster)|{"capture_manifest.json"})
    if roster!=capture_roster_digests(root,exclude={"capture_manifest.json"}): raise ValueError("capture_artifact_roster_changed")
    result={}
    for name in sorted(EXPECTED_CAPTURE_NAMES):
        body,wire,receipt=_load_capture(root,name)
        if receipt!=manifest["receipts"].get(name): raise ValueError("capture_receipt_manifest_mismatch:"+name)
        result[name]=(body,receipt)
    return result


def _validate(captures: dict[str,tuple[bytes,dict]]):
    if set(captures)!=EXPECTED_CAPTURE_NAMES: raise ValueError("source_capture_name_roster_drift")
    docs={name:json.loads(body) for name,(body,_receipt) in captures.items()}
    service=docs["service_before"]
    advertised=[(x.get("id"),x.get("name")) for x in service.get("layers",[])]
    if service.get("serviceDescription")!="USGS Tectonic Plate Boundaries" or advertised!=[(0,"Microplates"),(1,"Plates")]:
        raise ValueError("official_usgs_service_identity_drift")
    layer=docs["layer_before"]
    if layer.get("id")!=LAYER_ID or layer.get("name")!="Plates" or layer.get("geometryType")!="esriGeometryPolyline" or layer.get("fields")!=EXPECTED_FIELDS or layer.get("maxRecordCount")!=1000:
        raise ValueError("source_layer_schema_or_limit_drift")
    _validate_spatial_reference(layer.get("spatialReference"),"source_layer")
    if {k:layer.get("extent",{}).get(k) for k in EXPECTED_EXTENT}!=EXPECTED_EXTENT: raise ValueError("source_extent_drift")
    extent_sr=layer.get("extent",{}).get("spatialReference")
    if extent_sr is not None: _validate_spatial_reference(extent_sr,"source_extent")
    if docs["service_after"]!=service or docs["layer_after"]!=layer: raise ValueError("source_changed_during_capture")
    if docs["count_before"].get("count")!=COUNT or docs["count_after"].get("count")!=COUNT: raise ValueError("source_count_drift")
    ids=docs["ids_before"].get("objectIds"); after=docs["ids_after"].get("objectIds")
    if (not isinstance(ids,list) or any(type(x)is not int for x in ids) or len(ids)!=COUNT or len(set(ids))!=COUNT
        or _sha(_canonical(sorted(ids)))!=EXPECTED_ID_SHA or after!=ids): raise ValueError("source_id_roster_drift")
    features=[]; seen=set(); nulls={k:0 for k in EXPECTED_NULL_COUNTS}; vertices=0; types=set(); bounds=[float("inf"),float("inf"),float("-inf"),float("-inf")]
    for name in sorted(n for n in captures if n.startswith("page_")):
        page=docs[name]
        if page.get("error") or page.get("exceededTransferLimit") is True: raise ValueError("source_page_error_or_truncation")
        _validate_spatial_reference(page.get("spatialReference"),name+"_response")
        rows=page.get("features"); expected=ids[int(name[-3:])*BATCH_SIZE:(int(name[-3:])+1)*BATCH_SIZE]
        if not isinstance(rows,list) or len(rows)!=len(expected): raise ValueError("source_page_population_incomplete:"+name)
        page_ids=[]
        for feature in rows:
            attrs,geom=feature.get("attributes"),feature.get("geometry")
            if not isinstance(attrs,dict) or set(attrs)!={f["name"] for f in ATTRIBUTE_FIELDS}: raise ValueError("source_attribute_roster_drift")
            oid=attrs.get("OBJECTID")
            if type(oid)is not int or oid not in expected or oid in seen: raise ValueError("source_native_id_drift")
            for f in ATTRIBUTE_FIELDS:
                key,kind=f["name"],f["type"]; value=attrs[key]
                if value is None:
                    if key not in nulls: raise ValueError("source_nullability_drift:"+key)
                    nulls[key]+=1; continue
                if kind=="esriFieldTypeOID" and type(value)is not int: raise ValueError("source_oid_type_drift")
                if kind=="esriFieldTypeString" and not isinstance(value,str): raise ValueError("source_string_type_drift:"+key)
                if kind=="esriFieldTypeDouble" and type(value) not in (int,float): raise ValueError("source_double_type_drift")
            if not isinstance(geom,dict) or set(geom)-{"paths","hasZ","hasM","spatialReference"}: raise ValueError("source_geometry_structure_drift")
            if geom.get("spatialReference") is not None: _validate_spatial_reference(geom["spatialReference"],"feature_geometry")
            if geom.get("hasZ",False) is not False or geom.get("hasM",False) is not False: raise ValueError("source_zm_drift")
            paths=geom.get("paths")
            if not isinstance(paths,list) or not paths: raise ValueError("source_paths_missing")
            for path in paths:
                if not isinstance(path,list) or len(path)<2: raise ValueError("source_path_invalid")
                for point in path:
                    if not isinstance(point,list) or len(point)!=2 or any(type(v) not in (int,float) or not math.isfinite(v) for v in point): raise ValueError("source_xy_drift")
                    bounds=[min(bounds[0],point[0]),min(bounds[1],point[1]),max(bounds[2],point[0]),max(bounds[3],point[1])]; vertices+=1
            page_ids.append(oid); seen.add(oid); features.append(feature)
        if sorted(page_ids)!=expected: raise ValueError("source_page_ids_drift:"+name)
    if seen!=set(ids) or len(features)!=COUNT: raise ValueError("source_population_incomplete")
    if nulls!=EXPECTED_NULL_COUNTS: raise ValueError("source_null_count_drift")
    if any(abs(a-b)>1e-3 for a,b in zip(bounds,EXPECTED_BOUNDS)): raise ValueError("source_geometry_coverage_drift")
    features.sort(key=lambda f:f["attributes"]["OBJECTID"])
    audit={"source_feature_count":len(features),"source_id_sha256":_sha(_canonical(sorted(seen))),"source_id_min":min(seen),"source_id_max":max(seen),
        "source_schema_sha256":_sha(_canonical(layer["fields"])),"source_record_sha256":_sha(_canonical([f["attributes"] for f in features])),
        "source_geometry_sha256":_sha(_canonical([[f["attributes"]["OBJECTID"],f["geometry"]] for f in features])),
        "source_geometry_type":"esriGeometryPolyline","source_wkid":3857,"source_vertex_count":vertices,"source_bounds":bounds,
        "source_extent":layer["extent"],"attribute_null_counts":nulls,"source_coverage_scope":"Complete current global layer 1 Plates, native USGS EPSG:3857; successor relation to HIFLD 2024 capture unverified."}
    return layer["fields"],features,audit


def _wkb(geometry:dict)->bytes:
    paths=geometry["paths"]; out=bytearray(struct.pack("<BI",1,5)); out.extend(struct.pack("<I",len(paths)))
    for path in paths:
        out.extend(struct.pack("<BI",1,2)); out.extend(struct.pack("<I",len(path)))
        for x,y in path: out.extend(struct.pack("<dd",float(x),float(y)))
    return bytes(out)


def _parquet_bytes(fields,features):
    schema=[f for f in fields if f["type"]!="esriFieldTypeGeometry"]
    arrow={"esriFieldTypeOID":pa.int64(),"esriFieldTypeString":pa.string(),"esriFieldTypeDouble":pa.float64()}
    arrays=[pa.array([row["attributes"][f["name"]] for row in features],type=arrow[f["type"]]) for f in schema]
    arrays.append(pa.array([_wkb(row["geometry"]) for row in features],type=pa.binary()))
    table=pa.Table.from_arrays(arrays,names=[f["name"] for f in schema]+["geometry"])
    crs=EXPECTED_CRS.to_json_dict()
    metadata={b"geo":_canonical({"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":["MultiLineString"],"crs":crs}}})}
    import io
    sink=io.BytesIO(); pq.write_table(table.replace_schema_metadata(metadata),sink,compression="zstd",use_dictionary=False)
    return sink.getvalue()


def _identity_hash(captures,root):
    roster={}
    for name in sorted(captures):
        body,wire,_receipt=_load_capture(root,name); roster[name]={"body_sha256":_sha(body),"wire_sha256":_sha(wire)}
    return _sha(_canonical(roster)),roster


def _preserve_original_receipts(captures, original_receipts, root):
    if not isinstance(original_receipts,dict) or set(original_receipts)!=set(captures):
        raise ValueError("cached_original_receipt_roster_changed")
    for name,(body,_receipt) in captures.items():
        current_body,wire,current_receipt=_load_capture(root,name); sealed=original_receipts[name]
        if any(current_receipt.get(field)!=sealed.get(field) for field in
               ("complete_body","status","bytes","sha256","wire_bytes","wire_sha256")):
            raise ValueError("cached_source_capture_content_changed:"+name)
        if body!=current_body or len(wire)!=sealed.get("wire_bytes") or _sha(wire)!=sealed.get("wire_sha256"):
            raise ValueError("cached_source_capture_content_changed:"+name)
    return original_receipts


def capture_plate_boundaries(storage:StagingStorageResource,*,version:str,capture_directory:str|None=None)->dict[str,object]:
    if (not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource)
        or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix): raise ValueError("explicit_isolated_local_staging_required")
    if not isinstance(version,str) or not version or Path(version).name!=version or version in {".",".."} or "/" in version or "\\" in version: raise ValueError("unsafe_capture_version")
    staging=Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(p.is_symlink() for p in (staging,*staging.parents)): raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(staging).free<300_000_000: raise ValueError("disk_floor_before_acquisition")
    requested=Path(capture_directory).absolute() if capture_directory else None
    replay=bool(requested and (requested/"capture_manifest.json").exists())
    if replay:
        root=requested; captures=_read_replay(root)
    else:
        if requested and any(requested.glob("*.body")): raise ValueError("unsealed_capture_attempt_exists")
        root=(requested/version/uuid4().hex) if requested else Path("/tmp/hifld-native-reconstruction/plate-boundaries")/(version+"-"+uuid4().hex)
        if any(p.is_symlink() for p in (root,*root.parents)): raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True,exist_ok=False); captures={}
        try:
            with httpx.Client(follow_redirects=False,timeout=httpx.Timeout(45,connect=12),headers={"User-Agent":"hifld-next-datasets/usgs-plates-draft"}) as client:
                for name,url in (("service_before",SERVICE_URL+"?f=pjson"),("layer_before",LAYER_URL+"?f=pjson"),
                    ("count_before",_query({"where":"1=1","returnCountOnly":"true","f":"json"})),
                    ("ids_before",_query({"where":"1=1","returnIdsOnly":"true","f":"json"}))):
                    captures[name]=_capture(client,url,root,name)
                ids=json.loads(captures["ids_before"][0]).get("objectIds")
                if not isinstance(ids,list) or len(ids)!=COUNT or _sha(_canonical(sorted(ids)))!=EXPECTED_ID_SHA: raise ValueError("source_seed_id_roster_drift")
                for i,start in enumerate(range(0,len(ids),BATCH_SIZE)):
                    pageids=ids[start:start+BATCH_SIZE]
                    url=_query({"objectIds":",".join(map(str,pageids)),"outFields":"*","returnGeometry":"true","returnZ":"true","returnM":"true","orderByFields":"OBJECTID ASC","f":"json"})
                    captures[f"page_{i:03d}"]=_capture(client,url,root,f"page_{i:03d}")
                for name,url in (("layer_after",LAYER_URL+"?f=pjson"),("count_after",_query({"where":"1=1","returnCountOnly":"true","f":"json"})),
                    ("ids_after",_query({"where":"1=1","returnIdsOnly":"true","f":"json"})),("service_after",SERVICE_URL+"?f=pjson")):
                    captures[name]=_capture(client,url,root,name)
            fields,features,audit=_validate(captures)
        except Exception as exc:
            receipts={}; names=set()
            for path in sorted(root.glob("*.receipt.json")):
                name=path.name.removesuffix(".receipt.json"); names.add(name)
                try: receipts[name]=json.loads(path.read_bytes())
                except (OSError,ValueError): receipts[name]={"receipt_readback_failed":True}
            (root/"capture_manifest.json").write_bytes(_canonical({"complete":False,"captured_at":datetime.now(timezone.utc).isoformat(),"error_code":type(exc).__name__,"receipts":receipts})+b"\n")
            for name in names:
                bodyp,wirep=root/f"{name}.body",root/f"{name}.wire"
                if bodyp.is_file() and wirep.is_file(): captures[name]=(bodyp.read_bytes(),receipts.get(name,{}))
            if captures: quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(exc).__name__)
            raise
        (root/"capture_manifest.json").write_bytes(_canonical({"complete":True,"captured_at":datetime.now(timezone.utc).isoformat(),
            "source_feature_count":audit["source_feature_count"],"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],
            "receipts":{n:r for n,(_b,r) in sorted(captures.items())},"artifact_roster":capture_roster_digests(root,exclude={"capture_manifest.json"})})+b"\n")
        captures=_read_replay(root)
    fields,features,audit=_validate(captures)
    repo=Path(__file__).parents[3]; identity=processing_identity(repo)
    capture_hash,capture_identity=_identity_hash(captures,root)
    parquet=_parquet_bytes(fields,features)
    version=content_derived_version(audit["source_record_sha256"],audit["source_geometry_sha256"],audit["source_schema_sha256"],identity)
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version); output_path=f"geoparquet/{FILE_SLUG}.parquet"
    fingerprint=_sha(_canonical({"identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"source_capture_sha256":capture_hash}))
    state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    original_payload=None
    if storage.object_snapshot(state_key) is not None:
        original_payload=json.loads(load_first_construction(storage,state_key,version,fingerprint)).get("payload")
        receipts=_preserve_original_receipts(captures,original_payload.get("capture_receipts") if isinstance(original_payload,dict) else None,root)
    audit.update({"source_capture_evidence_sha256":capture_hash,"source_capture_evidence":capture_identity,"source_population_complete":True,
        "source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"baseline_accepted":False})
    if original_payload is None: receipts={n:r for n,(_b,r) in sorted(captures.items())}
    inputs=[{"input_id":name,"producer":PUBLISHER,"host":urlsplit(r["url"]).hostname,"role":"producer_publication",
        "canonical_url":LAYER_URL,"retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],
        "http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],"wire_bytes":r["wire_bytes"],"wire_sha256":r["wire_sha256"],
        "release_date":None,"release_date_unknown_reason":"USGS service does not declare the source model's release date.",
        "license":"Rights review pending.","attribution":"USGS; final attribution review pending."} for name,r in receipts.items()]
    manifest={"schema_version":1,"approved":False,"title":"Plate Boundaries","description":"Complete capture of official USGS Plates feature layer; candidate only.",
        "publisher":PUBLISHER,"agency":"U.S. Geological Survey","source_url":LAYER_URL,"license":None,"source_modified":None,"date_issued":None,"date_modified":None,
        "processor":"HIFLD Next","code_commit":identity["code_commit"],"inputs":inputs,
        "transformations":[{"operation":"Native ESRI polyline to typed GeoParquet WKB","parameters":{"source_fields":"all declared attributes retained in declared types","source_ids":"OBJECTID retained unchanged",
            "source_crs":"EPSG:3857","geometry":"all XY paths and vertex order preserved without reprojection/repair/simplification","raw_evidence":"raw/wire request bodies and checksummed receipts retained","candidate_status":"unapproved; archive equivalence/current edition/schema/rights/release decisions pending"}}],
        "lineage":"Service metadata cites the USGS Seismicity of the Earth Map Series, Bird (2003), and DeMets et al. (2010). HIFLD archive is 2024; exact archive equality and successor status are unresolved.",
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native attributes and EPSG:3857 XY paths preserved as typed GeoParquet.","objects":[{"path":output_path,"bytes":len(parquet),"sha256":_sha(parquet)}]}}},
        "unsupported_output_formats":{"geopackage":"No format-specific mapping approved.","pmtiles":"No display generalization/release policy approved."}}
    validate_release_provenance(manifest);validate_output_policy(manifest);provenance_projection(manifest)
    outputs={output_path:parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n",
        "metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,_receipt) in captures.items():
        _b,wire,_r=_load_capture(root,name);outputs[f"raw/{name}.body"]=body;outputs[f"raw/{name}.wire"]=wire;outputs[f"raw/{name}.receipt.json"]=_canonical(receipts[name])+b"\n"
    ready={"approved":False,"version":version,"objects":{output_path:{"bytes":len(parquet),"sha256":_sha(parquet)}},
        "source_population_complete":True,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],
        "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"]=_canonical(ready)+b"\n"
    if original_payload is not None:
        verify_cached_candidate(storage,repo,state_key,prefix,version,fingerprint,identity,audit["source_record_sha256"],audit["source_schema_sha256"],original_payload["objects"],receipts)
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":COUNT,"source_population_complete":True,
            "source_id_sha256":audit["source_id_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"source_capture_evidence_sha256":capture_hash,"reused":True,
            "source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
            "baseline_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    descriptors={}
    for name,body in sorted(outputs.items()):
        key=f"{prefix}/{name}";write_immutable(storage,key,body);snapshot=storage.object_snapshot(key)
        descriptor={"bytes":len(body),"sha256":_sha(body)}
        if snapshot is None or snapshot.size!=len(body) or storage.sha256_key(key)!=descriptor["sha256"]: raise ValueError("candidate_readback_failed:"+name)
        descriptors[name]=descriptor
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],
        "source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":receipts,"objects":descriptors})
    validate_construction_code_identity(repo,identity["code_commit"],identity["files"],identity=identity)
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":COUNT,"source_population_complete":True,
        "source_id_sha256":audit["source_id_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"source_capture_evidence_sha256":capture_hash,
        "candidate_ready":prefix+"/metadata/candidate_ready.json","capture_directory":str(root),"reused":False,
        "source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "baseline_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
