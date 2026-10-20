"""Native, receipt-bound draft capture of the official USACE IENC building layer."""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import struct
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable,
)

DATASET_SLUG = FILE_SLUG = "building-locations-usace-ienc"
SERVICE_URL = "https://ienccloud.us/arcgis/rest/services/IENC/USACE_IENC_Master_Service/MapServer"
SERVICE_DIRECTORY_URL = "https://ienccloud.us/arcgis/rest/services/IENC?f=pjson"
BTS_FEATURE_CLASSES_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes"
LAYER_ID = 5
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
BTS_ITEM_ID = "3607d485a9854d6fa458bf507058ceae"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
PUBLISHER = "U.S. Army Corps of Engineers, Inland Electronic Navigational Charts Program"
EXPECTED_COUNT = 892
EXPECTED_ID_SHA256 = "683cd4972133d31f7ac111fa459a538c1ccbc59c38838a65bb6d8d850f73bff0"
BATCH_SIZE = 200
MAX_CAPTURE_BYTES = 32_000_000
EXPECTED_FIELDS = [
    {"name":"OBJECTID","type":"esriFieldTypeOID","alias":"OBJECTID","domain":None},
    {"name":"SHAPE","type":"esriFieldTypeGeometry","alias":"SHAPE","domain":None},
    *[{"name":name,"type":"esriFieldTypeString","alias":name,"length":254,"domain":None} for name in (
        "Visually_Conspicuous","Building_Function","Object_Name","Condition","Information")],
    {"name":"Scale_Minimum","type":"esriFieldTypeInteger","alias":"Scale_Minimum","domain":None},
    *[{"name":name,"type":"esriFieldTypeString","alias":name,"length":254,"domain":None} for name in (
        "Source_Date","Source_Indication","Source_Dataset","Feature_ID")],
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELDS if field["type"] != "esriFieldTypeGeometry"]
EXPECTED_EXTENT = {"xmin":-96.41946399999995,"ymin":29.69832880000007,
                   "xmax":-79.51543889999994,"ymax":45.106480600000054}
EXPECTED_CRS = CRS.from_epsg(4326)


def _canonical(value: object) -> bytes:
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False).encode()


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _validate_spatial_reference(value: object, label: str, *, required: bool=True) -> bool:
    if not isinstance(value,dict): raise ValueError(label+"_crs_drift")
    found=[]
    for key in ("wkid","latestWkid"):
        if key in value:
            wkid=value[key]
            if type(wkid) is not int or wkid != 4326: raise ValueError(label+"_crs_drift")
            found.append(4326)
    for key in ("wkt","wkt2"):
        if key in value:
            raw=value[key]
            if not isinstance(raw,str) or not raw.strip(): raise ValueError(label+"_crs_drift")
            try: parsed=CRS.from_wkt(raw)
            except Exception as exc: raise ValueError(label+"_crs_drift") from exc
            if not parsed.equals(EXPECTED_CRS,ignore_axis_order=True): raise ValueError(label+"_crs_drift")
            found.append(4326)
    if (required and not found) or len(set(found))>1: raise ValueError(label+"_crs_drift")
    return bool(found)


def _query(params: dict[str,object]) -> str:
    return LAYER_URL+"/query?"+urlencode(params)


def _capture(client: httpx.Client, url: str, root: Path, name: str):
    retained=sum(p.stat().st_size for suffix in ("*.body","*.wire") for p in root.glob(suffix) if p.is_file())
    if retained+2*2_000_000>MAX_CAPTURE_BYTES: raise ValueError("source_capture_budget_exceeded")
    return capture_get(client,url,root,name,max_bytes=2_000_000)


def _load_capture(root: Path,name: str):
    paths=[root/f"{name}.body",root/f"{name}.wire",root/f"{name}.receipt.json"]
    if any(p.is_symlink() for p in paths): raise ValueError("capture_symlink_rejected")
    body,wire,receipt=paths[0].read_bytes(),paths[1].read_bytes(),json.loads(paths[2].read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status")!=200 or receipt.get("bytes")!=len(body)
        or receipt.get("sha256")!=_sha(body) or receipt.get("wire_bytes")!=len(wire) or receipt.get("wire_sha256")!=_sha(wire)):
        raise ValueError("capture_receipt_invalid:"+name)
    return body,wire,receipt


def _expected_capture_names(ids: list[int]) -> set[str]:
    return {"bts_item","directory","service_before","layer_before","count_before","ids_before",
            "service_after","layer_after","count_after","ids_after",
            *{f"page_{n:03d}" for n in range((len(ids)+BATCH_SIZE-1)//BATCH_SIZE)}}


def _read_replay(root: Path, ids: list[int]):
    manifest=json.loads((root/"capture_manifest.json").read_bytes())
    names=_expected_capture_names(ids)
    roster=manifest.get("artifact_roster")
    if manifest.get("complete") is not True or not isinstance(roster,dict) or set(manifest.get("receipts",{}))!=names:
        raise ValueError("capture_manifest_invalid")
    validate_capture_roster(root,set(roster)|{"capture_manifest.json"})
    if roster!=capture_roster_digests(root,exclude={"capture_manifest.json"}): raise ValueError("capture_artifact_roster_changed")
    result={}
    for name in sorted(names):
        body,wire,receipt=_load_capture(root,name)
        if receipt!=manifest["receipts"].get(name): raise ValueError("capture_receipt_manifest_mismatch:"+name)
        result[name]=(body,receipt)
    return result


def _validate(captures: dict[str,tuple[bytes,dict]]):
    docs={name:json.loads(body) for name,(body,_receipt) in captures.items()}
    ids=docs["ids_before"].get("objectIds")
    if not isinstance(ids,list) or any(type(i)is not int for i in ids) or len(ids)!=EXPECTED_COUNT or len(set(ids))!=EXPECTED_COUNT:
        raise ValueError("source_id_population_drift")
    if _sha(_canonical(sorted(ids)))!=EXPECTED_ID_SHA256 or docs["ids_after"].get("objectIds")!=ids:
        raise ValueError("source_id_roster_drift")
    if docs["count_before"].get("count")!=EXPECTED_COUNT or docs["count_after"].get("count")!=EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    if docs["bts_item"].get("id")!=BTS_ITEM_ID or docs["bts_item"].get("owner")!="USDOT_BTS": raise ValueError("bts_item_identity_drift")
    bts_description=" ".join(str(docs["bts_item"].get(k,"")) for k in ("title","description"))
    if ("Inland Electronic Navigational Charts" not in bts_description or "updated bi-monthly" not in bts_description
        or BTS_FEATURE_CLASSES_URL not in bts_description or "USACE_IENC_Master_Service_gdb.zip" not in bts_description):
        raise ValueError("bts_ienc_distribution_link_drift")
    directory=docs["directory"]
    if not any(s.get("name")=="IENC/USACE_IENC_Master_Service" and s.get("type")=="MapServer" for s in directory.get("services",[])):
        raise ValueError("official_ienc_service_directory_drift")
    service=docs["service_before"]
    if service.get("mapName")!="USACE_IENC_Master_Service" or not any(l.get("id")==LAYER_ID and l.get("name")=="BUILDING_SINGLE_POINT" for l in service.get("layers",[])):
        raise ValueError("official_building_service_drift")
    if docs["service_after"]!=service: raise ValueError("source_service_changed_during_capture")
    layer=docs["layer_before"]
    if docs["layer_after"]!=layer: raise ValueError("source_layer_changed_during_capture")
    if layer.get("id")!=LAYER_ID or layer.get("name")!="BUILDING_SINGLE_POINT" or layer.get("geometryType")!="esriGeometryPoint" or layer.get("maxRecordCount")!=2000:
        raise ValueError("source_layer_identity_drift")
    if layer.get("fields")!=EXPECTED_FIELDS: raise ValueError("source_schema_drift")
    _validate_spatial_reference(layer.get("spatialReference"),"source_layer")
    extent=layer.get("extent",{})
    if {k:extent.get(k) for k in EXPECTED_EXTENT}!=EXPECTED_EXTENT: raise ValueError("source_extent_drift")
    if "spatialReference" in extent: _validate_spatial_reference(extent["spatialReference"],"source_extent")
    seen=set(); feature_ids=set(); features=[]; bounds=[math.inf,math.inf,-math.inf,-math.inf]
    for name in sorted(n for n in captures if n.startswith("page_")):
        page=docs[name]
        if page.get("error") or page.get("exceededTransferLimit") is True: raise ValueError("source_page_error_or_truncation")
        _validate_spatial_reference(page.get("spatialReference"),name+"_response")
        rows=page.get("features"); expected=ids[int(name[-3:])*BATCH_SIZE:(int(name[-3:])+1)*BATCH_SIZE]
        if not isinstance(rows,list) or len(rows)!=len(expected): raise ValueError("source_page_population_incomplete:"+name)
        got=[]
        for feature in rows:
            attrs,geom=feature.get("attributes"),feature.get("geometry")
            if not isinstance(attrs,dict) or set(attrs)!={f["name"] for f in ATTRIBUTE_FIELDS}: raise ValueError("source_attribute_roster_drift")
            oid=attrs.get("OBJECTID")
            if type(oid)is not int or oid not in expected or oid in seen: raise ValueError("source_objectid_drift")
            for field in ATTRIBUTE_FIELDS:
                value=attrs[field["name"]]
                if field["type"]=="esriFieldTypeOID" and type(value)is not int: raise ValueError("source_oid_type_drift")
                if field["type"]=="esriFieldTypeString" and not isinstance(value,str): raise ValueError("source_string_type_drift:"+field["name"])
                if field["type"]=="esriFieldTypeInteger" and type(value)is not int: raise ValueError("source_integer_type_drift")
            if not isinstance(geom,dict) or set(geom)-{"x","y","spatialReference"}: raise ValueError("source_point_geometry_drift")
            if "spatialReference" in geom: _validate_spatial_reference(geom["spatialReference"],"feature_geometry")
            x,y=geom.get("x"),geom.get("y")
            if type(x) not in (int,float) or type(y) not in (int,float) or not math.isfinite(x) or not math.isfinite(y): raise ValueError("source_xy_drift")
            bounds=[min(bounds[0],x),min(bounds[1],y),max(bounds[2],x),max(bounds[3],y)]
            fid=attrs.get("Feature_ID")
            if not fid or fid in feature_ids: raise ValueError("source_feature_id_missing_or_duplicate")
            seen.add(oid); feature_ids.add(fid); got.append(oid); features.append(feature)
        if sorted(got)!=expected: raise ValueError("source_page_ids_drift:"+name)
    if seen!=set(ids) or len(features)!=EXPECTED_COUNT or len(feature_ids)!=EXPECTED_COUNT: raise ValueError("source_population_incomplete")
    if any(abs(a-b)>1e-10 for a,b in zip(bounds,(EXPECTED_EXTENT["xmin"],EXPECTED_EXTENT["ymin"],EXPECTED_EXTENT["xmax"],EXPECTED_EXTENT["ymax"]))): raise ValueError("source_geometry_coverage_drift")
    features.sort(key=lambda f:f["attributes"]["OBJECTID"])
    audit={"source_feature_count":len(features),"source_id_sha256":_sha(_canonical(sorted(seen)),),"source_id_min":min(seen),"source_id_max":max(seen),
        "source_feature_id_unique_count":len(feature_ids),"source_schema_sha256":_sha(_canonical(layer["fields"])),
        "source_record_sha256":_sha(_canonical([f["attributes"] for f in features])),
        "source_geometry_sha256":_sha(_canonical([[f["attributes"]["OBJECTID"],f["geometry"]] for f in features])),
        "source_extent":extent,"source_bounds":bounds,"source_geometry_type":"esriGeometryPoint","source_wkid":4326,
        "source_coverage_scope":"Complete current USACE IENC BUILDING_SINGLE_POINT layer, inland chart coverage only; not nationwide building coverage.",
        "source_population_complete":True}
    return layer["fields"],features,audit


def _parquet_bytes(fields,features):
    attributes=[f for f in fields if f["type"]!="esriFieldTypeGeometry"]
    types={"esriFieldTypeOID":pa.int64(),"esriFieldTypeInteger":pa.int32(),"esriFieldTypeString":pa.string()}
    arrays=[pa.array([r["attributes"][f["name"]] for r in features],type=types[f["type"]]) for f in attributes]
    # WKB Point retains the native coordinate values without changing the source attribute schema.
    arrays.append(pa.array([struct.pack("<BIdd",1,1,float(r["geometry"]["x"]),float(r["geometry"]["y"])) for r in features],type=pa.binary()))
    table=pa.Table.from_arrays(arrays,names=[f["name"] for f in attributes]+["geometry"])
    geo={"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":["Point"],"crs":EXPECTED_CRS.to_json_dict()}}}
    import io
    sink=io.BytesIO();pq.write_table(table.replace_schema_metadata({b"geo":_canonical(geo)}),sink,compression="zstd",use_dictionary=False)
    return sink.getvalue()


def _capture_identity(captures,root):
    return {name:{"body_sha256":_sha(_load_capture(root,name)[0]),"wire_sha256":_sha(_load_capture(root,name)[1])} for name in sorted(captures)}


def capture_building_locations(storage:StagingStorageResource,*,version:str,capture_directory:str|None=None)->dict[str,object]:
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_isolated_local_staging_required")
    if not isinstance(version,str) or not version or Path(version).name!=version or version in {".",".."} or "/" in version or "\\" in version: raise ValueError("unsafe_capture_version")
    staging=Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(p.is_symlink() for p in (staging,*staging.parents)): raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(staging).free<300_000_000: raise ValueError("disk_floor_before_acquisition")
    requested=Path(capture_directory).absolute() if capture_directory else None
    replay=bool(requested and (requested/"capture_manifest.json").exists())
    if replay:
        root=requested
        manifest=json.loads((root/"capture_manifest.json").read_bytes())
        ids=json.loads((root/"ids_before.body").read_bytes()).get("objectIds")
        if not isinstance(ids,list): raise ValueError("capture_seed_ids_missing")
        captures=_read_replay(root,ids)
    else:
        if requested and any(requested.glob("*.body")): raise ValueError("unsealed_capture_attempt_exists")
        root=(requested/version/uuid4().hex) if requested else Path("/tmp/hifld-native-reconstruction/building-locations")/(version+"-"+uuid4().hex)
        if any(p.is_symlink() for p in (root,*root.parents)): raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True,exist_ok=False);captures={}
        try:
            with httpx.Client(follow_redirects=False,timeout=httpx.Timeout(45,connect=12),headers={"User-Agent":"hifld-next-datasets/usace-ienc-building-draft"}) as client:
                urls=[("bts_item",BTS_ITEM_URL),("directory",SERVICE_DIRECTORY_URL),("service_before",SERVICE_URL+"?f=pjson"),("layer_before",LAYER_URL+"?f=pjson"),
                      ("count_before",_query({"where":"1=1","returnCountOnly":"true","f":"json"})),("ids_before",_query({"where":"1=1","returnIdsOnly":"true","f":"json"}))]
                for name,url in urls: captures[name]=_capture(client,url,root,name)
                ids=json.loads(captures["ids_before"][0]).get("objectIds")
                if not isinstance(ids,list) or len(ids)!=EXPECTED_COUNT or _sha(_canonical(sorted(ids)))!=EXPECTED_ID_SHA256: raise ValueError("source_seed_id_roster_drift")
                for i,start in enumerate(range(0,len(ids),BATCH_SIZE)):
                    pageids=ids[start:start+BATCH_SIZE]
                    url=_query({"objectIds":",".join(map(str,pageids)),"outFields":"*","returnGeometry":"true","orderByFields":"OBJECTID ASC","f":"json"})
                    captures[f"page_{i:03d}"]=_capture(client,url,root,f"page_{i:03d}")
                for name,url in [("service_after",SERVICE_URL+"?f=pjson"),("layer_after",LAYER_URL+"?f=pjson"),
                    ("count_after",_query({"where":"1=1","returnCountOnly":"true","f":"json"})),("ids_after",_query({"where":"1=1","returnIdsOnly":"true","f":"json"}))]: captures[name]=_capture(client,url,root,name)
            fields,features,audit=_validate(captures)
        except Exception as exc:
            receipts={};names=set()
            for path in sorted(root.glob("*.receipt.json")):
                name=path.name.removesuffix(".receipt.json");names.add(name)
                try:receipts[name]=json.loads(path.read_bytes())
                except (OSError,ValueError):receipts[name]={"receipt_readback_failed":True}
            (root/"capture_manifest.json").write_bytes(_canonical({"complete":False,"error_code":type(exc).__name__,"receipts":receipts})+b"\n")
            for name in names:
                try:
                    body=(root/f"{name}.body").read_bytes()
                    receipt=json.loads((root/f"{name}.receipt.json").read_bytes())
                    if (root/f"{name}.wire").is_file(): captures[name]=(body,receipt)
                except (OSError,ValueError): pass
            if captures:quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(exc).__name__)
            raise
        (root/"capture_manifest.json").write_bytes(_canonical({"complete":True,"receipts":{n:r for n,(_b,r) in sorted(captures.items())},
            "artifact_roster":capture_roster_digests(root,exclude={"capture_manifest.json"})})+b"\n")
        captures=_read_replay(root,ids)
    fields,features,audit=_validate(captures)
    repo=Path(__file__).parents[3];identity=processing_identity(repo);capture_identity=_capture_identity(captures,root)
    capture_hash=_sha(_canonical(capture_identity));parquet=_parquet_bytes(fields,features)
    version=content_derived_version(audit["source_record_sha256"],audit["source_geometry_sha256"],audit["source_schema_sha256"],identity)
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version);output_path=f"geoparquet/{FILE_SLUG}.parquet"
    fingerprint=_sha(_canonical({"identity":identity,"record":audit["source_record_sha256"],"geometry":audit["source_geometry_sha256"],"schema":audit["source_schema_sha256"],"capture":capture_hash}))
    state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    existing=None
    if storage.object_snapshot(state_key) is not None:
        existing=json.loads(load_first_construction(storage,state_key,version,fingerprint)).get("payload")
        previous=existing.get("capture_receipts") if isinstance(existing,dict) else None
        if not isinstance(previous,dict) or set(previous)!=set(captures):raise ValueError("cached_capture_receipt_roster_changed")
        for name,(body,_r) in captures.items():
            cur,wire,receipt=_load_capture(root,name);old=previous[name]
            if any(receipt.get(k)!=old.get(k) for k in ("complete_body","status","bytes","sha256","wire_bytes","wire_sha256")) or cur!=body or _sha(wire)!=old.get("wire_sha256"):
                raise ValueError("cached_source_capture_content_changed:"+name)
    receipts=existing["capture_receipts"] if existing else {n:r for n,(_b,r) in sorted(captures.items())}
    audit.update({"source_capture_evidence_sha256":capture_hash,"source_capture_evidence":capture_identity,
        "source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "baseline_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})
    inputs=[{"input_id":n,"producer":"USDOT Bureau of Transportation Statistics" if n=="bts_item" else PUBLISHER,
        "host":urlsplit(r["url"]).hostname,"role":"official_distributor" if n=="bts_item" else "producer_publication",
        "canonical_url":BTS_ITEM_URL if n=="bts_item" else LAYER_URL,"retrieval_url":r["url"],"observed_final_url":r["final_url"],
        "retrieved_at":r["observed_at"],"http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],
        "wire_bytes":r["wire_bytes"],"wire_sha256":r["wire_sha256"],"release_date":None,
        "release_date_unknown_reason":"The service and official distribution item do not publish a release date for the captured layer.",
        "license":"Rights review pending.","attribution":"USACE IENC; final attribution review pending.",
        **({"distributor":"USDOT Bureau of Transportation Statistics (BTS)","delegation_evidence":{
            "bts_item_url":BTS_ITEM_URL,"bts_item_owner":"USDOT_BTS","item_linked_feature_classes_service":BTS_FEATURE_CLASSES_URL,
            "item_linked_master_geodatabase":"https://ienccloud.us/ienc/products/files/U37/ienc_master_dataset_gdb/USACE_IENC_Master_Service_gdb.zip",
            "official_master_service_directory":SERVICE_DIRECTORY_URL,
            "relationship_note":"BTS lists the Feature_Classes endpoint and master GDB; the exact MapServer route captured here is separately listed in USACE's ArcGIS directory, but direct link equivalence is unverified."}}
            if n=="bts_item" else {})} for n,r in sorted(receipts.items())]
    manifest={"schema_version":1,"approved":False,"title":"Building Locations (USACE IENC)",
        "description":"Complete current BUILDING_SINGLE_POINT feature layer capture; candidate only; inland chart coverage is not nationwide.",
        "publisher":PUBLISHER,"agency":"U.S. Army Corps of Engineers","source_url":LAYER_URL,"license":None,
        "source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],"inputs":inputs,
        "transformations":[{"operation":"Native ESRI Point to typed GeoParquet","parameters":{"fields":"all declared attributes and values preserved","id":"OBJECTID and Feature_ID preserved unchanged",
            "crs":"EPSG:4326","xy":"native X/Y retained with no reprojection or geometry repair","candidate_status":"unapproved; archive equivalence, source edition, contract, rights, baseline and release decisions pending"}}],
        "lineage":"USDOT BTS ArcGIS item 3607d485a9854d6fa458bf507058ceae describes bi-monthly USACE/AGC IENC updates and links the IENC_Feature_Classes service and master file geodatabase. The captured layer is in the separately listed USACE_IENC_Master_Service service directory; exact deployment equivalence to the BTS linked layer/GDB and HIFLD archive remains unverified. This is an inland chart class, not a nationwide buildings inventory.",
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native point coordinates and all attributes retained without reprojection.","objects":[{"path":output_path,"bytes":len(parquet),"sha256":_sha(parquet)}]}}},
        "unsupported_output_formats":{"geopackage":"No mapping approved.","pmtiles":"No display policy approved."}}
    validate_release_provenance(manifest);validate_output_policy(manifest);provenance_projection(manifest)
    outputs={output_path:parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,_r) in captures.items():
        _body,wire,_receipt=_load_capture(root,name);outputs[f"raw/{name}.body"]=body;outputs[f"raw/{name}.wire"]=wire;outputs[f"raw/{name}.receipt.json"]=_canonical(receipts[name])+b"\n"
    outputs["metadata/candidate_ready.json"]=_canonical({"approved":False,"version":version,"objects":{output_path:{"bytes":len(parquet),"sha256":_sha(parquet)}},
        "source_population_complete":True,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"source_release_accepted":False,
        "freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"baseline_accepted":False,"coverage_accepted":False,"rights_accepted":False,"release_accepted":False})+b"\n"
    if existing:
        verify_cached_candidate(storage,repo,state_key,prefix,version,fingerprint,identity,audit["source_record_sha256"],audit["source_schema_sha256"],existing["objects"],receipts)
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"source_population_complete":True,
            "source_id_sha256":audit["source_id_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"source_capture_evidence_sha256":capture_hash,"reused":True,
            "source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"baseline_accepted":False,
            "coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    objects={}
    for name,body in sorted(outputs.items()):
        key=f"{prefix}/{name}";write_immutable(storage,key,body);snap=storage.object_snapshot(key)
        if snap is None or snap.size!=len(body) or storage.sha256_key(key)!=_sha(body):raise ValueError("candidate_readback_failed:"+name)
        objects[name]={"bytes":len(body),"sha256":_sha(body)}
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],
        "source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":receipts,"objects":objects})
    validate_construction_code_identity(repo,identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"source_population_complete":True,
        "source_id_sha256":audit["source_id_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"source_capture_evidence_sha256":capture_hash,
        "candidate_ready":prefix+"/metadata/candidate_ready.json","capture_directory":str(root),"reused":False,
        "source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "baseline_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
