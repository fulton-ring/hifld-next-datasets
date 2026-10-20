"""Pinned historical EIA Power Plants GIS candidate for issue #274."""
from __future__ import annotations
import hashlib, io, json, math, shutil, struct, tempfile, zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
import geopandas as gpd
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import candidate_prefix, capture_get, processing_identity, quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable

DATASET_SLUG = FILE_SLUG = "power-plants-1"
SOURCE_URL = "https://www.eia.gov/maps/map_data/PowerPlants_US_EIA.zip"
SOURCE_RELEASE = "2021-08"
EXPECTED_LAYER = "PowerPlants_US_202108"
EXPECTED_ZIP_BYTES = 1539615
EXPECTED_ZIP_SHA256 = "b65e4f0f49fdb5b9709549169c4be23fd8ad42512025b5b1df13a94a843aa4d8"
EXPECTED_COUNT = 10847
EXPECTED_FIELDS = [
    ("Plant_Code","N",10,0),("Plant_Name","C",254,0),("Utility_Na","C",254,0),("Utility_ID","N",10,0),
    ("sector_nam","C",254,0),("City","C",254,0),("County","C",254,0),("StateName","C",254,0),
    ("Zip","N",10,0),("Street_Add","C",254,0),("PrimSource","C",254,0),
    *[(n,"F",19,11) for n in ("Install_MW","Total_MW","Coal_MW","NG_MW","Crude_MW","Hydro_MW","HydroPS_MW","Nuclear_MW","Solar_MW","Wind_MW")],
    ("source_des","C",254,0),("tech_desc","C",254,0),("Source","C",254,0),("Period","N",10,0),
    ("Latitude","F",19,11),("Longitude","F",19,11),*[(n,"F",19,11) for n in ("Bio_MW","Geo_MW","Other_MW","Bat_MW")],
]
MEMBERS={f"{EXPECTED_LAYER}.{s}" for s in ("cpg","dbf","prj","sbn","sbx","shp","shp.xml","shx")}
ID_SHA="c6ad5352230a7b4e1c2238e83ee461585b4054a9b9049b1790ee7edb2a5d51a9"
RECORD_SHA="07cdc819b12a200a8823e3f2b27f181f9c83cee384c715ac717b9110727a6c0f"
GEOMETRY_SHA="65728ac6666ed38eb75c0b88fbb88509e5a075716f29a2a3bd480f5dec6975df"
BOUNDS=[-171.71243899999996,17.947115000000053,-65.27955999999995,71.29200000000003]
WGS84=CRS.from_epsg(4326)

def _canonical(x): return json.dumps(x,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False).encode()
def _sha(b): return hashlib.sha256(b).hexdigest()
def _native(v):
    if hasattr(v,"item"): v=v.item()
    if v is None or (isinstance(v,float) and math.isnan(v)): return None
    if isinstance(v,float) and not math.isfinite(v): raise ValueError("source_nonfinite_attribute")
    return v

def _schema_dbf(path):
    b=path.read_bytes(); h=int.from_bytes(b[8:10],"little")
    if h<33 or h>len(b): raise ValueError("source_dbf_header_invalid")
    out=[]; o=32
    while o<h and b[o]!=0x0d:
        d=b[o:o+32]
        if len(d)!=32: raise ValueError("source_dbf_descriptor_incomplete")
        out.append((d[:11].split(b"\0",1)[0].decode("ascii"),chr(d[11]),d[16],d[17]));o+=32
    if o>=h or b[o]!=0x0d: raise ValueError("source_dbf_terminator_missing")
    return out

def _decode(body,tmp):
    if len(body)!=EXPECTED_ZIP_BYTES or _sha(body)!=EXPECTED_ZIP_SHA256: raise ValueError("pinned_eia_snapshot_drift")
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        names=[]
        for i in z.infolist():
            mode=i.external_attr>>16;parts=Path(i.filename).parts
            if not i.filename or i.filename.startswith(("/","\\")) or "\\" in i.filename or ".." in parts or len(parts)!=1 or i.is_dir() or (mode&0o170000)==0o120000: raise ValueError("source_zip_member_path_rejected")
            names.append(i.filename)
        if len(set(names))!=len(names) or set(names)!=MEMBERS or sum(i.file_size for i in z.infolist())>50_000_000: raise ValueError("source_zip_member_roster_drift")
        root=tmp/"shape";root.mkdir()
        for name in MEMBERS:
            with z.open(name) as src,(root/name).open("xb") as dst: shutil.copyfileobj(src,dst)
    shp=root/f"{EXPECTED_LAYER}.shp"
    if _schema_dbf(root/f"{EXPECTED_LAYER}.dbf")!=EXPECTED_FIELDS: raise ValueError("source_dbf_schema_drift")
    import pyogrio
    info=pyogrio.read_info(shp)
    if info["layer_name"]!=EXPECTED_LAYER or info["geometry_type"]!="Point" or info["crs"]!="EPSG:4326" or int(info["features"])!=EXPECTED_COUNT: raise ValueError("source_layer_metadata_drift")
    g=gpd.read_file(shp,engine="pyogrio")
    names=[x[0] for x in EXPECTED_FIELDS]
    if list(g.columns)!=names+["geometry"] or len(g)!=EXPECTED_COUNT or g.geometry.isna().any() or g.geometry.is_empty.any() or not (g.geometry.geom_type=="Point").all(): raise ValueError("source_population_or_schema_drift")
    if not CRS.from_user_input(g.crs).equals(WGS84,ignore_axis_order=True): raise ValueError("source_crs_drift")
    if g["Plant_Code"].isna().any() or g["Plant_Code"].duplicated().any(): raise ValueError("source_id_drift")
    if int(g.Period.min())!=202108 or int(g.Period.max())!=202108: raise ValueError("source_period_drift")
    # Shapefile DBF decimal coordinates are rounded to 11 places while the
    # corresponding SHP doubles may differ by a few floating-point ulps.
    if (abs(g.geometry.x.to_numpy()-g.Longitude.to_numpy())>1e-10).any() or (abs(g.geometry.y.to_numpy()-g.Latitude.to_numpy())>1e-10).any(): raise ValueError("source_xy_attribute_drift")
    attrs=[[_native(v) for v in row] for row in g[names].itertuples(index=False,name=None)]
    ids=sorted(int(r[0]) for r in attrs)
    geom=[[int(r[0]),float(g.geometry.iloc[i].x),float(g.geometry.iloc[i].y)] for i,r in enumerate(attrs)]
    idsha,recsha,geomsha=(_sha(_canonical(x)) for x in (ids,attrs,geom))
    if idsha!=ID_SHA or recsha!=RECORD_SHA or geomsha!=GEOMETRY_SHA: raise ValueError("source_content_snapshot_drift")
    bounds=list(map(float,g.total_bounds))
    if bounds!=BOUNDS: raise ValueError("source_bounds_drift")
    audit={"source_feature_count":len(g),"source_population_complete":True,"source_id_field":"Plant_Code","source_id_unique":True,"source_id_min":min(ids),"source_id_max":max(ids),"source_id_sha256":idsha,"source_record_sha256":recsha,"source_geometry_sha256":geomsha,"source_schema_sha256":_sha(_canonical(EXPECTED_FIELDS)),"source_dbf_schema":EXPECTED_FIELDS,"source_crs":"EPSG:4326","source_geometry_type":"Point","source_bounds":bounds,"source_period":SOURCE_RELEASE,"source_null_counts":{n:int(g[n].isna().sum()) for n in names},"source_geometry_null_count":int(g.geometry.isna().sum()),"source_geometry_empty_count":int(g.geometry.is_empty.sum()),"source_coverage_scope":"Complete EIA August 2021 map layer; map extent includes Alaska, Hawaii, and territories represented by this source. Not a current feed or accepted HIFLD equivalent.","drift_guard":"Pinned official archive checksum, complete zip members, exact DBF widths/types, layer name, CRS, all attributes and values, unique Plant_Code roster, period, native XY and bounds."}
    return g,audit

def _point_wkb(x,y): return struct.pack("<BIdd",1,1,float(x),float(y))
def _parquet(g):
    arrays=[];fields=[]
    for name,kind,*_ in EXPECTED_FIELDS:
        vals=[_native(v) for v in g[name].tolist()]
        dtype=pa.int64() if kind=="N" else pa.float64() if kind=="F" else pa.string()
        arrays.append(pa.array(vals,type=dtype,safe=True));fields.append(pa.field(name,dtype,nullable=True))
    arrays.append(pa.array([_point_wkb(x,y) for x,y in zip(g.geometry.x,g.geometry.y)],type=pa.binary()));fields.append(pa.field("geometry",pa.binary(),nullable=False))
    geo={"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":["Point"],"crs":WGS84.to_json_dict()}}}
    sink=io.BytesIO();pq.write_table(pa.Table.from_arrays(arrays,schema=pa.schema(fields,metadata={b"geo":_canonical(geo)})),sink,compression="zstd",use_dictionary=False)
    t=pq.read_table(pa.BufferReader(sink.getvalue()))
    if t.num_rows!=EXPECTED_COUNT: raise ValueError("candidate_row_count_readback_drift")
    for name,kind,*_ in EXPECTED_FIELDS:
        if t[name].to_pylist()!=[_native(v) for v in g[name].tolist()]: raise ValueError("candidate_attribute_readback_drift:"+name)
    if any(t["geometry"][i].as_py()!=_point_wkb(x,y) for i,(x,y) in enumerate(zip(g.geometry.x,g.geometry.y))): raise ValueError("candidate_geometry_readback_drift")
    return sink.getvalue()

def _load_capture(root):
    paths=[root/"eia_zip.body",root/"eia_zip.wire",root/"eia_zip.receipt.json"]
    if any(p.is_symlink() for p in paths): raise ValueError("capture_symlink_rejected")
    body,wire,receipt=paths[0].read_bytes(),paths[1].read_bytes(),json.loads(paths[2].read_bytes())
    if receipt.get("method")!="GET" or receipt.get("url")!=SOURCE_URL or receipt.get("final_url")!=SOURCE_URL or receipt.get("complete_body") is not True or receipt.get("status")!=200 or receipt.get("bytes")!=len(body) or receipt.get("sha256")!=_sha(body) or receipt.get("wire_bytes")!=len(wire) or receipt.get("wire_sha256")!=_sha(wire): raise ValueError("capture_receipt_invalid")
    return body,wire,receipt

def _capture(root,storage,version):
    mp=root/"capture_manifest.json"
    if root.is_symlink() or any(p.is_symlink() for p in root.parents): raise ValueError("unsafe_capture_directory")
    if mp.exists():
        m=json.loads(mp.read_bytes())
        if m.get("complete") is not True or set(m.get("receipts",{}))!={"eia_zip"}: raise ValueError("capture_manifest_invalid")
        roster=m.get("artifact_roster");validate_capture_roster(root,set(roster)|{"capture_manifest.json"})
        if roster!=capture_roster_digests(root,exclude={"capture_manifest.json"}): raise ValueError("capture_roster_digest_drift")
        body,wire,receipt=_load_capture(root)
        if receipt!=m["receipts"]["eia_zip"]: raise ValueError("capture_manifest_receipt_drift")
        return body,wire,receipt
    if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")): raise ValueError("unsealed_capture_attempt")
    root.mkdir(parents=True,exist_ok=True);caps={}
    try:
        with httpx.Client(follow_redirects=False,timeout=httpx.Timeout(60,connect=15),headers={"User-Agent":"hifld-next-datasets/power-plants-draft"}) as client: body,receipt=capture_get(client,SOURCE_URL,root,"eia_zip",max_bytes=10_000_000)
        if receipt.get("method")!="GET" or receipt.get("url")!=SOURCE_URL or receipt.get("final_url")!=SOURCE_URL: raise ValueError("source_request_identity_mismatch")
        caps["eia_zip"]=(body,receipt)
        with tempfile.TemporaryDirectory(dir=root) as tmp: _decode(body,Path(tmp))
        mp.write_bytes(_canonical({"complete":True,"captured_at":datetime.now(timezone.utc).isoformat(),"receipts":{"eia_zip":receipt},"artifact_roster":capture_roster_digests(root,exclude={"capture_manifest.json"})})+b"\n")
        return body,(root/"eia_zip.wire").read_bytes(),receipt
    except Exception as exc:
        rec={}
        for p in root.glob("*.receipt.json"):
            try: rec[p.name.removesuffix(".receipt.json")]=json.loads(p.read_bytes());caps.setdefault("eia_zip",((root/"eia_zip.body").read_bytes(),rec[p.name.removesuffix(".receipt.json")]))
            except Exception: pass
        mp.write_bytes(_canonical({"complete":False,"captured_at":datetime.now(timezone.utc).isoformat(),"error_code":type(exc).__name__,"receipts":rec})+b"\n")
        if caps: quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,caps,type(exc).__name__)
        raise

def capture_power_plants(storage:StagingStorageResource,*,version:str,capture_directory:str|None=None)->dict:
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix: raise ValueError("explicit_local_staging_resource_required")
    if not isinstance(version,str) or not version or Path(version).name!=version or "/" in version or "\\" in version: raise ValueError("unsafe_capture_version")
    stage=Path(storage.local_dir).absolute()
    if ".." in stage.parts or any(p.is_symlink() for p in (stage,*stage.parents)): raise ValueError("unsafe_local_staging_root")
    stage.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(stage).free<300_000_000: raise ValueError("disk_floor_before_acquisition")
    requested=Path(capture_directory).absolute() if capture_directory else None
    root=requested or Path("/tmp/hifld-native-reconstruction/274/captures")/(version+"-"+uuid4().hex)
    body,wire,receipt=_capture(root,storage,version)
    with tempfile.TemporaryDirectory() as tmp: frame,audit=_decode(body,Path(tmp));parquet=_parquet(frame)
    identity=processing_identity(Path(__file__).parents[3]); evidence_sha=_sha(_canonical({"body":receipt["sha256"],"wire":receipt["wire_sha256"],"receipt":receipt}))
    audit.update({"source_capture_evidence_sha256":evidence_sha,"source_receipt":receipt,"source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"baseline_accepted":False})
    cv="v"+_sha(_canonical({"archive_sha256":EXPECTED_ZIP_SHA256,"record_sha256":audit["source_record_sha256"],"geometry_sha256":audit["source_geometry_sha256"],"schema_sha256":audit["source_schema_sha256"],"processing_identity":identity}))[:32]
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,cv);op=f"geoparquet/{FILE_SLUG}.parquet"
    inp={"input_id":"eia_official_powerplants_archive","producer":"U.S. Energy Information Administration","host":urlsplit(SOURCE_URL).hostname,"role":"producer_publication","canonical_url":SOURCE_URL,"retrieval_url":SOURCE_URL,"observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],"http_status":receipt["status"],"bytes":receipt["bytes"],"sha256":receipt["sha256"],"wire_bytes":receipt["wire_bytes"],"wire_sha256":receipt["wire_sha256"],"release_date":SOURCE_RELEASE,"release_date_unknown_reason":None,"license":"Attribution/reuse review pending","attribution":"U.S. Energy Information Administration; review pending"}
    manifest={"schema_version":1,"approved":False,"title":"Power Plants (EIA GIS, August 2021 snapshot)","description":"Pinned historical EIA-hosted PowerPlants_US_202108 shapefile; not represented as a current release or accepted HIFLD successor.","publisher":"U.S. Energy Information Administration","agency":"U.S. Department of Energy","source_url":SOURCE_URL,"license":None,"source_modified":"2022-01-31","date_issued":None,"date_modified":"2021-08","processor":"HIFLD Next","code_commit":identity["code_commit"],"inputs":[inp],"transformations":[{"operation":"Native DBF attributes and EPSG:4326 point shapefile to GeoParquet","parameters":{"source_fields":"all native DBF fields preserved in order, source types and values","source_ids":"Plant_Code retained unchanged; no synthetic identifiers","geometry":"native WGS84 XY retained without reprojection","source_period":"202108","candidate_status":"historical, unapproved; equivalence/currentness unresolved"}}],"lineage":"Embedded EIA shapefile metadata cites EIA-860, EIA-860M and EIA-923 and labels the data period August 2021. Current EIA-860M tables are a distinct generator-level source and are not substituted by this plant-level snapshot.","prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"All source fields, Plant_Code IDs, and native point coordinates are retained.","objects":[{"path":op,"bytes":len(parquet),"sha256":_sha(parquet)}]}}},"unsupported_output_formats":{"geopackage":"No published contract/mapping approved","pmtiles":"No display transformation approved"},"promotion_allowed":False,"source_accepted":False,"baseline":None}
    validate_release_provenance(manifest);validate_output_policy(manifest)
    outputs={op:parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n","metadata/candidate_ready.json":_canonical({"approved":False,"version":cv,"objects":{op:{"bytes":len(parquet),"sha256":_sha(parquet)}},"source_population_complete":True,"source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"coverage_accepted":False,"release_accepted":False})+b"\n","raw/eia_zip.body":body,"raw/eia_zip.wire":wire,"raw/eia_zip.receipt.json":_canonical(receipt)+b"\n"}
    state=f"_construction-state/{DATASET_SLUG}/{cv}.json";fingerprint=_sha(_canonical({"identity":identity,"archive_sha256":EXPECTED_ZIP_SHA256,"record_sha256":audit["source_record_sha256"],"schema_sha256":audit["source_schema_sha256"]}))
    if storage.object_snapshot(state) is not None:
        original=json.loads(load_first_construction(storage,state,cv,fingerprint)).get("payload")
        if not isinstance(original,dict) or not isinstance(original.get("objects"),dict): raise ValueError("cached_candidate_state_invalid")
        verify_cached_candidate(storage,Path(__file__).parents[3],state,prefix,cv,fingerprint,identity,audit["source_record_sha256"],audit["source_schema_sha256"],original["objects"],original.get("capture_receipts",{}))
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":cv,"feature_count":EXPECTED_COUNT,"source_population_complete":True,"source_id_sha256":ID_SHA,"source_schema_sha256":audit["source_schema_sha256"],"candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,"source_capture_evidence_sha256":evidence_sha,"capture_directory":str(root),"release_accepted":False}
    descriptors={}
    for n,b in outputs.items():
        key=f"{prefix}/{n}";write_immutable(storage,key,b);snap=storage.object_snapshot(key)
        if snap is None or snap.size!=len(b) or storage.sha256_key(key)!=_sha(b):raise ValueError("candidate_object_readback_failed:"+n)
        descriptors[n]={"bytes":len(b),"sha256":_sha(b)}
    seal_ready_candidate(storage,state,cv,fingerprint,{"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":{"eia_zip":receipt},"objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":cv,"feature_count":EXPECTED_COUNT,"source_population_complete":True,"source_id_sha256":ID_SHA,"source_schema_sha256":audit["source_schema_sha256"],"source_record_sha256":audit["source_record_sha256"],"source_geometry_sha256":audit["source_geometry_sha256"],"source_coverage_scope":audit["source_coverage_scope"],"candidate_ready":prefix+"/metadata/candidate_ready.json","reused":False,"source_capture_evidence_sha256":evidence_sha,"capture_directory":str(root),"source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"release_accepted":False}
