"""Pinned historical EIA Hydrocarbon Gas Liquid Pipelines candidate for issue #117."""
from __future__ import annotations
import io, json, math, shutil, struct, tempfile, zipfile
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
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.capture_common import candidate_prefix, capture_get, processing_identity, retain_failed_capture, seal_ready_candidate, verify_cached_candidate, write_immutable
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = FILE_SLUG = "hydrocarbon-gas-liquid-pipelines-1"
SOURCE_URL = "https://www.eia.gov/maps/map_data/HGL_Pipelines_US_EIA.zip"
SOURCE_VINTAGE = "2020-01"
SOURCE_MODIFIED = "2020-04-27"
EXPECTED_LAYER = "HGL_Pipelines_US_202001"
EXPECTED_ZIP_BYTES = 52914
EXPECTED_ZIP_SHA256 = "f1a2f71f722f47bb50aaded99b13c68450046e9880f8555dcc5d4fa5117b829f"
EXPECTED_COUNT = 133
EXPECTED_FIELDS = [("Opername","C",150,0),("Pipename","C",150,0),("Shape_Leng","F",19,11)]
MEMBERS = {f"{EXPECTED_LAYER}.{s}" for s in ("cpg","dbf","prj","sbn","sbx","shp","shp.xml","shx")}
ID_SHA = "f5c07520840e4edc5511eb162cbe80fb033bdc48ef06d056b4272f01b3fd4711"
RECORD_SHA = "df2e3c25bc51d9892e2e32c4145eecc0008815868dd99fa1a936cd32eb2ce159"
GEOMETRY_SHA = "550d5cac621df48928b1690109467cd8a151bfc96de77618c15cae15c3feba6e"
BOUNDS = [-114.48867995999996, 25.273952101000077, -73.78826634899997, 53.76941817800008]
EXPECTED_LINESTRING_COUNT = 130
EXPECTED_MULTILINESTRING_COUNT = 3
EXPECTED_VERTEX_COUNT = 1759
WGS84 = CRS.from_epsg(4326)


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

def _paths(geom):
    if geom.geom_type == "LineString": return [list(geom.coords)]
    if geom.geom_type == "MultiLineString": return [list(part.coords) for part in geom.geoms]
    raise ValueError("source_geometry_type_drift")

def _linestring_wkb(points):
    out=bytearray(struct.pack("<BI",1,2)); out.extend(struct.pack("<I",len(points)))
    for x,y in points: out.extend(struct.pack("<dd",float(x),float(y)))
    return bytes(out)

def _multilinestring_wkb(paths):
    out=bytearray(struct.pack("<BI",1,5)); out.extend(struct.pack("<I",len(paths)))
    for path in paths:
        out.extend(struct.pack("<BI",1,2)); out.extend(struct.pack("<I",len(path)))
        for x,y in path: out.extend(struct.pack("<dd",float(x),float(y)))
    return bytes(out)

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
    if (root/f"{EXPECTED_LAYER}.cpg").read_bytes()!=b"UTF-8": raise ValueError("source_cpg_encoding_drift")
    if _schema_dbf(root/f"{EXPECTED_LAYER}.dbf")!=EXPECTED_FIELDS: raise ValueError("source_dbf_schema_drift")
    import pyogrio
    info=pyogrio.read_info(shp)
    if info["layer_name"]!=EXPECTED_LAYER or info["crs"]!="EPSG:4326" or int(info["features"])!=EXPECTED_COUNT: raise ValueError("source_layer_metadata_drift")
    g=pyogrio.read_dataframe(shp, fid_as_index=True)
    names_=[x[0] for x in EXPECTED_FIELDS]
    if list(g.columns)!=names_+["geometry"] or len(g)!=EXPECTED_COUNT: raise ValueError("source_population_or_schema_drift")
    if not CRS.from_user_input(g.crs).equals(WGS84,ignore_axis_order=True): raise ValueError("source_crs_drift")
    if list(g.index)!=list(range(EXPECTED_COUNT)): raise ValueError("source_record_order_drift")
    if g.geometry.isna().any() or g.geometry.is_empty.any(): raise ValueError("source_missing_or_empty_geometry_drift")
    types={geom_type:0 for geom_type in ("LineString","MultiLineString")}; vertices=0
    for geom in g.geometry:
        types[geom.geom_type]=types.get(geom.geom_type,0)+1
        paths=_paths(geom)
        for path in paths:
            if len(path)<2 or any(len(point)!=2 or not math.isfinite(point[0]) or not math.isfinite(point[1]) for point in path): raise ValueError("source_path_invalid")
            vertices+=len(path)
    if types!={"LineString":EXPECTED_LINESTRING_COUNT,"MultiLineString":EXPECTED_MULTILINESTRING_COUNT}: raise ValueError("source_geometry_type_drift")
    if any(len(_paths(geom))!=2 for geom in g.geometry if geom.geom_type=="MultiLineString"): raise ValueError("source_multipart_structure_drift")
    if vertices!=EXPECTED_VERTEX_COUNT: raise ValueError("source_vertex_count_drift")
    for name in names_:
        if g[name].isna().any(): raise ValueError("source_attribute_null_drift:"+name)
        if name!="Shape_Leng" and not all(isinstance(v,str) and v!="" for v in g[name].tolist()): raise ValueError("source_attribute_text_drift:"+name)
    if not all(math.isfinite(v) for v in g["Shape_Leng"].tolist()): raise ValueError("source_attribute_float_drift:Shape_Leng")
    ids=[int(index)+1 for index in g.index]
    attrs=[[fid, *[_native(v) for v in g.iloc[i][names_].tolist()]] for i, fid in enumerate(ids)]
    geoms=[[fid, {"paths": _paths(geom)}] for fid, geom in zip(ids, g.geometry)]
    idsha,recsha,geomsha=(_sha(_canonical(x)) for x in (sorted(ids),attrs,geoms))
    if idsha!=ID_SHA or recsha!=RECORD_SHA or geomsha!=GEOMETRY_SHA: raise ValueError("source_content_snapshot_drift")
    bounds=list(map(float,g.total_bounds))
    if bounds!=BOUNDS: raise ValueError("source_bounds_drift")
    audit={"source_feature_count":len(g),"source_population_complete":True,"source_id_field":"__source_fid","source_id_kind":"1-based physical shapefile record number; explicit proposed identifier, unapproved","source_id_unique":True,"source_id_min":min(ids),"source_id_max":max(ids),"source_id_sha256":idsha,"source_record_sha256":recsha,"source_geometry_sha256":geomsha,"source_schema_sha256":_sha(_canonical(EXPECTED_FIELDS)),"source_dbf_schema":EXPECTED_FIELDS,"source_crs":"EPSG:4326","source_geometry_type":"native per-record LineString/MultiLineString","source_linestring_count":types["LineString"],"source_multilinestring_count":types["MultiLineString"],"source_vertex_count":vertices,"source_bounds":bounds,"source_vintage":SOURCE_VINTAGE,"source_null_counts":{n:int(g[n].isna().sum()) for n in names_},"source_geometry_null_count":int(g.geometry.isna().sum()),"source_geometry_empty_count":int(g.geometry.is_empty.sum()),"source_coverage_scope":"Complete pinned EIA HGL pipelines layer, January 2020 data vintage: 133 major U.S. and selected Canadian HGL pipeline lines per producer metadata. Not a current feed or accepted HIFLD equivalent.","drift_guard":"Pinned official EIA archive checksum and size, complete zip member roster, exact DBF widths/types, layer name, CPG encoding, CRS, all attribute values and nullability, contiguous record order, per-record geometry types and part structure, vertex count, native vertex bounds and canonical content digests."}
    return g,audit

def _geometry_wkb(geom):
    paths=_paths(geom)
    if geom.geom_type == "LineString": return _linestring_wkb(paths[0])
    return _multilinestring_wkb(paths)

def _parquet(g):
    arrays=[];fields=[]
    for name,kind,*_ in EXPECTED_FIELDS:
        vals=[_native(v) for v in g[name].tolist()]
        dtype=pa.float64() if kind=="F" else pa.string()
        arrays.append(pa.array(vals,type=dtype,safe=True));fields.append(pa.field(name,dtype,nullable=False))
    arrays.append(pa.array([fid for fid in range(1,len(g)+1)],type=pa.int64()));fields.append(pa.field("__source_fid",pa.int64(),nullable=False))
    arrays.append(pa.array([_geometry_wkb(geom) for geom in g.geometry],type=pa.binary()));fields.append(pa.field("geometry",pa.binary(),nullable=False))
    geo={"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":["LineString","MultiLineString"],"crs":WGS84.to_json_dict()}}}
    sink=io.BytesIO();pq.write_table(pa.Table.from_arrays(arrays,schema=pa.schema(fields,metadata={b"geo":_canonical(geo)})),sink,compression="zstd",use_dictionary=False)
    t=pq.read_table(pa.BufferReader(sink.getvalue()))
    if t.num_rows!=EXPECTED_COUNT: raise ValueError("candidate_row_count_readback_drift")
    for name,kind,*_ in EXPECTED_FIELDS:
        if t[name].to_pylist()!=[_native(v) for v in g[name].tolist()]: raise ValueError("candidate_attribute_readback_drift:"+name)
    if t["__source_fid"].to_pylist()!=list(range(1,EXPECTED_COUNT+1)): raise ValueError("candidate_source_fid_readback_drift")
    if any(t["geometry"][i].as_py()!=_geometry_wkb(geom) for i,geom in enumerate(g.geometry)): raise ValueError("candidate_geometry_readback_drift")
    return sink.getvalue()

def _load_capture(root):
    paths=[root/"eia_hgl_zip.body",root/"eia_hgl_zip.wire",root/"eia_hgl_zip.receipt.json"]
    if any(p.is_symlink() for p in paths): raise ValueError("capture_symlink_rejected")
    body,wire,receipt=paths[0].read_bytes(),paths[1].read_bytes(),json.loads(paths[2].read_bytes())
    if receipt.get("method")!="GET" or receipt.get("url")!=SOURCE_URL or receipt.get("final_url")!=SOURCE_URL or receipt.get("complete_body") is not True or receipt.get("status")!=200 or receipt.get("bytes")!=len(body) or receipt.get("sha256")!=_sha(body) or receipt.get("wire_bytes")!=len(wire) or receipt.get("wire_sha256")!=_sha(wire): raise ValueError("capture_receipt_invalid")
    return body,wire,receipt

def _capture(root,storage,version):
    mp=root/"capture_manifest.json"
    if root.is_symlink() or any(p.is_symlink() for p in root.parents): raise ValueError("unsafe_capture_directory")
    if mp.exists():
        m=json.loads(mp.read_bytes())
        if m.get("complete") is not True or set(m.get("receipts",{}))!={"eia_hgl_zip"}: raise ValueError("capture_manifest_invalid")
        roster=m.get("artifact_roster");validate_capture_roster(root,set(roster)|{"capture_manifest.json"})
        if roster!=capture_roster_digests(root,exclude={"capture_manifest.json"}): raise ValueError("capture_roster_digest_drift")
        body,wire,receipt=_load_capture(root)
        if receipt!=m["receipts"]["eia_hgl_zip"]: raise ValueError("capture_manifest_receipt_drift")
        return body,wire,receipt
    if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")): raise ValueError("unsealed_capture_attempt")
    root.mkdir(parents=True,exist_ok=True);caps={}
    try:
        with httpx.Client(follow_redirects=False,timeout=httpx.Timeout(60,connect=15),headers={"User-Agent":"hifld-next-datasets/hgl-pipelines-draft"}) as client: body,receipt=capture_get(client,SOURCE_URL,root,"eia_hgl_zip",max_bytes=10_000_000)
        if receipt.get("method")!="GET" or receipt.get("url")!=SOURCE_URL or receipt.get("final_url")!=SOURCE_URL: raise ValueError("source_request_identity_mismatch")
        caps["eia_hgl_zip"]=(body,receipt)
        with tempfile.TemporaryDirectory(dir=root) as tmp: _decode(body,Path(tmp))
        mp.write_bytes(_canonical({"complete":True,"captured_at":datetime.now(timezone.utc).isoformat(),"receipts":{"eia_hgl_zip":receipt},"artifact_roster":capture_roster_digests(root,exclude={"capture_manifest.json"})})+b"\n")
        return body,(root/"eia_hgl_zip.wire").read_bytes(),receipt
    except Exception as exc:
        retain_failed_capture(storage,root,DATASET_SLUG,version,exc,error=ValueError)
        mp.write_bytes(_canonical({"complete":False,"captured_at":datetime.now(timezone.utc).isoformat(),"error_code":type(exc).__name__})+b"\n")
        raise

def capture_hgl_pipelines(storage:StagingStorageResource,*,version:str,capture_directory:str|None=None)->dict:
    """Refuse historical acquisition and replay before resource side effects."""
    raise ValueError(
        "historical_source_not_maintained: January2020 HGL line snapshot. A maintained public native-line successor remains unverified."
    )
