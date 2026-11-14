"""Bounded ArcGIS Extract/FileGDB acquisition and immutable replay helpers."""
from __future__ import annotations

import hashlib, json, os, shutil, stat, subprocess, zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable
from urllib.parse import urlsplit
import httpx
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import write_immutable

DEFAULT_MAX_ARCHIVE_BYTES = 64_000_000
MAX_UNCOMPRESSED_GDB_BYTES = 128_000_000
MIN_FREE_BYTES = 256_000_000

def sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()

def canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")

def capture_filegdb_replica(client: httpx.Client, service_url: str, layer_id: int, replica_name: str,
                            capture_dir: Path, *, max_bytes: int = DEFAULT_MAX_ARCHIVE_BYTES
                            ) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Capture an export-only (`syncModel=none`) native FileGDB ZIP."""
    root = Path(capture_dir).absolute()
    if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(root).free < MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_filegdb_export")
    params = {"replicaName": replica_name, "layers": str(layer_id), "syncModel": "none", "dataFormat": "filegdb",
              "transportType": "esriTransportTypeUrl", "returnAttachments": "false", "async": "false", "f": "json"}
    response = client.post(f"{service_url.rstrip('/')}/createReplica", data=params, timeout=120)
    raw = response.content
    sub_receipt = {"method":"POST", "url":f"{service_url.rstrip('/')}/createReplica", "status":response.status_code,
                   "bytes":len(raw), "sha256":sha256(raw), "observed_at":datetime.now(timezone.utc).isoformat(),
                   "request_parameters":params}
    (root/"replica_submission.body").write_bytes(raw)
    (root/"replica_submission.receipt.json").write_bytes(canonical_json(sub_receipt)+b"\n")
    if response.status_code != 200 or len(raw) > 8_000_000: raise ValueError("filegdb_export_submission_failed")
    doc = json.loads(raw)
    if isinstance(doc.get("error"), dict): raise ValueError("filegdb_export_rejected:"+str(doc["error"].get("code", "error")))
    if doc.get("replicaID") not in (None, ""): raise ValueError("filegdb_export_unexpected_replica_state")
    result_url = doc.get("resultUrl") or doc.get("responseUrl") or doc.get("URL")
    parts = urlsplit(result_url) if isinstance(result_url,str) else None
    if parts is None or parts.scheme != "https" or not parts.hostname: raise ValueError("filegdb_export_url_missing")
    archive, wire = root/"filegdb_export.body", root/"filegdb_export.wire"
    archive.unlink(missing_ok=True); wire.unlink(missing_ok=True)
    body_hash, wire_hash = hashlib.sha256(), hashlib.sha256(); body_size=wire_size=0
    def retain(path, digest, data, kind):
        nonlocal body_size, wire_size
        current = body_size if kind == "body" else wire_size
        if current + len(data) > max_bytes: raise ValueError("source_filegdb_archive_exceeds_budget")
        with path.open("ab") as f: f.write(data)
        digest.update(data)
        if kind == "body": body_size += len(data)
        else: wire_size += len(data)
    with client.stream("GET", result_url, timeout=120, headers={"Accept-Encoding":"identity"}) as downloaded:
        final=urlsplit(str(downloaded.url)); host=final.hostname or ""
        if host != parts.hostname and not host.endswith(".az.arcgis.com"): raise ValueError("source_artifact_redirect_host_rejected")
        result=capture_http_response(downloaded,max_wire_bytes=max_bytes,max_decoded_bytes=max_bytes,reserve=lambda _p:None,
             retain_wire=lambda b:retain(wire,wire_hash,b,"wire"),retain_decoded=lambda b:retain(archive,body_hash,b,"body"))
    receipt={"method":"GET","url":result_url,"final_host":host,"final_path":final.path,"status":result.status_code,
             "complete_body":result.complete,"bytes":body_size,"sha256":body_hash.hexdigest(),"wire_bytes":wire_size,
             "wire_sha256":wire_hash.hexdigest(),"content_length":result.content_length,
             "content_encoding":result.content_encoding,"observed_at":datetime.now(timezone.utc).isoformat()}
    (root/"filegdb_export.receipt.json").write_bytes(canonical_json(receipt)+b"\n")
    if (not result.complete or result.status_code!=200 or not body_size or body_size!=result.decoded_bytes_retained
            or wire_size!=result.wire_bytes_retained or sha256(archive.read_bytes())!=receipt["sha256"]):
        raise ValueError("incomplete_source_filegdb_export")
    return archive,receipt,doc,sub_receipt

def extract_filegdb(archive_path: Path, work_dir: Path, *, expected_layer: str,
                    expected_property_names: Iterable[str], expected_count: int, expected_ids: Iterable[int],
                    expected_zm: bool | tuple[bool,bool] | None = None,
                    binary_fields: Iterable[str] = ()) -> tuple[Path,str,dict[str,Any]]:
    """Safely unpack one GDB and validate source count, IDs, fields, BLOB and Z/M."""
    archive_path,work_dir=Path(archive_path),Path(work_dir); names_expected=list(expected_property_names); ids_expected=set(expected_ids)
    try:
        with zipfile.ZipFile(archive_path) as zf:
            members=zf.namelist(); roots={n.split("/",1)[0] for n in members if ".gdb/" in n}
            if len(roots)!=1: raise ValueError("source_filegdb_archive_roster_invalid")
            gdb_root=next(iter(roots))
            if not gdb_root.endswith(".gdb"): raise ValueError("source_filegdb_archive_roster_invalid")
            total_uncompressed=sum(info.file_size for info in zf.infolist())
            if total_uncompressed>MAX_UNCOMPRESSED_GDB_BYTES:
                raise ValueError("source_filegdb_uncompressed_budget_exceeded")
            for n in members:
                p=PurePosixPath(n)
                if p.is_absolute() or ".." in p.parts or "\\" in n: raise ValueError("unsafe_filegdb_archive_member")
                if n and not n.startswith(gdb_root+"/"): raise ValueError("unexpected_filegdb_archive_member")
            zf.extractall(work_dir)
    except zipfile.BadZipFile as exc: raise ValueError("source_filegdb_archive_invalid") from exc
    gdb=work_dir/gdb_root; binary=set(binary_fields)
    try:
        import fiona
        if fiona.listlayers(gdb)!=[expected_layer]: raise ValueError("source_filegdb_layer_roster_drift")
        with fiona.open(gdb,layer=expected_layer) as src:
            if len(src)!=expected_count: raise ValueError("source_filegdb_feature_count_drift")
            actual=list(src.schema["properties"])
            if actual!=names_expected: raise ValueError("source_filegdb_field_roster_drift")
            if binary-set(actual): raise ValueError("source_filegdb_blob_field_missing")
            seen=[];null_geometry_count=0
            for feat in src:
                try: fid=int(feat.id)
                except (TypeError,ValueError) as exc: raise ValueError("source_filegdb_ID_invalid") from exc
                seen.append(fid)
                if feat["geometry"] is None: null_geometry_count+=1
                for name in binary:
                    if not isinstance(feat["properties"].get(name),bytes): raise ValueError("source_filegdb_blob_value_type_drift")
            if len(seen)!=expected_count or set(seen)!=ids_expected: raise ValueError("source_filegdb_ID_population_drift")
    except (OSError,ValueError): raise
    except Exception as exc: raise ValueError("source_filegdb_unreadable") from exc
    try:
        info=subprocess.run(["ogrinfo","-ro","-so",str(gdb),expected_layer],check=True,capture_output=True,text=True,timeout=180).stdout
    except Exception as exc: raise ValueError("gdal_ogrinfo_unavailable_for_ZM_validation") from exc
    if expected_zm is not None:
        z,m=expected_zm if isinstance(expected_zm,tuple) else (expected_zm,expected_zm)
        if ("3D " in info," Measured " in info)!=(z,m): raise ValueError("source_filegdb_ZM_geometry_contract_drift")
    return gdb,gdb_root,{"feature_count":expected_count,"unique_id_count":len(ids_expected),"null_geometry_count":null_geometry_count,"field_names":names_expected,
                         "binary_fields":sorted(binary),"gdal_layer_summary":info.split("\n",1)[-1].strip()}

def stage_filegdb_archive(storage: StagingStorageResource,prefix:str,archive_path:Path,gdb_path:Path,gdb_root:str,
                          *,archive_rel:str="file_geodatabase/source_filegdb.zip") -> list[dict[str,Any]]:
    """Store original ZIP and all extracted GDB parts immutably."""
    if not isinstance(storage,StagingStorageResource): raise ValueError("staging_storage_required")
    rel=PurePosixPath(archive_rel)
    if rel.is_absolute() or ".." in rel.parts: raise ValueError("unsafe_filegdb_archive_key")
    archive=Path(archive_path).read_bytes(); write_immutable(storage,f"{prefix}/{rel.as_posix()}",archive)
    result=[{"path":rel.as_posix(),"bytes":len(archive),"sha256":sha256(archive)}]
    for path in sorted(Path(gdb_path).rglob("*")):
        if path.is_file():
            sub=path.relative_to(gdb_path).as_posix(); body=path.read_bytes(); target=f"file_geodatabase/{gdb_root}/{sub}"
            write_immutable(storage,f"{prefix}/{target}",body); result.append({"path":target,"bytes":len(body),"sha256":sha256(body)})
    return result

def verify_staged_filegdb(storage:StagingStorageResource,prefix:str,objects:list[dict[str,Any]]) -> dict[str,Any]:
    """Verify exact immutable object roster and bytes after catalog writes."""
    expected={storage._ensure_prefixed(f"{prefix}/{x['path']}"):x for x in objects}
    filegdb_prefix=storage._ensure_prefixed(f"{prefix}/file_geodatabase")
    archives=[key for key in expected if key.startswith(filegdb_prefix+"/") and key.lower().endswith(".zip")]
    gdb_roots={key.split("/file_geodatabase/",1)[1].split("/",1)[0]
               for key in expected if key.startswith(filegdb_prefix+"/") and not key.lower().endswith(".zip")}
    if len(gdb_roots)!=1 or len(archives)!=1:
        raise ValueError("cached_filegdb_expected_inventory_invalid")
    archive_key=archives[0]
    gdb_prefix=storage._ensure_prefixed(f"{prefix}/file_geodatabase/{next(iter(gdb_roots))}")
    candidates=set(storage.list_prefix(f"{prefix}/file_geodatabase"))
    actual={key for key in candidates if key==archive_key or key.startswith(gdb_prefix+"/")}
    wanted={key for key in expected if key==archive_key or key.startswith(gdb_prefix+"/")}
    if actual!=wanted: raise ValueError("cached_filegdb_candidate_roster_drift")
    for key,item in expected.items():
        snap=storage.object_snapshot(key)
        if snap is None or snap.size!=item["bytes"] or storage.sha256_key(key)!=item["sha256"] or storage.object_snapshot(key)!=snap:
            raise ValueError("cached_filegdb_candidate_object_drift")
    return {"object_count":len(expected),"bytes":sum(x["bytes"] for x in objects),"sha256":sha256(canonical_json(objects))}

def verify_extracted_filegdb_cache(storage:StagingStorageResource,prefix:str,objects:list[dict[str,Any]]) -> dict[str,dict[str,Any]]:
    """Accept only an exact byte-for-byte local catalog extraction of the staged GDB."""
    if not isinstance(storage,StagingStorageResource) or not storage.use_local or storage.bucket or not storage.local_dir:
        raise ValueError("local_filegdb_extraction_cache_requires_local_staging")
    gdb_objects={x["path"]:x for x in objects if x["path"].startswith("file_geodatabase/") and not x["path"].lower().endswith(".zip")}
    roots={name.split("/",2)[1] for name in gdb_objects}
    if len(roots)!=1:raise ValueError("cached_filegdb_expected_inventory_invalid")
    gdb_root=next(iter(roots));logical_prefix=storage._ensure_prefixed(prefix).rstrip("/")
    relative=f"{logical_prefix}/file_geodatabase/.extracted/source_filegdb/{gdb_root}"
    root=Path(storage.local_dir).absolute()
    target=root/relative
    for part in (root,*root.parents):
        if part.is_symlink():raise ValueError("cached_filegdb_extracted_cache_symlink")
    current=root
    for part in Path(relative).parts:
        current=current/part
        if current.is_symlink():raise ValueError("cached_filegdb_extracted_cache_symlink")
    if not target.exists():return {}
    expected={name.split(f"file_geodatabase/{gdb_root}/",1)[1]:{"bytes":descriptor["bytes"],"sha256":descriptor["sha256"]}
              for name,descriptor in gdb_objects.items()}
    actual=set()
    for directory,dirs,files in os.walk(target,followlinks=False):
        base=Path(directory)
        for name in dirs:
            mode=(base/name).lstat().st_mode
            if not stat.S_ISDIR(mode):raise ValueError("cached_filegdb_extracted_cache_nonregular")
        for name in files:
            path=base/name;mode=path.lstat().st_mode
            if not stat.S_ISREG(mode):raise ValueError("cached_filegdb_extracted_cache_nonregular")
            actual.add(path.relative_to(target).as_posix())
    if actual!=set(expected):raise ValueError("cached_filegdb_extracted_cache_roster_changed")
    result={}
    for name,descriptor in expected.items():
        path=target/name
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        try:
            before=os.fstat(fd)
            if not stat.S_ISREG(before.st_mode):raise ValueError("cached_filegdb_extracted_cache_nonregular")
            digest=hashlib.sha256();size=0
            with os.fdopen(fd,"rb",closefd=False) as stream:
                while block:=stream.read(1024*1024):digest.update(block);size+=len(block)
            after=os.fstat(fd)
            if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns):
                raise ValueError("cached_filegdb_extracted_cache_changed_during_read")
            if descriptor!={"bytes":size,"sha256":digest.hexdigest()}:raise ValueError("cached_filegdb_extracted_cache_integrity_failed:"+name)
        finally:os.close(fd)
        result[f"{logical_prefix}/file_geodatabase/.extracted/source_filegdb/{gdb_root}/{name}"]=descriptor
    return result
