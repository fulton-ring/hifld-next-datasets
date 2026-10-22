"""Lossless current-source reconstruction for USACE NDC/LPMS Locks.

The NDC Locks FeatureServer description matches the HIFLD inventory profile.
The "USACE IENC" title suffix is not treated as proof that this layer is an
IENC chart feature class. Historical DataLumos snapshot equivalence, release
vintage, baseline compatibility, rights acceptance, and publication remain
unapproved. The producer-declared EPSG:4019 unknown datum is preserved as-is.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import shutil
import stat
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    processing_identity as packaged_processing_identity,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "locks-usace-ienc"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Locks/FeatureServer"
LAYER_URL = SERVICE_URL + "/0"
CREATE_REPLICA_URL = SERVICE_URL + "/createReplica"
EXPECTED_COUNT = 234
EXPECTED_OIDS = list(range(1, EXPECTED_COUNT + 1))
SOURCE_WKID = 4019
LAYER_NAME = "Lock"
ARCHIVE_FORMAT = "filegdb"
MAX_RECEIPT_BYTES = 65_536
MAX_ARCHIVE_BYTES = 1_000_000
MAX_CAPTURE_BYTES = 4_000_000
MAX_DOWNLOAD_REDIRECTS = 3
OFFICIAL_DOWNLOAD_HOSTS = ("arcgis.com", "arcgisusercontent.com")
MAX_EXPANDED_BYTES = 10_000_000
MIN_FREE_BYTES = 400_000_000
MAX_POLL_COUNT = 10
POLL_INTERVAL_SECONDS = 3
MAX_POLL_ELAPSED_SECONDS = 45
CAPTURE_ROOT = Path("/tmp/hifld-native-reconstruction/151/captures")
LICENSE_TEXT = (
    "USACE ArcGIS item says publicly available without use restrictions; it also "
    "notes national-scale use and possible unsuitability for local-scale mapping. "
    "Project-level rights, source, and release acceptance remain pending."
)

# Service-declared field order/types/widths. OBJECTID is an OID FID, not an
# ordinary attribute; every other attribute, including the zero-length Binary
# Blob, is retained in the candidate.
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID", None),
    ("ID", "esriFieldTypeInteger", None),
    ("NDCCODE", "esriFieldTypeString", 25),
    ("REGION", "esriFieldTypeString", 5),
    ("RIVERCD", "esriFieldTypeString", 5),
    ("LOCKCD", "esriFieldTypeString", 5),
    ("CHMBCD", "esriFieldTypeString", 2),
    ("NOCHMB", "esriFieldTypeSmallInteger", None),
    ("PMSDATA", "esriFieldTypeString", 2),
    ("NAVSTR", "esriFieldTypeString", 50),
    ("CHAMBN", "esriFieldTypeString", 25),
    ("CHB1", "esriFieldTypeString", 2),
    ("STR1", "esriFieldTypeString", 2),
    ("PMSNAME", "esriFieldTypeString", 50),
    ("STATUS", "esriFieldTypeString", 2),
    ("RIVER", "esriFieldTypeString", 50),
    ("RIVERMI", "esriFieldTypeDouble", None),
    ("BANK", "esriFieldTypeString", 2),
    ("LIFT", "esriFieldTypeDouble", None),
    ("LENGTH", "esriFieldTypeDouble", None),
    ("CHMBUL", "esriFieldTypeDouble", None),
    ("WIDTH", "esriFieldTypeDouble", None),
    ("CHMBUW", "esriFieldTypeDouble", None),
    ("AUDPA", "esriFieldTypeDouble", None),
    ("AUDPB", "esriFieldTypeDouble", None),
    ("UPDPTHMS", "esriFieldTypeDouble", None),
    ("LWDPTHMS", "esriFieldTypeDouble", None),
    ("YEAROPEN", "esriFieldTypeDouble", None),
    ("GATETYPE", "esriFieldTypeString", 70),
    ("GATE", "esriFieldTypeString", 2),
    ("CHNLGTH", "esriFieldTypeDouble", None),
    ("CHNDPTHA", "esriFieldTypeDouble", None),
    ("CHNDPTHB", "esriFieldTypeDouble", None),
    ("CHNWDTHA", "esriFieldTypeDouble", None),
    ("CHNWDTHB", "esriFieldTypeDouble", None),
    ("WWPRJCT", "esriFieldTypeString", 70),
    ("MOORING", "esriFieldTypeString", 2),
    ("MULTI", "esriFieldTypeString", 12),
    ("DIVISION", "esriFieldTypeString", 12),
    ("DISTRICT", "esriFieldTypeString", 6),
    ("STATE", "esriFieldTypeString", 4),
    ("MAINT1", "esriFieldTypeString", 4),
    ("OPER1", "esriFieldTypeString", 4),
    ("OWNER1", "esriFieldTypeString", 4),
    ("TOWN", "esriFieldTypeString", 60),
    ("COUNTY1", "esriFieldTypeString", 60),
    ("MOORING_R", "esriFieldTypeString", 100),
    ("SE_ANNO_CAD_DATA", "esriFieldTypeBlob", None),
]
EXPECTED_ATTRIBUTE_NAMES = [name for name, _type, _width in EXPECTED_FIELDS if name != "OBJECTID"]
EXPECTED_PYARROW_TYPES = {
    "OBJECTID": pa.int64(), "ID": pa.int32(), "NOCHMB": pa.int16(),
    "RIVERMI": pa.float64(), "LIFT": pa.float64(), "LENGTH": pa.float64(),
    "CHMBUL": pa.float64(), "WIDTH": pa.float64(), "CHMBUW": pa.float64(),
    "AUDPA": pa.float64(), "AUDPB": pa.float64(), "UPDPTHMS": pa.float64(),
    "LWDPTHMS": pa.float64(), "YEAROPEN": pa.float64(), "CHNLGTH": pa.float64(),
    "CHNDPTHA": pa.float64(), "CHNDPTHB": pa.float64(), "CHNWDTHA": pa.float64(),
    "CHNWDTHB": pa.float64(), "SE_ANNO_CAD_DATA": pa.binary(), "SHAPE": pa.binary(),
}
for _name, _kind, _width in EXPECTED_FIELDS:
    if _name not in EXPECTED_PYARROW_TYPES and _name != "OBJECTID":
        EXPECTED_PYARROW_TYPES[_name] = pa.string()
EXPECTED_COLUMN_ORDER = [name for name, _kind, _width in EXPECTED_FIELDS] + ["SHAPE"]


class LocksQuarantineError(ValueError):
    """Source, capture, cache, schema, geometry, or binary drift quarantine."""


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                       allow_nan=False) + "\n").encode("utf-8")


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _strict_json(body: bytes, label: str) -> dict:
    if len(body) > MAX_RECEIPT_BYTES:
        raise LocksQuarantineError("json_artifact_over_limit:" + label)
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise LocksQuarantineError("duplicate_json_key:" + label)
            out[key] = value
        return out
    value = json.loads(body, object_pairs_hook=pairs,
                       parse_constant=lambda _: (_ for _ in ()).throw(LocksQuarantineError("nonfinite_json:" + label)))
    if not isinstance(value, dict):
        raise LocksQuarantineError("json_object_required:" + label)
    return value


def _local_storage_root(storage: StagingStorageResource) -> Path:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise LocksQuarantineError("explicit_unprefixed_local_staging_required")
    root = Path(storage.local_dir).absolute()
    if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)):
        raise LocksQuarantineError("unsafe_local_staging_root")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _validate_url(url: object, *, path_prefix: str) -> str:
    if not isinstance(url, str):
        raise LocksQuarantineError("source_result_url_missing")
    p = urlsplit(url)
    expected = urlsplit(SERVICE_URL)
    if (p.scheme != "https" or p.hostname != expected.hostname or p.port not in (None, 443)
            or p.username or p.password or p.query or p.fragment or not p.path.startswith(path_prefix)):
        raise LocksQuarantineError("source_result_url_outside_official_service")
    return url


def _field_declaration(fields: object) -> list[dict]:
    if not isinstance(fields, list):
        raise LocksQuarantineError("service_fields_missing")
    result = []
    for field in fields:
        if not isinstance(field, dict):
            raise LocksQuarantineError("service_field_invalid")
        result.append({"name": field.get("name"), "type": field.get("type"),
                       "length": field.get("length"), "nullable": field.get("nullable")})
    return result


def _validate_layer_metadata(document: dict) -> dict:
    expected = [{"name": n, "type": t, "length": w, "nullable": n != "OBJECTID"}
                for n, t, w in EXPECTED_FIELDS]
    if (_field_declaration(document.get("fields")) != expected
            or document.get("id") != 0 or document.get("name") != LAYER_NAME
            or document.get("geometryType") != "esriGeometryPoint"
            or document.get("objectIdField") != "OBJECTID"
            or document.get("spatialReference") != {"wkid": SOURCE_WKID, "latestWkid": SOURCE_WKID}
            or document.get("hasZ") is not False or document.get("hasM") is not False):
        raise LocksQuarantineError("source_layer_schema_identity_or_crs_drift")
    return {"id": document["id"], "name": document["name"],
            "geometryType": document["geometryType"], "objectIdField": document["objectIdField"],
            "fields": _field_declaration(document["fields"]),
            "spatialReference": document["spatialReference"],
            "hasZ": document["hasZ"], "hasM": document["hasM"]}


def _validate_count_ids(count_doc: dict, ids_doc: dict, label: str) -> list[int]:
    ids = ids_doc.get("objectIds")
    if (count_doc.get("count") != EXPECTED_COUNT or not isinstance(ids, list)
            or len(ids) != EXPECTED_COUNT or any(type(i) is not int for i in ids)
            or len(set(ids)) != EXPECTED_COUNT or sorted(ids) != EXPECTED_OIDS):
        raise LocksQuarantineError("source_count_or_id_roster_drift:" + label)
    return sorted(ids)


def _safe_gdb_archive(body: bytes) -> tuple[str, dict]:
    if not 0 < len(body) <= MAX_ARCHIVE_BYTES:
        raise LocksQuarantineError("archive_bytes_outside_limit")
    try:
        with zipfile.ZipFile(__import__("io").BytesIO(body)) as archive:
            members = archive.infolist()
            if not members or len({m.filename for m in members}) != len(members):
                raise LocksQuarantineError("archive_empty_or_duplicate_members")
            total = 0
            roots = set()
            roster = {}
            for item in members:
                path = PurePosixPath(item.filename)
                mode = item.external_attr >> 16
                if (path.is_absolute() or ".." in path.parts or "\\" in item.filename
                        or len(path.parts) != 2 or not path.parts[0].endswith(".gdb")
                        or item.flag_bits & 1 or stat.S_ISLNK(mode)):
                    raise LocksQuarantineError("unsafe_filegdb_zip_member")
                roots.add(path.parts[0]); total += item.file_size
                roster[item.filename] = {"bytes": item.file_size, "crc32": f"{item.CRC:08x}"}
            if len(roots) != 1 or total > MAX_EXPANDED_BYTES or archive.testzip() is not None:
                raise LocksQuarantineError("filegdb_zip_root_expansion_or_crc_drift")
            gdb_root = next(iter(roots))
            return gdb_root, {"expanded_bytes": total, "members": roster}
    except zipfile.BadZipFile as exc:
        raise LocksQuarantineError("invalid_filegdb_zip") from exc


def validate_native_table(table: pa.Table, metadata: dict, expected_oids: list[int]) -> tuple[pa.Table, dict]:
    """Validate exact native Arrow shape and return source-faithful candidate table."""
    if not isinstance(table, pa.Table) or table.column_names != EXPECTED_COLUMN_ORDER:
        raise LocksQuarantineError("native_arrow_field_order_or_roster_drift")
    if table.num_rows != EXPECTED_COUNT:
        raise LocksQuarantineError("native_feature_count_drift")
    if set(table.schema.names) != set(EXPECTED_COLUMN_ORDER):
        raise LocksQuarantineError("native_arrow_field_set_drift")
    for name, expected_type in EXPECTED_PYARROW_TYPES.items():
        if table.schema.field(name).type != expected_type:
            raise LocksQuarantineError("native_arrow_field_type_drift:" + name)
    oid = table.column("OBJECTID")
    oids = [int(v.as_py()) for v in oid]
    if (oid.null_count or len(set(oids)) != EXPECTED_COUNT or sorted(oids) != expected_oids):
        raise LocksQuarantineError("native_oid_roster_drift")
    blob = table.column("SE_ANNO_CAD_DATA")
    if blob.null_count or any(value is None or len(value) != 0 for value in blob.to_pylist()):
        raise LocksQuarantineError("native_blob_null_or_nonempty_drift")
    geometry = table.column("SHAPE")
    if geometry.null_count:
        raise LocksQuarantineError("native_geometry_null")
    shapes = [shapely.from_wkb(value) for value in geometry.to_pylist()]
    if (any(shape.geom_type != "Point" or shape.is_empty or not shape.is_valid for shape in shapes)
            or any(not math.isfinite(c) for shape in shapes for c in shape.coords[0])):
        raise LocksQuarantineError("native_point_geometry_invalid")
    if metadata.get("crs") != "EPSG:4019" or metadata.get("geometry_type") != "Point":
        raise LocksQuarantineError("native_geodatabase_crs_or_geometry_drift")
    service_schema = _validate_layer_metadata(metadata.get("layer_metadata", {}))
    # GDAL's Arrow reader returns OID as int64 FID, followed by 47 attributes
    # and SHAPE. The API contract declares OID separately from the attributes.
    if table.column_names != ["OBJECTID", *EXPECTED_ATTRIBUTE_NAMES, "SHAPE"]:
        raise LocksQuarantineError("native_attribute_order_drift")
    audit = {"feature_count": table.num_rows, "source_oids": oids,
             "source_id_sha256": digest(canonical(oids)),
             "source_schema_sha256": digest(canonical(service_schema)),
             "source_geometry_type": "Point", "source_wkid": SOURCE_WKID,
             "source_blob_type": "esriFieldTypeBlob -> Arrow binary",
             "source_blob_nonnull_count": EXPECTED_COUNT,
             "source_blob_empty_count": EXPECTED_COUNT,
             "source_blob_payload_bytes": 0,
             "native_field_count_including_oid": len(EXPECTED_FIELDS),
             "native_attribute_count_excluding_oid": len(EXPECTED_ATTRIBUTE_NAMES),
             "native_field_names": table.column_names[:-1],
             "native_crs_authority": "EPSG:4019",
             "native_crs_name": "Unknown datum based upon the GRS 1980 ellipsoid",
             "native_crs_mapping_approved": False,
             "coordinate_transformation": "none"}
    return table, audit


def _validate_service_bundle(captures: dict[str, tuple[bytes, dict]]) -> tuple[dict, dict, list[int], bytes, dict]:
    required={"metadata_before","count_before","ids_before","create_replica","result_head","archive","metadata_after","count_after","ids_after"}
    if not required <= captures.keys():
        raise LocksQuarantineError("capture_roster_missing_required_inputs")
    polls=sorted(name for name in captures if name.startswith("poll_"))
    if (set(captures) != required | set(polls) or not 1 <= len(polls) <= MAX_POLL_COUNT
            or polls != [f"poll_{index:02d}" for index in range(1, len(polls) + 1)]):
        raise LocksQuarantineError("capture_roster_has_unknown_or_noncontiguous_inputs")
    static_urls={
        "metadata_before":("GET",LAYER_URL+"?f=json"),
        "count_before":("GET",LAYER_URL+"/query?"+urlencode({"where":"1=1","returnCountOnly":"true","f":"json"})),
        "ids_before":("GET",LAYER_URL+"/query?"+urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"})),
        "create_replica":("POST",CREATE_REPLICA_URL),
        "result_head":("HEAD",None), "archive":("GET",None),
        "metadata_after":("GET",LAYER_URL+"?f=json"),
        "count_after":("GET",LAYER_URL+"/query?"+urlencode({"where":"1=1","returnCountOnly":"true","f":"json"})),
        "ids_after":("GET",LAYER_URL+"/query?"+urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"})),
    }
    for name,(method,url) in static_urls.items():
        receipt=captures[name][1]
        if receipt.get("method")!=method or (url is not None and receipt.get("url")!=url):
            raise LocksQuarantineError("capture_request_identity_drift:"+name)
    before=_strict_json(captures["metadata_before"][0],"metadata_before")
    after=_strict_json(captures["metadata_after"][0],"metadata_after")
    schema_before=_validate_layer_metadata(before); schema_after=_validate_layer_metadata(after)
    if schema_before != schema_after:
        raise LocksQuarantineError("source_schema_changed_during_capture")
    ids_before=_validate_count_ids(_strict_json(captures["count_before"][0],"count_before"),
                                   _strict_json(captures["ids_before"][0],"ids_before"),"before")
    ids_after=_validate_count_ids(_strict_json(captures["count_after"][0],"count_after"),
                                  _strict_json(captures["ids_after"][0],"ids_after"),"after")
    if ids_before != ids_after:
        raise LocksQuarantineError("source_ids_changed_during_capture")
    create=_strict_json(captures["create_replica"][0],"create_replica")
    status_url=_validate_url(create.get("statusUrl"),path_prefix="/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Locks/FeatureServer/jobs/")
    job=None
    for index,name in enumerate(polls):
        if captures[name][1].get("method")!="GET" or captures[name][1].get("url")!=status_url+"?f=json":
            raise LocksQuarantineError("replica_status_request_identity_drift")
        job=_strict_json(captures[name][0],name)
        if job.get("status") == "Completed":
            if index != len(polls)-1:
                raise LocksQuarantineError("replica_status_poll_after_completion")
            break
        if job.get("status") in {"Failed","Cancelled","TimedOut","esriJobFailed","esriJobCancelled","esriJobTimedOut"}:
            raise LocksQuarantineError("replica_job_failed")
    if not isinstance(job,dict) or job.get("status") != "Completed" or job.get("recordCount") != EXPECTED_COUNT:
        raise LocksQuarantineError("replica_job_not_complete_or_count_drift")
    result_url=_validate_url(job.get("resultUrl"),path_prefix="/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Locks/FeatureServer/replicafilescache/")
    if (captures["result_head"][1].get("url")!=result_url
            or captures["archive"][1].get("url")!=result_url):
        raise LocksQuarantineError("replica_download_request_identity_drift")
    for name in ("result_head","archive"):
        receipt=captures[name][1];redirects=receipt.get("redirects",[]);current=result_url
        if not isinstance(redirects,list) or len(redirects)>MAX_DOWNLOAD_REDIRECTS:
            raise LocksQuarantineError("replica_download_redirect_chain_invalid:"+name)
        for index,item in enumerate(redirects,1):
            if (not isinstance(item,dict) or item.get("url")!=current
                    or item.get("status") not in {301,302,303,307,308}
                    or item.get("artifact_prefix")!=f"{name}.redirect_{index:02d}"):
                raise LocksQuarantineError("replica_download_redirect_chain_invalid:"+name)
            target=_official_download_target(current,item.get("location",""))
            if item.get("target_url")!=target:
                raise LocksQuarantineError("replica_download_redirect_chain_invalid:"+name)
            current=target
        if receipt.get("final_url")!=current:
            raise LocksQuarantineError("replica_download_redirect_final_url_drift:"+name)
    head=captures["result_head"][1]
    head_length = head.get("response_headers", {}).get("content-length")
    if (head.get("status") != 200 or not isinstance(head_length, str)
            or not head_length.isdigit() or not 0 < int(head_length) <= MAX_ARCHIVE_BYTES):
        raise LocksQuarantineError("replica_result_head_failed_or_size_invalid")
    archive=captures["archive"][0]
    gdb_root,zip_info=_safe_gdb_archive(archive)
    if job.get("replicaName") != create.get("replicaName"):
        raise LocksQuarantineError("replica_name_response_mismatch")
    if head_length != str(len(archive)):
        raise LocksQuarantineError("replica_archive_length_header_mismatch")
    return before, {"status_url":status_url,"result_url":result_url,"job":job,"zip":zip_info,"gdb_root":gdb_root}, ids_before, archive, {**schema_before,"metadata":before}


def _load_gdb_table(archive_path: Path, gdb_root: str, metadata: dict) -> pa.Table:
    uri=f"/vsizip/{archive_path}/{gdb_root}"
    try:
        info=pyogrio.read_info(uri,layer=LAYER_NAME)
        if info.get("geometry_type") != "Point" or info.get("crs") != "EPSG:4019" or info.get("features") != EXPECTED_COUNT:
            raise LocksQuarantineError("native_gdb_layer_metadata_drift")
        _meta,table=pyogrio.read_arrow(uri,layer=LAYER_NAME,return_fids=True)
    except LocksQuarantineError:
        raise
    except Exception as exc:
        raise LocksQuarantineError("native_gdb_layer_unreadable:"+type(exc).__name__) from exc
    return table


def _capture_manifest(root: Path) -> dict:
    path=root/"capture_manifest.json"
    if path.is_symlink() or not path.is_file():
        raise LocksQuarantineError("capture_manifest_missing_or_unsafe")
    manifest=_strict_json(path.read_bytes(),"capture_manifest")
    if manifest.get("complete") is not True or not isinstance(manifest.get("artifact_roster"),dict):
        raise LocksQuarantineError("capture_manifest_not_sealed")
    expected=manifest["artifact_roster"]
    actual={}
    for p in sorted(root.iterdir()):
        if p.name=="capture_manifest.json":continue
        if p.is_symlink() or not stat.S_ISREG(p.lstat().st_mode):
            raise LocksQuarantineError("capture_path_nonregular")
        b=p.read_bytes();actual[p.name]={"bytes":len(b),"sha256":digest(b)}
    if actual != expected:
        raise LocksQuarantineError("capture_artifact_roster_mismatch")
    if sum(v["bytes"] for v in actual.values()) > MAX_CAPTURE_BYTES:
        raise LocksQuarantineError("capture_bundle_over_limit")
    return manifest


def _load_capture(root: Path) -> tuple[dict[str,tuple[bytes,dict]],dict]:
    manifest=_capture_manifest(root)
    continued=manifest.get("continued_completed_replica")
    if continued is not None:
        if (not isinstance(continued,dict) or continued.get("failed_head_status")!=302
                or continued.get("completed_replica_record_count")!=EXPECTED_COUNT
                or not isinstance(continued.get("artifact_roster_sha256"),str)
                or not re.fullmatch(r"[0-9a-f]{64}",continued["artifact_roster_sha256"])
                or not isinstance(continued.get("failed_head_observed_at"),str)):
            raise LocksQuarantineError("continued_replica_manifest_invalid")
    receipts=manifest.get("receipts")
    if not isinstance(receipts,dict):raise LocksQuarantineError("capture_receipts_missing")
    captures={}
    expected_artifacts={"create_replica.request.body"}
    for name,receipt in receipts.items():
        if not isinstance(name,str) or not name.replace("_","").isalnum() or not isinstance(receipt,dict):
            raise LocksQuarantineError("capture_receipt_roster_invalid")
        paths=[root/f"{name}.body",root/f"{name}.wire",root/f"{name}.receipt.json"]
        expected_artifacts.update(path.name for path in paths)
        if any(p.is_symlink() or not p.is_file() for p in paths):
            raise LocksQuarantineError("capture_receipt_object_missing_or_unsafe:"+name)
        body,wire,receipt_body=(p.read_bytes() for p in paths)
        persisted=_strict_json(receipt_body,name+"_receipt")
        if (persisted != receipt or receipt.get("complete_body") is not True or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != digest(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != digest(wire)):
            raise LocksQuarantineError("capture_receipt_hash_or_completeness_drift:"+name)
        redirects=receipt.get("redirects",[])
        if not isinstance(redirects,list) or len(redirects)>MAX_DOWNLOAD_REDIRECTS:
            raise LocksQuarantineError("capture_redirect_receipt_roster_invalid:"+name)
        current=receipt.get("url")
        for index,item in enumerate(redirects,1):
            if (not isinstance(item,dict) or item.get("artifact_prefix")!=f"{name}.redirect_{index:02d}"
                    or item.get("url")!=current or item.get("status") not in {301,302,303,307,308}
                    or item.get("complete_body") is not True):
                raise LocksQuarantineError("capture_redirect_receipt_invalid:"+name)
            target=_official_download_target(current,item.get("location",""))
            if item.get("target_url")!=target:
                raise LocksQuarantineError("capture_redirect_receipt_invalid:"+name)
            prefix=item["artifact_prefix"]
            rb=root/f"{prefix}.body";rw=root/f"{prefix}.wire";rr=root/f"{prefix}.receipt.json"
            if any(p.is_symlink() or not p.is_file() for p in (rb,rw,rr)):
                raise LocksQuarantineError("capture_redirect_artifact_missing_or_unsafe:"+name)
            redirect_body=rb.read_bytes();redirect_wire=rw.read_bytes()
            redirect_receipt=_strict_json(rr.read_bytes(),prefix+"_receipt")
            expected_redirect={k:v for k,v in item.items() if k not in {"artifact_prefix","target_url"}}
            if (redirect_receipt!=expected_redirect or len(redirect_body)!=item.get("bytes")
                    or digest(redirect_body)!=item.get("sha256") or len(redirect_wire)!=item.get("wire_bytes")
                    or digest(redirect_wire)!=item.get("wire_sha256")):
                raise LocksQuarantineError("capture_redirect_artifact_hash_drift:"+name)
            expected_artifacts.update(p.name for p in (rb,rw,rr));current=target
        if receipt.get("final_url")!=current:
            raise LocksQuarantineError("capture_redirect_final_url_drift:"+name)
        captures[name]=(body,receipt)
    # createReplica request body is separately pinned because its response receipt
    # describes the HTTP response, not the submitted form body.
    request_body=root/"create_replica.request.body"
    if request_body.is_symlink() or not request_body.is_file():
        raise LocksQuarantineError("create_replica_request_body_missing")
    request_info=manifest.get("create_replica_request")
    request_raw=request_body.read_bytes()
    if (not isinstance(request_info,dict) or request_info.get("bytes")!=len(request_raw)
            or request_info.get("sha256")!=digest(request_raw)):
        raise LocksQuarantineError("create_replica_request_hash_drift")
    params=request_info.get("parameters")
    expected_params={"layers":"0","syncModel":"none","dataFormat":"filegdb",
        "transportType":"esriTransportTypeUrl","async":"true","returnAttachments":"false","f":"json"}
    if (not isinstance(params,dict) or any(params.get(k)!=v for k,v in expected_params.items())
            or set(params)!=(set(expected_params)|{"replicaName"})
            or not isinstance(params.get("replicaName"),str)
            or not params["replicaName"].startswith("hifld_locks_151_")):
        raise LocksQuarantineError("create_replica_request_parameters_drift")
    try:
        from urllib.parse import parse_qsl
        parsed_pairs=parse_qsl(request_raw.decode("ascii"),strict_parsing=True)
        parsed_params=dict(parsed_pairs)
    except Exception as exc:
        raise LocksQuarantineError("create_replica_request_body_invalid") from exc
    if len(parsed_params)!=len(parsed_pairs) or parsed_params!=params:
        raise LocksQuarantineError("create_replica_request_body_parameters_drift")
    actual_artifacts={path.name for path in root.iterdir() if path.name!="capture_manifest.json"}
    if actual_artifacts!=expected_artifacts:
        raise LocksQuarantineError("capture_manifest_receipt_file_roster_drift")
    return captures,manifest


def _write_artifact(path: Path, body: bytes) -> None:
    if path.exists() or path.is_symlink():
        raise LocksQuarantineError("capture_attempt_object_already_exists")
    with path.open("xb") as f:
        f.write(body)


def _official_download_target(current_url: str, location: str) -> str:
    target=urljoin(current_url,location)
    parsed=urlsplit(target)
    host=(parsed.hostname or "").lower()
    allowed=any(host==domain or host.endswith("."+domain) for domain in OFFICIAL_DOWNLOAD_HOSTS)
    if (parsed.scheme!="https" or not allowed or parsed.username is not None
            or parsed.password is not None or parsed.port not in (None,443)
            or not parsed.path.startswith("/")):
        raise LocksQuarantineError("replica_download_redirect_target_not_official_https")
    return target


def _perform(client: httpx.Client, method: str, url: str, root: Path, name: str, *,
             content: bytes | None = None, max_bytes: int = MAX_RECEIPT_BYTES,
             request_headers: dict[str,str] | None = None,
             budget: list[int] | None = None,
             allow_official_redirects: bool = False) -> tuple[bytes,dict]:
    observed=None
    body_path=root/f"{name}.body";wire_path=root/f"{name}.wire";receipt_path=root/f"{name}.receipt.json"
    for p in (body_path,wire_path,receipt_path):
        if p.exists() or p.is_symlink():raise LocksQuarantineError("capture_artifact_exists:"+name)
    req_headers={"Accept-Encoding":"identity","User-Agent":"fulton-ring/hifld-next-datasets/151-draft"}
    if request_headers:req_headers.update(request_headers)
    redirects=[];current_url=url;final_result=None;response=None;request_error=None
    final_url=None;status=None;headers={};body_hash=hashlib.sha256();wire_hash=hashlib.sha256()
    body_size=wire_size=0
    def retain(path,kind,counts):
        def sink(block):
            if budget is not None and budget[0]+len(block)>MAX_CAPTURE_BYTES:
                raise LocksQuarantineError("capture_bundle_over_limit")
            with path.open("ab") as stream:stream.write(block)
            counts[kind]+=len(block)
            if budget is not None:budget[0]+=len(block)
        return sink
    try:
        for hop in range(MAX_DOWNLOAD_REDIRECTS+1):
            client.cookies.clear()
            with client.stream(method,current_url,content=content if hop==0 else None,
                               headers=req_headers,follow_redirects=False) as response:
                status=response.status_code;headers={k.lower():v for k,v in response.headers.items()
                    if k.lower() in {"content-type","content-length","last-modified","etag","content-disposition","content-encoding","location"}}
                if status in {301,302,303,307,308} and allow_official_redirects:
                    if method not in {"GET","HEAD"}:
                        raise LocksQuarantineError("replica_download_redirect_limit_or_method_invalid")
                    location=response.headers.get("location")
                    if not isinstance(location,str) or not location:
                        raise LocksQuarantineError("replica_download_redirect_location_missing")
                    prefix=f"{name}.redirect_{hop+1:02d}"
                    rb=root/f"{prefix}.body";rw=root/f"{prefix}.wire";rr=root/f"{prefix}.receipt.json"
                    for path in (rb,rw,rr):
                        if path.exists() or path.is_symlink():raise LocksQuarantineError("capture_artifact_exists:"+prefix)
                    rb.touch();rw.touch()
                    counts={"body":0,"wire":0}
                    redirect_result=None
                    if method=="HEAD":
                        pass
                    else:
                        redirect_result=capture_http_response(response,max_wire_bytes=MAX_RECEIPT_BYTES,
                            max_decoded_bytes=MAX_RECEIPT_BYTES,reserve=lambda progress: None,
                            retain_wire=retain(rw,"wire",counts),retain_decoded=retain(rb,"body",counts))
                    redirect_receipt={"method":method,"url":current_url,"status":status,
                        "observed_at":datetime.now(timezone.utc).isoformat(),"location":location,
                        "response_headers":headers,"complete_body":bool(method=="HEAD" or
                            redirect_result is not None and redirect_result.complete),
                        "bytes":counts["body"],"sha256":digest(rb.read_bytes()),
                        "wire_bytes":counts["wire"],"wire_sha256":digest(rw.read_bytes())}
                    _write_artifact(rr,canonical(redirect_receipt))
                    target=_official_download_target(current_url,location)
                    redirects.append({**redirect_receipt,"artifact_prefix":prefix,"target_url":target})
                    if hop>=MAX_DOWNLOAD_REDIRECTS:
                        raise LocksQuarantineError("replica_download_redirect_limit_or_method_invalid")
                    current_url=target
                    content=None
                    continue
                final_url=str(response.url)
                if method=="HEAD":
                    body_path.touch(exist_ok=True);wire_path.touch(exist_ok=True)
                else:
                    counts={"body":0,"wire":0}
                    final_result=capture_http_response(response,max_wire_bytes=max_bytes,max_decoded_bytes=max_bytes,
                        reserve=lambda progress: None,retain_wire=retain(wire_path,"wire",counts),
                        retain_decoded=retain(body_path,"body",counts))
                    body_size=counts["body"];wire_size=counts["wire"]
                observed=datetime.now(timezone.utc).isoformat()
                break
        else:
            raise LocksQuarantineError("replica_download_redirect_limit_exceeded")
    except LocksQuarantineError:
        raise
    except Exception as exc:
        request_error=type(exc).__name__;body_path.touch(exist_ok=True);wire_path.touch(exist_ok=True)
    body=body_path.read_bytes() if body_path.exists() else b"";wire=wire_path.read_bytes() if wire_path.exists() else b""
    body_hash.update(body);wire_hash.update(wire)
    complete=bool((final_result is not None and final_result.complete and request_error is None)
                  or (method=="HEAD" and status==200 and request_error is None))
    receipt={"method":method,"url":url,"final_url":final_url,"observed_at":observed,"status":status,
        "complete_body":complete,"wire_bytes":len(wire),"wire_sha256":wire_hash.hexdigest(),
        "bytes":len(body),"sha256":body_hash.hexdigest(),"redirects":redirects,
        "content_encoding":final_result.content_encoding if final_result is not None else None,
        "content_length":final_result.content_length if final_result is not None else headers.get("content-length"),
        "response_headers":headers,"error_code":final_result.error_code if final_result is not None else
            ("request_error" if request_error else None),
        "error_type":final_result.error_type if final_result is not None else request_error}
    if method=="POST":receipt.update({"request_body_bytes":len(content or b""),"request_body_sha256":digest(content or b"")})
    _write_artifact(receipt_path,canonical(receipt))
    if (not complete or status!=200 or final_url is None or len(body)>max_bytes or len(wire)>max_bytes
            or digest(body)!=receipt["sha256"] or digest(wire)!=receipt["wire_sha256"]):
        raise LocksQuarantineError("http_capture_incomplete_or_status_invalid:"+name)
    return body,receipt


def _load_partial_completed_replica(root: Path) -> tuple[dict[str,tuple[bytes,dict]],dict,dict,str,dict]:
    if root.is_symlink() or not root.is_dir() or any(p.is_symlink() for p in (root,*root.parents)):
        raise LocksQuarantineError("unsafe_resume_capture_root")
    artifact_names={p.name for p in root.iterdir()}
    if "capture_manifest.json" in artifact_names:
        raise LocksQuarantineError("resume_source_must_be_unsealed_partial_capture")
    receipts={};expected={"create_replica.request.body"}
    names={"metadata_before","count_before","ids_before","create_replica","result_head"}
    names.update(p.name.removesuffix(".body") for p in root.glob("poll_*.body"))
    polls=sorted(n for n in names if n.startswith("poll_"))
    if not 1<=len(polls)<=MAX_POLL_COUNT or polls!=[f"poll_{i:02d}" for i in range(1,len(polls)+1)]:
        raise LocksQuarantineError("resume_replica_poll_receipt_roster_invalid")
    for name in sorted(names):
        paths=(root/f"{name}.body",root/f"{name}.wire",root/f"{name}.receipt.json")
        if any(p.is_symlink() or not p.is_file() or not stat.S_ISREG(p.lstat().st_mode) for p in paths):
            raise LocksQuarantineError("resume_replica_artifact_missing_or_unsafe:"+name)
        body,wire,receipt_raw=(p.read_bytes() for p in paths)
        receipt=_strict_json(receipt_raw,name+"_receipt")
        expected.update(p.name for p in paths)
        status_ok=(receipt.get("status")==302 and receipt.get("complete_body") is False
                   if name=="result_head" else receipt.get("status")==200 and receipt.get("complete_body") is True)
        if (not status_ok or receipt.get("bytes")!=len(body) or receipt.get("sha256")!=digest(body)
                or receipt.get("wire_bytes")!=len(wire) or receipt.get("wire_sha256")!=digest(wire)):
            raise LocksQuarantineError("resume_replica_receipt_integrity_failure:"+name)
        if name!="result_head":
            receipts[name]=(body,receipt)
    if artifact_names!=expected:
        raise LocksQuarantineError("resume_replica_artifact_roster_drift")
    request_path=root/"create_replica.request.body"
    request_raw=request_path.read_bytes()
    try:
        pairs=parse_qsl(request_raw.decode("ascii"),strict_parsing=True)
        params=dict(pairs)
    except Exception as exc:
        raise LocksQuarantineError("resume_replica_request_body_invalid") from exc
    expected_params={"layers":"0","syncModel":"none","dataFormat":"filegdb",
        "transportType":"esriTransportTypeUrl","async":"true","returnAttachments":"false","f":"json"}
    if (len(params)!=len(pairs) or any(params.get(k)!=v for k,v in expected_params.items())
            or set(params)!=(set(expected_params)|{"replicaName"})
            or not params["replicaName"].startswith("hifld_locks_151_")):
        raise LocksQuarantineError("resume_replica_request_parameters_invalid")
    request_info={"parameters":params,"bytes":len(request_raw),"sha256":digest(request_raw)}
    metadata=_strict_json(receipts["metadata_before"][0],"metadata_before")
    schema=_validate_layer_metadata(metadata)
    ids=_validate_count_ids(_strict_json(receipts["count_before"][0],"count_before"),
        _strict_json(receipts["ids_before"][0],"ids_before"),"before")
    create=_strict_json(receipts["create_replica"][0],"create_replica")
    status_url=_validate_url(create.get("statusUrl"),path_prefix="/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Locks/FeatureServer/jobs/")
    job=None
    for index,name in enumerate(polls):
        receipt=receipts[name][1]
        if receipt.get("url")!=status_url+"?f=json" or receipt.get("method")!="GET":
            raise LocksQuarantineError("resume_replica_poll_request_identity_drift")
        job=_strict_json(receipts[name][0],name)
        if job.get("status")=="Completed" and index!=len(polls)-1:
            raise LocksQuarantineError("resume_replica_poll_after_completion")
    if not isinstance(job,dict) or job.get("status")!="Completed" or job.get("recordCount")!=EXPECTED_COUNT:
        raise LocksQuarantineError("resume_replica_not_completed_or_count_drift")
    result_url=_validate_url(job.get("resultUrl"),path_prefix="/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Locks/FeatureServer/replicafilescache/")
    head= _strict_json((root/"result_head.receipt.json").read_bytes(),"result_head_receipt")
    if (head.get("method")!="HEAD" or head.get("url")!=result_url
            or not isinstance(head.get("response_headers",{}).get("content-length"),str)):
        raise LocksQuarantineError("resume_replica_failed_head_identity_drift")
    prior_roster={p.name:{"bytes":p.stat().st_size,"sha256":digest(p.read_bytes())} for p in sorted(root.iterdir())}
    prior={"artifact_roster_sha256":digest(canonical(prior_roster)),
           "failed_head_status":head["status"],"failed_head_observed_at":head.get("observed_at"),
           "completed_replica_record_count":job["recordCount"]}
    return receipts,request_info,params,result_url,prior


def _capture_live(root: Path, *, resume_from: Path | None = None) -> tuple[dict[str,tuple[bytes,dict]],dict]:
    if root.is_symlink() or ".." in root.parts or any(parent.is_symlink() for parent in root.parents):
        raise LocksQuarantineError("unsafe_live_capture_root")
    root.mkdir(parents=True,exist_ok=True)
    if root.is_symlink() or not root.is_dir():
        raise LocksQuarantineError("unsafe_live_capture_root")
    if any(root.iterdir()):raise LocksQuarantineError("live_capture_directory_not_empty")
    free=shutil.disk_usage(root).free
    if free < MIN_FREE_BYTES+MAX_CAPTURE_BYTES:
        raise LocksQuarantineError("storage_floor_before_source_capture")
    captures={};budget=[0];continued=None
    if resume_from is None:
        request_params={"replicaName":"hifld_locks_151_"+uuid4().hex,"layers":"0","syncModel":"none",
            "dataFormat":"filegdb","transportType":"esriTransportTypeUrl","async":"true",
            "returnAttachments":"false","f":"json"}
        request_body=urlencode(request_params).encode("ascii")
        _write_artifact(root/"create_replica.request.body",request_body)
        request_info={"parameters":request_params,"bytes":len(request_body),"sha256":digest(request_body)}
    else:
        resume_from=Path(resume_from).absolute()
        captures,request_info,request_params,result_url,continued=_load_partial_completed_replica(resume_from)
        for name,(body,receipt) in captures.items():
            for suffix,raw in (("body",body),( "wire",(resume_from/f"{name}.wire").read_bytes()),
                               ("receipt.json",canonical(receipt))):
                _write_artifact(root/f"{name}.{suffix}",raw)
                if suffix in {"body","wire"}:budget[0]+=len(raw)
        _write_artifact(root/"create_replica.request.body",(resume_from/"create_replica.request.body").read_bytes())
        if budget[0]>MAX_CAPTURE_BYTES:raise LocksQuarantineError("capture_bundle_over_limit")
    with httpx.Client(timeout=httpx.Timeout(15,connect=10),follow_redirects=False) as client:
        def get(name,url,limit=MAX_RECEIPT_BYTES):
            body,receipt=_perform(client,"GET",url,root,name,max_bytes=limit,budget=budget)
            captures[name]=(body,receipt);return body
        meta_url=LAYER_URL+"?f=json";query=LAYER_URL+"/query?"
        if resume_from is None:
            get("metadata_before",meta_url)
            get("count_before",query+urlencode({"where":"1=1","returnCountOnly":"true","f":"json"}))
            get("ids_before",query+urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"}))
            response,receipt=_perform(client,"POST",CREATE_REPLICA_URL,root,"create_replica",
                content=request_body,max_bytes=MAX_RECEIPT_BYTES,
                request_headers={"Content-Type":"application/x-www-form-urlencoded"},budget=budget)
            captures["create_replica"]=(response,receipt)
            result=_strict_json(response,"create_replica")
            status_url=_validate_url(result.get("statusUrl"),path_prefix="/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Locks/FeatureServer/jobs/")
            job=None;poll_started=time.monotonic()
            for index in range(1,MAX_POLL_COUNT+1):
                if time.monotonic()-poll_started >= MAX_POLL_ELAPSED_SECONDS:break
                poll_name=f"poll_{index:02d}"
                poll_body,poll_receipt=_perform(client,"GET",status_url+"?f=json",root,poll_name,
                                               max_bytes=MAX_RECEIPT_BYTES,budget=budget)
                captures[poll_name]=(poll_body,poll_receipt);job=_strict_json(poll_body,poll_name)
                if job.get("status")=="Completed":break
                if job.get("status") in {"Failed","Cancelled","TimedOut","esriJobFailed","esriJobCancelled","esriJobTimedOut"}:
                    raise LocksQuarantineError("replica_job_failed")
                if index<MAX_POLL_COUNT:time.sleep(POLL_INTERVAL_SECONDS)
            if not isinstance(job,dict) or job.get("status")!="Completed" or job.get("recordCount")!=EXPECTED_COUNT:
                raise LocksQuarantineError("replica_job_not_complete_or_count_drift")
            result_url=_validate_url(job.get("resultUrl"),path_prefix="/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Locks/FeatureServer/replicafilescache/")
        head_body,head_receipt=_perform(client,"HEAD",result_url,root,"result_head",max_bytes=MAX_RECEIPT_BYTES,
                                        budget=budget,allow_official_redirects=True)
        captures["result_head"]=(head_body,head_receipt)
        content_length=head_receipt.get("response_headers",{}).get("content-length")
        if (head_receipt.get("status")!=200 or not isinstance(content_length,str)
                or not content_length.isdigit() or int(content_length)>MAX_ARCHIVE_BYTES):
            raise LocksQuarantineError("replica_filegdb_head_size_or_status_invalid")
        body,receipt=_perform(client,"GET",result_url,root,"archive",max_bytes=MAX_ARCHIVE_BYTES,
                              budget=budget,allow_official_redirects=True)
        captures["archive"]=(body,receipt)
        get("metadata_after",meta_url)
        get("count_after",query+urlencode({"where":"1=1","returnCountOnly":"true","f":"json"}))
        get("ids_after",query+urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"}))
    if request_info["bytes"]>MAX_RECEIPT_BYTES:raise LocksQuarantineError("replica_request_body_over_limit")
    artifact_roster={p.name:{"bytes":p.stat().st_size,"sha256":digest(p.read_bytes())}
                     for p in sorted(root.iterdir()) if p.name!="capture_manifest.json"}
    if sum(item["bytes"] for item in artifact_roster.values())>MAX_CAPTURE_BYTES:
        raise LocksQuarantineError("capture_bundle_over_limit")
    manifest={"schema_version":1,"complete":True,"captured_at":datetime.now(timezone.utc).isoformat(),
        "replica_name":request_params["replicaName"],"create_replica_request":request_info,
        "receipts":{name:receipt for name,(_body,receipt) in sorted(captures.items())},
        "artifact_roster":artifact_roster}
    if continued is not None:manifest["continued_completed_replica"]=continued
    _write_artifact(root/"capture_manifest.json",canonical(manifest))
    return _load_capture(root)


def _write_capture_history(storage: StagingStorageResource, capture_root: Path, manifest: dict,
                           captures: dict[str,tuple[bytes,dict]]) -> str:
    capture_id=digest(canonical({"archive_sha256":captures["archive"][1]["sha256"],
                                 "roster":manifest["artifact_roster"]}))
    prefix=f"_raw-captures/{DATASET_SLUG}/{capture_id}"
    for path in sorted(capture_root.iterdir()):
        if path.name=="capture_manifest.json":continue
        body=path.read_bytes()
        write_immutable(storage,f"{prefix}/{path.name}",body)
    write_immutable(storage,f"{prefix}/capture_manifest.json",canonical(manifest))
    return prefix


def _preserve_failed_capture(storage: StagingStorageResource, root: Path, exc: Exception) -> str:
    """Retain every regular artifact left by a failed source attempt."""
    prefix=f"_quarantine/{DATASET_SLUG}/{uuid4().hex}"
    for path in sorted(root.iterdir()) if root.exists() and root.is_dir() and not root.is_symlink() else []:
        if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
            continue
        body=path.read_bytes()
        write_immutable(storage,f"{prefix}/raw/{path.name}",body)
    write_immutable(storage,f"{prefix}/metadata/failure.json",canonical({
        "approved":False,"reason_code":type(exc).__name__,
        "artifact_directory_complete":(root/"capture_manifest.json").is_file()}))
    return prefix


def _load_native_table(captures: dict[str,tuple[bytes,dict]], schema: dict, ids: list[int]) -> tuple[pa.Table,dict]:
    archive=captures["archive"][0]
    gdb_root,zip_audit=_safe_gdb_archive(archive)
    # The original ZIP is preserved unchanged; /vsizip reads it in place.
    temp_dir=Path("/tmp/hifld-native-reconstruction/151/vsizip")
    if temp_dir.is_symlink() or ".." in temp_dir.parts or any(parent.is_symlink() for parent in temp_dir.parents):
        raise LocksQuarantineError("unsafe_native_archive_cache_root")
    temp_dir.mkdir(parents=True,exist_ok=True)
    path=temp_dir/(digest(archive)+".zip")
    if path.is_symlink():
        raise LocksQuarantineError("content_addressed_archive_path_mismatch")
    if not path.exists():
        try:
            with path.open("xb") as stream:
                stream.write(archive)
        except FileExistsError as exc:
            raise LocksQuarantineError("content_addressed_archive_path_raced") from exc
    elif not path.is_file() or path.read_bytes()!=archive:
        raise LocksQuarantineError("content_addressed_archive_path_mismatch")
    if path.stat().st_nlink!=1:
        raise LocksQuarantineError("archive_path_link_count_invalid")
    uri=f"/vsizip/{path}/{gdb_root}"
    try:
        info=pyogrio.read_info(uri,layer=LAYER_NAME)
        if (info.get("features")!=EXPECTED_COUNT or info.get("geometry_type")!="Point"
                or info.get("crs")!="EPSG:4019" or info.get("fid_column")!="OBJECTID"):
            raise LocksQuarantineError("native_filegdb_layer_identity_drift")
        gdal_meta,table=pyogrio.read_arrow(uri,layer=LAYER_NAME,return_fids=True)
    except LocksQuarantineError:raise
    except Exception as exc:raise LocksQuarantineError("native_filegdb_unreadable:"+type(exc).__name__) from exc
    if gdal_meta.get("fid_column")!="OBJECTID" or gdal_meta.get("crs")!="EPSG:4019":
        raise LocksQuarantineError("native_filegdb_ogr_metadata_drift")
    if table.column_names!=EXPECTED_COLUMN_ORDER or table.num_rows!=EXPECTED_COUNT:
        raise LocksQuarantineError("native_filegdb_arrow_schema_or_count_drift")
    for f in schema["fields"]:
        if f["name"]!="OBJECTID":
            i=table.schema.get_field_index(f["name"])
            field=table.schema.field(i)
            if f["type"]=="esriFieldTypeString":
                width=(field.metadata or {}).get(b"GDAL:OGR:width")
                if width is None or int(width)!=f["length"]:
                    raise LocksQuarantineError("native_string_width_drift:"+f["name"])
    table,native_audit=validate_native_table(table,{"crs":info.get("crs"),"geometry_type":info.get("geometry_type"),"layer_metadata":schema["metadata"]},ids)
    native_audit.update({"filegdb_root":gdb_root,"zip_expanded_bytes":zip_audit["expanded_bytes"],
                         "zip_member_count":len(zip_audit["members"]),"zip_member_sha256":digest(canonical(zip_audit["members"]))})
    return table,native_audit


def _canonical_geoparquet(table: pa.Table) -> bytes:
    import io
    geoms=[shapely.from_wkb(v) for v in table.column("SHAPE").to_pylist()]
    xs=[float(g.x) for g in geoms];ys=[float(g.y) for g in geoms]
    crs_json=pyproj.CRS.from_epsg(SOURCE_WKID).to_json_dict()
    geo={"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{
        "encoding":"WKB","geometry_types":["Point"],"crs":crs_json,
        "bbox":[min(xs),min(ys),max(xs),max(ys)]}}}
    arrays=[];names=[]
    for name in table.column_names:
        source_name="SHAPE" if name=="geometry" else name
        if source_name=="SHAPE": name="geometry"
        arrays.append(table.column(source_name));names.append(name)
    output=pa.Table.from_arrays(arrays,names=names).replace_schema_metadata(
        {b"geo":json.dumps(geo,sort_keys=True,separators=(",",":"),allow_nan=False).encode()})
    sink=io.BytesIO();pq.write_table(output,sink,compression="zstd",use_dictionary=False)
    return sink.getvalue()


def _write_outputs(storage: StagingStorageResource, prefix: str, outputs: dict[str,bytes]) -> dict:
    descriptors={}
    for name,body in outputs.items():
        key=f"{prefix}/{name}";write_immutable(storage,key,body)
        snap=storage.object_snapshot(key);descriptor={"bytes":len(body),"sha256":digest(body)}
        if (snap is None or snap.size!=len(body) or storage.sha256_key(key)!=digest(body)
                or storage.object_snapshot(key)!=snap):
            raise LocksQuarantineError("candidate_write_readback_failed:"+name)
        descriptors[name]=descriptor
    return descriptors


def capture_locks_usace_ienc(storage: StagingStorageResource, *,
                             capture_directory: str | None = None,
                             resume_completed_replica_from: str | None = None) -> dict[str,object]:
    """Create/replay a bounded complete unapproved NDC/LPMS Locks candidate.

    An explicit empty capture directory starts a live capture. A populated explicit
    directory is offline-only and must contain a valid sealed capture manifest.
    A resume directory may supply an integrity-checked partial capture with an
    already-completed replica; continuation never submits another createReplica job.
    """
    storage_root=_local_storage_root(storage)
    if shutil.disk_usage(storage_root).free < MIN_FREE_BYTES:
        raise LocksQuarantineError("storage_floor_before_candidate_write")
    if resume_completed_replica_from and not capture_directory:
        raise LocksQuarantineError("replica_resume_requires_explicit_capture_directory")
    if capture_directory:
        root=Path(capture_directory).absolute()
        if ".." in root.parts or any(p.is_symlink() for p in (root,*root.parents)):
            raise LocksQuarantineError("unsafe_capture_directory")
        if root.exists() and (root.is_symlink() or not root.is_dir()):
            raise LocksQuarantineError("unsafe_capture_directory")
        if root.exists() and any(root.iterdir()):
            if resume_completed_replica_from:
                raise LocksQuarantineError("replica_resume_target_must_be_empty")
            if not (root/"capture_manifest.json").is_file():
                raise LocksQuarantineError("explicit_offline_capture_must_be_sealed")
            _capture_manifest(root)
            captures,cap_manifest=_load_capture(root)
        else:
            try:
                captures,cap_manifest=_capture_live(
                    root,resume_from=Path(resume_completed_replica_from) if resume_completed_replica_from else None)
            except Exception as exc:
                _preserve_failed_capture(storage,root,exc)
                raise
    else:
        root=CAPTURE_ROOT/(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")+"-"+uuid4().hex)
        try:
            captures,cap_manifest=_capture_live(root)
        except Exception as exc:
            _preserve_failed_capture(storage,root,exc)
            raise
    try:
        before,job_info,ids,snapshot,schema_projection=_validate_service_bundle(captures)
        archive_receipt=captures["archive"][1]
        if archive_receipt.get("bytes")!=len(snapshot) or digest(snapshot)!=archive_receipt.get("sha256"):
            raise LocksQuarantineError("archive_response_receipt_drift")
        table,native_audit=_load_native_table(captures,schema_projection,ids)
    except Exception as exc:
        _preserve_failed_capture(storage,root,exc)
        raise
    history_prefix=_write_capture_history(storage,root,cap_manifest,captures)
    parquet=_canonical_geoparquet(table)
    identity=packaged_processing_identity(Path(__file__).parents[3])
    record_sha=digest(parquet)
    schema_sha=digest(canonical({key:value for key,value in schema_projection.items() if key!="metadata"}))
    capture_sha=digest(canonical({"archive_sha256":archive_receipt["sha256"],"schema_sha256":schema_sha,
                                  "ids_sha256":native_audit["source_id_sha256"]}))
    version="v"+digest(canonical({"archive":archive_receipt["sha256"],"record":record_sha,
                                    "schema":schema_sha,"processing_identity":identity}))[:32]
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    audit={**native_audit,"source_record_sha256":record_sha,"source_schema_sha256":schema_sha,
        "source_capture_sha256":capture_sha,"source_archive_sha256":archive_receipt["sha256"],
        "source_archive_bytes":archive_receipt["bytes"],"source_capture_history_prefix":history_prefix,
        "source_feature_count":EXPECTED_COUNT,"source_id_sha256":native_audit["source_id_sha256"],
        "source_id_min":min(ids),"source_id_max":max(ids),"source_coverage_scope":"Current USACE NDC/LPMS national lock-characteristics point layer; historical HIFLD snapshot identity and baseline equivalence not yet established.",
        "drift_guard":"Before/after service metadata schema and exact 234-object ID roster; native FileGDB layer type, CRS, field names/types/string widths, IDs, non-null zero-length BLOB; ZIP entry safety/CRC/size.",
        "producer_release_date":None,"producer_release_date_reason":"ArcGIS service editingInfo dataLastEditDate is a data edit timestamp, not a producer release date.",
        "source_data_last_edit_epoch_ms":(before.get("editingInfo") or {}).get("dataLastEditDate"),
        "source_schema_last_edit_epoch_ms":(before.get("editingInfo") or {}).get("schemaLastEditDate"),
        "source_release_accepted":False,"source_freshness_accepted":False,"baseline_comparison_accepted":False,
        "schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,
        "rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    manifest={"schema_version":1,"approved":False,"title":"Locks (USACE NDC/LPMS; HIFLD inventory title: Locks (USACE IENC))",
        "description":"Current USACE Navigation Data Center Lock Characteristics/LPMS point layer. The inventory description matches the official layer description verbatim. Historical archive equivalence and the IENC title suffix remain unresolved.",
        "publisher":"U.S. Army Corps of Engineers, Navigation Data Center (USACE ArcGIS item owner usace_iwr_cac)",
        "agency":"U.S. Army Corps of Engineers","source_url":LAYER_URL,"license":LICENSE_TEXT,
        "source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next",
        "code_commit":identity["code_commit"],"code_identity_scheme":identity["code_identity_scheme"],
        "inputs":[{"input_id":name,"producer":"U.S. Army Corps of Engineers, Navigation Data Center",
            "host":urlsplit(receipt["url"]).hostname,"role":"producer_publication","canonical_url":receipt["url"],
            "retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],
            "retrieved_at":receipt["observed_at"],"http_status":receipt["status"],
            "bytes":receipt["bytes"],"sha256":receipt["sha256"],"wire_bytes":receipt["wire_bytes"],
            "wire_sha256":receipt["wire_sha256"],"license":"Publicly available without use restrictions per ArcGIS item; national-scale mapping caveat; acceptance pending.",
            "attribution":"U.S. Army Corps of Engineers, Navigation Data Center.","release_date":None,
            "release_date_unknown_reason":"Fetch/edit timestamp is not a producer release date."}
            for name,(_body,receipt) in sorted(captures.items())],
        "lineage":"The HIFLD inventory description for issue 151 exactly matches the USACE Locks service layer metadata, which names NDC, LPMS, the Lock Characteristics database, and NDC surveys. The current official source is the public USACE ArcGIS item 78ef8693f30b40b980737c561ce6b103. The separate USACE IENC master chart service has lock gate/basin classes and no LOCK_POINT class; no IENC equivalence is claimed. The DataLumos historical archive bytes/FGDC metadata have not been compared.",
        "transformations":[{"operation":"native_filegdb_to_geoparquet_candidate","parameters":{"source_format":"Official anonymous createReplica syncModel=none FileGDB export","source_layer":"Lock","record_population":"234; OBJECTID roster exact match to before/after service IDs","source_fields":"All 48 service-declared fields including OBJECTID OID, all 47 attributes and SE_ANNO_CAD_DATA Binary field retained.","binary_blob":"All 234 current BLOB values are non-null zero-length bytes. Refresh validation requires the field, no nulls, and zero-length values; drift quarantines.","geometry":"Native Point WKB values copied without repair, coordinate transform, or reprojection.","crs":"EPSG:4019 preserved. EPSG definition is unknown datum based on GRS 1980; no NAD83/4326 inference.","release_status":"Unapproved; historical HIFLD archive equivalence, vintage, baseline, rights and mapping decisions remain pending."}}],
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Source-faithful draft retains every attribute, native OID values, Binary Blob, WKB point geometry and producer-declared EPSG:4019; compatibility/release decisions remain pending.","objects":[{"path":f"geoparquet/{DATASET_SLUG}.parquet","bytes":len(parquet),"sha256":digest(parquet)}]}}},
        "unsupported_output_formats":{"shapefile_zip":"Would truncate or alter field names/types and drop the binary attribute."}}
    validate_release_provenance(manifest);validate_output_policy(manifest)
    ready={"approved":False,"version":version,"objects":{"geoparquet":{"bytes":len(parquet),"sha256":digest(parquet)}},
        "source_population_complete":True,"source_record_sha256":record_sha,"source_schema_sha256":schema_sha,
        "source_release_accepted":False,"source_freshness_accepted":False,"baseline_comparison_accepted":False,
        "schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,
        "rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs={f"geoparquet/{DATASET_SLUG}.parquet":parquet,
        "metadata/source_manifest.json":canonical(manifest),"metadata/source_audit.json":canonical(audit),
        "metadata/processing_identity.json":canonical(identity),"metadata/candidate_ready.json":canonical(ready),
        "raw/capture_manifest.json":canonical(cap_manifest),"raw/create_replica.request.body":(root/"create_replica.request.body").read_bytes()}
    receipt_map={}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"]=body
        outputs[f"raw/{name}.wire"]=(root/f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"]=canonical(receipt)
        for redirect in receipt.get("redirects",[]):
            prefix=redirect["artifact_prefix"]
            for suffix in ("body","wire","receipt.json"):
                outputs[f"raw/{prefix}.{suffix}"]=(root/f"{prefix}.{suffix}").read_bytes()
        receipt_map[name]=receipt
    fingerprint=digest(canonical(identity));state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    descriptors={name:{"bytes":len(body),"sha256":digest(body)} for name,body in outputs.items()}
    if storage.object_snapshot(state_key) is not None:
        raw_saved=storage.read_key(f"{prefix}/raw/archive.body")
        if raw_saved!=captures["archive"][0]:
            raise LocksQuarantineError("same_source_version_archive_bytes_changed")
        original=__import__("json").loads(load_first_construction(storage,state_key,version,fingerprint))
        original_receipts=original["payload"].get("capture_receipts")
        if not isinstance(original_receipts,dict):raise LocksQuarantineError("cached_candidate_receipts_missing")
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,
            identity,record_sha,schema_sha,original["payload"]["objects"],original_receipts)
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,
            "source_population_complete":True,"source_id_sha256":native_audit["source_id_sha256"],
            "source_schema_sha256":schema_sha,"candidate_ready":prefix+"/metadata/candidate_ready.json",
            "source_capture_evidence_sha256":capture_sha,"reused":True,"capture_directory":str(root),
            "source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,
            "id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    descriptors=_write_outputs(storage,prefix,outputs)
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,
        "source_record_sha256":record_sha,"source_schema_sha256":schema_sha,"capture_receipts":receipt_map,
        "objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"],identity=identity)
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,
        "source_population_complete":True,"source_id_sha256":native_audit["source_id_sha256"],
        "source_schema_sha256":schema_sha,"candidate_ready":prefix+"/metadata/candidate_ready.json",
        "source_capture_evidence_sha256":capture_sha,"reused":False,"capture_directory":str(root),
        "source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
