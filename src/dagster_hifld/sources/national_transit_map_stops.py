"""Bounded native candidate capture for current BTS NTAD transit stops.

This is a current BTS compilation, not an accepted replacement for the retired
HIFLD/DataLumos snapshot. Source and release approvals remain false.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import stat
import struct
from contextlib import nullcontext
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj

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
from dagster_hifld.http_capture import validate_capture_roster

ITEM_ID = "959b4adc3ff94bc6a46f2c1d515d09aa"
PORTAL_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"
SERVICE_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_National_Transit_Map_Stops/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "National Transit Map Stops"
DATASET_SLUG = "national-transit-map-stops"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Department of Transportation, Bureau of Transportation Statistics (USDOT_BTS)"
DESCRIPTION = (
    "The National Transit Map - Stops dataset was compiled on September 23, 2026 from the Bureau of "
    "Transportation Statistics (BTS) and is part of the U.S. Department of Transportation "
    "(USDOT)/Bureau of Transportation Statistics (BTS) National Transportation Atlas Database (NTAD). "
    "The National Transit Map (NTM) is a nationwide catalog of fixed-guideway and fixed-route transit "
    "service in America. It is compiled using General Transit Feed Specification (GTFS) Schedule data. "
    "The NTM Stops dataset shows stops where vehicles pick up or drop off riders. "
    "This dataset uses the GTFS stops.txt file to locate transit stops and joins information from the "
    "GTFS stop_times.txt, trips.txt, and routes.txt files to indicate the type of transit that services "
    "a stop. The GTFS schedule format and structure documentation is available at, https://gtfs.org/schedule/. "
    "To improve the spatial accuracy of the NTM Stops, the Bureau of Transportation Statistics (BTS) "
    "adjusts transit stops using context from the submitted GTFS source data and/or from other publicly "
    "available information about the transit service. A data dictionary, or other source of attribute "
    "information, is accessible at https://doi.org/10.21949/1529049"
)
EXPECTED_COUNT = 673_251
PAGE_SIZE = 2_000
MAX_CAPTURE_BYTES = 2_000_000_000
MAX_RESPONSE_BYTES = 4_000_000
MAX_ID_ROSTER_RESPONSE_BYTES = 12_000_000
MAX_CAPTURE_MANIFEST_BYTES = 8_000_000
MAX_CAPTURE_RECEIPT_BYTES = 64_000
MAX_CAPTURE_RECEIPT_TOTAL_BYTES = 16_000_000
MAX_CAPTURE_REQUESTS = 512
MIN_FREE_BYTES = 400_000_000
STAGING_OVERHEAD_BYTES = 8_000_000
MIN_CAPTURE_BYTES = 1

# Exact metadata field contract from the official public FeatureServer layer 0.
FIELD_DECLARATIONS = [
    {"name":"OBJECTID","type":"esriFieldTypeOID","alias":"OBJECTID","length":None,"nullable":False,"domain":None},
    {"name":"ntd_id","type":"esriFieldTypeString","alias":"ntd_id","length":254,"nullable":True,"domain":None},
    {"name":"feed_id","type":"esriFieldTypeString","alias":"feed_id","length":254,"nullable":True,"domain":None},
    {"name":"stop_id","type":"esriFieldTypeString","alias":"stop_id","length":254,"nullable":True,"domain":None},
    {"name":"stop_name","type":"esriFieldTypeString","alias":"stop_name","length":254,"nullable":True,"domain":None},
    {"name":"stop_desc","type":"esriFieldTypeString","alias":"stop_desc","length":254,"nullable":True,"domain":None},
    {"name":"stop_lat","type":"esriFieldTypeDouble","alias":"stop_lat","length":None,"nullable":True,"domain":None},
    {"name":"stop_lon","type":"esriFieldTypeDouble","alias":"stop_lon","length":None,"nullable":True,"domain":None},
    {"name":"zone_id","type":"esriFieldTypeString","alias":"zone_id","length":254,"nullable":True,"domain":None},
    {"name":"stop_url","type":"esriFieldTypeString","alias":"stop_url","length":254,"nullable":True,"domain":None},
    {"name":"stop_code","type":"esriFieldTypeString","alias":"stop_code","length":254,"nullable":True,"domain":None},
    {"name":"location_type","type":"esriFieldTypeString","alias":"location_type","length":254,"nullable":True,"domain":None},
    {"name":"parent_station","type":"esriFieldTypeString","alias":"parent_station","length":254,"nullable":True,"domain":None},
    {"name":"stop_timezone","type":"esriFieldTypeString","alias":"stop_timezone","length":254,"nullable":True,"domain":None},
    {"name":"wheelchair_boarding","type":"esriFieldTypeString","alias":"wheelchair_boarding","length":254,"nullable":True,"domain":None},
    {"name":"level_id","type":"esriFieldTypeString","alias":"level_id","length":254,"nullable":True,"domain":None},
    {"name":"platform_code","type":"esriFieldTypeString","alias":"platform_code","length":254,"nullable":True,"domain":None},
    {"name":"stop_type","type":"esriFieldTypeString","alias":"stop_type","length":254,"nullable":True,"domain":None},
    {"name":"stop_type_text","type":"esriFieldTypeString","alias":"stop_type_text","length":254,"nullable":True,"domain":None},
    {"name":"download_date","type":"esriFieldTypeString","alias":"download_date","length":254,"nullable":True,"domain":None},
]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _metadata_url(name: str) -> str:
    params = {
        "portal_item": (PORTAL_ITEM_URL, None),
        "service_before": (SERVICE_URL + "?f=pjson", None),
        "layer_before": (LAYER_URL + "?f=pjson", None),
        "count_before": (LAYER_URL + "/query", {"where":"1=1","returnCountOnly":"true","f":"json"}),
        "ids_before": (LAYER_URL + "/query", {"where":"1=1","returnIdsOnly":"true","f":"json"}),
        "layer_after": (LAYER_URL + "?f=pjson", None),
        "service_after": (SERVICE_URL + "?f=pjson", None),
        "count_after": (LAYER_URL + "/query", {"where":"1=1","returnCountOnly":"true","f":"json"}),
        "ids_after": (LAYER_URL + "/query", {"where":"1=1","returnIdsOnly":"true","f":"json"}),
    }
    url, query = params[name]
    return url + ("?" + urlencode(query) if query else "")


def _page_url(offset: int, limit: int, has_z: bool, has_m: bool) -> str:
    return LAYER_URL + "/query?" + urlencode({
        "where":"1=1", "outFields":"*", "returnGeometry":"true",
        "returnZ":str(has_z).lower(), "returnM":str(has_m).lower(), "outSR":"4326",
        "orderByFields":"OBJECTID ASC", "resultOffset":offset,
        "resultRecordCount":limit, "f":"json",
    })


def _bounded_read(path: Path, limit: int, *, expected_bytes: int | None = None) -> bytes:
    try:
        before = path.lstat()
    except OSError as exc:
        raise ValueError("capture_artifact_unavailable") from exc
    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or path.is_symlink()
            or type(limit) is not int or limit < 0 or before.st_size > limit
            or (expected_bytes is not None and before.st_size != expected_bytes)):
        raise ValueError("capture_artifact_size_or_type_invalid")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        raise ValueError("capture_artifact_type_invalid") from exc
    try:
        opened = os.fstat(fd)
        if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                or (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
                != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)):
            raise ValueError("capture_artifact_changed_before_read")
        with os.fdopen(os.dup(fd), "rb") as stream:
            data = stream.read(limit + 1)
        after = os.fstat(fd)
        if (len(data) > limit or len(data) != opened.st_size
                or (after.st_size, after.st_mtime_ns, after.st_nlink)
                != (opened.st_size, opened.st_mtime_ns, 1)):
            raise ValueError("capture_artifact_changed_during_read")
        return data
    finally:
        os.close(fd)


def _load_retained_capture(root: Path) -> tuple[dict, dict[str, tuple[bytes, dict]], str]:
    if not root.is_dir() or root.is_symlink():
        raise ValueError("retained_capture_directory_invalid")
    encoded = _bounded_read(root / "capture_manifest.json", MAX_CAPTURE_MANIFEST_BYTES)
    manifest = json.loads(encoded)
    if (not isinstance(manifest, dict) or manifest.get("schema_version") != 1
            or manifest.get("capture_complete") is not True or not isinstance(manifest.get("captures"), dict)
            or type(manifest.get("capture_budget_bytes")) is not int
            or type(manifest.get("retained_bytes")) is not int
            or manifest.get("response_budget_bytes") != MAX_RESPONSE_BYTES
            or manifest.get("id_roster_response_budget_bytes") != MAX_ID_ROSTER_RESPONSE_BYTES
            or not 0 < manifest["retained_bytes"] <= manifest["capture_budget_bytes"] <= MAX_CAPTURE_BYTES
            or not 0 < len(manifest["captures"]) <= MAX_CAPTURE_REQUESTS):
        raise ValueError("retained_capture_manifest_invalid")
    expected = {"capture_manifest.json"}
    total = receipt_total = 0
    for name, desc in manifest["captures"].items():
        if (not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", name)
                or not isinstance(desc, dict) or set(desc) != {"body", "wire", "receipt", "request_url"}):
            raise ValueError("retained_capture_manifest_invalid")
        response_cap = MAX_ID_ROSTER_RESPONSE_BYTES if name in {"ids_before", "ids_after"} else MAX_RESPONSE_BYTES
        if desc.get("response_budget_bytes") != response_cap:
            raise ValueError("retained_capture_response_budget_invalid:" + name)
        for kind, suffix, cap in (("body", ".body", response_cap),
                                  ("wire", ".wire", response_cap),
                                  ("receipt", ".receipt.json", MAX_CAPTURE_RECEIPT_BYTES)):
            item = desc[kind]
            if (not isinstance(item, dict) or set(item) != {"file", "bytes", "sha256"}
                    or item.get("file") != name + suffix or type(item.get("bytes")) is not int
                    or item["bytes"] < 0 or item["bytes"] > cap
                    or not isinstance(item.get("sha256"), str)
                    or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"])):
                raise ValueError("retained_capture_manifest_invalid")
            expected.add(item["file"])
        total += desc["body"]["bytes"] + desc["wire"]["bytes"]
        receipt_total += desc["receipt"]["bytes"]
        if desc["body"]["bytes"] + desc["wire"]["bytes"] > response_cap:
            raise ValueError("retained_capture_response_budget_exceeded")
    if total != manifest["retained_bytes"] or total > manifest["capture_budget_bytes"] or receipt_total > MAX_CAPTURE_RECEIPT_TOTAL_BYTES:
        raise ValueError("retained_capture_budget_invalid")
    validate_capture_roster(root, expected)
    captures = {}
    for name, desc in manifest["captures"].items():
        response_cap = MAX_ID_ROSTER_RESPONSE_BYTES if name in {"ids_before", "ids_after"} else MAX_RESPONSE_BYTES
        contents = {}
        for kind in ("body", "wire", "receipt"):
            item = desc[kind]
            data = _bounded_read(root / item["file"], MAX_CAPTURE_RECEIPT_BYTES if kind == "receipt" else response_cap,
                                 expected_bytes=item["bytes"])
            if _sha(data) != item["sha256"]:
                raise ValueError("retained_capture_hash_mismatch:" + name + ":" + kind)
            contents[kind] = data
        receipt = json.loads(contents["receipt"])
        body, wire = contents["body"], contents["wire"]
        if (receipt.get("method") != "GET" or receipt.get("url") != desc["request_url"]
                or receipt.get("final_url") != desc["request_url"] or receipt.get("status") != 200
                or receipt.get("complete_body") is not True or receipt.get("bytes") != len(body)
                or receipt.get("sha256") != _sha(body) or receipt.get("wire_bytes") != len(wire)
                or receipt.get("wire_sha256") != _sha(wire) or receipt.get("error_code") is not None):
            raise ValueError("retained_capture_receipt_invalid:" + name)
        captures[name] = (body, receipt)
    return manifest, captures, _sha(encoded)


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID":pa.int64(), "esriFieldTypeString":pa.string(),
            "esriFieldTypeDouble":pa.float64()}[field["type"]]


def _point_wkb(geometry: dict | None, has_z: bool, has_m: bool) -> bytes | None:
    if geometry is None:
        return None
    coords = [geometry["x"], geometry["y"]]
    if has_z:
        coords.append(geometry["z"])
    if has_m:
        coords.append(geometry["m"])
    type_code = 1 + (1000 if has_z else 0) + (2000 if has_m else 0)
    return struct.pack("<BI" + "d" * len(coords), 1, type_code, *coords)


def _native_parquet(fields: list[dict], features: list[dict], has_z: bool, has_m: bool) -> bytes:
    arrays = {f["name"]: pa.array([row["attributes"][f["name"]] for row in features], type=_arrow_type(f))
              for f in fields}
    arrays["geometry"] = pa.array([_point_wkb(row["geometry"], has_z, has_m) for row in features], type=pa.binary())
    schema_fields = [pa.field(f["name"], _arrow_type(f), nullable=bool(f.get("nullable", True))) for f in fields]
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    dims = " ZM" if has_z and has_m else " Z" if has_z else " M" if has_m else ""
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{
        "encoding":"WKB", "geometry_types":["Point" + dims],
        "crs":pyproj.CRS.from_epsg(4326).to_json_dict(), "edges":"planar"}}}
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(arrays, schema=pa.schema(schema_fields, metadata={b"geo":_canonical(geo)})),
                   sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _field_contract(layer: dict) -> list[dict]:
    fields = layer.get("fields")
    if not isinstance(fields, list):
        raise ValueError("source_field_roster_missing")
    plain = [{key:f.get(key) for key in ("name","type","alias","length","nullable","domain")} for f in fields]
    if plain != FIELD_DECLARATIONS:
        raise ValueError("source_schema_drift")
    if layer.get("objectIdField") != "OBJECTID" or layer.get("geometryType") != "esriGeometryPoint":
        raise ValueError("source_layer_identity_drift")
    return fields


def capture_national_transit_map_stops(storage: StagingStorageResource, *, version: str,
        capture_directory: str | None = None, retained_capture_directory: str | None = None,
        retained_capture_manifest_sha256: str | None = None,
        max_capture_bytes: int = MAX_CAPTURE_BYTES) -> dict:
    """Capture and stage the complete observed BTS layer or refuse on any drift/budget limit."""
    if type(max_capture_bytes) is not int or not MIN_CAPTURE_BYTES <= max_capture_bytes <= MAX_CAPTURE_BYTES:
        raise ValueError("capture_budget_invalid")
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    base.mkdir(parents=True, exist_ok=True)
    if base.is_symlink() or not base.is_dir():
        raise ValueError("unsafe_staging_root")
    if retained_capture_directory and capture_directory:
        raise ValueError("capture_directory_conflict")
    root = (Path(retained_capture_directory).absolute() if retained_capture_directory else
            Path(capture_directory or "/workspace/work/national-transit-map-stops/captures").absolute() / version)
    if retained_capture_directory:
        if not isinstance(retained_capture_manifest_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", retained_capture_manifest_sha256):
            raise ValueError("retained_capture_manifest_digest_required")
        if not root.is_dir() or root.is_symlink():
            raise ValueError("retained_capture_directory_invalid")
    else:
        if retained_capture_manifest_sha256 is not None:
            raise ValueError("retained_capture_digest_without_directory")
        root.mkdir(parents=True, exist_ok=True)
        if root.is_symlink() or any(root.iterdir()):
            raise ValueError("capture_directory_not_empty_or_unsafe")
    if shutil.disk_usage(base).free < MIN_FREE_BYTES or shutil.disk_usage(root).free < MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_capture")

    offline = None
    captures: dict[str, tuple[bytes, dict]] = {}
    retained = 0
    if retained_capture_directory:
        offline, captures, manifest_sha = _load_retained_capture(root)
        if manifest_sha != retained_capture_manifest_sha256:
            raise ValueError("retained_capture_manifest_digest_mismatch")
        if (offline["capture_budget_bytes"] != max_capture_bytes
                or offline["response_budget_bytes"] != MAX_RESPONSE_BYTES):
            raise ValueError("retained_capture_budget_config_mismatch")
        retained = offline["retained_bytes"]

    def get(client, name: str, url: str) -> dict:
        nonlocal retained
        response_cap = MAX_ID_ROSTER_RESPONSE_BYTES if name in {"ids_before", "ids_after"} else MAX_RESPONSE_BYTES
        if offline is not None:
            if name not in captures or captures[name][1].get("url") != url:
                raise ValueError("retained_capture_request_mismatch:" + name)
            body = captures[name][0]
        else:
            response_used = [0]
            def reserve(progress):
                if retained + progress.reserve_bytes > max_capture_bytes:
                    raise ValueError("source_capture_budget_exceeded")
                if shutil.disk_usage(root).free < MIN_FREE_BYTES + progress.reserve_bytes + STAGING_OVERHEAD_BYTES:
                    raise ValueError("capture_disk_reserve_exceeded")
            def before_retain(_kind: str, data: bytes):
                nonlocal retained
                if retained + len(data) > max_capture_bytes or response_used[0] + len(data) > response_cap:
                    raise ValueError("source_capture_budget_exceeded")
                if shutil.disk_usage(root).free < MIN_FREE_BYTES + len(data) + STAGING_OVERHEAD_BYTES:
                    raise ValueError("capture_disk_reserve_exceeded")
                response_used[0] += len(data)
                retained += len(data)
            try:
                body, receipt = capture_get(client, url, root, name, max_bytes=response_cap,
                                            reserve=reserve, before_retain=before_retain)
            except Exception:
                receipt_path = root / f"{name}.receipt.json"
                body_path = root / f"{name}.body"
                if receipt_path.is_file() and body_path.is_file():
                    try:
                        partial_receipt = json.loads(_bounded_read(receipt_path, MAX_CAPTURE_RECEIPT_BYTES))
                        partial_body = _bounded_read(body_path, response_cap)
                        if isinstance(partial_receipt, dict):
                            captures[name] = (partial_body, partial_receipt)
                    except (OSError, ValueError, TypeError, json.JSONDecodeError):
                        pass
                raise
            captures[name] = (body, receipt)
        result = json.loads(body)
        if not isinstance(result, dict) or "error" in result:
            raise ValueError("source_response_error:" + name)
        return result

    try:
        client_context = nullcontext(None) if offline is not None else httpx.Client(
            follow_redirects=False, timeout=90, headers={"User-Agent":"hifld-next-datasets/national-transit-map-stops-draft"})
        with client_context as client:
            portal = get(client,"portal_item",_metadata_url("portal_item"))
            if (portal.get("id") != ITEM_ID or portal.get("owner") != "USDOT_BTS"
                    or portal.get("orgId") != "xOi1kZaI0eWDREZv" or portal.get("title") != LAYER_NAME
                    or portal.get("type") != "Feature Service" or portal.get("url") != SERVICE_URL
                    or portal.get("access") != "public" or portal.get("description") != DESCRIPTION):
                raise ValueError("official_BTS_item_identity_or_description_drift")
            service = get(client,"service_before",_metadata_url("service_before"))
            layer = get(client,"layer_before",_metadata_url("layer_before"))
            if (service.get("serviceItemId") != ITEM_ID or service.get("description") != DESCRIPTION
                    or not any(isinstance(x,dict) and x.get("id")==LAYER_ID and x.get("name")==LAYER_NAME
                               for x in service.get("layers",[]))):
                raise ValueError("BTS_service_identity_drift")
            fields = _field_contract(layer)
            has_z, has_m = layer.get("hasZ"), layer.get("hasM")
            if has_z is not True or has_m is not False:
                raise ValueError("source_geometry_dimensions_unpinned")
            sr = layer.get("spatialReference", layer.get("extent",{}).get("spatialReference",{}))
            if sr.get("latestWkid",sr.get("wkid")) != 4326:
                raise ValueError("source_native_crs_drift")
            count_doc = get(client,"count_before",_metadata_url("count_before"))
            ids_doc = get(client,"ids_before",_metadata_url("ids_before"))
            count, ids = count_doc.get("count"), ids_doc.get("objectIds")
            if (type(count) is not int or count != EXPECTED_COUNT or not isinstance(ids,list)
                    or len(ids) != count or any(type(i) is not int for i in ids)
                    or len(set(ids)) != count or ids_doc.get("objectIdFieldName") != "OBJECTID"):
                raise ValueError("source_initial_count_or_id_roster_invalid")
            seed = sorted(ids)
            if (not isinstance(layer.get("advancedQueryCapabilities"), dict)
                    or layer["advancedQueryCapabilities"].get("supportsPagination") is not True
                    or layer["advancedQueryCapabilities"].get("supportsOrderBy") is not True):
                raise ValueError("source_ordered_pagination_not_supported")
            if layer.get("maxRecordCount") != PAGE_SIZE:
                raise ValueError("source_page_size_contract_drift")
            if 10 + math.ceil(count/PAGE_SIZE) > MAX_CAPTURE_REQUESTS:
                raise ValueError("source_request_count_exceeded")
            features=[]
            for page_no, offset in enumerate(range(0,count,PAGE_SIZE)):
                expected = seed[offset:offset+PAGE_SIZE]
                page = get(client,f"page_{page_no:03d}",_page_url(offset,len(expected),has_z,has_m))
                rows = page.get("features")
                if not isinstance(rows,list) or len(rows)!=len(expected) or page.get("exceededTransferLimit") is True:
                    raise ValueError("source_page_incomplete")
                page_sr=page.get("spatialReference",{})
                if not isinstance(page_sr,dict) or page_sr.get("latestWkid",page_sr.get("wkid"))!=4326:
                    raise ValueError("source_page_crs_drift")
                page_ids=[row.get("attributes",{}).get("OBJECTID") for row in rows if isinstance(row,dict)]
                if sorted(page_ids) != expected or len(set(page_ids)) != len(expected):
                    raise ValueError("source_page_id_slice_mismatch")
                features.extend(rows)
            layer_after=get(client,"layer_after",_metadata_url("layer_after"))
            service_after=get(client,"service_after",_metadata_url("service_after"))
            count_after=get(client,"count_after",_metadata_url("count_after")).get("count")
            ids_after_doc=get(client,"ids_after",_metadata_url("ids_after"))
            ids_after=ids_after_doc.get("objectIds")
        if (layer_after != layer or service_after != service or count_after != count
                or ids_after_doc.get("objectIdFieldName") != "OBJECTID" or not isinstance(ids_after,list)
                or sorted(ids_after) != seed or len(ids_after) != count or len(set(ids_after)) != count):
            raise ValueError("source_changed_during_capture")
        names=[field["name"] for field in fields]
        got=[]
        geometry_repr=[]
        null_geometry_count=0
        invalid_geometry_count=0
        for row in features:
            attrs, geom=row.get("attributes"),row.get("geometry")
            if not isinstance(attrs,dict) or list(attrs) != names:
                raise ValueError("source_record_schema_drift")
            if geom is None:
                null_geometry_count += 1
            else:
                expected_geom_keys={"x","y"}|({"z"} if has_z else set())|({"m"} if has_m else set())
                if not isinstance(geom,dict) or set(geom)!=expected_geom_keys:
                    raise ValueError("source_geometry_dimensions_drift")
                if any(type(geom[k]) not in (int,float) or not math.isfinite(geom[k]) for k in expected_geom_keys):
                    raise ValueError("source_coordinate_invalid")
                if not -180 <= geom["x"] <= 180 or not -90 <= geom["y"] <= 90:
                    invalid_geometry_count += 1
            got.append(attrs.get("OBJECTID"))
            geometry_repr.append([attrs.get("OBJECTID"),geom])
        if (len(features)!=count or sorted(got)!=seed or len(set(got))!=count):
            raise ValueError("source_population_incomplete")
        features.sort(key=lambda x:x["attributes"]["OBJECTID"])
        if offline is not None and set(captures) != set(offline["captures"]):
            raise ValueError("retained_capture_request_roster_mismatch")
    except Exception as exc:
        if captures and offline is None:
            quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise

    if offline is None:
        descriptors={}
        receipts_total=0
        for name,(body,receipt) in sorted(captures.items()):
            response_cap = MAX_ID_ROSTER_RESPONSE_BYTES if name in {"ids_before", "ids_after"} else MAX_RESPONSE_BYTES
            body_file=_bounded_read(root/f"{name}.body",response_cap)
            wire_file=_bounded_read(root/f"{name}.wire",response_cap)
            receipt_file=_bounded_read(root/f"{name}.receipt.json",MAX_CAPTURE_RECEIPT_BYTES)
            receipts_total+=len(receipt_file)
            descriptors[name]={"request_url":receipt["url"],"response_budget_bytes":response_cap,
                "body":{"file":f"{name}.body","bytes":len(body_file),"sha256":_sha(body_file)},
                "wire":{"file":f"{name}.wire","bytes":len(wire_file),"sha256":_sha(wire_file)},
                "receipt":{"file":f"{name}.receipt.json","bytes":len(receipt_file),"sha256":_sha(receipt_file)}}
        if receipts_total>MAX_CAPTURE_RECEIPT_TOTAL_BYTES:
            raise ValueError("capture_receipt_total_exceeded")
        capture_manifest={"schema_version":1,"capture_complete":True,"capture_budget_bytes":max_capture_bytes,
            "response_budget_bytes":MAX_RESPONSE_BYTES,
            "id_roster_response_budget_bytes":MAX_ID_ROSTER_RESPONSE_BYTES,
            "retained_bytes":retained,"captures":descriptors}
        capture_manifest_bytes=_canonical(capture_manifest)+b"\n"
        if len(capture_manifest_bytes)>MAX_CAPTURE_MANIFEST_BYTES:
            raise ValueError("capture_manifest_exceeded")
        (root/"capture_manifest.json").write_bytes(capture_manifest_bytes)
        capture_manifest_sha=_sha(capture_manifest_bytes)
    else:
        capture_manifest, captures, capture_manifest_sha=offline
    record_sha=_sha(_canonical(features))
    geometry_sha=_sha(_canonical(geometry_repr))
    schema_sha=_sha(_canonical(FIELD_DECLARATIONS))
    identity=processing_identity(Path(__file__).parents[3])
    native=_native_parquet(fields,features,has_z,has_m)
    version_id=content_derived_version(record_sha,geometry_sha,schema_sha,identity)
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version_id)
    native_name=f"geoparquet/{DATASET_SLUG}.parquet"
    source_modified=next((r.get("lastEditDate") for r in [layer] if isinstance(r.get("lastEditDate"),int)),None)
    audit={"feature_count":count,"source_id_sha256":_sha(_canonical(seed)),"source_record_sha256":record_sha,
        "source_geometry_sha256":geometry_sha,"source_schema_sha256":schema_sha,"source_crs_epsg":4326,
        "geometry_type":"Point","has_z":has_z,"has_m":has_m,"null_geometry_count":null_geometry_count,
        "invalid_geometry_count":invalid_geometry_count,"all_ids_captured_once":True,"ids_equal_before_after":True,
        "producer_stated_compilation_date":"2026-09-23","portal_item_modified_epoch_ms":portal["modified"],
        "portal_item_modified_is_release_date":False,"layer_last_edit_epoch_ms":source_modified,
        "capture_manifest_sha256":capture_manifest_sha,"capture_retained_wire_plus_decoded_bytes":capture_manifest["retained_bytes"],
        "capture_budget_bytes":max_capture_bytes,"archive_baseline_accepted":False}
    manifest={"schema_version":1,"approved":False,"title":LAYER_NAME,"publisher":PUBLISHER,
        "agency":"Bureau of Transportation Statistics","source_url":LAYER_URL,
        "source_release_date":"2026-09-23","source_release_date_type":"producer-stated compilation date; independent release record not found",
        "source_modified":None,"portal_item_modified_epoch_ms":portal.get("modified"),
        "portal_item_modified_is_source_release_date":False,"data_vintage_unknown":True,
        "processor":"HIFLD Next","code_commit":identity["code_commit"],"source_item_id":ITEM_ID,
        "archive_baseline_accepted":False,"historical_equivalence_accepted":False,"source_release_accepted":False,
        "freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "rights_accepted":False,"mapping_approved":False,"release_accepted":False,
        "source_scope":"Current public BTS NTAD National Transit Map Stops layer 0 only; no historical HIFLD equivalence asserted.",
        "source_lineage":"BTS describes compiling stops from GTFS Schedule stops.txt, joining stop_times.txt/trips.txt/routes.txt, and adjusting spatial accuracy using submitted GTFS and/or other public information. DataLumos project 239958 V1 is an archive candidate only and was not used as ingest input.",
        "license_observed":"Creative Commons Attribution 3.0 United States (CC-BY-3.0); rights acceptance remains pending.",
        "inputs":[{"input_id":n,"producer":PUBLISHER,"host":urlsplit(r["url"]).hostname,
            "role":"producer_publication","canonical_url":PORTAL_ITEM_URL.split("?",1)[0],
            "retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],
            "license":"Creative Commons Attribution 3.0 United States (CC-BY-3.0), as displayed in the public ArcGIS item metadata; acceptance pending.",
            "attribution":"U.S. Department of Transportation, Bureau of Transportation Statistics (USDOT_BTS), as identified by the public ArcGIS item; attribution acceptance pending.",
            "http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],
            "wire_bytes":r["wire_bytes"],"wire_sha256":r["wire_sha256"],"release_date":None,
            "release_date_unknown_reason":"The ArcGIS item modified time is not an agency release record."}
            for n,(_body,r) in sorted(captures.items())],
        "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{
            "feature_layer_id":LAYER_ID,"source_fields":"all 20 declared source attributes including OBJECTID retained in source order",
            "geometry":"native point XY/Z/M ordinates encoded as WKB in EPSG:4326 without reprojection or repair",
            "mapping_approved":False,"release_accepted":False}}],
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native source fields, IDs, and point ordinates retained; approval pending.","objects":[{"path":native_name,"bytes":len(native),"sha256":_sha(native)}]}}},
        "unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No approved display transformation."}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    outputs={native_name:native,"metadata/source_manifest.json":_canonical(manifest)+b"\n",
        "metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n",
        "metadata/capture_manifest.json":_canonical(capture_manifest)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"]=body
        response_cap = MAX_ID_ROSTER_RESPONSE_BYTES if name in {"ids_before", "ids_after"} else MAX_RESPONSE_BYTES
        outputs[f"raw/{name}.wire"]=_bounded_read(root/f"{name}.wire",response_cap)
        outputs[f"raw/{name}.receipt.json"]=_canonical(receipt)+b"\n"
    ready={"approved":False,"version":version_id,"objects":{native_name:{"bytes":len(native),"sha256":_sha(native)},
        "metadata/capture_manifest.json":{"bytes":len(outputs["metadata/capture_manifest.json"]),"sha256":_sha(outputs["metadata/capture_manifest.json"])},
        **{k:{"bytes":len(v),"sha256":_sha(v)} for k,v in outputs.items() if k.startswith("raw/")}},
        "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"]=_canonical(ready)+b"\n"
    state_key=f"_construction-state/{DATASET_SLUG}/{version_id}.json"; fingerprint=_sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle=json.loads(load_first_construction(storage,state_key,version_id,fingerprint)); payload=bundle.get("payload")
        if not isinstance(payload,dict): raise ValueError("cached_candidate_payload_missing")
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version_id,fingerprint,identity,
            record_sha,schema_sha,payload.get("objects",{}),payload.get("capture_receipts",{}))
        if set(payload["objects"])!=set(outputs): raise ValueError("cached_candidate_roster_changed")
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version_id,"feature_count":count,
            "source_null_geometry_count":null_geometry_count,"source_invalid_geometry_count":invalid_geometry_count,
            "candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,
            "source_release_accepted":False,"release_accepted":False,"capture_directory":str(root)}
    if shutil.disk_usage(base).free < MIN_FREE_BYTES + sum(map(len,outputs.values())) + STAGING_OVERHEAD_BYTES:
        raise ValueError("candidate_staging_disk_reserve_exceeded")
    for name,body in outputs.items():
        key=f"{prefix}/{name}"; write_immutable(storage,key,body); snapshot=storage.object_snapshot(key)
        if snapshot is None or snapshot.size!=len(body) or storage.sha256_key(key)!=_sha(body) or storage.object_snapshot(key)!=snapshot:
            raise ValueError("candidate_output_readback_failed:"+name)
    descriptors={name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    seal_ready_candidate(storage,state_key,version_id,fingerprint,{"processing_identity":identity,
        "source_record_sha256":record_sha,"source_schema_sha256":schema_sha,
        "capture_receipts":{name:receipt for name,(_body,receipt) in captures.items()},"objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version_id,"feature_count":count,
        "source_null_geometry_count":null_geometry_count,"source_invalid_geometry_count":invalid_geometry_count,
        "candidate_ready":prefix+"/metadata/candidate_ready.json","reused":False,"capture_directory":str(root),
        "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
