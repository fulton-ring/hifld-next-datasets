"""Bounded native capture for USACE IENC crane locations.

This creates an unapproved source-faithful candidate from the official IENC
feature class. Each ArcGIS response is retained verbatim with its wire receipt.
"""
from __future__ import annotations

import json
import hashlib
import math
import shutil
import struct
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    _canonical, _sha, candidate_prefix, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
)
from dagster_hifld.sources.usace_pylon_areas import (
    _load_replay, _write_outputs,
    PAGE_SIZE,
)
from dagster_hifld.sources.usace_pylon_locations import _validate_crs, _validate_dimensions

DATASET_SLUG = "crane-locations-usace-ienc"
FILE_SLUG = DATASET_SLUG
LAYER_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/CRANES_POINT/MapServer/8"
SERVICE_URL = LAYER_URL.rsplit("/", 1)[0]
SERVICE_DIRECTORY_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson"
BTS_ITEM_ID = "7ec34864c8804468a9cf52d437fe4b6c"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; official IENC Feature Classes service"
EXPECTED_COUNT = 148
EXPECTED_IDS = list(range(1, EXPECTED_COUNT + 1))
MAX_RESPONSE_BYTES = 8_000_000
MAX_CAPTURE_BYTES = 16_000_000
EXPECTED_LAYER_EXTENT = {
    "xmin": -91.74366579999997, "ymin": 30.528435600000023,
    "xmax": -79.51109279999997, "ymax": 42.72636520000003,
}
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID"), ("SHAPE", "esriFieldTypeGeometry"),
    ("Category_of_Crane", "esriFieldTypeString"), ("Object_Name", "esriFieldTypeString"),
    ("Vertical_Clearance", "esriFieldTypeString"), ("Vertical_Datum", "esriFieldTypeString"),
    ("Information", "esriFieldTypeString"), ("Condition", "esriFieldTypeString"),
    ("Scale_Minimum", "esriFieldTypeInteger"), ("Source_Date", "esriFieldTypeString"),
    ("Source_Indication", "esriFieldTypeString"), ("Source_Dataset", "esriFieldTypeString"),
    ("Feature_ID", "esriFieldTypeString"),
]
EXPECTED_FIELD_DECLARATIONS = [
    {"name":name,"type":kind,"alias":name,"domain":None,
     **({"length":254} if kind == "esriFieldTypeString" else {})}
    for name,kind in EXPECTED_FIELDS
]
UNSUPPORTED_OUTPUT_FORMATS = {
    "shapefile_zip": "Shapefile field-name and value coercion can lose native schema; no export mapping is approved.",
    "geopackage": "No pinned complete native GDB inventory or schema/compatibility approval.",
    "pmtiles": "No display transformation approved.",
}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_started_at(captures: dict[str, tuple[bytes, dict]]) -> str:
    """Use the first request receipt as capture time, including on replay."""
    receipt = captures.get("ids_seed", (b"", {}))[1]
    observed_at = receipt.get("observed_at")
    if not isinstance(observed_at, str) or not observed_at:
        raise ValueError("source_capture_start_receipt_missing")
    return observed_at


def _fetch(client: httpx.Client, url: str, root: Path, name: str, budget: list[int]):
    root.mkdir(parents=True, exist_ok=True)
    body_path, wire_path = root / f"{name}.body", root / f"{name}.wire"
    for path in (body_path, wire_path):
        path.unlink(missing_ok=True)
    hashes = {"body": hashlib.sha256(), "wire": hashlib.sha256()}
    sizes = {"body": 0, "wire": 0}
    budget_start = budget[0]
    observed = datetime.now(timezone.utc).isoformat()

    def retain(path, kind):
        def write(chunk):
            with path.open("ab") as stream:
                stream.write(chunk)
            hashes[kind].update(chunk)
            sizes[kind] += len(chunk)
            budget[0] = budget_start + sizes["wire"] + sizes["body"]
        return write

    def reserve(progress):
        if budget_start + progress.wire_bytes_retained + progress.decoded_bytes_retained + progress.reserve_bytes > MAX_CAPTURE_BYTES:
            raise ValueError("source_capture_budget_exceeded")

    response = None
    captured = None
    transport_error = None
    final_url = url
    try:
        with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
            final_url = str(response.url)
            captured = capture_http_response(
                response, max_wire_bytes=MAX_RESPONSE_BYTES, max_decoded_bytes=MAX_RESPONSE_BYTES,
                reserve=reserve, retain_wire=retain(wire_path, "wire"),
                retain_decoded=retain(body_path, "body"),
            )
    except Exception as exc:
        transport_error = type(exc).__name__
    receipt = {
        "method": "GET", "url": url, "final_url": final_url, "observed_at": observed,
        "status": captured.status_code if captured is not None else (response.status_code if response is not None else None),
        "complete_body": bool(captured is not None and captured.complete and transport_error is None),
        "wire_bytes": sizes["wire"], "wire_sha256": hashes["wire"].hexdigest(),
        "bytes": sizes["body"], "sha256": hashes["body"].hexdigest(),
        "content_encoding": captured.content_encoding if captured is not None else None,
        "content_length": captured.content_length if captured is not None else None,
        "error_code": captured.error_code if captured is not None else ("transport_error" if transport_error else "capture_result_missing"),
        "error_type": captured.error_type if captured is not None else transport_error,
    }
    (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    if transport_error:
        raise httpx.TransportError("capture_transport_error:" + transport_error)
    if captured is None:
        raise ValueError("capture_result_missing")
    body, wire = body_path.read_bytes(), wire_path.read_bytes()
    if (not captured.complete or captured.status_code != 200 or final_url != url
            or len(body) != receipt["bytes"] or _sha(body) != receipt["sha256"]
            or len(wire) != receipt["wire_bytes"] or _sha(wire) != receipt["wire_sha256"]):
        raise ValueError("incomplete_source_capture:" + name)
    return body, receipt


def _fetch_retry(client: httpx.Client, url: str, root: Path, name: str, budget: list[int],
                 *, schedule: tuple[float, ...] = (0.0, 2.0, 8.0)):
    last = None
    for attempt, wait in enumerate(schedule):
        if wait:
            time.sleep(wait)
        attempt_name = f"{name}.attempt{attempt + 1}"
        try:
            body, receipt = _fetch(client, url, root, attempt_name, budget)
            for suffix in ("body", "wire", "receipt.json"):
                (root / f"{attempt_name}.{suffix}").replace(root / f"{name}.{suffix}")
            return body, receipt
        except (httpx.TransportError, ValueError) as exc:
            last = exc
            if isinstance(exc, ValueError) and not str(exc).startswith("incomplete_source_capture:"):
                raise
    raise last


def _capture_roster(ids: list[int]) -> list[tuple[str, str]]:
    urls = [("bts_item", BTS_ITEM_URL), ("service_directory", SERVICE_DIRECTORY_URL),
            ("service_before", SERVICE_URL + "?f=pjson"),
            ("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", _query({"where":"1=1","returnCountOnly":"true","f":"json"})),
            ("ids_before", _query({"where":"1=1","returnIdsOnly":"true","f":"json"}))]
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        page_ids = ids[start:start + PAGE_SIZE]
        urls.append((f"page_{index:03d}", _query({"objectIds":",".join(map(str,page_ids)),
            "outFields":"*", "returnGeometry":"true", "returnZ":"true", "returnM":"true",
            "outSR":"4326", "orderByFields":"OBJECTID ASC", "f":"json"})))
    urls.extend([("layer_after", LAYER_URL + "?f=pjson"),
                 ("count_after", _query({"where":"1=1","returnCountOnly":"true","f":"json"})),
                 ("ids_after", _query({"where":"1=1","returnIdsOnly":"true","f":"json"})),
                 ("service_after", SERVICE_URL + "?f=pjson")])
    return urls


def _validate(captures: dict[str, tuple[bytes, dict]]):
    docs = {name: json.loads(body) for name, (body, _r) in captures.items()}
    layer = docs["layer_before"]
    fields = layer.get("fields")
    projected=[{key:f[key] for key in ("name","type","alias","length","domain","nullable") if key in f}
               for f in fields] if isinstance(fields,list) else None
    if projected != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    directory_services = docs["service_directory"].get("services")
    if not isinstance(directory_services, list) or not any(
        entry.get("name") == "IENC_Feature_Classes/CRANES_POINT" and entry.get("type") == "MapServer"
        for entry in directory_services if isinstance(entry, dict)
    ):
        raise ValueError("source_service_missing_from_directory")
    bts = docs["bts_item"]
    if bts.get("id") != BTS_ITEM_ID or bts.get("owner") != "USDOT_BTS" or "bi-monthly" not in bts.get("description", "").lower():
        raise ValueError("source_distribution_attestation_drift")
    service_before, service_after = docs["service_before"], docs["service_after"]
    if (service_before.get("mapName") != "CRANES_POINT"
            or service_after.get("mapName") != "CRANES_POINT"
            or service_before.get("layers") != service_after.get("layers")
            or not any(entry.get("id") == 8 and entry.get("name") == "CRANES_POINT"
                       for entry in service_before.get("layers", []) if isinstance(entry, dict))):
        raise ValueError("source_service_identity_drift")
    if layer.get("name") != "CRANES_POINT" or layer.get("id") != 8 or layer.get("geometryType") != "esriGeometryPoint":
        raise ValueError("source_layer_identity_drift")
    _validate_crs(layer.get("spatialReference"))
    _validate_crs(layer.get("extent", {}).get("spatialReference"))
    _validate_dimensions(layer)
    extent = layer.get("extent")
    if not isinstance(extent, dict) or any(extent.get(k) != v for k, v in EXPECTED_LAYER_EXTENT.items()):
        raise ValueError("source_extent_drift")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_spatial_reference_or_page_limit_drift")
    if (docs["layer_after"].get("fields") != fields
            or docs["layer_after"].get("geometryType") != "esriGeometryPoint"
            or docs["layer_after"].get("extent") != layer.get("extent")
            or docs["layer_after"].get("spatialReference") != layer.get("spatialReference")):
        raise ValueError("source_schema_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    seed = docs["ids_seed"].get("objectIds")
    before, after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (not isinstance(seed,list) or not isinstance(before,list) or not isinstance(after,list)
            or any(type(value) is not int for value in [*seed, *before, *after])
            or sorted(seed) != EXPECTED_IDS or sorted(before) != EXPECTED_IDS
            or sorted(after) != EXPECTED_IDS):
        raise ValueError("source_id_population_drift")
    if (docs["ids_before"].get("objectIdFieldName") != "OBJECTID"
            or any(type(value) is not int for value in before)):
        raise ValueError("source_object_id_field_or_type_drift")
    if (docs["count_before"].get("count") != len(before)
            or docs["count_after"].get("count") != len(after)):
        raise ValueError("source_count_id_roster_mismatch")
    features=[]
    pages=sorted(n for n in captures if n.startswith("page_"))
    if len(pages) != (EXPECTED_COUNT + PAGE_SIZE - 1)//PAGE_SIZE:
        raise ValueError("source_page_roster_incomplete")
    for name in pages:
        page=docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncation")
        _validate_crs(page.get("spatialReference"))
        _validate_dimensions(page)
        rows=page.get("features")
        if not isinstance(rows,list): raise ValueError("source_page_features_missing")
        page_schema = [(field.get("name"), field.get("type")) for field in page.get("fields", [])]
        expected_page_schema = [field for field in EXPECTED_FIELDS if field[0] != "SHAPE"]
        if page_schema != expected_page_schema or page.get("geometryType") != "esriGeometryPoint":
            raise ValueError("source_page_schema_or_geometry_type_drift")
        page_index = int(name.removeprefix("page_"))
        expected_ids = EXPECTED_IDS[page_index * PAGE_SIZE:(page_index + 1) * PAGE_SIZE]
        returned_ids = [feature.get("attributes", {}).get("OBJECTID") for feature in rows]
        if returned_ids != expected_ids:
            raise ValueError("source_page_id_order_or_membership_drift")
        features.extend(rows)
    expected_attrs=[f["name"] for f in fields if f["name"]!="SHAPE"]
    got=[]
    null_geometry_count = 0
    empty_geometry_count = 0
    invalid_geometry_count = 0
    for feature in features:
        attrs, geom=feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs,dict) or list(attrs)!=expected_attrs or not isinstance(geom,dict):
            raise ValueError("source_record_schema_or_geometry_drift")
        if set(geom)-{"x","y","spatialReference"}:
            raise ValueError("unexpected_source_geometry_ordinates")
        if "spatialReference" in geom:
            _validate_crs(geom["spatialReference"])
        x, y = geom.get("x"), geom.get("y")
        if (type(x) not in (int, float) or type(y) not in (int, float)
                or not math.isfinite(x) or not math.isfinite(y)
                or not (-180 <= x <= 180 and -90 <= y <= 90)):
            raise ValueError("source_point_coordinate_drift")
        parsed_geometry = shapely.Point(float(x), float(y))
        null_geometry_count += int(parsed_geometry is None)
        empty_geometry_count += int(parsed_geometry.is_empty)
        invalid_geometry_count += int(not parsed_geometry.is_valid)
        got.append(attrs.get("OBJECTID"))
    if len(features)!=EXPECTED_COUNT or sorted(got)!=sorted(before) or len(set(got))!=EXPECTED_COUNT:
        raise ValueError("source_feature_population_incomplete")
    features.sort(key=lambda f:f["attributes"]["OBJECTID"])
    rows=[{"attributes":f["attributes"],"geometry":f["geometry"]} for f in features]
    record_sha=_sha(_canonical([f["attributes"] for f in features]))
    geometry_sha=_sha(_canonical([[f["attributes"]["OBJECTID"],f["geometry"]] for f in features]))
    schema_sha=_sha(_canonical(fields))
    return fields, features, {"source_record_sha256":record_sha,"source_geometry_sha256":geometry_sha,"source_schema_sha256":schema_sha,
        "source_id_sha256":_sha(_canonical(sorted(got))),
        "source_feature_count":len(features),"source_id_min":min(got),"source_id_max":max(got),
        "source_null_geometry_count":null_geometry_count,
        "source_empty_geometry_count":empty_geometry_count,
        "source_invalid_geometry_count":invalid_geometry_count,
        "source_geometry_type":"esriGeometryPoint","source_wkid":4326,
        "source_has_z":bool(layer.get("hasZ")),"source_has_m":bool(layer.get("hasM")),
        "source_coverage_scope":"Complete official IENC CRANES_POINT feature class; not a claim of all crane locations.",
        "drift_guard":"Exact layer id/name, count, field names/types, CRS, page errors, and before/after ID membership are checked."}


def _arrow_type(field: dict):
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeSingle": pa.float32(),
            "esriFieldTypeDouble": pa.float64(), "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64()}.get(field.get("type"), pa.string())


def _parquet_bytes(fields: list[dict], features: list[dict], wkid: int) -> bytes:
    properties = [field for field in fields if field["name"] != "SHAPE"]
    arrays = []
    for field in properties:
        name, kind = field["name"], field["type"]
        values = [feature["attributes"].get(name) for feature in features]
        if name == "OBJECTID" and any(type(value) is not int for value in values):
            raise ValueError("source_object_id_value_drift")
        if kind == "esriFieldTypeInteger" and any(value is not None and type(value) is not int for value in values):
            raise ValueError("source_integer_attribute_type_drift:" + name)
        if kind == "esriFieldTypeString" and any(value is not None and not isinstance(value, str) for value in values):
            raise ValueError("source_string_attribute_type_drift:" + name)
        arrays.append(pa.array(values, type=_arrow_type(field)))
    geometries = []
    for feature in features:
        geometry = feature["geometry"]
        geometries.append(struct.pack("<BIdd", 1, 1, float(geometry["x"]), float(geometry["y"])))
    arrays.append(pa.array(geometries, type=pa.binary()))
    names = [field["name"] for field in properties] + ["geometry"]
    table = pa.Table.from_arrays(arrays, names=names)
    crs = pyproj.CRS.from_epsg(wkid).to_json_dict()
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": crs}}})}
    sink = pa.BufferOutputStream()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue().to_pybytes()


def capture_usace_crane_locations(storage: StagingStorageResource, *, version: str,
                                       capture_directory: str | None = None) -> dict[str, object]:
    """Acquire a complete bounded source snapshot, or replay an explicit capture directory."""
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    base=Path(storage.local_dir).absolute()
    if ".." in base.parts or any(p.is_symlink() for p in (base,*base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(base).free < 300_000_000: raise ValueError("disk_floor_before_acquisition")
    requested=Path(capture_directory).absolute() if capture_directory else None
    replay=bool(requested and (requested/"capture_manifest.json").exists())
    recover_receipted=bool(requested and not replay and any(requested.glob("*.receipt.json")))
    if replay:
        root=requested
    elif recover_receipted:
        root=requested
    elif requested:
        if any(requested.glob("*.body")) or any(requested.glob("*.wire")) or any(requested.glob("*.receipt.json")):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root=requested/(version+"-"+uuid4().hex)
    else:
        root=Path("/tmp/hifld-native-reconstruction/47/captures")/(version+"-"+uuid4().hex)
    if ".." in root.parts or any(p.is_symlink() for p in (root,*root.parents)):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True,exist_ok=True)
    captures={}
    try:
        if replay:
            retained_manifest=json.loads((root/"capture_manifest.json").read_bytes())
            if retained_manifest.get("complete") is not True or not isinstance(retained_manifest.get("receipts"),dict):
                raise ValueError("retained_capture_manifest_invalid")
            artifact_roster=retained_manifest.get("artifact_roster")
            if not isinstance(artifact_roster,dict):
                raise ValueError("retained_capture_roster_manifest_missing")
            validate_capture_roster(root,set(artifact_roster)|{"capture_manifest.json"})
            if capture_roster_digests(root,exclude={"capture_manifest.json"})!=artifact_roster:
                raise ValueError("retained_capture_artifact_roster_mismatch")
            ids_body,ids_receipt=_load_replay(root,"ids_before")
            ids=json.loads(ids_body).get("objectIds")
            if not isinstance(ids,list): raise ValueError("retained_id_seed_invalid")
            names=["ids_seed","bts_item","service_directory","service_before","layer_before","count_before","ids_before",*[f"page_{i:03d}" for i in range((len(ids)+PAGE_SIZE-1)//PAGE_SIZE)],"layer_after","count_after","ids_after","service_after"]
            if set(retained_manifest["receipts"])!=set(names):
                raise ValueError("retained_capture_roster_changed")
            captures={name:_load_replay(root,name) for name in names}
            if any(receipt!=retained_manifest["receipts"][name] for name,(_body,receipt) in captures.items()):
                raise ValueError("retained_capture_manifest_receipt_mismatch")
            attempt_map=retained_manifest.get("attempts",{})
            if not isinstance(attempt_map,dict): raise ValueError("retained_capture_attempt_manifest_invalid")
            for name,receipt in attempt_map.items():
                if not isinstance(name,str) or Path(name).name!=name or "/" in name or "\\" in name:
                    raise ValueError("retained_capture_attempt_name_invalid")
                attempt_paths=[root/f"{name}.body",root/f"{name}.wire"]
                attempt_receipt_path=root/f"{name}.receipt.json"
                if any(path.is_symlink() for path in [*attempt_paths,attempt_receipt_path]): raise ValueError("retained_capture_attempt_symlink_rejected")
                attempt_body,attempt_wire=attempt_paths[0].read_bytes(),attempt_paths[1].read_bytes()
                retained_attempt_receipt=json.loads(attempt_receipt_path.read_bytes())
                if (receipt.get("bytes")!=len(attempt_body) or receipt.get("sha256")!=_sha(attempt_body)
                    or receipt.get("wire_bytes")!=len(attempt_wire) or receipt.get("wire_sha256")!=_sha(attempt_wire)
                    or receipt!=retained_attempt_receipt):
                    raise ValueError("retained_failed_attempt_bytes_changed:"+name)
        elif recover_receipted:
            ids_body, ids_receipt = _load_replay(root, "ids_seed")
            ids = json.loads(ids_body).get("objectIds")
            if (not isinstance(ids, list) or len(ids) != EXPECTED_COUNT
                    or any(type(value) is not int for value in ids) or sorted(ids) != EXPECTED_IDS):
                raise ValueError("retained_unsealed_seed_roster_invalid")
            names_urls = [("ids_seed", _query({"where":"1=1","returnIdsOnly":"true","f":"json"})), *_capture_roster(EXPECTED_IDS)]
            expected_names = {name for name, _url in names_urls}
            actual_names = {path.name.removesuffix(".receipt.json") for path in root.glob("*.receipt.json")}
            if actual_names != expected_names:
                raise ValueError("retained_unsealed_capture_roster_mismatch")
            captures = {}
            for name, expected_url in names_urls:
                body, receipt = _load_replay(root, name)
                if receipt.get("url") != expected_url or receipt.get("final_url") != expected_url:
                    raise ValueError("retained_unsealed_capture_url_mismatch:" + name)
                captures[name] = (body, receipt)
            expected_files = {f"{name}.{suffix}" for name in expected_names for suffix in ("body", "wire", "receipt.json")}
            if {path.name for path in root.iterdir() if path.is_file()} != expected_files:
                raise ValueError("retained_unsealed_capture_artifact_roster_mismatch")
        else:
            budget_root=requested if requested else root.parent
            existing_bytes=sum(p.stat().st_size for p in budget_root.rglob("*")
                               if p.is_file() and p.suffix in {".body", ".wire"})
            capture_budget=[existing_bytes]
            if existing_bytes>MAX_CAPTURE_BYTES:
                raise ValueError("source_capture_budget_exceeded_before_request")
            with httpx.Client(follow_redirects=False,timeout=httpx.Timeout(45,connect=12),headers={"User-Agent":"hifld-next-datasets/usace-ienc-draft"}) as client:
                seed_body,seed_receipt=_fetch_retry(client,_query({"where":"1=1","returnIdsOnly":"true","f":"json"}),root,"ids_seed",capture_budget)
                ids=json.loads(seed_body).get("objectIds")
                if (not isinstance(ids,list) or len(ids)!=EXPECTED_COUNT
                        or any(type(value) is not int for value in ids) or sorted(ids)!=EXPECTED_IDS):
                    raise ValueError("source_seed_id_roster_drift")
                ids=EXPECTED_IDS
                captures["ids_seed"]=(seed_body,seed_receipt)
                for name,url in _capture_roster(ids): captures[name]=_fetch_retry(client,url,root,name,capture_budget)
        fields,features,audit=_validate(captures)
    except Exception as exc:
        retained={}
        for receipt_path in sorted(root.glob("*.receipt.json")):
            try: retained[receipt_path.name.removesuffix(".receipt.json")]=json.loads(receipt_path.read_bytes())
            except (OSError,ValueError): retained[receipt_path.name]={"receipt_readback_failed":True}
        (root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":False,"error_code":type(exc).__name__,"receipts":retained})+b"\n")
        if captures: quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise

    identity=processing_identity(Path(__file__).parents[3])
    wkid=4326
    parquet=_parquet_bytes(fields,features,wkid)
    capture_sha=_sha(_canonical({name:_sha(body) for name,(body,_receipt) in sorted(captures.items())}))
    audit["source_capture_sha256"]=capture_sha
    version="v"+_sha(_canonical({"source_record_sha256":audit["source_record_sha256"],
        "source_geometry_sha256":audit["source_geometry_sha256"],"source_schema_sha256":audit["source_schema_sha256"],
        "source_capture_sha256":capture_sha,"processing_identity":identity}))[:32]
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    inputs=[{"input_id":name,"producer":PUBLISHER,"host":urlsplit(receipt["url"]).hostname,"role":"official_distributor" if name=="bts_item" else "producer_publication","delegation_evidence":{"distributor":"USDOT BTS","item_id":BTS_ITEM_ID,"item_url":BTS_ITEM_URL,"service_directory_url":SERVICE_DIRECTORY_URL,"basis":"Captured USDOT BTS item identifies the IENC dataset as a USACE/AGC product and links the official IENC Feature Classes service; this supports distribution provenance only, not edition, freshness, rights, or release acceptance."} if name=="bts_item" else None,"canonical_url":BTS_ITEM_URL if name=="bts_item" else SERVICE_DIRECTORY_URL if name=="service_directory" else LAYER_URL,"retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],"http_status":receipt["status"],"bytes":receipt["bytes"],"sha256":receipt["sha256"],"license":"Unresolved; rights review pending.","attribution":"USACE IENC; rights review pending.","release_date":None,"release_date_unknown_reason":"BTS describes bi-monthly IENC updates but does not identify this layer snapshot's edition date."} for name,(_body,receipt) in sorted(captures.items())]
    manifest={"schema_version":1,"approved":False,"title":"Crane Locations (USACE IENC)","description":"Complete observed CRANES_POINT feature class from the official USACE IENC service; unapproved faithful candidate. The inland IENC chart extent is not a nationwide crane inventory. Historical HIFLD equivalence, source edition/freshness, schema/ID contract, rights, baseline and release decisions remain pending.","publisher":PUBLISHER,"agency":"US Army Corps of Engineers","source_url":LAYER_URL,"license":None,"source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],"code_identity_scheme":identity["code_identity_scheme"],"inputs":inputs,"transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"source_fields":"all declared attributes retained in source order and declared physical types","source_ids":"OBJECTID and Feature_ID values retained unchanged","geometry":"Native point x/y retained in captured raw Esri JSON and WKB at source EPSG:4326; no reprojection or geometry repair","candidate_status":"unapproved; source release, baseline, ID/schema, coverage and rights decisions remain pending"}}],"lineage":{"distribution_attestation":{"item_id":BTS_ITEM_ID,"owner":"USDOT_BTS","url":BTS_ITEM_URL,"description":"BTS item describes bi-monthly USACE/AGC IENC updates and links the official service."},"official_service_directory":SERVICE_DIRECTORY_URL,"official_service":SERVICE_URL,"layer":LAYER_URL,"scope":"This candidate represents only the complete observed CRANES_POINT layer, not every USACE crane or a nationwide inventory."},"prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Only native fields, IDs and point coordinates are represented in the candidate; output mapping, source rights and release decisions remain pending.","objects":[{"path":"geoparquet/crane-locations-usace-ienc.parquet","bytes":len(parquet),"sha256":_sha(parquet)}]}}},"unsupported_output_formats":UNSUPPORTED_OUTPUT_FORMATS}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"source_fields":fields,"official_layer_name":"CRANES_POINT","official_source_item":f"USDOT_BTS {BTS_ITEM_ID}","source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})
    attempts={}
    for receipt_path in sorted(root.glob("*.attempt*.receipt.json")):
        try: attempts[receipt_path.name.removesuffix(".receipt.json")]=json.loads(receipt_path.read_bytes())
        except (OSError,ValueError): attempts[receipt_path.name]={"receipt_readback_failed":True}
    (root/"capture_manifest.json").write_bytes(_canonical({"captured_at":_capture_started_at(captures),"complete":True,"source_feature_count":audit["source_feature_count"],"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"receipts":{n:r for n,(_b,r) in sorted(captures.items())},"attempts":attempts,"artifact_roster":capture_roster_digests(root,exclude={"capture_manifest.json"})})+b"\n")
    outputs={"geoparquet/crane-locations-usace-ienc.parquet":parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"]=body; outputs[f"raw/{name}.wire"]=(root/f"{name}.wire").read_bytes(); outputs[f"raw/{name}.receipt.json"]=_canonical(receipt)+b"\n"
    ready={"approved":False,"version":version,"objects":{"geoparquet/crane-locations-usace-ienc.parquet":{"bytes":len(parquet),"sha256":_sha(parquet)}},"source_population_complete":True,"source_record_sha256":audit["source_record_sha256"],"source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"]=_canonical(ready)+b"\n"
    fingerprint=_sha(_canonical(identity)); state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    receipts={n:r for n,(_b,r) in captures.items()}
    if storage.object_snapshot(state_key) is not None:
        bundle=json.loads(load_first_construction(storage,state_key,version,fingerprint))
        payload=bundle.get("payload")
        if not isinstance(payload,dict) or not isinstance(payload.get("capture_receipts"),dict):
            raise ValueError("cached_candidate_original_receipts_missing")
        original_receipts=payload["capture_receipts"]
        if set(original_receipts)!=set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name,(fresh_body,_receipt) in captures.items():
            retained=storage.read_key(f"{prefix}/raw/{name}.body")
            if retained!=fresh_body:
                raise ValueError("cached_source_bytes_changed:"+name)
            retained_wire=storage.read_key(f"{prefix}/raw/{name}.wire")
            if retained_wire!=(root/f"{name}.wire").read_bytes():
                raise ValueError("cached_source_wire_bytes_changed:"+name)
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,
            fingerprint,identity,audit["source_record_sha256"],audit["source_schema_sha256"],
            payload.get("objects",{}),original_receipts)
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,
            "feature_count":audit["source_feature_count"],"source_population_complete":True,
            "source_id_sha256":audit["source_id_sha256"],"source_id_min":audit["source_id_min"],"source_id_max":audit["source_id_max"],
            "source_schema_sha256":audit["source_schema_sha256"],"source_coverage_scope":audit["source_coverage_scope"],"drift_guard":audit["drift_guard"],
            "source_null_geometry_count":audit["source_null_geometry_count"],
            "source_empty_geometry_count":audit["source_empty_geometry_count"],
            "source_invalid_geometry_count":audit["source_invalid_geometry_count"],
            "candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,
            "source_release_accepted":False,"source_freshness_accepted":False,
            "schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,
            "rights_accepted":False,"mapping_approved":False,"release_accepted":False,
            "capture_directory":str(root)}
    descriptors=_write_outputs(storage,prefix,outputs)
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":receipts,"objects":descriptors})
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity
    )
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["source_feature_count"],"source_population_complete":True,"source_id_sha256":audit["source_id_sha256"],"source_id_min":audit["source_id_min"],"source_id_max":audit["source_id_max"],"source_schema_sha256":audit["source_schema_sha256"],"source_coverage_scope":audit["source_coverage_scope"],"drift_guard":audit["drift_guard"],"source_null_geometry_count":audit["source_null_geometry_count"],"source_empty_geometry_count":audit["source_empty_geometry_count"],"source_invalid_geometry_count":audit["source_invalid_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json","source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(root)}
