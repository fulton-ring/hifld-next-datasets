"""Bounded native capture for USACE IENC navigation system of marks areas.

This creates an unapproved source-faithful candidate from the official IENC
feature class. Each ArcGIS response is retained verbatim with its wire receipt.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import struct
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from pyproj import CRS
from shapely.geometry import LinearRing, MultiPolygon, Polygon

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response, capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.esri_common import arrow_type as _arrow_type, canonical as _canonical, sha as _sha
from dagster_hifld.sources.arcgis_layer import (
    esri_polygon as _esri_polygon, fetch_capture_retry,
    layer_parquet_bytes as _parquet_bytes,
    load_replay as _load_replay, write_outputs as _write_outputs,
)

from dagster_hifld.validation import sha256_file
from dagster_hifld.sources.capture_common import (
    candidate_prefix, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "navigation-system-of-marks-areas-usace-ienc"
FILE_SLUG = DATASET_SLUG
OLD_BTS_ITEM_ID = "3607d485a9854d6fa458bf507058ceae"
BTS_ITEM_ID = "7ec34864c8804468a9cf52d437fe4b6c"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
SERVICE_DIRECTORY_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson"
SERVICE_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/NAVIGATIONAL_SYSTEM_OF_MARKS_AREA/MapServer"
LAYER_ID = 68
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; official IENC Feature Classes service"
EXPECTED_COUNT = 109
PAGE_SIZE = 250
MAX_BODY_BYTES = 48_000_000
MAX_CAPTURE_BYTES = 450_000_000
SOURCE_CRS = CRS.from_epsg(4326)
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID"), ("SHAPE", "esriFieldTypeGeometry"),
    ("System_of_Navigational_Marks", "esriFieldTypeString"),
    ("Source_Date", "esriFieldTypeString"), ("Source_Indication", "esriFieldTypeString"),
    ("Source_Dataset", "esriFieldTypeString"), ("Feature_ID", "esriFieldTypeString"),
]
EXPECTED_FIELD_DECLARATIONS = [
    {"name":name,"type":kind,"alias":name,"domain":None,
     **({"length":254} if kind == "esriFieldTypeString" else {})}
    for name,kind in EXPECTED_FIELDS
]


def _validate_spatial_reference(value: object, label: str, *, required: bool = True) -> bool:
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            if type(value[key]) is not int or value[key] != 4326:
                raise ValueError(f"{label}_crs_drift")
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            wkt = value[key]
            if not isinstance(wkt, str) or not wkt.strip():
                raise ValueError(f"{label}_crs_drift")
            try:
                crs = CRS.from_wkt(wkt)
            except Exception as exc:
                raise ValueError(f"{label}_crs_drift") from exc
            if not crs.equals(SOURCE_CRS, ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
            found = True
    if required and not found:
        raise ValueError(f"{label}_crs_drift")
    return found


def _fetch_retry(client: httpx.Client, url: str, root: Path, name: str, budget: list[int]):
    """Retry one transient transport/incomplete read, retaining every attempt."""
    return fetch_capture_retry(client, url, root, name, budget,
                               max_capture_bytes=MAX_CAPTURE_BYTES, max_body_bytes=MAX_BODY_BYTES)


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


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


def _load_prefilled(root: Path, name: str, expected_url: str) -> tuple[bytes, dict]:
    body, receipt = _load_replay(root, name)
    if (receipt.get("method") != "GET" or receipt.get("url") != expected_url
            or receipt.get("final_url") != expected_url):
        raise ValueError("prefilled_capture_request_identity_mismatch:" + name)
    return body, receipt


def _validate(captures: dict[str, tuple[bytes, dict]]):
    docs = {name: json.loads(body) for name, (body, _r) in captures.items()}
    item = docs["bts_item"]
    description = item.get("description")
    if (item.get("id") != BTS_ITEM_ID or item.get("owner") != "USDOT_BTS"
            or item.get("type") != "PDF" or item.get("access") != "public"
            or not isinstance(item.get("title"), str) or "Inland Electronic Navigational Charts" not in item["title"]
            or not isinstance(description, str)
            or "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes" not in description
            or "USACE_IENC_Master_Service_gdb.zip" not in description):
        raise ValueError("official_bts_distribution_reference_drift")
    directory = docs["service_directory"]
    matching_services = [entry for entry in directory.get("services", [])
                         if isinstance(entry, dict)
                         and entry.get("name") == "IENC_Feature_Classes/NAVIGATIONAL_SYSTEM_OF_MARKS_AREA"
                         and entry.get("type") == "MapServer"]
    if len(matching_services) != 1:
        raise ValueError("official_ienc_service_directory_drift")
    service_before, service_after = docs["service_before"], docs["service_after"]
    service_layers = service_before.get("layers")
    advertised = [(entry.get("id"), entry.get("name"), entry.get("geometryType"))
                  for entry in service_layers if isinstance(entry, dict)] if isinstance(service_layers, list) else []
    if (service_before.get("mapName") != "NAVIGATIONAL_SYSTEM_OF_MARKS_AREA"
            or service_before.get("copyrightText") != "USACE IENC"
            or advertised != [(LAYER_ID, "NAVIGATIONAL_SYSTEM_OF_MARKS_AREA", "esriGeometryPolygon")]
            or service_after != service_before):
        raise ValueError("official_ienc_service_metadata_changed_or_layer_missing")
    layer = docs["layer_before"]
    fields = layer.get("fields")
    projected=[{key:f[key] for key in ("name","type","alias","length","domain","nullable") if key in f}
               for f in fields] if isinstance(fields,list) else None
    if projected != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if layer.get("name") != "NAVIGATIONAL_SYSTEM_OF_MARKS_AREA" or layer.get("id") != 68 or layer.get("geometryType") != "esriGeometryPolygon":
        raise ValueError("source_layer_identity_drift")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_spatial_reference_or_page_limit_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent_reference = layer.get("extent", {}).get("spatialReference")
    if extent_reference is not None:
        _validate_spatial_reference(extent_reference, "source_layer_extent")
    if docs["layer_after"] != layer:
        raise ValueError("source_schema_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    before, after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if not isinstance(before,list) or before != after or len(before) != EXPECTED_COUNT or len(set(before)) != EXPECTED_COUNT:
        raise ValueError("source_id_population_drift")
    if docs["ids_seed"].get("objectIds") != before:
        raise ValueError("source_seed_id_population_changed")
    features=[]
    pages=sorted(n for n in captures if n.startswith("page_"))
    if len(pages) != (EXPECTED_COUNT + PAGE_SIZE - 1)//PAGE_SIZE:
        raise ValueError("source_page_roster_incomplete")
    for name in pages:
        page=docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncation")
        _validate_spatial_reference(page.get("spatialReference"), "source_" + name + "_page")
        rows=page.get("features")
        if not isinstance(rows,list): raise ValueError("source_page_features_missing")
        features.extend(rows)
    property_fields=[f for f in fields if f["name"]!="SHAPE"]
    expected_attrs=[f["name"] for f in property_fields]
    got=[]
    vertex_count=0
    for feature in features:
        attrs, geom=feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs,dict) or list(attrs)!=expected_attrs or not isinstance(geom,dict) or "rings" not in geom:
            raise ValueError("source_record_schema_or_geometry_drift")
        if set(geom)-{"rings","hasZ","hasM","spatialReference"}:
            raise ValueError("unexpected_source_geometry_ordinates")
        for field in property_fields:
            value=attrs[field["name"]]
            if value is None:
                continue
            if field["type"] == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                raise ValueError("source_oid_value_type_drift")
            if field["type"] == "esriFieldTypeString" and not isinstance(value, str):
                raise ValueError("source_string_value_type_drift:" + field["name"])
        if geom.get("spatialReference") is not None:
            _validate_spatial_reference(geom["spatialReference"], "source_" + name + "_feature")
        rings=geom.get("rings")
        if not isinstance(rings,list) or not rings or any(not isinstance(ring,list) for ring in rings):
            raise ValueError("source_polygon_rings_missing")
        vertex_count += sum(len(ring) for ring in rings)
        got.append(attrs.get("OBJECTID"))
    if (len(features)!=EXPECTED_COUNT or any(type(oid) is not int for oid in got)
            or sorted(got)!=sorted(before) or len(set(got))!=EXPECTED_COUNT):
        raise ValueError("source_feature_population_incomplete")
    features.sort(key=lambda f:f["attributes"]["OBJECTID"])
    rows=[{"attributes":f["attributes"],"geometry":f["geometry"]} for f in features]
    record_sha=_sha(_canonical([f["attributes"] for f in features]))
    geometry_sha=_sha(_canonical([[f["attributes"]["OBJECTID"],f["geometry"]] for f in features]))
    schema_sha=_sha(_canonical(fields))
    return fields, features, {"source_record_sha256":record_sha,"source_geometry_sha256":geometry_sha,"source_schema_sha256":schema_sha,
        "source_id_sha256":_sha(_canonical(sorted(got))),
        "source_feature_count":len(features),"source_id_min":min(got),"source_id_max":max(got),
        "source_geometry_type":"esriGeometryPolygon","source_wkid":4326,
        "source_has_z":bool(layer.get("hasZ")),"source_has_m":bool(layer.get("hasM")),
        "source_vertex_count":vertex_count,"source_extent":layer.get("extent"),
        "source_coverage_scope":"Complete official IENC navigational system of marks areas feature class; coverage is the published inland charting area.",
        "drift_guard":"Exact BTS item owner/type/id, official service directory, service/layer identity, count, field schema, CRS, page errors, and before/after ID membership are checked.",
        "official_source_item":{"id":item["id"],"owner":item["owner"],"type":item["type"],
            "title":item["title"],"created":item.get("created"),"modified":item.get("modified")},
        "bts_item_succession_accepted":False}


def capture_usace_navigation_marks_areas(storage: StagingStorageResource, *, version: str,
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
    if replay:
        root=requested
    elif requested:
        root=requested
    else:
        root=default_capture_dir("hifld-native-reconstruction", "6", "captures")/(version+"-"+uuid4().hex)
    if ".." in root.parts or any(p.is_symlink() for p in (root,*root.parents)):
        raise ValueError("unsafe_capture_directory")
    if replay:
        root.mkdir(parents=True,exist_ok=True)
    captures={}
    try:
        if replay:
            retained_manifest=json.loads((root/"capture_manifest.json").read_bytes())
            if retained_manifest.get("complete") is not True or not isinstance(retained_manifest.get("receipts"),dict):
                raise ValueError("retained_capture_manifest_invalid")
            roster=retained_manifest.get("artifact_roster")
            if not isinstance(roster,dict): raise ValueError("retained_capture_roster_manifest_missing")
            validate_capture_roster(root,set(roster)|{"capture_manifest.json"})
            if roster != capture_roster_digests(root,exclude={"capture_manifest.json"}):
                raise ValueError("retained_capture_artifact_digest_mismatch")
            ids_body,ids_receipt=_load_replay(root,"ids_before")
            ids=json.loads(ids_body).get("objectIds")
            if not isinstance(ids,list): raise ValueError("retained_id_seed_invalid")
            roster = _capture_roster(ids)
            names=["ids_seed", *[name for name, _url in roster]]
            if set(retained_manifest["receipts"])!=set(names):
                raise ValueError("retained_capture_roster_changed")
            captures={name:_load_replay(root,name) for name in names}
            if any(receipt!=retained_manifest["receipts"][name] for name,(_body,receipt) in captures.items()):
                raise ValueError("retained_capture_manifest_receipt_mismatch")
            expected_urls = {name: url for name, url in roster}
            expected_urls["ids_seed"] = _query({"where":"1=1","returnIdsOnly":"true","f":"json"})
            for name, (_body, receipt) in captures.items():
                if (receipt.get("method") != "GET" or receipt.get("url") != expected_urls[name]
                        or receipt.get("final_url") != expected_urls[name]):
                    raise ValueError("retained_capture_request_identity_mismatch:" + name)
            attempt_map=retained_manifest.get("attempts",{})
            if not isinstance(attempt_map,dict): raise ValueError("retained_capture_attempt_manifest_invalid")
            for name,receipt in attempt_map.items():
                if not isinstance(name,str) or Path(name).name!=name or "/" in name or "\\" in name:
                    raise ValueError("retained_capture_attempt_name_invalid")
                attempt_paths=[root/f"{name}.body",root/f"{name}.wire"]
                if any(path.is_symlink() for path in attempt_paths): raise ValueError("retained_capture_attempt_symlink_rejected")
                attempt_body,attempt_wire=attempt_paths[0].read_bytes(),attempt_paths[1].read_bytes()
                if (receipt.get("bytes")!=len(attempt_body) or receipt.get("sha256")!=_sha(attempt_body)
                    or receipt.get("wire_bytes")!=len(attempt_wire) or receipt.get("wire_sha256")!=_sha(attempt_wire)):
                    raise ValueError("retained_failed_attempt_bytes_changed:"+name)
        else:
            prefilled = {}
            prefill_urls = {"bts_item": BTS_ITEM_URL, "service_directory": SERVICE_DIRECTORY_URL}
            if requested:
                for name, url in prefill_urls.items():
                    paths = [requested / f"{name}{suffix}" for suffix in (".body", ".wire", ".receipt.json")]
                    present = [path.exists() or path.is_symlink() for path in paths]
                    if any(present) and not all(present):
                        raise ValueError("incomplete_prefilled_capture:" + name)
                    if all(present):
                        prefilled[name] = _load_prefilled(requested, name, url)
                if prefilled:
                    expected_prefills = {f"{name}{suffix}" for name in prefilled
                                         for suffix in (".body", ".wire", ".receipt.json")}
                    unexpected = {path.name for suffix in ("*.body", "*.wire", "*.receipt.json")
                                  for path in requested.glob(suffix)} - expected_prefills
                    if unexpected:
                        raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
                    root = requested
                else:
                    if any(requested.glob("*.body")) or any(requested.glob("*.wire")) or any(requested.glob("*.receipt.json")):
                        raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
                    root = requested / (version + "-" + uuid4().hex)
            else:
                root = default_capture_dir("hifld-native-reconstruction", "6", "captures") / (version + "-" + uuid4().hex)
            if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)):
                raise ValueError("unsafe_capture_directory")
            root.mkdir(parents=True, exist_ok=True)
            budget_root=requested if requested else root.parent
            existing_bytes=sum(p.stat().st_size for p in budget_root.rglob("*") if p.is_file() and p.suffix in {".body",".wire"})
            capture_budget=[existing_bytes]
            if existing_bytes>MAX_CAPTURE_BYTES: raise ValueError("source_capture_budget_exceeded_before_request")
            captures.update(prefilled)
            with httpx.Client(follow_redirects=False,timeout=httpx.Timeout(45,connect=12),headers={"User-Agent":"hifld-next-datasets/usace-ienc-draft"}) as client:
                seed_body,seed_receipt=_fetch_retry(client,_query({"where":"1=1","returnIdsOnly":"true","f":"json"}),root,"ids_seed",capture_budget)
                ids=json.loads(seed_body).get("objectIds")
                if not isinstance(ids,list) or len(ids)!=EXPECTED_COUNT: raise ValueError("source_seed_id_count_drift")
                captures["ids_seed"]=(seed_body,seed_receipt)
                for name,url in _capture_roster(ids):
                    if name not in captures:
                        captures[name]=_fetch_retry(client,url,root,name,capture_budget)
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
    capture_evidence={}
    for name,(body,receipt) in sorted(captures.items()):
        wire=(root/f"{name}.wire").read_bytes()
        capture_evidence[name]={"decoded_sha256":_sha(body),"wire_sha256":_sha(wire),
            "request_url":receipt["url"],"observed_at":receipt["observed_at"]}
    capture_sha=_sha(_canonical(capture_evidence))
    audit.update({"source_capture_sha256":capture_sha,"source_capture_evidence":capture_evidence,
        "source_receipts":{name:receipt for name,(_body,receipt) in sorted(captures.items())},
        "source_population_complete":True,"source_release_accepted":False,"source_freshness_accepted":False,
        "schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,
        "rights_accepted":False,"mapping_approved":False,"release_accepted":False,"baseline_accepted":False})
    version="v"+_sha(_canonical({"source_record_sha256":audit["source_record_sha256"],
        "source_geometry_sha256":audit["source_geometry_sha256"],"source_schema_sha256":audit["source_schema_sha256"],
        "source_capture_sha256":capture_sha,"processing_identity":identity}))[:32]
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    inputs=[]
    for name,(_body,receipt) in sorted(captures.items()):
        distributor=name=="bts_item"
        inputs.append({"input_id":name,
            "producer":"USDOT BTS IENC public ArcGIS item" if distributor else PUBLISHER,
            "host":urlsplit(receipt["url"]).hostname,
            "role":"official_distributor" if distributor else "producer_publication",
            "canonical_url":BTS_ITEM_URL if distributor else LAYER_URL,
            "retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],
            "retrieved_at":receipt["observed_at"],"http_status":receipt["status"],
            "bytes":receipt["bytes"],"sha256":receipt["sha256"],
            "wire_bytes":receipt["wire_bytes"],"wire_sha256":receipt["wire_sha256"],
            "license":"Unresolved; rights review pending.",
            "attribution":"USACE IENC / BTS; attribution and rights review pending.","release_date":None,
            "release_date_unknown_reason":"The BTS item describes IENC update cadence but does not identify this layer's release date.",
            **({"distributor":"USACE IENC Feature Classes service","delegation_evidence":{
                "bts_item_url":BTS_ITEM_URL,"bts_item_id":BTS_ITEM_ID,"bts_item_owner":"USDOT_BTS",
                "bts_item_type":"PDF","official_service_directory":SERVICE_DIRECTORY_URL,
                "linked_feature_classes_service":"https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes",
                "basis":"The captured public BTS item identifies IENC and links the USACE Feature Classes service and master geodatabase; the captured service metadata identifies USACE IENC copyright."}} if distributor else {})})
    manifest={"schema_version":1,"approved":False,"title":"Navigation System of Marks Areas (USACE IENC)",
        "description":"Complete official USACE IENC NAVIGATIONAL_SYSTEM_OF_MARKS_AREA feature class; unapproved faithful candidate. Does not assert national coverage or compatibility with the historical HIFLD/FEMA baseline.",
        "publisher":PUBLISHER,"agency":"US Army Corps of Engineers","source_url":LAYER_URL,
        "license":None,"source_modified":None,"date_issued":None,"date_modified":None,
        "processor":"HIFLD Next","code_commit":identity["code_commit"],
        "code_identity_scheme":identity["code_identity_scheme"],"inputs":inputs,
        "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate",
            "parameters":{"source_fields":"all declared attributes retained in source order and declared physical types",
                "source_ids":"OBJECTID and Feature_ID values retained unchanged",
                "geometry":"Polygon rings retained in captured raw Esri JSON; WKB candidate at source EPSG:4326; no reprojection or geometry repair",
                "candidate_status":"unapproved; BTS-item succession, source release, baseline, ID/schema, coverage, rights and compatibility decisions remain pending"}}],
        "lineage":(f"Current public BTS IENC PDF item {BTS_ITEM_ID} links the official USACE IENC Feature Classes service and master geodatabase. "
            f"The previous BTS map-service item {OLD_BTS_ITEM_ID} returned an anonymous-access denial on 2026-10-21; "
            "succession continuity is recorded but not accepted. This candidate contains only the complete observed service layer 68."),
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native layer attributes, identifiers and unmodified rings are retained as a candidate; release and compatibility decisions remain pending.",
            "objects":[{"path":"geoparquet/navigation-system-of-marks-areas-usace-ienc.parquet","bytes":len(parquet),"sha256":_sha(parquet)}]}}},
        "unsupported_output_formats":{"geopackage":"No pinned complete native GDB inventory or schema/compatibility approval.",
            "pmtiles":"No display transformation approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"source_fields":fields,"official_layer_name":"NAVIGATIONAL_SYSTEM_OF_MARKS_AREA",
        "official_layer_id":LAYER_ID,"official_source_item":"USDOT_BTS " + BTS_ITEM_ID,
        "bts_item_succession_accepted":False,"source_release_accepted":False,
        "source_freshness_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,
        "mapping_approved":False,"release_accepted":False})
    attempts={}
    for receipt_path in sorted(root.glob("*.attempt*.receipt.json")):
        try: attempts[receipt_path.name.removesuffix(".receipt.json")]=json.loads(receipt_path.read_bytes())
        except (OSError,ValueError): attempts[receipt_path.name]={"receipt_readback_failed":True}
    (root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,"source_feature_count":audit["source_feature_count"],"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"receipts":{n:r for n,(_b,r) in sorted(captures.items())},"attempts":attempts,"artifact_roster":capture_roster_digests(root,exclude={"capture_manifest.json"})})+b"\n")
    outputs={"geoparquet/navigation-system-of-marks-areas-usace-ienc.parquet":parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"]=body; outputs[f"raw/{name}.wire"]=(root/f"{name}.wire").read_bytes(); outputs[f"raw/{name}.receipt.json"]=_canonical(receipt)+b"\n"
    ready={"approved":False,"version":version,"objects":{"geoparquet/navigation-system-of-marks-areas-usace-ienc.parquet":{"bytes":len(parquet),"sha256":_sha(parquet)}},"source_population_complete":True,"source_record_sha256":audit["source_record_sha256"],"source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
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
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,
            fingerprint,identity,audit["source_record_sha256"],audit["source_schema_sha256"],
            payload.get("objects",{}),original_receipts)
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,
            "feature_count":audit["source_feature_count"],"source_population_complete":True,
            "source_id_sha256":audit["source_id_sha256"],"source_id_min":audit["source_id_min"],"source_id_max":audit["source_id_max"],
            "source_schema_sha256":audit["source_schema_sha256"],"source_coverage_scope":audit["source_coverage_scope"],"drift_guard":audit["drift_guard"],
            "candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,
            "source_release_accepted":False,"source_freshness_accepted":False,
            "schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,
            "rights_accepted":False,"mapping_approved":False,"release_accepted":False,
            "capture_directory":str(root)}
    descriptors=_write_outputs(storage,prefix,outputs)
    from dagster_hifld.sources.capture_common import seal_ready_candidate
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":receipts,"objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"],identity=identity)
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["source_feature_count"],"source_population_complete":True,"source_id_sha256":audit["source_id_sha256"],"source_id_min":audit["source_id_min"],"source_id_max":audit["source_id_max"],"source_schema_sha256":audit["source_schema_sha256"],"source_coverage_scope":audit["source_coverage_scope"],"drift_guard":audit["drift_guard"],"candidate_ready":prefix+"/metadata/candidate_ready.json","source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(root)}
