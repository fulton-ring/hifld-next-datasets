"""Lossless draft capture of the official BTS National Transit Map Agencies layer.

The candidate pins the 2026-09-23 source compilation. It remains blocked from
publication until the source/schema/ID contract and historical baseline have
been accepted by a maintainer.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    quarantine_incomplete_capture, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "national-transit-map-agencies"
FILE_SLUG = DATASET_SLUG
ITEM_ID = "ad6b0823f7364cac86c5421834eaba84"
ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}"
OWNER = "USDOT_BTS"
SERVICE_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_National_Transit_Map_Agencies/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
SOURCE_RELEASE = "2026-09-23"
SOURCE_RELEASE_TEXT = "September 23, 2026"
EXPECTED_COUNT = 1239
EXPECTED_IDS_SHA256 = "7a2baf2d47556afe754208a28bfd7da99a3115bcc32d79fc4f1cb680650682b0"
EXPECTED_SCHEMA_SHA256 = "a9f1aff709a051aaa6809b575c9f4c8c803a1e9c42f0dc135a0566220be888bc"
EXPECTED_RECORD_SHA256 = "e7c048ce039bffa402b2e819b9f4a213fa72c4e6bcef94a5ed4e2e7a6d27fa2b"
EXPECTED_GEOMETRY_SHA256 = "cf3d7364dbfa56b5a5557495baac94d57e6ac1df308c3f20413194c29b1b49bb"
PAGE_SIZE = 250
MAX_BODY_BYTES = 16_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _fetch(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    wire, body = root / f"{name}.wire", root / f"{name}.body"
    wire.unlink(missing_ok=True); body.unlink(missing_ok=True)
    wh, bh = hashlib.sha256(), hashlib.sha256(); ws = bs = 0
    def retain(path: Path, digest, data: bytes, kind: str) -> None:
        nonlocal ws, bs
        with path.open("ab") as out: out.write(data)
        if kind == "wire": ws += len(data); wh.update(data)
        else: bs += len(data); bh.update(data)
    observed = datetime.now(timezone.utc).isoformat()
    with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
        result = capture_http_response(response, max_wire_bytes=MAX_BODY_BYTES,
            max_decoded_bytes=MAX_BODY_BYTES, reserve=lambda _: None,
            retain_wire=lambda b: retain(wire, wh, b, "wire"),
            retain_decoded=lambda b: retain(body, bh, b, "body"))
        final = str(response.url)
    receipt = {"method":"GET","url":url,"final_url":final,"observed_at":observed,
        "status":result.status_code,"complete_body":result.complete,"wire_bytes":result.wire_bytes_retained,
        "wire_sha256":result.wire_sha256,"bytes":result.decoded_bytes_retained,"sha256":result.decoded_sha256,
        "content_encoding":result.content_encoding,"content_length":result.content_length,"error_code":result.error_code}
    (root/f"{name}.receipt.json").write_bytes(_canonical(receipt)+b"\n")
    if (not result.complete or result.status_code != 200 or final != url or ws != receipt["wire_bytes"]
        or bs != receipt["bytes"] or _sha(wire.read_bytes()) != receipt["wire_sha256"]
        or _sha(body.read_bytes()) != receipt["sha256"]):
        raise ValueError("incomplete_source_capture:"+name)
    return body.read_bytes(), receipt


def _field_profile(fields: object) -> tuple[list[dict[str, object]], str]:
    if not isinstance(fields, list) or any(not isinstance(f, dict) for f in fields):
        raise ValueError("source_field_roster_missing")
    profile = [{k:f[k] for k in ("name","type","alias","length","nullable","domain") if k in f} for f in fields]
    return profile, _sha(_canonical(profile))


def validate_captures(captures: Mapping[str, tuple[bytes, Mapping[str, object]]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    docs = {n:json.loads(body) for n,(body,_) in captures.items()}
    if set(docs) != {"item","service_before","layer_before","count_before","ids_before","ids_seed",
        "count_after","ids_after","layer_after","service_after",*(f"page_{i:03}" for i in range(5))}:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    item, service, layer = docs["item"], docs["service_before"], docs["layer_before"]
    if item.get("id") != ITEM_ID or item.get("owner") != OWNER or item.get("access") != "public" or item.get("url") != SERVICE_URL:
        raise ValueError("official_BTS_item_identity_drift")
    if SOURCE_RELEASE_TEXT.casefold() not in str(item.get("description", "")).casefold():
        raise ValueError("official_source_release_date_drift")
    if service.get("serviceItemId") != ITEM_ID or LAYER_ID not in [x.get("id") for x in service.get("layers",[]) if isinstance(x,dict)]:
        raise ValueError("official_BTS_service_identity_drift")
    if layer.get("id") != LAYER_ID or layer.get("name") != "National Transit Map Agencies" or layer.get("geometryType") != "esriGeometryPoint":
        raise ValueError("official_BTS_layer_identity_or_geometry_drift")
    if layer.get("objectIdField") != "OBJECTID" or layer.get("maxRecordCount",0) < PAGE_SIZE:
        raise ValueError("official_BTS_ID_or_transfer_contract_drift")
    if layer.get("extent",{}).get("spatialReference",{}).get("latestWkid") != 4326:
        raise ValueError("official_BTS_CRS_drift")
    if layer.get("hasZ") is not True or layer.get("hasM") is not False:
        raise ValueError("official_BTS_geometry_dimension_drift")
    fields, schema_sha = _field_profile(layer.get("fields"))
    if schema_sha != EXPECTED_SCHEMA_SHA256 or _field_profile(docs["layer_after"].get("fields"))[0] != fields:
        raise ValueError("source_schema_drift")
    if docs["service_after"] != service or docs["layer_after"] != layer:
        raise ValueError("source_metadata_changed_during_capture")
    ids = docs["ids_before"].get("objectIds"); seed = docs["ids_seed"].get("objectIds")
    if (docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT
        or ids != docs["ids_after"].get("objectIds") or not isinstance(ids,list) or len(ids)!=EXPECTED_COUNT
        or sorted(seed or []) != sorted(ids)):
        raise ValueError("source_population_drift")
    ids = sorted(ids)
    if _sha(_canonical(ids)) != EXPECTED_IDS_SHA256:
        raise ValueError("source_id_population_drift")
    features=[]
    for name in sorted(n for n in docs if n.startswith("page_")):
        page=docs[name]
        if page.get("error") or page.get("exceededTransferLimit") is True or page.get("spatialReference",{}).get("latestWkid") != 4326:
            raise ValueError("source_page_error_or_truncation")
        rows=page.get("features")
        if not isinstance(rows,list): raise ValueError("source_page_missing_features")
        features.extend(rows)
    names={f["name"] for f in fields}
    if len(features)!=EXPECTED_COUNT or sorted(f.get("attributes",{}).get("OBJECTID") for f in features)!=ids:
        raise ValueError("source_page_population_incomplete")
    for f in features:
        if set(f.get("attributes",{}))!=names or not isinstance(f.get("geometry"),dict) or set(f["geometry"])!={"x","y","z"}:
            raise ValueError("source_record_or_geometry_schema_drift")
    features.sort(key=lambda f:f["attributes"]["OBJECTID"])
    record_sha=_sha(_canonical(features)); geometry_sha=_sha(_canonical([[f["attributes"]["OBJECTID"],f["geometry"]["x"],f["geometry"]["y"],f["geometry"]["z"]] for f in features]))
    if record_sha!=EXPECTED_RECORD_SHA256 or geometry_sha!=EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_values_or_geometry_drift")
    return fields, features


def _load_retained_capture(directory: Path, destination: Path) -> dict[str, tuple[bytes, dict[str, object]]]:
    """Replay a previously retained complete capture without contacting BTS."""
    root=Path(directory).absolute()
    if ".." in root.parts or any(p.is_symlink() for p in (root,*root.parents)) or not root.is_dir():
        raise ValueError("unsafe_or_missing_retained_capture")
    try: manifest=json.loads((root/"capture_manifest.json").read_bytes())
    except (OSError,ValueError) as exc: raise ValueError("retained_capture_manifest_missing_or_invalid") from exc
    source_count=manifest.get("source_feature_count",manifest.get("feature_count"))
    if manifest.get("complete") is not True or source_count!=EXPECTED_COUNT:
        raise ValueError("retained_capture_is_not_complete_pinned_population")
    receipts=manifest.get("receipts")
    if not isinstance(receipts,dict): raise ValueError("retained_capture_receipts_missing")
    captures={}
    for name,receipt in receipts.items():
        if not isinstance(name,str) or not isinstance(receipt,dict): raise ValueError("retained_capture_receipt_invalid")
        body_path=root/f"{name}.body"; wire_path=root/f"{name}.wire"; receipt_path=root/f"{name}.receipt.json"
        try: body=body_path.read_bytes();wire=wire_path.read_bytes();receipt_body=receipt_path.read_bytes()
        except OSError as exc: raise ValueError("retained_capture_object_missing:"+name) from exc
        if (receipt.get("complete_body") is not True or receipt.get("status")!=200
            or receipt.get("final_url")!=receipt.get("url") or receipt.get("bytes")!=len(body)
            or receipt.get("sha256")!=_sha(body) or receipt.get("wire_bytes")!=len(wire)
            or receipt.get("wire_sha256")!=_sha(wire) or receipt_body!=_canonical(receipt)+b"\n"):
            raise ValueError("retained_capture_integrity_failed:"+name)
        shutil.copyfile(wire_path,destination/f"{name}.wire")
        captures[name]=(body,receipt)
    if manifest.get("receipts")!={name:receipt for name,(_,receipt) in sorted(captures.items())}:
        raise ValueError("retained_capture_receipt_roster_changed")
    return captures


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    t=field["type"]
    if t=="esriFieldTypeOID": return pa.int64()
    if t=="esriFieldTypeDouble": return pa.float64()
    if t=="esriFieldTypeString": return pa.string()
    raise ValueError("unsupported_BTS_physical_type:"+str(t))


def _parquet(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    columns={f["name"]:pa.array([r["attributes"][f["name"]] for r in features], type=_arrow_type(f)) for f in fields}
    columns["geometry"]=pa.array([Point(r["geometry"]["x"],r["geometry"]["y"],r["geometry"]["z"]).wkb for r in features],type=pa.binary())
    schema=pa.schema([pa.field(f["name"],_arrow_type(f),nullable=f.get("nullable",True)) for f in fields]+
        [pa.field("geometry",pa.binary(),nullable=False)])
    geo={"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":["Point Z"],"crs":pyproj.CRS.from_epsg(4326).to_json_dict(),"edges":"planar"}}}
    sink=pa.BufferOutputStream();pq.write_table(pa.Table.from_pydict(columns,schema=schema.with_metadata({b"geo":_canonical(geo)})),sink,compression="zstd")
    return sink.getvalue().to_pybytes()


def capture_national_transit_map_agencies(storage: StagingStorageResource, *, version: str,
    capture_directory: str | None = None, replay_capture_directory: str | None = None) -> dict[str, object]:
    """Acquire complete BTS layer, retaining raw/wire bodies and receipts."""
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version): raise ValueError("invalid_version")
    base=Path(storage.local_dir).absolute()
    if ".." in base.parts or any(p.is_symlink() for p in (base,*base.parents)): raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(base).free<256_000_000: raise ValueError("disk_floor_before_acquisition")
    root=(Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/167/captures"))/f"{version}-{uuid4().hex}"
    root=root.absolute();root.mkdir(parents=True,exist_ok=False)
    captures={}
    try:
        if replay_capture_directory:
            captures=_load_retained_capture(Path(replay_capture_directory),root)
        else:
          with httpx.Client(follow_redirects=False,timeout=60,headers={"User-Agent":"hifld-next-datasets/national-transit-map-agencies-draft"}) as client:
            urls=[("item",ITEM_URL+"?f=json"),("service_before",SERVICE_URL+"?f=pjson"),("layer_before",LAYER_URL+"?f=pjson"),
                ("count_before",LAYER_URL+"/query?"+urlencode({"where":"1=1","returnCountOnly":"true","f":"json"})),
                ("ids_before",LAYER_URL+"/query?"+urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"}))]
            b,r=_fetch(client,LAYER_URL+"/query?"+urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"}),root,"ids_seed");captures["ids_seed"]=(b,r)
            ids=json.loads(b).get("objectIds")
            if not isinstance(ids,list) or len(ids)!=EXPECTED_COUNT: raise ValueError("source_seed_population_drift")
            urls.extend((f"page_{i//PAGE_SIZE:03}",LAYER_URL+"/query?"+urlencode({"objectIds":",".join(map(str,ids[i:i+PAGE_SIZE])),"outFields":"*","returnGeometry":"true","returnZ":"true","returnM":"true","outSR":"4326","orderByFields":"OBJECTID ASC","f":"json"})) for i in range(0,len(ids),PAGE_SIZE))
            urls.extend([("layer_after",LAYER_URL+"?f=pjson"),("service_after",SERVICE_URL+"?f=pjson"),
                ("count_after",LAYER_URL+"/query?"+urlencode({"where":"1=1","returnCountOnly":"true","f":"json"})),
                ("ids_after",LAYER_URL+"/query?"+urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"}))])
            for name,url in urls:
                b,r=_fetch(client,url,root,name);captures[name]=(b,r)
        fields,features=validate_captures(captures)
    except Exception as exc:
        retained=dict(captures)
        # Preserve a response whose stream failed before _fetch could return it.
        for receipt_path in root.glob("*.receipt.json"):
            name=receipt_path.name.removesuffix(".receipt.json")
            if name in retained: continue
            try: receipt=json.loads(receipt_path.read_bytes())
            except (OSError,ValueError): receipt={"complete_body":False,"status":None,"error_code":"receipt_unreadable"}
            body_path=root/f"{name}.body"
            retained[name]=(body_path.read_bytes() if body_path.exists() else b"",receipt)
        for wire_path in root.glob("*.wire"):
            name=wire_path.name.removesuffix(".wire")
            if name not in retained:
                body_path=root/f"{name}.body"
                retained[name]=(body_path.read_bytes() if body_path.exists() else b"",
                    {"complete_body":False,"status":None,"error_code":"capture_interrupted"})
        if retained: quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,retained,type(exc).__name__)
        raise
    identity=processing_identity(Path(__file__).parents[3]); identity["gdal"]=__import__("pyogrio").__gdal_version_string__
    data=_parquet(fields,features); record_sha=EXPECTED_RECORD_SHA256; schema_sha=EXPECTED_SCHEMA_SHA256
    version=content_derived_version(record_sha,EXPECTED_GEOMETRY_SHA256,schema_sha,identity)
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    source_objects={f"geoparquet/{DATASET_SLUG}.parquet":{"bytes":len(data),"sha256":_sha(data)}}
    receipts={n:dict(r) for n,(_,r) in sorted(captures.items())}
    manifest={"schema_version":1,"approved":False,"title":"National Transit Map Agencies","description":"BTS National Transit Map Agencies compiled 2026-09-23; historical candidate comparison and release approvals pending.",
        "publisher":"U.S. Department of Transportation, Bureau of Transportation Statistics (USDOT_BTS)","agency":"Bureau of Transportation Statistics","source_url":LAYER_URL,"license":"CC-BY-3.0; post-2023 FTA NTD GTFS inputs described as public domain; rights review pending",
        "source_modified":None,"date_issued":SOURCE_RELEASE,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
        "inputs":[{"input_id":n,"producer":"Bureau of Transportation Statistics (USDOT_BTS)","host":urlsplit(r["url"]).hostname,"role":"producer_publication","canonical_url":ITEM_URL,"retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],"http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],"license":"Creative Commons Attribution 3.0 United States (CC-BY-3.0)","attribution":"Acknowledgment of USDOT and BTS, as stated by the USDOT_BTS ArcGIS item; underlying GTFS source rights require review.","release_date":SOURCE_RELEASE,"item_modified_is_release_date":False} for n,r in receipts.items()],
        "lineage":"USDOT_BTS ArcGIS item ad6b0823f7364cac86c5421834eaba84, exact National Transit Map Agencies layer 0. Description states compiled 2026-09-23. The separately observed portal modified timestamp is not treated as source release date. GCS v1.0.0 is compiled 2025-06-30 and differs (1,246 vs 1,239 records; candidate native formats also omit or alter physical fields); no historical continuation or source approval is assumed.",
        "transformations":[{"operation":"Esri JSON to native GeoParquet candidate","parameters":{"all_source_fields":"preserved in declared order and physical types","source_ids":"OBJECTID retained verbatim; other ID-like fields retained verbatim","geometry":"x,y,z point coordinates retained as WKB in EPSG:4326; no repair or reproject","release_status":"unapproved"}}],
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"One-to-one typed source fields and point coordinates retained; baseline and release approval pending.","objects":[{"path":f"geoparquet/{DATASET_SLUG}.parquet",**source_objects[f"geoparquet/{DATASET_SLUG}.parquet"]}]}}},
        "unsupported_output_formats":{"geopackage":"Historical candidate does not match the current source contract; explicit approved mapping pending.","shapefile_zip":"Historical candidate truncates field names and omits source OBJECTID.","pmtiles":"Display-only format; no publication transformation approved."}}
    validate_release_provenance(manifest)
    audit={"feature_count":EXPECTED_COUNT,"source_id_sha256":EXPECTED_IDS_SHA256,"source_record_sha256":record_sha,"source_geometry_sha256":EXPECTED_GEOMETRY_SHA256,"source_schema_sha256":schema_sha,"source_fields":fields,"source_crs_epsg":4326,"geometry_type":"Point Z","null_geometry_count":0,"item_id":ITEM_ID,"item_owner":OWNER,"source_release":SOURCE_RELEASE,"portal_modified_is_not_release_date":True,
        "historical_baseline_acceptance_pending":True,"source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    root.joinpath("capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,"feature_count":EXPECTED_COUNT,"receipts":receipts})+b"\n")
    state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs={f"geoparquet/{DATASET_SLUG}.parquet":data,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for n,(body,r) in captures.items():
        outputs[f"raw/{n}.body"]=body;outputs[f"raw/{n}.wire"]=root.joinpath(f"{n}.wire").read_bytes();outputs[f"raw/{n}.receipt.json"]=_canonical(dict(r))+b"\n"
    ready={"approved":False,"version":version,"objects":source_objects,"source_record_sha256":record_sha,"historical_baseline_acceptance_pending":True,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"]=_canonical(ready)+b"\n"
    fingerprint=_sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        descriptors={n:{"bytes":len(b),"sha256":_sha(b)} for n,b in outputs.items()}
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,identity,record_sha,schema_sha,descriptors,receipts)
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"reused":True,"candidate_ready":prefix+"/metadata/candidate_ready.json","historical_baseline_acceptance_pending":True,"source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    for n,b in outputs.items(): write_immutable(storage,f"{prefix}/{n}",b)
    descriptors={n:{"bytes":len(b),"sha256":_sha(b)} for n,b in outputs.items()}
    for n,d in descriptors.items():
        snap=storage.object_snapshot(f"{prefix}/{n}")
        if snap is None or snap.size!=d["bytes"] or storage.sha256_key(f"{prefix}/{n}")!=d["sha256"] or storage.object_snapshot(f"{prefix}/{n}")!=snap: raise ValueError("candidate_readback_failed:"+n)
    from dagster_hifld.sources.epa_frs_common import seal_ready_candidate
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,"source_record_sha256":record_sha,"source_schema_sha256":schema_sha,"capture_receipts":receipts,"objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"source_null_geometry_count":0,"candidate_ready":prefix+"/metadata/candidate_ready.json","historical_baseline_acceptance_pending":True,"source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
