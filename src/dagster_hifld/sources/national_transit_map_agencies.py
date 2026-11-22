"""Lossless draft capture of the official BTS National Transit Map Agencies layer.

Exact-ID JSON slices retain the producer fields and native XYZ geometry.
Compilation statements are observations, not immutable editions or schedules.
Source, rights, historical equivalence and release acceptance remain false.
"""
from __future__ import annotations

import json
import re
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

from dagster_hifld.http_capture import CaptureDocuments
from dagster_hifld.capture_completeness import validate_capture_witness
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.http_capture import validate_capture_roster
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.sources.arcgis_capture import capture_layer, RetainedCaptureView
from dagster_hifld.sources.arcgis_layer import layer_capture_roster, load_replay
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    validate_capture_request_identity,
    quarantine_incomplete_capture, verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "national-transit-map-agencies"
FILE_SLUG = DATASET_SLUG
ITEM_ID = "ad6b0823f7364cac86c5421834eaba84"
ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}"
OWNER = "USDOT_BTS"
SERVICE_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_National_Transit_Map_Agencies/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
EXPECTED_SCHEMA_SHA256 = "a9f1aff709a051aaa6809b575c9f4c8c803a1e9c42f0dc135a0566220be888bc"
PAGE_SIZE = 250
MAX_BODY_BYTES = 16_000_000
MAX_CAPTURE_BYTES = 256_000_000
MAX_CAPTURE_RESPONSES = 1024
MAX_RECEIPT_BYTES = 64_000
MAX_MANIFEST_BYTES = 4_000_000
METADATA_NAMES = {"ids_seed", "item", "service_before", "layer_before", "count_before",
                  "ids_before", "layer_after", "service_after", "count_after", "ids_after"}
LICENSE = "Creative Commons Attribution 3.0 United States (CC-BY-3.0)"


def _capture_roster(seed: list[int]) -> list[tuple[str, str]]:
    urls = layer_capture_roster(
        seed, layer_url=LAYER_URL, service_url=SERVICE_URL, page_size=PAGE_SIZE,
        extra_documents=(("item", ITEM_URL + "?f=json"),),
        page_params={"returnZ": "true", "returnM": "true", "outSR": "4326"},
        after_order=("layer_after", "service_after", "count_after", "ids_after"),
    )
    return [("ids_seed", dict(urls)["ids_before"]), *urls]


def _validate_seed(seed: object) -> list[int]:
    if not isinstance(seed, list) or not seed:
        raise ValueError("source_seed_population_drift")
    validate_capture_witness(len(seed), seed, len(seed), seed)
    if len(METADATA_NAMES) + (len(seed) + PAGE_SIZE - 1) // PAGE_SIZE > MAX_CAPTURE_RESPONSES:
        raise ValueError("source_capture_request_limit")
    return seed


def _admit_capture_directory(root: Path) -> int:
    if (".." in root.parts or any(path.is_symlink() for path in (root, *root.parents))
            or not root.is_dir()):
        raise ValueError("unsafe_or_missing_retained_capture")
    names = set()
    for path in root.iterdir():
        names.add(path.name)
        if len(names) > 3 * MAX_CAPTURE_RESPONSES + 1:
            raise ValueError("source_capture_request_limit")
        if path.name == "capture_manifest.json":
            continue
        match = re.fullmatch(r"([a-z][a-z0-9_]*)\.(body|wire|receipt\.json)", path.name)
        if (match is None or match[1] not in METADATA_NAMES
                and re.fullmatch(r"page_[0-9]{3,}", match[1]) is None):
            raise ValueError("retained_capture_artifact_unexpected")
    validate_capture_roster(root, names)
    total = 0
    for name in names:
        size = (root / name).lstat().st_size
        cap = (MAX_MANIFEST_BYTES if name == "capture_manifest.json" else
               MAX_RECEIPT_BYTES if name.endswith(".receipt.json") else MAX_BODY_BYTES)
        total += size
        if size > cap or total > MAX_CAPTURE_BYTES:
            raise ValueError("source_capture_budget_exceeded")
    return total


def _fetch(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    retained = _admit_capture_directory(root)
    cap = min(MAX_BODY_BYTES, (MAX_CAPTURE_BYTES - retained - MAX_RECEIPT_BYTES) // 2)
    if cap <= 0:
        raise ValueError("source_capture_budget_exceeded")
    if shutil.disk_usage(root).free < 256_000_000 + 2 * cap + MAX_RECEIPT_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    return capture_get(client, url, root, name, max_bytes=cap, max_wire_block_bytes=cap)


def _compilation_date(item: Mapping[str, object]) -> str | None:
    match = re.search(r"\bcompiled on ([A-Za-z]+ [0-9]{1,2}, [0-9]{4})",
                      str(item.get("description", "")), re.IGNORECASE)
    if match is None:
        return None
    try:
        return datetime.strptime(match[1], "%B %d, %Y").date().isoformat()
    except ValueError:
        return None

def _field_profile(fields: object) -> tuple[list[dict[str, object]], str]:
    if not isinstance(fields, list) or any(not isinstance(f, dict) for f in fields):
        raise ValueError("source_field_roster_missing")
    profile = [{k:f[k] for k in ("name","type","alias","length","nullable","domain") if k in f} for f in fields]
    return profile, _sha(_canonical(profile))


def validate_captures(captures: Mapping[str, tuple[bytes, Mapping[str, object]]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    docs = CaptureDocuments(captures)
    if "ids_seed" not in docs:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    seed = _validate_seed(docs["ids_seed"].get("objectIds"))
    expected_urls = dict(_capture_roster(seed))
    if set(docs) != set(expected_urls):
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    item, service, layer = docs["item"], docs["service_before"], docs["layer_before"]
    if item.get("id") != ITEM_ID or item.get("owner") != OWNER or item.get("access") != "public" or item.get("url") != SERVICE_URL:
        raise ValueError("official_BTS_item_identity_drift")
    if (item.get("orgId") != "xOi1kZaI0eWDREZv"
            or item.get("title") != "National Transit Map Agencies"
            or item.get("type") != "Feature Service"):
        raise ValueError("official_BTS_item_identity_drift")
    if item.get("licenseInfo") != LICENSE or layer.get("copyrightText") != LICENSE:
        raise ValueError("official_BTS_license_drift")
    for name, (_body, receipt) in captures.items():
        validate_capture_request_identity(name, receipt, expected_urls[name])
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
    ids = validate_capture_witness(
        docs["count_before"].get("count"), docs["ids_before"].get("objectIds"),
        docs["count_after"].get("count"), docs["ids_after"].get("objectIds"))
    if sorted(seed) != ids:
        raise ValueError("source_population_drift")
    features=[]
    page_names = [name for name in expected_urls if name.startswith("page_")]
    for index, name in enumerate(page_names):
        page=docs[name]
        if page.get("error") or page.get("exceededTransferLimit") is True or page.get("spatialReference",{}).get("latestWkid") != 4326:
            raise ValueError("source_page_error_or_truncation")
        rows=page.get("features")
        if not isinstance(rows,list): raise ValueError("source_page_missing_features")
        expected_ids = sorted(seed[index * PAGE_SIZE:(index + 1) * PAGE_SIZE])
        actual_ids = [row.get("attributes", {}).get("OBJECTID") for row in rows
                      if isinstance(row, dict)]
        if (any(type(oid) is not int for oid in actual_ids) or actual_ids != expected_ids):
            raise ValueError("source_page_id_slice_mismatch")
        features.extend(rows)
    names={f["name"] for f in fields}
    if len(features)!=len(ids) or sorted(f.get("attributes",{}).get("OBJECTID") for f in features)!=ids:
        raise ValueError("source_page_population_incomplete")
    for f in features:
        if set(f.get("attributes",{}))!=names or not isinstance(f.get("geometry"),dict) or set(f["geometry"])!={"x","y","z"}:
            raise ValueError("source_record_or_geometry_schema_drift")
    features.sort(key=lambda f:f["attributes"]["OBJECTID"])
    return fields, features


def _load_retained_capture(directory: Path, destination: Path) -> dict[str, tuple[bytes, dict[str, object]]]:
    """Replay the native complete manifest after bounded artifact admission."""
    root = Path(directory).absolute()
    _admit_capture_directory(root)
    manifest = json.loads((root / "capture_manifest.json").read_bytes())
    if not isinstance(manifest, dict):
        raise ValueError("retained_capture_manifest_missing_or_invalid")
    source_count = manifest.get("source_feature_count", manifest.get("feature_count"))
    receipts = manifest.get("receipts")
    if (manifest.get("complete") is not True or type(source_count) is not int or source_count <= 0
            or not isinstance(receipts, dict) or not METADATA_NAMES <= set(receipts)
            or len(receipts) > MAX_CAPTURE_RESPONSES
            or any(not isinstance(name, str) or name not in METADATA_NAMES
                   and re.fullmatch(r"page_[0-9]{3,}", name) is None for name in receipts)):
        raise ValueError("retained_capture_is_not_complete_population")
    expected = {"capture_manifest.json"} | {
        f"{name}.{suffix}" for name in receipts for suffix in ("body", "wire", "receipt.json")}
    validate_capture_roster(root, expected)

    def load(name: str, url: str) -> tuple[bytes, dict[str, object]]:
        body, receipt = load_replay(root, name)
        if (receipt != receipts[name]
                or (root / f"{name}.receipt.json").read_bytes() != _canonical(receipt) + b"\n"):
            raise ValueError("retained_capture_integrity_failed:" + name)
        validate_capture_request_identity(name, receipt, url)
        return body, receipt

    seed_capture = load("ids_seed", dict(_capture_roster([]))["ids_seed"])
    seed = _validate_seed(json.loads(seed_capture[0]).get("objectIds"))
    urls = dict(_capture_roster(seed))
    if set(receipts) != set(urls) or len(seed) != source_count:
        raise ValueError("retained_capture_receipt_roster_changed")
    captures = {"ids_seed": seed_capture}
    for name, url in urls.items():
        if name != "ids_seed":
            captures[name] = load(name, url)
    validate_captures(captures)
    for name in captures:
        for suffix in ("body", "wire", "receipt.json"):
            shutil.copyfile(root / f"{name}.{suffix}", destination / f"{name}.{suffix}")
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
    root=(Path(capture_directory) if capture_directory else default_capture_dir("hifld-native-reconstruction", "167", "captures"))/f"{version}-{uuid4().hex}"
    root = root.absolute()
    if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True,exist_ok=False)
    captures = (_load_retained_capture(Path(replay_capture_directory), root)
                if replay_capture_directory else {})
    try:
        if not replay_capture_directory:
            with httpx.Client(follow_redirects=False, timeout=60, headers={
                    "User-Agent": "hifld-next-datasets/national-transit-map-agencies-draft"}) as client:
                captures = RetainedCaptureView(root, captures)
                capture_layer(LAYER_URL, root, MAX_CAPTURE_BYTES, PAGE_SIZE, client=client,
                    captures=captures, seed_name="ids_seed", seed_url=dict(_capture_roster([]))["ids_seed"],
                    validate_seed=lambda body: _validate_seed(json.loads(body).get("objectIds")),
                    roster=_capture_roster, fetch=_fetch, on_validation_error=validate_captures)
        fields,features=validate_captures(captures)
    except Exception as exc:
        _admit_capture_directory(root)
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
    feature_count=len(features)
    source_ids=sorted(f["attributes"]["OBJECTID"] for f in features)
    ids_sha=_sha(_canonical(source_ids))
    record_sha=_sha(_canonical(features))
    geometry_sha=_sha(_canonical([[f["attributes"]["OBJECTID"],f["geometry"]["x"],f["geometry"]["y"],f["geometry"]["z"]] for f in features]))
    data=_parquet(fields,features); schema_sha=EXPECTED_SCHEMA_SHA256
    version=content_derived_version(record_sha,geometry_sha,schema_sha,identity)
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    source_objects={f"geoparquet/{DATASET_SLUG}.parquet":{"bytes":len(data),"sha256":_sha(data)}}
    receipts={n:dict(r) for n,(_,r) in ((name, captures[name]) for name in sorted(captures))}
    item = json.loads(captures["item"][0])
    compilation_date = _compilation_date(item)
    attribution = str(item.get("accessInformation") or
                      "Acknowledgment of USDOT and BTS; constituent credits require review.")
    manifest={"schema_version":1,"approved":False,"title":"National Transit Map Agencies","description":"Current BTS National Transit Map Agencies; compilation observations, historical comparison and release acceptance remain separate.",
        "publisher":"U.S. Department of Transportation, Bureau of Transportation Statistics (USDOT_BTS)","agency":"Bureau of Transportation Statistics","source_url":LAYER_URL,"license":"CC-BY-3.0; post-2023 FTA NTD GTFS inputs described as public domain; rights review pending",
        "source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
        "inputs":[{"input_id":n,"producer":"Bureau of Transportation Statistics (USDOT_BTS)","host":urlsplit(r["url"]).hostname,"role":"producer_publication","canonical_url":ITEM_URL,"retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],"http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],"license":"Creative Commons Attribution 3.0 United States (CC-BY-3.0)","attribution":attribution,"release_date":None,"release_date_unknown_reason":"A producer compilation statement is not a guaranteed immutable edition or publication date.","item_modified_is_release_date":False} for n,r in receipts.items()],
        "lineage":"USDOT_BTS ArcGIS item ad6b0823f7364cac86c5421834eaba84, exact National Transit Map Agencies layer 0. The compilation statement is recorded from the retained item and is not an immutable edition or refresh schedule. Portal modification is not a source release date. Historical HIFLD equivalence and source approval are not assumed.",
        "transformations":[{"operation":"Esri JSON to native GeoParquet candidate","parameters":{"all_source_fields":"preserved in declared order and physical types","source_ids":"OBJECTID retained verbatim; other ID-like fields retained verbatim","geometry":"x,y,z point coordinates retained as WKB in EPSG:4326; no repair or reproject","release_status":"unapproved"}}],
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"One-to-one typed source fields and point coordinates retained; baseline and release approval pending.","objects":[{"path":f"geoparquet/{DATASET_SLUG}.parquet",**source_objects[f"geoparquet/{DATASET_SLUG}.parquet"]}]}}},
        "unsupported_output_formats":{"geopackage":"Historical candidate does not match the current source contract; explicit approved mapping pending.","shapefile_zip":"Historical candidate truncates field names and omits source OBJECTID.","pmtiles":"Display-only format; no publication transformation approved."}}
    validate_release_provenance(manifest)
    audit={"feature_count":feature_count,"source_reported_count":feature_count,"source_population_complete":True,"source_id_sha256":ids_sha,"source_record_sha256":record_sha,"source_geometry_sha256":geometry_sha,"source_schema_sha256":schema_sha,"source_fields":fields,"source_crs_epsg":4326,"geometry_type":"Point Z","null_geometry_count":0,"item_id":ITEM_ID,"item_owner":OWNER,"producer_stated_compilation_date":compilation_date,"producer_compilation_description":item.get("description"),"publisher_attribution":attribution,"portal_modified_is_not_release_date":True,
        "historical_baseline_acceptance_pending":True,"source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    capture_manifest = _canonical({"captured_at":datetime.now(timezone.utc).isoformat(),
        "complete":True,"feature_count":feature_count,"receipts":receipts}) + b"\n"
    if (len(capture_manifest) > MAX_MANIFEST_BYTES
            or _admit_capture_directory(root) + len(capture_manifest) > MAX_CAPTURE_BYTES):
        raise ValueError("source_capture_budget_exceeded")
    root.joinpath("capture_manifest.json").write_bytes(capture_manifest)
    state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs={f"geoparquet/{DATASET_SLUG}.parquet":data,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for n,(body,r) in captures.items():
        outputs[f"raw/{n}.body"]=body;outputs[f"raw/{n}.wire"]=root.joinpath(f"{n}.wire").read_bytes();outputs[f"raw/{n}.receipt.json"]=_canonical(dict(r))+b"\n"
    ready={"approved":False,"version":version,"objects":source_objects,"source_record_sha256":record_sha,"historical_baseline_acceptance_pending":True,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"]=_canonical(ready)+b"\n"
    fingerprint=_sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if (not isinstance(payload, dict) or payload.get("processing_identity") != identity
                or payload.get("source_record_sha256") != record_sha
                or payload.get("source_schema_sha256") != schema_sha):
            raise ValueError("cached_first_candidate_identity_or_source_changed")
        descriptors = payload["objects"]
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,
                                fingerprint,identity,record_sha,schema_sha,descriptors,
                                payload["capture_receipts"])
        if descriptors.get(f"geoparquet/{DATASET_SLUG}.parquet") != {
                "bytes":len(data), "sha256":_sha(data)}:
            raise ValueError("cached_native_candidate_changed")
        original_audit = json.loads(storage.read_key(prefix + "/metadata/source_audit.json"))
        compilation_date = original_audit.get("producer_stated_compilation_date")
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":feature_count,"source_reported_count":feature_count,"source_population_complete":True,"source_id_sha256":ids_sha,"source_null_geometry_count":0,"producer_stated_compilation_date":compilation_date,"reused":True,"candidate_ready":prefix+"/metadata/candidate_ready.json","historical_baseline_acceptance_pending":True,"source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    for n,b in outputs.items(): write_immutable(storage,f"{prefix}/{n}",b)
    descriptors={n:{"bytes":len(b),"sha256":_sha(b)} for n,b in outputs.items()}
    for n,d in descriptors.items():
        snap=storage.object_snapshot(f"{prefix}/{n}")
        if snap is None or snap.size!=d["bytes"] or storage.sha256_key(f"{prefix}/{n}")!=d["sha256"] or storage.object_snapshot(f"{prefix}/{n}")!=snap: raise ValueError("candidate_readback_failed:"+n)
    from dagster_hifld.sources.capture_common import seal_ready_candidate
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,"source_record_sha256":record_sha,"source_schema_sha256":schema_sha,"capture_receipts":receipts,"objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":feature_count,"source_reported_count":feature_count,"source_population_complete":True,"source_id_sha256":ids_sha,"source_null_geometry_count":0,"producer_stated_compilation_date":compilation_date,"candidate_ready":prefix+"/metadata/candidate_ready.json","historical_baseline_acceptance_pending":True,"source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
