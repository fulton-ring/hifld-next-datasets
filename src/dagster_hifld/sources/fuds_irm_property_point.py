"""Faithful, approval-gated capture of the USACE IRM Property Point layer.

The maintained map service does not publish an exact release date. Captures
therefore make a complete unapproved candidate and keep freshness and public
schema acceptance as explicit release gates.
"""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import base64
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
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "interim-risk-management-irm-property-point"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://geospatial.sec.usace.army.mil/server/rest/services/Military/FUDS_Data/MapServer"
LAYER_ID = 6
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "IRM Property Point"
PUBLISHER = "U.S. Army Corps of Engineers (USACE), Military/FUDS_Data MapServer"
PAGE_SIZE = 200
MAX_CAPTURE_BYTES = 32_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    types = {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
             "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeDouble": pa.float64(),
             "esriFieldTypeSingle": pa.float32(), "esriFieldTypeString": pa.string(),
             "esriFieldTypeGUID": pa.string(), "esriFieldTypeGlobalID": pa.string(),
             "esriFieldTypeDate": pa.timestamp("ms", tz="UTC"), "esriFieldTypeBlob": pa.binary()}
    try:
        return types[str(field["type"])]
    except KeyError as exc:
        raise ValueError("unsupported_source_physical_type:" + str(field.get("type"))) from exc


def _validate(captures: dict[str, tuple[bytes, dict[str, object]]]):
    docs = {name: json.loads(body) for name, (body, _) in captures.items()}
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or service.get("serviceItemId") != "df5f8412d041469692ff4846c7131a56" or service.get("mapName") != "FUDS Data" or service.get("copyrightText") != "US Army Corps of Engineers":
        raise ValueError("official_usace_service_identity_drift")
    roster = service.get("layers")
    if not isinstance(roster, list) or not any(x.get("id") == 5 and x.get("name") == "Interim Risk Management (IRM)" and x.get("type") == "Group Layer" for x in roster if isinstance(x, dict)):
        raise ValueError("irm_group_layer_missing")
    if not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME and x.get("type") == "Feature Layer" and x.get("parentLayerId") == 5 for x in roster if isinstance(x, dict)):
        raise ValueError("exact_irm_property_point_layer_missing")
    if not any(x.get("id") == 7 and x.get("name") == "IRM Project Boundary" for x in roster if isinstance(x, dict)) or not any(x.get("id") == 8 and x.get("name") == "IRM Property Boundary" for x in roster if isinstance(x, dict)):
        raise ValueError("irm_boundary_layers_missing")
    if layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPoint":
        raise ValueError("exact_layer_metadata_drift")
    if layer.get("spatialReference", {}).get("latestWkid") != 4326 or layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_crs_or_pagination_capability_drift")
    fields = layer.get("fields")
    if not isinstance(fields, list) or not fields or any(not isinstance(x, dict) for x in fields) or fields[0].get("name") != "OBJECTID" or fields[0].get("type") != "esriFieldTypeOID":
        raise ValueError("source_field_roster_drift")
    plain = [_field(x) for x in fields]
    schema_sha = _sha(_canonical(plain))
    if docs["layer_after"] != layer or docs["service_after"] != service:
        raise ValueError("source_metadata_changed_during_capture")
    count = docs["count_before"].get("count")
    if type(count) is not int or count < 0 or docs["count_after"].get("count") != count:
        raise ValueError("source_count_changed_or_invalid")
    id_rosters = [docs[x].get("objectIds") for x in ("ids_before", "ids_seed", "ids_after")]
    if any(not isinstance(ids, list) or any(type(i) is not int for i in ids) or len(set(ids)) != len(ids) or len(ids) != count for ids in id_rosters):
        raise ValueError("source_id_population_drift")
    ids = sorted(id_rosters[0])
    if any(sorted(other) != ids for other in id_rosters[1:]):
        raise ValueError("source_id_population_changed_during_capture")
    page_names = [f"page_{i:04d}" for i in range((count + PAGE_SIZE - 1) // PAGE_SIZE)]
    if set(captures) != {"service_before", "layer_before", "count_before", "ids_before", "ids_seed", "layer_after", "service_after", "count_after", "ids_after", *page_names}:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    features = []
    attr_names = [f["name"] for f in fields if f["type"] != "esriFieldTypeGeometry"]
    invalid_geometry_count = 0
    for offset, name in enumerate(page_names):
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncated")
        if page.get("spatialReference", {}).get("latestWkid") != 4326:
            raise ValueError("source_page_crs_drift")
        rows = page.get("features")
        expected = ids[offset * PAGE_SIZE:(offset + 1) * PAGE_SIZE]
        if not isinstance(rows, list) or len(rows) != len(expected):
            raise ValueError("source_page_incomplete")
        got = [r.get("attributes", {}).get("OBJECTID") for r in rows]
        if got != expected:
            raise ValueError("source_page_ids_incomplete_or_reordered")
        for row in rows:
            attrs, geom = row.get("attributes"), row.get("geometry")
            if not isinstance(attrs, dict) or list(attrs) != attr_names:
                raise ValueError("source_record_schema_drift")
            if geom is not None:
                valid = isinstance(geom, dict) and all(k in geom for k in ("x", "y")) and not (set(geom) - {"x", "y", "z", "m", "spatialReference"})
                valid = valid and all(type(geom[k]) in (int, float) and math.isfinite(geom[k]) for k in ("x", "y", *[k for k in ("z", "m") if k in geom]))
                valid = valid and -180 <= geom["x"] <= 180 and -90 <= geom["y"] <= 90
                geom_sr = geom.get("spatialReference") if isinstance(geom, dict) else None
                valid = valid and (geom_sr is None or geom_sr.get("latestWkid", geom_sr.get("wkid")) == 4326)
                if not valid:
                    invalid_geometry_count += 1
        features.extend(rows)
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    record_sha = _sha(_canonical(features))
    geom_sha = _sha(_canonical([[row["attributes"]["OBJECTID"], row.get("geometry")] for row in features]))
    audit = {"source_feature_count": count, "source_id_sha256": _sha(_canonical(ids)), "source_record_sha256": record_sha,
             "source_geometry_sha256": geom_sha, "source_schema_sha256": schema_sha, "source_fields": plain,
             "hifld_inventory_id": "DataLumos239309V1",
             "source_spatial_reference": layer.get("extent", {}).get("spatialReference", layer.get("spatialReference")),
             "source_hasZ": layer.get("hasZ"), "source_hasM": layer.get("hasM"),
             "null_geometry_count": sum(row.get("geometry") is None for row in features),
             "invalid_geometry_count": invalid_geometry_count, "source_release_date": None,
             "source_release_date_unknown_reason": "The service does not publish a source release date; modification and retrieval times are not release dates."}
    return layer, fields, features, audit


def _parquet(fields: list[dict[str, object]], features: list[dict[str, object]], spatial_ref: object) -> bytes:
    schema_fields, columns = [], {}
    for field in fields:
        if field["type"] == "esriFieldTypeGeometry":
            continue
        name = field["name"]
        if name in {"geometry", "source_geometry"}:
            raise ValueError("source_field_name_collision")
        typ = _arrow_type(field)
        schema_fields.append(pa.field(name, typ, nullable=field.get("nullable", True)))
        values = [row["attributes"].get(name) for row in features]
        if field["type"] == "esriFieldTypeBlob":
            try:
                values = [None if value is None else base64.b64decode(value, validate=True) for value in values]
            except (TypeError, ValueError) as exc:
                raise ValueError("source_blob_encoding_drift") from exc
        columns[name] = pa.array(values, type=typ)
    has_z = any(row.get("geometry") is not None and "z" in row["geometry"] for row in features)
    has_m = any(row.get("geometry") is not None and "m" in row["geometry"] for row in features)
    vertex_fields = [pa.field(k, pa.float64()) for k in ("x", "y", "z", "m")]
    source_type = pa.struct([pa.field("hasZ", pa.bool_()), pa.field("hasM", pa.bool_()),
                             pa.field("spatialReference", pa.string()), pa.field("point", pa.struct(vertex_fields)),
                             pa.field("raw_json", pa.string())])
    native = []
    projected = []
    for row in features:
        geom = row.get("geometry")
        if geom is None:
            native.append(None); projected.append(None); continue
        valid = isinstance(geom, dict) and all(k in geom for k in ("x", "y")) and not (set(geom) - {"x", "y", "z", "m", "spatialReference"})
        valid = valid and all(type(geom[k]) in (int, float) and math.isfinite(geom[k]) for k in ("x", "y", *[k for k in ("z", "m") if k in geom]))
        valid = valid and -180 <= geom["x"] <= 180 and -90 <= geom["y"] <= 90
        geom_sr = geom.get("spatialReference") if isinstance(geom, dict) else None
        valid = valid and (geom_sr is None or geom_sr.get("latestWkid", geom_sr.get("wkid")) == 4326)
        point = {k: geom[k] for k in ("x", "y", "z", "m") if k in geom} if valid else None
        native.append({"hasZ": isinstance(geom, dict) and "z" in geom, "hasM": isinstance(geom, dict) and "m" in geom,
                       "spatialReference": _canonical(geom.get("spatialReference", spatial_ref) if isinstance(geom, dict) else spatial_ref).decode(),
                       "point": point, "raw_json": _canonical(geom).decode()})
        projected.append((Point(geom["x"], geom["y"], geom.get("z")).wkb if "z" in geom else Point(geom["x"], geom["y"]).wkb) if valid else None)
    columns["source_geometry"] = pa.array(native, type=source_type)
    schema_fields.append(pa.field("source_geometry", source_type, nullable=True))
    columns["geometry"] = pa.array(projected, type=pa.binary()); schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{"encoding":"WKB", "geometry_types":["Point Z" if has_z else "Point"], "crs":pyproj.CRS.from_epsg(4326).to_json_dict(), "edges":"planar"}}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields).with_metadata({b"geo":_canonical(geo)}))
    sink = pa.BufferOutputStream(); pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity():
    return processing_identity(Path(__file__).parents[3])


def capture_fuds_irm_property_point(storage: StagingStorageResource, *, version: str, capture_directory: str | None = None) -> dict[str, object]:
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute(); base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000: raise ValueError("disk_floor_before_acquisition")
    caproot = Path(capture_directory).absolute() if capture_directory else Path("/tmp/hifld-native-reconstruction/125/captures") / f"{version}-{uuid4().hex}"
    replay = capture_directory is not None and (caproot / "service_before.body").is_file()
    if not replay:
        caproot = (caproot / f"{version}-{uuid4().hex}").absolute(); caproot.mkdir(parents=True, exist_ok=False)
    captures = {}
    def get(client, name, url):
        return capture_get(client, url, caproot, name, max_bytes=MAX_CAPTURE_BYTES)
    try:
        if replay:
            for receipt_path in sorted(caproot.glob("*.receipt.json")):
                name=receipt_path.name.removesuffix(".receipt.json")
                receipt=json.loads(receipt_path.read_bytes()); body=(caproot/f"{name}.body").read_bytes()
                if receipt.get("complete_body") is not True or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body):
                    raise ValueError("replayed_capture_receipt_mismatch:"+name)
                captures[name]=(body,receipt)
        else:
          with httpx.Client(follow_redirects=False, timeout=60, headers={"User-Agent":"hifld-next-datasets/irm-property-point-draft"}) as client:
            u = LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})
            seed_body, seed_receipt = get(client, "ids_seed", u); seed = json.loads(seed_body).get("objectIds")
            if not isinstance(seed, list) or any(type(i) is not int for i in seed) or len(set(seed)) != len(seed): raise ValueError("source_seed_id_population_drift")
            captures["ids_seed"] = (seed_body, seed_receipt)
            for name, url in [("service_before", SERVICE_URL+"?f=pjson"), ("layer_before", LAYER_URL+"?f=pjson"),
                ("count_before", LAYER_URL+"/query?"+urlencode({"where":"1=1","returnCountOnly":"true","f":"json"})),
                ("ids_before", u)]: captures[name] = get(client, name, url)
            count = json.loads(captures["count_before"][0])["count"]
            ids = sorted(seed)
            if type(count) is not int or count != len(ids): raise ValueError("source_seed_count_drift")
            for offset in range(0, count, PAGE_SIZE):
                page_ids = ids[offset:offset+PAGE_SIZE]
                params = {"objectIds":",".join(map(str,page_ids)),"outFields":"*","returnGeometry":"true","returnZ":"true","returnM":"true","outSR":"4326","orderByFields":"OBJECTID ASC","f":"json"}
                captures[f"page_{offset//PAGE_SIZE:04d}"] = get(client, f"page_{offset//PAGE_SIZE:04d}", LAYER_URL+"/query?"+urlencode(params))
            for name, url in [("layer_after", LAYER_URL+"?f=pjson"), ("service_after", SERVICE_URL+"?f=pjson"),
                ("count_after", LAYER_URL+"/query?"+urlencode({"where":"1=1","returnCountOnly":"true","f":"json"})), ("ids_after", u)]: captures[name] = get(client,name,url)
        layer, fields, features, audit = _validate(captures)
    except Exception as exc:
        receipts = {n:r for n,(b,r) in captures.items()}
        (caproot/"capture_manifest.json").write_bytes(_canonical({"complete":False,"captured_at":datetime.now(timezone.utc).isoformat(),"error_code":type(exc).__name__,"receipts":receipts})+b"\n")
        if captures: quarantine_incomplete_capture(storage,caproot,DATASET_SLUG,version,captures,type(exc).__name__)
        raise
    identity = _code_identity(); rel = f"geoparquet/{DATASET_SLUG}.parquet"
    body = _parquet(fields, features, audit["source_spatial_reference"])
    source_objects = {rel:{"bytes":len(body),"sha256":_sha(body)}}
    manifest = {"schema_version":1,"approved":False,"title":LAYER_NAME,
        "description":"Official USACE Military/FUDS_Data MapServer layer 6 IRM Property Point in IRM group layer 5. Complete attributes and IDs are retained; native geometry is retained in source_geometry and projected to GeoParquet WKB. Release date, freshness, public schema contract, and rights remain unresolved.",
        "publisher":PUBLISHER,"agency":"U.S. Army Corps of Engineers","source_url":LAYER_URL,"license":None,"source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
        "inputs":[{"input_id":n,"producer":PUBLISHER,"host":urlsplit(r["url"]).hostname,"role":"producer_publication","canonical_url":SERVICE_URL if n.startswith("service_") else LAYER_URL,"retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],"http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],"license":"unknown_pending_rights_review","attribution":"USACE MapServer copyrightText is US Army Corps of Engineers; formal downstream attribution review remains pending.","release_date":None,"release_date_unknown_reason":audit["source_release_date_unknown_reason"]} for n,(b,r) in sorted(captures.items())],
        "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"layer_id":LAYER_ID,"source_fields":"all source attributes retained in producer order and declared physical types","source_ids":"source OBJECTID and all values preserved","geometry":"all returned point ordinates and per-feature native Esri geometry/SRS preserved in source_geometry; GeoParquet geometry is standards-compliant XY/XYZ WKB, without coordinate transform or repair","candidate_status":"unapproved; freshness, schema/ID contract, rights and release approval pending"}}],
        "lineage":"USACE Military/FUDS_Data MapServer layer 6, IRM Property Point, child of group layer 5 Interim Risk Management (IRM). Layers 7 IRM Project Boundary and 8 IRM Property Boundary are distinct source products.",
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Faithful complete source capture; approvals unresolved.","objects":[{"path":rel,**source_objects[rel]}]}}},"unsupported_output_formats":{"geopackage":"No accepted public schema mapping.","pmtiles":"No approved display/generalization mapping."}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    version = content_derived_version(audit["source_record_sha256"],audit["source_geometry_sha256"],audit["source_schema_sha256"],identity)
    prefix = candidate_prefix(DATASET_SLUG,FILE_SLUG,version); state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs={rel:body,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n","metadata/candidate_ready.json":_canonical({"approved":False,"version":version,"objects":source_objects,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"release_accepted":False})+b"\n"}
    for name,(raw,receipt) in captures.items():
        outputs[f"raw/{name}.body"]=raw; outputs[f"raw/{name}.wire"]=(caproot/f"{name}.wire").read_bytes(); outputs[f"raw/{name}.receipt.json"]=_canonical(receipt)+b"\n"
    (caproot/"capture_manifest.json").write_bytes(_canonical({"complete":True,"source_feature_count":audit["source_feature_count"],"receipts":{n:r for n,(b,r) in sorted(captures.items())}})+b"\n")
    fingerprint=_sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle=json.loads(load_first_construction(storage,state_key,version,fingerprint)); payload=bundle.get("payload")
        if not isinstance(payload,dict): raise ValueError("cached_first_candidate_invalid")
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,identity,audit["source_record_sha256"],audit["source_schema_sha256"],payload.get("objects",{}),payload.get("capture_receipts",{}))
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["source_feature_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,"release_accepted":False}
    for name,value in outputs.items(): write_immutable(storage,f"{prefix}/{name}",value)
    descriptors={n:{"bytes":len(b),"sha256":_sha(b)} for n,b in outputs.items()}
    for name,desc in descriptors.items():
        key=f"{prefix}/{name}"; snap=storage.object_snapshot(key)
        if snap is None or snap.size!=desc["bytes"] or storage.sha256_key(key)!=desc["sha256"] or storage.object_snapshot(key)!=snap: raise ValueError("candidate_output_readback_failed:"+name)
    payload={"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":{n:r for n,(b,r) in captures.items()},"objects":descriptors}
    seal_ready_candidate(storage,state_key,version,fingerprint,payload)
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["source_feature_count"],"source_null_geometry_count":audit["null_geometry_count"],"source_invalid_geometry_count":audit["invalid_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json","release_accepted":False,"capture_directory":str(caproot)}
