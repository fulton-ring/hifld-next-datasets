"""Draft capture for FHWA's National Tunnel Inventory (NTI) ArcGIS layer.

The BTS-hosted item identifies FHWA as the source. Its own description has a
conflicting compilation/publication chronology, so this extractor creates an
unapproved native candidate and keeps every release gate closed.
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
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

ITEM_ID = "77879ddb06eb49d4a4a57298177bcd87"
SERVICE_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_National_Tunnel_Inventory/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "National Tunnel Inventory"
DATASET_SLUG = "inventorynational-tunnel-inventory"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "Federal Highway Administration (FHWA); distributed by USDOT Bureau of Transportation Statistics (BTS) NTAD"
EDITION_NOTE = "The source item says compiled September 02, 2025 but published August 26, 2025; edition chronology and release acceptance require FHWA confirmation. ArcGIS item modified time is not a release date."
EXPECTED_COUNT = 580
EXPECTED_ID_SHA256 = "43840e76f01c40e59bff2beebd44da00d0370fcc8a9dde197067985ae848c4c8"
EXPECTED_RECORD_SHA256 = "2f7a0b1fe698b1528ab0af87552951c033834b3cf861f6e35571b02a4afc6564"
EXPECTED_GEOMETRY_SHA256 = "645fc954ba27e4607932c0d574a7586e0ca71c458648ac5471649bbe8708b9a7"
EXPECTED_SCHEMA_SHA256 = "faa10e3aa64029d3f69acf211d88c1d692b29edeecf6255c750681b458508d9d"
MAX_BODY_BYTES = 100_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    # Preserve every producer-declared field property, including SQL type,
    # editability, precision, and defaults, in the source-schema fingerprint.
    return dict(field)


def _capture_urls() -> dict[str, str]:
    return {
        "portal_item": f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json",
        "service_before": SERVICE_URL + "?f=pjson",
        "layer_before": LAYER_URL + "?f=pjson",
        "count_before": LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnCountOnly": "true", "f": "json"}),
        "ids_before": LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
        # Pagination avoids oversized query strings while preserving a complete
        # OBJECTID-ordered population in the source's declared spatial reference.
        "page_000": LAYER_URL + "/query?" + urlencode({
            "where": "1=1", "outFields": "*", "returnGeometry": "true",
            "returnZ": "true", "returnM": "true", "outSR": "4326",
            "orderByFields": "OBJECTID ASC", "resultOffset": "0",
            "resultRecordCount": "2000", "f": "json",
        }),
        "layer_after": LAYER_URL + "?f=pjson",
        "service_after": SERVICE_URL + "?f=pjson",
        "count_after": LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnCountOnly": "true", "f": "json"}),
        "ids_after": LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
    }


def _validate(captures: Mapping[str, tuple[bytes, Mapping[str, object]]]) -> tuple[dict, list[dict], dict]:
    names = {"portal_item", "service_before", "layer_before", "count_before", "ids_before",
             "page_000", "layer_after", "service_after", "count_after", "ids_after"}
    if set(captures) != names:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    docs = {name: json.loads(body) for name, (body, _) in captures.items()}
    portal, service, layer = docs["portal_item"], docs["service_before"], docs["layer_before"]
    if (portal.get("id") != ITEM_ID or portal.get("owner") != "USDOT_BTS"
            or portal.get("title") != "National Tunnel Inventory"
            or portal.get("url") != SERVICE_URL or portal.get("access") != "public"):
        raise ValueError("official_BTS_item_identity_or_publication_drift")
    description = str(service.get("description", ""))
    if (service.get("serviceItemId") != ITEM_ID or service.get("isView") is not True
            or not any(isinstance(row, dict) and row.get("id") == LAYER_ID and row.get("name") == LAYER_NAME
                       and row.get("geometryType") == "esriGeometryPoint" for row in service.get("layers", []))):
        raise ValueError("official_NTAD_service_or_FHWA_lineage_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("objectIdField") != "OBJECTID"
            or layer.get("hasZ") is not False or layer.get("hasM") is not False
            or layer.get("extent", {}).get("spatialReference", {}).get("latestWkid") != 4326):
        raise ValueError("source_layer_geometry_or_identity_drift")
    if not all(phrase in description for phrase in ("Federal Highway Administration (FHWA)",
                 "compiled on September 02, 2025", "published on August 26, 2025")):
        raise ValueError("source_lineage_or_edition_evidence_drift")
    fields_raw = layer.get("fields")
    if not isinstance(fields_raw, list) or not all(isinstance(f, dict) for f in fields_raw):
        raise ValueError("source_field_roster_drift")
    fields = [_plain_field(f) for f in fields_raw]
    schema_sha = _sha(_canonical(fields))
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_drift")
    if docs["layer_after"] != layer or docs["service_after"] != service:
        raise ValueError("source_metadata_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    ids_before = docs["ids_before"].get("objectIds")
    ids_after = docs["ids_after"].get("objectIds")
    if (not isinstance(ids_before, list) or sorted(ids_before) != sorted(ids_after or [])
            or len(ids_before) != EXPECTED_COUNT or len(set(ids_before)) != EXPECTED_COUNT
            or docs["ids_before"].get("objectIdFieldName") != "OBJECTID"
            or _sha(_canonical(sorted(ids_before))) != EXPECTED_ID_SHA256):
        raise ValueError("source_id_population_drift")
    page = docs["page_000"]
    if "error" in page or page.get("exceededTransferLimit") is True:
        raise ValueError("source_page_error_or_truncated")
    if (page.get("spatialReference", {}).get("latestWkid") != 4326
            or page.get("geometryType") != "esriGeometryPoint"):
        raise ValueError("source_page_crs_or_geometry_drift")
    features = page.get("features")
    if not isinstance(features, list) or len(features) != EXPECTED_COUNT:
        raise ValueError("source_page_population_incomplete")
    expected_names = [f["name"] for f in fields]
    got_ids = []
    geometry_rows = []
    for feature in features:
        attrs, geom = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or list(attrs) != expected_names:
            raise ValueError("source_record_field_roster_drift")
        oid = attrs.get("OBJECTID")
        if not isinstance(oid, int) or isinstance(oid, bool):
            raise ValueError("source_objectid_physical_type_drift")
        if not isinstance(geom, dict) or set(geom) != {"x", "y"}:
            raise ValueError("source_point_geometry_drift")
        if any(not isinstance(geom[k], (int, float)) or isinstance(geom[k], bool) for k in ("x", "y")):
            raise ValueError("source_coordinate_physical_type_drift")
        got_ids.append(oid)
        geometry_rows.append([oid, geom["x"], geom["y"]])
        for field in fields:
            _validate_value_type(attrs[field["name"]], field)
    if sorted(got_ids) != sorted(ids_before) or len(set(got_ids)) != EXPECTED_COUNT:
        raise ValueError("source_page_id_population_incomplete")
    features = sorted(features, key=lambda f: f["attributes"]["OBJECTID"])
    record_sha, geometry_sha = _sha(_canonical(features)), _sha(_canonical(sorted(geometry_rows)))
    if record_sha != EXPECTED_RECORD_SHA256 or geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_value_or_geometry_baseline_drift")
    audit = {
        "source_feature_count": EXPECTED_COUNT, "source_id_sha256": EXPECTED_ID_SHA256,
        "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": schema_sha, "source_crs_epsg": 4326,
        "geometry_type": "Point", "null_geometry_count": 0,
        "unique_source_id_count": EXPECTED_COUNT, "source_fields": fields,
        "arcgis_item_id": ITEM_ID, "portal_owner": portal["owner"],
        "portal_modified_epoch_ms": portal.get("modified"),
        "portal_modified_is_source_release_date": False,
        "source_edition_note": EDITION_NOTE,
    }
    return layer, fields, {"features": features, "audit": audit}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    kind = field["type"]
    if kind == "esriFieldTypeOID": return pa.int64()
    if kind == "esriFieldTypeInteger": return pa.int32()
    if kind == "esriFieldTypeSmallInteger": return pa.int16()
    if kind == "esriFieldTypeDouble": return pa.float64()
    if kind == "esriFieldTypeSingle": return pa.float32()
    if kind == "esriFieldTypeString": return pa.string()
    if kind == "esriFieldTypeDate": return pa.timestamp("ms", tz="UTC")
    raise ValueError("unsupported_source_physical_type:" + str(kind))


def _validate_value_type(value: object, field: Mapping[str, object]) -> None:
    if value is None:
        if field.get("nullable") is False:
            raise ValueError("unexpected_null_source_value:" + str(field["name"]))
        return
    kind = field["type"]
    integer_types = {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"}
    if kind in integer_types and (not isinstance(value, int) or isinstance(value, bool)):
        raise ValueError("source_integer_physical_type_drift:" + str(field["name"]))
    if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"} and (
            not isinstance(value, (int, float)) or isinstance(value, bool)):
        raise ValueError("source_float_physical_type_drift:" + str(field["name"]))
    if kind == "esriFieldTypeString" and not isinstance(value, str):
        raise ValueError("source_string_physical_type_drift:" + str(field["name"]))
    if kind == "esriFieldTypeDate" and (not isinstance(value, int) or isinstance(value, bool)):
        raise ValueError("source_date_epoch_milliseconds_drift:" + str(field["name"]))


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    schema_fields = [pa.field(str(f["name"]), _arrow_type(f), nullable=f.get("nullable", True)) for f in fields]
    arrays = [pa.array([row["attributes"][f["name"]] for row in features], type=_arrow_type(f)) for f in fields]
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=False))
    arrays.append(pa.array([Point(row["geometry"]["x"], row["geometry"]["y"]).wkb for row in features], type=pa.binary()))
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{
        "encoding":"WKB", "geometry_types":["Point"], "crs":pyproj.CRS.from_epsg(4326).to_json_dict(), "edges":"spherical",
    }}}
    table = pa.Table.from_arrays(arrays, schema=pa.schema(schema_fields, metadata={b"geo":_canonical(geo)}))
    sink = pa.BufferOutputStream(); pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def capture_fhwa_tunnel_inventory(storage: StagingStorageResource, *, version: str,
                                  capture_directory: str | None = None) -> dict[str, object]:
    """Capture all exact source records and stage an unapproved candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000:
        raise ValueError("disk_floor_before_acquisition")
    capture_root = (Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/170/captures")) / f"{version}-{uuid4().hex}"
    capture_root = capture_root.absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=90,
                          headers={"User-Agent":"hifld-next-datasets/fhwa-nti-draft", "Accept-Encoding":"identity"}) as client:
            for name, url in _capture_urls().items():
                body, receipt = capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES)
                captures[name] = (body, receipt)
        layer, fields, extracted = _validate(captures)
    except Exception as exc:
        failed: dict[str, tuple[bytes, dict[str, object]]] = dict(captures)
        for receipt_path in sorted(capture_root.glob("*.receipt.json")):
            name=receipt_path.name.removesuffix(".receipt.json")
            try:
                receipt=json.loads(receipt_path.read_bytes())
                body=(capture_root/f"{name}.body").read_bytes()
                (capture_root/f"{name}.wire").read_bytes()
                failed.setdefault(name,(body,receipt))
            except (OSError,ValueError):
                continue
        if failed:
            quarantine_incomplete_capture(storage,capture_root,DATASET_SLUG,version,failed,type(exc).__name__)
        raise
    identity = _code_identity()
    parquet_body = _parquet_bytes(fields, extracted["features"])
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name:{"bytes":len(parquet_body),"sha256":_sha(parquet_body)}}
    manifest = {
        "schema_version":1, "approved":False,
        "title":LAYER_NAME,
        "description":"FHWA National Tunnel Inventory distributed through USDOT/BTS NTAD. Source item contains an unresolved compiled-versus-published date conflict; candidate is not release-approved.",
        "publisher":PUBLISHER, "agency":"Federal Highway Administration", "source_url":LAYER_URL,
        "license":"https://www.usa.gov/publicdomain/label/1.0/", "source_modified":None,
        "date_issued":None, "date_modified":None, "processor":"HIFLD Next", "code_commit":identity["code_commit"],
        "inputs":[{"input_id":name,"producer":PUBLISHER,"host":urlsplit(receipt["url"]).hostname,
            "role":"producer_publication","canonical_url":SERVICE_URL,"retrieval_url":receipt["url"],
            "observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],
            "http_status":receipt["status"],"bytes":receipt["bytes"],"sha256":receipt["sha256"],
            "license":"https://www.usa.gov/publicdomain/label/1.0/",
            "attribution":"ArcGIS item attributes the data to FHWA and BTS distribution; rights review is pending.",
            "release_date":None,"release_date_unknown_reason":EDITION_NOTE} for name,(_,receipt) in sorted(captures.items())],
        "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{
            "feature_layer_id":LAYER_ID,"source_fields":"all 69 fields in producer order with declared physical types",
            "source_ids":"Preserve every OBJECTID value exactly; no replacement IDs",
            "geometry":"Esri point x/y encoded as WKB in declared EPSG:4326; no reprojection or repair",
            "date_fields":"Esri epoch milliseconds represented as UTC timestamp[ms]",
            "candidate_status":"unapproved; FHWA source edition, schema/ID contract, rights, and public-field mapping acceptance remain pending"}}],
        "lineage":"Exact public NTAD ArcGIS item 77879ddb06eb49d4a4a57298177bcd87 is owned by USDOT_BTS, identifies FHWA as source, and serves the National Tunnel Inventory point layer. The description links the annual FHWA Specifications for the National Tunnel Inventory (https://doi.org/10.21949/1519104) and data dictionary (https://doi.org/10.21949/1529051). The compilation/publication dates conflict; portal modified time is not a source release date.",
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native Esri values, every source attribute and OBJECTID, and source point geometry retained; approval gates remain closed.","objects":[{"path":parquet_name,**source_objects[parquet_name]}]}}},
        "unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit={**extracted["audit"],"official_layer_name":LAYER_NAME,"service_is_view":True,
           "freshness_accepted":False,"source_release_accepted":False,"schema_contract_accepted":False,
           "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    version=content_derived_version(audit["source_record_sha256"],audit["source_geometry_sha256"],audit["source_schema_sha256"],identity)
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    (capture_root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,
        "source_feature_count":EXPECTED_COUNT,"source_record_sha256":audit["source_record_sha256"],
        "source_geometry_sha256":audit["source_geometry_sha256"],"receipts":{n:r for n,(_,r) in sorted(captures.items())}})+b"\n")
    state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs={parquet_name:parquet_body,"metadata/source_manifest.json":_canonical(manifest)+b"\n",
        "metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"]=body
        outputs[f"raw/{name}.wire"]=(capture_root/f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"]=_canonical(receipt)+b"\n"
    outputs["metadata/candidate_ready.json"]=_canonical({"approved":False,"version":version,"objects":source_objects,
        "source_record_sha256":audit["source_record_sha256"],"source_release_accepted":False,
        "freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "rights_accepted":False,"mapping_approved":False,"release_accepted":False})+b"\n"
    fingerprint=_sha(_canonical(identity))
    descriptors={name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    if storage.object_snapshot(state_key) is not None:
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,identity,
            audit["source_record_sha256"],audit["source_schema_sha256"],descriptors,
            {name:receipt for name,(_,receipt) in captures.items()})
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,
            "source_null_geometry_count":0,"candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,
            "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
            "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,
            "quality_check_passed":False}
    for name,body in outputs.items():
        write_immutable(storage,f"{prefix}/{name}",body)
    for name,descriptor in descriptors.items():
        key=f"{prefix}/{name}"; snapshot=storage.object_snapshot(key)
        if (snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"]
                or storage.object_snapshot(key) != snapshot):
            raise ValueError("candidate_output_physical_readback_failed:"+name)
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,
        "source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],
        "capture_receipts":{name:receipt for name,(_,receipt) in captures.items()},"objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,
        "source_null_geometry_count":0,"source_invalid_geometry_count":0,
        "candidate_ready":prefix+"/metadata/candidate_ready.json","capture_directory":str(capture_root),
        "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,
        "release_accepted":False,"quality_check_passed":False}
