"""Draft native capture for EPA FRS's TRI program-interest layer.

This is the FRS-maintained TRI facility relationship layer, not the annual
chemical-release reporting map.  All producer responses are retained verbatim;
the GeoParquet is an unapproved preservation candidate with explicit physical
type proposals pending contract review.
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
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

ITEM_ID = "de71cdcedc924aca86af4f838744fce7"
SERVICE_URL = "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_INTERESTS_TRI/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "FRS_INTERESTS_TRI"
DATASET_SLUG = "epa-frs-toxic-release-inventory"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Environmental Protection Agency, Facility Registry Service (EPA_GEO hosted FeatureServer)"
TRI_PROGRAM_URL = "https://www.epa.gov/toxics-release-inventory-tri-program"
EPA_FRS_URL = "https://www.epa.gov/frs"
PAGE_SIZE = 1000
MAX_BODY_BYTES = 8_000_000
MAX_CAPTURE_TOTAL_BYTES = 480_000_000
MAX_OBJECT_ID_URL_LENGTH = 1800
EXPECTED_SOURCE_SCHEMA_SHA256 = "f0f50c2204dfd72bcc5df3b2faae12f45b9b9c0f02c059078a1bfc2d2054184c"
EXPECTED_FIELD_NAMES = [
    "OBJECTID", "REGISTRY_ID", "PRIMARY_NAME", "LOCATION_ADDRESS", "CITY_NAME", "COUNTY_NAME",
    "FIPS_CODE", "STATE_CODE", "POSTAL_CODE", "LATITUDE83", "LONGITUDE83", "HUC8_CODE",
    "ACCURACY_VALUE", "COLLECT_MTH_DESC", "REF_POINT_DESC", "CREATE_DATE", "UPDATE_DATE",
    "LAST_REPORTED_DATE", "FAC_URL", "PGM_SYS_ID", "PGM_SYS_ACRNM", "INTEREST_TYPE",
    "PROGRAM_URL", "PGM_REPORT_URL", "PUBLIC_IND", "ACTIVE_STATUS", "FEDERAL_AGENCY_NAME",
    "HUC_12", "FEDERAL_LAND_IND", "FED_FACILITY_CODE", "EPA_REGION_CODE", "KEY_FIELD",
]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    # This is a proposed native-preservation encoding. Esri dates remain their
    # source epoch-millisecond integers; review can approve a richer mapping.
    return {
        "esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
        "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeDouble": pa.float64(),
        "esriFieldTypeSingle": pa.float32(), "esriFieldTypeString": pa.string(),
        "esriFieldTypeDate": pa.int64(), "esriFieldTypeGUID": pa.string(),
        "esriFieldTypeGlobalID": pa.string(),
    }[str(field["type"])]


def _query_url(**params: object) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _source_metadata_urls() -> list[tuple[str, str]]:
    return [
        ("portal_item", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"),
        ("portal_data", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}/data?f=json"),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query_url(where="1=1", returnCountOnly="true", f="json")),
        ("ids_before", _query_url(where="1=1", returnIdsOnly="true", f="json")),
    ]


def _source_after_urls() -> list[tuple[str, str]]:
    return [
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", _query_url(where="1=1", returnCountOnly="true", f="json")),
        ("ids_after", _query_url(where="1=1", returnIdsOnly="true", f="json")),
    ]


def _supports_ordered_pagination(layer: Mapping[str, object]) -> bool:
    capabilities = layer.get("advancedQueryCapabilities")
    return (isinstance(capabilities, dict)
            and capabilities.get("supportsPagination") is True
            and capabilities.get("supportsOrderBy") is True)


def _object_id_chunks(ids: list[int]) -> list[list[int]]:
    """Split fallback ObjectID GET requests below the server URL limit."""
    chunks: list[list[int]] = []
    current: list[int] = []
    for object_id in ids:
        candidate = [*current, object_id]
        params = {"objectIds": ",".join(map(str, candidate)), "outFields": "*",
                  "returnGeometry": "true", "returnZ": "true", "returnM": "true",
                  "outSR": "4269", "orderByFields": "OBJECTID ASC", "f": "json"}
        if len(_query_url(**params)) >= MAX_OBJECT_ID_URL_LENGTH:
            if not current:
                raise ValueError("single_object_id_exceeds_get_url_limit")
            chunks.append(current)
            current = [object_id]
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def _page_plan(ids: list[int], layer: Mapping[str, object]) -> tuple[str, list[tuple[str, str, list[int]]]]:
    """Plan deterministic source pages with expected ordered IDs."""
    ordered_ids = sorted(ids)
    pages: list[tuple[str, str, list[int]]] = []
    if _supports_ordered_pagination(layer):
        mode = "ordered_offset"
        for offset in range(0, len(ordered_ids), PAGE_SIZE):
            page_ids = ordered_ids[offset:offset + PAGE_SIZE]
            url = _query_url(where="1=1", outFields="*", returnGeometry="true", returnZ="true",
                returnM="true", outSR="4269", orderByFields="OBJECTID ASC",
                resultOffset=offset, resultRecordCount=PAGE_SIZE, f="json")
            if len(url) >= MAX_OBJECT_ID_URL_LENGTH:
                raise ValueError("pagination_get_url_exceeds_safe_limit")
            pages.append((f"page_{len(pages):03d}", url, page_ids))
    else:
        mode = "bounded_object_ids"
        for page_ids in _object_id_chunks(ordered_ids):
            params = {"objectIds": ",".join(map(str, page_ids)), "outFields": "*",
                      "returnGeometry": "true", "returnZ": "true", "returnM": "true",
                      "outSR": "4269", "orderByFields": "OBJECTID ASC", "f": "json"}
            url = _query_url(**params)
            if len(url) >= MAX_OBJECT_ID_URL_LENGTH:
                raise ValueError("object_id_get_url_exceeds_safe_limit")
            pages.append((f"page_{len(pages):03d}", url, page_ids))
    if not pages:
        raise ValueError("source_page_plan_empty")
    return mode, pages


def _validate(captures: dict[str, tuple[bytes, dict[str, object]]]) -> tuple[dict, list[dict], list[dict], dict]:
    docs = {name: json.loads(body) for name, (body, _) in captures.items()}
    fields = docs["layer_before"].get("fields")
    if not isinstance(fields, list) or not fields or any(not isinstance(f, dict) for f in fields):
        raise ValueError("source_field_roster_missing")
    plain_fields = [_plain_field(f) for f in fields]
    schema_sha = _sha(_canonical(plain_fields))
    if schema_sha != EXPECTED_SOURCE_SCHEMA_SHA256:
        raise ValueError("source_schema_drift")
    portal = docs["portal_item"]
    if (portal.get("id") != ITEM_ID or portal.get("owner") != "EPA_GEO"
            or portal.get("title") != "EPA Facility Registry Service - Toxic Release Inventory (TRI) Sites"
            or portal.get("url") != SERVICE_URL or portal.get("access") != "public"):
        raise ValueError("source_item_identity_or_access_drift")
    desc = str(portal.get("description", ""))
    if ("maintained by the EPA FRS Support team" not in desc
            or "based on data from the EPA's national environmental program for the Toxic Release Inventory" not in desc
            or "United States, including territories and tribal areas" not in desc):
        raise ValueError("official_FRS_TRI_relationship_or_coverage_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if (service.get("serviceItemId") != ITEM_ID
            or not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME for x in service.get("layers", []) if isinstance(x, dict))
            or layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("objectIdField") != "OBJECTID" or layer.get("hasZ") is not False
            or layer.get("hasM") is not False):
        raise ValueError("exact_FRS_TRI_layer_identity_drift")
    if layer.get("extent", {}).get("spatialReference", {}).get("latestWkid") != 4269:
        raise ValueError("source_native_crs_drift")
    if len(fields) != len(EXPECTED_FIELD_NAMES) or [f.get("name") for f in fields] != EXPECTED_FIELD_NAMES:
        raise ValueError("source_schema_roster_drift")
    if docs["layer_before"] != docs["layer_after"] or docs["service_before"] != docs["service_after"]:
        raise ValueError("source_metadata_changed_during_capture")
    if [_plain_field(f) for f in docs["layer_after"].get("fields", [])] != plain_fields:
        raise ValueError("source_field_schema_changed_during_capture")
    counts = (docs["count_before"].get("count"), docs["count_after"].get("count"))
    ids = (docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds"))
    if (type(counts[0]) is not int or counts[0] < 1 or type(counts[1]) is not int or counts[1] != counts[0]
            or not isinstance(ids[0], list) or ids[0] != ids[1] or len(ids[0]) != counts[0]
            or any(type(value) is not int for value in ids[0])):
        raise ValueError("source_count_or_id_roster_drift")
    got_ids = sorted(ids[0])
    if len(set(got_ids)) != counts[0]:
        raise ValueError("source_id_roster_duplicate")
    seed = docs["ids_seed"].get("objectIds")
    if not isinstance(seed, list) or any(type(value) is not int for value in seed) or sorted(seed) != got_ids:
        raise ValueError("source_id_roster_changed_before_capture")
    page_names = sorted(name for name in captures if name.startswith("page_"))
    pagination_mode, page_plan = _page_plan(got_ids, layer)
    expected_pages = [name for name, _url, _ids in page_plan]
    if page_names != expected_pages:
        raise ValueError("source_page_roster_incomplete")
    features = []
    for page_index, (name, expected_url, expected_page_ids) in enumerate(page_plan):
        page = docs[name]
        receipt = captures[name][1]
        if receipt.get("url") != expected_url:
            raise ValueError("source_page_query_plan_drift")
        if pagination_mode == "bounded_object_ids" and len(expected_url) >= MAX_OBJECT_ID_URL_LENGTH:
            raise ValueError("source_page_get_url_exceeds_safe_limit")
        if "error" in page:
            raise ValueError("source_page_error_or_truncated")
        # The EPA layer sets exceededTransferLimit on a full offset page when
        # additional ordered rows remain. The independently pinned ID roster
        # and exact requested slice prove page completeness; a partial or
        # unexpectedly flagged final page still fails closed.
        if (page.get("exceededTransferLimit") is True
                and (pagination_mode != "ordered_offset" or len(expected_page_ids) != PAGE_SIZE
                     or page_index == len(page_plan) - 1)):
            raise ValueError("source_page_error_or_truncated")
        if page.get("spatialReference", {}).get("latestWkid") != 4269:
            raise ValueError("source_page_crs_drift")
        rows = page.get("features")
        if not isinstance(rows, list):
            raise ValueError("source_page_missing_features")
        page_ids = [row.get("attributes", {}).get("OBJECTID") for row in rows
                    if isinstance(row, dict) and isinstance(row.get("attributes"), dict)]
        if page_ids != expected_page_ids:
            raise ValueError("source_page_ids_do_not_match_requested_slice")
        features.extend(rows)
    page_ids = [f.get("attributes", {}).get("OBJECTID") for f in features]
    if (len(features) != counts[0] or any(type(value) is not int for value in page_ids)
            or sorted(page_ids) != got_ids or len(set(page_ids)) != counts[0]):
        raise ValueError("source_feature_population_incomplete")
    field_names = [f["name"] for f in fields]
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    geom_repr = []
    null_geometry_count = 0
    invalid_geometry_count = 0
    for row in features:
        attrs, geom = row.get("attributes"), row.get("geometry")
        if not isinstance(attrs, dict) or list(attrs) != field_names:
            raise ValueError("source_attribute_roster_drift")
        if geom is None:
            null_geometry_count += 1
            geom_repr.append([attrs["OBJECTID"], None])
            continue
        if not isinstance(geom, dict) or set(geom) != {"x", "y"}:
            raise ValueError("source_point_geometry_roster_drift")
        if not all(type(geom[k]) in (int, float) for k in ("x", "y")):
            raise ValueError("source_point_coordinate_invalid")
        if not (-180 <= geom["x"] <= 180 and -90 <= geom["y"] <= 90):
            invalid_geometry_count += 1
        geom_repr.append([attrs["OBJECTID"], geom["x"], geom["y"]])
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(got_ids)),
        "source_population_complete": True,
        "source_record_sha256": _sha(_canonical(features)),
        "source_geometry_sha256": _sha(_canonical(geom_repr)), "source_schema_sha256": schema_sha,
        "source_fields": plain_fields, "source_crs_wkid": 4269,
        "source_geometry_type": "esriGeometryPoint", "source_null_geometry_count": null_geometry_count,
        "source_invalid_geometry_count": invalid_geometry_count,
        "pagination_mode":pagination_mode,"page_count":len(page_plan),"page_size":PAGE_SIZE,
        "proposed_physical_field_mappings":[{"field":field["name"],"source_type":field["type"],
            "proposed_arrow_type":str(_arrow_type(field)),"approved":False} for field in plain_fields],
    }
    return layer, plain_fields, features, audit


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    arrays = {}
    schema_fields = []
    for field in fields:
        name = field["name"]
        kind = _arrow_type(field)
        arrays[name] = pa.array([row["attributes"][name] for row in features], type=kind)
        schema_fields.append(pa.field(name, kind, nullable=bool(field.get("nullable", True))))
    arrays["geometry"] = pa.array([
        None if row["geometry"] is None else Point(row["geometry"]["x"], row["geometry"]["y"]).wkb
        for row in features
    ], type=pa.binary())
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{
        "encoding":"WKB", "geometry_types":["Point"], "crs":pyproj.CRS.from_epsg(4269).to_json_dict(), "edges":"planar",
    }}}
    schema = pa.schema(schema_fields, metadata={b"geo":_canonical(geo)})
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(arrays, schema=schema), sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict:
    return processing_identity(Path(__file__).parents[3])


def _verify_first_candidate(storage: StagingStorageResource, prefix: str, state_key: str,
                            version: str, fingerprint: str, identity: Mapping[str, object],
                            audit: Mapping[str, object], expected_names: set[str]) -> dict:
    import json as _json
    bundle = _json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity", "source_record_sha256",
            "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise ValueError("cached_first_candidate_identity_or_source_changed")
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
        identity, audit["source_record_sha256"], audit["source_schema_sha256"], payload["objects"], payload["capture_receipts"])
    if set(payload["objects"]) != expected_names:
        raise ValueError("cached_candidate_roster_changed")
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,
        "feature_count":audit["source_feature_count"],"source_population_complete":True,
        "source_null_geometry_count":audit["source_null_geometry_count"],
        "source_invalid_geometry_count":audit["source_invalid_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json",
        "reused":True,"source_release_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,
        "mapping_approved":False,"release_accepted":False}


def capture_epa_frs_tri(storage: StagingStorageResource, *, version: str,
                        capture_directory: str | None = None) -> dict:
    """Acquire, validate and stage a complete unapproved FRS-TRI candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000:
        raise ValueError("disk_floor_before_acquisition")
    capture_base = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/72/captures")
    capture_root = (capture_base / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict]] = {}
    current_capture_name = "ids_seed"
    current_capture_url = _query_url(where="1=1", returnIdsOnly="true", f="json")
    try:
        with httpx.Client(follow_redirects=False, timeout=90, headers={"User-Agent":"hifld-next-datasets/epa-frs-tri-draft"}) as client:
            seed_body, seed_receipt = capture_get(client,
                current_capture_url, capture_root, current_capture_name, max_bytes=MAX_BODY_BYTES)
            seed = json.loads(seed_body).get("objectIds")
            if not isinstance(seed, list) or not seed or any(type(value) is not int for value in seed):
                raise ValueError("source_seed_id_roster_missing_or_invalid")
            captures["ids_seed"] = (seed_body, seed_receipt)
            total_captured_bytes = seed_receipt["bytes"] + seed_receipt["wire_bytes"]
            for name, url in _source_metadata_urls():
                current_capture_name, current_capture_url = name, url
                body, receipt = capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES)
                captures[name] = (body, receipt)
                total_captured_bytes += receipt["bytes"] + receipt["wire_bytes"]
                if total_captured_bytes > MAX_CAPTURE_TOTAL_BYTES:
                    raise ValueError("source_capture_exceeds_scratch_budget")
            layer_before = json.loads(captures["layer_before"][0])
            _pagination_mode, page_plan = _page_plan(seed, layer_before)
            for name, url, _page_ids in page_plan:
                current_capture_name, current_capture_url = name, url
                body, receipt = capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES)
                captures[name] = (body, receipt)
                total_captured_bytes += receipt["bytes"] + receipt["wire_bytes"]
                if total_captured_bytes > MAX_CAPTURE_TOTAL_BYTES:
                    raise ValueError("source_capture_exceeds_scratch_budget")
            for name, url in _source_after_urls():
                current_capture_name, current_capture_url = name, url
                body, receipt = capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES)
                captures[name] = (body, receipt)
                total_captured_bytes += receipt["bytes"] + receipt["wire_bytes"]
                if total_captured_bytes > MAX_CAPTURE_TOTAL_BYTES:
                    raise ValueError("source_capture_exceeds_scratch_budget")
        layer, fields, features, audit = _validate(captures)
    except Exception as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try: receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError): receipts[path.name] = {"receipt_readback_failed":True}
        # The shared capture helper may fail during a stream before it can
        # return a receipt. Preserve every partial body/wire file with a
        # clearly incomplete receipt so a later operator can inspect it.
        partial_names = {p.name.removesuffix(".wire") for p in capture_root.glob("*.wire")}
        partial_names.update(p.name.removesuffix(".body") for p in capture_root.glob("*.body"))
        for name in sorted(partial_names - captures.keys()):
            body_path, wire_path = capture_root/f"{name}.body", capture_root/f"{name}.wire"
            body = body_path.read_bytes() if body_path.exists() else b""
            wire = wire_path.read_bytes() if wire_path.exists() else b""
            receipt = receipts.get(name)
            if not isinstance(receipt, dict):
                receipt = {"method":"GET","url":current_capture_url if name == current_capture_name else None,
                    "final_url":None,"observed_at":datetime.now(timezone.utc).isoformat(),"status":None,
                    "complete_body":False,"wire_bytes":len(wire),"wire_sha256":_sha(wire),
                    "bytes":len(body),"sha256":_sha(body),"content_encoding":None,
                    "content_length":None,"error_code":type(exc).__name__}
            receipts[name] = receipt
            captures[name] = (body, receipt)
        (capture_root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":False,"error_code":type(exc).__name__,"receipts":receipts})+b"\n")
        if captures:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, captures, type(exc).__name__)
        raise
    identity = _code_identity()
    parquet = _parquet_bytes(fields, features)
    native_path = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {native_path:{"bytes":len(parquet),"sha256":_sha(parquet)}}
    manifest = {
        "schema_version":1,"approved":False,"title":"EPA Facility Registry Service - Toxic Release Inventory (TRI) Sites",
        "description":"Exact FRS-maintained TRI program-interest facility layer. Physical mapping and release acceptance remain pending.",
        "publisher":PUBLISHER,"agency":"Environmental Protection Agency","source_url":LAYER_URL,
        "license":"https://www.usa.gov/publicdomain/label/1.0/","source_modified":None,"date_issued":None,
        "date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
        "inputs":[{"input_id":name,"producer":PUBLISHER,"host":urlsplit(receipt["url"]).hostname,
            "role":"producer_publication","canonical_url":f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json" if name.startswith("portal") else LAYER_URL,
            "retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],
            "http_status":receipt["status"],"bytes":receipt["bytes"],"sha256":receipt["sha256"],
            "license":"https://www.usa.gov/publicdomain/label/1.0/","attribution":"EPA FRS; producer metadata states U.S. public domain; rights review remains pending.",
            "release_date":None,"release_date_unknown_reason":"ArcGIS modified timestamp is not a source release date."}
            for name,(_,receipt) in sorted(captures.items())],
        "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{
            "feature_layer_id":LAYER_ID,"source_fields":"all source fields retained with declared physical types; date values preserved as epoch milliseconds",
            "source_ids":"OBJECTID and REGISTRY_ID preserved unchanged","geometry":"source point x/y encoded as WKB at EPSG:4269 without reprojection or repair",
            "physical_mapping":"Per-field source-to-Arrow proposals are recorded in source_audit.json; date epoch integers and OID widening preserve values, and all mapping approvals remain false.",
            "candidate_status":"unapproved; production publication prohibited pending source freshness, schema/ID contract, rights and mapping acceptance"}}],
        "lineage":f"EPA_GEO item {ITEM_ID} exposes exact FeatureServer layer 0. Item states this is maintained by EPA FRS Support and based on the national TRI program; it links to {TRI_PROGRAM_URL} and {EPA_FRS_URL}.",
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native Esri fields, IDs, source point geometry and date epoch integers retained; approvals pending.","objects":[{"path":native_path,**source_objects[native_path]}]}}},
        "unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"official_layer_name":LAYER_NAME,"arcgis_item_id":ITEM_ID,"source_relationship":"FRS-maintained EPA TRI interest layer",
        "physical_mapping_approved":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})
    version = content_derived_version(audit["source_record_sha256"],audit["source_geometry_sha256"],audit["source_schema_sha256"],identity)
    prefix = candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    (capture_root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,
        "source_feature_count":audit["source_feature_count"],"source_record_sha256":audit["source_record_sha256"],"source_geometry_sha256":audit["source_geometry_sha256"],
        "receipts":{name:receipt for name,(_,receipt) in sorted(captures.items())}})+b"\n")
    outputs = {native_path:parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n",
        "metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root/f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt)+b"\n"
    ready = {"approved":False,"version":version,"objects":source_objects,"source_record_sha256":audit["source_record_sha256"],
        "freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready)+b"\n"
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        return _verify_first_candidate(storage,prefix,state_key,version,fingerprint,identity,audit,set(outputs))
    for name,body in outputs.items(): write_immutable(storage,f"{prefix}/{name}",body)
    descriptors = {name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    for name,d in descriptors.items():
        key=f"{prefix}/{name}"; snap=storage.object_snapshot(key)
        if snap is None or snap.size != d["bytes"] or storage.sha256_key(key) != d["sha256"] or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_output_physical_readback_failed:"+name)
    payload={"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],
        "capture_receipts":{name:receipt for name,(_,receipt) in captures.items()},"objects":descriptors}
    seal_ready_candidate(storage,state_key,version,fingerprint,payload)
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["source_feature_count"],"source_population_complete":True,
        "source_null_geometry_count":audit["source_null_geometry_count"],
        "source_invalid_geometry_count":audit["source_invalid_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json",
        "capture_directory":str(capture_root),"source_release_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
