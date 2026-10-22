"""Draft extractor for EPA's exact FRS TSCA program layer.

The EPA hosted FeatureServer publishes the TSCA-associated subset of EPA FRS.
This module captures native Esri JSON and
stages only an unapproved candidate; it does not establish currentness or permit
publication.
"""
from __future__ import annotations

import hashlib
import html
import json
import platform
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file

ITEM_ID = "3c8a335f4b5243a188fd75472459e567"
SERVICE_URL = "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_INTERESTS_TSCA/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "TSCA"
DATASET_SLUG = "epa-frs-toxic-substances-control-act-tsca"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Environmental Protection Agency (EPA_GEO hosted FeatureServer); EPA Facility Registry Service TSCA subset"
SOURCE_VINTAGE = None
SOURCE_VINTAGE_NOTE = "The ArcGIS item modification timestamp is not a source release date; authoritative source release date requires review."
EPA_FRS_DATA_SOURCES_URL = "https://www.epa.gov/frs/frs-data-sources"
LICENSE_URL = "https://www.usa.gov/publicdomain/label/1.0/"
PROPOSED_PHYSICAL_TYPE_MAPPING = {
    "esriFieldTypeOID": {"arrow": "int64", "status": "proposed_unapproved"},
    "esriFieldTypeDate": {"arrow": "timestamp(ms, UTC)", "status": "proposed_unapproved"},
}
EXPECTED_COUNT = 26454
EXPECTED_ID_SHA256 = ""
EXPECTED_RECORD_SHA256 = ""
EXPECTED_GEOMETRY_SHA256 = ""
PAGE_SIZE = 1000
MAX_BODY_BYTES = 16_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _fetch(client: httpx.Client, url: str, directory: Path, name: str, *, max_bytes: int = MAX_BODY_BYTES) -> tuple[bytes, dict[str, object]]:
    return capture_get(client, url, directory, name, max_bytes=max_bytes)


def _feature_urls(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("epa_data_sources_page", EPA_FRS_DATA_SOURCES_URL),
        ("portal_item", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
        ("ids_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})),
    ]
    for offset in range(0, len(ids), PAGE_SIZE):
        params = {
            "where": "1=1", "outFields": "*", "returnGeometry": "true",
            "outSR": "4269", "orderByFields": "OBJECTID ASC",
            "resultOffset": str(offset), "resultRecordCount": str(PAGE_SIZE), "f": "json",
        }
        urls.append((f"page_{offset // PAGE_SIZE:03d}", LAYER_URL + "/query?" + urlencode(params)))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
        ("ids_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"})),
    ])
    return urls


def _item_provenance(portal: Mapping[str, object]) -> dict[str, str]:
    """Return license and attribution claims declared by the exact EPA item."""
    license_info = html.unescape(str(portal.get("licenseInfo", "")))
    if (LICENSE_URL not in license_info or "public domain" not in license_info.casefold()
            or "17 U.S.C. § 105" not in license_info):
        raise ValueError("item_license_declaration_missing_or_changed")
    attribution = portal.get("accessInformation")
    if (not isinstance(attribution, str) or "EPA FRS Support team" not in attribution
            or "TSCA" not in attribution):
        raise ValueError("item_attribution_declaration_missing_or_changed")
    return {"license": LICENSE_URL, "attribution": attribution}


def _profile_schema(layer: dict[str, object]) -> tuple[list[dict[str, object]], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise ValueError("source_field_roster_drift")
    plain = [_plain_field(x) for x in fields]
    return plain, _sha(_canonical(plain))


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]) -> tuple[dict[str, object], list[dict[str, object]], dict[str, object]]:
    docs = {name: json.loads(body) for name, (body, _) in captures.items() if name != "epa_data_sources_page"}
    expected_names = {
        "epa_data_sources_page", "portal_item", "service_before", "layer_before", "count_before", "ids_before",
        "ids_seed", "layer_after", "service_after", "count_after", "ids_after",
        *(f"page_{i:03d}" for i in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE)),
    }
    if set(captures) != expected_names:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    portal = docs["portal_item"]
    item_provenance = _item_provenance(portal)
    service = docs["service_before"]; layer = docs["layer_before"]
    if (portal.get("id") != ITEM_ID or portal.get("owner") != "EPA_GEO"
            or portal.get("title") != "EPA Facility Registry Service - Toxic Substances Control Act (TSCA)"
            or portal.get("url") != SERVICE_URL or portal.get("access") != "public"):
        raise ValueError("official_EPA_portal_ownership_or_publication_drift")
    if not isinstance(service, dict) or service.get("serviceItemId") != ITEM_ID:
        raise ValueError("source_item_identity_drift")
    if not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME for x in service.get("layers", []) if isinstance(x, dict)):
        raise ValueError("exact_layer_missing")
    if layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPoint":
        raise ValueError("exact_layer_metadata_drift")
    spatial = layer.get("extent", {}).get("spatialReference", {})
    if spatial.get("latestWkid") != 4269:
        raise ValueError("source_crs_drift")
    fields, schema_sha = _profile_schema(layer)
    after_fields = docs["layer_after"].get("fields")
    if not isinstance(after_fields, list) or [_plain_field(x) for x in after_fields] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    if schema_sha != "f0f50c2204dfd72bcc5df3b2faae12f45b9b9c0f02c059078a1bfc2d2054184c":
        raise ValueError("source_schema_drift")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    ids_before = docs["ids_before"].get("objectIds"); ids_after = docs["ids_after"].get("objectIds")
    ids_seed = docs["ids_seed"].get("objectIds")
    if not isinstance(ids_before, list) or ids_before != ids_after or len(ids_before) != EXPECTED_COUNT:
        raise ValueError("source_id_population_drift")
    if not isinstance(ids_seed, list) or sorted(ids_seed) != sorted(ids_before):
        raise ValueError("source_id_population_changed_before_capture")
    ids_before = sorted(ids_before)
    if EXPECTED_ID_SHA256 and _sha(_canonical(ids_before)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if docs["layer_before"] != docs["layer_after"] or docs["service_before"] != docs["service_after"]:
        raise ValueError("source_metadata_changed_during_capture")
    page_names = [name for name in captures if name.startswith("page_")]
    page_names.sort()
    features = []
    for name in page_names:
        page = docs[name]
        if "error" in page:
            raise ValueError("source_page_error")
        if page.get("spatialReference", {}).get("latestWkid") != 4269:
            raise ValueError("source_page_spatial_reference_drift")
        rows = page.get("features")
        if not isinstance(rows, list): raise ValueError("source_page_missing_features")
        page_index = int(name.removeprefix("page_"))
        page_ids = [row.get("attributes", {}).get("OBJECTID") for row in rows]
        expected_page_ids = ids_before[page_index * PAGE_SIZE:(page_index + 1) * PAGE_SIZE]
        if page_ids != expected_page_ids:
            raise ValueError("source_page_id_slice_incomplete_or_out_of_order")
        features.extend(rows)
    got_ids = [f.get("attributes", {}).get("OBJECTID") for f in features]
    if len(features) != EXPECTED_COUNT or sorted(got_ids) != ids_before or len(set(got_ids)) != EXPECTED_COUNT:
        raise ValueError("source_page_population_incomplete")
    expected_names = [x["name"] for x in fields]
    geometries = []
    for feature in features:
        attrs = feature.get("attributes")
        geom = feature.get("geometry")
        if not isinstance(attrs, dict) or list(attrs) != expected_names or not isinstance(geom, dict) or set(geom) != {"x", "y"}:
            raise ValueError("source_record_schema_or_geometry_drift")
        if not all(isinstance(geom[k], (int, float)) for k in ("x", "y")):
            raise ValueError("source_point_coordinate_invalid")
        geometries.append([attrs["OBJECTID"], geom["x"], geom["y"]])
    features.sort(key=lambda f: f["attributes"]["OBJECTID"])
    records_sha = _sha(_canonical(features))
    geometry_sha = _sha(_canonical(geometries))
    if EXPECTED_RECORD_SHA256 and records_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_value_or_geometry_baseline_drift")
    if EXPECTED_GEOMETRY_SHA256 and geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids_before)),
        "source_record_sha256": records_sha, "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": schema_sha, "source_crs_epsg": 4269,
        "geometry_type": "Point", "null_geometry_count": 0, "unique_source_id_count": len(set(got_ids)),
        "portal_item_id": portal["id"], "portal_owner": portal["owner"],
        "portal_modified_epoch_ms": portal.get("modified"),
        "portal_modified_is_source_release_date": False,
    }
    return layer, fields, {"features": features, "audit": audit, "ids": ids_before}


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


def _parquet_bytes(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    columns = {}
    for field in fields:
        name = field["name"]
        columns[name] = pa.array([row["attributes"][name] for row in features], type=_arrow_type(field))
    columns["geometry"] = pa.array([Point(row["geometry"]["x"], row["geometry"]["y"]).wkb for row in features], type=pa.binary())
    schema = pa.schema([pa.field(field["name"], _arrow_type(field), nullable=field.get("nullable", True)) for field in fields] + [pa.field("geometry", pa.binary(), nullable=False)])
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{
        "encoding":"WKB", "geometry_types":["Point"], "crs":pyproj.CRS.from_epsg(4269).to_json_dict(), "edges":"planar",
    }}}
    table = pa.Table.from_pydict(columns, schema=schema.with_metadata({b"geo":_canonical(geo)}))
    sink = pa.BufferOutputStream(); pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _write_immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    write_immutable(storage, key, body)


def _verify_first_candidate(storage: StagingStorageResource, prefix: str, state_key: str,
                           version: str, fingerprint: str, identity: Mapping[str, object],
                           audit: Mapping[str, object], expected_keys: set[str], current_parquet: bytes) -> dict[str, object]:
    """Validate an earlier candidate before treating a same-version run as reuse."""
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity", "source_record_sha256", "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise ValueError("cached_first_candidate_identity_or_source_changed")
    objects = payload["objects"]
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], objects, payload["capture_receipts"])
    if not isinstance(objects, dict) or set(objects) != expected_keys:
        raise ValueError("cached_candidate_roster_changed")
    for name, descriptor in objects.items():
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if (not isinstance(descriptor, dict) or snapshot is None
                or descriptor != {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
                or storage.object_snapshot(key) != snapshot):
            raise ValueError("cached_candidate_object_integrity_failed:" + name)
    parquet_key = f"{prefix}/geoparquet/{DATASET_SLUG}.parquet"
    if objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != {"bytes": len(current_parquet), "sha256": _sha(current_parquet)}:
        raise ValueError("cached_native_candidate_changed")
    receipt_map = payload["capture_receipts"]
    if not isinstance(receipt_map, dict): raise ValueError("cached_capture_receipts_missing")
    for name, receipt in receipt_map.items():
        raw_key = f"{prefix}/raw/{name}.body"; wire_key = f"{prefix}/raw/{name}.wire"
        body = storage.read_key(raw_key); wire = storage.read_key(wire_key)
        receipt_body = storage.read_key(f"{prefix}/raw/{name}.receipt.json")
        if (receipt.get("complete_body") is not True or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt_body != _canonical(receipt)+b"\n"):
            raise ValueError("cached_original_capture_changed:" + name)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,
            "source_null_geometry_count":0,"source_invalid_geometry_count":0,"candidate_ready":prefix+"/metadata/candidate_ready.json",
            "reused":True,"source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
            "freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def capture_epa_tsca(storage: StagingStorageResource, *, version: str, capture_directory: str | None = None,
                     replay_capture_directory: str | None = None) -> dict[str, object]:
    """Capture and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000: raise ValueError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/73/captures")
    capture_root = attempt_root / f"{version}-{uuid4().hex}"
    capture_root = capture_root.absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    replay_root: Path | None = None
    try:
        if replay_capture_directory:
            replay_root = Path(replay_capture_directory).absolute()
            if ".." in replay_root.parts or any(parent.is_symlink() for parent in (replay_root, *replay_root.parents)):
                raise ValueError("unsafe_replay_capture_directory")
            for receipt_path in sorted(replay_root.glob("*.receipt.json")):
                name = receipt_path.name.removesuffix(".receipt.json")
                receipt_body = receipt_path.read_bytes()
                receipt = json.loads(receipt_body)
                body = (replay_root / f"{name}.body").read_bytes()
                wire = (replay_root / f"{name}.wire").read_bytes()
                if (receipt_body != _canonical(receipt) + b"\n" or receipt.get("method") != "GET"
                        or receipt.get("complete_body") is not True or receipt.get("status") != 200
                        or receipt.get("url") != receipt.get("final_url")
                        or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                        or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
                    raise ValueError("replay_capture_receipt_or_body_invalid:" + name)
                captures[name] = (body, receipt)
                (capture_root / f"{name}.body").write_bytes(body)
                (capture_root / f"{name}.wire").write_bytes(wire)
                (capture_root / f"{name}.receipt.json").write_bytes(receipt_body)
            seed = json.loads(captures.get("ids_seed", (b"{}", {}))[0]).get("objectIds")
            if not isinstance(seed, list) or len(seed) != EXPECTED_COUNT:
                raise ValueError("replay_capture_seed_invalid")
            expected_urls = {"ids_seed":LAYER_URL + "/query?" + urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"}), **dict(_feature_urls(seed))}
            if set(captures) != set(expected_urls):
                raise ValueError("replay_capture_roster_incomplete_or_unexpected")
            for name, url in expected_urls.items():
                if captures[name][1].get("url") != url:
                    raise ValueError("replay_capture_request_url_mismatch:" + name)
        else:
            with httpx.Client(follow_redirects=False, timeout=60, headers={"User-Agent":"hifld-next-datasets/epa-power-generation-draft"}) as client:
                seed_body, seed_receipt = _fetch(client, LAYER_URL + "/query?" + urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"}), capture_root, "ids_seed")
                seed = json.loads(seed_body).get("objectIds")
                if not isinstance(seed, list) or len(seed) != EXPECTED_COUNT:
                    raise ValueError("source_seed_id_count_drift")
                captures["ids_seed"] = (seed_body, seed_receipt)
                for name, url in _feature_urls(seed):
                    body, receipt = _fetch(client, url, capture_root, name)
                    captures[name] = (body, receipt)
        layer, fields, extracted = _validate_captures(captures)
        item_provenance = _item_provenance(json.loads(captures["portal_item"][0]))
    except Exception as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try: receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError): receipts[path.name] = {"receipt_readback_failed":True}
        (capture_root / "capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":False,"error_code":type(exc).__name__,"receipts":receipts})+b"\n")
        if captures:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          captures, type(exc).__name__)
        raise

    return _finalize_epa_tsca(storage, version, capture_root, captures, fields, extracted,
                              item_provenance, replayed_from=str(replay_root) if replay_root else None)


def _finalize_epa_tsca(storage: StagingStorageResource, version: str, capture_root: Path,
                      captures: dict[str, tuple[bytes, dict[str, object]]],
                      fields: list[dict[str, object]], extracted: dict[str, object],
                      item_provenance: dict[str, str], *, replayed_from: str | None = None) -> dict[str, object]:
    """Build and seal a candidate from the same validated live or replay packet."""
    identity = _code_identity()
    parquet_body = _parquet_bytes(fields, extracted["features"])
    source_objects = {"geoparquet/epa-frs-toxic-substances-control-act-tsca.parquet":{"bytes":len(parquet_body),"sha256":_sha(parquet_body)}}
    manifest = {
      "schema_version":1,"approved":False,"title":LAYER_NAME,"description":"Exact EPA_GEO hosted EPA FRS TSCA-associated point layer. Source release date, currentness, and any successor decision remain pending.",
      "publisher":PUBLISHER,"agency":"Environmental Protection Agency","source_url":LAYER_URL,"license":item_provenance["license"],"license_evidence":{"source_item_id":ITEM_ID,"metadata_field":"licenseInfo","url":item_provenance["license"]},
      "source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
      "inputs":[{"input_id":name,"producer":PUBLISHER,"host":urlsplit(receipt["url"]).hostname,"role":"producer_publication","canonical_url":EPA_FRS_DATA_SOURCES_URL if name == "epa_data_sources_page" else LAYER_URL,"retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],"http_status":receipt["status"],"bytes":receipt["bytes"],"sha256":receipt["sha256"],"license":("unknown" if name == "epa_data_sources_page" else item_provenance["license"]),"license_unknown_reason":("The EPA FRS data sources page response has no separate license declaration in the captured item metadata." if name == "epa_data_sources_page" else None),"license_evidence":(None if name == "epa_data_sources_page" else {"source_item_id":ITEM_ID,"metadata_field":"licenseInfo","url":item_provenance["license"]}),"attribution":item_provenance["attribution"],"release_date":None,"release_date_unknown_reason":SOURCE_VINTAGE_NOTE} for name,(_,receipt) in sorted(captures.items())],
      "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"feature_layer_id":LAYER_ID,"source_fields":"all 32 fields preserved in producer order and declared physical types","source_ids":"Preserve each OBJECTID and REGISTRY_ID unchanged; all other source fields retained verbatim","proposed_physical_type_mapping":PROPOSED_PHYSICAL_TYPE_MAPPING,"raw_source_values_preserved":True,"mapping_approved":False,"geometry":"Esri point x/y retained as WKB at EPSG:4269; no reprojection or repair","candidate_status":"unapproved; production publication prohibited pending source freshness, schema/ID contract, rights and mapping acceptance"}}],
      "lineage":"EPA_GEO ArcGIS item 3c8a335f4b5243a188fd75472459e567 serves exact FeatureServer layer 0 (TSCA). Item describes its records as the subset of EPA FRS facilities linked to the Toxic Substances Control Act Chemical Substance Inventory System. Item modification time is not asserted as source release date.",
      "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native Esri query values, all attributes, IDs and source point geometry retained; source vintage and release approvals remain unresolved.","objects":[{"path":next(iter(source_objects)),**next(iter(source_objects.values()))}]}}},
      "unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {**extracted["audit"],"source_fields":fields,"official_layer_name":LAYER_NAME,"arcgis_item_id":ITEM_ID,"item_license_declaration":item_provenance["license"],"item_license_declaration_field":"licenseInfo","source_attribution":item_provenance["attribution"],"proposed_physical_type_mapping":PROPOSED_PHYSICAL_TYPE_MAPPING,"raw_source_values_preserved":True,"service_modified_is_not_release_date":True,"source_vintage":SOURCE_VINTAGE,"source_vintage_note":SOURCE_VINTAGE_NOTE,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    version = content_derived_version(audit["source_record_sha256"],
                                      audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    provenance_name = "replay_provenance.json" if replayed_from else "capture_manifest.json"
    (capture_root / provenance_name).write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,"replayed_from":replayed_from,"source_feature_count":EXPECTED_COUNT,"source_record_sha256":audit["source_record_sha256"],"source_geometry_sha256":audit["source_geometry_sha256"],"receipts":{name:receipt for name,(_,receipt) in sorted(captures.items())}})+b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs = {
        "geoparquet/epa-frs-toxic-substances-control-act-tsca.parquet":parquet_body,
        "metadata/source_manifest.json":_canonical(manifest)+b"\n",
        "metadata/source_audit.json":_canonical(audit)+b"\n",
        "metadata/processing_identity.json":_canonical(identity)+b"\n",
    }
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root/f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt)+b"\n"
    ready = {"approved":False,"version":version,"objects":source_objects,"source_record_sha256":audit["source_record_sha256"],"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready)+b"\n"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        return _verify_first_candidate(storage,prefix,state_key,version,fingerprint,identity,audit,set(outputs),parquet_body)
    for relative, body in outputs.items(): _write_immutable(storage,f"{prefix}/{relative}",body)
    descriptors = {name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    for name, descriptor in descriptors.items():
        key=f"{prefix}/{name}"; snapshot=storage.object_snapshot(key)
        if snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] or storage.object_snapshot(key) != snapshot:
            raise ValueError("candidate_output_physical_readback_failed:"+name)
    payload = {"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":{name:receipt for name,(_,receipt) in captures.items()},"objects":descriptors}
    seal_ready_candidate(storage,state_key,version,fingerprint,payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":EXPECTED_COUNT,"source_null_geometry_count":0,"source_invalid_geometry_count":0,"candidate_ready":prefix+"/metadata/candidate_ready.json","source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(capture_root),"capture_replayed":bool(replayed_from)}
