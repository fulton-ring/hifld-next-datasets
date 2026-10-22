"""Draft extractor for EPA's exact full-universe FRS INTERESTS FeatureServer layer.

The EPA_GEO hosted feature service publishes the full universe of EPA Facility
Registry Service (FRS) interests. The layer holds millions of point features, so
this module streams the source: each keyset page is retained as raw evidence in
a resumable capture directory before any candidate is sealed. It stages only an
unapproved candidate; it does not establish currentness or permit publication.
"""
from __future__ import annotations

import hashlib
import html
import json
import shutil
from array import array
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

ITEM_ID = "cdff193a3e3743a5bc770e2743f215b3"
ITEM_TITLE = "Facility Registry Service (FRS) Interests"
SERVICE_URL = "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_INTERESTS/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "FRS_INTERESTS"
DATASET_SLUG = "frs-interests"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Environmental Protection Agency (EPA_GEO hosted FeatureServer); EPA Facility Registry Service full interest universe"
SOURCE_VINTAGE = None
SOURCE_VINTAGE_NOTE = ("The hosted service states it is updated weekly; no dated release "
                       "version exists, so the exact source release date requires review.")
LICENSE_URL = "https://edg.epa.gov/EPA_Data_License.html"
PROPOSED_PHYSICAL_TYPE_MAPPING = {
    "esriFieldTypeOID": {"arrow": "int64", "status": "proposed_unapproved"},
    "esriFieldTypeInteger": {"arrow": "int32", "status": "proposed_unapproved"},
    "esriFieldTypeDouble": {"arrow": "float64", "status": "proposed_unapproved"},
    "esriFieldTypeDate": {"arrow": "timestamp(ms, UTC)", "status": "proposed_unapproved"},
}
EXPECTED_COUNT = 7_097_772
EXPECTED_SCHEMA_SHA256 = "749193bd01b2b1410e27b79c3996e8525035fc61bd15c7143c750cb91740c467"
EXPECTED_RECORD_SHA256 = ""
EXPECTED_GEOMETRY_SHA256 = ""
EXPECTED_IDS_SHA256 = ""
PAGE_SIZE = 2000
MAX_BODY_BYTES = 6_000_000
NATIVE_FIELDS: tuple[tuple[str, str, bool, int | None], ...] = (
    ("OBJECTID", "esriFieldTypeOID", False, None),
    ("REGISTRY_ID", "esriFieldTypeString", False, 12),
    ("PRIMARY_NAME", "esriFieldTypeString", True, 150),
    ("LOCATION_ADDRESS", "esriFieldTypeString", True, 100),
    ("CITY_NAME", "esriFieldTypeString", True, 60),
    ("COUNTY_NAME", "esriFieldTypeString", True, 35),
    ("FIPS_CODE", "esriFieldTypeString", True, 5),
    ("STATE_CODE", "esriFieldTypeString", True, 5),
    ("POSTAL_CODE", "esriFieldTypeString", True, 14),
    ("LATITUDE83", "esriFieldTypeDouble", True, None),
    ("LONGITUDE83", "esriFieldTypeDouble", True, None),
    ("HUC8_CODE", "esriFieldTypeString", True, 8),
    ("ACCURACY_VALUE", "esriFieldTypeInteger", True, None),
    ("COLLECT_MTH_DESC", "esriFieldTypeString", True, 250),
    ("REF_POINT_DESC", "esriFieldTypeString", True, 250),
    ("CREATE_DATE", "esriFieldTypeDate", False, 8),
    ("UPDATE_DATE", "esriFieldTypeDate", True, 8),
    ("LAST_REPORTED_DATE", "esriFieldTypeDate", True, 8),
    ("FAC_URL", "esriFieldTypeString", True, 100),
    ("PGM_SYS_ID", "esriFieldTypeString", False, 30),
    ("PGM_SYS_ACRNM", "esriFieldTypeString", False, 20),
    ("INTEREST_TYPE", "esriFieldTypeString", False, 60),
    ("PROGRAM_URL", "esriFieldTypeString", True, 100),
    ("PGM_REPORT_URL", "esriFieldTypeString", True, 11),
    ("PUBLIC_IND", "esriFieldTypeString", True, 1),
    ("ACTIVE_STATUS", "esriFieldTypeString", True, 50),
    ("FEDERAL_AGENCY_NAME", "esriFieldTypeString", True, 100),
    ("HUC_12", "esriFieldTypeString", True, 12),
    ("FEDERAL_LAND_IND", "esriFieldTypeString", True, 1),
    ("FED_FACILITY_CODE", "esriFieldTypeString", True, 1),
    ("EPA_REGION_CODE", "esriFieldTypeString", True, 2),
    ("KEY_FIELD", "esriFieldTypeString", True, 50),
)
METADATA_NAMES = ("portal_item", "service_before", "layer_before", "count_before")


class EPAFRSInterestsError(ValueError):
    """The source snapshot cannot be represented as the pinned FRS_INTERESTS candidate."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _item_provenance(portal: Mapping[str, object]) -> dict[str, str]:
    """Return license and attribution claims declared by the exact EPA item."""
    license_info = html.unescape(str(portal.get("licenseInfo", "")))
    if ("EPA Data Licensing Agreement" not in license_info or "public" not in license_info.casefold()
            or LICENSE_URL not in license_info):
        raise EPAFRSInterestsError("item_license_declaration_missing_or_changed")
    attribution = portal.get("accessInformation")
    if not isinstance(attribution, str) or "EPA Geospatial" not in attribution:
        raise EPAFRSInterestsError("item_attribution_declaration_missing_or_changed")
    return {"license": LICENSE_URL, "attribution": attribution}


def _profile_schema(layer: Mapping[str, object]) -> tuple[list[dict[str, object]], str]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise EPAFRSInterestsError("source_field_roster_drift")
    plain = [_plain_field(x) for x in fields]
    return plain, _sha(_canonical(plain))


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    kind = field["type"]
    if kind == "esriFieldTypeOID":
        return pa.int64()
    if kind == "esriFieldTypeInteger":
        return pa.int32()
    if kind == "esriFieldTypeDouble":
        return pa.float64()
    if kind == "esriFieldTypeString":
        return pa.string()
    if kind == "esriFieldTypeDate":
        return pa.timestamp("ms", tz="UTC")
    raise EPAFRSInterestsError("unsupported_source_physical_type:" + str(kind))


def _validate_feature(feature: object, expected_names: tuple[str, ...]) -> None:
    if not isinstance(feature, dict):
        raise EPAFRSInterestsError("source_feature_shape_changed")
    attrs, geom = feature.get("attributes"), feature.get("geometry")
    if not isinstance(attrs, dict) or tuple(attrs.keys()) != expected_names or not isinstance(geom, dict) or set(geom) != {"x", "y"}:
        raise EPAFRSInterestsError("source_record_schema_or_geometry_drift")
    if type(attrs["OBJECTID"]) is not int:
        raise EPAFRSInterestsError("source_object_id_type_changed")
    if not all(isinstance(geom[k], (int, float)) for k in ("x", "y")):
        raise EPAFRSInterestsError("source_point_coordinate_invalid")
    for name, esri_type, nullable, _length in NATIVE_FIELDS:
        value = attrs[name]
        if value is None and not nullable:
            raise EPAFRSInterestsError("source_nonnullable_value_is_null:" + name)
        if value is None or isinstance(value, str):
            if esri_type != "esriFieldTypeString" and value is not None:
                raise EPAFRSInterestsError("source_native_type_changed:" + name)
        elif esri_type == "esriFieldTypeString" or (esri_type == "esriFieldTypeInteger" and type(value) is not int):
            raise EPAFRSInterestsError("source_native_type_changed:" + name)


def _page_url(last_objectid: int) -> str:
    return LAYER_URL + "/query?" + urlencode({
        "where": f"OBJECTID>{last_objectid}", "outFields": "*", "returnGeometry": "true",
        "outSR": "4269", "orderByFields": "OBJECTID ASC", "resultRecordCount": str(PAGE_SIZE),
        "f": "json",
    })


def _count_url() -> str:
    return LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnCountOnly": "true", "f": "json"})


def _expected_capture_names(count: int) -> list[str]:
    data_pages = (count + PAGE_SIZE - 1) // PAGE_SIZE
    return [*METADATA_NAMES, *(f"page_{index:06d}" for index in range(data_pages)),
            "page_tail", "layer_after", "service_after", "count_after"]


def _read_capture(capture_root: Path, name: str) -> tuple[bytes, dict[str, object]]:
    body_path = capture_root / f"{name}.body"
    receipt_path = capture_root / f"{name}.receipt.json"
    body, wire = body_path.read_bytes(), (capture_root / f"{name}.wire").read_bytes()
    receipt_body = receipt_path.read_bytes()
    try:
        receipt = json.loads(receipt_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EPAFRSInterestsError("retained_capture_receipt_invalid:" + name) from exc
    if (receipt.get("method") != "GET" or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise EPAFRSInterestsError("retained_capture_receipt_invalid:" + name)
    return body, receipt


def _load_state(capture_root: Path) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        return {"data_pages": [], "tail": None, "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EPAFRSInterestsError("capture_state_invalid") from exc
    if (not isinstance(state, dict) or not isinstance(state.get("data_pages"), list)
            or not (state.get("tail") is None or isinstance(state.get("tail"), dict))):
        raise EPAFRSInterestsError("capture_state_invalid")
    return state


def _save_state(capture_root: Path, state: Mapping[str, object]) -> None:
    tmp = capture_root / "capture_state.json.tmp"
    tmp.write_bytes(_canonical(dict(state)) + b"\n")
    tmp.replace(capture_root / "capture_state.json")


def _retained_or_fetch(client: httpx.Client | None, capture_root: Path, name: str,
                       url: str) -> tuple[bytes, dict[str, object]]:
    if (capture_root / f"{name}.body").exists():
        try:
            return _read_capture(capture_root, name)
        except EPAFRSInterestsError:
            if client is None:
                raise
            for suffix in ("body", "wire", "receipt.json"):
                (capture_root / f"{name}.{suffix}").unlink(missing_ok=True)
    if client is None:
        raise EPAFRSInterestsError("retained_capture_file_missing:" + name)
    return capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES)


def _load_capture_receipts(capture_root: Path) -> dict[str, tuple[bytes, dict[str, object]]]:
    return {name: _read_capture(capture_root, name) for name in _expected_capture_names(EXPECTED_COUNT)}


def _stream_validate(capture_root: Path, *, parquet_sink: Path | None) -> dict[str, object]:
    """Re-verify every retained page byte and optionally stream the native geoparquet."""
    docs = {name: json.loads(_read_capture(capture_root, name)[0]) for name in METADATA_NAMES}
    portal = docs["portal_item"]
    item_provenance = _item_provenance(portal)
    service, layer = docs["service_before"], docs["layer_before"]
    if (portal.get("id") != ITEM_ID or portal.get("owner") != "EPA_GEO"
            or portal.get("title") != ITEM_TITLE or portal.get("url") != SERVICE_URL
            or portal.get("access") != "public"):
        raise EPAFRSInterestsError("official_EPA_portal_ownership_or_publication_drift")
    if not isinstance(service, dict) or service.get("serviceItemId") != ITEM_ID:
        raise EPAFRSInterestsError("source_item_identity_drift")
    if not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME
               for x in service.get("layers", []) if isinstance(x, dict)):
        raise EPAFRSInterestsError("exact_layer_missing")
    if layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPoint":
        raise EPAFRSInterestsError("exact_layer_metadata_drift")
    spatial = layer.get("extent", {}).get("spatialReference", {})
    if spatial.get("latestWkid") != 4269:
        raise EPAFRSInterestsError("source_crs_drift")
    fields, schema_sha = _profile_schema(layer)
    if tuple((f["name"], f["type"], f["nullable"], f["length"]) for f in fields) != NATIVE_FIELDS:
        raise EPAFRSInterestsError("source_schema_drift")
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise EPAFRSInterestsError("source_schema_baseline_drift")
    if docs["count_before"].get("count") != EXPECTED_COUNT:
        raise EPAFRSInterestsError("source_count_drift")
    for name in ("layer_after", "service_after", "count_after"):
        body, _receipt = _read_capture(capture_root, name)
        mirror = {"layer_after": "layer_before", "service_after": "service_before",
                  "count_after": "count_before"}[name]
        if json.loads(body) != docs[mirror]:
            raise EPAFRSInterestsError("source_metadata_or_count_changed_during_capture")
    tail = json.loads(_read_capture(capture_root, "page_tail")[0])
    if tail.get("features") != []:
        raise EPAFRSInterestsError("source_tail_page_not_empty")
    after_fields = json.loads(_read_capture(capture_root, "layer_after")[0]).get("fields")
    if not isinstance(after_fields, list) or [_plain_field(x) for x in after_fields] != fields:
        raise EPAFRSInterestsError("source_field_schema_changed_during_capture")

    expected_names = tuple(f["name"] for f in fields)
    arrow_fields = [pa.field(f["name"], _arrow_type(f), nullable=f.get("nullable", True))
                    for f in fields]
    arrow_fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Point"],
        "crs": pyproj.CRS.from_epsg(4269).to_json_dict(), "edges": "planar",
    }}}
    schema = pa.schema(arrow_fields, metadata={b"geo": _canonical(geo)})
    sink_path = parquet_sink
    writer = (pq.ParquetWriter(sink_path, schema, compression="zstd")
              if sink_path is not None and not sink_path.exists() else None)
    if sink_path is not None:
        sink_path.parent.mkdir(parents=True, exist_ok=True)

    ids = array("q")
    record_hash, geometry_hash = hashlib.sha256(), hashlib.sha256()
    last_objectid = 0
    null_geometry_count = 0
    page_bodies = sorted(name for name in _expected_capture_names(EXPECTED_COUNT)
                         if name.startswith("page_") and name != "page_tail")
    for page_name in page_bodies:
        page = json.loads(_read_capture(capture_root, page_name)[0])
        if "error" in page:
            raise EPAFRSInterestsError("source_page_error")
        if page.get("spatialReference", {}).get("latestWkid") != 4269:
            raise EPAFRSInterestsError("source_page_spatial_reference_drift")
        features = page.get("features")
        if not isinstance(features, list) or len(features) > PAGE_SIZE:
            raise EPAFRSInterestsError("source_page_row_count_incomplete")
        columns: dict[str, list[object]] = {name: [] for name in expected_names}
        geometry: list[bytes] = []
        for feature in features:
            _validate_feature(feature, expected_names)
            attrs, geom = feature["attributes"], feature["geometry"]
            objectid = attrs["OBJECTID"]
            if objectid <= last_objectid:
                raise EPAFRSInterestsError("source_object_ids_not_strictly_increasing")
            last_objectid = objectid
            record_hash.update(_canonical(feature) + b"\x00")
            geometry_hash.update(_canonical([objectid, geom["x"], geom["y"]]) + b"\x00")
            ids.append(objectid)
            if geom.get("x") is None or geom.get("y") is None:
                null_geometry_count += 1
            for name in expected_names:
                columns[name].append(attrs[name])
            geometry.append(Point(geom["x"], geom["y"]).wkb)
        if writer is not None:
            batch = pa.Table.from_pydict({**columns, "geometry": geometry}, schema=schema)
            writer.write_table(batch)
    if writer is not None:
        writer.close()
    if len(ids) != EXPECTED_COUNT or last_objectid == 0:
        raise EPAFRSInterestsError("source_page_population_incomplete")
    ids_sha = _sha(_canonical(list(ids)))
    record_sha, geometry_sha = record_hash.hexdigest(), geometry_hash.hexdigest()
    if EXPECTED_RECORD_SHA256 and record_sha != EXPECTED_RECORD_SHA256:
        raise EPAFRSInterestsError("source_value_or_geometry_baseline_drift")
    if EXPECTED_GEOMETRY_SHA256 and geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise EPAFRSInterestsError("source_geometry_or_coverage_drift")
    if EXPECTED_IDS_SHA256 and ids_sha != EXPECTED_IDS_SHA256:
        raise EPAFRSInterestsError("source_id_roster_drift")
    return {"item_provenance": item_provenance, "fields": fields, "schema_sha": schema_sha,
            "record_sha": record_sha, "geometry_sha": geometry_sha, "ids_sha": ids_sha,
            "null_geometry_count": null_geometry_count, "parquet_path": sink_path}


def _code_identity() -> dict[str, object]:
    return processing_identity(Path(__file__).parents[3])


def _seal_candidate(storage: StagingStorageResource, capture_root: Path,
                    extracted: Mapping[str, object], *,
                    resumed_pages: int, first_captured_at: str, last_captured_at: str) -> dict[str, object]:
    identity = _code_identity()
    version = content_derived_version(extracted["record_sha"], extracted["geometry_sha"],
                                      extracted["schema_sha"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        parquet_body = Path(extracted["parquet_path"]).read_bytes()
        return _verify_sealed_candidate(
            storage, prefix, state_key, version, identity,
            {"source_record_sha256": extracted["record_sha"],
             "source_schema_sha256": extracted["schema_sha"],
             "source_geometry_sha256": extracted["geometry_sha"]},
            parquet_body)
    audit = {
        "source_feature_count": EXPECTED_COUNT, "source_id_sha256": extracted["ids_sha"],
        "source_record_sha256": extracted["record_sha"], "source_geometry_sha256": extracted["geometry_sha"],
        "source_schema_sha256": extracted["schema_sha"], "source_crs_epsg": 4269,
        "geometry_type": "Point", "null_geometry_count": extracted["null_geometry_count"],
        "unique_source_id_count": EXPECTED_COUNT, "portal_item_id": ITEM_ID,
        "portal_owner": "EPA_GEO", "portal_modified_epoch_ms": None,
        "portal_modified_is_source_release_date": False, "source_fields": extracted["fields"],
        "official_layer_name": LAYER_NAME, "arcgis_item_id": ITEM_ID,
        "item_license_declaration": extracted["item_provenance"]["license"],
        "item_license_declaration_field": "licenseInfo",
        "source_attribution": extracted["item_provenance"]["attribution"],
        "proposed_physical_type_mapping": PROPOSED_PHYSICAL_TYPE_MAPPING,
        "raw_source_values_preserved": True, "service_modified_is_not_release_date": True,
        "source_vintage": SOURCE_VINTAGE, "source_vintage_note": SOURCE_VINTAGE_NOTE,
        "pagination": {"mode": "objectid_keyset_asc", "page_size": PAGE_SIZE,
                       "pages": (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE,
                       "tail_witness_page": True, "resumable": True, "resumed_pages": resumed_pages},
        "capture_window_utc": {"first_page": first_captured_at, "last_page": last_captured_at},
        "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    }
    parquet_body = Path(extracted["parquet_path"]).read_bytes()
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    captures = _load_capture_receipts(capture_root)
    manifest = {
        "schema_version": 1, "approved": False, "title": LAYER_NAME,
        "description": ("Exact EPA_GEO hosted full-universe EPA FRS INTERESTS point layer. "
                        "Source release date, currentness, and any successor decision remain pending."),
        "publisher": PUBLISHER, "agency": "Environmental Protection Agency", "source_url": LAYER_URL,
        "license": extracted["item_provenance"]["license"],
        "license_evidence": {"source_item_id": ITEM_ID, "metadata_field": "licenseInfo",
                             "url": extracted["item_provenance"]["license"]},
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [
            {"input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
             "role": "producer_publication", "canonical_url": LAYER_URL,
             "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
             "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
             "bytes": receipt["bytes"], "sha256": receipt["sha256"], "license": extracted["item_provenance"]["license"],
             "license_unknown_reason": None,
             "license_evidence": {"source_item_id": ITEM_ID, "metadata_field": "licenseInfo",
                                  "url": extracted["item_provenance"]["license"]},
             "attribution": extracted["item_provenance"]["attribution"], "release_date": None,
             "release_date_unknown_reason": SOURCE_VINTAGE_NOTE}
            for name, (_body, receipt) in sorted(captures.items())
        ],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"feature_layer_id": LAYER_ID,
                                            "source_fields": "all 32 fields preserved in producer order and declared physical types",
                                            "source_ids": "Preserve each OBJECTID and REGISTRY_ID unchanged; all other source fields retained verbatim",
                                            "proposed_physical_type_mapping": PROPOSED_PHYSICAL_TYPE_MAPPING,
                                            "raw_source_values_preserved": True, "mapping_approved": False,
                                            "geometry": "Esri point x/y retained as WKB at EPSG:4269; native NAD83, no reprojection or repair",
                                            "candidate_status": "unapproved; production publication prohibited pending source freshness, schema/ID contract, rights and mapping acceptance"}}],
        "lineage": (f"EPA_GEO ArcGIS item {ITEM_ID} ({ITEM_TITLE}) serves exact FeatureServer layer {LAYER_ID} "
                    f"({LAYER_NAME}), the full universe of EPA FRS interests behind the individual program "
                    "layers. The item snippet states the service is updated weekly; item modification time "
                    "is not asserted as source release date."),
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": ("Native Esri query values, all attributes, IDs and source point geometry retained; "
                       "source vintage and release approvals remain unresolved."),
            "objects": [{"path": parquet_name, **source_objects[parquet_name]}]}}},
        "unsupported_output_formats": {"geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    ready = {"approved": False, "version": version, "objects": source_objects,
             "source_record_sha256": audit["source_record_sha256"], "freshness_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False,
             "rights_accepted": False, "mapping_approved": False, "release_accepted": False}

    outputs: dict[str, bytes] = {
        parquet_name: parquet_body,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical(ready) + b"\n",
    }
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    fingerprint = _sha(_canonical(identity))
    for relative, body in outputs.items():
        write_immutable(storage, f"{prefix}/{relative}", body)
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    payload = {"processing_identity": identity, "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {name: receipt for name, (body, receipt) in sorted(captures.items())},
               "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_null_geometry_count": extracted["null_geometry_count"],
            "source_invalid_geometry_count": 0, "reused": False,
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "source_release_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "freshness_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(capture_root)}


def _verify_sealed_candidate(storage: StagingStorageResource, prefix: str, state_key: str,
                             version: str, identity: Mapping[str, object], audit: Mapping[str, object],
                             current_parquet: bytes) -> dict[str, object]:
    fingerprint = _sha(_canonical(identity))
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity", "source_record_sha256",
            "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise EPAFRSInterestsError("cached_first_candidate_identity_or_source_changed")
    expected_objects = {name: descriptor for name, descriptor in payload["objects"].items()}
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                            fingerprint, identity, audit["source_record_sha256"],
                            audit["source_schema_sha256"], expected_objects,
                            payload["capture_receipts"])
    if expected_objects.get(f"geoparquet/{DATASET_SLUG}.parquet") != {"bytes": len(current_parquet), "sha256": _sha(current_parquet)}:
        raise EPAFRSInterestsError("cached_native_candidate_changed")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_null_geometry_count": 0,
            "source_invalid_geometry_count": 0, "reused": True,
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "source_release_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "freshness_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}


def _default_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=False, timeout=httpx.Timeout(15.0, read=45.0),
        headers={"Connection": "close", "User-Agent": "hifld-next-datasets/frs-interests-draft"},
    )


def fetch_frs_interests_pages(capture_root: Path, *, client: httpx.Client | None = None) -> dict[str, object]:
    """Retain every raw response for the pinned layer, resuming earlier progress.

    Pure-local when the capture directory already holds every required page.
    """
    capture_root = Path(capture_root).absolute()
    capture_root.mkdir(parents=True, exist_ok=True)
    state = _load_state(capture_root)
    pages: list[dict[str, object]] = list(state.get("data_pages", []))
    tail: dict[str, object] | None = state.get("tail")
    owner_client = client is None
    active = client or _default_client()
    first_captured_at = pages[0]["captured_at"] if pages else datetime.now(timezone.utc).isoformat()
    try:
        retained: dict[str, tuple[bytes, dict[str, object]]] = {}
        for name in METADATA_NAMES:
            url = {"portal_item": f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json",
                   "service_before": SERVICE_URL + "?f=pjson", "layer_before": LAYER_URL + "?f=pjson",
                   "count_before": _count_url()}[name]
            retained[name] = _retained_or_fetch(active, capture_root, name, url)
        count_before = json.loads(retained["count_before"][0]).get("count")
        if count_before != EXPECTED_COUNT:
            raise EPAFRSInterestsError("source_count_drift")
        last_objectid = pages[-1]["last_objectid"] if pages else 0
        while not (capture_root / "page_tail.body").exists():
            name = f"page_{len(pages):06d}"
            body, receipt = _retained_or_fetch(active, capture_root, name, _page_url(last_objectid))
            page = json.loads(body)
            if "error" in page:
                raise EPAFRSInterestsError("source_page_error")
            features = page.get("features")
            if not isinstance(features, list):
                raise EPAFRSInterestsError("source_page_missing_features")
            if not features:
                if last_objectid == 0:
                    raise EPAFRSInterestsError("source_page_population_incomplete")
                # An empty page in a data slot is the completeness witness: the same
                # query URL defines page_tail, so retain its exact bytes under that name.
                for suffix in ("body", "wire", "receipt.json"):
                    (capture_root / f"page_tail.{suffix}").write_bytes(
                        (capture_root / f"{name}.{suffix}").read_bytes())
                break
            for feature in features:
                _validate_feature(feature, tuple(f[0] for f in NATIVE_FIELDS))
                objectid = feature["attributes"]["OBJECTID"]
                if objectid <= last_objectid:
                    raise EPAFRSInterestsError("source_object_ids_not_strictly_increasing")
                last_objectid = objectid
            pages.append({"name": name, "last_objectid": last_objectid, "features": len(features),
                          "captured_at": receipt["observed_at"], "body_sha256": _sha(body)})
            _save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": False})
            if len(features) < PAGE_SIZE:
                tail_body, tail_receipt = _retained_or_fetch(active, capture_root, "page_tail",
                                                             _page_url(last_objectid))
                if json.loads(tail_body).get("features") != []:
                    raise EPAFRSInterestsError("source_tail_page_not_empty")
                tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                        "captured_at": tail_receipt["observed_at"], "body_sha256": _sha(tail_body)}
                _save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": False})
                break
            if len(pages) > (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE:
                raise EPAFRSInterestsError("source_page_count_exceeds_pinned_population")
        if tail is None and (capture_root / "page_tail.body").exists():
            tail_receipt = json.loads((capture_root / "page_tail.receipt.json").read_bytes())
            tail = {"name": "page_tail", "last_objectid": last_objectid, "features": 0,
                    "captured_at": tail_receipt["observed_at"]}
        if tail is None or not (capture_root / "page_tail.body").exists():
            raise EPAFRSInterestsError("source_tail_page_not_empty")
        for name, url in (("layer_after", LAYER_URL + "?f=pjson"),
                          ("service_after", SERVICE_URL + "?f=pjson"),
                          ("count_after", _count_url())):
            body, _receipt = _retained_or_fetch(active, capture_root, name, url)
            retained[name] = (body, _read_capture(capture_root, name)[1])
        if json.loads(retained["count_after"][0]).get("count") != count_before:
            raise EPAFRSInterestsError("source_metadata_or_count_changed_during_capture")
    except EPAFRSInterestsError:
        if owner_client:
            active.close()
        raise
    except Exception as exc:
        if owner_client:
            active.close()
        raise EPAFRSInterestsError("source_capture_failed:" + type(exc).__name__) from exc
    if owner_client:
        active.close()

    last_captured_at = str(tail["captured_at"])
    extracted = _stream_validate(capture_root, parquet_sink=capture_root / f"{FILE_SLUG}.parquet")
    (capture_root / "computed_baseline.json").write_bytes(_canonical({
        "source_record_sha256": extracted["record_sha"],
        "source_geometry_sha256": extracted["geometry_sha"],
        "source_ids_sha256": extracted["ids_sha"],
        "source_schema_sha256": extracted["schema_sha"],
        "source_feature_count": EXPECTED_COUNT,
        "resumed_pages": len(pages),
        "first_captured_at": first_captured_at, "last_captured_at": last_captured_at,
    }) + b"\n")
    _save_state(capture_root, {"data_pages": pages, "tail": tail, "complete": True})
    return {"capture_directory": str(capture_root), "pages": len(pages), "complete": True,
            "source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"]}


def capture_epa_frs_interests(storage: StagingStorageResource, *, version: str,
                              capture_directory: str | None = None) -> dict[str, object]:
    """Fetch (resumable), validate, and stage one complete unapproved source candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise EPAFRSInterestsError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise EPAFRSInterestsError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise EPAFRSInterestsError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000:
        raise EPAFRSInterestsError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/101/captures")
    capture_root = Path(capture_directory).absolute() if capture_directory else (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise EPAFRSInterestsError("unsafe_capture_directory")
    resumed_pages = len(_load_state(capture_root).get("data_pages", [])) if capture_root.exists() else 0
    try:
        fetch_frs_interests_pages(capture_root)
        state = _load_state(capture_root)
        captured = [page["captured_at"] for page in state["data_pages"]]
        extracted = _stream_validate(capture_root, parquet_sink=capture_root / f"{FILE_SLUG}.parquet")
        return _seal_candidate(storage, capture_root, extracted, resumed_pages=resumed_pages,
                               first_captured_at=captured[0], last_captured_at=str(state["tail"]["captured_at"]))
    except Exception as exc:
        captures: dict[str, tuple[bytes, dict[str, object]]] = {}
        for receipt_path in sorted(capture_root.glob("*.receipt.json")) if capture_root.exists() else []:
            name = receipt_path.name.removesuffix(".receipt.json")
            try:
                captures[name] = _read_capture(capture_root, name)
            except (OSError, EPAFRSInterestsError, ValueError):
                continue
        if captures:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, captures,
                                          getattr(exc, "code", type(exc).__name__) or type(exc).__name__)
        raise


def build_frs_interests_baseline(capture_root: str) -> dict[str, object]:
    """Recompute the content baseline from a completed capture (used to pin constants)."""
    extracted = _stream_validate(Path(capture_root).absolute(), parquet_sink=None)
    return {"source_record_sha256": extracted["record_sha"],
            "source_geometry_sha256": extracted["geometry_sha"],
            "source_ids_sha256": extracted["ids_sha"],
            "source_schema_sha256": extracted["schema_sha"]}
