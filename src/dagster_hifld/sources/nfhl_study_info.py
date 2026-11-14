"""Lossless, approval-gated capture of the FEMA NFHL "Study_Info" table.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes nonspatial table 41
"Study_Info" as the producer's own publication of this HIFLD inventory dataset
(``nfhl/study-info``, DataLumos 239989 historical archive copy used for
identification only). The table is continuously updated and publishes no
per-snapshot release date; source freshness, rights, and public-schema
compatibility are not accepted by this draft.

The table is one FIRM Database record per row (study name, county, projection,
index/database revision dates and related metadata; the FIRM Database
Technical Reference documents this table). It carries no geometry, so the
candidate is attributes-only Parquet under ``unknown/`` with the producer's
31 fields in producer order and physical types, exactly as served; the
candidate fabricates no geometry column. Producer sentinel values are
retained verbatim and never coerced (observed on the 2026-11-12 complete
capture: numeric sentinels such as LANDWD_VAL/AVG_CFACTR -9999.0 and -8888.0,
text sentinels such as "NP", and 9999-12-31 epoch-millisecond date values;
31 producer rows also hold nulls in at least one field, per-column null
counts pinned below). Date fields are retained as the producer's raw epoch
milliseconds with no timezone or calendar interpretation.

The producer's ArcGIS Online registration of this service is item
``ae38b6f94eaf4abf97f986fa01921e13`` ("National Flood Hazard Layer", owned by
FEMA_MAC in FEMA's own "FEMA AGOL" org, URL equal to the REST service used
here, ``sublayer=41`` in the harvested HIFLD Open identity). The item is
captured as identification evidence and pinned by identity keys, never as a
data source.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

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

DATASET_SLUG = "nfhl-study-info"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
TABLE_ID = 41
TABLE_URL = f"{SERVICE_URL}/{TABLE_ID}"
TABLE_NAME = "Study_Info"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
PORTAL_ITEM_ID = "ae38b6f94eaf4abf97f986fa01921e13"
PORTAL_ITEM_OWNER = "FEMA_MAC"
PORTAL_ITEM_ORG_ID = "XG15cJAlne2vxtgt"
PORTAL_ITEM_ORG_NAME = "FEMA AGOL"
PORTAL_ITEM_TITLE = "National Flood Hazard Layer"
PAGE_SIZE = 2000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MIN_FREE_BYTES = 1_500_000_000
# The producer's MapServer intermittently answers valid requests with complete
# HTTP 200 Esri error documents or transport failures (observed 2026-11-12 on
# sibling NFHL layers; identical requests succeed moments later). Every
# request retries a bounded number of times for that transient mode; a retry
# re-issues the exact same request, and completeness is still enforced by the
# pinned before/after id rosters, counts, exact page id slices, and
# full-record digests, so retries can never mask drift.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
PAGE_CAPTURE_ATTEMPTS = 3
PAGE_CAPTURE_BASE_DELAY_SECONDS = 5.0
# Pins below are the observed 2026-11-12 complete-table population contract.
EXPECTED_COUNT = 2654
EXPECTED_ID_SHA256 = "9e0d2260d50ece4d9203ced212f586c9c233a8d485be1aa0217c4360b0090d34"
EXPECTED_RECORD_SHA256 = "24357a590c3bc65d161abe152c87d498ddf2e007d33234ea059332d12a45984c"
EXPECTED_SCHEMA_SHA256 = "5172f9eeb14cbf55809fb85a3b25b2af8cd9e6856d104b4f10bc6641a96bd37f"
EXPECTED_NULL_VALUE_TOTAL = 9076
# Per-column null counts observed on the pinned capture; producer nulls are
# retained verbatim, never imputed, dropped, or filled.
EXPECTED_NULL_VALUE_COUNTS = {
    "OBJECTID": 0, "DFIRM_ID": 1, "VERSION_ID": 1, "STD_NFO_ID": 17, "STUDY_PRE": 1468,
    "STUDY_NM": 4, "STATE_NM": 1, "CNTY_NM": 3, "JURIS_TYP": 22, "LG_PAN_NO": 3,
    "OPP_TF": 1, "H_DATUM": 1, "V_DATUM": 2, "PROJECTION": 3, "PROJ_ZONE": 21,
    "PROJ_UNIT": 9, "PROJ_SECND": 1392, "PROJ_SUNIT": 1390, "PROJ_SZONE": 1419,
    "LANDWD_VAL": 3, "CW_TF": 1, "RTROFT_TF": 2, "META_NM": 5, "FIS_NM": 6,
    "LOGO_NM": 17, "INDX_EFFDT": 2, "DBREV_DT": 731, "AVG_CFACTR": 9, "GFID": 471,
    "INDX_SUFFX": 2071, "GlobalID": 0,
}
EXPECTED_SERVICE_LAYER_ROSTER = [
    (0, "NFHL Availability", "Feature Layer"), (3, "FIRM Panels", "Feature Layer"),
    (1, "LOMRs", "Feature Layer"), (34, "LOMAs", "Feature Layer"),
    (22, "Political Jurisdictions", "Feature Layer"), (17, "Profile Baselines", "Feature Layer"),
    (20, "Water Lines", "Feature Layer"), (14, "Cross-Sections", "Feature Layer"),
    (16, "Base Flood Elevations", "Feature Layer"), (23, "Levees", "Feature Layer"),
    (29, "Seclusion Boundaries", "Feature Layer"), (15, "Coastal Transects", "Feature Layer"),
    (18, "Transect Baselines", "Feature Layer"), (24, "General Structures", "Feature Layer"),
    (7, "River Mile Markers", "Feature Layer"), (32, "Water Areas", "Feature Layer"),
    (5, "PLSS", "Feature Layer"), (19, "Limit of Moderate Wave Action", "Feature Layer"),
    (27, "Flood Hazard Boundaries", "Feature Layer"), (28, "Flood Hazard Zones", "Feature Layer"),
    (25, "Primary Frontal Dunes", "Feature Layer"), (4, "Base Index", "Feature Layer"),
    (6, "Topographic Low Confidence Areas", "Feature Layer"), (8, "Datum Conversion Points", "Feature Layer"),
    (9, "Coastal Gages", "Feature Layer"), (10, "Gages", "Feature Layer"),
    (11, "Nodes", "Feature Layer"), (12, "High Water Marks", "Feature Layer"),
    (13, "Station Start Points", "Feature Layer"), (26, "Hydrologic Reaches", "Feature Layer"),
    (30, "Alluvial Fans", "Feature Layer"), (31, "Subbasins", "Feature Layer"),
]
EXPECTED_SERVICE_TABLES_ROSTER = [(TABLE_ID, TABLE_NAME, "Table")]
EXPECTED_PORTAL_ITEM = {"id": PORTAL_ITEM_ID, "owner": PORTAL_ITEM_OWNER,
                        "orgId": PORTAL_ITEM_ORG_ID, "title": PORTAL_ITEM_TITLE,
                        "type": "Map Service", "url": SERVICE_URL}
EXPECTED_PORTAL_ORG = {"id": PORTAL_ITEM_ORG_ID, "name": PORTAL_ITEM_ORG_NAME}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _query(params: dict[str, object]) -> str:
    return TABLE_URL + "/query?" + urlencode(params)


def _capture_urls() -> list[tuple[str, str]]:
    return [
        ("ids_seed", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
        ("service_doc", SERVICE_DOC_URL),
        ("portal_item", "https://www.arcgis.com/sharing/rest/content/items/" + PORTAL_ITEM_ID + "?f=json"),
        ("portal_org", "https://www.arcgis.com/sharing/rest/portals/" + PORTAL_ITEM_ORG_ID + "?f=json"),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("table_before", TABLE_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ] + [
        (f"page_{offset // PAGE_SIZE:03d}", _query({
            "where": "1=1", "outFields": "*", "returnGeometry": "false",
            "orderByFields": "OBJECTID ASC", "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE, "f": "json"}))
        for offset in range(0, EXPECTED_COUNT, PAGE_SIZE)
    ] + [
        ("table_after", TABLE_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]


def _producer_error_doc(body: bytes) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        doc = json.loads(body)
    except ValueError:
        return False
    return isinstance(doc, dict) and "error" in doc


def _preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    for suffix in (".body", ".wire", ".receipt.json"):
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def _capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                        max_bytes: int, attempts: int, base_delay: float) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures.

    Retries cover the producer's transient complete-200 Esri error documents and
    incomplete transport captures; failed attempts are preserved on disk under
    ``<name>.attempt-NN`` evidence names and never enter the capture roster. The
    final attempt's body is always returned as captured, so a persistent
    producer failure surfaces to the caller unchanged.
    """
    for attempt in range(1, attempts + 1):
        try:
            body, receipt = capture_get(client, url, capture_root, name, max_bytes=max_bytes)
        except ValueError as exc:
            if attempt >= attempts or not str(exc).startswith("incomplete_source_capture:"):
                raise
            _preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        if _producer_error_doc(body) and attempt < attempts:
            _preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        return body, receipt
    raise AssertionError("unreachable_retry_state")


def _validate_spatial_reference_absent(payload: Mapping[str, object], label: str) -> None:
    spatial = payload.get("spatialReference")
    if spatial is not None:
        raise ValueError(label + "_unexpected_spatial_reference")


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]):
    docs = {name: json.loads(body) for name, (body, _) in captures.items() if name != "service_doc"}
    page_count = (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE
    expected_names = {"service_doc", "portal_item", "portal_org", "service_before", "table_before",
                      "count_before", "ids_before", "ids_seed",
                      "table_after", "service_after", "count_after", "ids_after",
                      *(f"page_{i:03d}" for i in range(page_count))}
    if set(captures) != expected_names:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    if SERVICE_URL.encode("utf-8") not in captures["service_doc"][0]:
        raise ValueError("fema_service_documentation_link_drift")
    item = docs["portal_item"]
    if not isinstance(item, dict) or {key: item.get(key) for key in EXPECTED_PORTAL_ITEM} != EXPECTED_PORTAL_ITEM:
        raise ValueError("fema_portal_service_item_identity_drift")
    org = docs["portal_org"]
    if not isinstance(org, dict) or {key: org.get(key) for key in EXPECTED_PORTAL_ORG} != EXPECTED_PORTAL_ORG:
        raise ValueError("fema_portal_org_identity_drift")
    service = docs["service_before"]
    if not isinstance(service, dict) or service.get("mapName") != "NFHL":
        raise ValueError("official_fema_nfhl_service_identity_drift")
    service_layers = service.get("layers")
    roster = [(x.get("id"), x.get("name"), x.get("type")) for x in service_layers if isinstance(x, dict)] \
        if isinstance(service_layers, list) else None
    if roster != EXPECTED_SERVICE_LAYER_ROSTER:
        raise ValueError("fema_nfhl_service_layer_roster_drift")
    service_tables = service.get("tables")
    tables_roster = [(x.get("id"), x.get("name"), x.get("type")) for x in service_tables if isinstance(x, dict)] \
        if isinstance(service_tables, list) else None
    if tables_roster != EXPECTED_SERVICE_TABLES_ROSTER:
        raise ValueError("fema_nfhl_service_tables_roster_drift")
    table = docs["table_before"]
    if (table.get("id") != TABLE_ID or table.get("name") != TABLE_NAME
            or table.get("type") != "Table" or table.get("geometryType") is not None):
        raise ValueError("exact_nfhl_study_info_table_metadata_drift")
    if table.get("maxRecordCount") != PAGE_SIZE:
        # PAGE_SIZE is pinned to the producer's declared per-request transfer
        # cap (2000); test fixtures override both together.
        raise ValueError("source_record_limit_drift")
    advanced = table.get("advancedQueryCapabilities", {})
    if advanced.get("supportsPagination") is not True or advanced.get("supportsOrderBy") is not True:
        raise ValueError("source_pagination_capability_changed")
    fields_value = table.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(x, dict) for x in fields_value):
        raise ValueError("source_field_roster_drift")
    fields = [_plain_field(x) for x in fields_value]
    field_names = [field["name"] for field in fields]
    schema_sha = _sha(_canonical(fields))
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_contract_drift")
    if docs["table_after"].get("fields") is None or [_plain_field(x) for x in docs["table_after"]["fields"]] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    if docs["table_after"] != table:
        raise ValueError("source_table_metadata_changed_during_capture")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    rosters = [docs[name].get("objectIds") for name in ("ids_before", "ids_seed", "ids_after")]
    if any(not isinstance(roster, list) or len(roster) != EXPECTED_COUNT
           or any(type(value) is not int for value in roster) or len(set(roster)) != EXPECTED_COUNT
           for roster in rosters):
        raise ValueError("source_id_population_drift")
    if any(sorted(roster) != sorted(rosters[0]) for roster in rosters[1:]):
        raise ValueError("source_id_population_drift")
    ids = sorted(rosters[0])
    if _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID" for name in ("ids_before", "ids_after", "ids_seed")):
        raise ValueError("source_id_field_changed")
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")

    esri_type_by_name = {field["name"]: field["type"] for field in fields}
    string_types = {"esriFieldTypeString", "esriFieldTypeGlobalID"}
    features = []
    null_counts = {name: 0 for name in field_names}
    for page_idx, offset in enumerate(range(0, EXPECTED_COUNT, PAGE_SIZE)):
        page = docs[f"page_{page_idx:03d}"]
        if not isinstance(page, dict) or "error" in page:
            raise ValueError("source_page_error")
        _validate_spatial_reference_absent(page, f"source_page_{page_idx:03d}")
        rows = page.get("features")
        if not isinstance(rows, list):
            raise ValueError("source_page_missing_features")
        expected = ids[offset:offset + PAGE_SIZE]
        actual = [r.get("attributes", {}).get("OBJECTID") for r in rows if isinstance(r, dict)]
        full_nonfinal = len(expected) == PAGE_SIZE and offset + PAGE_SIZE < EXPECTED_COUNT
        exceeded = page.get("exceededTransferLimit") is True
        if actual != expected or len(rows) != len(expected):
            raise ValueError("source_page_id_slice_incomplete_or_reordered")
        if exceeded and not full_nonfinal:
            raise ValueError("source_page_unexpected_exceeded_transfer_limit")
        for feature in rows:
            if not isinstance(feature, dict):
                raise ValueError("source_record_schema_drift")
            # The producer serves this dataset as a nonspatial table; any
            # geometry value at all would be an unexpected source change and is
            # quarantined, never silently accepted or stored.
            if "geometry" in feature:
                raise ValueError("source_table_returned_unexpected_geometry")
            attrs = feature.get("attributes")
            if not isinstance(attrs, dict) or list(attrs) != field_names:
                raise ValueError("source_record_attribute_schema_drift")
            for name in field_names:
                value = attrs[name]
                esri_type = esri_type_by_name[name]
                if value is None:
                    null_counts[name] += 1
                    continue
                if esri_type in string_types and not isinstance(value, str):
                    raise ValueError("source_native_string_type_changed")
                if esri_type in ("esriFieldTypeOID", "esriFieldTypeDate") and type(value) is not int:
                    # Date fields arrive as raw epoch-millisecond integers and
                    # stay integers; no calendar or timezone reinterpretation.
                    raise ValueError("source_native_integer_type_changed")
                if esri_type == "esriFieldTypeDouble" and (not isinstance(value, (int, float))
                                                            or isinstance(value, bool)):
                    raise ValueError("source_native_double_type_changed")
            features.append(feature)
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    record_sha = _sha(_canonical(features))
    if record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if null_counts != EXPECTED_NULL_VALUE_COUNTS:
        raise ValueError("source_null_value_population_drift")
    audit = {"source_row_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": record_sha, "source_schema_sha256": schema_sha,
             "null_value_counts": null_counts,
             "null_value_total": sum(null_counts.values()),
             "geometry": "absent",
             "unique_source_id_count": len(ids),
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "FEMA updates the NFHL continuously and publishes no release date for this table; "
                 "fetch time and the DataLumos archive modification date (2024-10-31) are not release dates."}
    return table, fields_value, {"features": features, "audit": audit, "ids": ids}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeGlobalID": pa.string(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDouble": pa.float64(),
            # Esri JSON delivers date fields as raw epoch milliseconds; the
            # producer's exact numeric wire values are retained verbatim as
            # int64 with no timezone or calendar interpretation applied.
            "esriFieldTypeDate": pa.int64()}[str(field["type"])]


def _parquet_bytes(fields: list[dict[str, object]],
                   features: list[dict[str, object]]) -> bytes:
    """Attributes-only Parquet with the producer's fields, order, types, and nulls."""
    columns = {}
    schema_fields = []
    for field in fields:
        name = field["name"]
        typ = _arrow_type(field)
        values = [row["attributes"].get(name) for row in features]
        columns[name] = pa.array(values, type=typ)
        schema_fields.append(pa.field(
            name, typ, nullable=True, metadata={b"original_esri_field": _canonical(_plain_field(field))}))
    schema = pa.schema(schema_fields, metadata={
        b"hifld:source_url": TABLE_URL.encode(),
        b"hifld:source_table": TABLE_NAME.encode(),
        # The producer publishes a nonspatial FIRM Database table; the
        # candidate is attributes-only and fabricates no geometry column.
        b"hifld:geometry": b"absent",
    })
    table = pa.Table.from_pydict(columns, schema=schema)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict[str, object]:
    return processing_identity(Path(__file__).parents[3])


def capture_nfhl_study_info(storage: StagingStorageResource, *, version: str,
                            capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL Study_Info candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/212/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/nfhl-study-info-draft"}) as client:
            for name, url in _capture_urls():
                body, receipt = _capture_with_retry(
                    client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                    attempts=PAGE_CAPTURE_ATTEMPTS if name.startswith("page_") else DOC_CAPTURE_ATTEMPTS,
                    base_delay=PAGE_CAPTURE_BASE_DELAY_SECONDS if name.startswith("page_")
                    else DOC_CAPTURE_BASE_DELAY_SECONDS)
                captures[name] = (body, receipt)
        table, source_fields, extracted = _validate_captures(captures)
    except Exception as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try:
                receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError):
                receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
        (capture_root / "capture_manifest.json").write_bytes(
            _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
                        "error_code": type(exc).__name__, "receipts": receipts}) + b"\n")
        retained = dict(captures)
        for receipt_path in capture_root.glob("*.receipt.json"):
            name = receipt_path.name.removesuffix(".receipt.json")
            if name in retained:
                continue
            try:
                retained[name] = ((capture_root / f"{name}.body").read_bytes(), json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, retained, type(exc).__name__)
        raise
    identity = _code_identity()
    parquet_body = _parquet_bytes(source_fields, extracted["features"])
    rel = f"unknown/{DATASET_SLUG}.parquet"
    source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    manifest = {
        "schema_version": 1, "approved": False, "title": "Study Info (FEMA NFHL)",
        "description": "FEMA National Flood Hazard Layer (NFHL) MapServer table 41 'Study_Info', captured "
                       "completely from the producer's own REST publication. The producer publishes a "
                       "nonspatial FIRM Database table (one study record per row); the candidate retains "
                       "all 31 producer fields in producer order and physical types, including the "
                       "INDX_EFFDT and DBREV_DT epoch-millisecond integers exactly as served, producer "
                       "sentinel values verbatim, and every producer null. The NFHL is continuously "
                       "updated and publishes no per-snapshot release date; freshness, rights, public "
                       "schema/ID contract, and release acceptance remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "Federal Emergency Management Agency", "source_url": TABLE_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication",
                    "canonical_url": SERVICE_DOC_URL if name == "service_doc" else
                    ("https://www.arcgis.com/sharing/rest/content/items/" + PORTAL_ITEM_ID
                     if name == "portal_item" else
                     ("https://www.arcgis.com/sharing/rest/portals/" + PORTAL_ITEM_ORG_ID
                      if name == "portal_org" else TABLE_URL)),
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason": "The NFHL is continuously updated; the service and its "
                                                   "documentation page publish no per-snapshot release date.",
                    "license": "Not stated by the captured endpoints; rights review pending.",
                    "attribution": "FEMA National Flood Hazard Layer; formal attribution terms require maintainer "
                                   "review."}
                   for name, (_, receipt) in sorted(captures.items())],
        "transformations": [{"operation": "native_esri_json_table_to_parquet_candidate",
                             "parameters": {"table_id": TABLE_ID,
                                            "source_fields": "all 31 producer fields retained in producer "
                                                             "order and physical types, including the "
                                                             "INDX_EFFDT and DBREV_DT epoch-millisecond "
                                                             "integers and the LANDWD_VAL/AVG_CFACTR doubles "
                                                             "with producer sentinel values exactly as served",
                                            "source_ids": "Preserve producer OBJECTID, GFID and GlobalID "
                                                          "values unchanged; no substitute IDs, casts, "
                                                          "trimming, or joins",
                                            "geometry": "The producer publishes no geometry for this table; "
                                                        "the candidate is attributes-only and fabricates no "
                                                        "geometry column",
                                            "candidate_status": "unapproved; publication blocked pending freshness, "
                                                                "schema/ID contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FEMA publishes the NFHL as its own geospatial database (fema.gov/flood-maps/national-flood-hazard-"
                   "layer). FEMA's MSC page 'GIS Web Services for the FEMA National Flood Hazard Layer (NFHL)' "
                   "documents this exact REST URL (https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"
                   "MapServer) as the ArcGIS REST service for effective NFHL data; its HTML is captured as "
                   "'service_doc' evidence. FEMA's own ArcGIS Online org (FEMA AGOL) registers that service as "
                   "item ae38b6f94eaf4abf97f986fa01921e13 owned by FEMA_MAC; the harvested HIFLD Open identity "
                   "for this dataset is that item with sublayer=41, which is exactly table 41 'Study_Info' of "
                   "the producer service. The HIFLD Open inventory lists this dataset as nfhl/study-info with "
                   "DataLumos project 239989; that ICPSR archive copy and the 2024-10-31 archive modification "
                   "date are identification and comparison context only, never pipeline inputs.",
        "unsupported_output_formats": {"geoparquet": "The producer publishes a nonspatial table with no "
                                                     "geometry; the geometry-free candidate is staged under "
                                                     "unknown/ and no geometry-bearing published mapping is "
                                                     "approved.",
                                       "shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "The source has no geometry; no display transformation "
                                                  "or generalized geometry is approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {**extracted["audit"], "source_fields": [_plain_field(f) for f in source_fields],
             "official_table_name": TABLE_NAME, "table_id": TABLE_ID, "service_url": SERVICE_URL,
             "release_date_unknown": True, "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
             "release_accepted": False}
    version = content_derived_version(audit["source_record_sha256"],
                                      hashlib.sha256(b"nonspatial").hexdigest(),
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    (capture_root / "capture_manifest.json").write_bytes(
        _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                    "source_row_count": EXPECTED_COUNT,
                    "source_record_sha256": audit["source_record_sha256"],
                    "receipts": {name: receipt for name, (_, receipt) in sorted(captures.items())}}) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs = {rel: parquet_body, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    outputs["metadata/candidate_ready.json"] = _canonical(
        {"approved": False, "version": version, "objects": source_objects,
         "source_record_sha256": audit["source_record_sha256"], "freshness_accepted": False,
         "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
         "mapping_approved": False, "release_accepted": False}) + b"\n"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("cached_first_candidate_invalid")
        objects = payload.get("objects", {})
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"], objects,
                                payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "row_count": EXPECTED_COUNT, "source_null_value_total": audit["null_value_total"],
                "geometry": "absent",
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}
    for relative, body in outputs.items():
        write_immutable(storage, f"{prefix}/{relative}", body)
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    for name, descriptor in descriptors.items():
        key = f"{prefix}/{name}"
        snap = storage.object_snapshot(key)
        if snap is None or snap.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] \
                or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_output_physical_readback_failed:" + name)
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {name: receipt for name, (_, receipt) in captures.items()},
               "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "row_count": EXPECTED_COUNT, "source_null_value_total": audit["null_value_total"],
            "geometry": "absent",
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}
