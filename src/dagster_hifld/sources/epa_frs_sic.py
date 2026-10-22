"""Draft native capture for the EPA FRS SIC code table.

The HIFLD "FRS SIC" entry points at the EPA OEI ``FRS_INTERESTS`` service's
``FRS_SIC`` table. The producer's native ArcGIS Server
(``https://geodata.epa.gov/arcgis/rest/services/OEI/FRS_INTERESTS/MapServer``,
table 25) serves the complete table including an untruncated ObjectID roster;
the EPA_GEO-hosted ArcGIS Online FeatureServer (table 2, item
``cdff193a3e3743a5bc770e2743f215b3``) carries the same table but caps
``returnIdsOnly`` responses at 1,000,000 IDs, so the native server is the
capture source. All producer responses are retained verbatim; the Parquet
candidate is an unapproved preservation candidate pending contract review.
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

SERVICE_URL = "https://geodata.epa.gov/arcgis/rest/services/OEI/FRS_INTERESTS/MapServer"
TABLE_ID = 25
TABLE_NAME = "FRS_SIC"
TABLE_URL = f"{SERVICE_URL}/{TABLE_ID}"
AGOL_SERVICE_URL = "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_INTERESTS/FeatureServer"
AGOL_ITEM_ID = "cdff193a3e3743a5bc770e2743f215b3"
AGOL_TABLE_URL = f"{AGOL_SERVICE_URL}/2"
DATASET_SLUG = "epa-frs-sic-codes"
FILE_SLUG = DATASET_SLUG
NATIVE_PATH = f"geoparquet/{FILE_SLUG}.parquet"
PUBLISHER = "U.S. Environmental Protection Agency, Facility Registry Service (OEI native ArcGIS Server)"
EPA_FRS_URL = "https://www.epa.gov/frs"
GEOSPATIAL_DOWNLOAD_URL = "https://www.epa.gov/frs/geospatial-data-download-service"
PAGE_SIZE = 5000
MAX_RESPONSE_BYTES = 32 * 1024 * 1024
EXPECTED_ROWS = 2_056_054
FIELDS = (
    ("OBJECTID", "esriFieldTypeOID", False, None),
    ("PGM_SYS_ACRNM", "esriFieldTypeString", False, 20),
    ("PGM_SYS_ID", "esriFieldTypeString", False, 30),
    ("SIC_CODE", "esriFieldTypeString", False, 4),
    ("PRIMARY_INDICATOR", "esriFieldTypeString", False, 10),
    ("KEY_FIELD", "esriFieldTypeString", True, 50),
)
FIELD_NAMES = tuple(field[0] for field in FIELDS)


class EpaFrsSicCaptureError(ValueError):
    """The source table is incomplete, malformed, or has drifted."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _plain_field(field: Mapping[str, object]) -> tuple:
    return (field.get("name"), field.get("type"), field.get("nullable"), field.get("length"))


def _typed_parquet(rows: list[dict], fields: list[dict]) -> bytes:
    arrow_fields = []
    for field in fields:
        name = field["name"]
        kind = pa.int64() if field["type"] == "esriFieldTypeOID" else pa.string()
        arrow_fields.append(pa.field(name, kind, nullable=bool(field.get("nullable", True)),
                                     metadata={b"original_esri_field": _canonical(field)}))
    schema = pa.schema(arrow_fields, metadata={
        b"hifld:source_url": TABLE_URL.encode(), b"hifld:source_table": TABLE_NAME.encode(),
        b"hifld:geometry": b"absent",
    })
    table = pa.Table.from_pydict({name: [row[name] for row in rows] for name in FIELD_NAMES},
                                 schema=schema)
    out = pa.BufferOutputStream()
    pq.write_table(table, out, compression="zstd", version="2.6")
    return out.getvalue().to_pybytes()


def _row_count_from_candidate(storage: StagingStorageResource, prefix: str) -> int:
    parquet = storage.read_key(f"{prefix}/{NATIVE_PATH}")
    return pq.read_table(pa.BufferReader(parquet), columns=["OBJECTID"]).num_rows


def _capture_table(client: httpx.Client, capture_root: Path) -> tuple[list[dict], dict,
        dict[str, tuple[bytes, dict]]]:
    """Fetch and retain exact GET responses, bracketing all rows with count/ID reads."""
    captures: dict[str, tuple[bytes, dict]] = {}

    def request(name: str, url: str, params: Mapping[str, object]) -> dict:
        request_url = url + "?" + urlencode(params)
        body, receipt = capture_get(client, request_url, capture_root, name,
                                    max_bytes=MAX_RESPONSE_BYTES)
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EpaFrsSicCaptureError("source_response_is_not_valid_json") from exc
        if not isinstance(payload, dict) or "error" in payload:
            raise EpaFrsSicCaptureError("source_response_has_arcgis_error")
        captures[name] = (body, receipt)
        return payload

    service = request("service", SERVICE_URL, {"f": "json"})
    if not isinstance(service.get("tables"), list) or not any(
            isinstance(item, dict) and item.get("id") == TABLE_ID and item.get("name") == TABLE_NAME
            for item in service["tables"]):
        raise EpaFrsSicCaptureError("source_service_table_identity_changed")
    table_before = request("table_before", TABLE_URL, {"f": "json"})
    if (table_before.get("name") != TABLE_NAME or table_before.get("type") != "Table"
            or table_before.get("geometryType") is not None):
        raise EpaFrsSicCaptureError("source_table_identity_or_geometry_changed")
    fields = table_before.get("fields")
    if not isinstance(fields, list) or tuple(_plain_field(f) for f in fields) != FIELDS:
        raise EpaFrsSicCaptureError("source_table_schema_changed")
    max_record_count = table_before.get("maxRecordCount")
    if not isinstance(max_record_count, int) or max_record_count < PAGE_SIZE:
        raise EpaFrsSicCaptureError("source_page_size_not_supported")
    count_query = {"where": "1=1", "returnCountOnly": "true", "f": "json"}
    ids_query = {"where": "1=1", "returnIdsOnly": "true", "f": "json"}
    count_before = request("count_before", TABLE_URL + "/query", count_query).get("count")
    ids_before = request("ids_before", TABLE_URL + "/query", ids_query).get("objectIds")
    if (count_before != EXPECTED_ROWS or not isinstance(ids_before, list)
            or len(ids_before) != count_before):
        raise EpaFrsSicCaptureError("source_count_and_id_roster_disagree_before_capture")
    if any(type(value) is not int for value in ids_before) or len(set(ids_before)) != count_before:
        raise EpaFrsSicCaptureError("source_object_id_roster_is_invalid")

    rows: list[dict] = []
    for page_number, offset in enumerate(range(0, count_before, PAGE_SIZE)):
        page = request(
            f"page_{page_number:03d}", TABLE_URL + "/query",
            {"where": "1=1", "outFields": ",".join(FIELD_NAMES), "returnGeometry": "false",
             "orderByFields": "OBJECTID ASC", "resultOffset": offset,
             "resultRecordCount": PAGE_SIZE, "f": "json"},
        )
        features = page.get("features")
        wanted = min(PAGE_SIZE, count_before - offset)
        if not isinstance(features, list) or len(features) != wanted:
            raise EpaFrsSicCaptureError("source_page_row_count_incomplete")
        if page.get("exceededTransferLimit") is True and wanted < PAGE_SIZE:
            raise EpaFrsSicCaptureError("source_final_page_exceeded_transfer_limit")
        for feature in features:
            if not isinstance(feature, dict) or feature.get("geometry") is not None:
                raise EpaFrsSicCaptureError("source_table_returned_unexpected_geometry")
            attrs = feature.get("attributes")
            if not isinstance(attrs, dict) or tuple(attrs.keys()) != FIELD_NAMES:
                raise EpaFrsSicCaptureError("source_record_fields_changed")
            if type(attrs["OBJECTID"]) is not int:
                raise EpaFrsSicCaptureError("source_object_id_type_changed")
            for name, _esri_type, nullable, _length in FIELDS:
                value = attrs[name]
                if value is None and not nullable:
                    raise EpaFrsSicCaptureError("source_nonnullable_value_is_null")
                if name != "OBJECTID" and value is not None and not isinstance(value, str):
                    raise EpaFrsSicCaptureError("source_native_string_type_changed")
            rows.append(attrs)

    table_after = request("table_after", TABLE_URL, {"f": "json"})
    service_after = request("service_after", SERVICE_URL, {"f": "json"})
    after_count = request("count_after", TABLE_URL + "/query", count_query).get("count")
    ids_after = request("ids_after", TABLE_URL + "/query", ids_query).get("objectIds")
    captured_ids = [row["OBJECTID"] for row in rows]
    before_ids = sorted(ids_before)
    if (tuple(_plain_field(f) for f in table_after.get("fields", [])) != FIELDS
            or table_after.get("relationships") != table_before.get("relationships")
            or service_after.get("description") != service.get("description")
            or service_after.get("tables") != service.get("tables")
            or after_count != count_before or not isinstance(ids_after, list)
            or sorted(ids_after) != before_ids or len(ids_after) != count_before
            or sorted(captured_ids) != before_ids or len(set(captured_ids)) != count_before):
        raise EpaFrsSicCaptureError("source_capture_incomplete_or_changed_during_read")
    manifest = {
        "source_url": TABLE_URL, "service_url": SERVICE_URL, "source_table": TABLE_NAME,
        "service_description": service.get("description"),
        "equivalent_official_distribution": {
            "service_url": AGOL_SERVICE_URL, "table_url": AGOL_TABLE_URL,
            "portal_item_id": AGOL_ITEM_ID, "portal_owner": "EPA_GEO",
            "ids_roster_limit": "returnIdsOnly capped at 1,000,000 IDs (exceededTransferLimit)",
        },
        "count_before": count_before, "count_after": after_count,
        "id_count_before": len(ids_before), "id_count_after": len(ids_after),
        "page_size": PAGE_SIZE, "page_count": len(range(0, count_before, PAGE_SIZE)),
        "rows_captured": len(rows), "all_ids_captured_once": True,
        "source_ids_equal_before_after": True,
        "fields": list(fields),
        "response_receipts": {name: receipt for name, (_body, receipt) in captures.items()},
    }
    return rows, manifest, captures


def _verify_first_candidate(storage: StagingStorageResource, prefix: str, state_key: str,
                            version: str, fingerprint: str, identity: Mapping[str, object],
                            record_sha: str, schema_sha: str,
                            expected_objects: Mapping[str, Mapping[str, object]],
                            receipts: Mapping[str, Mapping[str, object]]) -> dict:
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity", "source_record_sha256",
            "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != record_sha
            or payload["source_schema_sha256"] != schema_sha):
        raise ValueError("cached_first_candidate_identity_or_source_changed")
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
        fingerprint, identity, record_sha, schema_sha, payload["objects"], payload["capture_receipts"])
    if set(payload["objects"]) != set(expected_objects):
        raise ValueError("cached_candidate_roster_changed")
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "row_count": _row_count_from_candidate(storage, prefix),
            "source_record_sha256": record_sha, "source_schema_sha256": schema_sha,
            "source_population_complete": True,
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "reused": True, "source_release_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "freshness_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}


def capture_epa_frs_sic(storage: StagingStorageResource, *, version: str,
                        capture_directory: str | None = None) -> dict:
    """Acquire, validate and stage a complete unapproved FRS SIC table candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 4_000_000_000:
        raise ValueError("disk_floor_before_acquisition")
    capture_base = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/103/captures")
    capture_root = (capture_base / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict]] = {}
    current_capture_url = TABLE_URL + "/query?where=1%3D1&returnCountOnly=true&f=json"
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/epa-frs-sic-draft"}) as client:
            rows, capture_manifest, captures = _capture_table(client, capture_root)
    except Exception as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try: receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError): receipts[path.name] = {"receipt_readback_failed": True}
        partial_names = {p.name.removesuffix(".wire") for p in capture_root.glob("*.wire")}
        partial_names.update(p.name.removesuffix(".body") for p in capture_root.glob("*.body"))
        for name in sorted(partial_names - captures.keys()):
            body_path, wire_path = capture_root / f"{name}.body", capture_root / f"{name}.wire"
            body = body_path.read_bytes() if body_path.exists() else b""
            wire = wire_path.read_bytes() if wire_path.exists() else b""
            receipt = receipts.get(name)
            if not isinstance(receipt, dict):
                receipt = {"method": "GET", "url": current_capture_url,
                           "final_url": None, "observed_at": datetime.now(timezone.utc).isoformat(),
                           "status": None, "complete_body": False, "wire_bytes": len(wire),
                           "wire_sha256": _sha(wire), "bytes": len(body), "sha256": _sha(body),
                           "content_encoding": None, "content_length": None,
                           "error_code": type(exc).__name__}
            receipts[name] = receipt
            captures[name] = (body, receipt)
        (capture_root / "capture_manifest.json").write_bytes(_canonical({
            "captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
            "error_code": type(exc).__name__, "receipts": receipts}) + b"\n")
        if captures:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, captures,
                                          type(exc).__name__)
        raise
    identity = processing_identity(Path(__file__).parents[3])
    identity_fingerprint = _sha(_canonical(identity))
    record_sha = _sha(_canonical(rows))
    ids_sha = _sha(_canonical(sorted(row["OBJECTID"] for row in rows)))
    triplets_sha = _sha(_canonical(sorted([row["PGM_SYS_ACRNM"], row["PGM_SYS_ID"], row["SIC_CODE"],
                                           row["PRIMARY_INDICATOR"], row["KEY_FIELD"]] for row in rows)))
    schema_sha = _sha(_canonical(capture_manifest["fields"]))
    parquet = _typed_parquet(rows, capture_manifest["fields"])
    source_objects = {NATIVE_PATH: {"bytes": len(parquet), "sha256": _sha(parquet)}}
    inputs = [
        {"input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
         "role": "producer_publication", "canonical_url": TABLE_URL, "retrieval_url": receipt["url"],
         "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
         "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
         "license": "https://www.usa.gov/publicdomain/label/1.0/",
         "attribution": "EPA FRS; producer metadata states U.S. public domain; rights review remains pending.",
         "release_date": None,
         "release_date_unknown_reason": "ArcGIS service metadata carries no source release date."}
        for name, (_body, receipt) in sorted(captures.items())]
    release_manifest = {
        "schema_version": 1, "approved": False,
        "title": "EPA Facility Registry Service - SIC Codes",
        "description": ("EPA OEI FRS_INTERESTS MapServer table 25 FRS_SIC: the standard industrial "
                        "classification codes attached to FRS program-system interests across the full "
                        "universe of FRS regulated facilities. This is the producer table referenced by "
                        "the historical HIFLD Open 'FRS SIC' item."),
        "publisher": PUBLISHER, "agency": "Environmental Protection Agency", "source_url": TABLE_URL,
        "license": "https://www.usa.gov/publicdomain/label/1.0/", "source_modified": None,
        "date_issued": None, "date_modified": None, "processor": "HIFLD Next",
        "code_commit": identity["code_commit"], "inputs": inputs,
        "transformations": [{"operation": "native_esri_json_to_parquet_candidate",
                             "parameters": {"table_id": TABLE_ID,
                                            "source_fields": "all six source fields retained with declared types and nullability",
                                            "source_ids": "OBJECTID preserved unchanged",
                                            "geometry": "source is a nonspatial ArcGIS table; no geometry is invented",
                                            "candidate_status": "unapproved; production publication prohibited pending source freshness, schema/ID contract, rights and mapping acceptance"}}],
        "lineage": ["EPA OEI FRS_INTERESTS native ArcGIS Server table 25: FRS_SIC",
                    "Equivalent EPA_GEO ArcGIS Online distribution: " + AGOL_TABLE_URL
                    + " (item " + AGOL_ITEM_ID + "); its returnIdsOnly roster is capped at 1,000,000 IDs",
                    "EPA FRS geospatial data download service also publishes SIC classifications: "
                    + GEOSPATIAL_DOWNLOAD_URL,
                    "Facility Registry Service: " + EPA_FRS_URL],
        "prebuilt_output_policy": {"schema_version": 1,
                                   "formats": {"geoparquet": {"reason": "Native Esri fields, IDs and declared types retained; no geometry source exists and none is invented; approvals pending.",
                                                              "objects": [{"path": NATIVE_PATH, **source_objects[NATIVE_PATH]}]}}},
        "unsupported_output_formats": {"geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No geometry source is available."},
    }
    validate_release_provenance(release_manifest)
    validate_output_policy(release_manifest)
    audit = {**capture_manifest, "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG,
             "captured_at": min(receipt["observed_at"] for _, receipt in captures.values()),
             "source_record_sha256": record_sha, "source_schema_sha256": schema_sha,
             "source_ids_sha256": ids_sha, "source_triplets_sha256": triplets_sha,
             "row_count": len(rows), "source_population_complete": True,
             "arcgis_item_id": AGOL_ITEM_ID, "publisher": PUBLISHER,
             "native_geometry": "absent", "source_relationship": "FRS-maintained EPA SIC code table",
             "refresh_status": "STOPPED",
             "publication_approval": "not_established_by_source_capture",
             "release_accepted": False, "source_release_accepted": False,
             "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False}
    version = content_derived_version(record_sha, "", schema_sha, identity)
    ready = {"approved": False, "version": version, "source_record_sha256": record_sha,
             "source_schema_sha256": schema_sha, "objects": source_objects,
             "refresh_status": "STOPPED", "release_accepted": False}
    objects: dict[str, bytes] = {NATIVE_PATH: parquet,
                                 "metadata/source_manifest.json": _canonical(release_manifest) + b"\n",
                                 "metadata/source_audit.json": _canonical(audit) + b"\n",
                                 "metadata/processing_identity.json": _canonical(identity) + b"\n",
                                 "metadata/candidate_ready.json": _canonical(ready) + b"\n"}
    for name, (body, receipt) in sorted(captures.items()):
        for suffix, raw in (("body", body), ("wire", (capture_root / f"{name}.wire").read_bytes()),
                            ("receipt.json", _canonical(receipt) + b"\n")):
            objects[f"raw/{name}.{suffix}"] = raw
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in objects.items()}
    payload = {"processing_identity": identity, "source_record_sha256": record_sha,
               "source_schema_sha256": schema_sha,
               "capture_receipts": {name: receipt for name, (_body, receipt) in captures.items()},
               "objects": descriptors}
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        return _verify_first_candidate(storage, prefix, state_key, version, identity_fingerprint,
                                       identity, record_sha, schema_sha, descriptors,
                                       payload["capture_receipts"])
    try:
        for name, body in objects.items():
            write_immutable(storage, f"{prefix}/{name}", body)
        for name, descriptor in descriptors.items():
            key = f"{prefix}/{name}"
            snapshot = storage.object_snapshot(key)
            if (snapshot is None or snapshot.size != descriptor["bytes"]
                    or storage.sha256_key(key) != descriptor["sha256"]):
                raise ValueError("candidate_output_physical_readback_failed:" + name)
        seal_ready_candidate(storage, state_key, version, identity_fingerprint, payload)
    except Exception as exc:
        quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, captures,
                                      getattr(exc, "code", type(exc).__name__))
        raise
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    (capture_root / "capture_manifest.json").write_bytes(_canonical({
        "captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
        "row_count": len(rows), "source_record_sha256": record_sha,
        "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())}}) + b"\n")
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "row_count": len(rows), "source_population_complete": True,
            "source_record_sha256": record_sha, "source_schema_sha256": schema_sha,
            "source_ids_sha256": ids_sha, "source_triplets_sha256": triplets_sha,
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "capture_directory": str(capture_root), "reused": False,
            "source_release_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "freshness_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}
