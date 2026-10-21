"""Draft native capture for the EPA FRS SIC code table.

The HIFLD "FRS SIC" entry points at the EPA OEI FRS_INTERESTS service's
``FRS_SIC`` table. The producer serves the same table from both EPA-operated
endpoints: the native ArcGIS Server at geodata.epa.gov (``OEI/FRS_INTERESTS``
MapServer table 25, the URL referenced by the historical HIFLD item) and the
EPA_GEO-hosted FeatureServer on ArcGIS Online (table 2), which carries a stable
portal item identity. This capture reads the EPA_GEO FeatureServer table and
records the native server endpoint as the equivalent producer source. All
producer responses are retained verbatim; the Parquet candidate is an unapproved
preservation candidate pending contract review.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_arcgis_table import capture_table
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

ITEM_ID = "cdff193a3e3743a5bc770e2743f215b3"
SERVICE_URL = "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_INTERESTS/FeatureServer"
TABLE_ID = 2
TABLE_NAME = "FRS_SIC"
TABLE_URL = f"{SERVICE_URL}/{TABLE_ID}"
NATIVE_SERVER_TABLE_URL = "https://geodata.epa.gov/arcgis/rest/services/OEI/FRS_INTERESTS/MapServer/25"
DATASET_SLUG = "epa-frs-sic-codes"
FILE_SLUG = DATASET_SLUG
NATIVE_PATH = f"unknown/{FILE_SLUG}.parquet"
PUBLISHER = "U.S. Environmental Protection Agency, Facility Registry Service (EPA_GEO hosted FeatureServer)"
EPA_FRS_URL = "https://www.epa.gov/frs"
GEOSPATIAL_DOWNLOAD_URL = "https://www.epa.gov/frs/geospatial-data-download-service"
PAGE_SIZE = 2000
MAX_RESPONSE_BYTES = 32 * 1024 * 1024
EXPECTED_ROWS = 2_056_054
EXPECTED_FIELD_NAMES = (
    "OBJECTID", "PGM_SYS_ACRNM", "PGM_SYS_ID", "SIC_CODE", "PRIMARY_INDICATOR", "KEY_FIELD",
)
PORTAL_ITEM_TITLE = "Facility Registry Service (FRS) Interests"

PROFILE = ArcGISTableProfile(
    service_url=SERVICE_URL, table_id=TABLE_ID, table_name=TABLE_NAME,
    fields=(
        ("OBJECTID", "esriFieldTypeOID", False, None),
        ("PGM_SYS_ACRNM", "esriFieldTypeString", False, 20),
        ("PGM_SYS_ID", "esriFieldTypeString", False, 30),
        ("SIC_CODE", "esriFieldTypeString", False, 4),
        ("PRIMARY_INDICATOR", "esriFieldTypeString", False, 10),
        ("KEY_FIELD", "esriFieldTypeString", True, 50),
    ),
    relationship_name=None, related_table_id=None, relationship_key=None,
    relationship_cardinality=None,
    source_vintage=None, expected_rows=EXPECTED_ROWS,
    portal_item_id=ITEM_ID, portal_item_owner="EPA_GEO",
    portal_item_title=PORTAL_ITEM_TITLE,
    page_size=PAGE_SIZE, max_response_bytes=MAX_RESPONSE_BYTES,
)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
    table = pa.Table.from_pydict({name: [row[name] for row in rows] for name in PROFILE.field_names},
                                 schema=schema)
    out = pa.BufferOutputStream()
    pq.write_table(table, out, compression="zstd", version="2.6")
    return out.getvalue().to_pybytes()


def _row_count_from_candidate(storage: StagingStorageResource, prefix: str) -> int:
    parquet = storage.read_key(f"{prefix}/{NATIVE_PATH}")
    return pq.read_table(pa.BufferReader(parquet), columns=["OBJECTID"]).num_rows


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
            probe_body, probe_receipt = capture_get(client, current_capture_url, capture_root,
                                                    "count_probe", max_bytes=MAX_RESPONSE_BYTES)
            probe_count = json.loads(probe_body).get("count")
            if probe_count != EXPECTED_ROWS:
                raise ValueError("source_count_drift_before_capture")
            captures["count_probe"] = (probe_body, probe_receipt)
            pre_captures = dict(captures)
            rows, capture_manifest, table_captures = capture_table(client, PROFILE, capture_root)
            captures = {**pre_captures, **table_captures}
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
                receipt = {"method": "GET", "url": current_capture_url if name == "count_probe" else None,
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
         "release_date_unknown_reason": "ArcGIS modified timestamps are not source release dates."}
        for name, (_body, receipt) in sorted(captures.items())]
    release_manifest = {
        "schema_version": 1, "approved": False,
        "title": "EPA Facility Registry Service - SIC Codes",
        "description": ("EPA FRS_INTERESTS FeatureServer table 2 FRS_SIC: the standard industrial "
                        "classification codes attached to FRS program-system interests across the full "
                        "universe of FRS regulated facilities. The historical HIFLD Open item referenced "
                        "the equivalent producer table at " + NATIVE_SERVER_TABLE_URL + "."),
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
        "lineage": ["EPA OEI FRS_INTERESTS FeatureServer table 2: FRS_SIC",
                    "Equivalent native producer table: " + NATIVE_SERVER_TABLE_URL,
                    "EPA FRS geospatial data download service also publishes SIC classifications: "
                    + GEOSPATIAL_DOWNLOAD_URL,
                    "Portal item " + ITEM_ID + " is maintained by EPA (owner EPA_GEO); see " + EPA_FRS_URL],
        "prebuilt_output_policy": {"schema_version": 1,
                                   "formats": {"parquet": {"reason": "Native Esri fields, IDs and declared types retained; approvals pending.",
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
             "native_producer_server_table_url": NATIVE_SERVER_TABLE_URL,
             "arcgis_item_id": ITEM_ID, "publisher": PUBLISHER,
             "native_geometry": "absent", "source_relationship": "FRS-maintained EPA SIC code table",
             "refresh_status": "STOPPED",
             "publication_approval": "not_established_by_source_capture",
             "release_accepted": False, "source_release_accepted": False,
             "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False}
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
    version = content_derived_version(record_sha, "", schema_sha, identity)
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
