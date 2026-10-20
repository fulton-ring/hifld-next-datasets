"""EPA FRS interest-contact table; retained as a nonspatial stopped draft."""
from __future__ import annotations

import hashlib
import json
import shutil
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Field,
                     DefaultScheduleStatus, ScheduleDefinition, asset, asset_check,
                     define_asset_job)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    seal_ready_candidate, write_immutable,
)
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.sources.epa_frs_contact_stream import (
    EXPECTED_ROWS, SERVICE_URL, TABLE_NAME, TABLE_URL, capture_to_parquet, canonical,
    compact_capture_receipt_index,
)


DATASET_SLUG = "epa-frs-interest-contact"
FILE_SLUG = DATASET_SLUG
EXPECTED_FIELDS = (
    "OBJECTID", "PGM_SYS_ACRNM", "PGM_SYS_ID", "FULL_NAME", "PHONE_NUMBER",
    "MAILING_ADDRESS", "SUPPLEMENTAL_ADDRESS", "CITY_NAME", "STATE_CODE",
    "POSTAL_CODE", "KEY_FIELD",
)
SOURCE_METADATA = {
    "title": "EPA Facility Registry Service Interest Contacts",
    "description": (
        "Native nonspatial FRS_INTEREST_CONTACT table from EPA OEI's FRS_INTERESTS "
        "MapServer. EPA service metadata links an EDG file-geodatabase package and "
        "says the package refreshes with the service. No source release date or "
        "vintage is established by the available metadata."
    ),
    "agency": "Environmental Protection Agency",
    "office": "Office of Environmental Information",
    "publisher": "U.S. Environmental Protection Agency, Office of Environmental Information",
    "source_url": TABLE_URL,
    "license": None,
    "source_modified": None,
    "date_issued": None,
    "date_modified": None,
    "source_vintage": None,
    "lineage": [
        "EPA OEI FRS_INTERESTS MapServer table 27: FRS_INTEREST_CONTACT",
        "EPA FRS data sources: https://www.epa.gov/frs/frs-data-sources",
        "EPA EDG package: https://edg.epa.gov/data/Public/OEI/FRS/FRS_Interests_Download.zip",
        "Historical DataLumos inventory: https://www.datalumos.org/datalumos/project/239255/version/V1/view",
    ],
}


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _upload_immutable_file(storage: StagingStorageResource, path: Path, key: str) -> dict[str, object]:
    """Upload a bounded-memory artifact and verify any content-addressed reuse."""
    expected_sha = _sha(path)
    expected_size = path.stat().st_size
    before = storage.object_snapshot(key)
    if before is not None:
        if before.size != expected_size or storage.sha256_key(key) != expected_sha:
            raise ValueError("immutable_candidate_object_changed:" + key)
        if storage.object_snapshot(key) != before:
            raise ValueError("candidate_object_changed_during_reuse:" + key)
    else:
        storage.upload_local_file(path, key)
        after = storage.object_snapshot(key)
        if (after is None or after.size != expected_size or storage.sha256_key(key) != expected_sha
                or storage.object_snapshot(key) != after):
            raise ValueError("uploaded_candidate_object_integrity_failed:" + key)
    return {"bytes": expected_size, "sha256": expected_sha}


def _quarantine(storage: StagingStorageResource, capture_root: Path, *, reason: str) -> str:
    prefix = f"_quarantine/{DATASET_SLUG}/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}-{uuid4().hex}"
    for path in (sorted(capture_root.glob("*.wire.gz"))
                 + sorted(capture_root.glob("*.wire"))
                 + sorted(capture_root.glob("*.receipt.json"))):
        storage.upload_local_file(path, f"{prefix}/raw/{path.name}")
    write_immutable(storage, f"{prefix}/metadata/failure.json", canonical({
        "reason_code": reason, "captured_objects": sorted(path.name for path in capture_root.iterdir()
                                                            if path.is_file() and path.name != "id-roster.sqlite")}) + b"\n")
    return prefix


def _recover_retained_candidate(storage: StagingStorageResource, old_prefix: str,
                                identity: dict[str, object]) -> dict[str, object]:
    """Seal an exact complete retained capture under this code identity, offline.

    This path is intentionally explicit in asset config. It verifies source
    receipts, object snapshots, the Parquet footer, and completeness metadata;
    the source observation timestamp and receipt bytes remain unchanged.
    """
    expected_prefix = f"{DATASET_SLUG}/{FILE_SLUG}/"
    if (not old_prefix.startswith(expected_prefix) or "/" in old_prefix[len(expected_prefix):]
            or not old_prefix[len(expected_prefix):].startswith("v")):
        raise ValueError("resume_candidate_prefix_invalid")
    capture = json.loads(storage.read_key(f"{old_prefix}/metadata/source_capture.json"))
    ready = json.loads(storage.read_key(f"{old_prefix}/metadata/candidate_ready.json"))
    roster = json.loads(storage.read_key(f"{old_prefix}/metadata/source_id_roster_summary.json"))
    old_identity = json.loads(storage.read_key(f"{old_prefix}/metadata/processing_identity.json"))
    if (not capture.get("captured_at") or capture.get("source_complete") is False
            or capture.get("row_count") != EXPECTED_ROWS
            or capture.get("source_count_before") != EXPECTED_ROWS
            or capture.get("source_count_after") != EXPECTED_ROWS
            or capture.get("id_count_before") != EXPECTED_ROWS
            or capture.get("id_count_after") != EXPECTED_ROWS
            or capture.get("id_roster_equal_before_after") is not True
            or capture.get("captured_ids_match_roster") is not True
            or capture.get("source_release_date") is not None
            or ready.get("approved") is not False
            or ready.get("version") != old_prefix.rsplit("/", 1)[-1]
            or ready.get("source_record_sha256") != capture.get("source_record_sha256")
            or ready.get("source_schema_sha256") != capture.get("source_schema_sha256")
            or roster != {"count": EXPECTED_ROWS, "sha256": capture.get("source_ids_sha256"), "complete": True}
            or not isinstance(old_identity, dict)):
        raise ValueError("resume_candidate_completeness_witness_invalid")
    version = content_derived_version(capture["source_record_sha256"],
        hashlib.sha256(b"nonspatial").hexdigest(), capture["source_schema_sha256"], identity)
    current_prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    if old_prefix == current_prefix:
        # A previously sealed candidate is the receipt of its original
        # successful observation. Reuse its first construction after catalog
        # without reading or decoding the population-sized wire archive again.
        state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
        state_header = json.loads(storage.read_key(state_key))
        bundle = json.loads(load_first_construction(
            storage, state_key, version, state_header["processing_fingerprint"]))
        payload = bundle.get("payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("capture_receipts"), dict):
            raise ValueError("resume_candidate_sealed_receipt_index_invalid")
        result = _reuse_existing_contact_candidate(
            storage, Path(__file__).parents[3], current_prefix, version, identity,
            {key: capture[key] for key in ("row_count", "source_record_sha256",
                                           "source_schema_sha256", "source_ids_sha256")},
            payload["capture_receipts"])
        if result is None:
            raise ValueError("resume_candidate_sealed_state_missing")
        return {**result, "resumed_offline": True}
    names = (
        "raw/source-wire-capture.tar", f"unknown/{FILE_SLUG}.parquet",
        "metadata/source_id_roster_summary.json", "metadata/source_capture.json",
        "metadata/source_manifest.json", "metadata/source_audit.json",
        "metadata/processing_identity.json", "metadata/candidate_ready.json",
    )
    expected_keys = {f"{old_prefix}/{name}" for name in names}
    if set(storage.list_prefix(old_prefix)) != expected_keys:
        raise ValueError("resume_candidate_storage_roster_changed")
    snapshots = {}
    for name in names:
        key = f"{old_prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if snapshot is None:
            raise ValueError("resume_candidate_object_missing")
        sha = storage.sha256_key(key)
        if storage.object_snapshot(key) != snapshot:
            raise ValueError("resume_candidate_object_changed_during_verify")
        snapshots[name] = {"bytes": snapshot.size, "sha256": sha}

    if ready.get("objects") != {f"unknown/{FILE_SLUG}.parquet":
                                  snapshots[f"unknown/{FILE_SLUG}.parquet"]}:
        raise ValueError("resume_candidate_completeness_witness_invalid")

    capture_root = Path(tempfile.mkdtemp(prefix="frs-contact-resume-", dir="/tmp"))
    try:
        archive_path = capture_root / "source-wire-capture.tar"
        parquet_path = capture_root / f"{FILE_SLUG}.parquet"
        storage.download_key_to(f"{old_prefix}/raw/source-wire-capture.tar", archive_path)
        storage.download_key_to(f"{old_prefix}/unknown/{FILE_SLUG}.parquet", parquet_path)
        receipts = []
        with tarfile.open(archive_path, "r:") as archive:
            for member in archive.getmembers():
                if member.name.endswith(".receipt.json"):
                    stream = archive.extractfile(member)
                    if stream is None:
                        raise ValueError("resume_candidate_receipt_missing")
                    receipts.append(json.loads(stream.read()))
        capture_index = compact_capture_receipt_index(receipts, archive_path)
        if (capture_index["receipt_count"] != capture.get("response_count")
                or capture_index["page_count"] != capture.get("page_count")
                or capture_index["page_count"] <= 0):
            raise ValueError("resume_candidate_receipt_summary_changed")
        parquet = pq.ParquetFile(parquet_path)
        if (parquet.metadata.num_rows != EXPECTED_ROWS
                or tuple(parquet.schema_arrow.names) != EXPECTED_FIELDS
                or parquet.schema_arrow.field("OBJECTID").type != pa.int64()
                or any(parquet.schema_arrow.field(name).type != pa.string()
                       for name in EXPECTED_FIELDS[1:])):
            raise ValueError("resume_candidate_parquet_schema_or_count_changed")

        version = content_derived_version(capture["source_record_sha256"],
            hashlib.sha256(b"nonspatial").hexdigest(), capture["source_schema_sha256"], identity)
        prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
        output_keys = {f"{prefix}/{name}" for name in names}
        if set(storage.list_prefix(prefix)) - output_keys:
            raise ValueError("resume_candidate_output_roster_changed")
        objects = {}
        replaced_metadata = {"metadata/candidate_ready.json", "metadata/processing_identity.json"}
        for name in names:
            if name in replaced_metadata:
                continue
            destination_key = f"{prefix}/{name}"
            source_key = f"{old_prefix}/{name}"
            if storage.object_snapshot(destination_key) is None:
                storage.copy_key_to(storage, source_key, destination_key)
            source_after = storage.object_snapshot(source_key)
            if (source_after is None or source_after.size != snapshots[name]["bytes"]
                    or storage.sha256_key(source_key) != snapshots[name]["sha256"]
                    or storage.object_snapshot(source_key) != source_after):
                raise ValueError("resume_candidate_source_snapshot_changed")
            destination = storage.object_snapshot(destination_key)
            if (destination is None or destination.size != snapshots[name]["bytes"]
                    or storage.sha256_key(destination_key) != snapshots[name]["sha256"]
                    or storage.object_snapshot(destination_key) != destination):
                raise ValueError("resume_candidate_copied_object_integrity_failed")
            objects[name] = snapshots[name]
        ready_key = f"{prefix}/metadata/candidate_ready.json"
        ready_body = canonical({**ready, "version": version}) + b"\n"
        objects["metadata/candidate_ready.json"] = {
            "bytes": len(ready_body), "sha256": hashlib.sha256(ready_body).hexdigest()}
        write_immutable(storage, ready_key, ready_body)
        # New construction identity is separate from the original observation.
        identity_body = canonical(identity) + b"\n"
        objects["metadata/processing_identity.json"] = {
            "bytes": len(identity_body), "sha256": hashlib.sha256(identity_body).hexdigest()}
        write_immutable(storage, f"{prefix}/metadata/processing_identity.json", identity_body)
        if set(storage.list_prefix(prefix)) != output_keys:
            raise ValueError("resume_candidate_output_roster_changed")
        payload = {"processing_identity": identity,
                   "source_record_sha256": capture["source_record_sha256"],
                   "source_schema_sha256": capture["source_schema_sha256"],
                   "capture_receipts": capture_index, "objects": objects}
        fingerprint = hashlib.sha256(canonical(payload)).hexdigest()
        seal_ready_candidate(storage, f"_construction-state/{DATASET_SLUG}/{version}.json",
                             version, fingerprint, payload)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "row_count": EXPECTED_ROWS, "source_record_sha256": capture["source_record_sha256"],
                "source_schema_sha256": capture["source_schema_sha256"],
                "source_ids_sha256": capture["source_ids_sha256"],
                "native_fields": list(EXPECTED_FIELDS), "source_vintage": None,
                "captured_at": capture["captured_at"], "refresh_status": "STOPPED",
                "drift_reasons": ["source_release_date_unknown", "license_unverified",
                                   "historical_published_membership_not_yet_compared"],
                "parquet_key": f"{prefix}/unknown/{FILE_SLUG}.parquet",
                "candidate_ready": ready_key, "reused": False, "resumed_offline": True,
                "release_accepted": False}
    finally:
        shutil.rmtree(capture_root, ignore_errors=True)


def _reuse_existing_contact_candidate(storage: StagingStorageResource, repo_root: Path,
                                      prefix: str, version: str, identity: dict[str, object],
                                      observed: dict[str, object],
                                      capture_index: dict[str, object]) -> dict[str, object] | None:
    """Strictly reuse the first sealed candidate after a same-content observation."""
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    state_snapshot = storage.object_snapshot(state_key)
    if state_snapshot is None:
        return None
    state_body = storage.read_key(state_key)
    state_header = json.loads(state_body)
    state = json.loads(load_first_construction(storage, state_key, version,
                                               state_header["processing_fingerprint"]))
    if storage.object_snapshot(state_key) != state_snapshot:
        raise ValueError("cached_contact_candidate_state_changed")
    payload = state.get("payload")
    if (not isinstance(payload, dict) or payload.get("processing_identity") != identity
            or payload.get("source_record_sha256") != observed.get("source_record_sha256")
            or payload.get("source_schema_sha256") != observed.get("source_schema_sha256")
            or payload.get("capture_receipts") != capture_index
            or not isinstance(payload.get("objects"), dict)):
        raise ValueError("cached_contact_candidate_identity_or_source_changed")
    objects = payload["objects"]
    expected = {f"{prefix}/{name}" for name in objects}
    derived = {"metadata/quality_manifest.json", "metadata/data_dictionary.json",
               "metadata/resolved_provenance.json", "metadata/source/quality_manifest.json",
               "metadata/source/data_dictionary.json"}
    manifest_descriptor = objects.get("metadata/source_manifest.json")
    snapshots = set()
    if manifest_descriptor:
        snapshots = {"metadata/source/source_manifest.json",
                     f"metadata/source/version_{manifest_descriptor['sha256']}_source_manifest.json"}
    allowed = expected | {f"{prefix}/{name}" for name in derived | snapshots}
    actual = set(storage.list_prefix(prefix))
    if not expected <= actual or actual - allowed:
        raise ValueError("cached_contact_candidate_storage_roster_changed")
    for name, descriptor in objects.items():
        key = f"{prefix}/{name}"
        snap = storage.object_snapshot(key)
        if (snap is None or descriptor != {"bytes": snap.size, "sha256": storage.sha256_key(key)}
                or storage.object_snapshot(key) != snap):
            raise ValueError("cached_contact_candidate_object_changed")
    for name in snapshots:
        key = f"{prefix}/{name}"
        if key in actual:
            snap = storage.object_snapshot(key)
            if (snap is None or manifest_descriptor != {"bytes": snap.size,
                    "sha256": storage.sha256_key(key)} or storage.object_snapshot(key) != snap):
                raise ValueError("cached_contact_candidate_catalog_snapshot_changed")
    manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_capture.json"))
    if (manifest.get("source_ids_sha256") != observed.get("source_ids_sha256")
            or manifest.get("row_count") != observed.get("row_count")):
        raise ValueError("cached_contact_candidate_observation_changed")
    from dagster_hifld.construction_provenance import validate_construction_code_identity
    validate_construction_code_identity(repo_root, identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "row_count": observed["row_count"],
            "source_record_sha256": observed["source_record_sha256"],
            "source_schema_sha256": observed["source_schema_sha256"],
            "source_ids_sha256": observed["source_ids_sha256"],
            "native_fields": list(EXPECTED_FIELDS), "source_vintage": None,
            "captured_at": manifest["captured_at"], "refresh_status": "STOPPED",
            "drift_reasons": ["source_release_date_unknown", "license_unverified",
                               "historical_published_membership_not_yet_compared"],
            "parquet_key": f"{prefix}/unknown/{FILE_SLUG}.parquet",
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "reused": True, "release_accepted": False}


def capture_epa_frs_contact(staging_storage: StagingStorageResource) -> dict[str, object]:
    """Read EPA pages with bounded heap and preserve compressed wire responses."""
    repo_root = Path(__file__).parents[3]
    source_manifest = SOURCE_METADATA
    identity = processing_identity(repo_root)
    capture_root = Path(tempfile.mkdtemp(prefix="frs-contact-", dir="/tmp"))
    parquet_path = capture_root / f"{FILE_SLUG}.parquet"
    try:
        with httpx.Client(timeout=httpx.Timeout(60.0, connect=20.0),
                          headers={"Accept-Encoding": "gzip"}, follow_redirects=False) as client:
            observed = capture_to_parquet(client, capture_root, parquet_path)
        if tuple(field["name"] for field in observed["fields"]) != EXPECTED_FIELDS:
            raise ValueError("source_schema_changed")

        geometry_sha = hashlib.sha256(b"nonspatial").hexdigest()
        version = content_derived_version(observed["source_record_sha256"], geometry_sha,
                                          observed["source_schema_sha256"], identity)
        prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
        page_receipts = observed["page_receipts"]
        metadata_receipts = observed["metadata_receipts"]
        wire_entries: list[dict[str, object]] = []
        objects: dict[str, dict[str, object]] = {}

        # A single tar keeps raw response bytes lossless and avoids thousands of
        # small object writes. Each member remains a separate named HTTP response.
        raw_tar = capture_root / "source-wire-capture.tar"
        with tarfile.open(raw_tar, "w") as archive:
            for receipt in metadata_receipts + page_receipts:
                name = str(receipt["name"])
                for suffix in ("wire.gz", "receipt.json"):
                    path = capture_root / f"{name}.{suffix}"
                    archive.add(path, arcname=f"{name}.{suffix}", recursive=False)
                wire_entries.append({"name": name, "wire_bytes": receipt["wire_bytes"],
                                     "wire_sha256": receipt["wire_sha256"],
                                     "decoded_bytes": receipt["decoded_bytes"],
                                     "decoded_sha256": receipt["decoded_sha256"]})
        capture_index = compact_capture_receipt_index(metadata_receipts + page_receipts, raw_tar)
        cached = _reuse_existing_contact_candidate(staging_storage, repo_root, prefix, version,
                                                  identity, observed, capture_index)
        if cached is not None:
            return cached
        objects[f"raw/source-wire-capture.tar"] = _upload_immutable_file(
            staging_storage, raw_tar, f"{prefix}/raw/source-wire-capture.tar")
        objects[f"unknown/{FILE_SLUG}.parquet"] = _upload_immutable_file(
            staging_storage, parquet_path, f"{prefix}/unknown/{FILE_SLUG}.parquet")

        source_record = {"source_record_sha256": observed["source_record_sha256"],
                         "source_ids_sha256": observed["source_ids_sha256"],
                         "source_schema_sha256": observed["source_schema_sha256"],
                         "row_count": observed["row_count"], "maximum_object_id": observed["maximum_object_id"],
                         "fields": observed["fields"]}
        manifest = {
            **source_record,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "source_url": TABLE_URL, "service_url": SERVICE_URL, "source_table": TABLE_NAME,
            "source_release_date": None,
            "source_vintage": source_manifest["source_vintage"],
            "source_freshness_gate": "blocked_until_EPA_release_or_refresh_date_is_established",
            "source_count_before": observed["source_count_before"],
            "source_count_after": observed["source_count_after"],
            "id_count_before": observed["id_count_before"], "id_count_after": observed["id_count_after"],
            "id_roster_equal_before_after": observed["id_roster_equal_before_after"],
            "captured_ids_match_roster": observed["captured_ids_match_roster"],
            "page_size": observed["page_size"], "page_count": observed["page_count"],
            "response_count": len(wire_entries), "wire_response_inventory": wire_entries,
            "service_tables_before": observed["service_tables_before"],
            "service_tables_after": observed["service_tables_after"],
            "table_relationships": observed["table_before"].get("relationships"),
            "geometry": "absent", "refresh_status": "STOPPED",
            "release_accepted": False, "publication_approval": "not_established",
        }
        static_source_manifest = {
            **source_manifest,
            "title": "EPA Facility Registry Service Interest Contacts",
            "description": source_manifest["description"],
            "agency": "Environmental Protection Agency",
            "office": "Office of Environmental Information",
            "source_modified": None, "date_issued": None, "date_modified": None,
            "capture_observed_at": manifest["captured_at"],
            "observed_row_count": observed["row_count"],
            "observed_source_record_sha256": observed["source_record_sha256"],
            "observed_source_schema_sha256": observed["source_schema_sha256"],
        }
        audit = {"source_complete": True, "exact_fields_preserved": True,
                 "object_ids_complete_and_stable": True, "raw_wire_receipts_complete": True,
                 "geometry": "absent", "source_release_date_known": False,
                 "rights_accepted": False, "historical_candidate_compatibility_accepted": False,
                 "release_accepted": False, "refresh_status": "STOPPED",
                 "review_reasons": ["source_release_date_unknown", "license_unverified",
                                    "historical_published_membership_not_yet_compared"]}
        ready = {"approved": False, "version": version,
                 "source_record_sha256": observed["source_record_sha256"],
                 "source_schema_sha256": observed["source_schema_sha256"],
                 "objects": {"unknown/" + FILE_SLUG + ".parquet": objects["unknown/" + FILE_SLUG + ".parquet"]},
                 "refresh_status": "STOPPED", "release_accepted": False}
        source_ids_path = capture_root / "source_id_roster_summary.json"
        # Avoid making the population-sized roster a Python list or object.
        with source_ids_path.open("wb") as stream:
            stream.write(canonical({"count": observed["id_count_before"],
                                    "sha256": observed["source_ids_sha256"],
                                    "complete": True}) + b"\n")
        objects["metadata/source_id_roster_summary.json"] = _upload_immutable_file(
            staging_storage, source_ids_path, f"{prefix}/metadata/source_id_roster_summary.json")
        for name, payload in (("source_capture.json", manifest), ("source_manifest.json", static_source_manifest),
                              ("source_audit.json", audit), ("processing_identity.json", identity),
                              ("candidate_ready.json", ready)):
            body = canonical(payload) + b"\n"
            objects[f"metadata/{name}"] = {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            write_immutable(staging_storage, f"{prefix}/metadata/{name}", body)

        state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
        payload = {"processing_identity": identity,
                   "source_record_sha256": observed["source_record_sha256"],
                   "source_schema_sha256": observed["source_schema_sha256"],
                   "capture_receipts": capture_index,
                   "objects": objects}
        fingerprint = hashlib.sha256(canonical(payload)).hexdigest()
        seal_ready_candidate(staging_storage, state_key, version, fingerprint, payload)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "row_count": observed["row_count"], "source_record_sha256": observed["source_record_sha256"],
                "source_schema_sha256": observed["source_schema_sha256"],
                "source_ids_sha256": observed["source_ids_sha256"],
                "native_fields": list(EXPECTED_FIELDS), "source_vintage": None,
                "refresh_status": "STOPPED", "drift_reasons": audit["review_reasons"],
                "parquet_key": f"{prefix}/unknown/{FILE_SLUG}.parquet",
                "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
                "reused": False, "release_accepted": False}
    except Exception as exc:
        _quarantine(staging_storage, capture_root,
                    reason=getattr(exc, "code", type(exc).__name__))
        raise
    finally:
        shutil.rmtree(capture_root, ignore_errors=True)


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="epa", compute_kind="http",
       config_schema={"resume_candidate": Field(str, default_value="")},
       description="EPA FRS interest-contact table; full native contact values are preserved in a stopped draft pending release-date, rights, and compatibility review.")
def epa_frs_interest_contact(context, staging_storage: StagingStorageResource) -> dict[str, object]:
    resume_candidate = context.op_execution_context.op_config.get("resume_candidate", "")
    if resume_candidate:
        repo_root = Path(__file__).parents[3]
        result = _recover_retained_candidate(staging_storage, resume_candidate,
                                             processing_identity(repo_root))
    else:
        result = capture_epa_frs_contact(staging_storage)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, str(result["version"]))])
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_frs_interest_contact, blocking=True)
def epa_frs_interest_contact_complete(candidate: dict[str, object]) -> AssetCheckResult:
    passed = (candidate.get("row_count") == EXPECTED_ROWS
              and candidate.get("native_fields") == list(EXPECTED_FIELDS)
              and candidate.get("release_accepted") is False)
    return AssetCheckResult(passed=passed, metadata={
        "source_complete_rows": candidate.get("row_count", 0),
        "source_schema_sha256": str(candidate.get("source_schema_sha256", "")),
        "source_vintage": "unknown", "release_accepted": False,
    })


epa_frs_interest_contact_draft_job = define_asset_job(
    "epa_frs_interest_contact_draft_refresh",
    selection=AssetSelection.assets(epa_frs_interest_contact))
epa_frs_interest_contact_draft_schedule = ScheduleDefinition(
    name="epa_frs_interest_contact_draft_refresh_schedule",
    job=epa_frs_interest_contact_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED)
