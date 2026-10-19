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
from dagster import (AssetCheckResult, AssetKey, AssetSelection,
                     DefaultScheduleStatus, ScheduleDefinition, asset, asset_check,
                     define_asset_job)

from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    seal_ready_candidate, write_immutable,
)
from dagster_hifld.sources.epa_frs_contact_stream import (
    EXPECTED_ROWS, SERVICE_URL, TABLE_NAME, TABLE_URL, capture_to_parquet, canonical,
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
                   "capture_receipts": {str(item["name"]): item for item in metadata_receipts + page_receipts},
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
       description="EPA FRS interest-contact table; full native contact values are preserved in a stopped draft pending release-date, rights, and compatibility review.")
def epa_frs_interest_contact(context, staging_storage: StagingStorageResource) -> dict[str, object]:
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
