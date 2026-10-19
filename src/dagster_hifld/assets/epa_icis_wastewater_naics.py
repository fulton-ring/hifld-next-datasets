"""EPA ICIS wastewater NAICS table, preserved as a nonspatial draft candidate."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (AssetCheckResult, AssetKey, AssetSelection, Config,
                     DefaultScheduleStatus, ScheduleDefinition, asset, asset_check,
                     define_asset_job)

from dagster_hifld.construction_state import ConstructionStateError, load_first_construction
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_arcgis_table import (
    ArcGISTableCaptureError, ArcGISTableProfile, capture_table,
)
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)


DATASET_SLUG = "icis-wastewater-treatment-plants-naics-codes"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_Wastewater/FeatureServer"
PORTAL_ITEM_ID = "0895b107f9184e7cb31707767b506a64"
PORTAL_ITEM_OWNER = "EPA_GEO"
PORTAL_ITEM_TITLE = "EPA Facility Registry Service - Integrated Compliance Information System (ICIS) Wastewater Treatment Plants"
PROFILE = ArcGISTableProfile(
    service_url=SERVICE_URL, table_id=1,
    table_name="Integrated Compliance Information System (ICIS) Wastewater Treatment Plants NAICS Codes",
    fields=(
        ("OBJECTID", "esriFieldTypeOID", False, None),
        ("REGISTRY_ID", "esriFieldTypeString", True, 12),
        ("NAICS_CODE", "esriFieldTypeString", False, 6),
        ("PRIMARY_INDICATOR", "esriFieldTypeString", False, 10),
    ),
    relationship_name=None, related_table_id=None, relationship_key=None,
    relationship_cardinality=None, source_vintage=None, expected_rows=24104,
    portal_item_id=PORTAL_ITEM_ID, portal_item_owner=PORTAL_ITEM_OWNER,
    portal_item_title=PORTAL_ITEM_TITLE,
)
TABLE_URL = PROFILE.table_url
EXPECTED_ROWS = PROFILE.expected_rows


class EPAICISNAICSError(ValueError):
    """The EPA ICIS NAICS snapshot is incomplete or has drifted."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _schema_sha(fields: list[dict[str, Any]]) -> str:
    return _sha(_canonical(fields))


def _typed_parquet(rows: list[dict[str, Any]], fields: tuple[dict[str, Any], ...]) -> bytes:
    arrow_fields = []
    for field in fields:
        name = field["name"]
        dtype = pa.int64() if name == "OBJECTID" else pa.string()
        metadata = {b"original_esri_field": _canonical(field)}
        arrow_fields.append(pa.field(name, dtype, nullable=field["nullable"], metadata=metadata))
    schema = pa.schema(arrow_fields, metadata={
        b"hifld:source_url": TABLE_URL.encode(), b"hifld:source_table": PROFILE.table_name.encode(),
        b"hifld:geometry": b"absent",
    })
    table = pa.Table.from_pydict({name: [row[name] for row in rows] for name in PROFILE.field_names}, schema=schema)
    out = pa.BufferOutputStream()
    pq.write_table(table, out, compression="zstd", version="2.6")
    return out.getvalue().to_pybytes()


def _triplets_sha(rows: list[dict[str, Any]]) -> str:
    rows_for_hash = [[row["REGISTRY_ID"], row["NAICS_CODE"], row["PRIMARY_INDICATOR"]]
                     for row in rows]
    rows_for_hash.sort(key=_canonical)
    return _sha(_canonical(rows_for_hash))


def _reuse(storage: StagingStorageResource, *, version: str, identity: dict[str, Any],
           fingerprint: str, offline: bool,
           observation: tuple[dict[str, tuple[bytes, dict[str, Any]]], Path] | None = None) -> dict[str, Any]:
    root = Path(__file__).parents[3]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    try:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    except (ConstructionStateError, ValueError, OSError) as exc:
        raise EPAICISNAICSError("cached_candidate_state_invalid") from exc
    payload = bundle.get("payload")
    if not isinstance(payload, dict):
        raise EPAICISNAICSError("cached_candidate_payload_missing")
    receipts, objects = payload.get("capture_receipts"), payload.get("objects")
    page_count = (EXPECTED_ROWS + PROFILE.page_size - 1) // PROFILE.page_size
    expected_receipts = {"service", "portal_item", "service_after", "table_before", "count_before", "ids_before",
                         "table_after", "count_after", "ids_after"} | {
                             f"page_{index:03d}" for index in range(page_count)}
    expected_objects = {"unknown/" + FILE_SLUG + ".parquet", "metadata/source_ids.json",
                        "metadata/source_capture.json",
                        "metadata/source_manifest.json", "metadata/source_audit.json",
                        "metadata/processing_identity.json", "metadata/candidate_ready.json"}
    if (not isinstance(receipts, dict) or set(receipts) != expected_receipts or not isinstance(objects, dict)
            or set(objects) != expected_objects | {f"raw/{name}.{ext}" for name in receipts
                                                   for ext in ("body", "wire", "receipt.json")}):
        raise EPAICISNAICSError("cached_candidate_roster_invalid")
    verified = verify_cached_candidate(
        storage, root, state_key, prefix, version, fingerprint, identity,
        payload.get("source_record_sha256"), payload.get("source_schema_sha256"), objects, receipts,
    )
    observation_key = None
    if observation is not None:
        fresh_captures, capture_root = observation
        observation_key = f"_observations/{DATASET_SLUG}/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}-{uuid4().hex}"
        for name, (body, receipt) in sorted(fresh_captures.items()):
            for suffix, raw in (("body", body), ("wire", (capture_root / f"{name}.wire").read_bytes()),
                                ("receipt.json", _canonical(receipt) + b"\n")):
                write_immutable(storage, f"{observation_key}/raw/{name}.{suffix}", raw)
        observation_summary = {"captured_at": datetime.now(timezone.utc).isoformat(),
                               "row_count": EXPECTED_ROWS, "source_record_sha256": payload["source_record_sha256"],
                               "source_schema_sha256": payload["source_schema_sha256"],
                               "response_receipts": {name: receipt for name, (_body, receipt) in fresh_captures.items()},
                               "reuse_of_candidate_version": version}
        write_immutable(storage, f"{observation_key}/observation.json", _canonical(observation_summary) + b"\n")
    audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "row_count": EXPECTED_ROWS, "source_record_sha256": payload["source_record_sha256"],
            "source_schema_sha256": payload["source_schema_sha256"], "refresh_status": "STOPPED",
            "drift_reasons": audit["drift_reasons"], "offline_replay": offline,
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "fresh_observation": observation_key, **verified}


def capture_epa_icis_wastewater_naics(staging_storage: StagingStorageResource, *,
                              offline_replay_version: str | None = None,
                              client: httpx.Client | None = None) -> dict[str, Any]:
    """Capture fresh source evidence before any versioned cache reuse."""
    if not isinstance(staging_storage, StagingStorageResource) or isinstance(staging_storage, PublishedStorageResource):
        raise EPAICISNAICSError("explicit_staging_resource_required")
    repo_root = Path(__file__).parents[3]
    identity = processing_identity(repo_root)
    identity_fingerprint = _sha(_canonical(identity))
    if offline_replay_version:
        return _reuse(staging_storage, version=offline_replay_version, identity=identity,
                      fingerprint=identity_fingerprint, offline=True)

    capture_root = Path("/tmp/hifld-native-reconstruction/119/captures") / f"refresh-{uuid4().hex}"
    captures: dict[str, tuple[bytes, dict[str, Any]]] = {}
    owned_client = client is None
    active = client or httpx.Client(follow_redirects=False, timeout=60.0)
    try:
        rows, manifest, captures = capture_table(active, PROFILE, capture_root)
    except Exception as exc:
        for receipt_path in sorted(capture_root.glob("*.receipt.json")) if capture_root.exists() else []:
            name = receipt_path.name.removesuffix(".receipt.json")
            try:
                receipt = json.loads(receipt_path.read_bytes())
                captures.setdefault(name, ((capture_root / f"{name}.body").read_bytes(), receipt))
            except (OSError, ValueError):
                pass
        if captures:
            quarantine_incomplete_capture(staging_storage, capture_root, DATASET_SLUG,
                                          "unversioned-refresh", captures,
                                          getattr(exc, "code", type(exc).__name__))
        raise
    finally:
        if owned_client:
            active.close()

    record_sha = _sha(_canonical(rows))
    ids_sha = _sha(_canonical(sorted(row["OBJECTID"] for row in rows)))
    triplets_sha = _triplets_sha(rows)
    schema_sha = _schema_sha(manifest["fields"])
    if len(rows) != EXPECTED_ROWS:
        quarantine_incomplete_capture(staging_storage, capture_root, DATASET_SLUG,
                                      "unversioned-refresh", captures, "complete_source_snapshot_drift")
        raise EPAICISNAICSError("complete_source_snapshot_drift")

    version = content_derived_version(record_sha, "", schema_sha, identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if staging_storage.object_snapshot(state_key) is not None:
        return _reuse(staging_storage, version=version, identity=identity,
                      fingerprint=identity_fingerprint, offline=False,
                      observation=(captures, capture_root))

    # No accepted public schema/value baseline exists yet. Retain a complete draft
    # candidate and require maintainer acceptance before promotion.
    drift = ["maintainer_schema_and_id_contract_pending"]
    data = _typed_parquet(rows, tuple(manifest["fields"]))
    objects: dict[str, bytes] = {"unknown/" + FILE_SLUG + ".parquet": data}
    objects["metadata/source_ids.json"] = _canonical(sorted(row["OBJECTID"] for row in rows)) + b"\n"
    capture_objects = []
    for name, (body, receipt) in sorted(captures.items()):
        for suffix, raw in (("body", body), ("wire", (capture_root / f"{name}.wire").read_bytes()),
                            ("receipt.json", _canonical(receipt) + b"\n")):
            objects[f"raw/{name}.{suffix}"] = raw
        capture_objects.append({"name": name, "bytes": len(body), "sha256": _sha(body)})
    manifest.update({"captured_at": datetime.now(timezone.utc).isoformat(),
                     "source_record_sha256": record_sha, "source_schema_sha256": schema_sha,
                     "source_ids_sha256": ids_sha, "source_triplets_sha256": triplets_sha,
                     "refresh_status": "STOPPED", "drift_reasons": drift,
                     "publication_approval": "not_established_by_source_capture",
                     "capture_objects": capture_objects,
                     "parquet_object": {"key": f"{prefix}/unknown/{FILE_SLUG}.parquet",
                                        "bytes": len(data), "sha256": _sha(data)}})
    release_unknown = (
        "The EPA service and item metadata do not state a source data release date; "
        "the ArcGIS item modified timestamp is portal metadata, not a source release date."
    )
    input_host = urlsplit(SERVICE_URL).hostname
    provenance_inputs = []
    for name, (_body, receipt) in sorted(captures.items()):
        if not name.startswith("page_"):
            continue
        provenance_inputs.append({
            "input_id": f"epa-icis-wastewater-naics-{name}",
            "producer": "U.S. Environmental Protection Agency, Office of Environmental Information",
            "host": input_host,
            "canonical_url": TABLE_URL,
            "retrieval_url": receipt["url"],
            "requested_url": receipt["url"],
            "observed_final_url": receipt["final_url"],
            "retrieved_at": receipt["observed_at"],
            "sha256": receipt["sha256"],
            "bytes": receipt["bytes"],
            "http_status": receipt["status"],
            "license": "U.S. EPA public domain per the EPA_GEO ArcGIS item license statement",
            "attribution": "U.S. Environmental Protection Agency, Office of Environmental Information",
            "role": "producer_publication",
            "release_date": None,
            "release_date_unknown_reason": release_unknown,
        })
    source_manifest = {
        "schema_version": 1,
        "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "inputs": provenance_inputs,
        "transformations": [
            {"name": "capture_complete_arcgis_table", "parameters": {
                "page_size": PROFILE.page_size, "return_geometry": False,
                "order_by": "OBJECTID ASC", "bracketed_count_and_id_rosters": True}},
            {"name": "write_native_typed_parquet", "parameters": {
                "objectid_type": "int64", "string_fields_preserved": list(PROFILE.field_names[1:]),
                "geometry": False, "compression": "zstd"}},
        ],
        "title": PORTAL_ITEM_TITLE,
        "source_landing_url": f"https://www.arcgis.com/home/item.html?id={PROFILE.portal_item_id}",
        "machine_download_url": TABLE_URL,
        "historical_archive": {"archive": "DataLumos", "project_id": "239287", "version": "V3",
                               "url": "https://www.datalumos.org/datalumos/project/239287/version/V3/view",
                               "role": "historical_archive_not_refresh_authority"},
        "description": "EPA Office of Environmental Information's public FRS_Wastewater FeatureServer table 1, titled ICIS Wastewater Treatment Plants NAICS Codes. The service is explicitly compiled from EPA FRS and ICIS. The table declares no relationships. REGISTRY_ID also appears on layer 0; ArcGIS does not declare this table-to-layer relationship, so a shared key name is not treated as proof of linked records or coverage. The DataLumos candidate is a historical archive and is not treated as the current source.",
        "publisher": "U.S. Environmental Protection Agency, Office of Environmental Information (EPA_GEO)",
        "agency": "Environmental Protection Agency", "source_url": TABLE_URL,
        "license": "U.S. EPA public domain (source service item license statement)",
        "date_issued": None, "date_modified": None, "source_modified": None,
        "lineage": ["EPA_GEO hosted ArcGIS item 0895b107f9184e7cb31707767b506a64",
                    "EPA FRS_Wastewater FeatureServer table 1 NAICS Codes",
                    "DataLumos project 239287 V3 is a historical archive, not the refresh source",
                    "EPA ICIS wastewater treatment facility information combined with EPA FRS facility data",
                    "REGISTRY_ID is present in both table 1 and layer 0; ArcGIS declares no relationship, so no cross-table coverage is asserted"],
        "portal_item_modified": manifest.get("portal_item", {}).get("modified"),
        "source_release_date": None, "source_vintage": None,
        "source_release_date_unknown_reason": release_unknown,
        "native_geometry": "absent", "refresh_status": "STOPPED",
        "publication_approval": "not_established_by_source_capture",
    }
    audit = {**manifest, "source_count_contract_passed": True,
             "source_id_roster_contract_passed": True, "source_value_contract_passed": True,
             "published_candidate_membership_compatible": not drift, "release_accepted": False,
             "source_release_accepted": False, "freshness_accepted": False,
             "published_candidate_geometry_contract_compatible": False}
    ready = {"approved": False, "version": version, "source_record_sha256": record_sha,
             "source_schema_sha256": schema_sha,
             "objects": {"unknown/" + FILE_SLUG + ".parquet": {"bytes": len(data), "sha256": _sha(data)}},
             "refresh_status": "STOPPED", "release_accepted": False}
    objects.update({"metadata/source_capture.json": _canonical(manifest) + b"\n",
                    "metadata/source_manifest.json": _canonical(source_manifest) + b"\n",
                    "metadata/source_audit.json": _canonical(audit) + b"\n",
                    "metadata/processing_identity.json": _canonical(identity) + b"\n",
                    "metadata/candidate_ready.json": _canonical(ready) + b"\n"})
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in objects.items()}
    payload = {"processing_identity": identity, "source_record_sha256": record_sha,
               "source_schema_sha256": schema_sha,
               "capture_receipts": {name: receipt for name, (_body, receipt) in captures.items()},
               "objects": descriptors}
    try:
        for name, body in objects.items():
            write_immutable(staging_storage, f"{prefix}/{name}", body)
        seal_ready_candidate(staging_storage, state_key, version, identity_fingerprint, payload)
    except Exception as exc:
        quarantine_incomplete_capture(staging_storage, capture_root, DATASET_SLUG, version,
                                      captures, getattr(exc, "code", type(exc).__name__))
        raise
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "row_count": len(rows), "source_record_sha256": record_sha,
            "source_schema_sha256": schema_sha, "refresh_status": "STOPPED",
            "drift_reasons": drift, "native_fields": list(PROFILE.field_names),
            "parquet_key": f"{prefix}/unknown/{FILE_SLUG}.parquet",
            "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "reused": False, "release_accepted": False}


class EPAICISNAICSConfig(Config):
    offline_replay_version: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="epa", compute_kind="http",
       description="Exact EPA ICIS wastewater NAICS table 1 as a nonspatial candidate; maintainer acceptance blocks release.")
def epa_icis_wastewater_naics_codes(context, config: EPAICISNAICSConfig,
                            staging_storage: StagingStorageResource) -> dict:
    result = capture_epa_icis_wastewater_naics(staging_storage,
                                       offline_replay_version=config.offline_replay_version or None)
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_icis_wastewater_naics_codes, blocking=True)
def epa_icis_wastewater_naics_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("row_count") == EXPECTED_ROWS
              and bool(candidate.get("source_schema_sha256")))
    return AssetCheckResult(passed=passed, metadata={"source_complete_rows": candidate.get("row_count", 0),
        "source_record_sha256": candidate.get("source_record_sha256", ""),
        "source_schema_sha256": candidate.get("source_schema_sha256", ""),
        "source_vintage": None, "release_accepted": False})


@asset_check(asset=epa_icis_wastewater_naics_codes, blocking=True)
def epa_icis_wastewater_naics_release_approved(candidate: dict) -> AssetCheckResult:
    approved = candidate.get("release_accepted") is True
    return AssetCheckResult(passed=approved, metadata={
        "release_accepted": approved,
        "gate": "maintainer_approval_of_raw_published_schema_and_id_contract_required",
    })


@asset_check(asset=epa_icis_wastewater_naics_codes, blocking=False)
def epa_icis_wastewater_naics_published_membership_compatible(candidate: dict) -> AssetCheckResult:
    drift = candidate.get("drift_reasons", [])
    return AssetCheckResult(passed=not drift, metadata={"source_rows": candidate.get("row_count", 0),
        "published_candidate_rows": None, "drift_reasons": drift, "release_accepted": False})


epa_icis_wastewater_naics_draft_job = define_asset_job("epa_icis_wastewater_naics_draft_refresh",
    selection=AssetSelection.assets(epa_icis_wastewater_naics_codes))
epa_icis_wastewater_naics_draft_schedule = ScheduleDefinition(name="epa_icis_wastewater_naics_draft_refresh_schedule",
    job=epa_icis_wastewater_naics_draft_job, cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    run_config={"ops": {"epa_icis_wastewater_naics_codes": {"config": {"offline_replay_version": ""}}}})
