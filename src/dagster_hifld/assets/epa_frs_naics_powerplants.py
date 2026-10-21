"""EPA FRS power plant NAICS table, captured without spatial inference."""

import hashlib
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSelection,
    Config,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

from dagster_hifld.construction_state import ConstructionStateError, load_first_construction
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    content_derived_version,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.sources.epa_frs_arcgis_table import ArcGISTableProfile, capture_table


DATASET_SLUG = "epa-frs-eia-860-power-generation-facilities-naics-codes"
FILE_SLUG = DATASET_SLUG
TABLE_URL = "https://geodata.epa.gov/arcgis/rest/services/OEI/FRS_PowerPlants/MapServer/14"
SERVICE_URL = "https://geodata.epa.gov/arcgis/rest/services/OEI/FRS_PowerPlants/MapServer"
PAGE_SIZE = 500
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
EXPECTED_SOURCE_ROWS = 1395
EXPECTED_SOURCE_RECORDS_SHA256 = "5281573910df7d293625278e87cbca80c0807535ef96f8ff40db515dc0ae0a13"
EXPECTED_SOURCE_TRIPLETS_SHA256 = "a6c8b02a2a0f57733a16ca23f72adec675a258272584780fab22e9e686b4e311"
EXPECTED_SOURCE_IDS_SHA256 = "fd121d92498cda5f2a2f1fa04cc7ea0378828d449dd3cb367ab6c8af1deef891"
EXPECTED_SOURCE_SCHEMA_SHA256 = "65fd1168a343a5f4c6a8cc97f0cc6df6e6b62332b836259062fcff89d64b9124"
EXPECTED_FIELDS = ("OBJECTID", "REGISTRY_ID", "NAICS_CODE", "PRIMARY_INDICATOR")
EXPECTED_ARCGIS_FIELDS = (
    ("OBJECTID", "esriFieldTypeOID", False, None),
    ("REGISTRY_ID", "esriFieldTypeString", True, 12),
    ("NAICS_CODE", "esriFieldTypeString", False, 6),
    ("PRIMARY_INDICATOR", "esriFieldTypeString", False, 10),
)
TABLE_PROFILE = ArcGISTableProfile(
    service_url=SERVICE_URL,
    table_id=14,
    table_name="FRS_NAICS_POWERPLANTS",
    fields=EXPECTED_ARCGIS_FIELDS,
    relationship_name="POWERPLANT_LAYER",
    related_table_id=12,
    relationship_key="REGISTRY_ID",
    relationship_cardinality="esriRelCardinalityOneToMany",
    source_vintage="2014-06",
    expected_rows=EXPECTED_SOURCE_ROWS,
    page_size=PAGE_SIZE,
    max_response_bytes=MAX_RESPONSE_BYTES,
)

# This fingerprint describes the complete, independently fetched v1.0.0 public
# HIFLD Next candidate. It is a quarantine comparator, not an accepted contract.
PUBLISHED_CANDIDATE_ROWS = 1711
PUBLISHED_CANDIDATE_TRIPLETS_SHA256 = "f280276bb00f918a78797ca8a7393e02c927f8d367abb6a5b5887be4c02ea14a"


class EPAFRSNAICSError(ValueError):
    """The upstream table response is incomplete or has changed schema."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _fetch_complete_table(
    client: httpx.Client, capture_root: Path, *, replay_captures: bool = False
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, tuple[bytes, dict[str, Any]]]]:
    records, manifest, captures = capture_table(
        client, TABLE_PROFILE, capture_root, replay_captures=replay_captures
    )
    manifest.update({"schema_version": 1, "source_release_date": None})
    return records, manifest, captures


def _typed_parquet(records: list[dict[str, Any]], fields: tuple[dict[str, Any], ...]) -> bytes:
    arrow_fields: list[pa.Field] = []
    for field in fields:
        name = field["name"]
        if name == "OBJECTID":
            arrow_type = pa.int64()
        else:
            arrow_type = pa.string()
        metadata = {b"original_esri_field": json.dumps(field, sort_keys=True, separators=(",", ":")).encode()}
        arrow_fields.append(pa.field(name, arrow_type, nullable=field["nullable"], metadata=metadata))
    schema = pa.schema(
        arrow_fields,
        metadata={
            b"hifld:source_url": TABLE_URL.encode(),
            b"hifld:source_table": b"FRS_NAICS_POWERPLANTS",
            b"hifld:geometry": b"absent",
        },
    )
    columns = {name: [row[name] for row in records] for name in EXPECTED_FIELDS}
    table = pa.Table.from_pydict(columns, schema=schema)
    output = pa.BufferOutputStream()
    pq.write_table(table, output, compression="zstd", version="2.6")
    return output.getvalue().to_pybytes()


def _triple_fingerprint(records: list[dict[str, Any]]) -> str:
    triplets = sorted(
        [[record["REGISTRY_ID"], record["NAICS_CODE"], record["PRIMARY_INDICATOR"]] for record in records]
    )
    body = json.dumps(triplets, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(body).hexdigest()


def _reuse_candidate(
    staging_storage: StagingStorageResource,
    *,
    version: str,
    identity: dict[str, Any],
    fingerprint: str,
    offline: bool,
) -> dict[str, Any]:
    repo_root = Path(__file__).parents[3]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    try:
        bundle = json.loads(load_first_construction(staging_storage, state_key, version, fingerprint))
    except (ConstructionStateError, ValueError, OSError) as exc:
        raise EPAFRSNAICSError("cached_candidate_state_invalid") from exc
    payload = bundle.get("payload")
    if not isinstance(payload, dict):
        raise EPAFRSNAICSError("cached_candidate_payload_missing")
    expected_names = {"unknown/" + FILE_SLUG + ".parquet", "metadata/source_capture.json",
                      "metadata/source_manifest.json", "metadata/source_audit.json",
                      "metadata/processing_identity.json", "metadata/candidate_ready.json"}
    receipts = payload.get("capture_receipts")
    objects = payload.get("objects")
    expected_receipt_names = {"service", "table_before", "count_before", "ids_before",
                              "table_after", "service_after", "count_after", "ids_after",
                              *(f"page_{index:03d}" for index in range((EXPECTED_SOURCE_ROWS + PAGE_SIZE - 1) // PAGE_SIZE))}
    if (not isinstance(receipts, dict) or not isinstance(objects, dict)
            or set(receipts) != expected_receipt_names
            or set(objects) != expected_names | {
                f"raw/{name}.{suffix}" for name in receipts for suffix in ("body", "wire", "receipt.json")
            }):
        raise EPAFRSNAICSError("cached_candidate_roster_invalid")
    cached = verify_cached_candidate(
        staging_storage, repo_root, state_key, prefix, version, fingerprint, identity,
        payload.get("source_record_sha256"), payload.get("source_schema_sha256"), objects, receipts,
    )
    audit = json.loads(staging_storage.read_key(f"{prefix}/metadata/source_audit.json"))
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "row_count": EXPECTED_SOURCE_ROWS, "source_record_sha256": payload["source_record_sha256"],
        "source_schema_sha256": payload["source_schema_sha256"], "refresh_status": "STOPPED",
        "drift_reasons": audit.get("drift_reasons", []), "offline_replay": offline,
        "candidate_ready": f"{prefix}/metadata/candidate_ready.json", **cached,
    }


def capture_epa_frs_naics_table(
    staging_storage: StagingStorageResource,
    *,
    offline_replay_version: str | None = None,
    offline_capture_version: str | None = None,
    capture_directory: str | Path | None = None,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Observe the source before content-addressed reuse; offline replay is explicit."""
    if not isinstance(staging_storage, StagingStorageResource) or isinstance(staging_storage, PublishedStorageResource):
        raise EPAFRSNAICSError("explicit_staging_resource_required")
    repo_root = Path(__file__).parents[3]
    identity = processing_identity(repo_root)
    fingerprint = hashlib.sha256(_canonical(identity)).hexdigest()
    if offline_replay_version and offline_capture_version:
        raise EPAFRSNAICSError("choose_cached_reuse_or_retained_capture_replay")
    if offline_replay_version:
        return _reuse_candidate(staging_storage, version=offline_replay_version, identity=identity,
                                fingerprint=fingerprint, offline=True)

    capture_base = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/70/captures")
    capture_root = capture_base / f"refresh-{uuid4().hex}"
    captures: dict[str, tuple[bytes, dict[str, Any]]] = {}
    owned_client = client is None
    active_client = client or httpx.Client(follow_redirects=False, timeout=60.0)
    capture_mode = "live_source_capture"
    try:
        if offline_capture_version:
            capture_mode = "offline_retained_source_replay"
            source_prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, offline_capture_version)
            retained_names = {
                "service", "table_before", "count_before", "ids_before", "table_after",
                "service_after", "count_after", "ids_after",
                *(f"page_{index:03d}" for index in range(
                    (EXPECTED_SOURCE_ROWS + PAGE_SIZE - 1) // PAGE_SIZE
                )),
            }
            capture_root.mkdir(parents=True, exist_ok=True)
            for name in retained_names:
                for suffix in ("body", "wire", "receipt.json"):
                    body = staging_storage.read_key(f"{source_prefix}/raw/{name}.{suffix}")
                    (capture_root / f"{name}.{suffix}").write_bytes(body)
            records, manifest, captures = _fetch_complete_table(
                active_client, capture_root, replay_captures=True
            )
        else:
            records, manifest, captures = _fetch_complete_table(active_client, capture_root)
    except Exception as exc:
        for receipt_path in sorted(capture_root.glob("*.receipt.json")) if capture_root.exists() else []:
            name = receipt_path.name.removesuffix(".receipt.json")
            body_path = capture_root / f"{name}.body"
            try:
                receipt = json.loads(receipt_path.read_bytes())
                captures.setdefault(name, (body_path.read_bytes(), receipt))
            except (OSError, ValueError):
                continue
        if captures:
            quarantine_incomplete_capture(staging_storage, capture_root, DATASET_SLUG, "unversioned-refresh", captures,
                                          getattr(exc, "code", type(exc).__name__))
        raise
    finally:
        if owned_client:
            active_client.close()

    record_sha = hashlib.sha256(_canonical(records)).hexdigest()
    ids_sha = hashlib.sha256(_canonical(sorted(row["OBJECTID"] for row in records))).hexdigest()
    triplet_sha = _triple_fingerprint(records)
    schema_sha = hashlib.sha256(_canonical(list(manifest["fields"]))).hexdigest()
    if (len(records) != EXPECTED_SOURCE_ROWS or record_sha != EXPECTED_SOURCE_RECORDS_SHA256
            or ids_sha != EXPECTED_SOURCE_IDS_SHA256 or schema_sha != EXPECTED_SOURCE_SCHEMA_SHA256):
        quarantine_incomplete_capture(staging_storage, capture_root, DATASET_SLUG, "unversioned-refresh", captures,
                                      "complete_source_snapshot_drift")
        raise EPAFRSNAICSError("complete_source_snapshot_drift")

    version = content_derived_version(record_sha, "", schema_sha, identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if staging_storage.object_snapshot(state_key) is not None:
        return _reuse_candidate(staging_storage, version=version, identity=identity,
                                fingerprint=fingerprint, offline=False)

    comparison_reasons = []
    if len(records) != PUBLISHED_CANDIDATE_ROWS:
        comparison_reasons.append("feature_count_changed_from_published_candidate")
    if triplet_sha != PUBLISHED_CANDIDATE_TRIPLETS_SHA256:
        comparison_reasons.append("source_rows_changed_from_published_candidate")
    data = _typed_parquet(records, tuple(manifest["fields"]))
    data_key = f"{prefix}/unknown/{FILE_SLUG}.parquet"
    capture_objects = []
    outputs: dict[str, bytes] = {f"unknown/{FILE_SLUG}.parquet": data}
    for name, (body, receipt) in sorted(captures.items()):
        for suffix, payload in (("body", body), ("wire", (capture_root / f"{name}.wire").read_bytes()),
                                ("receipt.json", _canonical(receipt) + b"\n")):
            outputs[f"raw/{name}.{suffix}"] = payload
        capture_objects.append({"name": name, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()})
    manifest.update({
        "captured_at": min(receipt["observed_at"] for _, receipt in captures.values()),
        "capture_mode": capture_mode,
        "source_record_sha256": record_sha,
        "source_schema_sha256": schema_sha, "source_ids_sha256": ids_sha,
        "source_triplets_sha256": triplet_sha, "published_candidate_count": PUBLISHED_CANDIDATE_ROWS,
        "published_candidate_triplets_sha256": PUBLISHED_CANDIDATE_TRIPLETS_SHA256,
        "refresh_status": "STOPPED", "drift_reasons": comparison_reasons,
        "publication_approval": "not_established_by_source_capture", "capture_objects": capture_objects,
        "parquet_object": {"key": data_key, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()},
    })
    source_manifest = {
        "title": "EPA Facility Registry Service - EIA-860 Power Generation Facilities NAICS Codes",
        "description": "EPA OEI FRS_POWERPLANTS MapServer table FRS_NAICS_POWERPLANTS; nonspatial exact NAICS table. The service describes EIA-860 source vintage as June 2014. Published-candidate membership differs; release is blocked pending maintainer review.",
        "publisher": "U.S. Environmental Protection Agency, Office of Environmental Information (EPA_GEO)",
        "agency": "Environmental Protection Agency", "source_url": TABLE_URL,
        "license": "U.S. EPA public domain (source service item license statement)",
        "date_issued": None, "date_modified": None, "source_modified": None,
        "lineage": ["EPA_GEO FRS_POWERPLANTS MapServer table 14: FRS_NAICS_POWERPLANTS",
                    "Declared one-to-many POWERPLANT_LAYER relationship to table 12 on REGISTRY_ID",
                    "Service description states EIA-860 powerplant source vintage most-current as of June 2014"],
        "source_vintage": "2014-06", "native_geometry": "absent", "refresh_status": "STOPPED",
        "publication_approval": "not_established_by_source_capture",
    }
    audit = {
        **manifest, "published_candidate_count": PUBLISHED_CANDIDATE_ROWS,
        "source_record_sha256": record_sha, "source_schema_sha256": schema_sha,
        "source_ids_sha256": ids_sha, "source_triplets_sha256": triplet_sha,
        "source_count_contract_passed": True, "source_id_roster_contract_passed": True,
        "source_value_contract_passed": True, "published_candidate_membership_compatible": not comparison_reasons,
        "release_accepted": False, "source_release_accepted": False, "freshness_accepted": False,
    }
    ready = {"approved": False, "version": version, "source_record_sha256": record_sha,
             "source_schema_sha256": schema_sha, "objects": {"unknown/" + FILE_SLUG + ".parquet": {
                 "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()},
             }, "refresh_status": "STOPPED", "release_accepted": False}
    outputs.update({
        "metadata/source_capture.json": _canonical(manifest) + b"\n",
        "metadata/source_manifest.json": _canonical(source_manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical(ready) + b"\n",
    })
    descriptors = {name: {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
                   for name, body in outputs.items()}
    payload = {"processing_identity": identity, "source_record_sha256": record_sha,
               "source_schema_sha256": schema_sha,
               "capture_receipts": {name: receipt for name, (_, receipt) in captures.items()},
               "objects": descriptors}
    fingerprint = hashlib.sha256(_canonical(identity)).hexdigest()
    try:
        for relative, body in outputs.items():
            write_immutable(staging_storage, f"{prefix}/{relative}", body)
        seal_ready_candidate(staging_storage, state_key, version, fingerprint, payload)
    except Exception as exc:
        quarantine_incomplete_capture(staging_storage, capture_root, DATASET_SLUG, version, captures,
                                      getattr(exc, "code", type(exc).__name__))
        raise
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "row_count": len(records), "source_record_sha256": record_sha,
        "source_schema_sha256": schema_sha, "refresh_status": "STOPPED",
        "drift_reasons": comparison_reasons, "native_fields": list(EXPECTED_FIELDS),
        "parquet_key": data_key, "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
        "reused": False, "release_accepted": False,
    }


class EPAFRSNAICSConfig(Config):
    offline_replay_version: str = ""
    offline_capture_version: str = ""
    capture_directory: str = ""


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="epa",
    compute_kind="http",
    description="Exact official EPA NAICS table 14 as a nonspatial draft; source vintage and published membership are unresolved.",
)
def epa_frs_naics_powerplants(context, config: EPAFRSNAICSConfig,
                              staging_storage: StagingStorageResource) -> dict:
    result = capture_epa_frs_naics_table(
        staging_storage,
        offline_replay_version=config.offline_replay_version or None,
        offline_capture_version=config.offline_capture_version or None,
        capture_directory=config.capture_directory or None,
    )
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_frs_naics_powerplants, blocking=True)
def epa_frs_naics_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("row_count") == EXPECTED_SOURCE_ROWS
              and candidate.get("source_record_sha256") == EXPECTED_SOURCE_RECORDS_SHA256
              and candidate.get("source_schema_sha256") == EXPECTED_SOURCE_SCHEMA_SHA256)
    return AssetCheckResult(passed=passed, metadata={
        "source_complete_rows": candidate.get("row_count", 0),
        "source_record_sha256": candidate.get("source_record_sha256", ""),
        "source_schema_sha256": candidate.get("source_schema_sha256", ""),
        "source_vintage": "2014-06", "release_accepted": False,
    })


@asset_check(asset=epa_frs_naics_powerplants, blocking=False)
def epa_frs_naics_published_membership_compatible(candidate: dict) -> AssetCheckResult:
    compatible = not candidate.get("drift_reasons")
    return AssetCheckResult(passed=compatible, metadata={
        "candidate_count": candidate.get("row_count", 0),
        "published_candidate_count": PUBLISHED_CANDIDATE_ROWS,
        "drift_reasons": candidate.get("drift_reasons", []),
        "release_accepted": False,
    })


epa_frs_naics_draft_job = define_asset_job(
    "epa_frs_naics_draft_refresh", selection=AssetSelection.assets(epa_frs_naics_powerplants)
)
epa_frs_naics_draft_schedule = ScheduleDefinition(
    name="epa_frs_naics_draft_refresh_schedule", job=epa_frs_naics_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    run_config={"ops": {"epa_frs_naics_powerplants": {"config": {"offline_replay_version": ""}}}},
)
