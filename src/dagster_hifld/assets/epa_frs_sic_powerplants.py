"""EPA FRS EIA-860 SIC table, preserved as a nonspatial draft candidate."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
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


DATASET_SLUG = "epa-frs-eia-860-power-generation-facilities-sic-codes"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://geodata.epa.gov/arcgis/rest/services/OEI/FRS_PowerPlants/MapServer"
PROFILE = ArcGISTableProfile(
    service_url=SERVICE_URL, table_id=15, table_name="FRS_SIC_POWERPLANTS",
    fields=(
        ("OBJECTID", "esriFieldTypeOID", False, None),
        ("REGISTRY_ID", "esriFieldTypeString", True, 12),
        ("SIC_CODE", "esriFieldTypeString", False, 4),
        ("PRIMARY_INDICATOR", "esriFieldTypeString", False, 10),
    ),
    relationship_name="POWERPLANT_LAYER", related_table_id=12,
    relationship_key="REGISTRY_ID", relationship_cardinality="esriRelCardinalityOneToMany",
    source_vintage="2014-06", expected_rows=72,
)
TABLE_URL = PROFILE.table_url
EXPECTED_ROWS = 72
EXPECTED_RECORD_SHA256 = "c21fed35ea448b9d34e6f41e2261ad2bccd58c7932bcfc086cc908cccd0fa77c"
EXPECTED_IDS_SHA256 = "a41b29774d93b917cd36702e7c27e62b24a7d0200697a59789894aaed81f2344"
EXPECTED_TRIPLETS_SHA256 = "6676be546d267c4587f389fafcbff65b56bead32a65294c00e9aaff0955e6c70"
EXPECTED_SCHEMA_SHA256 = "d95302c6e626ef7806877b59e6b57c513fcb39c4ea65d630b411201eb6482aa3"
PUBLISHED_ROWS = 74
PUBLISHED_IDS = tuple(range(1, 75))  # Exact feature OBJECTID roster from the pinned public GeoJSON.
PUBLISHED_IDS_SHA256 = "6de1799d84572b796f70a2087b5e26016f8a514824f437d72a3d7a8ced2db045"
PUBLISHED_TRIPLETS_SHA256 = "ca53076ede0c22019b3fe959c521910fb46cf47f27177599227dd19a663961c3"
PUBLISHED_COLLECTION_URL = (
    "https://storage.googleapis.com/hifld-next-datasets-prod/hifld/"
    f"{DATASET_SLUG}/{DATASET_SLUG}/v1.0.0/collection.json"
)
PUBLISHED_COLLECTION_SHA256 = "d885ccf9520bc0cb4d5fa7b7feb84436b6d65a1ae497fbea4a54f1421a325e8f"
PUBLISHED_ASSET_PINS = {
    "geojson": {"sha256": "521966dbb2f59ccae036750c665ab0e6f6322a9e544a58e65de69a4bc5f76013",
                "bytes": 11089, "md5_multihash": "d501102f5c736ad08202e3310ac7e7d0adeae6",
                "md5": "2f5c736ad08202e3310ac7e7d0adeae6", "etag": "2f5c736ad08202e3310ac7e7d0adeae6",
                "gcs_hash": "crc32c=RZXFSw==, md5=L1xzatCCAuMxCsfn0K3q5g==",
                "generation": "1791506366453982"},
    "geopackage": {"sha256": "fa48b18be3d6efe5b85a0536d1e13304afe2645e591a0ffe408bf3432b71d6cb",
                   "bytes": 106496, "md5_multihash": "d501104acf6ee0ff1e49f7cad0554ceb096882",
                   "md5": "4acf6ee0ff1e49f7cad0554ceb096882", "etag": "4acf6ee0ff1e49f7cad0554ceb096882",
                   "gcs_hash": "crc32c=sGEhhA==, md5=Ss9u4P8eSffK0FVM6wlogg==",
                   "generation": "1791506366423396"},
    "file_geodatabase": {"sha256": "a2954d0a3d9349b8b03c404dd1263c0f459daaeae903f37d177b62877ef2eb84",
                         "bytes": 30627, "md5_multihash": "d5011092c0575047a4538f49688e002b99a446",
                         "md5": "92c0575047a4538f49688e002b99a446", "etag": "92c0575047a4538f49688e002b99a446",
                         "gcs_hash": "crc32c=kvj5WA==, md5=ksBXUEekU49JaI4AK5mkRg==",
                         "generation": "1791506366402264"},
}


class EPAFRSSICError(ValueError):
    """The source snapshot cannot be represented as the pinned SIC candidate."""


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
        b"hifld:source_url": TABLE_URL.encode(), b"hifld:source_table": b"FRS_SIC_POWERPLANTS",
        b"hifld:geometry": b"absent",
    })
    table = pa.Table.from_pydict({name: [row[name] for row in rows] for name in PROFILE.field_names}, schema=schema)
    out = pa.BufferOutputStream()
    pq.write_table(table, out, compression="zstd", version="2.6")
    return out.getvalue().to_pybytes()


def _triplets_sha(rows: list[dict[str, Any]]) -> str:
    rows_for_hash = sorted([[row["REGISTRY_ID"], row["SIC_CODE"], row["PRIMARY_INDICATOR"]] for row in rows])
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
        raise EPAFRSSICError("cached_candidate_state_invalid") from exc
    payload = bundle.get("payload")
    if not isinstance(payload, dict):
        raise EPAFRSSICError("cached_candidate_payload_missing")
    receipts, objects = payload.get("capture_receipts"), payload.get("objects")
    expected_receipts = {"service", "service_after", "table_before", "count_before", "ids_before", "table_after",
                         "count_after", "ids_after", "page_000"}
    expected_objects = {"unknown/" + FILE_SLUG + ".parquet", "metadata/source_ids.json",
                        "metadata/published_candidate_ids.json", "metadata/source_capture.json",
                        "metadata/source_manifest.json", "metadata/source_audit.json",
                        "metadata/processing_identity.json", "metadata/candidate_ready.json"}
    if (not isinstance(receipts, dict) or set(receipts) != expected_receipts or not isinstance(objects, dict)
            or set(objects) != expected_objects | {f"raw/{name}.{ext}" for name in receipts
                                                   for ext in ("body", "wire", "receipt.json")}):
        raise EPAFRSSICError("cached_candidate_roster_invalid")
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


def capture_epa_frs_sic_table(staging_storage: StagingStorageResource, *,
                              offline_replay_version: str | None = None,
                              offline_capture_version: str | None = None,
                              client: httpx.Client | None = None) -> dict[str, Any]:
    """Capture fresh source evidence before any versioned cache reuse."""
    if not isinstance(staging_storage, StagingStorageResource) or isinstance(staging_storage, PublishedStorageResource):
        raise EPAFRSSICError("explicit_staging_resource_required")
    repo_root = Path(__file__).parents[3]
    identity = processing_identity(repo_root)
    identity_fingerprint = _sha(_canonical(identity))
    if offline_replay_version and offline_capture_version:
        raise EPAFRSSICError("choose_cached_reuse_or_retained_capture_replay")
    if offline_replay_version:
        return _reuse(staging_storage, version=offline_replay_version, identity=identity,
                      fingerprint=identity_fingerprint, offline=True)

    capture_root = Path("/tmp/hifld-native-reconstruction/71/captures") / f"refresh-{uuid4().hex}"
    captures: dict[str, tuple[bytes, dict[str, Any]]] = {}
    owned_client = client is None
    active = client or httpx.Client(follow_redirects=False, timeout=60.0)
    capture_mode = "live_source_capture"
    try:
        if offline_capture_version:
            capture_mode = "offline_retained_source_replay"
            source_prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, offline_capture_version)
            source_manifest = json.loads(staging_storage.read_key(f"{source_prefix}/metadata/source_capture.json"))
            retained_names = set(source_manifest.get("response_receipts", {}))
            expected_names = {"service", "service_after", "table_before", "count_before", "ids_before",
                              "table_after", "count_after", "ids_after", "page_000"}
            if retained_names != expected_names:
                raise EPAFRSSICError("retained_capture_roster_invalid")
            capture_root.mkdir(parents=True, exist_ok=True)
            for name in retained_names:
                for suffix in ("body", "wire", "receipt.json"):
                    payload = staging_storage.read_key(f"{source_prefix}/raw/{name}.{suffix}")
                    (capture_root / f"{name}.{suffix}").write_bytes(payload)
            rows, manifest, captures = capture_table(active, PROFILE, capture_root, replay_captures=True)
        else:
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
    if (len(rows) != EXPECTED_ROWS or record_sha != EXPECTED_RECORD_SHA256
            or ids_sha != EXPECTED_IDS_SHA256 or schema_sha != EXPECTED_SCHEMA_SHA256):
        quarantine_incomplete_capture(staging_storage, capture_root, DATASET_SLUG,
                                      "unversioned-refresh", captures, "complete_source_snapshot_drift")
        raise EPAFRSSICError("complete_source_snapshot_drift")

    version = content_derived_version(record_sha, "", schema_sha, identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if staging_storage.object_snapshot(state_key) is not None:
        return _reuse(staging_storage, version=version, identity=identity,
                      fingerprint=identity_fingerprint, offline=False,
                      observation=(captures, capture_root))

    drift = []
    if len(rows) != PUBLISHED_ROWS:
        drift.append("row_count_changed_from_published_candidate")
    if triplets_sha != PUBLISHED_TRIPLETS_SHA256:
        drift.append("row_values_changed_from_published_candidate")
    data = _typed_parquet(rows, tuple(manifest["fields"]))
    objects: dict[str, bytes] = {"unknown/" + FILE_SLUG + ".parquet": data}
    objects["metadata/source_ids.json"] = _canonical(sorted(row["OBJECTID"] for row in rows)) + b"\n"
    objects["metadata/published_candidate_ids.json"] = _canonical(list(PUBLISHED_IDS)) + b"\n"
    capture_objects = []
    for name, (body, receipt) in sorted(captures.items()):
        for suffix, raw in (("body", body), ("wire", (capture_root / f"{name}.wire").read_bytes()),
                            ("receipt.json", _canonical(receipt) + b"\n")):
            objects[f"raw/{name}.{suffix}"] = raw
        capture_objects.append({"name": name, "bytes": len(body), "sha256": _sha(body)})
    manifest.update({"captured_at": min(receipt["observed_at"] for _, receipt in captures.values()),
                     "capture_mode": capture_mode,
                     "source_record_sha256": record_sha, "source_schema_sha256": schema_sha,
                     "source_ids_sha256": ids_sha, "source_triplets_sha256": triplets_sha,
                     "published_candidate_count": PUBLISHED_ROWS,
                     "published_candidate_ids_sha256": PUBLISHED_IDS_SHA256,
                     "published_candidate_triplets_sha256": PUBLISHED_TRIPLETS_SHA256,
                     "published_collection_url": PUBLISHED_COLLECTION_URL,
                     "published_collection_sha256": PUBLISHED_COLLECTION_SHA256,
                     "published_asset_pins": PUBLISHED_ASSET_PINS,
                     "refresh_status": "STOPPED", "drift_reasons": drift,
                     "publication_approval": "not_established_by_source_capture",
                     "capture_objects": capture_objects,
                     "parquet_object": {"key": f"{prefix}/unknown/{FILE_SLUG}.parquet",
                                        "bytes": len(data), "sha256": _sha(data)}})
    source_manifest = {
        "title": "EPA Facility Registry Service - EIA-860 Power Generation Facilities SIC Codes",
        "description": "EPA OEI FRS_POWERPLANTS MapServer table 15 FRS_SIC_POWERPLANTS; native nonspatial table. Service description dates the EIA-860 compilation to June 2014. The public HIFLD candidate has 74 rows and differs from the live table; publication is blocked pending maintainer decision.",
        "publisher": "U.S. Environmental Protection Agency, Office of Environmental Information (EPA_GEO)",
        "agency": "Environmental Protection Agency", "source_url": TABLE_URL,
        "license": "U.S. EPA public domain (source service item license statement)",
        "date_issued": None, "date_modified": None, "source_modified": None,
        "lineage": ["EPA OEI FRS_PowerPlants MapServer table 15: FRS_SIC_POWERPLANTS",
                    "Declared one-to-many POWERPLANT_LAYER relationship to table 12 on REGISTRY_ID",
                    "Service description states EIA-860 content was current as of June 2014"],
        "source_vintage": "2014-06", "native_geometry": "absent", "refresh_status": "STOPPED",
        "published_metadata_issue": "collection table:columns omits OBJECTID, while the three fetched published files contain it",
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


class EPAFRSSICConfig(Config):
    offline_replay_version: str = ""
    offline_capture_version: str = ""


@asset(key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]), group_name="epa", compute_kind="http",
       description="Exact EPA SIC table 15 as a nonspatial candidate; vintage and published membership block release.")
def epa_frs_sic_powerplants(context, config: EPAFRSSICConfig,
                            staging_storage: StagingStorageResource) -> dict:
    result = capture_epa_frs_sic_table(
        staging_storage,
        offline_replay_version=config.offline_replay_version or None,
        offline_capture_version=config.offline_capture_version or None,
    )
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])])
    context.add_output_metadata(result)
    return result


@asset_check(asset=epa_frs_sic_powerplants, blocking=True)
def epa_frs_sic_complete(candidate: dict) -> AssetCheckResult:
    passed = (candidate.get("row_count") == EXPECTED_ROWS
              and candidate.get("source_record_sha256") == EXPECTED_RECORD_SHA256
              and candidate.get("source_schema_sha256") == EXPECTED_SCHEMA_SHA256)
    return AssetCheckResult(passed=passed, metadata={"source_complete_rows": candidate.get("row_count", 0),
        "source_record_sha256": candidate.get("source_record_sha256", ""),
        "source_schema_sha256": candidate.get("source_schema_sha256", ""),
        "source_vintage": "2014-06", "release_accepted": False})


@asset_check(asset=epa_frs_sic_powerplants, blocking=False)
def epa_frs_sic_published_membership_compatible(candidate: dict) -> AssetCheckResult:
    drift = candidate.get("drift_reasons", [])
    return AssetCheckResult(passed=not drift, metadata={"source_rows": candidate.get("row_count", 0),
        "published_candidate_rows": PUBLISHED_ROWS, "drift_reasons": drift, "release_accepted": False})


epa_frs_sic_draft_job = define_asset_job("epa_frs_sic_draft_refresh",
    selection=AssetSelection.assets(epa_frs_sic_powerplants))
epa_frs_sic_draft_schedule = ScheduleDefinition(name="epa_frs_sic_draft_refresh_schedule",
    job=epa_frs_sic_draft_job, cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
    run_config={"ops": {"epa_frs_sic_powerplants": {"config": {"offline_replay_version": ""}}}})
