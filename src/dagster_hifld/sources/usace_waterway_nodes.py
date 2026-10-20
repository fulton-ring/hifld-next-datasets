"""Native, unapproved FileGDB candidate for BTS NTAD waterway network nodes."""
from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import (
    canonical_construction_bundle, load_first_construction, seal_first_construction,
)
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.sources.arcgis_filegdb import (
    canonical_json,
    capture_filegdb_replica,
    extract_filegdb,
    sha256,
    stage_filegdb_archive,
    verify_extracted_filegdb_cache,
    verify_staged_filegdb,
)
from dagster_hifld.sources.epa_frs_common import write_immutable
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "navigable-waterway-network-node-locations-1"
FILE_SLUG = DATASET_SLUG
BTS_ITEM_ID = "5bc82e3026354c9d92cbb551b3b2e2d2"
USACE_SERVICE_ITEM_ID = "ace7645d305647448a84492a3b909d48"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
USACE_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{USACE_SERVICE_ITEM_ID}?f=json"
BTS_DATA_DICTIONARY_DOI = "https://doi.org/10.21949/1529054"
HIFLD_CROSSWALK_DOI = "https://doi.org/10.3886/E241367V1"
HISTORICAL_ARCHIVE_URL = "https://www.datalumos.org/datalumos/project/239729/version/V1/view"
SERVICE_URL = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Waterway_Networks/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
EXPECTED_COUNT = 6255
EXPECTED_SOURCE_IDS = tuple(range(1, EXPECTED_COUNT + 1))
EXPECTED_FIELDS = (
    "OBJECTID", "DATA", "NODENUM", "FEATURID", "PORT_ID", "PORT_NAME",
    "STATE", "WHO_MOD", "DATE_MOD", "NODE_SRC", "FIPS", "NON_US",
)
EXPECTED_FIELD_TYPES = (
    ("OBJECTID", "esriFieldTypeOID", None, False),
    ("DATA", "esriFieldTypeBigInteger", None, True),
    ("NODENUM", "esriFieldTypeInteger", None, True),
    ("FEATURID", "esriFieldTypeInteger", None, True),
    ("PORT_ID", "esriFieldTypeString", 15, True),
    ("PORT_NAME", "esriFieldTypeString", 35, True),
    ("STATE", "esriFieldTypeString", 2, True),
    ("WHO_MOD", "esriFieldTypeString", 3, True),
    ("DATE_MOD", "esriFieldTypeString", 8, True),
    ("NODE_SRC", "esriFieldTypeString", 5, True),
    ("FIPS", "esriFieldTypeString", 2, True),
    ("NON_US", "esriFieldTypeString", 2, True),
)
EXPECTED_ID_FIELD = "OBJECTID"
EXPECTED_CRS = 4269
EXPECTED_EXTENT = (-179.3121, -52.99999999999994, 179.13519000000008, 72.19972000000007)
MAX_METADATA_BYTES = 8_000_000
RUNTIME_ROOT = Path("/workspace/work/hifld-coordinator/179/runtime")


def _capture(client: httpx.Client, url: str, directory: Path, name: str) -> tuple[bytes, dict[str, object]]:
    """Capture one bounded UTF-8 metadata/query response with a digest receipt."""
    with client.stream("GET", url, headers={"Accept-Encoding": "identity"}, timeout=60) as response:
        chunks = []
        size = 0
        digest = hashlib.sha256()
        for chunk in response.iter_bytes():
            size += len(chunk)
            if size > MAX_METADATA_BYTES:
                raise ValueError("source_metadata_response_exceeds_limit:" + name)
            chunks.append(chunk)
            digest.update(chunk)
        body = b"".join(chunks)
        final = response.url
        receipt = {
            "method": "GET", "url": url, "final_host": final.host, "final_path": final.path,
            "status": response.status_code, "complete_body": response.is_closed,
            "bytes": len(body), "sha256": digest.hexdigest(),
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }
    if receipt["status"] != 200 or not body:
        raise ValueError("source_metadata_capture_failed:" + name)
    (directory / f"{name}.body").write_bytes(body)
    (directory / f"{name}.receipt.json").write_bytes(canonical_json(receipt) + b"\n")
    return body, receipt


def _validate_source(bts: dict, item: dict, service: dict, layer: dict, count: dict, ids: dict) -> tuple[list[dict], list[int]]:
    """Fail closed on identity, schema, geometry, population, or ID drift."""
    if (bts.get("id") != BTS_ITEM_ID or bts.get("owner") != "USDOT_BTS"
            or (bts.get("url") or "").rstrip("/").casefold() != LAYER_URL.casefold()):
        raise ValueError("BTS_official_item_identity_or_layer_link_drift")
    if (item.get("id") != USACE_SERVICE_ITEM_ID or item.get("owner") != "usace_iwr_cac"
            or (item.get("url") or "").rstrip("/").casefold() != SERVICE_URL.casefold()):
        raise ValueError("USACE_actual_service_owner_or_item_drift")
    if service.get("serviceItemId") != USACE_SERVICE_ITEM_ID:
        raise ValueError("USACE_service_item_identity_drift")
    if "Extract" not in str(service.get("capabilities", "")).split(","):
        raise ValueError("USACE_extract_capability_drift")
    if "filegdb" not in str(service.get("supportedExportFormats", "")).lower().split(","):
        raise ValueError("USACE_filegdb_export_no_longer_supported")
    if (layer.get("id") != LAYER_ID or layer.get("name") != "Waterway_Network_Node"
            or layer.get("geometryType") != "esriGeometryPoint"):
        raise ValueError("USACE_exact_node_layer_or_geometry_drift")
    if layer.get("hasZ") is not False or layer.get("hasM") is not False:
        raise ValueError("USACE_node_ZM_contract_drift")
    extent = layer.get("extent", {})
    if extent.get("spatialReference", {}).get("latestWkid") != EXPECTED_CRS:
        raise ValueError("USACE_node_CRS_drift")
    observed_extent = (extent.get("xmin"), extent.get("ymin"), extent.get("xmax"), extent.get("ymax"))
    if (any(not isinstance(value, (int, float)) for value in observed_extent)
            or any(not math.isclose(float(got), expected, rel_tol=0.0, abs_tol=1e-6)
                   for got, expected in zip(observed_extent, EXPECTED_EXTENT))):
        raise ValueError("USACE_node_coverage_extent_drift")
    fields = layer.get("fields")
    if not isinstance(fields, list) or [f.get("name") for f in fields] != list(EXPECTED_FIELDS):
        raise ValueError("USACE_node_field_roster_drift")
    field_contract = [(f.get("name"), f.get("type"), f.get("length"), f.get("nullable")) for f in fields]
    if field_contract != list(EXPECTED_FIELD_TYPES):
        raise ValueError("USACE_node_field_type_or_nullability_drift")
    id_meta = next(f for f in fields if f["name"] == EXPECTED_ID_FIELD)
    if id_meta.get("type") != "esriFieldTypeOID" or id_meta.get("nullable") is not False:
        raise ValueError("USACE_node_ID_contract_drift")
    if count.get("count") != EXPECTED_COUNT:
        raise ValueError("USACE_node_population_drift")
    values = ids.get("objectIds")
    if (not isinstance(values, list) or len(values) != EXPECTED_COUNT
            or len(set(values)) != EXPECTED_COUNT or any(type(value) is not int for value in values)):
        raise ValueError("USACE_node_ID_population_invalid")
    source_ids = sorted(values)
    if tuple(source_ids) != EXPECTED_SOURCE_IDS:
        raise ValueError("USACE_node_ID_baseline_drift")
    return fields, source_ids


def _processing_identity() -> dict[str, object]:
    root = Path(__file__).parents[3]
    files = {str(path.relative_to(root)): sha256_file(path)
             for path in sorted(root.joinpath("src/dagster_hifld").rglob("*.py"))}
    files.update({name: sha256_file(root / name) for name in ("pyproject.toml", "uv.lock")})
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    return {"code_commit": commit, "files": files, "python": os.sys.version.split()[0]}


def _version(archive_hash: str, fields: list[dict], ids: list[int], identity: dict) -> str:
    return "v" + hashlib.sha256(canonical_json({
        "archive": archive_hash, "schema": fields, "ids": ids,
        "processing_identity": identity,
    })).hexdigest()[:32]


def _construction_key(version: str) -> str:
    return f"_construction-state/{DATASET_SLUG}/{FILE_SLUG}/{version}.json"


def _preserved_receipts_for_same_construction(storage, version: str, identity: dict,
                                              archive_hash: str, archive_bytes: int, native_objects: list[dict],
                                              observed_receipts: dict):
    """Keep first receipts when a refresh recreates the exact same version."""
    state_key = _construction_key(version)
    if storage.object_snapshot(state_key) is None:
        return None
    try:
        marker = json.loads(storage.read_key(state_key))
        fingerprint = marker["processing_fingerprint"]
        body = load_first_construction(storage, state_key, version, fingerprint)
        bundle = json.loads(body)
        payload = bundle["payload"]
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("node_first_construction_witness_invalid") from exc
    if (set(payload) != {"processing_identity", "archive_sha256", "objects", "native_objects", "capture_receipts"}
            or payload.get("processing_identity") != identity
            or payload.get("archive_sha256") != archive_hash
            or payload.get("native_objects") != native_objects
            or not isinstance(payload.get("capture_receipts"), dict)):
        raise ValueError("node_same_version_first_construction_changed")
    original_receipts = payload["capture_receipts"]
    if not isinstance(observed_receipts, dict) or set(observed_receipts) != set(original_receipts):
        raise ValueError("node_same_version_refresh_receipt_roster_changed")
    for receipt in observed_receipts.values():
        if (not isinstance(receipt, dict) or receipt.get("status") != 200
                or not isinstance(receipt.get("observed_at"), str)
                or type(receipt.get("bytes")) is not int
                or not isinstance(receipt.get("sha256"), str) or len(receipt["sha256"]) != 64):
            raise ValueError("node_same_version_refresh_receipt_invalid")
    current_archive = observed_receipts["filegdb_export"]
    if (current_archive.get("sha256") != archive_hash
            or current_archive.get("bytes") != archive_bytes):
        raise ValueError("node_same_version_refresh_archive_changed")
    return payload


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_retained_input(root: Path) -> tuple[Path, dict, dict, list[int]]:
    """Load only the captured native archive and its original receipt documents."""
    root = Path(root).absolute()
    manifest_path, audit_path = root / "metadata/source_manifest.json", root / "metadata/source_audit.json"
    archive_path = root / "file_geodatabase/source_filegdb.zip"
    if any(not path.is_file() or path.is_symlink() for path in (manifest_path, audit_path, archive_path)):
        raise ValueError("retained_native_input_missing_or_unsafe")
    manifest, audit = json.loads(manifest_path.read_text()), json.loads(audit_path.read_text())
    if (manifest.get("source_url") != LAYER_URL or manifest.get("bts_distribution_item") != BTS_ITEM_URL
            or manifest.get("source_count") != EXPECTED_COUNT or audit.get("source_count") != EXPECTED_COUNT
            or manifest.get("source_id_field") != EXPECTED_ID_FIELD
            or manifest.get("source_CRS") != f"EPSG:{EXPECTED_CRS}"
            or manifest.get("source_geometry_type") != "Point"
            or manifest.get("source_hasZ") is not False or manifest.get("source_hasM") is not False
            or audit.get("source_archive_sha256") != manifest.get("source_archive_sha256")
            or audit.get("source_archive_bytes") != manifest.get("source_archive_bytes")):
        raise ValueError("retained_native_input_contract_drift")
    fields = manifest.get("source_fields")
    if (not isinstance(fields, list)
            or [(f.get("name"), f.get("type"), f.get("length"), f.get("nullable")) for f in fields]
            != list(EXPECTED_FIELD_TYPES)):
        raise ValueError("retained_native_input_schema_drift")
    receipts = audit.get("capture_receipts")
    if not isinstance(receipts, dict) or receipts != manifest.get("capture_receipts"):
        raise ValueError("retained_native_input_receipts_drift")
    required = {"bts_item", "usace_item", "service_before", "layer_before", "count_before", "ids_before",
                "replica_submission", "filegdb_export", "layer_after", "count_after", "ids_after"}
    if set(receipts) != required:
        raise ValueError("retained_native_input_receipt_roster_drift")
    for name, receipt in receipts.items():
        if (receipt.get("status") != 200 or not isinstance(receipt.get("url"), str)
                or not isinstance(receipt.get("observed_at"), str)
                or not isinstance(receipt.get("bytes"), int)
                or not isinstance(receipt.get("sha256"), str)
                or len(receipt["sha256"]) != 64):
            raise ValueError("retained_native_input_receipt_invalid:" + name)
    export = receipts["filegdb_export"]
    if (export.get("sha256") != audit.get("source_archive_sha256")
            or export.get("bytes") != audit.get("source_archive_bytes")
            or _hash_file(archive_path) != export["sha256"]
            or archive_path.stat().st_size != export["bytes"]):
        raise ValueError("retained_native_archive_digest_drift")
    ids = list(EXPECTED_SOURCE_IDS)
    if sha256(canonical_json(ids)) != audit.get("source_id_sha256"):
        raise ValueError("retained_native_input_ID_baseline_drift")
    if sha256(canonical_json(fields)) != audit.get("source_schema_sha256"):
        raise ValueError("retained_native_input_schema_digest_drift")
    if (receipts["replica_submission"].get("request_parameters", {}).get("syncModel") != "none"
            or receipts["replica_submission"].get("request_parameters", {}).get("dataFormat") != "filegdb"):
        raise ValueError("retained_native_input_replica_mode_drift")
    return archive_path, manifest, audit, ids


def _validate_cached_candidate(storage, prefix: str, state_key: str, version: str, fingerprint: str,
                               identity: dict, archive_hash: str, objects: dict, receipts: dict) -> None:
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or payload.get("processing_identity") != identity
            or payload.get("archive_sha256") != archive_hash or payload.get("objects") != objects
            or payload.get("capture_receipts") != receipts):
        raise ValueError("cached_node_first_construction_identity_drift")
    expected = {f"{prefix}/{name}" for name in objects}
    source_manifest = objects.get("metadata/source_manifest.json")
    allowed_sidecars = {"metadata/quality_manifest.json", "metadata/data_dictionary.json",
                        "metadata/resolved_provenance.json", "metadata/source/quality_manifest.json",
                        "metadata/source/data_dictionary.json", "metadata/source/source_manifest.json"}
    if source_manifest:
        allowed_sidecars.add(f"metadata/source/version_{source_manifest['sha256']}_source_manifest.json")
    extracted_cache = verify_extracted_filegdb_cache(
        storage, prefix, [{"path": name, **descriptor} for name, descriptor in objects.items()],
    )
    actual = set(storage.list_prefix(prefix))
    allowed = expected | set(extracted_cache) | {f"{prefix}/{name}" for name in allowed_sidecars}
    if not expected.issubset(actual) or actual - allowed:
        raise ValueError("cached_node_candidate_roster_changed")
    for name, descriptor in objects.items():
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if (snapshot is None or descriptor != {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
                or storage.object_snapshot(key) != snapshot):
            raise ValueError("cached_node_candidate_object_integrity_failed:" + name)
    source_snapshots = {name for name in allowed_sidecars if name.startswith("metadata/source/")}
    for name in source_snapshots & {key[len(prefix) + 1:] for key in actual}:
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if name.endswith("source_manifest.json"):
            descriptor = source_manifest
        else:
            source_name = name.removeprefix("metadata/source/")
            primary_key = f"{prefix}/metadata/{source_name}"
            primary = storage.object_snapshot(primary_key)
            descriptor = ({"bytes": primary.size, "sha256": storage.sha256_key(primary_key)}
                          if primary is not None else None)
        if (snapshot is None or descriptor != {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
                or storage.object_snapshot(key) != snapshot):
            raise ValueError("cached_node_catalog_source_snapshot_changed:" + name)
    resolved_key = f"{prefix}/metadata/resolved_provenance.json"
    if resolved_key in actual:
        resolved = json.loads(storage.read_key(resolved_key))
        dictionary_key = f"{prefix}/metadata/data_dictionary.json"
        expected_resolved = (provenance_projection(
            load_resolved_source_manifest(storage, DATASET_SLUG, FILE_SLUG, version).metadata
        ) if dictionary_key in actual else provenance_projection(
            json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))))
        if resolved != expected_resolved:
            raise ValueError("cached_node_resolved_provenance_changed")
        if dictionary_key in actual and provenance_projection(json.loads(storage.read_key(dictionary_key))) != expected_resolved:
            raise ValueError("cached_node_catalog_dictionary_provenance_changed")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])


def _provenance_inputs(receipts: dict) -> list[dict]:
    delegation = {
        "bts_catalog_item_url": BTS_ITEM_URL, "bts_item_id": BTS_ITEM_ID,
        "bts_item_owner": "USDOT_BTS", "bts_item_layer_link": LAYER_URL,
        "usace_service_item_url": USACE_ITEM_URL,
        "usace_service_item_id": USACE_SERVICE_ITEM_ID,
        "usace_service_item_owner": "usace_iwr_cac",
        "basis": "The BTS NTAD item identifies the official distribution and links this exact USACE Feature Service layer; the linked service item identifies its owner.",
    }
    result = []
    for name, receipt in sorted(receipts.items()):
        retrieval = receipt["url"]
        result.append({
            "input_id": name, "producer": "USDOT Bureau of Transportation Statistics NTAD / U.S. Army Corps of Engineers",
            "host": urlsplit(retrieval).hostname, "role": "official_distributor",
            "canonical_url": LAYER_URL if name not in ("bts_item", "usace_item") else (BTS_ITEM_URL if name == "bts_item" else USACE_ITEM_URL),
            "retrieval_url": retrieval, "retrieved_at": receipt["observed_at"],
            "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "license": "Unknown; rights review pending.",
            "attribution": "Provisional attribution is recorded in BTS/USACE portal metadata; rights and attribution review remain pending.",
            "delegation_evidence": delegation, "release_date": None,
            "release_date_unknown_reason": "Portal/service edit timestamps do not establish a named source release date.",
        })
    return result


def capture_usace_waterway_nodes(
    storage: StagingStorageResource, *, capture_directory: str | None = None,
    offline_replay: bool = False, retained_input_directory: str | None = None,
    expected_cache_version: str | None = None,
) -> dict[str, object]:
    """Build or replay one native FileGDB draft; no publication contract is approved."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if offline_replay != bool(retained_input_directory):
        raise ValueError("offline_replay_requires_explicit_retained_input_directory")
    scratch_root = RUNTIME_ROOT / "scratch"
    scratch_root.mkdir(parents=True, exist_ok=True)
    if offline_replay:
        archive_path, prior_manifest, prior_audit, source_ids = _load_retained_input(Path(retained_input_directory))
        fields = prior_manifest["source_fields"]
        receipts = prior_audit["capture_receipts"]
        source_release_metadata = {
            key: prior_manifest.get(key) for key in
            ("title", "description", "publisher", "agency", "producer_lineage", "bts_data_dictionary",
             "hifld_crosswalk_archive", "source_release_date", "source_release_date_note", "historical_archive")
        }
        capture_root = None
        filegdb_dir = Path(retained_input_directory) / "file_geodatabase"
        prior_gdb_roots = list(filegdb_dir.glob("*.gdb"))
        if len(prior_gdb_roots) != 1 or prior_gdb_roots[0].is_symlink():
            raise ValueError("retained_native_input_GDB_roster_drift")
        prior_gdb_root = prior_gdb_roots[0]
        if {path.name for path in filegdb_dir.iterdir()} != {"source_filegdb.zip", prior_gdb_root.name}:
            raise ValueError("retained_native_input_filegdb_directory_drift")
        if any(path.is_symlink() or (not path.is_dir() and not path.is_file())
               for path in prior_gdb_root.rglob("*")):
            raise ValueError("retained_native_input_GDB_member_unsafe")
    else:
        capture_base = Path(capture_directory) if capture_directory else RUNTIME_ROOT / "captures"
        capture_base.mkdir(parents=True, exist_ok=True)
        capture_root = capture_base / uuid4().hex
        capture_root.mkdir(parents=True, exist_ok=False)
        count_url = LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnCountOnly": "true", "f": "json"})
        ids_url = LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnIdsOnly": "true", "f": "json"})
        urls = {"bts_item": BTS_ITEM_URL, "usace_item": USACE_ITEM_URL,
                "service_before": SERVICE_URL + "?f=json", "layer_before": LAYER_URL + "?f=json",
                "count_before": count_url, "ids_before": ids_url}
        captured: dict[str, tuple[bytes, dict[str, object]]] = {}
        with httpx.Client(follow_redirects=True, timeout=60,
                          headers={"User-Agent": "hifld-next-datasets/usace-waterway-node-draft"}) as client:
            for name, url in urls.items():
                captured[name] = _capture(client, url, capture_root, name)
            docs = {name: json.loads(body) for name, (body, _) in captured.items()}
            fields, source_ids = _validate_source(
                docs["bts_item"], docs["usace_item"], docs["service_before"],
                docs["layer_before"], docs["count_before"], docs["ids_before"],
            )
            archive_path, archive_receipt, _replica, submission_receipt = capture_filegdb_replica(
                client, SERVICE_URL, LAYER_ID, f"hifld-179-{uuid4().hex}", capture_root,
            )
            captured["replica_submission"] = ((capture_root / "replica_submission.body").read_bytes(), submission_receipt)
            captured["filegdb_export"] = (archive_path.read_bytes(), archive_receipt)
            for name, url in (("layer_after", urls["layer_before"]), ("count_after", count_url), ("ids_after", ids_url)):
                captured[name] = _capture(client, url, capture_root, name)
        after = {name: json.loads(body) for name, (body, _) in captured.items() if name.endswith("_after")}
        if (after["layer_after"] != docs["layer_before"]
                or after["count_after"].get("count") != EXPECTED_COUNT
                or sorted(after["ids_after"].get("objectIds", [])) != source_ids):
            raise ValueError("USACE_node_source_changed_during_capture")
        receipts = {name: receipt for name, (_, receipt) in sorted(captured.items())}
        source_release_metadata = {"title": "Navigable Waterway Network Node Locations",
            "description": "BTS NTAD / USACE native point FileGDB source candidate; schema, ID, release vintage, rights, and any published mapping remain pending maintainer decision.",
            "publisher": "USDOT Bureau of Transportation Statistics (NTAD); official layer hosted by USACE item owner usace_iwr_cac",
            "agency": "U.S. Army Corps of Engineers",
            "producer_lineage": "BTS metadata says NWN is periodically updated by USACE, is part of BTS NTAD, and was created on behalf of BTS, USACE, Census, and USCG by Vanderbilt University and Oak Ridge National Laboratory.",
            "bts_data_dictionary": BTS_DATA_DICTIONARY_DOI, "hifld_crosswalk_archive": HIFLD_CROSSWALK_DOI,
            "source_release_date": None,
            "source_release_date_note": "ArcGIS portal/service edit timestamps do not establish a named source release date.",
            "historical_archive": HISTORICAL_ARCHIVE_URL + " (recovery reference only; not a live refresh source)"}

    archive_hash = str(receipts["filegdb_export"]["sha256"])
    identity = _processing_identity()
    version = _version(archive_hash, fields, source_ids, identity)
    if expected_cache_version and expected_cache_version != version:
        raise ValueError("requested_node_cache_version_mismatch")
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    ids_sha = sha256(canonical_json(source_ids))
    schema_sha = sha256(canonical_json(fields))
    with tempfile.TemporaryDirectory(prefix="hifld-179-", dir=scratch_root) as scratch:
        gdb_path, gdb_root, integrity = extract_filegdb(
            archive_path, Path(scratch), expected_layer="Waterway_Network_Node",
            expected_property_names=[name for name in EXPECTED_FIELDS if name != EXPECTED_ID_FIELD],
            expected_count=EXPECTED_COUNT, expected_ids=source_ids, expected_zm=False, binary_fields=(),
        )
        if offline_replay:
            current_files = {path.relative_to(prior_gdb_root).as_posix(): _hash_file(path)
                             for path in prior_gdb_root.rglob("*") if path.is_file()}
            replay_files = {path.relative_to(gdb_path).as_posix(): _hash_file(path)
                            for path in gdb_path.rglob("*") if path.is_file()}
            if prior_gdb_root.name != gdb_root or current_files != replay_files:
                raise ValueError("retained_native_GDB_does_not_match_original_archive")
        import fiona
        with fiona.open(gdb_path, layer="Waterway_Network_Node") as source:
            bounds = [math.inf, math.inf, -math.inf, -math.inf]
            geometry_count = null_geometry_count = 0
            for feature in source:
                geometry = feature.geometry
                if geometry is None:
                    null_geometry_count += 1
                    continue
                if geometry.type != "Point":
                    raise ValueError("USACE_node_nonpoint_geometry")
                x, y = geometry.coordinates[:2]
                if not math.isfinite(x) or not math.isfinite(y):
                    raise ValueError("USACE_node_nonfinite_coordinate")
                bounds = [min(bounds[0], x), min(bounds[1], y), max(bounds[2], x), max(bounds[3], y)]
                geometry_count += 1
        if (geometry_count + null_geometry_count != EXPECTED_COUNT or geometry_count == 0
                or any(not math.isclose(actual, expected, rel_tol=0.0, abs_tol=1e-6)
                       for actual, expected in zip(bounds, EXPECTED_EXTENT))):
            raise ValueError("USACE_node_export_geometry_coverage_drift")
        objects_list = stage_filegdb_archive(storage, prefix, archive_path, gdb_path, gdb_root)
    archive_descriptor = next(obj for obj in objects_list
                              if obj["sha256"] == archive_hash and obj["bytes"] == receipts["filegdb_export"]["bytes"])
    objects = {obj["path"]: {"bytes": obj["bytes"], "sha256": obj["sha256"]}
               for obj in objects_list}
    fingerprint = hashlib.sha256(canonical_json({"version": version, "identity": identity,
                                                  "native_objects": objects_list})).hexdigest()
    state_key = _construction_key(version)
    original = _preserved_receipts_for_same_construction(
        storage, version, identity, archive_hash, archive_descriptor["bytes"], objects_list, receipts,
    )
    if original is not None:
        receipts = original["capture_receipts"]
        original_objects = original.get("objects")
        if not isinstance(original_objects, dict):
            raise ValueError("node_first_construction_objects_invalid")
        _validate_cached_candidate(storage, prefix, state_key, version, fingerprint,
                                   identity, archive_hash, original_objects, receipts)
        staged_integrity = verify_staged_filegdb(storage, prefix, objects_list)
        original_audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_id_sha256": ids_sha,
            "source_archive_sha256": archive_hash, "source_archive_bytes": archive_descriptor["bytes"],
            "candidate_ready": True, "native_filegdb_preserved": True, "reused": True,
            "staged_filegdb_integrity": staged_integrity,
            "source_null_geometry_count": original_audit.get("source_null_geometry_count"),
            "source_release_accepted": False, "freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "historical_published_ids_reconciled": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}
    manifest = {
        "schema_version": 1, "approved": False, "title": source_release_metadata["title"],
        "description": source_release_metadata["description"], "publisher": source_release_metadata["publisher"],
        "agency": source_release_metadata["agency"], "producer_lineage": source_release_metadata["producer_lineage"],
        "bts_distribution_item": BTS_ITEM_URL, "bts_data_dictionary": source_release_metadata["bts_data_dictionary"],
        "hifld_crosswalk_archive": source_release_metadata["hifld_crosswalk_archive"],
        "source_url": LAYER_URL, "source_release_date": None,
        "source_release_date_note": source_release_metadata["source_release_date_note"],
        "license": "Unknown; rights review pending.", "candidate": True,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": _provenance_inputs(receipts),
        "transformations": [{"operation": "Export-only ArcGIS syncModel=none FileGDB capture; exact point layer; no geometry or attribute conversion",
            "parameters": {"layer_id": LAYER_ID, "source_fields": len(fields), "source_CRS": f"EPSG:{EXPECTED_CRS}",
                "hasZ": False, "hasM": False, "feature_count": EXPECTED_COUNT, "archive_sha256": archive_hash,
                "capture_mode": "retained_input_offline_replay" if offline_replay else "live_capture",
                "retained_metadata_body_policy": "original metadata response bodies were not retained; original receipt hashes/timestamps are preserved" if offline_replay else "metadata response bodies are available in the retained capture directory"}}],
        "lineage": "BTS NTAD item 5bc82e3026354c9d92cbb551b3b2e2d2 links layer 0 of USACE service item ace7645d305647448a84492a3b909d48, owned by usace_iwr_cac. The native source identity remains pending maintainer decision.",
        "source_fields": fields, "source_id_field": EXPECTED_ID_FIELD, "source_count": EXPECTED_COUNT,
        "source_CRS": f"EPSG:{EXPECTED_CRS}", "source_geometry_type": "Point",
        "source_hasZ": False, "source_hasM": False, "source_extent": list(EXPECTED_EXTENT),
        "exported_geometry_extent": bounds, "source_binary_fields": [],
        "source_archive_sha256": archive_hash, "source_archive_bytes": archive_descriptor["bytes"],
        "source_null_geometry_count": null_geometry_count, "capture_receipts": receipts,
        "historical_archive": source_release_metadata["historical_archive"],
        "unsupported_output_formats": {
            "shapefile_zip": "No approved mapping for complete source field names/types and source ID behavior.",
            "geopackage": "No accepted mapping for the complete native field and ID contract.",
            "geoparquet": "No accepted type, ID, and geometry preservation mapping for this source.",
            "pmtiles": "No approved display generalization or loss policy exists for analytic network nodes.",
        },
        "publication_gate": {"source_release_accepted": False, "freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "historical_published_ids_reconciled": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False},
        "processing_identity": identity,
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    resolved_provenance = provenance_projection(manifest)
    audit = {"source_count": EXPECTED_COUNT, "source_id_sha256": ids_sha,
        "source_schema_sha256": schema_sha, "source_fields": fields,
        "source_CRS": f"EPSG:{EXPECTED_CRS}", "source_geometry_type": "Point",
        "source_hasZ": False, "source_hasM": False, "source_extent": list(EXPECTED_EXTENT),
        "exported_geometry_extent": bounds, "source_archive_sha256": archive_hash,
        "source_archive_bytes": archive_descriptor["bytes"], "source_null_geometry_count": null_geometry_count,
        "filegdb_integrity": integrity, "capture_receipts": receipts,
        "source_release_date": None, **manifest["publication_gate"]}
    metadata = {"metadata/source_manifest.json": canonical_json(manifest) + b"\n",
                "metadata/source_audit.json": canonical_json(audit) + b"\n"}
    for name, body in metadata.items():
        write_immutable(storage, f"{prefix}/{name}", body)
        objects[name] = {"bytes": len(body), "sha256": sha256(body)}
    # Catalog owns the resolved projection after catalog materialization. Keep
    # the initial candidate projection for pre-catalog consumers, but do not
    # overwrite the catalog-enriched version during a strict cached replay.
    resolved_key = f"{prefix}/metadata/resolved_provenance.json"
    resolved_snapshot = storage.object_snapshot(resolved_key)
    if resolved_snapshot is None:
        write_immutable(storage, resolved_key, canonical_json(resolved_provenance) + b"\n")
    else:
        dictionary_key = f"{prefix}/metadata/data_dictionary.json"
        expected_resolved = (provenance_projection(
            load_resolved_source_manifest(storage, DATASET_SLUG, FILE_SLUG, version).metadata
        ) if storage.object_snapshot(dictionary_key) is not None else provenance_projection(manifest))
        if json.loads(storage.read_key(resolved_key)) != expected_resolved:
            raise ValueError("cached_node_resolved_provenance_changed")
        if (storage.object_snapshot(dictionary_key) is not None
                and provenance_projection(json.loads(storage.read_key(dictionary_key))) != expected_resolved):
            raise ValueError("cached_node_catalog_dictionary_provenance_changed")
    fingerprint = hashlib.sha256(canonical_json({"version": version, "identity": identity,
                                                  "native_objects": objects_list})).hexdigest()
    state_key = _construction_key(version)
    replayed = storage.object_snapshot(state_key) is not None
    payload = {"processing_identity": identity, "archive_sha256": archive_hash,
               "objects": objects, "native_objects": objects_list, "capture_receipts": receipts}
    bundle = canonical_construction_bundle(version, fingerprint, payload)
    if replayed:
        _validate_cached_candidate(storage, prefix, state_key, version, fingerprint,
                                   identity, archive_hash, objects, receipts)
        seal_first_construction(storage, state_key, version, fingerprint, bundle)
    else:
        seal_first_construction(storage, state_key, version, fingerprint, bundle)
        validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    staged_integrity = verify_staged_filegdb(storage, prefix, objects_list)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": EXPECTED_COUNT, "source_id_sha256": ids_sha,
        "source_archive_sha256": archive_hash, "source_archive_bytes": archive_descriptor["bytes"],
        "candidate_ready": True, "native_filegdb_preserved": True, "reused": replayed,
        "staged_filegdb_integrity": staged_integrity, "source_null_geometry_count": null_geometry_count,
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "historical_published_ids_reconciled": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False}
