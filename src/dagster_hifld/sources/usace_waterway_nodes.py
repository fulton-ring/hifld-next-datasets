"""Native, unapproved FileGDB candidate for BTS NTAD waterway network nodes."""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from uuid import uuid4

import httpx

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.arcgis_filegdb import (
    canonical_json,
    capture_filegdb_replica,
    extract_filegdb,
    sha256,
    stage_filegdb_archive,
    verify_staged_filegdb,
)
from dagster_hifld.sources.epa_frs_common import write_immutable

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


def capture_usace_waterway_nodes(
    storage: StagingStorageResource, *, capture_directory: str | None = None,
) -> dict[str, object]:
    """Capture a complete native FileGDB draft; no publication contract is approved."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    capture_base = Path(capture_directory) if capture_directory else RUNTIME_ROOT / "captures"
    capture_base.mkdir(parents=True, exist_ok=True)
    capture_root = capture_base / uuid4().hex
    capture_root.mkdir(parents=True, exist_ok=False)
    scratch_root = RUNTIME_ROOT / "scratch"
    scratch_root.mkdir(parents=True, exist_ok=True)
    try:
        return _capture_usace_waterway_nodes(storage, capture_root, scratch_root)
    finally:
        shutil.rmtree(capture_root, ignore_errors=True)


def _capture_usace_waterway_nodes(
    storage: StagingStorageResource, capture_root: Path, scratch_root: Path,
) -> dict[str, object]:
    count_url = LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnCountOnly": "true", "f": "json"})
    ids_url = LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnIdsOnly": "true", "f": "json"})
    urls = {
        "bts_item": BTS_ITEM_URL, "usace_item": USACE_ITEM_URL,
        "service_before": SERVICE_URL + "?f=json", "layer_before": LAYER_URL + "?f=json",
        "count_before": count_url, "ids_before": ids_url,
    }
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

    archive_hash = str(archive_receipt["sha256"])
    version = "v" + hashlib.sha256(canonical_json({"archive": archive_hash, "schema": fields, "ids": source_ids})).hexdigest()[:32]
    with tempfile.TemporaryDirectory(prefix="hifld-179-", dir=scratch_root) as scratch:
        gdb_path, gdb_root, integrity = extract_filegdb(
            archive_path, Path(scratch), expected_layer="Waterway_Network_Node",
            expected_property_names=[name for name in EXPECTED_FIELDS if name != EXPECTED_ID_FIELD],
            expected_count=EXPECTED_COUNT, expected_ids=source_ids, expected_zm=False, binary_fields=(),
        )
        import fiona
        with fiona.open(gdb_path, layer="Waterway_Network_Node") as source:
            bounds = [math.inf, math.inf, -math.inf, -math.inf]
            geometry_count = 0
            null_geometry_count = 0
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
        if geometry_count + null_geometry_count != EXPECTED_COUNT or any(
            not math.isclose(actual, expected, rel_tol=0.0, abs_tol=1e-6)
            for actual, expected in zip(bounds, EXPECTED_EXTENT)
        ):
            raise ValueError("USACE_node_export_geometry_coverage_drift")
        prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
        objects = stage_filegdb_archive(storage, prefix, archive_path, gdb_path, gdb_root)
    receipts = {name: receipt for name, (_, receipt) in sorted(captured.items())}
    ids_sha = sha256(canonical_json(source_ids))
    schema_sha = sha256(canonical_json(fields))
    prior_audit_key = f"{prefix}/metadata/source_audit.json"
    if storage.object_snapshot(prior_audit_key) is not None:
        prior = json.loads(storage.read_key(prior_audit_key))
        expected_identity = {
            "source_count": EXPECTED_COUNT,
            "source_id_sha256": ids_sha,
            "source_schema_sha256": schema_sha,
            "source_archive_sha256": archive_hash,
        }
        if any(prior.get(key) != value for key, value in expected_identity.items()):
            raise ValueError("cached_USACE_node_candidate_identity_drift")
        staged_integrity = verify_staged_filegdb(storage, prefix, objects)
        return {
            "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_id_sha256": ids_sha,
            "source_archive_sha256": archive_hash, "source_archive_bytes": archive_receipt["bytes"],
            "candidate_ready": True, "native_filegdb_preserved": True, "reused": True,
            "staged_filegdb_integrity": staged_integrity,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "historical_published_ids_reconciled": False,
            "source_release_accepted": False, "freshness_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
            "source_null_geometry_count": integrity["null_geometry_count"],
        }
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Navigable Waterway Network Node Locations",
        "description": "BTS NTAD / USACE native point FileGDB source candidate; schema, ID, release vintage, rights, and any published mapping remain pending maintainer decision.",
        "publisher": "USDOT Bureau of Transportation Statistics (NTAD); official layer hosted by USACE item owner usace_iwr_cac",
        "agency": "U.S. Army Corps of Engineers",
        "producer_lineage": "BTS metadata says NWN is periodically updated by USACE, is part of BTS NTAD, and was created on behalf of BTS, USACE, Census, and USCG by Vanderbilt University and Oak Ridge National Laboratory.",
        "bts_distribution_item": BTS_ITEM_URL,
        "bts_data_dictionary": BTS_DATA_DICTIONARY_DOI,
        "hifld_crosswalk_archive": HIFLD_CROSSWALK_DOI,
        "source_url": LAYER_URL, "source_release_date": None,
        "source_release_date_note": "ArcGIS portal/service edit timestamps do not establish a named source release date.",
        "license": "Unknown; rights review pending.", "candidate": True,
        "source_fields": fields, "source_id_field": EXPECTED_ID_FIELD,
        "source_count": EXPECTED_COUNT, "source_CRS": f"EPSG:{EXPECTED_CRS}",
        "source_geometry_type": "Point", "source_hasZ": False, "source_hasM": False,
        "source_extent": list(EXPECTED_EXTENT), "exported_geometry_extent": bounds,
        "source_binary_fields": [], "source_archive_sha256": archive_hash,
        "source_archive_bytes": archive_receipt["bytes"],
        "source_null_geometry_count": null_geometry_count, "capture_receipts": receipts,
        "historical_archive": HISTORICAL_ARCHIVE_URL + " (recovery reference only; not a live refresh source)",
        "unsupported_formats": {
            "shapefile": "No approved mapping for complete source names/types and source ID behavior.",
            "geojson": "No approved mapping for all native types and IDs.",
            "geoparquet": "No accepted type/ID/geometry preservation mapping for this source.",
            "pmtiles": "No approved display generalization or loss policy exists for the analytic network nodes.",
        },
        "publication_gate": {
            "source_release_accepted": False, "freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "historical_published_ids_reconciled": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        },
    }
    audit = {
        "source_count": EXPECTED_COUNT,
        "source_id_sha256": ids_sha,
        "source_schema_sha256": schema_sha,
        "source_fields": fields, "source_CRS": f"EPSG:{EXPECTED_CRS}",
        "source_geometry_type": "Point", "source_hasZ": False, "source_hasM": False,
        "source_extent": list(EXPECTED_EXTENT), "exported_geometry_extent": bounds,
        "source_archive_sha256": archive_hash, "source_archive_bytes": archive_receipt["bytes"],
        "source_null_geometry_count": null_geometry_count,
        "filegdb_integrity": integrity, "staged_filegdb_integrity": verify_staged_filegdb(storage, prefix, objects),
        "source_release_date": None, **manifest["publication_gate"], "capture_receipts": receipts,
    }
    write_immutable(storage, f"{prefix}/metadata/source_manifest.json", canonical_json(manifest) + b"\n")
    write_immutable(storage, f"{prefix}/metadata/source_audit.json", canonical_json(audit) + b"\n")
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": EXPECTED_COUNT, "source_id_sha256": audit["source_id_sha256"],
        "source_archive_sha256": archive_hash, "source_archive_bytes": archive_receipt["bytes"],
        "candidate_ready": True, "native_filegdb_preserved": True,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "historical_published_ids_reconciled": False,
        "source_release_accepted": False, "freshness_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "source_null_geometry_count": null_geometry_count,
    }
