"""Native, bounded capture for the official USACE IENC OBSTRUCTION_AREA layer.

Release and historical compatibility decisions remain pending. ArcGIS JSON
responses are retained verbatim, while this candidate keeps every declared
source attribute and every source identifier unchanged.
"""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import shapely
from shapely import from_wkb

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.usace_navigation_marks_areas import (
    MAX_CAPTURE_BYTES, _canonical, _esri_polygon, _fetch_retry, _load_replay,
    _parquet_bytes, _sha, _write_outputs,
)

DATASET_SLUG = "obstruction-areas-usace-ienc-2"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/OBSTRUCTION_AREA/MapServer"
LAYER_URL = SERVICE_URL + "/81"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; distributed through USDOT/BTS NTAD"
OFFICIAL_ITEM_ID = "3607d485a9854d6fa458bf507058ceae"
OFFICIAL_ITEM_URL = "https://www.arcgis.com/sharing/rest/content/items/" + OFFICIAL_ITEM_ID + "?f=json"
PAGE_SIZE = 250
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID"), ("SHAPE", "esriFieldTypeGeometry"),
    ("Category_of_Obstruction", "esriFieldTypeString"),
    ("Value_of_Sounding", "esriFieldTypeString"),
    ("Water_Level", "esriFieldTypeString"),
    ("Natural_Surface", "esriFieldTypeString"),
    ("Object_Name", "esriFieldTypeString"), ("Information", "esriFieldTypeString"),
    ("Scale_Minimum", "esriFieldTypeInteger"), ("Source_Date", "esriFieldTypeString"),
    ("Source_Indication", "esriFieldTypeString"), ("Source_Dataset", "esriFieldTypeString"),
    ("Nature_of_Construction", "esriFieldTypeString"),
    ("Exposition_of_Sounding", "esriFieldTypeString"),
    ("Feature_ID", "esriFieldTypeString"),
]
EXPECTED_FIELD_DECLARATIONS = [
    {"name": name, "type": kind, "alias": name, "domain": None,
     **({"length": 254} if kind == "esriFieldTypeString" else {})}
    for name, kind in EXPECTED_FIELDS
]


def _query(params: dict[str, object], *, layer_url: str = LAYER_URL) -> str:
    return layer_url + "/query?" + urlencode(params)


def _capture_roster(ids: list[int]) -> list[tuple[str, str]]:
    urls = [("official_item", OFFICIAL_ITEM_URL),
            ("service_before", SERVICE_URL + "?f=pjson"),
            ("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"}))]
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        page_ids = ids[start:start + PAGE_SIZE]
        urls.append((f"page_{index:03d}", _query({
            "objectIds": ",".join(map(str, page_ids)), "outFields": "*",
            "returnGeometry": "true", "returnZ": "true", "returnM": "true",
            "outSR": "4326", "orderByFields": "OBJECTID ASC", "f": "json",
        })))
    urls.extend([("layer_after", LAYER_URL + "?f=pjson"),
                 ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                 ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                 ("service_after", SERVICE_URL + "?f=pjson")])
    return urls


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]]) -> str:
    """Bind versions to immutable decoded and wire bytes, never retrieval time."""
    digests = {}
    for name, (body, receipt) in sorted(captures.items()):
        if (receipt.get("complete_body") is not True or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or type(receipt.get("wire_bytes")) is not int
                or not isinstance(receipt.get("wire_sha256"), str)):
            raise ValueError("source_capture_receipt_invalid:" + name)
        digests[name] = {"body_sha256": receipt["sha256"],
                         "wire_sha256": receipt["wire_sha256"]}
    return _sha(_canonical(digests))


def _validate(captures: dict[str, tuple[bytes, dict]]):
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    item = docs["official_item"]
    if (item.get("id") != OFFICIAL_ITEM_ID or item.get("owner") != "USDOT_BTS"
            or item.get("title") != "Inland Electronic Navigational Charts"
            or "bi-monthly" not in item.get("description", "")
            or "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes" not in item.get("description", "")
            or "USACE_IENC_Master_Service_gdb.zip" not in item.get("description", "")):
        raise ValueError("official_bts_lineage_metadata_changed")
    service = docs["service_before"]
    advertised = service.get("layers")
    if not isinstance(advertised, list) or [(x.get("id"), x.get("name")) for x in advertised if isinstance(x, dict)] != [(81, "OBSTRUCTION_AREA")]:
        raise ValueError("source_service_layer_roster_drift")
    layer = docs["layer_before"]
    fields = layer.get("fields")
    projected = [{key: field[key] for key in ("name", "type", "alias", "length", "domain", "nullable") if key in field}
                 for field in fields] if isinstance(fields, list) else None
    if projected != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("name") != "OBSTRUCTION_AREA" or layer.get("id") != 81
            or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("maxRecordCount") != 2000
            or layer.get("spatialReference", {}).get("latestWkid") != 4326):
        raise ValueError("source_layer_identity_or_spatial_reference_drift")
    if docs["service_after"].get("layers") != service.get("layers"):
        raise ValueError("source_service_changed_during_capture")
    if docs["layer_after"].get("fields") != fields or docs["layer_after"].get("geometryType") != layer["geometryType"]:
        raise ValueError("source_schema_changed_during_capture")
    if (docs["layer_after"].get("extent") != layer.get("extent")
            or docs["layer_after"].get("spatialReference") != layer.get("spatialReference")
            or docs["layer_after"].get("maxRecordCount") != layer.get("maxRecordCount")):
        raise ValueError("source_coverage_or_transfer_limit_drift")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    before, after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (type(count_before) is not int or count_before < 0 or count_before != count_after
            or not isinstance(before, list) or before != after or len(before) != count_before
            or len({json.dumps(value, separators=(",", ":")) for value in before}) != count_before):
        raise ValueError("source_count_or_id_population_drift")
    page_names = sorted(name for name in docs if name.startswith("page_"))
    if len(page_names) != (count_before + PAGE_SIZE - 1) // PAGE_SIZE:
        raise ValueError("source_page_roster_incomplete")
    features = []
    for name in page_names:
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncation")
        if page.get("spatialReference", {}).get("latestWkid") != 4326:
            raise ValueError("source_page_spatial_reference_drift")
        rows = page.get("features")
        if not isinstance(rows, list):
            raise ValueError("source_page_features_missing")
        features.extend(rows)
    expected_attrs = [field["name"] for field in fields if field["type"] != "esriFieldTypeGeometry"]
    actual_ids = []
    for feature in features:
        attrs, geom = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or list(attrs) != expected_attrs:
            raise ValueError("source_record_attribute_schema_drift")
        if not isinstance(geom, dict) or not isinstance(geom.get("rings"), list):
            raise ValueError("source_record_geometry_missing")
        if set(geom) - {"rings", "hasZ", "hasM", "spatialReference"}:
            raise ValueError("unexpected_source_geometry_ordinates")
        actual_ids.append(attrs.get("OBJECTID"))
    id_keys = [json.dumps(value, separators=(",", ":")) for value in actual_ids]
    roster_keys = [json.dumps(value, separators=(",", ":")) for value in before]
    if len(features) != count_before or sorted(id_keys) != sorted(roster_keys) or len(set(id_keys)) != count_before:
        raise ValueError("source_feature_population_incomplete")
    features.sort(key=lambda f: f["attributes"]["OBJECTID"])
    record_hash = _sha(_canonical([feature["attributes"] for feature in features]))
    geometry_hash = _sha(_canonical([[f["attributes"]["OBJECTID"], f["geometry"]] for f in features]))
    geometries = [from_wkb(_esri_polygon(feature["geometry"])) for feature in features]
    schema_hash = _sha(_canonical(fields))
    ids_sorted = sorted(actual_ids)
    extent = layer.get("extent", {})
    audit = {
        "source_record_sha256": record_hash, "source_geometry_sha256": geometry_hash,
        "source_schema_sha256": schema_hash, "source_id_sha256": _sha(_canonical(ids_sorted)),
        "source_feature_count": count_before, "source_id_min": min(ids_sorted) if ids_sorted else None,
        "source_id_max": max(ids_sorted) if ids_sorted else None,
        "source_geometry_type": layer["geometryType"], "source_wkid": 4326,
        "source_geometry_z_counts": {str(dim): sum(len(point) == dim for feature in features
            for ring in feature["geometry"]["rings"] for point in ring) for dim in (2, 3, 4)},
        "geometry_valid_count": sum(geom.is_valid for geom in geometries),
        "geometry_invalid_count": sum(not geom.is_valid for geom in geometries),
        "geometry_empty_count": sum(geom.is_empty for geom in geometries),
        "geometry_bounds": [min(g.bounds[0] for g in geometries), min(g.bounds[1] for g in geometries),
                            max(g.bounds[2] for g in geometries), max(g.bounds[3] for g in geometries)] if geometries else None,
        "extent_declared": [extent.get("xmin"), extent.get("ymin"), extent.get("xmax"), extent.get("ymax")],
        "source_coverage_scope": "Official USACE IENC obstruction polygons within inland electronic chart coverage; BTS describes IENC coverage as 7,260 miles across 22 states. This is not nationwide land coverage.",
        "drift_guard": "Exact service layer roster and layer ID/name, field declarations/types, CRS, count, before/after ID population, exact feature IDs, geometry presence and transfer-limit errors checked.",
    }
    return fields, features, audit


def capture_usace_obstruction_areas(storage: StagingStorageResource, *, version: str,
                                    capture_directory: str | None = None) -> dict[str, object]:
    """Capture and catalog the complete official polygon layer or replay a sealed capture."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(p.is_symlink() for p in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    root = requested if replay else (requested / (version + "-" + uuid4().hex) if requested
           else Path("/tmp/hifld-native-reconstruction/252/captures") / (version + "-" + uuid4().hex))
    if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict]] = {}
    try:
        if replay:
            retained = json.loads((root / "capture_manifest.json").read_bytes())
            roster = retained.get("artifact_roster")
            if retained.get("complete") is not True or not isinstance(roster, dict):
                raise ValueError("retained_capture_manifest_invalid")
            validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
            if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
                raise ValueError("retained_capture_artifact_digest_mismatch")
            ids_body, _receipt = _load_replay(root, "ids_before")
            ids = json.loads(ids_body).get("objectIds")
            if not isinstance(ids, list):
                raise ValueError("retained_id_seed_invalid")
            names = ["official_item", "service_before", "layer_before", "count_before", "ids_before",
                     *[f"page_{i:03d}" for i in range((len(ids) + PAGE_SIZE - 1) // PAGE_SIZE)],
                     "layer_after", "count_after", "ids_after", "service_after"]
            if set(retained.get("receipts", {})) != set(names):
                raise ValueError("retained_capture_roster_changed")
            captures = {name: _load_replay(root, name) for name in names}
            if any(receipt != retained["receipts"][name] for name, (_body, receipt) in captures.items()):
                raise ValueError("retained_capture_receipt_changed")
        else:
            existing_bytes = sum(p.stat().st_size for p in (requested or root.parent).rglob("*")
                                 if p.is_file() and p.suffix in {".body", ".wire"})
            budget = [existing_bytes]
            if existing_bytes > MAX_CAPTURE_BYTES:
                raise ValueError("source_capture_budget_exceeded_before_request")
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/usace-ienc-draft"}) as client:
                ids_body, ids_receipt = _fetch_retry(client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}), root, "ids_before", budget)
                ids = json.loads(ids_body).get("objectIds")
                if not isinstance(ids, list):
                    raise ValueError("source_id_population_missing")
                captures["ids_before"] = (ids_body, ids_receipt)
                for name, url in _capture_roster(ids):
                    captures[name] = _fetch_retry(client, url, root, name, budget)
        fields, features, audit = _validate(captures)
    except Exception as exc:
        receipts = {}
        for path in sorted(root.glob("*.receipt.json")):
            try:
                receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (OSError, ValueError):
                receipts[path.name] = {"receipt_readback_failed": True}
        (root / "capture_manifest.json").write_bytes(_canonical({
            "captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
            "error_code": type(exc).__name__, "receipts": receipts,
        }) + b"\n")
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
        raise

    identity = processing_identity(Path(__file__).parents[3])
    parquet = _parquet_bytes(fields, features, 4326)
    capture_hash = _capture_identity_hash(captures)
    audit["source_capture_sha256"] = capture_hash
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    receipts = {name: receipt for name, (_body, receipt) in captures.items()}
    inputs = [{"input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication", "canonical_url": LAYER_URL, "retrieval_url": receipt["url"],
        "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "license": "Unresolved; rights review pending.", "attribution": "USACE IENC; rights review pending.",
        "release_date": None, "release_date_unknown_reason": "BTS item describes a bi-monthly publication cadence but does not identify this layer's release date."}
        for name, receipt in sorted(receipts.items())]
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    manifest = {"schema_version": 1, "approved": False, "title": "Obstruction Areas (USACE IENC)",
        "description": "Complete official USACE IENC OBSTRUCTION_AREA feature class; unapproved faithful candidate.",
        "publisher": PUBLISHER, "agency": "US Army Corps of Engineers", "source_url": LAYER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"], "inputs": inputs,
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate", "parameters": {
            "source_fields": "all declared attributes retained in source order and source physical type",
            "source_ids": "OBJECTID and Feature_ID values retained unchanged",
            "geometry": "Polygon rings preserved in raw Esri JSON; source EPSG:4326; no reprojection or geometry repair",
            "candidate_status": "unapproved; source release, baseline, schema/ID, coverage and rights remain pending"}}],
        "lineage": f"USDOT/BTS IENC item {OFFICIAL_ITEM_ID} links the USACE IENC Feature Classes service and master geodatabase. This candidate contains the complete OBSTRUCTION_AREA layer only.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Faithful unapproved native-layer candidate.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No approved mapping or GDB cross-check.", "pmtiles": "No display transformation approved."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit.update({"source_fields": fields, "official_layer_name": "OBSTRUCTION_AREA", "official_layer_id": 81,
        "official_source_item": OFFICIAL_ITEM_ID, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False, "release_accepted": False})
    (root / "capture_manifest.json").write_bytes(_canonical({"captured_at": datetime.now(timezone.utc).isoformat(),
        "complete": True, "source_feature_count": audit["source_feature_count"],
        "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "receipts": receipts, "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
    outputs = {output_path: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n", "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version, "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True, "source_record_sha256": audit["source_record_sha256"],
        "source_release_accepted": False, "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    fingerprint = _sha(_canonical(identity))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("capture_receipts"), dict):
            raise ValueError("cached_candidate_original_receipts_missing")
        if set(payload["capture_receipts"]) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name, (body, _receipt) in captures.items():
            if (storage.read_key(f"{prefix}/raw/{name}.body") != body
                    or storage.read_key(f"{prefix}/raw/{name}.wire") != (root / f"{name}.wire").read_bytes()):
                raise ValueError("cached_source_bytes_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
            fingerprint, identity, audit["source_record_sha256"], audit["source_schema_sha256"],
            payload.get("objects", {}), payload["capture_receipts"])
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["source_feature_count"], "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"],
            "source_id_max": audit["source_id_max"], "source_schema_sha256": audit["source_schema_sha256"],
            "source_coverage_scope": audit["source_coverage_scope"], "drift_guard": audit["drift_guard"],
            "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_empty_count": audit["geometry_empty_count"], "geometry_bounds": audit["geometry_bounds"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(root)}
    descriptors = _write_outputs(storage, prefix, outputs)
    seal_ready_candidate(storage, state_key, version, fingerprint, {"processing_identity": identity,
        "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": receipts, "objects": descriptors})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["source_feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"],
        "source_id_max": audit["source_id_max"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_coverage_scope": audit["source_coverage_scope"], "drift_guard": audit["drift_guard"],
        "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_valid_count": audit["geometry_valid_count"],
        "geometry_empty_count": audit["geometry_empty_count"], "geometry_bounds": audit["geometry_bounds"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "capture_directory": str(root)}
