"""Lossless candidate extraction for EPA's live FRS/RMP facility view.

The view is an EPA_GEO hosted distribution whose metadata identifies EPA FRS
and the RMP national program. It is kept separate from the historical
DataLumos export. Candidate creation is local-only and never grants release.
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
import pyproj
import shapely
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    capture_get, content_derived_version, processing_identity, quarantine_incomplete_capture,
    verify_cached_candidate, write_immutable,
)

DATASET_SLUG = "epa-emergency-response-rmp-facilities"
FILE_SLUG = DATASET_SLUG
ITEM_ID = "370365ee65ef46bc8f1b010aad931294"
ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"
SERVICE_URL = "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_INTERESTS_RMP/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
EPA_FRS_URL = "https://www.epa.gov/frs"
EPA_FRS_DATA_SOURCES_URL = "https://www.epa.gov/frs/frs-data-sources"
EPA_FRS_DOWNLOAD_OPTIONS_URL = "https://www.epa.gov/frs/frs-data-download-options"
EPA_FRS_GEOSPATIAL_DOWNLOAD_URL = "https://www.epa.gov/frs/geospatial-data-download-service"
LAYER_NAME = "RMP"
SOURCE_FIELD_COUNT = 32
SOURCE_COUNT = 21853
SOURCE_SCHEMA_SHA256 = "f0f50c2204dfd72bcc5df3b2faae12f45b9b9c0f02c059078a1bfc2d2054184c"
PAGE_SIZE = 1000
MAX_BODY_BYTES = 16 * 1024 * 1024
_DEFAULT_SCHEMA_PIN = object()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _field_profile(fields: object) -> list[dict[str, object]]:
    if not isinstance(fields, list) or not fields or any(not isinstance(f, dict) for f in fields):
        raise ValueError("source_field_roster_missing")
    return [{key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}
            for field in fields]


def _stable_item(item: Mapping[str, object]) -> dict[str, object]:
    """ArcGIS increments numViews when its item endpoint is retrieved."""
    return {key: value for key, value in item.items() if key != "numViews"}


def _query_url(params: Mapping[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _source_urls(ids: list[int]) -> list[tuple[str, str]]:
    urls: list[tuple[str, str]] = [
        ("epa_frs_page", EPA_FRS_URL),
        ("epa_frs_sources_page", EPA_FRS_DATA_SOURCES_URL),
        ("epa_frs_download_options_page", EPA_FRS_DOWNLOAD_OPTIONS_URL),
        ("epa_frs_geospatial_download_page", EPA_FRS_GEOSPATIAL_DOWNLOAD_URL),
        ("portal_item_before", ITEM_URL),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query_url({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query_url({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]
    for offset in range(0, len(ids), PAGE_SIZE):
        urls.append((f"features_{offset // PAGE_SIZE:03d}", _query_url({
            "where": "1=1", "outFields": "*", "returnGeometry": "true", "returnZ": "true",
            "returnM": "true", "outSR": "4269", "orderByFields": "OBJECTID ASC",
            "resultOffset": offset, "resultRecordCount": PAGE_SIZE, "f": "json",
        })))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", _query_url({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query_url({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
        ("portal_item_after", ITEM_URL),
    ])
    return urls


def _plain_object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("source_json_object_missing:" + name)
    return value


def validate_source_captures(captures: Mapping[str, tuple[bytes, Mapping[str, object]]], *,
                             expected_count: int | None = None,
                             expected_field_count: int | None = None,
                             expected_schema_sha256: str | None | object = _DEFAULT_SCHEMA_PIN,
                             page_size: int = PAGE_SIZE) -> dict[str, object]:
    """Validate a complete pair of source snapshots and ordered feature pages."""
    expected_count = SOURCE_COUNT if expected_count is None else expected_count
    expected_field_count = SOURCE_FIELD_COUNT if expected_field_count is None else expected_field_count
    if expected_schema_sha256 is _DEFAULT_SCHEMA_PIN:
        expected_schema_sha256 = SOURCE_SCHEMA_SHA256
    docs = {name: json.loads(body) for name, (body, _) in captures.items()
            if name not in {"epa_frs_page", "epa_frs_sources_page", "epa_frs_download_options_page",
                            "epa_frs_geospatial_download_page"}}
    item0 = _plain_object(docs.get("portal_item_before"), "portal_item_before")
    item1 = _plain_object(docs.get("portal_item_after"), "portal_item_after")
    service0 = _plain_object(docs.get("service_before"), "service_before")
    service1 = _plain_object(docs.get("service_after"), "service_after")
    layer0 = _plain_object(docs.get("layer_before"), "layer_before")
    layer1 = _plain_object(docs.get("layer_after"), "layer_after")

    expected_pages = (expected_count + page_size - 1) // page_size
    expected_names = {
        "epa_frs_page", "epa_frs_sources_page", "epa_frs_download_options_page",
        "epa_frs_geospatial_download_page", "portal_item_before", "service_before",
        "layer_before", "count_before", "ids_before", "ids_seed", "portal_item_after", "service_after",
        "layer_after", "count_after", "ids_after", *(f"features_{i:03d}" for i in range(expected_pages)),
    }
    if set(captures) != expected_names:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    if _stable_item(item0) != _stable_item(item1):
        raise ValueError("official_item_metadata_changed_during_capture")
    if item0.get("id") != ITEM_ID or item0.get("owner") != "EPA_GEO" or item0.get("access") != "public":
        raise ValueError("official_EPA_portal_publication_drift")
    if item0.get("url") != SERVICE_URL or item0.get("type") != "Feature Service":
        raise ValueError("official_EPA_service_identity_drift")
    item_text = " ".join(str(item0.get(key, "")) for key in ("description", "accessInformation"))
    if ("Risk Management Plan (RMP)" not in item_text or "FRS Support" not in item_text
            or "17 U.S.C." not in str(item0.get("licenseInfo", ""))
            or "public domain" not in str(item0.get("licenseInfo", "")).casefold()):
        raise ValueError("official_item_lineage_or_license_evidence_drift")
    sources_page = captures["epa_frs_sources_page"][0]
    if b"Risk Management Plan (RMP)" not in sources_page or b"112(r)" not in sources_page:
        raise ValueError("official_EPA_RMP_program_relationship_evidence_drift")
    if service0 != service1 or layer0 != layer1:
        raise ValueError("official_source_metadata_changed_during_capture")
    if service0.get("serviceItemId") != ITEM_ID or not any(
        isinstance(row, dict) and row.get("id") == LAYER_ID and row.get("name") == LAYER_NAME
        for row in service0.get("layers", [])
    ):
        raise ValueError("official_service_layer_identity_drift")
    if (layer0.get("name") != LAYER_NAME or layer0.get("geometryType") != "esriGeometryPoint"
            or layer0.get("objectIdField") != "OBJECTID" or layer0.get("maxRecordCount", 0) < PAGE_SIZE
            or layer0.get("spatialReference", {}).get("latestWkid") != 4269
            or layer0.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True):
        raise ValueError("official_layer_geometry_crs_or_id_drift")
    fields = _field_profile(layer0.get("fields"))
    schema_sha = _sha(_canonical(fields))
    if len(fields) != expected_field_count or (expected_schema_sha256 is not None and schema_sha != expected_schema_sha256):
        raise ValueError("source_field_schema_drift")
    for fname in ("count_before", "count_after"):
        if _plain_object(docs.get(fname), fname).get("count") != expected_count:
            raise ValueError("source_count_drift:" + fname)
    ids0 = _plain_object(docs.get("ids_before"), "ids_before").get("objectIds")
    ids1 = _plain_object(docs.get("ids_after"), "ids_after").get("objectIds")
    ids_seed = _plain_object(docs.get("ids_seed"), "ids_seed").get("objectIds")
    if not all(isinstance(x, list) for x in (ids0, ids1, ids_seed)):
        raise ValueError("source_ids_missing")
    if ids0 != ids1 or len(ids0) != expected_count or sorted(ids0) != sorted(ids_seed) or len(set(ids0)) != expected_count:
        raise ValueError("source_id_population_drift_during_capture")
    ordered_ids = sorted(ids0)
    expected_fields = [field["name"] for field in fields]
    features: list[dict[str, object]] = []
    for page_index in range(expected_pages):
        page = _plain_object(docs.get(f"features_{page_index:03d}"), f"features_{page_index:03d}")
        expected_page_ids = ordered_ids[page_index * page_size:(page_index + 1) * page_size]
        transfer_limit = page.get("exceededTransferLimit") is True
        if "error" in page or (transfer_limit and (page_index == expected_pages - 1 or len(expected_page_ids) != page_size)):
            raise ValueError("source_page_error_or_truncation")
        if page.get("spatialReference", {}).get("latestWkid") != 4269:
            raise ValueError("source_page_crs_drift")
        rows = page.get("features")
        if not isinstance(rows, list) or [row.get("attributes", {}).get("OBJECTID") for row in rows] != expected_page_ids:
            raise ValueError("source_page_population_or_order_incomplete")
        if len(rows) < page_size and transfer_limit:
            raise ValueError("final_source_page_exceeded_transfer_limit")
        features.extend(rows)
    if len(features) != expected_count:
        raise ValueError("source_feature_population_incomplete")
    registry_ids = [row["attributes"].get("REGISTRY_ID") for row in features]
    unique_registry_id_count = len(set(registry_ids))
    object_ids = [row["attributes"].get("OBJECTID") for row in features]
    unique_object_id_count = len(set(object_ids))
    minx = miny = float("inf"); maxx = maxy = float("-inf")
    layer_extent = layer0.get("extent", {})
    try:
        extent_bounds = (layer_extent["xmin"], layer_extent["ymin"], layer_extent["xmax"], layer_extent["ymax"])
    except (KeyError, TypeError):
        raise ValueError("source_layer_extent_missing")
    geometries: list[list[object]] = []
    null_geometry_count = 0
    invalid_geometry_count = 0
    for feature in features:
        attrs = feature.get("attributes"); geometry = feature.get("geometry")
        if not isinstance(attrs, dict) or list(attrs) != expected_fields:
            raise ValueError("source_record_field_order_or_roster_drift")
        if geometry is None:
            null_geometry_count += 1
            geometries.append([attrs["OBJECTID"], None])
            continue
        if not isinstance(geometry, dict) or set(geometry) != {"x", "y"}:
            invalid_geometry_count += 1
            geometries.append([attrs["OBJECTID"], geometry])
            continue
        x, y = geometry["x"], geometry["y"]
        if (not isinstance(x, (int, float)) or isinstance(x, bool) or not isinstance(y, (int, float))
                or isinstance(y, bool)):
            invalid_geometry_count += 1
            geometries.append([attrs["OBJECTID"], geometry])
            continue
        if not (-180 <= x <= 180 and -90 <= y <= 90):
            invalid_geometry_count += 1
            geometries.append([attrs["OBJECTID"], geometry])
            continue
        xmin, ymin, xmax, ymax = extent_bounds
        if x < xmin - 1e-6 or x > xmax + 1e-6 or y < ymin - 1e-6 or y > ymax + 1e-6:
            invalid_geometry_count += 1
            geometries.append([attrs["OBJECTID"], geometry])
            continue
        minx = min(minx, x); maxx = max(maxx, x); miny = min(miny, y); maxy = max(maxy, y)
        geometries.append([attrs["OBJECTID"], x, y])
    values_sha = _sha(b"".join(captures[f"features_{i:03d}"][0] for i in range(expected_pages)))
    geometry_fingerprint_rows = []
    for row in geometries:
        try:
            _canonical(row)
            geometry_fingerprint_rows.append(row)
        except ValueError:
            geometry_fingerprint_rows.append([row[0], json.dumps(row[1], sort_keys=True, separators=(",", ":"), allow_nan=True)])
    geometry_sha = _sha(_canonical(geometry_fingerprint_rows))
    count_before = _plain_object(docs["count_before"], "count_before")
    return {
        "features": features, "fields": fields,
        "audit": {
            "source_feature_count": len(features), "source_id_count": len(ordered_ids),
            "unique_registry_id_count": unique_registry_id_count,
            "null_registry_id_count": sum(value is None for value in registry_ids),
            "duplicate_registry_id_count": len(registry_ids) - unique_registry_id_count,
            "unique_object_id_count": unique_object_id_count,
            "source_id_sha256": _sha(_canonical(ordered_ids)), "source_record_sha256": values_sha,
            "source_registry_id_sha256": _sha(_canonical(sorted(registry_ids))),
            "source_geometry_sha256": geometry_sha, "source_schema_sha256": schema_sha,
            "source_crs_epsg": 4269, "geometry_type": "Point",
            "invalid_geometry_count": invalid_geometry_count,
            "observed_geometry_bbox": [minx, miny, maxx, maxy] if minx != float("inf") else None,
            "null_geometry_count": null_geometry_count,
            "service_extent": layer_extent, "arcgis_item_id": ITEM_ID, "portal_owner": item0["owner"],
            "portal_numviews_before": item0.get("numViews"), "portal_numviews_after": item1.get("numViews"),
            "portal_numviews_is_volatile": True, "portal_modified_epoch_ms": item0.get("modified"),
            "portal_modified_is_source_release_date": False,
            "data_last_edit_epoch_ms": layer0.get("editingInfo", {}).get("dataLastEditDate"),
            "schema_last_edit_epoch_ms": layer0.get("editingInfo", {}).get("schemaLastEditDate"),
            "source_max_record_count": layer0.get("maxRecordCount"), "first_page_count": len(docs["features_000"]["features"]),
            "count_snapshot_before": count_before.get("count"),
        },
    }


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {
        "esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
        "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeDouble": pa.float64(),
        "esriFieldTypeSingle": pa.float32(), "esriFieldTypeString": pa.string(),
        # Esri JSON exposes date values as epoch-millisecond integers. Retain
        # that native value until the date contract has explicit approval.
        "esriFieldTypeDate": pa.int64(),
    }.get(str(field["type"])) or _unsupported_type(field["type"])


def _unsupported_type(kind: object):
    raise ValueError("unsupported_source_physical_type:" + str(kind))


def _parquet_bytes(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    schema = pa.schema([pa.field(field["name"], _arrow_type(field), nullable=field.get("nullable", True))
                        for field in fields] + [pa.field("geometry", pa.binary(), nullable=True)])
    columns = {field["name"]: pa.array([row["attributes"][field["name"]] for row in features],
                                       type=_arrow_type(field)) for field in fields}
    columns["geometry"] = pa.array([
        Point(row["geometry"]["x"], row["geometry"]["y"]).wkb
        if isinstance(row.get("geometry"), dict)
        and isinstance(row["geometry"].get("x"), (int, float))
        and not isinstance(row["geometry"].get("x"), bool)
        and isinstance(row["geometry"].get("y"), (int, float))
        and not isinstance(row["geometry"].get("y"), bool)
        and -180 <= row["geometry"]["x"] <= 180 and -90 <= row["geometry"]["y"] <= 90
        else None for row in features], type=pa.binary())
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Point"], "crs": pyproj.CRS.from_epsg(4269).to_json_dict(),
        "edges": "planar",
    }}}
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(columns, schema=schema.with_metadata({b"geo": _canonical(geo)})),
                   sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _identity(repo_root: Path) -> dict[str, object]:
    identity = processing_identity(repo_root)
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def capture_epa_rmp_facilities(storage: StagingStorageResource, *, version: str,
                               capture_directory: str | None = None) -> dict[str, object]:
    """Acquire the entire current official feed and stage one sealed local candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for char in version):
        raise ValueError("invalid_version")
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 256_000_000:
        raise ValueError("disk_floor_before_acquisition")
    attempt = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/66/captures")
    capture_root = (attempt / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(path.is_symlink() for path in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=60,
                          headers={"User-Agent": "hifld-next-datasets/epa-rmp-facilities-draft"}) as client:
            seed_body, seed_receipt = capture_get(client,
                _query_url({"where": "1=1", "returnIdsOnly": "true", "f": "json"}), capture_root,
                "ids_seed", max_bytes=MAX_BODY_BYTES)
            seed = json.loads(seed_body).get("objectIds")
            if not isinstance(seed, list) or len(seed) != SOURCE_COUNT:
                raise ValueError("source_seed_id_count_drift")
            captures["ids_seed"] = (seed_body, seed_receipt)
            for name, url in _source_urls(sorted(seed)):
                body, receipt = capture_get(client, url, capture_root, name, max_bytes=MAX_BODY_BYTES)
                captures[name] = (body, receipt)
        extracted = validate_source_captures(captures)
    except Exception as exc:
        receipts = {}
        failed_captures: dict[str, tuple[bytes, dict[str, object]]] = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try:
                name = path.name.removesuffix(".receipt.json")
                receipt = json.loads(path.read_bytes())
                receipts[name] = receipt
                body_path, wire_path = capture_root / f"{name}.body", capture_root / f"{name}.wire"
                failed_captures[name] = (body_path.read_bytes() if body_path.exists() else b"", receipt)
            except (ValueError, OSError):
                receipts[path.name] = {"receipt_readback_failed": True}
        (capture_root / "capture_manifest.json").write_bytes(_canonical({
            "captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
            "error_code": type(exc).__name__, "receipts": receipts,
        }) + b"\n")
        if failed_captures:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, failed_captures,
                                          type(exc).__name__)
        raise

    root = Path(__file__).parents[3]
    identity = _identity(root)
    parquet = _parquet_bytes(extracted["fields"], extracted["features"])
    audit = extracted["audit"]
    version_id = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                         audit["source_schema_sha256"], identity)
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version_id}"
    objects = {f"geoparquet/{DATASET_SLUG}.parquet": {"bytes": len(parquet), "sha256": _sha(parquet)}}
    receipts = {name: receipt for name, (_, receipt) in sorted(captures.items())}
    inputs = [{"input_id": name, "producer": "U.S. Environmental Protection Agency, FRS Support Team; RMP national program",
               "host": urlsplit(str(receipt["url"])).hostname,
               "role": "producer_publication",
               "canonical_url": EPA_FRS_DATA_SOURCES_URL if name == "epa_frs_sources_page" else EPA_FRS_URL if name == "epa_frs_page" else EPA_FRS_DOWNLOAD_OPTIONS_URL if name == "epa_frs_download_options_page" else EPA_FRS_GEOSPATIAL_DOWNLOAD_URL if name == "epa_frs_geospatial_download_page" else LAYER_URL,
               "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
               "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
               "bytes": receipt["bytes"], "sha256": receipt["sha256"],
               "license": "EPA item cites U.S. Government public-domain status under 17 U.S.C. § 105; formal terms review pending.",
               "attribution": "U.S. EPA Facility Registry Service (FRS), Risk Management Plan (RMP) national program; hosted by EPA_GEO.",
               "release_date": None, "release_date_unknown_reason": "EPA provides edit timestamps but this service metadata does not identify a source release date; timestamp is retained as actual data/schema editing evidence."}
              for name, receipt in sorted(receipts.items())]
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "EPA Emergency Response (ER) Risk Management Plan (RMP) Facilities",
        "description": "Live EPA FRS subset linked to the Risk Management Plan system. EPA's current hosted FRS/RMP view is used as official producer distribution; DataLumos 239122/V1 is historical only.",
        "publisher": "U.S. Environmental Protection Agency, Office of Environmental Information / EPA FRS Support (EPA_GEO hosted service)",
        "agency": "Environmental Protection Agency", "source_url": LAYER_URL,
        "license": "EPA ArcGIS item states U.S. EPA data are generally public domain under 17 U.S.C. §105; see https://www.usa.gov/publicdomain/label/1.0/. Rights review remains pending.",
        "source_modified": datetime.fromtimestamp(audit["data_last_edit_epoch_ms"] / 1000, timezone.utc).isoformat()
        if isinstance(audit["data_last_edit_epoch_ms"], (int, float)) else None,
        "date_issued": None, "date_modified": None, "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": inputs,
        "transformations": [{"operation": "native_esri_feature_json_to_geoparquet_candidate", "parameters": {
            "source_item": ITEM_ID, "layer": LAYER_ID, "fields": "all 32 fields in producer order with declared physical types",
            "ids": "preserve OBJECTID and REGISTRY_ID unchanged; OBJECTID is refresh-volatile per EPA metadata",
            "dates": "retain Esri epoch-millisecond values as int64 pending approved date conversion contract",
            "geometry": "preserve source x/y as WKB Point in EPSG:4269; no reprojection or repair",
            "status": "unapproved candidate; source/schema/ID/rights/compatibility release decisions pending"}}],
        "lineage": "EPA's FRS Support team publishes this view from the RMP national program integrated into FRS. The exact public item says the facilities are the subset linked to the RMP System. See EPA FRS and Data Sources pages captured with the source request. DataLumos 239122/V1 is retained only as a historical HIFLD archive and does not drive refresh.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Native Esri fields, source IDs, raw millisecond date values, and point geometry retained without contract-dependent conversion.",
            "objects": [{"path": f"geoparquet/{DATASET_SLUG}.parquet", **objects[f"geoparquet/{DATASET_SLUG}.parquet"]}]}}},
        "unsupported_output_formats": {"geopackage": "Source/public column mapping and date contract have not been approved.",
                                         "pmtiles": "No approved public display mapping or simplification policy."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit.update({"source_fields": extracted["fields"], "official_layer_name": LAYER_NAME,
                  "historical_archive": "DataLumos 239122/V1", "source_release_date": None,
                  "source_release_date_unknown_reason": "No source release date is present in the live service metadata.",
                  "license_status": "source says EPA public domain under 17 U.S.C. §105; human rights review pending",
                  "source_release_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
                  "freshness_accepted": False, "rights_accepted": False, "mapping_approved": False,
                  "release_accepted": False})
    fingerprint = _sha(_canonical(identity))
    state_key = f"_construction-state/{DATASET_SLUG}/{version_id}.json"
    if storage.object_snapshot(state_key) is not None:
        prior = json.loads(load_first_construction(storage, state_key, version_id, fingerprint))
        payload = prior.get("payload", {})
        if not isinstance(payload, dict) or payload.get("processing_identity") != identity:
            raise ValueError("cached_candidate_source_or_code_changed")
        verify_cached_candidate(storage, root, state_key, prefix, version_id, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"],
                                payload.get("objects", {}), payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version_id,
                "feature_count": len(extracted["features"]),
                "source_null_geometry_count": audit["null_geometry_count"],
                "source_invalid_geometry_count": audit["invalid_geometry_count"],
                "unique_registry_id_count": audit["unique_registry_id_count"],
                "null_registry_id_count": audit["null_registry_id_count"],
                "duplicate_registry_id_count": audit["duplicate_registry_id_count"],
                "unique_object_id_count": audit["unique_object_id_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json",
                "reused": True, "release_accepted": False}
    outputs: dict[str, bytes] = {
        f"geoparquet/{DATASET_SLUG}.parquet": parquet,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical({"approved": False, "version": version_id, "objects": objects,
            "source_record_sha256": audit["source_record_sha256"], "source_release_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False, "freshness_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False}) + b"\n",
    }
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(dict(receipt)) + b"\n"
    capture_manifest = {"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                        "source_feature_count": len(extracted["features"]),
                        "source_record_sha256": audit["source_record_sha256"],
                        "source_geometry_sha256": audit["source_geometry_sha256"], "receipts": receipts}
    (capture_root / "capture_manifest.json").write_bytes(_canonical(capture_manifest) + b"\n")
    for name, body in outputs.items():
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        if snapshot is None or snapshot.size != len(body) or storage.sha256_key(key) != _sha(body) or storage.object_snapshot(key) != snapshot:
            raise ValueError("candidate_physical_readback_failed:" + name)
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    payload = {"processing_identity": identity, "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"], "capture_receipts": receipts,
               "objects": descriptors}
    seal_first_construction(storage, f"_construction-state/{DATASET_SLUG}/{version_id}.json", version_id,
                            fingerprint, canonical_construction_bundle(version_id, fingerprint, payload))
    validate_construction_code_identity(root, identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version_id,
            "feature_count": len(extracted["features"]), "source_null_geometry_count": audit["null_geometry_count"],
            "source_invalid_geometry_count": audit["invalid_geometry_count"],
            "unique_registry_id_count": audit["unique_registry_id_count"],
            "null_registry_id_count": audit["null_registry_id_count"],
            "duplicate_registry_id_count": audit["duplicate_registry_id_count"],
            "unique_object_id_count": audit["unique_object_id_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "capture_directory": str(capture_root),
            "source_release_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
            "freshness_accepted": False, "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
