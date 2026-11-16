"""Bounded, source-faithful capture for the official USACE IENC wrecks points."""
from __future__ import annotations

import io
import json
import math
import shutil
import struct
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from shapely import from_wkb

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.capture_common import (
    candidate_prefix, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
    validate_capture_request_identity,
)
from dagster_hifld.sources.usace_obstruction_lines import (
    _canonical, _capture, _capture_identity, _load_capture, _sha,
    _validate_spatial_reference,
)

DATASET_SLUG = "wrecks-usace-ienc"
FILE_SLUG = DATASET_SLUG
# The USDOT/BTS NTAD IENC catalog record explicitly links this USACE chart-server
# Feature Classes service and master geodatabase. Its earlier item is no longer
# anonymously readable and is retained as prior evidence only; catalog succession
# and layer edition continuity remain unaccepted.
BTS_ITEM_ID = "7ec34864c8804468a9cf52d437fe4b6c"
PRIOR_BTS_ITEM_ID = "3607d485a9854d6fa458bf507058ceae"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
SERVICE_DIRECTORY_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson"
SERVICE_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/WRECKS_POINT/MapServer"
LAYER_ID = 31
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; official IENC Feature Classes service"
EXPECTED_COUNT = 560
EXPECTED_IDS = list(range(1, EXPECTED_COUNT + 1))
EXPECTED_ID_SHA256 = "edd5524dc0f61f6bbcd2c2730e4db6a1b259fd622be7cb42a6c6d91348f57e7d"
PAGE_MAX_BYTES = 8_000_000
CAPTURE_MAX_BYTES = 192_000_000
EXPECTED_EXTENT = {"xmin": -95.17360009999999, "ymin": 29.695133700000042,
                   "xmax": -79.76815199999999, "ymax": 44.899819000000036}
# Every XY ordinate must stay inside the EPSG:4326 geographic domain and the pinned
# layer extent within binary64 float-noise tolerance; drift outside either fails
# closed before any candidate write or seal.
EXTENT_TOLERANCE_DEGREES = 1e-9
EXPECTED_CAPTURE_NAMES = {"bts_item", "service_directory", "service_before", "layer_before",
                          "count_before", "ids_before", "features", "layer_after", "service_after",
                          "count_after", "ids_after"}
EXPECTED_FIELD_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    *[
        {"name": name, "type": "esriFieldTypeString", "alias": name, "length": 254, "domain": None}
        for name in ("Category_of_Wreck", "Object_Name", "Quality_of_Sounding_Measurement",
                     "Technique_of_Sounding_Measurement", "Water_Level", "Information")
    ],
    {"name": "Scale_Minimum", "type": "esriFieldTypeInteger", "alias": "Scale_Minimum", "domain": None},
    *[
        {"name": name, "type": "esriFieldTypeString", "alias": name, "length": 254, "domain": None}
        for name in ("Source_Date", "Source_Indication", "Source_Dataset", "Exposition_of_Sounding",
                     "Feature_ID")
    ],
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELD_DECLARATIONS if field["name"] != "SHAPE"]
# The feature query answers with a name/type/alias(/length) field profile and no
# domain member; derive it from the pinned layer declarations instead of a copy.
QUERY_FIELD_DECLARATIONS = [
    {key: field[key] for key in ("name", "type", "alias") if key in field}
    | ({"length": field["length"]} if "length" in field else {})
    for field in ATTRIBUTE_FIELDS
]
SOURCE_CRS = CRS.from_epsg(4326)


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_urls(ids: list[int]) -> list[tuple[str, str]]:
    return [
        ("bts_item", BTS_ITEM_URL),
        ("service_directory", SERVICE_DIRECTORY_URL),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
        ("features", _query({"objectIds": ",".join(map(str, ids)), "outFields": "*",
            "returnGeometry": "true", "returnZ": "true", "returnM": "true",
            "outSR": "4326", "orderByFields": "OBJECTID ASC", "f": "json"})),
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
        ("service_after", SERVICE_URL + "?f=pjson"),
    ]


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    roster = manifest.get("artifact_roster")
    if not isinstance(roster, dict) or set(manifest["receipts"]) != EXPECTED_CAPTURE_NAMES:
        raise ValueError("retained_capture_roster_or_names_invalid")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    expected_urls = dict(_capture_urls(EXPECTED_IDS))
    captures = {}
    for name in sorted(manifest["receipts"]):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        validate_capture_request_identity(name, receipt, expected_urls[name])
        captures[name] = (body, receipt)
    return captures


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    if set(captures) != EXPECTED_CAPTURE_NAMES:
        raise ValueError("source_capture_name_roster_drift")
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    item = docs["bts_item"]
    if (item.get("id") != BTS_ITEM_ID or item.get("owner") != "USDOT_BTS"
            or item.get("type") != "PDF"):
        raise ValueError("bts_ienc_item_identity_drift")
    item_text = " ".join(str(item.get(key, "")) for key in ("title", "description"))
    if ("Inland Electronic Navigational Charts" not in item_text
            or "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes" not in item_text
            or "USACE_IENC_Master_Service_gdb.zip" not in item_text):
        raise ValueError("bts_ienc_official_distribution_link_drift")
    directory = docs["service_directory"]
    if not any(service.get("name") == "IENC_Feature_Classes/WRECKS_POINT"
               and service.get("type") == "MapServer" for service in directory.get("services", [])):
        raise ValueError("official_ienc_service_directory_drift")
    service = docs["service_before"]
    service_layers = [(layer.get("id"), layer.get("name"), layer.get("geometryType"))
                      for layer in service.get("layers", [])]
    if (service.get("mapName") != "WRECKS_POINT" or service.get("copyrightText") != "USACE IENC"
            or service_layers != [(LAYER_ID, "WRECKS_POINT", "esriGeometryPoint")]):
        raise ValueError("official_wreck_point_service_drift")
    description = " ".join(str(service.get(key, "")) for key in ("serviceDescription", "description"))
    if "ruined remains of a stranded or sunken vessel" not in description:
        raise ValueError("official_wreck_point_description_drift")
    layer = docs["layer_before"]
    fields = layer.get("fields")
    if fields != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != "WRECKS_POINT"
            or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("maxRecordCount") != 2000
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError("source_layer_identity_extent_or_limit_drift")
    if layer.get("hasZ") or layer.get("hasM"):
        raise ValueError("source_layer_dimensional_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent_reference = layer.get("extent", {}).get("spatialReference")
    if extent_reference is not None:
        _validate_spatial_reference(extent_reference, "source_layer_extent")
    if docs["layer_after"] != layer or docs["service_after"] != service:
        raise ValueError("source_service_or_layer_changed_during_capture")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (count_before != EXPECTED_COUNT or count_after != count_before or not isinstance(ids_before, list)
            or any(type(oid) is not int for oid in ids_before) or ids_before != EXPECTED_IDS
            or ids_after != ids_before
            or len(set(ids_before)) != EXPECTED_COUNT or _sha(_canonical(ids_before)) != EXPECTED_ID_SHA256):
        raise ValueError("source_count_or_id_population_drift")
    page = docs["features"]
    if "error" in page or page.get("exceededTransferLimit") is True:
        raise ValueError("source_feature_query_error_or_truncation")
    if page.get("hasZ") or page.get("hasM"):
        raise ValueError("source_page_dimensional_drift")
    if page.get("geometryType") != "esriGeometryPoint":
        raise ValueError("source_page_geometry_type_drift")
    if page.get("fields") != QUERY_FIELD_DECLARATIONS:
        raise ValueError("source_page_field_schema_drift")
    _validate_spatial_reference(page.get("spatialReference"), "source_feature_query")
    features = page.get("features")
    if not isinstance(features, list) or len(features) != EXPECTED_COUNT:
        raise ValueError("source_feature_population_incomplete")
    expected_attrs = [field["name"] for field in ATTRIBUTE_FIELDS]
    ids = []
    vertices = 0
    coordinate_values = []
    geometry_valid = geometry_empty = 0
    for feature in features:
        attrs, geometry = feature.get("attributes"), feature.get("geometry")
        if (not isinstance(attrs, dict) or set(attrs) != set(expected_attrs)
                or len(attrs) != len(expected_attrs)):
            raise ValueError("source_attribute_roster_or_order_drift")
        for field in ATTRIBUTE_FIELDS:
            value = attrs[field["name"]]
            if value is None:
                continue
            kind = field["type"]
            if kind == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                raise ValueError("source_oid_value_type_drift")
            if kind == "esriFieldTypeInteger" and type(value) is not int:
                raise ValueError("source_integer_value_type_drift:" + field["name"])
            if kind == "esriFieldTypeString" and not isinstance(value, str):
                raise ValueError("source_string_value_type_drift:" + field["name"])
        if not isinstance(geometry, dict) or set(geometry) - {"x", "y", "hasZ", "hasM", "spatialReference"}:
            raise ValueError("source_point_geometry_structure_drift")
        if "x" not in geometry or "y" not in geometry:
            raise ValueError("source_point_ordinates_missing")
        if geometry.get("hasZ") or geometry.get("hasM"):
            raise ValueError("source_geometry_dimensional_drift")
        if geometry.get("spatialReference") is not None:
            _validate_spatial_reference(geometry.get("spatialReference"), "source_feature_geometry")
        wkb = _esri_point_wkb(geometry)
        vertices += 1
        coordinate_values.append((geometry["x"], geometry["y"]))
        geom = from_wkb(wkb)
        geometry_empty += int(geom.is_empty)
        geometry_valid += int(geom.is_valid and not geom.is_empty)
        oid = attrs.get("OBJECTID")
        if type(oid) is not int or oid not in EXPECTED_IDS:
            raise ValueError("source_oid_roster_drift")
        ids.append(oid)
        feature["attributes"] = {name: attrs[name] for name in expected_attrs}
    if sorted(ids) != EXPECTED_IDS or len(set(ids)) != EXPECTED_COUNT:
        raise ValueError("source_feature_id_population_drift")
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    observed_extent = {
        "xmin": min(x for x, _ in coordinate_values), "ymin": min(y for _, y in coordinate_values),
        "xmax": max(x for x, _ in coordinate_values), "ymax": max(y for _, y in coordinate_values),
    }
    feature_ids = [row["attributes"]["Feature_ID"] for row in features]
    feature_id_counts: dict[str, int] = {}
    for value in feature_ids:
        if value is None:
            continue
        feature_id_counts[value] = feature_id_counts.get(value, 0) + 1
    source_dates = [row["attributes"]["Source_Date"] for row in features
                    if row["attributes"]["Source_Date"]]
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(sorted(ids))),
        "source_id_min": min(ids), "source_id_max": max(ids),
        "source_schema_sha256": _sha(_canonical(fields)),
        "source_record_sha256": _sha(_canonical([row["attributes"] for row in features])),
        "source_geometry_sha256": _sha(_canonical([[row["attributes"]["OBJECTID"], row["geometry"]]
                                                   for row in features])),
        "source_geometry_type": "esriGeometryPoint", "source_wkid": 4326,
        "source_vertex_count": vertices, "source_vertex_bounds": observed_extent,
        "source_extent": layer["extent"],
        "geometry_valid_count": geometry_valid, "geometry_empty_count": geometry_empty,
        "geometry_invalid_count": len(features) - geometry_valid - geometry_empty,
        "feature_id_distinct_count": len(feature_id_counts),
        "feature_id_null_count": sum(1 for value in feature_ids if value is None),
        "feature_id_duplicate_values": {value: count for value, count in sorted(feature_id_counts.items())
                                        if count > 1},
        "source_date_span": {"min": min(source_dates), "max": max(source_dates)} if source_dates else None,
        "scale_minimum_distinct_values": sorted({row["attributes"]["Scale_Minimum"] for row in features}),
        "source_coverage_scope": "Complete current WRECKS_POINT feature class returned by the official "
                                 "IENC service; the service describes the features as the ruined "
                                 "remains of a stranded or sunken vessel which has been rendered "
                                 "useless (IHO S-32 5th Edition 6027), and the observed inland river "
                                 "coverage is not a nationwide wreck inventory.",
        "bts_catalog_item_id": BTS_ITEM_ID,
        "prior_bts_catalog_item_id": PRIOR_BTS_ITEM_ID,
        "bts_catalog_item_successor_review_pending": True,
        "drift_guard": "Current BTS PDF catalog link, exact service directory/service/layer roster and service "
                       "description, full layer and query-page field declarations/types, explicit EPSG:4326 "
                       "layer/page/feature CRS representations, XY-only dimensional flags, count and "
                       "before/after ID roster, complete returned attributes, finite point ordinates, "
                       "geographic coordinate ranges and declared-extent containment are checked; "
                       "duplicate Feature_ID values are preserved.",
    }
    return fields, features, audit


def _esri_point_wkb(geometry: dict) -> bytes:
    x, y = geometry.get("x"), geometry.get("y")
    if type(x) not in (int, float) or type(y) not in (int, float):
        raise ValueError("source_xy_ordinate_contract_drift")
    if not (math.isfinite(x) and math.isfinite(y)):
        raise ValueError("source_xy_ordinate_contract_drift")
    if not (-180.0 <= x <= 180.0 and -90.0 <= y <= 90.0):
        raise ValueError("source_xy_geographic_domain_violation")
    if not (EXPECTED_EXTENT["xmin"] - EXTENT_TOLERANCE_DEGREES <= x
            <= EXPECTED_EXTENT["xmax"] + EXTENT_TOLERANCE_DEGREES
            and EXPECTED_EXTENT["ymin"] - EXTENT_TOLERANCE_DEGREES <= y
            <= EXPECTED_EXTENT["ymax"] + EXTENT_TOLERANCE_DEGREES):
        raise ValueError("source_point_outside_declared_extent")
    # The pinned layer contract is XY-only; dimensional flags are rejected by the
    # feature structure check and must fail closed, never be sealed.
    return struct.pack("<BIdd", 1, 1, float(x), float(y))


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeString": pa.string()}[field["type"]]


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    properties = [field for field in fields if field["name"] != "SHAPE"]
    arrays = [pa.array([row["attributes"][field["name"]] for row in features], type=_arrow_type(field))
              for field in properties]
    arrays.append(pa.array([_esri_point_wkb(row["geometry"]) for row in features], type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=[field["name"] for field in properties] + ["geometry"])
    crs = {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS", "name": "WGS 84",
           "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
                     "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
                     "id": {"authority": "EPSG", "code": 6326}},
           "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
                                                                     {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
           "id": {"authority": "EPSG", "code": 4326}}
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
                                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"],
                                                             "crs": crs}}})}
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    return _sha(_canonical(_capture_identity(captures, root)))


def capture_wrecks_usace_ienc(storage: StagingStorageResource, *, version: str,
                              capture_directory: str | None = None) -> dict:
    """Capture or replay one complete, unapproved native IENC source candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if (not isinstance(version, str) or not version or version in {".", ".."}
            or Path(version).name != version or "/" in version or "\\" in version):
        raise ValueError("unsafe_capture_version")
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        captures = _read_replay(root)
    else:
        if requested and (any(requested.glob("*.body")) or any(requested.glob("*.wire"))
                          or any(requested.glob("*.receipt.json"))):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root = ((requested / version / uuid4().hex) if requested else
                default_capture_dir("hifld-native-reconstruction", "388", "captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/usace-ienc-draft"}) as client:
                # IDs are acquired first to form an exact full-population query;
                # count and ID brackets below independently detect source drift.
                body, receipt = _capture(client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                                         root, "ids_before")
                ids = json.loads(body).get("objectIds")
                if (not isinstance(ids, list) or ids != EXPECTED_IDS or len(set(ids)) != EXPECTED_COUNT
                        or any(type(oid) is not int for oid in ids)):
                    raise ValueError("source_seed_id_population_drift")
                captures["ids_before"] = (body, receipt)
                for name, url in _capture_urls(ids):
                    if name == "ids_before":
                        continue
                    captures[name] = _capture(client, url, root, name)
            fields, features, audit = _validate(captures)
        except Exception as exc:
            retained = {}
            for path in sorted(root.glob("*.receipt.json")):
                try:
                    retained[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    retained[path.name] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({
                "captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
                "error_code": type(exc).__name__, "receipts": retained}) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
            raise
        (root / "capture_manifest.json").write_bytes(_canonical({
            "captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
            "source_feature_count": audit["source_feature_count"],
            "source_record_sha256": audit["source_record_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        captures = _read_replay(root)
    fields, features, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    evidence = _capture_identity(captures, root)
    capture_sha = _sha(_canonical(evidence))
    audit.update({"source_capture_evidence_sha256": capture_sha, "source_capture_evidence": evidence,
                  "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
                  "source_release_accepted": False, "source_freshness_accepted": False,
                  "schema_contract_accepted": False, "id_contract_accepted": False,
                  "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
                  "release_accepted": False, "baseline_accepted": False, "source_population_complete": True})
    version = "v" + _sha(_canonical({"record_sha256": audit["source_record_sha256"],
                                     "geometry_sha256": audit["source_geometry_sha256"],
                                     "schema_sha256": audit["source_schema_sha256"],
                                     "decoded_wire_evidence_sha256": capture_sha,
                                     "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    parquet = _parquet_bytes(fields, features)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    inputs = [{"input_id": name, "producer": ("USDOT Bureau of Transportation Statistics (BTS) IENC NTAD item"
                                              if name == "bts_item" else PUBLISHER),
               "host": urlsplit(receipt["url"]).hostname,
               "role": "official_distributor" if name == "bts_item" else "producer_publication",
               "canonical_url": BTS_ITEM_URL if name == "bts_item" else LAYER_URL,
               "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
               "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
               "bytes": receipt["bytes"], "sha256": receipt["sha256"],
               "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
               "release_date": None,
               "release_date_unknown_reason": "The BTS IENC catalog record describes a bi-monthly USACE/AGC "
                                              "chart delivery cadence but does not identify this layer's "
                                              "release date, and the queried service publishes no edition "
                                              "date. Captured Source_Date attribute values span 20020715 "
                                              "through 20260722 (source-encounter vintages), not a service "
                                              "release date.",
               "license": "Unresolved; rights review pending.",
               "attribution": "USACE IENC / BTS; attribution and rights review pending.",
               **({"distributor": "USACE IENC Feature Classes service", "delegation_evidence": {
                   "bts_item_url": BTS_ITEM_URL, "bts_item_id": BTS_ITEM_ID, "bts_item_owner": "USDOT_BTS",
                   "official_service_directory": SERVICE_DIRECTORY_URL,
                   "linked_feature_classes_service": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes",
                   "prior_bts_item_id": PRIOR_BTS_ITEM_ID,
                   "succession_status": "unaccepted; the current public PDF catalog item links the producer "
                                        "service, while a prior draft cited a different BTS item; continuity "
                                        "of the catalog record and layer edition has not been approved",
                   "basis": "The captured BTS PDF record identifies the IENC program, describes "
                            "USACE/Army Geospatial Center updates, and explicitly links the USACE IENC "
                            "Feature Classes service and master file geodatabase. The queried layer's own "
                            "service metadata identifies copyright USACE IENC; the direct service response, "
                            "not the PDF item, supplies the captured geometry and attributes."}}
                  if name == "bts_item" else {})}
              for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "Wrecks (USACE IENC)",
        "description": "Complete current WRECKS_POINT feature class from the official USACE IENC Feature "
                       "Classes service; unapproved faithful candidate. Observed coverage is limited to "
                       "records returned by this layer.",
        "publisher": PUBLISHER, "agency": "US Army Corps of Engineers", "source_url": LAYER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Esri JSON Point to GeoParquet WKB candidate",
                             "parameters": {
            "feature_count": EXPECTED_COUNT,
            "source_fields": "all declared non-geometry fields retained in source order and declared types",
            "source_ids": "OBJECTID and Feature_ID retained unchanged, including any duplicates",
            "source_crs": "EPSG:4326",
            "geometry": "2D point coordinates preserved; no reprojection, repair, or simplification",
            "raw_evidence": "complete decoded response and wire bodies retained with checksummed receipts",
            "candidate_status": "unapproved; historical/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": f"Current public BTS IENC PDF item {BTS_ITEM_ID} links the USACE IENC Feature Classes "
                   f"service and master geodatabase. A prior draft cited BTS item {PRIOR_BTS_ITEM_ID}; the "
                   "relationship between the records and continuity of the layer edition remain unapproved. "
                   f"This candidate captures the complete directly queried service layer {LAYER_ID} "
                   "WRECKS_POINT only; the service describes the features as the ruined remains of a "
                   "stranded or sunken vessel which has been rendered useless (IHO S-32 5th Edition 6027). "
                   "It makes no claim of historical HIFLD/DataLumos equivalence or nationwide wreck coverage.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason":
            "All declared source attributes and IDs are typed and retained; the 2D source points are stored "
            "as WKB with source EPSG:4326. Candidate remains unapproved pending compatibility and release "
            "decisions.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No approved format-specific delivery contract exists.",
                                       "pmtiles": "No approved display/generalization policy exists for "
                                                  "this navigation feature class."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    provenance_projection(manifest)
    audit.update({"source_fields": fields, "official_layer_name": "WRECKS_POINT",
                  "official_layer_id": LAYER_ID, "official_source_item": "USDOT_BTS " + BTS_ITEM_ID,
                  "prior_bts_catalog_item_id": PRIOR_BTS_ITEM_ID,
                  "bts_catalog_item_successor_review_pending": True})
    outputs = {output_path: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, _receipt) in sorted(captures.items()):
        _body, wire, _wire_receipt = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipts[name]) + b"\n"
    ready = {"approved": False, "version": version,
             "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
             "source_population_complete": True, "source_record_sha256": audit["source_record_sha256"],
             "source_schema_sha256": audit["source_schema_sha256"], "source_release_accepted": False,
             "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
             "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
             "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    fingerprint = _sha(_canonical({"identity": identity, "source_record_sha256": audit["source_record_sha256"],
                                   "source_schema_sha256": audit["source_schema_sha256"],
                                   "capture_evidence_sha256": capture_sha}))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(storage, state_key, version, fingerprint)).get("payload")
        if (not isinstance(original, dict) or original.get("processing_identity") != identity
                or original.get("source_record_sha256") != audit["source_record_sha256"]
                or original.get("source_schema_sha256") != audit["source_schema_sha256"]
                or not isinstance(original.get("capture_receipts"), dict)):
            raise ValueError("cached_first_candidate_identity_or_source_changed")
        original_receipts = original["capture_receipts"]
        if set(original_receipts) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name in captures:
            fresh_body, fresh_wire, _receipt = _load_capture(root, name)
            if (storage.read_key(f"{prefix}/raw/{name}.body") != fresh_body
                    or storage.read_key(f"{prefix}/raw/{name}.wire") != fresh_wire):
                raise ValueError("cached_source_evidence_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
                                identity, audit["source_record_sha256"], audit["source_schema_sha256"],
                                original.get("objects", {}), original_receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": EXPECTED_COUNT, "source_population_complete": True,
                "source_id_sha256": audit["source_id_sha256"], "source_id_min": 1,
                "source_id_max": EXPECTED_COUNT, "source_schema_sha256": audit["source_schema_sha256"],
                "source_coverage_scope": audit["source_coverage_scope"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_capture_evidence_sha256": capture_sha, "source_vertex_count": audit["source_vertex_count"],
                "geometry_valid_count": audit["geometry_valid_count"],
                "geometry_invalid_count": audit["geometry_invalid_count"],
                "geometry_empty_count": audit["geometry_empty_count"],
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
                "release_accepted": False, "capture_directory": str(root)}
    descriptors = {}
    for name, body in sorted(outputs.items()):
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if (snapshot is None or snapshot.size != descriptor["bytes"]
                or storage.sha256_key(key) != descriptor["sha256"]
                or storage.object_snapshot(key) != snapshot):
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    seal_ready_candidate(storage, state_key, version, fingerprint, {"processing_identity": identity,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "capture_receipts": receipts,
        "objects": descriptors})
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity
    )
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": 1,
            "source_id_max": EXPECTED_COUNT, "source_schema_sha256": audit["source_schema_sha256"],
            "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_capture_evidence_sha256": capture_sha,
            "source_vertex_count": audit["source_vertex_count"],
            "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_invalid_count": audit["geometry_invalid_count"],
            "geometry_empty_count": audit["geometry_empty_count"],
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(root)}
