"""Bounded, source-faithful capture for the official USACE IENC mooring facility points."""
from __future__ import annotations

import hashlib
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

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)

DATASET_SLUG = "mooring-facility-locations-usace-ienc"
FILE_SLUG = DATASET_SLUG
BTS_ITEM_ID = "7ec34864c8804468a9cf52d437fe4b6c"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
SERVICE_DIRECTORY_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson"
SERVICE_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/MOORING_FACILITY_POINT/MapServer"
LAYER_ID = 17
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; official IENC Feature Classes service"
EXPECTED_COUNT = 9772
EXPECTED_IDS = list(range(1, EXPECTED_COUNT + 1))
EXPECTED_ID_SHA256 = "c03246396967c330211e871aaa5d14cbcdf0a997ef2e6b0f0f89a2b92171123f"
PAGE_SIZE = 2000
PAGE_MAX_BYTES = 8_000_000
CAPTURE_MAX_BYTES = 320_000_000
EXPECTED_EXTENT = {"xmin": -96.39449399999995, "ymin": 30.280838000000074,
                   "xmax": -79.47969589999997, "ymax": 45.02776800000004}
EXPECTED_FIELD_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    {"name": "Category_of_Mooring_Facility", "type": "esriFieldTypeString",
     "alias": "Category_of_Mooring_Facility", "length": 254, "domain": None},
    *[
        {"name": name, "type": "esriFieldTypeString", "alias": name, "length": 254, "domain": None}
        for name in ("Nature_of_Construction", "Object_Name", "Water_Level", "Condition", "Information")
    ],
    {"name": "Scale_Minimum", "type": "esriFieldTypeInteger", "alias": "Scale_Minimum", "domain": None},
    *[
        {"name": name, "type": "esriFieldTypeString", "alias": name, "length": 254, "domain": None}
        for name in ("Source_Date", "Source_Indication", "Source_Dataset", "Feature_ID")
    ],
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELD_DECLARATIONS if field["name"] != "SHAPE"]
SOURCE_CRS = CRS.from_epsg(4326)


def _validate_spatial_reference(value: object, label: str, *, required: bool = True) -> bool:
    """Validate every advertised ArcGIS CRS representation without guessing.

    ArcGIS may return either/both WKID properties and optional WKT strings.
    All present representations must identify EPSG:4326; missing feature-level
    spatial references inherit only after their page CRS has been validated.
    """
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            wkid = value[key]
            if type(wkid) is not int or wkid != 4326:
                raise ValueError(f"{label}_crs_drift")
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            wkt = value[key]
            if not isinstance(wkt, str) or not wkt.strip():
                raise ValueError(f"{label}_crs_drift")
            try:
                parsed = CRS.from_wkt(wkt)
            except Exception as exc:
                raise ValueError(f"{label}_crs_drift") from exc
            if not parsed.equals(SOURCE_CRS, ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
            found = True
    if required and not found:
        raise ValueError(f"{label}_crs_drift")
    return found


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _page_names() -> list[str]:
    return [f"page_{index:03d}" for index in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE)]


def _capture_urls(ids: list[int]) -> list[tuple[str, str]]:
    page_count = (len(ids) + PAGE_SIZE - 1) // PAGE_SIZE
    urls = [
        ("bts_item", BTS_ITEM_URL),
        ("service_directory", SERVICE_DIRECTORY_URL),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]
    for index in range(page_count):
        urls.append((f"page_{index:03d}", _query({"where": "1=1", "resultOffset": index * PAGE_SIZE,
                                                  "resultRecordCount": PAGE_SIZE, "outFields": "*",
                                                  "returnGeometry": "true", "returnZ": "true", "returnM": "true",
                                                  "outSR": "4326", "orderByFields": "OBJECTID ASC", "f": "json"})))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ])
    return urls


def _capture(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire") for path in root.glob(suffix)
                   if path.is_file())
    if retained + 2 * PAGE_MAX_BYTES > CAPTURE_MAX_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    body, receipt = capture_get(client, url, root, name, max_bytes=PAGE_MAX_BYTES)
    return body, receipt


def _load_capture(root: Path, name: str) -> tuple[bytes, bytes, dict]:
    body_path, wire_path, receipt_path = (root / f"{name}.body", root / f"{name}.wire",
                                          root / f"{name}.receipt.json")
    if any(path.is_symlink() for path in (body_path, wire_path, receipt_path)):
        raise ValueError("retained_capture_symlink_rejected")
    body, wire, receipt = body_path.read_bytes(), wire_path.read_bytes(), json.loads(receipt_path.read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("retained_capture_receipt_invalid:" + name)
    return body, wire, receipt


def _validate_capture_request_identity(name: str, receipt: dict, expected_url: str) -> None:
    """Bind retained response bytes to the exact GET endpoint that names them."""
    if (receipt.get("method") != "GET" or receipt.get("url") != expected_url
            or receipt.get("final_url") != expected_url):
        raise ValueError("retained_capture_request_identity_mismatch:" + name)


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    roster = manifest.get("artifact_roster")
    if not isinstance(roster, dict):
        raise ValueError("retained_capture_roster_missing")
    if set(manifest["receipts"]) != {name for name, _url in _capture_urls(EXPECTED_IDS)}:
        raise ValueError("retained_capture_name_roster_changed")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    expected_urls = dict(_capture_urls(EXPECTED_IDS))
    captures = {}
    for name in sorted(manifest["receipts"]):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"][name]:
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        _validate_capture_request_identity(name, receipt, expected_urls[name])
        captures[name] = (body, receipt)
    return captures


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    if (docs["bts_item"].get("id") != BTS_ITEM_ID
            or docs["bts_item"].get("owner") != "USDOT_BTS"
            or docs["bts_item"].get("type") != "PDF"):
        raise ValueError("bts_ienc_item_identity_drift")
    item_text = " ".join(str(docs["bts_item"].get(key, "")) for key in ("title", "description"))
    if ("Inland Electronic Navigational Charts" not in item_text
            or "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes" not in item_text
            or "USACE_IENC_Master_Service_gdb.zip" not in item_text):
        raise ValueError("bts_ienc_official_distribution_link_drift")
    directory = docs["service_directory"]
    if not any(item.get("name") == "IENC_Feature_Classes/MOORING_FACILITY_POINT" and item.get("type") == "MapServer"
               for item in directory.get("services", [])):
        raise ValueError("official_ienc_service_directory_drift")
    service = docs["service_before"]
    service_layers = [(layer.get("id"), layer.get("name"), layer.get("geometryType"))
                      for layer in service.get("layers", [])]
    if (service.get("mapName") != "MOORING_FACILITY_POINT" or service.get("copyrightText") != "USACE IENC"
            or service_layers != [(LAYER_ID, "MOORING_FACILITY_POINT", "esriGeometryPoint")]):
        raise ValueError("official_mooring_facility_point_service_drift")
    layer = docs["layer_before"]
    fields = layer.get("fields")
    if fields != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != "MOORING_FACILITY_POINT"
            or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("maxRecordCount") != 2000
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError("source_layer_geometry_crs_or_page_limit_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent = layer.get("extent", {})
    if "spatialReference" in extent:
        _validate_spatial_reference(extent["spatialReference"], "source_layer_extent")
    # ArcGIS does not declare hasZ/hasM on this layer. Every retained source
    # vertex is checked below to be exactly XY; no ordinate is discarded.
    if docs["layer_after"] != layer:
        raise ValueError("source_layer_changed_during_capture")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (not isinstance(ids_before, list) or sorted(ids_before) != EXPECTED_IDS or ids_after != ids_before
            or len(set(ids_before)) != EXPECTED_COUNT or _sha(_canonical(sorted(ids_before))) != EXPECTED_ID_SHA256):
        raise ValueError("source_id_population_drift")
    features = []
    page_names = sorted(name for name in captures if name.startswith("page_"))
    if len(page_names) != (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE:
        raise ValueError("source_page_roster_incomplete")
    for name in page_names:
        page = docs[name]
        if "error" in page:
            raise ValueError("source_page_error_or_truncation")
        _validate_spatial_reference(page.get("spatialReference"), "source_page")
        rows = page.get("features")
        expected_page_size = min(PAGE_SIZE, EXPECTED_COUNT - len(features))
        if not isinstance(rows, list) or len(rows) != expected_page_size:
            raise ValueError("source_page_features_missing_or_incomplete")
        final_page = len(features) + len(rows) == EXPECTED_COUNT
        if page.get("exceededTransferLimit") is True and final_page:
            raise ValueError("source_page_transfer_limit_truncated")
        features.extend(rows)
    expected_attrs = [field["name"] for field in ATTRIBUTE_FIELDS]
    ids = []
    coordinate_values = []
    for feature in features:
        attrs, geometry = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or set(attrs) != set(expected_attrs) or len(attrs) != len(expected_attrs):
            raise ValueError("source_attribute_roster_drift")
        for field in ATTRIBUTE_FIELDS:
            value = attrs[field["name"]]
            if value is None:
                continue
            if field["type"] == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                raise ValueError("source_oid_value_type_drift")
            if field["type"] == "esriFieldTypeInteger" and type(value) is not int:
                raise ValueError("source_integer_value_type_drift:" + field["name"])
            if field["type"] == "esriFieldTypeString" and not isinstance(value, str):
                raise ValueError("source_string_value_type_drift:" + field["name"])
        feature["attributes"] = {name: attrs[name] for name in expected_attrs}
        oid = attrs["OBJECTID"]
        if oid not in EXPECTED_IDS:
            raise ValueError("source_oid_roster_drift")
        ids.append(oid)
        if not isinstance(geometry, dict) or set(geometry) - {"x", "y", "spatialReference"}:
            raise ValueError("source_point_geometry_structure_drift")
        if "spatialReference" in geometry:
            _validate_spatial_reference(geometry["spatialReference"], "source_feature_geometry")
        if any(key not in geometry for key in ("x", "y")):
            raise ValueError("source_point_coordinates_missing")
        point = (geometry["x"], geometry["y"])
        if any(type(coord) not in (float, int) or not math.isfinite(coord) for coord in point):
            raise ValueError("source_xy_ordinate_contract_drift")
        longitude, latitude = point
        if not (-180 <= longitude <= 180 and -90 <= latitude <= 90):
            raise ValueError("source_coordinate_range_drift")
        if not (EXPECTED_EXTENT["xmin"] <= longitude <= EXPECTED_EXTENT["xmax"]
                and EXPECTED_EXTENT["ymin"] <= latitude <= EXPECTED_EXTENT["ymax"]):
            raise ValueError("source_feature_outside_coverage_extent")
        coordinate_values.append((longitude, latitude))
    if sorted(ids) != EXPECTED_IDS or len(set(ids)) != EXPECTED_COUNT:
        raise ValueError("source_feature_ID_population_drift")
    observed_extent = {
        "xmin": min(x for x, _ in coordinate_values), "ymin": min(y for _, y in coordinate_values),
        "xmax": max(x for x, _ in coordinate_values), "ymax": max(y for _, y in coordinate_values),
    }
    features.sort(key=lambda feature: feature["attributes"]["OBJECTID"])
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(sorted(ids))),
        "source_id_min": min(ids), "source_id_max": max(ids), "source_schema_sha256": _sha(_canonical(fields)),
        "source_record_sha256": _sha(_canonical([row["attributes"] for row in features])),
        "source_geometry_sha256": _sha(_canonical([[row["attributes"]["OBJECTID"], row["geometry"]]
                                                   for row in features])),
        "source_geometry_type": "esriGeometryPoint", "source_wkid": 4326, "source_vertex_count": len(coordinate_values),
        "source_extent": layer["extent"], "source_vertex_bounds": observed_extent,
        "source_coverage_scope": "Complete current MOORING_FACILITY_POINT feature class returned by the official "
                                 "IENC service; observed central/eastern-US inland waterway coverage only, not "
                                 "national coverage.",
        "drift_guard": "Exact BTS item/service link, advertised service/layer identity, declared field roster/types, "
                       "every present layer/page/feature wkid, latestWkid and WKT/WKT2 CRS representation, count, "
                       "before/after IDs, per-page feature completeness, complete returned attributes and XY "
                       "ordinates, geographic coordinate ranges, and every observed point against the declared "
                       "layer coverage extent are checked.",
    }
    return fields, features, audit


def _esri_point_wkb(geometry: dict) -> bytes:
    return struct.pack("<BIdd", 1, 1, float(geometry["x"]), float(geometry["y"]))


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
                                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": crs}}})}
    import io
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _capture_identity(captures: dict[str, tuple[bytes, dict]], root: Path) -> dict:
    # Candidate identity binds every decoded and wire artifact. Receipt JSON is
    # retained as provenance but its observation time does not change content identity.
    digests = {}
    for name in sorted(captures):
        _body, _receipt = captures[name]
        _body2, wire, _receipt2 = _load_capture(root, name)
        digests[name] = {"decoded_sha256": _sha(_body), "wire_sha256": _sha(wire)}
    return digests


def capture_mooring_facility_locations_usace_ienc(storage: StagingStorageResource, *, version: str,
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
        root = (requested / (version + "-" + uuid4().hex)) if requested else Path(
            "/tmp/hifld-native-reconstruction/162/captures") / (version + "-" + uuid4().hex)
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/usace-ienc-draft"}) as client:
                # IDs are acquired first to establish the exact population
                # contract; count and ID brackets below independently detect
                # source drift across the paginated feature retrieval.
                body, receipt = _capture(client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                                         root, "ids_before")
                ids = json.loads(body).get("objectIds")
                if (not isinstance(ids, list) or sorted(ids) != EXPECTED_IDS or len(set(ids)) != EXPECTED_COUNT
                        or any(type(oid) is not int for oid in ids)):
                    raise ValueError("source_seed_ID_population_drift")
                urls = _capture_urls(ids)
                urls = [(name, url) for name, url in urls if name != "ids_before"]
                captures["ids_before"] = (body, receipt)
                for name, url in urls:
                    captures[name] = _capture(client, url, root, name)
            fields, features, audit = _validate(captures)
        except Exception as exc:
            retained = {}
            for path in sorted(root.glob("*.receipt.json")):
                try:
                    retained[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    retained[path.name] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({"captured_at": datetime.now(timezone.utc).isoformat(),
                                                                     "complete": False,
                                                                     "error_code": type(exc).__name__, "receipts": retained}) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
            raise
        (root / "capture_manifest.json").write_bytes(_canonical({"captured_at": datetime.now(timezone.utc).isoformat(),
                                                                 "complete": True,
                                                                 "source_feature_count": audit["source_feature_count"],
                                                                 "source_record_sha256": audit["source_record_sha256"],
                                                                 "source_schema_sha256": audit["source_schema_sha256"],
                                                                 "receipts": {name: receipt for name, (_b, receipt) in sorted(captures.items())},
                                                                 "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        captures = _read_replay(root)
    fields, features, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    evidence = _capture_identity(captures, root)
    capture_sha = _sha(_canonical(evidence))
    record_body = _canonical([row["attributes"] for row in features])
    geometry_body = _canonical([[row["attributes"]["OBJECTID"], row["geometry"]] for row in features])
    schema_body = _canonical(fields)
    audit.update({"source_capture_evidence_sha256": capture_sha, "source_capture_evidence": evidence,
                  "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
                  "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
                  "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
                  "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    version = "v" + _sha(_canonical({"record_sha256": _sha(record_body), "geometry_sha256": _sha(geometry_body),
                                     "schema_sha256": _sha(schema_body), "decoded_wire_evidence_sha256": capture_sha,
                                     "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    parquet = _parquet_bytes(fields, features)
    inputs = [{"input_id": name, "producer": PUBLISHER if name != "bts_item" else "USDOT Bureau of Transportation Statistics (BTS) IENC NTAD item",
               "host": urlsplit(receipt["url"]).hostname,
               "role": "official_distributor" if name == "bts_item" else "producer_publication",
               "canonical_url": BTS_ITEM_URL if name == "bts_item" else LAYER_URL,
               "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
               "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
               "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
               "release_date": None, "release_date_unknown_reason": "The IENC item describes bi-monthly updates but does not provide a release date for this extracted layer snapshot.",
               "license": "Unresolved; rights review pending.", "attribution": "USACE IENC / BTS; attribution and rights review pending.",
               **({"distributor": "USACE IENC Feature Classes service", "delegation_evidence": {"bts_item_url": BTS_ITEM_URL,
                   "bts_item_id": BTS_ITEM_ID, "bts_item_owner": "USDOT_BTS", "official_service_directory": SERVICE_DIRECTORY_URL,
                   "linked_feature_classes_service": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes",
                   "basis": "Captured BTS IENC record identifies the program and explicitly links the official USACE IENC Feature Classes service and master file geodatabase; linked service metadata identifies copyright USACE IENC."}} if name == "bts_item" else {})}
              for name, (_body, receipt) in sorted(captures.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "Mooring Facility Locations (USACE IENC)",
                "description": "Complete current MOORING_FACILITY_POINT feature class from the official USACE IENC Feature Classes service; unapproved faithful candidate. Observed coverage is limited to records returned by this layer.",
                "publisher": PUBLISHER, "agency": "US Army Corps of Engineers", "source_url": LAYER_URL, "license": None,
                "source_modified": None, "date_issued": None, "date_modified": None, "processor": "HIFLD Next",
                "code_commit": identity["code_commit"], "inputs": inputs,
                "transformations": [{"operation": "Esri JSON Point to GeoParquet WKB candidate; source XY ordinates preserved",
                                     "parameters": {"feature_count": EXPECTED_COUNT, "source_fields": "all declared non-geometry fields retained in source order and declared types",
                                                    "source_ids": "OBJECTID and Feature_ID preserved unchanged", "source_crs": "EPSG:4326", "source_geometry": "2D point ordinates exactly as captured; no projection or coordinate repair",
                                                    "raw_evidence": "complete decoded response and wire bytes retained with checksummed receipts", "candidate_status": "unapproved"}}],
                "lineage": "BTS NTAD IENC item 7ec34864c8804468a9cf52d437fe4b6c identifies the IENC program, states that USACE/Army Geospatial Center updates it twice monthly, and links this official USACE IENC Feature Classes service and master file geodatabase. This candidate is exactly the MOORING_FACILITY_POINT service layer 17; its observed 9772 features cover inland waterway mooring facilities and are not represented as national coverage. The DataLumos item 239578 (HIFLD archive inventory entry mooring-facility-locations-usace-ienc-2) is the historical archive fallback, not the maintained feed.",
                "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "All declared source attributes and IDs are typed and retained; the 2D source point ordinates are stored as WKB with source EPSG:4326. Candidate remains unapproved pending compatibility and release decisions.",
                                                                                           "objects": [{"path": f"geoparquet/{FILE_SLUG}.parquet", "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
                "unsupported_output_formats": {"geopackage": "No approved format-specific delivery contract exists.",
                                               "pmtiles": "No approved display/generalization policy exists for this navigation feature class."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    provenance_projection(manifest)
    audit["source_fields"] = fields
    audit["official_layer_name"] = "MOORING_FACILITY_POINT"
    audit["official_layer_id"] = LAYER_ID
    audit["official_source_item"] = "USDOT_BTS " + BTS_ITEM_ID
    outputs = {f"geoparquet/{FILE_SLUG}.parquet": parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n", "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        _body, wire, _receipt = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version, "objects": {f"geoparquet/{FILE_SLUG}.parquet": {"bytes": len(parquet), "sha256": _sha(parquet)}},
             "source_population_complete": True, "source_record_sha256": audit["source_record_sha256"],
             "source_schema_sha256": audit["source_schema_sha256"], "source_release_accepted": False, "freshness_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
             "mapping_approved": False, "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    fingerprint = _sha(_canonical({"identity": identity, "source_record_sha256": audit["source_record_sha256"],
                                   "source_schema_sha256": audit["source_schema_sha256"], "capture_evidence_sha256": capture_sha}))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
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
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"], original.get("objects", {}), original_receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version, "feature_count": EXPECTED_COUNT,
                "source_population_complete": True, "source_id_sha256": audit["source_id_sha256"], "source_id_min": 1,
                "source_id_max": EXPECTED_COUNT, "source_schema_sha256": audit["source_schema_sha256"],
                "source_coverage_scope": audit["source_coverage_scope"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_capture_evidence_sha256": capture_sha,
                "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
                "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False,
                "capture_directory": str(root)}
    descriptors = {}
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
        snapshot = storage.object_snapshot(f"{prefix}/{name}")
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if (snapshot is None or snapshot.size != descriptor["bytes"]
                or storage.sha256_key(f"{prefix}/{name}") != descriptor["sha256"]
                or storage.object_snapshot(f"{prefix}/{name}") != snapshot):
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    seal_ready_candidate(storage, state_key, version, fingerprint, {"processing_identity": identity,
                                                                    "source_record_sha256": audit["source_record_sha256"],
                                                                    "source_schema_sha256": audit["source_schema_sha256"],
                                                                    "capture_receipts": receipts, "objects": descriptors})
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity
    )
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version, "feature_count": EXPECTED_COUNT,
            "source_population_complete": True, "source_id_sha256": audit["source_id_sha256"], "source_id_min": 1,
            "source_id_max": EXPECTED_COUNT, "source_schema_sha256": audit["source_schema_sha256"],
            "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "source_capture_evidence_sha256": capture_sha,
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(root)}
