"""Source-faithful draft capture of the official USACE Principal Ports successor layer."""

from __future__ import annotations

import io
import json
import math
import shutil
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
from dagster_hifld.sources.arcgis_capture import capture_layer, RetainedCaptureView
from dagster_hifld.sources.arcgis_layer import capture_witness, load_replay, read_ienc_layer_replay
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.esri_geometry import to_wkb
from dagster_hifld.sources.capture_common import (
    candidate_prefix,
    capture_get,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "principal-ports"
FILE_SLUG = DATASET_SLUG
BTS_ITEM_ID = "16d570a7aa054943aabf52b0032a3b57"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
DATA_GOV_LANDING_URL = "https://catalog.data.gov/dataset/principal-ports"
DATA_GOV_METADATA_URL = "https://transtats.bts.gov/NTADmetadata/USDOT_BTS_NTAD_Principal_Ports.xml"
DATA_GOV_RESOURCE_DOI = "https://doi.org/10.21949/1528014"
BTS_CATALOG_DOI = "https://doi.org/10.21949/1522248"
SERVICE_URL = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/Principal_Ports/FeatureServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "United States Army Corps of Engineers (USACE), USACE Institute for Water Resources / Waterborne Commerce Statistics Center; published through the USACE ArcGIS service and linked by the US DOT BTS NTAD catalog"
PAGE_SIZE = 1000
PAGE_MAX_BYTES = 24_000_000
CAPTURE_MAX_BYTES = 128_000_000
SOURCE_CRS = CRS.from_epsg(3857)
EXPECTED_FIELD_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "nullable": False, "domain": None},
    {"name": "RANK", "type": "esriFieldTypeDouble", "alias": "RANK", "nullable": True, "domain": None},
    {"name": "PORT", "type": "esriFieldTypeDouble", "alias": "PORT", "nullable": True, "domain": None},
    {"name": "TYPE", "type": "esriFieldTypeString", "alias": "TYPE", "length": 11, "nullable": True, "domain": None},
    {"name": "PORTNAME", "type": "esriFieldTypeString", "alias": "PORTNAME", "length": 80, "nullable": True, "domain": None},
    {"name": "TOTAL", "type": "esriFieldTypeDouble", "alias": "TOTAL", "nullable": True, "domain": None},
    {"name": "DOMESTIC", "type": "esriFieldTypeDouble", "alias": "DOMESTIC", "nullable": True, "domain": None},
    {"name": "FOREIGN_", "type": "esriFieldTypeDouble", "alias": "FOREIGN", "nullable": True, "domain": None},
    {"name": "IMPORTS", "type": "esriFieldTypeDouble", "alias": "IMPORTS", "nullable": True, "domain": None},
    {"name": "EXPORTS", "type": "esriFieldTypeDouble", "alias": "EXPORTS", "nullable": True, "domain": None},
    {"name": "Shape__Area", "type": "esriFieldTypeDouble", "alias": "Shape__Area", "nullable": True, "domain": None},
    {"name": "Shape__Length", "type": "esriFieldTypeDouble", "alias": "Shape__Length", "nullable": True, "domain": None},
]
ATTRIBUTE_FIELDS = EXPECTED_FIELD_DECLARATIONS
_BASE_CAPTURE_NAMES = {
    "data_gov_landing", "bts_metadata", "bts_item", "service_before", "layer_before", "count_before",
    "ids_before", "layer_after", "count_after", "ids_after",
}


def _capture_names(ids: list[int]) -> set[str]:
    return set(_BASE_CAPTURE_NAMES) | {
        f"page_{index:03d}" for index in range((len(ids) + PAGE_SIZE - 1) // PAGE_SIZE)}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_urls(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("data_gov_landing", DATA_GOV_LANDING_URL),
        ("bts_metadata", DATA_GOV_METADATA_URL),
        ("bts_item", BTS_ITEM_URL),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        page_ids = ids[start:start + PAGE_SIZE]
        urls.append((f"page_{index:03d}", _query({
            "objectIds": ",".join(map(str, page_ids)), "outFields": "*",
            "returnGeometry": "true", "orderByFields": "OBJECTID ASC", "f": "json",
        })))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ])
    return urls


def _capture(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * PAGE_MAX_BYTES > CAPTURE_MAX_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    return capture_get(client, url, root, name, max_bytes=PAGE_MAX_BYTES)


def _load_capture(root: Path, name: str) -> tuple[bytes, bytes, dict]:
    body, receipt = load_replay(root, name)
    return body, (root / f"{name}.wire").read_bytes(), receipt


def _validate_request_identity(name: str, receipt: dict, expected_url: str) -> None:
    if (receipt.get("method") != "GET" or receipt.get("url") != expected_url
            or receipt.get("final_url") != expected_url):
        raise ValueError("retained_capture_request_identity_mismatch:" + name)


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    if any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError("retained_capture_symlink_rejected")
    validate_capture_roster(root, {path.name for path in root.iterdir()})
    sizes = [path.stat().st_size for path in root.iterdir()]
    if any(size > PAGE_MAX_BYTES for size in sizes):
        raise ValueError("retained_capture_response_budget_exceeded")
    if sum(sizes) > CAPTURE_MAX_BYTES:
        raise ValueError("retained_capture_total_budget_exceeded")
    return read_ienc_layer_replay(
        root, capture_roster=_capture_urls,
        load_capture=_load_capture,
        validate_request_identity=_validate_request_identity)

def _validate_crs(value: object, label: str, *, required: bool = True) -> bool:
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            code = value[key]
            if type(code) is not int or code not in {102100, 3857}:
                raise ValueError(f"{label}_crs_drift")
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            text = value[key]
            if not isinstance(text, str) or not text.strip():
                raise ValueError(f"{label}_crs_drift")
            try:
                parsed = CRS.from_wkt(text)
            except Exception as exc:
                raise ValueError(f"{label}_crs_drift") from exc
            if not parsed.equals(SOURCE_CRS, ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
            found = True
    if required and not found:
        raise ValueError(f"{label}_crs_drift")
    return found


def _feature_geometry_wkb(geometry: dict) -> bytes:
    """Convert native ArcGIS rings to WKB through GDAL without moving or repairing XYs.

    Esri encodes multipart polygons as a flat ring array. Ring orientation and
    hole assignment are GDAL's, and every served coordinate is verified
    unchanged; the raw feature capture remains the definitive record of the
    producer's ring array and order. Structural ring contracts (closure, XY
    ordinates, dimension flags, feature spatial reference) are checked here,
    so invalid served rings refuse the capture instead of being repaired.
    """
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    for flag in ("hasZ", "hasM"):
        if flag in geometry and type(geometry[flag]) is not bool:
            raise ValueError("source_polygon_ordinate_flag_invalid")
        if geometry.get(flag, False):
            raise ValueError("source_polygon_not_xy")
    if "spatialReference" in geometry:
        _validate_crs(geometry["spatialReference"], "feature_geometry")
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
            raise ValueError("source_polygon_ring_invalid_or_open")
        if any(not isinstance(point, list) or len(point) != 2
               or any(isinstance(value, bool) or not isinstance(value, (int, float))
                      or not math.isfinite(float(value)) for value in point)
               for point in ring):
            raise ValueError("source_polygon_coordinate_not_finite_xy")
    return to_wkb(geometry, "esriGeometryPolygon", has_z=False, has_m=False)


def _attribute_type(field: dict) -> pa.DataType:
    types = {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
             "esriFieldTypeDouble": pa.float64(), "esriFieldTypeString": pa.string()}
    try:
        return types[field["type"]]
    except KeyError as exc:
        raise ValueError("source_attribute_type_drift:" + str(field.get("name"))) from exc


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    arrays = []
    schema_fields = []
    attributes = [field for field in fields if field["name"] != "SHAPE"]
    for field in attributes:
        name, dtype = field["name"], _attribute_type(field)
        values = [feature["attributes"][name] for feature in features]
        if pa.types.is_integer(dtype) and any(value is not None and type(value) is not int for value in values):
            raise ValueError("source_integer_attribute_type_drift:" + name)
        if pa.types.is_floating(dtype) and any(value is not None and (
            isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
        ) for value in values):
            raise ValueError("source_double_attribute_type_drift:" + name)
        if pa.types.is_string(dtype) and any(value is not None and not isinstance(value, str) for value in values):
            raise ValueError("source_string_attribute_type_drift:" + name)
        arrays.append(pa.array(values, type=dtype, safe=True))
        schema_fields.append(pa.field(name, dtype, nullable=True,
                                      metadata={b"esri_field": _canonical(field)}))
    wkb = [_feature_geometry_wkb(feature["geometry"]) for feature in features]
    arrays.append(pa.array(wkb, type=pa.binary()))
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {
        "geometry": {"encoding": "WKB", "geometry_types": ["MultiPolygon", "Polygon"],
                     "crs": SOURCE_CRS.to_json_dict()}}}
    schema = pa.schema(schema_fields, metadata={b"geo": _canonical(geo)})
    table = pa.Table.from_arrays(arrays, schema=schema)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    ids_before = capture_witness(captures)
    if not ids_before or set(captures) != _capture_names(ids_before):
        raise ValueError("source_capture_roster_incomplete")
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()
            if name not in {"data_gov_landing", "bts_metadata"}}
    catalog = captures["data_gov_landing"][0].decode("utf-8", errors="replace")
    source_metadata = captures["bts_metadata"][0].decode("utf-8-sig", errors="replace")
    item = docs["bts_item"]
    item_text = " ".join(str(item.get(key, "")) for key in ("title", "description", "snippet"))
    if (item.get("id") != BTS_ITEM_ID or item.get("owner") != "usace_iwr_cac"
            or item.get("access") != "public" or item.get("url") != SERVICE_URL
            or "CY 2023" not in item_text):
        raise ValueError("principal_ports_service_item_identity_drift")
    if ("publisher\": \"United States Army Corp of Engineers (USACE)" not in catalog
            or DATA_GOV_RESOURCE_DOI not in catalog or "CY 2023" not in catalog
            or "https://transtats.bts.gov/NTADmetadata/USDOT_BTS_NTAD_Principal_Ports.xml" not in catalog):
        raise ValueError("principal_ports_data_gov_catalog_lineage_drift")
    if BTS_CATALOG_DOI not in catalog:
        raise ValueError("principal_ports_bts_catalog_doi_drift")
    if (DATA_GOV_RESOURCE_DOI not in source_metadata or "CY 2023" not in source_metadata
            or "United States Army Corp of Engineers (USACE)" not in source_metadata
            or "Waterborne Commerce Statistics Center" not in source_metadata):
        raise ValueError("principal_ports_bts_metadata_identity_drift")
    service = docs["service_before"]
    service_text = " ".join(str(service.get(key, "")) for key in ("serviceDescription", "description"))
    if (service.get("serviceItemId") != BTS_ITEM_ID or service.get("maxRecordCount") != 2000
            or service.get("copyrightText") != "U.S. Army Corps of Engineers"
            or "CY 2023" not in service_text):
        raise ValueError("principal_ports_service_identity_drift")
    _validate_crs(service.get("spatialReference"), "source_service")
    layer = docs["layer_before"]
    fields = layer.get("fields")
    projected = [{key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}
                 for field in fields] if isinstance(fields, list) else None
    if projected != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("name") != "Principal_Ports" or layer.get("id") != LAYER_ID
            or layer.get("serviceItemId") != BTS_ITEM_ID
            or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("objectIdField") != "OBJECTID"
            or layer.get("maxRecordCount") != 2000
            or layer.get("hasZ") is not False or layer.get("hasM") is not False):
        raise ValueError("source_layer_identity_geometry_extent_or_page_limit_drift")
    _validate_crs(layer.get("spatialReference"), "source_layer")
    _validate_crs(layer.get("extent", {}).get("spatialReference"), "source_layer_extent")
    declared_extent = {key: layer["extent"].get(key) for key in ("xmin", "ymin", "xmax", "ymax")}
    if (any(type(value) not in (int, float) or not math.isfinite(value)
            for value in declared_extent.values())
            or declared_extent["xmin"] >= declared_extent["xmax"]
            or declared_extent["ymin"] >= declared_extent["ymax"]):
        raise ValueError("source_layer_extent_invalid")
    if docs["layer_after"] != layer:
        raise ValueError("source_layer_changed_during_capture")
    features = []
    page_ids = []
    for index, start in enumerate(range(0, len(ids_before), PAGE_SIZE)):
        page = docs[f"page_{index:03d}"]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_transfer_limit")
        if page.get("geometryType") != "esriGeometryPolygon":
            raise ValueError("source_page_geometry_type_drift")
        _validate_crs(page.get("spatialReference"), "page_spatial_reference")
        records = page.get("features")
        if not isinstance(records, list):
            raise ValueError("source_page_features_missing")
        observed_ids = [feature.get("attributes", {}).get("OBJECTID") for feature in records]
        if (any(type(oid) is not int for oid in observed_ids)
                or observed_ids != ids_before[start:start + PAGE_SIZE]):
            raise ValueError("source_page_id_slice_incomplete")
        features.extend(records)
        page_ids.extend(observed_ids)
    capture_witness(captures, captured_ids=page_ids)
    expected_attributes = [field["name"] for field in ATTRIBUTE_FIELDS]
    port_values = []
    for feature in features:
        attrs, geometry = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or list(attrs) != expected_attributes:
            raise ValueError("source_record_attribute_schema_drift")
        if isinstance(attrs.get("PORT"), bool) or not isinstance(attrs.get("PORT"), (int, float)):
            raise ValueError("source_port_code_missing_or_type_drift")
        if not math.isfinite(float(attrs["PORT"])):
            raise ValueError("source_port_code_nonfinite")
        port_values.append(float(attrs["PORT"]))
        if not isinstance(geometry, dict) or not isinstance(geometry.get("rings"), list):
            raise ValueError("source_record_polygon_missing")
        _feature_geometry_wkb(geometry)
    if len(set(port_values)) != len(ids_before):
        raise ValueError("source_port_code_roster_missing_or_duplicate")
    features.sort(key=lambda feature: feature["attributes"]["OBJECTID"])
    records_sha = _sha(_canonical([feature["attributes"] for feature in features]))
    geometry_sha = _sha(_canonical([[feature["attributes"]["OBJECTID"], feature["geometry"]]
                                    for feature in features]))
    schema_sha = _sha(_canonical(fields))
    xs, ys = [], []
    vertices = 0
    ring_count_histogram: dict[str, int] = {}
    ring_vertex_order_sha = []
    invalid_geometry_count = 0
    for feature in features:
        rings = feature["geometry"]["rings"]
        ring_count_histogram[str(len(rings))] = ring_count_histogram.get(str(len(rings)), 0) + 1
        for ring in rings:
            vertices += len(ring)
            xs.extend(point[0] for point in ring)
            ys.extend(point[1] for point in ring)
        converted = from_wkb(_feature_geometry_wkb(feature["geometry"]))
        if not converted.is_valid:
            invalid_geometry_count += 1
        ring_vertex_order_sha.append([feature["attributes"]["OBJECTID"], rings])
    extent = {"xmin": min(xs), "ymin": min(ys), "xmax": max(xs), "ymax": max(ys)}
    if (extent["xmin"] < declared_extent["xmin"] - 0.001
            or extent["ymin"] < declared_extent["ymin"] - 0.001
            or extent["xmax"] > declared_extent["xmax"] + 0.001
            or extent["ymax"] > declared_extent["ymax"] + 0.001):
        raise ValueError("source_geometry_outside_declared_extent")
    source_port_roster = sorted(port_values)
    source_port_sha = _sha(_canonical(source_port_roster))
    audit = {
        "source_feature_count": len(features), "source_population_complete": True,
        "source_id_sha256": source_port_sha, "source_id_min": min(source_port_roster),
        "source_id_max": max(source_port_roster), "source_objectid_sha256": _sha(_canonical(sorted(ids_before))),
        "source_schema_sha256": schema_sha, "source_record_sha256": records_sha,
        "source_geometry_sha256": geometry_sha, "source_coverage_extent": extent,
        "source_declared_extent": declared_extent,
        "source_polygon_vertex_count": vertices, "source_ring_count_histogram": ring_count_histogram,
        "source_ring_xy_order_sha256": _sha(_canonical(ring_vertex_order_sha)),
        "source_invalid_geometry_count_without_repair": invalid_geometry_count,
        "source_id_field": "PORT (source float64 port code; preserved without coercion)",
        "source_object_id_field": "OBJECTID (service-generated ObjectID; retained, not treated as stable key)",
        "source_wkid": 3857, "source_geometry_type": "esriGeometryPolygon",
        "source_coverage_scope": "All records witnessed at capture time in the official USACE Principal Ports CY2023 polygon layer; not claimed equivalent to the historical HIFLD CY2022 point layer.",
        "drift_guard": "Catalog/service/item identity, CY2023 marker, geometry type, source CRS, attribute declaration, count, exact OID roster consistency, and unique PORT identifiers are validated; data content hashes identify each unapproved snapshot and do not hard-pin values.",
    }
    return fields, features, audit

def _read_capture(root: Path, storage: StagingStorageResource,
                  version: str) -> dict[str, tuple[bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    if manifest_path.exists():
        return _read_replay(root)
    if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")):
        raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
    root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(90, connect=12),
                          headers={"User-Agent": "hifld-next-datasets/principal-ports-draft"}) as client:
            def validate_seed(ids_body):
                ids = json.loads(ids_body).get("objectIds")
                if (not isinstance(ids, list) or not ids
                        or any(type(oid) is not int or oid <= 0 for oid in ids)
                        or len(set(ids)) != len(ids)):
                    raise ValueError("source_seed_id_population_drift")
                ids = sorted(ids)
                return ids
            captures = RetainedCaptureView(root, captures)
            capture_layer(LAYER_URL, root, 192_000_000, PAGE_SIZE, client=client,
                captures=captures, seed_name='ids_before', seed_url=_query({'where': '1=1', 'returnIdsOnly': 'true', 'f': 'json'}),
                validate_seed=validate_seed, roster=_capture_urls,
                fetch=lambda client, url, root, name: _capture(client, url, root, name),
                on_validation_error=_validate)
            _validate(captures)
    except Exception as exc:
        retained = {}
        for path in sorted(root.glob("*.receipt.json")):
            try:
                name = path.name.removesuffix(".receipt.json")
                receipt = json.loads(path.read_bytes())
                retained[name] = receipt
                body_path, wire_path = root / f"{name}.body", root / f"{name}.wire"
                if body_path.is_file() and wire_path.is_file():
                    captures.setdefault(name, (body_path.read_bytes(), receipt))
            except (OSError, ValueError):
                retained[path.name] = {"receipt_readback_failed": True}
        (root / "capture_manifest.json").write_bytes(_canonical({
            "captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
            "error_code": type(exc).__name__, "receipts": retained,
        }) + b"\n")
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, version,
                                           captures, type(exc).__name__)
        raise
    (root / "capture_manifest.json").write_bytes(_canonical({
        "captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
        "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"}),
    }) + b"\n")
    return _read_replay(root)


def _write_outputs(storage: StagingStorageResource, prefix: str,
                   outputs: dict[str, bytes]) -> dict[str, dict[str, object]]:
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
    return descriptors


def capture_principal_ports(storage: StagingStorageResource, *, version: str,
                               capture_directory: str | None = None) -> dict:
    """Capture/replay one complete local-only source candidate with sealed caching."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not isinstance(version, str) or not version or Path(version).name != version or "/" in version or "\\" in version:
        raise ValueError("unsafe_capture_version")
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    if requested and (".." in requested.parts or any(path.is_symlink() for path in (requested, *requested.parents))):
        raise ValueError("unsafe_capture_directory")
    root = requested or default_capture_dir("hifld-native-reconstruction", "276", "captures") / (version + "-" + uuid4().hex)
    if not requested:
        root.mkdir(parents=True, exist_ok=False)
    captures = _read_capture(root, storage, version)
    fields, features, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    capture_evidence = {}
    for name in sorted(captures):
        _body, _receipt = captures[name]
        _body2, wire, _receipt2 = _load_capture(root, name)
        capture_evidence[name] = {"decoded_sha256": _sha(_body), "wire_sha256": _sha(wire)}
    capture_sha = _sha(_canonical(capture_evidence))
    audit.update({
        "source_capture_evidence_sha256": capture_sha, "source_capture_evidence": capture_evidence,
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False, "baseline_accepted": False,
    })
    record_sha = audit["source_record_sha256"]
    geometry_sha = audit["source_geometry_sha256"]
    schema_sha = audit["source_schema_sha256"]
    identity_sha = _sha(_canonical(identity))
    candidate_version = "v" + _sha(_canonical({
        "record_sha256": record_sha, "geometry_sha256": geometry_sha,
        "schema_sha256": schema_sha, "decoded_wire_evidence_sha256": capture_sha,
        "processing_identity": identity,
    }))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, candidate_version)
    parquet = _parquet_bytes(fields, features)
    inputs = []
    for name, (_body, receipt) in sorted(captures.items()):
        distributor = name == "bts_item"
        catalog_record = name == "data_gov_landing"
        inputs.append({
            "input_id": name,
            "producer": "US DOT Data.gov catalog" if catalog_record else "BTS NTAD ISO metadata" if name == "bts_metadata" else PUBLISHER,
            "host": urlsplit(receipt["url"]).hostname,
            "role": "producer_publication",
            "record_classification": "official_catalog_record" if catalog_record else "official_distributor_metadata" if name == "bts_metadata" else "producer_feature_service",
            "canonical_url": DATA_GOV_LANDING_URL if catalog_record else DATA_GOV_METADATA_URL if name == "bts_metadata" else BTS_ITEM_URL if distributor else LAYER_URL,
            "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
            "release_date": None,
            "release_date_unknown_reason": "The catalog and layer identify the data period as CY 2023 and the catalog reports an update date of 2025-04-17, but no exact release date for the captured layer snapshot is established.",
            "license": "ArcGIS item says public without use restrictions; rights review remains pending.",
            "attribution": "U.S. Army Corps of Engineers (USACE), with USDOT/BTS NTAD catalog attribution; final attribution decision pending.",
            **({"catalog_relationship": {
                "resource_doi": DATA_GOV_RESOURCE_DOI, "dataset_landing": DATA_GOV_LANDING_URL,
                "bts_ntad_landing_doi": BTS_CATALOG_DOI,
                "basis": "Captured Data.gov and BTS NTAD ISO metadata name USACE as publisher and expose the exact ArcGIS REST resource DOI and BTS NTAD landing DOI, which resolve to this USACE feature layer and dataset landing page.",
            }} if catalog_record else {}),
        })
    manifest = {
        "schema_version": 1, "approved": False, "title": "Principal Ports (official USACE CY2023 successor candidate)",
        "description": "Unapproved faithful candidate for the complete witnessed population in the official USACE CY2023 polygon successor layer. The historical HIFLD inventory is CY2022; the geometry has changed from point-like source descriptions to polygons, so equivalence and mapping are not asserted.",
        "publisher": PUBLISHER, "agency": "US Army Corps of Engineers", "source_url": DATA_GOV_LANDING_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Native Esri JSON polygon rings to GeoParquet WKB MultiPolygon candidate",
            "parameters": {"source_fields": "all 12 declared attribute fields retained in source order and declared types; PORT remains float64; OBJECTID remains present",
                           "source_ids": "PORT code values and generated OBJECTID values are retained unchanged; no substitute IDs",
                           "geometry": "all XY vertices remain in native EPSG:3857 values; Esri exterior/hole rings are grouped into WKB MultiPolygon without reprojection, coordinate changes, winding changes, simplification or geometry repair; exact source ring arrays remain in retained raw Esri JSON",
                           "candidate_status": "unapproved CY2023 successor snapshot; source release date, relation to historical CY2022 point source, schema/ID/coverage contract and rights remain unresolved"}}],
        "lineage": "The official Data.gov record identifies USACE as publisher, cites the CY2023 product and links the service resource DOI 10.21949/1528014. That DOI resolves to USACE's public ArcGIS Principal_Ports FeatureServer layer 0; the ArcGIS item account is usace_iwr_cac and the service copyright is U.S. Army Corps of Engineers. Data.gov links the BTS NTAD product landing DOI 10.21949/1522248 and data dictionary DOI 10.21949/1529073. This polygon layer is a geometry-changed successor candidate to the HIFLD archive's CY2022 data, not an accepted equivalent or continuity claim. The archived HIFLD copy is not used as a refresh source.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "All 12 native attributes, including PORT as float64 and OBJECTID, are retained; exact XY sequences are captured in the native raw Esri JSON; unapproved geometry conversion and schema/continuity decisions remain pending.",
            "objects": [{"path": f"geoparquet/{FILE_SLUG}.parquet", "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {
            "geopackage": "No approved historical geometry/schema continuity contract or approved output-format policy.",
            "pmtiles": "No projection, simplification, or display transformation approved.",
            "shapefile_zip": "No approved schema mapping or field-width and precision review.",
        },
        "promotion_allowed": False, "source_accepted": False, "approved_contract": None,
        "baseline": None,
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs = {
        f"geoparquet/{FILE_SLUG}.parquet": parquet,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
    }
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {
        "approved": False, "version": candidate_version,
        "objects": {f"geoparquet/{FILE_SLUG}.parquet": {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True, "source_record_sha256": record_sha,
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False,
    }
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    state_key = f"_construction-state/{DATASET_SLUG}/{candidate_version}.json"
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, candidate_version, identity_sha))
        payload = bundle.get("payload")
        if not isinstance(payload, dict) or payload.get("objects") is None or payload.get("capture_receipts") is None:
            raise ValueError("cached_candidate_construction_state_missing")
        for name, (body, _receipt) in captures.items():
            if storage.read_key(f"{prefix}/raw/{name}.body") != body:
                raise ValueError("cached_source_bytes_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix,
                                candidate_version, identity_sha, identity, record_sha,
                                schema_sha, payload["objects"], payload["capture_receipts"])
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
                "feature_count": len(features), "source_reported_count": len(features), "source_population_complete": True,
                "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"], "source_id_max": audit["source_id_max"],
                "source_schema_sha256": schema_sha, "source_coverage_extent": audit["source_coverage_extent"],
                "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
                "source_objectid_sha256": audit["source_objectid_sha256"],
                "source_invalid_geometry_count_without_repair": audit["source_invalid_geometry_count_without_repair"],
                "source_coverage_scope": audit["source_coverage_scope"], "drift_guard": audit["drift_guard"],
                "source_capture_evidence_sha256": capture_sha,
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "release_accepted": False, "capture_directory": str(root)}
    descriptors = _write_outputs(storage, prefix, outputs)
    seal_ready_candidate(storage, state_key, candidate_version, identity_sha, {
        "processing_identity": identity, "source_record_sha256": record_sha,
        "source_schema_sha256": schema_sha,
        "capture_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "objects": descriptors,
    })
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
    )
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
            "feature_count": len(features), "source_reported_count": len(features), "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"], "source_id_max": audit["source_id_max"],
            "source_schema_sha256": schema_sha, "source_coverage_extent": audit["source_coverage_extent"],
            "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
            "source_objectid_sha256": audit["source_objectid_sha256"],
            "source_invalid_geometry_count_without_repair": audit["source_invalid_geometry_count_without_repair"],
            "source_coverage_scope": audit["source_coverage_scope"], "drift_guard": audit["drift_guard"],
            "source_capture_evidence_sha256": capture_sha,
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "capture_directory": str(root),
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False}
