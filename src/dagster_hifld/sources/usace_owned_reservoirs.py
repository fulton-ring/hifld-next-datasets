"""Complete native capture for the official USACE Owned and Operated Reservoirs layer."""
from __future__ import annotations

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
from shapely import from_wkb, is_valid_reason
from shapely.geometry import LinearRing, MultiPolygon

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_retained_capture_replay
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.arcgis_capture import capture_layer, RetainedCaptureView
from dagster_hifld.sources.esri_geometry import to_wkb, zero_area_rings
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
    validate_capture_request_identity,
)
from dagster_hifld.sources.arcgis_layer import (
    capture_witness, retained_capture_ids, _canonical, _capture_identity, _load_capture, _sha,
    _validate_spatial_reference, layer_capture_roster,
)

DATASET_SLUG = "usace-owned-reservoirs"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Army Corps of Engineers (USACE)"
# The DHS HIFLD Open crosswalk (August 2025) records the retired GeoPlatform-hosted
# HIFLD item e8abf1e31e2644458a9cd62145f9048c as Deprecate and names this
# USACE-hosted ArcGIS item and service as the agency's current open REST publication.
CATALOG_ITEM_ID = "03e322d7e89b48a9b48e9c3f4bcaf29e"
CATALOG_ITEM_OWNER = "usace_erd_aep"
CATALOG_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{CATALOG_ITEM_ID}?f=json"
LANDING_URL = f"https://geospatial-usace.opendata.arcgis.com/datasets/{CATALOG_ITEM_ID}_0/about"
SERVICE_URL = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/usace_rez/FeatureServer"
LAYER_URL = f"{SERVICE_URL}/0"
LAYER_ID = 0
LAYER_NAME = "USACE Reservoirs"
PAGE_SIZE = 100
EXPECTED_EXTENT = {"xmin": -123.35038402899994, "ymin": 25.761094021000076,
                   "xmax": -71.59594579999998, "ymax": 48.99956974400004}
# The advertised extent is rounded relative to the observed vertex bounds by a
# few ulps; the containment guard uses a tolerance far below any real change.
EXPECTED_EXTENT_TOLERANCE = 1e-9
EXPECTED_EDITING_INFO = {"lastEditDate": 1712070929726, "schemaLastEditDate": 1674050291606,
                         "dataLastEditDate": 1712070929726}
EXPECTED_LAST_DATA_EDIT_UTC = "2024-04-02T15:55:29.726000Z"
ITEM_DESCRIPTION = ("This dataset shows maximum conservation pool or is a reasonable "
                    "representation of the boundaries for reservoirs and lakes owned and "
                    "operated by USACE. Data is from USACE Districts.")
SERVICE_DESCRIPTION = "Boundary data for Corps owned and operated reservoirs.\n"
ITEM_LICENSE = ("This data set is publicly available without any use restrictions; however, "
                "this data set was developed for use at a national scale and may not be "
                "appropriate for use in local scale mapping.")
EXPECTED_GEOMETRY_PROPERTIES = {"shapeAreaFieldName": "Shape__Area",
                                "shapeLengthFieldName": "Shape__Length",
                                "units": "esriDecimalDegrees"}
# The live service computes Shape__Area with its own planar method; the observed
# parity against shapely's planar area peaks at 7.7e-8 relative across the whole
# population, so the parity guard sits just above that and far below real drift.
SHAPE_PARITY_TOLERANCE = 1e-6
_STRING_FIELDS = (("OMBIL_SITE_ID", 10), ("NAME", 50), ("DISTRICT", 50), ("DIST_SYM", 3),
                  ("DIVISION", 50), ("DIV_SYM", 3), ("DRY", 10), ("DAM_NAME", 50),
                  ("NIDID", 10))


def _string_field(name: str, length: int) -> dict:
    return {"name": name, "type": "esriFieldTypeString", "alias": name,
            "sqlType": "sqlTypeOther", "length": length, "nullable": True,
            "editable": True, "domain": None, "defaultValue": None}


EXPECTED_FIELD_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID",
     "sqlType": "sqlTypeOther", "nullable": False, "editable": False,
     "domain": None, "defaultValue": None},
    *[_string_field(name, length) for name, length in _STRING_FIELDS],
    {"name": "Shape__Area", "type": "esriFieldTypeDouble", "alias": "Shape__Area",
     "sqlType": "sqlTypeDouble", "nullable": True, "editable": False,
     "domain": None, "defaultValue": None},
    {"name": "Shape__Length", "type": "esriFieldTypeDouble", "alias": "Shape__Length",
     "sqlType": "sqlTypeDouble", "nullable": True, "editable": False,
     "domain": None, "defaultValue": None},
]
ATTRIBUTE_FIELDS = EXPECTED_FIELD_DECLARATIONS
EXPECTED_SCHEMA_SHA256 = "319daded87c7314f171a5229ff3fb163ddd17832f3bd96ac83fec3d9d9b57569"
# The per-value record and geometry baseline digests were computed from the
# complete dated population probe (2026-11-14, 418 features in one observed
# page) and pin the expected content for later drift refusal.
# Observed population facts recorded in the audit for provenance; the pinned
# record/geometry baselines above already refuse any content change.
OBSERVED_VERTEX_COUNT = 2_134_520
OBSERVED_WINDING_COUNTS = {"cw_shells": 3971, "ccw_holes": 8461}
# Geometry quarantines retired by the shared GDAL Esri polygon converter
# (issue #997): the 16 rows the retired hand-written assembler gated (6
# zero-area sliver rings, 9 self-intersecting exteriors, 1 assembly-invalid)
# all convert through GDAL and none is nulled. A live read of the 418-feature
# layer leaves 12 of them OGC-invalid, reported in ogc_invalid_geometry_counts;
# the rest assemble into OGC-valid geometry. Coordinates are never repaired.
EXPECTED_UNUSABLE_GEOMETRY_COUNTS = {}
_BASE_CAPTURE_NAMES = {"catalog_item", "service_before", "layer_before", "count_before",
                      "ids_before", "layer_after", "count_after", "ids_after", "service_after"}


def _capture_names(ids: list[int]) -> set[str]:
    return set(_BASE_CAPTURE_NAMES) | {
        f"page_{index:03d}" for index in range((len(ids) + PAGE_SIZE - 1) // PAGE_SIZE)}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


# The complete population decodes to about 68 MB across pages; the shared
# 8 MB page budget cannot hold a page, so this dataset carries its own limits.
PAGE_MAX_BYTES = 32_000_000
CAPTURE_MAX_BYTES = 256_000_000


def _capture(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * PAGE_MAX_BYTES > CAPTURE_MAX_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    body, receipt = capture_get(client, url, root, name, max_bytes=PAGE_MAX_BYTES)
    return body, receipt


def _capture_roster(ids: list[int]) -> list[tuple[str, str]]:
    return layer_capture_roster(
        ids, layer_url=LAYER_URL, service_url=SERVICE_URL, page_size=PAGE_SIZE,
        extra_documents=(("catalog_item", CATALOG_ITEM_URL),),
        page_params={"returnZ": "false", "returnM": "false", "outSR": "4326"},
    )


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    ids = retained_capture_ids(root)
    return validate_retained_capture_replay(
        root,
        capture_names=_capture_names(ids),
        expected_urls=dict(_capture_roster(ids)),
        load_capture=_load_capture,
        validate_request_identity=validate_capture_request_identity,
    )


def _validate_page_fields(page_fields: object, layer_fields: list[dict]) -> None:
    """Reject page-level physical schema drift before constructing a native table."""
    if not isinstance(page_fields, list) or len(page_fields) != len(layer_fields):
        raise ValueError("source_page_physical_schema_changed")
    for page_field, layer_field in zip(page_fields, layer_fields):
        if not isinstance(page_field, dict) or not isinstance(layer_field, dict):
            raise ValueError("source_page_physical_schema_changed")
        for key, value in page_field.items():
            if layer_field.get(key) != value:
                raise ValueError("source_page_physical_schema_changed")


def _validate_catalog_item(item: dict) -> None:
    if (item.get("id") != CATALOG_ITEM_ID or item.get("owner") != CATALOG_ITEM_OWNER
            or item.get("type") != "Feature Service" or item.get("title") != LAYER_NAME):
        raise ValueError("usace_catalog_item_identity_drift")
    if item.get("description") != ITEM_DESCRIPTION or item.get("licenseInfo") != ITEM_LICENSE:
        raise ValueError("usace_catalog_item_text_drift")
    if item.get("spatialReference") != "4326":
        raise ValueError("usace_catalog_item_crs_drift")
    extent = item.get("extent")
    if (not isinstance(extent, list) or len(extent) != 2
            or extent[0] != [EXPECTED_EXTENT["xmin"], EXPECTED_EXTENT["ymin"]]
            or extent[1] != [EXPECTED_EXTENT["xmax"], EXPECTED_EXTENT["ymax"]]):
        raise ValueError("usace_catalog_item_extent_drift")


def _validate_service(service: dict, label: str) -> None:
    if (service.get("serviceItemId") != CATALOG_ITEM_ID
            or service.get("description") != ITEM_DESCRIPTION
            or service.get("serviceDescription") != SERVICE_DESCRIPTION
            or service.get("capabilities") != "Query,Extract"
            or service.get("maxRecordCount") != 1000
            or service.get("hasVersionedData") is not False
            or service.get("tables") != []):
        raise ValueError(f"{label}_service_identity_drift")
    _validate_spatial_reference(service.get("spatialReference"), label + "_service")
    full = service.get("fullExtent")
    if (not isinstance(full, dict)
            or {key: full.get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError(f"{label}_service_extent_drift")
    _validate_spatial_reference(full.get("spatialReference"), label + "_service_full_extent")
    layers = service.get("layers")
    if (not isinstance(layers, list) or len(layers) != 1
            or layers[0].get("id") != LAYER_ID or layers[0].get("name") != LAYER_NAME
            or layers[0].get("parentLayerId") != -1 or layers[0].get("subLayerIds") is not None
            or layers[0].get("type") != "Feature Layer"
            or layers[0].get("geometryType") != "esriGeometryPolygon"):
        raise ValueError(f"{label}_service_layer_roster_drift")


def _validate_layer(layer: dict) -> None:
    if layer.get("fields") != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (_sha(_canonical(layer.get("fields"))) != EXPECTED_SCHEMA_SHA256
            or _sha(_canonical(layer.get("fields"))) != _sha(_canonical(EXPECTED_FIELD_DECLARATIONS))):
        raise ValueError("source_schema_baseline_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("geometryType") != "esriGeometryPolygon" or layer.get("maxRecordCount") != 1000
            or layer.get("objectIdField") != "OBJECTID" or layer.get("globalIdField") != ""
            or layer.get("typeIdField") != ""
            or layer.get("hasZ") is not False or layer.get("hasM") is not False
            or layer.get("editingInfo") != EXPECTED_EDITING_INFO
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT
            or layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True):
        raise ValueError("source_layer_identity_extent_or_freshness_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent_reference = layer.get("extent", {}).get("spatialReference")
    if extent_reference is not None:
        _validate_spatial_reference(extent_reference, "source_layer_extent")


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    ids_before = capture_witness(captures)
    if set(captures) != _capture_names(ids_before):
        raise ValueError("source_capture_name_roster_drift")
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    _validate_catalog_item(docs["catalog_item"])
    service = docs["service_before"]
    _validate_service(service, "service_before")
    layer = docs["layer_before"]
    _validate_layer(layer)
    if docs["service_after"] != service or docs["layer_after"] != layer:
        raise ValueError("source_service_or_layer_changed_during_capture")
    features = []
    vertices = 0
    winding = {"cw_shells": 0, "ccw_holes": 0}
    ogc_invalid_geometry: dict[str, int] = {}
    ogc_invalid_sample_objectids: list[int] = []
    null_geometry_count = 0
    geometry_valid = 0
    coverage = {key: EXPECTED_EXTENT[key] for key in ("xmin", "ymin", "xmax", "ymax")}
    for name in sorted(n for n in captures if n.startswith("page_")):
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_transfer_limit")
        if page.get("hasZ") or page.get("hasM"):
            raise ValueError("source_page_dimensional_drift")
        _validate_spatial_reference(page.get("spatialReference"), "source_" + name + "_page")
        if page.get("objectIdFieldName") != "OBJECTID" or page.get("geometryType") != "esriGeometryPolygon":
            raise ValueError("source_page_identity_drift")
        if page.get("geometryProperties") != EXPECTED_GEOMETRY_PROPERTIES:
            raise ValueError("source_page_geometry_properties_drift")
        _validate_page_fields(page.get("fields"), layer["fields"])
        expected_index = int(name[-3:])
        expected_ids = ids_before[expected_index * PAGE_SIZE:(expected_index + 1) * PAGE_SIZE]
        rows = page.get("features")
        if not isinstance(rows, list) or len(rows) != len(expected_ids):
            raise ValueError("source_page_population_incomplete:" + name)
        page_ids = []
        for feature in rows:
            attrs, geometry = feature.get("attributes"), feature.get("geometry")
            if (not isinstance(attrs, dict) or len(attrs) != len(ATTRIBUTE_FIELDS)
                    or tuple(attrs) != tuple(field["name"] for field in ATTRIBUTE_FIELDS)):
                raise ValueError("source_attribute_roster_or_order_drift")
            for field in ATTRIBUTE_FIELDS:
                value = attrs[field["name"]]
                if value is None:
                    continue
                kind = field["type"]
                if kind == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                    raise ValueError("source_oid_value_type_drift")
                if kind == "esriFieldTypeString" and not isinstance(value, str):
                    raise ValueError("source_string_value_type_drift:" + field["name"])
                if kind == "esriFieldTypeDouble" and (type(value) not in (int, float)
                                                      or not math.isfinite(float(value))):
                    raise ValueError("source_double_value_type_drift:" + field["name"])
            if not isinstance(geometry, dict) or set(geometry) - {"rings", "hasZ", "hasM", "spatialReference"}:
                raise ValueError("source_polygon_geometry_structure_drift")
            if "rings" not in geometry:
                raise ValueError("source_polygon_rings_missing")
            if geometry.get("hasZ") or geometry.get("hasM"):
                raise ValueError("source_geometry_dimensional_drift")
            if geometry.get("spatialReference") is not None:
                _validate_spatial_reference(geometry.get("spatialReference"),
                                            "source_" + name + "_feature")
            rings = geometry.get("rings")
            if not isinstance(rings, list) or not rings:
                raise ValueError("source_polygon_rings_missing")
            zero_area = frozenset(zero_area_rings(rings))
            for ring_index, ring in enumerate(rings):
                if not isinstance(ring, list) or len(ring) < 4:
                    raise ValueError("source_polygon_ring_invalid")
                if ring[0] != ring[-1]:
                    raise ValueError("source_polygon_ring_not_closed")
                for point in ring:
                    if (not isinstance(point, list) or len(point) != 2
                            or any(type(coord) not in (int, float) or not math.isfinite(coord) for coord in point)):
                        raise ValueError("source_xy_ordinate_contract_drift")
                    if not (coverage["xmin"] - EXPECTED_EXTENT_TOLERANCE <= point[0] <= coverage["xmax"] + EXPECTED_EXTENT_TOLERANCE
                            and coverage["ymin"] - EXPECTED_EXTENT_TOLERANCE <= point[1] <= coverage["ymax"] + EXPECTED_EXTENT_TOLERANCE):
                        raise ValueError("source_geometry_outside_declared_coverage")
                    vertices += 1
                if ring_index not in zero_area:
                    # Zero-area rings carry no orientation; the census counts
                    # winding only for winding-bearing rings, measured with
                    # shapely instead of a hand-written signed area.
                    if LinearRing([(point[0], point[1]) for point in ring]).is_ccw:
                        winding["ccw_holes"] += 1
                    else:
                        winding["cw_shells"] += 1
            oid = attrs.get("OBJECTID")
            if type(oid) is not int or oid not in expected_ids:
                raise ValueError("source_feature_id_roster_drift")
            page_ids.append(oid)
            # Ring orientation and hole assignment are GDAL's through the
            # shared native Esri polygon conversion; a conversion failure
            # refuses the capture, and OGC invalidity is a producer defect
            # retained verbatim with a counted reason, never repaired or
            # coerced to null geometry.
            wkb = to_wkb(geometry, "esriGeometryPolygon", has_z=False, has_m=False)
            decoded = from_wkb(wkb)
            reason = is_valid_reason(decoded)
            if reason != "Valid Geometry":
                ogc_invalid_geometry[reason] = ogc_invalid_geometry.get(reason, 0) + 1
                if len(ogc_invalid_sample_objectids) < 20:
                    ogc_invalid_sample_objectids.append(oid)
            else:
                # Shape parity is a contract about an OGC-valid assembly;
                # rows retained as OGC-invalid are counted above instead.
                for parity_field, observed in (("source_shape_area_parity_drift", attrs.get("Shape__Area")),
                                               ("source_shape_length_parity_drift", attrs.get("Shape__Length"))):
                    if observed is None:
                        continue
                    computed = decoded.area if parity_field.startswith("source_shape_area") else decoded.length
                    if abs(computed - observed) > SHAPE_PARITY_TOLERANCE * max(abs(observed), 1e-12):
                        raise ValueError(parity_field)
            geometry_valid += 1
            feature["attributes"] = {field["name"]: attrs[field["name"]] for field in ATTRIBUTE_FIELDS}
            features.append(feature)
        if sorted(page_ids) != expected_ids or len(set(page_ids)) != len(page_ids):
            raise ValueError("source_page_id_population_drift:" + name)
    ids = [feature["attributes"]["OBJECTID"] for feature in features]
    if sorted(ids) != ids_before or len(features) != len(ids_before) or len(set(ids)) != len(ids_before):
        raise ValueError("source_feature_id_population_drift")
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    record_sha = _sha(_canonical([row["attributes"] for row in features]))
    geometry_sha = _sha(_canonical([[row["attributes"]["OBJECTID"], row["geometry"]] for row in features]))
    bounds = [from_wkb(to_wkb(row["geometry"], "esriGeometryPolygon",
                              has_z=False, has_m=False)).bounds for row in features]
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(sorted(ids))),
        "source_id_min": min(ids), "source_id_max": max(ids),
        "source_schema_sha256": _sha(_canonical(layer["fields"])),
        "source_record_sha256": record_sha,
        "source_geometry_sha256": geometry_sha,
        "source_geometry_type": "esriGeometryPolygon", "source_wkid": 4326,
        "source_vertex_count": vertices,
        "source_ring_winding_counts": dict(sorted(winding.items())),
        "geometry_valid_count": geometry_valid,
        "null_geometry_count": null_geometry_count,
        "unusable_geometry_counts": {},
        "ogc_invalid_geometry_counts": dict(sorted(ogc_invalid_geometry.items())),
        "ogc_invalid_geometry_sample_objectids": ogc_invalid_sample_objectids,
        "geometry_bounds": [min(b[0] for b in bounds), min(b[1] for b in bounds),
                            max(b[2] for b in bounds), max(b[3] for b in bounds)],
        "source_extent": layer["extent"],
        "source_last_data_edit_utc": EXPECTED_LAST_DATA_EDIT_UTC,
        "source_coverage_scope": ("Complete observed USACE Reservoirs layer 0 from the "
                                  "USACE-hosted services7 FeatureServer named by the DHS HIFLD Open "
                                  "crosswalk as the agency's current REST publication; the layer "
                                  "holds 418 reservoir/lake boundary features from USACE Districts "
                                  "and is not asserted to be a worldwide water-body inventory."),
        "catalog_item_id": CATALOG_ITEM_ID, "catalog_item_owner": CATALOG_ITEM_OWNER,
        "catalog_item_url": CATALOG_ITEM_URL, "landing_page_url": LANDING_URL,
        "dhs_crosswalk_row": {"layer_name": "US Army Corps of Engineers (USACE) Owned and Operated Reservoirs",
                              "status": "Deprecate", "agency": "U.S. Army Corps of Engineers",
                              "old_hosting_item_id": "e8abf1e31e2644458a9cd62145f9048c",
                              "open_rest_service": LAYER_URL,
                              "external_landing_page": LANDING_URL,
                              "evidence_url": "https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx"},
        "drift_guard": ("Current USACE catalog item identity (id, owner, title, description, "
                        "license text, CRS and extent), service identity roster, exact field "
                        "declarations/types/nullability, editingInfo freshness pins, explicit "
                        "EPSG:4326 layer/page/feature SRs, XY-only dimensional flags, count and "
                        "before/after ID roster, per-page IDs/attributes/transfer limits, ring "
                        "closure/winding/hole ownership, declared-extent containment, pinned "
                        "record/geometry/ID/schema baselines and null-geometry and gate counts, "
                        "and Shape__Area/Shape__Length parity are checked."),
    }
    return layer["fields"], features, audit


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeString": pa.string(),
            "esriFieldTypeDouble": pa.float64()}[field["type"]]


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    arrays = [pa.array([row["attributes"][field["name"]] for row in features], type=_arrow_type(field))
              for field in fields]
    geometries = [to_wkb(row["geometry"], "esriGeometryPolygon", has_z=False, has_m=False)
                  for row in features]
    arrays.append(pa.array(geometries, type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=[field["name"] for field in fields] + ["geometry"])
    crs = {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS", "name": "WGS 84",
           "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
                     "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
                     "id": {"authority": "EPSG", "code": 6326}},
           "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
                                                                    {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
           "id": {"authority": "EPSG", "code": 4326}}
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
                                   "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["MultiPolygon", "Polygon"], "crs": crs}}})}
    import io
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    return _sha(_canonical(_capture_identity(captures, root)))


def capture_usace_owned_reservoirs(storage: StagingStorageResource, *, version: str,
                                   capture_directory: str | None = None) -> dict[str, object]:
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
    if shutil.disk_usage(staging).free < 400_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        captures = _read_replay(root)
    else:
        if requested and any(requested.glob("*.body")):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root = ((requested / version / uuid4().hex) if requested else
                default_capture_dir("hifld-native-reconstruction", "358", "captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/usace-owned-reservoirs-draft"}) as client:
                def validate_seed(ids_body):
                    ids = json.loads(ids_body).get("objectIds")
                    if (not isinstance(ids, list) or not ids
                            or any(type(oid) is not int or oid <= 0 for oid in ids)
                            or len(set(ids)) != len(ids)):
                        raise ValueError("source_seed_id_population_drift")
                    return sorted(ids)
                captures = RetainedCaptureView(root, captures)
                capture_layer(LAYER_URL, root, 192_000_000, PAGE_SIZE, client=client,
                    captures=captures, seed_name='ids_before', seed_url=_query({'where': '1=1', 'returnIdsOnly': 'true', 'f': 'json'}),
                    validate_seed=validate_seed, roster=_capture_roster,
                    fetch=lambda client, url, root, name: _capture(client, url, root, name),
                    on_validation_error=_validate)
            fields, features, audit = _validate(captures)
        except Exception as exc:
            receipts = {}
            for path in sorted(root.glob("*.receipt.json")):
                try:
                    receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    receipts[path.name] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({"complete": False,
                "captured_at": datetime.now(timezone.utc).isoformat(), "error_code": type(exc).__name__,
                "receipts": receipts}) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
            raise
        (root / "capture_manifest.json").write_bytes(_canonical({"complete": True,
            "captured_at": datetime.now(timezone.utc).isoformat(), "source_feature_count": audit["source_feature_count"],
            "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
            "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        captures = _read_replay(root)

    fields, features, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    capture_hash = _capture_identity_hash(captures, root)
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_capture_evidence": _capture_identity(captures, root),
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    parquet = _parquet_bytes(fields, features)
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    gate_note = ("no row gates to null geometry: served rings convert through the shared "
                 "GDAL Esri polygon converter, and OGC-invalid converted geometry is "
                 "retained verbatim and counted by reason as "
                 + json.dumps(audit["ogc_invalid_geometry_counts"], sort_keys=True))
    inputs = [{"input_id": name, "producer": PUBLISHER,
        "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": CATALOG_ITEM_URL if name == "catalog_item" else LAYER_URL, "retrieval_url": receipt["url"],
        "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": ("The service editingInfo.dataLastEditDate ("
                                        + EXPECTED_LAST_DATA_EDIT_UTC + ") is a service edit timestamp, "
                                        "not a producer release date; no release-date field exists on the layer."),
        "license": "USACE ArcGIS item licenseInfo states public availability without use restrictions; formal rights acceptance remains pending.",
        "attribution": "U.S. Army Corps of Engineers",
        **({"delegation_evidence": {
            "dhs_crosswalk_status": "Deprecate",
            "dhs_crosswalk_layer_name": "US Army Corps of Engineers (USACE) Owned and Operated Reservoirs",
            "old_hosting_item_id": "e8abf1e31e2644458a9cd62145f9048c",
            "old_hosting_location": "https://geoplatform.maps.arcgis.com/home/item.html?id=e8abf1e31e2644458a9cd62145f9048c",
            "landing_page_url": LANDING_URL,
            "catalog_item_owner": CATALOG_ITEM_OWNER,
            "crosswalk_evidence_url": "https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx",
            "basis": "The DHS HIFLD Open crosswalk (August 2025) names this USACE-owned item and service as the agency's current open REST publication for the retired GeoPlatform-hosted HIFLD layer; the captured service response, not the item page, supplies the data."}}
           if name == "catalog_item" else {})}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "USACE Reservoirs",
        "description": ("Complete observed USACE Reservoirs layer 0 from the USACE-published "
                        "services7 FeatureServer; DHS crosswalk succession, release acceptance and "
                        "rights review remain unaccepted."),
        "publisher": PUBLISHER, "agency": "U.S. Army Corps of Engineers", "source_url": LAYER_URL,
        "license": None, "source_modified": EXPECTED_LAST_DATA_EDIT_UTC, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Esri XY Polygon to GeoParquet WKB candidate", "parameters": {
            "source_fields": "all 12 declared attributes retained in source order and declared types",
            "source_ids": "OBJECTID retained unchanged (served roster is 1..419 without deleted OBJECTID 10)",
            "source_crs": "EPSG:4326",
            "geometry": ("2D ring order, winding and hole ownership preserved; no reprojection, "
                         "repair, or simplification; multi-owned holes attach to the smallest "
                         "containing exterior per the merged USGS 3DHP catchment precedent and "
                         "rows that still cannot form a valid OGC polygon are counted by "
                         "failure mode; " + gate_note),
            "raw_evidence": "complete decoded response and wire bodies retained with checksummed receipts",
            "candidate_status": "unapproved; baseline/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": ("DHS HIFLD Open crosswalk (August 2025) row 'US Army Corps of Engineers (USACE) "
                    "Owned and Operated Reservoirs' marks the retired GeoPlatform-hosted item "
                    "e8abf1e31e2644458a9cd62145f9048c as Deprecate and names the USACE OpenData "
                    "landing page " + LANDING_URL + " and the USACE-hosted services7 FeatureServer "
                    "layer as the agency's current publication. The captured ArcGIS item is owned by "
                    "the USACE account usace_erd_aep. The old GeoPlatform item answers CONT_0001 "
                    "(item does not exist). No third-party mirror or archive is used; DataLumos "
                    "copies are research context only."),
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Native attributes, IDs and 2D rings are retained in a typed candidate.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest); provenance_projection(manifest)
    audit.update({"source_fields": fields, "official_layer_name": LAYER_NAME, "official_layer_id": LAYER_ID,
                  "official_source_item": CATALOG_ITEM_OWNER + " " + CATALOG_ITEM_ID,
                  "source_population_complete": True})
    outputs = {output_path: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n", "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in sorted(captures.items()):
        _body, wire, _receipt = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body; outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version,
        "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_release_accepted": False,
        "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    fingerprint = _sha(_canonical({"identity": identity, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_capture_sha256": capture_hash}))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(storage, state_key, version, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("capture_receipts"), dict):
            raise ValueError("cached_first_candidate_receipts_missing")
        original_receipts = original["capture_receipts"]
        if set(original_receipts) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name in captures:
            body, wire, _receipt = _load_capture(root, name)
            if (storage.read_key(f"{prefix}/raw/{name}.body") != body
                    or storage.read_key(f"{prefix}/raw/{name}.wire") != wire):
                raise ValueError("cached_source_evidence_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
            identity, audit["source_record_sha256"], audit["source_schema_sha256"],
            original.get("objects", {}), original_receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["source_feature_count"],
            "source_reported_count": audit["source_feature_count"],
            "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"], "source_id_max": audit["source_id_max"],
            "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_capture_evidence_sha256": capture_hash, "null_geometry_count": audit["null_geometry_count"],
            "unusable_geometry_counts": audit["unusable_geometry_counts"],
            "geometry_valid_count": audit["geometry_valid_count"],
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
    descriptors = {}
    for name, body in sorted(outputs.items()):
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"]:
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    seal_ready_candidate(storage, state_key, version, fingerprint, {"processing_identity": identity,
        "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": receipts, "objects": descriptors})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["source_feature_count"],
            "source_reported_count": audit["source_feature_count"],
            "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"], "source_id_max": audit["source_id_max"],
            "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_capture_evidence_sha256": capture_hash, "null_geometry_count": audit["null_geometry_count"],
            "unusable_geometry_counts": audit["unusable_geometry_counts"],
            "geometry_valid_count": audit["geometry_valid_count"],
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
