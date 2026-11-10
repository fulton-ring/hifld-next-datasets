"""Complete official USACE IENC DATA_COVERAGE_AREA layer draft.

The retained Esri responses are inputs as observed. This module preserves every
OBJECTID, attribute, native coordinate and receipt; it does not claim historical
HIFLD equivalence, a dated source release, rights, or publication acceptance.
"""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import struct
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from pyproj import CRS
from shapely.geometry import LinearRing, Polygon, mapping

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    capture_get,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "data-coverage-areas-usace-ienc"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/DATA_COVERAGE_AREA/MapServer"
LAYER_ID = 63
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
AGC_PRODUCT_PAGE = "https://www.agc.army.mil/What-we-do/Inland-Electronic-Navigation-Charts-IENC/Product-Downloads/"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; official IENC Feature Classes service"
EXPECTED_COUNT = 317
EXPECTED_IDS = list(range(1, EXPECTED_COUNT + 1))
PAGE_MAX_BYTES = 50_000_000
CAPTURE_MAX_BYTES = 500_000_000
SOURCE_CRS = CRS.from_epsg(4326)
EXPECTED_LAYER_EXTENT = {
    "xmin": -96.73799799999995,
    "ymin": 29.610281000000043,
    "xmax": -79.46555599999994,
    "ymax": 45.14219500000007,
}
EXPECTED_FIELD_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID"},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE"},
    *[
        {"name": name, "type": "esriFieldTypeString", "alias": name, "length": 254}
        for name in (
            "Category_of_Data_Coverage", "Source_Date", "Source_Indication",
            "Source_Dataset", "Feature_ID",
        )
    ],
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELD_DECLARATIONS if field["name"] != "SHAPE"]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _validate_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            if type(value[key]) is not int or value[key] != 4326:
                raise ValueError(f"{label}_crs_drift")
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            if not isinstance(value[key], str) or not CRS.from_wkt(value[key]).equals(
                SOURCE_CRS, ignore_axis_order=True
            ):
                raise ValueError(f"{label}_crs_drift")
            found = True
    if not found:
        raise ValueError(f"{label}_crs_missing")


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_urls(ids: list[int], max_record_count: int) -> list[tuple[str, str]]:
    urls = [
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]
    if len(ids) <= max_record_count:
        urls.append(("features", _query({
            "where": "1=1", "outFields": "*", "returnGeometry": "true",
            "returnTrueCurves": "true", "orderByFields": "OBJECTID ASC", "f": "json",
        })))
    else:
        for index, start in enumerate(range(0, len(ids), max_record_count)):
            page_ids = ids[start:start + max_record_count]
            urls.append((f"page_{index:03d}", _query({
                "objectIds": ",".join(map(str, page_ids)), "outFields": "*",
                "returnGeometry": "true", "returnTrueCurves": "true",
                "orderByFields": "OBJECTID ASC", "f": "json",
            })))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
        ("service_after", SERVICE_URL + "?f=pjson"),
    ])
    return urls


def _esri_polygon(geometry: dict) -> bytes:
    """Encode ArcGIS Polygon rings as WKB without reprojection or repair."""
    rings = geometry.get("rings")
    has_z, has_m = geometry.get("hasZ", False), geometry.get("hasM", False)
    if not isinstance(rings, list) or not rings or type(has_z) is not bool or type(has_m) is not bool:
        raise ValueError("source_polygon_rings_missing_or_ordinate_flags_invalid")
    shells, holes = [], []
    dimension = None
    for coords in rings:
        if not isinstance(coords, list) or len(coords) < 4 or coords[0] != coords[-1]:
            raise ValueError("source_polygon_ring_invalid_or_open")
        if any(not isinstance(point, list) for point in coords):
            raise ValueError("source_polygon_coordinate_invalid")
        dimensions = {len(point) for point in coords}
        if len(dimensions) != 1:
            raise ValueError("source_polygon_ordinate_count_drift")
        point_dimension = next(iter(dimensions))
        if dimension is None:
            dimension = point_dimension
        if dimension != point_dimension or point_dimension != 2 + int(has_z) + int(has_m):
            raise ValueError("source_polygon_ordinate_count_drift")
        xy = [point[:2] for point in coords]
        (holes if LinearRing(xy).is_ccw else shells).append([list(point) for point in coords])
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    shell_polygons = [Polygon([p[:2] for p in ring]) for ring in shells]
    assigned = [[] for _ in shell_polygons]
    for ring in holes:
        hole = Polygon([p[:2] for p in ring])
        owners = [i for i, shell in enumerate(shell_polygons)
                  if shell.contains(hole.representative_point())]
        if len(owners) != 1:
            raise ValueError("source_hole_ownership_ambiguous")
        assigned[owners[0]].append(ring)
    dimensions = dimension or 2
    dimension_code = {2: 0, 3: (1000 if has_z else 2000), 4: 3000}.get(dimensions)
    if dimension_code is None:
        raise ValueError("source_polygon_dimension_unsupported")

    def polygon(shell, inner):
        value = bytearray(struct.pack("<BI", 1, dimension_code + 3))
        value.extend(struct.pack("<I", 1 + len(inner)))
        for ring in [shell, *inner]:
            value.extend(struct.pack("<I", len(ring)))
            for point in ring:
                value.extend(struct.pack("<" + "d" * dimensions, *point))
        return bytes(value)

    polys = [polygon(shell, assigned[index]) for index, shell in enumerate(shells)]
    if len(polys) == 1:
        return polys[0]
    output = bytearray(struct.pack("<BI", 1, dimension_code + 6))
    output.extend(struct.pack("<I", len(polys)))
    for value in polys:
        output.extend(value)
    return bytes(output)


def _arrow_type(field: dict):
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeSingle": pa.float32(),
            "esriFieldTypeDouble": pa.float64(), "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64()}.get(field.get("type"), pa.string())


def _parquet(fields: list[dict], features: list[dict], geometry_types: list[str]) -> bytes:
    attributes = [field for field in fields if field["name"] != "SHAPE"]
    arrays = [pa.array([feature["attributes"][field["name"]] for feature in features],
                       type=_arrow_type(field)) for field in attributes]
    arrays.append(pa.array([_esri_polygon(feature["geometry"]) for feature in features],
                           type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=[field["name"] for field in attributes] + ["geometry"])
    crs = SOURCE_CRS.to_json_dict()
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {
        "geometry": {"encoding": "WKB", "geometry_types": geometry_types, "crs": crs}}}
    sink = __import__("io").BytesIO()
    pq.write_table(table.replace_schema_metadata({b"geo": _canonical(geo)}), sink,
                   compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _geojson(fields: list[dict], features: list[dict]) -> bytes:
    output = []
    for feature in features:
        geometry = shapely.from_wkb(_esri_polygon(feature["geometry"]))
        output.append({"type": "Feature", "id": feature["attributes"]["OBJECTID"],
                       "properties": {field["name"]: feature["attributes"][field["name"]]
                                      for field in fields if field["name"] != "SHAPE"},
                       "geometry": mapping(geometry)})
    return _canonical({"type": "FeatureCollection", "name": DATASET_SLUG,
                       "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
                       "features": output}) + b"\n"


def _capture(client: httpx.Client, url: str, root: Path, name: str,
             budget: list[int]) -> tuple[bytes, dict]:
    if budget[0] + 2 * PAGE_MAX_BYTES > CAPTURE_MAX_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    body, receipt = capture_get(client, url, root, name, max_bytes=PAGE_MAX_BYTES)
    budget[0] += receipt["bytes"] + receipt["wire_bytes"]
    return body, receipt


def _load_replay(root: Path, name: str) -> tuple[bytes, dict]:
    body = (root / f"{name}.body").read_bytes()
    wire = (root / f"{name}.wire").read_bytes()
    receipt = json.loads((root / f"{name}.receipt.json").read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("final_url") != receipt.get("url")
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire)
            or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("retained_capture_receipt_or_bytes_invalid:" + name)
    return body, receipt


def _validate(captures: dict[str, tuple[bytes, dict]]):
    required = {"service_before", "layer_before", "count_before", "ids_before",
                "layer_after", "service_after", "count_after", "ids_after"}
    if not required <= set(captures):
        raise ValueError("source_capture_required_responses_missing")
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    service, layer = docs["service_before"], docs["layer_before"]
    if docs["service_after"] != service or docs["layer_after"] != layer:
        raise ValueError("source_metadata_changed_during_capture")
    if (service.get("mapName") != "DATA_COVERAGE_AREA"
            or service.get("copyrightText") != "USACE IENC"
            or [(item.get("id"), item.get("name"), item.get("type"))
                for item in service.get("layers", [])] != [(LAYER_ID, "DATA_COVERAGE_AREA", "Feature Layer")]):
        raise ValueError("official_source_service_identity_drift")
    projected = [{key: field[key] for key in ("name", "type", "alias", "length")
                  if key in field} for field in layer.get("fields", [])]
    if projected != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != "DATA_COVERAGE_AREA"
            or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("copyrightText") != "USACE IENC"):
        raise ValueError("source_layer_identity_or_geometry_drift")
    if type(layer.get("maxRecordCount")) is not int or layer["maxRecordCount"] < 1:
        raise ValueError("source_page_limit_missing")
    if {key: layer.get("extent", {}).get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise ValueError("source_coverage_extent_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    _validate_spatial_reference(layer.get("extent", {}).get("spatialReference"), "source_extent")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    before, after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (type(count_before) is not int or count_before != EXPECTED_COUNT
            or count_after != count_before or not isinstance(before, list) or before != after
            or len(before) != count_before or len(set(before)) != count_before
            or sorted(before) != EXPECTED_IDS):
        raise ValueError("source_count_or_full_id_roster_drift")
    if docs["ids_before"].get("objectIdFieldName") != "OBJECTID":
        raise ValueError("source_objectid_field_drift")
    page_names = sorted(name for name in captures if name == "features" or name.startswith("page_"))
    expected_pages = (len(before) + layer["maxRecordCount"] - 1) // layer["maxRecordCount"]
    if len(page_names) != expected_pages:
        raise ValueError("source_page_roster_incomplete")
    features = []
    for name in page_names:
        page = docs[name]
        if page.get("error") is not None or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_transfer_limit")
        if page.get("geometryType") != "esriGeometryPolygon":
            raise ValueError("source_page_geometry_type_drift")
        _validate_spatial_reference(page.get("spatialReference"), "source_page")
        page_fields = [{key: field[key] for key in ("name", "type", "alias", "length")
                        if key in field} for field in page.get("fields", [])]
        if page_fields != [field for field in EXPECTED_FIELD_DECLARATIONS if field["name"] != "SHAPE"]:
            raise ValueError("source_query_field_schema_drift")
        rows = page.get("features")
        if not isinstance(rows, list):
            raise ValueError("source_page_features_missing")
        features.extend(rows)
    if len(features) != count_before:
        raise ValueError("source_feature_count_differs_from_ids")
    attribute_names = [field["name"] for field in ATTRIBUTE_FIELDS]
    ids = []
    categories = {}
    geometry_types = set()
    vertex_count = 0
    valid_count = 0
    for feature in features:
        attrs, geometry = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or list(attrs) != attribute_names:
            raise ValueError("source_record_attribute_schema_drift")
        if not isinstance(geometry, dict) or set(geometry) - {"rings", "hasZ", "hasM", "spatialReference"}:
            raise ValueError("source_native_polygon_geometry_shape_drift")
        if "rings" not in geometry:
            raise ValueError("source_polygon_rings_missing")
        for field in ATTRIBUTE_FIELDS:
            value = attrs.get(field["name"])
            if value is None:
                raise ValueError("source_null_requires_review:" + field["name"])
            if field["type"] == "esriFieldTypeOID":
                if type(value) is not int or not 1 <= value <= 2**63 - 1:
                    raise ValueError("source_objectid_type_drift")
            elif not isinstance(value, str) or len(value) > field.get("length", 2**31):
                raise ValueError("source_string_value_or_width_drift:" + field["name"])
        oid = attrs["OBJECTID"]
        ids.append(oid)
        category = attrs["Category_of_Data_Coverage"]
        categories[category] = categories.get(category, 0) + 1
        for ring in geometry["rings"]:
            for point in ring:
                if (not isinstance(point, list) or len(point) != 2
                        or any(type(value) not in (int, float) or not math.isfinite(value) for value in point)):
                    raise ValueError("source_polygon_coordinate_contract_drift")
                if not (EXPECTED_LAYER_EXTENT["xmin"] <= point[0] <= EXPECTED_LAYER_EXTENT["xmax"]
                        and EXPECTED_LAYER_EXTENT["ymin"] <= point[1] <= EXPECTED_LAYER_EXTENT["ymax"]):
                    raise ValueError("source_geometry_outside_native_extent")
                vertex_count += 1
        geom = shapely.from_wkb(_esri_polygon(geometry))
        geometry_types.add(geom.geom_type)
        if geom.is_valid and not geom.is_empty:
            valid_count += 1
    if sorted(ids) != sorted(before) or len(set(ids)) != count_before:
        raise ValueError("source_feature_ids_do_not_match_complete_roster")
    features.sort(key=lambda feature: feature["attributes"]["OBJECTID"])
    expected_urls = dict(_capture_urls(before, layer["maxRecordCount"]))
    if set(captures) != set(expected_urls):
        raise ValueError("source_capture_endpoint_roster_drift")
    for name, (_body, receipt) in captures.items():
        if (receipt.get("method") != "GET" or receipt.get("status") != 200
                or receipt.get("complete_body") is not True
                or receipt.get("url") != expected_urls[name]
                or receipt.get("final_url") != expected_urls[name]):
            raise ValueError("source_capture_receipt_url_or_status_invalid:" + name)
    record_sha = _sha(_canonical([feature["attributes"] for feature in features]))
    geometry_sha = _sha(_canonical([[feature["attributes"]["OBJECTID"], feature["geometry"]]
                                    for feature in features]))
    schema_sha = _sha(_canonical(layer["fields"]))
    id_sha = _sha(_canonical(ids))
    audit = {
        "approved": False, "source_population_complete": True,
        "source_feature_count": len(features), "source_id_count": len(ids),
        "source_id_min": min(ids), "source_id_max": max(ids), "source_id_sha256": id_sha,
        "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": schema_sha, "source_geometry_type": "esriGeometryPolygon",
        "source_crs": "EPSG:4326", "source_has_z": bool(layer.get("hasZ", False)),
        "source_has_m": bool(layer.get("hasM", False)), "source_vertex_count": vertex_count,
        "geometry_valid_count": valid_count, "geometry_invalid_count": len(features) - valid_count,
        "geometry_types": sorted(geometry_types), "category_counts": dict(sorted(categories.items())),
        "source_coverage_scope": "Complete observed current official USACE IENC DATA_COVERAGE_AREA layer 63. This layer contains 317 coverage/no-coverage polygons in the inland-chart extent; no claim of equivalence to the archived HIFLD inventory, the full IENC master geodatabase, or nationwide coverage beyond this layer.",
        "source_release_date": None,
        "source_release_date_unknown_reason": "The layer metadata and feature Source_Date attributes do not declare a dataset release date; they are preserved as source values.",
        "drift_guard": "Pinned layer/service identity, schema, geometry type, EPSG:4326, page limit, native extent, complete 317-OBJECTID roster, transfer-limit marker, and stable before/after counts and IDs are required. Any change quarantines pending review.",
    }
    return layer["fields"], features, audit


def _write_outputs(storage: StagingStorageResource, prefix: str,
                   outputs: dict[str, bytes]) -> dict[str, dict[str, object]]:
    descriptors = {}
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
        snapshot = storage.object_snapshot(f"{prefix}/{name}")
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if (snapshot is None or snapshot.size != len(body)
                or storage.sha256_key(f"{prefix}/{name}") != descriptor["sha256"]
                or storage.object_snapshot(f"{prefix}/{name}") != snapshot):
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    return descriptors


def _replay_capture(root: Path):
    manifest = json.loads((root / "capture-manifest.json").read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_incomplete")
    roster = manifest.get("artifact_roster")
    if not isinstance(roster, dict) or not roster:
        raise ValueError("retained_capture_artifact_roster_missing")
    validate_capture_roster(root, set(roster) | {"capture-manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture-manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    captures = {name: _load_replay(root, name) for name in manifest["receipts"]}
    if set(captures) != set(roster_name.removesuffix(".body") for roster_name in roster
                             if roster_name.endswith(".body")):
        raise ValueError("retained_capture_receipt_roster_mismatch")
    for name, (_body, receipt) in captures.items():
        if receipt != manifest["receipts"][name]:
            raise ValueError("retained_capture_receipt_manifest_mismatch:" + name)
    return captures, manifest


def _live_capture(root: Path, storage: StagingStorageResource):
    captures = {}
    failures = {}
    budget = [0]
    urls = [
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(60, connect=15),
                          headers={"User-Agent": "hifld-next-datasets/usace-ienc-draft"}) as client:
            for name, url in urls:
                captures[name] = _capture(client, url, root, name, budget)
            ids = json.loads(captures["ids_before"][0]).get("objectIds")
            layer = json.loads(captures["layer_before"][0])
            if not isinstance(ids, list) or type(layer.get("maxRecordCount")) is not int:
                raise ValueError("source_id_roster_or_page_limit_invalid")
            urls = _capture_urls(ids, layer["maxRecordCount"])
            for name, url in urls[4:]:
                captures[name] = _capture(client, url, root, name, budget)
    except Exception as exc:
        for path in sorted(root.glob("*.receipt.json")):
            try:
                failures[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (OSError, ValueError):
                failures[path.name] = {"receipt_readback_failed": True}
        (root / "capture-manifest.json").write_bytes(_canonical({
            "schema_version": 1, "complete": False,
            "error_type": type(exc).__name__, "receipts": failures,
            "artifact_roster": capture_roster_digests(root),
        }) + b"\n")
        if captures:
            quarantine_incomplete_capture(storage, capture_root=root, dataset_slug=DATASET_SLUG,
                                          version="unassigned", captures=captures,
                                          reason_code=type(exc).__name__)
        raise
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    (root / "capture-manifest.json").write_bytes(_canonical({
        "schema_version": 1, "complete": True, "source_url": LAYER_URL,
        "capture_scope": "bounded official REST capture; before/after service, layer, count and full object ID roster; all feature pages retained; per-response observed timestamps preserved in receipts",
        "receipts": receipts,
        "artifact_roster": capture_roster_digests(root, exclude={"capture-manifest.json"}),
    }) + b"\n")
    return captures


def capture_usace_data_coverage_areas(
    storage: StagingStorageResource, *, capture_directory: str | None = None,
) -> dict[str, object]:
    """Replay a retained complete capture or acquire one bounded local draft."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(path.is_symlink() for path in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 300_000_000:
        raise ValueError("disk_floor_before_source_materialization")
    requested = Path(capture_directory).absolute() if capture_directory else None
    if requested and (requested / "capture-manifest.json").exists():
        root = requested
        captures, capture_manifest = _replay_capture(root)
        replay = True
    else:
        if requested is not None:
            if any(requested.glob("*.body")) or any(requested.glob("*.wire")) or any(requested.glob("*.receipt.json")):
                raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
            root = requested / ("attempt-" + uuid4().hex)
        else:
            root = Path("/tmp/hifld-native-reconstruction/51") / ("attempt-" + uuid4().hex)
        root.mkdir(parents=True, exist_ok=True)
        captures = _live_capture(root, storage)
        replay = False
        capture_manifest = {}
    try:
        fields, features, audit = _validate(captures)
    except Exception as exc:
        if not replay:
            quarantine_incomplete_capture(
                storage, capture_root=root, dataset_slug=DATASET_SLUG,
                version="unassigned", captures=captures,
                reason_code="source_validation_" + type(exc).__name__,
            )
        raise
    identity = processing_identity(Path(__file__).parents[3])
    geometry_types = audit["geometry_types"]
    parquet = _parquet(fields, features, geometry_types)
    geojson = _geojson(fields, features)
    capture_manifest_bytes = (root / "capture-manifest.json").read_bytes()
    capture_sha = _sha(capture_manifest_bytes)
    audit["source_capture_sha256"] = capture_sha
    version_hash = _sha(_canonical({
        "source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_sha,
        "processing_identity": identity,
    }))
    version = "v" + version_hash[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    inputs = []
    for name, (_body, receipt) in sorted(captures.items()):
        inputs.append({
            "input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
            "role": "producer_publication", "canonical_url": LAYER_URL,
            "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
            "retrieved_at": receipt["observed_at"], "release_date": None,
            "release_date_unknown_reason": "The live service response and Source_Date feature values do not declare a dataset release date.",
            "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
            "license": "Rights review pending.", "attribution": "USACE IENC; rights review pending.",
            "authority_evidence": AGC_PRODUCT_PAGE,
        })
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Data Coverage Areas (USACE IENC) — working draft",
        "description": audit["source_coverage_scope"],
        "publisher": PUBLISHER, "agency": "US Army Corps of Engineers, Army Geospatial Center",
        "source_url": LAYER_URL, "license": "Rights review pending.",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{
            "operation": "native_esri_polygon_json_to_geojson_and_geoparquet",
            "parameters": {
                "source_attributes": "all source-declared attributes retained in source order and native logical types",
                "source_identifier": "OBJECTID values retained unchanged; Feature_ID attribute retained unchanged",
                "geometry": "all ArcGIS rings retained as EPSG:4326 polygonal WKB/GeoJSON; no reprojection, simplification or repair",
                "acceptance": "source, historical HIFLD baseline, rights, coverage, ID/schema mapping and release remain unapproved",
            },
        }],
        "lineage": "USACE IENC Feature Classes service DATA_COVERAGE_AREA MapServer layer 63; the AGC Product Downloads page identifies the official IENC product channel. The exact archived HIFLD inventory package and its relationship to this layer were not recovered or asserted.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Lossless typed draft of the captured native fields, OBJECTID values and EPSG:4326 polygon coordinates; acceptance remains external.",
            "objects": [{"path": f"geoparquet/{FILE_SLUG}.parquet", "bytes": len(parquet), "sha256": _sha(parquet)}],
        }}},
        "unsupported_output_formats": {
            "shapefile_zip": "No approved target schema, field mapping, identifier contract or compatibility baseline.",
            "geopackage": "No approved target schema, field mapping, identifier contract or compatibility baseline.",
            "pmtiles": "No display transformation or release format approved.",
        },
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit.update({
        "source_fields": fields, "official_service_url": SERVICE_URL, "official_layer_id": LAYER_ID,
        "source_release_accepted": False, "source_freshness_accepted": False,
        "baseline_comparison_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "historical_hifld_equivalence": "not_evaluated",
    })
    outputs = {
        f"geojson/{FILE_SLUG}.geojson": geojson,
        f"geoparquet/{FILE_SLUG}.parquet": parquet,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/proposed_source_contract.json": _canonical({
            "approved": False, "id_fields": ["OBJECTID", "Feature_ID"],
            "geometry": True, "crs": "EPSG:4326", "columns": {
                field["name"]: {"source_type": field["type"], "length": field.get("length"),
                                "nullable": None, "accepted": False}
                for field in ATTRIBUTE_FIELDS
            }, "coverage_field": "Category_of_Data_Coverage",
            "status": "proposed; historical HIFLD mapping and rights pending",
        }) + b"\n",
        "metadata/baseline_comparison.json": _canonical({
            "status": "blocked", "approved": False,
            "reason_codes": ["historical_hifld_baseline_not_recovered", "schema_id_geometry_mapping_unapproved"],
            "acceptance": "not_established",
        }) + b"\n",
        "raw/capture-manifest.json": capture_manifest_bytes,
    }
    for name, (body, receipt) in sorted(captures.items()):
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {
        "approved": False, "version": version,
        "source_population_complete": True,
        "source_count": EXPECTED_COUNT, "source_id_sha256": audit["source_id_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"],
        "source_capture_sha256": capture_sha,
        "quality_check_passed": None,
        "source_release_accepted": False, "source_freshness_accepted": False,
        "baseline_comparison_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "objects": {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()},
    }
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint = _sha(_canonical(identity))
    receipts = {name: receipt for name, (_body, receipt) in captures.items()}
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("capture_receipts"), dict):
            raise ValueError("cached_candidate_original_receipts_missing")
        if set(payload["capture_receipts"]) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name, (body, _receipt) in captures.items():
            if storage.read_key(f"{prefix}/raw/{name}.body") != body:
                raise ValueError("cached_source_bytes_changed:" + name)
        verify_cached_candidate(
            storage, Path(__file__).parents[3], state_key, prefix, version,
            fingerprint, identity, audit["source_record_sha256"],
            audit["source_schema_sha256"], payload.get("objects", {}),
            payload["capture_receipts"],
        )
        return {
            "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["source_feature_count"], "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_capture_sha256": capture_sha, "source_coverage_scope": audit["source_coverage_scope"],
            "drift_guard": audit["drift_guard"], "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "reused": True, "release_accepted": False, "capture_directory": str(root),
        }
    descriptors = _write_outputs(storage, prefix, outputs)
    seal_ready_candidate(storage, state_key, version, fingerprint, {
        "processing_identity": identity, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": receipts, "objects": descriptors,
    })
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
    )
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["source_feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_sha, "source_coverage_scope": audit["source_coverage_scope"],
        "drift_guard": audit["drift_guard"], "candidate_ready": prefix + "/metadata/candidate_ready.json",
        "reused": False, "release_accepted": False, "capture_directory": str(root),
    }
