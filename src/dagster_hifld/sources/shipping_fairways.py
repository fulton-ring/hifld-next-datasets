"""Bounded, source-faithful capture for the NOAA OCS ENCDirect shipping fairways layer."""
from __future__ import annotations

import json
import math
import shutil
import struct
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb
from shapely.geometry import LinearRing, Polygon

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
from dagster_hifld.sources.usace_obstruction_lines import (
    _canonical, _capture_identity, _load_capture, _sha,
    _validate_capture_request_identity, _validate_spatial_reference,
)

DATASET_SLUG = "shipping-fairways"
FILE_SLUG = DATASET_SLUG
# The original HIFLD "Shipping Fairways" item reproduces the FGDC metadata of
# NOAA's "Shipping Fairways, Lanes, and Zones for US waters" (Office of Coast
# Survey; InPort item 39986). The producer's maintained distribution is the
# ENCDirect NavigationChartData/MarineTransportation service; the InPort record
# is the producer's own metadata lineage. The HIFLD inventory row's agency
# column says US Army Corps of Engineers, but no IENC feature class describes
# these coastal shipping zones and the inventory row's DataLumos archive and
# DHS crosswalk both deny anonymous access; routing to USACE IENC is a
# documented mismatch, not an accepted substitution.
INPORT_URL = "https://www.fisheries.noaa.gov/inport/item/39986"
INPORT_TITLE = "Shipping Fairways, Lanes, and Zones for US waters"
SHIPPING_LANES_ZIP_URL = "https://encdirect.noaa.gov/theme_layers/data/shipping_lanes/shippinglanes.zip"
SERVICE_DIRECTORY_URL = "https://encdirect.noaa.gov/arcgis/rest/services/NavigationChartData?f=json"
SERVICE_URL = "https://encdirect.noaa.gov/arcgis/rest/services/NavigationChartData/MarineTransportation/MapServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = ("NOAA National Ocean Service, Office of Coast Survey; ENCDirect "
             "NavigationChartData MarineTransportation map service")
SERVICE_COPYRIGHT = ("Department of Commerce (DOC), National Oceanic and Atmospheric "
                     "Administration (NOAA), National Ocean Service (NOS), Office Coast Survey (OCS) ")
EXPECTED_COUNT = 346
EXPECTED_IDS = [144533, 144534, 144535, 144536, 144537, 144538, 144539, 144540, 144541,
    144542, 144543, 144544, 144833, 144834, 144835, 144836, 144837, 144838, 144839, 144840,
    144841, 144842, 144843, 144844, 144845, 144846, 144847, 144848, 144849, 144850, 144851,
    144852, 144853, 144854, 144855, 144856, 144857, 144858, 144859, 144860, 144861, 144862,
    144863, 144864, 144865, 144866, 144867, 144868, 144869, 144870, 144871, 144872, 144873,
    144874, 144875, 144876, 144877, 144878, 144879, 144880, 144881, 144882, 144883, 144884,
    144885, 144886, 144887, 144888, 144889, 144890, 144891, 144892, 144893, 144894, 144895,
    144896, 144897, 144898, 144899, 144900, 144901, 144902, 144903, 144904, 144905, 144906,
    144907, 144908, 144909, 144910, 144911, 144912, 144913, 144914, 144915, 144916, 144917,
    144918, 144919, 144920, 144921, 144922, 144923, 144924, 144925, 144926, 144927, 144928,
    144929, 144930, 144931, 144932, 144933, 144934, 144935, 144936, 144937, 144938, 144939,
    144940, 144941, 144942, 144943, 144944, 144945, 144946, 144947, 144948, 144949, 144950,
    144951, 144952, 144953, 144954, 144955, 144956, 144957, 144958, 144959, 144960, 144961,
    144962, 144963, 144964, 144965, 144966, 144967, 144968, 144969, 144970, 144971, 144972,
    144973, 144974, 144975, 144976, 144977, 144978, 144979, 144980, 144981, 144982, 144983,
    144984, 144985, 144986, 144987, 144988, 144989, 144990, 144991, 144992, 144993, 144994,
    144995, 144996, 144997, 144998, 144999, 145000, 145001, 145002, 145003, 145004, 145005,
    145006, 145007, 145008, 145009, 145010, 145011, 145012, 145013, 145014, 145015, 145016,
    145017, 145018, 145019, 145020, 145021, 145022, 145023, 145024, 145025, 145026, 145027,
    145028, 145029, 145030, 145031, 145032, 145033, 145034, 145035, 145036, 145037, 145038,
    145039, 145040, 145041, 145042, 145043, 145044, 145045, 145046, 145047, 145048, 145049,
    145050, 145051, 145052, 145053, 145054, 145055, 145056, 145057, 145058, 145059, 145060,
    145061, 145062, 145063, 145064, 145065, 145066, 145067, 145068, 145069, 145070, 145071,
    145072, 145073, 145074, 145075, 145076, 145077, 145078, 145079, 145080, 145081, 145082,
    145083, 145084, 145085, 145086, 145087, 145088, 145089, 145090, 145091, 145092, 145093,
    145094, 145095, 145096, 145097, 145098, 145099, 145100, 145101, 145102, 145103, 145104,
    145105, 145106, 145107, 145108, 145109, 145110, 145111, 145112, 145113, 145114, 145115,
    145116, 145117, 145118, 145119, 145120, 145121, 145122, 145123, 145124, 145125, 145126,
    145127, 145128, 145129, 145130, 145131, 145132, 145133, 145134, 145135, 145136, 145137,
    145138, 145139, 145140, 145141, 145142, 145143, 145144, 145145, 145146, 145147, 145148,
    145149, 145150, 145151, 145152, 145153, 145154, 145155, 145156, 145157, 145158, 145159,
    145160, 145161, 145162, 145163, 145164, 145165, 145166]
EXPECTED_ID_SHA256 = "a2588be717502bf3999fbff37d4459b9d5db4adbc1a04cb400f37118dcabd589"
PAGE_SIZE = 100
PAGE_MAX_BYTES = 8_000_000
MAX_CAPTURE_BYTES = 192_000_000
EXPECTED_LAYER_NAME = "Shipping Lanes and Regulations"
EXPECTED_SERVICE_LAYERS = [
    (0, "Shipping Lanes and Regulations", "esriGeometryPolygon"),
    (1, "Coastal Maintained Channels", "esriGeometryPolygon"),
]
EXPECTED_FIELD_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "THEMELAYER", "type": "esriFieldTypeString", "alias": "THEMELAYER", "length": 200, "domain": None},
    {"name": "INFORM", "type": "esriFieldTypeString", "alias": "INFORM", "length": 400, "domain": None},
    {"name": "OBJNAM", "type": "esriFieldTypeString", "alias": "OBJNAM", "length": 400, "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "Shape", "domain": None},
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELD_DECLARATIONS if field["name"] != "SHAPE"]
EXPECTED_EXTENT = {"xmin": -180, "ymin": 9.30976600000002, "xmax": 180, "ymax": 66.566622}
CAPTURE_NAMES = {"inport_item", "service_directory", "service_before", "layer_before",
                 "count_before", "ids_before", "layer_after", "count_after", "ids_after",
                 "service_after",
                 *{f"page_{index:03d}" for index in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE)}}
RETRYABLE_HTTP_STATUS = {500, 502, 503, 504, 429}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_roster(ids: list[int]) -> list[tuple[str, str]]:
    urls = [("inport_item", INPORT_URL),
            ("service_directory", SERVICE_DIRECTORY_URL),
            ("service_before", SERVICE_URL + "?f=json"), ("layer_before", LAYER_URL + "?f=json"),
            ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
            ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}))]
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        page_ids = ids[start:start + PAGE_SIZE]
        urls.append((f"page_{index:03d}", _query({"objectIds": ",".join(map(str, page_ids)),
            "outFields": "*", "returnGeometry": "true", "returnZ": "true", "returnM": "true",
            "outSR": "4326", "orderByFields": "OBJECTID ASC", "f": "json"})))
    urls.extend([("layer_after", LAYER_URL + "?f=json"),
                 ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                 ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                 ("service_after", SERVICE_URL + "?f=json")])
    return urls


def _retry_budget(attempt: int) -> float:
    return (1.5, 3.0, 5.0, 8.0, 12.0)[min(attempt, 4)]


def _retained_bytes(root: Path) -> int:
    return sum(path.stat().st_size for suffix in ("*.body", "*.wire")
               for path in root.glob(suffix) if path.is_file())


def _discard_partial(root: Path, name: str) -> None:
    for suffix in (".body", ".wire", ".receipt.json"):
        (root / f"{name}{suffix}").unlink(missing_ok=True)


def _capture_with_retry(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    """Capture one GET with bounded retries for the ENCDirect flaky backend.

    The ENCDirect service intermittently answers HTTP 500 with a "Could not
    find service" body (observed roughly one request in three on 2026-11-12).
    Failed attempts leave partial evidence via capture_get; that evidence is
    discarded before each retry so the sealed roster contains only the exact
    retained response. Permanent client errors and redirects fail closed.
    """
    for attempt in range(6):
        if _retained_bytes(root) + 2 * PAGE_MAX_BYTES > MAX_CAPTURE_BYTES:
            raise ValueError("source_capture_budget_exceeded_before_request")
        try:
            body, receipt = capture_get(client, url, root, name, max_bytes=PAGE_MAX_BYTES)
        except ValueError as exc:
            if not str(exc).startswith("incomplete_source_capture:"):
                raise
            receipt_path = root / f"{name}.receipt.json"
            status = None
            if receipt_path.is_file():
                try:
                    status = json.loads(receipt_path.read_bytes()).get("status")
                except (OSError, ValueError):
                    status = None
            if status is not None and status not in RETRYABLE_HTTP_STATUS:
                # Permanent client-side failure: retain the error evidence and
                # fail closed; the quarantine path keeps it out of the roster.
                raise
            _discard_partial(root, name)
            time.sleep(_retry_budget(attempt))
            continue
        try:
            doc = json.loads(body)
        except ValueError:
            if name == "inport_item":
                return body, receipt
            raise
        if isinstance(doc, dict) and "error" in doc:
            code = (doc.get("error") or {}).get("code")
            if code in RETRYABLE_HTTP_STATUS or code is None:
                _discard_partial(root, name)
                time.sleep(_retry_budget(attempt))
                continue
            # Transport-200 ArcGIS error: retain the error evidence and fail closed.
            raise ValueError(f"source_endpoint_error:{name}:{code}")
        return body, receipt
    raise ValueError("source_endpoint_retries_exhausted:" + name)


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    roster = manifest.get("artifact_roster")
    if not isinstance(roster, dict) or set(manifest["receipts"]) != CAPTURE_NAMES:
        raise ValueError("retained_capture_roster_or_names_invalid")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    expected_urls = dict(_capture_roster(EXPECTED_IDS))
    captures = {}
    for name in sorted(CAPTURE_NAMES):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        _validate_capture_request_identity(name, receipt, expected_urls[name])
        captures[name] = (body, receipt)
    return captures


def _esri_polygon_wkb(geometry: dict) -> bytes:
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    has_z, has_m = geometry.get("hasZ", False), geometry.get("hasM", False)
    if type(has_z) is not bool or type(has_m) is not bool:
        raise ValueError("source_ordinate_flags_invalid")
    if has_z or has_m:
        # The pinned layer contract is XY-only; correctly flagged dimensional
        # geometry is source drift and must fail closed, never be sealed.
        raise ValueError("source_polygon_dimensional_flags_rejected")
    shells, holes = [], []
    for coords in rings:
        if not isinstance(coords, list) or len(coords) < 4:
            raise ValueError("source_polygon_ring_invalid")
        if coords[0] != coords[-1]:
            raise ValueError("source_polygon_ring_not_closed")
        if any(not isinstance(point, list) for point in coords):
            raise ValueError("source_polygon_coordinate_invalid")
        dims = {len(point) for point in coords}
        if len(dims) != 1:
            raise ValueError("source_polygon_ordinate_count_drift")
        if next(iter(dims)) != 2:
            raise ValueError("source_polygon_zm_flags_do_not_match_coordinates")
        ring = LinearRing(coords)
        (holes if ring.is_ccw else shells).append([list(point) for point in coords])
    # ArcGIS encodes exterior rings clockwise and interior rings counter-clockwise.
    # Reject non-standard winding rather than repairing or guessing.
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    shell_polys = [Polygon([point[:2] for point in ring]) for ring in shells]
    assignments = [[] for _ in shell_polys]
    for ring in holes:
        hole = Polygon([point[:2] for point in ring])
        owners = [i for i, shell in enumerate(shell_polys) if shell.contains(hole.representative_point())]
        if len(owners) != 1:
            raise ValueError("source_hole_ownership_ambiguous")
        assignments[owners[0]].append(ring)

    def polygon_wkb(shell, hole_rings):
        out = bytearray(struct.pack("<BI", 1, 3))
        out.extend(struct.pack("<I", 1 + len(hole_rings)))
        for ring in [shell, *hole_rings]:
            out.extend(struct.pack("<I", len(ring)))
            for point in ring:
                out.extend(struct.pack("<dd", *point))
        return bytes(out)

    polygons = [polygon_wkb(shells[i], assignments[i]) for i in range(len(shells))]
    if len(polygons) == 1:
        return polygons[0]
    multi = bytearray(struct.pack("<BI", 1, 6))
    multi.extend(struct.pack("<I", len(polygons)))
    for polygon in polygons:
        multi.extend(polygon)
    return bytes(multi)


def _validate_inport_item(body: bytes) -> None:
    text = body.decode("utf-8", errors="replace")
    markers = (INPORT_TITLE, SHIPPING_LANES_ZIP_URL,
               "encdirect.noaa.gov/arcgis/rest/services/NavigationChartData/MarineTransportation/MapServer",
               "Office of Coast Survey")
    for marker in markers:
        if marker not in text:
            raise ValueError("inport_lineage_record_drift:" + marker)


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    if set(captures) != CAPTURE_NAMES:
        raise ValueError("source_capture_name_roster_drift")
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()
            if not name.startswith("inport")}
    _validate_inport_item(captures["inport_item"][0])
    directory = docs["service_directory"]
    if not any(service.get("name") == "NavigationChartData/MarineTransportation"
               and service.get("type") == "MapServer" for service in directory.get("services", [])):
        raise ValueError("official_encdirect_service_directory_drift")
    service = docs["service_before"]
    advertised = [(layer.get("id"), layer.get("name"), layer.get("geometryType"))
                  for layer in service.get("layers", [])]
    if (service.get("mapName") != "MarineTransportation" or service.get("copyrightText") != SERVICE_COPYRIGHT
            or service.get("maxRecordCount") != 10000 or advertised != EXPECTED_SERVICE_LAYERS):
        raise ValueError("official_encdirect_service_drift")
    layer = docs["layer_before"]
    if layer.get("fields") != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != EXPECTED_LAYER_NAME
            or layer.get("geometryType") != "esriGeometryPolygon" or layer.get("maxRecordCount") != 10000
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError("source_layer_identity_extent_or_limit_drift")
    if layer.get("hasZ") or layer.get("hasM"):
        raise ValueError("source_layer_dimensional_drift")
    if layer.get("spatialReference") is not None:
        _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent_reference = layer.get("extent", {}).get("spatialReference")
    if extent_reference is not None:
        _validate_spatial_reference(extent_reference, "source_layer_extent")
    if docs["service_after"] != service or docs["layer_after"] != layer:
        raise ValueError("source_service_or_layer_changed_during_capture")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (count_before != EXPECTED_COUNT or count_after != count_before or not isinstance(ids_before, list)
            or any(type(oid) is not int for oid in ids_before) or ids_before != EXPECTED_IDS
            or ids_after != ids_before
            or len(set(ids_before)) != EXPECTED_COUNT or _sha(_canonical(ids_before)) != EXPECTED_ID_SHA256):
        raise ValueError("source_count_or_id_population_drift")
    features = []
    vertices = 0
    geometry_valid = geometry_empty = 0
    for name in sorted(n for n in captures if n.startswith("page_")):
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_transfer_limit")
        if page.get("hasZ") or page.get("hasM"):
            raise ValueError("source_page_dimensional_drift")
        _validate_spatial_reference(page.get("spatialReference"), "source_" + name + "_page")
        rows = page.get("features")
        expected_index = int(name[-3:])
        expected_ids = EXPECTED_IDS[expected_index * PAGE_SIZE:(expected_index + 1) * PAGE_SIZE]
        if not isinstance(rows, list) or len(rows) != len(expected_ids):
            raise ValueError("source_page_population_incomplete:" + name)
        page_ids = []
        for feature in rows:
            attrs, geometry = feature.get("attributes"), feature.get("geometry")
            if (not isinstance(attrs, dict) or len(attrs) != len(ATTRIBUTE_FIELDS)
                    or set(attrs) != {field["name"] for field in ATTRIBUTE_FIELDS}):
                raise ValueError("source_attribute_roster_or_order_drift")
            for field in ATTRIBUTE_FIELDS:
                value = attrs[field["name"]]
                if value is None:
                    continue
                if field["type"] == "esriFieldTypeOID":
                    if type(value) is not int or value <= 0:
                        raise ValueError("source_oid_value_type_drift")
                elif field["type"] == "esriFieldTypeString" and not isinstance(value, str):
                    raise ValueError("source_string_value_type_drift:" + field["name"])
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
            for ring in rings:
                if not isinstance(ring, list) or len(ring) < 4:
                    raise ValueError("source_polygon_ring_invalid")
                if ring[0] != ring[-1]:
                    raise ValueError("source_polygon_ring_not_closed")
                for point in ring:
                    if (not isinstance(point, list) or len(point) != 2
                            or any(type(coord) not in (int, float) or not math.isfinite(coord) for coord in point)):
                        raise ValueError("source_xy_ordinate_contract_drift")
                    if not (EXPECTED_EXTENT["xmin"] <= point[0] <= EXPECTED_EXTENT["xmax"]
                            and EXPECTED_EXTENT["ymin"] <= point[1] <= EXPECTED_EXTENT["ymax"]):
                        raise ValueError("source_geometry_outside_declared_coverage")
                    vertices += 1
            oid = attrs.get("OBJECTID")
            if type(oid) is not int or oid not in expected_ids:
                raise ValueError("source_feature_id_roster_drift")
            page_ids.append(oid)
            geom = from_wkb(_esri_polygon_wkb(geometry))
            geometry_empty += int(geom.is_empty)
            geometry_valid += int(geom.is_valid and not geom.is_empty)
            feature["attributes"] = {field["name"]: attrs[field["name"]] for field in ATTRIBUTE_FIELDS}
            features.append(feature)
        if sorted(page_ids) != expected_ids or len(set(page_ids)) != len(page_ids):
            raise ValueError("source_page_id_population_drift:" + name)
    ids = [feature["attributes"]["OBJECTID"] for feature in features]
    if sorted(ids) != EXPECTED_IDS or len(features) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT:
        raise ValueError("source_feature_id_population_drift")
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    bounds = [from_wkb(_esri_polygon_wkb(row["geometry"])).bounds for row in features]
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
        "source_id_min": min(ids), "source_id_max": max(ids),
        "source_schema_sha256": _sha(_canonical(layer["fields"])),
        "source_record_sha256": _sha(_canonical([row["attributes"] for row in features])),
        "source_geometry_sha256": _sha(_canonical([[row["attributes"]["OBJECTID"], row["geometry"]] for row in features])),
        "source_geometry_type": "esriGeometryPolygon", "source_wkid": 4326,
        "source_vertex_count": vertices, "geometry_valid_count": geometry_valid,
        "geometry_empty_count": geometry_empty,
        "geometry_invalid_count": len(features) - geometry_valid - geometry_empty,
        "geometry_bounds": [min(b[0] for b in bounds), min(b[1] for b in bounds),
                            max(b[2] for b in bounds), max(b[3] for b in bounds)],
        "source_extent": layer["extent"],
        "source_coverage_scope": "Complete observed NOAA OCS ENCDirect Shipping Lanes and Regulations layer: coastal and ocean shipping zones (traffic lanes, separation zones, precautionary areas, shipping safety fairways, areas to be avoided, recommended routes, right whale speed-restriction areas and similar regulated zones) for U.S. waters. The producer publishes this layer weekly; it is not a census of all water areas.",
        "inport_lineage_record": INPORT_URL,
        "producer_service_copyright": SERVICE_COPYRIGHT,
        "inventory_agency_column_mismatch": "The HIFLD inventory row lists the agency as US Army Corps of Engineers, but the item's own FGDC metadata credits NOAA OCS and no USACE IENC feature class describes these coastal shipping zones; the NOAA OCS ENCDirect service is the original producer publication, not a mirror substitute.",
        "drift_guard": "Producer InPort lineage record, exact ENCDirect service directory/service/layer roster, full field declarations/types and aliases, explicit EPSG:4326 layer and response-page SRs, XY-only layer/page/geometry dimensional flags, count and before/after OBJECTID roster (sparse native IDs), per-page IDs/attributes/transfer limits, ring closure/winding/hole ownership and declared-extent containment are checked.",
    }
    return layer["fields"], features, audit


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeString": pa.string()}[field["type"]]


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    properties = [field for field in fields if field["name"] != "SHAPE"]
    arrays = [pa.array([row["attributes"][field["name"]] for row in features], type=_arrow_type(field))
              for field in properties]
    arrays.append(pa.array([_esri_polygon_wkb(row["geometry"]) for row in features], type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=[field["name"] for field in properties] + ["geometry"])
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


def capture_shipping_fairways(storage: StagingStorageResource, *, version: str,
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
        if requested and any(requested.glob("*.body")):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root = ((requested / version / uuid4().hex) if requested else
                Path("/tmp/hifld-native-reconstruction/310/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(60, connect=15),
                              headers={"User-Agent": "hifld-next-datasets/noaa-ocs-draft"}) as client:
                for name, url in _capture_roster(EXPECTED_IDS):
                    captures[name] = _capture_with_retry(client, url, root, name)
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
    inputs = [{"input_id": name, "producer": PUBLISHER,
        "host": urlsplit(receipt["url"]).hostname,
        "role": "producer_publication",
        "canonical_url": INPORT_URL if name == "inport_item" else LAYER_URL,
        "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
        "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": "The InPort metadata record was published 2015-12-04 and describes weekly maintenance; the service exposes no per-feature or layer edition date, so the captured snapshot's release date is unknown.",
        "license": "Unresolved; rights review pending.",
        "attribution": "NOAA Office of Coast Survey; the service also carries a U.S. Coast Guard layer copyright string. Attribution and rights review pending."}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "Shipping Fairways",
        "description": "Complete observed NOAA OCS ENCDirect Shipping Lanes and Regulations layer (the original HIFLD 'Shipping Fairways' product); historical HIFLD continuity, baseline, contract, coverage, rights, and release remain unaccepted.",
        "publisher": PUBLISHER,
        "agency": "U.S. Department of Commerce, NOAA, National Ocean Service, Office of Coast Survey",
        "source_url": LAYER_URL, "license": None, "source_modified": None,
        "date_issued": None, "date_modified": None, "processor": "HIFLD Next",
        "code_commit": identity["code_commit"], "code_identity_scheme": identity["code_identity_scheme"],
        "inputs": inputs,
        "transformations": [{"operation": "Esri XY Polygon to GeoParquet WKB candidate", "parameters": {
            "source_fields": "all declared attributes retained in source order and declared types",
            "source_ids": "OBJECTID retained unchanged; the native roster is sparse and is preserved exactly",
            "source_crs": "EPSG:4326",
            "geometry": "2D ring order, winding and hole ownership preserved; no reprojection, repair, or simplification",
            "raw_evidence": "complete decoded response and wire bodies retained with checksummed receipts",
            "candidate_status": "unapproved; historical/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": ("The HIFLD inventory row 'shipping-fairways' (agency column US Army Corps of Engineers) carries the abstract of NOAA's 'Shipping Fairways, Lanes, and Zones for US waters' (Office of Coast Survey; InPort item 39986; FGDC originator NOAA OCS, contact Patrick Keown). No USACE IENC feature class describes these coastal shipping zones, so the board routing to USACE IENC was a documented mismatch, not an accepted substitution. This candidate captures the producer's maintained ENCDirect NavigationChartData/MarineTransportation MapServer layer 0 'Shipping Lanes and Regulations' in full; the InPort record also lists a static shippinglanes.zip download whose server Last-Modified (2020-11-09) predates the maintained service. The historical HIFLD/DataLumos archive (project 240646) and the DHS crosswalk DOI (10.3886/E241367V1) return HTTP 403 anonymously, so historical ID/geometry continuity with the 2017 HIFLD item is unverified."),
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Native attributes, IDs and 2D rings are retained in a typed candidate.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}
    }
    validate_release_provenance(manifest); validate_output_policy(manifest); provenance_projection(manifest)
    audit.update({"source_fields": fields, "official_layer_name": EXPECTED_LAYER_NAME,
                  "official_layer_id": LAYER_ID,
                  "official_source_item": "NOAA InPort item 39986 (Office of Coast Survey)",
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
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": min(EXPECTED_IDS),
            "source_id_max": max(EXPECTED_IDS), "source_schema_sha256": audit["source_schema_sha256"],
            "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
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
        "feature_count": EXPECTED_COUNT, "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_id_min": min(EXPECTED_IDS),
        "source_id_max": max(EXPECTED_IDS), "source_schema_sha256": audit["source_schema_sha256"],
        "source_coverage_scope": audit["source_coverage_scope"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
        "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
        "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
        "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
