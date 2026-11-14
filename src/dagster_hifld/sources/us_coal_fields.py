"""Bounded, source-faithful capture of the maintained USGS coal-fields map service.

The assigned dataset is the "US Coal Fields" polygon layer of the retired HIFLD
Open catalog (inventory ID ``us-coal-fields-1``). The DHS crosswalk row for
"US Coal Fields" records Status "Deprecate" (New Hosting N/A, justification "No
longer supported as part of the HIFLD mission") and names the producer
publication landing page https://pubs.usgs.gov/of/2012/1205/Downloads/. The
producer is the U.S. Geological Survey.

The pipeline's producer input is the USGS Energy Resources Program's maintained
publication of the national coal-fields GIS mapping: the ``CoalByRank``
MapServer layer 0 ("CoalFieldsbyRank") on USGS's own ArcGIS Server
(``energy.usgs.gov``, ArcGIS Server 12.1), alongside the sibling ``CoalByAge``
and ``CoalProvinces`` services. USGS's active Evolving Utilization of Solid
Energy Fuels (EUSEF) project states that its Data Management task keeps
"Geographic Information System coal fields mapping" available to all users.
The service description itself cites the origin publication: East, J.A., 2013,
*Coal Fields of the Conterminous United States: National Coal Resource
Assessment Updated Version*, USGS Open-File Report 2012-1205 (first posted
April 2, 2013). That Open-File Report is a completed one-time publication
(FGDC: Progress Complete, maintenance None planned); it is permanently
available but not maintained, and neither it nor its Downloads package is
fetched by this pipeline. An earlier draft of this module captured the OFR
2012-1205 Downloads package directly; that one-time release is retired as a
pipeline input and the claim that it was the "current maintained USGS release"
is withdrawn: the maintained producer publication is the NCRDS coal-fields map
service captured here (verified 2026-11-14 to serve 602 features whose
attributes equal the OFR 2012-1205 ``Coal_Fields`` shapefile row-for-row and
whose served geometry is vertex-identical after projection).

This module performs one complete, bounded capture of the maintained service
(producer directory listing, service/layer documents, count and id-roster
queries before and after, and the paged feature queries), validates pinned
canonical digests and structural identity anchors, reconstructs the served Esri
JSON rings natively (polygon grouping without repair, no reprojection of the
served Web Mercator coordinates), and stages the typed GeoParquet candidate
plus full source provenance. No source authority, schema/ID contract, baseline,
rights, coverage or release acceptance is performed; every acceptance flag
stays false.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import Polygon

from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import processing_identity

DATASET_SLUG = "us-coal-fields-1"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Geological Survey (Energy Resources Program)"
SERVICE_HOST = "https://energy.usgs.gov"
NCRDS_DIRECTORY_URL = f"{SERVICE_HOST}/arcgis/rest/services/NCRDS?f=pjson"
SERVICE_URL = f"{SERVICE_HOST}/arcgis/rest/services/NCRDS/CoalByRank/MapServer"
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
# The served feature geometry is Web Mercator (Esri wkid 102100 / EPSG:3857).
SERVICE_CRS_WKID = 102100
SERVICE_CRS_LATEST_WKID = 3857
DOC_CAPTURE_LIMIT = 2_000_000
PAGE_CAPTURE_LIMIT = 32_000_000
TOTAL_CAPTURE_BUDGET = 96_000_000
PAGE_SIZE = 2000
EXPECTED_RECORD_COUNT = 602
PAGE_COUNT = (EXPECTED_RECORD_COUNT + PAGE_SIZE - 1) // PAGE_SIZE

# Byte-level content contract observed on 2026-11-14 against
# https://energy.usgs.gov/arcgis/rest/services/NCRDS/CoalByRank/MapServer.
# Digests are computed over canonical JSON (sorted keys, compact separators) of
# the served values; any producer-side change fails the capture closed.
EXPECTED_COUNT_SHA256 = "ce5888e8b7d5e9875a77163c67b3e8bae27b918becd14b881c474d3453535345"
EXPECTED_ID_SHA256 = "2b6da30ea0cab998ea45ccbb4304ddae40d43c8c4198d99f0b886fd71580bc1c"
EXPECTED_RECORD_SHA256 = "d5ae4cff12338007bb59d14ff322c1b8b2f001e2a488923d03b14bc5aff0feda"
EXPECTED_GEOMETRY_SHA256 = "0228f160addd3950becdeaac9b8783e263c3805674762a33873ef311c62626e9"
EXPECTED_SCHEMA_SHA256 = "c673dd4abee20e29665b812a2842c8b979a7bb1cb117794dd43d77c585366642"
# The producer directory must still list this service family on its own server.
EXPECTED_NCRDS_SERVICES = (
    ("NCRDS/CoalByAge", "MapServer"),
    ("NCRDS/CoalByRank", "MapServer"),
    ("NCRDS/CoalProvinces", "MapServer"),
    ("NCRDS/ustrat_disl", "MapServer"),
    ("NCRDS/ustrat_dyn", "MapServer"),
    ("NCRDS/ustrat", "MapServer"),
)
SERVICE_TITLE = (
    "Coal Fields of the Conterminous United States: National Coal Resource Assessment Updated Version"
)
SERVICE_CITATION_MARK = (
    "East, J.A., 2013, Coal fields of the conterminous United States—National Coal Resource "
    "Assessment updated version: U.S. Geological Survey Open-File Report 2012–1205"
)
SERVICE_OFR_LINK_MARK = "http://pubs.usgs.gov/of/2012/1205/"
SERVICE_MAP_NAME = "CoalByRank"
LAYER_NAME = "CoalFieldsbyRank"
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID", None, False),
    ("Shape", "esriFieldTypeGeometry", None, True),
    ("AREA", "esriFieldTypeDouble", None, True),
    ("PERIMETER", "esriFieldTypeDouble", None, True),
    ("PROVINCE", "esriFieldTypeString", 31, True),
    ("NAME", "esriFieldTypeString", 36, True),
    ("AGE", "esriFieldTypeString", 30, True),
    ("RANK", "esriFieldTypeString", 50, True),
    ("Shape_Length", "esriFieldTypeDouble", None, True),
    ("Shape_Area", "esriFieldTypeDouble", None, True),
]
ATTRIBUTE_NAMES = ("OBJECTID", "AREA", "PERIMETER", "PROVINCE", "NAME", "AGE", "RANK", "Shape_Length", "Shape_Area")
NUMERIC_ATTRIBUTE_NAMES = ("AREA", "PERIMETER", "Shape_Length", "Shape_Area")
STRING_ATTRIBUTE_NAMES = ("PROVINCE", "NAME", "AGE", "RANK")
EXPECTED_LAYER_EXTENT = {"xmin": -13844079.967, "ymin": 3003294.840300001,
                         "xmax": -7920327.6513, "ymax": 6275165.573600002}
EXPECTED_BOUNDS = [-13844079.967, 3003294.840300001, -7920327.6513, 6275165.573600002]
# One source-published ring self-intersection ("Ring Self-intersection" at the
# Great Falls Field, served record 95; the same defect exists in the origin
# OFR 2012-1205 shapefile and is retained unchanged, not repaired).
EXPECTED_INVALID_RECORD_NUMBERS = (95,)
PROVINCE_COUNTS = {
    "Eastern": 247, "Gulf": 4, "Interior": 56, "Northern Great Plains": 53,
    "Pacific Coast": 70, "Rocky Mountain": 172,
}
AGE_COUNTS = {
    "Cretaceous": 187, "Cretaceous-Jurassic": 2, "Mississippian": 9,
    "Pennsylvanian-Permian": 266, "Tertiary": 123, "Tertiary-Cretaceous": 8,
    "Triassic": 7,
}
RANK_COUNTS = {
    "Anthracite": 29, "Lignite": 57, "Low Volatile Bituminous": 29,
    "Medium and High Volatile Bituminous": 381, "Subbituminous": 106,
}
DISTINCT_NAME_COUNT = 110
LICENSE = (
    "US government publication distributed by the U.S. Geological Survey without fees "
    "(Energy Resources Program service data; OFR 2012-1205 distribution liability statement). "
    "No source, contract, baseline, rights or release acceptance by this pipeline."
)
ATTRIBUTION = (
    "U.S. Geological Survey Energy Resources Program, National Coal Resources Data System "
    "(NCRDS) CoalByRank map service; content published as East, J.A., 2013, USGS Open-File "
    "Report 2012-1205."
)
RELEASE_DATE = "2013-04-02"
RELEASE_DATE_BASIS = (
    "Content release basis: this service publishes the National Coal Resource Assessment "
    "updated coal-fields mapping, whose source publication USGS Open-File Report 2012-1205 was "
    "first posted April 2, 2013 (per the service description's own citation). The service "
    "publishes no per-snapshot or per-feature release date and reports no retirement; fetch "
    "timestamps are not release dates."
)
_MAINTAINED_SERVICE_NOTE = (
    "The USGS Energy Resources Program currently maintains and publishes the national GIS "
    "coal-fields mapping as live services on its own ArcGIS Server (energy.usgs.gov, ArcGIS "
    "Server 12.1): NCRDS 'CoalByRank', 'CoalByAge' and 'CoalProvinces'. The producer's active "
    "Evolving Utilization of Solid Energy Fuels (EUSEF) project states its Data Management task "
    "keeps 'Geographic Information System coal fields mapping' available to all users. On "
    "2026-11-14 the CoalByRank service served 602 features whose AREA/PERIMETER/PROVINCE/NAME/"
    "AGE/RANK values equal the OFR 2012-1205 Coal_Fields shapefile row-for-row in record order "
    "and whose served geometry is vertex-identical after projection (298,530/298,530 vertices, "
    "maximum Web-Mercator offset 1.95 m from the NAD83 source under the standard "
    "EPSG:4269-to-EPSG:3857 transform)."
)
_ORIGIN_PUBLICATION_NOTE = (
    "The content originates from USGS Open-File Report 2012-1205 (East, J.A., 2013, first "
    "posted April 2, 2013, https://pubs.usgs.gov/of/2012/1205/), a completed one-time report "
    "(FGDC: Progress Complete, maintenance and update frequency None planned) that remains "
    "permanently available but is not maintained. The report and its Downloads package are "
    "never fetched by this pipeline; the maintained NCRDS coal-fields map service above is the "
    "pipeline's only producer input. This corrects the earlier draft's claim that the OFR "
    "publication itself was the 'current maintained USGS release'."
)
_BANNED_COPIES_NOTE = (
    "DataLumos archive project 240310 (inventory Download Location), the deprecated "
    "services1.arcgis.com/Hp6G80Pky0om7QvQ FeatureServer, GeoPlatform item "
    "13ad01f317f74e5989c3ee199496fa32, and the Esri Living Atlas 'USA Coal Fields' item "
    "5dffaad2b2074d0dad20e9d5f52f6641 (owner esri_landscape2, not the producing agency) are "
    "retired/rescued/mirrored copies; none is fetched or pinned as an input."
)

CAPTURE_NAMES = (
    "ncrds_directory",
    "service_before",
    "layer_before",
    "count_before",
    "ids_before",
    *(f"page_{index:03d}" for index in range(PAGE_COUNT)),
    "layer_after",
    "service_after",
    "count_after",
    "ids_after",
)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _json_bytes(value: object) -> bytes:
    return (_canonical(value) + b"\n")


def _digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _request_url(name: str) -> str:
    if name == "ncrds_directory":
        return NCRDS_DIRECTORY_URL
    if name.startswith("service_"):
        return SERVICE_URL + "?f=pjson"
    if name.startswith("layer_"):
        return LAYER_URL + "?f=pjson"
    if name.startswith("count_"):
        return _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})
    if name.startswith("ids_"):
        return _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})
    if name.startswith("page_"):
        index = int(name.removeprefix("page_"))
        return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                       "orderByFields": "OBJECTID ASC", "resultOffset": str(index * PAGE_SIZE),
                       "resultRecordCount": str(PAGE_SIZE), "f": "json"})
    raise ValueError("unknown_capture_document:" + name)


def _document_capture_limit(name: str) -> int:
    return PAGE_CAPTURE_LIMIT if name.startswith("page_") else DOC_CAPTURE_LIMIT


def _plain_fields(layer: dict) -> list[tuple]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(field, dict) for field in fields):
        raise ValueError("source_field_roster_drift")
    return [(field.get("name"), field.get("type"), field.get("length"), field.get("nullable")) for field in fields]


def _validate_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    if value.get("wkid") != SERVICE_CRS_WKID or value.get("latestWkid") != SERVICE_CRS_LATEST_WKID:
        raise ValueError(label + "_crs_drift")


def _validate_document(name: str, body: bytes) -> dict:
    """Fail-closed identity anchors for one captured producer document."""
    try:
        doc = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(f"invalid_source_document:{name}") from exc
    if not isinstance(doc, dict) or "error" in doc:
        raise ValueError(f"source_document_error_response:{name}")
    if name == "ncrds_directory":
        services = doc.get("services")
        if (not isinstance(services, list)
                or [(item.get("name"), item.get("type")) for item in services if isinstance(item, dict)]
                != list(EXPECTED_NCRDS_SERVICES)):
            raise ValueError("producer_service_directory_roster_drift")
        return doc
    if name.startswith("service_"):
        if doc.get("mapName") != SERVICE_MAP_NAME:
            raise ValueError("producer_service_identity_drift")
        document_info = doc.get("documentInfo")
        if (not isinstance(document_info, dict) or document_info.get("Title") != SERVICE_TITLE
                or SERVICE_CITATION_MARK not in (document_info.get("Comments") or "")
                or SERVICE_OFR_LINK_MARK not in (document_info.get("Comments") or "")
                or doc.get("description") != document_info.get("Comments")):
            raise ValueError("producer_service_citation_or_description_drift")
        return doc
    if name.startswith("layer_"):
        if (doc.get("id") != LAYER_ID or doc.get("name") != LAYER_NAME
                or doc.get("type") != "Feature Layer" or doc.get("geometryType") != "esriGeometryPolygon"
                or doc.get("displayField") != "NAME"):
            raise ValueError("producer_layer_identity_drift")
        if doc.get("hasZ") is True or doc.get("hasM") is True:
            raise ValueError("source_z_or_m_geometry_unsupported")
        if doc.get("maxRecordCount") != PAGE_SIZE:
            raise ValueError("source_record_limit_drift")
        if doc.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
            raise ValueError("source_pagination_capability_changed")
        _validate_spatial_reference(doc.get("sourceSpatialReference"), "source_layer")
        extent = doc.get("extent")
        if not isinstance(extent, dict):
            raise ValueError("source_layer_extent_missing")
        _validate_spatial_reference(extent.get("spatialReference"), "source_layer_extent")
        if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
            raise ValueError("source_layer_extent_drift")
        fields = _plain_fields(doc)
        if fields != EXPECTED_FIELDS or _digest(_canonical(fields)) != EXPECTED_SCHEMA_SHA256:
            raise ValueError("source_schema_contract_drift")
        return doc
    if name.startswith("count_"):
        if doc.get("count") != EXPECTED_RECORD_COUNT:
            raise ValueError("source_count_drift")
        if _digest(_canonical(doc)) != EXPECTED_COUNT_SHA256:
            raise ValueError("source_count_digest_drift")
        return doc
    if name.startswith("ids_"):
        if doc.get("objectIdFieldName") != "OBJECTID":
            raise ValueError("source_id_field_changed")
        roster = doc.get("objectIds")
        if (not isinstance(roster, list) or roster != sorted(roster) or len(set(roster)) != EXPECTED_RECORD_COUNT
                or any(type(value) is not int for value in roster)
                or roster != list(range(1, EXPECTED_RECORD_COUNT + 1))):
            raise ValueError("source_id_population_drift")
        if _digest(_canonical(sorted(roster))) != EXPECTED_ID_SHA256:
            raise ValueError("source_id_roster_drift")
        return doc
    if name.startswith("page_"):
        _validate_spatial_reference(doc.get("spatialReference"), name)
        return doc
    raise ValueError("unknown_capture_document:" + name)


def _validate_page(name: str, body: bytes, expected_ids: list[int]) -> list[dict]:
    page = _validate_document(name, body)
    _validate_spatial_reference(page.get("spatialReference"), name)
    rows = page.get("features")
    if not isinstance(rows, list) or len(rows) != len(expected_ids):
        raise ValueError("source_page_feature_count_drift")
    if page.get("exceededTransferLimit") is True:
        raise ValueError("source_page_unexpected_exceeded_transfer_limit")
    features = []
    for row, expected_id in zip(rows, expected_ids):
        if not isinstance(row, dict):
            raise ValueError("source_page_feature_invalid")
        attributes = row.get("attributes")
        geometry = row.get("geometry")
        if not isinstance(attributes, dict) or set(attributes) != set(ATTRIBUTE_NAMES):
            raise ValueError("source_record_attribute_schema_drift")
        object_id = attributes["OBJECTID"]
        if type(object_id) is not int or object_id != expected_id:
            raise ValueError("source_page_id_slice_incomplete_or_reordered")
        for field in NUMERIC_ATTRIBUTE_NAMES:
            value = attributes[field]
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError("null_nonfinite_or_unparseable_source_attribute")
        for field in STRING_ATTRIBUTE_NAMES:
            if not isinstance(attributes[field], str):
                raise ValueError("null_or_unexpected_source_attribute_type")
        if set(geometry) != {"rings"} or not isinstance(geometry["rings"], list) or not geometry["rings"]:
            raise ValueError("source_polygon_geometry_invalid")
        for ring in geometry["rings"]:
            if (not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]
                    or any(not isinstance(point, list) or len(point) != 2
                           or type(point[0]) not in (int, float) or type(point[1]) not in (int, float)
                           or not math.isfinite(point[0]) or not math.isfinite(point[1])
                           for point in ring)):
                raise ValueError("source_polygon_ring_invalid")
        features.append(row)
    return features


def _ring_area(ring: list[list[float]]) -> float:
    # Translate to the first vertex to avoid catastrophic cancellation.
    x0, y0 = ring[0]
    return sum((ring[i][0] - x0) * (ring[i + 1][1] - y0) - (ring[i + 1][0] - x0) * (ring[i][1] - y0)
               for i in range(len(ring) - 1)) / 2


def build_polygon(rings: list[list[list[float]]]):
    """Group served Esri JSON rings into Polygon/MultiPolygon without repair."""
    shells = [(ring, Polygon(ring)) for ring in rings if _ring_area(ring) < 0]
    holes = [ring for ring in rings if _ring_area(ring) >= 0]
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    assigned: dict[int, list[list[list[float]]]] = {index: [] for index in range(len(shells))}
    for hole in holes:
        hole_polygon = Polygon(hole)
        try:
            containers = [(shells[index][1].area, index) for index in range(len(shells))
                          if shells[index][1].contains(hole_polygon)]
        except Exception as exc:
            raise ValueError("source_polygon_ring_nesting_ambiguous") from exc
        if not containers:
            raise ValueError("source_polygon_ring_nesting_ambiguous")
        _area, owner = min(containers)
        assigned[owner].append(hole)
    members = [Polygon(shells[index][0], assigned[index]) for index in range(len(shells))]
    geometry = members[0] if len(members) == 1 else shapely.MultiPolygon(members)
    if shapely.is_empty(geometry):
        raise ValueError("empty_source_polygon")
    return geometry


def verify_attribute_counts(attributes: list[dict]) -> None:
    provinces: dict[str, int] = {}
    ages: dict[str, int] = {}
    ranks: dict[str, int] = {}
    names: set[str] = set()
    for row in attributes:
        provinces[row["PROVINCE"]] = provinces.get(row["PROVINCE"], 0) + 1
        ages[row["AGE"]] = ages.get(row["AGE"], 0) + 1
        ranks[row["RANK"]] = ranks.get(row["RANK"], 0) + 1
        names.add(row["NAME"])
    if provinces != PROVINCE_COUNTS or ages != AGE_COUNTS or ranks != RANK_COUNTS:
        raise ValueError("source_attribute_value_population_drift")
    if len(names) != DISTINCT_NAME_COUNT:
        raise ValueError("source_name_population_drift")


def _validate_capture_set_brackets(docs: dict) -> None:
    """Before/after identity brackets across the capture window."""
    if docs["service_before"] != docs["service_after"]:
        raise ValueError("source_service_metadata_changed_during_capture")
    if _stable_layer_view(docs["layer_before"]) != _stable_layer_view(docs["layer_after"]):
        raise ValueError("source_layer_metadata_changed_during_capture")
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")
    if docs["ids_before"].get("objectIds") != docs["ids_after"].get("objectIds"):
        raise ValueError("source_id_population_changed_during_capture")


def _stable_layer_view(doc: object) -> object:
    """Layer document with the producer-volatile renderer color ramp removed.

    The producer's ArcGIS Server rotates
    ``drawingInfo.renderer.authoringInfo.colorRamp`` on every layer-document
    request (observed 2026-11-14: the colorRamp value differs between two
    consecutive reads seconds apart). That rotation is renderer presentation
    metadata, not dataset content; every data-bearing layer anchor (identity,
    fields/schema, extent, capabilities, CRS) is validated separately by
    ``_validate_document``. Only this exact path is excluded.
    """
    if not isinstance(doc, dict):
        return doc
    view = copy.deepcopy(doc)
    node = view
    for key in ("drawingInfo", "renderer", "authoringInfo"):
        if not isinstance(node, dict) or key not in node:
            return view
        node = node[key]
    if isinstance(node, dict):
        node.pop("colorRamp", None)
    return view


def validate_capture_set(bodies: dict[str, bytes]) -> dict:
    """Validate the complete capture roster and return the parsed feature rows."""
    if set(bodies) != set(CAPTURE_NAMES):
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    docs = {name: _validate_document(name, body) for name, body in bodies.items() if not name.startswith("page_")}
    _validate_capture_set_brackets(docs)
    ids = list(docs["ids_before"]["objectIds"])
    features: list[dict] = []
    for index in range(PAGE_COUNT):
        name = f"page_{index:03d}"
        features.extend(_validate_page(name, bodies[name], ids[index * PAGE_SIZE:(index + 1) * PAGE_SIZE]))
    if len(features) != EXPECTED_RECORD_COUNT:
        raise ValueError("source_feature_count_drift")
    attributes = [row["attributes"] for row in features]
    rings = [row["geometry"]["rings"] for row in features]
    if _digest(_canonical(attributes)) != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if _digest(_canonical(rings)) != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    geometries = [build_polygon(ring_list) for ring_list in rings]
    bounds = shapely.total_bounds(geometries).tolist()
    if bounds != EXPECTED_BOUNDS:
        raise ValueError("source_geometry_bounds_drift")
    invalid = [record_number for record_number, geometry in enumerate(geometries, start=1)
               if not shapely.is_valid(geometry)]
    if tuple(invalid) != EXPECTED_INVALID_RECORD_NUMBERS:
        raise ValueError("source_geometry_validity_drift")
    verify_attribute_counts(attributes)
    return {"features": features, "geometries": geometries, "ids": ids}


def _make_parquet(features: list[dict], geometries: list) -> bytes:
    fields = [
        pa.field("OBJECTID", pa.int64(), nullable=False, metadata={
            b"identity": b"Served Esri OID row identifier of the USGS NCRDS CoalByRank layer, retained verbatim; "
                         b"served values equal the source record order (1..602)"}),
    ]
    for name in NUMERIC_ATTRIBUTE_NAMES:
        fields.append(pa.field(name, pa.float64(), nullable=False, metadata={
            b"served_esri_type": b"esriFieldTypeDouble"}))
    for name in STRING_ATTRIBUTE_NAMES:
        fields.append(pa.field(name, pa.string(), nullable=False, metadata={
            b"served_esri_type": b"esriFieldTypeString"}))
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    table = pa.Table.from_pylist([
        {**{name: row["attributes"][name] for name in ATTRIBUTE_NAMES},
         "geometry": shapely.to_wkb(geometries[index], byte_order=1, output_dimension=2)}
        for index, row in enumerate(features)
    ], schema=pa.schema(fields))
    metadata_bytes = dict(table.schema.metadata or {})
    metadata_bytes[b"geo"] = _json_bytes({
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"], "edges": "planar",
                                 "crs": pyproj.CRS.from_epsg(SERVICE_CRS_LATEST_WKID).to_json_dict(),
                                 "bbox": EXPECTED_BOUNDS}},
    }).strip()
    table = table.replace_schema_metadata(metadata_bytes)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", version="2.6")
    return sink.getvalue().to_pybytes()


def _request(client: httpx.Client, staging, version_key: str, name: str, budget: list[int]) -> bytes:
    """Stream one bounded GET, retain wire/body/receipt evidence, validate identity."""
    max_bytes = _document_capture_limit(name)
    retained = getattr(client, "retained_capture", None)
    if retained is not None:
        body, wire, receipt = retained.read_response(name)
        budget[0] += len(body) + len(wire)
        if budget[0] > TOTAL_CAPTURE_BUDGET:
            raise ValueError("source_total_capture_budget_exceeded")
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}", body)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}.wire", wire)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{name}.receipt.json", _json_bytes(receipt))
        _validate_document(name, body)
        return body
    url = _request_url(name)
    for attempt in range(3):
        raw_parts: list[bytes] = []
        wire_parts: list[bytes] = []
        try:
            with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
                capture = capture_http_response(
                    response, max_wire_bytes=max_bytes, max_decoded_bytes=max_bytes,
                    reserve=lambda progress: _reserve(budget, progress.wire_bytes_retained + progress.decoded_bytes_retained + progress.reserve_bytes),
                    retain_wire=wire_parts.append, retain_decoded=raw_parts.append,
                )
        except Exception as exc:
            raise ValueError("source_transport_error:" + type(exc).__name__) from exc
        body = b"".join(raw_parts)
        wire = b"".join(wire_parts)
        status_ok = capture.complete and 200 <= capture.status_code < 300
        # Preserve unsuccessful responses under attempt-specific names before retrying.
        artifact_name = name if status_ok else f"{name}.attempt_{attempt + 1}"
        receipt = {
            "method": "GET", "url": url, "final_url": str(response.request.url), "status": capture.status_code,
            "attempt": attempt + 1, "observed_at": datetime.now(timezone.utc).isoformat(), "bytes": len(body),
            "sha256": _digest(body), "wire_bytes": len(wire), "wire_sha256": _digest(wire),
            "content_type": response.headers.get("content-type"), "content_encoding": capture.content_encoding,
            "complete": capture.complete, "error_code": capture.error_code,
        }
        budget[0] += len(body) + len(wire)
        if budget[0] > TOTAL_CAPTURE_BUDGET:
            raise ValueError("source_total_capture_budget_exceeded")
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}", body)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}.wire", wire)
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"raw/{artifact_name}.receipt.json", _json_bytes(receipt))
        if status_ok:
            _validate_document(name, body)
            return body
        if capture.status_code in {429, 500, 502, 503, 504} and attempt < 2:
            time.sleep(0.25 * (attempt + 1))
            continue
        raise ValueError("incomplete_source_response:" + name)
    raise ValueError("source_retry_limit_exceeded:" + name)


def _reserve(budget: list[int], upcoming: int) -> None:
    if budget[0] + upcoming > TOTAL_CAPTURE_BUDGET:
        raise ValueError("source_total_capture_budget_exceeded")


class _RetainedCaptureClient:
    """Read immutable successful responses from an earlier retained capture."""

    def __init__(self, staging, source_version: str):
        self.staging = staging
        self.source_version = source_version
        self.retained_capture = self

    def read_response(self, name: str):
        root = self.staging.build_target_location(DATASET_SLUG, FILE_SLUG, self.source_version, "")
        try:
            body = self.staging.read_key(root + f"raw/{name}")
            wire = self.staging.read_key(root + f"raw/{name}.wire")
            receipt = json.loads(self.staging.read_key(root + f"raw/{name}.receipt.json"))
        except (OSError, ValueError) as exc:
            raise ValueError("retained_source_response_missing:" + name) from exc
        if (receipt.get("method") != "GET" or receipt.get("complete") is not True
                or type(receipt.get("status")) is not int or not 200 <= receipt["status"] < 300
                or receipt.get("bytes") != len(body) or receipt.get("wire_bytes") != len(wire)
                or receipt.get("sha256") != _digest(body) or receipt.get("wire_sha256") != _digest(wire)):
            raise ValueError("retained_source_receipt_mismatch:" + name)
        if receipt.get("url") != _request_url(name):
            raise ValueError("retained_source_request_identity_mismatch:" + name)
        return body, wire, receipt


def _validate_capture_receipt(name: str, body: bytes, wire: bytes, receipt: dict) -> None:
    if (receipt.get("url") != _request_url(name) or type(receipt.get("status")) is not int
            or receipt["status"] != 200 or receipt.get("complete") is not True
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _digest(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _digest(wire)):
        raise ValueError(f"capture_receipt_drift:{name}")


def _write_source_manifest(staging, version_key: str, capture_receipts: dict[str, dict],
                           identity: dict, record_digest: str, raw_source_version: str | None,
                           parquet: bytes) -> dict:
    def input_note(name: str) -> str:
        if name == "ncrds_directory":
            return ("USGS Energy Resources Program ArcGIS Server directory listing of the NCRDS service family "
                    "(producer service-directory evidence).")
        if name.startswith("service_"):
            return ("USGS Energy Resources Program NCRDS 'CoalByRank' MapServer service document (producer "
                    "publication metadata citing the origin OFR 2012-1205).")
        if name.startswith("layer_"):
            return ("USGS Energy Resources Program NCRDS 'CoalByRank' MapServer layer 0 'CoalFieldsbyRank' "
                    "document (served schema, extent and capabilities).")
        if name.startswith("count_"):
            return "USGS Energy Resources Program NCRDS 'CoalByRank' layer feature-count query (completeness bracket)."
        if name.startswith("ids_"):
            return "USGS Energy Resources Program NCRDS 'CoalByRank' layer OBJECTID roster query (completeness bracket)."
        return ("USGS Energy Resources Program NCRDS 'CoalByRank' layer paged feature query (the assigned "
                "coal-fields polygons and attributes as served).")

    inputs = []
    for name in CAPTURE_NAMES:
        receipt = capture_receipts[name]
        inputs.append({
            "input_id": f"{name}:{receipt['sha256']}", "producer": PUBLISHER,
            "host": urlsplit(receipt["url"]).hostname, "role": "producer_publication",
            "canonical_url": receipt["url"], "retrieval_url": receipt["url"],
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"], "raw_path": f"raw/{name}",
            "license": LICENSE,
            "attribution": ATTRIBUTION,
            "release_date": RELEASE_DATE,
            "release_date_basis": RELEASE_DATE_BASIS + " " + input_note(name),
        })
    manifest = {
        "schema_version": 1, "processor": "HIFLD Next", "approved": False,
        "title": "US Coal Fields — USGS NCRDS CoalByRank maintained service capture (unapproved draft)",
        "description": "Complete paged capture of the maintained USGS Energy Resources Program NCRDS "
                       "'CoalByRank' MapServer layer 'CoalFieldsbyRank' (602 coal-field polygons, served Web "
                       "Mercator EPSG:3857) with the typed GeoParquet candidate and full provenance. The origin "
                       "OFR 2012-1205 publication and all retired/mirrored copies are never fetched. Edition, "
                       "HIFLD continuity, baseline, schema/ID contract, coverage, rights and release decisions "
                       "remain pending maintainer acceptance.",
        "agency": "U.S. Geological Survey (producer; authority acceptance pending)",
        "publisher": PUBLISHER,
        "source_url": SERVICE_URL,
        "documentation_url": SERVICE_URL,
        "license": LICENSE,
        "date_issued": RELEASE_DATE, "date_modified": None,
        "code_commit": identity["code_commit"], "code_identity_scheme": identity["code_identity_scheme"],
        "processing_code": identity, "inputs": inputs,
        "transformations": [{"operation": "Complete paged capture of the maintained USGS NCRDS CoalByRank layer "
                                          "to a staged native GeoParquet candidate",
                             "parameters": {
                                 "brackets": "Producer directory listing, service and layer documents, count and "
                                             "OBJECTID-roster queries are captured before and after the feature "
                                             "pages and must be unchanged across the capture; the producer-volatile "
                                             "layer drawingInfo.renderer.authoringInfo.colorRamp (renderer "
                                             "presentation rotated per request) is excluded from that comparison.",
                                 "attributes": "Served AREA, PERIMETER, Shape_Length and Shape_Area preserved as "
                                               "IEEE-754 doubles; PROVINCE, NAME, AGE and RANK preserved verbatim; "
                                               "no null or non-finite served value is tolerated.",
                                 "identifiers": "OBJECTID is the served esriFieldTypeOID row identifier, retained "
                                                "verbatim in served order (1..602, equal to source record order). "
                                                "No synthetic identifier is generated.",
                                 "geometry": "Served Esri JSON rings decoded without reprojection or repair: "
                                             "clockwise shells and counterclockwise holes grouped by containment "
                                             "into Polygon/MultiPolygon WKB in the served Web Mercator CRS "
                                             "(EPSG:3857). The one source-published ring self-intersection "
                                             "(record 95, Great Falls Field) is retained unchanged.",
                                 "candidate_status": "Unapproved draft; no output or release format approval implied."}}],
        "lineage": {
            "scope": "The 'CoalFieldsbyRank' polygon layer (id 0) of the USGS Energy Resources Program NCRDS "
                     "'CoalByRank' map service; 602 coal-field polygons with AREA, PERIMETER, PROVINCE, NAME, AGE "
                     "and RANK attributes (plus service-generated Shape_Length/Shape_Area) served in Web Mercator "
                     "for the conterminous United States.",
            "maintained_publication": _MAINTAINED_SERVICE_NOTE,
            "origin_publication": _ORIGIN_PUBLICATION_NOTE,
            "producer_publication_service": SERVICE_URL,
            "hifld_continuity": {
                "basis": "The DHS crosswalk row for 'US Coal Fields' records Status Deprecate with New Hosting N/A "
                         "and justification 'No longer supported as part of the HIFLD mission', and names "
                         "https://pubs.usgs.gov/of/2012/1205/Downloads/ as the external landing page. The retired "
                         "HIFLD FeatureServer export (602 features) was previously verified to match the OFR "
                         "2012-1205 shapefile row-for-row with identical attributes and a vertex-faithful "
                         "NAD83-to-CRS84 datum offset (298,530/298,530 vertices); that export is retired and "
                         "never fetched.",
                "hifld_continuity_accepted": False,
            },
            "banned_copies_never_fetched": _BANNED_COPIES_NOTE,
            "source_release_date_known": True,
            "source_release_date": RELEASE_DATE,
            "source_release_date_basis": RELEASE_DATE_BASIS,
            "source_record_sha256": record_digest,
            "source_geometry_sha256": EXPECTED_GEOMETRY_SHA256,
            "source_schema_sha256": EXPECTED_SCHEMA_SHA256,
            "raw_capture_mode": "retained_response_replay" if raw_source_version else "live_http_capture",
            "retained_source_capture_version": raw_source_version,
            "failed_retry_responses": [],
            "source_contract_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "coverage_accepted": False,
            "rights_accepted": False, "baseline_accepted": False, "release_accepted": False,
        },
        "unsupported_output_formats": {
            "shapefile_zip": "The producer publishes this layer as a live service; no approved packaging transformation exists.",
            "geopackage": "No approved container-schema or release mapping exists.",
            "pmtiles": "No approved reprojection, quantization, or visualization mapping exists.",
        },
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Draft GeoParquet decoded directly from the served Esri JSON geometry in the served Web "
                      "Mercator CRS; the field mapping and CRS are unapproved and the candidate is not accepted "
                      "for release.",
            "objects": [{"path": f"geoparquet/{FILE_SLUG}.parquet", "bytes": len(parquet), "sha256": _digest(parquet)}],
        }}},
    }
    validate_provenance(manifest)
    staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/source_manifest.json", _json_bytes(manifest))
    return manifest


def _capture(staging, version_key: str, client: httpx.Client | None = None,
             retained_source_version: str | None = None) -> dict:
    """Fetch, validate, and stage one complete pinned maintained-service capture."""
    owned = client is None
    client = client or httpx.Client(timeout=300, follow_redirects=False)
    try:
        budget = [0]
        bodies = {}
        for name in CAPTURE_NAMES:
            bodies[name] = _request(client, staging, version_key, name, budget)
        capture_set = validate_capture_set(bodies)
        capture_receipts = {}
        for name in bodies:
            root = staging.build_target_location(DATASET_SLUG, FILE_SLUG, version_key, "")
            body = staging.read_key(root + f"raw/{name}")
            wire = staging.read_key(root + f"raw/{name}.wire")
            receipt = json.loads(staging.read_key(root + f"raw/{name}.receipt.json"))
            _validate_capture_receipt(name, body, wire, receipt)
            capture_receipts[name] = receipt
        record_digest = _digest(_canonical([row["attributes"] for row in capture_set["features"]]))
        parquet = _make_parquet(capture_set["features"], capture_set["geometries"])
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, f"geoparquet/{FILE_SLUG}.parquet", parquet)
        processor_identity = processing_identity(Path(__file__).parents[3])
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/native_capture.json", _json_bytes({
            "source_service_url": SERVICE_URL,
            "source_service_layer": "CoalFieldsbyRank (id 0)",
            "producer": "U.S. Geological Survey (Energy Resources Program)",
            "content_origin_publication": (
                "USGS Open-File Report 2012-1205 (East, J.A., 2013), first posted April 2, 2013; "
                "https://pubs.usgs.gov/of/2012/1205/"),
            "source_release_date": RELEASE_DATE,
            "source_release_date_known": True,
            "service_crs_wkid": SERVICE_CRS_WKID,
            "service_crs_latest_wkid": SERVICE_CRS_LATEST_WKID,
            "record_count": EXPECTED_RECORD_COUNT,
            "expected_count_sha256": EXPECTED_COUNT_SHA256,
            "expected_id_sha256": EXPECTED_ID_SHA256,
            "expected_record_sha256": EXPECTED_RECORD_SHA256,
            "expected_geometry_sha256": EXPECTED_GEOMETRY_SHA256,
            "expected_schema_sha256": EXPECTED_SCHEMA_SHA256,
            "served_fields": EXPECTED_FIELDS,
            "geometry_bounds": EXPECTED_BOUNDS,
            "source_published_invalid_records": list(EXPECTED_INVALID_RECORD_NUMBERS),
            "attribute_value_counts": {"PROVINCE": PROVINCE_COUNTS, "AGE": AGE_COUNTS, "RANK": RANK_COUNTS,
                                       "distinct_NAME": DISTINCT_NAME_COUNT},
            "record_digest": record_digest, "parquet_sha256": _digest(parquet),
            "capture_mode": "retained_response_replay" if retained_source_version else "live_http_capture",
            "retained_source_capture_version": retained_source_version,
            "coverage_statement": "Complete observed USGS NCRDS CoalByRank 'CoalFieldsbyRank' polygon layer (602 "
                                  "coal-field polygons, conterminous United States, Web Mercator); no claim is "
                                  "made beyond this published layer.",
            "code_identity": processor_identity,
            "refresh_semantics": "Default runs capture a fresh source snapshot. An explicit retained_response_replay "
                                 "is an evidence-only reprocessing operation, not a cache or currentness claim.",
            "historical_hifld_continuity_accepted": False,
            "source_contract_accepted": False,
            "schema_contract_accepted": False,
            "id_contract_accepted": False,
            "coverage_accepted": False,
            "rights_accepted": False,
            "release_accepted": False,
        }))
        _write_source_manifest(staging, version_key, capture_receipts, processor_identity, record_digest,
                               retained_source_version, parquet)
        return {
            "feature_count": len(capture_set["features"]), "records_sha256": record_digest,
            "parquet_sha256": _digest(parquet), "release_accepted": False,
            "source_population_complete": True,
            "source_coverage_scope": "Complete observed USGS NCRDS CoalByRank 'CoalFieldsbyRank' polygon layer "
                                     "(602 coal-field polygons, conterminous United States, Web Mercator).",
        }
    finally:
        if owned:
            client.close()


def _quarantined_capture(staging, version_key: str, client: httpx.Client | None,
                         retained_source_version: str | None) -> dict:
    if isinstance(staging, PublishedStorageResource) or not staging.use_local or staging.bucket:
        raise ValueError("us_coal_fields_draft_requires_isolated_local_staging")
    try:
        if retained_source_version is not None:
            if client is not None:
                raise ValueError("retained_replay_does_not_accept_http_client")
            client = _RetainedCaptureClient(staging, retained_source_version)
        return _capture(staging, version_key, client, retained_source_version)
    except Exception as exc:
        error = str(exc).split(":", 1)[0]
        staging.write(DATASET_SLUG, FILE_SLUG, version_key, "metadata/quarantine.json", _json_bytes({
            "status": "quarantined", "source_service_url": SERVICE_URL,
            "observed_at": datetime.now(timezone.utc).isoformat(), "failure_code": error,
            "capture_module_sha256": _digest(Path(__file__).read_bytes()),
            "release_accepted": False,
        }))
        raise


def capture(staging, version_key: str, client: httpx.Client | None = None,
            retained_source_version: str | None = None) -> dict:
    """Baseline capture primitive: one complete pinned maintained-service capture
    with fail-closed identity, roster, schema, geometry and value validation,
    retaining failed attempts as quarantine evidence."""
    return _quarantined_capture(staging, version_key, client, retained_source_version)


def capture_validated(staging, version_key: str, client: httpx.Client | None = None,
                      retained_source_version: str | None = None) -> dict:
    """Production capture entry: capture() plus a staged-bytes readback proving
    the digests of the candidate that was actually persisted. Retained replay
    replays the earlier capture's exact bodies."""
    result = _quarantined_capture(staging, version_key, client, retained_source_version)
    root = staging.build_target_location(DATASET_SLUG, FILE_SLUG, version_key, "")
    bodies = {name: staging.read_key(root + f"raw/{name}") for name in CAPTURE_NAMES}
    revalidated = validate_capture_set(bodies)
    if _digest(_canonical([row["attributes"] for row in revalidated["features"]])) != result["records_sha256"]:
        raise ValueError("staged_source_records_changed")
    return result
