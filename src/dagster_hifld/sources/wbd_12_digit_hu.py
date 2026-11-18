"""Complete streaming capture of the USGS WBD 12-digit HU (Subwatershed) layer.

The HIFLD Open inventory entry ``wbd/12-digit-hu-subwatershed`` ("12-digit HU
(Subwatershed)", publisher "U.S. Geological Survey, National Geospatial
Technical Operations Center") carries the Watershed Boundary Dataset
description. The DHS HIFLD Open crosswalk (August 2025,
https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx)
marks the retired GeoPlatform-hosted HIFLD item 409515c7627c444fb08b66cf89634028
as Deprecate and names the USGS hydro.nationalmap.gov ``wbd`` MapServer as the
agency's current open REST service; its layer 6 "12-digit HU (Subwatershed)" is
the exact maintained producer publication captured here. The service documents
itself as compiled from USGS NHD and USDA NRCS WBD sources and is refreshed by
USGS ("Data refreshed October, 2026" in the observed copyrightText). DataLumos
project 240693 is archive identification context only and is never fetched.

Scale (observed 2026-11-14): the layer holds 103,068 polygon features carrying
about 125M web-mercator vertices, so the declared ``maxRecordCount`` of 2000
cannot actually be served for this geometry-heavy layer (queries at 1000 and
2000 features fail server-side with "Error performing query operation"; 500
features decode to about 24 MB of JSON). This module therefore pins
``PAGE_SIZE = 500`` and streams the capture: every page is retained as raw
evidence in a capture directory, then validated page-by-page into a native
geoparquet candidate with streaming record/geometry/ID digests instead of
holding the population in memory. Ring assembly keeps the producer's winding
and hole ownership; rows that still cannot form a valid OGC polygon gate to
null geometry and are counted by failure mode instead of being repaired, with
the counts pinned for later drift refusal after the first complete walk.

The producer additionally refuses some full-size page windows outright:
queries return HTTP 200 with ``{"error": {"code": 500, "message": "Error
performing query operation"}}`` for windows whose served features are
individually large enough that the combined response cannot be constructed
server-side (content-dependent and deterministic, observed 2026-11-14: 29 of
207 nominal 500-feature windows refused during the first complete walk while
every refused window served at smaller record counts down to single features,
the largest observed serving about 10 MB for one subwatershed polygon). The
capture therefore bisects a refused window recursively into two half-size
sub-windows (``a`` = lower OBJECTIDs, ``b`` = upper OBJECTIDs), retaining
every refusal document as evidence and each served sub-window as a named page
part whose URL is re-derived from the pinned roster so a replay needs no
extra trusted state. A single-feature window is retried bounded times first;
when its geometry stays refused its attributes are captured through the
producer's working attributes-only channel and the row's geometry is gated to
null with the OBJECTID recorded (the merged USGS 3DHP waterbody precedent for
permanently unservable rows), never invented, dropped or silently repaired.
Individual layer resources
also omit the top-level ``spatialReference`` the service root carries; the
layer's source/extent references and every page carry validated CRS data.

The per-value record and geometry baseline digests, the ID roster digest and
the geometry-gate counts are pinned below from the complete dated population
walk (2026-11-14). Publication stays blocked pending succession, freshness,
schema/ID contract, coverage, rights and release acceptance; all runtime
approval flags are false and the draft refresh schedule is stopped.

The service validation, refused-window capture/bisection, retained-capture
replay, streaming validation and Esri polygon assembly primitives are shared
with the other WBD hydrologic-unit keyset-window layer through
``dagster_hifld.sources.wbd_hu_family``; this module supplies only the layer
identity, HUC code width, schema/baseline pins, page and capture budgets and
the evidence text below.
"""

from __future__ import annotations

from pathlib import Path

import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import wbd_hu_family as family

DATASET_SLUG = FILE_SLUG = "wbd-12-digit-hu-subwatershed"
PUBLISHER = "U.S. Geological Survey, National Geospatial Technical Operations Center"
AGENCY = "U.S. Geological Survey"
# The USGS page that publishes both this service and the producer's staged
# products; documentation evidence only, the data comes from the service.
SERVICE_DOC_URL = "https://www.usgs.gov/national-hydrography/access-national-hydrography-products"
WBD_PROGRAM_DOC_URL = "https://www.usgs.gov/national-hydrography/watershed-boundary-dataset"
SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer"
LAYER_ID = 6
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "12-digit HU (Subwatershed)"
MAP_NAME = "wbd"
DHS_CROSSWALK_EVIDENCE_URL = ("https://atcoordinates.info/wp-content/uploads/2025/08/"
                              "hifld_open_crosswalk_geoplatform.xlsx")
EXPECTED_COUNT = 103_068
EXPECTED_ID_MIN = 1
EXPECTED_ID_MAX = 106_609
# Digest of the canonical-JSON sorted OBJECTID roster (103,068 unique ids,
# 1..106,609), computed from the complete dated population walk (2026-11-14).
EXPECTED_ID_SHA256 = "7b0e4a783d86a92a383a69c8a477ef74c57839839aa95b129bda2f152ef8e533"
PAGE_SIZE = 500
DECLARED_MAX_RECORD_COUNT = 2000
EXPECTED_EXTENT = {"xmin": -19951755.682, "ymin": -1644316.000599999,
                   "xmax": 20021551.7474, "ymax": 11554273.612599999}
# The declared extent is rounded relative to the served vertices by a few
# decimal places; in web-mercator meters a 1e-3 guard is far below any real
# coverage change while above the rounding noise.
EXPECTED_EXTENT_TOLERANCE = 1e-3
EXPECTED_COPYRIGHT_TEXT = "USGS WBD - Watershed Boundary Dataset. Data refreshed October, 2026."
EXPECTED_SERVICE_LAYERS = [
    (0, "WBDLine"), (1, "2-digit HU (Region)"), (2, "4-digit HU (Subregion)"),
    (3, "6-digit HU (Basin)"), (4, "8-digit HU  (Subbasin)"), (5, "10-digit HU (Watershed)"),
    (6, "12-digit HU (Subwatershed)"), (7, "14-digit HU"), (8, "16-digit HU"),
]
NATIVE_WKID = 102100
NATIVE_LATEST_WKID = 3857
SOURCE_CRS_WKID = 3857
DISPLAY_FIELD_NAME = "NAME"
USER_AGENT = "hifld-next-datasets/wbd-12-digit-hu-draft"
DOC_USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
PAGE_MAX_BYTES = 48_000_000
DOC_MAX_BYTES = 4_000_000
# Retained-evidence budget. The 2026-11-14 complete walk showed the nominal
# 207-page population alone retains about 10 GB of body+wire evidence and the
# tail's giant subwatersheds are re-fetched as bisected single-feature parts
# (each up to about 10 MB served) on top of the retained refusal documents,
# pushing the complete walk past the draft's 12 GB budget; 16 GB bounds the
# same runaway-evidence risk while fitting the observed complete walk.
CAPTURE_MAX_BYTES = 16_000_000_000
DISK_FLOOR_BYTES = 24_000_000_000
# The live service computes shape_Area/shape_Length planarly in the layer's
# web-mercator SR; the observed parity against a shapely recomputation peaked
# at 5.9e-15 relative across a 500-feature probe (2026-11-14), so the parity
# guard sits far above that and far below real drift.
SHAPE_PARITY_TOLERANCE = 1e-6
# The producer sometimes refuses even a single-feature geometry window
# (observed 2026-11-14: OBJECTID 46057 persistently returned the error
# document across three spaced attempts while its attributes-only query and
# its roster/count entries stayed servable). Before staging the refusal the
# walk retries the single-feature geometry window this many times with this
# delay, then captures the row's attributes through the producer's working
# attributes-only channel and gates the row's geometry to null.
SINGLE_REFUSAL_RETRIES = 2
SINGLE_REFUSAL_RETRY_SECONDS = 10.0
# Native baselines pinned from the complete dated population walk (2026-11-14,
# sealed candidate v5eee4c1c755f33decc2af5addc3fb186: 103,068 features with
# 155,028,829 served web-mercator vertices; 103,056 valid polygons, 12 rows
# gate to null geometry, of which 5 are producer-refused single-feature rows
# whose attributes were captured through the attributes-only channel and 7 are
# assembly-gated served rings). Pins are checked only when non-empty.
EXPECTED_SCHEMA_SHA256 = "c95002464d58e1cff306035acc325e7070a6c3764a5fa02277be612a8e7c6f59"
EXPECTED_RECORD_SHA256 = "393ff32e209b061ffe28dc9c8218a8f139e35e38e66f57a6d05a85f545a0c800"
# Geometry digest over the raw served rings of every row that produces a
# candidate geometry, one [objectid, served rings] document per feature in
# ascending OBJECTID order, computed from the complete dated population walk
# (2026-11-14).
# The digest covered only post-gate assembled rows before the shared GDAL
# converter (issue #997); every served row now assembles, so the coverage grew
# and this transcribed pin moves to the designed pending state until the next
# complete capture re-derives it.
EXPECTED_GEOMETRY_SHA256 = None
# Polygon-ring quarantines retired by the shared GDAL Esri polygon converter
# (issue #997): the 7 rows the retired assembler gated (4 assembly-invalid,
# 3 self-intersecting exteriors) now convert through GDAL; any that stay
# OGC-invalid are retained verbatim in ogc_invalid_geometry_counts. The 5
# producer-refused attributes-only windows keep their explicit null geometry
# and pinned count.
EXPECTED_NULL_GEOMETRY_COUNT = 5
EXPECTED_UNUSABLE_GEOMETRY_COUNTS = {"source_page_window_refused_geometry": 5}
# Refused single-feature windows (attributes-only rows published with null
# geometry), pinned from the complete dated population walk; checked only when
# non-empty.
EXPECTED_REFUSED_GEOMETRY_OIDS = (46057, 101177, 106424, 106517, 106521)

EXPECTED_PAGE_FIELD_DECLARATIONS = [{"name": "objectid", "type": "esriFieldTypeOID", "alias": "ObjectID"}, {"name": "tnmid", "type": "esriFieldTypeString", "alias": "TNMID", "length": 40}, {"name": "metasourceid", "type": "esriFieldTypeString", "alias": "MetaSourceID", "length": 40}, {"name": "sourcedatadesc", "type": "esriFieldTypeString", "alias": "SourceDataDesc", "length": 100}, {"name": "sourceoriginator", "type": "esriFieldTypeString", "alias": "SourceOriginator", "length": 130}, {"name": "sourcefeatureid", "type": "esriFieldTypeString", "alias": "SourceFeatureID", "length": 40}, {"name": "loaddate", "type": "esriFieldTypeDate", "alias": "LoadDate", "length": 8}, {"name": "referencegnis_ids", "type": "esriFieldTypeString", "alias": "ReferenceGNIS_IDs", "length": 50}, {"name": "areaacres", "type": "esriFieldTypeDouble", "alias": "AreaAcres"}, {"name": "areasqkm", "type": "esriFieldTypeDouble", "alias": "AreaSqKm"}, {"name": "states", "type": "esriFieldTypeString", "alias": "States", "length": 50}, {"name": "huc12", "type": "esriFieldTypeString", "alias": "HUC12", "length": 12}, {"name": "name", "type": "esriFieldTypeString", "alias": "Name", "length": 120}, {"name": "hutype", "type": "esriFieldTypeString", "alias": "HUType", "length": 1}, {"name": "humod", "type": "esriFieldTypeString", "alias": "HUMod", "length": 30}, {"name": "tohuc", "type": "esriFieldTypeString", "alias": "ToHUC", "length": 16}, {"name": "noncontributingareaacres", "type": "esriFieldTypeDouble", "alias": "NonContributingAreaAcres"}, {"name": "noncontributingareasqkm", "type": "esriFieldTypeDouble", "alias": "NonContributingAreaSqKm"}, {"name": "globalid", "type": "esriFieldTypeGlobalID", "alias": "globalid", "length": 38}, {"name": "shape_Length", "type": "esriFieldTypeDouble", "alias": "shape_Length"}, {"name": "shape_Area", "type": "esriFieldTypeDouble", "alias": "shape_Area"}]
EXPECTED_LAYER_FIELDS = [{"name": "objectid", "type": "esriFieldTypeOID", "alias": "ObjectID", "domain": None}, {"name": "tnmid", "type": "esriFieldTypeString", "alias": "TNMID", "length": 40, "domain": None}, {"name": "metasourceid", "type": "esriFieldTypeString", "alias": "MetaSourceID", "length": 40, "domain": None}, {"name": "sourcedatadesc", "type": "esriFieldTypeString", "alias": "SourceDataDesc", "length": 100, "domain": None}, {"name": "sourceoriginator", "type": "esriFieldTypeString", "alias": "SourceOriginator", "length": 130, "domain": None}, {"name": "sourcefeatureid", "type": "esriFieldTypeString", "alias": "SourceFeatureID", "length": 40, "domain": None}, {"name": "loaddate", "type": "esriFieldTypeDate", "alias": "LoadDate", "length": 8, "domain": None}, {"name": "referencegnis_ids", "type": "esriFieldTypeString", "alias": "ReferenceGNIS_IDs", "length": 50, "domain": None}, {"name": "areaacres", "type": "esriFieldTypeDouble", "alias": "AreaAcres", "domain": None}, {"name": "areasqkm", "type": "esriFieldTypeDouble", "alias": "AreaSqKm", "domain": None}, {"name": "states", "type": "esriFieldTypeString", "alias": "States", "length": 50, "domain": None}, {"name": "huc12", "type": "esriFieldTypeString", "alias": "HUC12", "length": 12, "domain": None}, {"name": "name", "type": "esriFieldTypeString", "alias": "Name", "length": 120, "domain": None}, {"name": "hutype", "type": "esriFieldTypeString", "alias": "HUType", "length": 1, "domain": {"type": "codedValue", "name": "HUType Domain", "description": "", "codedValues": [{"name": "Standard", "code": "S"}, {"name": "Closed Basin", "code": "C"}, {"name": "Frontal", "code": "F"}, {"name": "Multiple Outlet", "code": "M"}, {"name": "Water", "code": "W"}, {"name": "Island", "code": "I"}, {"name": "Urban", "code": "U"}, {"name": "Indeterminate", "code": "D"}], "mergePolicy": "esriMPTDefaultValue", "splitPolicy": "esriSPTDuplicate"}}, {"name": "humod", "type": "esriFieldTypeString", "alias": "HUMod", "length": 30, "domain": None}, {"name": "tohuc", "type": "esriFieldTypeString", "alias": "ToHUC", "length": 16, "domain": None}, {"name": "noncontributingareaacres", "type": "esriFieldTypeDouble", "alias": "NonContributingAreaAcres", "domain": None}, {"name": "noncontributingareasqkm", "type": "esriFieldTypeDouble", "alias": "NonContributingAreaSqKm", "domain": None}, {"name": "shape", "type": "esriFieldTypeGeometry", "alias": "Shape", "domain": None}, {"name": "globalid", "type": "esriFieldTypeGlobalID", "alias": "globalid", "length": 38, "domain": None}, {"name": "shape_Length", "type": "esriFieldTypeDouble", "alias": "shape_Length", "domain": None}, {"name": "shape_Area", "type": "esriFieldTypeDouble", "alias": "shape_Area", "domain": None}]
ATTRIBUTE_FIELDS = EXPECTED_PAGE_FIELD_DECLARATIONS
FIELD_NAMES = tuple(field["name"] for field in ATTRIBUTE_FIELDS)
DIGEST_SEPARATOR = family.DIGEST_SEPARATOR

PAGE_COUNT = (EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE
METADATA_NAMES = family.METADATA_NAMES
CAPTURE_NAMES = {*METADATA_NAMES, *(f"page_{index:04d}" for index in range(PAGE_COUNT))}

GATE_MODES = frozenset({family.REFUSED_GEOMETRY_GATE_MODE})
_EPSG3857_PROJJSON = family.EPSG3857_PROJJSON

_COVERAGE_SCOPE = ("Complete observed USGS wbd MapServer layer 6 "
                   "'12-digit HU (Subwatershed)': every hydrologic-unit "
                   "subwatershed polygon the producer publishes for the United "
                   "States, Puerto Rico and the U.S. Virgin Islands; not "
                   "asserted beyond that layer.")
_DIGEST_SCHEME = ("sha256 of the concatenation of one canonical "
                  "compact-JSON document per feature in ascending "
                  "OBJECTID order, each followed by a 0x00 byte; the "
                  "record document is the served attribute object and "
                  "the geometry document is [objectid, served rings]")


def _spec() -> family.WbdHuSpec:
    return family.WbdHuSpec(
        dataset_slug=DATASET_SLUG, file_slug=FILE_SLUG, publisher=PUBLISHER,
        agency=AGENCY, service_doc_url=SERVICE_DOC_URL,
        wbd_program_doc_url=WBD_PROGRAM_DOC_URL,
        dhs_crosswalk_evidence_url=DHS_CROSSWALK_EVIDENCE_URL,
        service_url=SERVICE_URL, layer_id=LAYER_ID, layer_name=LAYER_NAME,
        map_name=MAP_NAME, expected_count=EXPECTED_COUNT,
        expected_id_min=EXPECTED_ID_MIN, expected_id_max=EXPECTED_ID_MAX,
        expected_id_sha256=EXPECTED_ID_SHA256, page_size=PAGE_SIZE,
        page_count=PAGE_COUNT, declared_max_record_count=DECLARED_MAX_RECORD_COUNT,
        expected_extent=EXPECTED_EXTENT,
        expected_extent_tolerance=EXPECTED_EXTENT_TOLERANCE,
        expected_layer_extent=EXPECTED_EXTENT,
        expected_copyright_text=EXPECTED_COPYRIGHT_TEXT,
        expected_layer_copyright_text="U.S. Geological Survey",
        expected_service_layers=tuple(EXPECTED_SERVICE_LAYERS),
        native_wkid=NATIVE_WKID, native_latest_wkid=NATIVE_LATEST_WKID,
        source_crs_wkid=SOURCE_CRS_WKID, display_field_name=DISPLAY_FIELD_NAME,
        oid_field_name="objectid",
        user_agent=USER_AGENT, doc_user_agent=DOC_USER_AGENT,
        page_max_bytes=PAGE_MAX_BYTES, doc_max_bytes=DOC_MAX_BYTES,
        capture_max_bytes=CAPTURE_MAX_BYTES, disk_floor_bytes=DISK_FLOOR_BYTES,
        shape_parity_tolerance=SHAPE_PARITY_TOLERANCE,
        expected_schema_sha256=EXPECTED_SCHEMA_SHA256,
        expected_record_sha256=EXPECTED_RECORD_SHA256,
        expected_geometry_sha256=EXPECTED_GEOMETRY_SHA256,
        expected_null_geometry_count=EXPECTED_NULL_GEOMETRY_COUNT,
        expected_unusable_geometry_counts=EXPECTED_UNUSABLE_GEOMETRY_COUNTS,
        expected_refused_geometry_oids=EXPECTED_REFUSED_GEOMETRY_OIDS,
        attribute_fields=tuple(EXPECTED_PAGE_FIELD_DECLARATIONS),
        expected_layer_fields=EXPECTED_LAYER_FIELDS,
        gate_modes=GATE_MODES,
        parts_policy=family.PARTS_POLICY_BISECT_ATTRS,
        max_bisection_depth=10**6, single_refusal_retries=SINGLE_REFUSAL_RETRIES,
        single_refusal_retry_seconds=SINGLE_REFUSAL_RETRY_SECONDS, attrs_parts=True,
        digest_geometry_before_gate=False, capture_root_family="376",
        baseline_pending_note=None, audit_page_refusal_count=True,
        coverage_scope=_COVERAGE_SCOPE, baseline_digest_scheme=_DIGEST_SCHEME,
        manifest_title="12-digit HU (Subwatershed)",
        manifest_description=(
            "Complete observed USGS Watershed Boundary Dataset 12-digit HU "
            "(Subwatershed) layer 6 from the USGS hydro.nationalmap.gov wbd "
            "MapServer named by the DHS HIFLD Open crosswalk; succession, "
            "freshness, contract, coverage, rights and release decisions "
            "remain unaccepted."),
        transformation_source_fields=(
            "all 21 served attributes retained in source order with "
            "declared physical types; esriFieldTypeDate served as "
            "epoch-milliseconds and represented as Arrow int64 "
            "without conversion; no field added or dropped"),
        transformation_source_ids=(
            "OBJECTID retained unchanged; served roster is 103,068 unique "
            "ids from 1 to 106,609; HUC12 and TNMID retained verbatim "
            "including leading zeros; no substitute or synthetic identifier"),
        transformation_raw_evidence=(
            "complete decoded response and wire bodies retained with "
            "checksummed receipts for every service document, page and "
            "bisected page part, including retained producer refusal "
            "documents for refused windows"),
        lineage=(
            "DHS HIFLD Open crosswalk (August 2025) row 'Watershed Boundary Dataset "
            "(WBD)' (agency U.S. Geological Survey) marks the retired GeoPlatform "
            "item 409515c7627c444fb08b66cf89634028 as Deprecate and names the USGS "
            "hydro.nationalmap.gov wbd MapServer as the agency's current open REST "
            "service; its layer 6 '12-digit HU (Subwatershed)' is the exact producer "
            "publication for the HIFLD Open inventory entry wbd/12-digit-hu-"
            "subwatershed (publisher U.S. Geological Survey, National Geospatial "
            "Technical Operations Center). The service documents itself as compiled "
            "from USGS NHD and USDA NRCS WBD sources. The old GeoPlatform item is "
            "not fetched; DataLumos project 240693 copies are research context only "
            "and are never fetched."),
        policy_reason=("Native attributes, OBJECTID/HUC12/TNMID identifiers and served rings "
                       "are retained in a typed EPSG:3857 candidate."),
    )


def _query(params: dict[str, object]) -> str:
    return family.query_url(LAYER_URL, params)


def _count_url() -> str:
    return family.count_url(_spec())


def _ids_url() -> str:
    return family.ids_url(_spec())


def _page_url(last_objectid: int, record_count: int = PAGE_SIZE) -> str:
    return family.page_window_url(_spec(), last_objectid, record_count)


def _capture_roster(roster: list[int]) -> list[tuple[str, str]]:
    return family.capture_roster(_spec(), roster)


def _capture(client: httpx.Client, url: str, root: Path, name: str,
             max_bytes: int = PAGE_MAX_BYTES) -> tuple[bytes, dict]:
    return family.capture_budgeted(root, max_bytes, CAPTURE_MAX_BYTES, client, url, name)


def _is_page_part_name(name: str) -> bool:
    return family.is_page_part_name(_spec(), name)


def _is_attrs_part_name(name: str) -> bool:
    return family.is_attrs_part_name(_spec(), name)


def _is_refusal(body: bytes) -> bool:
    return family.is_refusal(body)


def _retain_refusal(root: Path, name: str, body: bytes, receipt: dict) -> tuple[bytes, dict]:
    return family.retain_refusal(root, name, body, receipt)


def _page_part_url(roster: list[int], index: int, part_name: str) -> str:
    return family.page_part_url(_spec(), roster, index, part_name)


def _record_attrs_url(oid: int) -> str:
    return family.record_attrs_url(_spec(), oid)


def _capture_page_window(client: httpx.Client, roster: list[int], index: int,
                         root: Path) -> tuple[dict[str, tuple[bytes, dict]], list[str]]:
    """Capture one nominal page window, recursively bisecting refused windows."""
    return family.capture_page_window(_spec(), client, roster, index, root,
                                      capture=_capture)


def _validate_spatial_reference(value: object, label: str, *, required: bool = True) -> bool:
    return family.validate_spatial_reference(value, label, required=required,
                                             native_wkid=NATIVE_WKID,
                                             native_latest_wkid=NATIVE_LATEST_WKID,
                                             source_crs_wkid=SOURCE_CRS_WKID)


def _validate_service(service: dict, label: str) -> None:
    family.validate_service(_spec(), service, label)


def _validate_layer(layer: dict) -> None:
    family.validate_layer(_spec(), layer)


def _validate_roster_document(document: object, label: str) -> list[int]:
    return family.validate_roster_document(_spec(), document, label)


def _validate_count(document: object, label: str) -> int:
    return family.validate_count(_spec(), document, label)


def _validate_service_doc(body: bytes) -> None:
    family.validate_service_doc(body)


def _native_schema():
    return family.native_schema(_spec())


def _arrow_type(field: dict):
    return family.arrow_type(field)


def _validate_stream(root: Path, *, parquet_sink: Path | None,
                     page_parts: dict[str, list[str]] | None = None) -> tuple[dict, dict]:
    return family.validate_stream(_spec(), root, parquet_sink=parquet_sink,
                                  page_parts=page_parts)


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    return family._capture_identity_hash(captures, root)


def _read_replay(root: Path) -> tuple[dict[str, tuple[bytes, dict]], dict[str, list[str]],
                                      list[str]]:
    return family.read_replay(_spec(), root)


def capture_wbd_12_digit_hu(storage: StagingStorageResource, *, version: str,
                            capture_directory: str | None = None) -> dict[str, object]:
    """Capture or replay one complete, unapproved native WBD 12-digit candidate."""
    return family.capture_candidate(_spec(), storage, version=version,
                                    capture_directory=capture_directory, capture=_capture)
