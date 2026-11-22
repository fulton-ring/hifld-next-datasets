"""Complete capture of the USGS WBD 4-digit HU (Subregion) layer.

The HIFLD Open inventory entry ``wbd/4-digit-hu-subregion`` ("4-digit HU
(Subregion)", publisher "U.S. Geological Survey, National Geospatial Technical
Operations Center") carries the Watershed Boundary Dataset description. The DHS
HIFLD Open crosswalk (August 2025,
https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx)
marks the retired GeoPlatform-hosted HIFLD Watershed Boundary Dataset item as
Deprecate and names the USGS hydro.nationalmap.gov ``wbd`` MapServer as the
agency's current open REST service; its layer 2 "4-digit HU (Subregion)" is the
exact maintained producer publication captured here. The service documents
itself as compiled from USGS NHD and USDA NRCS WBD sources and is refreshed by
USGS ("Data refreshed October, 2026" in the observed copyrightText). DataLumos
project 240698 is archive identification context only and is never fetched.

Scale (observed 2026-11-15): the layer holds 245 polygon features (served
OBJECTID roster 1..246 with one unused OBJECTID), and the declared
``maxRecordCount`` of 2000 cannot actually be served for this geometry-heavy
layer: query windows that decode to more than roughly 28-40 MB of JSON fail
server-side with Esri error 500 "Error performing query operation" (observed:
a 50-feature window refused; 20-feature windows up to 32.4 MB of JSON served).
This module therefore pins ``PAGE_SIZE = 20`` and streams the capture: every
page is retained as raw evidence in a capture directory, then validated
page-by-page into a native geoparquet candidate with streaming
record/geometry/ID digests. A persistently refusing window is retained as
evidence and re-fetched as two half-size parts derived from the pinned roster
(one bisection level, never a repair loop); a refusing half fails the capture.
Ring assembly keeps the producer's winding and hole ownership; rows that still
cannot form a valid OGC polygon gate to null geometry and are counted by
failure mode instead of being repaired, with the counts pinned for later drift
refusal after the first complete walk. The geometry digest is updated from the
raw served rings of every row before assembly and gating, so a coordinate
change that still gates is refused by the pinned geometry baseline rather than
hiding behind unchanged aggregate gate counts.

Population completeness comes from the producer's own before/after count and
ID witnesses at capture time (see ``dagster_hifld.capture_completeness``):
the layer's observed population, however it has been edited upstream, is the
validated population, and the candidate's drift is reported by the
non-blocking approved-baseline asset check instead of a code pin. The
layer-schema digest is still pinned from the dated live service documents
(2026-11-15); the record/geometry baseline digest and geometry-gate count
slots stay empty (they are checked only when non-empty) so a maintainer can
pin them from an approved release without a code change. Publication stays
blocked pending succession, freshness, schema/ID contract, coverage, rights
and release acceptance; all runtime approval flags are false and the draft
refresh schedule is stopped.

The service validation, refused-window capture/bisection, retained-capture
replay, streaming validation and Esri polygon assembly primitives are shared
with the other WBD hydrologic-unit keyset-window layers through
``dagster_hifld.sources.wbd_hu_family``; this module supplies only the layer
identity, HUC code width, schema/baseline pins, page and capture budgets and
the evidence text above.
"""
from __future__ import annotations

from functools import partial
from pathlib import Path

import httpx

from dagster_hifld.sources import esri_geometry as crs_validation
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import wbd_hu_family as family

DATASET_SLUG = FILE_SLUG = "wbd-4-digit-hu-subregion"
PUBLISHER = "U.S. Geological Survey, National Geospatial Technical Operations Center"
AGENCY = "U.S. Geological Survey"
# The USGS page that publishes both this service and the producer's staged
# products; documentation evidence only, the data comes from the service.
SERVICE_DOC_URL = "https://www.usgs.gov/national-hydrography/access-national-hydrography-products"
WBD_PROGRAM_DOC_URL = "https://www.usgs.gov/national-hydrography/watershed-boundary-dataset"
SERVICE_URL = "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer"
LAYER_ID = 2
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "4-digit HU (Subregion)"
MAP_NAME = "wbd"
DHS_CROSSWALK_EVIDENCE_URL = ("https://atcoordinates.info/wp-content/uploads/2025/08/"
                              "hifld_open_crosswalk_geoplatform.xlsx")
# Optional baseline pins, checked only when non-empty; left empty so
# completeness comes from the producer's capture-time witnesses (issue 1094).
EXPECTED_ID_SHA256 = ""
# This 4-digit layer refuses any window whose JSON would exceed roughly
# 28-40 MB: 50-feature and some 10-feature windows failed with Esri 500
# "Error performing query operation" while other 20-feature windows up to
# 32.4 MB of JSON served (observed 2026-11-15); the largest single served
# feature is 5.1 MB (Arctic Alaska). Ten features per page with one-level
# bisection of a refusing window keeps every retained document, including
# five-feature halves of the largest ten-feature windows, inside the observed
# serving envelope.
PAGE_SIZE = 10
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
USER_AGENT = "hifld-next-datasets/wbd-4-digit-hu-subregion-draft"
DOC_USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
PAGE_MAX_BYTES = 48_000_000
DOC_MAX_BYTES = 4_000_000
CAPTURE_MAX_BYTES = 2_000_000_000
DISK_FLOOR_BYTES = 2_000_000_000
# The live service computes shape_Area/shape_Length planarly in the layer's
# web-mercator SR; the observed parity against a shapely recomputation peaked
# far below this guard across the served population (2026-11-14), so the
# parity guard sits far above the numeric noise and far below real drift.
SHAPE_PARITY_TOLERANCE = 1e-6
# Layer-schema digest pinned from the dated live layer document (observed
# 2026-11-15 UTC); the record/geometry baselines and gate counts stay unpinned
# until the first complete population walk; empty/None pins are checked only
# when non-empty.
EXPECTED_SCHEMA_SHA256 = "7457266cc07d5067461ce5549079e7ff814c1c8c68a8483bdf5e009e19e4b062"
EXPECTED_RECORD_SHA256 = ""
EXPECTED_GEOMETRY_SHA256 = ""
EXPECTED_NULL_GEOMETRY_COUNT = None
EXPECTED_UNUSABLE_GEOMETRY_COUNTS = None

EXPECTED_PAGE_FIELD_DECLARATIONS = [{"name": "objectid", "type": "esriFieldTypeOID", "alias": "ObjectID"}, {"name": "tnmid", "type": "esriFieldTypeString", "alias": "TNMID", "length": 40}, {"name": "metasourceid", "type": "esriFieldTypeString", "alias": "MetaSourceID", "length": 40}, {"name": "sourcedatadesc", "type": "esriFieldTypeString", "alias": "SourceDataDesc", "length": 100}, {"name": "sourceoriginator", "type": "esriFieldTypeString", "alias": "SourceOriginator", "length": 130}, {"name": "sourcefeatureid", "type": "esriFieldTypeString", "alias": "SourceFeatureID", "length": 40}, {"name": "loaddate", "type": "esriFieldTypeDate", "alias": "LoadDate", "length": 8}, {"name": "referencegnis_ids", "type": "esriFieldTypeString", "alias": "ReferenceGNIS_IDs", "length": 50}, {"name": "areaacres", "type": "esriFieldTypeDouble", "alias": "AreaAcres"}, {"name": "areasqkm", "type": "esriFieldTypeDouble", "alias": "AreaSqKm"}, {"name": "states", "type": "esriFieldTypeString", "alias": "States", "length": 50}, {"name": "huc4", "type": "esriFieldTypeString", "alias": "HUC4", "length": 4}, {"name": "name", "type": "esriFieldTypeString", "alias": "Name", "length": 120}, {"name": "globalid", "type": "esriFieldTypeGlobalID", "alias": "globalid", "length": 38}, {"name": "shape_Length", "type": "esriFieldTypeDouble", "alias": "shape_Length"}, {"name": "shape_Area", "type": "esriFieldTypeDouble", "alias": "shape_Area"}]
EXPECTED_LAYER_FIELDS = [{"name": "objectid", "type": "esriFieldTypeOID", "alias": "ObjectID", "domain": None}, {"name": "tnmid", "type": "esriFieldTypeString", "alias": "TNMID", "length": 40, "domain": None}, {"name": "metasourceid", "type": "esriFieldTypeString", "alias": "MetaSourceID", "length": 40, "domain": None}, {"name": "sourcedatadesc", "type": "esriFieldTypeString", "alias": "SourceDataDesc", "length": 100, "domain": None}, {"name": "sourceoriginator", "type": "esriFieldTypeString", "alias": "SourceOriginator", "length": 130, "domain": None}, {"name": "sourcefeatureid", "type": "esriFieldTypeString", "alias": "SourceFeatureID", "length": 40, "domain": None}, {"name": "loaddate", "type": "esriFieldTypeDate", "alias": "LoadDate", "length": 8, "domain": None}, {"name": "referencegnis_ids", "type": "esriFieldTypeString", "alias": "ReferenceGNIS_IDs", "length": 50, "domain": None}, {"name": "areaacres", "type": "esriFieldTypeDouble", "alias": "AreaAcres", "domain": None}, {"name": "areasqkm", "type": "esriFieldTypeDouble", "alias": "AreaSqKm", "domain": None}, {"name": "states", "type": "esriFieldTypeString", "alias": "States", "length": 50, "domain": None}, {"name": "huc4", "type": "esriFieldTypeString", "alias": "HUC4", "length": 4, "domain": None}, {"name": "name", "type": "esriFieldTypeString", "alias": "Name", "length": 120, "domain": None}, {"name": "shape", "type": "esriFieldTypeGeometry", "alias": "Shape", "domain": None}, {"name": "globalid", "type": "esriFieldTypeGlobalID", "alias": "globalid", "length": 38, "domain": None}, {"name": "shape_Length", "type": "esriFieldTypeDouble", "alias": "shape_Length", "domain": None}, {"name": "shape_Area", "type": "esriFieldTypeDouble", "alias": "shape_Area", "domain": None}]
ATTRIBUTE_FIELDS = EXPECTED_PAGE_FIELD_DECLARATIONS
FIELD_NAMES = tuple(field["name"] for field in ATTRIBUTE_FIELDS)
DIGEST_SEPARATOR = family.DIGEST_SEPARATOR



_EPSG3857_PROJJSON = family.EPSG3857_PROJJSON


GATE_MODES: frozenset = frozenset()
_COVERAGE_SCOPE = "Complete observed USGS wbd MapServer layer 2 '4-digit HU (Subregion)': every hydrologic-unit subregion polygon the producer publishes for the United States, Puerto Rico and the U.S. Virgin Islands; not asserted beyond that layer."
_DIGEST_SCHEME = 'sha256 of the concatenation of one canonical compact-JSON document per feature in ascending OBJECTID order, each followed by a 0x00 byte; the record document is the served attribute object and the geometry document is [objectid, served rings], hashed for every served row including rows that gate to null geometry'


def _spec() -> family.WbdHuSpec:
    return family.WbdHuSpec(
        dataset_slug=DATASET_SLUG, file_slug=FILE_SLUG, publisher=PUBLISHER,
        agency=AGENCY, service_doc_url=SERVICE_DOC_URL,
        wbd_program_doc_url=WBD_PROGRAM_DOC_URL,
        dhs_crosswalk_evidence_url=DHS_CROSSWALK_EVIDENCE_URL,
        service_url=SERVICE_URL, layer_id=LAYER_ID, layer_name=LAYER_NAME,
        map_name=MAP_NAME, expected_count=None,
        expected_id_min=None, expected_id_max=None,
        expected_id_sha256=EXPECTED_ID_SHA256, page_size=PAGE_SIZE,
        page_count=None, declared_max_record_count=DECLARED_MAX_RECORD_COUNT,
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
        expected_refused_geometry_oids=None,
        attribute_fields=tuple(EXPECTED_PAGE_FIELD_DECLARATIONS),
        expected_layer_fields=EXPECTED_LAYER_FIELDS,
        gate_modes=GATE_MODES,
        parts_policy=family.PARTS_POLICY_SINGLE_OR_AB,
        max_bisection_depth=1, single_refusal_retries=0,
        single_refusal_retry_seconds=0.0, attrs_parts=False,
        digest_geometry_before_gate=True, capture_root_family="381",
        baseline_pending_note=None,
        audit_page_refusal_count=False, coverage_scope=_COVERAGE_SCOPE,
        baseline_digest_scheme=_DIGEST_SCHEME,
        manifest_title='4-digit HU (Subregion)',
        manifest_description='Complete observed USGS Watershed Boundary Dataset 4-digit HU (Subregion) layer 2 from the USGS hydro.nationalmap.gov wbd MapServer named by the DHS HIFLD Open crosswalk; succession, freshness, contract, coverage, rights and release decisions remain unaccepted.',
        transformation_source_fields='all 16 served attributes retained in source order with declared physical types; esriFieldTypeDate served as epoch-milliseconds and represented as Arrow int64 without conversion; no field added or dropped',
        transformation_source_ids='OBJECTID retained unchanged; served roster is 245 unique ids from 1 to 246 (one unused OBJECTID); HUC4 and TNMID retained verbatim including leading zeros; no substitute or synthetic identifier',
        transformation_raw_evidence='complete decoded response and wire bodies retained with checksummed receipts for every page and service document',
        lineage="DHS HIFLD Open crosswalk (August 2025) row 'Watershed Boundary Dataset (WBD)' (agency U.S. Geological Survey) marks the retired GeoPlatform item 409515c7627c444fb08b66cf89634028 as Deprecate and names the USGS hydro.nationalmap.gov wbd MapServer as the agency's current open REST service; its layer 2 '4-digit HU (Subregion)' is the exact producer publication for the HIFLD Open inventory entry wbd/4-digit-hu-subregion (publisher U.S. Geological Survey, National Geospatial Technical Operations Center). The service documents itself as compiled from USGS NHD and USDA NRCS WBD sources. The old GeoPlatform item is not fetched; DataLumos project 240698 copies are research context only and are never fetched.",
        policy_reason='Native attributes, OBJECTID/HUC4/TNMID identifiers and served rings are retained in a typed EPSG:3857 candidate.',
    )


def _query(params: dict[str, object]) -> str:
    return family.query_url(LAYER_URL, params)


def _count_url() -> str:
    return family.count_url(_spec())


def _ids_url() -> str:
    return family.ids_url(_spec())


def _page_window_url(last_objectid: int, record_count: int) -> str:
    return family.page_window_url(_spec(), last_objectid, record_count)


def _page_url(last_objectid: int) -> str:
    return family.page_url(_spec(), last_objectid)


def _capture_roster(roster: list[int]) -> list[tuple[str, str]]:
    return family.capture_roster(_spec(), roster)


def _capture(client: httpx.Client, url: str, root: Path, name: str,
             max_bytes: int = PAGE_MAX_BYTES) -> tuple[bytes, dict]:
    return family.capture_budgeted(root, max_bytes, CAPTURE_MAX_BYTES, client, url, name)


def _is_refusal(body: bytes) -> bool:
    return family.is_refusal(body)


def _part_url(roster: list[int], index: int, part_name: str) -> str:
    return family.page_part_url(_spec(), roster, index, part_name)


def _capture_page_window(client: httpx.Client, roster: list[int], index: int,
                         root: Path) -> tuple[dict[str, tuple[bytes, dict]], list[str]]:
    return family.capture_page_window(_spec(), client, roster, index, root,
                                      capture=_capture)


_validate_spatial_reference = partial(
    crs_validation.validate_native_spatial_reference,
    native_wkid=NATIVE_WKID, native_latest_wkid=NATIVE_LATEST_WKID,
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


def _read_replay(root: Path) -> tuple[dict[str, tuple[bytes, dict]], dict[str, list[str]]]:
    captures, page_parts, _page_refusals = family.read_replay(_spec(), root)
    return captures, page_parts


def capture_wbd_4_digit_hu_subregion(storage: StagingStorageResource, *, version: str,
                            capture_directory: str | None = None) -> dict[str, object]:
    """Capture or replay one complete, unapproved native WBD 4-digit HU (Subregion) candidate."""
    return family.capture_candidate(_spec(), storage, version=version,
                                    capture_directory=capture_directory, capture=_capture)
