"""Lossless, approval-gated capture of the FEMA NFHL "Cross-Sections" line layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 14
"Cross-Sections" as the producer's own publication of this HIFLD inventory dataset
(``nfhl/cross-sections-1``, DataLumos 239967 historical archive copy used
for identification only). The layer is continuously updated and publishes no
per-snapshot release date; source freshness, rights, and public-schema
compatibility are not accepted by this draft.

The layer publishes 954,282 polyline features (dated 2026-11-12 capture) and
the producer also intermittently answers valid requests with complete HTTP 200
Esri error documents or resets the transport outright (both observed while
paging this layer on 2026-11-12), so the complete layer is captured as
adaptively bisected segments of the sorted OBJECTID roster: each accepted
segment must return exactly its expected id slice, the accepted segments must
tile the complete roster without gaps or overlaps, and the before/after
count/id/metadata brackets plus full-record digests prove that nothing was
skipped, duplicated, or reordered.

This adapter is a thin entry point over the shared NFHL capture and replay
machinery in ``dagster_hifld.sources.nfhl_common``; it contributes only this
layer's URL/id, schema/population pins, capture budgets, geometry validation
and provenance text.
"""
from __future__ import annotations

import httpx
import time

from dagster_hifld.sources import nfhl_common as _nfhl
from dagster_hifld.sources.epa_frs_common import capture_get
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "nfhl-cross-sections"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
LAYER_ID = 14
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Cross-Sections"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
# Initial segment width, capped by the producer's maxRecordCount (2000). Any
# segment whose response would approach the producer's ~36 MB transfer cap is
# bisected until the producer serves it; see the module docstring.
SEGMENT_START_RECORDS = 2000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MIN_FREE_BYTES = 12_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 2
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0
# Pins below are the observed 2026-11-12 complete-layer population contract.
EXPECTED_COUNT = 954282
EXPECTED_ID_SHA256 = "88c4e3387078077abe66c7273b85893fae441d9c554cf3e575a803a0b496b622"
EXPECTED_SCHEMA_SHA256 = "7a15c06a35f35fa4aa4c9bcc3a840db04f8c8442f9a9cf32de52bc6ef4448956"
EXPECTED_RECORD_SHA256 = "ff9d8995bf2d27fb434668d2e6bc2e84c93f2b692c3f0d64dd439155c77fe8b7"
EXPECTED_GEOMETRY_SHA256 = "e3d290b57b0728f263e05d639061d78687bfd00eb7f74271397a28812c167e9d"
EXPECTED_OBSERVED_BOUNDS = {"xmin": -170.8101923523096, "ymin": -14.35089818917669,
                            "xmax": 145.7600641593766, "ymax": 64.89803977710869}
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_NAN_GEOMETRY_COUNT = 0
EXPECTED_LAYER_EXTENT = {"xmin": -170.8101923523096, "ymin": -14.35089818917669,
                         "xmax": 145.7600641593766, "ymax": 64.89803977710869}
EXPECTED_SERVICE_LAYER_ROSTER = [
    (0, "NFHL Availability", "Feature Layer"), (3, "FIRM Panels", "Feature Layer"),
    (1, "LOMRs", "Feature Layer"), (34, "LOMAs", "Feature Layer"),
    (22, "Political Jurisdictions", "Feature Layer"), (17, "Profile Baselines", "Feature Layer"),
    (20, "Water Lines", "Feature Layer"), (14, "Cross-Sections", "Feature Layer"),
    (16, "Base Flood Elevations", "Feature Layer"), (23, "Levees", "Feature Layer"),
    (29, "Seclusion Boundaries", "Feature Layer"), (15, "Coastal Transects", "Feature Layer"),
    (18, "Transect Baselines", "Feature Layer"), (24, "General Structures", "Feature Layer"),
    (7, "River Mile Markers", "Feature Layer"), (32, "Water Areas", "Feature Layer"),
    (5, "PLSS", "Feature Layer"), (19, "Limit of Moderate Wave Action", "Feature Layer"),
    (27, "Flood Hazard Boundaries", "Feature Layer"), (28, "Flood Hazard Zones", "Feature Layer"),
    (25, "Primary Frontal Dunes", "Feature Layer"), (4, "Base Index", "Feature Layer"),
    (6, "Topographic Low Confidence Areas", "Feature Layer"), (8, "Datum Conversion Points", "Feature Layer"),
    (9, "Coastal Gages", "Feature Layer"), (10, "Gages", "Feature Layer"),
    (11, "Nodes", "Feature Layer"), (12, "High Water Marks", "Feature Layer"),
    (13, "Station Start Points", "Feature Layer"), (26, "Hydrologic Reaches", "Feature Layer"),
    (30, "Alluvial Fans", "Feature Layer"), (31, "Subbasins", "Feature Layer"),
]
# Complete-population pins above are established by one dated complete capture;
# the module docstring records the adaptive segment plan that served it.

_RECONSTRUCTION_DIR = "/tmp/hifld-native-reconstruction/190/captures"

_FRAGMENTS = _nfhl.ManifestFragments(
    title="Cross-Sections (FEMA NFHL)",
    description="FEMA National Flood Hazard Layer (NFHL) MapServer layer 14 'Cross-Sections', captured "
                "completely from the producer's own REST publication. Complete source attributes and NAD83 "
                "polyline geometry are represented in the candidate. The NFHL is continuously updated and "
                "publishes no per-snapshot release date; freshness, rights, public schema/ID contract, and "
                "release acceptance remain unresolved and unapproved.",
    source_fields_note="all producer fields retained in producer order and "
                       "physical types, including the producer's derived "
                       "SHAPE.STLength() double; SHAPE is the original "
                       "geometry field represented as WKB",
    source_ids_note="Preserve producer OBJECTID, DFIRM_ID, VERSION_ID, XS_LN_ID, START_ID, "
                    "MODEL_ID, GFID and GlobalID values unchanged; no "
                    "substitute IDs, casts, trimming, or joins",
    geometry_note="Source NAD83 (EPSG:4269) polyline paths retained as WKB "
                  "without projection; null source geometry retained as null; "
                  "producer string-'NaN' ordinate rows are retained as "
                  "records with null candidate geometry, raw evidence intact",
    lineage_sentences="This candidate is exactly layer 14 'Cross-Sections' of that producer "
                      "service. The HIFLD Open inventory lists this dataset as nfhl/cross-sections-1 with DataLumos "
                      "project 239967; that ICPSR archive copy and the 2024-10-31 archive modification date are "
                      "identification and comparison context only, never pipeline inputs.")


def _contract() -> _nfhl.LayerContract:
    return _nfhl.LayerContract(
        dataset_slug=DATASET_SLUG, file_slug=FILE_SLUG, layer_id=LAYER_ID, layer_name=LAYER_NAME,
        geometry_family="polyline", esri_geometry_type="esriGeometryPolyline", mode="bisect",
        max_capture_bytes=MAX_CAPTURE_BYTES, min_free_bytes=MIN_FREE_BYTES,
        reconstruction_dir=_RECONSTRUCTION_DIR, segment_start_records=SEGMENT_START_RECORDS,
        doc_attempts=DOC_CAPTURE_ATTEMPTS, doc_base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS,
        segment_attempts=SEGMENT_PROBE_ATTEMPTS, segment_base_delay=SEGMENT_PROBE_BASE_DELAY_SECONDS,
        client_timeout=180.0)


def _pins() -> _nfhl.PopulationPins:
    return _nfhl.PopulationPins(
        count=EXPECTED_COUNT, id_sha256=EXPECTED_ID_SHA256, schema_sha256=EXPECTED_SCHEMA_SHA256,
        record_sha256=EXPECTED_RECORD_SHA256, geometry_sha256=EXPECTED_GEOMETRY_SHA256,
        observed_bounds=EXPECTED_OBSERVED_BOUNDS, layer_extent=EXPECTED_LAYER_EXTENT,
        service_layer_roster=EXPECTED_SERVICE_LAYER_ROSTER)


def _segment_query(offset: int, count: int) -> str:
    return _nfhl.segment_query_url(LAYER_URL, offset, count)


_segment_name = _nfhl.segment_name
_parse_segment_name = _nfhl.parse_segment_name
_list_sha = _nfhl.streamed_list_sha
_code_identity = _nfhl.code_identity


def _classify_geometries(features: list, attr_names: list[str]):
    records, null_geometry, nan_geometry, bounds, _unusable = _nfhl.classify_geometries(
        features, attr_names, "polyline", wkid=SOURCE_CRS_WKID)
    return records, null_geometry, nan_geometry, bounds


def _parquet_bytes(fields: list, features: list) -> bytes:
    return _nfhl.parquet_bytes(fields, features, "polyline", wkid=SOURCE_CRS_WKID)


def _capture_with_retry(client, url: str, capture_root, name: str, *,
                        max_bytes: int, attempts: int, base_delay: float):
    return _nfhl.capture_with_retry(client, url, capture_root, name, max_bytes=max_bytes,
                                    attempts=attempts, base_delay=base_delay, capture_get=capture_get)


def _capture_segments(client, capture_root, total: int):
    return _nfhl.capture_bisect_segments(client, _contract(), capture_root, total, capture_get)


def _validate_captures(captures: dict):
    return _nfhl.validate_in_memory_capture(_contract(), _pins(), captures)


def capture_nfhl_cross_sections(storage, *, version: str,
                                capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL Cross-Sections candidate."""
    return _nfhl.capture_layer(_contract(), _pins(), storage, version=version,
                               capture_directory=capture_directory, capture_get=capture_get,
                               fragments=_FRAGMENTS)
