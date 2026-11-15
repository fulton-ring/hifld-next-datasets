"""Lossless, approval-gated capture of the FEMA NFHL "Base Flood Elevations" line layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 16 "Base Flood
Elevations" as the producer's own publication of this HIFLD inventory dataset
(``nfhl/base-flood-elevations-1``, DataLumos 239788 historical archive copy
used for identification only). The layer is continuously updated and publishes
no per-snapshot release date; source freshness, rights, and public-schema
compatibility are not accepted by this draft.

The complete 1,506,494-feature layer is captured as adaptively bisected
segments of the sorted OBJECTID roster starting at the producer's
maxRecordCount (2000): a segment the producer refuses (its serialized response
would approach the producer's transfer cap, observed as a complete HTTP 200
Esri error document on the sibling Hydrologic Reaches layer) is bisected until
the producer serves both halves, and the before/after count/id/metadata
brackets plus exact segment tiling and full-record digests prove nothing was
skipped, duplicated, or reordered. The 2026-11-12 complete capture served all
754 full-width segments (845.7 MB of raw bodies, largest page 11.7 MB) with no
refusals.

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

DATASET_SLUG = "nfhl-base-flood-elevations"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
LAYER_ID = 16
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Base Flood Elevations"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
# Initial segment width, capped by the producer's maxRecordCount (2000). Any
# segment whose response would approach the producer's transfer cap is
# bisected until the producer serves it; see the module docstring.
SEGMENT_START_RECORDS = 2000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MIN_FREE_BYTES = 12_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
SEGMENT_PROBE_ATTEMPTS = 2
SEGMENT_PROBE_BASE_DELAY_SECONDS = 5.0
# Pins below are the observed 2026-11-12 complete-layer population contract,
# established by a dated complete capture of all 754 full-width roster segments
# (before/after count/id/metadata brackets unchanged during capture); the
# module's own capture must reproduce every pin.
EXPECTED_COUNT = 1506494
EXPECTED_ID_SHA256 = "37a5dba604f49c3a8497b4c8c106b296ce30e6802a855ca8118ca506da6975d2"
EXPECTED_SCHEMA_SHA256 = "d967a447cd88c3970bd97511088b69329fee2c4ca6bed3ae149037229b7f6570"
EXPECTED_RECORD_SHA256 = "370cb7b88afbc6a10dc9954138cc26977c0106f226704db5148e0a0f0bd0b7a3"
EXPECTED_GEOMETRY_SHA256 = "09b064897b5c19d416411526099abd7c501bf6106bd8196035ec9b38595e9d7c"
EXPECTED_OBSERVED_BOUNDS = {"xmin": -170.80986931360613, "ymin": -14.3518476916243,
                            "xmax": 145.75961438754587, "ymax": 64.89287134513614}
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_NAN_GEOMETRY_COUNT = 0
EXPECTED_LAYER_EXTENT = {"xmin": -170.80986931360613, "ymin": -14.3518476916243,
                         "xmax": 145.75961438754587, "ymax": 64.89287134513614}
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

_RECONSTRUCTION_DIR = "/tmp/hifld-native-reconstruction/186/captures"

_FRAGMENTS = _nfhl.ManifestFragments(
    title="Base Flood Elevations (FEMA NFHL)",
    description="FEMA National Flood Hazard Layer (NFHL) MapServer layer 16 'Base Flood Elevations', captured "
                "completely from the producer's own REST publication. Complete source attributes and NAD83 "
                "polyline geometry are represented in the candidate. The NFHL is continuously updated and "
                "publishes no per-snapshot release date; freshness, rights, public schema/ID contract, and "
                "release acceptance remain unresolved and unapproved.",
    source_fields_note="all producer fields retained in producer order and "
                       "physical types, including the producer's derived "
                       "SHAPE.STLength() double and the BFE elevation fields "
                       "(ELEV, LEN_UNIT, V_DATUM); SHAPE is the original "
                       "geometry field represented as WKB",
    source_ids_note="Preserve producer OBJECTID, BFE_LN_ID, GFID and GlobalID "
                    "values unchanged; no substitute IDs, casts, trimming, or "
                    "joins",
    geometry_note="Source NAD83 (EPSG:4269) polyline paths retained as WKB "
                  "without projection; null source geometry retained as null; "
                  "producer string-'NaN' ordinate rows are retained as "
                  "records with null candidate geometry, raw evidence intact",
    lineage_sentences="This candidate is exactly layer 16 'Base Flood Elevations' of that "
                      "producer service. The HIFLD Open inventory lists this dataset as nfhl/base-flood-elevations-1 "
                      "with DataLumos project 239788; that ICPSR archive copy and the 2024-10-31 archive modification "
                      "date are identification and comparison context only, never pipeline inputs.")


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


_segment_name = _nfhl.segment_name
_parse_segment_name = _nfhl.parse_segment_name
_list_sha = _nfhl.streamed_list_sha
_code_identity = _nfhl.code_identity


def _segment_query(offset: int, count: int) -> str:
    return _nfhl.segment_query_url(LAYER_URL, offset, count)


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


def capture_nfhl_base_flood_elevations(storage, *, version: str,
                                       capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL Base Flood Elevations candidate."""
    return _nfhl.capture_layer(_contract(), _pins(), storage, version=version,
                               capture_directory=capture_directory, capture_get=capture_get,
                               fragments=_FRAGMENTS)
