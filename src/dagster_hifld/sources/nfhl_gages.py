"""Lossless, approval-gated capture of the FEMA NFHL "Gages" point layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 10 "Gages" as the
producer's own publication of this HIFLD inventory dataset
(``nfhl/gages-2``, DataLumos 239972 historical archive copy used for
identification only). The layer is continuously updated and publishes no
per-snapshot release date; source freshness, rights, and public-schema
compatibility are not accepted by this draft.

The producer serializes the Esri date fields START_PD/END_PD as epoch-
millisecond integers and publishes one row (OBJECTID 5390) with string-"NaN"
point ordinates; both are retained verbatim in the candidate, the NaN row
gated as unusable geometry rather than silently coerced to a location. The
2026-11-12 complete capture also observed that this producer layer currently
serves the same 683-row population as FEMA's sibling NFHL layer 9 "Coastal
Gages" (identical per-row attributes and coordinates) with its own field
order and renderer; this module captures exactly what layer 10 itself serves
and shares no pins, or candidate with any sibling module.

This adapter is a thin entry point over the shared NFHL capture and replay
machinery in ``dagster_hifld.sources.nfhl_common``; it contributes only this
layer's URL/id, schema/population pins, capture budgets, geometry validation
and provenance text.
"""
from __future__ import annotations

import httpx
import time

from dagster_hifld.sources import nfhl_common as _nfhl
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "nfhl-gages"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
LAYER_ID = 10
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Gages"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
PAGE_SIZE = 2000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MIN_FREE_BYTES = 1_500_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
PAGE_CAPTURE_ATTEMPTS = 3
PAGE_CAPTURE_BASE_DELAY_SECONDS = 5.0
# Pins below are the observed 2026-11-12 complete-layer population contract.
EXPECTED_COUNT = 683
EXPECTED_ID_SHA256 = "17e3f21e11e0b2cbf57f089696154c09a8d3acefebc2ef9f7c8f45739bd99198"
EXPECTED_RECORD_SHA256 = "fa96061a5654f14fb5805baf061dd5190aa72eb6a5f8021eb6b89cabecd235f4"
EXPECTED_GEOMETRY_SHA256 = "7d0ec57d71efad3b585d54c1fbe6a4adae80f98e4afcbb108a7ef8c7d17aeaa7"
EXPECTED_SCHEMA_SHA256 = "935f54f9dd06cdddd6a36f234c9f1da8ccb4253bae03d179533ff9cdaf940244"
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_NAN_GEOMETRY_COUNT = 1
EXPECTED_OBSERVED_BOUNDS = {"xmin": -146.36167678737675, "ymin": 24.550829989111662,
                            "xmax": -66.98286549880623, "ymax": 61.13199762857323}
EXPECTED_LAYER_EXTENT = {"xmin": -146.36167678737675, "ymin": 24.550829989111662,
                         "xmax": -66.98286549880623, "ymax": 61.13199762857323}
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

_RECONSTRUCTION_DIR = "/tmp/hifld-native-reconstruction/195/captures"

_FRAGMENTS = _nfhl.ManifestFragments(
    title="Gages (FEMA NFHL)",
    description="FEMA National Flood Hazard Layer (NFHL) MapServer layer 10 'Gages', captured "
                "completely from the producer's own REST publication. Complete source attributes and NAD83 "
                "point geometry are represented in the candidate. The NFHL is continuously updated and "
                "publishes no per-snapshot release date; freshness, rights, public schema/ID contract, and "
                "release acceptance remain unresolved and unapproved.",
    source_fields_note="all producer fields retained in producer order and "
                       "physical types, including the producer's START_PD and "
                       "END_PD Esri date fields serialized as epoch-"
                       "millisecond integers; SHAPE is the original geometry "
                       "field represented as WKB",
    source_ids_note="Preserve producer OBJECTID, CSTGAGE_ID, GFID and GlobalID "
                    "values unchanged; no substitute IDs, casts, trimming, or "
                    "joins",
    geometry_note="Source NAD83 (EPSG:4269) point x/y retained as WKB without "
                  "projection; null source geometry retained as null; the "
                  "one producer string-'NaN' ordinate row is retained as a "
                  "record with null candidate geometry, raw evidence intact",
    lineage_sentences="This candidate is exactly layer 10 'Gages' of that producer service. "
                      "The HIFLD Open inventory lists this dataset as nfhl/gages-2 with DataLumos project 239972; "
                      "that ICPSR archive copy and the 2024-10-31 archive modification date are identification and "
                      "comparison context only, never pipeline inputs.")


def _contract() -> _nfhl.LayerContract:
    return _nfhl.LayerContract(
        dataset_slug=DATASET_SLUG, file_slug=FILE_SLUG, layer_id=LAYER_ID, layer_name=LAYER_NAME,
        geometry_family="point", esri_geometry_type="esriGeometryPoint", mode="fixed",
        max_capture_bytes=MAX_CAPTURE_BYTES, min_free_bytes=MIN_FREE_BYTES,
        reconstruction_dir=_RECONSTRUCTION_DIR, page_size=PAGE_SIZE,
        doc_attempts=DOC_CAPTURE_ATTEMPTS, doc_base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS,
        page_attempts=PAGE_CAPTURE_ATTEMPTS, page_base_delay=PAGE_CAPTURE_BASE_DELAY_SECONDS,
        client_timeout=180.0)


def _pins() -> _nfhl.PopulationPins:
    return _nfhl.PopulationPins(
        count=EXPECTED_COUNT, id_sha256=EXPECTED_ID_SHA256, schema_sha256=EXPECTED_SCHEMA_SHA256,
        record_sha256=EXPECTED_RECORD_SHA256, geometry_sha256=EXPECTED_GEOMETRY_SHA256,
        observed_bounds=EXPECTED_OBSERVED_BOUNDS, layer_extent=EXPECTED_LAYER_EXTENT,
        service_layer_roster=EXPECTED_SERVICE_LAYER_ROSTER)


def _feature_urls(ids: list[int]) -> list[tuple[str, str]]:
    return _nfhl.fixed_page_urls(SERVICE_URL, SERVICE_DOC_URL, LAYER_URL, PAGE_SIZE, len(ids))


def _query(params: dict[str, object]) -> str:
    return _nfhl.query_url(LAYER_URL, params)


def _validate_spatial_reference(value: object, label: str) -> None:
    _nfhl.validate_spatial_reference(value, label, SOURCE_CRS_WKID)


def _parquet_bytes(fields: list, features: list) -> bytes:
    return _nfhl.parquet_bytes(fields, features, "point", wkid=SOURCE_CRS_WKID)


def _capture_with_retry(client, url: str, capture_root, name: str, *,
                        max_bytes: int, attempts: int, base_delay: float):
    return _nfhl.capture_with_retry(client, url, capture_root, name, max_bytes=max_bytes,
                                    attempts=attempts, base_delay=base_delay)


def _validate_captures(captures: dict):
    return _nfhl.validate_in_memory_capture(_contract(), _pins(), captures)


def capture_nfhl_gages(storage, *, version: str,
                       capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL Gages candidate."""
    return _nfhl.capture_layer(_contract(), _pins(), storage, version=version,
                               capture_directory=capture_directory, capture_get=_nfhl.capture_get,
                               fragments=_FRAGMENTS)
