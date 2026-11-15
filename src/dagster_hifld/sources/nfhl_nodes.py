"""Lossless, approval-gated capture of the FEMA NFHL "Nodes" point layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 11 "Nodes" as the
producer's own publication of this HIFLD inventory dataset (``nfhl/nodes``,
DataLumos 239981 historical archive copy used for identification only). The
layer is continuously updated and publishes no per-snapshot release date;
source freshness, rights, and public-schema compatibility are not accepted by
this draft.

This adapter is a thin entry point over the shared NFHL capture and replay
machinery in ``dagster_hifld.sources.nfhl_common``; it contributes only this
layer's URL/id, schema/population pins, fixed-pagination capture plan,
geometry validation and provenance text.
"""
from __future__ import annotations

import httpx

from dagster_hifld.sources import nfhl_common as _nfhl
from dagster_hifld.sources.epa_frs_common import capture_get
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "nfhl-nodes"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
LAYER_ID = 11
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Nodes"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
PAGE_SIZE = 2000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MIN_FREE_BYTES = 1_500_000_000
# Pins below are the observed 2026-11-07 complete-layer population contract.
EXPECTED_COUNT = 379653
EXPECTED_ID_SHA256 = "77f5f3d53c921e5a70260f1d8191d0a331d774ffe819e9f085a58f324cf2fd94"
EXPECTED_RECORD_SHA256 = "4deaf06292c5bd001698b5637538ef274d587540889b3984bec26dcfbfa41ba6"
EXPECTED_GEOMETRY_SHA256 = "09aec31d6695f22ef6c16a2aea29509056ea463eb243f8d0ef25770d23059b5a"
EXPECTED_SCHEMA_SHA256 = "88241dd0fb877c4029cf3ae30493eafc8cef3c216ef1df75c55a1965f354408d"
EXPECTED_NULL_GEOMETRY_COUNT = 1
EXPECTED_NAN_GEOMETRY_COUNT = 8
EXPECTED_OBSERVED_BOUNDS = {"xmin": -159.6655269033425, "ymin": 21.272407815676047,
                            "xmax": -66.98810790110986, "ymax": 64.84163586737333}
EXPECTED_LAYER_EXTENT = {"xmin": -159.6655269033425, "ymin": 21.272407815676047,
                         "xmax": -66.98810786443903, "ymax": 64.84163586737333}
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

_RECONSTRUCTION_DIR = "/tmp/hifld-native-reconstruction/204/captures"

_FRAGMENTS = _nfhl.ManifestFragments(
    title="Nodes (FEMA NFHL)",
    description="FEMA National Flood Hazard Layer (NFHL) MapServer layer 11 'Nodes', captured completely from "
                "the producer's own REST publication. Complete source attributes and NAD83 point geometry are "
                "represented in the candidate. The NFHL is continuously updated and publishes no per-snapshot "
                "release date; freshness, rights, public schema/ID contract, and release acceptance remain "
                "unresolved and unapproved.",
    source_fields_note="all producer fields retained in producer order and "
                       "physical types; SHAPE is the original geometry field "
                       "represented as WKB",
    source_ids_note="Preserve producer OBJECTID, NODE_ID, GFID and GlobalID "
                    "values unchanged; no substitute IDs, casts, trimming, or "
                    "joins",
    geometry_note="Source NAD83 (EPSG:4269) point x/y retained as WKB without "
                  "projection; null source geometry retained as null; the "
                  "eight producer string-'NaN' ordinate rows are retained as "
                  "records with null candidate geometry, raw evidence intact",
    lineage_sentences="This candidate is exactly layer 11 'Nodes' of that producer service. The "
                      "HIFLD Open inventory lists this dataset as nfhl/nodes with DataLumos project 239981; that ICPSR "
                      "archive copy and the 2024-10-31 archive modification date are identification and comparison "
                      "context only, never pipeline inputs.")


def _contract() -> _nfhl.LayerContract:
    return _nfhl.LayerContract(
        dataset_slug=DATASET_SLUG, file_slug=FILE_SLUG, layer_id=LAYER_ID, layer_name=LAYER_NAME,
        geometry_family="point", esri_geometry_type="esriGeometryPoint", mode="fixed",
        max_capture_bytes=MAX_CAPTURE_BYTES, min_free_bytes=MIN_FREE_BYTES,
        reconstruction_dir=_RECONSTRUCTION_DIR, page_size=PAGE_SIZE,
        doc_attempts=1, doc_base_delay=0.0, client_timeout=60.0)


def _pins() -> _nfhl.PopulationPins:
    return _nfhl.PopulationPins(
        count=EXPECTED_COUNT, id_sha256=EXPECTED_ID_SHA256, schema_sha256=EXPECTED_SCHEMA_SHA256,
        record_sha256=EXPECTED_RECORD_SHA256, geometry_sha256=EXPECTED_GEOMETRY_SHA256,
        observed_bounds=EXPECTED_OBSERVED_BOUNDS, layer_extent=EXPECTED_LAYER_EXTENT,
        service_layer_roster=EXPECTED_SERVICE_LAYER_ROSTER)


_code_identity = _nfhl.code_identity


def _parquet_bytes(fields: list, features: list) -> bytes:
    return _nfhl.parquet_bytes(fields, features, "point", wkid=SOURCE_CRS_WKID)


def _validate_captures(captures: dict):
    return _nfhl.validate_in_memory_capture(_contract(), _pins(), captures)


def capture_nfhl_nodes(storage, *, version: str,
                       capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NFHL Nodes source candidate."""
    return _nfhl.capture_layer(_contract(), _pins(), storage, version=version,
                               capture_directory=capture_directory, capture_get=capture_get,
                               fragments=_FRAGMENTS)
