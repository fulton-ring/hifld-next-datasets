"""Approval-gated draft capture of the USGS GNIS Historical Hydrographic Points.

The HIFLD Open inventory entry ``nmg/historical-hydrographic-points-1``
("Historical Hydrographic Points", agency U.S. Geological Survey) carries the
description of The National Map Gazetteer / Geographic Names Information System
(GNIS) verbatim. The DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1; the
byte-identical public copy is sha256
7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc) consolidates
the whole GNIS family into one row, "Geographic Names Information System (GNIS)
Feature Layers" (Status Deprecate, DUA/GII not required, Agency U.S.
Geological Survey), whose Open REST Service is the producer's own
``carto.nationalmap.gov`` ``geonames`` MapServer. Its layer 13 "Historical
Hydrographic Points" carries exactly the inventory title and serves the
maintained producer publication captured here (service copyrightText: "USGS
The National Map: Geographic Names Information System. Data Refreshed October,
2026."). The layer held 2,095 features on 2026-11-13, so this module retains
every raw response in a resumable capture directory before any candidate is
sealed. The DataLumos project 240196 archive copy and the Esri "U.S. Federal
Datasets" repackaging are identification context only and are never fetched.

The producer serves the complete OBJECTID roster (``returnIdsOnly`` returned
exactly 2,095 ids spanning 8..19461 with gaps on 2026-11-13), so the layer is
captured as adaptively bisected ``objectIds`` pages of the sorted roster: each
accepted page must return exactly its expected id slice, the accepted pages
must tile the complete roster without gaps or overlaps, and before/after
count/roster/layer/service brackets prove that nothing was skipped,
duplicated, or reordered. A single-record page the producer still refuses ends
the capture explicitly rather than guessing.

Geometry is the layer's native Z-aware multipoint: every feature geometry is
``{"points": [[x, y, z], ...]}`` in the page CRS (EPSG:3857), usually one
location per gazetteer point and up to 14 points for features with multiple
recorded locations (observed 2026-11-13: 2,695 ordinates over 2,095 features,
every z ordinate was 0 and no geometry was null). Ordinates are validated
numerically and range-checked in the Web Mercator
domain, then stored verbatim as MultiPoint Z WKB: no rounding, reordering,
reprojection or repair is applied.

Completeness comes from the producer's own before/after count and
OBJECTID roster at capture time. The observed per-value histograms and bounds
are diagnostics reported by
``build_gnis_historical_hydrographic_points_baseline`` from a completed capture
directory, not release baselines. Publication stays blocked pending the
succession, freshness, schema/ID contract, rights and release decisions; all
runtime approval flags are false and the draft refresh schedule is stopped."""
from __future__ import annotations

import httpx
from pathlib import Path
from typing import Mapping

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import gnis_historical_points_family as family
from dagster_hifld.sources.capture_common import processing_identity

DATASET_SLUG = "historical-hydrographic-points-1"
FILE_SLUG = DATASET_SLUG
# Repository/package root: in the shipped container layout (/app/src/dagster_hifld)
# this resolves to /app, which carries no .git, so the processing identity uses the
# packaged scheme. Tests redirect this constant to a no-Git packaged layout.
REPO_ROOT = Path(__file__).parents[3]
LAYER_ID = 13
LAYER_NAME = "Historical Hydrographic Points"
USER_AGENT = "hifld-next-datasets/historical-hydrographic-points-draft"
PAGE_START_RECORDS = 1000
PAGE_PROBE_BASE_DELAY_SECONDS = 5.0
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
EXPECTED_LAYER_EXTENT = {"xmin": -19818952.8156, "ymin": -1608688.4560999982,
                         "xmax": 19325062.035899997, "ymax": 11526405.634300001}
# sandbox); unpinned values ("", None) are checked only once pinned.
EXPECTED_SCHEMA_SHA256 = "125c6f6982d87bb339a539257aa9a7f77dd90a8401a9a5e7eb9813d160063522"

# Layer-agnostic pins re-exported so per-layer tooling keeps one entry point.
BASELINE_PENDING_NOTE = family.BASELINE_PENDING_NOTE
CROSSWALK_COPY_SHA256 = family.CROSSWALK_COPY_SHA256
CROSSWALK_OPEN_ID = family.CROSSWALK_OPEN_ID
CROSSWALK_ROW = family.CROSSWALK_ROW
EXPECTED_ATTRIBUTE_NAMES = family.EXPECTED_ATTRIBUTE_NAMES
EXPECTED_COPYRIGHT_TEXT = family.EXPECTED_COPYRIGHT_TEXT
EXPECTED_SERVICE_LAYERS = family.EXPECTED_SERVICE_LAYERS
GEOMETRY_FIELD_NAME = family.GEOMETRY_FIELD_NAME
GEOMETRY_TYPE = family.GEOMETRY_TYPE
MAX_RECORD_COUNT = family.MAX_RECORD_COUNT
NATIVE_FIELDS = family.NATIVE_FIELDS
NATIVE_LAYER_SR = family.NATIVE_LAYER_SR
SERVICE_URL = family.SERVICE_URL


class GnisHistoricalHydrographicPointsError(family.GnisHistoricalPointsError):
    """The source snapshot cannot be represented as the pinned candidate."""


LAYER = family.GnisHistoricalPointLayer(
    dataset_slug=DATASET_SLUG,
    file_slug=FILE_SLUG,
    layer_id=LAYER_ID,
    layer_name=LAYER_NAME,
    user_agent=USER_AGENT,
    metadata_drift_code="gnis_historical_hydrographic_layer_metadata_drift",
    expected_layer_extent=EXPECTED_LAYER_EXTENT,
    manifest_title="Historical Hydrographic Points (USGS GNIS gazetteer)",
    manifest_description="USGS The National Map Gazetteer (GNIS) geonames MapServer layer "
                       "12 'Historical Hydrographic Points' (the exact HIFLD Open "
                       "inventory title nmg/historical-hydrographic-points-1), "
                       "captured completely from the producer's own REST publication. "
                       "Complete source attributes and native EPSG:3857 Z-aware "
                       "multipoint geometry are represented in the candidate. "
                       "Succession, freshness, schema/ID contract, coverage, rights and "
                       "release acceptance remain unresolved and unapproved.",
    manifest_lineage="The HIFLD Open inventory entry nmg/"
                   "historical-hydrographic-points-1 ('Historical "
                   "Hydrographic Points', agency U.S. Geological Survey) carries "
                   "the description of The National Map Gazetteer / GNIS verbatim. The "
                   "DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1, byte-identical "
                   "public copy sha256 " + CROSSWALK_COPY_SHA256 + ", consolidated row '"
                   + CROSSWALK_ROW + "', Status Deprecate, Open ID " + CROSSWALK_OPEN_ID
                   + ") directs the GNIS-family layers to the carto.nationalmap.gov "
                   "geonames MapServer captured here; its layer 13 carries exactly the "
                   "inventory title and the service copyrightText states 'Data "
                   "Refreshed October, 2026'. This is the producer's own current, "
                   "maintained publication (a continuation of the layer HIFLD Open "
                   "pointed at), not a successor substitute. The DataLumos project "
                   "240196 archive copy (archive inventory modification 2025-03-20) "
                   "and the Esri 'U.S. Federal Datasets' repackaging are identification "
                   "and comparison context only and are never fetched.",
)


def _default_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=False, timeout=httpx.Timeout(60.0, read=120.0),
        headers={"Connection": "close", "User-Agent": USER_AGENT},
    )


def _code_identity() -> dict[str, object]:
    return processing_identity(REPO_ROOT)


def _runtime() -> family.GnisHistoricalPointRuntime:
    return family.GnisHistoricalPointRuntime(
        repo_root=REPO_ROOT,
        page_start_records=PAGE_START_RECORDS,
        page_probe_delay=PAGE_PROBE_BASE_DELAY_SECONDS,
        doc_delay=DOC_CAPTURE_BASE_DELAY_SECONDS,
        expected_schema_sha256=EXPECTED_SCHEMA_SHA256,
        default_client=_default_client,
        code_identity=_code_identity,
        validate_code_identity=validate_construction_code_identity,
    )


def capture_gnis_historical_hydrographic_points(
        storage: StagingStorageResource, *, version: str,
        capture_directory: str | None = None) -> dict[str, object]:
    return family.capture_gnis_historical_points(
        LAYER, GnisHistoricalHydrographicPointsError, _runtime(), storage, version=version,
        capture_directory=capture_directory)


def fetch_gnis_historical_hydrographic_points_pages(capture_root: Path, *,
                       client: httpx.Client | None = None) -> dict[str, object]:
    return family.fetch_gnis_historical_points_pages(
        LAYER, GnisHistoricalHydrographicPointsError, _runtime(), capture_root, client=client)


def build_gnis_historical_hydrographic_points_baseline(capture_root: str) -> dict[str, object]:
    return family.build_gnis_historical_points_baseline(
        LAYER, GnisHistoricalHydrographicPointsError, _runtime(), capture_root)


def _capture_spec():
    return family._adaptive_capture_spec(LAYER, GnisHistoricalHydrographicPointsError, _runtime())


def _count_url() -> str:
    return family._count_url(LAYER)


def _ids_url() -> str:
    return family._ids_url(LAYER)


def _page_url(ids_slice: list[int]) -> str:
    return family._page_url(LAYER, ids_slice)


def _profile_schema(layer: Mapping[str, object]):
    return family._profile_schema(layer, GnisHistoricalHydrographicPointsError)


def _validate_multipoint(geometry: object):
    return family._validate_multipoint(geometry, GnisHistoricalHydrographicPointsError)


_multipoint_wkb = family._multipoint_wkb
