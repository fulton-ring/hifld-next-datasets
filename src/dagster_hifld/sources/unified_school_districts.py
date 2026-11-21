"""Issue #335 adapter for the native Census Unified layer.

Discovers the maintained nationwide school geodatabase at capture time and
preserves its complete ``School_District_Unified`` layer, native fields,
identifiers, EPSG:4269 coordinates and MultiPolygon WKB in an immutable local
candidate. No TGRGDB edition, archive digest or exact population is pinned; the
configured population range is a plausibility guard.
"""
from __future__ import annotations

import pyarrow as pa

from dagster_hifld.sources import census_tiger_common as _shared

DATASET_SLUG = "unified-school-districts"
FILE_SLUG = DATASET_SLUG
SOURCE_HOST = "www2.census.gov"
LAYER_NAME = "School_District_Unified"
# Nationwide unified school districts have sat near 10,800 for the current
# decade (10,897 in the 2024 edition, 10,801 observed in 2026). The band
# brackets that stable population while excluding a single-state subset (the
# largest state holds far fewer) and contamination from the smaller
# Secondary and Administrative layers.
FEATURE_COUNT_RANGE = (9_000, 13_000)
MAX_ARCHIVE_BYTES = _shared.MAX_ARCHIVE_BYTES
MAX_UNCOMPRESSED_BYTES = _shared.MAX_UNCOMPRESSED_BYTES
# 2026 Census native layout for School_District_Unified. Five fields were
# added to the 2024 layout this adapter previously inherited: STATEFP, UNSDLEA,
# LSAD, MTFCC and FUNCSTAT. Verified against the 2026 national school GDB.
EXPECTED_SOURCE_FIELDS = [
    ("STATEFP", pa.string()),
    ("UNSDLEA", pa.string()),
    ("GEOID", pa.string()),
    ("GEOIDFQ", pa.string()),
    ("NAME", pa.string()),
    ("LSAD", pa.string()),
    ("LOGRADE", pa.string()),
    ("HIGRADE", pa.string()),
    ("MTFCC", pa.string()),
    ("SDTYP", pa.string()),
    ("FUNCSTAT", pa.string()),
    ("ALAND", pa.float64()),
    ("AWATER", pa.float64()),
    ("INTPTLAT", pa.string()),
    ("INTPTLON", pa.string()),
    ("SHAPE", pa.binary()),
]
EXPECTED_OUTPUT_FIELDS = ["OBJECTID", "STATEFP", "UNSDLEA", "GEOID", "GEOIDFQ", "NAME", "LSAD",
                          "LOGRADE", "HIGRADE", "MTFCC", "SDTYP", "FUNCSTAT", "ALAND", "AWATER",
                          "INTPTLAT", "INTPTLON", "geometry"]

CONFIG = _shared.SchoolDistrictLayerConfig(
    dataset_slug=DATASET_SLUG,
    layer_name=LAYER_NAME,
    expected_count=None,
    issue_number=335,
    inventory_id=DATASET_SLUG,
    title="Unified School Districts",
    coverage_statement=(
        "The full producer-declared native layer population is checked against its scanned rows "
        "and native ID roster. The configured population range is a plausibility guard, not an "
        "assertion of the current population. All source attributes and coordinates are retained."
    ),
    source_url="",
    source_vintage="",
    expected_source_fields=list(EXPECTED_SOURCE_FIELDS),
    discover_current=True,
    feature_count_range=FEATURE_COUNT_RANGE,
)


def capture(staging, version: str, client=None) -> dict:
    """Capture the full native Unified layer through the shared immutable spec."""
    return _shared.capture(staging, version, CONFIG, client)


def replay_retained_capture(staging, version: str, archive: bytes, receipt_bytes: bytes) -> dict:
    return _shared.replay_retained_capture(
        staging, version, archive, receipt_bytes, config=CONFIG
    )
