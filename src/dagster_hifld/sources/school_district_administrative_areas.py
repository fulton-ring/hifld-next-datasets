"""Issue #303 adapter over the shared Census school-district GDB extractor.

Discovers the maintained nationwide school geodatabase and preserves its
complete ``School_District_Administrative`` layer, native fields, identifiers,
EPSG:4269 coordinates and MultiPolygon WKB in an immutable local candidate.
"""
from __future__ import annotations

import httpx
import json
import pyarrow as pa

from dagster_hifld.sources import census_tiger_common as _shared

DATASET_SLUG = "school-district-administrative-areas"
FILE_SLUG = DATASET_SLUG
SOURCE_URL = ""
SOURCE_HOST = "www2.census.gov"
LAYER_NAME = "School_District_Administrative"
FEATURE_COUNT_RANGE = (1, 1_000)
MAX_ARCHIVE_BYTES = _shared.MAX_ARCHIVE_BYTES
MAX_UNCOMPRESSED_BYTES = _shared.MAX_UNCOMPRESSED_BYTES
EXPECTED_SOURCE_FIELDS = [
    ("STATEFP", pa.string()),
    ("SDADMLEA", pa.string()),
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
EXPECTED_OUTPUT_FIELDS = ["OBJECTID", "STATEFP", "SDADMLEA", "GEOID", "GEOIDFQ", "NAME", "LSAD",
                          "LOGRADE", "HIGRADE", "MTFCC", "SDTYP", "FUNCSTAT", "ALAND", "AWATER",
                          "INTPTLAT", "INTPTLON", "geometry"]

CONFIG = _shared.SchoolDistrictLayerConfig(
    dataset_slug=DATASET_SLUG,
    layer_name=LAYER_NAME,
    expected_count=None,
    issue_number=303,
    inventory_id=DATASET_SLUG,
    title="School District Administrative Areas",
    coverage_statement=(
        "The full producer-declared native layer population is checked against its scanned rows "
        "and native ID roster. The configured population range is a plausibility guard, not an "
        "assertion of the current population. All source attributes and coordinates are retained."
    ),
    source_url=SOURCE_URL,
    source_vintage="",
    expected_source_fields=list(EXPECTED_SOURCE_FIELDS),
    discover_current=True,
    feature_count_range=FEATURE_COUNT_RANGE,
)


def _json_bytes(value: object) -> bytes:
    return _shared._json_bytes(value)


def _sha256(body: bytes) -> str:
    return _shared._sha256(body)


def _capture_archive(staging, version: str, client: httpx.Client) -> tuple[bytes, dict]:
    return _shared._capture_archive(staging, version, client, CONFIG)


def _reserve(current: int, upcoming: int) -> None:
    return _shared._reserve(current, upcoming)


def _validate_archive(archive: bytes) -> None:
    return _shared._validate_archive(archive)


def _native_table(archive_path):
    return _shared._native_table(archive_path, CONFIG)


def _build_geoparquet(
    table: pa.Table,
    ids: list[int],
    *,
    config: _shared.SchoolDistrictLayerConfig = CONFIG,
) -> tuple[bytes, dict]:
    return _shared._build_geoparquet(table, ids, config)


def _assert_full_parity(
    source: pa.Table,
    ids: list[int],
    parquet: bytes,
    *,
    config: _shared.SchoolDistrictLayerConfig = CONFIG,
) -> None:
    return _shared._assert_full_parity(source, ids, parquet, config)


def _source_records_sha256(table: pa.Table, ids: list[int]) -> str:
    return _shared._source_records_sha256(table, ids, CONFIG)


def _packaged_code_sha256() -> str:
    return _shared._packaged_code_sha256()


def _capture(
    staging,
    version: str | None,
    client: httpx.Client | None = None,
    *,
    retained_archive: bytes | None = None,
    retained_receipt: dict | None = None,
    retained_receipt_bytes: bytes | None = None,
) -> dict:
    return _shared._capture(
        staging,
        version,
        CONFIG,
        client,
        retained_archive=retained_archive,
        retained_receipt=retained_receipt,
        retained_receipt_bytes=retained_receipt_bytes,
    )


def capture(staging, version: str | None = None, client: httpx.Client | None = None) -> dict:
    """Capture the complete administrative layer through the shared immutable spec."""
    return _capture(staging, version, client)


def replay_retained_capture(staging, version: str, archive: bytes, receipt_bytes: bytes) -> dict:
    try:
        receipt = json.loads(receipt_bytes)
    except (TypeError, ValueError) as exc:
        raise ValueError("retained_capture_receipt_invalid") from exc
    return _capture(
        staging,
        version,
        retained_archive=archive,
        retained_receipt=receipt,
        retained_receipt_bytes=receipt_bytes,
    )
