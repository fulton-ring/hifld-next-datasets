"""Issue #303 adapter over the shared Census school-district GDB extractor.

Targets the complete 2024 U.S. Census TIGER/Line nationwide school geodatabase
layer ``School_District_Administrative`` (52 Vermont supervisory unions and
interstate supervisory districts, native EPSG:4269 MultiPolygons). This is the
inventory's fourth school-district type; the unified, secondary and elementary
siblings cover the other layers of the same official archive.
"""
from __future__ import annotations

import httpx
import json
import pyarrow as pa

from dagster_hifld.sources import census_school_districts as _shared

DATASET_SLUG = "school-district-administrative-areas"
FILE_SLUG = DATASET_SLUG
SOURCE_URL = _shared.SOURCE_URL
SOURCE_HOST = _shared.SOURCE_HOST
LAYER_NAME = "School_District_Administrative"
EXPECTED_COUNT = 52
MAX_ARCHIVE_BYTES = _shared.MAX_ARCHIVE_BYTES
MAX_UNCOMPRESSED_BYTES = _shared.MAX_UNCOMPRESSED_BYTES
EXPECTED_SOURCE_FIELDS = _shared.EXPECTED_SOURCE_FIELDS
EXPECTED_OUTPUT_FIELDS = _shared.EXPECTED_OUTPUT_FIELDS

CONFIG = _shared.SchoolDistrictLayerConfig(
    dataset_slug=DATASET_SLUG,
    layer_name=LAYER_NAME,
    expected_count=EXPECTED_COUNT,
    issue_number=303,
    inventory_id=DATASET_SLUG,
    title="School District Administrative Areas",
    coverage_statement=(
        "The complete observed 2024 administrative school district layer contains "
        "52 features, all Vermont supervisory unions or interstate supervisory "
        "districts (GEOID state 50); the Census publishes no administrative "
        "districts for other states in this vintage. Equivalence to the historical "
        "HIFLD archive population is not established."
    ),
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
    return _shared._source_records_sha256(table, ids)


def _packaged_code_sha256() -> str:
    return _shared._packaged_code_sha256()


def _capture(
    staging,
    version: str,
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


def capture(staging, version: str, client: httpx.Client | None = None) -> dict:
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
