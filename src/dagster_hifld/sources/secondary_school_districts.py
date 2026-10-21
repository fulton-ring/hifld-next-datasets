"""Issue #307 adapter over the shared Census school-district GDB extractor."""
from __future__ import annotations

import httpx
import json
import pyarrow as pa

from dagster_hifld.sources import census_school_districts as _shared

DATASET_SLUG = "secondary-school-districts"
FILE_SLUG = DATASET_SLUG
SOURCE_URL = _shared.SOURCE_URL
SOURCE_HOST = _shared.SOURCE_HOST
LAYER_NAME = "School_District_Secondary"
EXPECTED_COUNT = 481
MAX_ARCHIVE_BYTES = _shared.MAX_ARCHIVE_BYTES
MAX_UNCOMPRESSED_BYTES = _shared.MAX_UNCOMPRESSED_BYTES
EXPECTED_SOURCE_FIELDS = _shared.EXPECTED_SOURCE_FIELDS
EXPECTED_OUTPUT_FIELDS = _shared.EXPECTED_OUTPUT_FIELDS

CONFIG = _shared.SchoolDistrictLayerConfig(
    dataset_slug=DATASET_SLUG,
    layer_name=LAYER_NAME,
    expected_count=EXPECTED_COUNT,
    issue_number=307,
    inventory_id=DATASET_SLUG,
    title="Secondary School Districts",
    coverage_statement="Equivalence to the historical HIFLD archive population is not established.",
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


def _assert_full_parity(source: pa.Table, ids: list[int], parquet: bytes) -> None:
    return _shared._assert_full_parity(source, ids, parquet, CONFIG)


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
    """Preserve the established #307 source adapter API."""
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
