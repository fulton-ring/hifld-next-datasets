"""Issue #335 adapter for the native 2024 Census Unified layer."""
from __future__ import annotations

from dagster_hifld.sources import census_school_districts as _shared

DATASET_SLUG = "unified-school-districts"
FILE_SLUG = DATASET_SLUG
LAYER_NAME = "School_District_Unified"
EXPECTED_COUNT = 10897
CONFIG = _shared.SchoolDistrictLayerConfig(
    dataset_slug=DATASET_SLUG,
    layer_name=LAYER_NAME,
    expected_count=EXPECTED_COUNT,
    issue_number=335,
    inventory_id=DATASET_SLUG,
    title="Unified School Districts",
    coverage_statement=(
        "This is the complete nationwide 2024 Census source layer. Its relationship "
        "to the historical HIFLD archive and inventory coverage has not been established."
    ),
)


def capture(staging, version: str, client=None) -> dict:
    """Capture the full native Unified layer through the shared immutable spec."""
    return _shared.capture(staging, version, CONFIG, client)


def replay_retained_capture(staging, version: str, archive: bytes, receipt_bytes: bytes) -> dict:
    return _shared.replay_retained_capture(
        staging, version, archive, receipt_bytes, config=CONFIG
    )
