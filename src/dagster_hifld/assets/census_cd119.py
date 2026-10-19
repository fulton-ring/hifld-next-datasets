"""119th Congressional Districts (TIGER/Line 2025 CD119).

The Census Bureau distributes this national geography as 56 state/equivalent
ZIP files. Keep the complete GEOID roster here; there is no national ZIP in
the official directory.
"""

from __future__ import annotations

from dagster import AssetKey, asset

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.census_cd119 import stage_cd119_candidate

CD119_DATASET_SLUG = "119th-congressional-districts"
CD119_FILE_SLUG = "119th-congressional-districts"
CD119_SOURCE_GEOIDS = (
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
    "56", "60", "66", "69", "72", "78",
)
CD119_SOURCE_BASE = "https://www2.census.gov/geo/tiger/TIGER2025/CD"
CD119_SOURCE_FILES = tuple(f"tl_2025_{geoid}_cd119.zip" for geoid in CD119_SOURCE_GEOIDS)
CD119_SOURCE_URLS = tuple(
    (f"{CD119_SOURCE_BASE}/{name}", name) for name in CD119_SOURCE_FILES
)

_DESCRIPTION = (
    "119th Congressional Districts. Publisher: U.S. Census Bureau. "
    "TIGER/Line 2025 CD119 source, captured as all 56 state/equivalent ZIP "
    "files from the official Census Bureau directory; strict source validation and "
    "native GeoParquet staging run before a version is written."
)


@asset(
    key=AssetKey(["ingest", CD119_DATASET_SLUG, CD119_FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=_DESCRIPTION,
)
def census_119th_congressional_districts(
    context, staging_storage: StagingStorageResource
) -> dict:
    """Acquire live official sources or replay the pinned preserved capture."""
    return stage_cd119_candidate(
        staging_storage,
        CD119_DATASET_SLUG,
        CD119_FILE_SLUG,
    )
