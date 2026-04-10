"""2020 Census tab blocks (TIGER/Line 2024 TABBLOCK20).

Dataset-specific slugs and (dataset_slug, file_slug) pairs live here so
`partitions.py` only merges `CENSUS_2020_DATASET_FILE_PAIRS` alongside the base
list. Add other large multi-file datasets the same way in their own modules.
"""

from __future__ import annotations

CENSUS_2020_BLOCKS_DATASET_SLUG = "2020-census-blocks-1"

# TIGER/Line 2024 TABBLOCK20 layers: one file per state / equivalent area (GEOID).
CENSUS_2020_TABBLOCK20_STATE_GEOIDS = (
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
    "60",
    "66",
    "69",
    "72",
    "78",
)


def tabblock20_file_slug(geoid: str) -> str:
    return f"tl_2024_{geoid}_tabblock20"


CENSUS_2020_TABBLOCK20_FILE_SLUGS = tuple(
    tabblock20_file_slug(g) for g in CENSUS_2020_TABBLOCK20_STATE_GEOIDS
)

CENSUS_2020_DATASET_FILE_PAIRS = [
    (CENSUS_2020_BLOCKS_DATASET_SLUG, file_slug)
    for file_slug in CENSUS_2020_TABBLOCK20_FILE_SLUGS
]

# ── Dagster ingest assets (one HTTP download per TIGER file) ────────────────

from dagster import AssetKey, asset

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import StagingStorageResource

_TIGER_TABBLOCK20_BASE = (
    "https://www2.census.gov/geo/tiger/TIGER2024/TABBLOCK20"
)
_GROUP_NAME = "census_bureau"
_DESCRIPTION = (
    "2020 Census Blocks (HIFLD inventory). Publisher: U.S. Census Bureau. "
    "TIGER/Line 2024 TABBLOCK20 shapefile per state or equivalent area. "
    "Source: www2.census.gov TIGER2024 TABBLOCK20."
)


def _make_tabblock20_ingest_asset(file_slug: str):
    zip_name = f"{file_slug}.zip"
    url = f"{_TIGER_TABBLOCK20_BASE}/{zip_name}"

    def compute_fn(
        context, staging_storage: StagingStorageResource
    ) -> dict:
        version = build_version_id(context)
        return download_convert_and_stage(
            [(url, zip_name)],
            CENSUS_2020_BLOCKS_DATASET_SLUG,
            file_slug,
            version,
            staging_storage,
        )

    compute_fn.__name__ = f"census_2020_blocks_{file_slug}"
    compute_fn.__qualname__ = f"census_2020_blocks_{file_slug}"

    return asset(
        key=AssetKey([CENSUS_2020_BLOCKS_DATASET_SLUG, file_slug]),
        group_name=_GROUP_NAME,
        compute_kind="http",
        description=_DESCRIPTION,
    )(compute_fn)


census_blocks_2020_ingest_assets = [
    _make_tabblock20_ingest_asset(file_slug) for file_slug in CENSUS_2020_TABBLOCK20_FILE_SLUGS
]
