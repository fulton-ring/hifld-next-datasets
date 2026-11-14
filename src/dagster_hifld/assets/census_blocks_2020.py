"""2020 Census tab blocks (TIGER/Line TABBLOCK20, current maintained vintage).

Dataset-specific slugs and file slug helpers live here so custom ingest code can
address the state-level source files. Publishing now discovers staged versions
generically from `dataset_slug/file_slug/version`.

Source: U.S. Census Bureau TIGER/Line TABBLOCK20 shapefiles (2020 tabulation
blocks carried forward in the current annual TIGER release). The vintage below
tracks the maintained publication; verified live 2026-11-14 against the
TIGER2026 release (files last modified 2026-10-06).
"""

from __future__ import annotations

CENSUS_2020_BLOCKS_DATASET_SLUG = "2020-census-blocks-1"

# TIGER/Line TABBLOCK20 layers: one file per state / equivalent area (GEOID).
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


# Current maintained TIGER/Line vintage carrying the 2020 tabulation blocks.
# Verified live 2026-11-14: TIGER2026/TABBLOCK20 serves all 56 files
# (last modified 2026-10-06); superseded vintages remain but are no longer
# the maintained publication.
TIGER_TABBLOCK20_VINTAGE = "TIGER2026"
TIGER_TABBLOCK20_BASE = (
    f"https://www2.census.gov/geo/tiger/{TIGER_TABBLOCK20_VINTAGE}/TABBLOCK20"
)
_GROUP_NAME = "census_bureau"
_DESCRIPTION = (
    "2020 Census Blocks (HIFLD inventory). Publisher: U.S. Census Bureau. "
    f"TIGER/Line {TIGER_TABBLOCK20_VINTAGE} TABBLOCK20 shapefile per state or "
    "equivalent area (2020 tabulation blocks in the maintained annual TIGER "
    "release). Source: www2.census.gov TIGER2026 TABBLOCK20."
)


def tabblock20_file_slug(geoid: str) -> str:
    return f"tl_2026_{geoid}_tabblock20"


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


def _make_tabblock20_ingest_asset(file_slug: str):
    zip_name = f"{file_slug}.zip"
    url = f"{TIGER_TABBLOCK20_BASE}/{zip_name}"

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
        key=AssetKey(["ingest", CENSUS_2020_BLOCKS_DATASET_SLUG, file_slug]),
        group_name=_GROUP_NAME,
        compute_kind="http",
        description=_DESCRIPTION,
    )(compute_fn)


census_blocks_2020_ingest_assets = [
    _make_tabblock20_ingest_asset(file_slug) for file_slug in CENSUS_2020_TABBLOCK20_FILE_SLUGS
]
