"""Focused tests for the 2020 Census Blocks (TABBLOCK20) ingest wiring."""

import unittest
from urllib.parse import urlparse

from dagster import AssetKey

from dagster_hifld.assets.census_blocks_2020 import (
    CENSUS_2020_BLOCKS_DATASET_SLUG,
    CENSUS_2020_DATASET_FILE_PAIRS,
    CENSUS_2020_TABBLOCK20_FILE_SLUGS,
    CENSUS_2020_TABBLOCK20_STATE_GEOIDS,
    TIGER_TABBLOCK20_BASE,
    census_blocks_2020_ingest_assets,
    tabblock20_file_slug,
)
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILE_PAIRS
from dagster_hifld.definitions import defs

# Census state / equivalent-area FIPS for 2020 tabulation blocks: 50 states,
# DC (11), American Samoa (60), Guam (66), CNMI (69), Puerto Rico (72) and
# US Virgin Islands (78).
EXPECTED_2020_BLOCK_GEOIDS = frozenset(
    {
        "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
        "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
        "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
        "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
        "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
        "56", "60", "66", "69", "72", "78",
    }
)


class CensusBlocks2020Tests(unittest.TestCase):
    def test_roster_covers_every_state_and_equivalent_area_once(self):
        self.assertEqual(len(CENSUS_2020_TABBLOCK20_STATE_GEOIDS), 56)
        self.assertEqual(set(CENSUS_2020_TABBLOCK20_STATE_GEOIDS), EXPECTED_2020_BLOCK_GEOIDS)
        self.assertEqual(len(set(CENSUS_2020_TABBLOCK20_STATE_GEOIDS)), 56)

    def test_file_slugs_track_the_current_maintained_tiger_vintage(self):
        self.assertEqual(tabblock20_file_slug("01"), "tl_2026_01_tabblock20")
        self.assertEqual(
            CENSUS_2020_TABBLOCK20_FILE_SLUGS,
            tuple(tabblock20_file_slug(g) for g in CENSUS_2020_TABBLOCK20_STATE_GEOIDS),
        )
        self.assertEqual(len(set(CENSUS_2020_TABBLOCK20_FILE_SLUGS)), 56)

    def test_every_download_url_is_the_census_bureau_publication(self):
        parsed = urlparse(TIGER_TABBLOCK20_BASE)
        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "www2.census.gov")
        self.assertTrue(parsed.path.startswith("/geo/tiger/TIGER2026/TABBLOCK20"))
        for geoid, file_slug in zip(
            CENSUS_2020_TABBLOCK20_STATE_GEOIDS, CENSUS_2020_TABBLOCK20_FILE_SLUGS
        ):
            expected_url = f"{TIGER_TABBLOCK20_BASE}/tl_2026_{geoid}_tabblock20.zip"
            self.assertEqual(f"{TIGER_TABBLOCK20_BASE}/{file_slug}.zip", expected_url)
            self.assertEqual(file_slug, f"tl_2026_{geoid}_tabblock20")

    def test_pairs_match_the_file_roster_and_are_supported_downstream(self):
        self.assertEqual(
            CENSUS_2020_DATASET_FILE_PAIRS,
            [(CENSUS_2020_BLOCKS_DATASET_SLUG, slug) for slug in CENSUS_2020_TABBLOCK20_FILE_SLUGS],
        )
        self.assertTrue(set(CENSUS_2020_DATASET_FILE_PAIRS).issubset(set(SUPPORTED_DATASET_FILE_PAIRS)))

    def test_one_unique_registered_ingest_asset_per_source_file(self):
        asset_keys = [a.key for a in census_blocks_2020_ingest_assets]
        self.assertEqual(len(asset_keys), 56)
        self.assertEqual(len(set(asset_keys)), 56)
        for file_slug in CENSUS_2020_TABBLOCK20_FILE_SLUGS:
            self.assertIn(
                AssetKey(["ingest", CENSUS_2020_BLOCKS_DATASET_SLUG, file_slug]),
                asset_keys,
            )
        registered = {
            key.to_user_string()
            for definition in hifld_ingest_assets
            for key in definition.keys
        }
        self.assertTrue(
            {k.to_user_string() for k in asset_keys}.issubset(registered)
        )

    def test_ingest_assets_declare_http_capture_for_the_census_bureau(self):
        for definition in census_blocks_2020_ingest_assets:
            self.assertEqual(
                definition.op.tags.get("dagster/compute_kind"), "http"
            )
            self.assertIn("U.S. Census Bureau", definition.op.description)
            self.assertIn("TIGER2026", definition.op.description)

    def test_no_schedule_publishes_census_blocks_without_acceptance(self):
        scheduled = {s.name for s in defs.schedules}
        for name in scheduled:
            self.assertNotIn("census_blocks_2020", name)
            self.assertNotIn("2020-census-blocks", name)


if __name__ == "__main__":
    unittest.main()
