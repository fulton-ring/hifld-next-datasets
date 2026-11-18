"""Latest published NCES EDGE release discovery."""

from __future__ import annotations

import unittest

from dagster_hifld.sources import nces_edge_common as common


def _page(body: str) -> bytes:
    return body.encode("utf-8")


class NcesEdgeDiscoveryTest(unittest.TestCase):
    def test_newest_active_anchor_wins_and_commented_future_links_are_ignored(self) -> None:
        page = _page(
            '<main><a href="/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2324.zip">old</a>'
            '<a href="/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2425.zip">new</a>'
            '<!-- <a href="~/data/EDGE_GEOCODE_PUBLICSCH_2526.zip">future</a> -->'
            '<a href="/programs/edge/data/EDGE_GEOCODE_PRIVATESCH_1920.zip">old</a>'
            '<a href="/programs/edge/data/EDGE_GEOCODE_PRIVATESCH_2324.zip">new</a>'
            '<!-- <a href="~/data/EDGE_GEOCODE_PRIVATESCH_2425.zip">future</a> -->'
            "</main>"
        )
        public = common.discover_release(page, "PUBLICSCH")
        self.assertEqual(public["schoolyear"], "2024-2025")
        self.assertEqual(
            public["source_url"],
            "https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2425.zip",
        )
        self.assertEqual(public["stem"], "EDGE_GEOCODE_PUBLICSCH_2425")
        private = common.discover_release(page, "PRIVATESCH")
        self.assertEqual(private["schoolyear"], "2023-2024")
        self.assertEqual(private["stem"], "EDGE_GEOCODE_PRIVATESCH_2324")

    def test_missing_or_ambiguous_release_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "release_missing"):
            common.discover_release(_page("<main>No data links</main>"), "PUBLICSCH")
        duplicate = _page(
            '<main><a href="/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2425.zip">a</a>'
            '<a href="https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2425.zip">b</a>'
            "</main>"
        )
        with self.assertRaisesRegex(ValueError, "release_ambiguous"):
            common.discover_release(duplicate, "PUBLICSCH")

    def test_release_from_url_accepts_only_official_adjacent_year_archives(self) -> None:
        release = common.release_from_url(
            "https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PRIVATESCH_2324.zip",
            "PRIVATESCH",
        )
        self.assertEqual(release["schoolyear"], "2023-2024")
        self.assertEqual(release["stem"], "EDGE_GEOCODE_PRIVATESCH_2324")
        for url in (
            "https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PRIVATESCH_2412.zip",
            "https://example.com/programs/edge/data/EDGE_GEOCODE_PRIVATESCH_2324.zip",
            "https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2324.zip",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                common.release_from_url(url, "PRIVATESCH")

    def test_publisher_record_count_helpers_reject_broken_headers(self) -> None:
        dbf = bytearray(33 + 1)
        dbf[4:8] = (1).to_bytes(4, "little")
        dbf[8:10] = (33).to_bytes(2, "little")
        dbf[10:12] = (1).to_bytes(2, "little")
        dbf[32] = 0x0D
        dbf[33] = 0x20
        self.assertEqual(common.dbf_record_count(bytes(dbf), "test"), 1)
        broken = bytes(dbf[:-1])
        with self.assertRaisesRegex(ValueError, "test_dbf_header_or_length_drift"):
            common.dbf_record_count(broken, "test")


if __name__ == "__main__":
    unittest.main()
