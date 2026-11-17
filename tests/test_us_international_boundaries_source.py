"""Pinned Census TIGER/Line international boundary source contract, drift refusals, and wiring tests."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from shapely.geometry import LineString, MultiLineString, Point

from dagster_hifld.assets.us_international_boundaries import us_international_boundaries_draft_schedule
from dagster_hifld.sources.us_international_boundaries import (
    DATASET_SLUG, EXPECTED_BOUNDS, EXPECTED_COUNT, EXPECTED_FIELDS,
    EXPECTED_LAYER, EXPECTED_LINESTRING_COUNT, EXPECTED_MULTILINESTRING_COUNT,
    EXPECTED_PRJ_SHA256, EXPECTED_VALUE_SETS, EXPECTED_VERTEX_COUNT,
    EXPECTED_ZIP_BYTES, EXPECTED_ZIP_SHA256, GEOMETRY_SHA, ID_SHA, MEMBERS,
    RECORD_SHA, SOURCE_MODIFIED, SOURCE_URL, SOURCE_VINTAGE,
    _geometry_wkb, _linestring_wkb, _multilinestring_wkb, _paths, _schema_dbf,
)


class UsInternationalBoundariesSourceContractTest(unittest.TestCase):
    def test_pinned_source_and_complete_native_contract(self):
        self.assertEqual(DATASET_SLUG, "us-international-boundaries-1")
        self.assertEqual(SOURCE_URL, "https://www2.census.gov/geo/tiger/TIGER2026/INTERNATIONALBOUNDARY/tl_2026_us_internationalboundary.zip")
        self.assertEqual(SOURCE_VINTAGE, "2026")
        self.assertEqual(SOURCE_MODIFIED, "2026-10-07")
        self.assertEqual(EXPECTED_LAYER, "tl_2026_us_internationalboundary")
        self.assertEqual(EXPECTED_ZIP_BYTES, 847497)
        self.assertEqual(EXPECTED_ZIP_SHA256, "1d139e519a018b7d7f294ef4f7fe2c4068d62d2a7a25b8b0a09e4899fdb835a3")
        self.assertEqual(EXPECTED_COUNT, 4255)
        self.assertEqual(EXPECTED_FIELDS, [("IBTYPE", "C", 1, 0), ("IBSTATUS", "C", 1, 0), ("MTFCC", "C", 5, 0)])
        self.assertEqual(MEMBERS, {"tl_2026_us_internationalboundary." + suffix for suffix in
                                   ("cpg", "dbf", "prj", "shp", "shp.ea.iso.xml", "shp.iso.xml", "shx")})
        self.assertEqual(EXPECTED_LINESTRING_COUNT + EXPECTED_MULTILINESTRING_COUNT, EXPECTED_COUNT)
        self.assertEqual(EXPECTED_VERTEX_COUNT, 68151)
        self.assertEqual(EXPECTED_BOUNDS[0] < EXPECTED_BOUNDS[2] and EXPECTED_BOUNDS[1] < EXPECTED_BOUNDS[3], True)
        self.assertEqual(EXPECTED_BOUNDS, [-141.002782, 25.837048, -66.885444, 69.703648])
        self.assertEqual(EXPECTED_VALUE_SETS, {"IBTYPE": ("A", "C", "M"), "IBSTATUS": ("U", "V"),
                                               "MTFCC": ("H1100", "H3010", "H3020", "P0001", "S1400", "S1740")})
        self.assertEqual(EXPECTED_PRJ_SHA256, "0b9041e921d9ebb43247d314608fe9e38a0b008ee793067fc1806199ea1fb9dd")
        for value in (ID_SHA, RECORD_SHA, GEOMETRY_SHA, EXPECTED_ZIP_SHA256, EXPECTED_PRJ_SHA256):
            self.assertEqual(len(value), 64)

    def test_geometry_encoding_preserves_native_line_vertices(self):
        line = _linestring_wkb([(1.0, 2.0), (3.0, 4.5)])
        self.assertEqual(line, bytes.fromhex("010200000002000000" "000000000000f03f" "0000000000000040"
                                             "0000000000000840" "0000000000001240"))
        multi = _multilinestring_wkb([[(1.0, 2.0), (3.0, 4.5)], [(-1.0, -2.0), (-3.0, -4.5)]])
        self.assertEqual(multi[:9].hex(), "010500000002000000")
        self.assertEqual(len(_paths(MultiLineString([LineString([(1, 2), (3, 4)]), LineString([(5, 6), (7, 8)])]))), 2)
        self.assertEqual(_paths(LineString([(1, 2), (3, 4)])), [[(1.0, 2.0), (3.0, 4.0)]])
        self.assertEqual(_geometry_wkb(LineString([(1, 2), (3, 4)])), _linestring_wkb([(1, 2), (3, 4)]))
        self.assertEqual(_geometry_wkb(MultiLineString([LineString([(1, 2), (3, 4)])])), _multilinestring_wkb([[(1, 2), (3, 4)]]))

    def test_unexpected_geometry_is_refused(self):
        with self.assertRaises(ValueError):
            _paths(Point(1, 2))
        with self.assertRaises(ValueError):
            _geometry_wkb(Point(1, 2))

    def test_pinned_archive_drift_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "tampered.zip").write_bytes(b"PK\x03\x04not-the-pinned-census-archive")
            with self.assertRaises(ValueError):
                from dagster_hifld.sources.us_international_boundaries import _decode
                _decode((root / "tampered.zip").read_bytes(), root)

    def test_dbf_schema_reader_rejects_truncated_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "x"
            root.mkdir()
            (root / "t.dbf").write_bytes(b"\x03" + b"\x00" * 8)
            with self.assertRaises(ValueError):
                _schema_dbf(root / "t.dbf")

    def test_refresh_schedule_is_stopped(self):
        self.assertEqual(us_international_boundaries_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)


class UsInternationalBoundariesRegistrationTest(unittest.TestCase):
    def test_explicit_ingest_asset_registered_and_never_scheduled_live(self):
        with patch.dict(os.environ, {"HIFLD_SHADOW_MANUAL_ONLY": "1"}):
            from dagster_hifld.definitions import defs
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
            slug = "us-international-boundaries-1"
            graph = defs.resolve_asset_graph()
            keys = {tuple(key.path) for key in graph.get_all_asset_keys()}
            self.assertIn(("ingest", slug, slug), keys)
            self.assertIn(("publish", "catalog"), keys)
            self.assertEqual(sum(any(tuple(key.path) == ("ingest", slug, slug) for key in asset.keys)
                                 for asset in hifld_ingest_assets), 1)
            self.assertIsNotNone(defs.resolve_job_def("us_international_boundaries_draft_refresh"))
            self.assertEqual(us_international_boundaries_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()
