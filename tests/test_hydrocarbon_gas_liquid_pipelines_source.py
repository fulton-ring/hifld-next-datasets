"""Pinned EIA HGL pipelines source contract, drift refusals, and wiring tests."""
import os
import unittest
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from shapely.geometry import LineString, MultiLineString

from dagster_hifld.assets.hydrocarbon_gas_liquid_pipelines import eia_hgl_pipelines_draft_schedule
from dagster_hifld.sources.hydrocarbon_gas_liquid_pipelines import (
    BOUNDS, DATASET_SLUG, EXPECTED_COUNT, EXPECTED_FIELDS, EXPECTED_LAYER,
    EXPECTED_LINESTRING_COUNT, EXPECTED_MULTILINESTRING_COUNT, EXPECTED_VERTEX_COUNT,
    EXPECTED_ZIP_BYTES, EXPECTED_ZIP_SHA256, GEOMETRY_SHA, ID_SHA, MEMBERS,
    RECORD_SHA, SOURCE_MODIFIED, SOURCE_URL, SOURCE_VINTAGE,
    _geometry_wkb, _linestring_wkb, _multilinestring_wkb, _paths, _schema_dbf,
)


class EiaHglPipelinesSourceContractTest(unittest.TestCase):
    def test_pinned_source_and_complete_native_contract(self):
        self.assertEqual(DATASET_SLUG, "hydrocarbon-gas-liquid-pipelines-1")
        self.assertEqual(SOURCE_URL, "https://www.eia.gov/maps/map_data/HGL_Pipelines_US_EIA.zip")
        self.assertEqual(SOURCE_VINTAGE, "2020-01")
        self.assertEqual(SOURCE_MODIFIED, "2020-04-27")
        self.assertEqual(EXPECTED_LAYER, "HGL_Pipelines_US_202001")
        self.assertEqual(EXPECTED_ZIP_BYTES, 52914)
        self.assertEqual(len(EXPECTED_ZIP_SHA256), 64)
        self.assertEqual(EXPECTED_COUNT, 133)
        self.assertEqual(EXPECTED_FIELDS, [("Opername", "C", 150, 0), ("Pipename", "C", 150, 0), ("Shape_Leng", "F", 19, 11)])
        self.assertEqual(MEMBERS, {"HGL_Pipelines_US_202001." + suffix for suffix in
                                   ("cpg", "dbf", "prj", "sbn", "sbx", "shp", "shp.xml", "shx")})
        self.assertEqual(EXPECTED_LINESTRING_COUNT + EXPECTED_MULTILINESTRING_COUNT, EXPECTED_COUNT)
        self.assertEqual(EXPECTED_VERTEX_COUNT, 1759)
        self.assertEqual(BOUNDS[0] < BOUNDS[2] and BOUNDS[1] < BOUNDS[3], True)
        for value in (ID_SHA, RECORD_SHA, GEOMETRY_SHA):
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
        from shapely.geometry import Point
        with self.assertRaises(ValueError):
            _paths(Point(1, 2))
        with self.assertRaises(ValueError):
            _geometry_wkb(Point(1, 2))

    def test_pinned_archive_drift_is_refused(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "tampered.zip").write_bytes(b"PK\x03\x04not-the-pinned-archive")
            with self.assertRaises(ValueError):
                from dagster_hifld.sources.hydrocarbon_gas_liquid_pipelines import _decode
                _decode((root / "tampered.zip").read_bytes(), root)

    def test_dbf_schema_reader_rejects_truncated_header(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "x"
            root.mkdir()
            (root / "t.dbf").write_bytes(b"\x03" + b"\x00" * 8)
            with self.assertRaises(ValueError):
                _schema_dbf(root / "t.dbf")

    def test_refresh_schedule_is_stopped(self):
        self.assertEqual(eia_hgl_pipelines_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)


class EiaHglPipelinesRegistrationTest(unittest.TestCase):
    def test_explicit_ingest_asset_registered_and_never_scheduled_live(self):
        with patch.dict(os.environ, {"HIFLD_SHADOW_MANUAL_ONLY": "1"}):
            from dagster_hifld.definitions import defs
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
            slug = "hydrocarbon-gas-liquid-pipelines-1"
            graph = defs.resolve_asset_graph()
            keys = {tuple(key.path) for key in graph.get_all_asset_keys()}
            self.assertIn(("ingest", slug, slug), keys)
            self.assertIn(("publish", "catalog"), keys)
            self.assertEqual(sum(any(tuple(key.path) == ("ingest", slug, slug) for key in asset.keys)
                                 for asset in hifld_ingest_assets), 1)
            self.assertIsNotNone(defs.resolve_job_def("eia_hgl_pipelines_draft_refresh"))
            self.assertEqual(eia_hgl_pipelines_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()
