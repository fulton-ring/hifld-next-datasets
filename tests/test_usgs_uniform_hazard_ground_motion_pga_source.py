"""Pinned USGS 2023 NSHM uniform-hazard PGA source contract, drift refusals, and wiring tests."""
import os
import unittest
from unittest.mock import patch

from dagster import DefaultScheduleStatus

from dagster_hifld.assets.usgs_uniform_hazard_ground_motion_pga import (
    usgs_uniform_hazard_ground_motion_pga_draft_schedule,
)
from dagster_hifld.sources.usgs_uniform_hazard_ground_motion_pga import (
    BOUNDS, DATASET_SLUG, EXPECTED_COUNT, EXPECTED_FIELDS, EXPECTED_INVALID_GEOMETRY_COUNT,
    EXPECTED_LAYER, EXPECTED_MULTIPOLYGON_COUNT, EXPECTED_POLYGON_COUNT, EXPECTED_PRJ,
    EXPECTED_RING_COUNT, EXPECTED_VERTEX_COUNT, EXPECTED_ZIP_BYTES, EXPECTED_ZIP_SHA256,
    FILE_SLUG, GEOMETRY_SHA, ID_SHA, MEMBERS, POLY_MEMBERS, RECORD_SHA, SOURCE_DOI,
    SOURCE_ITEM_ID, SOURCE_ITEM_URL, SOURCE_PACKAGE, SOURCE_URL, _schema_dbf,
)


class UsgsUniformHazardPgaSourceContractTest(unittest.TestCase):
    def test_pinned_source_and_complete_native_contract(self):
        self.assertEqual(DATASET_SLUG, "uniform-hazard-ground-motion-1")
        self.assertEqual(FILE_SLUG, "us-pga-10pct50yrs-bc-poly-1")
        self.assertEqual(SOURCE_ITEM_ID, "64ff886dd34ed30c2057b4d9")
        self.assertEqual(
            SOURCE_ITEM_URL, "https://www.sciencebase.gov/catalog/item/64ff886dd34ed30c2057b4d9")
        self.assertTrue(SOURCE_URL.startswith("https://www.sciencebase.gov/catalog/file/get/" + SOURCE_ITEM_ID))
        self.assertEqual(SOURCE_PACKAGE, "US_PGA_2Pct_5Pct_10Pct_50Yrs_BC.zip")
        self.assertEqual(SOURCE_DOI, "https://doi.org/10.5066/P9GNPCOD")
        self.assertEqual(EXPECTED_LAYER, "US_PGA_10Pct50Yrs_BC_poly")
        self.assertEqual(EXPECTED_ZIP_BYTES, 59_162_562)
        self.assertEqual(EXPECTED_ZIP_SHA256,
                         "b7c5106b66398776f7506a7608e93a51f028b1d21b5fb4d7f738e6aabc621af1")
        self.assertEqual(EXPECTED_COUNT, 4072)
        self.assertEqual(EXPECTED_FIELDS, [
            ("low_cont", "C", 254, 0), ("high_cont", "C", 254, 0),
            ("range_cont", "C", 254, 0), ("valley", "C", 254, 0),
            ("Shape_Leng", "F", 19, 11), ("Shape_Area", "F", 19, 11)])
        self.assertEqual(EXPECTED_POLYGON_COUNT + EXPECTED_MULTIPOLYGON_COUNT, EXPECTED_COUNT)
        self.assertEqual(EXPECTED_RING_COUNT, 6941)
        self.assertEqual(EXPECTED_VERTEX_COUNT, 768552)
        self.assertEqual(EXPECTED_INVALID_GEOMETRY_COUNT, 5)
        self.assertEqual(BOUNDS[0] < BOUNDS[2] and BOUNDS[1] < BOUNDS[3], True)
        self.assertEqual(len(MEMBERS), 29)
        self.assertIn("US_PGA_10Pct50Yrs_BC_poly.shp", MEMBERS)
        self.assertIn("PGA_5Pct_10Pct.lyrx", MEMBERS)
        for suffix in ("dbf", "prj", "shp", "shx"):
            self.assertIn(f"US_PGA_10Pct50Yrs_BC_poly.{suffix}", POLY_MEMBERS)
        for value in (ID_SHA, RECORD_SHA, GEOMETRY_SHA, EXPECTED_ZIP_SHA256):
            self.assertEqual(len(value), 64)
        self.assertTrue(EXPECTED_PRJ.startswith('GEOGCS["GCS_WGS_1984"'))

    def test_dbf_schema_reader_rejects_truncated_header(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "x"
            root.mkdir()
            (root / "t.dbf").write_bytes(b"\x03" + b"\x00" * 8)
            with self.assertRaises(ValueError):
                _schema_dbf(root / "t.dbf")

    def test_pinned_archive_drift_is_refused(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "tampered.zip").write_bytes(b"PK\x03\x04not-the-pinned-archive")
            with self.assertRaises(ValueError):
                from dagster_hifld.sources.usgs_uniform_hazard_ground_motion_pga import _decode
                _decode((root / "tampered.zip").read_bytes(), root)

    def test_same_size_forged_archive_is_refused_by_the_pin(self):
        import io
        import zipfile
        from dagster_hifld.sources.usgs_uniform_hazard_ground_motion_pga import _decode
        from dagster_hifld.sources.usgs_uniform_hazard_ground_motion_pga import (
            EXPECTED_ZIP_BYTES as zbytes,
        )
        # A same-size, same-checksum body is impossible without the pinned bytes;
        # exercise the roster guard through a forged zip that fails the pin first.
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("PGA_2Pct.lyrx", "x")
        body = buf.getvalue() + b"\x00" * (zbytes - len(buf.getvalue()))
        self.assertEqual(len(body), zbytes)
        with self.assertRaises(ValueError):
            _decode(body, None)

    def test_refresh_schedule_is_stopped(self):
        self.assertEqual(usgs_uniform_hazard_ground_motion_pga_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)

    def test_source_admission_records_no_maintained_source_outcome(self):
        from dagster_hifld.sources.usgs_uniform_hazard_ground_motion_pga import (
            ADMISSION_ERROR, SOURCE_ADMISSION,
        )
        self.assertEqual(SOURCE_ADMISSION, "no_maintained_source_park_or_retire")
        self.assertEqual(ADMISSION_ERROR,
                         "source_admission_unresolved_no_maintained_successor_park_or_retire")

    def test_historical_acquisition_is_stopped_by_the_admission_gate(self):
        import tempfile
        from pathlib import Path
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources.usgs_uniform_hazard_ground_motion_pga import (
            capture_usgs_uniform_hazard_ground_motion_pga,
        )
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp) / "stage"
            storage = StagingStorageResource(local_dir=str(staging))
            with self.assertRaises(ValueError) as ctx:
                capture_usgs_uniform_hazard_ground_motion_pga(storage, version="v-test")
            self.assertIn("source_admission_unresolved", str(ctx.exception))
            self.assertFalse(staging.exists())


class UsgsUniformHazardPgaRegistrationTest(unittest.TestCase):
    def test_explicit_ingest_asset_registered_and_never_scheduled_live(self):
        with patch.dict(os.environ, {"HIFLD_SHADOW_MANUAL_ONLY": "1"}):
            from dagster_hifld.definitions import defs
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
            slug, file_slug = DATASET_SLUG, FILE_SLUG
            graph = defs.resolve_asset_graph()
            keys = {tuple(key.path) for key in graph.get_all_asset_keys()}
            self.assertIn(("ingest", slug, file_slug), keys)
            self.assertIn(("publish", "catalog"), keys)
            self.assertEqual(sum(any(tuple(key.path) == ("ingest", slug, file_slug) for key in asset.keys)
                                 for asset in hifld_ingest_assets), 1)
            self.assertIsNotNone(defs.resolve_job_def("usgs_uniform_hazard_ground_motion_pga_draft_refresh"))
            self.assertEqual(usgs_uniform_hazard_ground_motion_pga_draft_schedule.default_status,
                             DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()
