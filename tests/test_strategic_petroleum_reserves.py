"""Focused contract tests for the pinned EIA Strategic Petroleum Reserves adapter."""
import io
import zipfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.strategic_petroleum_reserves import (
    strategic_petroleum_reserves, strategic_petroleum_reserves_complete,
    strategic_petroleum_reserves_draft_schedule,
)
from dagster_hifld.sources import strategic_petroleum_reserves as spr

_MEMBERS = sorted(f"SPR_202101.{suffix}" for suffix in
                  ("cpg", "dbf", "prj", "sbn", "sbx", "shp", "shp.xml", "shx"))


def _zip_with(names, contents=None):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as target:
        for name in names:
            target.writestr(name, (contents or {}).get(name, b"x"))
    return buffer.getvalue()


class StrategicPetroleumReservesSourceContractTest(unittest.TestCase):
    def test_pinned_source_and_complete_native_contract(self):
        self.assertEqual(spr.EXPECTED_COUNT, 4)
        self.assertEqual(spr.SOURCE_RELEASE, "2021-01")
        self.assertEqual(spr.SOURCE_URL, "https://www.eia.gov/maps/map_data/SPR_US_EIA.zip")
        self.assertEqual(spr.EXPECTED_LAYER, "SPR_202101")
        self.assertEqual(spr.EXPECTED_ZIP_BYTES, 5106)
        self.assertEqual(spr.EXPECTED_ZIP_SHA256,
                         "40ef9584d910603980eb1782672136125eb57f749c158e38b5f9174242da4481")
        self.assertEqual([f[0] for f in spr.EXPECTED_FIELDS],
                         ["OBJECTID", "Name", "POINT_X", "POINT_Y", "Source", "Capacity",
                          "Caverns", "PADD", "Data_Perio", "State", "PADD_temp", "Caverns_te"])
        self.assertEqual(spr.CROSSWALK_OPEN_ID, "6bb5f32811f64ca4aed9c50bf3d444e5")
        self.assertIn("GWM_0003", spr.CROSSWALK_GATE_NOTE)
        self.assertIn("499", spr.CROSSWALK_GATE_NOTE)
        self.assertEqual(spr.MEMBERS, set(_MEMBERS))

    def test_geometry_encoding_preserves_native_xy(self):
        self.assertEqual(spr._point_wkb(1.5, -2.25).hex(),
                         "0101000000000000000000f83f00000000000002c0")

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(strategic_petroleum_reserves, hifld_ingest_assets)
        self.assertEqual(strategic_petroleum_reserves_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = strategic_petroleum_reserves_complete({"feature_count": 4,
                                                       "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["source_freshness_accepted"].value)
        failed = strategic_petroleum_reserves_complete({"feature_count": 3,
                                                        "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_decode_refuses_non_pinned_body(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "pinned_eia_snapshot_drift"):
                spr._decode(b"not the pinned zip", Path(tmp))

    def test_decode_refuses_changed_zip_member_roster(self):
        synthetic = _zip_with(_MEMBERS[:-1] + ["SPR_202101.extra"])
        with patch.object(spr, "EXPECTED_ZIP_BYTES", len(synthetic)), \
                patch.object(spr, "EXPECTED_ZIP_SHA256", spr._sha(synthetic)), \
                tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
                spr._decode(synthetic, Path(tmp))

    def test_decode_refuses_unsafe_zip_member_paths(self):
        synthetic = _zip_with(["../SPR_202101.dbf", *_MEMBERS[1:]])
        with patch.object(spr, "EXPECTED_ZIP_BYTES", len(synthetic)), \
                patch.object(spr, "EXPECTED_ZIP_SHA256", spr._sha(synthetic)), \
                tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "source_zip_member_path_rejected"):
                spr._decode(synthetic, Path(tmp))

    def test_decode_refuses_wrong_dbf_schema(self):
        # A valid single-field DBF header must still fail the pinned schema.
        dbf = bytearray(65)
        dbf[0] = 0x03
        dbf[4:8] = (1).to_bytes(4, "little")
        dbf[8:10] = (65).to_bytes(2, "little")
        dbf[10:12] = (20).to_bytes(2, "little")
        dbf[32:64] = b"OnlyField".ljust(11, b"\x00") + b"C" + b"\x00" * 4 + bytes([18]) + b"\x00" * 15
        dbf[64] = 0x0D
        contents = {name: b"x" for name in _MEMBERS}
        contents["SPR_202101.dbf"] = bytes(dbf) + b" " * 19 + b"\x1a"
        synthetic = _zip_with(_MEMBERS, contents)
        with patch.object(spr, "EXPECTED_ZIP_BYTES", len(synthetic)), \
                patch.object(spr, "EXPECTED_ZIP_SHA256", spr._sha(synthetic)), \
                tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "source_dbf_schema_drift"):
                spr._decode(synthetic, Path(tmp))


if __name__ == "__main__":
    unittest.main()
