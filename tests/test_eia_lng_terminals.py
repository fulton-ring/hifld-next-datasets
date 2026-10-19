"""Focused safeguards for the frozen EIA LNG source candidate."""
import io
import zipfile
from pathlib import Path

import unittest

from dagster_hifld.sources.eia_lng_terminals import (
    EXPECTED_ARCHIVE_SHA256,
    SOURCE_URL,
    _safe_zip,
    build_candidate,
)


class EiaLNGTests(unittest.TestCase):
    def test_pinned_eia_archive_builds_lossless_native_candidate(self):
        source = Path("/tmp/hifld-native-reconstruction/144/source-evidence/eia_lng_terminals.zip")
        if not source.exists():
            self.skipTest("external official source snapshot is not part of the test fixture")
        archive = source.read_bytes()
        parquet, audit = build_candidate(archive)
        self.assertGreater(len(parquet), 0)
        self.assertEqual(audit["archive_sha256"], EXPECTED_ARCHIVE_SHA256)
        self.assertEqual(audit["feature_count"], 8)
        self.assertEqual(audit["source_fields"], ["Facility", "Owner", "Operator", "City", "County", "State", "Functions", "Regas_Bcfd", "Stora_Bcf", "Liq_Bcfd", "Period", "Source", "Latitude", "Longitude"])
        self.assertTrue(audit["source_fid_semantics"].startswith("Shapefile positional"))
        self.assertEqual(audit["crs"], "EPSG:4326")
        self.assertTrue(SOURCE_URL.startswith("https://www.eia.gov/"))

    def test_changed_archive_is_quarantined_before_native_build(self):
        with self.assertRaisesRegex(ValueError, "source_archive_digest_drift"):
            build_candidate(b"changed source bytes")

    def test_zip_path_traversal_is_rejected(self):
        body = io.BytesIO()
        with zipfile.ZipFile(body, "w") as zf:
            for suffix in ("cpg", "dbf", "prj", "sbn", "sbx", "shp", "shx", "shp.xml"):
                zf.writestr("../LNG." + suffix, b"x")
        with self.assertRaisesRegex(ValueError, "source_zip_member_roster_or_path_drift"):
            _safe_zip(body.getvalue(), Path(self._testMethodName))

if __name__ == "__main__":
    unittest.main()
