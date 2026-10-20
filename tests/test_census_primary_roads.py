import io
import unittest
import zipfile
from unittest.mock import patch

from dagster import DefaultScheduleStatus

from dagster_hifld.assets.census_primary_roads import (
    census_bureau_primary_roads,
    census_primary_roads_draft_schedule,
    census_primary_roads_source_complete,
)
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.sources.census_primary_roads import (
    EXPECTED_BYTES,
    EXPECTED_FEATURE_COUNT,
    EXPECTED_SHA256,
    SOURCE_URL,
    _capture,
    _inspect_archive,
)


class CensusPrimaryRoadsTests(unittest.TestCase):
    def test_source_asset_is_registered_with_stopped_refresh(self):
        keys = {key for definition in hifld_ingest_assets for key in definition.keys}
        self.assertIn(next(iter(census_bureau_primary_roads.keys)), keys)
        self.assertEqual(census_primary_roads_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)

    def test_source_check_requires_pinned_archive_and_complete_population(self):
        candidate = {
            "feature_count": EXPECTED_FEATURE_COUNT,
            "source_population_complete": True,
            "source_archive_bytes": EXPECTED_BYTES,
            "source_archive_sha256": EXPECTED_SHA256,
            "source_id_sha256": "0" * 64,
            "source_schema_sha256": "1" * 64,
        }
        self.assertTrue(census_primary_roads_source_complete(candidate).passed)
        candidate["feature_count"] -= 1
        self.assertFalse(census_primary_roads_source_complete(candidate).passed)

    def test_archive_capture_rejects_changed_url_or_bytes(self):
        with patch(
            "dagster_hifld.sources.census_primary_roads.capture_get",
            return_value=(b"not the pinned Census archive", {
                "method": "GET", "url": SOURCE_URL, "final_url": SOURCE_URL,
                "status": 200, "complete_body": True, "bytes": 27,
                "sha256": "0" * 64, "content_encoding": "identity",
                "wire_bytes": 27, "wire_sha256": "0" * 64,
            }),
        ):
            with self.assertRaisesRegex(ValueError, "official_tiger_archive_identity_or_release_drift"):
                _capture(None)

    def test_archive_member_roster_drift_is_rejected_before_extract(self):
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w") as archive:
            archive.writestr("unexpected.txt", "not a TIGER archive")
        with self.assertRaisesRegex(ValueError, "tiger_archive_member_roster_drift"):
            _inspect_archive(sink.getvalue())


if __name__ == "__main__":
    unittest.main()
