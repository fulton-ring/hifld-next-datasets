import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Point

from dagster_hifld.real_gcs_e2e import (
    REAL_GCS_E2E_CASES,
    RealGcsE2ECase,
    run_real_gcs_e2e_case,
)
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class GcsE2EHelperTests(unittest.TestCase):
    def test_requires_both_cloud_write_gates_before_accessing_storage(self):
        for environment, expected in (
            ({}, "HIFLD_RUN_GCS_E2E"),
            ({"HIFLD_RUN_GCS_E2E": "1"}, "HIFLD_E2E_CONFIRM_PROD_WRITE"),
        ):
            with (
                self.subTest(environment=environment),
                patch.dict(os.environ, environment, clear=True),
                self.assertRaisesRegex(RuntimeError, expected),
            ):
                run_real_gcs_e2e_case(RealGcsE2ECase("dataset", "file"))

    def test_smoke_helper_verifies_storage_and_metadata_without_api(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            staging = StagingStorageResource(
                local_dir=f"{tmpdir}/staging", use_local=True
            )
            published = PublishedStorageResource(
                local_dir=f"{tmpdir}/published", use_local=True
            )
            source = (
                Path(staging.local_dir) / "dataset/file/v1.0.0/geopackage/source.gpkg"
            )
            source.parent.mkdir(parents=True)
            gpd.GeoDataFrame(
                {"ID": ["001", "007"]},
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            ).to_file(source, driver="GPKG", index=False)
            staging.write_key(
                "dataset/file/metadata/source_manifest.json", b'{"title":"File"}'
            )
            with (
                patch.dict(
                    os.environ,
                    {
                        "HIFLD_RUN_GCS_E2E": "1",
                        "HIFLD_E2E_CONFIRM_PROD_WRITE": "1",
                        "PATH": os.environ.get("PATH", ""),
                    },
                    clear=True,
                ),
                patch(
                    "dagster_hifld.real_gcs_e2e.StagingStorageResource",
                    return_value=staging,
                ),
                patch(
                    "dagster_hifld.real_gcs_e2e.PublishedStorageResource",
                    return_value=published,
                ),
            ):
                result = run_real_gcs_e2e_case(RealGcsE2ECase("dataset", "file"))

            self.assertEqual(result["quality_manifest"]["feature_count"], 2)
            self.assertEqual(result["data_dictionary"]["title"], "File")
            self.assertIn("columns", result["data_dictionary"])
            self.assertTrue(result["outputs"])
            self.assertTrue(
                all(published.object_exists(key) for key in result["published_keys"])
            )


@unittest.skipUnless(
    os.environ.get("HIFLD_RUN_GCS_E2E") == "1"
    and os.environ.get("HIFLD_E2E_CONFIRM_PROD_WRITE") == "1",
    "Set HIFLD_RUN_GCS_E2E=1 and HIFLD_E2E_CONFIRM_PROD_WRITE=1 to publish real GCS E2E cases.",
)
class RealGcsE2ETests(unittest.TestCase):
    def test_allowlisted_real_staging_cases_publish_expected_outputs(self):
        for case in REAL_GCS_E2E_CASES:
            with self.subTest(case=f"{case.dataset_slug}/{case.file_slug}"):
                result = run_real_gcs_e2e_case(case)
                formats = {output.format_type for output in result["outputs"]}

                self.assertIn("metadata", formats)
                self.assertIn("geopackage", formats)
                self.assertIn("geoparquet", formats)
                self.assertTrue(result["published_keys"])
                self.assertTrue(
                    any("/metadata/" in key for key in result["published_keys"])
                )
                self.assertTrue(
                    any("/geoparquet/" in key for key in result["published_keys"])
                )
                feature_count = result["quality_manifest"]["feature_count"]
                self.assertIsInstance(feature_count, int)
                if isinstance(feature_count, int):
                    self.assertGreater(feature_count, 0)
                self.assertIn("columns", result["data_dictionary"])
                self.assertIn("title", result["data_dictionary"])


if __name__ == "__main__":
    unittest.main()
