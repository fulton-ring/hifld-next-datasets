import os
import unittest

from dagster_hifld.real_gcs_e2e import REAL_GCS_E2E_CASES, run_real_gcs_e2e_case


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
                formats = {file["format"] for file in result["api_payload"]["files"]}

                self.assertIn("metadata", formats)
                self.assertIn("geopackage", formats)
                self.assertIn("geoparquet", formats)
                self.assertTrue(result["quality_manifest"]["feature_count"] > 0)
                self.assertIn("columns", result["data_dictionary"])
                self.assertIn("title", result["data_dictionary"])


if __name__ == "__main__":
    unittest.main()
