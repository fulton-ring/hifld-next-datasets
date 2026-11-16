"""Capture integrity is independent of approved-release drift."""
import unittest

from dagster import AssetCheckSeverity
from dagster_hifld.capture_completeness import validate_capture_witness, approved_baseline_drift


class CaptureCompletenessTests(unittest.TestCase):
    def test_complete_addition_or_removal_passes(self):
        for ids in ([1, 2], [1, 2, 3], [2]):
            self.assertEqual(validate_capture_witness(len(ids), ids, len(ids), list(reversed(ids)), ids), ids)

    def test_inconsistent_incomplete_duplicate_or_missing_ids_refused(self):
        for args in [(2, [1, 2], 3, [1, 2, 3], [1, 2]),
                     (2, [1, 2], 2, [1, 3], [1, 2]),
                     (2, [1, 1], 2, [1, 1], [1, 1]),
                     (2, [1, 2], 2, [1, 2], [1]),
                     (2, [1, 2], 2, [1, 2], [1, 1]),
                     (True, [1], 1, [1], [1]),
                     (1, [None], 1, [None], [None])]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                validate_capture_witness(*args)

    def test_drift_uses_explicitly_approved_same_scope_reference(self):
        current = {"dataset_slug": "agency", "file_slug": "points", "feature_count": 3,
                   "source_id_sha256": "new"}
        baseline = {**current, "version": "v-approved", "release_accepted": True,
                    "feature_count": 2, "source_id_sha256": "old"}
        result = approved_baseline_drift(current, baseline)
        self.assertFalse(result.passed)
        self.assertEqual(result.severity, AssetCheckSeverity.WARN)
        self.assertEqual(result.metadata["baseline_version"].value, "v-approved")
        for invalid in ({**baseline, "release_accepted": False}, {**baseline, "file_slug": "other"}):
            with self.assertRaises(ValueError):
                approved_baseline_drift(current, invalid)
        unchanged = approved_baseline_drift(current, {**baseline, **current})
        self.assertTrue(unchanged.passed)

    def test_missing_baseline_is_explicit_skip_and_warning(self):
        result = approved_baseline_drift({"feature_count": 3})
        self.assertFalse(result.passed)
        self.assertEqual(result.severity, AssetCheckSeverity.WARN)
        self.assertEqual(result.metadata["comparison_status"].value, "skipped_no_approved_baseline")
