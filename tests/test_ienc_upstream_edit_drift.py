"""Capture-time completeness accepts an upstream edit and warns on baseline drift."""
import unittest

from dagster import AssetCheckSeverity

from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.sources import (
    roads_usace_ienc,
    usace_crane_locations,
    usace_military_divisions,
    usace_overhead_pipeline_lines,
    wreck_areas_usace_ienc,
)
from dagster_hifld.sources.arcgis_layer import capture_witness


def _witness(before: list[int], after: list[int]) -> dict[str, tuple[bytes, dict]]:
    return {
        "count_before": (b'{"count":%d}' % len(before), {}),
        "ids_before": (b'{"objectIds":%s}' % str(before).replace(" ", "").encode(), {}),
        "count_after": (b'{"count":%d}' % len(after), {}),
        "ids_after": (b'{"objectIds":%s}' % str(after).replace(" ", "").encode(), {}),
    }


class IencUpstreamEditDriftTests(unittest.TestCase):
    def _assert_upstream_add_passes_and_warns(self, module) -> None:
        self.assertIs(module.capture_witness, capture_witness)
        captures = _witness([1, 2, 3], [1, 2, 3])
        self.assertEqual(capture_witness(captures), [1, 2, 3])
        candidate = {
            "dataset_slug": "fixture", "file_slug": "fixture",
            "feature_count": 3, "source_id_sha256": "b" * 64,
        }
        baseline = {
            "release_accepted": True, "version": "v-baseline",
            "dataset_slug": "fixture", "file_slug": "fixture",
            "feature_count": 2, "source_id_sha256": "a" * 64,
        }
        result = approved_baseline_drift(candidate, baseline)
        self.assertFalse(result.passed)
        self.assertEqual(result.severity, AssetCheckSeverity.WARN)
        self.assertEqual(result.metadata["feature_count_delta"].value, 1)
        self.assertTrue(result.metadata["native_id_roster_changed"].value)

    def test_manual_replay_local_roster_upstream_add(self):
        self._assert_upstream_add_passes_and_warns(wreck_areas_usace_ienc)

    def test_manual_replay_shared_roster_upstream_add(self):
        self._assert_upstream_add_passes_and_warns(usace_crane_locations)

    def test_retained_replay_local_roster_upstream_add(self):
        self._assert_upstream_add_passes_and_warns(usace_military_divisions)

    def test_retained_capture_names_shared_roster_upstream_add(self):
        self._assert_upstream_add_passes_and_warns(usace_overhead_pipeline_lines)

    def test_single_module_variant_upstream_add(self):
        self._assert_upstream_add_passes_and_warns(roads_usace_ienc)


if __name__ == "__main__":
    unittest.main()
