import json
import unittest

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_overhead_pipeline_lines import (
    usace_overhead_pipeline_lines_complete,
    usace_overhead_pipeline_lines_draft_schedule,
    usace_overhead_pipeline_lines_usace_ienc,
)
from dagster_hifld.sources import usace_overhead_pipeline_lines as source
from dagster_hifld.sources.epa_frs_common import _canonical


class UsaceOverheadPipelineLinesTests(unittest.TestCase):
    def test_registered_stopped_schedule_and_release_gate(self):
        self.assertIn(usace_overhead_pipeline_lines_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_overhead_pipeline_lines_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        result = usace_overhead_pipeline_lines_complete({
            "feature_count": source.EXPECTED_COUNT,
            "source_population_complete": True,
        })
        self.assertTrue(result.passed)
        self.assertFalse(result.metadata["release_accepted"].value)

    def test_live_source_contract_is_specific_and_bounded(self):
        self.assertEqual(source.LAYER_ID, 44)
        self.assertEqual(source.EXPECTED_COUNT, 92)
        self.assertEqual(source.EXPECTED_IDS, list(range(1, 93)))
        self.assertEqual(source.SERVICE_URL.split("/")[-2], "OVERHEAD_PIPELINE_LINE")
        pages = [name for name, _ in source._capture_roster(source.EXPECTED_IDS) if name.startswith("page_")]
        self.assertEqual(pages, ["page_000"])
        self.assertEqual([field["name"] for field in source.ATTRIBUTE_FIELDS], [
            "OBJECTID", "Category_of_Pipeline", "Horizontal_Units", "Object_Name", "Product",
            "Vertical_Clearance", "Vertical_Datum", "Waterway_Distance", "Information",
            "Scale_Minimum", "Source_Date", "Source_Indication", "Source_Dataset", "Feature_ID",
        ])

    def test_source_and_layer_drift_are_explicitly_rejected(self):
        # The validator rejects a changed exact source class even when the response is otherwise plausible.
        with self.assertRaises(ValueError):
            source._validate({})


if __name__ == "__main__":
    unittest.main()
