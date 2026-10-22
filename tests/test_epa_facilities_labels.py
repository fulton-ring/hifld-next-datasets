import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import AssetKey, DefaultScheduleStatus

from dagster_hifld.assets.epa_facilities_labels import (
    epa_facilities_labels,
    epa_facilities_labels_draft_schedule,
)
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.sources.epa_facilities import (
    EPA_FACILITIES_PROFILE,
    _profile_urls,
    decode_captures,
    inspect_captures,
    validate_native_table,
)
from dagster_hifld.sources.epa_facilities_labels import (
    EPA_FACILITIES_LABELS_PROFILE,
    stage_epa_facilities_labels,
)


class EpaFacilitiesLabelsTest(unittest.TestCase):
    def test_layer_zero_profile_is_separate_and_frozen(self):
        self.assertIs(EPA_FACILITIES_LABELS_PROFILE, EPA_FACILITIES_PROFILE.labels)
        self.assertEqual(EPA_FACILITIES_LABELS_PROFILE.layer_id, 0)
        self.assertEqual(EPA_FACILITIES_LABELS_PROFILE.slug, "epa-facilities-labels")
        self.assertTrue(EPA_FACILITIES_LABELS_PROFILE.schema_verified)
        self.assertTrue(EPA_FACILITIES_LABELS_PROFILE.population_verified)
        self.assertEqual(EPA_FACILITIES_LABELS_PROFILE.count, 77)
        self.assertEqual(EPA_FACILITIES_LABELS_PROFILE.object_ids, tuple(range(1, 78)))
        self.assertEqual(
            EPA_FACILITIES_LABELS_PROFILE.source_record_sha256,
            "074a1648b1913e6d09fd2bd4322084d3a81e01bad87265884c628962addc037e",
        )
        with self.assertRaises((AttributeError, TypeError)):
            EPA_FACILITIES_LABELS_PROFILE.layer_id = 1

    def test_wrapper_delegates_to_shared_lifecycle(self):
        storage = object()
        with patch("dagster_hifld.sources.epa_facilities_labels.stage_epa_facilities") as stage:
            stage.return_value = {"version": "v0.0.0-draft-example"}
            result = stage_epa_facilities_labels(storage, capture_dir="/tmp/capture")
        self.assertEqual(result, {"version": "v0.0.0-draft-example"})
        stage.assert_called_once_with(
            storage,
            profile=EPA_FACILITIES_LABELS_PROFILE,
            capture_dir="/tmp/capture",
        )

    def test_refresh_schedule_remains_stopped(self):
        self.assertEqual(
            epa_facilities_labels_draft_schedule.default_status,
            DefaultScheduleStatus.STOPPED,
        )

    def test_label_asset_is_registered_as_its_own_dataset_file(self):
        self.assertIn(epa_facilities_labels, hifld_ingest_assets)
        key = AssetKey(["ingest", "epa-facilities-labels", "epa-facilities-labels"])
        self.assertEqual(next(iter(epa_facilities_labels.keys)), key)
        from dagster_hifld.definitions import defs

        job = defs.resolve_job_def("epa_facilities_labels_draft_refresh")
        self.assertEqual(job.asset_layer.executable_asset_keys, {key})
        self.assertIn(epa_facilities_labels_draft_schedule, defs.schedules)

    @unittest.skipUnless(
        os.environ.get("HIFLD_EPA_FACILITIES_LABELS_CAPTURE_DIR"),
        "complete producer layer 0 capture is supplied outside the repository",
    )
    def test_complete_real_capture_and_native_decode(self):
        root = Path(os.environ["HIFLD_EPA_FACILITIES_LABELS_CAPTURE_DIR"])
        profile = EPA_FACILITIES_LABELS_PROFILE
        captures = {
            name: (
                (root / f"{name}.body").read_bytes(),
                json.loads((root / f"{name}.receipt.json").read_bytes()),
            )
            for name in _profile_urls(profile)
        }
        audit = inspect_captures(captures, profile=profile)
        table, decoded_audit = decode_captures(captures, profile=profile)
        validate_native_table(table, profile=profile)
        self.assertEqual(audit["feature_count"], 77)
        self.assertEqual(audit["source_record_sha256"], profile.source_record_sha256)
        self.assertEqual(decoded_audit, audit)
        self.assertEqual(table.column_names, ["OBJECTID", "Shape", "SHORTNAME", "RuleID_1"])
        self.assertEqual(str(table.schema.field("RuleID_1").type), "int32")


if __name__ == "__main__":
    unittest.main()
