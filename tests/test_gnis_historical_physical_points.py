"""Focused tests for the USGS GNIS Historical Physical Points draft pipeline."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import AssetCheckSeverity

from dagster_hifld.assets.gnis_historical_physical_points import (
    gnis_historical_physical_points,
    gnis_historical_physical_points_approved_baseline_drift,
    gnis_historical_physical_points_complete,
)
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.sources import gnis_historical_physical_points as src
from dagster_hifld.sources import gnis_historical_points_family as family_src

import gnis_historical_points_support as support
from gnis_historical_points_support import (
    FIXTURE_COUNT,
    FIXTURE_OBJECTIDS,
    GnisLayerTestSpec,
    _fixture_attrs,
    _fixture_layer_fields,
    _fixture_rows,
    _staging,
    fixture_mode,
    make_responder,
)

SPEC = GnisLayerTestSpec(
    source=src,
    featureclasses=("Island", "Bar", "Summit", "Bend", "Valley", "Cape", "Gut",
                    "Woods", "Flat"),
    dataset_slug="historical-physical-points-1",
    layer_id=14,
    layer_name="Historical Physical Points",
)


class GnisHistoricalPhysicalPointsTests(support.GnisHistoricalPointsLayerTests):
    spec = SPEC

    def test_upstream_feature_addition_passes_capture_and_warns_drift(self):
        rows = _fixture_rows(SPEC.featureclasses)
        added = {
            "attributes": {**_fixture_attrs(FIXTURE_OBJECTIDS[0],
                                            SPEC.featureclasses),
                           "OBJECTID": 144, "gaz_id": 9999,
                           "gaz_name": "Newly published feature"},
            "geometry": {"hasZ": True, "points": [[-9496000.0, 4081000.0, 0]]},
        }
        after_ids = [*FIXTURE_OBJECTIDS, 144]
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields(src)})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(src, schema_sha, make_responder(src, rows)):
                before = self._capture()(
                    _staging(root), version="before-run",
                    capture_directory=str(root / "before"))
            with fixture_mode(src, schema_sha,
                              make_responder(src, rows + [added], ids=after_ids)):
                after = self._capture()(
                    _staging(root), version="after-run",
                    capture_directory=str(root / "after"))
        self.assertTrue(after["capture_complete"])
        self.assertEqual(after["feature_count"], FIXTURE_COUNT + 1)
        self.assertTrue(gnis_historical_physical_points_complete(after).passed)
        skipped = gnis_historical_physical_points_approved_baseline_drift(after)
        self.assertEqual(skipped.severity, AssetCheckSeverity.WARN)
        self.assertEqual(skipped.metadata["comparison_status"].value,
                         "skipped_no_approved_baseline")
        drift = approved_baseline_drift(after, {**before, "release_accepted": True})
        self.assertFalse(drift.passed)
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertEqual(drift.metadata["feature_count_delta"].value, 1)
        self.assertTrue(drift.metadata["native_id_roster_changed"].value)

    def test_corrupt_receipt_is_recorded_not_dropped(self):
        src = self.src
        rows = _fixture_rows(SPEC.featureclasses)
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields(src)})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            transport = make_responder(src, rows)

            def corrupt_then_fail(spec, error, runtime, capture_root, **kwargs):
                (Path(capture_root) / "layer_before.receipt.json").write_bytes(b"{")
                raise error("source_schema_contract_drift")

            with fixture_mode(src, schema_sha, transport):
                storage = _staging(root)
                with patch.object(family_src, "_stream_validate",
                                  side_effect=corrupt_then_fail):
                    with self.assertRaisesRegex(
                            self.spec.error, "source_schema_contract_drift"):
                        self._capture()(storage, version="corrupt-run",
                                        capture_directory=str(capture_root))
            quarantined = sorted(
                (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
            self.assertTrue(quarantined)
            failure = json.loads((quarantined[0] / "metadata" / "failure.json")
                                 .read_bytes())
            self.assertEqual(failure["reason_code"], "source_schema_contract_drift")
            not_retained = {entry["name"]: entry["reason"]
                            for entry in failure["not_retained"]}
            self.assertIn("layer_before", not_retained)
            self.assertTrue(not_retained["layer_before"].startswith(
                "retained_capture_receipt_unreadable:layer_before"))
            self.assertIn("ids_seed", failure["captured_names"])


if __name__ == "__main__":
    unittest.main()
