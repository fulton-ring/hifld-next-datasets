from __future__ import annotations

import copy
import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs

import httpx
import pyarrow.parquet as pq
from dagster import AssetCheckSeverity, DagsterInstance, materialize

from dagster_hifld.assets import pad_us_protection_mechanism_category as assets
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import pad_us_protection_mechanism_category as source
from tests import test_pad_us_protection_mechanism_category as fixture


class PadUsProtectionCompletenessTests(unittest.TestCase):
    def fixture_context(self, transport):
        stack = ExitStack()
        stack.enter_context(patch.object(source, "PAGE_SIZE", 2))
        stack.enter_context(patch.object(source, "MIN_FREE_BYTES", 1))
        stack.enter_context(patch.object(source, "EXPECTED_SCHEMA_SHA256",
                                         source._profile_schema({"fields": fixture._fixture_layer_fields()})[1]))
        stack.enter_context(patch.object(source, "_default_client",
                                         side_effect=lambda: httpx.Client(transport=transport)))
        stack.enter_context(patch.object(source, "_doc_client",
                                         side_effect=lambda: httpx.Client(transport=transport)))
        return stack

    def test_population_add_remove_acquisition_replay_and_refresh(self):
        rows = fixture._fixture_rows()
        added = copy.deepcopy(rows[0])
        added["attributes"]["OBJECTID"] = 13
        added["geometry"] = None
        with tempfile.TemporaryDirectory() as tmp, DagsterInstance.ephemeral() as instance:
            root = Path(tmp)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "stage"))
            versions = set()
            baseline = None
            for index, population in enumerate((rows, [*rows, added], rows[:-1])):
                transport = fixture.make_responder(population)
                with self.fixture_context(transport):
                    run = materialize(
                        [assets.pad_us_protection_mechanism_category, *assets.ASSET_CHECKS],
                        instance=instance, resources={"staging_storage": storage},
                        run_config={"ops": {assets.pad_us_protection_mechanism_category.op.name: {
                            "config": {"capture_directory": str(root / f"capture-{index}")}}}})
                    self.assertTrue(run.success)
                    candidate = run.output_for_node(assets.pad_us_protection_mechanism_category.op.name)
                    versions.add(candidate["version"])
                    self.assertEqual(candidate["feature_count"], len(population))
                    self.assertEqual(candidate["source_reported_count"], len(population))
                    self.assertTrue(candidate["source_population_complete"])
                    self.assertEqual(candidate["source_null_geometry_count"], int(index == 1))
                    self.assertFalse(candidate["release_accepted"])
                    checks = {event.check_name: event for event in run.get_asset_check_evaluations()}
                    self.assertTrue(checks["pad_us_protection_mechanism_category_complete"].passed)
                    skipped = checks["pad_us_protection_mechanism_category_drift"]
                    self.assertEqual(skipped.severity, AssetCheckSeverity.WARN)
                    self.assertEqual(skipped.metadata["comparison_status"].value,
                                     "skipped_no_approved_baseline")
                    if baseline is None:
                        baseline = {**candidate, "version": "approved-fixture", "release_accepted": True}
                    else:
                        drift = approved_baseline_drift(candidate, baseline)
                        self.assertFalse(drift.passed)
                        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
                        self.assertEqual(drift.metadata["feature_count_delta"].value, len(population) - 5)
                    prefix = candidate["candidate_ready"].removesuffix("metadata/candidate_ready.json")
                    original = {key: storage.read_key(key) for key in storage.list_prefix(prefix)}
                    with patch.object(httpx.Client, "send", side_effect=AssertionError(
                            "retained replay must not attempt HTTP")) as send:
                        replay = source.capture_pad_us_protection_mechanism_category(
                            storage, version="replay", capture_directory=str(root / f"capture-{index}"))
                    send.assert_not_called()
                    refreshed = source.capture_pad_us_protection_mechanism_category(
                        storage, version="refresh", capture_directory=str(root / f"refresh-{index}"))
                    for result in (replay, refreshed):
                        self.assertTrue(result["reused"])
                        self.assertEqual(result["version"], candidate["version"])
                        self.assertEqual(result["source_null_geometry_count"], int(index == 1))
                    self.assertEqual(original, {key: storage.read_key(key) for key in original})
                    table = pq.read_table(root / f"capture-{index}" / f"{source.FILE_SLUG}.parquet")
                    self.assertEqual(table.column("OBJECTID").to_pylist(),
                                     [row["attributes"]["OBJECTID"] for row in population])
                    self.assertEqual(table.num_rows, len(population))
            self.assertEqual(len(versions), 3)

    def test_torn_witnesses_and_missing_rows_are_quarantined(self):
        rows = fixture._fixture_rows()
        for failure in ("count_after", "ids_after", "missing_row"):
            base = fixture.make_responder(rows, data_rows=rows[:-1] if failure == "missing_row" else rows)
            seen = {"count": 0, "ids": 0}

            def respond(request):
                query = parse_qs(request.url.query.decode())
                response = base.handler(request)
                kind = "count" if query.get("returnCountOnly") == ["true"] else \
                    "ids" if query.get("returnIdsOnly") == ["true"] else None
                if kind:
                    seen[kind] += 1
                    if failure == kind + "_after" and seen[kind] == 2:
                        document = json.loads(response.read())
                        if kind == "count":
                            document["count"] += 1
                        else:
                            document["objectIds"][-1] = 13
                        return httpx.Response(200, stream=fixture.JSONStream(document))
                return response

            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp, \
                    self.fixture_context(httpx.MockTransport(respond)):
                root = Path(tmp)
                storage = StagingStorageResource(use_local=True, local_dir=str(root / "stage"))
                with self.assertRaises(ValueError):
                    source.capture_pad_us_protection_mechanism_category(
                        storage, version="torn", capture_directory=str(root / "capture"))
                self.assertFalse(storage.list_prefix(source.DATASET_SLUG))
                self.assertTrue(storage.list_prefix("_quarantine/" + source.DATASET_SLUG))


if __name__ == "__main__":
    unittest.main()
