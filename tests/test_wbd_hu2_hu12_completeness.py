from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qsl, urlsplit

import httpx
import pyarrow.parquet as pq
from dagster import AssetCheckSeverity, DagsterInstance, materialize

from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import wbd_hu_family as family
from dagster_hifld.sources import wbd_2_digit_hu as two
from dagster_hifld.sources import wbd_12_digit_hu as twelve
from dagster_hifld.assets import wbd_2_digit_hu as two_assets
from dagster_hifld.assets import wbd_12_digit_hu as twelve_assets
from tests import test_wbd_2_digit_hu as two_fixture
from tests import test_wbd_12_digit_hu as twelve_fixture


CASES = ((two, two_assets, two_fixture), (twelve, twelve_assets, twelve_fixture))


def population_documents(fixture, roster):
    documents = fixture._documents()
    template = documents["page_0000"]
    features = {row["attributes"]["objectid"]: row
                for name, page in documents.items() if name.startswith("page_")
                for row in page["features"]}
    documents = {name: value for name, value in documents.items() if not name.startswith("page_")}
    for suffix in ("before", "after"):
        documents["ids_" + suffix]["objectIds"] = list(roster)
        documents["count_" + suffix]["count"] = len(roster)
    for index, start in enumerate(range(0, len(roster), 2)):
        rows = []
        for oid in roster[start:start + 2]:
            rows.append(features.get(oid) or {
                "attributes": fixture._attributes(oid),
                "geometry": {"rings": fixture._feature_rings(oid)}})
        documents[f"page_{index:04d}"] = {
            **template, "features": rows, "exceededTransferLimit": start + 2 < len(roster)}
    return documents


class WbdHu2Hu12CompletenessTests(unittest.TestCase):
    def test_http_acquisition_asset_checks_and_sealed_replay(self):
        client_class = httpx.Client

        def request_key(url):
            parsed = urlsplit(str(url))
            return parsed.netloc, parsed.path, tuple(sorted(parse_qsl(parsed.query)))

        for source, assets, fixture in CASES:
            with self.subTest(layer=source.LAYER_ID), tempfile.TemporaryDirectory() as tmp, \
                    DagsterInstance.ephemeral() as instance, \
                    patch.object(source, "PAGE_SIZE", 2), \
                    patch.object(source, "DISK_FLOOR_BYTES", 1):
                roster = [*fixture._TEST_ROSTER, 200_005]
                documents = population_documents(fixture, roster)
                responses = {request_key(url): fixture._encode(documents[name])
                             for name, url in source._capture_roster(roster)}

                def respond(request):
                    return httpx.Response(200, stream=httpx.ByteStream(responses[request_key(request.url)]))

                def client(**kwargs):
                    return client_class(transport=httpx.MockTransport(respond), **kwargs)

                stem = source.__name__.split(".")[-1]
                asset = getattr(assets, stem)
                storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "stage"))

                def run(capture_directory):
                    return materialize(
                        [asset, *assets.ASSET_CHECKS], instance=instance,
                        resources={"staging_storage": storage},
                        run_config={"ops": {asset.op.name: {"config": {
                            "capture_directory": str(capture_directory)}}}})

                with patch.object(family.httpx, "Client", side_effect=client):
                    fresh = run(Path(tmp) / "capture")
                    self.assertTrue(fresh.success)
                    candidate = fresh.output_for_node(asset.op.name)
                    replay = run(candidate["capture_directory"])
                    self.assertTrue(replay.success)
                    reused = replay.output_for_node(asset.op.name)
                self.assertFalse(candidate["reused"])
                self.assertTrue(reused["reused"])
                self.assertEqual(candidate["version"], reused["version"])
                self.assertEqual(candidate["feature_count"], len(roster))
                self.assertEqual(candidate["source_reported_count"], len(roster))
                self.assertTrue(candidate["source_population_complete"])
                self.assertFalse(candidate["release_accepted"])
                self.assertIn(build_publish_partition_key(source.DATASET_SLUG, source.FILE_SLUG,
                                                         candidate["version"]),
                              instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))
                checks = {event.check_name: event for event in fresh.get_asset_check_evaluations()}
                self.assertTrue(checks[stem + "_complete"].passed)
                drift = checks[stem + "_drift"]
                self.assertFalse(drift.passed)
                self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
                self.assertEqual(drift.metadata["comparison_status"].value,
                                 "skipped_no_approved_baseline")
                prefix = candidate["candidate_ready"].removesuffix("metadata/candidate_ready.json")
                table = pq.read_table(io.BytesIO(storage.read_key(
                    prefix + f"geoparquet/{source.FILE_SLUG}.parquet")))
                self.assertEqual(table.num_rows, len(roster))
                self.assertEqual(table.column("objectid").to_pylist(), roster)
                self.assertEqual(table.column("loaddate").to_pylist(),
                                 [fixture._attributes(oid)["loaddate"] for oid in roster])

    def test_added_and_removed_population_passes_and_warns_against_approved_release(self):
        for source, assets, fixture in CASES:
            with self.subTest(layer=source.LAYER_ID), tempfile.TemporaryDirectory() as tmp, \
                    patch.object(source, "PAGE_SIZE", 2):
                baseline = None
                for index, roster in enumerate((fixture._TEST_ROSTER,
                                                [*fixture._TEST_ROSTER, 200_005],
                                                [*fixture._TEST_ROSTER, 200_005, 200_006],
                                                fixture._TEST_ROSTER[:-1])):
                    documents = population_documents(fixture, roster)
                    root = fixture._write_capture_root(Path(tmp) / str(index), documents)
                    audit, _ = source._validate_stream(root, parquet_sink=None)
                    two_fixture._seal_manifest(root, documents)
                    source._read_replay(root)
                    self.assertEqual(audit["source_feature_count"], len(roster))
                    candidate = {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
                                 "feature_count": audit["source_feature_count"],
                                 "source_reported_count": len(roster),
                                 "source_population_complete": True,
                                 "source_id_sha256": audit["source_id_sha256"]}
                    complete = getattr(assets, source.__name__.split(".")[-1] + "_complete")
                    self.assertTrue(complete(candidate).passed)
                    if baseline is None:
                        baseline = {**candidate, "version": "approved-fixture", "release_accepted": True}
                    else:
                        drift = approved_baseline_drift(candidate, baseline)
                        self.assertFalse(drift.passed)
                        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
                        self.assertEqual(drift.metadata["feature_count_delta"].value,
                                         len(roster) - len(fixture._TEST_ROSTER))

    def test_torn_witnesses_and_incomplete_pages_are_refused(self):
        for source, _, fixture in CASES:
            for failure in ("count_after", "ids_after", "duplicate_ids", "missing_row"):
                with self.subTest(layer=source.LAYER_ID, failure=failure), \
                        tempfile.TemporaryDirectory() as tmp, patch.object(source, "PAGE_SIZE", 2):
                    documents = population_documents(fixture, fixture._TEST_ROSTER)
                    if failure == "count_after":
                        documents["count_after"]["count"] += 1
                    elif failure == "ids_after":
                        documents["ids_after"]["objectIds"][-1] += 1
                    elif failure == "duplicate_ids":
                        documents["ids_before"]["objectIds"][1] = documents["ids_before"]["objectIds"][0]
                    else:
                        documents["page_0001"]["features"].pop()
                    root = fixture._write_capture_root(Path(tmp) / "capture", documents)
                    with self.assertRaises(ValueError):
                        source._validate_stream(root, parquet_sink=None)

    def test_asset_checks_require_positive_typed_source_witness(self):
        for source, assets, _ in CASES:
            complete = getattr(assets, source.__name__.split(".")[-1] + "_complete")
            candidate = {"feature_count": 5, "source_reported_count": 5,
                         "source_population_complete": True}
            self.assertTrue(complete(candidate).passed)
            for updates in ({"source_reported_count": None}, {"source_reported_count": True},
                            {"source_reported_count": 0}, {"source_reported_count": -1},
                            {"source_reported_count": 4}, {"source_population_complete": False}):
                with self.subTest(layer=source.LAYER_ID, updates=updates):
                    self.assertFalse(complete({**candidate, **updates}).passed)


if __name__ == "__main__":
    unittest.main()
