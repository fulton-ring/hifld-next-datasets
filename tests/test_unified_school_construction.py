"""Native school FileGDB current-vintage construction regressions for the Unified layer."""
from __future__ import annotations

import io
import json
import tempfile
import unittest
import zipfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import httpx
import pyogrio

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import census_latest
from dagster_hifld.sources import unified_school_districts as source
from test_census_substate_construction import snapshot
from test_unified_school_districts_source import _fixture_table


ROOT = census_latest.ROOT
DIRECTORY = ROOT + "TGRGDB26/"
URL = DIRECTORY + "tlgdb_2026_us_school.gdb.zip"
ANNUAL = b'<a href="TGRGDB26/">2026 geodatabases</a>'
LISTING = b'<a href="tlgdb_2026_us_school.gdb.zip">school</a>'


def native_archive(root: Path, rows: int) -> bytes:
    root.mkdir(parents=True, exist_ok=True)
    gdb = root / "school.gdb"
    pyogrio.write_arrow(_fixture_table(rows), gdb, layer=source.LAYER_NAME,
                        driver="OpenFileGDB", geometry_name="SHAPE",
                        geometry_type="MultiPolygon", crs="EPSG:4269")
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w") as archive:
        for path in sorted(gdb.iterdir()):
            archive.writestr(gdb.name + "/" + path.name, path.read_bytes())
    return sink.getvalue()


class UnifiedSchoolConstructionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.archive = native_archive(self.root / "a2", rows=2)
        self.archive_changed = native_archive(self.root / "a3", rows=3)
        self.storage = StagingStorageResource(local_dir=str(self.root / "stage"))
        self.responses = {ROOT: ANNUAL, DIRECTORY: LISTING, URL: self.archive}
        self.requests = []

        def respond(request):
            self.requests.append(str(request.url))
            return httpx.Response(200, stream=httpx.ByteStream(self.responses[str(request.url)]),
                                  headers={"Content-Type": "application/zip"})

        self.client = httpx.Client(transport=httpx.MockTransport(respond))
        self.addCleanup(self.client.close)

    def guarded(self):
        """The two-row fixture stands in for the nationwide layer inside the guard."""
        return patch.object(source, "CONFIG",
                            replace(source.CONFIG, feature_count_range=(1, 10)))

    def first_capture(self):
        with self.guarded():
            result = source.capture(self.storage, "first-run", self.client)
        target = self.root / "stage" / self.storage.prefix / source.DATASET_SLUG / source.FILE_SLUG / result["version"]
        before = snapshot(target)
        return result, target, before, before["raw/source_archive.receipt.json"]

    def test_discovery_capture_reports_the_resolved_vintage(self):
        result, target, before, receipt_bytes = self.first_capture()
        self.assertEqual(self.requests, [ROOT, DIRECTORY, URL])
        receipt = json.loads(receipt_bytes)
        release = census_latest.geodatabase_release_from_receipt(
            {**receipt, "url": receipt["requested_url"]}, "school")
        self.assertEqual(release.vintage, 2026)
        self.assertEqual(release.source_file, "tlgdb_2026_us_school.gdb.zip")
        self.assertEqual(release.listing_sha256, source._sha256(LISTING))
        self.assertEqual(release.annual_listing_sha256, source._sha256(ANNUAL))
        self.assertEqual(receipt["source_release"], release.as_receipt_evidence())
        manifest = json.loads((target / "metadata/source_manifest.json").read_bytes())
        self.assertEqual(manifest["source_release"]["vintage"], 2026)
        self.assertEqual(manifest["inputs"][0]["retrieval_url"], URL)
        self.assertIn("2026", manifest["inputs"][0]["input_id"])
        report = json.loads((target / "metadata/native_capture.json").read_bytes())
        self.assertEqual(report["source_release"]["vintage"], 2026)
        self.assertEqual(report["plausible_feature_count_range"], [1, 10])
        self.assertEqual(report["feature_count"], 2)
        self.assertEqual(report["source_url"], URL)

    def test_cold_replay_is_network_free_and_preserves_real_transfer_evidence(self):
        first, _, before, receipt = self.first_capture()
        cold = StagingStorageResource(local_dir=str(self.root / "cold"), prefix="isolated")
        with patch.object(source._shared.httpx, "Client", side_effect=AssertionError("replay used HTTP")):
            result = source.replay_retained_capture(cold, first["version"], self.archive, receipt)
        self.assertEqual(result["version"], first["version"])
        self.assertFalse(result["reused"])
        target = self.root / "cold" / "isolated" / source.DATASET_SLUG / source.FILE_SLUG / result["version"]
        after = snapshot(target)
        for name in before:
            if name != "metadata/native_capture.json":
                self.assertEqual(after[name], before[name], name)
        report = json.loads(after["metadata/native_capture.json"])
        self.assertEqual(report["capture_replay"], {
            "mode": "retained_transfer_replay", "new_network_retrieval_performed": False,
            "original_retrieved_at": json.loads(receipt)["observed_at"]})
        with patch.object(source._shared.httpx, "Client", side_effect=AssertionError("replay used HTTP")):
            reused = source.replay_retained_capture(cold, first["version"], self.archive, receipt)
        self.assertTrue(reused["reused"])
        self.assertEqual(snapshot(target), after)

    def test_changed_upstream_population_within_guard_does_not_hard_fail(self):
        first, target, before, _ = self.first_capture()
        self.responses[URL] = self.archive_changed
        with self.guarded():
            changed = source.capture(self.storage, "changed-run", self.client)
        self.assertEqual(changed["feature_count"], 3)
        self.assertNotEqual(changed["version"], first["version"])
        self.assertEqual(snapshot(target), before)
        report = json.loads(self.storage.read_key(
            f'{source.DATASET_SLUG}/{source.FILE_SLUG}/{changed["version"]}/metadata/native_capture.json'))
        self.assertEqual(report["feature_count"], 3)
        self.assertEqual(report["plausible_feature_count_range"], [1, 10])
        self.assertFalse((self.root / "stage" / "_quarantine").exists())

    def test_population_outside_the_guard_is_quarantined(self):
        with patch.object(source, "CONFIG", replace(source.CONFIG, feature_count_range=(3, 10))):
            with self.assertRaisesRegex(ValueError, "source_layer_population_or_geometry_drift"):
                source.capture(self.storage, "ignored-run", self.client)
        evidence = snapshot(self.root / "stage" / "_quarantine")
        self.assertIn(self.archive, evidence.values())
        self.assertFalse((self.root / "stage" / source.DATASET_SLUG).exists())

    def test_new_release_identity_does_not_reuse_the_prior_vintage(self):
        first, target, before, _ = self.first_capture()
        next_directory = ROOT + "TGRGDB27/"
        next_url = next_directory + "tlgdb_2027_us_school.gdb.zip"
        self.responses.update({ROOT: b'<a href="TGRGDB27/">2027</a>',
                               next_directory: b'<a href="tlgdb_2027_us_school.gdb.zip">school</a>',
                               next_url: self.archive})
        with self.guarded():
            result = source.capture(self.storage, "refresh-run", self.client)
        self.assertNotEqual(result["version"], first["version"])
        self.assertEqual(snapshot(target), before)
        report = json.loads(self.storage.read_key(
            f'{source.DATASET_SLUG}/{source.FILE_SLUG}/{result["version"]}/metadata/native_capture.json'))
        self.assertEqual(report["source_url"], next_url)
        self.assertEqual(report["source_release"]["vintage"], 2027)
        self.assertFalse((self.root / "stage" / "_quarantine").exists())

    def test_published_directory_without_school_archive_has_no_fallback(self):
        self.responses[DIRECTORY] = b'<a href="another-product.gdb.zip">other</a>'
        with self.guarded():
            with self.assertRaisesRegex(ValueError, "published_geodatabase_missing_or_ambiguous"):
                source.capture(self.storage, "ignored-run", self.client)
        self.assertEqual(self.requests, [ROOT, DIRECTORY])
        self.assertFalse((self.root / "stage" / source.DATASET_SLUG).exists())


if __name__ == "__main__":
    unittest.main()
