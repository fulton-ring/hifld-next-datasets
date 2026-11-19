"""Native school FileGDB discovery and immutable construction regressions."""
from __future__ import annotations

import io
import copy
import json
import tempfile
import unittest
import zipfile
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpx
import pyogrio

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import census_latest
from dagster_hifld.sources import school_district_administrative_areas as source
from test_census_substate_construction import snapshot
from test_school_district_administrative_areas_source import _fixture_table


ROOT = census_latest.ROOT
DIRECTORY = ROOT + "TGRGDB26/"
URL = DIRECTORY + "tlgdb_2026_us_school.gdb.zip"
ANNUAL = b'<a href="TGRGDB26/">2026 geodatabases</a>'
LISTING = b'<a href="tlgdb_2026_us_school.gdb.zip">school</a>'


def native_archive(root):
    gdb = root / "school.gdb"
    pyogrio.write_arrow(_fixture_table(), gdb, layer=source.LAYER_NAME,
                        driver="OpenFileGDB", geometry_name="SHAPE",
                        geometry_type="MultiPolygon", crs="EPSG:4269")
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w") as archive:
        for path in sorted(gdb.iterdir()):
            archive.writestr(gdb.name + "/" + path.name, path.read_bytes())
    return sink.getvalue()


class AdministrativeSchoolConstructionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.archive = native_archive(self.root)
        self.storage = StagingStorageResource(local_dir=str(self.root / "stage"))
        self.responses = {ROOT: ANNUAL, DIRECTORY: LISTING, URL: self.archive}
        self.requests = []

        def respond(request):
            self.requests.append(str(request.url))
            return httpx.Response(200, stream=httpx.ByteStream(self.responses[str(request.url)]),
                                  headers={"Content-Type": "application/zip"})

        self.client = httpx.Client(transport=httpx.MockTransport(respond))
        self.addCleanup(self.client.close)

    def first_capture(self):
        result = source.capture(self.storage, client=self.client)
        target = self.root / "stage" / source.DATASET_SLUG / source.FILE_SLUG / result["version"]
        before = snapshot(target)
        return result, target, before, before["raw/source_archive.receipt.json"]

    def test_discovery_capture_and_original_preservation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = native_archive(root)
            storage = StagingStorageResource(local_dir=str(root / "stage"))
            listings = {ROOT: ANNUAL, DIRECTORY: LISTING}
            requests = []

            def respond(request):
                requests.append(str(request.url))
                return httpx.Response(200, stream=httpx.ByteStream({**listings, URL: archive}[str(request.url)]),
                                      headers={"Content-Type": "application/zip"})

            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                first = source.capture(storage, "ignored-run-id", client)
                self.assertEqual(requests, [ROOT, DIRECTORY, URL])
                target = root / "stage" / source.DATASET_SLUG / source.FILE_SLUG / first["version"]
                before = snapshot(target)
                witnesses = snapshot(root / "stage" / "_construction-state")
                receipt_bytes = before["raw/source_archive.receipt.json"]
                receipt = json.loads(receipt_bytes)
                release = census_latest.geodatabase_release_from_receipt(
                    {**receipt, "url": receipt["requested_url"]}, "school")
                self.assertEqual(receipt["source_release"], release.as_receipt_evidence())
                self.assertEqual(release.listing_sha256, source._sha256(LISTING))
                self.assertEqual(release.annual_listing_sha256, source._sha256(ANNUAL))
                self.assertEqual(release.listing_bytes, len(LISTING))
                self.assertTrue(witnesses)
                listings[ROOT] += b"<!-- unrelated annual update -->"
                listings[DIRECTORY] += b"<!-- unrelated directory update -->"
                refreshed = source.capture(storage, "another-run-id", client)
                self.assertEqual(refreshed["version"], first["version"])
                self.assertTrue(refreshed["reused"])
                self.assertEqual(snapshot(target), before)
                self.assertEqual(snapshot(root / "stage" / "_construction-state"), witnesses)
                with patch.object(source._shared.httpx, "Client", side_effect=AssertionError("replay used HTTP")):
                    replay = source.replay_retained_capture(storage, first["version"], archive, receipt_bytes)
                self.assertTrue(replay["reused"])
                self.assertEqual(snapshot(target), before)
                self.assertEqual(snapshot(root / "stage" / "_construction-state"), witnesses)
                self.assertFalse((root / "stage" / "_quarantine").exists())

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

    def test_each_native_runtime_identity_change_gets_a_distinct_candidate(self):
        first, target, before, receipt = self.first_capture()
        witnesses = snapshot(self.root / "stage" / "_construction-state")
        shared = source._shared
        versions = {first["version"]}
        changes = [(shared.pyogrio, "__gdal_version_string__"), (shared.pyogrio, "__version__"),
                   (shared.np, "__version__"), (shared.pa, "__version__"),
                   (shared.pyproj, "__version__"), (shared.pyproj, "proj_version_str"),
                   (shared.shapely, "__version__"), (shared.shapely, "geos_version_string"),
                   (shared.httpx, "__version__")]
        for module, name in changes:
            with self.subTest(module=module.__name__, name=name), patch.object(module, name, "99.0.0"):
                result = source.replay_retained_capture(self.storage, first["version"], self.archive, receipt)
                self.assertNotIn(result["version"], versions)
                versions.add(result["version"])
                self.assertFalse(result["reused"])
                self.assertEqual(snapshot(target), before)
                all_witnesses = snapshot(self.root / "stage" / "_construction-state")
                self.assertEqual({name: all_witnesses[name] for name in witnesses}, witnesses)
        self.assertFalse((self.root / "stage" / "_quarantine").exists())

    def test_new_release_identity_does_not_reuse_the_prior_vintage(self):
        first, target, before, _ = self.first_capture()
        next_directory = ROOT + "TGRGDB27/"
        next_url = next_directory + "tlgdb_2027_us_school.gdb.zip"
        self.responses.update({ROOT: b'<a href="TGRGDB27/">2027</a>',
                               next_directory: b'<a href="tlgdb_2027_us_school.gdb.zip">school</a>',
                               next_url: self.archive})
        result = source.capture(self.storage, client=self.client)
        self.assertNotEqual(result["version"], first["version"])
        self.assertEqual(snapshot(target), before)
        report = json.loads(self.storage.read_key(
            f'{source.DATASET_SLUG}/{source.FILE_SLUG}/{result["version"]}/metadata/native_capture.json'))
        self.assertEqual(report["source_url"], next_url)
        self.assertEqual(report["source_release"]["vintage"], 2027)

    def test_invalid_receipts_are_quarantined_without_touching_original_candidates(self):
        first, target, before, receipt_bytes = self.first_capture()
        original = json.loads(receipt_bytes)
        witnesses = snapshot(self.root / "stage" / "_construction-state")
        mutations = {
            "alias": lambda receipt: receipt.update(url=ROOT + "TGRGDB25/tlgdb_2025_us_school.gdb.zip"),
            "source": lambda receipt: receipt.update(requested_url="https://mirror.invalid/school.zip"),
            "host": lambda receipt: receipt.update(final_host="mirror.invalid"),
            "redirect": lambda receipt: receipt.update(final_url="https://mirror.invalid/school.zip"),
            "method": lambda receipt: receipt.update(method="POST"),
            "status": lambda receipt: receipt.update(status=503),
            "encoding": lambda receipt: receipt.update(content_encoding="gzip"),
            "type": lambda receipt: receipt.update(content_type="text/html"),
            "partial": lambda receipt: receipt.update(transfer_complete=False),
            "error": lambda receipt: receipt.update(transfer_error_code="incomplete"),
            "hash": lambda receipt: receipt.update(wire_sha256="f" * 64),
            "length": lambda receipt: receipt.update(content_length_header=len(self.archive) + 1),
            "timestamp": lambda receipt: receipt.update(observed_at="2026-11-19"),
            "path": lambda receipt: receipt.update(raw_archive_key="../escape.zip"),
            "release": lambda receipt: receipt["source_release"].update(vintage=2025),
            "evidence": lambda receipt: receipt["source_release"].update(listing_sha256="0" * 64),
        }
        for name, mutate in mutations.items():
            with self.subTest(mutation=name):
                receipt = copy.deepcopy(original)
                mutate(receipt)
                with self.assertRaises(ValueError):
                    source.replay_retained_capture(self.storage, first["version"], self.archive,
                                                   source._json_bytes(receipt))
                self.assertEqual(snapshot(target), before)
                self.assertEqual(snapshot(self.root / "stage" / "_construction-state"), witnesses)
        self.assertEqual(len(list((self.root / "stage" / "_quarantine").rglob("failure.json"))), len(mutations))

    def test_cache_and_first_construction_corruption_fail_closed(self):
        first, target, before, receipt = self.first_capture()
        witnesses_root = self.root / "stage" / "_construction-state"
        witnesses = snapshot(witnesses_root)
        parquet_name = "geoparquet/" + source.FILE_SLUG + ".parquet"
        (target / parquet_name).write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "cached_candidate_object_integrity_failed"):
            source.replay_retained_capture(self.storage, first["version"], self.archive, receipt)
        self.assertEqual((target / parquet_name).read_bytes(), b"tampered")
        self.assertEqual(snapshot(witnesses_root), witnesses)
        self.assertFalse((target / "metadata/quarantine.json").exists())
        (target / parquet_name).write_bytes(before[parquet_name])
        next(witnesses_root.rglob("bundle.json.first-construction.json")).unlink()
        damaged = snapshot(witnesses_root)
        with self.assertRaisesRegex(ValueError, "construction_missing_original_state"):
            source.replay_retained_capture(self.storage, first["version"], self.archive, receipt)
        self.assertEqual(snapshot(target), before)
        self.assertEqual(snapshot(witnesses_root), damaged)

    def test_failed_live_capture_retains_transfer_and_discovery_evidence(self):
        self.responses[URL] = b"not-a-file-geodatabase"
        with self.assertRaisesRegex(ValueError, "source_archive_invalid_zip"):
            source.capture(self.storage, client=self.client)
        evidence = snapshot(self.root / "stage" / "_quarantine")
        self.assertTrue(any(name.endswith("annual-listing.body") for name in evidence))
        self.assertIn(b"not-a-file-geodatabase", evidence.values())
        self.assertFalse((self.root / "stage" / source.DATASET_SLUG).exists())

    def test_partial_live_transfer_preserves_actual_wire_and_failed_receipt(self):
        class Interrupted(httpx.SyncByteStream):
            def __iter__(self):
                yield b"partial-school-archive"
                raise httpx.ReadError("fixture stream interrupted")

        def respond(request):
            stream = Interrupted() if str(request.url) == URL else httpx.ByteStream(
                self.responses[str(request.url)])
            return httpx.Response(200, stream=stream, headers={"Content-Type": "application/zip"})

        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            with self.assertRaisesRegex(ValueError, "source_archive_transfer_drift"):
                source.capture(self.storage, client=client)
        evidence = snapshot(self.root / "stage" / "_quarantine")
        self.assertIn(b"partial-school-archive", evidence.values())
        receipts = [json.loads(body) for name, body in evidence.items()
                    if name.endswith("source_archive.receipt.json")]
        self.assertEqual(len(receipts), 1)
        self.assertFalse(receipts[0]["transfer_complete"])
        self.assertEqual(receipts[0]["wire_sha256"], source._sha256(b"partial-school-archive"))
        self.assertFalse((self.root / "stage" / source.DATASET_SLUG).exists())

    def test_published_directory_without_school_archive_has_no_fallback(self):
        self.responses[DIRECTORY] = b'<a href="another-product.gdb.zip">other</a>'
        with self.assertRaisesRegex(ValueError, "published_geodatabase_missing_or_ambiguous"):
            source.capture(self.storage, client=self.client)
        self.assertEqual(self.requests, [ROOT, DIRECTORY])
        evidence = snapshot(self.root / "stage" / "_quarantine")
        self.assertTrue(any(name.endswith("annual-listing.body") for name in evidence))
        self.assertFalse((self.root / "stage" / source.DATASET_SLUG).exists())

    def test_native_population_outside_the_guard_is_quarantined(self):
        with patch.object(source, "CONFIG", replace(source.CONFIG, feature_count_range=(3, 1_000))):
            with self.assertRaisesRegex(ValueError, "source_layer_population_or_geometry_drift"):
                source.capture(self.storage, client=self.client)
        evidence = snapshot(self.root / "stage" / "_quarantine")
        self.assertIn(self.archive, evidence.values())
        self.assertFalse((self.root / "stage" / source.DATASET_SLUG).exists())

    def test_asset_registers_the_resolved_candidate_version(self):
        from dagster_hifld.assets import school_district_administrative_areas as asset_module
        from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key

        first, _, _, _ = self.first_capture()
        context = SimpleNamespace(add_output_metadata=Mock(), instance=Mock())
        function = asset_module.census_school_district_administrative_areas.op.compute_fn.decorated_fn
        with patch.object(asset_module, "capture", return_value=first) as capture:
            result = function(context, self.storage)
        capture.assert_called_once_with(self.storage)
        key = build_publish_partition_key(source.DATASET_SLUG, source.FILE_SLUG, first["version"])
        context.instance.add_dynamic_partitions.assert_called_once_with(PUBLISH_PARTITION_NAME, [key])
        self.assertEqual(result["partition_key"], key)
