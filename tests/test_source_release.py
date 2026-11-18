"""Shared release detection: unchanged polls, corrections and staging boundaries."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from dagster_hifld.assets.staging_refresh import (
    PUBLICATION_ASSET_KEYS, STAGING_SHARED_ASSET_KEYS, staging_capture_selection,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_release import (
    CORRECTION_NOTE, ReleaseSignal, canonical, decide_release, decide_unchanged_poll,
    load_release_state, probe_transport, record_release_state, release_state_key,
    release_state_record, unchanged_result,
)
from dagster_hifld.sources import census_primary_roads as roads
from dagster_hifld.sources import epa_rcra_tsd as rcra
from test_census_latest import ROOT, CurrentRoadsTests
from test_epa_rcra_tsd import _fixture


def _signal(**overrides):
    values = {
        "release_id": "2027", "release_date": None, "last_edit": "2026-11-01",
        "poll_fingerprint": "a" * 64, "content_sha256": "b" * 64,
        "selection_rule": "fixture", "evidence": {"edition": "2027"},
    }
    values.update(overrides)
    return ReleaseSignal(**values)


def _state(signal=None, *, version="v1", action="new_release", summary=None):
    signal = signal or _signal()
    return release_state_record(
        signal, dataset_slug="d", file_slug="f", version=version,
        accepted_at="2026-11-16T00:00:00Z", action=action,
        candidate_summary=summary or {"feature_count": 1}, note=None,
    )


class ReleaseDecisionTests(unittest.TestCase):
    def test_new_unchanged_and_same_release_correction(self):
        current = _signal()
        self.assertEqual(decide_release(None, current).action, "new_release")
        prior = _state(current)
        self.assertEqual(decide_release(prior, current).action, "unchanged")
        correction = decide_release(prior, _signal(content_sha256="c" * 64))
        self.assertEqual(correction.action, "same_release_correction")
        self.assertEqual(correction.note, CORRECTION_NOTE)
        self.assertEqual(decide_release(prior, _signal(release_id="2028")).action, "new_release")

    def test_poll_needs_prior_identity_and_reliable_state(self):
        current = _signal()
        self.assertEqual(decide_unchanged_poll(None, current).action, "new_release")
        prior = _state(current)
        self.assertEqual(decide_unchanged_poll(prior, current).action, "unchanged")
        self.assertEqual(decide_unchanged_poll(prior, _signal(release_id="2028")).action, "new_release")
        self.assertEqual(
            decide_unchanged_poll(prior, _signal(poll_fingerprint=None)).action,
            "new_release",
        )
        self.assertEqual(
            decide_unchanged_poll(prior, _signal(poll_fingerprint="d" * 64)).action,
            "new_release",
        )

    def test_content_is_required_for_a_correction_decision(self):
        with self.assertRaisesRegex(ValueError, "release_content_fingerprint_required"):
            decide_release(_state(), _signal(content_sha256=None))

    def test_state_round_trip_and_idempotent_write(self):
        signal = _signal()
        record = _state(signal)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            self.assertIsNone(load_release_state(storage, "d", "f"))
            self.assertEqual(record_release_state(storage, record), record)
            self.assertEqual(record_release_state(storage, record), record)
            loaded = load_release_state(storage, "d", "f")
            self.assertEqual(loaded, record)
            self.assertEqual(loaded["state_fingerprint"], signal.state_fingerprint)
            key = release_state_key("d", "f")
            self.assertTrue(storage.object_exists(key))
            noop = unchanged_result(loaded, dataset_slug="d", file_slug="f")
            self.assertTrue(noop["unchanged"])
            self.assertTrue(noop["reused"])
            self.assertEqual(noop["version"], "v1")

    def test_tampered_state_is_rejected(self):
        record = _state()
        record["content_sha256"] = "e" * 64
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            storage.write_key(release_state_key("d", "f"),
                              json.dumps(record, sort_keys=True, separators=(",", ":")).encode() + b"\n")
            with self.assertRaisesRegex(ValueError, "release_state_fingerprint_mismatch"):
                load_release_state(storage, "d", "f")

    def test_transport_probe_requires_a_publisher_validator(self):
        def respond(request):
            return httpx.Response(200, headers={}, request=request)
        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            self.assertIsNone(probe_transport(client, "https://example.test/file.zip").fingerprint())

        def etag(request):
            return httpx.Response(200, headers={"ETag": '"v1"'}, request=request)
        with httpx.Client(transport=httpx.MockTransport(etag)) as client:
            first = probe_transport(client, "https://example.test/file.zip")
            self.assertIsNotNone(first.fingerprint())
            self.assertEqual(first.fingerprint(), first.fingerprint())

    def test_staging_selection_excludes_publication(self):
        self.assertFalse(PUBLICATION_ASSET_KEYS & STAGING_SHARED_ASSET_KEYS)
        for key in PUBLICATION_ASSET_KEYS:
            with self.assertRaises(ValueError):
                staging_capture_selection(type("Asset", (), {"keys": {key}})())


class TigerReleaseTests(unittest.TestCase):
    def _run(self, storage, state, requests):
        zip_url = ROOT + "TIGER2027/PRIMARYROADS/tl_2027_us_primaryroads.zip"

        def respond(request):
            url = str(request.url)
            if url == ROOT:
                return httpx.Response(200, stream=httpx.ByteStream(b'<a href="TIGER2027/">2027</a>'), request=request)
            if url.endswith("/PRIMARYROADS/"):
                return httpx.Response(200, stream=httpx.ByteStream(b'<a href="tl_2027_us_primaryroads.zip">zip</a>'), request=request)
            if url == zip_url:
                if request.method == "HEAD":
                    return httpx.Response(200, headers={"ETag": state["etag"],
                                                        "Last-Modified": state["modified"]}, request=request)
                requests.append(("GET", url))
                return httpx.Response(200, stream=httpx.ByteStream(state["archive"]), request=request)
            raise AssertionError(url)

        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            return roads.capture_latest_census_primary_roads(storage, client=client)

    def test_unchanged_poll_writes_nothing_and_correction_adds_one_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first_archive, _ = CurrentRoadsTests.tiny_archive(root, edition="2027")
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "stage"))
            state = {"etag": '"v1"', "modified": "Tue, 01 Nov 2026 00:00:00 GMT",
                     "archive": first_archive}
            requests = []
            first = self._run(storage, state, requests)
            before = set(storage.list_prefix(roads.DATASET_SLUG))
            self.assertFalse(first["unchanged"])
            self.assertFalse(first["release_accepted"])

            requests.clear()
            second = self._run(storage, state, requests)
            self.assertTrue(second["unchanged"])
            self.assertEqual(second["version"], first["version"])
            self.assertEqual([method for method, _ in requests], [], "unchanged poll must not download the archive")
            self.assertEqual(set(storage.list_prefix(roads.DATASET_SLUG)), before)

            corrected_archive, _ = CurrentRoadsTests.tiny_archive(root, count=2, edition="2027")
            state.update({"etag": '"v2"', "modified": "Wed, 02 Nov 2026 00:00:00 GMT",
                          "archive": corrected_archive})
            requests.clear()
            corrected = self._run(storage, state, requests)
            self.assertEqual(corrected["release_action"], "same_release_correction")
            self.assertEqual(corrected["release_note"], CORRECTION_NOTE)
            self.assertNotEqual(corrected["version"], first["version"])
            self.assertEqual([method for method, _ in requests], ["GET"])
            self.assertTrue(before.issubset(set(storage.list_prefix(roads.DATASET_SLUG))))
            old_prefix = f"{roads.DATASET_SLUG}/{roads.FILE_SLUG}/{first['version']}"
            self.assertTrue(all(storage.object_exists(key) for key in before))
            state_record = load_release_state(storage, roads.DATASET_SLUG, roads.FILE_SLUG)
            self.assertEqual(state_record["action"], "same_release_correction")
            self.assertEqual(state_record["note"], CORRECTION_NOTE)
            self.assertEqual(state_record["version"], corrected["version"])
            audit = json.loads(storage.read_key(
                f"{roads.DATASET_SLUG}/{roads.FILE_SLUG}/{corrected['version']}/metadata/source_audit.json"))
            self.assertEqual(audit["release_content_sha256"], corrected["source_archive_sha256"])
            self.assertIn(old_prefix + "/metadata/source_manifest.json", before)


class ArcgisReleaseTests(unittest.TestCase):
    def _fixture_payloads(self, *, last_edit=100, registry_id="000000000001"):
        fields, rows, docs = _fixture()
        rows = [dict(row) for row in rows]
        rows[0] = {"attributes": {**rows[0]["attributes"], "REGISTRY_ID": registry_id},
                   "geometry": rows[0]["geometry"]}
        docs = json.loads(json.dumps(docs))
        docs["layer_before"]["editingInfo"] = {"lastEditDate": last_edit}
        docs["layer_after"]["editingInfo"] = {"lastEditDate": last_edit}
        payloads = {name: json.dumps(value, separators=(",", ":")).encode()
                    for name, value in docs.items()}
        payloads["ids_seed"] = json.dumps({"objectIds": [1, 2]}).encode()
        payloads["page_000"] = json.dumps({"features": rows, "exceededTransferLimit": False,
                                           "spatialReference": {"wkid": 4269}}).encode()
        return payloads

    def _capture(self, storage, root, payloads, fetched):
        def fake_capture(_client, url, capture_root, name, *, max_bytes):
            fetched.append(name)
            body = payloads[name]
            capture_root.mkdir(parents=True, exist_ok=True)
            (capture_root / f"{name}.wire").write_bytes(body)
            receipt = {"method": "GET", "url": url, "final_url": url,
                       "observed_at": "2026-11-16T00:00:00+00:00", "status": 200,
                       "complete_body": True, "wire_bytes": len(body),
                       "wire_sha256": hashlib.sha256(body).hexdigest(),
                       "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            return body, receipt

        identity = {"code_commit": "fixture", "files": {}, "python": "fixture"}
        original_validator = rcra.validate_capture_documents
        with (patch.object(rcra, "capture_get", side_effect=fake_capture),
              patch.object(rcra, "validate_capture_documents",
                           side_effect=lambda captured: original_validator(captured, expected_schema_sha256=None)),
              patch.object(rcra, "_identity", return_value=identity),
              patch.object(rcra, "validate_construction_code_identity")):
            return rcra.capture_epa_rcra_tsd(storage, version="fixture-run",
                                             capture_directory=str(root / "captures"))

    def test_unchanged_poll_skips_pages_and_correction_creates_a_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "stage"))
            fetched = []
            first = self._capture(storage, root, self._fixture_payloads(), fetched)
            before = set(storage.list_prefix(rcra.DATASET_SLUG))
            self.assertIn("page_000", fetched)

            fetched.clear()
            second = self._capture(storage, root, self._fixture_payloads(), fetched)
            self.assertTrue(second["unchanged"])
            self.assertEqual(second["version"], first["version"])
            self.assertNotIn("page_000", fetched)
            self.assertEqual(set(storage.list_prefix(rcra.DATASET_SLUG)), before)

            fetched.clear()
            corrected = self._capture(
                storage, root,
                self._fixture_payloads(last_edit=101, registry_id="000000000099"), fetched,
            )
            self.assertEqual(corrected["release_action"], "same_release_correction")
            self.assertEqual(corrected["release_note"], CORRECTION_NOTE)
            self.assertNotEqual(corrected["version"], first["version"])
            self.assertIn("page_000", fetched)
            self.assertTrue(before.issubset(set(storage.list_prefix(rcra.DATASET_SLUG))))
            state_record = load_release_state(storage, rcra.DATASET_SLUG, rcra.FILE_SLUG)
            self.assertEqual(state_record["action"], "same_release_correction")
            self.assertEqual(state_record["note"], CORRECTION_NOTE)
            self.assertEqual(state_record["version"], corrected["version"])
            audit = json.loads(storage.read_key(
                f"{rcra.DATASET_SLUG}/{rcra.FILE_SLUG}/{corrected['version']}/metadata/source_audit.json"))
            self.assertEqual(audit["release_content_sha256"], state_record["content_sha256"])
            expected_fingerprint = hashlib.sha256(canonical({
                "identity_fingerprint": state_record["identity_fingerprint"],
                "content_sha256": state_record["content_sha256"],
            })).hexdigest()
            self.assertEqual(audit["release_state_fingerprint"], expected_fingerprint)


if __name__ == "__main__":
    unittest.main()
