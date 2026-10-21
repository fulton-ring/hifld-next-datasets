import hashlib
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import httpx
from dagster import DefaultScheduleStatus

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.puerto_rico_subbarrios import (
    puerto_rico_subbarrios_census,
    puerto_rico_subbarrios_draft_schedule,
    puerto_rico_subbarrios_source_complete,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.puerto_rico_subbarrios import (
    DATASET_SLUG,
    EXPECTED_BYTES,
    EXPECTED_FEATURE_COUNT,
    EXPECTED_GEOID_SHA256,
    EXPECTED_SHA256,
    SOURCE_URL,
    _capture,
    _inspect_archive,
)


def _receipt(body: bytes, wire: bytes) -> dict:
    return {
        "method": "GET", "url": SOURCE_URL, "final_url": SOURCE_URL,
        "status": 200, "complete_body": True, "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(), "content_encoding": "identity",
        "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest(),
        "content_length": len(body), "observed_at": "2026-10-21T00:00:00+00:00",
    }


def _fixture_body() -> bytes | None:
    fixture_path = os.environ.get("HIFLD_PUERTO_RICO_SUBBARRIOS_FIXTURE")
    if fixture_path:
        body = Path(fixture_path).expanduser().read_bytes()
    elif os.environ.get("HIFLD_FETCH_PUERTO_RICO_SUBBARRIOS_FIXTURE") == "1":
        response = httpx.get(SOURCE_URL, follow_redirects=False, timeout=120.0)
        response.raise_for_status()
        body = response.content
    else:
        return None
    if len(body) != EXPECTED_BYTES or hashlib.sha256(body).hexdigest() != EXPECTED_SHA256:
        raise ValueError("fixture_archive_identity_mismatch")
    return body


class PuertoRicoSubbarriosTests(unittest.TestCase):
    def test_explicit_fixture_path_rejects_unpinned_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            fixture_path = Path(tmp) / "source.zip"
            fixture_path.write_bytes(b"changed archive")
            with patch.dict(os.environ, {"HIFLD_PUERTO_RICO_SUBBARRIOS_FIXTURE": str(fixture_path)}, clear=True):
                with self.assertRaisesRegex(ValueError, "fixture_archive_identity_mismatch"):
                    _fixture_body()

    def test_source_asset_is_registered_with_stopped_refresh(self):
        keys = {key for definition in hifld_ingest_assets for key in definition.keys}
        self.assertIn(next(iter(puerto_rico_subbarrios_census.keys)), keys)
        self.assertEqual(puerto_rico_subbarrios_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)

    def test_source_check_requires_pinned_archive_and_complete_population(self):
        candidate = {
            "feature_count": EXPECTED_FEATURE_COUNT,
            "source_population_complete": True,
            "source_archive_bytes": EXPECTED_BYTES,
            "source_archive_sha256": EXPECTED_SHA256,
            "source_id_sha256": EXPECTED_GEOID_SHA256,
        }
        self.assertTrue(puerto_rico_subbarrios_source_complete(candidate).passed)
        candidate["feature_count"] -= 1
        self.assertFalse(puerto_rico_subbarrios_source_complete(candidate).passed)
        candidate["feature_count"] = EXPECTED_FEATURE_COUNT
        candidate["source_id_sha256"] = "0" * 64
        self.assertFalse(puerto_rico_subbarrios_source_complete(candidate).passed)

    def test_archive_capture_rejects_changed_bytes(self):
        body = b"not the pinned Census archive"
        wire = b"wire bytes for changed archive"
        receipt = _receipt(body, wire)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))

            def changed_capture(_client, _url, root, _name, **_kwargs):
                root.mkdir(parents=True, exist_ok=True)
                (root / "source.body").write_bytes(body)
                (root / "source.wire").write_bytes(wire)
                (root / "source.receipt.json").write_text(json.dumps(receipt))
                return body, receipt

            with patch("dagster_hifld.sources.puerto_rico_subbarrios.capture_get", side_effect=changed_capture):
                with self.assertRaisesRegex(ValueError, "official_tiger_archive_identity_or_release_drift"):
                    _capture(None, storage, "vtest")

            keys = storage.list_prefix("_quarantine/" + DATASET_SLUG)
            self.assertEqual(len(keys), 4)
            body_key = next(key for key in keys if key.endswith("/raw/source.body"))
            self.assertEqual(storage.read_key(body_key), body)
            failure_key = next(key for key in keys if key.endswith("/metadata/failure.json"))
            self.assertEqual(json.loads(storage.read_key(failure_key))["reason_code"],
                             "official_tiger_archive_identity_or_release_drift")

    def test_archive_capture_rejects_truncated_response(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))

            def incomplete_capture(_client, _url, root, _name, **_kwargs):
                root.mkdir(parents=True, exist_ok=True)
                (root / "source.body").write_bytes(b"partial")
                (root / "source.wire").write_bytes(b"partial")
                (root / "source.receipt.json").write_text(json.dumps(_receipt(b"partial", b"partial") | {"complete_body": False}))
                raise ValueError("incomplete_source_capture:source")

            with patch("dagster_hifld.sources.puerto_rico_subbarrios.capture_get", side_effect=incomplete_capture):
                with self.assertRaisesRegex(ValueError, "official_tiger_source_capture_failed"):
                    _capture(None, storage, "vtest")
            self.assertTrue(len(storage.list_prefix("_quarantine/" + DATASET_SLUG)) >= 4)

    def test_inspect_archive_rejects_member_roster_drift(self):
        body = _fixture_body()
        if body is None:
            self.skipTest("set HIFLD_PUERTO_RICO_SUBBARRIOS_FIXTURE or HIFLD_FETCH_PUERTO_RICO_SUBBARRIOS_FIXTURE=1")
        mutated = io_bytes_with_extra_member(body)
        with self.assertRaisesRegex(ValueError, "tiger_archive_member_roster_drift"):
            _inspect_archive(mutated)

    def test_inspect_archive_on_pinned_fixture(self):
        body = _fixture_body()
        if body is None:
            self.skipTest("set HIFLD_PUERTO_RICO_SUBBARRIOS_FIXTURE or HIFLD_FETCH_PUERTO_RICO_SUBBARRIOS_FIXTURE=1")
        self.assertEqual(hashlib.sha256(body).hexdigest(), EXPECTED_SHA256)
        audit = _inspect_archive(body)
        self.assertEqual(audit["feature_count"], EXPECTED_FEATURE_COUNT)
        self.assertEqual(audit["source_id_sha256"], EXPECTED_GEOID_SHA256)
        self.assertEqual(audit["source_crs"], "EPSG:4269")
        self.assertEqual(audit["invalid_geometry_count"], 0)


def io_bytes_with_extra_member(body: bytes) -> bytes:
    import io
    source = zipfile.ZipFile(io.BytesIO(body))
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as target:
        for info in source.infolist():
            target.writestr(info.filename, source.read(info.filename))
        target.writestr("tl_2023_72_subbarrio.extra", b"unexpected")
    return buffer.getvalue()


if __name__ == "__main__":
    unittest.main()
