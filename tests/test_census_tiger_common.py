"""Focused tests for the shared Census TIGER ZIP capture and archive inspection helpers."""
import hashlib
import httpx
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import census_primary_roads as primary_roads
from dagster_hifld.sources import census_railroads as railroads
from dagster_hifld.sources.census_tiger_common import (
    candidate_version,
    capture_tiger_zip,
    packaged_code_identity,
    read_tiger_shapefile,
)

MEMBERS = {"m.cpg", "m.dbf", "m.prj", "m.shp", "m.shp.ea.iso.xml", "m.shp.iso.xml", "m.shx"}
OTHER_TIGER_URL = railroads.SOURCE_URL


def _receipt(body: bytes, wire: bytes, url: str) -> dict:
    return {
        "method": "GET", "url": url, "final_url": url,
        "status": 200, "complete_body": True,
        "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
        "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest(),
        "content_encoding": "identity", "content_length": len(body),
        "observed_at": "2026-10-20T00:00:00+00:00",
    }


def _archive(names, payload: bytes = b"fixture") -> bytes:
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w") as archive:
        for name in names:
            archive.writestr(zipfile.ZipInfo(name), payload, compress_type=zipfile.ZIP_STORED)
    return sink.getvalue()


def _write_replay_cache(root: Path, body: bytes, wire: bytes, receipt: dict) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "source.body").write_bytes(body)
    (root / "source.wire").write_bytes(wire)
    (root / "source.receipt.json").write_text(json.dumps(receipt))


def _no_live_capture(*_args, **_kwargs):
    raise AssertionError("replay tests must not perform a live capture")


class PackagedIdentityTests(unittest.TestCase):
    def test_packaged_code_identity_hashes_package_tree_without_git(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "src"
            package = root / "dagster_hifld"
            package.mkdir(parents=True)
            (package / "alpha.py").write_text("VALUE = 1\n")
            (package / "nested").mkdir()
            (package / "nested" / "beta.py").write_text("VALUE = 2\n")
            first = packaged_code_identity(root, package)
            self.assertRegex(first, r"^[0-9a-f]{64}$")
            self.assertEqual(first, packaged_code_identity(root, package))
            (package / "alpha.py").write_text("VALUE = 3\n")
            self.assertNotEqual(first, packaged_code_identity(root, package))

    def test_candidate_version_binds_source_and_code_identity(self):
        base = candidate_version("a" * 64, "b" * 64)
        self.assertRegex(base, r"^v[0-9a-f]{32}$")
        self.assertEqual(base, candidate_version("a" * 64, "b" * 64))
        self.assertNotEqual(base, candidate_version("c" * 64, "b" * 64))
        self.assertNotEqual(base, candidate_version("a" * 64, "d" * 64))


class CaptureTigerZipTests(unittest.TestCase):
    URL = primary_roads.SOURCE_URL
    BODY = b"exact pinned census archive bytes"
    WIRE = BODY

    def _replay(self, storage, cache: Path, url=None):
        return capture_tiger_zip(
            cache, storage, "vreplay",
            dataset_slug="test-tiger-zip", source_url=url or self.URL,
            max_source_bytes=1_000_000,
            expected_bytes=len(self.BODY), expected_sha256=hashlib.sha256(self.BODY).hexdigest(),
            client_timeout=httpx.Timeout(180.0, connect=20.0),
            capture_tmp_prefix="test-tiger-capture-",
            capture_get=_no_live_capture,
        )

    def test_replay_returns_body_receipt_wire_and_raw_receipt(self):
        receipt = _receipt(self.BODY, self.WIRE, self.URL)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            cache = Path(tmp) / "cache"
            _write_replay_cache(cache, self.BODY, self.WIRE, receipt)
            body, captured_receipt, wire, receipt_bytes = self._replay(storage, cache)
            self.assertEqual(body, self.BODY)
            self.assertEqual(captured_receipt, receipt)
            self.assertEqual(wire, self.WIRE)
            self.assertEqual(receipt_bytes, (cache / "source.receipt.json").read_bytes())

    def test_replay_rejects_receipt_url_from_another_dataset(self):
        receipt = _receipt(self.BODY, self.WIRE, OTHER_TIGER_URL)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            cache = Path(tmp) / "cache"
            _write_replay_cache(cache, self.BODY, self.WIRE, receipt)
            with self.assertRaisesRegex(ValueError, "source_capture_receipt_identity_mismatch"):
                self._replay(storage, cache)

    def test_replay_rejects_corrupt_cached_body(self):
        receipt = _receipt(self.BODY, self.WIRE, self.URL)
        cached = b"corrupted cached body bytes"
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            cache = Path(tmp) / "cache"
            _write_replay_cache(cache, cached, self.WIRE, receipt)
            with self.assertRaisesRegex(ValueError, "source_capture_receipt_identity_mismatch"):
                self._replay(storage, cache)

    def test_replay_quarantines_self_consistent_release_drift(self):
        drifted = b"self-consistent but unpinned archive bytes"
        receipt = _receipt(drifted, self.WIRE, self.URL)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            cache = Path(tmp) / "cache"
            _write_replay_cache(cache, drifted, self.WIRE, receipt)
            with self.assertRaisesRegex(ValueError, "official_tiger_archive_identity_or_release_drift"):
                self._replay(storage, cache)
            keys = storage.list_prefix("_quarantine/test-tiger-zip")
            self.assertEqual(len(keys), 4)
            body_key = next(key for key in keys if key.endswith("/raw/source.body"))
            self.assertEqual(storage.read_key(body_key), drifted)
            self.assertEqual(
                json.loads(storage.read_key(next(key for key in keys if key.endswith("/metadata/failure.json"))))["reason_code"],
                "official_tiger_archive_identity_or_release_drift",
            )

    def test_symlinked_capture_roster_member_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            cache = Path(tmp) / "cache"
            real = Path(tmp) / "real.body"
            real.write_bytes(self.BODY)
            _write_replay_cache(cache, self.BODY, self.WIRE, _receipt(self.BODY, self.WIRE, self.URL))
            (cache / "source.body").unlink()
            (cache / "source.body").symlink_to(real)
            with self.assertRaisesRegex(ValueError, "source_capture_nonregular_file"):
                self._replay(storage, cache)

    def test_capture_roster_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            cache = Path(tmp) / "cache"
            _write_replay_cache(cache, self.BODY, self.WIRE, _receipt(self.BODY, self.WIRE, self.URL))
            (cache / "source.extra").write_bytes(b"unexpected")
            with self.assertRaisesRegex(ValueError, "source_capture_roster_mismatch"):
                self._replay(storage, cache)
            (cache / "source.extra").unlink()
            (cache / "source.receipt.json").unlink()
            with self.assertRaisesRegex(ValueError, "source_capture_roster_mismatch"):
                self._replay(storage, cache)

    def test_symlinked_capture_directory_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            real = Path(tmp) / "real-cache"
            _write_replay_cache(real, self.BODY, self.WIRE, _receipt(self.BODY, self.WIRE, self.URL))
            link = Path(tmp) / "link-cache"
            link.symlink_to(real)
            with self.assertRaisesRegex(ValueError, "source_capture_directory_invalid"):
                self._replay(storage, link)

    def test_partial_download_failure_quarantines_retained_evidence(self):
        partial_body = b"partial downloaded bytes"
        observed = {}

        def failing_capture_get(_client, url, root, name, **kwargs):
            observed.update({"url": url, "name": name, "max_bytes": kwargs.get("max_bytes")})
            root.mkdir(parents=True, exist_ok=True)
            (root / "source.body").write_bytes(partial_body)
            (root / "source.wire").write_bytes(b"partial wire")
            raise OSError("mid_stream_failure")

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            with self.assertRaisesRegex(ValueError, "official_tiger_source_capture_failed"):
                capture_tiger_zip(
                    None, storage, "vpartial",
                    dataset_slug="test-tiger-zip", source_url=self.URL,
                    max_source_bytes=1_000_000,
                    expected_bytes=len(self.BODY), expected_sha256=hashlib.sha256(self.BODY).hexdigest(),
                    client_timeout=httpx.Timeout(180.0, connect=20.0),
                    capture_tmp_prefix="test-tiger-capture-",
                    capture_get=failing_capture_get,
                )
            self.assertEqual(observed, {"url": self.URL, "name": "source", "max_bytes": 1_000_000})
            keys = storage.list_prefix("_quarantine/test-tiger-zip")
            self.assertEqual(len(keys), 4)
            self.assertEqual(storage.read_key(next(key for key in keys if key.endswith("/raw/source.body"))), partial_body)
            self.assertEqual(
                json.loads(storage.read_key(next(key for key in keys if key.endswith("/metadata/failure.json"))))["reason_code"],
                "OSError",
            )


class ReadTigerShapefileTests(unittest.TestCase):
    def test_reads_shapefile_member_after_safety_checks(self):
        seen = {}

        def read_file(path):
            seen["path"] = path
            return "frame"

        frame = read_tiger_shapefile(
            _archive(sorted(MEMBERS)),
            expected_members=MEMBERS, shapefile_name="m.shp",
            extract_tmp_prefix="test-tiger-read-", read_file=read_file,
        )
        self.assertEqual(frame, "frame")
        self.assertEqual(seen["path"].name, "m.shp")

    def test_rejects_member_roster_drift(self):
        with self.assertRaisesRegex(ValueError, "tiger_archive_member_roster_drift"):
            read_tiger_shapefile(
                _archive(sorted(MEMBERS - {"m.dbf"})),
                expected_members=MEMBERS, shapefile_name="m.shp",
                extract_tmp_prefix="test-tiger-read-", read_file=lambda path: "frame",
            )
        extra = _archive(sorted(MEMBERS) + ["unexpected.txt"])
        with self.assertRaisesRegex(ValueError, "tiger_archive_member_roster_drift"):
            read_tiger_shapefile(
                extra,
                expected_members=MEMBERS, shapefile_name="m.shp",
                extract_tmp_prefix="test-tiger-read-", read_file=lambda path: "frame",
            )

    def test_rejects_path_traversal_before_extraction(self):
        names = ["../m.cpg", "m.dbf", "m.prj", "m.shp", "m.shp.ea.iso.xml", "m.shp.iso.xml", "m.shx"]
        with self.assertRaisesRegex(ValueError, "tiger_archive_unsafe_member"):
            read_tiger_shapefile(
                _archive(names),
                expected_members=MEMBERS, shapefile_name="m.shp",
                extract_tmp_prefix="test-tiger-read-", read_file=lambda path: "frame",
            )

    def test_rejects_subdirectory_member(self):
        names = ["sub/m.cpg", "m.dbf", "m.prj", "m.shp", "m.shp.ea.iso.xml", "m.shp.iso.xml", "m.shx"]
        with self.assertRaisesRegex(ValueError, "tiger_archive_unsafe_member"):
            read_tiger_shapefile(
                _archive(names),
                expected_members=MEMBERS, shapefile_name="m.shp",
                extract_tmp_prefix="test-tiger-read-", read_file=lambda path: "frame",
            )

    def test_rejects_crc_failure(self):
        body = bytearray(_archive(["m.shp"], payload=b"A" * 64))
        with zipfile.ZipFile(io.BytesIO(bytes(body))) as archive:
            info = archive.infolist()[0]
            payload_start = info.header_offset + 30 + len(info.filename) + len(info.extra)
        body[payload_start + 3] ^= 0xFF
        with self.assertRaisesRegex(ValueError, "tiger_archive_crc_failure:m.shp"):
            read_tiger_shapefile(
                bytes(body),
                expected_members={"m.shp"}, shapefile_name="m.shp",
                extract_tmp_prefix="test-tiger-read-", read_file=lambda path: "frame",
            )


class AdapterIsolationTests(unittest.TestCase):
    """Prove each adapter wires its own URL, member pins and byte budget into the shared capture."""

    def test_primary_roads_pins_differ_from_railroads(self):
        self.assertNotEqual(primary_roads.SOURCE_URL, railroads.SOURCE_URL)
        self.assertNotEqual(primary_roads.SOURCE_FILE, railroads.SOURCE_FILE)
        self.assertNotEqual(primary_roads.EXPECTED_SHA256, railroads.EXPECTED_SHA256)
        self.assertNotEqual(primary_roads.EXPECTED_MEMBERS, railroads.EXPECTED_MEMBERS)
        self.assertNotEqual(primary_roads.MAX_SOURCE_BYTES, railroads.MAX_SOURCE_BYTES)

    def _capture_wiring_drift(self, module):
        """Run a module capture whose stub writes an unpinned self-consistent archive."""
        body = b"unpinned capture body"
        receipt = _receipt(body, body, module.SOURCE_URL)
        observed = {}

        def stub_capture_get(_client, url, root, name, **kwargs):
            observed.update({"url": url, "name": name, "max_bytes": kwargs.get("max_bytes")})
            root.mkdir(parents=True, exist_ok=True)
            (root / "source.body").write_bytes(body)
            (root / "source.wire").write_bytes(body)
            (root / "source.receipt.json").write_text(json.dumps(receipt))
            return body, receipt

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            with patch.object(module, "capture_get", side_effect=stub_capture_get):
                with self.assertRaisesRegex(ValueError, "official_tiger_archive_identity_or_release_drift"):
                    module._capture(None, storage, "vwiring")
            self.assertEqual(observed, {"url": module.SOURCE_URL, "name": "source",
                                        "max_bytes": module.MAX_SOURCE_BYTES})
            return len(storage.list_prefix(f"_quarantine/{module.DATASET_SLUG}"))

    def test_primary_roads_capture_wires_its_own_pins(self):
        self.assertEqual(self._capture_wiring_drift(primary_roads), 4)

    def test_railroads_capture_wires_its_own_pins(self):
        self.assertEqual(self._capture_wiring_drift(railroads), 4)


if __name__ == "__main__":
    unittest.main()
