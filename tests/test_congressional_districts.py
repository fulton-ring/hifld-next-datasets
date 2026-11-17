import json
import os
import tempfile
import unittest
from pathlib import Path

import httpx

from dagster_hifld.assets.congressional_districts import congressional_districts
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILE_PAIRS
from dagster_hifld.resources import StagingStorageResource
import dagster_hifld.sources.congressional_districts as source

GEOIDS = (
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17",
    "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
    "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47",
    "48", "49", "50", "51", "53", "54", "55", "56", "60", "66", "69", "72", "78",
)
CAPTURE_RECEIPTS = Path("/tmp/hifld-native-reconstruction/387/capture/receipts.json")


def _listing(vintage, congress, geoids=GEOIDS):
    return "\n".join(
        f'<a href="tl_{vintage}_{geoid}_cd{congress:03d}.zip">'
        f'tl_{vintage}_{geoid}_cd{congress:03d}.zip</a>'
        for geoid in geoids
    )


def _receipt(vintage, congress, geoid, body=b"zip"):
    name = f"tl_{vintage}_{geoid}_cd{congress:03d}.zip"
    url = f"{source.TIGER_ROOT_URL}TIGER{vintage}/CD/{name}"
    import hashlib
    return {"url": url, "final_url": url, "status": 200, "complete_body": True,
            "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
            "content_encoding": "identity", "retrieved_at": "2026-11-17T00:00:00+00:00"}


def _write_capture(root, rows):
    (root / "receipts.json").write_bytes(source._canonical(rows))
    for row in rows:
        name = row["url"].rsplit("/", 1)[-1]
        (root / name).write_bytes(b"zip")


class _FakeResponse:
    def __init__(self, status_code, body=b"", headers=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "status", request=httpx.Request("GET", "https://example.invalid"),
                response=httpx.Response(self.status_code))

    def iter_bytes(self):
        yield self._body


class _FakeStream:
    def __init__(self, response):
        self.response = response

    def __enter__(self):
        return self.response

    def __exit__(self, *exc):
        return False


class _FakeClient:
    def __init__(self, pages):
        self.pages = pages

    def stream(self, method, url):
        if url not in self.pages:
            return _FakeStream(_FakeResponse(404))
        body, headers = self.pages[url]
        return _FakeStream(_FakeResponse(200, body, headers))


class CongressNumberTests(unittest.TestCase):
    def test_ordinal_matches_the_edition_title(self):
        self.assertEqual(source.congress_ordinal(101), "101st")
        self.assertEqual(source.congress_ordinal(111), "111th")
        self.assertEqual(source.congress_ordinal(112), "112th")
        self.assertEqual(source.congress_ordinal(119), "119th")
        self.assertEqual(source.congress_ordinal(120), "120th")
        self.assertEqual(source.congress_ordinal(121), "121st")
        self.assertEqual(source.congress_ordinal(122), "122nd")
        self.assertEqual(source.congress_ordinal(123), "123rd")
        with self.assertRaises(source.CongressionalDistrictsQuarantineError):
            source.congress_ordinal(0)

    def test_district_field_tracks_the_congress_number(self):
        self.assertEqual(source.source_fields(119)[1], "CD119FP")
        self.assertEqual(source.source_fields(120)[1], "CD120FP")
        self.assertEqual(source.source_fields(121)[1], "CD121FP")


class DirectoryDiscoveryTests(unittest.TestCase):
    def test_parse_derives_the_current_edition_and_complete_roster(self):
        edition = source.parse_cd_directory(
            _listing(2026, 120), vintage=2026,
            directory_url=f"{source.TIGER_ROOT_URL}TIGER2026/CD/")
        self.assertEqual(edition.vintage, 2026)
        self.assertEqual(edition.congress, 120)
        self.assertEqual(edition.ordinal, "120th")
        self.assertEqual(edition.layer, "CD120")
        self.assertEqual(len(edition.geoids), len(GEOIDS))
        self.assertIn("tl_2026_01_cd120.zip", edition.archives)
        self.assertEqual(edition.fields[1], "CD120FP")

    def test_parse_rejects_mixed_congress_archives(self):
        body = _listing(2026, 120) + "\n" + _listing(2026, 119, ("01",))
        with self.assertRaisesRegex(
                source.CongressionalDistrictsQuarantineError,
                "mixed_congress_editions_in_directory"):
            source.parse_cd_directory(
                body, vintage=2026, directory_url=f"{source.TIGER_ROOT_URL}TIGER2026/CD/")

    def test_parse_rejects_a_truncated_listing(self):
        with self.assertRaisesRegex(
                source.CongressionalDistrictsQuarantineError,
                "cd_directory_roster_incomplete"):
            source.parse_cd_directory(
                _listing(2026, 120, GEOIDS[:49]), vintage=2026,
                directory_url=f"{source.TIGER_ROOT_URL}TIGER2026/CD/")

    def test_candidate_vintages_are_recent_and_newest_first(self):
        index = "".join(f'<a href="TIGER{year}/">TIGER{year}/</a>'
                        for year in (2019, 2020, 2024, 2025, 2026))
        self.assertEqual(source._candidate_vintages(index), [2026, 2025])

    def test_discovery_skips_a_vintage_without_a_cd_directory(self):
        index = "".join(f'<a href="TIGER{year}/">TIGER{year}/</a>'
                        for year in (2025, 2026))
        client = _FakeClient({
            source.TIGER_ROOT_URL: (index.encode(), {}),
            f"{source.TIGER_ROOT_URL}TIGER2025/CD/": (_listing(2025, 119).encode(), {}),
        })
        edition = source.discover_edition(client)
        self.assertEqual((edition.vintage, edition.congress), (2025, 119))

    def test_discovery_prefers_the_newest_complete_edition(self):
        index = "".join(f'<a href="TIGER{year}/">TIGER{year}/</a>'
                        for year in (2025, 2026))
        client = _FakeClient({
            source.TIGER_ROOT_URL: (index.encode(), {}),
            f"{source.TIGER_ROOT_URL}TIGER2026/CD/": (
                _listing(2026, 120).encode(), {"last-modified": "Mon, 16 Nov 2026 15:11:12 GMT"}),
            f"{source.TIGER_ROOT_URL}TIGER2025/CD/": (_listing(2025, 119).encode(), {}),
        })
        edition = source.discover_edition(client)
        self.assertEqual((edition.vintage, edition.congress), (2026, 120))
        self.assertEqual(edition.directory_last_modified, "Mon, 16 Nov 2026 15:11:12 GMT")


class ReceiptTests(unittest.TestCase):
    def test_receipts_derive_the_edition_without_code_pins(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _write_capture(root, [_receipt(2026, 120, geoid) for geoid in GEOIDS])
            rows, by_name, edition = source._verify_receipts(root)
            self.assertEqual(len(rows), len(GEOIDS))
            self.assertEqual(len(by_name), len(GEOIDS))
            self.assertEqual((edition.vintage, edition.congress), (2026, 120))
            self.assertEqual(edition.geoids, GEOIDS)

    def test_receipts_reject_mixed_congress_editions(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _write_capture(root, [_receipt(2026, 120, geoid) for geoid in GEOIDS[:55]]
                           + [_receipt(2026, 119, GEOIDS[55])])
            with self.assertRaisesRegex(
                    source.CongressionalDistrictsQuarantineError,
                    "mixed_congress_editions_in_receipts"):
                source._verify_receipts(root)

    def test_receipts_reject_a_missing_archive(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = [_receipt(2026, 120, geoid) for geoid in GEOIDS]
            _write_capture(root, rows)
            (root / rows[0]["url"].rsplit("/", 1)[-1]).unlink()
            with self.assertRaisesRegex(
                    source.CongressionalDistrictsQuarantineError,
                    "capture_archive_roster_changed"):
                source._verify_receipts(root)

    def test_archive_bytes_must_match_the_capture_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = [_receipt(2026, 120, geoid) for geoid in GEOIDS]
            _write_capture(root, rows)
            _, by_name, edition = source._verify_receipts(root)
            name = edition.archives[0]
            with self.assertRaisesRegex(
                    source.CongressionalDistrictsQuarantineError,
                    "capture_archive_receipt_mismatch"):
                source._read_archive(root, name, {**by_name[name], "bytes": 1}, edition)


class RegistrationTests(unittest.TestCase):
    def test_dataset_is_a_single_explicit_registered_ingest_asset(self):
        matching = [
            asset_def
            for asset_def in hifld_ingest_assets
            if next(iter(asset_def.keys)).path[-2:]
            == [source.DATASET_SLUG, source.FILE_SLUG]
        ]
        self.assertEqual(matching, [congressional_districts])
        self.assertEqual(source.DATASET_SLUG, "congressional-districts")
        self.assertEqual(
            SUPPORTED_DATASET_FILE_PAIRS.count((source.DATASET_SLUG, source.FILE_SLUG)), 1)

    def test_source_module_keeps_no_per_file_digest_or_count_pins(self):
        for name in ("SOURCE_PINS", "COUNTS", "CAPTURE_RECEIPTS_SHA256",
                     "EXPECTED_FEATURE_COUNT", "EXPECTED_COUNT", "GEOIDS"):
            self.assertFalse(hasattr(source, name), name)


@unittest.skipUnless(CAPTURE_RECEIPTS.is_file(), "retained Census source capture is unavailable")
class RetainedCaptureTests(unittest.TestCase):
    def test_retained_capture_passes_every_archive_and_feature_gate(self):
        root = source._capture_root()
        _, by_name, edition = source._verify_receipts(root)
        seen = set()
        total = 0
        for geoid in edition.geoids:
            table = source._read_archive(root, edition.archive_name(geoid), by_name[edition.archive_name(geoid)], edition)
            total += table.num_rows
            for value in table["GEOID"].to_pylist():
                self.assertNotIn(value, seen)
                seen.add(value)
        self.assertEqual(total, len(seen))
        self.assertGreaterEqual(len(seen), source.MIN_SOURCE_GEOIDS)

    def test_replay_staging_is_stable(self):
        with tempfile.TemporaryDirectory(prefix="cd-stage-test-") as temp:
            storage = StagingStorageResource(local_dir=temp, prefix="pinned-prefix", use_local=True)
            old_mode = os.environ.get("HIFLD_CD_MODE")
            old_root = os.environ.get("HIFLD_CAPTURE_ROOT")
            os.environ["HIFLD_CD_MODE"] = "replay"
            os.environ["HIFLD_CAPTURE_ROOT"] = "/tmp"
            try:
                first = source.stage_congressional_districts_candidate(
                    storage, source.DATASET_SLUG, source.FILE_SLUG)
                second = source.stage_congressional_districts_candidate(
                    storage, source.DATASET_SLUG, source.FILE_SLUG)
            finally:
                if old_mode is None:
                    os.environ.pop("HIFLD_CD_MODE", None)
                else:
                    os.environ["HIFLD_CD_MODE"] = old_mode
                if old_root is None:
                    os.environ.pop("HIFLD_CAPTURE_ROOT", None)
                else:
                    os.environ["HIFLD_CAPTURE_ROOT"] = old_root
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(first["candidate_identity"], second["candidate_identity"])
            self.assertEqual(first["source_feature_count"], first["source_unique_geoid_count"])
            self.assertEqual(first["prebuilt_geoparquet_sha256"], second["prebuilt_geoparquet_sha256"])
            self.assertEqual(first["source_manifest_key"], second["source_manifest_key"])
            self.assertIn("pinned-prefix/", first["source_manifest_key"])
            manifest = json.loads(storage.read_key(first["source_manifest_key"]))
            self.assertEqual(manifest["edition"]["congress"], first["congress"])
            self.assertIn(first["congress_ordinal"], manifest["title"])


if __name__ == "__main__":
    unittest.main()
