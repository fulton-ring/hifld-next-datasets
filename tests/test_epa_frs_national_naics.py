from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
from dagster import DagsterInstance, DefaultScheduleStatus, build_schedule_context, materialize

from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets.epa_frs_naics_national import (
    epa_frs_national_naics,
    epa_frs_national_naics_capture_complete,
    epa_frs_national_naics_draft_schedule,
    epa_frs_national_naics_membership_compatible,
)
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_naics_national as source
from dagster_hifld.assets import epa_frs_naics_national as assets_module

CSV_HEADER = '"REGISTRY_ID","PGM_SYS_ACRNM","PGM_SYS_ID","INTEREST_TYPE","NAICS_CODE","PRIMARY_INDICATOR","CODE_DESCRIPTION"'
CSV_ROWS = [
    '"110000491735","EIS","12663611","AIR EMISSIONS CLASSIFICATION UNKNOWN","002211","PRIMARY","PETROLEUM BULK STATIONS AND TERMINALS."',
    '"110000491735","NPDES","AKR06AE60","STORM WATER INDUSTRIAL","424710","SECONDARY","PETROLEUM BULK STATIONS AND TERMINALS."',
    '"110000491744","AIR","AK0000370","AIR MAJOR","424720","UNKNOWN","PETROLEUM AND PETROLEUM PRODUCTS MERCHANT WHOLESALERS."',
]
_REAL_HTTP_CLIENT = httpx.Client


class Fixture:
    """A miniature EPA combined archive with the NAICS member and authority page."""

    def __init__(self):
        csv_body = (CSV_HEADER + "\n" + "\n".join(CSV_ROWS) + "\n").encode("utf-8")
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("NATIONAL_NAICS_FILE.CSV", csv_body)
            archive.writestr("NATIONAL_FACILITY_FILE.CSV", '"REGISTRY_ID","PRIMARY_NAME"\n')
        self.zip_bytes = buffer.getvalue()
        info = zipfile.ZipFile(io.BytesIO(self.zip_bytes)).getinfo("NATIONAL_NAICS_FILE.CSV")
        self.member_compressed = info.compress_size
        self.member_uncompressed = info.file_size
        self.member_crc32 = f"{info.CRC & 0xffffffff:08x}"
        self.csv_sha256 = hashlib.sha256(csv_body).hexdigest()
        self.csv_rows = len(CSV_ROWS)
        self.page_bytes = (
            "<html><body><p>EPA State Combined CSV Download Files</p>"
            "<p>A combined National Zip file of 732 MB is available "
            f'<a href="{source.NATIONAL_COMBINED_ZIP_URL}">here.</a></p></body></html>'
        ).encode("utf-8")
        self.member_names = frozenset({"NATIONAL_NAICS_FILE.CSV", "NATIONAL_FACILITY_FILE.CSV"})

    @staticmethod
    def _stream(body: bytes):
        def generate():
            for offset in range(0, len(body), 8192):
                yield body[offset:offset + 8192]
        return generate()

    def handler(self, request: httpx.Request) -> httpx.Response:
        if str(request.url) == source.AUTHORITY_PAGE_URL:
            return httpx.Response(200, content=self._stream(self.page_bytes), request=request,
                                 headers={"Accept-Ranges": "bytes",
                                          "Content-Length": str(len(self.page_bytes))})
        if str(request.url) == source.NATIONAL_COMBINED_ZIP_URL:
            start_end = request.headers.get("Range")
            if not start_end:
                return httpx.Response(200, content=self._stream(self.zip_bytes), request=request)
            start, end = (int(part) for part in start_end.removeprefix("bytes=").split("-"))
            body = self.zip_bytes[start:end + 1]
            return httpx.Response(
                206, content=self._stream(body), request=request,
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes {start}-{end}/{len(self.zip_bytes)}",
                    "Content-Length": str(len(body)),
                    "ETag": '"fixture-etag"',
                    "Last-Modified": "Mon, 09 Nov 2026 00:00:00 GMT",
                },
            )
        return httpx.Response(404, request=request)


class EpaFrsNationalNaicsTests(unittest.TestCase):
    def setUp(self):
        # Unit fixtures exercise capture policy with tiny responses; production
        # disk capacity behavior is checked by the live guard.
        self.disk_guard = patch.object(source, "_require_disk_space")
        self.disk_guard.start()
        self.fixture = Fixture()

    def tearDown(self):
        self.disk_guard.stop()

    def pins(self, **overrides):
        values = {
            "EXPECTED_ZIP_TOTAL_BYTES": len(self.fixture.zip_bytes),
            "EXPECTED_ARCHIVE_MEMBER_NAMES": self.fixture.member_names,
            "EXPECTED_MEMBER_COMPRESSED_BYTES": self.fixture.member_compressed,
            "EXPECTED_MEMBER_UNCOMPRESSED_BYTES": self.fixture.member_uncompressed,
            "EXPECTED_MEMBER_CRC32": self.fixture.member_crc32,
            "EXPECTED_SOURCE_ROWS": self.fixture.csv_rows,
            "EXPECTED_CSV_SHA256": self.fixture.csv_sha256,
            "PRIOR_CANDIDATE_ROWS": self.fixture.csv_rows,
        }
        values.update(overrides)
        patchers = [patch.object(source, name, value) for name, value in values.items()]
        # The Dagster asset/check bindings were imported separately.
        for module in (source, assets_module):
            patchers.append(patch.object(module, "EXPECTED_SOURCE_ROWS", values["EXPECTED_SOURCE_ROWS"]))
            patchers.append(patch.object(module, "EXPECTED_CSV_SHA256", values["EXPECTED_CSV_SHA256"]))
        return patchers

    def mock_client(self, fixture=None):
        transport = httpx.MockTransport((fixture or self.fixture).handler)
        return lambda **kwargs: _REAL_HTTP_CLIENT(transport=transport)

    def run_capture(self, storage, version, capture_directory, **pin_overrides):
        with ExitStack() as stack:
            for patcher in self.pins(**pin_overrides):
                stack.enter_context(patcher)
            stack.enter_context(patch.object(source.httpx, "Client", self.mock_client()))
            return source.capture_epa_frs_national_naics(
                storage, version=version, capture_directory=capture_directory)

    def test_bounded_capture_preserves_native_columns_values_and_unapproved_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            captures = Path(tmp) / "captures" / "vfixture"
            result = self.run_capture(storage, "vfixture", str(captures.parent))
            self.assertFalse(result["reused"])
            self.assertEqual(result["row_count"], self.fixture.csv_rows)
            self.assertEqual(result["source_csv_sha256"], self.fixture.csv_sha256)
            self.assertFalse(result["release_accepted"])
            self.assertFalse(result["drift_reasons"])
            ready = json.loads(storage.read_key(result["candidate_ready"]))
            self.assertFalse(ready["approved"])
            self.assertFalse(ready["release_accepted"])
            table = pq.read_table(Path(tmp) / result["parquet_key"])
            self.assertEqual(table.schema.names, list(source.EXPECTED_COLUMNS))
            self.assertEqual(table.num_rows, self.fixture.csv_rows)
            self.assertEqual(table["REGISTRY_ID"].to_pylist(),
                             ["110000491735", "110000491735", "110000491744"])
            self.assertEqual(table["NAICS_CODE"].to_pylist()[0], "002211")
            self.assertEqual(table["PRIMARY_INDICATOR"].to_pylist(),
                             ["PRIMARY", "SECONDARY", "UNKNOWN"])
            self.assertEqual(table.schema.metadata[b"hifld:geometry"], b"absent")
            prefix = result["candidate_ready"].removesuffix("metadata/candidate_ready.json")
            manifest = json.loads(storage.read_key(f"{prefix}metadata/source_manifest.json"))
            self.assertEqual([item["input_id"] for item in manifest["inputs"]],
                             ["authority_page", "member_chunk_000", "member_local_header",
                              "zip_central_directory", "zip_eocd", "zip_probe"])
            for item in manifest["inputs"]:
                self.assertEqual(item["role"], "producer_publication")
                self.assertEqual(item["release_date"], None)
            capture_manifest = json.loads((captures / "capture_manifest.json").read_bytes())
            self.assertTrue(capture_manifest["capture_complete"])
            self.assertEqual(capture_manifest["capture_budget_bytes"], source.MAX_CAPTURE_BYTES)

    def test_retained_replay_reuses_candidate_without_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            captures = Path(tmp) / "captures" / "vfixture"
            result = self.run_capture(storage, "vfixture", str(captures.parent))
            with ExitStack() as stack:
                for patcher in self.pins():
                    stack.enter_context(patcher)
                stack.enter_context(patch.object(
                    source.httpx, "Client", side_effect=AssertionError("replay attempted HTTP")))
                replayed = source.capture_epa_frs_national_naics(
                    storage, version="vrefresh",
                    retained_capture_directory=str(captures),
                    retained_capture_manifest_sha256=result["capture_manifest_sha256"])
            self.assertTrue(replayed["reused"])
            self.assertEqual(replayed["version"], result["version"])
            self.assertFalse(replayed["release_accepted"])

    def test_retained_capture_manifest_digest_mismatch_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            captures = Path(tmp) / "captures" / "vfixture"
            result = self.run_capture(storage, "vfixture", str(captures.parent))
            with ExitStack() as stack:
                for patcher in self.pins():
                    stack.enter_context(patcher)
                stack.enter_context(patch.object(
                    source.httpx, "Client", side_effect=AssertionError("replay attempted HTTP")))
                with self.assertRaisesRegex(ValueError,
                                            "retained_capture_manifest_digest_mismatch"):
                    source.capture_epa_frs_national_naics(
                        storage, version="vrefresh",
                        retained_capture_directory=str(captures),
                        retained_capture_manifest_sha256="0" * 64)

    def test_tampered_retained_artifact_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            captures = Path(tmp) / "captures" / "vfixture"
            result = self.run_capture(storage, "vfixture", str(captures.parent))
            member_chunk = captures / "member_chunk_000.body"
            original = member_chunk.read_bytes()
            member_chunk.write_bytes(original[:-1] + bytes([original[-1] ^ 0xFF]))
            with ExitStack() as stack:
                for patcher in self.pins():
                    stack.enter_context(patcher)
                stack.enter_context(patch.object(
                    source.httpx, "Client", side_effect=AssertionError("replay attempted HTTP")))
                with self.assertRaisesRegex(ValueError, "retained_capture"):
                    source.capture_epa_frs_national_naics(
                        storage, version="vrefresh",
                        retained_capture_directory=str(captures),
                        retained_capture_manifest_sha256=result["capture_manifest_sha256"])

    def test_csv_sha_drift_quarantines_and_refuses_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            captures = Path(tmp) / "captures" / "vfixture"
            with self.assertRaisesRegex(ValueError, "complete_source_snapshot_drift"):
                self.run_capture(storage, "vfixture", str(captures.parent),
                                 EXPECTED_CSV_SHA256="0" * 64)
            quarantined = [key for key in storage.list_prefix(f"_quarantine/{source.DATASET_SLUG}")
                           if storage.object_exists(key)]
            self.assertTrue(any(key.endswith("raw/authority_page.body") for key in quarantined))
            self.assertTrue(any(key.endswith("raw/member_chunk_000.body") for key in quarantined))
            self.assertTrue(any(key.endswith("metadata/failure.json") for key in quarantined))

    def test_row_count_drift_quarantines_and_refuses_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            captures = Path(tmp) / "captures" / "vfixture"
            with self.assertRaisesRegex(ValueError, "complete_source_row_count_drift"):
                self.run_capture(storage, "vfixture", str(captures.parent),
                                 EXPECTED_SOURCE_ROWS=self.fixture.csv_rows + 1)
            quarantined = [key for key in storage.list_prefix(f"_quarantine/{source.DATASET_SLUG}")
                           if storage.object_exists(key)]
            self.assertTrue(any(key.endswith("raw/member_chunk_000.body") for key in quarantined))

    def test_member_declaration_drift_quarantines_before_parse(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            captures = Path(tmp) / "captures" / "vfixture"
            with self.assertRaisesRegex(ValueError, "zip_naics_member_declaration_drift"):
                self.run_capture(storage, "vfixture", str(captures.parent),
                                 EXPECTED_MEMBER_CRC32="deadbeef")
            quarantined = [key for key in storage.list_prefix(f"_quarantine/{source.DATASET_SLUG}")
                           if storage.object_exists(key)]
            self.assertTrue(any(key.endswith("raw/zip_central_directory.body") for key in quarantined))

    def test_authority_page_drift_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            captures = Path(tmp) / "captures" / "vfixture"
            broken = Fixture()
            broken.page_bytes = broken.page_bytes.replace(b"national_combined.zip", b"other_file.zip")
            with ExitStack() as stack:
                for patcher in self.pins():
                    stack.enter_context(patcher)
                stack.enter_context(patch.object(source.httpx, "Client", self.mock_client(broken)))
                with self.assertRaisesRegex(ValueError, "authority_page_link_or_text_drift"):
                    source.capture_epa_frs_national_naics(
                        storage, version="vfixture", capture_directory=str(captures.parent))

    def test_dagster_ingest_and_catalog_materialize_with_cached_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            instance_dir = Path(tmp) / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            with ExitStack() as stack:
                for patcher in self.pins():
                    stack.enter_context(patcher)
                stack.enter_context(patch.object(source.httpx, "Client", self.mock_client()))
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    run = materialize([epa_frs_national_naics],
                                      resources={"staging_storage": storage}, instance=instance)
                    self.assertTrue(run.success)
                    version = None
                    for event in run.all_events:
                        if event.event_type_value == "ASSET_MATERIALIZATION" and event.asset_key:
                            version = event.materialization.metadata["version"].value
                    self.assertIsNotNone(version)
                    check = epa_frs_national_naics_capture_complete.op.compute_fn.decorated_fn(
                        {"row_count": self.fixture.csv_rows,
                         "source_csv_sha256": self.fixture.csv_sha256,
                         "native_columns": list(source.EXPECTED_COLUMNS)})
                    self.assertTrue(check.passed)
                    partition = build_publish_partition_key(
                        source.DATASET_SLUG, source.FILE_SLUG, version)
                    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
                    catalog = materialize([publish_catalog],
                                          resources={"staging_storage": storage},
                                          instance=instance, partition_key=partition)
                    self.assertTrue(catalog.success)
                    prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{version}"
                    quality = json.loads(
                        storage.read_key(f"{prefix}/metadata/quality_manifest.json"))
                    self.assertEqual(quality["feature_count"], self.fixture.csv_rows)
                    self.assertEqual(quality["spatial_status"], "non_spatial_source")
                    audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
                    self.assertFalse(audit["release_accepted"])
                    cached = source.capture_epa_frs_national_naics(
                        storage, version="vrefresh",
                        capture_directory=str(Path(tmp) / "captures"))
                    self.assertTrue(cached["reused"])
                    self.assertEqual(cached["version"], version)

    def test_schedule_stopped_and_membership_check_reports_prior_candidate(self):
        self.assertIs(epa_frs_national_naics_draft_schedule.default_status,
                      DefaultScheduleStatus.STOPPED)
        request = epa_frs_national_naics_draft_schedule.evaluate_tick(
            build_schedule_context()).run_requests[0]
        config = request.run_config["ops"][epa_frs_national_naics.op.name]["config"]
        self.assertEqual(config, {"capture_directory": "", "retained_capture_directory": "",
                                  "retained_capture_manifest_sha256": ""})
        check_fn = epa_frs_national_naics_membership_compatible.op.compute_fn.decorated_fn
        incompatible = check_fn({
            "row_count": self.fixture.csv_rows,
            "drift_reasons": ["source_rows_changed_from_prior_hifld_candidate"],
        })
        self.assertFalse(incompatible.passed)
        compatible = check_fn({"row_count": self.fixture.csv_rows, "drift_reasons": []})
        self.assertTrue(compatible.passed)


if __name__ == "__main__":
    unittest.main()
