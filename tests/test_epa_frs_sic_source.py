from __future__ import annotations

import csv
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

from dagster_hifld.assets import supported_assets as _supported_assets  # noqa: F401
from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets import epa_frs_sic_powerplants as assets_module
from dagster_hifld.assets.epa_frs_sic_powerplants import (
    epa_frs_sic_complete,
    epa_frs_sic_draft_schedule,
    epa_frs_sic_powerplants,
    epa_frs_sic_published_membership_compatible,
)
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_national_archive as archive
from dagster_hifld.sources import epa_frs_sic_national as source

_REAL_HTTP_CLIENT = httpx.Client


def _program_row(acronym: str, system_id: str, registry_id: str, name: str = "") -> dict[str, str]:
    row = {name: "" for name in source.PROGRAM_COLUMNS}
    row.update({"PGM_SYS_ACRNM": acronym, "PGM_SYS_ID": system_id,
                "REGISTRY_ID": registry_id, "PRIMARY_NAME": name})
    return row


class Fixture:
    """A miniature EPA combined archive with program and SIC members."""

    def __init__(self):
        program_rows = [
            _program_row("EIA-860", "EIA1", "110000000001", "PLANT\nONE"),
            _program_row("EIA-860", "EIA2", "110000000001", "PLANT ONE"),
            _program_row("EIA-860", "EIA3", "110000000002", "PLANT TWO"),
            _program_row("AIR", "AIR1", "110000000003", "OTHER PLANT"),
        ]
        sic_rows = [
            ("110000000001", "AIR", "A1", "AIR MAJOR", "4911", "PRIMARY",
             "ELECTRIC SERVICES"),
            ("110000000001", "NPDES", "N1", "NPDES", "4911", "PRIMARY",
             "ELECTRIC SERVICES"),
            ("110000000002", "TRIS", "T1", "TRI", "2869", "SECONDARY",
             "INDUSTRIAL ORGANIC CHEMICALS"),
            ("110000000003", "AIR", "A2", "AIR MAJOR", "9999", "PRIMARY",
             "OTHER"),
        ]
        self.program_rows = program_rows
        self.sic_rows = sic_rows
        self.output_rows = [
            ["110000000001", "4911", "PRIMARY"],
            ["110000000001", "4911", "PRIMARY"],
            ["110000000002", "2869", "SECONDARY"],
        ]
        program_body = self._csv(source.PROGRAM_COLUMNS, program_rows)
        sic_body = self._csv(source.SIC_COLUMNS, sic_rows)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zip_archive:
            zip_archive.writestr(source.PROGRAM_MEMBER_NAME, program_body)
            zip_archive.writestr(source.SIC_MEMBER_NAME, sic_body)
        self.zip_bytes = buffer.getvalue()
        zip_archive = zipfile.ZipFile(io.BytesIO(self.zip_bytes))
        self.program_info = zip_archive.getinfo(source.PROGRAM_MEMBER_NAME)
        self.sic_info = zip_archive.getinfo(source.SIC_MEMBER_NAME)
        self.member_names = frozenset({source.PROGRAM_MEMBER_NAME, source.SIC_MEMBER_NAME})
        self.page_bytes = (
            "<html><body><p>EPA State Combined CSV Download Files</p>"
            "<p>A combined National Zip file of 732 MB is available "
            f'<a href="{archive.NATIONAL_COMBINED_ZIP_URL}">here.</a></p></body></html>'
        ).encode("utf-8")
        self.output_sha = source._sha(source._canonical(sorted(self.output_rows)))
        self.schema_sha = source._sha(source._canonical(list(source.OUTPUT_COLUMNS)))

    @staticmethod
    def _csv(header: tuple[str, ...], rows) -> bytes:
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(header)
        for row in rows:
            if isinstance(row, dict):
                writer.writerow([row[name] for name in header])
            else:
                writer.writerow(row)
        return buffer.getvalue().encode("utf-8")

    @staticmethod
    def _stream(body: bytes):
        def generate():
            for offset in range(0, len(body), 4096):
                yield body[offset:offset + 4096]
        return generate()

    def handler(self, request: httpx.Request) -> httpx.Response:
        if str(request.url) == archive.AUTHORITY_PAGE_URL:
            return httpx.Response(200, content=self._stream(self.page_bytes), request=request,
                                  headers={"Accept-Ranges": "bytes",
                                           "Content-Length": str(len(self.page_bytes))})
        if str(request.url) == archive.NATIONAL_COMBINED_ZIP_URL:
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
                    "Last-Modified": "Wed, 04 Nov 2026 21:06:05 GMT",
                },
            )
        return httpx.Response(404, request=request)


class EPAFRSEIA860SICTests(unittest.TestCase):
    def setUp(self):
        self.disk_guard = patch.object(archive, "_require_disk_space")
        self.disk_guard.start()
        self.fixture = Fixture()

    def tearDown(self):
        self.disk_guard.stop()

    def pins(self, **overrides):
        values = {
            "EXPECTED_PROGRAM_ROWS": len(self.fixture.program_rows),
            "EXPECTED_EIA860_PROGRAM_ROWS": 3,
            "EXPECTED_EIA860_FACILITIES": 2,
            "EXPECTED_SIC_ROWS": len(self.fixture.sic_rows),
            "EXPECTED_MATCHED_SIC_ROWS": 3,
            "EXPECTED_MATCHED_DISTINCT_TRIPLETS": 2,
            "EXPECTED_MATCHED_DISTINCT_FACILITIES": 2,
            "EXPECTED_OUTPUT_ROWS": 3,
            "EXPECTED_OUTPUT_SHA256": self.fixture.output_sha,
            "EXPECTED_SCHEMA_SHA256": self.fixture.schema_sha,
            "ENVIROFACTS_EIA860_FACILITY_COUNT": 3,
            "PROGRAM_MEMBER_COMPRESSED_BYTES": self.fixture.program_info.compress_size,
            "PROGRAM_MEMBER_UNCOMPRESSED_BYTES": self.fixture.program_info.file_size,
            "PROGRAM_MEMBER_CRC32": f"{self.fixture.program_info.CRC & 0xffffffff:08x}",
            "SIC_MEMBER_COMPRESSED_BYTES": self.fixture.sic_info.compress_size,
            "SIC_MEMBER_UNCOMPRESSED_BYTES": self.fixture.sic_info.file_size,
            "SIC_MEMBER_CRC32": f"{self.fixture.sic_info.CRC & 0xffffffff:08x}",
            "MEMBER_CHUNK_BYTES": 64,
            "STREAM_WINDOW": 2,
        }
        values.update(overrides)
        patchers = [patch.object(source, name, value) for name, value in values.items()]
        patchers.extend([
            patch.object(archive, "EXPECTED_ZIP_TOTAL_BYTES", len(self.fixture.zip_bytes)),
            patch.object(archive, "EXPECTED_ARCHIVE_MEMBER_NAMES", self.fixture.member_names),
            patch.object(archive, "STREAM_ATTEMPTS", 1),
            patch.object(assets_module, "EXPECTED_OUTPUT_ROWS", values["EXPECTED_OUTPUT_ROWS"]),
            patch.object(assets_module, "EXPECTED_OUTPUT_SHA256", values["EXPECTED_OUTPUT_SHA256"]),
            patch.object(assets_module, "EXPECTED_SCHEMA_SHA256", values["EXPECTED_SCHEMA_SHA256"]),
            patch.object(assets_module, "EXPECTED_EIA860_PROGRAM_ROWS",
                         values["EXPECTED_EIA860_PROGRAM_ROWS"]),
            patch.object(assets_module, "ENVIROFACTS_EIA860_FACILITY_COUNT",
                         values["ENVIROFACTS_EIA860_FACILITY_COUNT"]),
        ])
        return patchers

    def mock_client(self, fixture=None):
        transport = httpx.MockTransport((fixture or self.fixture).handler)
        return lambda **kwargs: _REAL_HTTP_CLIENT(transport=transport)

    def run_capture(self, storage, version, capture_directory, **pin_overrides):
        with ExitStack() as stack:
            for patcher in self.pins(**pin_overrides):
                stack.enter_context(patcher)
            stack.enter_context(patch.object(source.httpx, "Client", self.mock_client()))
            return source.capture_epa_frs_eia860_sic(
                storage, version=version, capture_directory=capture_directory)

    def test_bounded_stream_join_preserves_native_values_and_unapproved_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            captures = Path(tmp) / "captures" / "vfixture"
            result = self.run_capture(storage, "vfixture", str(captures))
            self.assertFalse(result["reused"])
            self.assertEqual(result["row_count"], 3)
            self.assertEqual(result["eia860_program_rows"], 3)
            self.assertEqual(result["eia860_facilities"], 2)
            self.assertEqual(result["envirofacts_eia860_facility_count"], 3)
            self.assertFalse(result["release_accepted"])
            self.assertFalse(result["source_release_accepted"])
            self.assertFalse(result["rights_accepted"])
            self.assertEqual(result["drift_reasons"], [])

            table = pq.read_table(Path(tmp) / result["parquet_key"])
            self.assertEqual(table.schema.names, list(source.OUTPUT_COLUMNS))
            self.assertEqual(table.num_rows, 3)
            self.assertEqual(table["REGISTRY_ID"].to_pylist(),
                             ["110000000001", "110000000001", "110000000002"])
            self.assertEqual(table["SIC_CODE"].to_pylist(), ["4911", "4911", "2869"])
            self.assertEqual(table["PRIMARY_INDICATOR"].to_pylist(),
                             ["PRIMARY", "PRIMARY", "SECONDARY"])
            self.assertEqual(table.schema.metadata[b"hifld:geometry"], b"absent")

            prefix = result["candidate_ready"].removesuffix("metadata/candidate_ready.json")
            audit = json.loads(storage.read_key(f"{prefix}metadata/source_audit.json"))
            self.assertEqual(audit["program_member"]["rows"], 4)
            self.assertEqual(audit["program_member"]["eia860_rows"], 3)
            self.assertEqual(audit["program_member"]["eia860_distinct_registry_id"], 2)
            self.assertEqual(audit["sic_member"]["rows"], 4)
            self.assertEqual(audit["sic_member"]["matched_rows"], 3)
            self.assertEqual(audit["sic_member"]["matched_distinct_triplets"], 2)
            self.assertEqual(audit["sic_member"]["matched_distinct_registry_id"], 2)
            self.assertTrue(audit["envirofacts_count_matches_program_rows"])
            self.assertGreater(len(audit["program_member"]["chunk_receipts"]), 1)
            self.assertFalse(audit["release_accepted"])
            for receipt in audit["program_member"]["chunk_receipts"]:
                self.assertEqual(receipt["status"], 206)
                self.assertEqual(receipt["zip_total_bytes"], len(self.fixture.zip_bytes))

            manifest = json.loads(storage.read_key(f"{prefix}metadata/source_manifest.json"))
            input_ids = [item["input_id"] for item in manifest["inputs"]]
            self.assertIn("authority_page", input_ids)
            self.assertIn("national_program_file", input_ids)
            self.assertIn("national_sic_file", input_ids)
            for item in manifest["inputs"]:
                self.assertEqual(item["role"], "producer_publication")
                self.assertEqual(item["release_date"], None)
            hosts = {item["input_id"]: item["host"] for item in manifest["inputs"]}
            self.assertEqual(hosts["authority_page"], "www.epa.gov")
            self.assertEqual(hosts["national_program_file"], "ordsext.epa.gov")
            self.assertEqual(hosts["national_sic_file"], "ordsext.epa.gov")

            capture_manifest = json.loads((captures / "capture_manifest.json").read_bytes())
            self.assertTrue(capture_manifest["capture_complete"])
            self.assertIn("program_member_local_header", capture_manifest["captures"])
            self.assertIn("sic_member_local_header", capture_manifest["captures"])

    def test_cached_reuse_returns_the_same_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            first = self.run_capture(storage, "vfixture", str(Path(tmp) / "captures" / "first"))
            second = self.run_capture(storage, "vrefresh", str(Path(tmp) / "captures" / "second"))
            self.assertFalse(first["reused"])
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])
            self.assertEqual(second["source_record_sha256"], first["source_record_sha256"])

    def test_program_filter_count_drift_quarantines(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "eia860_program_row_count_drift"):
                self.run_capture(storage, "vfixture", str(Path(tmp) / "captures" / "vfixture"),
                                 EXPECTED_EIA860_PROGRAM_ROWS=99)
            quarantined = [key for key in storage.list_prefix(f"_quarantine/{source.DATASET_SLUG}")
                           if storage.object_exists(key)]
            self.assertTrue(any(key.endswith("raw/authority_page.body") for key in quarantined))
            self.assertTrue(any(key.endswith("metadata/failure.json") for key in quarantined))

    def test_output_sha_drift_quarantines(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "complete_source_snapshot_drift"):
                self.run_capture(storage, "vfixture", str(Path(tmp) / "captures" / "vfixture"),
                                 EXPECTED_OUTPUT_SHA256="0" * 64)
            quarantined = [key for key in storage.list_prefix(f"_quarantine/{source.DATASET_SLUG}")
                           if storage.object_exists(key)]
            self.assertTrue(any(key.endswith("metadata/failure.json") for key in quarantined))

    def test_schema_drift_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "sic_csv_schema_changed"):
                self.run_capture(storage, "vfixture", str(Path(tmp) / "captures" / "vfixture"),
                                 SIC_COLUMNS=source.SIC_COLUMNS + ("EXTRA",))

    def test_incomplete_stream_is_refused(self):
        class Broken(Fixture):
            def handler(self, request):
                range_header = request.headers.get("Range")
                if str(request.url) == archive.NATIONAL_COMBINED_ZIP_URL and range_header:
                    start, end = (int(part) for part in
                                  range_header.removeprefix("bytes=").split("-"))
                    if start > 0 and end - start + 1 <= 64:
                        return httpx.Response(503, content=b"unavailable", request=request)
                return super().handler(request)

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with ExitStack() as stack:
                for patcher in self.pins():
                    stack.enter_context(patcher)
                stack.enter_context(patch.object(source.httpx, "Client",
                                                 self.mock_client(Broken())))
                with self.assertRaisesRegex(ValueError, "stream_range_response_contract_failed"):
                    source.capture_epa_frs_eia860_sic(
                        storage, version="vfixture",
                        capture_directory=str(Path(tmp) / "captures" / "vfixture"))

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
                    run = materialize([epa_frs_sic_powerplants],
                                      resources={"staging_storage": storage}, instance=instance)
                    self.assertTrue(run.success)
                    version = None
                    for event in run.all_events:
                        if (event.event_type_value == "ASSET_MATERIALIZATION"
                                and event.asset_key):
                            version = event.materialization.metadata["version"].value
                    self.assertIsNotNone(version)
                    check_fn = epa_frs_sic_complete.op.compute_fn.decorated_fn
                    check = check_fn({
                        "row_count": 3,
                        "source_record_sha256": self.fixture.output_sha,
                        "source_schema_sha256": self.fixture.schema_sha,
                        "native_columns": list(source.OUTPUT_COLUMNS),
                        "eia860_program_rows": 3,
                    })
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
                    self.assertEqual(quality["feature_count"], 3)
                    self.assertEqual(quality["spatial_status"], "non_spatial_source")
                    audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
                    self.assertFalse(audit["release_accepted"])

    def test_schedule_stopped_and_checks_report_envirofacts_count(self):
        self.assertIs(epa_frs_sic_draft_schedule.default_status,
                      DefaultScheduleStatus.STOPPED)
        request = epa_frs_sic_draft_schedule.evaluate_tick(
            build_schedule_context()).run_requests[0]
        config = request.run_config["ops"][epa_frs_sic_powerplants.op.name]["config"]
        self.assertEqual(config, {"capture_directory": ""})
        with ExitStack() as stack:
            for patcher in self.pins():
                stack.enter_context(patcher)
            compatible = epa_frs_sic_published_membership_compatible.op.compute_fn.decorated_fn(
                {"eia860_program_rows": 3, "drift_reasons": []})
            self.assertTrue(compatible.passed)
            incompatible = epa_frs_sic_published_membership_compatible.op.compute_fn.decorated_fn(
                {"eia860_program_rows": 2, "drift_reasons": ["drift"]})
            self.assertFalse(incompatible.passed)


if __name__ == "__main__":
    unittest.main()
