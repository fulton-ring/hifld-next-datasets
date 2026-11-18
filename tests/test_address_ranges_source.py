"""Focused tests for the Census TIGER Address Ranges candidate capture."""
from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pandas as pd
import pyarrow as pa

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import census_latest
from dagster_hifld.sources import address_ranges as source


def _release(vintage=2026):
    source_file = f"tlgdb_{vintage}_us_addr.gdb.zip"
    directory = f"https://www2.census.gov/geo/tiger/TGRGDB{vintage % 100:02d}/"
    return census_latest.GeodatabaseRelease(
        vintage=vintage, product="addr", source_file=source_file,
        source_url=directory + source_file, directory_url=directory,
        listing_sha256="", listing_bytes=0,
        latest_checked_at="2026-11-15T00:00:00+00:00", annual_listing_sha256="",
    )


ARCHIVE_MEMBERS = [
    "tlgdb_2026_us_addr.gdb/a00000001.TablesByName.atx",
    "tlgdb_2026_us_addr.gdb/a00000001.gdbindexes",
    "tlgdb_2026_us_addr.gdb/a00000001.gdbtable",
    "tlgdb_2026_us_addr.gdb/a00000001.gdbtablx",
    "tlgdb_2026_us_addr.gdb/a00000002.gdbtable",
    "tlgdb_2026_us_addr.gdb/a00000002.gdbtablx",
    "tlgdb_2026_us_addr.gdb/a00000003.gdbindexes",
    "tlgdb_2026_us_addr.gdb/a00000003.gdbtable",
    "tlgdb_2026_us_addr.gdb/a00000003.gdbtablx",
    "tlgdb_2026_us_addr.gdb/a00000004.CatItemsByPhysicalName.atx",
    "tlgdb_2026_us_addr.gdb/a00000004.CatItemsByType.atx",
    "tlgdb_2026_us_addr.gdb/a00000004.FDO_UUID.atx",
    "tlgdb_2026_us_addr.gdb/a00000004.gdbindexes",
    "tlgdb_2026_us_addr.gdb/a00000004.gdbtable",
    "tlgdb_2026_us_addr.gdb/a00000004.gdbtablx",
    "tlgdb_2026_us_addr.gdb/a00000004.horizon",
    "tlgdb_2026_us_addr.gdb/a00000005.CatItemTypesByName.atx",
    "tlgdb_2026_us_addr.gdb/a00000005.CatItemTypesByParentTypeID.atx",
    "tlgdb_2026_us_addr.gdb/a00000005.CatItemTypesByUUID.atx",
    "tlgdb_2026_us_addr.gdb/a00000005.gdbindexes",
    "tlgdb_2026_us_addr.gdb/a00000005.gdbtable",
    "tlgdb_2026_us_addr.gdb/a00000005.gdbtablx",
    "tlgdb_2026_us_addr.gdb/a00000006.CatRelsByDestinationID.atx",
    "tlgdb_2026_us_addr.gdb/a00000006.CatRelsByOriginID.atx",
    "tlgdb_2026_us_addr.gdb/a00000006.CatRelsByType.atx",
    "tlgdb_2026_us_addr.gdb/a00000006.FDO_UUID.atx",
    "tlgdb_2026_us_addr.gdb/a00000006.gdbindexes",
    "tlgdb_2026_us_addr.gdb/a00000006.gdbtable",
    "tlgdb_2026_us_addr.gdb/a00000006.gdbtablx",
    "tlgdb_2026_us_addr.gdb/a00000007.CatRelTypesByBackwardLabel.atx",
    "tlgdb_2026_us_addr.gdb/a00000007.CatRelTypesByDestItemTypeID.atx",
    "tlgdb_2026_us_addr.gdb/a00000007.CatRelTypesByForwardLabel.atx",
    "tlgdb_2026_us_addr.gdb/a00000007.CatRelTypesByName.atx",
    "tlgdb_2026_us_addr.gdb/a00000007.CatRelTypesByOriginItemTypeID.atx",
    "tlgdb_2026_us_addr.gdb/a00000007.CatRelTypesByUUID.atx",
    "tlgdb_2026_us_addr.gdb/a00000007.freelist",
    "tlgdb_2026_us_addr.gdb/a00000007.gdbindexes",
    "tlgdb_2026_us_addr.gdb/a00000007.gdbtable",
    "tlgdb_2026_us_addr.gdb/a00000007.gdbtablx",
    "tlgdb_2026_us_addr.gdb/a00000009.gdbindexes",
    "tlgdb_2026_us_addr.gdb/a00000009.gdbtable",
    "tlgdb_2026_us_addr.gdb/a00000009.gdbtablx",
    "tlgdb_2026_us_addr.gdb/gdb",
    "tlgdb_2026_us_addr.gdb/timestamps",
]


def _members_for(release=None) -> list[str]:
    root = (release or _release()).source_file[:-4]
    return [name.replace("tlgdb_2026_us_addr.gdb", root) for name in ARCHIVE_MEMBERS]


def _canonical_archive(member_count: int | None = None, corrupt: bool = False,
                       release=None) -> bytes:
    members = _members_for(release)
    members = members if member_count is None else members[:member_count]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
        for name in members:
            archive.writestr(name, b"fixture-" + name.encode())
    body = buffer.getvalue()
    if corrupt:
        marker = body.index(b"fixture-" + members[-1].encode())
        mutated = bytearray(body)
        mutated[marker] ^= 0xFF
        body = bytes(mutated)
    return body


def _validate_archive_bytes(body: bytes, release=None) -> dict:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "archive.zip"
        path.write_bytes(body)
        return source._validate_archive(path, release or _release())


def _fixture_rows() -> pd.DataFrame:
    return pd.DataFrame({
        "TLID": [105318504.0, 636341628.0],
        "TFIDL": [271099478.0, None],
        "TFIDR": [212584882.0, 265946721.0],
        "LINEARID": ["1106092744639", "11094825728"],
        "FULLNAME": ["800 East Rd", None],
        "FROMHN": ["800", "N2600"],
        "TOHN": ["898", '"Ñ99"'],
        "EDGE_MTFCC": ["S1400", "S1400"],
        "ROAD_MTFCC": ["S1400", "S1200"],
        "PARITYL": [None, "E"],
        "PARITYR": ["E", None],
        "OFFSETL": ["N", "N"],
        "OFFSETR": ["N", "N"],
        "SIDE": ["R", "L"],
        "ZIP": ["83350", "32322"],
        "PLUS4": [None, None],
        "FROMTYP": [" ", " "],
        "TOTYP": [" ", "I"],
        "ARID": ["40015504468870", "4004712747861"],
        "MTFCC": ["D1000", "D1000"],
    })


def _build_fixture_gdb(directory: Path, rows: pd.DataFrame | None = None,
                       name: str = "fixture.gdb") -> Path:
    import pyogrio

    path = directory / name
    pyogrio.write_dataframe(rows if rows is not None else _fixture_rows(), path,
                            layer="Address_Ranges", driver="OpenFileGDB")
    return path


def _fixture_batch(**overrides) -> pa.RecordBatch:
    rows = _fixture_rows()
    for name, values in overrides.items():
        rows[name] = values
    columns, fields = [], []
    for name, kind, width in source.EXPECTED_FIELD_DECLARATIONS:
        series = rows[name].astype(object).where(rows[name].notna(), None)
        arrow_type = pa.float64() if kind == "float64" else pa.string()
        metadata = {b"GDAL:OGR:width": str(width).encode()} if width else None
        columns.append(pa.array(list(series), type=arrow_type))
        fields.append(pa.field(name, arrow_type, metadata=metadata))
    return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))


def _fresh_accumulators():
    nulls = {name: 0 for name in source.FIELD_NAMES}
    domains = {name: {} for name in source.DOMAIN_FIELDS}
    violations = {name: 0 for name in source.VALUE_PATTERNS}
    violations.update({"integral_doubles": 0, "fullname_length": 0, "fullname_max_length": 0})
    return nulls, domains, violations, hashlib.sha256()


def _batch_digest(batch: pa.RecordBatch, batch_index: int = 1) -> str:
    digest = hashlib.sha256()
    digest.update(f"batch:{source.SCAN_BATCH_ROWS}:{batch_index}\n".encode())
    for name in source.FIELD_NAMES:
        digest.update(json.dumps(batch.column(name).to_pylist(), separators=(",", ":"),
                                 ensure_ascii=False, allow_nan=False).encode())
    return digest.hexdigest()


class AddressRangesContractTests(unittest.TestCase):
    def test_declares_current_tgrgdb_discovery_contract(self):
        self.assertEqual(source.DATASET_SLUG, "address-ranges")
        self.assertEqual(source.FILE_SLUG, "address-ranges")
        self.assertEqual(source.GEODATABASE_PRODUCT, "addr")
        self.assertEqual(source.SOURCE_HOST, "www2.census.gov")
        self.assertEqual(source.SCAN_BATCH_ROWS, 200_000)
        self.assertEqual(source.FEATURE_COUNT_RANGE, (25_000_000, 50_000_000))
        self.assertLess(source.FEATURE_COUNT_RANGE[0], source.FEATURE_COUNT_RANGE[1])
        self.assertFalse(hasattr(source, "SOURCE_URL"))
        self.assertFalse(hasattr(source, "EXPECTED_FEATURE_COUNT"))

    def test_pins_the_attribute_only_layer_schema(self):
        self.assertEqual(source.LAYER_NAME, "Address_Ranges")
        self.assertEqual([name for name, _kind, _width in source.EXPECTED_FIELD_DECLARATIONS],
                         ["TLID", "TFIDL", "TFIDR", "LINEARID", "FULLNAME", "FROMHN", "TOHN",
                          "EDGE_MTFCC", "ROAD_MTFCC", "PARITYL", "PARITYR", "OFFSETL", "OFFSETR",
                          "SIDE", "ZIP", "PLUS4", "FROMTYP", "TOTYP", "ARID", "MTFCC"])
        self.assertEqual(source.EXPECTED_FIELD_KINDS,
                         [("TLID", "float64"), ("TFIDL", "float64"), ("TFIDR", "float64"),
                          ("LINEARID", "object"), ("FULLNAME", "object"), ("FROMHN", "object"),
                          ("TOHN", "object"), ("EDGE_MTFCC", "object"), ("ROAD_MTFCC", "object"),
                          ("PARITYL", "object"), ("PARITYR", "object"), ("OFFSETL", "object"),
                          ("OFFSETR", "object"), ("SIDE", "object"), ("ZIP", "object"),
                          ("PLUS4", "object"), ("FROMTYP", "object"), ("TOTYP", "object"),
                          ("ARID", "object"), ("MTFCC", "object")])
        self.assertEqual(source.EXPECTED_FIELD_WIDTHS["ZIP"], 5)
        self.assertEqual(source.REQUIRED_NON_NULL_FIELDS, ("LINEARID", "ARID", "MTFCC"))
        self.assertEqual(source.VALUE_PATTERNS["ZIP"].pattern, "^[0-9]{5}$")
        self.assertEqual(source.VALUE_PATTERNS["FROMHN"].pattern, "^[^\\x00-\\x1F\\x7F]{1,12}$")

    def test_lineage_names_producer_and_banned_context(self):
        self.assertIn("Address Range geodatabase", source.LINEAGE)
        self.assertNotIn("TGRGDB", source.LINEAGE)
        self.assertNotIn("tlgdb_", source.LINEAGE)
        self.assertIn("DOI 10.3886/E241367V1", source.LINEAGE)
        self.assertIn("DataLumos", source.LINEAGE)
        self.assertIn("never fetched", source.LINEAGE)


class ArchiveValidationTests(unittest.TestCase):
    def test_accepts_a_bounded_canonical_roster(self):
        release = _release()
        integrity = _validate_archive_bytes(_canonical_archive(release=release), release)
        self.assertEqual(integrity["member_count"], len(_members_for(release)))
        self.assertGreater(integrity["uncompressed_bytes"], 0)

    def test_rejects_member_roster_and_crc_and_unsafe_drift(self):
        with self.assertRaisesRegex(ValueError, "source_archive_member_roster_drift"):
            _validate_archive_bytes(_canonical_archive(member_count=0))
        with patch.object(source, "MAX_ARCHIVE_MEMBERS", len(ARCHIVE_MEMBERS) - 1):
            with self.assertRaisesRegex(ValueError, "source_archive_member_roster_drift"):
                _validate_archive_bytes(_canonical_archive())
        with self.assertRaisesRegex(ValueError, "source_archive_crc_failure"):
            _validate_archive_bytes(_canonical_archive(corrupt=True))
        extra_root = io.BytesIO()
        with zipfile.ZipFile(extra_root, "w") as archive:
            for name in _members_for():
                archive.writestr(name, b"fixture")
            archive.writestr("other.gdb/a00000001.gdbtable", b"fixture")
        with self.assertRaisesRegex(ValueError, "source_archive_geodatabase_roster_drift"):
            _validate_archive_bytes(extra_root.getvalue())
        unsafe = io.BytesIO()
        with zipfile.ZipFile(unsafe, "w") as archive:
            for name in _members_for():
                archive.writestr(name, b"fixture")
            archive.writestr("../escape.gdb/gdb", b"fixture")
        with self.assertRaisesRegex(ValueError, "source_archive_unsafe_member_path"):
            _validate_archive_bytes(unsafe.getvalue())
        with patch.object(source, "MAX_UNCOMPRESSED_BYTES", 16):
            with self.assertRaisesRegex(ValueError, "source_archive_expansion_limit_exceeded"):
                _validate_archive_bytes(_canonical_archive())

    def test_rejects_invalid_zip(self):
        with self.assertRaisesRegex(ValueError, "source_archive_invalid_zip"):
            _validate_archive_bytes(b"not a zip at all")


class LayoutAndScanTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory(prefix="addr408-tests-")
        directory = Path(cls._temporary.name)
        cls.gdb = _build_fixture_gdb(directory)
        cls.rows = 2
        cls.release = _release()
        for applied in (patch.object(source, "FEATURE_COUNT_RANGE", (1, 10)),
                        patch.object(source, "EXPECTED_FIELD_WIDTHS", {})):
            applied.start()
            cls.addClassCleanup(applied.stop)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def test_layout_pins_layer_schema_and_feature_count(self):
        layout = source._require_geodatabase_layout(self.gdb, self.release)
        self.assertEqual(layout["layers"], ["Address_Ranges"])
        self.assertEqual(layout["feature_count"], self.rows)
        self.assertIsNone(layout["geometry_type"])
        self.assertIsNone(layout["crs"])
        self.assertEqual(layout["field_kinds"], source.EXPECTED_FIELD_KINDS)

    def test_layout_rejects_count_geometry_schema_and_layer_drift(self):
        with patch.object(source, "FEATURE_COUNT_RANGE", (self.rows + 1, 10)):
            with self.assertRaisesRegex(ValueError, "source_feature_count_drift"):
                source._require_geodatabase_layout(self.gdb, self.release)
        with self.subTest(change="unexpected_geometry"):
            import pyogrio

            real_read_info = pyogrio.read_info

            def spatial_info(*args, **kwargs):
                info = dict(real_read_info(*args, **kwargs))
                info["geometry_type"] = "MultiPolygon"
                return info

            with patch.object(pyogrio, "read_info", spatial_info):
                with self.assertRaisesRegex(ValueError, "source_attribute_only_contract_drift"):
                    source._require_geodatabase_layout(self.gdb, self.release)
        with self.subTest(change="renamed_field"):
            import pyogrio

            renamed = _build_fixture_gdb(self.gdb.parent, _fixture_rows().rename(columns={"ZIP": "ZIP5"}),
                                         name="renamed.gdb")
            with self.assertRaisesRegex(ValueError, "source_schema_or_field_order_drift"):
                source._require_geodatabase_layout(renamed, self.release)
        with self.subTest(change="wrong_layer_roster"):
            import pyogrio

            extra_layer = self.gdb.parent / "extra.gdb"
            pyogrio.write_dataframe(_fixture_rows(), extra_layer, layer="Other_Layer",
                                    driver="OpenFileGDB")
            with self.assertRaisesRegex(ValueError, "source_geodatabase_layer_roster_drift"):
                source._require_geodatabase_layout(extra_layer, self.release)

    def test_native_id_roster_is_contiguous(self):
        roster = source._verify_native_id_roster(self.gdb, self.rows)
        self.assertEqual(roster, {"min": 1, "max": self.rows, "count": self.rows})

    def test_scan_records_domains_nulls_and_deterministic_digest(self):
        scan = source._scan_native_values(self.gdb, self.rows)
        self.assertEqual(scan["row_count"], self.rows)
        self.assertEqual(scan["scan_batches"], 1)
        self.assertEqual(scan["value_domains"]["MTFCC"], {"D1000": self.rows})
        self.assertEqual(scan["value_domains"]["SIDE"], {"R": 1, "L": 1})
        self.assertEqual(scan["null_counts"]["PARITYL"], 1)
        self.assertEqual(scan["null_counts"]["FULLNAME"], 1)
        self.assertEqual(scan["null_counts"]["LINEARID"], 0)
        self.assertEqual(scan["value_violations"]["ZIP"], 0)
        self.assertEqual(scan["value_violations"]["FROMHN"], 0)
        self.assertEqual(scan["value_violations"]["integral_doubles"], 0)
        self.assertEqual(scan["value_violations"]["fullname_length"], 0)
        again = source._scan_native_values(self.gdb, self.rows)
        self.assertEqual(again["record_digest"], scan["record_digest"])
        self.assertEqual(scan["record_digest_scheme"], source.SCAN_DIGEST_SCHEME)

    def test_scan_rejects_row_count_field_roster_and_width_drift(self):
        with self.assertRaisesRegex(ValueError, "source_scan_row_count_drift"):
            source._scan_native_values(self.gdb, self.rows + 1)
        import pyogrio

        renamed = _build_fixture_gdb(self.gdb.parent, _fixture_rows().rename(columns={"ZIP": "ZIP5"}),
                                     name="renamed-scan.gdb")
        with self.assertRaisesRegex(ValueError, "source_arrow_field_roster_drift"):
            source._scan_native_values(renamed, self.rows)
        with patch.object(source, "EXPECTED_FIELD_WIDTHS", {"ZIP": 9, "PLUS4": 4}):
            with self.assertRaisesRegex(ValueError, "source_arrow_field_width_drift"):
                source._scan_native_values(self.gdb, self.rows)


class BatchValidationTests(unittest.TestCase):
    def test_happy_batch_accumulates_domains_nulls_and_digest(self):
        nulls, domains, violations, digest = _fresh_accumulators()
        batch = _fixture_batch()
        source._validate_batch(batch, nulls, domains, violations, digest, 1)
        self.assertEqual(nulls["PARITYL"], 1)
        self.assertEqual(nulls["LINEARID"], 0)
        self.assertEqual(domains["SIDE"], {"R": 1, "L": 1})
        self.assertEqual(domains["MTFCC"], {"D1000": 2})
        self.assertEqual(violations["ZIP"], 0)
        self.assertEqual(violations["FROMHN"], 0)
        self.assertEqual(violations["fullname_max_length"], 11)
        self.assertEqual(digest.hexdigest(), _batch_digest(batch))

    def test_batch_rejects_value_required_null_double_and_length_drift(self):
        cases = [
            ("source_value_pattern_drift", {"ZIP": ["8335", "32322"]}),
            ("source_value_pattern_drift", {"ZIP": ["833501", "32322"]}),
            ("source_value_pattern_drift", {"FROMHN": ["800", "8000000000000"]}),
            ("source_value_pattern_drift", {"ARID": ["40015504468870", "40-015"]}),
            ("source_required_value_null:ARID", {"ARID": ["40015504468870", None]}),
            ("source_required_value_null:LINEARID", {"LINEARID": [None, "11094825728"]}),
            ("source_double_value_drift", {"TLID": [105318504.5, 636341628.0]}),
            ("source_double_value_drift", {"TLID": [-105318504.0, 636341628.0]}),
            ("source_fullname_length_drift", {"FULLNAME": ["x" * 101, None]}),
        ]
        for label, override in cases:
            with self.subTest(change=label):
                nulls, domains, violations, digest = _fresh_accumulators()
                with self.assertRaisesRegex(ValueError, label):
                    source._validate_batch(_fixture_batch(**override), nulls, domains,
                                           violations, digest, 1)

    def test_batch_rejects_field_roster_drift(self):
        nulls, domains, violations, digest = _fresh_accumulators()
        batch = _fixture_batch()
        names = ["ZIP5" if name == "ZIP" else name for name in source.FIELD_NAMES]
        renamed = pa.RecordBatch.from_arrays(list(batch.columns), names=names)
        with self.assertRaisesRegex(ValueError, "source_arrow_field_roster_drift"):
            source._validate_batch(renamed, nulls, domains, violations, digest, 1)


class CaptureTransferTests(unittest.TestCase):
    def _stream_client(self, response: httpx.Response):
        @contextmanager
        def stream(*args, **kwargs):
            yield response

        return SimpleNamespace(stream=stream)

    def _official_response(self, body: bytes, last_modified: str = "Wed, 29 Jul 2026 15:01:52 GMT",
                           url: str | None = None):
        return httpx.Response(
            200,
            headers={"content-type": "application/zip", "last-modified": last_modified},
            content=body,
            request=httpx.Request("GET", url or _release().source_url),
        )

    def _stub_transfer(self, body: bytes, wire_sha256: str | None = None):
        """Stand in for the streamed decoder; the decoder itself is covered by
        the repo's dedicated http_capture tests. This stub feeds the exact
        retained body so the staging, receipt and identity gates run for real."""
        digest = wire_sha256 or hashlib.sha256(body).hexdigest()
        def capture(response, *, reserve, retain_wire, retain_decoded, **kwargs):
            retain_wire(body)
            return SimpleNamespace(
                status_code=200, content_encoding="identity",
                decoded_bytes_produced=len(body), complete=True,
                wire_bytes_received=len(body), wire_bytes_retained=len(body),
                wire_sha256=digest, error_code=None,
                content_length_header=str(len(body)),
            )

        return capture

    def _patched_transfer(self, body: bytes, wire_sha256: str | None = None):
        return patch.object(source, "capture_http_response",
                            side_effect=self._stub_transfer(body, wire_sha256))

    def test_transfer_writes_staged_canonical_copy_and_receipt(self):
        release = _release()
        body = _canonical_archive(release=release)
        storage = _RecordingStorage()
        with self._patched_transfer(body):
            _archive, receipt, version = source._capture_archive(
                storage, "vtest", self._stream_client(self._official_response(body)), release)
        self.assertEqual(version, "vtest")
        self.assertEqual(receipt["raw_sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(receipt["raw_archive_key"], "file_geodatabase/tlgdb_2026_us_addr.gdb.zip")
        self.assertEqual(
            storage.objects["address-ranges/address-ranges/vtest/file_geodatabase/tlgdb_2026_us_addr.gdb.zip"],
            body)
        persisted = json.loads(
            storage.objects["address-ranges/address-ranges/vtest/raw/source_archive.receipt.json"])
        self.assertEqual(persisted["status"], 200)
        self.assertEqual(persisted["last_modified"], "Wed, 29 Jul 2026 15:01:52 GMT")
        self.assertTrue(persisted["transfer_complete"])

    def test_transfer_identity_drift_fails_closed_with_evidence(self):
        release = _release()
        body = _canonical_archive(release=release)
        storage = _RecordingStorage()
        with self._patched_transfer(body, wire_sha256="0" * 64):
            with self.assertRaisesRegex(ValueError, "source_archive_transfer_or_identity_drift"):
                source._capture_archive(storage, "vdrift",
                                        self._stream_client(self._official_response(body)), release)
        self.assertIn(
            "address-ranges/address-ranges/vdrift/file_geodatabase/tlgdb_2026_us_addr.gdb.zip",
            storage.objects)
        receipt = json.loads(
            storage.objects["address-ranges/address-ranges/vdrift/raw/source_archive.receipt.json"])
        self.assertEqual(receipt["raw_sha256"], "0" * 64)
        self.assertNotIn(
            "address-ranges/address-ranges/vdrift/raw/source_archive.failure_receipt.json",
            storage.objects)

    def test_transfer_final_host_drift_fails_closed(self):
        release = _release()
        body = _canonical_archive(release=release)
        storage = _RecordingStorage()
        response = self._official_response(body, url="https://mirror.example.net/addr.gdb.zip")
        with self._patched_transfer(body):
            with self.assertRaisesRegex(ValueError, "source_archive_transfer_or_identity_drift"):
                source._capture_archive(storage, "vheader", self._stream_client(response), release)

    def test_failed_transport_retains_partial_wire_and_receipt(self):
        storage = _RecordingStorage()

        class Client:
            def stream(self, *args, **kwargs):
                class Context:
                    def __enter__(self):
                        return SimpleNamespace(
                            request=SimpleNamespace(url=SimpleNamespace(host=source.SOURCE_HOST)),
                            headers={})

                    def __exit__(self, *args):
                        return False

                return Context()

        def interrupted(_response, **kwargs):
            kwargs["retain_wire"](b"partial-wire")
            raise OSError("connection interrupted")

        with patch.object(source, "capture_http_response", side_effect=interrupted):
            with self.assertRaisesRegex(ValueError, "source_archive_transport_error"):
                source._capture_archive(storage, "vpartial", Client(), _release())
        self.assertEqual(
            storage.objects["address-ranges/address-ranges/vpartial/raw/source_archive.partial.zip"],
            b"partial-wire")
        receipt = json.loads(storage.objects[
            "address-ranges/address-ranges/vpartial/raw/source_archive.failure_receipt.json"])
        self.assertEqual(receipt["wire_bytes_retained"], 12)
        self.assertEqual(receipt["wire_sha256"], hashlib.sha256(b"partial-wire").hexdigest())
        self.assertEqual(receipt["transfer_error_type"], "OSError")

    def test_capture_quarantines_validation_failures(self):
        release = _release()
        body = _canonical_archive(release=release)
        identity = {"code_identity_scheme": "git-commit-blobs-v1", "code_commit": "fixture",
                    "files": {}, "python": "3.12.0"}
        with tempfile.TemporaryDirectory() as directory:
            staging = StagingStorageResource(local_dir=directory)
            with self._patched_transfer(body), \
                    patch.object(census_latest, "discover_geodatabase", return_value=release), \
                    patch.object(source, "processing_identity", return_value=identity), \
                    patch.object(source, "validate_construction_code_identity"), \
                    patch.object(StagingStorageResource, "sha256_key", return_value="0" * 64):
                with self.assertRaisesRegex(ValueError, "staged_source_archive_integrity_failure"):
                    source.capture(staging, "vq", self._stream_client(self._official_response(body)))
            quarantine = json.loads(
                (Path(directory) / "address-ranges/address-ranges/vq/metadata/quarantine.json").read_bytes())
            self.assertEqual(quarantine["status"], "quarantined")
            self.assertEqual(quarantine["failure_code"], "staged_source_archive_integrity_failure")
            self.assertFalse(quarantine["release_accepted"])

    def test_capture_discovers_current_release_and_derives_version(self):
        release = _release(2027)
        body = _canonical_archive(release=release)
        identity = {"code_identity_scheme": "git-commit-blobs-v1", "code_commit": "fixture",
                    "files": {}, "python": "3.12.0"}
        fixture_result = {"feature_count": 2, "archive_sha256": hashlib.sha256(body).hexdigest(),
                          "archive_bytes": len(body), "record_digest": "f" * 64,
                          "native_objectid": {"min": 1, "max": 2, "count": 2},
                          "value_domains": {}, "null_counts": {}, "release_accepted": False}
        with tempfile.TemporaryDirectory() as directory:
            staging = StagingStorageResource(local_dir=directory)
            with self._patched_transfer(body), \
                    patch.object(census_latest, "discover_geodatabase", return_value=release) as discover, \
                    patch.object(source, "processing_identity", return_value=identity), \
                    patch.object(source, "validate_construction_code_identity"), \
                    patch.object(source, "_validate_staged_population", return_value=fixture_result) as population:
                result = source.capture(
                    staging, None,
                    self._stream_client(self._official_response(body, url=release.source_url)))
        self.assertEqual(discover.call_args.args[2], source.GEODATABASE_PRODUCT)
        self.assertEqual(population.call_args.kwargs["release"], release)
        self.assertRegex(result["version"], r"^v[0-9a-f]{32}$")
        self.assertEqual(result["version"], census_latest.release_version(
            "2027", hashlib.sha256(body).hexdigest(), "fixture"))


class ReplayTests(unittest.TestCase):
    def _receipt(self, archive: bytes, release=None) -> dict:
        release = release or _release()
        return {
            "method": "GET", "url": release.source_url,
            "requested_url": release.source_url,
            "final_url": release.source_url, "final_host": source.SOURCE_HOST,
            "status": 200, "observed_at": "2026-11-15T14:40:00+00:00",
            "content_type": "application/zip",
            "last_modified": "Wed, 29 Jul 2026 15:01:52 GMT",
            "content_encoding": "identity", "transfer_complete": True,
            "decoded_bytes_produced": len(archive), "wire_bytes_received": len(archive),
            "wire_bytes_retained": len(archive), "wire_sha256": hashlib.sha256(archive).hexdigest(),
            "raw_archive_key": source._canonical_source_key(release),
            "raw_sha256": hashlib.sha256(archive).hexdigest(),
        }

    def test_replay_requires_valid_and_matching_receipt(self):
        with self.assertRaisesRegex(ValueError, "retained_capture_receipt_invalid"):
            source.replay_retained_capture(StagingStorageResource(local_dir="/tmp/never"), "v1",
                                           b"archive", b"not-json")
        release = _release()
        archive = _canonical_archive(release=release)
        bad_sha = self._receipt(archive, release)
        bad_sha["raw_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "retained_capture_archive_receipt_mismatch"):
            source.replay_retained_capture(StagingStorageResource(local_dir="/tmp/never"), "v1",
                                           archive, json.dumps(bad_sha).encode())
        bad_key = self._receipt(archive, release)
        bad_key["raw_archive_key"] = "raw/other.zip"
        with self.assertRaisesRegex(ValueError, "retained_capture_archive_receipt_mismatch"):
            source.replay_retained_capture(StagingStorageResource(local_dir="/tmp/never"), "v1",
                                           archive, json.dumps(bad_key).encode())

    def test_replay_rejects_old_or_mismatched_receipt_url(self):
        release = _release()
        archive = _canonical_archive(release=release)
        for url in (
            "https://www2.census.gov/geo/tiger/TIGER2024/tl_2024_us_addr.zip",
            "https://www2.census.gov/geo/tiger/TGRGDB26/tlgdb_2026_us_nationgeo.gdb.zip",
        ):
            with self.subTest(url=url):
                receipt = self._receipt(archive, release)
                receipt["url"] = url
                with self.assertRaisesRegex(ValueError, "retained_capture_release_mismatch"):
                    source.replay_retained_capture(StagingStorageResource(local_dir="/tmp/never"),
                                                   "v1", archive, json.dumps(receipt).encode())

    def test_replay_restages_bytes_and_flags_replay(self):
        release = _release()
        archive = _canonical_archive(release=release)
        receipt = self._receipt(archive, release)
        identity = {"code_identity_scheme": "git-commit-blobs-v1", "code_commit": "fixture",
                    "files": {}, "python": "3.12.0"}
        fixture_result = {"feature_count": 2,
                          "archive_sha256": receipt["raw_sha256"], "archive_bytes": len(archive),
                          "record_digest": "f" * 64, "native_objectid": {"min": 1, "max": 2, "count": 2},
                          "value_domains": {}, "null_counts": {}, "release_accepted": False}
        with tempfile.TemporaryDirectory() as directory:
            staging = StagingStorageResource(local_dir=directory)
            with patch.object(source, "processing_identity", return_value=identity), \
                    patch.object(source, "validate_construction_code_identity"), \
                    patch.object(source, "_validate_staged_population", return_value=fixture_result):
                result = source.replay_retained_capture(staging, "vreplay", archive,
                                                        json.dumps(receipt).encode())
            self.assertTrue(result["replayed"])
            self.assertEqual(result["archive_sha256"], receipt["raw_sha256"])
            staged = (Path(directory) / "address-ranges/address-ranges/vreplay/file_geodatabase/tlgdb_2026_us_addr.gdb.zip")
            receipt_staged = (Path(directory) / "address-ranges/address-ranges/vreplay/raw/source_archive.receipt.json")
            self.assertEqual(staged.read_bytes(), archive)
            self.assertEqual(json.loads(receipt_staged.read_bytes())["observed_at"],
                             "2026-11-15T14:40:00+00:00")

    def test_replay_rejects_saved_receipts_that_capture_would_reject(self):
        """A retained receipt rejected by the fresh-transfer acquisition policy
        must fail replay closed with replay quarantine, never a normal manifest."""
        release = _release()
        archive = _canonical_archive(release=release)
        identity = {"code_identity_scheme": "git-commit-blobs-v1", "code_commit": "fixture",
                    "files": {}, "python": "3.12.0"}
        drifted = {
            "foreign_producer_host": {"final_host": "mirror.example.net"},
            "failed_incomplete_transfer": {"status": 503, "transfer_complete": False},
            "missing_transfer_completeness": {"transfer_complete": None},
            "inconsistent_wire_digest": {"wire_sha256": "1" * 64},
        }
        for label, drift in drifted.items():
            with self.subTest(drift=label):
                receipt = self._receipt(archive, release)
                receipt.update(drift)
                with tempfile.TemporaryDirectory() as directory:
                    staging = StagingStorageResource(local_dir=directory)
                    with patch.object(source, "processing_identity", return_value=identity), \
                            patch.object(source, "validate_construction_code_identity"), \
                            patch.object(source, "_validate_staged_population") as population:
                        with self.assertRaisesRegex(
                                ValueError, "source_archive_transfer_or_identity_drift"):
                            source.replay_retained_capture(staging, "vreject", archive,
                                                           json.dumps(receipt).encode())
                    self.assertFalse(population.called)
                    quarantine = json.loads(
                        (Path(directory)
                         / "address-ranges/address-ranges/vreject/metadata/quarantine.json").read_bytes())
                    self.assertEqual(quarantine["status"], "quarantined")
                    self.assertEqual(quarantine["failure_code"],
                                     "source_archive_transfer_or_identity_drift")
                    self.assertTrue(quarantine["replay"])
                    self.assertFalse(quarantine["release_accepted"])
                    self.assertFalse((Path(directory)
                                      / "address-ranges/address-ranges/vreject/metadata/native_capture.json").exists())
                    self.assertFalse((Path(directory)
                                      / "address-ranges/address-ranges/vreject/metadata/source_manifest.json").exists())


class _RecordingStorage:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def write(self, dataset, file, version, key, body):
        self.objects[f"{dataset}/{file}/{version}/{key}"] = body
        return f"{dataset}/{file}/{version}/{key}"


class AssetRegistrationTests(unittest.TestCase):
    def test_local_only_asset_and_registry_registration(self):
        from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
        from dagster_hifld.assets.address_ranges import (
            address_ranges_draft_refresh_schedule,
            census_tiger_address_ranges,
        )
        from dagster_hifld.definitions import defs

        self.assertEqual(sum(asset is census_tiger_address_ranges for asset in hifld_ingest_assets), 1)
        job = defs.resolve_job_def("address_ranges_draft_refresh")
        self.assertIn("ingest__address_ranges__address_ranges", job.graph.node_names())
        self.assertIn(address_ranges_draft_refresh_schedule, defs.schedules)
        self.assertEqual(
            address_ranges_draft_refresh_schedule.default_status.value, "STOPPED")

    def test_supported_pair_is_registered_once_from_the_live_ingest(self):
        from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILES

        pairs = [(spec.dataset_slug, spec.file_slug) for spec in SUPPORTED_DATASET_FILES]
        self.assertEqual(pairs.count(("address-ranges", "address-ranges")), 1)
        spec = next(item for item in SUPPORTED_DATASET_FILES
                    if (item.dataset_slug, item.file_slug) == ("address-ranges", "address-ranges"))
        self.assertEqual(spec.ingest_asset_key.path, ["ingest", "address-ranges", "address-ranges"])

    def test_asset_refuses_remote_or_published_staging(self):
        from dagster import build_asset_context
        from dagster_hifld.assets.address_ranges import census_tiger_address_ranges

        fn = census_tiger_address_ranges.op.compute_fn.decorated_fn
        with self.assertRaisesRegex(ValueError, "draft_requires_isolated_local_staging"):
            fn(build_asset_context(), PublishedStorageResource(use_local=True, local_dir="/tmp/x"))
        with self.assertRaisesRegex(ValueError, "draft_requires_isolated_local_staging"):
            fn(build_asset_context(),
               StagingStorageResource(bucket="remote", use_local=False, local_dir="/tmp/x"))


if __name__ == "__main__":
    unittest.main()
