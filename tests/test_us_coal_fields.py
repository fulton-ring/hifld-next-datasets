"""Synthetic-fixture and registration tests for the USGS OFR 2012-1205 coal-fields capture.

Real-capture evidence tests require HIFLD_US_COAL_FIELDS_TEST_CAPTURE_ARCHIVE;
the synthetic tests below exercise the full capture flow, every fail-closed
drift path, staging, retained replay and the catalog read without network
access or pinned production bytes.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import struct
import tarfile
import tempfile
from contextlib import ExitStack
import unittest
import zipfile
import zlib
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow
import pyarrow.parquet as pq

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.us_coal_fields import (
    us_coal_fields, us_coal_fields_complete, us_coal_fields_draft_refresh_schedule,
)
from dagster_hifld.catalog import load_staged_geodata, summarize_staged_catalog
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import us_coal_fields as source

_CAPTURE_ARCHIVE_ENV = "HIFLD_US_COAL_FIELDS_TEST_CAPTURE_ARCHIVE"


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _crc32(body: bytes) -> int:
    return zlib.crc32(body)


def _dbf_record(area: str, perimeter: str, province: str, name: str, age: str, rank: str) -> bytes:
    record = b" "
    for value, width in ((area, 12), (perimeter, 12), (province, 31), (name, 36), (age, 30), (rank, 50)):
        record += value.encode("ascii").ljust(width)[:width]
    assert len(record) == source.DBF_RECORD_WIDTH, len(record)
    return record


_PROVINCES = ["Eastern", "Gulf", "Interior", "Northern Great Plains", "Pacific Coast", "Rocky Mountain"]
_AGES = ["Cretaceous", "Cretaceous-Jurassic", "Mississippian", "Pennsylvanian-Permian", "Tertiary", "Tertiary-Cretaceous", "Triassic"]
_RANKS = ["Anthracite", "Lignite", "Low Volatile Bituminous", "Medium and High Volatile Bituminous", "Subbituminous"]
_DISTINCT_NAMES = 110


def _field_values(fid: int) -> tuple[str, str, str, str, str, str]:
    return (
        f"{Decimal('0.018') + fid * Decimal('0.001'):.3f}",
        f"{Decimal('0.718') + fid * Decimal('0.004'):.3f}",
        _PROVINCES[fid % len(_PROVINCES)],
        f"Field {fid % _DISTINCT_NAMES}",
        _AGES[fid % len(_AGES)],
        _RANKS[fid % len(_RANKS)],
    )


def _synthetic_ring(fid: int) -> list[tuple[float, float]]:
    # Clockwise (negative shoelace area) as ESRI shells.
    base = fid * 10.0
    return [(base, 0.0), (base, 1.0), (base + 1.0, 1.0), (base + 1.0, 0.0), (base, 0.0)]


def _polygon_record_body(record_number: int, rings: list[list[tuple[float, float]]]) -> bytes:
    points: list[tuple[float, float]] = []
    parts = []
    for ring in rings:
        parts.append(len(points))
        points.extend(ring)
    num_parts, num_points = len(parts), len(points)
    content = struct.pack("<i", 5)
    content += struct.pack("<4d", min(p[0] for p in points), min(p[1] for p in points),
                           max(p[0] for p in points), max(p[1] for p in points))
    content += struct.pack("<ii", num_parts, num_points)
    content += struct.pack(f"<{num_parts}i", *parts)
    content += struct.pack(f"<{2 * num_points}d", *[value for point in points for value in point])
    return struct.pack(">ii", record_number, len(content) // 2) + content


def _synthetic_shapefile(records: int, *, invalid_record: int | None = None, unclosed_record: int | None = None,
                         holed: int = 5, multi_shell: int = 7) -> tuple[bytes, bytes]:
    body = bytearray(100)
    body[0:4] = struct.pack(">i", 9994)
    body[4:24] = struct.pack(">iiiii", 0, 0, 0, 0, 0)
    body[28:36] = struct.pack("<ii", 1000, 5)
    entries = []
    bounds = [float("inf"), float("inf"), float("-inf"), float("-inf")]

    def track(ring):
        for x, y in ring:
            bounds[0] = min(bounds[0], x); bounds[1] = min(bounds[1], y)
            bounds[2] = max(bounds[2], x); bounds[3] = max(bounds[3], y)

    for record_number in range(1, records + 1):
        rings = [_synthetic_ring(record_number)]
        if invalid_record == record_number:
            # A shell plus a self-intersecting ("bowtie") hole inside it; published
            # as-is the feature is invalid, which the capture pins, never repairs.
            base = record_number * 10.0
            rings.append([(base + 0.2, 0.2), (base + 0.8, 0.8), (base + 0.8, 0.2),
                          (base + 0.2, 0.8), (base + 0.2, 0.2)])
        if record_number % holed == 0:
            base = record_number * 10.0
            # Counterclockwise (positive shoelace area) as ESRI holes.
            rings.append([(base + 0.25, 0.25), (base + 0.75, 0.25), (base + 0.75, 0.75),
                          (base + 0.25, 0.75), (base + 0.25, 0.25)])
        if record_number % multi_shell == 0:
            rings.append([(record_number * 10.0 + 4.0, 0.0), (record_number * 10.0 + 4.0, 1.0),
                          (record_number * 10.0 + 5.0, 1.0), (record_number * 10.0 + 5.0, 0.0),
                          (record_number * 10.0 + 4.0, 0.0)])
        if unclosed_record == record_number:
            rings = [rings[0][:-1]]
        for ring in rings:
            track(ring)
        record = _polygon_record_body(record_number, rings)
        # SHX entries carry the record start in 16-bit words and the CONTENT
        # length in words (the 8-byte record header is excluded).
        entries.append((len(body) // 2, struct.unpack(">i", record[4:8])[0]))
        body += record
    body[24:28] = struct.pack(">i", len(body) // 2)
    body[36:68] = struct.pack("<4d", *bounds)
    body[68:100] = struct.pack("<4d", 0.0, 0.0, 0.0, 0.0)
    index = bytearray(100)
    index[0:4] = struct.pack(">i", 9994)
    index[4:24] = struct.pack(">iiiii", 0, 0, 0, 0, 0)
    index[28:36] = struct.pack("<ii", 1000, 5)
    for offset, words in entries:
        index += struct.pack(">ii", offset, words)
    index[24:28] = struct.pack(">i", len(index) // 2)
    index[36:68] = struct.pack("<4d", *bounds)
    index[68:100] = struct.pack("<4d", 0.0, 0.0, 0.0, 0.0)
    return bytes(body), bytes(index)


def _synthetic_dbf(records: int) -> bytes:
    header = bytearray(32)
    header[0] = 3
    struct.pack_into("<IHH", header, 4, records, 32 + 6 * 32 + 1, source.DBF_RECORD_WIDTH)
    struct.pack_into("<H", header, 29, 87)
    for name, kind, width, scale in source.DBF_FIELDS:
        header += name.encode("ascii").ljust(11, b"\0") + kind.encode("ascii") + bytes(4)
        header += bytes([width, scale]) + bytes(14)
    header += b"\r"
    body = bytearray(header)
    for fid in range(records):
        body += _dbf_record(*_field_values(fid))
    body += b"\x1a"
    return bytes(body)


def _synthetic_package(records: int = 602, *, invalid_record: int | None = None,
                       unclosed_record: int | None = None,
                       member_overrides: dict[str, bytes] | None = None,
                       skip_member: str | None = None, family_rogue: bool = False,
                       include_rogue_in_roster: bool = False,
                       prj_expectation: str | None = None) -> dict:
    """Build a complete synthetic producer package with matching pins.

    ``skip_member`` removes the member from the package and roster pin but
    keeps its member pin, so the capture must refuse it as missing.
    ``family_rogue`` adds an unknown member inside the Updated Coal Fields
    family; it is pinned in the roster only when ``include_rogue_in_roster``.
    ``prj_expectation`` decouples the PRJ the module expects from the PRJ in
    the package, to exercise the PRJ equality check.
    """
    shp, shx = _synthetic_shapefile(records, invalid_record=invalid_record, unclosed_record=unclosed_record)
    members = {
        "Coal_Fields.shp": shp,
        "Coal_Fields.shx": shx,
        "Coal_Fields.dbf": _synthetic_dbf(records),
        "Coal_Fields.prj": (b'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",'
                            b'SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],'
                            b'UNIT["Degree",0.0174532925199433]]'),
        "Coal_Fields.sbn": b"synthetic-sbn" * 30,
        "Coal_Fields.sbx": b"synthetic-sbx" * 12,
        "Coal_Fields.shp.xml": (
            '<?xml version="1.0"?><metadata xml:lang="en"><Esri><CreaDate>20111130</CreaDate>'
            "<SyncDate>20111122</SyncDate></Esri><idinfo><descript><abstract>"
            "This data set is a polygon shapefile representing the coal field of the United States. "
            "Resolution of data is 1:5000000. OFR 96-92 was an update of Trumbull.</abstract></descript>"
            "</idinfo></metadata>"
        ).encode(),
    }
    members.update(member_overrides or {})
    roster: dict[str, tuple[int, int]] = {source.UPDATED_COAL_FIELDS_PREFIX: (0, 0)}
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("GIS/", b"")
        archive.writestr("GIS/File Reference Chart.xlsx", b"synthetic-xlsx")
        archive.writestr("GIS/Coal_Fields_Gis.mxd", b"synthetic-mxd")
        archive.writestr(source.UPDATED_COAL_FIELDS_PREFIX, b"")
        for member_name, body in members.items():
            if skip_member == member_name:
                continue
            roster[f"{source.UPDATED_COAL_FIELDS_PREFIX}{member_name}"] = (len(body), _crc32(body))
            archive.writestr(f"{source.UPDATED_COAL_FIELDS_PREFIX}{member_name}", body)
        if family_rogue:
            archive.writestr(f"{source.UPDATED_COAL_FIELDS_PREFIX}rogue.txt", b"rogue")
            if include_rogue_in_roster:
                roster[f"{source.UPDATED_COAL_FIELDS_PREFIX}rogue.txt"] = (len(b"rogue"), _crc32(b"rogue"))
    package = stream.getvalue()
    infos = {info.filename: info for info in zipfile.ZipFile(io.BytesIO(package)).infolist()}
    member_pins = {
        member_name: {"bytes": len(body), "sha256": _sha(body)}
        for member_name, body in members.items()
    }
    return {
        "package": package, "members": members, "roster": roster, "member_pins": member_pins,
        "member_count": len(infos), "records": records,
        "prj": prj_expectation if prj_expectation is not None else members["Coal_Fields.prj"].decode("ascii"),
        "bounds": _shapefile_bounds(members["Coal_Fields.shp"]),
    }


def _shapefile_bounds(shp: bytes) -> list[float]:
    return list(struct.unpack_from("<4d", shp, 36))


_LANDING_TEXT = "USGS report landing page with ofr-2012-1205-landing-anchor and first-posted-anchor."
_DOWNLOADS_TEXT = "Index of synthetic downloads with gis-zip-anchor readme-anchor metadata-anchor."
_README_TEXT = "synthetic readme with readme-title-anchor and readme-author-anchor and readme-mxd-anchor."
_METADATA_TEXT = ("<synthetic-metadata>metadata-title-anchor metadata-originator-anchor metadata-pubdate-anchor "
                  "metadata-publish-anchor metadata-place-anchor metadata-link-anchor "
                  "metadata-abstract-anchor</synthetic-metadata>")
_DOCUMENT_BODIES = {
    "landing.html": _LANDING_TEXT.encode(),
    "downloads.html": _DOWNLOADS_TEXT.encode(),
    "Readme.txt": _README_TEXT.encode(),
    "Coal_Fields.xml": _METADATA_TEXT.encode(),
}


def _patched_source(fixtures: dict, documents: dict[str, bytes] | None = None,
                    invalid_records: tuple[int, ...] = ()):
    """Patch byte pins and structural markers to the synthetic fixtures."""
    records = fixtures["records"]
    documents = dict(documents or _DOCUMENT_BODIES)
    return patch.multiple(
        source,
        GIS_ZIP_PIN={"bytes": len(fixtures["package"]), "sha256": _sha(fixtures["package"])},
        README_PIN={"bytes": len(documents["Readme.txt"]), "sha256": _sha(documents["Readme.txt"])},
        METADATA_XML_PIN={"bytes": len(documents["Coal_Fields.xml"]), "sha256": _sha(documents["Coal_Fields.xml"])},
        ZIP_MEMBER_COUNT=fixtures["member_count"],
        UPDATED_COAL_FIELDS_ROSTER=fixtures["roster"],
        COAL_FIELDS_MEMBER_PINS=fixtures["member_pins"],
        NATIVE_PRJ=fixtures["prj"],
        EXPECTED_BOUNDS=fixtures["bounds"],
        DBF_RECORDS=records,
        PROVINCE_COUNTS={province: sum(1 for fid in range(records) if _field_values(fid)[2] == province)
                         for province in _PROVINCES},
        AGE_COUNTS={age: sum(1 for fid in range(records) if _field_values(fid)[4] == age) for age in _AGES},
        RANK_COUNTS={rank: sum(1 for fid in range(records) if _field_values(fid)[5] == rank) for rank in _RANKS},
        DISTINCT_NAME_COUNT=min(records, _DISTINCT_NAMES),
        EXPECTED_INVALID_RECORD_NUMBERS=invalid_records,
        LANDING_MARKERS=("ofr-2012-1205-landing-anchor", "first-posted-anchor"),
        DOWNLOADS_INDEX_MARKERS=("gis-zip-anchor", "readme-anchor", "metadata-anchor"),
        README_MARKERS=("readme-title-anchor", "readme-author-anchor", "readme-mxd-anchor"),
        METADATA_XML_MARKERS=("metadata-title-anchor", "metadata-originator-anchor", "metadata-pubdate-anchor",
                              "metadata-publish-anchor", "metadata-place-anchor", "metadata-link-anchor",
                              "metadata-abstract-anchor"),
        IN_PACKAGE_SHAPEFILE_XML_MARKERS=("<CreaDate>20111130</CreaDate>", "1:5000000", "OFR 96-92"),
    )


class _FixtureTransport(httpx.BaseTransport):
    def __init__(self, fixtures: dict, *, documents: dict[str, bytes] | None = None,
                 fail_zip_once: bool = False, corrupt_zip_byte: bool = False,
                 landing_marker_drift: bool = False, readme_marker_drift: bool = False,
                 metadata_marker_drift: bool = False):
        self.fixtures = fixtures
        self.documents = dict(documents or _DOCUMENT_BODIES)
        if landing_marker_drift:
            self.documents["landing.html"] = self.documents["landing.html"].replace(b"first-posted-anchor", b"drifted-anchor")
        if readme_marker_drift:
            self.documents["Readme.txt"] = self.documents["Readme.txt"].replace(b"readme-title-anchor", b"drifted-anchor")
        if metadata_marker_drift:
            self.documents["Coal_Fields.xml"] = self.documents["Coal_Fields.xml"].replace(b"metadata-title-anchor", b"drifted-anchor")
        self.fail_zip_once = fail_zip_once
        self.corrupt_zip_byte = corrupt_zip_byte
        self.zip_calls = 0

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == source.PUBS_LANDING_URL:
            return self._ok(self.documents["landing.html"], "text/html", request)
        if url == source.DOWNLOADS_DIR_URL:
            return self._ok(self.documents["downloads.html"], "text/html", request)
        if url == source.README_URL:
            return self._ok(self.documents["Readme.txt"], "text/plain", request)
        if url == source.METADATA_XML_URL:
            return self._ok(self.documents["Coal_Fields.xml"], "text/xml", request)
        if url == source.GIS_ZIP_URL:
            self.zip_calls += 1
            if self.fail_zip_once and self.zip_calls == 1:
                return httpx.Response(503, stream=httpx.ByteStream(b"temporarily unavailable"), request=request,
                                      headers={"content-type": "text/plain"})
            body = bytearray(self.fixtures["package"])
            if self.corrupt_zip_byte:
                body[len(body) // 2] ^= 0xFF
            return self._ok(bytes(body), "application/zip", request)
        return httpx.Response(404, content=b"not found", request=request, headers={"content-type": "text/plain"})

    @staticmethod
    def _ok(body: bytes, content_type: str, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(body), request=request,
                              headers={"content-type": content_type})


class UsCoalFieldsRegistrationTests(unittest.TestCase):
    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(us_coal_fields, hifld_ingest_assets)
        self.assertEqual(us_coal_fields.key.to_user_string(), "ingest/us-coal-fields-1/us-coal-fields-1")
        self.assertEqual(us_coal_fields_draft_refresh_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = us_coal_fields_complete({"feature_count": source.DBF_RECORDS, "source_population_complete": True})
        self.assertTrue(check.passed)
        for flag in ("baseline_comparison_accepted", "source_release_accepted", "schema_contract_accepted",
                     "id_contract_accepted", "coverage_accepted", "rights_accepted", "release_accepted"):
            self.assertFalse(check.metadata[flag].value)
        self.assertFalse(us_coal_fields_complete({"feature_count": source.DBF_RECORDS - 1,
                                                  "source_population_complete": True}).passed)
        self.assertFalse(us_coal_fields_complete({"feature_count": source.DBF_RECORDS,
                                                  "source_population_complete": False}).passed)


class UsCoalFieldsCaptureTests(unittest.TestCase):
    def _new_storage(self) -> StagingStorageResource:
        temporary = tempfile.TemporaryDirectory(prefix="us-coal-fields-source-")
        self.addCleanup(temporary.cleanup)
        return StagingStorageResource(use_local=True, local_dir=temporary.name)

    def _capture(self, fixtures: dict, transport: _FixtureTransport, *, version: str = "v20261114T000000Z",
                 retained_source_version: str | None = None, invalid_records: tuple[int, ...] = (),
                 storage: StagingStorageResource | None = None):
        storage = storage or self._new_storage()
        with _patched_source(fixtures, transport.documents, invalid_records=invalid_records):
            if retained_source_version is not None:
                result = source.capture_validated(storage, version, retained_source_version=retained_source_version)
            else:
                with httpx.Client(transport=transport) as client:
                    result = source.capture_validated(storage, version, client=client)
        return storage, result

    def _prefix(self, storage, version="v20261114T000000Z"):
        return storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, version, "")

    def _local_path(self, storage, key):
        return Path(storage.local_dir) / key

    def _refuse(self, fixtures: dict, transport: _FixtureTransport, expected: str,
                extra_patches: dict | None = None):
        temporary = tempfile.TemporaryDirectory(prefix="us-coal-fields-refusal-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with ExitStack() as patches:
            patches.enter_context(_patched_source(fixtures, transport.documents))
            if extra_patches:
                patches.enter_context(patch.multiple(source, **extra_patches))
            with httpx.Client(transport=transport) as client:
                with self.assertRaisesRegex(ValueError, expected):
                    source.capture_validated(storage, "v20261114T000000Z", client=client)
        prefix = self._prefix(storage)
        self.assertFalse(storage.object_exists(prefix + "metadata/native_candidate/us-coal-fields-1.parquet"))
        quarantine = json.loads(storage.read_key(prefix + "metadata/quarantine.json"))
        self.assertEqual(quarantine["status"], "quarantined")
        self.assertIs(quarantine["release_accepted"], False)
        return storage

    def test_complete_capture_stages_exact_members_values_ids_and_geometry(self):
        fixtures = _synthetic_package()
        storage, result = self._capture(fixtures, _FixtureTransport(fixtures))
        self.assertEqual(result["feature_count"], fixtures["records"])
        self.assertTrue(result["source_population_complete"])
        prefix = self._prefix(storage)
        for member_name in source.COAL_FIELDS_MEMBER_PINS:
            staged = storage.read_key(prefix + f"shapefile/{source.staged_shapefile_name(member_name)}")
            self.assertEqual(staged, fixtures["members"][member_name], member_name)
        table = pq.read_table(self._local_path(storage, prefix + "metadata/native_candidate/us-coal-fields-1.parquet"))
        self.assertEqual(table.num_rows, fixtures["records"])
        self.assertEqual(table.column("AREA").type, pyarrow.decimal128(12, 3))
        self.assertEqual(table.column("PERIMETER").type, pyarrow.decimal128(12, 3))
        self.assertEqual(table.column("PROVINCE").type, pyarrow.string())
        self.assertEqual([int(value) for value in table.column("source_shp_record_number").to_pylist()],
                         list(range(1, fixtures["records"] + 1)))
        self.assertEqual(table.column("PROVINCE").to_pylist()[:6], _PROVINCES)
        first = table.slice(0, 1).to_pylist()[0]
        self.assertEqual(first["AREA"], Decimal("0.018"))
        self.assertEqual(first["NAME"], "Field 0")
        native_capture = json.loads(storage.read_key(prefix + "metadata/native_capture.json"))
        self.assertEqual(native_capture["record_count"], fixtures["records"])
        self.assertEqual(native_capture["capture_mode"], "live_http_capture")
        for flag in ("historical_hifld_continuity_accepted", "source_contract_accepted", "schema_contract_accepted",
                     "id_contract_accepted", "coverage_accepted", "rights_accepted", "release_accepted"):
            self.assertIs(native_capture[flag], False)
        receipts = [name for name in storage.list_keys(source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z")
                    if "/raw/" in name and name.endswith(".receipt.json") and ".attempt_" not in name]
        self.assertEqual(len(receipts), 5)
        zip_receipt = json.loads(storage.read_key(prefix + "raw/GIS.zip.receipt.json"))
        self.assertEqual(zip_receipt["url"], source.GIS_ZIP_URL)
        self.assertEqual(zip_receipt["status"], 200)
        self.assertEqual(zip_receipt["bytes"], len(fixtures["package"]))

    def test_staged_shapefile_reads_through_catalog_with_native_crs(self):
        fixtures = _synthetic_package(records=12)
        storage, _result = self._capture(fixtures, _FixtureTransport(fixtures))
        gdf = load_staged_geodata(storage, source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z")
        self.assertEqual(len(gdf), 12)
        self.assertEqual(gdf.crs.to_epsg(), 4326)  # the synthetic package's own PRJ
        self.assertEqual(list(gdf.columns), ["AREA", "PERIMETER", "PROVINCE", "NAME", "AGE", "RANK", "geometry"])
        manifest = json.loads(storage.read_key(self._prefix(storage) + "metadata/source_manifest.json"))
        summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z",
                                           "us-coal-fields-1", source_metadata=manifest)
        self.assertEqual(summary.quality_manifest["feature_count"], 12)
        self.assertEqual(summary.quality_manifest["invalid_geometry_count"], 0)

    def test_source_manifest_declares_pending_contracts_producer_inputs_and_banned_outputs(self):
        fixtures = _synthetic_package()
        storage, _result = self._capture(fixtures, _FixtureTransport(fixtures))
        manifest = json.loads(storage.read_key(self._prefix(storage) + "metadata/source_manifest.json"))
        self.assertIs(manifest["approved"], False)
        self.assertEqual(manifest["agency"], "U.S. Geological Survey (producer; authority acceptance pending)")
        self.assertEqual(manifest["date_issued"], source.REPORT_FIRST_POSTED_DATE)
        self.assertEqual({item["input_id"].split(":")[0] for item in manifest["inputs"]},
                         {"landing.html", "downloads.html", "Readme.txt", "Coal_Fields.xml", "GIS.zip"})
        for item in manifest["inputs"]:
            self.assertEqual(item["role"], "producer_publication")
            self.assertIn("pubs.usgs.gov", item["canonical_url"])
            self.assertIn("pubs.usgs.gov", item["host"])
            self.assertIn("No source, contract, baseline", item["license"])
            self.assertTrue(item["release_date_basis"])
        lineage = manifest["lineage"]
        self.assertIn("Open-File Report 2012-1205", lineage["producer_publication"])
        self.assertIn("Deprecate", lineage["hifld_continuity"]["basis"])
        self.assertIn("DataLumos archive project 240310", lineage["banned_copies_never_fetched"])
        for flag in ("source_contract_accepted", "schema_contract_accepted", "id_contract_accepted",
                     "coverage_accepted", "rights_accepted", "baseline_accepted", "release_accepted"):
            self.assertIs(lineage[flag], False)
        self.assertEqual(set(manifest["unsupported_output_formats"]),
                         {"shapefile_zip", "geopackage", "pmtiles"})

    def test_retained_capture_replay_reuses_original_bodies_and_emits_provenance(self):
        fixtures = _synthetic_package()
        storage, first = self._capture(fixtures, _FixtureTransport(fixtures))
        replay_transport = _FixtureTransport(fixtures)
        result = self._capture(fixtures, replay_transport, version="v20261114T000001Z",
                               retained_source_version="v20261114T000000Z", storage=storage)[1]
        self.assertEqual(result["feature_count"], first["feature_count"])
        self.assertEqual(result["records_sha256"], first["records_sha256"])
        self.assertEqual(result["parquet_sha256"], first["parquet_sha256"])
        self.assertEqual(replay_transport.zip_calls, 0)
        replay_prefix = self._prefix(storage, "v20261114T000001Z")
        self.assertEqual(storage.read_key(replay_prefix + "raw/GIS.zip"),
                         storage.read_key(self._prefix(storage) + "raw/GIS.zip"))
        native_capture = json.loads(storage.read_key(replay_prefix + "metadata/native_capture.json"))
        self.assertEqual(native_capture["capture_mode"], "retained_response_replay")
        self.assertEqual(native_capture["retained_source_capture_version"], "v20261114T000000Z")
        manifest = json.loads(storage.read_key(replay_prefix + "metadata/source_manifest.json"))
        self.assertEqual(manifest["lineage"]["raw_capture_mode"], "retained_response_replay")

    def test_retained_replay_of_absent_capture_is_refused_and_quarantined(self):
        fixtures = _synthetic_package()
        storage, _first = self._capture(fixtures, _FixtureTransport(fixtures))
        with _patched_source(fixtures):
            with self.assertRaisesRegex(ValueError, "retained_source_response_missing"):
                source.capture_validated(storage, "v20261114T000002Z", retained_source_version="v20261114T000009Z")
        prefix = self._prefix(storage, "v20261114T000002Z")
        quarantine = json.loads(storage.read_key(prefix + "metadata/quarantine.json"))
        self.assertEqual(quarantine["failure_code"], "retained_source_response_missing")

    def test_transient_package_failure_is_retained_before_bounded_retry(self):
        fixtures = _synthetic_package()
        storage, result = self._capture(fixtures, _FixtureTransport(fixtures, fail_zip_once=True))
        self.assertEqual(result["feature_count"], fixtures["records"])
        keys = storage.list_keys(source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z")
        self.assertTrue(any(key.endswith("raw/GIS.zip.attempt_1.receipt.json") for key in keys))
        failed_receipt = json.loads(storage.read_key(self._prefix(storage) + "raw/GIS.zip.attempt_1.receipt.json"))
        self.assertEqual(failed_receipt["status"], 503)

    def test_changed_package_byte_is_refused_and_quarantined(self):
        fixtures = _synthetic_package()
        self._refuse(fixtures, _FixtureTransport(fixtures, corrupt_zip_byte=True), "pinned_source_bytes_drift:GIS.zip")

    def test_unknown_family_member_is_refused_but_pinned_roster_accepts_declared_members(self):
        refusing = _synthetic_package(family_rogue=True)
        self._refuse(refusing, _FixtureTransport(refusing), "source_package_member_roster_drift:GIS/Updated Coal Fields/rogue.txt")
        accepting = _synthetic_package(family_rogue=True, include_rogue_in_roster=True)
        storage, result = self._capture(accepting, _FixtureTransport(accepting))
        self.assertEqual(result["feature_count"], accepting["records"])

    def test_missing_member_is_refused(self):
        fixtures = _synthetic_package(skip_member="Coal_Fields.sbx")
        self._refuse(fixtures, _FixtureTransport(fixtures), "source_package_member_missing:Coal_Fields.sbx")

    def test_landing_readme_and_metadata_marker_drift_is_refused(self):
        fixtures = _synthetic_package()
        self._refuse(fixtures, _FixtureTransport(fixtures, landing_marker_drift=True),
                     "source_publication_marker_drift:landing.html")
        self._refuse(fixtures, _FixtureTransport(fixtures, readme_marker_drift=True),
                     "source_publication_marker_drift:Readme.txt")
        self._refuse(fixtures, _FixtureTransport(fixtures, metadata_marker_drift=True),
                     "source_publication_marker_drift:Coal_Fields.xml")

    def test_prj_drift_is_refused(self):
        original = _synthetic_package()
        fixtures = _synthetic_package(
            member_overrides={"Coal_Fields.prj": b'GEOGCS["GCS_Drifted",DATUM["D_Drifted",SPHEROID["GRS_1980",6378137.0,298.257222101]]]'},
            prj_expectation=original["prj"])
        self._refuse(fixtures, _FixtureTransport(fixtures), "source_prj_drift")

    def test_in_package_metadata_marker_drift_is_refused(self):
        fixtures = _synthetic_package(member_overrides={
            "Coal_Fields.shp.xml": (
                '<?xml version="1.0"?><metadata xml:lang="en"><Esri><CreaDate>20111130</CreaDate>'
                "<SyncDate>20111122</SyncDate></Esri><idinfo><descript><abstract>"
                "This data set is a polygon shapefile representing the coal field of the United States. "
                "Resolution of data is 1:9000000. OFR 96-92 was an update of Trumbull.</abstract></descript>"
                "</idinfo></metadata>"
            ).encode()})
        self._refuse(fixtures, _FixtureTransport(fixtures),
                     "source_package_member_metadata_marker_drift:Coal_Fields.shp.xml")

    def test_dbf_schema_record_and_value_drift_is_refused(self):
        fixtures = _synthetic_package()
        dbf = fixtures["members"]["Coal_Fields.dbf"]
        renamed = dbf.replace(b"PROVINCE\x00\x00\x00", b"REGIONS\x00\x00\x00\x00", 1)
        self.assertEqual(len(renamed), len(dbf))
        self.assertNotEqual(renamed, dbf)
        renamed_fixtures = _synthetic_package(member_overrides={"Coal_Fields.dbf": renamed})
        self._refuse(renamed_fixtures, _FixtureTransport(renamed_fixtures),
                     "missing_renamed_extra_or_changed_source_dbf_schema")
        with self.assertRaisesRegex(ValueError, "incomplete_or_oversized_source_dbf"):
            source.decode_dbf(dbf + b"tail")
        with self.assertRaisesRegex(ValueError, "incomplete_or_oversized_source_dbf"):
            source.decode_dbf(dbf[:-4])
        deleted = bytearray(dbf)
        deleted[source.DBF_HEADER_BYTES + 5 * source.DBF_RECORD_WIDTH] = 0x2A
        with self.assertRaisesRegex(ValueError, "deleted_or_unknown_source_dbf_record"):
            source.decode_dbf(bytes(deleted))
        non_ascii = bytearray(dbf)
        non_ascii[source.DBF_HEADER_BYTES + 5 * source.DBF_RECORD_WIDTH + 20] = 0xC3
        with self.assertRaisesRegex(ValueError, "undeclared_nonASCII_source_dbf_encoding"):
            source.decode_dbf(bytes(non_ascii))
        scale_drift = bytearray(dbf)
        scale_drift[source.DBF_HEADER_BYTES + 5 * source.DBF_RECORD_WIDTH + 1:
                    source.DBF_HEADER_BYTES + 5 * source.DBF_RECORD_WIDTH + 13] = b"0.01805".ljust(12)
        with self.assertRaisesRegex(ValueError, "declared_source_dbf_scale_drift"):
            source.decode_dbf(bytes(scale_drift))
        null_number = bytearray(dbf)
        null_number[source.DBF_HEADER_BYTES + 5 * source.DBF_RECORD_WIDTH + 1:
                    source.DBF_HEADER_BYTES + 5 * source.DBF_RECORD_WIDTH + 13] = b" " * 12
        with self.assertRaisesRegex(ValueError, "null_nonfinite_or_unparseable_source_dbf_number"):
            source.decode_dbf(bytes(null_number))

    def test_geometry_drift_is_refused(self):
        fixtures = _synthetic_package()
        shp = fixtures["members"]["Coal_Fields.shp"]
        shx = fixtures["members"]["Coal_Fields.shx"]
        with self.assertRaisesRegex(ValueError, "source_shx_record_count_drift"):
            source.decode_shp(shp, shx[:-8])
        with self.assertRaisesRegex(ValueError, "source_shx_record_count_drift"):
            source.decode_shx(shx[:-8])
        with self.assertRaisesRegex(ValueError, "incomplete_source_shx"):
            source.decode_shx(b"")
        with self.assertRaisesRegex(ValueError, "source_shp_header_drift"):
            drifted = bytearray(shp)
            struct.pack_into("<i", drifted, 32, 15)
            source.decode_shp(bytes(drifted), shx)
        with self.assertRaisesRegex(ValueError, "source_shp_shape_type_drift"):
            drifted = bytearray(shp)
            struct.pack_into("<i", drifted, 108, 1)
            source.decode_shp(bytes(drifted), shx)
        with self.assertRaisesRegex(ValueError, "source_geometry_bounds_drift"):
            drifted = bytearray(shp)
            struct.pack_into("<d", drifted, 36, 12.5)
            source.decode_shp(bytes(drifted), shx)
        grown = bytearray(shp + shp[-40:])
        struct.pack_into(">i", grown, 24, len(grown) // 2)
        with self.assertRaisesRegex(ValueError, "extra_source_shp_records"):
            source.decode_shp(bytes(grown), shx)
        with self.assertRaisesRegex(ValueError, "source_shp_record_header_drift"):
            drifted = bytearray(shp)
            struct.pack_into(">i", drifted, 100, 99)
            source.decode_shp(bytes(drifted), shx)
        with self.assertRaisesRegex(ValueError, "source_shp_header_drift"):
            drifted = bytearray(shp)
            struct.pack_into(">i", drifted, 24, (len(shp) + 2) // 2)
            source.decode_shp(bytes(drifted), shx)

    def test_unclosed_ring_is_refused(self):
        fixtures = _synthetic_package(unclosed_record=2)
        self._refuse(fixtures, _FixtureTransport(fixtures), "source_polygon_ring_invalid")

    def test_invalid_source_geometry_is_pinned_not_repaired(self):
        fixtures = _synthetic_package(invalid_record=13)
        storage, result = self._capture(fixtures, _FixtureTransport(fixtures), invalid_records=(13,))
        self.assertEqual(result["feature_count"], fixtures["records"])
        self._refuse(fixtures, _FixtureTransport(fixtures), "source_geometry_validity_drift")

    def test_value_population_drift_is_refused(self):
        fixtures = _synthetic_package()
        self._refuse(fixtures, _FixtureTransport(fixtures), "source_attribute_value_population_drift",
                     extra_patches={"PROVINCE_COUNTS": {**source.PROVINCE_COUNTS, "Eastern": 246}})
        self._refuse(fixtures, _FixtureTransport(fixtures), "source_name_population_drift",
                     extra_patches={"DISTINCT_NAME_COUNT": _DISTINCT_NAMES + 1})

    def test_remote_or_published_staging_is_refused_before_any_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            for resource in (PublishedStorageResource(local_dir=tmp),
                             StagingStorageResource(use_local=False, bucket="forbidden")):
                with patch.object(source, "_request") as request:
                    with self.assertRaisesRegex(ValueError, "requires_isolated_local_staging"):
                        source.capture_validated(resource, "v20261114T000000Z")
                    request.assert_not_called()

    def test_total_capture_budget_is_enforced(self):
        fixtures = _synthetic_package()
        temporary = tempfile.TemporaryDirectory(prefix="us-coal-fields-budget-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with _patched_source(fixtures), patch.object(source, "TOTAL_CAPTURE_BUDGET", 1):
            with httpx.Client(transport=_FixtureTransport(fixtures)) as client:
                with self.assertRaisesRegex(ValueError, "source_total_capture_budget_exceeded|incomplete_source_response"):
                    source.capture_validated(storage, "v20261114T000000Z", client=client)


class _RealArchiveTransport(httpx.BaseTransport):
    """Serve the pinned real capture bodies from a local capture directory."""

    def __init__(self, package: Path):
        self.package = package

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        name = {
            source.PUBS_LANDING_URL: "landing.html",
            source.DOWNLOADS_DIR_URL: "downloads.html",
            source.README_URL: "Readme.txt",
            source.METADATA_XML_URL: "Coal_Fields.xml",
            source.GIS_ZIP_URL: "GIS.zip",
        }[str(request.url)]
        body = (self.package / f"{name}.body").read_bytes()
        return httpx.Response(200, stream=httpx.ByteStream(body), request=request,
                              headers={"content-type": "application/octet-stream"})


@unittest.skipUnless(os.environ.get(_CAPTURE_ARCHIVE_ENV),
                     "complete USGS OFR 2012-1205 capture opt-in required; see docs/dataset-sources/us-coal-fields/README.md")
class UsCoalFieldsRealCaptureTests(unittest.TestCase):
    """Replay the pinned real producer capture through the full flow."""

    @classmethod
    def setUpClass(cls):
        cls.package_tmp = tempfile.TemporaryDirectory()
        cls.package = Path(cls.package_tmp.name)
        with tarfile.open(os.environ[_CAPTURE_ARCHIVE_ENV], "r:gz") as archive:
            for member in archive:
                if not member.isfile() or Path(member.name).name != member.name:
                    raise ValueError("unsafe test archive")
                (cls.package / member.name).write_bytes(archive.extractfile(member).read())

    @classmethod
    def tearDownClass(cls):
        cls.package_tmp.cleanup()

    def _local_path(self, storage, key):
        return Path(storage.local_dir) / key

    def _real_capture(self, version: str = "v20261114T000000Z"):
        temporary = tempfile.TemporaryDirectory(prefix="us-coal-fields-real-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=_RealArchiveTransport(self.package)) as client:
            result = source.capture_validated(storage, version, client=client)
        return storage, result

    def test_real_capture_stages_real_members_values_and_crs(self):
        storage, result = self._real_capture()
        self.assertEqual(result["feature_count"], 602)
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z", "")
        for member_name, pin in source.COAL_FIELDS_MEMBER_PINS.items():
            staged = storage.read_key(prefix + f"shapefile/{source.staged_shapefile_name(member_name)}")
            self.assertEqual(len(staged), pin["bytes"])
            self.assertEqual(_sha(staged), pin["sha256"])
        table = pq.read_table(self._local_path(storage, prefix + "metadata/native_candidate/us-coal-fields-1.parquet"))
        self.assertEqual(table.num_rows, 602)
        self.assertEqual(table.column("AREA").type, pyarrow.decimal128(12, 3))
        self.assertIn("Great Falls Field", table.column("NAME").to_pylist())
        self.assertIn("Pennsylvanian-Permian", table.column("AGE").to_pylist())
        gdf = load_staged_geodata(storage, source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z")
        self.assertEqual(len(gdf), 602)
        self.assertEqual(gdf.crs.to_epsg(), 4269)
        self.assertEqual(list(gdf.columns), ["AREA", "PERIMETER", "PROVINCE", "NAME", "AGE", "RANK", "geometry"])
        manifest = json.loads(storage.read_key(prefix + "metadata/source_manifest.json"))
        self.assertIs(manifest["approved"], False)
        self.assertEqual(manifest["lineage"]["source_release_date"], source.REPORT_FIRST_POSTED_DATE)
        for item in manifest["inputs"]:
            self.assertEqual(item["role"], "producer_publication")
            self.assertTrue(item["host"].endswith("pubs.usgs.gov"))

    def test_real_capture_replay_reuses_bodies(self):
        storage, _first = self._real_capture()
        result = source.capture_validated(storage, "v20261114T000001Z", retained_source_version="v20261114T000000Z")
        self.assertEqual(result["feature_count"], 602)
        native_capture = json.loads(storage.read_key(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000001Z", "") + "metadata/native_capture.json"))
        self.assertEqual(native_capture["capture_mode"], "retained_response_replay")


if __name__ == "__main__":
    unittest.main()
