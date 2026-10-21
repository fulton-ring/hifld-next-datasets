"""Native fidelity, drift, cache, and approval tests for the Ferrous Metal Processing Plants capture."""
import hashlib
import io
import json
import struct
import tarfile
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
import geopandas as gpd
import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.catalog import CatalogSummary, generate_data_dictionary, write_catalog_metadata
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import ferrous_metal_processing_plants as source
from dagster_hifld.sources.epa_frs_common import capture_get


def fixture_rows():
    return [
        {"MINOP4X020": Decimal("1"), "COMMODITY": "Iron", "SITE_NAME": "Alpha Works",
         "COMPANY_NA": "Test Mining Co", "STATE_LOCA": "Michigan", "COUNTY": "Marquette",
         "LATITUDE": Decimal("34.356"), "LONGITUDE": Decimal("-77.865"), "PLANT_MIN": "P"},
        {"MINOP4X020": Decimal("2"), "COMMODITY": "Silicon", "SITE_NAME": "Beta Plant",
         "COMPANY_NA": "Beta Alloys Inc", "STATE_LOCA": "Ohio", "COUNTY": "Washington",
         "LATITUDE": Decimal("39.584"), "LONGITUDE": Decimal("-81.678"), "PLANT_MIN": "M/P"},
    ]


def fixture_points():
    return [(-77.8651, 34.3562), (-81.6779, 39.5843)]


def build_dbf(rows, fields=None):
    fields = fields or source.DBF_FIELDS
    descriptor = bytearray()
    for name, kind, width, scale in fields:
        entry = bytearray(32)
        entry[:len(name.encode("ascii"))] = name.encode("ascii")
        entry[11] = ord(kind)
        entry[16] = width
        entry[17] = scale
        descriptor += entry
    header_size = 32 + len(descriptor) + 1
    record_width = 1 + sum(field[2] for field in fields)
    header = bytearray(32)
    header[0] = 3
    header[1] = 105
    header[2] = 11
    struct.pack_into("<I", header, 4, len(rows))
    struct.pack_into("<H", header, 8, header_size)
    struct.pack_into("<H", header, 10, record_width)
    body = bytes(header) + bytes(descriptor) + b"\r"
    for row in rows:
        record = bytearray(b" ")
        for name, kind, width, scale in fields:
            value = row[name]
            if kind == "C":
                text = value.encode("latin-1")
                if len(text) > width:
                    raise AssertionError("fixture text overflow")
                record += text.ljust(width, b" ")
            elif scale:
                text = value if isinstance(value, str) else f"{value:.{scale}f}"
                record += text.rjust(width).encode("ascii")
            else:
                record += str(value).rjust(width).encode("ascii")
        if len(record) != record_width:
            raise AssertionError("fixture record width drift")
        body += bytes(record)
    return body + b"\x1a"


def build_shp_shx(points):
    bounds = (min(x for x, _ in points), min(y for _, y in points),
              max(x for x, _ in points), max(y for _, y in points))
    records = []
    offset = 100
    for index, (x, y) in enumerate(points):
        content = struct.pack("<idd", 1, x, y)
        record = struct.pack(">ii", index + 1, len(content) // 2) + content
        records.append((record, offset))
        offset += len(record)
    shp = bytearray(struct.pack(">iiiiiii", 9994, 0, 0, 0, 0, 0, offset // 2)
                    + struct.pack("<ii", 1000, 1) + struct.pack("<4d", *bounds)
                    + struct.pack("<4d", 0.0, 0.0, 0.0, 0.0))
    for record, _offset in records:
        shp += record
    shx = bytearray(struct.pack(">iiiiiii", 9994, 0, 0, 0, 0, 0, (100 + 8 * len(points)) // 2)
                    + struct.pack("<ii", 1000, 1) + struct.pack("<4d", *bounds)
                    + struct.pack("<4d", 0.0, 0.0, 0.0, 0.0))
    for record, record_offset in records:
        shx += struct.pack(">ii", record_offset // 2, (len(record) - 8) // 2)
    return bytes(shp), bytes(shx)


def build_metadata_text():
    return ("\n".join(source.EXPECTED_TXT_MARKERS) + "\n").encode("ascii")


def build_metadata_xml():
    return ("\n".join(source.EXPECTED_XML_MARKERS) + "\n").encode("ascii")


def fixture_members():
    shp, shx = build_shp_shx(fixture_points())
    return {"minop4x020.dbf": build_dbf(fixture_rows()), "minop4x020.shp": shp,
            "minop4x020.shx": shx, "minop4x020.txt": build_metadata_text()}


def build_package(members):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        for name, body in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(body)
            archive.addfile(info, io.BytesIO(body))
    return stream.getvalue()


def patched_pins(members, xml=None, count=2):
    values = {
        "COUNT": count,
        "EXPECTED_PACKAGE_BYTES": None,
        "EXPECTED_PACKAGE_SHA256": None,
        "EXPECTED_METADATA_BYTES": None,
        "EXPECTED_METADATA_SHA256": None,
        "MEMBER_PINS": {},
    }
    package = build_package(members)
    xml = xml if xml is not None else build_metadata_xml()
    values["EXPECTED_PACKAGE_BYTES"] = len(package)
    values["EXPECTED_PACKAGE_SHA256"] = hashlib.sha256(package).hexdigest()
    values["EXPECTED_METADATA_BYTES"] = len(xml)
    values["EXPECTED_METADATA_SHA256"] = hashlib.sha256(xml).hexdigest()
    values["MEMBER_PINS"] = {name: {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
                             for name, body in members.items()}
    return patch.multiple(source, **values), package, xml


def transport(package, xml, package_url=None, metadata_url=None):
    package_url = package_url or source.PACKAGE_CAPTURE_URL
    metadata_url = metadata_url or source.LEGACY_METADATA_URL

    def handler(request: httpx.Request) -> httpx.Response:
        def stream(payload):
            class Stream(httpx.SyncByteStream):
                def __iter__(self):
                    yield payload
            return Stream()
        if request.url.host == "web.archive.org" and request.url == httpx.URL(package_url):
            return httpx.Response(200, headers={"content-type": "application/x-gzip"},
                                  stream=stream(package))
        if request.url == httpx.URL(metadata_url):
            return httpx.Response(200, headers={"content-type": "application/xml"},
                                  stream=stream(xml))
        return httpx.Response(404, stream=stream(b"not found"))
    return httpx.MockTransport(handler)


def pinned_client(client):
    class PinnedClient:
        def __enter__(self):
            return client

        def __exit__(self, *_exc):
            return False
    return PinnedClient()


class FerrousValidationTests(unittest.TestCase):
    def pinned(self, **overrides):
        values = {"COUNT": 2}
        values.update(overrides)
        return patch.multiple(source, **values)

    def test_typed_geoparquet_preserves_native_values_ids_and_source_crs(self):
        members = fixture_members()
        with patched_pins(members)[0], self.pinned():
            rows = source._decode_dbf(members["minop4x020.dbf"])
            wkbs = source._decode_points(members["minop4x020.shp"], members["minop4x020.shx"])
            features, audit = source._validate_population(rows, wkbs)
            parquet = source._parquet_bytes(rows, wkbs)
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.column("MINOP4X020").to_pylist(), [Decimal("1"), Decimal("2")])
        self.assertEqual(table.schema.field("MINOP4X020").type.__str__(), "decimal128(2, 0)")
        self.assertEqual(table.schema.field("LATITUDE").type.__str__(), "decimal128(9, 3)")
        self.assertEqual(table.schema.field("LONGITUDE").type.__str__(), "decimal128(11, 3)")
        self.assertEqual(table.schema.field("COMMODITY").type.__str__(), "string")
        self.assertEqual(table.column("SITE_NAME").to_pylist(), ["Alpha Works", "Beta Plant"])
        self.assertEqual(table.column("LATITUDE").to_pylist(),
                         [Decimal("34.356"), Decimal("39.584")])
        self.assertEqual(table.column("LONGITUDE").to_pylist(),
                         [Decimal("-77.865"), Decimal("-81.678")])
        self.assertEqual([row["source_fid"] for row in table.to_pylist()], [0, 1])
        self.assertEqual([row["source_shp_record_number"] for row in table.to_pylist()], [1, 2])
        geoms = [from_wkb(value) for value in table.column("geometry").to_pylist()]
        self.assertEqual([(point.x, point.y) for point in geoms],
                         [(-77.8651, 34.3562), (-81.6779, 39.5843)])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
        from pyproj import CRS
        self.assertEqual(CRS.from_user_input(geo["columns"]["geometry"]["crs"]),
                         CRS.from_epsg(4269))
        self.assertIn(b"EPSG:4269",
                      table.schema.metadata[b"native_crs_declaration"])
        self.assertEqual(audit["source_feature_count"], 2)
        self.assertEqual(audit["plant_min_counts"], {"P": 1, "M/P": 1})
        self.assertEqual(audit["states"], 2)
        self.assertGreater(audit["max_coordinate_vs_declared_attribute_delta_degrees"], 0.0)

    def test_missing_renamed_or_extra_dbf_field_fails_closed(self):
        rows = fixture_rows()
        renamed_rows = [dict(row, COMMOD1TY=row["COMMODITY"]) for row in rows]
        renamed = [("MINOP4X020", "N", 2, 0), ("COMMOD1TY", "C", 26, 0)] + source.DBF_FIELDS[2:]
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "missing_renamed_extra_ID_type_width_count_or_DBF_field_drift"):
                source._decode_dbf(build_dbf(renamed_rows, fields=renamed))
        missing = source.DBF_FIELDS[1:]
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "missing_renamed_extra_ID_type_width_count_or_DBF_field_drift"):
                source._decode_dbf(build_dbf(rows, fields=missing))
        extra = source.DBF_FIELDS + [("UNEXPECTED", "C", 5, 0)]
        extra_rows = [dict(row, UNEXPECTED="x") for row in rows]
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "missing_renamed_extra_ID_type_width_count_or_DBF_field_drift"):
                source._decode_dbf(build_dbf(extra_rows, fields=extra))

    def test_value_and_type_loss_fails_closed(self):
        rows = fixture_rows()
        rows[0]["LATITUDE"] = "34.36"
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "declared_DBF_scale_drift:LATITUDE"):
                source._decode_dbf(build_dbf(rows))
        rows = fixture_rows()
        rows[0]["COMMODITY"] = "Iro\xf1"
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "undeclared_nonASCII_source_encoding:COMMODITY"):
                source._decode_dbf(build_dbf(rows))
        rows = fixture_rows()
        rows[0]["MINOP4X020"] = "x"
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "null_nonfinite_or_unparseable_DBF_number:MINOP4X020"):
                source._decode_dbf(build_dbf(rows))

    def test_id_roster_and_record_order_drift_fails_closed(self):
        rows = fixture_rows()
        rows[1]["MINOP4X020"] = Decimal("3")
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "missing_duplicate_reordered_original_MINOP4X020"):
                source._decode_dbf(build_dbf(rows))
        members = fixture_members()
        tampered = bytearray(members["minop4x020.shp"])
        struct.pack_into(">i", tampered, 100, 5)
        members["minop4x020.shp"] = bytes(tampered)
        with patched_pins(members)[0], self.pinned():
            rows = source._decode_dbf(members["minop4x020.dbf"])
            with self.assertRaisesRegex(ValueError, "SHP_SHX_record_ID_offset_type_or_coordinate_drift"):
                source._decode_points(members["minop4x020.shp"], members["minop4x020.shx"])
        shp, shx = build_shp_shx(fixture_points()[:1])
        with patched_pins(members)[0], self.pinned():
            with self.assertRaisesRegex(ValueError, "incomplete_SHP_SHX"):
                source._decode_points(shp, shx)

    def test_geometry_bounds_and_index_drift_fails_closed(self):
        points = fixture_points()
        shp, shx = build_shp_shx(points)
        tampered = bytearray(shp)
        struct.pack_into("<d", tampered, 36, 0.0)
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "native_bounds_drift"):
                source._decode_points(bytes(tampered), shx)
        tampered = bytearray(shx)
        struct.pack_into("<d", tampered, 36, 0.0)
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "native_bounds_index_drift"):
                source._decode_points(shp, bytes(tampered))
        tampered = bytearray(shx)
        struct.pack_into(">ii", tampered, 100, 999, 10)
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "SHP_SHX_record_ID_offset_type_or_coordinate_drift"):
                source._decode_points(shp, bytes(tampered))

    def test_metadata_marker_drift_fails_closed(self):
        text = build_metadata_text().decode("ascii").replace(
            "Horizontal_Datum_Name: North American Datum of 1983",
            "Horizontal_Datum_Name: North American Datum of 1927")
        with self.assertRaisesRegex(ValueError, "producer_metadata_marker_drift:Horizontal_Datum_Name"):
            source._validate_metadata_text(text.encode("ascii"))
        xml = build_metadata_xml().decode("ascii").replace(
            "<title>Ferrous Metal Processing Plants - Direct Download</title>",
            "<title>Some Other Layer - Direct Download</title>")
        with self.assertRaisesRegex(ValueError, "producer_metadata_marker_drift"):
            source._validate_metadata_xml(xml.encode("ascii"))
        for marker in source.EXPECTED_XML_MARKERS[3:]:
            with self.subTest(marker=marker), self.assertRaisesRegex(
                    ValueError, "producer_metadata_marker_drift"):
                source._validate_metadata_xml(build_metadata_xml().decode("ascii").replace(
                    marker, "", 1).encode("ascii"))


class FerrousCaptureCustodyTests(unittest.TestCase):
    def test_non_200_response_body_and_wire_are_retained_before_refusal(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir(parents=True)
            transport = httpx.MockTransport(lambda request: httpx.Response(
                503, stream=_stream(b"temporarily unavailable")))
            with httpx.Client(transport=transport) as client:
                with self.assertRaisesRegex(ValueError, "incomplete_source_capture"):
                    capture_get(client, source.PACKAGE_CAPTURE_URL, root, "national_atlas_package",
                                max_bytes=source.MAX_PACKAGE_BYTES)
            receipt = json.loads((root / "national_atlas_package.receipt.json").read_bytes())
            self.assertEqual(receipt["status"], 503)
            self.assertEqual((root / "national_atlas_package.wire").read_bytes(), b"temporarily unavailable")

    def test_capture_rejects_remote_or_published_storage(self):
        with TemporaryDirectory() as tmp:
            for storage in (PublishedStorageResource(use_local=True, local_dir=tmp),
                            StagingStorageResource(use_local=False, bucket="unused"),
                            StagingStorageResource(use_local=True, local_dir=tmp, prefix="nested")):
                with self.assertRaisesRegex(ValueError, "explicit_isolated_local_staging_required"):
                    source.capture_ferrous_metal_processing_plants(storage, version="v1")
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_ferrous_metal_processing_plants(
                    StagingStorageResource(use_local=True, local_dir=tmp), version="../escape")


def _stream(payload):
    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            yield payload
    return Stream()


class FerrousEndToEndTests(unittest.TestCase):
    def _fixture_repo(self, tmp):
        repo = Path(tmp) / "repo"
        (repo / "src/dagster_hifld").mkdir(parents=True)
        (repo / "src/dagster_hifld/runtime.py").write_text("candidate transform = 1\n")
        (repo / "pyproject.toml").write_text("[project]\nname='fixture'\n")
        (repo / "uv.lock").write_text("version = 1\n")
        return repo

    def _captured(self, tmp, members=None, served_members=None):
        members = members if members is not None else fixture_members()
        patcher, package, xml = patched_pins(members)
        storage = StagingStorageResource(use_local=True, local_dir=tmp, prefix="")
        served = build_package(served_members) if served_members is not None else package
        transport_instance = transport(served, xml)
        client = httpx.Client(transport=transport_instance)
        self.addCleanup(client.close)
        repo = self._fixture_repo(tmp)

        def run(**kwargs):
            with patcher, patch.object(source.httpx, "Client", lambda **_kw: pinned_client(client)):
                return source.capture_ferrous_metal_processing_plants(
                    storage, repo_root=repo, **kwargs)
        return run, storage, members

    def test_full_capture_cache_replay_and_tamper_fail_closed(self):
        with TemporaryDirectory() as tmp:
            run, storage, _members = self._captured(tmp)
            first = run(version="v1")
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], 2)
            prefix = f"{first['dataset_slug']}/{first['file_slug']}/{first['version']}"
            keys = set(storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/geoparquet/ferrous-metal-processing-plants.parquet", keys)
            self.assertNotIn(f"{prefix}/shapefile/minop4x020.shp", keys)
            self.assertIn(f"{prefix}/raw/minop4x020.shp", keys)
            self.assertIn(f"{prefix}/raw/national_atlas_package.body", keys)
            self.assertIn(f"{prefix}/raw/minop4x020.dbf", keys)
            ready = json.loads(storage.read_key(f"{prefix}/metadata/candidate_ready.json"))
            self.assertFalse(ready["approved"])
            self.assertFalse(ready["release_accepted"])
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            self.assertFalse(manifest["approved"])
            roles = {item["input_id"]: item["role"] for item in manifest["inputs"]}
            self.assertEqual(roles["national_atlas_package"], "archive")
            self.assertEqual(roles["legacy_metadata_xml"], "producer_publication")
            self.assertIsNone(manifest["inputs"][0]["release_date"])
            parquet = storage.read_key(f"{prefix}/geoparquet/ferrous-metal-processing-plants.parquet")

            second = run(version="v2")
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])
            self.assertEqual(storage.read_key(f"{prefix}/geoparquet/ferrous-metal-processing-plants.parquet"),
                             parquet)

            tampered = Path(tmp) / prefix / "raw" / "minop4x020.dbf"
            original = tampered.read_bytes()
            tampered.write_bytes(original[:-1] + b"\x00")
            with self.assertRaisesRegex(ValueError, "cached_candidate_object_integrity_failed:raw/minop4x020.dbf"):
                run(version="v3")
            self.assertEqual(storage.read_key(f"{prefix}/geoparquet/ferrous-metal-processing-plants.parquet"),
                             parquet)
            state_key = f"_construction-state/ferrous-metal-processing-plants/{first['version']}.json"
            self.assertIsNotNone(storage.object_snapshot(state_key))

    def test_catalog_cache_revalidates_canonical_metadata_and_source_snapshots(self):
        with TemporaryDirectory() as tmp:
            run, storage, _members = self._captured(tmp)
            first = run(version="v1")
            prefix = f"{first['dataset_slug']}/{first['file_slug']}/{first['version']}"
            snapshot_source_metadata(storage, first["dataset_slug"], first["file_slug"], first["version"])
            resolved = load_resolved_source_manifest(
                storage, first["dataset_slug"], first["file_slug"], first["version"]
            )
            quality = {"version": "v1", "feature_count": first["feature_count"],
                       "quality_check_passed": True}
            dictionary = generate_data_dictionary(
                gpd.GeoDataFrame(), first["dataset_slug"], source_metadata=resolved.metadata,
            )
            summary = CatalogSummary(quality, dictionary)
            write_catalog_metadata(
                storage, first["dataset_slug"], first["file_slug"], first["version"],
                summary.quality_manifest, summary.data_dictionary,
            )
            quality_key = f"{prefix}/metadata/quality_manifest.json"
            original_quality = storage.read_key(quality_key)
            tampered_quality = json.loads(original_quality)
            tampered_quality["feature_count"] += 1
            storage.write_key(quality_key, json.dumps(tampered_quality).encode())
            with patch("dagster_hifld.sources.epa_frs_common.summarize_staged_catalog",
                       return_value=summary):
                with self.assertRaisesRegex(ValueError, "cached_catalog_metadata_mismatch"):
                    run(version="v2")
            storage.write_key(quality_key, original_quality)

            snapshots = sorted((Path(tmp) / prefix / "metadata/source").glob("*source_manifest.json"))
            self.assertEqual(len(snapshots), 2)
            snapshots[0].unlink()
            with patch("dagster_hifld.sources.epa_frs_common.summarize_staged_catalog",
                       return_value=summary):
                with self.assertRaisesRegex(ValueError, "cached_catalog_source_snapshot_incomplete"):
                    run(version="v3")

    def test_drifted_archive_bytes_fail_closed_and_are_quarantined(self):
        with TemporaryDirectory() as tmp:
            members = fixture_members()
            drift = dict(members)
            drift["minop4x020.txt"] = build_metadata_text() + b"unexpected tail\n"
            run, storage, _members = self._captured(tmp, members=members, served_members=drift)
            with self.assertRaisesRegex(ValueError, "pinned_archive_bytes_drift:national_atlas_package"):
                run(version="v1")
            quarantine = list(storage.list_prefix("_quarantine/ferrous-metal-processing-plants"))
            self.assertTrue(any(name.endswith("/raw/national_atlas_package.body") for name in quarantine))
            failures = [name for name in quarantine if name.endswith("/metadata/failure.json")]
            self.assertTrue(failures)
            self.assertEqual(set(storage.list_prefix("ferrous-metal-processing-plants")), set())

    def test_incomplete_package_roster_fails_closed(self):
        with TemporaryDirectory() as tmp:
            members = fixture_members()
            del members["minop4x020.shx"]
            run, storage, _members = self._captured(tmp, members=members)
            with self.assertRaisesRegex(ValueError, "package_member_roster_drift"):
                run(version="v1")
            failures = [name for name in storage.list_prefix("_quarantine/ferrous-metal-processing-plants")
                        if name.endswith("/metadata/failure.json")]
            self.assertTrue(failures)


class FerrousRegistrationTests(unittest.TestCase):
    def test_explicit_source_ingest_wiring_and_stopped_schedule(self):
        with patch.dict("os.environ", {"HIFLD_SHADOW_MANUAL_ONLY": "1"}):
            from dagster import DefaultScheduleStatus

            from dagster_hifld.assets.ferrous_metal_processing_plants import (
                ferrous_metal_processing_plants_draft_schedule,
            )
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
            from dagster_hifld.definitions import defs
            slug = "ferrous-metal-processing-plants"
            graph = defs.resolve_asset_graph()
            keys = {tuple(key.path) for key in graph.get_all_asset_keys()}
            self.assertIn(("ingest", slug, slug), keys)
            self.assertIn(("publish", "catalog"), keys)
            self.assertIn(("publish", "verify"), keys)
            self.assertIn(("publish", "promote"), keys)
            self.assertEqual(
                sum(any(tuple(key.path) == ("ingest", slug, slug) for key in asset.keys)
                    for asset in hifld_ingest_assets),
                1)
            self.assertIsNotNone(defs.resolve_job_def("ferrous_metal_processing_plants_draft_refresh"))
            self.assertEqual(ferrous_metal_processing_plants_draft_schedule.default_status,
                             DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()
