"""Native fidelity, refusal, boundary, and approval-gate tests for the Miscellaneous Industrial Mineral Operations source."""
import hashlib
import io
import struct
import tarfile
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import miscellaneous_industrial_mineral_operations as source


def fixture_rows():
    return [
        {"MINOP5X020": Decimal("1"), "COMMODITY": "Barite", "SITE_NAME": "Alpha Works",
         "COMPANY_NA": "Test Mining Co", "STATE_LOCA": "Nevada", "COUNTY": "Nye",
         "LATITUDE": Decimal("34.356"), "LONGITUDE": Decimal("-77.865"), "PLANT_MIN": "P"},
        {"MINOP5X020": Decimal("2"), "COMMODITY": "Kaolin", "SITE_NAME": "Beta Plant",
         "COMPANY_NA": "Beta Clays Inc", "STATE_LOCA": "Georgia", "COUNTY": "Washington",
         "LATITUDE": Decimal("39.584"), "LONGITUDE": Decimal("-81.678"), "PLANT_MIN": "M/P"},
        {"MINOP5X020": Decimal("4"), "COMMODITY": "Trona", "SITE_NAME": "Delta Mine",
         "COMPANY_NA": "Delta Minerals", "STATE_LOCA": "Wyoming", "COUNTY": "Sweetwater",
         "LATITUDE": Decimal("41.123"), "LONGITUDE": Decimal("-109.456"), "PLANT_MIN": "M"},
    ]


def fixture_ids():
    return (1, 2, 4)


def fixture_points():
    return [(-77.8651, 34.3562), (-81.6779, 39.5843), (-109.4561, 41.1228)]


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
    body = ("\n".join(source.EXPECTED_TXT_MARKERS) + "\n"
            "    Clay operations (kaolin, fuller\u2019s earth) may have many satellite pits.\n")
    return body.encode(source.METADATA_TEXT_ENCODING)


def build_metadata_xml():
    return ("\n".join(source.EXPECTED_XML_MARKERS) + "\n").encode("ascii")


def fixture_members():
    shp, shx = build_shp_shx(fixture_points())
    return {"minop5x020.dbf": build_dbf(fixture_rows()), "minop5x020.shp": shp,
            "minop5x020.shx": shx, "minop5x020.txt": build_metadata_text()}


def build_package(members):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        for name, body in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(body)
            archive.addfile(info, io.BytesIO(body))
    return stream.getvalue()


def patched_pins(members):
    package = build_package(members)
    xml = build_metadata_xml()
    values = {
        "COUNT": 3,
        "EXPECTED_IDS": fixture_ids(),
        "EXPECTED_PACKAGE_BYTES": len(package),
        "EXPECTED_PACKAGE_SHA256": hashlib.sha256(package).hexdigest(),
        "EXPECTED_METADATA_BYTES": len(xml),
        "EXPECTED_METADATA_SHA256": hashlib.sha256(xml).hexdigest(),
        "MEMBER_PINS": {name: {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
                        for name, body in members.items()},
    }
    return patch.multiple(source, **values)


class MiscellaneousValidationTests(unittest.TestCase):
    def pinned(self, **overrides):
        values = {"COUNT": 3, "EXPECTED_IDS": fixture_ids()}
        values.update(overrides)
        return patch.multiple(source, **values)

    def test_typed_geoparquet_preserves_native_values_ids_and_source_crs(self):
        members = fixture_members()
        with patched_pins(members), self.pinned():
            rows = source._decode_dbf(members["minop5x020.dbf"])
            wkbs = source._decode_points(members["minop5x020.shp"], members["minop5x020.shx"])
            features, audit = source._validate_population(rows, wkbs)
            parquet = source._parquet_bytes(rows, wkbs)
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.column("MINOP5X020").to_pylist(),
                         [Decimal("1"), Decimal("2"), Decimal("4")])
        self.assertEqual(table.schema.field("MINOP5X020").type.__str__(), "decimal128(3, 0)")
        self.assertEqual(table.schema.field("LATITUDE").type.__str__(), "decimal128(15, 6)")
        self.assertEqual(table.schema.field("LONGITUDE").type.__str__(), "decimal128(16, 6)")
        self.assertEqual(table.schema.field("COMMODITY").type.__str__(), "string")
        self.assertEqual(table.column("SITE_NAME").to_pylist(), ["Alpha Works", "Beta Plant", "Delta Mine"])
        self.assertEqual(table.column("LATITUDE").to_pylist(),
                         [Decimal("34.356"), Decimal("39.584"), Decimal("41.123")])
        self.assertEqual([row["source_fid"] for row in table.to_pylist()], [0, 1, 2])
        self.assertEqual([row["source_shp_record_number"] for row in table.to_pylist()], [1, 2, 3])
        geoms = [from_wkb(value) for value in table.column("geometry").to_pylist()]
        self.assertEqual([(point.x, point.y) for point in geoms],
                         [(-77.8651, 34.3562), (-81.6779, 39.5843), (-109.4561, 41.1228)])
        import json
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
        from pyproj import CRS
        self.assertEqual(CRS.from_user_input(geo["columns"]["geometry"]["crs"]),
                         CRS.from_epsg(4269))
        self.assertIn(b"EPSG:4269",
                      table.schema.metadata[b"native_crs_declaration"])
        self.assertEqual(audit["source_feature_count"], 3)
        self.assertEqual(audit["plant_min_counts"], {"P": 1, "M/P": 1, "M": 1})
        self.assertEqual(audit["states"], 3)
        self.assertGreater(audit["max_coordinate_vs_declared_attribute_delta_degrees"], 0.0)
        self.assertIn("gap at 187", audit["id_roster_note"])
        self.assertIn("research context only", audit["classification"])
        self.assertIn(b"identity",
                      table.schema.field("source_shp_record_number").metadata)

    def test_missing_renamed_or_extra_dbf_field_fails_closed(self):
        rows = fixture_rows()
        renamed_rows = [dict(row, COMMOD1TY=row["COMMODITY"]) for row in rows]
        renamed = [("MINOP5X020", "N", 3, 0), ("COMMOD1TY", "C", 24, 0)] + source.DBF_FIELDS[2:]
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
        rows[0]["COMMODITY"] = "Bari\xf1e"
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "undeclared_nonASCII_source_encoding:COMMODITY"):
                source._decode_dbf(build_dbf(rows))
        rows = fixture_rows()
        rows[0]["MINOP5X020"] = "x"
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "null_nonfinite_or_unparseable_DBF_number:MINOP5X020"):
                source._decode_dbf(build_dbf(rows))

    def test_id_roster_and_record_order_drift_fails_closed(self):
        rows = fixture_rows()
        contiguous = [dict(row, MINOP5X020=Decimal("3")) for row in rows]
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "missing_duplicate_reordered_or_gapped_original_MINOP5X020"):
                source._decode_dbf(build_dbf(contiguous))
        duplicated = [dict(row, MINOP5X020=Decimal("2")) for row in rows]
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "missing_duplicate_reordered_or_gapped_original_MINOP5X020"):
                source._decode_dbf(build_dbf(duplicated))
        reordered = [fixture_rows()[2], fixture_rows()[0], fixture_rows()[1]]
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "missing_duplicate_reordered_or_gapped_original_MINOP5X020"):
                source._decode_dbf(build_dbf(reordered))
        members = fixture_members()
        tampered = bytearray(members["minop5x020.shp"])
        struct.pack_into(">i", tampered, 100, 5)
        members["minop5x020.shp"] = bytes(tampered)
        with patched_pins(members), self.pinned():
            rows = source._decode_dbf(members["minop5x020.dbf"])
            with self.assertRaisesRegex(ValueError, "SHP_SHX_record_ID_offset_type_or_coordinate_drift"):
                source._decode_points(members["minop5x020.shp"], members["minop5x020.shx"])
        shp, shx = build_shp_shx(fixture_points()[:1])
        with patched_pins(fixture_members()), self.pinned():
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
        text = build_metadata_text().decode(source.METADATA_TEXT_ENCODING).replace(
            "Horizontal_Datum_Name: North American Datum of 1983",
            "Horizontal_Datum_Name: North American Datum of 1927")
        with self.assertRaisesRegex(ValueError, "producer_metadata_marker_drift:Horizontal_Datum_Name"):
            source._validate_metadata_text(text.encode(source.METADATA_TEXT_ENCODING))
        xml = build_metadata_xml().decode("ascii").replace(
            "<title>Miscellaneous Industrial Minerals Operations - Direct Download</title>",
            "<title>Some Other Layer - Direct Download</title>")
        with self.assertRaisesRegex(ValueError, "producer_metadata_marker_drift"):
            source._validate_metadata_xml(xml.encode("ascii"))
        for marker in source.EXPECTED_XML_MARKERS[3:]:
            with self.subTest(marker=marker), self.assertRaisesRegex(
                    ValueError, "producer_metadata_marker_drift"):
                source._validate_metadata_xml(build_metadata_xml().decode("ascii").replace(
                    marker, "", 1).encode("ascii"))

    def test_metadata_text_encoding_drift_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "producer_metadata_text_encoding_drift"):
            source._validate_metadata_text(build_metadata_text() + b"\x93")
        with self.assertRaisesRegex(ValueError, "producer_metadata_text_encoding_drift"):
            source._validate_metadata_text(build_metadata_text().replace(b"\x92", b"'"))
        document = source._validate_metadata_text(build_metadata_text())
        self.assertIn("0x92", document["text_encoding_note"])

    def test_package_roster_member_pin_and_tar_drift_fail_closed(self):
        members = fixture_members()
        with patched_pins(members):
            extracted = source._extract_package(build_package(members))
            self.assertEqual(set(extracted), set(source.MEMBER_NAMES))
        missing = dict(members)
        del missing["minop5x020.shx"]
        with patched_pins(members):
            with self.assertRaisesRegex(ValueError, "package_member_roster_drift"):
                source._extract_package(build_package(missing))
        tampered = dict(members)
        tampered["minop5x020.txt"] = members["minop5x020.txt"] + b"unexpected tail\n"
        with patched_pins(members):
            with self.assertRaisesRegex(ValueError, "pinned_package_member_drift:minop5x020.txt"):
                source._extract_package(build_package(tampered))
        with self.assertRaisesRegex(ValueError, "package_tar_gz_unreadable"):
            source._extract_package(b"not-a-tarball")


class MiscellaneousSourceBoundaryTests(unittest.TestCase):
    def test_capture_rejects_remote_or_published_storage(self):
        with TemporaryDirectory() as tmp:
            for storage in (PublishedStorageResource(use_local=True, local_dir=tmp),
                            StagingStorageResource(use_local=False, bucket="unused"),
                            StagingStorageResource(use_local=True, local_dir=tmp, prefix="nested")):
                with self.assertRaisesRegex(ValueError, "explicit_isolated_local_staging_required"):
                    source.capture_miscellaneous_industrial_mineral_operations(storage, version="v1")
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_miscellaneous_industrial_mineral_operations(
                    StagingStorageResource(use_local=True, local_dir=tmp), version="../escape")

    def test_capture_refuses_candidate_construction_without_official_distribution(self):
        with TemporaryDirectory() as tmp:
            staging_root = Path(tmp) / "staging"
            storage = StagingStorageResource(use_local=True, local_dir=str(staging_root))
            with self.assertRaisesRegex(
                    ValueError, "national_atlas_publication_retired_no_official_distribution"):
                source.capture_miscellaneous_industrial_mineral_operations(
                    storage, version="v20261110")
            self.assertFalse(staging_root.exists())
            self.assertFalse(any(Path(tmp).iterdir()))
            self.assertFalse(hasattr(source, "httpx"))

    def test_executable_module_contains_no_third_party_archive_capture_input(self):
        module_bytes = Path(source.__file__).read_bytes()
        self.assertNotIn(b"web.archive.org", module_bytes)
        self.assertFalse(hasattr(source, "PACKAGE_CAPTURE_URL"))
        self.assertIn("dds.cr.usgs.gov/pub/data/nationalatlas/minop5x020_nt00022.tar.gz",
                      source.ORIGINAL_PACKAGE_URL)


class MiscellaneousRegistrationTests(unittest.TestCase):
    def test_explicit_source_ingest_wiring_and_stopped_schedule(self):
        with patch.dict("os.environ", {"HIFLD_SHADOW_MANUAL_ONLY": "1"}):
            from dagster import DefaultScheduleStatus

            from dagster_hifld.assets.miscellaneous_industrial_mineral_operations import (
                miscellaneous_industrial_mineral_operations_draft_schedule,
            )
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
            from dagster_hifld.definitions import defs
            slug = "miscellaneous-industrial-mineral-operations"
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
            self.assertIsNotNone(defs.resolve_job_def("miscellaneous_industrial_mineral_operations_draft_refresh"))
            self.assertEqual(miscellaneous_industrial_mineral_operations_draft_schedule.default_status,
                             DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()
