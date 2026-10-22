"""Synthetic-fixture tests for the ferrous-metal-mines preserved-capture draft pipeline.

The fixtures reconstruct the National Atlas minop3x020 shapefile layout at a
small scale; they exercise the strict decode/quarantine paths without granting
any source, schema, baseline, or release acceptance.
"""
import io
import json
import struct
import tarfile
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import ferrous_metal_mines as s

NAD83_WKT = ('GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",'
             'SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],'
             'UNIT["Degree",0.0174532925199433]]')
WGS84_WKT = ('GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,'
             '298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]')

ROWS = [
    {"MINOP3X020": 1, "COMMODITY": "Iron", "SITE_NAME": "Alpha Mine",
     "COMPANY_NA": "Alpha Iron Company", "STATE_LOCA": "AZ", "COUNTY": "Mohave",
     "LATITUDE": Decimal("34.500"), "LONGITUDE": Decimal("-112.300"), "PLANT_MIN": "Mine"},
    {"MINOP3X020": 2, "COMMODITY": "Iron", "SITE_NAME": "Beta Works",
     "COMPANY_NA": "Beta Steel LLC", "STATE_LOCA": "MI", "COUNTY": "Marquette",
     "LATITUDE": Decimal("46.400"), "LONGITUDE": Decimal("-87.650"), "PLANT_MIN": "Both"},
]


def _field(name, kind, width, scale):
    descriptor = name.encode("ascii")[:10].ljust(11, b"\0") + kind.encode("ascii") + b"\0" * 4
    return descriptor + bytes([width, scale]) + b"\0" * 14


def _dbf_number(value, width, scale):
    text = f"{value:.{scale}f}" if scale else str(int(value))
    if "." in text:
        whole, fraction = text.split(".")
        text = f"{whole[:width - scale - 1].rjust(width - scale - 1)}.{fraction}"
    return text.rjust(width).encode("ascii")[:width]


def dbf_bytes(rows, fields=None):
    fields = fields or [
        ("MINOP3X020", "N", 3, 0), ("COMMODITY", "C", 21, 0), ("SITE_NAME", "C", 46, 0),
        ("COMPANY_NA", "C", 51, 0), ("STATE_LOCA", "C", 14, 0), ("COUNTY", "C", 16, 0),
        ("LATITUDE", "N", 9, 3), ("LONGITUDE", "N", 11, 3), ("PLANT_MIN", "C", 10, 0)]
    width = 1 + sum(field[2] for field in fields)
    header = 32 + 32 * len(fields) + 1
    body = b"\x03" + b"\0" * 3 + struct.pack("<I", len(rows)) + struct.pack("<H", header)
    body += struct.pack("<H", width) + b"\0" * 20
    for field in fields:
        body += _field(*field)
    body += b"\x0d"
    for row in rows:
        record = b" "
        for name, kind, size, scale in fields:
            value = row[name]
            record += (_dbf_number(value, size, scale) if kind == "N"
                       else value.encode("ascii").ljust(size)[:size])
        body += record
    return body


def _shapefile_header(point_count, points):
    xs = [float(point[0]) for point in points]
    ys = [float(point[1]) for point in points]
    length_words = (100 + 28 * point_count) // 2
    header = struct.pack(">iiiiii", 9994, 0, 0, 0, 0, 0)
    header += struct.pack(">i", length_words)
    header += struct.pack("<ii", 1000, 1)
    header += struct.pack("<4d", min(xs), min(ys), max(xs), max(ys))
    header += struct.pack("<4d", 0.0, 0.0, 0.0, 0.0)
    return header


def shp_bytes(points):
    body = _shapefile_header(len(points), points)
    for index, (x, y) in enumerate(points, 1):
        body += struct.pack(">ii", index, 10) + struct.pack("<idd", 1, x, y)
    return body


def shx_bytes(points):
    length_words = (100 + 8 * len(points)) // 2
    header = struct.pack(">iiiiii", 9994, 0, 0, 0, 0, 0)
    header += struct.pack(">i", length_words)
    header += struct.pack("<ii", 1000, 1)
    xs = [float(point[0]) for point in points]
    ys = [float(point[1]) for point in points]
    header += struct.pack("<4d", min(xs), min(ys), max(xs), max(ys))
    header += struct.pack("<4d", 0.0, 0.0, 0.0, 0.0)
    offset = 50
    for _ in points:
        header += struct.pack(">ii", offset, 10)
        offset += 14
    return header


def archive_bytes(rows=None, prj=NAD83_WKT, members=None):
    points = [(float(row["LONGITUDE"]), float(row["LATITUDE"])) for row in (rows or ROWS)]
    members = members or {
        "minop3x020.dbf": dbf_bytes(rows or ROWS),
        "minop3x020.shp": shp_bytes(points),
        "minop3x020.shx": shx_bytes(points),
        "minop3x020.prj": prj.encode("ascii"),
        "minop3x020.shp.xml": b"<metadata>example</metadata>",
    }
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        for name, data in sorted(members.items()):
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return stream.getvalue()


def receipt(body, url="http://dds.cr.usgs.gov/pub/data/nationalatlas/minop3x020_nt00020.tar.gz"):
    return {"url": url, "final_url": url, "status": 200, "complete_body": True,
            "bytes": len(body), "sha256": s.digest(body),
            "observed_at": datetime.now(timezone.utc).isoformat()}


def metadata_capture(tmp):
    body = b"<.metadata>synthetic producer metadata capture</metadata>"
    (Path(tmp) / "producer_metadata.body").write_bytes(body)
    (Path(tmp) / "producer_metadata.receipt.json").write_text(json.dumps(receipt(
        body, url=s.METADATA_URL)))
    return Path(tmp) / "producer_metadata.body", Path(tmp) / "producer_metadata.receipt.json"


class FerrousMetalMinesTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.archive = self.root / "archive.tar.gz"
        self.archive.write_bytes(archive_bytes())
        self.receipt = self.root / "archive.receipt.json"
        self.receipt.write_text(json.dumps(receipt(self.archive.read_bytes())))
        self.storage = StagingStorageResource(local_dir=str(self.root / "stage"))
        # Hermetic fixed code identity: the real processing identity binds to
        # committed Git blobs, which tests cannot rely on mid-development.
        identity_patcher = patch.object(s, "processing_identity", return_value={
            "code_commit": "0" * 40, "code_identity_scheme": "test-fixed-identity",
            "files": {}})
        validation_patcher = patch.object(s, "validate_construction_code_identity",
                                          return_value=None)
        self.patchers = [identity_patcher, validation_patcher]
        for patcher in self.patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def tearDown(self):
        self.tmp.cleanup()

    def stage(self, storage=None, archive=None, receipt_file=None):
        metadata_body, metadata_receipt = metadata_capture(self.root)
        return s.stage_ferrous_metal_mines(
            storage or self.storage,
            archive_path=archive or self.archive,
            archive_receipt_path=receipt_file or self.receipt,
            metadata_path=metadata_body, metadata_receipt_path=metadata_receipt)

    def test_explicit_capture_required_and_remote_or_aliased_storage_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "explicit_preserved_producer_archive"):
                s.stage_ferrous_metal_mines(StagingStorageResource(local_dir=tmp))
            for resource in (PublishedStorageResource(local_dir=tmp),
                             StagingStorageResource(bucket="forbidden", use_local=False),
                             StagingStorageResource(local_dir=tmp + "/../unsafe")):
                with patch.object(s, "acquire_preserved_archive") as acquire:
                    with self.assertRaises(ValueError):
                        s.stage_ferrous_metal_mines(resource)
                    acquire.assert_not_called()

    def test_full_values_ids_types_geometry_and_metadata_preserved(self):
        result = self.stage()
        self.assertFalse(result["reused"])
        self.assertTrue(result["source_population_complete"])
        self.assertFalse(result["accepted"])
        prefix = f"{s.DATASET_SLUG}/{s.FILE_SLUG}/{result['version']}"
        table = pq.read_table(self.root / "stage" / prefix / s.GEOPARQUET_PATH)
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table["MINOP3X020"].to_pylist(), [1, 2])
        self.assertEqual(table["SITE_NAME"].to_pylist(), ["Alpha Mine", "Beta Works"])
        self.assertEqual(table["LATITUDE"][0].as_py(), Decimal("34.500"))
        self.assertEqual(table["LATITUDE"].type, __import__("pyarrow").decimal128(9, 3))
        self.assertEqual(table["LONGITUDE"].type, __import__("pyarrow").decimal128(11, 3))
        self.assertEqual([round(point.x, 6) for point in shapely.from_wkb(table["geometry"].to_pylist())],
                         [-112.3, -87.65])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
        crs_json = json.dumps(geo["columns"]["geometry"]["crs"])
        self.assertIn("1983", crs_json)
        audit = s.strict_json((self.root / "stage" / prefix / s.AUDIT_MARKER).read_bytes())
        self.assertEqual(audit["feature_count"], 2)
        self.assertFalse(audit["datum_contradiction"])
        self.assertEqual(audit["source_ids"]["MINOP3X020"]["contiguous_from_one"], True)
        self.assertEqual(audit["dbf_geometry_max_coordinate_deviation_degrees"], 0.0)
        manifest = s.strict_json((self.root / "stage" / prefix / "metadata/source_manifest.json").read_bytes())
        self.assertFalse(manifest["approved"])
        self.assertEqual([item["role"] for item in manifest["inputs"]], ["archive", "producer_publication"])
        self.assertIn("geoparquet", manifest["prebuilt_output_policy"]["formats"])
        self.assertTrue(manifest["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"])
        self.assertIn("pmtiles", manifest["unsupported_output_formats"])
        members = {path.name for path in (self.root / "stage" / prefix / "raw/archive_members").iterdir()}
        self.assertEqual(members, {"minop3x020.dbf", "minop3x020.shp", "minop3x020.shx",
                                   "minop3x020.prj", "minop3x020.shp.xml"})
        ready = s.strict_json((self.root / "stage" / prefix / s.READY_MARKER).read_bytes())
        self.assertFalse(ready["approved"])
        self.assertEqual(set(ready["objects"]), {name for name in ready["objects"]})

    def test_cache_reuse_and_tamper_quarantine(self):
        first = self.stage()
        second = self.stage()
        self.assertTrue(second["reused"])
        self.assertEqual(first["version"], second["version"])
        prefix = f"{s.DATASET_SLUG}/{s.FILE_SLUG}/{first['version']}"
        target = self.root / "stage" / prefix / "candidate/typed-source.parquet"
        original = target.read_bytes()
        target.write_bytes(original + b"x")
        with self.assertRaisesRegex(ValueError, "source_owned_object_changed"):
            self.stage()
        target.write_bytes(original)
        third = self.stage()
        self.assertTrue(third["reused"])

    def test_missing_renamed_extra_or_typed_columns_fail(self):
        points = [(-112.3, 34.5), (-87.65, 46.4)]
        good_geometry = {"minop3x020.shp": shp_bytes(points),
                         "minop3x020.shx": shx_bytes(points),
                         "minop3x020.prj": NAD83_WKT.encode()}
        extra = archive_bytes(members={**good_geometry, "minop3x020.dbf": dbf_bytes(ROWS),
                                       "notes.txt": b"unexpected member"})
        renamed_rows = [dict(row, **{"OLD_NAME": row["MINOP3X020"]}) for row in ROWS]
        renamed_fields = [("OLD_NAME", "N", 3, 0)] + [
            field for field in [
                ("MINOP3X020", "N", 3, 0), ("COMMODITY", "C", 21, 0), ("SITE_NAME", "C", 46, 0),
                ("COMPANY_NA", "C", 51, 0), ("STATE_LOCA", "C", 14, 0), ("COUNTY", "C", 16, 0),
                ("LATITUDE", "N", 9, 3), ("LONGITUDE", "N", 11, 3), ("PLANT_MIN", "C", 10, 0)][1:]]
        renamed = archive_bytes(members={**good_geometry,
                                         "minop3x020.dbf": dbf_bytes(renamed_rows, renamed_fields)})
        typed = bytearray(dbf_bytes(ROWS))
        typed[32 + 11] = ord("F")
        wrong_type = archive_bytes(members={**good_geometry, "minop3x020.dbf": bytes(typed)})
        for label, body in (("renamed", renamed), ("wrong_type", wrong_type),
                            ("extra_member", extra)):
            (self.root / "bad.tar.gz").write_bytes(body)
            (self.root / "bad.receipt.json").write_text(json.dumps(receipt(body)))
            with self.subTest(label), self.assertRaises(ValueError):
                self.stage(archive=self.root / "bad.tar.gz",
                           receipt_file=self.root / "bad.receipt.json")

    def test_duplicate_zero_and_deleted_ids_and_truncation_fail(self):
        rows = [dict(row) for row in ROWS]
        rows[1]["MINOP3X020"] = 1
        duplicate = archive_bytes(rows=rows)
        rows[1]["MINOP3X020"] = 0
        zero = archive_bytes(rows=rows)
        truncated = archive_bytes(members={
            "minop3x020.dbf": dbf_bytes(ROWS)[:-10],
            "minop3x020.shp": shp_bytes([(-112.3, 34.5), (-87.65, 46.4)]),
            "minop3x020.shx": shx_bytes([(-112.3, 34.5), (-87.65, 46.4)]),
            "minop3x020.prj": NAD83_WKT.encode()})
        deleted = bytearray(dbf_bytes(ROWS))
        deleted[321] = ord("*")
        deleted_archive = archive_bytes(members={
            "minop3x020.dbf": bytes(deleted),
            "minop3x020.shp": shp_bytes([(-112.3, 34.5), (-87.65, 46.4)]),
            "minop3x020.shx": shx_bytes([(-112.3, 34.5), (-87.65, 46.4)]),
            "minop3x020.prj": NAD83_WKT.encode()})
        for label, body in (("duplicate", duplicate), ("zero", zero),
                            ("truncated", truncated), ("deleted", deleted_archive)):
            (self.root / "bad.tar.gz").write_bytes(body)
            (self.root / "bad.receipt.json").write_text(json.dumps(receipt(body)))
            with self.subTest(label), self.assertRaises(ValueError):
                self.stage(archive=self.root / "bad.tar.gz",
                           receipt_file=self.root / "bad.receipt.json")

    def test_non_ascii_scale_drift_and_nonfinite_numbers_fail(self):
        good_geometry = {"minop3x020.shp": shp_bytes([(-112.3, 34.5), (-87.65, 46.4)]),
                         "minop3x020.shx": shx_bytes([(-112.3, 34.5), (-87.65, 46.4)]),
                         "minop3x020.prj": NAD83_WKT.encode()}
        non_ascii = bytearray(dbf_bytes(ROWS))
        non_ascii[321 + 1 + 3 + 21] = 0xE9
        scale_drift = bytearray(dbf_bytes(ROWS))
        lat_offset = 321 + 1 + 3 + 21 + 46 + 51 + 14 + 16
        scale_drift[lat_offset:lat_offset + 9] = b"   34.5  "
        for label, payload in (("non_ascii", bytes(non_ascii)),
                               ("scale_drift", bytes(scale_drift))):
            body = archive_bytes(members={**good_geometry, "minop3x020.dbf": payload})
            (self.root / "bad.tar.gz").write_bytes(body)
            (self.root / "bad.receipt.json").write_text(json.dumps(receipt(body)))
            with self.subTest(label), self.assertRaises(ValueError):
                self.stage(archive=self.root / "bad.tar.gz",
                           receipt_file=self.root / "bad.receipt.json")

    def test_geometry_index_and_coordinate_crosscheck_failures(self):
        good_points = [(-112.3, 34.5), (-87.65, 46.4)]
        truncated = shp_bytes(good_points)[:-28]
        index_drift = bytearray(shx_bytes(good_points))
        struct.pack_into(">i", index_drift, 100, 51)
        nan_point = bytearray(shp_bytes(good_points))
        struct.pack_into("<d", nan_point, 112, float("nan"))
        extra = shp_bytes(good_points) + b"junk"
        drifted_rows = [dict(row) for row in ROWS]
        drifted_rows[1]["LONGITUDE"] = Decimal("-80.100")
        drift = archive_bytes(rows=drifted_rows)
        for label, members in (
            ("truncated", {"minop3x020.dbf": dbf_bytes(ROWS), "minop3x020.shp": truncated,
                           "minop3x020.shx": shx_bytes(good_points), "minop3x020.prj": NAD83_WKT.encode()}),
            ("index", {"minop3x020.dbf": dbf_bytes(ROWS), "minop3x020.shp": shp_bytes(good_points),
                       "minop3x020.shx": bytes(index_drift), "minop3x020.prj": NAD83_WKT.encode()}),
            ("nan", {"minop3x020.dbf": dbf_bytes(ROWS), "minop3x020.shp": bytes(nan_point),
                     "minop3x020.shx": shx_bytes(good_points), "minop3x020.prj": NAD83_WKT.encode()}),
            ("extra", {"minop3x020.dbf": dbf_bytes(ROWS), "minop3x020.shp": extra,
                       "minop3x020.shx": shx_bytes(good_points), "minop3x020.prj": NAD83_WKT.encode()}),
            ("drift", {"minop3x020.dbf": dbf_bytes(drifted_rows), "minop3x020.shp": shp_bytes(good_points),
                       "minop3x020.shx": shx_bytes(good_points), "minop3x020.prj": NAD83_WKT.encode()}),
            ("projected", {"minop3x020.dbf": dbf_bytes(ROWS), "minop3x020.shp": shp_bytes(good_points),
                           "minop3x020.shx": shx_bytes(good_points),
                           "minop3x020.prj": b'PROJCS["unknown"]'}),
        ):
            body = archive_bytes(members=members)
            (self.root / "bad.tar.gz").write_bytes(body)
            (self.root / "bad.receipt.json").write_text(json.dumps(receipt(body)))
            with self.subTest(label), self.assertRaises(ValueError):
                self.stage(archive=self.root / "bad.tar.gz",
                           receipt_file=self.root / "bad.receipt.json")

    def test_wgs84_prj_recorded_as_datum_contradiction_not_failure(self):
        rows = [dict(row) for row in ROWS]
        body = archive_bytes(rows=rows, prj=WGS84_WKT)
        (self.root / "wgs.tar.gz").write_bytes(body)
        (self.root / "wgs.receipt.json").write_text(json.dumps(receipt(body)))
        result = self.stage(archive=self.root / "wgs.tar.gz",
                            receipt_file=self.root / "wgs.receipt.json")
        prefix = f"{s.DATASET_SLUG}/{s.FILE_SLUG}/{result['version']}"
        audit = s.strict_json((self.root / "stage" / prefix / s.AUDIT_MARKER).read_bytes())
        self.assertTrue(audit["datum_contradiction"])
        self.assertEqual(audit["fgdc_declared_horizontal_datum"], s.FGDC_HORIZONTAL_DATUM)

    def test_receipt_hash_or_status_mismatch_fails_and_retains_evidence(self):
        body = self.archive.read_bytes()
        for mutate in (lambda r: r.update(sha256="0" * 64),
                       lambda r: r.update(status=403),
                       lambda r: r.update(complete_body=False)):
            tampered = receipt(body)
            mutate(tampered)
            (self.root / "bad.receipt.json").write_text(json.dumps(tampered))
            with self.assertRaises(ValueError):
                self.stage(receipt_file=self.root / "bad.receipt.json")
            quarantines = list((self.root / "stage" / "_quarantine").rglob("failure.json"))
            self.assertTrue(quarantines)
            (self.root / "bad.receipt.json").unlink()

    def test_live_metadata_pin_drift_fails_closed(self):
        bad_body = b"<metadata>drifted</metadata>"
        with patch.object(s, "capture_get", return_value=(bad_body, {
                "method": "GET", "url": s.METADATA_URL, "final_url": s.METADATA_URL,
                "observed_at": datetime.now(timezone.utc).isoformat(), "status": 200,
                "complete_body": True, "wire_bytes": len(bad_body),
                "wire_sha256": s.digest(bad_body), "bytes": len(bad_body),
                "sha256": s.digest(bad_body), "content_encoding": None,
                "content_length": len(bad_body), "error_code": None, "error_type": None})):
            with self.assertRaisesRegex(ValueError, "producer_metadata_changed"):
                s.stage_ferrous_metal_mines(self.storage, archive_path=self.archive,
                                            archive_receipt_path=self.receipt)

    def test_preserved_metadata_without_receipt_fails(self):
        metadata_body, _ = metadata_capture(self.root)
        with self.assertRaisesRegex(ValueError, "preserved_metadata_requires_original_receipt"):
            s.stage_ferrous_metal_mines(self.storage, archive_path=self.archive,
                                        archive_receipt_path=self.receipt,
                                        metadata_path=metadata_body)


if __name__ == "__main__":
    unittest.main()
