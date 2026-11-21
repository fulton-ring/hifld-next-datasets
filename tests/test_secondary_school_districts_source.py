from __future__ import annotations

import io
import json
import re
import tempfile
import unittest
import zipfile
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import secondary_school_districts as source
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.sources.capture_common import CODE_IDENTITY_PACKAGED, processing_identity


def _fixture_table(rows=2):
    records = [
        ("04", "00082", "0400082", "9600000US0400082", "Colorado River Union High School District", "00",
         "09", "12", "G5410", None, "E",
         2711182208.0, 74627638.0, "+35.1052137", "-114.4678343",
         shapely.to_wkb(shapely.MultiPolygon([shapely.box(-114.5, 35.0, -114.4, 35.1)]))),
        ("04", "00450", "0400450", "9600000US0400450", "Agua Fria Union High School District", "00",
         "09", "12", "G5410", "A", "E",
         244749261.0, 709795.0, "+33.4820954", "-112.4096789",
         shapely.to_wkb(shapely.MultiPolygon([shapely.box(-112.5, 33.4, -112.4, 33.5)]))),
    ]
    for i in range(2, rows):
        lea = f"{1000 + i:05d}"
        geoid = f"04{100 + i:04d}"
        records.append((
            "04", lea, geoid, f"9600000US{geoid}", f"Synthetic Secondary District {i}", "00",
            "09", "12", "G5410", None, "E",
            89511446.0 + i, 476695.0 + i, "+44.1829927", "-72.4750489",
            shapely.to_wkb(shapely.MultiPolygon([shapely.box(-72.5 - i, 44.1, -72.4 - i, 44.2)])),
        ))
    records = records[:rows]
    columns = [
        pa.array([r[0] for r in records], type=pa.string()),
        pa.array([r[1] for r in records], type=pa.string()),
        pa.array([r[2] for r in records], type=pa.string()),
        pa.array([r[3] for r in records], type=pa.string()),
        pa.array([r[4] for r in records], type=pa.string()),
        pa.array([r[5] for r in records], type=pa.string()),
        pa.array([r[6] for r in records], type=pa.string()),
        pa.array([r[7] for r in records], type=pa.string()),
        pa.array([r[8] for r in records], type=pa.string()),
        pa.array([r[9] for r in records], type=pa.string()),
        pa.array([r[10] for r in records], type=pa.string()),
        pa.array([r[11] for r in records], type=pa.float64()),
        pa.array([r[12] for r in records], type=pa.float64()),
        pa.array([r[13] for r in records], type=pa.string()),
        pa.array([r[14] for r in records], type=pa.string()),
        pa.array([r[15] for r in records], type=pa.binary()),
    ]
    return pa.Table.from_arrays(columns, names=[name for name, _ in source.EXPECTED_SOURCE_FIELDS])


class SecondarySchoolDistrictsSourceTests(unittest.TestCase):
    def test_config_discovers_the_current_school_product(self):
        self.assertEqual(source.DATASET_SLUG, "secondary-school-districts")
        self.assertEqual(source.LAYER_NAME, "School_District_Secondary")
        self.assertIsNone(source.CONFIG.expected_count)
        self.assertEqual(source.CONFIG.feature_count_range, (300, 700))
        self.assertTrue(source.CONFIG.discover_current)
        self.assertEqual(source.CONFIG.issue_number, 307)
        self.assertEqual(source.CONFIG.source_url, "")
        self.assertEqual(source.CONFIG.source_vintage, "")
        self.assertEqual(source.SOURCE_HOST, "www2.census.gov")
        self.assertEqual([name for name, _ in source.EXPECTED_SOURCE_FIELDS],
                         ["STATEFP", "SCSDLEA", "GEOID", "GEOIDFQ", "NAME", "LSAD", "LOGRADE",
                          "HIGRADE", "MTFCC", "SDTYP", "FUNCSTAT", "ALAND", "AWATER",
                          "INTPTLAT", "INTPTLON", "SHAPE"])
        self.assertEqual(source.EXPECTED_OUTPUT_FIELDS,
                         ["OBJECTID", "STATEFP", "SCSDLEA", "GEOID", "GEOIDFQ", "NAME", "LSAD",
                          "LOGRADE", "HIGRADE", "MTFCC", "SDTYP", "FUNCSTAT", "ALAND", "AWATER",
                          "INTPTLAT", "INTPTLON", "geometry"])
        self.assertIsNone(re.search(r"TGRGDB\d{2}|tlgdb_\d{4}",
                                    Path(source.__file__).read_text(encoding="utf-8")))

    def test_build_geoparquet_preserves_all_attributes_ids_and_native_wkb(self):
        source_table = _fixture_table()
        ids = [1, 2]
        body, schema = source._build_geoparquet(
            source_table, ids, config=replace(source.CONFIG, expected_count=2)
        )
        result = pq.read_table(pa.BufferReader(body))
        self.assertEqual(result.num_rows, 2)
        self.assertEqual(result.column_names, source.EXPECTED_OUTPUT_FIELDS)
        self.assertEqual(result.column("OBJECTID").to_pylist(), ids)
        self.assertEqual(result.column("STATEFP").to_pylist(), ["04", "04"])
        self.assertEqual(result.column("SCSDLEA").to_pylist(), ["00082", "00450"])
        self.assertEqual(result.column("GEOIDFQ").to_pylist(),
                         ["9600000US0400082", "9600000US0400450"])
        self.assertEqual(result.column("MTFCC").to_pylist(), ["G5410", "G5410"])
        self.assertEqual(result.column("SDTYP").to_pylist(), [None, "A"])
        self.assertEqual(result.column("geometry").to_pylist(), source_table.column("SHAPE").to_pylist())
        self.assertEqual(schema["geometry_bbox"], [-114.5, 33.4, -112.4, 35.1])
        geo = json.loads(result.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["MultiPolygon"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 4269})

    def test_build_geoparquet_rejects_geometry_and_population_drift(self):
        cases = {
            "source_geometry_wkb_invalid": lambda table: table.set_column(
                table.column_names.index("SHAPE"), "SHAPE",
                pa.array([b"\x00\x01not-wkb",
                          shapely.to_wkb(shapely.MultiPolygon([shapely.box(-112.5, 33.4, -112.4, 33.5)]))],
                         type=pa.binary())),
            "source_geometry_type_or_validity_drift": lambda table: table.set_column(
                table.column_names.index("SHAPE"), "SHAPE",
                pa.array([shapely.to_wkb(shapely.box(-114.5, 35.0, -114.4, 35.1)),
                          shapely.to_wkb(shapely.MultiPolygon([shapely.box(-112.5, 33.4, -112.4, 33.5)]))],
                         type=pa.binary())),
            "source_geometry_coordinate_bounds_drift": lambda table: table.set_column(
                table.column_names.index("SHAPE"), "SHAPE",
                pa.array([shapely.to_wkb(shapely.MultiPolygon([shapely.box(200.0, 35.0, 200.1, 35.1)])),
                          shapely.to_wkb(shapely.MultiPolygon([shapely.box(-112.5, 33.4, -112.4, 33.5)]))],
                         type=pa.binary())),
        }
        for label, mutate in cases.items():
            with self.subTest(change=label), \
                    self.assertRaisesRegex(ValueError, label):
                source._build_geoparquet(mutate(_fixture_table()), [1, 2],
                                         config=replace(source.CONFIG, expected_count=2))
        table = _fixture_table()
        with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
            source._build_geoparquet(table, [1, 2], config=replace(source.CONFIG, expected_count=3))
        with self.assertRaisesRegex(ValueError, "native_output_duplicate_ids"):
            source._build_geoparquet(table, [2, 2], config=replace(source.CONFIG, expected_count=2))

    def test_parity_rejects_attribute_value_loss(self):
        config = replace(source.CONFIG, expected_count=2)
        table = _fixture_table()
        body, _schema = source._build_geoparquet(table, [1, 2], config=config)
        renamed = table.set_column(
            table.column_names.index("NAME"), "NAME",
            pa.array(["Tampered District", "Agua Fria Union High School District"], type=pa.string()))
        with self.assertRaisesRegex(ValueError, "native_output_attribute_parity_drift:NAME"):
            source._shared._assert_full_parity(renamed, [1, 2], body, config=config)
        reidentified = table.set_column(
            table.column_names.index("GEOID"), "GEOID",
            pa.array(["0400999", "0400450"], type=pa.string()))
        with self.assertRaisesRegex(ValueError, "native_output_attribute_parity_drift:GEOID"):
            source._shared._assert_full_parity(reidentified, [1, 2], body, config=config)
        altered = table.set_column(
            table.column_names.index("SCSDLEA"), "SCSDLEA",
            pa.array(["00082", "99999"], type=pa.string()))
        with self.assertRaisesRegex(ValueError, "native_output_attribute_parity_drift:SCSDLEA"):
            source._shared._assert_full_parity(altered, [1, 2], body, config=config)

    def test_archive_validation_requires_one_intact_geodatabase(self):
        valid = io.BytesIO()
        with zipfile.ZipFile(valid, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("school.gdb/a00000001.gdbtable", b"complete-table")
        source._validate_archive(valid.getvalue())

        split = io.BytesIO()
        with zipfile.ZipFile(split, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("school.gdb/a00000001.gdbtable", b"one")
            archive.writestr("other.gdb/a00000001.gdbtable", b"two")
        with self.assertRaisesRegex(ValueError, "source_archive_geodatabase_roster_drift"):
            source._validate_archive(split.getvalue())

        corrupt = io.BytesIO()
        with zipfile.ZipFile(corrupt, "w", zipfile.ZIP_STORED) as archive:
            archive.writestr("school.gdb/a00000001.gdbtable", b"complete-table")
        raw = corrupt.getvalue()
        marker = raw.index(b"complete-table") + 4
        body = bytearray(raw)
        body[marker] ^= 0xFF
        with self.assertRaisesRegex(ValueError, "source_archive_crc_failure"):
            source._validate_archive(bytes(body))

    def test_packaged_processing_identity_validates_without_git(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "src/dagster_hifld"
            package.mkdir(parents=True)
            (package / "__init__.py").write_text("# packaged identity fixture\n")
            (root / "pyproject.toml").write_text("[project]\nname='identity-fixture'\n")
            (root / "uv.lock").write_text("version = 1\n")
            identity = processing_identity(root)
            self.assertEqual(identity["code_identity_scheme"], CODE_IDENTITY_PACKAGED)
            self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
            validate_construction_code_identity(root, identity["code_commit"], identity["files"], identity=identity)

    def test_failed_transfer_retains_partial_wire_bytes_and_receipt(self):
        class Storage:
            def __init__(self):
                self.objects = {}

            def write(self, dataset, file, version, key, body):
                self.objects[key] = body

        class Response:
            request = SimpleNamespace(url=SimpleNamespace(host=source.SOURCE_HOST))
            headers = {}

        class Client:
            def stream(self, *args, **kwargs):
                class Context:
                    def __enter__(self):
                        return Response()
                    def __exit__(self, *args):
                        return False
                return Context()

        for error in (OSError("connection interrupted"), ValueError("capture budget exceeded")):
            storage = Storage()

            def interrupted(_response, **kwargs):
                kwargs["retain_wire"](b"partial-wire-body")
                raise error

            with patch.object(source._shared, "capture_http_response", side_effect=interrupted):
                with self.assertRaises(ValueError):
                    source._capture_archive(storage, "vpartial", Client())
            partial = storage.objects["raw/source_archive.partial.zip"]
            receipt = json.loads(storage.objects["raw/source_archive.failure_receipt.json"])
            self.assertEqual(partial, b"partial-wire-body")
            self.assertEqual(receipt["wire_bytes_retained"], len(partial))
            self.assertEqual(receipt["wire_sha256"], source._sha256(partial))
            self.assertEqual(receipt["partial_wire_key"], "raw/source_archive.partial.zip")


if __name__ == "__main__":
    unittest.main()
