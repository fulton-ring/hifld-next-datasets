from __future__ import annotations

import io
import json
import unittest
import zipfile
import tempfile
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import secondary_school_districts as source
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.sources.epa_frs_common import CODE_IDENTITY_PACKAGED, processing_identity


class SecondarySchoolDistrictsSourceTests(unittest.TestCase):
    def _table(self):
        fields = [
            pa.array(["0100001", "0200002"], type=pa.string()),
            pa.array(["9600000US0100001", "9600000US0200002"], type=pa.string()),
            pa.array(["District One", "District Two"], type=pa.string()),
            pa.array(["09", "09"], type=pa.string()),
            pa.array(["12", "12"], type=pa.string()),
            pa.array([None, "A"], type=pa.string()),
            pa.array([101.0, 202.0], type=pa.float64()),
            pa.array([3.0, 4.0], type=pa.float64()),
            pa.array(["+40.0000000", "+41.0000000"], type=pa.string()),
            pa.array(["-100.0000000", "-101.0000000"], type=pa.string()),
            pa.array([
                shapely.to_wkb(shapely.MultiPolygon([shapely.box(-101, 39, -100, 40)])),
                shapely.to_wkb(shapely.MultiPolygon([shapely.box(-102, 40, -101, 41)])),
            ], type=pa.binary()),
        ]
        return pa.Table.from_arrays(fields, names=[name for name, _ in source.EXPECTED_SOURCE_FIELDS])

    def test_build_geoparquet_preserves_all_attributes_ids_and_native_wkb(self):
        source_table = self._table()
        ids = [1, 2]
        body, schema = source._build_geoparquet(
            source_table, ids, config=replace(source.CONFIG, expected_count=2)
        )
        result = pq.read_table(pa.BufferReader(body))
        self.assertEqual(result.column_names, source.EXPECTED_OUTPUT_FIELDS)
        self.assertEqual(result.column("OBJECTID").to_pylist(), ids)
        self.assertEqual(result.column("GEOID").to_pylist(), ["0100001", "0200002"])
        self.assertEqual(result.column("SDTYP").to_pylist(), [None, "A"])
        self.assertEqual(result.column("geometry").to_pylist(), source_table.column("SHAPE").to_pylist())
        self.assertEqual(schema["geometry_bbox"], [-102.0, 39.0, -100.0, 41.0])
        self.assertIn('"geometry_types":["MultiPolygon"]', result.schema.metadata[b"geo"].decode())

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
