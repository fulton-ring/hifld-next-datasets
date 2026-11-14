"""Focused adapter tests for the Census School_District_Administrative layer."""
from __future__ import annotations

import io
import json
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

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import school_district_administrative_areas as source
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.sources.epa_frs_common import CODE_IDENTITY_PACKAGED, processing_identity


def _fixture_table(rows=2):
    records = [
        ("5000007", "9800000US5000007", "Barre Supervisory Union", "PK", "12", None,
         89511436.0, 476697.0, "+44.1829932", "-72.4750500",
         shapely.to_wkb(shapely.MultiPolygon([shapely.box(-72.5, 44.1, -72.4, 44.2)]))),
        ("5000384", "9800000US5000384", "Rivendell Interstate Supervisory District", "PK", "12", "C",
         203804729.0, 3502277.0, "+43.9385994", "-72.2468459",
         shapely.to_wkb(shapely.MultiPolygon([shapely.box(-72.3, 43.9, -72.2, 44.0)]))),
    ][:rows]
    columns = [
        pa.array([r[0] for r in records], type=pa.string()),
        pa.array([r[1] for r in records], type=pa.string()),
        pa.array([r[2] for r in records], type=pa.string()),
        pa.array([r[3] for r in records], type=pa.string()),
        pa.array([r[4] for r in records], type=pa.string()),
        pa.array([r[5] for r in records], type=pa.string()),
        pa.array([r[6] for r in records], type=pa.float64()),
        pa.array([r[7] for r in records], type=pa.float64()),
        pa.array([r[8] for r in records], type=pa.string()),
        pa.array([r[9] for r in records], type=pa.string()),
        pa.array([r[10] for r in records], type=pa.binary()),
    ]
    return pa.Table.from_arrays(columns, names=[name for name, _ in source.EXPECTED_SOURCE_FIELDS])


class SchoolDistrictAdministrativeAreasSourceTests(unittest.TestCase):
    def test_config_targets_the_official_administrative_layer(self):
        self.assertEqual(source.DATASET_SLUG, "school-district-administrative-areas")
        self.assertEqual(source.LAYER_NAME, "School_District_Administrative")
        self.assertEqual(source.EXPECTED_COUNT, 52)
        self.assertEqual(source.CONFIG.issue_number, 303)
        self.assertEqual(source.CONFIG.source_url,
                         "https://www2.census.gov/geo/tiger/TGRGDB24/tlgdb_2024_a_us_school.gdb.zip")
        self.assertEqual(source.SOURCE_HOST, "www2.census.gov")
        self.assertEqual(source.EXPECTED_OUTPUT_FIELDS,
                         ["OBJECTID", "GEOID", "GEOIDFQ", "NAME", "LOGRADE", "HIGRADE", "SDTYP",
                          "ALAND", "AWATER", "INTPTLAT", "INTPTLON", "geometry"])

    def test_build_geoparquet_preserves_all_attributes_ids_and_native_wkb(self):
        source_table = _fixture_table()
        ids = [1, 2]
        body, schema = source._build_geoparquet(
            source_table, ids, config=replace(source.CONFIG, expected_count=2))
        result = pq.read_table(pa.BufferReader(body))
        self.assertEqual(result.num_rows, 2)
        self.assertEqual(result.column_names, source.EXPECTED_OUTPUT_FIELDS)
        self.assertEqual(result.column("OBJECTID").to_pylist(), ids)
        self.assertEqual(result.column("GEOIDFQ").to_pylist(),
                         ["9800000US5000007", "9800000US5000384"])
        self.assertEqual(result.column("SDTYP").to_pylist(), [None, "C"])
        self.assertEqual(result.column("geometry").to_pylist(), source_table.column("SHAPE").to_pylist())
        self.assertEqual(schema["geometry_bbox"], [-72.5, 43.9, -72.2, 44.2])
        geo = json.loads(result.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["MultiPolygon"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 4269})

    def test_build_geoparquet_rejects_geometry_and_population_drift(self):
        cases = {
            "source_geometry_wkb_invalid": lambda table: table.set_column(
                table.column_names.index("SHAPE"), "SHAPE",
                pa.array([b"\x00\x01not-wkb",
                          shapely.to_wkb(shapely.MultiPolygon([shapely.box(-72.3, 43.9, -72.2, 44.0)]))],
                         type=pa.binary())),
            "source_geometry_type_or_validity_drift": lambda table: table.set_column(
                table.column_names.index("SHAPE"), "SHAPE",
                pa.array([shapely.to_wkb(shapely.box(-72.5, 44.1, -72.4, 44.2)),
                          shapely.to_wkb(shapely.MultiPolygon([shapely.box(-72.3, 43.9, -72.2, 44.0)]))],
                         type=pa.binary())),
            "source_geometry_coordinate_bounds_drift": lambda table: table.set_column(
                table.column_names.index("SHAPE"), "SHAPE",
                pa.array([shapely.to_wkb(shapely.MultiPolygon([shapely.box(200.0, 44.1, 200.1, 44.2)])),
                          shapely.to_wkb(shapely.MultiPolygon([shapely.box(-72.3, 43.9, -72.2, 44.0)]))],
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
            pa.array(["Tampered Union", "Rivendell Interstate Supervisory District"], type=pa.string()))
        with self.assertRaisesRegex(ValueError, "native_output_attribute_parity_drift:NAME"):
            source._assert_full_parity(renamed, [1, 2], body, config=config)
        reidentified = table.set_column(
            table.column_names.index("GEOID"), "GEOID",
            pa.array(["5000999", "5000384"], type=pa.string()))
        with self.assertRaisesRegex(ValueError, "native_output_attribute_parity_drift:GEOID"):
            source._assert_full_parity(reidentified, [1, 2], body, config=config)
        altered = table.set_column(
            table.column_names.index("INTPTLON"), "INTPTLON",
            pa.array(["-072.4750500", "-072.2468459"], type=pa.string()))
        with self.assertRaisesRegex(ValueError, "native_output_attribute_parity_drift:INTPTLON"):
            source._assert_full_parity(altered, [1, 2], body, config=config)

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

    def test_replay_requires_receipt_match_and_valid_receipt_bytes(self):
        with self.assertRaisesRegex(ValueError, "retained_capture_receipt_invalid"):
            source.replay_retained_capture(StagingStorageResource(local_dir="/tmp/never"), "v1",
                                           b"archive", b"not-json")
        archive = _fixture_table()
        receipt = {"raw_sha256": "0" * 64, "wire_bytes_retained": 3, "raw_archive_key": "raw/x"}
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory)
            with self.assertRaisesRegex(ValueError, "retained_capture_archive_receipt_mismatch"):
                source.replay_retained_capture(storage, "v1", b"abc", json.dumps(receipt).encode())

    def test_local_only_asset_and_registry_registration(self):
        from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
        from dagster_hifld.assets.school_district_administrative_areas import (
            census_school_district_administrative_areas,
            school_district_administrative_areas_draft_refresh_schedule,
        )
        from dagster_hifld.definitions import defs

        self.assertEqual(sum(asset is census_school_district_administrative_areas
                             for asset in hifld_ingest_assets), 1)
        job = defs.resolve_job_def("school_district_administrative_areas_draft_refresh")
        self.assertIn("ingest__school_district_administrative_areas__school_district_administrative_areas",
                      job.graph.node_names())
        self.assertIn(school_district_administrative_areas_draft_refresh_schedule, defs.schedules)
        self.assertEqual(
            school_district_administrative_areas_draft_refresh_schedule.default_status.value, "STOPPED")

    def test_asset_refuses_remote_or_published_staging(self):
        from dagster import build_asset_context
        from dagster_hifld.assets.school_district_administrative_areas import (
            census_school_district_administrative_areas,
        )

        fn = census_school_district_administrative_areas.op.compute_fn.decorated_fn
        with self.assertRaisesRegex(ValueError, "draft_requires_isolated_local_staging"):
            fn(build_asset_context(), PublishedStorageResource(use_local=True, local_dir="/tmp/x"))
        with self.assertRaisesRegex(ValueError, "draft_requires_isolated_local_staging"):
            fn(build_asset_context(),
               StagingStorageResource(bucket="remote", use_local=False, local_dir="/tmp/x"))


if __name__ == "__main__":
    unittest.main()
