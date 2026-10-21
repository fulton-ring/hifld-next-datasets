import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from dagster import DefaultScheduleStatus

from dagster_hifld.assets.census_tracts import (
    census_tracts_draft_schedule,
    census_tracts_source_complete,
)
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.definitions import defs
from dagster_hifld.sources import census_tracts as source


class CensusTractsTests(unittest.TestCase):
    def test_registered_candidate_and_stopped_refresh(self):
        asset_key = f"ingest/{source.DATASET_SLUG}/{source.FILE_SLUG}"
        registered = {key.to_user_string() for definition in hifld_ingest_assets for key in definition.keys}
        self.assertIn(asset_key, registered)
        graph = defs.resolve_asset_graph()
        self.assertIn((asset_key, "census_tracts_source_complete"), {
            (key.asset_key.to_user_string(), key.name) for key in graph.asset_check_keys
        })
        self.assertEqual(census_tracts_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(
            defs.resolve_schedule_def("census_tracts_draft_refresh_schedule").default_status,
            DefaultScheduleStatus.STOPPED,
        )

    def test_source_check_requires_exact_full_native_candidate(self):
        candidate = {
            "feature_count": source.SOURCE_FEATURE_COUNT,
            "source_population_complete": True,
            "source_archive_bytes": source.SOURCE_BYTES,
            "source_archive_sha256": source.SOURCE_SHA256,
            "source_id_sha256": "1" * 64,
            "native_parquet_sha256": "2" * 64,
            "native_value_parity": True,
            "code_identity_scheme": "sha256-packaged-dagster-hifld-v1",
        }
        self.assertTrue(census_tracts_source_complete(candidate).passed)
        self.assertFalse(census_tracts_source_complete(dict(candidate, feature_count=source.SOURCE_FEATURE_COUNT - 1)).passed)
        self.assertFalse(census_tracts_source_complete(dict(candidate, native_value_parity=False)).passed)

    def test_stream_preserves_all_source_values_fids_wkb_and_native_crs(self):
        geoms = [
            shapely.to_wkb(shapely.MultiPolygon([shapely.box(-86.5, 32.4, -86.4, 32.5)])),
            shapely.to_wkb(shapely.MultiPolygon([shapely.Polygon([
                (-86.4, 32.5), (-86.3, 32.6), (-86.4, 32.6), (-86.3, 32.5), (-86.4, 32.5),
            ])])),
        ]
        batch = pa.record_batch([
            pa.array([1, 2], type=pa.int64()),
            pa.array(["01001020100", "01001020200"]),
            pa.array(["1400000US01001020100", "1400000US01001020200"]),
            pa.array(["Census Tract 201", "Census Tract 202"]),
            pa.array([9825303.0, 3320818.0], type=pa.float64()),
            pa.array([28435.0, 5669.0], type=pa.float64()),
            pa.array(["+32.4819731", "+32.4757580"]),
            pa.array(["-86.4915648", "-86.4724678"]),
            pa.array(geoms, type=pa.binary()),
        ], names=list(source.EXPECTED_NAMES))

        class Reader:
            schema = batch.schema
            def __iter__(self):
                return iter([batch])

        @contextmanager
        def open_arrow(*args, **kwargs):
            yield ({"crs": "EPSG:4269", "geometry_type": "MultiPolygon", "fid_column": "OBJECTID"}, Reader())

        with tempfile.TemporaryDirectory() as temp, \
             patch.object(source.pyogrio, "read_info", return_value={}), \
             patch.object(source.pyogrio, "open_arrow", side_effect=open_arrow), \
             patch.object(source, "SOURCE_FEATURE_COUNT", 2):
            output_path = Path(temp) / "tracts.parquet"
            result = source._stream_layer("unused", output_path)
            output = pq.read_table(output_path)
            self.assertEqual(output.num_rows, 2)
            self.assertEqual(output.column_names, list(source.EXPECTED_NAMES))
            for index, name in enumerate(source.EXPECTED_NAMES):
                self.assertEqual(output.column(name).to_pylist(), batch.column(index).to_pylist())
            geo = __import__("json").loads(output.schema.metadata[b"geo"])
            self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["MultiPolygon"])
            self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"], {"authority": "EPSG", "code": 4269})
            self.assertTrue(result["native_objectid_sha256"])
            self.assertEqual(result["feature_count"], 2)
            self.assertEqual(result["invalid_geometries"], 1)

    def test_native_gdb_definition_captures_oid_width_and_tract_field_widths(self):
        fields = "".join(
            f"<GPFieldInfoEx><Name>{name}</Name><FieldType>{field_type}</FieldType>"
            f"<Length>{length}</Length><IsNullable>{nullable}</IsNullable></GPFieldInfoEx>"
            for name, (field_type, length, nullable) in source.EXPECTED_NATIVE_FIELDS.items()
        )
        xml = ("<DEFeatureClassInfo><OIDFieldName>OBJECTID</OIDFieldName><HasZ>false</HasZ><HasM>false</HasM>"
               "<ShapeType>esriGeometryPolygon</ShapeType><GPFieldInfoExs>" + fields + "</GPFieldInfoExs></DEFeatureClassInfo>")
        result = source._parse_layer_definition(xml)
        self.assertEqual(result["source_oid_field"], "OBJECTID")
        with self.assertRaisesRegex(source.CensusTractsError, "physical_type_or_width_drift:GEOID"):
            source._parse_layer_definition(xml.replace("<Length>11</Length>", "<Length>12</Length>", 1))

    def test_source_manifest_keeps_vintage_explicit_and_release_unapproved(self):
        identity = {
            "code_commit": "packaged-sha256:" + "a" * 64,
            "code_identity_scheme": "sha256-packaged-dagster-hifld-v1",
        }
        manifest = source._manifest(
            identity,
            {"parquet_bytes": 1234, "parquet_sha256": "b" * 64},
            {"observed_at": "2026-10-21T17:04:58.415344+00:00"},
            {"source_shape_type": "esriGeometryPolygon"},
        )
        self.assertIs(manifest["approved"], False)
        self.assertIn("2024", manifest["title"])
        self.assertIn("release_date_unknown_reason", manifest["inputs"][0])
        self.assertEqual(manifest["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"][0]["bytes"], 1234)
        self.assertNotIn("promotion_allowed", manifest)


if __name__ == "__main__":
    unittest.main()
