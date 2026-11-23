"""Shared streaming GeoParquet candidate writer: CRS, metadata, row counts."""
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyproj

from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.sources.arcgis_layer import layer_candidate_parquet
from dagster_hifld.sources.geoparquet_candidate import (
    source_crs_projjson, stream_geoparquet_candidate,
)
from dagster_hifld.validation import sha256_file


def _fields():
    return [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
        {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
        {"name": "Feature_ID", "type": "esriFieldTypeString", "alias": "Feature_ID", "domain": None, "length": 254},
    ]


def _features(count):
    features = []
    for oid in range(1, count + 1):
        features.append({
            "attributes": {"OBJECTID": oid, "Feature_ID": f"F-{oid}"},
            "geometry": {"rings": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]},
        })
    return features


def _plain_batches(rows_per_batch):
    def batches(total):
        for start in range(0, total, rows_per_batch):
            stop = min(start + rows_per_batch, total)
            yield pa.record_batch(
                [pa.array(list(range(start, stop)), type=pa.int64()),
                 pa.array([f"v-{i}" for i in range(start, stop)], type=pa.string())],
                names=["a", "b"])
    return batches


class SourceCrsProjjsonTests(unittest.TestCase):
    def test_wkid_body_matches_the_actual_crs_not_wgs84(self):
        nad83 = source_crs_projjson(4269)
        self.assertEqual(nad83["id"], {"authority": "EPSG", "code": 4269})
        self.assertEqual(nad83["type"], "GeographicCRS")
        self.assertNotEqual(nad83.get("name"), "WGS 84")
        self.assertEqual(pyproj.CRS.from_user_input(nad83).to_epsg(), 4269)

    def test_projected_wkid_body(self):
        mercator = source_crs_projjson(3857)
        self.assertEqual(mercator["id"], {"authority": "EPSG", "code": 3857})
        self.assertEqual(mercator["type"], "ProjectedCRS")

    def test_wkt_input(self):
        body = source_crs_projjson(pyproj.CRS.from_epsg(4269).to_wkt())
        self.assertEqual(body["id"], {"authority": "EPSG", "code": 4269})

    def test_4326_body_is_canonical_pyproj_output(self):
        body = source_crs_projjson(4326)
        self.assertEqual(body, pyproj.CRS.from_epsg(4326).to_json_dict())
        self.assertEqual(body["id"], {"authority": "EPSG", "code": 4326})


class LayerCandidateCrsTests(unittest.TestCase):
    def test_non_4326_layer_crs_comes_from_the_actual_wkid(self):
        with tempfile.TemporaryDirectory() as directory:
            path, digest, rows = layer_candidate_parquet(_fields(), _features(3), 4269, Path(directory))
            self.assertEqual(rows, 3)
            table = pq.read_table(path)
            geo = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
            crs = geo["columns"]["geometry"]["crs"]
            self.assertEqual(crs["id"], {"authority": "EPSG", "code": 4269})
            self.assertNotEqual(crs.get("name"), "WGS 84")
            self.assertEqual(pyproj.CRS.from_user_input(crs).to_epsg(), 4269)
            self.assertEqual(digest, sha256_file(path))

    def test_4326_layer_crs_stays_wgs84(self):
        with tempfile.TemporaryDirectory() as directory:
            path, _digest, _rows = layer_candidate_parquet(_fields(), _features(2), 4326, Path(directory))
            table = pq.read_table(path)
            _primary, _column, crs = geometry_metadata(table.schema)
            self.assertEqual(crs.to_epsg(), 4326)


class StreamingWriterTests(unittest.TestCase):
    def test_returns_row_count_equal_to_rows_written(self):
        with tempfile.TemporaryDirectory() as directory:
            schema = pa.schema([pa.field("a", pa.int64(), nullable=True),
                                pa.field("b", pa.string(), nullable=True)])
            path, digest, rows = stream_geoparquet_candidate(
                schema, None, _plain_batches(250)(1024), Path(directory) / "plain.parquet")
            self.assertEqual(rows, 1024)
            self.assertEqual(digest, sha256_file(path))
            table = pq.read_table(path)
            self.assertEqual(table.num_rows, 1024)
            self.assertEqual(pq.ParquetFile(path).metadata.num_rows, 1024)
            self.assertEqual(table.column("a").to_pylist(), list(range(1024)))
            self.assertEqual(table.column("b").to_pylist(), [f"v-{i}" for i in range(1024)])

    def test_geometry_free_table_gets_no_geo_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            schema = pa.schema([pa.field("a", pa.int64(), nullable=True)])
            path, _digest, rows = stream_geoparquet_candidate(
                schema, None,
                iter([pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], names=["a"])]),
                Path(directory) / "plain.parquet")
            self.assertEqual(rows, 3)
            table = pq.read_table(path)
            self.assertNotIn(b"geo", table.schema.metadata or {})
            self.assertEqual(table.column("a").to_pylist(), [1, 2, 3])

    def test_geometry_candidate_is_valid_geoparquet_and_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            first = layer_candidate_parquet(_fields(), _features(507), 4326, Path(directory))
            path, digest, rows = first
            self.assertEqual(rows, 507)
            table = pq.read_table(path)
            primary, _column, crs = geometry_metadata(table.schema)
            self.assertEqual(primary, "geometry")
            self.assertEqual(crs.to_epsg(), 4326)
            self.assertEqual(digest, sha256_file(path))
            self.assertEqual(layer_candidate_parquet(_fields(), _features(507), 4326, Path(directory)), first)

    def test_geometry_contract_requires_crs_and_types(self):
        with tempfile.TemporaryDirectory() as directory:
            schema = pa.schema([pa.field("geometry", pa.binary(), nullable=True)])
            batches = iter([pa.record_batch([pa.array([b"x"], type=pa.binary())], names=["geometry"])])
            with self.assertRaisesRegex(ValueError, "candidate_geometry_contract_incomplete"):
                stream_geoparquet_candidate(schema, None, batches, Path(directory) / "c.parquet",
                                            geometry_column="geometry", geometry_types=["Polygon"])
            batches = iter([pa.record_batch([pa.array([b"x"], type=pa.binary())], names=["geometry"])])
            with self.assertRaisesRegex(ValueError, "candidate_geometry_contract_incomplete"):
                stream_geoparquet_candidate(schema, 4326, batches, Path(directory) / "c.parquet",
                                            geometry_column="geometry", geometry_types=[])

    def test_preexisting_schema_metadata_survives_with_geo(self):
        with tempfile.TemporaryDirectory() as directory:
            schema = pa.schema(
                [pa.field("a", pa.int64(), nullable=True),
                 pa.field("geometry", pa.binary(), nullable=True)],
                metadata={b"hifld:source_esri_fields": b'[{"name": "OBJECTID"}]'})
            batches = iter([pa.record_batch(
                [pa.array([1, 2], type=pa.int64()),
                 pa.array([b"g1", b"g2"], type=pa.binary())],
                names=["a", "geometry"])])
            path, _digest, rows = stream_geoparquet_candidate(
                schema, 4326, batches, Path(directory) / "c.parquet",
                geometry_column="geometry", geometry_types=["Point"])
            self.assertEqual(rows, 2)
            table = pq.read_table(path)
        metadata = table.schema.metadata
        self.assertEqual(metadata[b"hifld:source_esri_fields"], b'[{"name": "OBJECTID"}]')
        primary, column, crs = geometry_metadata(table.schema)
        self.assertEqual(primary, "geometry")
        self.assertEqual(column["geometry_types"], ["Point"])
        self.assertEqual(crs.to_epsg(), 4326)

    def test_mismatched_batch_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            schema = pa.schema([pa.field("a", pa.int64(), nullable=True)])
            batches = iter([pa.record_batch([pa.array([1.0], type=pa.float64())], names=["a"])])
            with self.assertRaisesRegex(ValueError, "candidate_batch_schema_mismatch"):
                stream_geoparquet_candidate(schema, None, batches, Path(directory) / "c.parquet")


if __name__ == "__main__":
    unittest.main()
