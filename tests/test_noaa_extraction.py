"""Draft NOAA extraction behavior; synthetic snapshots grant no real acceptance."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.noaa_extract import (
    noaa_reference_fingerprints, stage_noaa_candidate, validate_noaa_capture,
)
from dagster_hifld.resources import StagingStorageResource


def fixture():
    types = {"OBJECTID": "esriFieldTypeOID", "BOUND_ID": "esriFieldTypeString", "REGION": "esriFieldTypeString", "FEAT_TYPE": "esriFieldTypeString", "UNILATERAL": "esriFieldTypeDouble", "PUB_DATE": "esriFieldTypeDate", "SHAPE": "esriFieldTypeGeometry"}
    layer = {"geometryType": "esriGeometryPolyline", "maxRecordCount": 1000, "fields": [{"name": k, "type": v} for k, v in types.items()]}
    features = {"geometryType": "esriGeometryPolyline", "spatialReference": {"wkid": 4326}, "features": [{"attributes": {"OBJECTID": key, "BOUND_ID": "00" + str(key), "REGION": "test", "FEAT_TYPE": "test", "UNILATERAL": 0.0, "PUB_DATE": None}, "geometry": {"paths": [[[1.0, 2.0], [3.0, 4.0]]]}} for key in [2, 1]]}
    geo = {"type": "FeatureCollection", "features": [{"type": "Feature", "id": f["attributes"]["OBJECTID"], "properties": f["attributes"], "geometry": {"type": "LineString", "coordinates": f["geometry"]["paths"][0]}} for f in features["features"]]}
    ids = {"objectIdFieldName": "OBJECTID", "objectIds": [1, 2]}
    objects = {"layer": layer, "features": features, "geojson": geo, "ids_before": ids, "ids_after": ids, "count_before": {"count": 2}, "count_after": {"count": 2}}
    captures = {k: json.dumps(v).encode() for k, v in objects.items()}
    kwargs = {"expected_fields": types, "expected_fingerprints": noaa_reference_fingerprints(layer, features), "expected_count": 2}
    return captures, kwargs


class NoaaExtractionTests(unittest.TestCase):
    def test_native_types_ids_vertices_and_staging_are_lossless_idempotent(self):
        captures, kwargs = fixture()
        table, report = validate_noaa_capture(captures, **kwargs)
        self.assertEqual(table.column_names, list(kwargs["expected_fields"]))
        self.assertEqual(table.schema.field("UNILATERAL").type, pa.float64())
        self.assertEqual(table.schema.field("PUB_DATE").type, pa.int64())
        self.assertEqual(table["BOUND_ID"].to_pylist(), ["001", "002"])
        self.assertEqual(table["OBJECTID"].to_pylist(), [1, 2])
        self.assertEqual(list(from_wkb(table["SHAPE"][0].as_py()).coords), [(1, 2), (3, 4)])
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            args = {"staging": storage, "dataset_slug": "synthetic", "file_slug": "synthetic", "captures": captures, "receipts": [], **kwargs}
            first = stage_noaa_candidate(**args)
            second = stage_noaa_candidate(**args)
            self.assertEqual(first, second)
            self.assertFalse(first["promotion_approved"])
            self.assertTrue(pq.read_table(Path(directory) / first["candidate_key"]).equals(table))
            raw = Path(directory) / "synthetic" / "synthetic" / first["version"] / "raw"
            for name, body in captures.items():
                self.assertEqual((raw / (name + ".json")).read_bytes(), body)

    def test_rejects_partial_error_type_id_schema_geometry_and_coverage_changes(self):
        cases = [
            ("features", lambda d: d.update(error={"code": 500})),
            ("features", lambda d: d.update(exceededTransferLimit=True)),
            ("count_after", lambda d: d.update(count=3)),
            ("ids_after", lambda d: d.update(objectIds=[1, 1])),
            ("features", lambda d: d["features"][0]["attributes"].update(BOUND_ID="001")),
            ("features", lambda d: d["features"][0]["attributes"].update(OBJECTID="2")),
            ("layer", lambda d: d["fields"].append({"name": "surprise", "type": "esriFieldTypeString"})),
            ("features", lambda d: d["features"][0]["geometry"]["paths"][0][0].__setitem__(0, 7.0)),
            ("features", lambda d: d["features"][0]["attributes"].update(REGION="changed")),
            ("features", lambda d: d["features"][0]["attributes"].update(PUB_DATE=1.5)),
        ]
        for name, mutate in cases:
            with self.subTest(name=name, mutate=mutate):
                captures, kwargs = fixture()
                obj = json.loads(captures[name]); mutate(obj); captures[name] = json.dumps(obj).encode()
                with tempfile.TemporaryDirectory() as directory:
                    with self.assertRaisesRegex(ValueError, "quarantined"):
                        stage_noaa_candidate(staging=StagingStorageResource(local_dir=directory, use_local=True), dataset_slug="synthetic", file_slug="synthetic", captures=captures, receipts=[], **kwargs)
                    self.assertEqual(len(list(Path(directory).rglob("quarantine.json"))), 1)
                    self.assertFalse(list(Path(directory).rglob("*.parquet")))
                    self.assertFalse(list(Path(directory).rglob("*.gpkg")))

    def test_remote_storage_rejected_for_unapproved_draft(self):
        captures, kwargs = fixture()
        with self.assertRaisesRegex(ValueError, "isolated local storage"):
            stage_noaa_candidate(staging=StagingStorageResource(use_local=False, bucket="not-real"), dataset_slug="synthetic", file_slug="synthetic", captures=captures, receipts=[], **kwargs)


if __name__ == "__main__":
    unittest.main()
