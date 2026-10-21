"""Native fidelity and drift tests for the USGS QFaults draft capture."""
import hashlib
import io
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.construction_provenance import ConstructionCodeIdentityError, validate_construction_code_identity
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import quaternary_fault_lines as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def sha(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def fixture():
    ids = [1, 3]
    features = []
    for oid, name, x in ((1, "alpha", 2.0), (3, None, 4.0)):
        attributes = {field["name"]: None for field in source.ATTRIBUTE_FIELDS}
        attributes.update({
            "OBJECTID": oid,
            "fault_name": name,
            "fault_id": str(oid),
            "section_id": "",
            "Location": "Test",
            "linetype": "Well Constrained",
            "age": "late Quaternary",
            "mapped_scale": "1:250,000",
            "class": "A",
            "mapped_certainty": "Good",
            "total_fault_length": oid * 10,
            "cooperators": "USGS",
            "last_review": 1458864000000 + oid,
            "fault_url": "https://example.test/fault",
            "symbology": "late Quaternary",
            "ref_id": str(oid),
            "Fault_ID_num": oid,
            "Shape_Length": 10.0,
            "archive_pdf": "https://example.test/report.pdf",
        })
        features.append({
            "attributes": attributes,
            "geometry": {"paths": [[[x, 0.0], [x + 1, 1.0]]],
                         "spatialReference": {"wkid": 3857}},
        })
    layer = {
        "id": 21,
        "name": "National Database",
        "geometryType": "esriGeometryPolyline",
        "fields": source.EXPECTED_FIELDS,
        "maxRecordCount": 2000,
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
        "extent": {"xmin": 2.0, "ymin": 0.0, "xmax": 5.0, "ymax": 1.0,
                   "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
    }
    pages = {"page_000": {"spatialReference": {"wkid": 3857}, "features": features}}
    return ids, layer, pages


class QuaternaryFaultLinesSourceTests(unittest.TestCase):
    def test_packaged_runtime_identity_is_versioned_and_recomputed(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "src/dagster_hifld"
            package.mkdir(parents=True)
            module = package / "runtime.py"
            module.write_text("candidate transform = 1\n")
            (root / "pyproject.toml").write_text("[project]\nname='fixture'\n")
            (root / "uv.lock").write_text("version = 1\n")

            identity = source._processing_identity(root)
            self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
            self.assertEqual(identity["working_tree_status"], "unknown")
            self.assertRegex(identity["code_commit"], r"^packaged-sha256:[0-9a-f]{64}$")
            validate_construction_code_identity(
                root, identity["code_commit"], identity["files"], identity=identity,
            )

            module.write_text("changed transform\n")
            with self.assertRaisesRegex(ConstructionCodeIdentityError, "construction_packaged_file_hash_mismatch"):
                validate_construction_code_identity(
                    root, identity["code_commit"], identity["files"], identity=identity,
                )

    def test_same_version_staging_objects_are_immutable(self):
        with TemporaryDirectory() as directory:
            storage = StagingStorageResource(use_local=True, local_dir=directory, prefix="")
            source.write_immutable(storage, "quaternary-fault-lines/v1/raw/page.body", b"captured bytes")
            source.write_immutable(storage, "quaternary-fault-lines/v1/raw/page.body", b"captured bytes")
            with self.assertRaisesRegex(ValueError, "immutable_staged_object_changed"):
                source.write_immutable(storage, "quaternary-fault-lines/v1/raw/page.body", b"changed bytes")

    def validate_fixture(self, ids, layer, pages, *, nulls=None, bounds=(2.0, 0.0, 5.0, 1.0)):
        features = pages["page_000"]["features"]
        record_sha = sha([feature["attributes"] for feature in features])
        geometry_sha = sha([[feature["attributes"]["OBJECTID"], feature["geometry"]]
                            for feature in features])
        null_counts = {field["name"]: 0 for field in source.ATTRIBUTE_FIELDS}
        for feature in features:
            for name, value in feature["attributes"].items():
                if value is None:
                    null_counts[name] += 1
        with patch.object(source, "EXPECTED_COUNT", 2), \
             patch.object(source, "EXPECTED_ID_SHA", sha(sorted(ids))), \
             patch.object(source, "EXPECTED_SCHEMA_SHA", sha(layer["fields"])), \
             patch.object(source, "EXPECTED_RECORD_SHA", record_sha), \
             patch.object(source, "EXPECTED_GEOMETRY_SHA", geometry_sha), \
             patch.object(source, "EXPECTED_NULL_COUNTS", nulls or null_counts), \
             patch.object(source, "EXPECTED_BOUNDS", bounds):
            return source._validate_population(layer, ids, pages)

    def test_all_source_attributes_and_epoch_values_survive_typed_geoparquet(self):
        ids, layer, pages = fixture()
        features, audit = self.validate_fixture(ids, layer, pages)
        parquet = source._parquet_bytes(features)
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.column("OBJECTID").to_pylist(), [1, 3])
        self.assertEqual(table.schema.field("OBJECTID").type.__str__(), "int64")
        self.assertEqual(table.schema.field("fault_name").type.__str__(), "string")
        self.assertEqual(table.schema.field("last_review").type.__str__(), "int64")
        self.assertEqual(table.column("last_review").to_pylist(), [1458864000001, 1458864000003])
        geoms = [from_wkb(value) for value in table.column("geometry").to_pylist()]
        self.assertEqual([list(geom.geoms[0].coords) for geom in geoms],
                         [[(2.0, 0.0), (3.0, 1.0)], [(4.0, 0.0), (5.0, 1.0)]])
        self.assertEqual(audit["source_vertex_count"], 4)
        self.assertEqual(json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"]["crs"]["id"],
                         {"authority": "EPSG", "code": 3857})

    def test_missing_id_fails_closed(self):
        ids, layer, pages = fixture()
        pages["page_000"]["features"].pop()
        with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
            self.validate_fixture(ids, layer, pages)

    def test_renamed_or_extra_column_fails_closed(self):
        ids, layer, pages = fixture()
        pages["page_000"]["features"][0]["attributes"]["unexpected"] = "x"
        with self.assertRaisesRegex(ValueError, "source_attribute_roster_drift"):
            self.validate_fixture(ids, layer, pages)
        ids, layer, pages = fixture()
        layer["fields"] = layer["fields"][:-1] + layer["fields"][:]
        with self.assertRaisesRegex(ValueError, "source_layer_schema_or_limit_drift"):
            source._validate_metadata({"mapName": source.SERVICE_MAP_NAME}, layer)

    def test_value_and_type_loss_fails_closed(self):
        ids, layer, pages = fixture()
        pages["page_000"]["features"][0]["attributes"]["last_review"] = "2016-03-25"
        with self.assertRaisesRegex(ValueError, "source_date_type_drift"):
            self.validate_fixture(ids, layer, pages)
        ids, layer, pages = fixture()
        pages["page_000"]["features"][0]["attributes"]["total_fault_length"] = 1.5
        with self.assertRaisesRegex(ValueError, "source_integer_type_drift"):
            self.validate_fixture(ids, layer, pages)

    def test_geometry_and_coverage_drift_fails_closed(self):
        ids, layer, pages = fixture()
        pages["page_000"]["features"][0]["geometry"]["paths"][0][0] = [99.0, 99.0]
        with self.assertRaisesRegex(ValueError, "source_geometry_coverage_drift"):
            self.validate_fixture(ids, layer, pages)
        ids, layer, pages = fixture()
        pages["page_000"]["features"][0]["geometry"]["hasZ"] = True
        with self.assertRaisesRegex(ValueError, "source_zm_drift"):
            self.validate_fixture(ids, layer, pages)

    def test_source_population_sha_and_null_drift_fails_closed(self):
        ids, layer, pages = fixture()
        with self.assertRaisesRegex(ValueError, "source_null_count_drift"):
            self.validate_fixture(ids, layer, pages, nulls={field["name"]: 999 for field in source.ATTRIBUTE_FIELDS})
        ids, layer, pages = fixture()
        actual_nulls = {field["name"]: 0 for field in source.ATTRIBUTE_FIELDS}
        for feature in pages["page_000"]["features"]:
            for name, value in feature["attributes"].items():
                if value is None:
                    actual_nulls[name] += 1
        with patch.object(source, "EXPECTED_COUNT", 2), \
             patch.object(source, "EXPECTED_ID_SHA", sha(sorted(ids))), \
             patch.object(source, "EXPECTED_SCHEMA_SHA", sha(layer["fields"])), \
             patch.object(source, "EXPECTED_RECORD_SHA", "0" * 64), \
             patch.object(source, "EXPECTED_GEOMETRY_SHA", "0" * 64), \
             patch.object(source, "EXPECTED_NULL_COUNTS", actual_nulls), \
             patch.object(source, "EXPECTED_BOUNDS", (2.0, 0.0, 5.0, 1.0)):
            with self.assertRaisesRegex(ValueError, "source_record_sha_drift"):
                source._validate_population(layer, ids, pages)


if __name__ == "__main__":
    unittest.main()
