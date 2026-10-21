"""Focused regression for the USACE FUDS public properties draft pipeline.

Covers native parquet preservation plus every drift branch of the capture
validator: missing/renamed/dropped source IDs, value/type loss, unexpected
columns, field-schema drift, geometry/coverage drift, null-geometry retention,
and incomplete or drifting source responses. The validator accepts injected
expectations so tests exercise the same branches as production with a small
synthetic population and no network traffic.
"""
import copy
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster import DagsterInstance, materialize

from dagster_hifld.assets.fuds_public_properties import (
    usace_fuds_public_properties,
    usace_fuds_public_properties_complete,
)
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fuds_public_properties import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_LAYER_ROSTER,
    FILE_SLUG,
    LAYER_NAME,
    PAGE_SIZE,
    SOURCE_DESCRIPTION,
    _canonical,
    _parquet_bytes,
    _plain_field,
    _sha,
    _validate_captures,
)

IDS = [11, 12, 13]
PROPERTY_IDS = ("A04MS0001", "0010000007", "B02MS0003")
COORDS = ((0.0, 0.0), (10.5, -20.25), (100.0, 100.0))


def _fields():
    return [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "length": 4,
         "nullable": False, "domain": None},
        {"name": "DODFUDSPROPERTYIDPK", "type": "esriFieldTypeString", "alias": "DODFUDSPROPERTYIDPK",
         "length": 40, "nullable": True, "domain": None},
        {"name": "SE_ANNO_CAD_DATA", "type": "esriFieldTypeBlob", "alias": "SE_ANNO_CAD_DATA",
         "length": 0, "nullable": True, "domain": None},
        {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE",
         "nullable": True, "domain": None},
    ]


def _features(ids=IDS, null_indexes=()):
    features = []
    for index, oid in enumerate(ids):
        property_id = PROPERTY_IDS[index] if index < len(PROPERTY_IDS) else f"P{oid:09d}"
        coords = COORDS[index] if index < len(COORDS) else (float(index), -float(index))
        geometry = None if index in null_indexes else {"x": coords[0], "y": coords[1]}
        features.append({
            "attributes": {"OBJECTID": oid, "DODFUDSPROPERTYIDPK": property_id,
                           "SE_ANNO_CAD_DATA": None},
            "geometry": geometry,
        })
    return features


def _documents(ids, features, fields, count):
    ids = list(ids)
    features = copy.deepcopy(features)
    fields = copy.deepcopy(fields)
    layer = {
        "id": 1, "name": LAYER_NAME, "description": SOURCE_DESCRIPTION,
        "geometryType": "esriGeometryPoint", "hasZ": False, "hasM": False,
        "advancedQueryCapabilities": {"supportsPagination": True},
        "maxRecordCount": 2000, "spatialReference": {"latestWkid": 4326},
        "fields": fields,
    }
    service = {
        "mapName": "FUDS Data",
        "copyrightText": "US Army Corps of Engineers",
        "layers": [{"id": layer_id, "name": name, "type": kind}
                   for layer_id, name, kind in EXPECTED_LAYER_ROSTER],
    }
    rosters = [{"objectIdFieldName": "OBJECTID", "objectIds": list(ids)} for _ in range(3)]
    documents = {
        "service_before": service, "service_after": copy.deepcopy(service),
        "layer_before": layer, "layer_after": copy.deepcopy(layer),
        "count_before": {"count": count}, "count_after": {"count": count},
        "ids_before": rosters[0], "ids_seed": rosters[1], "ids_after": rosters[2],
    }
    for index, offset in enumerate(range(0, len(ids), PAGE_SIZE)):
        documents[f"page_{index:03d}"] = {
            "features": features[offset:offset + PAGE_SIZE],
            "spatialReference": {"latestWkid": 4326},
        }
    return documents


def _expectations(ids, features, fields):
    geometry_records = []
    for feature in features:
        geometry = feature["geometry"]
        oid = feature["attributes"]["OBJECTID"]
        geometry_records.append([oid, None, None] if geometry is None
                                else [oid, geometry["x"], geometry["y"]])
    return {
        "expected_count": len(ids),
        "expected_id_sha256": _sha(_canonical(sorted(ids))),
        "expected_record_sha256": _sha(_canonical(
            sorted(features, key=lambda row: row["attributes"]["OBJECTID"]))),
        "expected_geometry_sha256": _sha(_canonical(geometry_records)),
        "expected_schema_sha256": _sha(_canonical([_plain_field(f) for f in fields])),
    }


def _captures(documents):
    return {name: (json.dumps(doc, ensure_ascii=False, allow_nan=False).encode(), {})
            for name, doc in documents.items()}


class FudsCaptureValidationTests(unittest.TestCase):
    def _validated(self, *, mutate=None, null_indexes=(), expectation_geometry_override=None):
        ids = list(IDS)
        features = _features(ids=ids, null_indexes=null_indexes)
        fields = _fields()
        documents = _documents(ids, features, fields, len(ids))
        if mutate is not None:
            mutate(documents)
        captures = _captures(documents)
        expectations = _expectations(ids, features, fields)
        if expectation_geometry_override is not None:
            expectations["expected_geometry_sha256"] = expectation_geometry_override
        return _validate_captures(captures, **expectations)

    def _assert_error(self, result_factory, code):
        with self.assertRaisesRegex(ValueError, code):
            result_factory()

    def test_complete_population_passes_and_counts_null_geometries(self):
        result = self._validated()
        layer, fields_value, extracted = result
        self.assertEqual(layer["name"], LAYER_NAME)
        self.assertEqual([f["name"] for f in fields_value],
                         ["OBJECTID", "DODFUDSPROPERTYIDPK", "SE_ANNO_CAD_DATA", "SHAPE"])
        audit = extracted["audit"]
        self.assertEqual(audit["source_feature_count"], len(IDS))
        self.assertEqual(audit["null_geometry_count"], 0)
        self.assertEqual(audit["unique_source_id_count"], len(IDS))
        self.assertEqual([row["attributes"]["OBJECTID"] for row in extracted["features"]], IDS)
        self.assertIsNone(audit["source_release_date"])

    def test_null_geometries_are_retained_and_counted_not_dropped(self):
        extracted = self._validated(null_indexes=(1,))[2]
        audit = extracted["audit"]
        self.assertEqual(audit["source_feature_count"], len(IDS))
        self.assertEqual(audit["null_geometry_count"], 1)
        self.assertIsNone(extracted["features"][1]["geometry"])
        self.assertEqual(len(extracted["ids"]), len(IDS))

    def test_missing_renamed_or_dropped_source_ids_fail(self):
        def rename_in_page(documents):
            documents["page_000"]["features"][1]["attributes"]["OBJECTID"] = 99

        self._assert_error(lambda: self._validated(mutate=rename_in_page),
                           "source_page_id_slice_incomplete_or_reordered")

        def rename_everywhere(documents):
            for name in ("ids_before", "ids_seed", "ids_after"):
                roster = documents[name]["objectIds"]
                roster[roster.index(12)] = 14
            documents["page_000"]["features"][1]["attributes"]["OBJECTID"] = 14

        self._assert_error(lambda: self._validated(mutate=rename_everywhere),
                           "source_id_roster_drift")

        def drop_seed_id(documents):
            documents["ids_seed"]["objectIds"].remove(12)

        self._assert_error(lambda: self._validated(mutate=drop_seed_id),
                           "source_id_population_drift")

    def test_value_type_loss_fails(self):
        def lose_leading_zero(documents):
            documents["page_000"]["features"][1]["attributes"]["DODFUDSPROPERTYIDPK"] = "10000007"

        self._assert_error(lambda: self._validated(mutate=lose_leading_zero),
                           "source_record_value_drift")

        def cast_double_to_text(documents):
            documents["page_000"]["features"][0]["geometry"]["x"] = "0.0"

        self._assert_error(lambda: self._validated(mutate=cast_double_to_text),
                           "source_point_geometry_invalid")

    def test_unexpected_column_fails(self):
        def add_unmapped_column(documents):
            documents["page_000"]["features"][0]["attributes"]["EXTRA_UNMAPPED"] = "x"

        self._assert_error(lambda: self._validated(mutate=add_unmapped_column),
                           "source_record_attribute_schema_drift")

    def test_field_schema_drift_fails(self):
        def rename_field_everywhere(documents):
            for name in ("layer_before", "layer_after"):
                documents[name]["fields"][1]["name"] = "RENAMED"

        self._assert_error(lambda: self._validated(mutate=rename_field_everywhere),
                           "source_schema_contract_drift")

        def drop_field_after_capture(documents):
            del documents["layer_after"]["fields"][1]

        self._assert_error(lambda: self._validated(mutate=drop_field_after_capture),
                           "source_field_schema_changed_during_capture")

    def test_geometry_and_coverage_drift_fails(self):
        def move_point(documents):
            documents["page_000"]["features"][0]["geometry"]["x"] = 5.0

        self._assert_error(lambda: self._validated(mutate=move_point),
                           "source_record_value_drift")

        drifted = _sha(_canonical([[11, 1.0, 0.0], [12, 10.5, -20.25], [13, 100.0, 100.0]]))
        self._assert_error(lambda: self._validated(expectation_geometry_override=drifted),
                           "source_geometry_or_coverage_drift")

    def test_incomplete_or_drifting_source_responses_fail(self):
        def drop_page(documents):
            del documents["page_000"]

        self._assert_error(lambda: self._validated(mutate=drop_page),
                           "source_capture_roster_incomplete_or_unexpected")

        def add_unexpected_page(documents):
            documents["page_001"] = {"features": []}

        self._assert_error(lambda: self._validated(mutate=add_unexpected_page),
                           "source_capture_roster_incomplete_or_unexpected")

        def error_page(documents):
            documents["page_000"] = {"error": {"message": "query failed"}}

        self._assert_error(lambda: self._validated(mutate=error_page), "source_page_error")

        def short_page(documents):
            documents["page_000"]["features"] = documents["page_000"]["features"][:2]

        self._assert_error(lambda: self._validated(mutate=short_page),
                           "source_page_id_slice_incomplete_or_reordered")

        def reordered_page(documents):
            documents["page_000"]["features"].reverse()

        self._assert_error(lambda: self._validated(mutate=reordered_page),
                           "source_page_id_slice_incomplete_or_reordered")

        def exceeded_on_final_page(documents):
            documents["page_000"]["exceededTransferLimit"] = True

        self._assert_error(lambda: self._validated(mutate=exceeded_on_final_page),
                           "source_page_unexpected_exceeded_transfer_limit")

        def count_drift(documents):
            documents["count_before"]["count"] = 2

        self._assert_error(lambda: self._validated(mutate=count_drift), "source_count_drift")

        def service_identity_drift(documents):
            documents["service_before"]["mapName"] = "Other"

        self._assert_error(lambda: self._validated(mutate=service_identity_drift),
                           "official_usace_service_identity_drift")

        def layer_roster_drift(documents):
            documents["service_before"]["layers"][1]["name"] = "Renamed Layer"

        self._assert_error(lambda: self._validated(mutate=layer_roster_drift),
                           "fuds_service_layer_roster_drift")

        def z_geometry(documents):
            documents["layer_before"]["hasZ"] = True

        self._assert_error(lambda: self._validated(mutate=z_geometry),
                           "source_z_or_m_geometry_unsupported")

        def pagination_removed(documents):
            documents["layer_before"]["advancedQueryCapabilities"]["supportsPagination"] = False

        self._assert_error(lambda: self._validated(mutate=pagination_removed),
                           "source_pagination_capability_changed")

        def crs_drift(documents):
            documents["layer_before"]["spatialReference"]["latestWkid"] = 3857

        self._assert_error(lambda: self._validated(mutate=crs_drift),
                           "source_record_limit_or_crs_drift")

    def test_page_count_matches_expected_population_multiple_pages(self):
        count = 2 * PAGE_SIZE + 3
        ids = list(range(1, count + 1))
        features = _features(ids=ids)
        fields = _fields()
        documents = _documents(ids, features, fields, count)
        result = _validate_captures(_captures(documents), **_expectations(ids, features, fields))
        self.assertEqual(result[2]["audit"]["source_feature_count"], count)
        self.assertEqual(result[2]["audit"]["unique_source_id_count"], count)


class FudsPublicPropertiesTests(unittest.TestCase):
    def test_native_candidate_preserves_all_properties_and_null_point_rows(self):
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "domain": None},
            {"name": "DODFUDSPROPERTYIDPK", "type": "esriFieldTypeString", "length": 40},
            {"name": "SE_ANNO_CAD_DATA", "type": "esriFieldTypeBlob"},
            {"name": "SHAPE", "type": "esriFieldTypeGeometry"},
        ]
        features = [
            {"attributes": {"OBJECTID": 17, "DODFUDSPROPERTYIDPK": "A04MS0001",
                            "SE_ANNO_CAD_DATA": None},
             "geometry": {"x": -89.25194443999999, "y": 31.264444440000034}},
            {"attributes": {"OBJECTID": 18, "DODFUDSPROPERTYIDPK": "0010000007",
                            "SE_ANNO_CAD_DATA": None}, "geometry": None},
        ]
        data = _parquet_bytes(fields, features)
        table = pq.ParquetFile(io.BytesIO(data)).read(use_threads=False)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [17, 18])
        self.assertEqual(table.column("DODFUDSPROPERTYIDPK").to_pylist(), ["A04MS0001", "0010000007"])
        self.assertEqual(table.column("SE_ANNO_CAD_DATA").to_pylist(), [None, None])
        geometries = table.column("SHAPE").to_pylist()
        self.assertAlmostEqual(shapely.from_wkb(geometries[0]).x, features[0]["geometry"]["x"])
        self.assertAlmostEqual(shapely.from_wkb(geometries[0]).y, features[0]["geometry"]["y"])
        self.assertIsNone(geometries[1])


class FudsRegisteredLifecycleTests(unittest.TestCase):
    def _instance(self, root):
        directory = Path(root) / "dagster"
        directory.mkdir()
        (directory / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
        return DagsterInstance.from_config(str(directory))

    def _candidate(self, null_geometries, version="vtest0001"):
        return {
            "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_null_geometry_count": null_geometries,
            "candidate_ready": f"geoparquet/{DATASET_SLUG}/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        }

    def _materialize(self, storage, instance, candidate):
        with patch(
            "dagster_hifld.assets.fuds_public_properties.capture_fuds_public_properties",
            return_value=dict(candidate),
        ):
            return materialize(
                [usace_fuds_public_properties, usace_fuds_public_properties_complete],
                resources={"staging_storage": storage}, instance=instance, raise_on_error=False,
            )

    def test_complete_candidate_registers_partition_and_passes_blocking_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self._instance(tmp) as instance:
                result = self._materialize(storage, instance, self._candidate(0))
                self.assertTrue(result.success)
                evaluations = result.get_asset_check_evaluations()
                self.assertEqual(evaluations[0].check_name, "usace_fuds_public_properties_complete")
                self.assertTrue(evaluations[0].passed)
                partition = build_publish_partition_key(DATASET_SLUG, FILE_SLUG, "vtest0001")
                self.assertIn(partition, instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))

    def test_retained_null_geometries_fail_blocking_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self._instance(tmp) as instance:
                result = self._materialize(storage, instance, self._candidate(1286))
                self.assertFalse(result.success)
                evaluations = result.get_asset_check_evaluations()
                self.assertEqual(evaluations[0].check_name, "usace_fuds_public_properties_complete")
                self.assertFalse(evaluations[0].passed)
                self.assertTrue(evaluations[0].blocking)
                self.assertEqual(evaluations[0].metadata["source_null_geometry_count"].value, 1286)


if __name__ == "__main__":
    unittest.main()
