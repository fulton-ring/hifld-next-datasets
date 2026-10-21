"""Source contract tests for the complete official IENC PYLONS_AREA layer."""

from __future__ import annotations

import copy
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources.usace_pylon_areas import (
    BTS_ITEM_ID,
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_FIELD_DECLARATIONS,
    EXPECTED_FIELDS,
    EXPECTED_ID_SHA256,
    EXPECTED_LAYER_EXTENT,
    EXPECTED_IDS,
    FILE_SLUG,
    LAYER_URL,
    PAGE_SIZE,
    _esri_polygon,
    _parquet_bytes,
    _validate,
    processing_identity,
    validate_processing_identity,
)
from dagster_hifld.assets.publish import _copy_version_files
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


def _ring(x: float, y: float, size: float = 0.001) -> list[list[float]]:
    # ArcGIS exterior rings are clockwise.
    return [
        [x, y], [x, y + size], [x + size, y + size], [x + size, y], [x, y],
    ]


def _features(ids: list[int]) -> list[dict]:
    features = []
    for index, oid in enumerate(ids):
        x = EXPECTED_LAYER_EXTENT["xmin"] + (index % 1000) * 0.005
        features.append({
            "attributes": {
                "OBJECTID": oid,
                "Category_of_Pylon": "Bridge Pier",
                "Water_Level": "Always Dry",
                "Condition": "",
                "Scale_Minimum": 30000,
                "Source_Date": "20150923",
                "Source_Indication": "US,U3,SURVY",
                "Information": "",
                "Source_Dataset": "U37AG001.000",
                "Feature_ID": f"U3 {oid:010d} 00011",
            },
            "geometry": {"rings": [_ring(x, EXPECTED_LAYER_EXTENT["ymin"])]},
        })
    return features


def _docs() -> dict[str, dict]:
    item = {
        "id": BTS_ITEM_ID,
        "owner": "USDOT_BTS",
        "title": "Inland Electronic Navigational Charts",
        "description": (
            "The Inland Electronic Navigational Charts (IENC) dataset is updated "
            "bi-monthly by the US Army Corps (USACE)/Army Geospatial Center (AGC), "
            "and part of U.S. Department of Transportation (USDOT)/Bureau of "
            "Transportation Statistics (BTS) National Transportation Atlas Database "
            "(NTAD). These vector data, that make up the charts, can be downloaded "
            "as a geodatabase here: https://ienccloud.us/ienc/products/files/U37/"
            "ienc_master_dataset_gdb/USACE_IENC_Master_Service_gdb.zip. In addition, "
            "web mapping services of the feature classes/datasets can be found here: "
            "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes."
        ),
    }
    directory = {
        "services": [
            {"name": "IENC_Feature_Classes/PYLONS_AREA", "type": "MapServer"},
            {"name": "IENC_Feature_Classes/PYLONS_POINT", "type": "MapServer"},
        ],
    }
    service = {
        "mapName": "PYLONS_AREA",
        "copyrightText": "USACE IENC",
        "layers": [{"id": 69, "name": "PYLONS_AREA", "geometryType": "esriGeometryPolygon"}],
    }
    layer = {
        "id": 69,
        "name": "PYLONS_AREA",
        "geometryType": "esriGeometryPolygon",
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {**EXPECTED_LAYER_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
        "maxRecordCount": 2000,
        "fields": copy.deepcopy(EXPECTED_FIELD_DECLARATIONS),
    }
    crs = {"wkid": 4326, "latestWkid": 4326}
    docs: dict[str, dict] = {
        "bts_item": item,
        "service_directory": directory,
        "service_before": service,
        "service_after": copy.deepcopy(service),
        "layer_before": layer,
        "layer_after": copy.deepcopy(layer),
        "count_before": {"count": EXPECTED_COUNT},
        "count_after": {"count": EXPECTED_COUNT},
        "ids_before": {"objectIds": list(EXPECTED_IDS)},
        "ids_after": {"objectIds": list(EXPECTED_IDS)},
        "ids_seed": {"objectIds": list(EXPECTED_IDS)},
    }
    features = _features(EXPECTED_IDS)
    for index, start in enumerate(range(0, EXPECTED_COUNT, PAGE_SIZE)):
        page_features = features[start:start + PAGE_SIZE]
        docs[f"page_{index:03d}"] = {
            "spatialReference": crs,
            "features": copy.deepcopy(page_features),
        }
    return docs


def _captures() -> dict[str, tuple[bytes, dict]]:
    docs = _docs()
    return {
        name: (json.dumps(doc).encode("utf-8"),
               {"complete_body": True, "status": 200})
        for name, doc in docs.items()
    }


class ValidateCompleteCapture(unittest.TestCase):
    def test_complete_capture_passes(self):
        fields, features, audit = _validate(_captures())
        self.assertEqual(len(features), EXPECTED_COUNT)
        self.assertEqual(audit["source_feature_count"], EXPECTED_COUNT)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], EXPECTED_COUNT)
        self.assertEqual(audit["feature_id_duplicate_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], EXPECTED_COUNT)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(
            fields,
            [
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
                {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
                {"name": "Category_of_Pylon", "type": "esriFieldTypeString", "alias": "Category_of_Pylon", "domain": None, "length": 254},
                {"name": "Water_Level", "type": "esriFieldTypeString", "alias": "Water_Level", "domain": None, "length": 254},
                {"name": "Condition", "type": "esriFieldTypeString", "alias": "Condition", "domain": None, "length": 254},
                {"name": "Scale_Minimum", "type": "esriFieldTypeInteger", "alias": "Scale_Minimum", "domain": None},
                {"name": "Source_Date", "type": "esriFieldTypeString", "alias": "Source_Date", "domain": None, "length": 254},
                {"name": "Source_Indication", "type": "esriFieldTypeString", "alias": "Source_Indication", "domain": None, "length": 254},
                {"name": "Information", "type": "esriFieldTypeString", "alias": "Information", "domain": None, "length": 254},
                {"name": "Source_Dataset", "type": "esriFieldTypeString", "alias": "Source_Dataset", "domain": None, "length": 254},
                {"name": "Feature_ID", "type": "esriFieldTypeString", "alias": "Feature_ID", "domain": None, "length": 254},
            ],
        )

    def test_expected_population_pins(self):
        self.assertEqual(EXPECTED_COUNT, 6440)
        self.assertEqual(EXPECTED_IDS[0], 1)
        self.assertEqual(EXPECTED_IDS[-1], 6440)
        self.assertEqual(len(EXPECTED_IDS), EXPECTED_COUNT)
        self.assertEqual(
            EXPECTED_ID_SHA256,
            "8dc3b41f3a226207090e7b947380144179fcb92a90018eb15a7daf21aa821b4e",
        )
        self.assertTrue(LAYER_URL.endswith("PYLONS_AREA/MapServer/69"))


class PackagedRuntimeIdentity(unittest.TestCase):
    def test_no_git_packaged_runtime_uses_verified_installed_code_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "src/dagster_hifld"
            source.mkdir(parents=True)
            (source / "pipeline.py").write_text("processor = 'pylon'\n", encoding="utf-8")
            (root / "pyproject.toml").write_text("[project]\nname='runtime-fixture'\n", encoding="utf-8")
            identity = processing_identity(root)
            self.assertEqual(identity["code_commit_kind"], "installed_processing_fingerprint_not_vcs_commit")
            self.assertTrue(str(identity["code_commit"]).startswith("installed-processing-sha256:"))
            self.assertNotIn("uv.lock", identity["files"])
            validate_processing_identity(root, identity)
            (source / "pipeline.py").write_text("processor = 'changed'\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "installed_processing_code_identity_changed"):
                validate_processing_identity(root, identity)

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(DATASET_SLUG, FILE_SLUG, "candidate-v1", "geoparquet/pylon.parquet", b"candidate")
            accepted_key = published.write(DATASET_SLUG, FILE_SLUG, "accepted-v1", "geoparquet/pylon.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError, "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, DATASET_SLUG, FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)


class ValidateDriftRejections(unittest.TestCase):
    def _mutate(self, name: str, mutation) -> None:
        docs = _docs()
        mutation(docs)
        captures = {
            key: (json.dumps(doc).encode("utf-8"), {"complete_body": True, "status": 200})
            for key, doc in docs.items()
        }
        with self.assertRaises(ValueError) as raised:
            _validate(captures)
        return raised.exception

    def test_bts_owner_drift(self):
        code = self._mutate("bts_item", lambda d: d["bts_item"].update(owner="SOMEONE_ELSE"))
        self.assertEqual(str(code), "bts_ienc_item_identity_drift")

    def test_bts_distribution_link_drift(self):
        code = self._mutate(
            "bts_item", lambda d: d["bts_item"].update(description="unrelated text"))
        self.assertEqual(str(code), "bts_ienc_official_distribution_link_drift")

    def test_service_directory_membership_drift(self):
        code = self._mutate(
            "service_directory",
            lambda d: d["service_directory"].update(services=[]))
        self.assertEqual(str(code), "official_ienc_service_directory_drift")

    def test_service_identity_drift(self):
        code = self._mutate(
            "service_before",
            lambda d: d["service_before"].update(mapName="SOMETHING_ELSE"))
        self.assertEqual(str(code), "official_pylons_area_service_drift")

    def test_service_change_during_capture(self):
        code = self._mutate(
            "service_after",
            lambda d: d["service_after"].update(currentVersion="9.9"))
        self.assertEqual(str(code), "source_service_changed_during_capture")

    def test_layer_change_during_capture(self):
        code = self._mutate(
            "layer_after", lambda d: d["layer_after"].update(id=70))
        self.assertEqual(str(code), "source_layer_changed_during_capture")

    def test_field_schema_drift(self):
        def mutate(docs):
            for key in ("layer_before", "layer_after"):
                docs[key]["fields"].pop(5)
        code = self._mutate("layer_before", mutate)
        self.assertEqual(str(code), "source_field_schema_drift")

    def test_field_type_drift(self):
        def mutate(docs):
            for key in ("layer_before", "layer_after"):
                docs[key]["fields"][5]["type"] = "esriFieldTypeString"
        code = self._mutate("layer_before", mutate)
        self.assertEqual(str(code), "source_field_schema_drift")

    def test_crs_drift(self):
        def mutate(docs):
            docs["layer_before"]["spatialReference"] = {"wkid": 3857, "latestWkid": 3857}
            docs["layer_after"]["spatialReference"] = {"wkid": 3857, "latestWkid": 3857}
        code = self._mutate("layer_before", mutate)
        self.assertEqual(str(code), "source_spatial_reference_or_page_limit_drift")

    def test_extent_drift(self):
        def mutate(docs):
            for key in ("layer_before", "layer_after"):
                docs[key]["extent"] = {**EXPECTED_LAYER_EXTENT, "xmax": -70.0,
                                       "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
        code = self._mutate("layer_before", mutate)
        self.assertEqual(str(code), "source_coverage_extent_drift")

    def test_page_limit_drift(self):
        def mutate(docs):
            docs["layer_before"]["maxRecordCount"] = 100
            docs["layer_after"]["maxRecordCount"] = 100
        code = self._mutate("layer_before", mutate)
        self.assertEqual(str(code), "source_spatial_reference_or_page_limit_drift")

    def test_count_drift(self):
        code = self._mutate(
            "count_after", lambda d: d["count_after"].update(count=6439))
        self.assertEqual(str(code), "source_count_drift")

    def test_id_roster_drift(self):
        code = self._mutate(
            "ids_after", lambda d: d["ids_after"].update(objectIds=list(range(1, EXPECTED_COUNT))))
        self.assertEqual(str(code), "source_id_population_drift")

    def test_seed_roster_drift(self):
        code = self._mutate(
            "ids_seed", lambda d: d["ids_seed"].update(objectIds=[2, *EXPECTED_IDS]))
        self.assertEqual(str(code), "source_seed_id_population_drift")

    def test_page_roster_incomplete(self):
        docs = _docs()
        docs.pop("page_013")
        captures = {
            key: (json.dumps(doc).encode("utf-8"), {"complete_body": True, "status": 200})
            for key, doc in docs.items()
        }
        with self.assertRaises(ValueError) as raised:
            _validate(captures)
        self.assertIn("source_capture_roster_incomplete", str(raised.exception))

    def test_page_error_rejected(self):
        code = self._mutate("page_000", lambda d: d["page_000"].update(
            error={"code": 500, "message": "internal"}))
        self.assertEqual(str(code), "source_page_error_or_truncation")

    def test_page_truncation_rejected(self):
        code = self._mutate(
            "page_000", lambda d: d["page_000"].update(exceededTransferLimit=True))
        self.assertEqual(str(code), "source_page_error_or_truncation")

    def test_feature_population_incomplete(self):
        code = self._mutate(
            "page_000", lambda d: d["page_000"].update(features=d["page_000"]["features"][1:]))
        self.assertEqual(str(code), "source_feature_population_incomplete")

    def test_unexpected_attribute_column_rejected(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["Unmapped"] = "value"
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_record_schema_or_geometry_drift")

    def test_missing_attribute_rejected(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"].pop("Condition")
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_record_schema_or_geometry_drift")

    def test_integer_value_type_drift(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["Scale_Minimum"] = "30000"
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_integer_value_type_drift:Scale_Minimum")

    def test_oid_type_drift(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["OBJECTID"] = "1"
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_oid_value_type_drift")

    def test_native_identifier_missing(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["Feature_ID"] = ""
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_native_identifier_missing_or_type_drift")

    def test_unexpected_geometry_ordinates_rejected(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["geometry"]["curveRings"] = []
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "unexpected_source_geometry_ordinates")

    def test_geometry_outside_declared_coverage(self):
        def mutate(docs):
            feature = docs["page_000"]["features"][0]
            feature["geometry"]["rings"][0] = [
                [-70.0, 30.0], [-69.9, 30.0], [-69.9, 30.1], [-70.0, 30.1], [-70.0, 30.0],
            ]
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_geometry_outside_declared_coverage")

    def test_unclosed_ring_rejected(self):
        def mutate(docs):
            ring = docs["page_000"]["features"][0]["geometry"]["rings"][0]
            docs["page_000"]["features"][0]["geometry"]["rings"][0] = ring[:-1]
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_polygon_ring_not_closed")

    def test_counter_clockwise_ring_rejected(self):
        def mutate(docs):
            ring = docs["page_000"]["features"][0]["geometry"]["rings"][0]
            docs["page_000"]["features"][0]["geometry"]["rings"][0] = list(reversed(ring))
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_polygon_shell_missing")

    def test_zm_flag_mismatch_rejected(self):
        def mutate(docs):
            feature = docs["page_000"]["features"][0]
            feature["geometry"]["hasZ"] = True
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_polygon_zm_flags_do_not_match_coordinates")


class ParquetCandidateContract(unittest.TestCase):
    def _sample(self):
        _fields, features, _audit = _validate(_captures())
        fields = copy.deepcopy(EXPECTED_FIELD_DECLARATIONS)
        return fields, features

    def test_round_trip_preserves_schema_values_and_geometry(self):
        fields, features = self._sample()
        parquet = _parquet_bytes(fields, features, 4326)
        import io

        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, EXPECTED_COUNT)
        self.assertEqual(
            [field.name for field in table.schema],
            ["OBJECTID", "Category_of_Pylon", "Water_Level", "Condition", "Scale_Minimum",
             "Source_Date", "Source_Indication", "Information", "Source_Dataset",
             "Feature_ID", "geometry"],
        )
        types = {field.name: str(field.type) for field in table.schema}
        self.assertEqual(types["OBJECTID"], "int64")
        self.assertEqual(types["Scale_Minimum"], "int32")
        self.assertEqual(types["Feature_ID"], "string")
        self.assertEqual(types["geometry"], "binary")
        column = json.loads(
            table.schema.metadata[b"geo"].decode("utf-8"))["columns"]["geometry"]
        self.assertEqual(column["encoding"], "WKB")
        self.assertEqual(column["crs"]["id"]["code"], 4326)
        first = features[0]
        self.assertEqual(table.column("OBJECTID").to_pylist()[0], 1)
        self.assertEqual(
            table.column("Feature_ID").to_pylist()[0], first["attributes"]["Feature_ID"])
        self.assertEqual(
            table.column("Category_of_Pylon").to_pylist()[0],
            first["attributes"]["Category_of_Pylon"])
        self.assertEqual(
            table.column("Scale_Minimum").to_pylist()[0], 30000)
        geometry = shapely.from_wkb(table.column("geometry").to_pylist()[0])
        self.assertEqual(geometry.geom_type, "Polygon")
        self.assertTrue(geometry.is_valid)
        x, y = geometry.representative_point().x, geometry.representative_point().y
        self.assertGreaterEqual(x, EXPECTED_LAYER_EXTENT["xmin"])
        self.assertLessEqual(x, EXPECTED_LAYER_EXTENT["xmax"])
        self.assertGreaterEqual(y, EXPECTED_LAYER_EXTENT["ymin"])
        self.assertLessEqual(y, EXPECTED_LAYER_EXTENT["ymax"])
        self.assertEqual(
            shapely.from_wkb(_esri_polygon(first["geometry"])).wkb,
            table.column("geometry").to_pylist()[0],
        )

    def test_wkb_bytes_are_stable_for_identical_geometry(self):
        _fields, features, _audit = _validate(_captures())
        first = features[0]
        self.assertEqual(
            _esri_polygon(first["geometry"]), _esri_polygon(first["geometry"]))

    def test_declared_fields_cover_all_non_geometry_sources(self):
        names = [name for name, _kind in EXPECTED_FIELDS if name != "SHAPE"]
        self.assertEqual(len(names), 10)
        self.assertIn("Scale_Minimum", names)
        self.assertIn("Feature_ID", names)
        self.assertIn("Category_of_Pylon", names)


if __name__ == "__main__":
    unittest.main()
