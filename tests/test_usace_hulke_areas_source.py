"""Source contract tests for the complete official IENC HULKES_AREA layer."""

from __future__ import annotations

import io
import json
import unittest
import copy

import pyarrow.parquet as pq
import shapely

from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.sources.usace_hulke_areas import (
    BTS_ITEM_ID,
    EXPECTED_COUNT,
    EXPECTED_FIELD_DECLARATIONS,
    EXPECTED_FIELDS,
    EXPECTED_ID_SHA256,
    EXPECTED_LAYER_EXTENT,
    _parquet_bytes,
    _validate,
    PAGE_SIZE,
)


def _docs(ids: list[int] | None = None) -> dict[str, dict]:
    object_ids = ids or list(range(1, EXPECTED_COUNT + 1))
    fields = EXPECTED_FIELD_DECLARATIONS
    item = {
        "id": BTS_ITEM_ID,
        "owner": "USDOT_BTS",
        "type": "PDF",
        "access": "public",
        "title": "Inland Electronic Navigational Charts",
        "description": (
            "IENC dataset is updated bi-monthly by USACE. "
            "Charts can be downloaded here: https://ienccloud.us/ienc/products/files/"
            "U37/ienc_master_dataset_gdb/USACE_IENC_Master_Service_gdb.zip. "
            "Services: https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes"
        ),
    }
    service = {
        "mapName": "HULKES_AREA",
        "copyrightText": "USACE IENC",
        "layers": [{"id": 88, "name": "HULKES_AREA", "geometryType": "esriGeometryPolygon"}],
    }
    layer = {
        "id": 88,
        "name": "HULKES_AREA",
        "geometryType": "esriGeometryPolygon",
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {"xmin": -95.91146229999998, "ymin": 31.55923000000007,
                   "xmax": -80.01800609999998, "ymax": 43.04139410000005,
                   "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
        "maxRecordCount": 2000,
        "fields": fields,
    }
    features = []
    for oid in object_ids:
        attrs = {name: None for name, _kind in EXPECTED_FIELDS if name != "SHAPE"}
        attrs.update({"OBJECTID": oid, "Category_of_Hulke": "Mooring Area", "Object_Name": f"HULK-{oid:03d}", "Feature_ID": f"U3 {oid:010d}"})
        features.append(
            {
                "attributes": attrs,
                "geometry": {
                    "rings": [[[-90, 38], [-90, 39], [-89, 39], [-89, 38], [-90, 38]]],
                    "spatialReference": {"wkid": 4326},
                },
            }
        )
    docs = {
        "bts_item": item,
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/HULKES_AREA", "type": "MapServer"}]},
        "service_before": service,
        "service_after": service,
        "layer_before": layer,
        "layer_after": layer,
        "count_before": {"count": EXPECTED_COUNT},
        "count_after": {"count": EXPECTED_COUNT},
        "ids_seed": {"objectIds": object_ids},
        "ids_before": {"objectIds": object_ids},
        "ids_after": {"objectIds": object_ids},
    }
    for page_index, start in enumerate(range(0, len(features), PAGE_SIZE)):
        docs[f"page_{page_index:03d}"] = {
            "spatialReference": {"wkid": 4326, "latestWkid": 4326},
            "features": features[start:start + PAGE_SIZE],
        }
    return docs


def _captures(docs: dict[str, dict]) -> dict[str, tuple[bytes, dict]]:
    return {name: (json.dumps(doc).encode("utf-8"), {}) for name, doc in docs.items()}


class UsaceHulkeAreasSourceTest(unittest.TestCase):
    def test_current_public_bts_pdf_is_required_prior_denied_item_rejected(self) -> None:
        captures = _captures(_docs())
        item = json.loads(captures["bts_item"][0])
        item["id"] = "different-item"
        captures["bts_item"] = (json.dumps(item).encode("utf-8"), {})
        with self.assertRaisesRegex(ValueError, "bts_ienc_item_identity_drift"):
            _validate(captures)

    def test_complete_id_roster_and_geometry_are_checked(self) -> None:
        fields, features, audit = _validate(_captures(_docs()))
        self.assertEqual(fields, EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], EXPECTED_ID_SHA256)
        self.assertEqual(audit["geometry_valid_count"], EXPECTED_COUNT)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["source_vertex_count"], EXPECTED_COUNT * 5)

    def test_changed_ids_fail_closed(self) -> None:
        docs = _docs()
        docs["ids_after"] = {"objectIds": list(range(1, EXPECTED_COUNT)) + [113]}
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            _validate(_captures(docs))

    def test_type_loss_fails_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][0]["attributes"]["Object_Name"] = 60000
        with self.assertRaisesRegex(ValueError, "source_string_value_type_drift"):
            _validate(_captures(docs))

    def test_duplicate_feature_ids_are_retained_without_synthesizing_replacements(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][1]["attributes"]["Feature_ID"] = \
            docs["page_000"]["features"][0]["attributes"]["Feature_ID"]
        _fields, features, audit = _validate(_captures(docs))
        self.assertEqual(audit["feature_id_duplicate_count"], 1)
        self.assertEqual(
            features[1]["attributes"]["Feature_ID"],
            features[0]["attributes"]["Feature_ID"],
        )

    def test_nullable_feature_id_is_preserved(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][0]["attributes"]["Feature_ID"] = None
        fields, features, audit = _validate(_captures(docs))
        self.assertEqual(audit["feature_id_null_count"], 1)
        self.assertIsNone(features[0]["attributes"]["Feature_ID"])
        parquet = pq.read_table(io.BytesIO(_parquet_bytes(fields, features, 4326)))
        self.assertEqual(parquet["Feature_ID"].null_count, 1)
        self.assertEqual(parquet["OBJECTID"].to_pylist(), list(range(1, EXPECTED_COUNT + 1)))

    def test_unexpected_column_fails_closed(self) -> None:
        docs = _docs()
        docs["layer_before"]["fields"] = docs["layer_before"]["fields"] + [
            {"name": "NEW_FIELD", "type": "esriFieldTypeString", "alias": "NEW_FIELD", "length": 254, "domain": None}
        ]
        docs["layer_after"] = copy.deepcopy(docs["layer_before"])
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            _validate(_captures(docs))

    def test_incomplete_page_fails_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["exceededTransferLimit"] = True
        with self.assertRaisesRegex(ValueError, "source_page_error_or_truncation"):
            _validate(_captures(docs))

    def test_extent_and_geometry_coverage_drift_fail_closed(self) -> None:
        docs = _docs()
        docs["layer_before"]["extent"]["xmax"] += 1
        docs["layer_after"] = copy.deepcopy(docs["layer_before"])
        with self.assertRaisesRegex(ValueError, "source_coverage_extent_drift"):
            _validate(_captures(docs))
        docs = _docs()
        docs["page_000"]["features"][0]["geometry"]["rings"][0][0][0] = -110
        with self.assertRaisesRegex(ValueError, "source_geometry_outside_declared_coverage"):
            _validate(_captures(docs))

    def test_native_identifier_values_and_polygon_coordinates_survive_geoparquet(self) -> None:
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID"},
            {"name": "SHAPE", "type": "esriFieldTypeGeometry"},
            {"name": "Object_Name", "type": "esriFieldTypeString"},
            {"name": "Information", "type": "esriFieldTypeString"},
            {"name": "Feature_ID", "type": "esriFieldTypeString"},
        ]
        feature = {
            "attributes": {"OBJECTID": 17, "Object_Name": "00107", "Information": "raw text", "Feature_ID": "U3 1393266544 00001"},
            "geometry": {"rings": [[[0, 0], [0, 4], [4, 4], [4, 0], [0, 0]]]},
        }
        body = _parquet_bytes(fields, [feature], 4326)
        table = pq.ParquetFile(io.BytesIO(body)).read(use_threads=False)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [17])
        self.assertEqual(table.column("Object_Name").to_pylist(), ["00107"])
        self.assertEqual(table.column("Information").to_pylist(), ["raw text"])
        self.assertEqual(table.column("Feature_ID").to_pylist(), ["U3 1393266544 00001"])
        self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
        self.assertEqual(str(table.schema.field("Information").type), "string")
        primary, _, crs = geometry_metadata(table.schema)
        self.assertEqual(primary, "geometry")
        self.assertEqual(crs.to_epsg(), 4326)
        geom = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertEqual(list(geom.exterior.coords), [(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)])
    def test_bts_linkage_is_required(self) -> None:
        docs = _docs()
        docs["bts_item"]["owner"] = "unverified"
        with self.assertRaisesRegex(ValueError, "bts_ienc_item_identity_drift"):
            _validate(_captures(docs))

    def test_conflicting_layer_wkids_fail_closed(self) -> None:
        docs = _docs()
        docs["layer_before"]["spatialReference"] = {"wkid": 3857, "latestWkid": 4326}
        docs["layer_after"] = copy.deepcopy(docs["layer_before"])
        with self.assertRaisesRegex(ValueError, "source_layer_crs_drift"):
            _validate(_captures(docs))

    def test_extent_and_feature_crs_overrides_fail_closed(self) -> None:
        for location, label in (("extent", "source_layer_extent"), ("feature", "source_feature")):
            with self.subTest(location=location):
                docs = _docs()
                if location == "extent":
                    docs["layer_before"]["extent"] = {
                        **EXPECTED_LAYER_EXTENT,
                        "spatialReference": {"wkid": 4269},
                    }
                    docs["layer_after"] = copy.deepcopy(docs["layer_before"])
                else:
                    docs["page_000"]["features"][0]["geometry"]["spatialReference"] = {
                        "wkid": 3857,
                        "latestWkid": 4326,
                    }
                with self.assertRaisesRegex(ValueError, label + "_crs_drift"):
                    _validate(_captures(docs))

    def test_page_crs_conflict_fails_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["spatialReference"] = {"wkid": 4269, "latestWkid": 4326}
        with self.assertRaisesRegex(ValueError, "source_page_crs_drift"):
            _validate(_captures(docs))

    def test_layer_after_crs_or_extent_change_fails_closed(self) -> None:
        for key, value in (
            ("spatialReference", {"latestWkid": 4269}),
            ("extent", {"xmin": -1, "ymin": -1, "xmax": 2, "ymax": 2}),
        ):
            with self.subTest(key=key):
                docs = _docs()
                docs["layer_after"] = copy.deepcopy(docs["layer_before"])
                docs["layer_after"][key] = value
                with self.assertRaisesRegex(ValueError, "source_layer_changed_during_capture"):
                    _validate(_captures(docs))


if __name__ == "__main__":
    unittest.main()
