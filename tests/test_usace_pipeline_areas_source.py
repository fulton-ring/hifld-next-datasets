"""Source contract tests for the complete official IENC PIPE_AREA layer."""

from __future__ import annotations

import io
import json
import unittest

import pyarrow.parquet as pq
import shapely

from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.sources.usace_pipeline_areas import (
    BTS_ITEM_ID,
    EXPECTED_COUNT,
    EXPECTED_FIELD_DECLARATIONS,
    EXPECTED_FIELDS,
    EXPECTED_ID_SHA256,
    _esri_polygon,
    _parquet_bytes,
    _validate,
)


def _docs(ids: list[int] | None = None) -> dict[str, dict]:
    object_ids = ids or list(range(1, EXPECTED_COUNT + 1))
    fields = EXPECTED_FIELD_DECLARATIONS
    item = {
        "id": BTS_ITEM_ID,
        "owner": "USDOT_BTS",
        "title": "Inland Electronic Navigational Charts",
        "description": (
            "IENC dataset is updated bi-monthly by USACE. "
            "Charts can be downloaded here: https://ienccloud.us/ienc/products/files/"
            "U37/ienc_master_dataset_gdb/USACE_IENC_Master_Service_gdb.zip. "
            "Services: https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes"
        ),
    }
    service = {
        "mapName": "PIPE_AREA",
        "copyrightText": "USACE IENC",
        "layers": [{"id": 82, "name": "PIPE_AREA", "geometryType": "esriGeometryPolygon"}],
    }
    layer = {
        "id": 82,
        "name": "PIPE_AREA",
        "geometryType": "esriGeometryPolygon",
        "spatialReference": {"latestWkid": 4326},
        "maxRecordCount": 2000,
        "fields": fields,
    }
    features = []
    for oid in object_ids:
        attrs = {name: None for name, _kind in EXPECTED_FIELDS if name != "SHAPE"}
        attrs.update({"OBJECTID": oid, "Feature_ID": f"PIPE-{oid:03d}"})
        features.append(
            {
                "attributes": attrs,
                "geometry": {
                    "rings": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                    "spatialReference": {"wkid": 4326},
                },
            }
        )
    return {
        "bts_item": item,
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/PIPE_AREA", "type": "MapServer"}]},
        "service_before": service,
        "service_after": service,
        "layer_before": layer,
        "layer_after": layer,
        "count_before": {"count": EXPECTED_COUNT},
        "count_after": {"count": EXPECTED_COUNT},
        "ids_seed": {"objectIds": object_ids},
        "ids_before": {"objectIds": object_ids},
        "ids_after": {"objectIds": object_ids},
        "page_000": {"spatialReference": {"latestWkid": 4326}, "features": features},
    }


def _captures(docs: dict[str, dict]) -> dict[str, tuple[bytes, dict]]:
    return {name: (json.dumps(doc).encode("utf-8"), {}) for name, doc in docs.items()}


class UsacePipelineAreasSourceTest(unittest.TestCase):
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
        docs["page_000"]["features"][0]["attributes"]["Scale_Minimum"] = "60000"
        with self.assertRaisesRegex(ValueError, "source_integer_value_type_drift"):
            _validate(_captures(docs))

    def test_native_identifier_values_and_polygon_coordinates_survive_geoparquet(self) -> None:
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID"},
            {"name": "SHAPE", "type": "esriFieldTypeGeometry"},
            {"name": "Object_Name", "type": "esriFieldTypeString"},
            {"name": "Scale_Minimum", "type": "esriFieldTypeInteger"},
            {"name": "Feature_ID", "type": "esriFieldTypeString"},
        ]
        feature = {
            "attributes": {"OBJECTID": 17, "Object_Name": "00107", "Scale_Minimum": 0, "Feature_ID": "U3 1393266544 00001"},
            "geometry": {"rings": [[[0, 0], [0, 4], [4, 4], [4, 0], [0, 0]]]},
        }
        body = _parquet_bytes(fields, [feature], 4326)
        table = pq.ParquetFile(io.BytesIO(body)).read(use_threads=False)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [17])
        self.assertEqual(table.column("Object_Name").to_pylist(), ["00107"])
        self.assertEqual(table.column("Scale_Minimum").to_pylist(), [0])
        self.assertEqual(table.column("Feature_ID").to_pylist(), ["U3 1393266544 00001"])
        self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
        self.assertEqual(str(table.schema.field("Scale_Minimum").type), "int32")
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


if __name__ == "__main__":
    unittest.main()
