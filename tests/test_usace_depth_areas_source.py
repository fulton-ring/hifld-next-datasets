"""Source fidelity and drift tests for USACE IENC Depth Areas."""

from __future__ import annotations

import copy
import io
import json
import unittest

import pyarrow.parquet as pq
import shapely

from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.sources import usace_depth_areas as source


def _docs(ids: list[int] | None = None) -> dict[str, dict]:
    object_ids = ids or source.EXPECTED_IDS
    service = {
        "mapName": "DEPTH_AREA",
        "copyrightText": "USACE IENC",
        "layers": [{"id": 96, "name": "DEPTH_AREA", "geometryType": "esriGeometryPolygon"}],
    }
    spatial_reference = {"wkid": 4326, "latestWkid": 4326}
    layer = {
        "id": 96,
        "name": "DEPTH_AREA",
        "geometryType": "esriGeometryPolygon",
        "spatialReference": spatial_reference,
        "sourceSpatialReference": spatial_reference,
        "extent": {"xmin": -1, "ymin": -1, "xmax": 2, "ymax": 2,
                   "spatialReference": spatial_reference},
        "hasZ": None,
        "hasM": None,
        "maxRecordCount": 2000,
        "advancedQueryCapabilities": {"supportsTrueCurve": True},
        "fields": source.EXPECTED_FIELD_DECLARATIONS,
    }
    features = []
    for oid in object_ids:
        attrs = {"OBJECTID": oid}
        attrs.update({name: f"{name}-{oid}" for name, kind in source.EXPECTED_FIELDS
                      if name != "SHAPE" and kind == "esriFieldTypeString"})
        x = oid / 100000
        features.append({
            "attributes": attrs,
            "geometry": {"rings": [[[x, 0], [x, 1], [x + 0.000005, 1],
                                     [x + 0.000005, 0], [x, 0]]]},
        })
    directory = {"services": [{"name": "IENC_Feature_Classes/DEPTH_AREA", "type": "MapServer"}]}
    captures = {
        "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
                     "access": "public", "title": "Inland Electronic Navigational Charts",
                     "description": "USACE IENC updated bi-monthly. " + source.SERVICE_DIRECTORY_URL.split("?")[0]
                         + " USACE_IENC_Master_Service_gdb.zip"},
        "service_directory_before": directory,
        "service_directory_after": copy.deepcopy(directory),
        "service_before": service,
        "service_after": copy.deepcopy(service),
        "layer_before": layer,
        "layer_after": copy.deepcopy(layer),
        "count_before": {"count": source.EXPECTED_COUNT},
        "count_after": {"count": source.EXPECTED_COUNT},
        "ids_before": {"objectIds": object_ids},
        "ids_after": {"objectIds": object_ids},
    }
    for index, start in enumerate(range(0, len(features), source.PAGE_SIZE)):
        captures[f"page_{index:03d}"] = {
            "spatialReference": spatial_reference,
            "features": features[start:start + source.PAGE_SIZE],
        }
    return captures


def _captures(docs: dict[str, dict]) -> dict[str, tuple[bytes, dict]]:
    return {name: (json.dumps(doc).encode("utf-8"), {}) for name, doc in docs.items()}


class UsaceDepthAreasSourceTest(unittest.TestCase):
    def test_complete_id_roster_and_polygon_population_are_checked(self) -> None:
        fields, features, audit = source._validate(_captures(_docs()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), source.EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["geometry_valid_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["source_vertex_count"], source.EXPECTED_COUNT * 5)
        self.assertIsNone(audit["source_has_z"])
        self.assertIsNone(audit["source_has_m"])
        self.assertEqual(audit["observed_coordinate_dimensions"], [2])

    def test_changed_id_roster_fails_closed(self) -> None:
        docs = _docs()
        docs["ids_after"] = {"objectIds": list(range(1, source.EXPECTED_COUNT)) + [111]}
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            source._validate(_captures(docs))

    def test_layer_and_page_crs_conflicts_fail_closed(self) -> None:
        for location, key, value, label in (
            ("layer", "sourceSpatialReference", {"wkid": 3857}, "source_layer_crs_drift"),
            ("page", "spatialReference", {"wkid": 3857}, "source_page_crs_drift"),
        ):
            with self.subTest(location=location):
                docs = _docs()
                if location == "layer":
                    docs["layer_before"][key] = value
                    docs["layer_after"] = copy.deepcopy(docs["layer_before"])
                else:
                    docs["page_000"]["spatialReference"] = value
                with self.assertRaisesRegex(ValueError, label):
                    source._validate(_captures(docs))

    def test_missing_page_and_unexpected_attribute_fail_closed(self) -> None:
        docs = _docs()
        del docs["page_001"]
        with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete"):
            source._validate(_captures(docs))

        docs = _docs()
        docs["page_000"]["features"][0]["attributes"]["UNEXPECTED"] = "kept in raw, blocks candidate"
        with self.assertRaisesRegex(ValueError, "source_record_schema_or_geometry_drift"):
            source._validate(_captures(docs))

    def test_true_curve_geometry_is_explicitly_rejected_without_lossless_encoding(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][0]["geometry"] = {
            "curveRings": [[[0, 0], {"c": [[1, 1], [2, 0]]}]]
        }
        with self.assertRaisesRegex(ValueError, "source_true_curve_geometry_unsupported_without_lossless_encoding"):
            source._validate(_captures(docs))

    def test_attribute_type_drift_is_rejected(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][0]["attributes"]["Depth_Area_Value_1"] = 12.5
        with self.assertRaisesRegex(ValueError, "source_string_value_type_drift"):
            source._validate(_captures(docs))

    def test_native_ids_strings_and_xy_polygon_survive_geoparquet(self) -> None:
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID"},
            {"name": "SHAPE", "type": "esriFieldTypeGeometry"},
            {"name": "Feature_ID", "type": "esriFieldTypeString"},
        ]
        feature = {
            "attributes": {"OBJECTID": 17, "Feature_ID": "U3 1393087507 00001"},
            "geometry": {"rings": [[[0, 0], [0, 4], [4, 4], [4, 0], [0, 0]]]},
        }
        table = pq.ParquetFile(io.BytesIO(source._parquet_bytes(fields, [feature], 4326))).read()
        self.assertEqual(table.column("OBJECTID").to_pylist(), [17])
        self.assertEqual(table.column("Feature_ID").to_pylist(), ["U3 1393087507 00001"])
        self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
        primary, _, crs = geometry_metadata(table.schema)
        self.assertEqual(primary, "geometry")
        self.assertEqual(crs.to_epsg(), 4326)
        geom = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertEqual(list(geom.exterior.coords), [(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)])


if __name__ == "__main__":
    unittest.main()
