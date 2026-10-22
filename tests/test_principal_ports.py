import copy
import json
import struct
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely import from_wkb

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.principal_ports import (
    principal_ports_draft_schedule,
    principal_ports_source_complete,
    principal_ports_usace,
)
from dagster_hifld.sources import principal_ports as source


def _clockwise(x0, y0, x1, y1):
    return [[x0, y0], [x0, y1], [x1, y1], [x1, y0], [x0, y0]]


def _counterclockwise(x0, y0, x1, y1):
    return [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]


def _captures():
    extent = source.EXPECTED_EXTENT
    ids = list(source.EXPECTED_IDS)
    fields = source.EXPECTED_FIELD_DECLARATIONS
    attributes = []
    geometry = {"rings": [_clockwise(extent["xmin"], extent["ymin"], extent["xmax"], extent["ymax"])]}
    for object_id in ids:
        row = {
            "OBJECTID": object_id,
            "RANK": float(object_id),
            "PORT": float(1000 + object_id),
            "TYPE": "Coastal",
            "PORTNAME": f"Port {object_id}",
            "TOTAL": float(object_id * 10),
            "DOMESTIC": float(object_id * 2),
            "FOREIGN_": float(object_id * 8),
            "IMPORTS": float(object_id * 3),
            "EXPORTS": float(object_id * 5),
            "Shape__Area": float(object_id * 100),
            "Shape__Length": float(object_id * 20),
        }
        attributes.append(row)
    layer = {
        "name": "Principal_Ports",
        "id": 0,
        "serviceItemId": source.BTS_ITEM_ID,
        "geometryType": "esriGeometryPolygon",
        "objectIdField": "OBJECTID",
        "maxRecordCount": 2000,
        "hasZ": False,
        "hasM": False,
        "extent": {**extent, "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
        "fields": copy.deepcopy(fields),
    }
    service = {
        "serviceItemId": source.BTS_ITEM_ID,
        "maxRecordCount": 2000,
        "copyrightText": "U.S. Army Corps of Engineers",
        "serviceDescription": "Principal Ports for CY 2023",
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
    }
    item = {
        "id": source.BTS_ITEM_ID,
        "owner": "usace_iwr_cac",
        "access": "public",
        "url": source.SERVICE_URL,
        "title": "Principal Ports",
        "description": "USACE ports for CY 2023",
        "snippet": "USACE ports for CY 2023",
    }
    page = {
        "geometryType": "esriGeometryPolygon",
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
        "features": [{"attributes": row, "geometry": geometry} for row in attributes],
    }
    body_docs = {
        "data_gov_landing": (
            f'"publisher": "United States Army Corp of Engineers (USACE)" '
            f'{source.DATA_GOV_RESOURCE_DOI} {source.BTS_CATALOG_DOI} CY 2023 '
            "https://transtats.bts.gov/NTADmetadata/USDOT_BTS_NTAD_Principal_Ports.xml"
        ).encode(),
        "bts_metadata": (
            f"{source.DATA_GOV_RESOURCE_DOI} CY 2023 United States Army Corp of Engineers (USACE) "
            "Waterborne Commerce Statistics Center"
        ).encode(),
        "bts_item": json.dumps(item).encode(),
        "service_before": json.dumps(service).encode(),
        "layer_before": json.dumps(layer).encode(),
        "count_before": json.dumps({"count": 150}).encode(),
        "ids_before": json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": ids}).encode(),
        "page_000": json.dumps(page).encode(),
        "layer_after": json.dumps(layer).encode(),
        "count_after": json.dumps({"count": 150}).encode(),
        "ids_after": json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": ids}).encode(),
    }
    return {name: (body, {}) for name, body in body_docs.items()}


class PrincipalPortsTests(unittest.TestCase):
    def test_asset_is_registered_and_scheduled_refresh_is_stopped(self):
        self.assertIn(principal_ports_usace, hifld_ingest_assets)
        self.assertEqual(principal_ports_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        result = principal_ports_source_complete({
            "feature_count": 150,
            "source_population_complete": True,
            "source_invalid_geometry_count_without_repair": 8,
        })
        self.assertTrue(result.passed)
        self.assertFalse(result.metadata["release_accepted"].value)
        self.assertEqual(result.metadata["unrepaired_invalid_source_geometries"].value, 8)

    def test_esri_multipart_rings_are_preserved_as_native_xy_without_repair(self):
        outer = _clockwise(0, 0, 10, 10)
        hole = _counterclockwise(2, 2, 3, 3)
        second_shell = _clockwise(20, 20, 30, 30)
        parsed = from_wkb(source._polygon_wkb({"rings": [outer, hole, second_shell]}))
        self.assertEqual(parsed.geom_type, "MultiPolygon")
        self.assertEqual(len(parsed.geoms), 2)
        self.assertEqual(list(parsed.geoms[0].exterior.coords), [tuple(p) for p in outer])
        self.assertEqual(list(parsed.geoms[0].interiors[0].coords), [tuple(p) for p in hole])
        self.assertEqual(list(parsed.geoms[1].exterior.coords), [tuple(p) for p in second_shell])

    def test_polygon_rejects_non_xy_nonfinite_open_and_contradictory_crs(self):
        ring = _clockwise(0, 0, 1, 1)
        cases = [
            ({"rings": [[*ring[:-1], [1, 1, 7], ring[-1]]]}, "source_polygon_coordinate_not_finite_xy"),
            ({"rings": [[*ring[:-1], [float("nan"), 0], ring[-1]]]}, "source_polygon_coordinate_not_finite_xy"),
            ({"rings": [ring[:-1]]}, "source_polygon_ring_invalid_or_open"),
            ({"rings": [ring], "hasM": True}, "source_polygon_not_xy"),
            ({"rings": [ring], "spatialReference": {"wkid": 4326}}, "feature_geometry_crs_drift"),
        ]
        for geometry, reason in cases:
            with self.subTest(reason=reason), self.assertRaisesRegex(ValueError, reason):
                source._polygon_wkb(geometry)

    def test_parquet_preserves_native_field_types_values_and_epsg3857(self):
        captures = _captures()
        fields, features, _audit = source._validate(captures)
        table = pq.read_table(pa.BufferReader(source._parquet_bytes(fields, features[:2])))
        self.assertEqual(table.column_names, [field["name"] for field in fields] + ["geometry"])
        self.assertEqual(table.schema.field("PORT").type, pa.float64())
        self.assertEqual(table["PORT"].to_pylist(), [1001.0, 1002.0])
        self.assertEqual(table["OBJECTID"].to_pylist(), [1, 2])
        metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(metadata["columns"]["geometry"]["geometry_types"], ["MultiPolygon"])
        self.assertEqual(metadata["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 3857})

    def test_complete_extract_validates_count_identity_fields_and_port_roster(self):
        captures = _captures()
        _fields, features, audit = source._validate(captures)
        self.assertEqual(len(features), 150)
        self.assertTrue(audit["source_population_complete"])
        self.assertEqual(audit["source_id_min"], 1001.0)
        self.assertEqual(audit["source_polygon_vertex_count"], 750)
        self.assertEqual(audit["source_wkid"], 3857)
        self.assertEqual(audit["source_invalid_geometry_count_without_repair"], 0)

    def test_schema_count_id_and_transfer_limit_drift_are_rejected(self):
        mutations = [
            ("field type", lambda c: _mutate_json(c, "layer_before", lambda d: d["fields"][2].update(type="esriFieldTypeString")), "source_field_schema_drift"),
            ("count", lambda c: _mutate_json(c, "count_after", lambda d: d.update(count=149)), "source_count_drift"),
            ("ID roster", lambda c: _mutate_json(c, "ids_after", lambda d: d["objectIds"].pop()), "source_objectid_population_drift"),
            ("transfer limit", lambda c: _mutate_json(c, "page_000", lambda d: d.update(exceededTransferLimit=True)), "source_page_error_or_transfer_limit"),
        ]
        for name, mutate, reason in mutations:
            with self.subTest(name=name):
                captures = _captures()
                mutate(captures)
                with self.assertRaisesRegex(ValueError, reason):
                    source._validate(captures)


def _mutate_json(captures, name, mutator):
    body, receipt = captures[name]
    document = json.loads(body)
    mutator(document)
    captures[name] = (json.dumps(document).encode(), receipt)


if __name__ == "__main__":
    unittest.main()
