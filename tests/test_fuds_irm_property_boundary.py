"""Focused source-faithfulness tests for the USACE IRM property boundary."""
from __future__ import annotations

import json
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb, is_valid
from shapely.geometry import Polygon

from dagster_hifld.sources.fuds_irm_property_boundary import (
    DATASET_SLUG, EXPECTED_COUNT, LAYER_ID, LAYER_NAME, _parquet_bytes,
    _source_polygon_geometry,
)
from dagster_hifld.definitions import defs
from dagster import DefaultScheduleStatus


FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
    {"name": "FEAT", "type": "esriFieldTypeString", "nullable": True},
]


class IrmPropertyBoundaryTests(unittest.TestCase):
    def test_candidate_checks_and_stopped_refresh_are_in_definitions(self):
        checks = {key.name for check in defs.asset_checks for key in check.check_keys
                  if key.asset_key.path[-2:] == [DATASET_SLUG, DATASET_SLUG]}
        self.assertTrue({"fuds_irm_property_boundary_complete",
                         "fuds_irm_property_boundary_geometry_quality"} <= checks)
        schedules = {schedule.name: schedule for schedule in defs.schedules}
        schedule = schedules["fuds_irm_property_boundary_draft_refresh_schedule"]
        self.assertIs(schedule.default_status, DefaultScheduleStatus.STOPPED)

    def test_source_identity_and_baseline_are_layer_specific(self):
        self.assertEqual((LAYER_ID, LAYER_NAME, EXPECTED_COUNT), (8, "IRM Property Boundary", 601))
        self.assertEqual(DATASET_SLUG, "interim-risk-management-irm-property-boundary")

    def test_native_json_and_ring_order_are_retained_in_candidate(self):
        ring = [[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [2.0, 0.0], [0.0, 0.0]]
        geom = {"rings": [ring]}
        feature = {"attributes": {"OBJECTID": 1, "FEAT": "sample"}, "geometry": geom}
        body = _parquet_bytes(FIELDS, [feature], {"wkid": 4326, "latestWkid": 4326})
        table = pq.read_table(pa.BufferReader(body), use_threads=False)
        self.assertEqual(json.loads(table.column("source_geometry")[0].as_py()), geom)
        projected = from_wkb(table.column("geometry")[0].as_py())
        self.assertEqual(list(projected.exterior.coords), [tuple(point) for point in ring])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Polygon", "MultiPolygon"])

    def test_adapter_rejects_undeclared_dimensions(self):
        for ring in (
            [[0, 0, 1], [1, 0, 1], [1, 1, 1], [0, 1, 1], [0, 0, 1]],
            [[0, 0, 1, 9], [1, 0, 1, 9], [1, 1, 1, 9], [0, 1, 1, 9], [0, 0, 1, 9]],
        ):
            with self.assertRaises(ValueError):
                _source_polygon_geometry([ring])

    def test_invalid_input_geometry_is_not_repaired(self):
        bowtie = [[0, 0], [2, 2], [0, 2], [2, 0], [0, 0]]
        result = _source_polygon_geometry([bowtie])
        self.assertIsInstance(result, Polygon)
        self.assertFalse(is_valid(result))


if __name__ == "__main__":
    unittest.main()
