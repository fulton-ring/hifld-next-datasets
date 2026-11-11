"""Focused source contract tests for the FUDS property polygon draft adapter."""
from __future__ import annotations

import json
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.assets.fuds_property_boundaries import (
    fuds_property_boundaries_complete,
    fuds_property_boundaries_draft_schedule,
    fuds_property_boundaries_ingest,
    fuds_property_boundaries_geometry_quality,
)
from dagster_hifld.sources import fuds_property_boundaries as source


class FudsPropertyBoundariesTests(unittest.TestCase):
    def setUp(self):
        self.fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
            {"name": "SITE_ID", "type": "esriFieldTypeString", "length": 32},
            {"name": "SHAPE", "type": "esriFieldTypeGeometry"},
        ]
        self.layer = {
            "id": 4,
            "name": source.LAYER_NAME,
            "geometryType": "esriGeometryPolygon",
            "hasZ": True,
            "hasM": True,
            "extent": {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0, "spatialReference": {"latestWkid": 4326}},
            "description": "FUDS program is limited to properties prior to October 17, 1986.",
            "fields": self.fields,
        }
        self.service = {
            "serviceItemId": source.SERVICE_ITEM_ID,
            "mapName": "FUDS Data",
            "copyrightText": "US Army Corps of Engineers",
            "description": "Formerly Used Defense Sites (FUDS) properties and projects",
            "layers": [{"id": 4, "name": source.LAYER_NAME}],
        }
        feature = {
            "attributes": {"OBJECTID": 9, "SITE_ID": "0009"},
            "geometry": {"rings": [[[0, 0, 10, None], [0, 1, 12, None], [1, 1, 11, None], [1, 0, 10, None], [0, 0, 10, None]]]},
        }
        self.docs = {
            "service_before": self.service,
            "service_after": self.service,
            "layer_before": self.layer,
            "layer_after": self.layer,
            "count_before": {"count": 1},
            "count_after": {"count": 1},
            "ids_before": {"objectIds": [9]},
            "ids_after": {"objectIds": [9]},
            "ids_seed": {"objectIds": [9]},
            "page_000": {"spatialReference": {"latestWkid": 4326}, "features": [feature]},
        }
        self.captures = {
            name: (json.dumps(value).encode(), {})
            for name, value in self.docs.items()
        }

    def test_exact_layer_population_and_xyz_m_round_trip_to_native_candidate(self):
        with patch.multiple(source, EXPECTED_COUNT=1, EXPECTED_ID_SHA256="", EXPECTED_SCHEMA_SHA256="",
                            EXPECTED_RECORD_SHA256="", EXPECTED_GEOMETRY_SHA256=""):
            layer, fields, extracted = source._validate_captures(self.captures)
            native = source._parquet_bytes(fields, extracted["features"], extracted["audit"]["source_spatial_reference"])
        self.assertEqual(layer["name"], "FUDS Property Polygon")
        self.assertEqual(extracted["audit"]["source_feature_count"], 1)
        self.assertEqual(extracted["audit"]["source_crs_epsg"], 4326)
        table = pq.read_table(pa.BufferReader(native))
        self.assertEqual(table.column("OBJECTID").to_pylist(), [9])
        self.assertEqual(table.column("SITE_ID").to_pylist(), ["0009"])
        self.assertEqual([v["m"] for v in table.column("source_geometry")[0].as_py()["rings"][0]], [None] * 5)
        self.assertEqual(table.column("source_geometry")[0].as_py()["spatialReference"], json.dumps({"latestWkid": 4326}, sort_keys=True, separators=(",", ":")))
        self.assertIn(b"geo", table.schema.metadata)
        self.assertEqual(json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"]["geometry_types"], ["Polygon Z", "MultiPolygon Z"])

    def test_id_population_drift_fails_closed(self):
        altered = dict(self.captures)
        altered["ids_after"] = (json.dumps({"objectIds": [10]}).encode(), {})
        with patch.multiple(source, EXPECTED_COUNT=1, EXPECTED_ID_SHA256="", EXPECTED_SCHEMA_SHA256="",
                            EXPECTED_RECORD_SHA256="", EXPECTED_GEOMETRY_SHA256=""):
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source._validate_captures(altered)

    def _canonical_pins(self) -> dict[str, str]:
        features = sorted(self.docs["page_000"]["features"], key=lambda f: f["attributes"]["OBJECTID"])
        _, schema_sha = source._profile_schema(self.layer)
        return {
            "EXPECTED_COUNT": 1,
            "EXPECTED_ID_SHA256": source._sha(source._canonical([9])),
            "EXPECTED_SCHEMA_SHA256": schema_sha,
            "EXPECTED_RECORD_SHA256": source._sha(source._canonical(features)),
            "EXPECTED_GEOMETRY_SHA256": source._sha(source._canonical(
                [[f["attributes"]["OBJECTID"], f["geometry"]["rings"]] for f in features])),
        }

    def test_unexpected_extra_column_fails_closed(self):
        altered = dict(self.captures)
        feature = json.loads(altered["page_000"][0])
        feature["features"][0]["attributes"]["UNMAPPED_EXTRA"] = "unexpected"
        altered["page_000"] = (json.dumps(feature).encode(), {})
        with patch.multiple(source, **{**{k: "" for k in self._canonical_pins()}, "EXPECTED_COUNT": 1}):
            with self.assertRaisesRegex(ValueError, "source_record_schema_or_geometry_drift"):
                source._validate_captures(altered)

    def test_missing_or_renamed_source_field_fails_closed(self):
        renamed = dict(self.captures)
        feature = json.loads(renamed["page_000"][0])
        feature["features"][0]["attributes"] = {"OBJECTID": 9, "SITE_ID_RENAMED": "0009"}
        renamed["page_000"] = (json.dumps(feature).encode(), {})
        dropped = dict(self.captures)
        feature = json.loads(dropped["page_000"][0])
        dropped_feature = {**feature, "features": [dict(feature["features"][0])]}
        dropped_feature["features"][0]["attributes"] = {"SITE_ID": "0009"}
        dropped["page_000"] = (json.dumps(dropped_feature).encode(), {})
        nulled = dict(self.captures)
        feature = json.loads(nulled["page_000"][0])
        nulled_feature = {**feature, "features": [dict(feature["features"][0])]}
        nulled_feature["features"][0]["attributes"] = {"OBJECTID": None, "SITE_ID": "0009"}
        nulled["page_000"] = (json.dumps(nulled_feature).encode(), {})
        with patch.multiple(source, **{**{k: "" for k in self._canonical_pins()}, "EXPECTED_COUNT": 1}):
            with self.assertRaisesRegex(ValueError, "source_record_schema_or_geometry_drift"):
                source._validate_captures(renamed)
            with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
                source._validate_captures(dropped)
            with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
                source._validate_captures(nulled)

    def test_source_value_loss_against_pinned_baseline_fails_closed(self):
        altered = dict(self.captures)
        feature = json.loads(altered["page_000"][0])
        feature["features"][0]["attributes"]["SITE_ID"] = "9"
        altered["page_000"] = (json.dumps(feature).encode(), {})
        with patch.multiple(source, **self._canonical_pins()):
            with self.assertRaisesRegex(ValueError, "source_value_or_geometry_baseline_drift"):
                source._validate_captures(altered)
        intact = dict(self.captures)
        with patch.multiple(source, **self._canonical_pins()):
            source._validate_captures(intact)

    def test_geometry_coverage_extent_drift_fails_closed(self):
        altered_layer = {**self.layer, "extent": {**self.layer["extent"], "xmin": 5.0}}
        altered = dict(self.captures)
        for name in ("layer_before", "layer_after"):
            altered[name] = (json.dumps(altered_layer).encode(), {})
        with patch.multiple(source, **{**{k: "" for k in self._canonical_pins()}, "EXPECTED_COUNT": 1}):
            with self.assertRaisesRegex(ValueError, "source_coverage_extent_drift"):
                source._validate_captures(altered)

    def test_incomplete_or_truncated_page_response_fails_closed(self):
        incomplete = {name: value for name, value in self.captures.items() if name != "page_000"}
        with patch.multiple(source, **{**{k: "" for k in self._canonical_pins()}, "EXPECTED_COUNT": 1}):
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
                source._validate_captures(incomplete)
        truncated = dict(self.captures)
        page = json.loads(truncated["page_000"][0])
        page["exceededTransferLimit"] = True
        truncated["page_000"] = (json.dumps(page).encode(), {})
        with patch.multiple(source, **{**{k: "" for k in self._canonical_pins()}, "EXPECTED_COUNT": 1}):
            with self.assertRaisesRegex(ValueError, "source_page_error_or_truncated"):
                source._validate_captures(truncated)

    def test_pipeline_wiring_is_candidate_only_and_stopped(self):
        self.assertEqual(fuds_property_boundaries_ingest.key.path[-1], source.FILE_SLUG)
        self.assertEqual(next(iter(fuds_property_boundaries_complete.check_specs)).asset_key, fuds_property_boundaries_ingest.key)
        self.assertEqual(fuds_property_boundaries_draft_schedule.default_status.value, "STOPPED")
        self.assertTrue(next(iter(fuds_property_boundaries_geometry_quality.check_specs)).blocking)
        self.assertFalse(fuds_property_boundaries_geometry_quality.computation.node_def.compute_fn.decorated_fn({"source_invalid_geometry_count": 16}).passed)


if __name__ == "__main__":
    unittest.main()
