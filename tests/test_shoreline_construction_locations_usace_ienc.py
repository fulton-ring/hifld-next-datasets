import json
import struct
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.shoreline_construction_locations_usace_ienc import (
    shoreline_construction_locations_usace_ienc, shoreline_construction_locations_usace_ienc_complete,
    shoreline_construction_locations_usace_ienc_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import shoreline_construction_locations_usace_ienc as source
from dagster_hifld.sources.epa_frs_common import _canonical


class ShorelineConstructionLocationsUsaceIencTests(unittest.TestCase):
    def _point(self, oid):
        return {"x": -93.5 + (oid % 100) * 0.02, "y": 35.0 + (oid // 100) * 0.004}

    def _documents(self):
        service = {"mapName": "SHORELINE_CONSTRUCTION_POINT", "copyrightText": "USACE IENC",
                   "layers": [{"id": 23, "name": "SHORELINE_CONSTRUCTION_POINT", "geometryType": "esriGeometryPoint"}]}
        layer = {"id": 23, "name": "SHORELINE_CONSTRUCTION_POINT", "geometryType": "esriGeometryPoint",
                 "maxRecordCount": 2000, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
                         "title": "Inland Electronic Navigational Charts",
                         "description": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/SHORELINE_CONSTRUCTION_POINT", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            "service_after": service, "layer_after": layer,
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }
        for page_index in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
            rows = []
            for oid in source.EXPECTED_IDS[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]:
                attrs = {field["name"]: oid if field["name"] == "OBJECTID" else
                         (60000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                         for field in source.ATTRIBUTE_FIELDS}
                rows.append({"attributes": attrs, "geometry": self._point(oid)})
            docs[f"page_{page_index:03d}"] = {"spatialReference": {"wkid": 4326}, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(shoreline_construction_locations_usace_ienc, hifld_ingest_assets)
        self.assertEqual(shoreline_construction_locations_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = shoreline_construction_locations_usace_ienc_complete({"feature_count": 4719, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = shoreline_construction_locations_usace_ienc_complete({"feature_count": 4718, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         [f"page_{index:03d}" for index in range(19)])
        self.assertIn("objectIds=1%2C2%2C3", urls["page_000"])
        self.assertIn("objectIds=4501%2C4502%2C4503", urls["page_018"])
        self.assertNotIn("objectIds=4720", urls["page_018"])
        self.assertIn("returnZ=true", urls["page_000"])
        self.assertIn("returnM=true", urls["page_000"])
        self.assertEqual(len(urls["page_018"].split("objectIds=")[1].split("&")[0].split("%2C")), 219)
        self.assertEqual(len(urls["page_000"].split("objectIds=")[1].split("&")[0].split("%2C")), 250)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 4719)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 4719)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], 4719)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["feature_id_distinct_count"], 4719)
        self.assertEqual(audit["feature_id_duplicate_count"], 0)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertIn("not a nationwide shoreline-construction inventory", audit["source_coverage_scope"])

    def test_point_wkb_is_xy_only_and_preserves_source_coordinates(self):
        wkb = source._esri_point_wkb({"x": -93.5, "y": 35.25})
        self.assertEqual(struct.unpack("<Bidd", wkb), (1, 1, -93.5, 35.25))
        with self.assertRaisesRegex(ValueError, "source_point_ordinate_type_drift"):
            source._esri_point_wkb({"x": None, "y": 35.25})
        with self.assertRaisesRegex(ValueError, "source_point_ordinate_type_drift"):
            source._esri_point_wkb({"x": "x", "y": 35.25})
        with self.assertRaisesRegex(ValueError, "source_point_outside_epsg4326_bounds"):
            source._esri_point_wkb({"x": -181.0, "y": 35.25})
        with self.assertRaisesRegex(ValueError, "source_geometry_outside_declared_coverage"):
            source._esri_point_wkb({"x": -120.0, "y": 35.5})

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_count_or_id_population_drift"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("transfer", lambda d: d["page_001"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_001"].update(spatialReference={"wkid": 3857}), "crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True),
             "source_layer_dimensional_drift"),
            ("page dimensional", lambda d: d["page_000"].update(hasZ=True),
             "source_page_dimensional_drift"),
            ("geometry dimensional", lambda d: d["page_000"]["features"][0]["geometry"].update(z=1.0),
             "source_point_geometry_structure_drift"),
            ("missing ordinates", lambda d: d["page_000"]["features"][0]["geometry"].pop("y"),
             "source_point_ordinates_missing"),
            ("outside coverage", lambda d: d["page_000"]["features"][0]["geometry"].update(
                x=-120.0, y=35.5),
             "source_geometry_outside_declared_coverage"),
            ("service changed", lambda d: d["service_after"].update(mapName="OTHER"),
             "source_service_or_layer_changed_during_capture"),
            ("bts identity", lambda d: d["bts_item"].update(owner="someone_else"),
             "bts_ienc_item_identity_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_shoreline_construction_locations_usace_ienc(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()
