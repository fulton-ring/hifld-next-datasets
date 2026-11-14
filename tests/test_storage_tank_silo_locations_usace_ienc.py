import json
import struct
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.storage_tank_silo_locations_usace_ienc import (
    storage_tank_silo_locations_usace_ienc, storage_tank_silo_locations_usace_ienc_complete,
    storage_tank_silo_locations_usace_ienc_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import storage_tank_silo_locations_usace_ienc as source
from dagster_hifld.sources.epa_frs_common import _canonical


class StorageTankSiloLocationsUsaceIencTests(unittest.TestCase):
    def _point(self, oid):
        return {"x": -91.5 + (oid % 200) * 0.001, "y": 30.8 + (oid % 100) * 0.001}

    def _documents(self):
        service = {"mapName": "STORAGE_TANK_SILO_POINT", "copyrightText": "USACE IENC",
                   "layers": [{"id": 25, "name": "STORAGE_TANK_SILO_POINT", "geometryType": "esriGeometryPoint"}]}
        layer = {"id": 25, "name": "STORAGE_TANK_SILO_POINT", "geometryType": "esriGeometryPoint",
                 "maxRecordCount": 2000, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
                         "title": "Inland Electronic Navigational Charts",
                         "description": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/STORAGE_TANK_SILO_POINT", "type": "MapServer"}]},
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
                         (30000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                         for field in source.ATTRIBUTE_FIELDS}
                rows.append({"attributes": attrs, "geometry": self._point(oid)})
            docs[f"page_{page_index:03d}"] = {"spatialReference": {"wkid": 4326}, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(storage_tank_silo_locations_usace_ienc, hifld_ingest_assets)
        self.assertEqual(storage_tank_silo_locations_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = storage_tank_silo_locations_usace_ienc_complete(
            {"feature_count": source.EXPECTED_COUNT, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = storage_tank_silo_locations_usace_ienc_complete(
            {"feature_count": source.EXPECTED_COUNT - 1, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         [f"page_{index:03d}" for index in range(5)])
        self.assertIn("objectIds=1%2C2%2C3", urls["page_000"])
        self.assertIn("objectIds=1001%2C1002%2C1003", urls["page_004"])
        self.assertNotIn("objectIds=1079", urls["page_004"])
        self.assertIn("returnZ=true", urls["page_000"])
        self.assertIn("returnM=true", urls["page_000"])
        self.assertEqual(len(urls["page_000"].split("objectIds=")[1].split("&")[0].split("%2C")), 250)
        self.assertEqual(len(urls["page_004"].split("objectIds=")[1].split("&")[0].split("%2C")), 78)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 1078)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 1078)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], 1078)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["feature_id_distinct_count"], 1078)
        self.assertEqual(audit["feature_id_duplicate_count"], 0)
        self.assertEqual(audit["category_distinct_count"], 1078)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertEqual(features[0]["attributes"]["Scale_Minimum"], 30000)
        self.assertIn("not a nationwide storage tank or silo inventory", audit["source_coverage_scope"])

    def test_point_wkb_is_xy_only_and_domain_checked(self):
        wkb = source._esri_point_wkb({"x": -90.5, "y": 38.25})
        self.assertEqual(struct.unpack("<BI", wkb[:5]), (1, 1))
        self.assertEqual(struct.unpack("<dd", wkb[5:]), (-90.5, 38.25))
        for bad in ({"x": 200.0, "y": 38.25}, {"x": -90.5, "y": 95.0},
                    {"x": float("nan"), "y": 38.25}, {"x": -90.5, "y": "38.25"},
                    {"y": 38.25}, {"x": -90.5}):
            with self.assertRaisesRegex(ValueError, "source_point"):
                source._esri_point_wkb(bad)

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_count_or_id_population_drift"),
            ("schema rename", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("schema length", lambda d: d["layer_before"]["fields"][2].update(length=254),
             "source_field_schema_drift"),
            ("schema type", lambda d: d["layer_before"]["fields"][6].update(type="esriFieldTypeDouble"),
             "source_field_schema_drift"),
            ("unexpected attribute", lambda d: d["page_000"]["features"][0]["attributes"].update(
                Extra_Column="value"), "source_attribute_roster_or_order_drift"),
            ("string oid", lambda d: d["page_000"]["features"][0]["attributes"].update(OBJECTID="1"),
             "source_oid_value_type_drift"),
            ("integer drift", lambda d: d["page_000"]["features"][0]["attributes"].update(
                Scale_Minimum="30000"), "source_integer_value_type_drift"),
            ("string drift", lambda d: d["page_000"]["features"][0]["attributes"].update(
                Product=30000), "source_string_value_type_drift"),
            ("transfer", lambda d: d["page_001"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("page error", lambda d: d["page_002"].update(error={"message": "boom"}),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_001"].update(spatialReference={"wkid": 3857}), "crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("page population", lambda d: d["page_004"]["features"].pop(),
             "source_page_population_incomplete"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True),
             "source_layer_dimensional_drift"),
            ("page dimensional", lambda d: d["page_000"].update(hasZ=True),
             "source_page_dimensional_drift"),
            ("geometry dimensional", lambda d: d["page_000"]["features"][0]["geometry"].update(hasZ=True),
             "source_geometry_dimensional_drift"),
            ("geometry z ordinate", lambda d: d["page_000"]["features"][0]["geometry"].update(z=1.0),
             "source_point_geometry_structure_drift"),
            ("bad ordinate", lambda d: d["page_000"]["features"][0]["geometry"].update(x="west"),
             "source_xy_ordinate_contract_drift"),
            ("outside coverage", lambda d: d["page_000"]["features"][0]["geometry"].update(x=-120.0),
             "source_geometry_outside_declared_coverage"),
            ("bts identity", lambda d: d["bts_item"].update(owner="someone_else"),
             "bts_ienc_item_identity_drift"),
            ("bts link", lambda d: d["bts_item"].update(
                description="https://example.invalid/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"),
             "bts_ienc_official_distribution_link_drift"),
            ("service roster", lambda d: d["service_before"]["layers"].append(
                {"id": 26, "name": "EXTRA", "geometryType": "esriGeometryPoint"}),
             "official_storage_tank_silo_point_service_drift"),
            ("directory", lambda d: d["service_directory"].update(
                services=[{"name": "IENC_Feature_Classes/STORAGE_TANK_SILO_AREA", "type": "MapServer"}]),
             "official_ienc_service_directory_drift"),
            ("service changed", lambda d: d["service_after"].update(copyrightText="someone else"),
             "source_service_or_layer_changed_during_capture"),
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
                source.capture_storage_tank_silo_locations_usace_ienc(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()
