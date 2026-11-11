import json
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_navigation_lights import (
    usace_navigation_lights_complete,
    usace_navigation_lights_draft_schedule,
    usace_navigation_lights_usace_ienc,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_navigation_lights as source
from dagster_hifld.sources.epa_frs_common import _canonical


class UsaceNavigationLightsTests(unittest.TestCase):
    def _documents(self):
        service = {"mapName": "LIGHTS_POINT", "copyrightText": "USACE IENC",
                   "layers": [{"id": 16, "name": "LIGHTS_POINT", "geometryType": "esriGeometryPoint"}]}
        layer = {"id": 16, "name": "LIGHTS_POINT", "geometryType": "esriGeometryPoint",
                 "maxRecordCount": 2000, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
                          "access": "public", "title": "Inland Electronic Navigational Charts",
                          "description": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/LIGHTS_POINT", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            "service_after": json.loads(json.dumps(service)), "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }
        for page_index in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
            rows = []
            for oid in source.EXPECTED_IDS[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]:
                attrs = {field["name"]: oid if field["name"] == "OBJECTID" else
                         (22000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                         for field in source.ATTRIBUTE_FIELDS}
                rows.append({"attributes": attrs,
                             "geometry": {"x": -79.5 + oid / 100000, "y": 40.0}})
            docs[f"page_{page_index:03d}"] = {"spatialReference": {"wkid": 4326}, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(usace_navigation_lights_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_navigation_lights_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = usace_navigation_lights_complete({"feature_count": 8892, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = usace_navigation_lights_complete({"feature_count": 8891, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         [f"page_{index:03d}" for index in range(36)])
        self.assertIn("objectIds=1%2C2%2C3", urls["page_000"])
        self.assertIn("objectIds=251%2C252%2C253", urls["page_001"])
        self.assertIn("objectIds=8751%2C8752%2C8753", urls["page_035"])
        self.assertNotIn("objectIds=8893", urls["page_035"])
        self.assertIn("returnZ=true", urls["page_000"])
        self.assertIn("returnM=true", urls["page_000"])
        self.assertEqual(len(urls["page_035"].split("objectIds=")[1].split("&")[0].split("%2C")), 142)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 8892)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], 8892)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], 8892)
        self.assertEqual(audit["geometry_bounds"],
                         [-79.5 + 1 / 100000, 40.0, -79.5 + 8892 / 100000, 40.0])
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertIn("not a nationwide navigation-light inventory", audit["source_coverage_scope"])

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_count_or_id_population_drift"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("extent", lambda d: d["layer_before"]["extent"].update(xmin=-100.0),
             "source_layer_identity_extent_or_limit_drift"),
            ("transfer", lambda d: d["page_001"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_002"].update(spatialReference={"wkid": 3857}), "crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("ordinate", lambda d: d["page_000"]["features"][0]["geometry"].update(x=200.0),
             "source_point_ordinate_contract_drift"),
            ("z ordinate", lambda d: d["page_000"]["features"][0]["geometry"].update(z=1.0),
             "source_point_geometry_structure_drift"),
            ("attribute type loss", lambda d: d["page_000"]["features"][0]["attributes"].update(
                Feature_ID=123), "source_string_value_type_drift:Feature_ID"),
            ("bts owner", lambda d: d["bts_item"].update(owner="third_party"),
             "official_bts_item_identity_drift"),
            ("bts delegation marker", lambda d: d["bts_item"].update(
                description="https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes"),
             "official_bts_distribution_reference_drift"),
            ("service directory", lambda d: d["service_directory"].update(
                services=[{"name": "IENC_Feature_Classes/DAYMARK_POINT", "type": "MapServer"}]),
             "official_ienc_service_directory_drift"),
            ("service changed", lambda d: d["service_after"].update(mapName="SOME_OTHER_MAP"),
             "source_service_or_layer_changed_during_capture"),
            ("roster", lambda d: d.pop("count_after"), "source_capture_name_roster_drift"),
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
                source.capture_usace_navigation_lights(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()
