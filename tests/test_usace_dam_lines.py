import json
import unittest

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_dam_lines import (
    usace_dam_lines_complete, usace_dam_lines_draft_schedule,
    usace_dam_lines_usace_ienc,
)
from dagster_hifld.sources import usace_dam_lines as source
from dagster_hifld.sources.epa_frs_common import _canonical


class UsaceDamLinesTests(unittest.TestCase):
    def _documents(self):
        service = {"mapName": "DAM_LINE", "copyrightText": "USACE IENC",
                   "layers": [{"id": 35, "name": "DAM_LINE", "geometryType": "esriGeometryPolyline"}]}
        layer = {"id": 35, "name": "DAM_LINE", "geometryType": "esriGeometryPolyline",
                 "maxRecordCount": 2000, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
                          "title": "Inland Electronic Navigational Charts",
                          "description": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/DAM_LINE", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            "service_after": service, "layer_after": layer,
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }
        rows = []
        for oid in source.EXPECTED_IDS:
            attrs = {field["name"]: oid if field["name"] == "OBJECTID" else
                     (30000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                     for field in source.ATTRIBUTE_FIELDS}
            rows.append({"attributes": attrs, "geometry": {"paths": [[[-90 + oid / 10000, 35], [-89, 36]]]}})
        docs["page_000"] = {"spatialReference": {"wkid": 4326}, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(usace_dam_lines_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_dam_lines_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = usace_dam_lines_complete({"feature_count": 40, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 40)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 80)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertIn("does not represent a nationwide dam inventory", audit["source_coverage_scope"])

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]), "source_count_or_id"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"), "source_field_schema_drift"),
            ("transfer", lambda d: d["page_000"].update(exceededTransferLimit=True), "source_page_error"),
            ("CRS", lambda d: d["page_000"].update(spatialReference={"wkid": 3857}), "crs_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))


if __name__ == "__main__":
    unittest.main()
