import json
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_overhead_pipeline_lines import (
    usace_overhead_pipeline_lines_complete, usace_overhead_pipeline_lines_draft_schedule,
    usace_overhead_pipeline_lines_usace_ienc,
)
from dagster_hifld.sources import usace_overhead_pipeline_lines as source
from dagster_hifld.sources.epa_frs_common import _canonical


class UsaceOverheadPipelineLinesTests(unittest.TestCase):
    def test_asset_is_registered_refresh_stopped_and_gate_remains_unapproved(self):
        self.assertIn(usace_overhead_pipeline_lines_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_overhead_pipeline_lines_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = usace_overhead_pipeline_lines_complete({"feature_count": 92, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)

    def test_capture_page_roster_covers_source_ids_without_overlapping(self):
        urls = source._capture_roster(source.EXPECTED_IDS)
        page_names = [name for name, _url in urls if name.startswith("page_")]
        self.assertEqual(page_names, [f"page_{index:03d}" for index in range(1)])
        self.assertIn("objectIds=1%2C2%2C3", dict(urls)["page_000"])
        self.assertIn("objectIds=1%2C2%2C3", dict(urls)["page_000"])
        self.assertIn("returnZ=true", dict(urls)["page_000"])
        self.assertIn("returnM=true", dict(urls)["page_000"])

    def _documents(self):
        service = {"mapName": "OVERHEAD_PIPELINE_LINE", "copyrightText": "USACE IENC",
                   "layers": [{"id": 44, "name": "OVERHEAD_PIPELINE_LINE", "geometryType": "esriGeometryPolyline"}]}
        layer = {"id": 44, "name": "OVERHEAD_PIPELINE_LINE", "geometryType": "esriGeometryPolyline",
                 "maxRecordCount": 2000, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": source.EXPECTED_EXTENT, "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS",
                         "title": "Inland Electronic Navigational Charts",
                         "description": "Official service https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes; "
                                        "USACE_IENC_Master_Service_gdb.zip"},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/OVERHEAD_PIPELINE_LINE", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT}, "ids_before": {"objectIds": source.EXPECTED_IDS},
            "layer_after": json.loads(json.dumps(layer)), "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS}, "service_after": json.loads(json.dumps(service)),
        }
        for page_index in range(1):
            ids = source.EXPECTED_IDS[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]
            rows = []
            for oid in ids:
                attrs = {field["name"]: oid if field["name"] == "OBJECTID" else
                         (30000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                         for field in source.ATTRIBUTE_FIELDS}
                rows.append({"attributes": attrs, "geometry": {"paths": [[[-90 + oid / 10000, 35],
                                                                             [-89 + oid / 10000, 36]]]}})
            docs[f"page_{page_index:03d}"] = {"spatialReference": {"latestWkid": 4326}, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def test_full_population_schema_ids_and_native_xy_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 92)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 184)
        self.assertEqual(audit["geometry_valid_count"], 92)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")

    def test_missing_rows_schema_drift_transfer_limit_and_crs_override_fail_closed(self):
        mutations = [
            ("missing IDs", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]), "source_count_or_id"),
            ("field rename", lambda d: d["layer_before"]["fields"][2].update(name="renamed"), "source_field_schema_drift"),
            ("page truncation", lambda d: d["page_000"].update(exceededTransferLimit=True), "source_page_error"),
            ("page CRS override", lambda d: d["page_000"].update(spatialReference={"wkid": 3857, "latestWkid": 4326}), "crs_drift"),
            ("feature CRS override", lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"wkid": 4326, "latestWkid": 3857}), "crs_drift"),
        ]
        for label, mutate, error in mutations:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, error):
                    source._validate(self._captures(docs))

    def test_version_is_path_safe(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = source.StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_usace_overhead_pipeline_lines(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()
