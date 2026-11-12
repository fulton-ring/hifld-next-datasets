import json
import struct
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.sloped_ground_lines_usace_ienc import (
    sloped_ground_lines_usace_ienc, sloped_ground_lines_usace_ienc_complete,
    sloped_ground_lines_usace_ienc_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import sloped_ground_lines_usace_ienc as source
from dagster_hifld.sources.epa_frs_common import _canonical


class SlopedGroundLinesUsaceIencTests(unittest.TestCase):
    def _line(self, oid):
        x0 = -90.2 + (oid % 100) * 0.003
        y0 = 35.05 + (oid // 100) * 0.003
        return [[x0, y0], [x0 + 0.002, y0 + 0.001], [x0 + 0.004, y0 + 0.002]]

    def _documents(self):
        service = {"mapName": "SLOPE_TOPLINE_LINE", "copyrightText": "USACE IENC",
                   "layers": [{"id": 50, "name": "SLOPE_TOPLINE_LINE", "geometryType": "esriGeometryPolyline"}]}
        layer = {"id": 50, "name": "SLOPE_TOPLINE_LINE", "geometryType": "esriGeometryPolyline",
                 "maxRecordCount": 2000, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
                         "title": "Inland Electronic Navigational Charts",
                         "description": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/SLOPE_TOPLINE_LINE", "type": "MapServer"}]},
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
                         (22000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                         for field in source.ATTRIBUTE_FIELDS}
                paths = [self._line(oid)]
                if oid % 10 == 0:
                    paths.append([[self._line(oid)[0][0] + 0.0005, 35.4],
                                  [self._line(oid)[0][0] + 0.0015, 35.4001]])
                rows.append({"attributes": attrs, "geometry": {"paths": paths}})
            docs[f"page_{page_index:03d}"] = {"spatialReference": {"wkid": 4326}, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(sloped_ground_lines_usace_ienc, hifld_ingest_assets)
        self.assertEqual(sloped_ground_lines_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = sloped_ground_lines_usace_ienc_complete({"feature_count": 233, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = sloped_ground_lines_usace_ienc_complete({"feature_count": 232, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")], ["page_000"])
        self.assertIn("objectIds=1%2C2%2C3", urls["page_000"])
        self.assertNotIn("objectIds=234", urls["page_000"])
        self.assertIn("returnZ=true", urls["page_000"])
        self.assertIn("returnM=true", urls["page_000"])
        self.assertEqual(len(urls["page_000"].split("objectIds=")[1].split("&")[0].split("%2C")), 233)
        self.assertEqual(sorted(name for name in urls), sorted(source.CAPTURE_NAMES))

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 233)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 745)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], 233)
        self.assertEqual(audit["feature_id_distinct_count"], 233)
        self.assertEqual(audit["feature_id_duplicate_count"], 0)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertIn("not a nationwide sloped-ground inventory", audit["source_coverage_scope"])

    def test_polyline_wkb_is_xy_only_and_preserves_path_structure(self):
        line = self._line(1)
        single = source._esri_polyline_wkb({"paths": [line]})
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 5))
        self.assertEqual(struct.unpack("<I", single[5:9])[0], 1)
        self.assertEqual(struct.unpack("<BI", single[9:14]), (1, 2))
        self.assertEqual(struct.unpack("<I", single[14:18])[0], 3)
        multi = source._esri_polyline_wkb({"paths": [line, self._line(2)]})
        self.assertEqual(struct.unpack("<BI", multi[:5]), (1, 5))
        self.assertEqual(struct.unpack("<I", multi[5:9])[0], 2)
        for flags in ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True}):
            with self.assertRaisesRegex(ValueError, "source_polyline_dimensional_flags_rejected"):
                source._esri_polyline_wkb({"paths": [line], **flags})
        unflagged = [[[0.0, 0.0, 1.0], [0.0, 1.0, 1.0]]]
        with self.assertRaisesRegex(ValueError, "source_polyline_zm_flags_do_not_match_coordinates"):
            source._esri_polyline_wkb({"paths": unflagged})

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_count_or_id_population_drift"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("transfer", lambda d: d["page_000"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_000"].update(spatialReference={"wkid": 3857}), "crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True),
             "source_layer_dimensional_drift"),
            ("page dimensional", lambda d: d["page_000"].update(hasZ=True),
             "source_page_dimensional_drift"),
            ("geometry dimensional", lambda d: d["page_000"]["features"][0]["geometry"].update(hasZ=True),
             "source_geometry_dimensional_drift"),
            ("single-point path", lambda d: d["page_000"]["features"][0]["geometry"].update(
                paths=[[d["page_000"]["features"][0]["geometry"]["paths"][0][0]]]),
             "source_polyline_path_invalid"),
            ("paths missing", lambda d: d["page_000"]["features"][0]["geometry"].pop("paths"),
             "source_polyline_paths_missing"),
            ("outside coverage", lambda d: d["page_000"]["features"][0]["geometry"]["paths"][0].__setitem__(
                2, [-120.0, 35.5]),
             "source_geometry_outside_declared_coverage"),
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
                source.capture_sloped_ground_lines_usace_ienc(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()
