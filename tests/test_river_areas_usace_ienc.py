import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.river_areas_usace_ienc import (
    river_areas_usace_ienc, river_areas_usace_ienc_complete,
    river_areas_usace_ienc_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import river_areas_usace_ienc as source
from dagster_hifld.sources.esri_common import canonical as _canonical

COUNT = 3644
IDS = list(range(1, COUNT + 1))
ID_SHA256 = "53b11018fb0f2b400ccf409d0f39865a6694200b78c162c74ad00d71ace9a4a4"


class RiverAreasUsaceIencTests(unittest.TestCase):
    def _shell(self, oid):
        x0 = -93.5 + (oid % 100) * 0.003
        y0 = 35.0 + (oid // 100) * 0.003
        return [[x0, y0], [x0, y0 + 0.001], [x0 + 0.001, y0 + 0.001], [x0 + 0.001, y0], [x0, y0]]

    def _documents(self):
        service = {"mapName": "RIVERS_AREA", "copyrightText": "USACE IENC",
                   "layers": [{"id": 75, "name": "RIVERS_AREA", "geometryType": "esriGeometryPolygon"}]}
        layer = {"id": 75, "name": "RIVERS_AREA", "geometryType": "esriGeometryPolygon",
                 "maxRecordCount": 2000, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
                         "title": "Inland Electronic Navigational Charts",
                         "description": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/RIVERS_AREA", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": COUNT},
            "ids_before": {"objectIds": IDS},
            "service_after": service, "layer_after": layer,
            "count_after": {"count": COUNT},
            "ids_after": {"objectIds": IDS},
        }
        for page_index in range((COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
            rows = []
            for oid in IDS[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]:
                attrs = {field["name"]: oid if field["name"] == "OBJECTID" else
                         (60000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                         for field in source.ATTRIBUTE_FIELDS}
                rings = [self._shell(oid)]
                if oid % 50 == 0:
                    shell = rings[0]
                    inset = 0.0002
                    rings.append([[shell[0][0] + inset, shell[0][1] + inset],
                                  [shell[2][0] - inset, shell[0][1] + inset],
                                  [shell[2][0] - inset, shell[2][1] - inset],
                                  [shell[0][0] + inset, shell[2][1] - inset],
                                  [shell[0][0] + inset, shell[0][1] + inset]])
                rows.append({"attributes": attrs, "geometry": {"rings": rings}})
            docs[f"page_{page_index:03d}"] = {"spatialReference": {"wkid": 4326}, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(river_areas_usace_ienc, hifld_ingest_assets)
        self.assertEqual(river_areas_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = river_areas_usace_ienc_complete({"feature_count": 3644, "source_reported_count": COUNT, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = river_areas_usace_ienc_complete({"feature_count": 3643, "source_reported_count": COUNT, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        urls = dict(source._capture_roster(IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         [f"page_{index:03d}" for index in range(15)])
        self.assertIn("objectIds=1%2C2%2C3", urls["page_000"])
        self.assertIn("objectIds=3501%2C3502%2C3503", urls["page_014"])
        self.assertNotIn("objectIds=3645", urls["page_014"])
        self.assertIn("returnZ=true", urls["page_000"])
        self.assertIn("returnM=true", urls["page_000"])
        self.assertEqual(len(urls["page_014"].split("objectIds=")[1].split("&")[0].split("%2C")), 144)
        self.assertEqual(len(urls["page_000"].split("objectIds=")[1].split("&")[0].split("%2C")), 250)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 3644)
        self.assertEqual(audit["source_id_sha256"], ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 18580)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], 3644)
        self.assertEqual(audit["feature_id_distinct_count"], 3644)
        self.assertEqual(audit["feature_id_duplicate_count"], 0)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertIn("not a nationwide river inventory", audit["source_coverage_scope"])

    def test_polygon_wkb_is_xy_only_and_preserves_ring_structure(self):
        shell = self._shell(1)
        single = source._esri_polygon_wkb({"rings": [shell]})
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 3))
        disjoint = source._esri_polygon_wkb({"rings": [shell, self._shell(2)]})
        self.assertEqual(struct.unpack("<BI", disjoint[:5]), (1, 6))
        inset = 0.0002
        hole = [[shell[0][0] + inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[0][1] + inset]]
        with_hole = source._esri_polygon_wkb({"rings": [shell, hole]})
        self.assertEqual(struct.unpack("<BI", with_hole[:5]), (1, 3))
        for flags in ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True}):
            with self.assertRaisesRegex(ValueError, "source_polygon_dimensional_flags_rejected"):
                source._esri_polygon_wkb({"rings": [shell], **flags})
        unflagged = [[0.0, 0.0, 1.0], [0.0, 1.0, 1.0], [1.0, 1.0, 1.0], [1.0, 0.0, 1.0], [0.0, 0.0, 1.0]]
        with self.assertRaisesRegex(ValueError, "source_polygon_zm_flags_do_not_match_coordinates"):
            source._esri_polygon_wkb({"rings": [unflagged]})

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population", lambda d: d["ids_after"].update(objectIds=IDS[:-1]),
             "source_count_or_id_roster_drift"),
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
            ("geometry dimensional", lambda d: d["page_000"]["features"][0]["geometry"].update(hasZ=True),
             "source_geometry_dimensional_drift"),
            ("open ring", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].pop(),
             "source_polygon_ring_not_closed"),
            ("outside coverage", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].__setitem__(
                2, [-120.0, 35.5]),
             "source_geometry_outside_declared_coverage"),
            ("hole without shell", lambda d: d["page_000"]["features"][0]["geometry"]["rings"].append(
                [[-93.6, 35.2], [-93.55, 35.2], [-93.55, 35.25], [-93.6, 35.25], [-93.6, 35.2]]),
             "source_hole_ownership_ambiguous"),
            ("shell missing", lambda d: d["page_000"]["features"][0]["geometry"].update(
                rings=[[[-93.6, 35.2], [-93.55, 35.2], [-93.55, 35.25], [-93.6, 35.25], [-93.6, 35.2]]]),
             "source_polygon_shell_missing"),
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
                source.capture_river_areas_usace_ienc(storage, version="../outside")
    def test_failed_capture_quarantines_readable_evidence_and_records_unreadable_receipt(self):
        docs = self._documents()
        final_page = f"page_{(COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE - 1:03d}"
        docs[final_page] = {**docs[final_page], "features": docs[final_page]["features"][:-1]}
        roster = dict(source._capture_roster(IDS))
        bodies = {url: json.dumps(doc, separators=(",", ":")).encode()
                  for name, url in roster.items() for doc in [docs[name]]}
        page_first_url = roster["page_000"]

        def serve(request):
            url = str(request.url)
            if url == page_first_url:
                root = next((Path(tmp) / "captures" / "torn-page").iterdir())
                (root / "count_before.receipt.json").write_bytes(b'{')
            return httpx.Response(200, stream=httpx.ByteStream(bodies[url]))

        real_client = httpx.Client
        with tempfile.TemporaryDirectory() as tmp, patch.object(
                source.httpx, "Client",
                side_effect=lambda **kwargs: real_client(transport=httpx.MockTransport(serve), **kwargs)):
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaises(ValueError) as raised:
                source.capture_river_areas_usace_ienc(storage, version="torn-page",
                                 capture_directory=str(Path(tmp) / "captures"))
            self.assertEqual(str(raised.exception), "source_page_population_incomplete:" + final_page)
            self.assertNotIsInstance(raised.exception, (json.JSONDecodeError, OSError))
            self.assertEqual(storage.list_prefix(source.DATASET_SLUG), [])
            quarantine_keys = storage.list_prefix("_quarantine/" + source.DATASET_SLUG)
            failure_key = next(key for key in quarantine_keys if key.endswith("/metadata/failure.json"))
            failure = json.loads(storage.read_key(failure_key))
            self.assertEqual(failure["reason_code"], "ValueError")
            self.assertEqual(failure["captured_names"],
                             sorted(name for name in roster if name != "count_before"))
            self.assertEqual(failure["not_retained"],
                             [{"name": "count_before",
                               "reason": "retained_capture_receipt_unreadable:count_before"}])
            self.assertTrue(any(key.endswith("/raw/bts_item.body") for key in quarantine_keys))
            self.assertTrue(any(key.endswith("/raw/" + final_page + ".wire") for key in quarantine_keys))
            self.assertTrue(any(key.endswith("/raw/ids_before.receipt.json") for key in quarantine_keys))
            root = next((Path(tmp) / "captures" / "torn-page").iterdir())
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])
            self.assertEqual(manifest["error_code"], "ValueError")
            self.assertNotIn("receipts", manifest)


if __name__ == "__main__":
    unittest.main()
