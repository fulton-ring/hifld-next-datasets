"""Native preservation and completeness checks for the official IENC LANDMARK_POINT layer."""
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
from shapely import from_wkb
from dagster import DefaultScheduleStatus

from dagster_hifld.sources import usace_landmark_locations as source
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.assets.usace_landmark_locations import (
    usace_landmark_locations_complete, usace_landmark_locations_draft_job,
    usace_landmark_locations_draft_schedule, usace_landmark_locations_usace_ienc,
)


def _capture_docs(count=source.EXPECTED_COUNT):
    layer = {
        "id": source.LAYER_ID, "name": "LANDMARK_POINT", "geometryType": "esriGeometryPoint",
        "maxRecordCount": 2000, "fields": source.EXPECTED_FIELD_DECLARATIONS,
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
    }
    service = {"mapName": "LANDMARK_POINT", "copyrightText": "USACE IENC",
               "layers": [{"id": 13, "name": "LANDMARK_POINT", "geometryType": "esriGeometryPoint"}]}
    ids = list(range(1, count + 1))
    docs = {
        "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "access": "public",
                     "title": "Inland Electronic Navigational Charts", "description": "Official link https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/LANDMARK_POINT", "type": "MapServer"}]},
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "count_before": {"count": count}, "count_after": {"count": count},
        "ids_before": {"objectIds": ids}, "ids_after": {"objectIds": ids},
    }
    for page_idx in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
        page_ids = ids[page_idx * source.PAGE_SIZE:(page_idx + 1) * source.PAGE_SIZE]
        rows = []
        for oid in page_ids:
            attrs = {field["name"]: None for field in source.ATTRIBUTE_FIELDS}
            attrs.update({"OBJECTID": oid, "Category_of_Landmark": "Chimney",
                "Visually_Conspicuous": "Visually Conspicuous", "Object_Name": "Reliant Energy Midwest",
                "Function": "Power Station", "Height": "", "Information": "two chimneys",
                "Scale_Minimum": 60000, "Source_Date": "20150812",
                "Source_Indication": "US,U3,IMAGE,Google_Earth", "Source_Dataset": "U37AG001.000",
                "Feature_ID": f"landmark-{oid}"})
            span_x = source.EXPECTED_EXTENT["xmax"] - source.EXPECTED_EXTENT["xmin"]
            span_y = source.EXPECTED_EXTENT["ymax"] - source.EXPECTED_EXTENT["ymin"]
            step = source.EXPECTED_COUNT - 1
            rows.append({"attributes": attrs,
                "geometry": {"x": source.EXPECTED_EXTENT["xmin"] + span_x * (oid - 1) / step,
                             "y": source.EXPECTED_EXTENT["ymin"] + span_y * (oid - 1) / step,
                             "spatialReference": {"wkid": 4326, "latestWkid": 4326}}})
        docs[f"page_{page_idx:03d}"] = {"features": rows, "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
    return {name: (json.dumps(doc).encode(), {}) for name, doc in docs.items()}


class LandmarkLocationsTests(unittest.TestCase):
    def test_registered_stopped_draft_and_unapproved_release_gate(self):
        self.assertIn(usace_landmark_locations_usace_ienc, hifld_ingest_assets)
        registered = defs.get_repository_def()
        self.assertIn(usace_landmark_locations_usace_ienc.key, registered.asset_graph.get_all_asset_keys())
        self.assertIn("usace_landmark_locations_complete",
                      {key.name for key in registered.asset_checks_defs_by_key})
        self.assertIn(usace_landmark_locations_draft_job.name,
                      {job.name for job in registered.get_all_jobs()})
        self.assertIn(usace_landmark_locations_draft_schedule.name,
                      {schedule.name for schedule in registered.schedule_defs})
        self.assertEqual(usace_landmark_locations_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check=usace_landmark_locations_complete({"feature_count":source.EXPECTED_COUNT,"source_population_complete":True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["bts_item_succession_accepted"].value)

    def test_retained_capture_is_bound_to_exact_named_get_request(self):
        name,url=source._capture_roster(source.EXPECTED_IDS)[0]
        receipt={"method":"GET","url":url,"final_url":url}
        source._validate_capture_request_identity(name,receipt,url)
        receipt["final_url"]="https://example.invalid/wrong"
        with self.assertRaisesRegex(ValueError,"retained_capture_request_identity_mismatch"):
            source._validate_capture_request_identity(name,receipt,url)

    def test_complete_population_native_geometry_and_declared_parquet_types(self):
        fields, features, audit = source._validate(_capture_docs())
        self.assertEqual(len(features), source.EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_population_complete"] if "source_population_complete" in audit else len(features), source.EXPECTED_COUNT)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "landmark-1")
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(__import__("io").BytesIO(parquet))
        self.assertTrue(table.schema.field("Feature_ID").type.equals(__import__("pyarrow").string()))
        self.assertEqual(table["Feature_ID"].null_count, 0)
        geometry = from_wkb(table["geometry"][0].as_py())
        self.assertEqual(geometry.geom_type, "Point")
        self.assertEqual(geometry.x, features[0]["geometry"]["x"])
        self.assertEqual(geometry.y, features[0]["geometry"]["y"])
        self.assertTrue(table.schema.field("Scale_Minimum").type.equals(__import__("pyarrow").int32()))

    def test_rejects_schema_and_population_drift(self):
        captures = _capture_docs()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"] = layer["fields"][:-1]
        captures["layer_before"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"].pop()
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
            source._validate(captures)

    def test_rejects_geometry_or_declared_type_loss(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = "-90"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_geometry_structure_drift"):
            source._validate(captures)

    def test_rejects_out_of_range_or_outside_coverage_coordinates(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = -90
        page["features"][0]["geometry"]["y"] = 1000
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_coordinate_range_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = 0
        page["features"][0]["geometry"]["y"] = 0
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_feature_outside_coverage_extent"):
            source._validate(captures)

    def test_point_queries_request_z_and_m_and_reject_returned_dimensions(self):
        pages = [(name, url) for name, url in source._capture_roster(source.EXPECTED_IDS)
                 if name.startswith("page_")]
        self.assertEqual(len(pages), 1)
        for _name, url in pages:
            query = parse_qs(urlsplit(url).query)
            self.assertEqual(query["returnZ"], ["true"])
            self.assertEqual(query["returnM"], ["true"])
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["z"] = 17.0
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_zm_dimensions_not_supported"):
            source._validate(captures)

    def test_observed_population_and_bounds_contracts_are_audited(self):
        _fields, _features, audit = source._validate(_capture_docs())
        self.assertEqual(audit["source_feature_id_null_count"], 0)
        self.assertEqual(audit["source_feature_id_distinct_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["source_extent_tolerance_degrees"], source.EXTENT_TOLERANCE)
        self.assertEqual(audit["geometry_bounds"], [source.EXPECTED_EXTENT[key]
                                                     for key in ("xmin", "ymin", "xmax", "ymax")])

    def test_rejects_null_or_duplicate_feature_id(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["Feature_ID"] = None
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_feature_id_null_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][1]["attributes"]["Feature_ID"] = page["features"][0]["attributes"]["Feature_ID"]
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_feature_id_duplicate_drift"):
            source._validate(captures)

    def test_rejects_fully_contracted_observed_bounds(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        for feature in page["features"]:
            feature["geometry"]["x"] = -90
            feature["geometry"]["y"] = 35
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_observed_coverage_extent_drift"):
            source._validate(captures)

    def test_raw_contract_failures_are_quarantined_before_sealing(self):
        docs = {name: json.loads(body) for name, (body, _receipt) in _capture_docs().items()}
        docs["page_000"]["features"][0]["attributes"]["Feature_ID"] = None
        bodies = {url: json.dumps(docs[name]).encode()
                  for name, url in source._capture_roster(source.EXPECTED_IDS)}
        page_body = json.dumps(docs["page_000"]).encode()

        def handler(request):
            body = bodies.get(str(request.url))
            if body is None:
                return httpx.Response(404, request=request)
            return httpx.Response(200, stream=httpx.ByteStream(body),
                                  headers={"content-length": str(len(body))}, request=request)

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with patch.object(source.httpx, "Client",
                              side_effect=lambda *args, **kwargs: httpx.Client(
                                  transport=httpx.MockTransport(handler), **kwargs)):
                with self.assertRaisesRegex(ValueError, "source_feature_id_null_drift"):
                    source.capture_usace_landmark_locations(
                        storage, version="vtest", capture_directory=str(Path(tmp) / "capture"))
            quarantine = Path(tmp) / "_quarantine" / source.DATASET_SLUG
            tokens = list(quarantine.iterdir())
            self.assertEqual(len(tokens), 1)
            raw = tokens[0] / "raw"
            self.assertEqual((raw / "page_000.body").read_bytes(), page_body)
            self.assertTrue((raw / "page_000.wire").is_file())
            self.assertTrue((raw / "page_000.receipt.json").is_file())
            failure = json.loads((tokens[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"], "ValueError")
            self.assertEqual(failure["captured_names"], sorted(source.CAPTURE_NAMES))
            self.assertFalse((Path(tmp) / source.DATASET_SLUG).exists())


if __name__ == "__main__":
    unittest.main()
