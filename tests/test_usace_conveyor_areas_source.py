import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely import from_wkb

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets

COUNT = 269
IDS = list(range(1, COUNT + 1))
ID_SHA256 = "82ca28bcb00ccbaae803c77bc62b5f38284b059735f9ff201ff6793f6b7bab5e"
from dagster_hifld.assets.usace_conveyor_areas import (
    usace_conveyor_areas_complete,
    usace_conveyor_areas_draft_schedule,
    usace_conveyor_areas_usace_ienc,
)
from dagster_hifld.sources import usace_conveyor_areas as source


def _document(value):
    return (json.dumps(value, separators=(",", ":")).encode(), {"status": 200})


def _synthetic_capture():
    ids = IDS
    fields = source.EXPECTED_FIELD_DECLARATIONS
    extent = {**source.EXPECTED_LAYER_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
    sr = {"wkid": 4326, "latestWkid": 4326}
    layer = {
        "id": 60, "name": "CONVEYOR_AREA", "geometryType": "esriGeometryPolygon",
        "spatialReference": sr, "extent": extent, "maxRecordCount": 2000,
        "hasZ": False, "hasM": False, "fields": fields,
    }
    service = {"mapName": "CONVEYOR_AREA", "copyrightText": "USACE IENC",
               "layers": [{"id": 60, "name": "CONVEYOR_AREA", "geometryType": "esriGeometryPolygon"}]}
    directory = {"services": [{"name": "IENC_Feature_Classes/CONVEYOR_AREA", "type": "MapServer"}]}
    item = {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "access": "public",
            "title": "Inland Electronic Navigational Charts",
            "description": ("updated bi-monthly https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes "
                            "USACE_IENC_Master_Service_gdb.zip")}
    rows = []
    for oid in ids:
        attrs = {name: None for name, _kind in source.EXPECTED_FIELDS if name != "SHAPE"}
        attrs.update({"OBJECTID": oid, "Feature_ID": f"conveyor-{oid}"})
        geometry = {"rings": [[[-91.0, 32.0], [-91.01, 32.0], [-91.01, 32.01],
                                [-91.0, 32.01], [-91.0, 32.0]]]}
        rows.append({"attributes": attrs, "geometry": geometry})
    docs = {
        "bts_item": item, "service_directory": directory, "service_before": service,
        "layer_before": layer, "count_before": {"count": len(ids)},
        "ids_before": {"objectIds": ids}, "ids_seed": {"objectIds": ids},
        "page_000": {"spatialReference": sr, "features": rows[:source.PAGE_SIZE]},
        "page_001": {"spatialReference": sr, "features": rows[source.PAGE_SIZE:]},
        "layer_after": layer, "count_after": {"count": len(ids)},
        "ids_after": {"objectIds": ids}, "service_after": service,
    }
    return {name: _document(value) for name, value in docs.items()}


class ConveyorAreasTests(unittest.TestCase):
    def test_registered_source_check_and_refresh_are_stopped(self):
        self.assertIn(usace_conveyor_areas_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_conveyor_areas_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertTrue(usace_conveyor_areas_complete({
            "feature_count": COUNT, "source_reported_count": COUNT, "source_population_complete": True
        }).passed)
        self.assertFalse(usace_conveyor_areas_complete({
            "feature_count": COUNT - 1, "source_reported_count": COUNT, "source_population_complete": True
        }).passed)

    def test_native_capture_contract_preserves_every_declared_attribute_and_polygon(self):
        fields, features, audit = source._validate(_synthetic_capture())
        self.assertEqual(len(fields), 13)
        self.assertEqual(len(features), 269)
        self.assertEqual(audit["source_id_sha256"], ID_SHA256)
        self.assertEqual([f["name"] for f in fields], [x["name"] for x in source.EXPECTED_FIELD_DECLARATIONS])
        parquet_bytes = source._parquet_bytes(fields, features, 4326)
        table = pq.read_table(__import__("io").BytesIO(parquet_bytes))
        self.assertEqual(table.schema.names, [x["name"] for x in fields if x["name"] != "SHAPE"] + ["geometry"])
        self.assertEqual(table.column("Scale_Minimum").type, __import__("pyarrow").int32())
        self.assertEqual(table.column("OBJECTID").to_pylist(), IDS)
        self.assertEqual(table.column("Feature_ID").to_pylist(), [f"conveyor-{i}" for i in IDS])
        source_wkb = source._esri_polygon(features[123]["geometry"])
        self.assertEqual(table.column("geometry")[123].as_py(), source_wkb)
        self.assertEqual(from_wkb(source_wkb).geom_type, "Polygon")

    def test_capture_quarantines_id_crs_and_schema_drift(self):
        cases = []
        ids_changed = _synthetic_capture()
        ids_doc = json.loads(ids_changed["ids_after"][0])
        ids_doc["objectIds"][-1] = 270
        ids_changed["ids_after"] = _document(ids_doc)
        cases.append((ids_changed, "source_count_or_id_roster_drift"))

        crs_changed = _synthetic_capture()
        layer = json.loads(crs_changed["layer_before"][0])
        layer["spatialReference"] = {"wkid": 3857, "latestWkid": 3857}
        crs_changed["layer_before"] = _document(layer)
        crs_changed["layer_after"] = _document(layer)
        cases.append((crs_changed, "source_spatial_reference_or_page_limit_drift"))

        schema_changed = _synthetic_capture()
        layer = json.loads(schema_changed["layer_before"][0])
        layer["fields"][-1]["name"] = "OTHER_ID"
        schema_changed["layer_before"] = _document(layer)
        schema_changed["layer_after"] = _document(layer)
        cases.append((schema_changed, "source_field_schema_drift"))

        for captures, expected in cases:
            with self.subTest(expected=expected):
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(captures)
    def test_failed_capture_quarantines_readable_evidence_and_records_unreadable_receipt(self):
        view = _synthetic_capture()
        docs = {name: json.loads(body) for name, (body, _receipt) in view.items()}
        docs["page_001"] = {**docs["page_001"], "features": docs["page_001"]["features"][:-1]}
        roster = dict(source._capture_roster(IDS))
        roster["ids_seed"] = source._query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})
        bodies = {url: json.dumps(doc, separators=(",", ":")).encode()
                  for name, url in roster.items() for doc in [docs[name]]}
        hook_url = roster["page_000"]

        def serve(request):
            url = str(request.url)
            if url == hook_url:
                root = next((Path(tmp) / "captures").iterdir())
                (root / "count_before.receipt.json").write_bytes(b'{')
            return httpx.Response(200, stream=httpx.ByteStream(bodies[url]))

        real_client = httpx.Client
        with tempfile.TemporaryDirectory() as tmp, patch.object(
                source.httpx, "Client",
                side_effect=lambda **kwargs: real_client(transport=httpx.MockTransport(serve), **kwargs)):
            storage = source.StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaises(ValueError) as raised:
                source.capture_usace_conveyor_areas(storage, version="torn-page",
                                 capture_directory=str(Path(tmp) / "captures"))
            self.assertEqual(str(raised.exception), "source_feature_population_incomplete")
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
            self.assertTrue(any(key.endswith("/raw/" + "page_000" + ".wire") for key in quarantine_keys))
            self.assertTrue(any(key.endswith("/raw/ids_before.receipt.json") for key in quarantine_keys))
            root = next((Path(tmp) / "captures").iterdir())
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])
            self.assertEqual(manifest["error_code"], "ValueError")
            self.assertNotIn("receipts", manifest)

if __name__ == "__main__":
    unittest.main()
