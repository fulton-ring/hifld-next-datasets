import json
import tempfile
import unittest
from pathlib import Path

import httpx
import pyarrow.parquet as pq
from shapely.geometry import Polygon
from shapely.wkb import loads as from_wkb

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.electric_retail_service_territories import (
    electric_retail_service_territories_population_complete,
    electric_retail_service_territories_geometry_valid,
    electric_retail_service_territories_preservation_schedule,
    electric_retail_service_territories_release_pending,
)
from dagster_hifld.sources import electric_retail_service_territories as source


class _InterruptedStream(httpx.SyncByteStream):
    def __iter__(self):
        yield b'{"partial":'
        raise OSError("connection reset")

    def close(self):
        pass


class ElectricRetailTerritoryTests(unittest.TestCase):
    def test_esri_ring_conversion_preserves_holes_and_native_geoparquet(self):
        shell = [[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]]
        hole = [[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]]
        geometry = source._polygon_from_esri({"rings": [shell, hole]})
        self.assertEqual(len(geometry.interiors), 1)
        fields = [("OBJECTID", "esriFieldTypeOID", None), ("NAME", "esriFieldTypeString", 80)]
        payload = source._make_geoparquet([({"OBJECTID": 7, "NAME": "territory"}, geometry)], fields)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "territories.parquet"
            path.write_bytes(payload)
            table = pq.read_table(path)
        self.assertEqual(table.column_names, ["OBJECTID", "NAME", "geometry"])
        restored = from_wkb(table["geometry"][0].as_py())
        self.assertEqual(len(restored.interiors), 1)
        metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(metadata["columns"]["geometry"]["crs"]["id"]["code"], 3857)

    def test_source_item_must_remain_the_verified_frozen_distribution(self):
        item = {
            "id": source.ITEM_ID, "owner": "NBAM_Org", "ownerId": "owner-id",
            "title": "Electric Retail Service Territories", "type": "Feature Service",
            "access": "public", "url": source.FEATURE_SERVICE_URL,
            "description": source.EXPECTED_COPY_NOTICE,
            "licenseInfo": "None (Public Use). Users are advised to read the data set's metadata thoroughly to understand appropriate use and data limitations.",
            "accessInformation": source.EXPECTED_COPYRIGHT,
        }
        owner = {"username": "NBAM_Org", "id": "owner-id"}
        source._validate_source_item(item, owner)
        item["owner"] = "unverified-account"
        with self.assertRaisesRegex(ValueError, "source_distribution_item_identity_drift"):
            source._validate_source_item(item, owner)

    def test_incomplete_transport_keeps_partial_payload_and_receipt(self):
        def handler(request):
            return httpx.Response(200, stream=_InterruptedStream(), request=request)

        captures = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with httpx.Client(transport=httpx.MockTransport(handler)) as client:
                with self.assertRaisesRegex(ValueError, "incomplete_source_capture"):
                    source._capture_request(client, root, "page_000000", "GET", "https://example.test/data", {}, [0], False, captures)
            self.assertEqual(len(captures), 1)
            receipt = json.loads((root / "page_000000.receipt.json").read_bytes())
            self.assertFalse(receipt["complete_body"])
            self.assertTrue((root / "page_000000.wire").read_bytes())
            self.assertTrue((root / "page_000000.body").read_bytes())

    def test_release_checks_and_schedule_remain_stopped(self):
        candidate = {"feature_count": source.EXPECTED_COUNT, "source_population_complete": True,
                     "release_accepted": False}
        self.assertTrue(electric_retail_service_territories_population_complete(candidate).passed)
        candidate.update(null_geometry_count=0, invalid_geometry_count=0)
        self.assertTrue(electric_retail_service_territories_geometry_valid(candidate).passed)
        self.assertFalse(electric_retail_service_territories_release_pending(candidate).passed)
        self.assertEqual(electric_retail_service_territories_preservation_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()
