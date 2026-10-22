from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.national_transit_map_stops import national_transit_map_stops_draft_schedule
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import national_transit_map_stops as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


class NationalTransitMapStopsTests(unittest.TestCase):
    def test_exact_native_schema_z_geometry_and_stopped_registration(self):
        self.assertEqual(len(source.FIELD_DECLARATIONS), 20)
        self.assertEqual(source.FIELD_DECLARATIONS[0]["name"], "OBJECTID")
        self.assertEqual(national_transit_map_stops_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        self.assertTrue(any(asset.key.path == ["ingest", source.DATASET_SLUG, source.FILE_SLUG]
                            for asset in hifld_ingest_assets))
        self.assertEqual(source._point_wkb({"x": -73.0, "y": 42.0, "z": 17.5}, True, False),
                         bytes.fromhex("01e903000000000000004052c000000000000045400000000000803140"))
        changed = [dict(field) for field in source.FIELD_DECLARATIONS]
        changed[3]["type"] = "esriFieldTypeInteger"
        with self.assertRaisesRegex(ValueError, "source_schema_drift"):
            source._field_contract({"fields": changed, "objectIdField": "OBJECTID",
                                   "geometryType": "esriGeometryPoint"})

    def test_tiny_mock_capture_brackets_ids_retains_every_field_and_native_z(self):
        fields = [dict(field) for field in source.FIELD_DECLARATIONS]
        layer = {
            "name": source.LAYER_NAME, "geometryType": "esriGeometryPoint",
            "objectIdField": "OBJECTID", "hasZ": True, "hasM": False,
            "spatialReference": {"wkid":4326,"latestWkid":4326},
            "extent": {"spatialReference":{"wkid":4326,"latestWkid":4326}},
            "maxRecordCount": 2,
            "advancedQueryCapabilities":{"supportsPagination":True,"supportsOrderBy":True},
            "fields":fields,
        }
        service={"serviceItemId":source.ITEM_ID,"description":source.DESCRIPTION,
                 "layers":[{"id":0,"name":source.LAYER_NAME}]}
        portal={"id":source.ITEM_ID,"owner":"USDOT_BTS","orgId":"xOi1kZaI0eWDREZv",
                "title":source.LAYER_NAME,"type":"Feature Service","url":source.SERVICE_URL,
                "access":"public","description":source.DESCRIPTION,"modified":1792186548000}
        ids={"objectIdFieldName":"OBJECTID","objectIds":[101,102]}
        rows=[]
        for oid,x,y,z in ((101,-73.0,42.0,17.5),(102,-122.0,47.0,18.5)):
            attributes={field["name"]:None for field in fields}
            attributes.update({"OBJECTID":oid,"ntd_id":"n-"+str(oid),"feed_id":"feed-A",
                               "stop_id":"stop-"+str(oid),"stop_name":"Station "+str(oid),
                               "stop_lat":y,"stop_lon":x,"stop_type":"1"})
            rows.append({"attributes":attributes,"geometry":{"x":x,"y":y,"z":z}})
        bodies={
            "portal_item":portal,"service_before":service,"service_after":service,
            "layer_before":layer,"layer_after":layer,
            "count_before":{"count":2},"count_after":{"count":2},
            "ids_before":ids,"ids_after":ids,
            "page_000":{"spatialReference":{"wkid":4326,"latestWkid":4326},
                        "features":rows,"exceededTransferLimit":False},
        }
        observed=[]
        def fake_capture(_client,url,root,name,**kwargs):
            body=json.dumps(bodies[name],separators=(",",":"),ensure_ascii=False).encode()
            digest=hashlib.sha256(body).hexdigest()
            receipt={"method":"GET","url":url,"final_url":url,"observed_at":"2026-10-22T00:00:00Z",
                     "status":200,"complete_body":True,"bytes":len(body),"sha256":digest,
                     "wire_bytes":len(body),"wire_sha256":digest,"error_code":None,"error_type":None}
            root.mkdir(parents=True,exist_ok=True)
            kwargs["reserve"](SimpleNamespace(reserve_bytes=min(1,len(body))))
            kwargs["before_retain"]("wire",body)
            kwargs["before_retain"]("body",body)
            (root/f"{name}.wire").write_bytes(body)
            (root/f"{name}.body").write_bytes(body)
            (root/f"{name}.receipt.json").write_bytes(canonical(receipt)+b"\n")
            observed.append(name)
            return body,receipt

        with tempfile.TemporaryDirectory() as tmp:
            tmp=Path(tmp)
            storage=StagingStorageResource(local_dir=str(tmp/"staging"),use_local=True,
                                           backend="local",prefix="")
            with patch.object(source,"EXPECTED_COUNT",2), patch.object(source,"PAGE_SIZE",2), \
                 patch.object(source,"capture_get",side_effect=fake_capture), \
                 patch.object(source.httpx,"Client"), \
                 patch.object(source.shutil,"disk_usage",return_value=SimpleNamespace(free=1_000_000_000)), \
                 patch.object(source,"processing_identity",return_value={
                     "code_identity_scheme":"test","working_tree_status":"unknown",
                     "code_commit":"test","files":{}}), \
                 patch.object(source,"validate_construction_code_identity"):
                result=source.capture_national_transit_map_stops(
                    storage,version="synthetic",capture_directory=str(tmp/"capture"))
            self.assertEqual(result["feature_count"],2)
            self.assertEqual(result["source_null_geometry_count"],0)
            self.assertFalse(result["release_accepted"])
            self.assertIn("ids_before",observed)
            self.assertIn("ids_after",observed)
            native=tmp/"staging"/result["candidate_ready"].replace(
                "metadata/candidate_ready.json",f"geoparquet/{source.DATASET_SLUG}.parquet")
            table=pq.read_table(native)
            self.assertEqual(table.column_names,[field["name"] for field in fields]+["geometry"])
            self.assertEqual(table["OBJECTID"].to_pylist(),[101,102])
            self.assertEqual(table["stop_id"].to_pylist(),["stop-101","stop-102"])
            self.assertEqual(table["geometry"].to_pylist(),[
                source._point_wkb(rows[0]["geometry"],True,False),
                source._point_wkb(rows[1]["geometry"],True,False)])
            geo=json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo["columns"]["geometry"]["geometry_types"],["Point Z"])
            manifest=json.loads(storage.read_key(result["candidate_ready"].replace(
                "metadata/candidate_ready.json","metadata/source_manifest.json")))
            self.assertFalse(manifest["historical_equivalence_accepted"])
            self.assertFalse(manifest["rights_accepted"])


if __name__ == "__main__":
    unittest.main()
