from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from copy import deepcopy
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


def _tiny_layer(page_size):
    fields = [dict(field) for field in source.FIELD_DECLARATIONS]
    layer = {
        "name": source.LAYER_NAME, "geometryType": "esriGeometryPoint",
        "objectIdField": "OBJECTID", "hasZ": True, "hasM": False,
        "spatialReference": {"wkid":4326,"latestWkid":4326},
        "extent": {"spatialReference":{"wkid":4326,"latestWkid":4326}},
        "maxRecordCount": page_size,
        "advancedQueryCapabilities":{"supportsPagination":True,"supportsOrderBy":True},
        "fields":fields,
    }
    return layer, fields


def _stop_row(fields, oid, x, y, z):
    attributes = {field["name"]: None for field in fields}
    attributes.update({"OBJECTID":oid,"ntd_id":"n-"+str(oid),"feed_id":"feed-A",
                       "stop_id":"stop-"+str(oid),"stop_name":"Station "+str(oid),
                       "stop_lat":y,"stop_lon":x,"stop_type":"1"})
    return {"attributes":attributes,"geometry":{"x":x,"y":y,"z":z}}


def _metadata_bodies(layer, count, object_ids):
    service={"serviceItemId":source.ITEM_ID,"description":source.DESCRIPTION,
             "layers":[{"id":0,"name":source.LAYER_NAME}]}
    portal={"id":source.ITEM_ID,"owner":"USDOT_BTS","orgId":"xOi1kZaI0eWDREZv",
            "title":source.LAYER_NAME,"type":"Feature Service","url":source.SERVICE_URL,
            "access":"public","description":source.DESCRIPTION,"modified":1792186548000}
    ids={"objectIdFieldName":"OBJECTID","objectIds":object_ids}
    return {"portal_item":portal,"service_before":service,"service_after":service,
            "layer_before":layer,"layer_after":layer,
            "count_before":{"count":count},"count_after":{"count":count},
            "ids_before":ids,"ids_after":ids}


def _fake_capture_get(observed, bodies):
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
    return fake_capture


def _capture_patches(expected_count, page_size, fake_capture):
    return [patch.object(source,"EXPECTED_COUNT",expected_count),
            patch.object(source,"PAGE_SIZE",page_size),
            patch.object(source,"capture_get",side_effect=fake_capture),
            patch.object(source.httpx,"Client"),
            patch.object(source.shutil,"disk_usage",
                         return_value=SimpleNamespace(free=1_000_000_000)),
            patch.object(source,"processing_identity",return_value={
                "code_identity_scheme":"test","working_tree_status":"unknown",
                "code_commit":"test","files":{}}),
            patch.object(source,"validate_construction_code_identity")]


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
        ids={"objectIdFieldName":"OBJECTID","objectIds":[1,2]}
        rows=[]
        for oid,x,y,z in ((1,-73.0,42.0,17.5),(2,-122.0,47.0,18.5)):
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
            self.assertEqual(table["OBJECTID"].to_pylist(),[1,2])
            self.assertEqual(table["stop_id"].to_pylist(),["stop-1","stop-2"])
            self.assertEqual(table["geometry"].to_pylist(),[
                source._point_wkb(rows[0]["geometry"],True,False),
                source._point_wkb(rows[1]["geometry"],True,False)])
            geo=json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo["columns"]["geometry"]["geometry_types"],["Point Z"])
            manifest=json.loads(storage.read_key(result["candidate_ready"].replace(
                "metadata/candidate_ready.json","metadata/source_manifest.json")))
            self.assertFalse(manifest["historical_equivalence_accepted"])
            self.assertFalse(manifest["rights_accepted"])

            # Python considers bool/float numerically equal to the pinned int
            # IDs, and Arrow int64 can coerce 1.0. Reject both before staging.
            valid_bodies=deepcopy(bodies)
            valid_bodies["ids_before"]=deepcopy(bodies["ids_before"])
            valid_bodies["ids_after"]=deepcopy(bodies["ids_after"])
            variants=(
                ("ids_after", lambda value: value.update(objectIds=[1.0,2]), "source_changed_during_capture"),
                ("ids_after", lambda value: value.update(objectIds=[True,2]), "source_changed_during_capture"),
                ("page_000", lambda value: value["features"][0]["attributes"].update(OBJECTID=1.0), "source_page_id_slice_mismatch"),
                ("page_000", lambda value: value["features"][0]["attributes"].update(OBJECTID=True), "source_page_id_slice_mismatch"),
            )
            for index,(name,mutate,reason) in enumerate(variants):
                with self.subTest(name=name, invalid_id=index):
                    bodies.clear()
                    bodies.update(deepcopy(valid_bodies))
                    mutate(bodies[name])
                    case_root=tmp/f"invalid-id-{index}"
                    case_root.mkdir()
                    case_storage=StagingStorageResource(local_dir=str(case_root/"staging"),use_local=True,
                                                        backend="local",prefix="")
                    with patch.object(source,"EXPECTED_COUNT",2), patch.object(source,"PAGE_SIZE",2), \
                         patch.object(source,"capture_get",side_effect=fake_capture), \
                         patch.object(source.httpx,"Client"), \
                         patch.object(source.shutil,"disk_usage",return_value=SimpleNamespace(free=1_000_000_000)), \
                         patch.object(source,"processing_identity",return_value={
                             "code_identity_scheme":"test","working_tree_status":"unknown",
                             "code_commit":"test","files":{}}), \
                         patch.object(source,"validate_construction_code_identity"), \
                         self.assertRaisesRegex(ValueError,reason):
                        source.capture_national_transit_map_stops(
                            case_storage,version=f"synthetic-invalid-{index}",
                            capture_directory=str(case_root/"capture"))
            bodies.clear()
            bodies.update(valid_bodies)

    def test_multipage_capture_accepts_bts_transfer_limit_on_full_pages(self):
        layer, fields = _tiny_layer(2)
        rows = {oid: _stop_row(fields, oid, x, y, z) for oid, x, y, z in
                ((1,-73.0,42.0,17.5),(2,-122.0,47.0,18.5),(3,-118.0,34.0,19.5))}
        bodies=_metadata_bodies(layer,3,[1,2,3])
        # The live BTS layer sets exceededTransferLimit on every full ordered
        # offset page while more rows remain (observed 2026-11-11 on page 1 of
        # 337) and omits it on the final partial page.
        bodies["page_000"]={"spatialReference":{"wkid":4326,"latestWkid":4326},
                            "features":[rows[1],rows[2]],"exceededTransferLimit":True}
        bodies["page_001"]={"spatialReference":{"wkid":4326,"latestWkid":4326},
                            "features":[rows[3]]}
        observed=[]
        with tempfile.TemporaryDirectory() as tmp:
            tmp=Path(tmp)
            storage=StagingStorageResource(local_dir=str(tmp/"staging"),use_local=True,
                                           backend="local",prefix="")
            patches=_capture_patches(3,2,_fake_capture_get(observed,bodies))
            for p in patches:
                p.start()
            try:
                result=source.capture_national_transit_map_stops(
                    storage,version="synthetic",capture_directory=str(tmp/"capture"))
            finally:
                for p in patches:
                    p.stop()
            self.assertEqual(result["feature_count"],3)
            self.assertIn("page_000",observed)
            self.assertIn("page_001",observed)
            native=tmp/"staging"/result["candidate_ready"].replace(
                "metadata/candidate_ready.json",f"geoparquet/{source.DATASET_SLUG}.parquet")
            self.assertEqual(pq.read_table(native)["OBJECTID"].to_pylist(),[1,2,3])

            variants=(
                ("flagged_final_page", lambda: bodies["page_001"].update(exceededTransferLimit=True),
                 "source_final_page_exceeded_transfer_limit"),
                ("truncated_nonfinal_page", lambda: bodies["page_000"].update(features=[rows[1]]),
                 "source_page_incomplete"),
            )
            for index,(name,mutate,reason) in enumerate(variants):
                with self.subTest(name=name, invalid_page=index):
                    valid_page_000=deepcopy(bodies["page_000"])
                    valid_page_001=deepcopy(bodies["page_001"])
                    mutate()
                    case_root=tmp/f"invalid-page-{index}"
                    case_root.mkdir()
                    case_storage=StagingStorageResource(local_dir=str(case_root/"staging"),use_local=True,
                                                        backend="local",prefix="")
                    patches=_capture_patches(3,2,_fake_capture_get([],bodies))
                    for p in patches:
                        p.start()
                    try:
                        with self.assertRaisesRegex(ValueError,reason):
                            source.capture_national_transit_map_stops(
                                case_storage,version=f"synthetic-invalid-{index}",
                                capture_directory=str(case_root/"capture"))
                    finally:
                        for p in patches:
                            p.stop()
                    bodies["page_000"]=valid_page_000
                    bodies["page_001"]=valid_page_001

    def test_retained_capture_replay_round_trip_and_refusals(self):
        layer, fields = _tiny_layer(2)
        rows=[_stop_row(fields,1,-73.0,42.0,17.5),_stop_row(fields,2,-122.0,47.0,18.5)]
        bodies=_metadata_bodies(layer,2,[1,2])
        bodies["page_000"]={"spatialReference":{"wkid":4326,"latestWkid":4326},
                            "features":rows,"exceededTransferLimit":False}
        observed=[]
        with tempfile.TemporaryDirectory() as tmp:
            tmp=Path(tmp)
            capture_parent=tmp/"capture"
            storage=StagingStorageResource(local_dir=str(tmp/"staging"),use_local=True,
                                           backend="local",prefix="")
            patches=_capture_patches(2,2,_fake_capture_get(observed,bodies))
            for p in patches:
                p.start()
            try:
                live=source.capture_national_transit_map_stops(
                    storage,version="synthetic",capture_directory=str(capture_parent))
            finally:
                for p in patches:
                    p.stop()
            self.assertFalse(live["reused"])
            capture_root=Path(live["capture_directory"])
            manifest_path=capture_root/"capture_manifest.json"
            digest=hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            calls_after_live=len(observed)

            # The writer's own five-key descriptors (request_url,
            # response_budget_bytes, body, wire, receipt) must replay through
            # the retained-capture loader and hit the sealed candidate cache.
            replay_patches=_capture_patches(2,2,_fake_capture_get(observed,bodies))
            for p in replay_patches:
                p.start()
            try:
                replay=source.capture_national_transit_map_stops(
                    storage,version="synthetic-replay",
                    retained_capture_directory=str(capture_root),
                    retained_capture_manifest_sha256=digest)
            finally:
                for p in replay_patches:
                    p.stop()
            self.assertTrue(replay["reused"])
            self.assertEqual(replay["version"],live["version"])
            self.assertEqual(replay["feature_count"],2)
            self.assertEqual(len(observed),calls_after_live)

            tampered_patches=_capture_patches(2,2,_fake_capture_get(observed,bodies))
            for p in tampered_patches:
                p.start()
            try:
                with self.assertRaisesRegex(ValueError,"retained_capture_manifest_digest_mismatch"):
                    source.capture_national_transit_map_stops(
                        storage,version="synthetic-wrong-digest",
                        retained_capture_directory=str(capture_root),
                        retained_capture_manifest_sha256="0"*64)
                manifest=json.loads(manifest_path.read_text())
                manifest["captures"]["portal_item"].pop("response_budget_bytes")
                manifest_path.write_bytes(canonical(manifest)+b"\n")
                with self.assertRaisesRegex(ValueError,"retained_capture_manifest_invalid"):
                    source.capture_national_transit_map_stops(
                        storage,version="synthetic-tampered",
                        retained_capture_directory=str(capture_root),
                        retained_capture_manifest_sha256=digest)
            finally:
                for p in tampered_patches:
                    p.stop()


if __name__ == "__main__":
    unittest.main()
