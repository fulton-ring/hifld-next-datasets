"""Native source fidelity and drift tests for the USACE IENC building layer."""
import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq

from dagster_hifld.sources.arcgis_layer import layer_capture_roster

from dagster_hifld.assets.building_locations_usace_ienc import building_locations_population_complete
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import building_locations_usace_ienc as source


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False).encode()


class BuildingLocationsSourceTests(unittest.TestCase):
    def fixture(self):
        ids=[1,2]
        layer={"id":5,"name":"BUILDING_SINGLE_POINT","geometryType":"esriGeometryPoint","maxRecordCount":2000,
            "fields":source.EXPECTED_FIELDS,"spatialReference":{"wkid":4326,"latestWkid":4326},
            "extent":{"xmin":-1.0,"ymin":2.0,"xmax":1.0,"ymax":4.0,"spatialReference":{"wkid":4326}}}
        attrs=[]; features=[]
        for oid,x,y in ((1,-1.0,2.0),(2,1.0,4.0)):
            row={"OBJECTID":oid,"Visually_Conspicuous":"yes","Building_Function":"bridge","Object_Name":f"b{oid}",
                "Condition":"good","Information":"info","Scale_Minimum":5000,"Source_Date":"2025","Source_Indication":"survey",
                "Source_Dataset":"IENC","Feature_ID":f"USACE-{oid}"}
            attrs.append(row);features.append({"attributes":row,"geometry":{"x":x,"y":y,"spatialReference":{"wkid":4326}}})
        service={"mapName":"USACE_IENC_Master_Service","layers":[{"id":5,"name":"BUILDING_SINGLE_POINT"}]}
        docs={"bts_item":{"id":source.BTS_ITEM_ID,"owner":"USDOT_BTS","type":"PDF","title":"Inland Electronic Navigational Charts",
                "description":f"updated bi-monthly {source.BTS_FEATURE_CLASSES_URL} USACE_IENC_Master_Service_gdb.zip"},
            "directory":{"services":[{"name":"IENC/USACE_IENC_Master_Service","type":"MapServer"}]},
            "service_before":service,"service_after":service,"layer_before":layer,"layer_after":layer,
            "count_before":{"count":2},"count_after":{"count":2},"ids_before":{"objectIds":ids},"ids_after":{"objectIds":ids},
            "page_000":{"spatialReference":{"wkid":4326},"features":features}}
        return docs

    def validate(self,docs):
        captures={k:(canonical(v),{}) for k,v in docs.items()}
        with patch.object(source,"BATCH_SIZE",200),patch.object(source,"EXPECTED_EXTENT",{"xmin":-1.0,"ymin":2.0,"xmax":1.0,"ymax":4.0}):
            return source._validate(captures)

    def test_native_xy_attributes_and_types_survive_geoparquet(self):
        fields,features,audit=self.validate(self.fixture())
        data=source._parquet_bytes(fields,features)
        table=pq.read_table(__import__("io").BytesIO(data))
        self.assertEqual(table.column("OBJECTID").to_pylist(),[1,2])
        self.assertEqual(table.column("Scale_Minimum").to_pylist(),[5000,5000])
        self.assertEqual(table.schema.field("Scale_Minimum").type.__str__(),"int32")
        self.assertEqual(table.column("Feature_ID").to_pylist(),["USACE-1","USACE-2"])
        self.assertEqual([struct.unpack("<BIdd",v)[2:] for v in table.column("geometry").to_pylist()],[(-1.0,2.0),(1.0,4.0)])
        geo=json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"],{"authority":"EPSG","code":4326})
        self.assertEqual(audit["source_feature_count"],2)

    def test_incomplete_page_and_schema_drift_fail_closed(self):
        docs=self.fixture();docs["page_000"]["features"].pop()
        with self.assertRaisesRegex(ValueError,"source_page_population_incomplete"):
            self.validate(docs)
        docs=self.fixture();docs["layer_before"]["fields"]=docs["layer_before"]["fields"][:-1];docs["layer_after"]=docs["layer_before"]
        with self.assertRaisesRegex(ValueError,"source_schema_drift"):
            self.validate(docs)

    def test_prior_bts_item_and_missing_pdf_type_fail_closed(self):
        self.assertEqual(source.PRIOR_BTS_ITEM_ID,"3607d485a9854d6fa458bf507058ceae")
        docs=self.fixture();docs["bts_item"]["id"]=source.PRIOR_BTS_ITEM_ID
        with self.assertRaisesRegex(ValueError,"bts_item_identity_drift"):
            self.validate(docs)
        docs=self.fixture();docs["bts_item"].pop("type")
        with self.assertRaisesRegex(ValueError,"bts_item_identity_drift"):
            self.validate(docs)
        fields,features,audit=self.validate(self.fixture())
        self.assertEqual(audit["bts_catalog_item_id"],source.BTS_ITEM_ID)
        self.assertEqual(audit["prior_bts_catalog_item_id"],source.PRIOR_BTS_ITEM_ID)
        self.assertTrue(audit["bts_catalog_item_successor_review_pending"])

    def test_conflicting_crs_declarations_and_layer_drift_fail_closed(self):
        for target,label in (("layer_before","source_layer_crs_drift"),("page_000","page_000_response_crs_drift")):
            docs=self.fixture();
            if target=="layer_before":
                docs[target]["spatialReference"]={"wkid":3857,"latestWkid":4326};docs["layer_after"]=docs[target]
            else:docs[target]["spatialReference"]={"wkid":3857,"latestWkid":4326}
            with self.subTest(target=target),self.assertRaisesRegex(ValueError,label):self.validate(docs)
        docs=self.fixture();docs["layer_after"]={**docs["layer_after"],"extent":{"xmin":0}}
        with self.assertRaisesRegex(ValueError,"source_layer_changed_during_capture"):
            self.validate(docs)

    def test_nonlocal_or_published_storage_is_rejected(self):
        with self.assertRaisesRegex(ValueError,"explicit_isolated_local_staging_required"):
            source.capture_building_locations(StagingStorageResource(bucket="remote",use_local=False,local_dir="/tmp/x"),version="v1")
        with self.assertRaisesRegex(ValueError,"explicit_isolated_local_staging_required"):
            source.capture_building_locations(PublishedStorageResource(use_local=True,local_dir="/tmp/x"),version="v1")

    def test_population_check_requires_complete_native_count(self):
        self.assertTrue(building_locations_population_complete({"source_population_complete":True,"feature_count":2,"source_reported_count":2}).passed)
        self.assertFalse(building_locations_population_complete({"source_population_complete":False,"feature_count":2,"source_reported_count":2}).passed)
        self.assertFalse(building_locations_population_complete({"source_population_complete":True,"feature_count":2,"source_reported_count":3}).passed)
    def test_failed_capture_quarantines_readable_evidence_and_records_unreadable_receipt(self):
        docs = self.fixture()
        docs["page_000"] = {**docs["page_000"], "features": docs["page_000"]["features"][:-1]}
        ids = [1, 2]
        def roster(object_ids):
            return layer_capture_roster(object_ids, layer_url=source.LAYER_URL, service_url=source.SERVICE_URL,
                page_size=source.BATCH_SIZE,
                extra_documents=(("bts_item", source.BTS_ITEM_URL), ("directory", source.SERVICE_DIRECTORY_URL)),
                after_order=("service_after", "layer_after", "count_after", "ids_after"))
        expected_urls = dict(roster(ids))
        bodies = {url: canonical(doc).decode() for name, url in expected_urls.items() for doc in [docs[name]]}
        page_first_url = expected_urls["page_000"]

        def serve(request):
            url = str(request.url)
            if url == page_first_url:
                root = next((Path(tmp) / "captures" / "torn-page").iterdir())
                (root / "count_before.receipt.json").write_bytes(b'{')
            return httpx.Response(200, stream=httpx.ByteStream(bodies[url].encode()))

        real_client = httpx.Client
        with tempfile.TemporaryDirectory() as tmp, patch.object(source, "BATCH_SIZE", 200), \
                patch.object(source, "EXPECTED_EXTENT", {"xmin": -1.0, "ymin": 2.0, "xmax": 1.0, "ymax": 4.0}), \
                patch.object(source.httpx, "Client",
                side_effect=lambda **kwargs: real_client(transport=httpx.MockTransport(serve), **kwargs)):
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaises(ValueError) as raised:
                source.capture_building_locations(storage, version="torn-page",
                                                  capture_directory=str(Path(tmp) / "captures"))
            self.assertEqual(str(raised.exception), "source_page_population_incomplete:page_000")
            self.assertNotIsInstance(raised.exception, (json.JSONDecodeError, OSError))
            self.assertEqual(storage.list_prefix(source.DATASET_SLUG), [])
            quarantine_keys = storage.list_prefix("_quarantine/" + source.DATASET_SLUG)
            failure_key = next(key for key in quarantine_keys if key.endswith("/metadata/failure.json"))
            failure = json.loads(storage.read_key(failure_key))
            self.assertEqual(failure["reason_code"], "ValueError")
            self.assertEqual(failure["captured_names"],
                             sorted(name for name in expected_urls if name != "count_before"))
            self.assertEqual(failure["not_retained"],
                             [{"name": "count_before",
                               "reason": "retained_capture_receipt_unreadable:count_before"}])
            self.assertTrue(any(key.endswith("/raw/bts_item.body") for key in quarantine_keys))
            self.assertTrue(any(key.endswith("/raw/page_000.wire") for key in quarantine_keys))
            self.assertTrue(any(key.endswith("/raw/ids_before.receipt.json") for key in quarantine_keys))
            root = next((Path(tmp) / "captures" / "torn-page").iterdir())
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])
            self.assertEqual(manifest["error_code"], "ValueError")
            self.assertNotIn("receipts", manifest)


if __name__=="__main__":unittest.main()
