"""Native source fidelity and drift tests for the USACE IENC building layer."""
import hashlib
import json
import struct
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq

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
        docs={"bts_item":{"id":source.BTS_ITEM_ID,"owner":"USDOT_BTS","title":"Inland Electronic Navigational Charts",
                "description":f"updated bi-monthly {source.BTS_FEATURE_CLASSES_URL} USACE_IENC_Master_Service_gdb.zip"},
            "directory":{"services":[{"name":"IENC/USACE_IENC_Master_Service","type":"MapServer"}]},
            "service_before":service,"service_after":service,"layer_before":layer,"layer_after":layer,
            "count_before":{"count":2},"count_after":{"count":2},"ids_before":{"objectIds":ids},"ids_after":{"objectIds":ids},
            "page_000":{"spatialReference":{"wkid":4326},"features":features}}
        return docs

    def validate(self,docs):
        captures={k:(canonical(v),{}) for k,v in docs.items()}
        with patch.object(source,"EXPECTED_COUNT",2),patch.object(source,"EXPECTED_ID_SHA256",hashlib.sha256(canonical([1,2])).hexdigest()), \
             patch.object(source,"BATCH_SIZE",200),patch.object(source,"EXPECTED_EXTENT",{"xmin":-1.0,"ymin":2.0,"xmax":1.0,"ymax":4.0}):
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
        self.assertTrue(building_locations_population_complete({"source_population_complete":True,"feature_count":892}).passed)
        self.assertFalse(building_locations_population_complete({"source_population_complete":False,"feature_count":892}).passed)


if __name__=="__main__":unittest.main()
