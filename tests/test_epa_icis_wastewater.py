from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely
from dagster import DagsterInstance, materialize

from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_icis_wastewater as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


class EpaIcisWastewaterTests(unittest.TestCase):
    def test_tiny_complete_capture_keeps_native_ids_values_geometry_and_unapproved_gate(self):
        fields = [
            {"name":"OBJECTID", "type":"esriFieldTypeOID", "alias":"OBJECTID", "nullable":False},
            {"name":"REGISTRY_ID", "type":"esriFieldTypeString", "alias":"Registry ID", "length":50, "nullable":True, "domain":None},
            {"name":"FAC_LAT", "type":"esriFieldTypeDouble", "alias":"FAC_LAT", "nullable":True},
            {"name":"FAC_LONG", "type":"esriFieldTypeDouble", "alias":"FAC_LONG", "nullable":True},
        ]
        plain = [{k:f[k] for k in ("name","type","alias","length","nullable","domain") if k in f} for f in fields]
        schema_sha = hashlib.sha256(canonical(plain)).hexdigest()
        portal={"id":source.ITEM_ID,"owner":"EPA_GEO","title":"EPA Facility Registry Service - Integrated Compliance Information System (ICIS) Wastewater Treatment Plants","url":source.SERVICE_URL,"access":"public","modified":1779295761000}
        service = {"serviceItemId":source.ITEM_ID, "layers":[{"id":0,"name":source.LAYER_NAME}]}
        layer = {"name":source.LAYER_NAME, "geometryType":"esriGeometryPoint", "hasZ":False, "hasM":False, "extent":{"spatialReference":{"latestWkid":4269}}, "fields":fields}
        ids = {"objectIds":[901,902]}
        row = {"attributes":{"OBJECTID":901,"REGISTRY_ID":"0000042007","FAC_LAT":40.1,"FAC_LONG":-72.25}, "geometry":{"x":-72.25,"y":40.1}}
        null_geometry_row = {"attributes":{"OBJECTID":902,"REGISTRY_ID":"0000042008","FAC_LAT":None,"FAC_LONG":None}, "geometry":None}
        bodies = {"portal_item":portal,"service_before":service,"service_after":service,"layer_before":layer,"layer_after":layer,
                 "count_before":{"count":2},"count_after":{"count":2},"ids_before":ids,"ids_after":ids,
                 "page_000":{"spatialReference":{"latestWkid":4269},"features":[row,null_geometry_row]}}

        def capture(_client, url, root, name, **_kwargs):
            payload = bodies[name]
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
            receipt = {"url":url,"final_url":url,"observed_at":"2026-01-01T00:00:00+00:00","status":200,
                       "complete_body":True,"bytes":len(body),"sha256":hashlib.sha256(body).hexdigest(),
                       "wire_bytes":len(body),"wire_sha256":hashlib.sha256(body).hexdigest()}
            root.mkdir(parents=True, exist_ok=True)
            (root/f"{name}.wire").write_bytes(body)
            (root/f"{name}.body").write_bytes(body)
            (root/f"{name}.receipt.json").write_bytes(canonical(receipt)+b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with patch.object(source, "SCHEMA_SHA256", schema_sha), \
                 patch.object(source, "capture_get", side_effect=capture), \
                 patch.object(source.httpx, "Client"):
                result=source.capture_epa_icis_wastewater(storage, version="vfixture", capture_directory=str(Path(tmp)/"captures"))
            self.assertEqual(result["feature_count"],2)
            self.assertEqual(result["source_null_geometry_count"],1)
            self.assertFalse(result["release_accepted"])
            ready=json.loads(storage.read_key(result["candidate_ready"]))
            self.assertFalse(ready["approved"])
            parquet_path=Path(tmp)/result["candidate_ready"].replace("metadata/candidate_ready.json",f"geoparquet/{source.DATASET_SLUG}.parquet")
            table=pq.read_table(parquet_path)
            self.assertEqual(table["OBJECTID"].to_pylist(),[901,902])
            self.assertEqual(table["REGISTRY_ID"].to_pylist(),["0000042007","0000042008"])
            self.assertEqual(table["geometry"].to_pylist(),[shapely.geometry.Point(-72.25,40.1).wkb,None])
            self.assertIn(b"geo", table.schema.metadata)

            partition=build_publish_partition_key(source.DATASET_SLUG,source.FILE_SLUG,result["version"])
            instance_dir=Path(tmp)/"dagster"; instance_dir.mkdir()
            (instance_dir/"dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            with DagsterInstance.from_config(str(instance_dir)) as instance:
                instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[partition])
                catalog=materialize([publish_catalog],resources={"staging_storage":storage},instance=instance,partition_key=partition)
                self.assertTrue(catalog.success)
                quality=json.loads(storage.read_key(f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}/metadata/quality_manifest.json"))
                self.assertFalse(quality["quality_check_passed"])
                with patch.object(source, "SCHEMA_SHA256", schema_sha), \
                     patch.object(source, "capture_get", side_effect=capture), \
                     patch.object(source.httpx, "Client"):
                    cached=source.capture_epa_icis_wastewater(storage,version="vrefresh",capture_directory=str(Path(tmp)/"captures"))
                self.assertTrue(cached["reused"])
            # A missing source ID invalidates the page before any candidate is written or reused.
            bodies["page_000"]={"spatialReference":{"latestWkid":4269},"features":[row]}
            with patch.object(source, "SCHEMA_SHA256", schema_sha), \
                 patch.object(source, "capture_get", side_effect=capture), \
                 patch.object(source.httpx, "Client"):
                with self.assertRaisesRegex(ValueError,"source_page_incomplete"):
                    source.capture_epa_icis_wastewater(storage,version="vbad",capture_directory=str(Path(tmp)/"bad-captures"))


if __name__ == "__main__":
    unittest.main()
