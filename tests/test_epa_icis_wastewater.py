from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely
import httpx
from dagster import DagsterInstance, DefaultScheduleStatus, build_schedule_context, materialize

from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets.epa_icis_wastewater import (
    epa_icis_wastewater_draft_schedule,
    epa_icis_wastewater_treatment_plants,
)
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_icis_wastewater as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


class EpaIcisWastewaterTests(unittest.TestCase):
    def setUp(self):
        # These unit fixtures exercise capture policy with tiny responses; disk
        # capacity behavior is checked separately by the production guard.
        self.disk_guard = patch.object(source, "_require_disk_space")
        self.disk_guard.start()

    def tearDown(self):
        self.disk_guard.stop()

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
        layer = {"name":source.LAYER_NAME, "geometryType":"esriGeometryPoint", "hasZ":False, "hasM":False, "advancedQueryCapabilities":{"supportsPagination":True,"supportsOrderBy":True}, "extent":{"spatialReference":{"latestWkid":4269}}, "fields":fields}
        ids = {"objectIds":list(range(10001,10252))}
        rows=[{"attributes":{"OBJECTID":i+10000,"REGISTRY_ID":f"00000{i+10000:05d}","FAC_LAT":40.1+i/1000,"FAC_LONG":-72.25-i/1000},
               "geometry":{"x":-72.25-i/1000,"y":40.1+i/1000}} for i in range(1,252)]
        rows[-1]={"attributes":{"OBJECTID":10251,"REGISTRY_ID":"0000010251","FAC_LAT":None,"FAC_LONG":None},"geometry":None}
        row=rows[0]
        bodies = {"portal_item":portal,"service_before":service,"service_after":service,"layer_before":layer,"layer_after":layer,
                 "count_before":{"count":251},"count_after":{"count":251},"ids_before":ids,"ids_after":ids,
                 "page_000":{"spatialReference":{"latestWkid":4269},"exceededTransferLimit":True,"features":rows[:250]},
                 "page_001":{"spatialReference":{"latestWkid":4269},"exceededTransferLimit":False,"features":rows[250:]}}

        request_urls={}
        capture_kwargs={}
        def capture(_client, url, root, name, **_kwargs):
            request_urls[name]=url
            capture_kwargs[name]=_kwargs
            payload = bodies[name]
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
            receipt = {"method":"GET","url":url,"final_url":url,"observed_at":"2026-01-01T00:00:00+00:00","status":200,
                       "complete_body":True,"bytes":len(body),"sha256":hashlib.sha256(body).hexdigest(),
                       "wire_bytes":len(body),"wire_sha256":hashlib.sha256(body).hexdigest()}
            root.mkdir(parents=True, exist_ok=True)
            for kind in ("wire", "body"):
                _kwargs["before_retain"](kind, body)
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
            self.assertEqual(result["feature_count"],251)
            self.assertEqual(result["source_null_geometry_count"],1)
            self.assertFalse(result["release_accepted"])
            ready=json.loads(storage.read_key(result["candidate_ready"]))
            self.assertFalse(ready["approved"])
            parquet_path=Path(tmp)/result["candidate_ready"].replace("metadata/candidate_ready.json",f"geoparquet/{source.DATASET_SLUG}.parquet")
            table=pq.read_table(parquet_path)
            self.assertEqual(table["OBJECTID"].to_pylist(),list(range(10001,10252)))
            self.assertEqual(table["REGISTRY_ID"].to_pylist()[0],"0000010001")
            self.assertEqual(table["geometry"].to_pylist()[0],shapely.geometry.Point(-72.251,40.101).wkb)
            self.assertIsNone(table["geometry"].to_pylist()[-1])
            self.assertIn(b"geo", table.schema.metadata)
            self.assertEqual(parse_qs(urlsplit(request_urls["page_000"]).query)["resultOffset"],["0"])
            self.assertEqual(parse_qs(urlsplit(request_urls["page_001"]).query)["resultOffset"],["250"])
            self.assertNotIn("objectIds",parse_qs(urlsplit(request_urls["page_001"]).query))
            self.assertTrue(all(kwargs.get("max_bytes") == source.MAX_RESPONSE_BYTES and callable(kwargs.get("before_retain")) for kwargs in capture_kwargs.values()))

            partition=build_publish_partition_key(source.DATASET_SLUG,source.FILE_SLUG,result["version"])
            instance_dir=Path(tmp)/"dagster"; instance_dir.mkdir()
            (instance_dir/"dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            with DagsterInstance.from_config(str(instance_dir)) as instance:
                instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[partition])
                catalog=materialize([publish_catalog],resources={"staging_storage":storage},instance=instance,partition_key=partition)
                self.assertTrue(catalog.success)
                quality=json.loads(storage.read_key(f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}/metadata/quality_manifest.json"))
                self.assertFalse(quality["quality_check_passed"])
                retained_dir=Path(tmp)/"captures"/"vfixture"
                with patch.object(source, "SCHEMA_SHA256", schema_sha), \
                     patch.object(source.httpx, "Client", side_effect=AssertionError("offline replay attempted HTTP")):
                    cached=source.capture_epa_icis_wastewater(storage,version="vrefresh",retained_capture_directory=str(retained_dir),retained_capture_manifest_sha256=result["capture_manifest_sha256"])
                self.assertTrue(cached["reused"])
                self.assertEqual(cached["capture_manifest_sha256"],result["capture_manifest_sha256"])
                wire_path=retained_dir/"page_000.wire"
                original_wire=wire_path.read_bytes()
                wire_path.write_bytes(original_wire+b"tamper")
                with patch.object(source, "SCHEMA_SHA256", schema_sha), \
                     patch.object(source.httpx, "Client", side_effect=AssertionError("tampered replay attempted HTTP")):
                    with self.assertRaisesRegex(ValueError,"retained_capture_artifact_size_mismatch"):
                        source.capture_epa_icis_wastewater(storage,version="vtampered",retained_capture_directory=str(retained_dir),retained_capture_manifest_sha256=result["capture_manifest_sha256"])
                with wire_path.open("wb") as stream:
                    stream.truncate(source.MAX_RESPONSE_BYTES + 1)
                with patch.object(source, "SCHEMA_SHA256", schema_sha), \
                     patch.object(source.httpx, "Client", side_effect=AssertionError("oversized replay attempted HTTP")):
                    with self.assertRaisesRegex(ValueError,"retained_capture_artifact_oversized"):
                        source.capture_epa_icis_wastewater(storage,version="voversized",retained_capture_directory=str(retained_dir),retained_capture_manifest_sha256=result["capture_manifest_sha256"])
                wire_path.write_bytes(original_wire)
            # A missing source ID invalidates the page before any candidate is written or reused.
            bodies["page_000"]={"spatialReference":{"latestWkid":4269},"features":[row]}
            with patch.object(source, "SCHEMA_SHA256", schema_sha), \
                 patch.object(source, "capture_get", side_effect=capture), \
                 patch.object(source.httpx, "Client"):
                with self.assertRaisesRegex(ValueError,"source_page_incomplete"):
                    source.capture_epa_icis_wastewater(storage,version="vbad",capture_directory=str(Path(tmp)/"bad-captures"))
            bodies["page_000"]={"spatialReference":{"latestWkid":4269},"exceededTransferLimit":True,"features":rows[:250]}
            bodies["page_001"]={"spatialReference":{"latestWkid":4269},"exceededTransferLimit":True,"features":rows[250:]}
            with patch.object(source, "SCHEMA_SHA256", schema_sha), \
                 patch.object(source, "capture_get", side_effect=capture), \
                 patch.object(source.httpx, "Client"):
                with self.assertRaisesRegex(ValueError,"source_final_page_exceeded_transfer_limit"):
                    source.capture_epa_icis_wastewater(storage,version="vbadfinal",capture_directory=str(Path(tmp)/"bad-final-captures"))

            # This adapter writes 2D WKB only; reject future dimensional layers before paging.
            for dimension in ("hasZ", "hasM"):
                layer[dimension]=True
                with patch.object(source, "SCHEMA_SHA256", schema_sha), \
                     patch.object(source, "capture_get", side_effect=capture), \
                     patch.object(source.httpx, "Client"):
                    with self.assertRaisesRegex(ValueError,"source_geometry_dimensions_unsupported"):
                        source.capture_epa_icis_wastewater(storage,version=f"v{dimension}",capture_directory=str(Path(tmp)/f"{dimension}-captures"))
                layer[dimension]=False

    def test_capture_budget_bounds_and_prewrite_response_overshoot_refusal(self):
        self.assertEqual(source.MAX_CAPTURE_BYTES, 96_000_000)
        self.assertEqual(epa_icis_wastewater_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        schedule_request = epa_icis_wastewater_draft_schedule.evaluate_tick(
            build_schedule_context()
        ).run_requests[0]
        self.assertEqual(
            schedule_request.run_config["ops"][epa_icis_wastewater_treatment_plants.op.name]["config"]["max_capture_bytes"],
            96_000_000,
        )
        source._validate_capture_limits(1_000_000, 100_000)
        for total, response in ((0, 1), (source.MAX_CAPTURE_BYTES + 1, 1), (10, 0), (10, source.MAX_RESPONSE_BYTES + 1), (10, 11)):
            with self.subTest(total=total, response=response), self.assertRaises(ValueError):
                source._validate_capture_limits(total, response)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/"capture"
            data=b"0123456789"
            client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200,content=data,request=request)))
            retained=[0]
            def refuse_before_overshoot(_kind, block):
                if retained[0]+len(block)>5:
                    raise ValueError("test_capture_budget_exceeded")
                retained[0]+=len(block)
            with client, self.assertRaisesRegex(ValueError,"incomplete_source_capture:oversized"):
                source.capture_get(client,"https://example.invalid/source",root,"oversized",max_bytes=64,before_retain=refuse_before_overshoot)
            self.assertEqual((root/"oversized.wire").stat().st_size,0)
            self.assertEqual((root/"oversized.body").stat().st_size,0)
            receipt=json.loads((root/"oversized.receipt.json").read_text())
            self.assertFalse(receipt["complete_body"])


if __name__ == "__main__":
    unittest.main()
