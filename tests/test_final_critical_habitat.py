"""Focused geometry and completeness tests for the FWS adapter."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from dagster import DagsterInstance, materialize

from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.partitions import build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import final_critical_habitat as source
from dagster_hifld.sources.final_critical_habitat import _esri_polygon, _feature_rows, _parquet


class FinalCriticalHabitatTests(unittest.TestCase):
    def test_aggregate_budget_reserves_and_refuses_over_cap(self):
        with tempfile.TemporaryDirectory(prefix="hifld-90-budget-") as temp:
            root=Path(temp); (root/"baseline.bin").write_bytes(b"baseline")
            with patch.object(source,"MAX_ISSUE_BYTES",12):
                source._enforce_issue_budget((root,),4)
                with self.assertRaisesRegex(OSError,"disk_budget"):
                    source._enforce_issue_budget((root,),5)

    def test_esri_rings_preserve_hole_and_coordinates(self):
        shell = [[0, 0], [0, 2], [2, 2], [2, 0], [0, 0]]  # Esri clockwise shell
        hole = [[0.5, 0.5], [1.5, 0.5], [1.5, 1.5], [0.5, 1.5], [0.5, 0.5]]
        geom = _esri_polygon({"rings": [shell, hole]})
        self.assertEqual(len(geom.geoms), 1)
        self.assertEqual(len(geom.geoms[0].interiors), 1)
        self.assertEqual(list(geom.geoms[0].exterior.coords), [tuple(x) for x in shell])
        self.assertEqual(list(geom.geoms[0].interiors[0].coords), [tuple(x) for x in hole])

    def test_nested_esri_island_rings_attach_holes_to_nearest_shell(self):
        outer=[[0,0],[0,10],[10,10],[10,0],[0,0]]
        outer_hole=[[1,1],[9,1],[9,9],[1,9],[1,1]]
        island=[[3,3],[3,7],[7,7],[7,3],[3,3]]
        island_hole=[[4,4],[6,4],[6,6],[4,6],[4,4]]
        geom=_esri_polygon({"rings":[outer,outer_hole,island,island_hole]})
        self.assertEqual(len(geom.geoms),2)
        self.assertEqual(sorted(len(p.interiors) for p in geom.geoms),[1,1])
        output_rings=[]
        for p in geom.geoms:
            output_rings.append([list(x) for x in p.exterior.coords])
            output_rings.extend([[list(x) for x in r.coords] for r in p.interiors])
        self.assertEqual({tuple(map(tuple,r)) for r in output_rings},
                         {tuple(map(tuple,r)) for r in (outer,outer_hole,island,island_hole)})

    def test_complete_capture_validator_preserves_all_fields_and_ids(self):
        fields = [{"name":"OBJECTID","type":"esriFieldTypeOID","nullable":False},
                  {"name":"entity_id","type":"esriFieldTypeInteger","nullable":True},
                  {"name":"code","type":"esriFieldTypeString","length":8,"nullable":True}]
        layer = {"name":"Final Critical Habitat Features","geometryType":"esriGeometryPolygon",
                 "extent":{"spatialReference":{"wkid":102100,"latestWkid":3857}},"fields":fields,"hasZ":False,"hasM":False}
        service = {"layers":[{"id":0,"name":"Final Critical Habitat Features"},{"id":2,"name":"Proposed Critical Habitat Features"}]}
        ids = [9, 7]
        ring1 = [[0,0],[0,2],[2,2],[2,0],[0,0]]
        ring2 = [[3,0],[3,2],[5,2],[5,0],[3,0]]
        features = [
            {"attributes":{"OBJECTID":7,"entity_id":None,"code":"007"},"geometry":{"rings":[ring1]}},
            {"attributes":{"OBJECTID":9,"entity_id":4001,"code":"0009"},"geometry":{"rings":[ring2]}},
        ]
        docs = {
            "portal_item":{"id":"794de45b9d774d21aed3bf9b5313ee24","owner":"aditya_sastry@fws.gov_fws","url":"https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/USFWS_Critical_Habitat/FeatureServer"},
            "service_before":service,"service_after":service,"layer_before":layer,"layer_after":layer,
            "count_before":{"count":2},"count_after":{"count":2},
            "ids_seed":{"objectIds":ids},"ids_before":{"objectIds":ids},"ids_after":{"objectIds":ids},
            "page_0000":{"spatialReference":{"wkid":102100,"latestWkid":3857},"features":features},
        }
        captures = {name:(json.dumps(value).encode(),{}) for name,value in docs.items()}
        rows,audit = _feature_rows(captures,fields)
        self.assertEqual([r["attributes"]["OBJECTID"] for r in rows],sorted(ids))
        self.assertEqual(rows[0]["attributes"]["code"],"007")
        self.assertIsNone(rows[0]["attributes"]["entity_id"])
        self.assertEqual(audit["source_unique_id_count"],2)
        parquet = _parquet(fields,rows)
        self.assertTrue(parquet.startswith(b"PAR1"))

    def test_tiny_full_candidate_catalog_and_first_receipt_cache(self):
        fields = [{"name":"OBJECTID","type":"esriFieldTypeOID","nullable":False},
                  {"name":"entity_id","type":"esriFieldTypeInteger","nullable":True},
                  {"name":"code","type":"esriFieldTypeString","length":8,"nullable":True}]
        layer = {"name":"Final Critical Habitat Features","geometryType":"esriGeometryPolygon",
                 "extent":{"spatialReference":{"wkid":102100,"latestWkid":3857}},"fields":fields}
        service = {"layers":[{"id":0,"name":"Final Critical Habitat Features"},{"id":2,"name":"Proposed Critical Habitat Features"}]}
        ids = [9, 7]
        features = [{"attributes":{"OBJECTID":7,"entity_id":None,"code":"007"},"geometry":{"rings":[[[0,0],[0,2],[2,2],[2,0],[0,0]]]}},
                    {"attributes":{"OBJECTID":9,"entity_id":4001,"code":"0009"},"geometry":{"rings":[[[3,0],[3,2],[5,2],[5,0],[3,0]]]}}]
        payloads = {
            "/sharing/rest/content/items/794de45b9d774d21aed3bf9b5313ee24": {"id":source.ITEM_ID,"owner":"aditya_sastry@fws.gov_fws","url":source.SERVICE_URL},
            "/USFWS_Critical_Habitat/FeatureServer":service,
            "/USFWS_Critical_Habitat/FeatureServer/0":{"name":source.LAYER_NAME,"geometryType":"esriGeometryPolygon","extent":layer["extent"],"fields":fields,"hasZ":False,"hasM":False},
            "/USFWS_Critical_Habitat/FeatureServer/0/query":None,
        }
        def handler(request):
            path=request.url.path
            if path.endswith("/0/query"):
                params=dict(request.url.params)
                if params.get("returnIdsOnly")=="true": body={"objectIds":ids}
                elif params.get("returnCountOnly")=="true": body={"count":len(ids)}
                else:
                    selected=set(map(int,params.get("objectIds","").split(",")))
                    body={"spatialReference":{"wkid":102100,"latestWkid":3857},
                          "features":[x for x in features if x["attributes"]["OBJECTID"] in selected]}
            elif path.endswith("/0"): body=payloads["/USFWS_Critical_Habitat/FeatureServer/0"]
            elif path.endswith("/FeatureServer"): body=service
            else: body=payloads["/sharing/rest/content/items/794de45b9d774d21aed3bf9b5313ee24"]
            return httpx.Response(200,stream=httpx.ByteStream(json.dumps(body).encode()),
                                  headers={"content-type":"application/json"},request=request)

        def identity(_root):
            return {"code_commit":"a"*40,"files":{"fixture":"b"*64},"python":"3.12","pyarrow":"fixture","proj":"fixture","geos":"fixture","gdal":"fixture"}

        with tempfile.TemporaryDirectory(prefix="hifld-90-fixture-") as temp:
            staging=StagingStorageResource(local_dir=str(Path(temp)/"staging"),use_local=True,bucket=None,prefix="")
            transport=httpx.MockTransport(handler)
            real_client=httpx.Client
            client_factory=lambda *args,**kwargs:real_client(transport=transport,**kwargs)
            patches=[patch.object(source.httpx,"Client",side_effect=client_factory),
                     patch.object(source,"PAGE_SIZE",1),
                     patch.object(source,"processing_identity",side_effect=identity),
                     patch.object(source,"validate_construction_code_identity"),
                     patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity")]
            with patches[0],patches[1],patches[2],patches[3],patches[4]:
                first=source.capture_final_critical_habitat(staging,version="fixture",capture_directory=str(Path(temp)/"captures"))
                partition=build_publish_partition_key(source.DATASET_SLUG,source.FILE_SLUG,first["version"])
                with DagsterInstance.ephemeral() as instance:
                    instance.add_dynamic_partitions("publish_dataset_file_version",[partition])
                    result=materialize([publish_catalog],instance=instance,partition_key=partition,
                                       resources={"staging_storage":staging})
                self.assertTrue(result.success)
                prefix=f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
                self.assertIsNotNone(staging.object_snapshot(prefix+"/metadata/quality_manifest.json"))
                self.assertIsNotNone(staging.object_snapshot(prefix+"/metadata/data_dictionary.json"))
                second=source.capture_final_critical_habitat(staging,version="fixture",capture_directory=str(Path(temp)/"captures"))
                source_snapshot_key=f"{prefix}/metadata/source/source_manifest.json"
                source_snapshot=staging.object_snapshot(source_snapshot_key)
                staging.write_key_if_unchanged(source_snapshot_key,b"tampered catalog snapshot",source_snapshot)
                with self.assertRaisesRegex(ValueError,"cached_catalog_source_snapshot_changed"):
                    source.capture_final_critical_habitat(staging,version="fixture",capture_directory=str(Path(temp)/"captures"))
            self.assertEqual(first["version"],second["version"])
            self.assertTrue(second["reused"])
            observations=list((Path(temp)/"observations").glob("*.json"))
            self.assertEqual(len(observations),1)
            observation=json.loads(observations[0].read_text())
            self.assertTrue(observation["receipts"])


if __name__ == "__main__":
    unittest.main()
