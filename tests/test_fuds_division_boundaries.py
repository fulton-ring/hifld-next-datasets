"""One bounded synthetic capture→seal→catalog→cache lifecycle and negatives."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.catalog import summarize_staged_catalog
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import fuds_division_boundaries as source


def _fixtures(*, bad_ids: bool = False, bad_schema: bool = False) -> dict[str, dict]:
    fields = [
        {"name":"OBJECTID","type":"esriFieldTypeOID","alias":"OBJECTID","domain":None},
        {"name":"DIV","type":"esriFieldTypeString","alias":"DIV","length":9,"domain":None},
        {"name":"MIL_DIV","type":"esriFieldTypeString","alias":"MIL_DIV","length":50,"domain":None},
        {"name":"MACOM_ID","type":"esriFieldTypeDouble","alias":"MACOM_ID","domain":None},
        {"name":"SHAPE","type":"esriFieldTypeGeometry","alias":"SHAPE","domain":None},
        {"name":"SE_ANNO_CAD_DATA","type":"esriFieldTypeBlob","alias":"SE_ANNO_CAD_DATA","domain":None},
        {"name":"SHAPE_Length","type":"esriFieldTypeDouble","alias":"SHAPE_Length","domain":None},
        {"name":"SHAPE_Area","type":"esriFieldTypeDouble","alias":"SHAPE_Area","domain":None},
    ]
    if bad_schema: fields[1]["length"] = 10
    layer = {"id":10,"name":source.LAYER_NAME,"geometryType":"esriGeometryPolygon","fields":fields,
        "extent":{"spatialReference":{"wkid":4326,"latestWkid":4326}},
        "advancedQueryCapabilities":{"supportsPagination":True,"supportsOrderBy":True}}
    sq1=[[0,0],[0,1],[1,1],[1,0],[0,0]]
    sq2=[[2,0],[2,1],[3,1],[3,0],[2,0]]
    features=[{"attributes":{"OBJECTID":1,"DIV":"LRD","MIL_DIV":"Great Lakes","MACOM_ID":1.0,"SHAPE_Length":4.0,"SHAPE_Area":1.0},"geometry":{"rings":[sq1]}},
              {"attributes":{"OBJECTID":2,"DIV":"MVD","MIL_DIV":"Mississippi","MACOM_ID":2.0,"SHAPE_Length":4.0,"SHAPE_Area":1.0},"geometry":{"rings":[sq2]}}]
    page=[features[1],features[0]] if bad_ids else features
    page0={"features":page[:1],"spatialReference":{"wkid":4326,"latestWkid":4326},"exceededTransferLimit":True}
    page1={"features":page[1:],"spatialReference":{"wkid":4326,"latestWkid":4326}}
    return {"service_before":{"serviceItemId":"fuds"},"service_after":{"serviceItemId":"fuds"},
        "layer_before":layer,"layer_after":layer,"count_before":{"count":2},"count_after":{"count":2},
        "ids_before":{"objectIds":[1,2]},"ids_after":{"objectIds":[2,1]},"page_000":page0,"page_001":page1}


class FudsDivisionBoundariesTests(unittest.TestCase):
    def test_tiny_full_candidate_catalog_cache_and_ordered_id_negative(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp,use_local=True,backend="local")
            docs=_fixtures()
            def fake_get(_client,url,root,name,max_bytes):
                body=json.dumps(docs[name],separators=(",",":")).encode(); wire=body
                (root/f"{name}.wire").write_bytes(wire)
                receipt={"method":"GET","url":url,"final_url":url,"observed_at":"2026-01-01T00:00:00Z",
                    "status":200,"complete_body":True,"wire_bytes":len(wire),"wire_sha256":hashlib.sha256(wire).hexdigest(),
                    "bytes":len(body),"sha256":hashlib.sha256(body).hexdigest(),"content_encoding":"identity","content_length":len(body),"error_code":None}
                return body,receipt
            with patch.object(source,"PAGE_SIZE",1), patch.object(source,"capture_get",side_effect=fake_get):
                candidate=source.capture_fuds_division_boundaries(storage,version="vfixture",capture_directory=tmp+"/captures")
            self.assertEqual(candidate["feature_count"],2)
            self.assertFalse(candidate["release_accepted"])
            summary=summarize_staged_catalog(storage,source.DATASET_SLUG,source.FILE_SLUG,candidate["version"],"FUDS divisions")
            prefix=f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
            storage.write_key(f"{prefix}/metadata/quality_manifest.json",json.dumps(summary.quality_manifest).encode())
            storage.write_key(f"{prefix}/metadata/data_dictionary.json",json.dumps(summary.data_dictionary).encode())
            with patch.object(source,"PAGE_SIZE",1), patch.object(source,"capture_get",side_effect=fake_get):
                reused=source.capture_fuds_division_boundaries(storage,version="vfixture2",capture_directory=tmp+"/captures")
            self.assertTrue(reused["reused"])

            bad=_fixtures(bad_ids=True)
            with patch.object(source,"PAGE_SIZE",1), patch.object(source,"capture_get",side_effect=lambda _c,url,root,name,max_bytes: fake_get(_c,url,root,name,max_bytes) if name != "page_000" else _bad_page(bad,name,url,root)):
                with self.assertRaisesRegex(ValueError,"source_page_id_slice_mismatch"):
                    source.capture_fuds_division_boundaries(storage,version="vbad",capture_directory=tmp+"/bad")

    def test_pinned_schema_and_polygon_hole_parent_are_fail_closed(self):
        with self.assertRaisesRegex(ValueError,"source_schema_drift"):
            source._schema(_fixtures(bad_schema=True)["layer_before"])
        outer=[[0,0],[0,4],[4,4],[4,0],[0,0]]
        orphan=[[5,5],[5,6],[6,6],[6,5],[5,5]]
        with self.assertRaisesRegex(ValueError,"hole_without_containing_shell"):
            source._esri_polygon({"rings":[outer,orphan[::-1]]})
        with self.assertRaisesRegex(ValueError,"coordinate_dimensions"):
            source._esri_polygon({"rings":[[[0,0,1],[0,1,1],[1,1,1],[1,0,1],[0,0,1]]]})


def _bad_page(bad,name,url,root):
    body=json.dumps(bad[name],separators=(",",":")).encode(); (root/f"{name}.wire").write_bytes(body)
    return body,{"method":"GET","url":url,"final_url":url,"observed_at":"2026-01-01T00:00:00Z","status":200,
        "complete_body":True,"wire_bytes":len(body),"wire_sha256":hashlib.sha256(body).hexdigest(),
        "bytes":len(body),"sha256":hashlib.sha256(body).hexdigest(),"content_encoding":"identity","content_length":len(body),"error_code":None}


if __name__ == "__main__": unittest.main()
