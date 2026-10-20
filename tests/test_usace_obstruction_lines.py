import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from dagster_hifld.sources import usace_obstruction_lines as source
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_obstruction_lines import (
    usace_obstruction_lines_complete, usace_obstruction_lines_draft_schedule,
    usace_obstruction_lines_usace_ienc,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _canonical


class UsaceObstructionLinesTests(unittest.TestCase):
    def test_registered_asset_and_stopped_refresh_gate(self):
        self.assertIn(usace_obstruction_lines_usace_ienc,hifld_ingest_assets)
        self.assertEqual(usace_obstruction_lines_draft_schedule.default_status,DefaultScheduleStatus.STOPPED)
        passed=usace_obstruction_lines_complete({"feature_count":16,"source_population_complete":True})
        self.assertTrue(passed.passed)
        self.assertFalse(passed.metadata["release_accepted"].value)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=str(Path(tmp)/"stage"))
            with self.assertRaisesRegex(ValueError,"unsafe_capture_version"):
                source.capture_usace_obstruction_lines(storage,version="../outside")

    def _documents(self):
        service={"mapName":"OBSTRUCTION_LINE","copyrightText":"USACE IENC",
                 "layers":[{"id":42,"name":"OBSTRUCTION_LINE","geometryType":"esriGeometryPolyline"}]}
        layer={"id":42,"name":"OBSTRUCTION_LINE","geometryType":"esriGeometryPolyline","maxRecordCount":2000,
               "spatialReference":{"latestWkid":4326},"extent":{**source.EXPECTED_EXTENT,"spatialReference":{"latestWkid":4326}},
               "fields":source.EXPECTED_FIELD_DECLARATIONS}
        attributes=[]
        for oid in source.EXPECTED_IDS:
            values={field["name"]:(oid if field["name"]=="OBJECTID" else
                                    30000 if field["name"]=="Scale_Minimum" else f"{field['name']}-{oid}")
                    for field in source.ATTRIBUTE_FIELDS}
            attributes.append({"attributes":values,"geometry":{"paths":[[[-88.0+oid/1000,37.0],[-87.9+oid/1000,37.1]]]}})
        features={"spatialReference":{"latestWkid":4326},"features":attributes}
        return {
            "bts_item":{"id":source.BTS_ITEM_ID,"owner":"USDOT_BTS","title":"Inland Electronic Navigational Charts",
                         "description":"USACE updates. Feature classes are https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes and USACE_IENC_Master_Service_gdb.zip."},
            "service_directory":{"services":[{"name":"IENC_Feature_Classes/OBSTRUCTION_LINE","type":"MapServer"}]},
            "service_before":service,"layer_before":layer,"count_before":{"count":16},"ids_before":{"objectIds":source.EXPECTED_IDS},
            "features":features,"layer_after":json.loads(json.dumps(layer)),"count_after":{"count":16},
            "ids_after":{"objectIds":source.EXPECTED_IDS},
        }

    def _capture_directory(self, root: Path):
        docs=self._documents()
        receipts={}
        expected_urls=dict(source._capture_urls(source.EXPECTED_IDS))
        for name,doc in docs.items():
            body=_canonical(doc)
            (root/f"{name}.body").write_bytes(body)
            (root/f"{name}.wire").write_bytes(body)
            receipt={"method":"GET","url":expected_urls[name],"final_url":expected_urls[name],
                     "observed_at":"2026-10-20T00:00:00Z","status":200,"complete_body":True,
                     "wire_bytes":len(body),"wire_sha256":source._sha(body),"bytes":len(body),"sha256":source._sha(body),
                     "content_encoding":"identity","content_length":len(body),"error_code":None}
            (root/f"{name}.receipt.json").write_bytes(_canonical(receipt)+b"\n")
            receipts[name]=receipt
        (root/"capture_manifest.json").write_bytes(_canonical({"complete":True,"receipts":receipts,
            "artifact_roster":capture_roster_digests(root,exclude={"capture_manifest.json"})})+b"\n")

    def _repin_receipt_manifest(self, root: Path, name: str, mutation):
        path=root/f"{name}.receipt.json"
        receipt=json.loads(path.read_bytes())
        mutation(receipt)
        path.write_bytes(_canonical(receipt)+b"\n")
        manifest=json.loads((root/"capture_manifest.json").read_bytes())
        manifest["receipts"][name]=receipt
        manifest["artifact_roster"]=capture_roster_digests(root,exclude={"capture_manifest.json"})
        (root/"capture_manifest.json").write_bytes(_canonical(manifest)+b"\n")

    def test_retained_capture_rejects_re_pinned_wrong_origin_query_and_method(self):
        cases=[
            ("origin", lambda r: r.update(url=r["url"].replace("ienccloud.us", "attacker.example"),
                                           final_url=r["final_url"].replace("ienccloud.us", "attacker.example"))),
            ("query", lambda r: r.update(url=r["url"]+"&unexpected=1", final_url=r["final_url"]+"&unexpected=1")),
            ("method", lambda r: r.update(method="POST")),
        ]
        for name,mutate in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp)/"capture";root.mkdir();self._capture_directory(root)
                self._repin_receipt_manifest(root,"features",mutate)
                manifest=json.loads((root/"capture_manifest.json").read_bytes())
                self.assertEqual(manifest["receipts"]["features"],json.loads((root/"features.receipt.json").read_bytes()))
                self.assertEqual(manifest["artifact_roster"],capture_roster_digests(root,exclude={"capture_manifest.json"}))
                with self.assertRaisesRegex(ValueError,"retained_capture_request_identity_mismatch:features"):
                    source._read_replay(root)

    def test_live_contract_fields_ids_coordinates_and_attributes_are_complete(self):
        captures={name:(_canonical(doc),{"status":200}) for name,doc in self._documents().items()}
        fields,features,audit=source._validate(captures)
        self.assertEqual(fields,source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features),16)
        self.assertEqual(audit["source_id_sha256"],source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"],32)
        self.assertEqual(features[0]["attributes"]["Feature_ID"],"Feature_ID-1")
        self.assertEqual(features[0]["geometry"]["paths"],[[[-87.999,37.0],[-87.899,37.1]]])

    def test_renamed_field_missing_id_or_extra_ordinate_quarantines_contract(self):
        docs=self._documents();docs=json.loads(json.dumps(docs));docs["layer_before"]["fields"][2]["name"]="Changed"
        with self.assertRaisesRegex(ValueError,"source_field_schema_drift"):
            source._validate({n:(_canonical(v),{}) for n,v in docs.items()})
        docs=self._documents();docs=json.loads(json.dumps(docs));docs["ids_after"]["objectIds"]=[*source.EXPECTED_IDS[:-1]]
        with self.assertRaisesRegex(ValueError,"source_id_population_drift"):
            source._validate({n:(_canonical(v),{}) for n,v in docs.items()})
        docs=self._documents();docs=json.loads(json.dumps(docs));docs["features"]["features"][0]["geometry"]["paths"][0][0].append(20)
        with self.assertRaisesRegex(ValueError,"source_xy_ordinate_contract_drift"):
            source._validate({n:(_canonical(v),{}) for n,v in docs.items()})

    def test_layer_page_and_explicit_feature_crs_reject_ambiguous_or_contradictory_values(self):
        mutations = [
            ("layer wkid/latestWkid contradiction", lambda d: d["layer_before"].update(
                spatialReference={"wkid":3857,"latestWkid":4326})),
            ("layer extent CRS contradiction", lambda d: d["layer_before"]["extent"].update(
                spatialReference={"wkid":3857})),
            ("page CRS drift", lambda d: d["features"].update(
                spatialReference={"wkid":3857,"latestWkid":3857})),
            ("feature CRS drift", lambda d: d["features"]["features"][0]["geometry"].update(
                spatialReference={"wkid":3857})),
            ("feature wkid/latestWkid contradiction", lambda d: d["features"]["features"][0]["geometry"].update(
                spatialReference={"wkid":3857,"latestWkid":4326})),
            ("feature WKT contradiction", lambda d: d["features"]["features"][0]["geometry"].update(
                spatialReference={"latestWkid":4326,"wkt":source.CRS.from_epsg(3857).to_wkt()})),
        ]
        for label, mutate in mutations:
            with self.subTest(label=label):
                docs=json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError,"crs_drift"):
                    source._validate({name:(_canonical(value),{}) for name,value in docs.items()})

    def test_fresh_same_source_reuses_first_manifest_and_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/"capture";root.mkdir();self._capture_directory(root)
            storage=StagingStorageResource(local_dir=str(Path(tmp)/"staging"))
            with patch.object(source,"validate_construction_code_identity"), patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                first=source.capture_usace_obstruction_lines(storage,version="draft",capture_directory=str(root))
            prefix=f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            manifest_before=storage.read_key(f"{prefix}/metadata/source_manifest.json")
            audit_before=storage.read_key(f"{prefix}/metadata/source_audit.json")
            receipts_before={name:storage.read_key(f"{prefix}/raw/{name}.receipt.json")
                             for name in json.loads((root/"capture_manifest.json").read_bytes())["receipts"]}
            capture_manifest=json.loads((root/"capture_manifest.json").read_bytes())
            for name,receipt in capture_manifest["receipts"].items():
                receipt["observed_at"]="2026-10-21T00:00:00Z"
                (root/f"{name}.receipt.json").write_bytes(_canonical(receipt)+b"\n")
            capture_manifest["receipts"]={n:json.loads((root/f"{n}.receipt.json").read_bytes())
                                           for n in capture_manifest["receipts"]}
            capture_manifest["artifact_roster"]=capture_roster_digests(root,exclude={"capture_manifest.json"})
            (root/"capture_manifest.json").write_bytes(_canonical(capture_manifest)+b"\n")
            with patch.object(source,"validate_construction_code_identity"), patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                second=source.capture_usace_obstruction_lines(storage,version="draft",capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"],second["version"])
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"),manifest_before)
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_audit.json"),audit_before)
            self.assertEqual({name:storage.read_key(f"{prefix}/raw/{name}.receipt.json") for name in receipts_before},receipts_before)
            self.assertEqual(second["source_capture_evidence_sha256"],first["source_capture_evidence_sha256"])


if __name__ == "__main__":
    unittest.main()
