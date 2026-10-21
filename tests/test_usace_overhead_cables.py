import json
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_overhead_cables import (
    usace_overhead_cables_complete, usace_overhead_cables_draft_schedule,
    usace_overhead_cables_usace_ienc,
)
from dagster_hifld.sources import usace_overhead_cables as source
from dagster_hifld.sources.epa_frs_common import _canonical


class UsaceOverheadCablesTests(unittest.TestCase):
    def test_asset_is_registered_refresh_stopped_and_gate_remains_unapproved(self):
        self.assertIn(usace_overhead_cables_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_overhead_cables_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = usace_overhead_cables_complete({"feature_count": 1631, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)

    def test_capture_page_roster_covers_source_ids_without_overlapping(self):
        urls = source._capture_roster(source.EXPECTED_IDS)
        page_names = [name for name, _url in urls if name.startswith("page_")]
        self.assertEqual(page_names, [f"page_{index:03d}" for index in range(7)])
        self.assertIn("objectIds=1%2C2%2C3", dict(urls)["page_000"])
        self.assertIn("objectIds=1501%2C1502", dict(urls)["page_006"])
        self.assertIn("returnZ=true", dict(urls)["page_000"])
        self.assertIn("returnM=true", dict(urls)["page_000"])

    def _documents(self):
        service = {"mapName": "OVERHEAD_CABLE_LINE", "copyrightText": "USACE IENC",
                   "layers": [{"id": 43, "name": "OVERHEAD_CABLE_LINE", "geometryType": "esriGeometryPolyline"}]}
        layer = {"id": 43, "name": "OVERHEAD_CABLE_LINE", "geometryType": "esriGeometryPolyline",
                 "maxRecordCount": 2000, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": source.EXPECTED_EXTENT, "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS",
                         "type": "PDF",
                         "title": "Inland Electronic Navigational Charts",
                         "description": "Official service https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes; "
                                        "USACE_IENC_Master_Service_gdb.zip"},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/OVERHEAD_CABLE_LINE", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT}, "ids_before": {"objectIds": source.EXPECTED_IDS},
            "layer_after": json.loads(json.dumps(layer)), "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS}, "service_after": json.loads(json.dumps(service)),
        }
        for page_index in range(7):
            ids = source.EXPECTED_IDS[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]
            rows = []
            for oid in ids:
                attrs = {field["name"]: oid if field["name"] == "OBJECTID" else
                         (30000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                         for field in source.ATTRIBUTE_FIELDS}
                rows.append({"attributes": attrs, "geometry": {"paths": [[[-90 + oid / 10000, 35],
                                                                             [-89 + oid / 10000, 36]]]}})
            docs[f"page_{page_index:03d}"] = {"spatialReference": {"latestWkid": 4326}, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def _capture_directory(self, root: Path):
        root.mkdir()
        expected_urls=dict(source._capture_roster(source.EXPECTED_IDS))
        receipts={}
        for name,doc in self._documents().items():
            body=_canonical(doc)
            (root/f"{name}.body").write_bytes(body)
            (root/f"{name}.wire").write_bytes(body)
            receipt={"method":"GET","url":expected_urls[name],"final_url":expected_urls[name],
                     "observed_at":"2026-10-20T00:00:00Z","status":200,"complete_body":True,
                     "wire_bytes":len(body),"wire_sha256":source._sha(body),"bytes":len(body),
                     "sha256":source._sha(body),"content_encoding":"identity",
                     "content_length":len(body),"error_code":None}
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
            ("origin", lambda r: (r.update(url=r["url"].replace("ienccloud.us", "attacker.example"),
                                           final_url=r["final_url"].replace("ienccloud.us", "attacker.example")))),
            ("query", lambda r: (r.update(url=r["url"]+"&unexpected=1", final_url=r["final_url"]+"&unexpected=1"))),
            ("method", lambda r: r.update(method="POST")),
        ]
        for name,mutate in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp)/"capture";self._capture_directory(root)
                self._repin_receipt_manifest(root,"page_000",mutate)
                manifest=json.loads((root/"capture_manifest.json").read_bytes())
                self.assertEqual(manifest["receipts"]["page_000"],json.loads((root/"page_000.receipt.json").read_bytes()))
                self.assertEqual(manifest["artifact_roster"],capture_roster_digests(root,exclude={"capture_manifest.json"}))
                with self.assertRaisesRegex(ValueError,"retained_capture_request_identity_mismatch:page_000"):
                    source._read_replay(root)

    def test_full_population_schema_ids_and_native_xy_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 1631)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 3262)
        self.assertEqual(audit["geometry_valid_count"], 1631)
        self.assertEqual(audit["bts_catalog_item_id"], "7ec34864c8804468a9cf52d437fe4b6c")
        self.assertEqual(audit["prior_bts_catalog_item_id"], "3607d485a9854d6fa458bf507058ceae")
        self.assertTrue(audit["bts_catalog_item_successor_review_pending"])
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")

    def test_obsolete_bts_map_service_item_is_not_accepted_as_current_receipt(self):
        docs = self._documents()
        docs["bts_item"]["id"] = source.PRIOR_BTS_ITEM_ID
        with self.assertRaisesRegex(ValueError, "bts_ienc_item_identity_drift"):
            source._validate(self._captures(docs))

    def test_missing_rows_schema_drift_transfer_limit_and_crs_override_fail_closed(self):
        mutations = [
            ("missing IDs", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]), "source_count_or_id"),
            ("field rename", lambda d: d["layer_before"]["fields"][2].update(name="renamed"), "source_field_schema_drift"),
            ("page truncation", lambda d: d["page_000"].update(exceededTransferLimit=True), "source_page_error"),
            ("page CRS override", lambda d: d["page_000"].update(spatialReference={"wkid": 3857, "latestWkid": 4326}), "crs_drift"),
            ("feature CRS override", lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"wkid": 4326, "latestWkid": 3857}), "crs_drift"),
        ]
        for label, mutate, error in mutations:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, error):
                    source._validate(self._captures(docs))

    def test_version_is_path_safe(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = source.StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_usace_overhead_cables(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()
