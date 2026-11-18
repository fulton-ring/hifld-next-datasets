import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus

from dagster_hifld.sources import national_transit_map_agencies as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def fixture(oids=(1,)):
    fields = [
        {"name":"OBJECTID","type":"esriFieldTypeOID","alias":"OBJECTID","nullable":False,"domain":None},
        {"name":"NTD_ID","type":"esriFieldTypeString","alias":"NTD_ID","length":254,"nullable":True,"domain":None},
        {"name":"NTAD_NTD_ID","type":"esriFieldTypeString","alias":"NTAD_NTD_ID","length":254,"nullable":True,"domain":None},
        {"name":"Agency_Name","type":"esriFieldTypeString","alias":"Agency_Name","length":254,"nullable":True,"domain":None},
        {"name":"Common_Name","type":"esriFieldTypeString","alias":"Common_Name","length":254,"nullable":True,"domain":None},
        {"name":"City","type":"esriFieldTypeString","alias":"City","length":254,"nullable":True,"domain":None},
        {"name":"State","type":"esriFieldTypeString","alias":"State","length":254,"nullable":True,"domain":None},
        {"name":"Lon","type":"esriFieldTypeDouble","alias":"Lon","nullable":True,"domain":None},
        {"name":"Lat","type":"esriFieldTypeDouble","alias":"Lat","nullable":True,"domain":None},
        {"name":"In_Latest_NTAD_Upload","type":"esriFieldTypeString","alias":"In_Latest_NTAD_Upload","length":254,"nullable":True,"domain":None},
        {"name":"License","type":"esriFieldTypeString","alias":"License","length":254,"nullable":True,"domain":None},
    ]
    layer = {"id":0,"name":"National Transit Map Agencies","geometryType":"esriGeometryPoint","objectIdField":"OBJECTID","maxRecordCount":2000,"hasZ":True,"hasM":False,
        "extent":{"spatialReference":{"latestWkid":4326}},"fields":fields}
    features = [
        {"attributes":{"OBJECTID":oid,"NTD_ID":f"{oid:04d}","NTAD_NTD_ID":f"{oid:04d}","Agency_Name":f"Example Transit {oid}","Common_Name":"Example","City":"Example","State":"CA","Lon":-120.0+oid,"Lat":37.0,"In_Latest_NTAD_Upload":"Yes","License":"Public Domain"},"geometry":{"x":-120.0+oid,"y":37.0,"z":0}}
        for oid in oids
    ]
    service={"serviceItemId":source.ITEM_ID,"layers":[{"id":0}]}
    item={"id":source.ITEM_ID,"owner":source.OWNER,"access":"public","url":source.SERVICE_URL,"description":"compiled on September 23, 2026"}
    ids=sorted(oids)
    docs={"item":item,"service_before":service,"layer_before":layer,"count_before":{"count":len(ids)},"ids_before":{"objectIds":ids},"ids_seed":{"objectIds":ids},
        "count_after":{"count":len(ids)},"ids_after":{"objectIds":ids},"layer_after":layer,"service_after":service}
    for i in range(5): docs[f"page_{i:03}"]={"spatialReference":{"latestWkid":4326},"features":features if i==0 else []}
    fields_profile,_=source._field_profile(fields)
    schema=hashlib.sha256(canonical(fields_profile)).hexdigest()
    captures={k:(canonical(v),{"complete_body":True,"status":200}) for k,v in docs.items()}
    return captures, schema


class NationalTransitMapAgenciesCaptureTests(unittest.TestCase):
    def setUp(self):
        self.captures, schema=fixture()
        self.patch=patch.object(source, "EXPECTED_SCHEMA_SHA256", schema)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()

    def test_accepts_complete_exact_population(self):
        fields,features=source.validate_captures(self.captures)
        self.assertEqual(len(fields),11)
        self.assertEqual(features[0]["attributes"]["NTD_ID"],"0001")
        self.assertFalse(hasattr(source, "EXPECTED_COUNT"))

    def test_rejects_missing_or_changed_ids(self):
        docs=json.loads(self.captures["ids_after"][0]);docs["objectIds"]=[]
        self.captures["ids_after"]=(canonical(docs),{})
        with self.assertRaisesRegex(ValueError,"source_count_or_id_roster_drift"):
            source.validate_captures(self.captures)

    def test_rejects_unexpected_column_and_type_drift(self):
        docs=json.loads(self.captures["layer_before"][0]);docs["fields"].append({"name":"NEW_FIELD","type":"esriFieldTypeString"})
        self.captures["layer_before"]=(canonical(docs),{})
        self.captures["layer_after"]=(canonical(docs),{})
        with self.assertRaisesRegex(ValueError,"source_schema_drift"):
            source.validate_captures(self.captures)

    def test_accepts_value_edits_and_rejects_source_type_change(self):
        page=json.loads(self.captures["page_000"][0]);page["features"][0]["attributes"]["NTD_ID"]="1"
        self.captures["page_000"]=(canonical(page),{})
        fields,features=source.validate_captures(self.captures)
        self.assertEqual(features[0]["attributes"]["NTD_ID"],"1")

        self.captures,_=fixture()
        layer=json.loads(self.captures["layer_before"][0]);layer["fields"][0]["type"]="esriFieldTypeInteger"
        self.captures["layer_before"]=(canonical(layer),{});self.captures["layer_after"]=(canonical(layer),{})
        with self.assertRaisesRegex(ValueError,"source_schema_drift"):
            source.validate_captures(self.captures)

    def test_rejects_geometry_and_truncated_pages(self):
        page=json.loads(self.captures["page_000"][0]);page["features"][0]["geometry"]={"x":-120.0}
        self.captures["page_000"]=(canonical(page),{})
        with self.assertRaisesRegex(ValueError,"source_record_or_geometry_schema_drift"):
            source.validate_captures(self.captures)

        self.captures,_=fixture()
        page=json.loads(self.captures["page_000"][0]);page["exceededTransferLimit"]=True
        self.captures["page_000"]=(canonical(page),{})
        with self.assertRaisesRegex(ValueError,"source_page_error_or_truncation"):
            source.validate_captures(self.captures)

    def test_accepts_an_upstream_added_feature_and_warns_on_unapproved_drift(self):
        from dagster_hifld.assets.national_transit_map_agencies import (
            bts_national_transit_map_agencies_approved_baseline_drift,
        )

        self.captures,_=fixture(oids=(1,2))
        fields,features=source.validate_captures(self.captures)
        self.assertEqual([f["attributes"]["OBJECTID"] for f in features],[1,2])
        candidate={"dataset_slug":source.DATASET_SLUG,"file_slug":source.FILE_SLUG,
            "feature_count":2,"source_id_sha256":source._sha(source._canonical([1,2])),
            "release_accepted":False}
        drift=bts_national_transit_map_agencies_approved_baseline_drift(candidate)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value,"skipped_no_approved_baseline")

    def test_replays_retained_body_wire_and_original_receipt_without_fetching(self):
        body=b'{"captured":"body"}';wire=b'wire-body'
        receipt={"url":"https://example.invalid/source","final_url":"https://example.invalid/source",
            "observed_at":"2026-10-19T20:00:00+00:00","complete_body":True,"status":200,
            "bytes":len(body),"sha256":hashlib.sha256(body).hexdigest(),
            "wire_bytes":len(wire),"wire_sha256":hashlib.sha256(wire).hexdigest()}
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);destination=root/"copy";destination.mkdir()
            (root/"sample.body").write_bytes(body);(root/"sample.wire").write_bytes(wire)
            (root/"sample.receipt.json").write_bytes(canonical(receipt)+b"\n")
            (root/"capture_manifest.json").write_bytes(canonical({"complete":True,"source_feature_count":1,"receipts":{"sample":receipt}}))
            replay=source._load_retained_capture(root,destination)
            self.assertEqual(replay["sample"],(body,receipt))
            self.assertEqual((destination/"sample.wire").read_bytes(),wire)


class NationalTransitMapAgenciesRegistrationTests(unittest.TestCase):
    def test_registered_checks_job_and_stopped_schedule(self):
        from dagster_hifld.assets.national_transit_map_agencies import (
            bts_national_transit_map_agencies,
            bts_national_transit_map_agencies_candidate_complete,
            bts_national_transit_map_agencies_release_gate,
        )
        from dagster_hifld.definitions import defs

        repo = defs.get_repository_def()
        job = repo.get_job("bts_national_transit_map_agencies_draft_refresh")
        self.assertEqual(job.asset_layer.executable_asset_keys, bts_national_transit_map_agencies.keys)
        graph = defs.resolve_asset_graph()
        self.assertTrue(set(bts_national_transit_map_agencies_candidate_complete.check_keys) <= graph.asset_check_keys)
        self.assertTrue(set(bts_national_transit_map_agencies_release_gate.check_keys) <= graph.asset_check_keys)
        for check in (bts_national_transit_map_agencies_candidate_complete, bts_national_transit_map_agencies_release_gate):
            self.assertTrue(next(iter(check.check_specs)).blocking)
        schedule = repo.get_schedule_def("bts_national_transit_map_agencies_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.cron_schedule, "0 8 1 * *")
        self.assertEqual(schedule.execution_timezone, "America/New_York")


if __name__ == "__main__":
    unittest.main()
