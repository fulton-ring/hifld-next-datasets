import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.sources import national_transit_map_agencies as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def fixture():
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
    feature = {"attributes":{"OBJECTID":1,"NTD_ID":"0012","NTAD_NTD_ID":"0012","Agency_Name":"Example Transit","Common_Name":"Example","City":"Example","State":"CA","Lon":-120.0,"Lat":37.0,"In_Latest_NTAD_Upload":"Yes","License":"Public Domain"},"geometry":{"x":-120.0,"y":37.0,"z":0}}
    service={"serviceItemId":source.ITEM_ID,"layers":[{"id":0}]}
    item={"id":source.ITEM_ID,"owner":source.OWNER,"access":"public","url":source.SERVICE_URL,"description":"compiled on September 23, 2026"}
    docs={"item":item,"service_before":service,"layer_before":layer,"count_before":{"count":1},"ids_before":{"objectIds":[1]},"ids_seed":{"objectIds":[1]},
        "count_after":{"count":1},"ids_after":{"objectIds":[1]},"layer_after":layer,"service_after":service}
    for i in range(5): docs[f"page_{i:03}"]={"spatialReference":{"latestWkid":4326},"features":[feature] if i==0 else []}
    fields_profile,_=source._field_profile(fields)
    profile={"features":[feature]}
    record=hashlib.sha256(canonical([feature])).hexdigest()
    geometry=hashlib.sha256(canonical([[1,-120.0,37.0,0]])).hexdigest()
    schema=hashlib.sha256(canonical(fields_profile)).hexdigest()
    ids=hashlib.sha256(canonical([1])).hexdigest()
    captures={k:(canonical(v),{"complete_body":True,"status":200}) for k,v in docs.items()}
    return captures, (record,geometry,schema,ids)


class NationalTransitMapAgenciesCaptureTests(unittest.TestCase):
    def setUp(self):
        self.captures, hashes=fixture()
        self.patch=patch.multiple(source, EXPECTED_COUNT=1,EXPECTED_RECORD_SHA256=hashes[0],EXPECTED_GEOMETRY_SHA256=hashes[1],
            EXPECTED_SCHEMA_SHA256=hashes[2],EXPECTED_IDS_SHA256=hashes[3])
        self.patch.start()

    def tearDown(self):
        self.patch.stop()

    def test_accepts_complete_exact_population(self):
        fields,features=source.validate_captures(self.captures)
        self.assertEqual(len(fields),11)
        self.assertEqual(features[0]["attributes"]["NTD_ID"],"0012")

    def test_rejects_missing_or_changed_ids(self):
        docs=json.loads(self.captures["ids_after"][0]);docs["objectIds"]=[]
        self.captures["ids_after"]=(canonical(docs),{})
        with self.assertRaisesRegex(ValueError,"source_population_drift"):
            source.validate_captures(self.captures)

    def test_rejects_unexpected_column_and_type_drift(self):
        docs=json.loads(self.captures["layer_before"][0]);docs["fields"].append({"name":"NEW_FIELD","type":"esriFieldTypeString"})
        self.captures["layer_before"]=(canonical(docs),{})
        self.captures["layer_after"]=(canonical(docs),{})
        with self.assertRaisesRegex(ValueError,"source_schema_drift"):
            source.validate_captures(self.captures)

    def test_rejects_value_loss_and_source_type_change(self):
        page=json.loads(self.captures["page_000"][0]);page["features"][0]["attributes"]["NTD_ID"]="12"
        self.captures["page_000"]=(canonical(page),{})
        with self.assertRaisesRegex(ValueError,"source_values_or_geometry_drift"):
            source.validate_captures(self.captures)

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
            with patch.object(source,"EXPECTED_COUNT",1):
                replay=source._load_retained_capture(root,destination)
            self.assertEqual(replay["sample"],(body,receipt))
            self.assertEqual((destination/"sample.wire").read_bytes(),wire)


if __name__ == "__main__":
    unittest.main()
