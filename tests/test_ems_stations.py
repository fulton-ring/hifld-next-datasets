"""Pinned USGS EMS source adapter and normal repository registration tests."""
import json
import unittest
from unittest.mock import patch
from dagster import AssetKey, DefaultScheduleStatus
from dagster_hifld.definitions import defs
from dagster_hifld.sources import ems_stations as source


class EMSStationTests(unittest.TestCase):
    def _fixture(self):
        fields = [
            {"name":"OBJECTID","type":"esriFieldTypeOID"}, {"name":"SHAPE","type":"esriFieldTypeGeometry"},
            {"name":"PERMANENT_IDENTIFIER","type":"esriFieldTypeString"}, {"name":"SOURCE_FEATUREID","type":"esriFieldTypeString"},
            {"name":"SOURCE_DATASETID","type":"esriFieldTypeString"}, {"name":"SOURCE_DATADESC","type":"esriFieldTypeString"},
            {"name":"SOURCE_ORIGINATOR","type":"esriFieldTypeString"}, {"name":"LOADDATE","type":"esriFieldTypeDate"},
            {"name":"FTYPE","type":"esriFieldTypeInteger"}, {"name":"FCODE","type":"esriFieldTypeInteger"},
            {"name":"NAME","type":"esriFieldTypeString"}, {"name":"POINTLOCATIONTYPE","type":"esriFieldTypeInteger"},
            {"name":"ADMINTYPE","type":"esriFieldTypeInteger"}, {"name":"ADDRESSBUILDINGNAME","type":"esriFieldTypeString"},
            {"name":"ADDRESS","type":"esriFieldTypeString"}, {"name":"CITY","type":"esriFieldTypeString"},
            {"name":"STATE","type":"esriFieldTypeString"}, {"name":"ZIPCODE","type":"esriFieldTypeString"},
            {"name":"GNIS_ID","type":"esriFieldTypeString"}, {"name":"GLOBALID","type":"esriFieldTypeGlobalID"},
        ]
        features=[]
        for oid in (17, 29, 43):
            attrs={"objectid":oid,"permanent_identifier":f"pid-{oid}","source_featureid":str(oid),"source_datasetid":"dataset",
              "source_datadesc":"USGS test","source_originator":"USGS","loaddate":1_700_000_000_000,
              "ftype":740,"fcode":74026,"name":f"Station {oid}","pointlocationtype":1,"admintype":None,
              "addressbuildingname":None,"address":None,"city":"X","state":"VA","zipcode":"00000","gnis_id":None,
              "globalid":f"{{00000000-0000-0000-0000-{oid:012d}}}"}
            features.append({"attributes":attrs,"geometry":{"x":-8_000_000.125+oid,"y":4_000_000.875+oid}})
        layer={"name":"Fire Stations/EMS Stations","geometryType":"esriGeometryPoint","maxRecordCount":2,
               "extent":{"spatialReference":{"wkid":102100,"latestWkid":3857}},"fields":fields}
        ids=[17,29,43]
        rows=[{"attributes":f["attributes"],"geometry":f["geometry"]} for f in features]
        bodies={"layer":json.dumps(layer).encode(),"count_before":json.dumps({"count":3}).encode(),
          "ids_before":json.dumps({"objectIds":ids}).encode(),"page_000":json.dumps({"geometryType":"esriGeometryPoint","spatialReference":{"wkid":102100,"latestWkid":3857},"features":features[:2]}).encode(),
          "page_001":json.dumps({"geometryType":"esriGeometryPoint","spatialReference":{"wkid":102100,"latestWkid":3857},"features":features[2:]}).encode(),
          "ids_after":json.dumps({"objectIds":ids}).encode(),"count_after":json.dumps({"count":3}).encode()}
        with patch.object(source,"COUNT",3), patch.object(source,"PAGE_SIZE",2):
            schema=source.canonical_hash(fields); id_hash=source.canonical_hash(ids); row_hash=source.canonical_hash(rows)
        return bodies, schema, id_hash, row_hash

    def test_full_three_row_native_schema_ids_and_xy_validate(self):
        bodies,schema,ids,rows=self._fixture()
        with patch.object(source,"COUNT",3), patch.object(source,"PAGE_SIZE",2), patch.object(source,"SCHEMA_SHA256",schema), patch.object(source,"IDS_SHA256",ids), patch.object(source,"ROWS_SHA256",rows):
            features,layer,quality=source.validate(bodies)
        self.assertEqual(quality["feature_count"],3)
        self.assertEqual(features[0]["geometry"],{"x":-8_000_000.125+17,"y":4_000_000.875+17})
        self.assertEqual(len(features[0]["attributes"]),19)
        self.assertEqual(features[0]["attributes"]["permanent_identifier"],"pid-17")

    def test_complete_row_fingerprint_rejects_coordinate_tampering(self):
        bodies,schema,ids,rows=self._fixture()
        page=json.loads(bodies["page_000"]); page["features"][0]["geometry"]["x"]+=0.001
        bodies["page_000"]=json.dumps(page).encode()
        with patch.object(source,"COUNT",3), patch.object(source,"PAGE_SIZE",2), patch.object(source,"SCHEMA_SHA256",schema), patch.object(source,"IDS_SHA256",ids), patch.object(source,"ROWS_SHA256",rows):
            with self.assertRaisesRegex(source.EMSQuarantineError,"source_rows_or_geometry_drift"):
                source.validate(bodies)

    def test_field_and_before_after_id_drift_fail_closed(self):
        bodies,schema,ids,rows=self._fixture()
        layer=json.loads(bodies["layer"]); layer["fields"][2]["type"]="esriFieldTypeInteger"
        drifted=dict(bodies); drifted["layer"]=json.dumps(layer).encode()
        with patch.object(source,"COUNT",3), patch.object(source,"PAGE_SIZE",2), patch.object(source,"SCHEMA_SHA256",schema), patch.object(source,"IDS_SHA256",ids), patch.object(source,"ROWS_SHA256",rows):
            with self.assertRaisesRegex(source.EMSQuarantineError,"source_schema_drift"):
                source.validate(drifted)
        bodies,schema,ids,rows=self._fixture(); drifted=dict(bodies)
        drifted["ids_after"]=json.dumps({"objectIds":[17,29,44]}).encode()
        with patch.object(source,"COUNT",3), patch.object(source,"PAGE_SIZE",2), patch.object(source,"SCHEMA_SHA256",schema), patch.object(source,"IDS_SHA256",ids), patch.object(source,"ROWS_SHA256",rows):
            with self.assertRaisesRegex(source.EMSQuarantineError,"source_id_drift"):
                source.validate(drifted)

    def test_incomplete_page_fails_closed(self):
        bodies,schema,ids,rows=self._fixture(); page=json.loads(bodies["page_001"]); page["features"]=[]
        bodies["page_001"]=json.dumps(page).encode()
        with patch.object(source,"COUNT",3), patch.object(source,"PAGE_SIZE",2), patch.object(source,"SCHEMA_SHA256",schema), patch.object(source,"IDS_SHA256",ids), patch.object(source,"ROWS_SHA256",rows):
            with self.assertRaisesRegex(source.EMSQuarantineError,"source_page_count_mismatch"):
                source.validate(bodies)

    def test_normal_repository_registers_stopped_local_refresh(self):
        repository=defs.get_repository_def()
        job=repository.get_job("usgs_ems_stations_draft_refresh")
        self.assertEqual(set(job.asset_layer.executable_asset_keys),{AssetKey(["ingest",source.SLUG,source.SLUG])})
        schedule=repository.get_schedule_def("usgs_ems_stations_draft_refresh_schedule")
        self.assertEqual(schedule.default_status,DefaultScheduleStatus.STOPPED)

if __name__ == "__main__": unittest.main()
