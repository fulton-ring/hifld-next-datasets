"""Small explicit synthetic source fixtures; complete NOAA capture is opt-in."""
import copy
import os
import struct
import tempfile
import unittest
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetKey, DefaultScheduleStatus
from dagster_hifld.sources import significant_earthquakes as f
from dagster_hifld.definitions import defs
from dagster_hifld.assets.significant_earthquakes import significant_earthquakes_draft_schedule, significant_earthquakes_full_quality


def page():
    features=[]
    for i in range(2):
        attrs={d['name']:None for d in f.FIELD_DEFINITIONS}
        attrs.update(YEAR=-2150 if i==0 else 2026,MONTH=1,DAY=None,HOUR=3,MINUTE=4,SECOND=0.1,DATE_STRING='-2150/01/00' if i==0 else '2026/01/01',LOCATION_NAME=' literal site ',COUNTRY='Explicit synthetic fixture',COMMENTS=' 001\n literal text ',OBJECTID=7000+i,ID=2+i,DRAWING_ORDER=99-i)
        features.append({'attributes':attrs,'geometry':{'x':1669681.0424083103,'y':4543887.574736076}if i==0 else None})
    return {'objectIdFieldName':'OBJECTID','geometryType':'esriGeometryPoint','spatialReference':{'wkid':102100,'latestWkid':3857},'fields':copy.deepcopy(f.PAGE_FIELDS),'features':features,'exceededTransferLimit':False}


def decode(p):return f.decode_pages([p],[7000,7001])


class SignificantEarthquakeTests(unittest.TestCase):
    def test_native_physical_types_distinct_ids_BCE_literal_dates_and_nulls(self):
        t=decode(page());self.assertEqual(t.num_rows,2)
        for name,kind in [('YEAR',pa.int32()),('MONTH',pa.int16()),('SECOND',pa.float32()),('LATITUDE',pa.float64()),('OBJECTID',pa.int64()),('ID',pa.int32())]:self.assertEqual(t[name].type,kind)
        self.assertEqual(t['YEAR'].to_pylist(),[-2150,2026]);self.assertEqual(t['SECOND'][0].as_py(),struct.unpack('<f',struct.pack('<f',0.1))[0]);self.assertEqual(t['DATE_STRING'][0].as_py(),'-2150/01/00');self.assertEqual(t['COMMENTS'][0].as_py(),' 001\n literal text ')
        self.assertEqual(t['geometry'].to_pylist(),[struct.pack('<BIdd',1,1,1669681.0424083103,4543887.574736076),None]);self.assertFalse(t.schema.field('OBJECTID').nullable)
        for d in f.FIELD_DEFINITIONS:self.assertEqual(t.schema.field(d['name']).metadata,{b'original_esri_field':f.encode(d)})
    def test_all_columns_and_deterministic_native_roundtrip(self):
        table=decode(page());self.assertEqual(table.column_names,[d['name']for d in f.FIELD_DEFINITIONS]+['geometry'])
        with tempfile.TemporaryDirectory()as d:
            root=Path(d);(root/'geoparquet').mkdir();path=root/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(table),path,compression='zstd');first=path.read_bytes();f.validate_output(root,table);pq.write_table(f.parquet_table(table),path,compression='zstd');self.assertEqual(first,path.read_bytes())
    def test_completeness_original_ids_and_unexpected_columns(self):
        for mutate in[lambda p:p['features'].pop(),lambda p:p['features'][0]['attributes'].pop('ID'),lambda p:p['features'][0]['attributes'].update(new_column=1),lambda p:p['features'][1]['attributes'].update(OBJECTID=7000),lambda p:p['features'][1]['attributes'].update(OBJECTID=9000)]:
            p=page();mutate(p)
            with self.assertRaises(ValueError):decode(p)
    def test_integer_widths_types_float32_overflow_and_literal_strings(self):
        for name,value in [('YEAR',2**31),('MONTH',32768),('MONTH',1.0),('YEAR',False),('OBJECTID',True),('OBJECTID',2**63),('SECOND',1e40),('SECOND',float('nan')),('SECOND',True),('LATITUDE',2**53+1),('COMMENTS',1),('DATE_STRING','x'*14)]:
            p=page();p['features'][0]['attributes'][name]=value
            with self.assertRaises(ValueError,msg=name):decode(p)
    def test_typed_schema_CRS_and_extended_geometry_drift(self):
        for mutate in[lambda p:p['fields'][0].update(nullable=0),lambda p:p['spatialReference'].update(latestWkid=3857.0),lambda p:p.update(exceededTransferLimit=0),lambda p:p['features'][0]['geometry'].update(z=1),lambda p:p['features'][0]['geometry'].update(x=float('inf'))]:
            p=page();mutate(p)
            with self.assertRaises(ValueError):decode(p)
    def test_complete_id_census_rejects_duplicate_boolean_and_float(self):
        for ids in[[7000,7000],[True,7001],[7000.0,7001],[7000,None]]:
            with self.assertRaises(ValueError):f.id_roster(f.encode({'objectIdFieldName':'OBJECTID','objectIds':ids}),2)
        self.assertEqual(f.id_roster(f.encode({'objectIdFieldName':'OBJECTID','objectIds':[7001,7000]}),2),[7000,7001])
    def test_receipt_true_bool_status_and_concrete_delegation(self):
        b=b'explicit synthetic receipt';name='count_before';r={'url':f.URLS[name],'final_url':f.URLS[name],'status':200,'bytes':len(b),'sha256':f.digest(b),'complete_body':True,'observed_at':'2026-10-19T00:00:00+00:00'}
        self.assertEqual(f.validate_receipt(name,b,r)['delegation_evidence']['portal_owner'],'ncei_noaa')
        for key,value in [('complete_body',1),('status',200.0),('bytes',float(len(b)))]:
            bad={**r,key:value}
            with self.assertRaises(ValueError):f.validate_receipt(name,b,bad)
    def test_registered_source_quality_and_stopped_monthly_refresh(self):
        job=defs.resolve_job_def('significant_earthquakes_draft_refresh');self.assertEqual(job.asset_layer.executable_asset_keys,{AssetKey(['ingest',f.SLUG,f.SLUG])});self.assertTrue(next(iter(significant_earthquakes_full_quality.check_specs)).blocking);self.assertEqual(significant_earthquakes_draft_schedule.default_status,DefaultScheduleStatus.STOPPED)
    @unittest.skipUnless(os.environ.get('HIFLD_EARTHQUAKES_REAL_CAPTURE_DIR'),'complete real source external artifact opt-in')
    def test_complete_real_noaa_capture(self):
        root=Path(os.environ['HIFLD_EARTHQUAKES_REAL_CAPTURE_DIR']);captures={n:((root/(n+'.body')).read_bytes(),f.strict_json((root/(n+'.receipt.json')).read_bytes()))for n in f.URLS};t,a=f.inspect_captures(captures)
        self.assertEqual((t.num_rows,len(t.column_names),a['BCE_count']),(6634,68,47));self.assertEqual(a['null_geometry_count'],0);self.assertEqual(a['SECOND_wire_to_declared_binary32_differences'],1927)
