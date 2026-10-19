"""Tiny explicit synthetic event fixtures; complete official source opt-in."""
import copy
import json
import os
import struct
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetKey, DefaultScheduleStatus
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import significant_volcanic_events as f
from dagster_hifld.definitions import defs
from dagster_hifld.assets.noaa_significant_volcanic import significant_volcanic_source_integrity_schedule


def page():
    features=[]
    for i in range(2):
        attrs={d['name']:None for d in f.FIELD_DEFINITIONS};attrs.update(YEAR=-4360 if i==0 else None,MO=None,DAY=4 if i else None,VEI=None,VOL_ID=101.0,NUM='0101-06=',NAME=' literal name ',COMMENTS='<p> 001 literal & text </p>',TIME_ERUPT='D1',SIG_ID=42+i,TSU_ID=2800+i,HAZ_EVENT_ID=1,OBJECTID=7000+i,ID=10106,COUNTRY='Explicit synthetic country')
        features.append({'attributes':attrs,'geometry':{'x':1669681.0424083103,'y':4543887.574736076}})
    return {'objectIdFieldName':'OBJECTID','geometryType':'esriGeometryPoint','spatialReference':{'wkid':102100,'latestWkid':3857},'fields':copy.deepcopy(f.PAGE_FIELDS),'features':features,'exceededTransferLimit':False}


def decode(p):return f.decode_pages([p],[7000,7001])


class SignificantVolcanicTests(unittest.TestCase):
    def test_original_ids_physical_types_BCE_nulls_literal_dates_and_WKB(self):
        t=decode(page());self.assertEqual(t.num_rows,2);self.assertEqual(t['YEAR'].type,pa.int16());self.assertEqual(t['SIG_ID'].type,pa.int32());self.assertEqual(t['VOL_ID'].type,pa.float64());self.assertEqual(t['OBJECTID'].type,pa.int32())
        self.assertEqual(t['YEAR'].to_pylist(),[-4360,None]);self.assertEqual(t['NUM'][0].as_py(),'0101-06=');self.assertEqual(t['TIME_ERUPT'][0].as_py(),'D1');self.assertEqual(t['NAME'][0].as_py(),' literal name ');self.assertEqual(t['COMMENTS'][0].as_py(),'<p> 001 literal & text </p>')
        self.assertEqual(t['geometry'][0].as_py(),struct.pack('<BIdd',1,1,1669681.0424083103,4543887.574736076));self.assertFalse(t.schema.field('OBJECTID').nullable);self.assertTrue(t.schema.field('YEAR').nullable)
        for d in f.FIELD_DEFINITIONS:self.assertEqual(t.schema.field(d['name']).metadata,{b'original_esri_field':f.encode(d)})
    def test_all_source_columns_and_deterministic_native_roundtrip(self):
        t=decode(page());self.assertEqual(t.column_names,[d['name']for d in f.FIELD_DEFINITIONS]+['geometry'])
        with tempfile.TemporaryDirectory()as d:
            r=Path(d);(r/'geoparquet').mkdir();p=r/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(t),p,compression='zstd');first=p.read_bytes();f.validate_output(r,t);pq.write_table(f.parquet_table(t),p,compression='zstd');self.assertEqual(first,p.read_bytes())
    def test_missing_extra_duplicate_and_unexpected_ids(self):
        for mutate in[lambda p:p['features'][0]['attributes'].pop('NUM'),lambda p:p['features'][0]['attributes'].update(new='x'),lambda p:p['features'][1]['attributes'].update(OBJECTID=7000),lambda p:p['features'][1]['attributes'].update(OBJECTID=9999)]:
            p=page();mutate(p)
            with self.assertRaises(ValueError):decode(p)
    def test_physical_widths_bools_fractional_integers_and_nonfinite(self):
        for n,v in[('YEAR',-32769),('YEAR',True),('YEAR',2020.0),('SIG_ID',2**31),('OBJECTID',False),('VOL_ID',True),('VOL_ID',float('nan')),('VOL_ID',2**53+1),('COMMENTS',8),('NUM','x'*10000)]:
            p=page();p['features'][0]['attributes'][n]=v
            with self.assertRaises(ValueError,msg=n):decode(p)
    def test_incomplete_transfer_CRS_geometry_and_metadata_type_drift(self):
        for mutate in[lambda p:p.update(exceededTransferLimit=True),lambda p:p.update(exceededTransferLimit=0),lambda p:p['features'].pop(),lambda p:p['spatialReference'].update(latestWkid=4326),lambda p:p['spatialReference'].update(latestWkid=3857.0),lambda p:p['features'][0]['geometry'].update(z=1),lambda p:p['features'][0]['geometry'].update(x=float('nan')),lambda p:p['features'][0]['geometry'].update(x=2**53+1),lambda p:p['fields'][0].update(nullable=1)]:
            p=page();mutate(p)
            with self.assertRaises(ValueError):decode(p)
    def test_complete_id_witness_types_not_python_numeric_equality(self):
        for ids in[[7000,7000],[True,7001],[7000.0,7001],[7000,None]]:
            with self.assertRaises(ValueError):f.id_roster(f.encode({'objectIdFieldName':'OBJECTID','objectIds':ids}),2)
        self.assertEqual(f.id_roster(f.encode({'objectIdFieldName':'OBJECTID','objectIds':[7001,7000]}),2),[7000,7001])
    def test_registered_one_asset_and_stopped_monthly_refresh(self):
        self.assertEqual(set(defs.resolve_job_def('significant_volcanic_source_integrity').asset_layer.executable_asset_keys),{AssetKey(['ingest',f.SLUG,f.SLUG])});self.assertEqual(significant_volcanic_source_integrity_schedule.default_status,DefaultScheduleStatus.STOPPED)
    def test_special_file_and_bounded_read_guard(self):
        with tempfile.TemporaryDirectory()as d:
            r=Path(d);fifo=r/'fifo';os.mkfifo(fifo)
            with self.assertRaises(ValueError):f.bounded_input(fifo,100)
            with self.assertRaises(ValueError):f.local_only(StagingStorageResource(local_dir=d))
            fifo.unlink();p=r/'regular';p.write_bytes(b'1234')
            with self.assertRaises(ValueError):f.bounded_input(p,3)
            os.link(p,r/'hardlink')
            with self.assertRaises(ValueError):f.bounded_input(p,4)
    def test_short_private_writes_and_duplicate_json_rejected(self):
        with tempfile.TemporaryDirectory()as d,mock.patch.object(f.os,'write',return_value=1):
            with self.assertRaises(ValueError):f.write_private(Path(d),'metadata/test.json',b'{}\n')
        with self.assertRaises(ValueError):f.strict_json(b'{"x":1,"x":2}')
    def test_HTTPX_consumed_oversize_block_retained(self):
        class Stream(httpx.SyncByteStream):
            def __iter__(self):yield b'x'*37
        with tempfile.TemporaryDirectory()as d:
            client=httpx.Client(transport=httpx.MockTransport(lambda req:httpx.Response(200,stream=Stream())))
            with mock.patch.object(f.httpx,'Client',return_value=client),mock.patch.object(f,'MAX_BODY_BYTES',36):
                with self.assertRaises(ValueError):f.acquire(StagingStorageResource(local_dir=d))
            wire=list(Path(d).glob('_acquisition-attempts/*/*/*/wire.body'));self.assertEqual(len(wire),1);self.assertEqual(wire[0].read_bytes(),b'x'*37)
    def test_malformed_receipt_retains_consumed_body_before_parse(self):
        with tempfile.TemporaryDirectory()as d:
            r=Path(d);cap=r/'inputs';cap.mkdir();first=next(iter(f.URLS));(cap/(first+'.body')).write_bytes(b'original literal source body');(cap/(first+'.receipt.json')).write_bytes(b'{bad')
            with self.assertRaises(ValueError):f.acquire(StagingStorageResource(local_dir=str(r/'stage')),cap)
            self.assertEqual(next((r/'stage').glob('_raw-captures/*/*/*.body')).read_bytes(),b'original literal source body');self.assertEqual(next((r/'stage').glob('_acquisition-attempts/*/*/*.raw-receipt.body')).read_bytes(),b'{bad')


@unittest.skipUnless(os.environ.get('SIGNIFICANT_VOLCANIC_TEST_CAPTURE_DIR'),'Complete real source is explicit external opt-in')
class CompleteSignificantVolcanicTests(unittest.TestCase):
    def test_complete903_all59attrs_and_native_geometry(self):
        r=Path(os.environ['SIGNIFICANT_VOLCANIC_TEST_CAPTURE_DIR']);c={n:((r/(n+'.body')).read_bytes(),f.strict_json((r/(n+'.receipt.json')).read_bytes()))for n in f.URLS};t,a=f.inspect_captures(c);self.assertEqual(t.num_rows,903);self.assertEqual(t.num_columns,60);self.assertEqual(a['BCE_count'],25);self.assertEqual(t['YEAR'].type,pa.int16())
        with tempfile.TemporaryDirectory()as d:
            p=Path(d);(p/'geoparquet').mkdir();pq.write_table(f.parquet_table(t),p/'geoparquet'/f'{f.SLUG}.parquet',compression='zstd');f.validate_output(p,t)

if __name__=='__main__':unittest.main()
