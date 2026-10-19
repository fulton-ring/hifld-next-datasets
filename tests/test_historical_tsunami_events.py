"""Source-native behavior tests; fixtures grant no real dataset acceptance."""
import copy
import fcntl
import gzip
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import httpx
import pyarrow as pa
import pyogrio
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import historical_tsunami_events as s

class TsunamiEventTests(unittest.TestCase):
    def fixture(self):
        meta={'fields':s.FIELD_DEFINITIONS,'objectIdField':'UNIQUE_ID','geometryType':'esriGeometryPoint','hasZ':False,'hasM':False,'extent':{'spatialReference':{'latestWkid':3857}},'serviceItemId':'5a44c3d4d465498993120b70ab568876','maxRecordCount':2000}
        attrs={f['name']:None for f in s.FIELD_DEFINITIONS};attrs.update(UNIQUE_ID=123,ID=9,YEAR=-20,SECOND=0.1,EVENT_DATE=0,COUNTRY=None,COMMENTS='  literal text\r\n<P>unchanged  ')
        docs={'authority_landing':b'NOAA /maps/tsunami-events/ DOI10.7289/V5PN93H7','authority_webmap':{'id':s.WEBMAP,'owner':'ncei_noaa'},'authority_webmap_data':{'operationalLayers':[{'url':s.BASE}]},'metadata_before':meta,'metadata_after':meta,'count_before':{'count':1},'count_after':{'count':1},'ids_before':{'objectIdFieldName':'UNIQUE_ID','objectIds':[123]},'ids_after':{'objectIdFieldName':'UNIQUE_ID','objectIds':[123]},'page_00000':{'geometryType':'esriGeometryPoint','spatialReference':{'latestWkid':3857},'fields':s.FIELD_DEFINITIONS,'features':[{'attributes':attrs,'geometry':{'x':1.,'y':2.}}]}}
        captures={}
        for name,url in s.request_plan([123]).items():
            body=docs[name] if isinstance(docs[name],bytes) else s.encode(docs[name]);captures[name]=(body,{'url':url,'final_url':url,'status':200,'bytes':len(body),'sha256':s.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00+00:00'})
        return captures
    def mutate(self,captures,name,transform):
        body,receipt=captures[name];value=json.loads(body);transform(value);body=s.encode(value);captures[name]=(body,receipt|{'bytes':len(body),'sha256':s.digest(body)})

    def test_declared_physical_types_dates_nulls_and_literal_text(self):
        table,audit=s.inspect_captures(self.fixture());self.assertEqual(table['SECOND'].type,pa.float32());self.assertEqual(table['MONTH'].type,pa.int16());self.assertEqual(table['ID'].type,pa.int32());self.assertEqual(table['EVENT_DATE'].type,pa.timestamp('ms',tz='UTC'));self.assertEqual(table['EVENT_DATE'].cast(pa.int64()).to_pylist(),[0]);self.assertEqual(table['COMMENTS'][0].as_py(),'  literal text\r\n<P>unchanged  ');self.assertEqual(audit['Single_wire_decimal_to_declared_binary32_differences'],{'SECOND':1});self.assertTrue(audit['roster_change_from_initial_requires_review'])

    def test_manifest_input_order_survives_canonical_original_binding(self):
        captures=self.fixture();identity={'files':{}};pin={'sha256':'a'*64,'bytes':1}
        original=s.manifest_for(captures,identity,pin)
        restored=s.manifest_for(dict(reversed(list(captures.items()))),identity,pin)
        self.assertEqual(original,restored)

    def test_gpkg_and_parquet_native_roundtrip_and_full_container(self):
        table,_=s.inspect_captures(self.fixture())
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for folder in ('candidate','geoparquet','geopackage'):(root/folder).mkdir()
            for f in ('candidate/typed-source.parquet',f'geoparquet/{s.SLUG}.parquet'):s.pq.write_table(s.expected_parquet(table),root/f,compression='zstd')
            s.write_gpkg(table,root/'geopackage'/f'{s.SLUG}.gpkg');s.verify_outputs(root,table)
            db=s.sqlite3.connect(root/'geopackage'/f'{s.SLUG}.gpkg')
            try:
                info={row[1]:row[2] for row in db.execute('PRAGMA table_info('+s.LAYER+')')};self.assertEqual(info['MONTH'],'SMALLINT');self.assertEqual(info['SECOND'],'FLOAT');self.assertEqual(info['EVENT_DATE'],'DATETIME');db.execute('UPDATE gpkg_contents SET srs_id=4326');db.commit()
            finally:db.close()
            with self.assertRaisesRegex(s.TsunamiQuarantineError,'full_container'):s.verify_outputs(root,table)

    def test_incomplete_limit_and_unrequested_ids(self):
        for label in ('missing','duplicate','unrequested','limit','renamed'):
            captures=self.fixture()
            def change(page):
                if label=='missing':page['features']=[]
                elif label=='duplicate':page['features']*=2
                elif label=='unrequested':page['features'][0]['attributes']['UNIQUE_ID']=999
                elif label=='limit':page['exceededTransferLimit']=True
                else:page['features'][0]['attributes']['renamed']=page['features'][0]['attributes'].pop('ID')
            self.mutate(captures,'page_00000',change)
            with self.subTest(label=label),self.assertRaises(s.TsunamiQuarantineError):s.inspect_captures(captures)

    def test_before_after_edit_ids_and_schema_drift(self):
        for name,change in [('metadata_after',lambda d:d.update(editingInfo={'lastEditDate':1})),('ids_after',lambda d:d.update(objectIds=[999])),('count_after',lambda d:d.update(count=2)),('metadata_before',lambda d:d['fields'][2].update(type='esriFieldTypeString'))]:
            captures=self.fixture();self.mutate(captures,name,change)
            with self.subTest(name=name),self.assertRaises(s.TsunamiQuarantineError):s.inspect_captures(captures)

    def test_authority_grain_and_original_identifiers(self):
        for name,change in [('authority_webmap',lambda d:d.update(owner='unrelated_mirror')),('authority_webmap_data',lambda d:d.update(operationalLayers=[{'url':s.BASE.replace('tsunami_events','tsunami_observations')}])),('ids_before',lambda d:d.update(objectIdFieldName='ID'))]:
            captures=self.fixture();self.mutate(captures,name,change)
            with self.subTest(name=name),self.assertRaises(s.TsunamiQuarantineError):s.inspect_captures(captures)

    def test_point_crs_and_extra_dimension_rejected(self):
        for change in [lambda d:d.update(spatialReference={'latestWkid':4326}),lambda d:d['features'][0].update(geometry=None),lambda d:d['features'][0]['geometry'].update(z=3)]:
            captures=self.fixture();self.mutate(captures,'page_00000',change)
            with self.assertRaises(s.TsunamiQuarantineError):s.inspect_captures(captures)

    def test_invalid_integer_and_date_types_do_not_coerce(self):
        for key,value in [('ID',True),('MONTH',40000),('EVENT_DATE','1970-01-01'),('SECOND',float('inf'))]:
            field=next(f for f in s.FIELD_DEFINITIONS if f['name']==key)
            with self.subTest(key=key),self.assertRaises(s.TsunamiQuarantineError):s.physical_value(field,value)

    def test_encoded_wire_gzip_and_partial_transport(self):
        real_client=httpx.Client;body=b'{"complete":true}';encoded=gzip.compress(body)
        def handler(request):return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(encoded))},stream=httpx.ByteStream(encoded))
        with patch.object(s.httpx,'Client',side_effect=lambda **kw:real_client(transport=httpx.MockTransport(handler),**kw)):
            data,receipt=s.fetch_one(s.BASE);self.assertEqual(data,body);self.assertEqual(receipt['wire_bytes_downloaded'],len(encoded))
        def short(request):return httpx.Response(200,headers={'Content-Length':'100'},stream=httpx.ByteStream(body))
        with patch.object(s.httpx,'Client',side_effect=lambda **kw:real_client(transport=httpx.MockTransport(short),**kw)),self.assertRaises(s.TsunamiFetchError) as caught:s.fetch_one(s.BASE)
        self.assertEqual(caught.exception.body,body);self.assertFalse(caught.exception.receipt['complete_body'])

    def test_stable_kernel_lock_and_missing_first_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp)
            with s.candidate_lock(base,'dataset/file/version') as target:
                fd=os.open(target.parent/('.'+target.name+'.lock'),os.O_RDWR)
                try:
                    with self.assertRaises(BlockingIOError):fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                finally:os.close(fd)
            storage=StagingStorageResource(local_dir=tmp,use_local=True);key=f'_source-provenance/{s.SLUG}/version/bundle.json';s.immutable(storage,key+'.first-construction.json',b'{}')
            with self.assertRaisesRegex(s.TsunamiQuarantineError,'missing_original_binding_after_first_construction'):s.original_bundle(storage,'version',{}, {},{},create=True)
            self.assertFalse(storage.object_exists(key))

    def test_local_only_and_duplicate_json(self):
        with self.assertRaises(s.TsunamiQuarantineError):s.local_only(PublishedStorageResource(local_dir='/tmp/unused-tsunami',use_local=True))
        with self.assertRaises(s.TsunamiQuarantineError):s.strict_json(b'{"approved":false,"approved":true}')

    def test_complete_real_source_when_explicitly_requested(self):
        captures=os.environ.get('TSUNAMI_EVENTS_TEST_CAPTURE_DIR')
        if not captures:self.skipTest('set TSUNAMI_EVENTS_TEST_CAPTURE_DIR for complete real capture')
        with tempfile.TemporaryDirectory() as tmp:
            captured=s.acquire(StagingStorageResource(local_dir=tmp,use_local=True),captures);table,audit=s.inspect_captures(captured)
        self.assertEqual(table.num_rows,2590);self.assertEqual(audit['invalid_geometries'],0);self.assertEqual(len(set(table['UNIQUE_ID'].to_pylist())),2590);self.assertEqual(len(set(table['ID'].to_pylist())),2590)

if __name__=='__main__':unittest.main()
