"""Small explicitly synthetic dialysis rows; complete real capture opt-in."""
import copy,json,os,struct,tempfile,unittest
from pathlib import Path
from unittest import mock
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetKey,DefaultScheduleStatus
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import dialysis_centers as f
from dagster_hifld.definitions import defs
from dagster_hifld.assets.healthcare_dialysis import dialysis_source_integrity_schedule

def page():
    features=[]
    for i in range(2):
        attrs={d['name']:None for d in f.FIELD_DEFINITIONS};attrs.update(UniqueID=123+i,Name=' literal name ',Address='123 Address ',City='Synthetic',State='PR',ZIP=68261,County='Mayagüez, PR',Phone=1234567890.0,FIPS=683,Latitude=0.0 if not i else 1.0,Longitude=0.0 if not i else 2.0,Facility_Type='Dialysis Center',Status='Open',Filter_Field='Dialysis Center Open',ObjectId=7000+i)
        features.append({'attributes':attrs,'geometry':{'x':0.0 if not i else 1669681.0424083103,'y':0.0 if not i else 4543887.574736076}})
    return {'objectIdFieldName':'ObjectId','geometryType':'esriGeometryPoint','spatialReference':{'wkid':102100,'latestWkid':3857},'fields':copy.deepcopy(f.PAGE_FIELDS),'features':features,'exceededTransferLimit':False}
def decode(p):return f.decode_pages([p],[7000,7001])

class DialysisTests(unittest.TestCase):
    def test_original_ids_types_literals_zeros_and_native_WKB(self):
        t=decode(page());self.assertEqual(t.num_rows,2);self.assertEqual(t['UniqueID'].type,pa.int32());self.assertEqual(t['ObjectId'].type,pa.int32());self.assertEqual(t['ZIP'].type,pa.int32());self.assertEqual(t['Phone'].type,pa.float64());self.assertEqual(t['Name'][0].as_py(),' literal name ');self.assertEqual(t['Address'][0].as_py(),'123 Address ');self.assertEqual(t['ZIP'][0].as_py(),68261);self.assertEqual(t['geometry'][0].as_py(),struct.pack('<BIdd',1,1,0.,0.));self.assertFalse(t.schema.field('ObjectId').nullable)
        for decl in f.FIELD_DEFINITIONS:self.assertEqual(t.schema.field(decl['name']).metadata,{b'original_esri_field':f.encode(decl)})
    def test_original_nullable_and_width_declarations_preserved(self):
        p=page();p['features'][0]['attributes']['Phone']=None;p['features'][0]['attributes']['ZIP']=None;t=decode(p);self.assertIsNone(t['Phone'][0].as_py());self.assertIsNone(t['ZIP'][0].as_py());self.assertEqual(t['UniqueID'].to_pylist(),[123,124]);self.assertEqual(t['ObjectId'].to_pylist(),[7000,7001])
    def test_complete_original_column_roster_deterministic_roundtrip(self):
        t=decode(page());self.assertEqual(t.column_names,[x['name']for x in f.FIELD_DEFINITIONS]+['geometry'])
        with tempfile.TemporaryDirectory()as d:
            root=Path(d);(root/'geoparquet').mkdir();path=root/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(t),path,compression='zstd');first=path.read_bytes();f.validate_output(root,t);pq.write_table(f.parquet_table(t),path,compression='zstd');self.assertEqual(first,path.read_bytes())
    def test_missing_extra_duplicate_id_and_wrong_category_rejected(self):
        for mutate in[lambda p:p['features'][0]['attributes'].pop('UniqueID'),lambda p:p['features'][0]['attributes'].update(extra='x'),lambda p:p['features'][1]['attributes'].update(ObjectId=7000),lambda p:p['features'][1]['attributes'].update(ObjectId=9999),lambda p:p['features'][0]['attributes'].update(Facility_Type='Pharmacy')]:
            p=page();mutate(p)
            with self.assertRaises(ValueError):decode(p)
    def test_integer_bool_double_width_type_and_null_drift(self):
        for n,v in[('UniqueID',2**31),('ZIP',True),('FIPS',1.0),('ObjectId',None),('Phone',True),('Phone',float('nan')),('Phone',2**53+1),('Name',4),('Address','x'*4001)]:
            p=page();p['features'][0]['attributes'][n]=v
            with self.assertRaises(ValueError,msg=n):decode(p)
    def test_incomplete_CRS_schema_and_extended_geometry_rejected(self):
        for mutate in[lambda p:p['features'].pop(),lambda p:p.update(exceededTransferLimit=0),lambda p:p['spatialReference'].update(latestWkid=3857.0),lambda p:p['fields'][0].update(nullable=1),lambda p:p['features'][0]['geometry'].update(z=1),lambda p:p['features'][0]['geometry'].update(x=float('inf'))]:
            p=page();mutate(p)
            with self.assertRaises(ValueError):decode(p)
    def test_real_zero_locations_fail_quality_without_filtering(self):
        table=decode(page())
        with tempfile.TemporaryDirectory()as d:
            storage=StagingStorageResource(local_dir=d);version='explicit-synthetic';root=Path(d)/f.SLUG/f.SLUG/version/'geoparquet';root.mkdir(parents=True);pq.write_table(f.parquet_table(table),root/f'{f.SLUG}.parquet',compression='zstd');before=(root/f'{f.SLUG}.parquet').read_bytes();passed,metadata=f.location_quality(storage,version);self.assertFalse(passed);self.assertEqual(metadata['literal_zero_coordinates'],1);self.assertEqual(metadata['source_rows'],2);self.assertEqual(before,(root/f'{f.SLUG}.parquet').read_bytes())
    def test_one_registered_asset_stopped_refresh_and_blocking_locations(self):
        self.assertEqual(set(defs.resolve_job_def('dialysis_source_integrity').asset_layer.executable_asset_keys),{AssetKey(['ingest',f.SLUG,f.SLUG])});self.assertEqual(dialysis_source_integrity_schedule.default_status,DefaultScheduleStatus.STOPPED)
        checks=[s for c in defs.asset_checks for s in c.check_specs if s.name=='dialysis_source_locations'];self.assertEqual(len(checks),1);self.assertTrue(checks[0].blocking)

    def test_complete_id_witness_types_not_python_numeric_equality(self):
            for ids in[[7000,7000],[True,7001],[7000.0,7001],[7000,None]]:
                with self.assertRaises(ValueError):f.id_roster(f.encode({'objectIdFieldName':'ObjectId','objectIds':ids}),2)
            self.assertEqual(f.id_roster(f.encode({'objectIdFieldName':'ObjectId','objectIds':[7001,7000]}),2),[7000,7001])

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

    def test_partial_wire_and_decoded_blocks_retained_before_rejection(self):
            delivered=bytes(range(64));original=Path.open
            class Stream(httpx.SyncByteStream):
                def __iter__(self):yield delivered
            for target,claimed in[('wire.body',False),('wire.body',True),('decoded.body',False)]:
                class Short:
                    def __init__(self,stream):self.stream=stream
                    def __enter__(self):return self
                    def __exit__(self,*args):return self.stream.__exit__(*args)
                    def write(self,body):
                        self.stream.write(body[:7]);return len(body)if claimed else 7
                    def flush(self):return self.stream.flush()
                    def fileno(self):return self.stream.fileno()
                def opening(path,*args,**kwargs):
                    stream=original(path,*args,**kwargs)
                    return Short(stream)if path.name==target and args and args[0]=='xb'else stream
                with tempfile.TemporaryDirectory()as d:
                    client=httpx.Client(transport=httpx.MockTransport(lambda req:httpx.Response(200,stream=Stream())))
                    with mock.patch.object(f.httpx,'Client',return_value=client),mock.patch.object(Path,'open',opening):
                        with self.assertRaises(ValueError):f.acquire(StagingStorageResource(local_dir=d))
                    pending=list(Path(d).glob('_acquisition-attempts/*/*/*/pending_'+target));self.assertEqual(len(pending),1);self.assertEqual(pending[0].read_bytes(),delivered)
                    physical=next(Path(d).glob('_acquisition-attempts/*/*/*/'+target));self.assertEqual(physical.read_bytes(),delivered[:7])
                    transfer=json.loads(next(Path(d).glob('_acquisition-attempts/*/*/*/transfer.json')).read_bytes());self.assertFalse(transfer['complete']);self.assertFalse(transfer['independent_physical_readback_matches_acknowledged_sinks']);self.assertEqual(transfer['physical_'+target.split('.')[0]]['bytes'],7)
                    self.assertFalse((Path(d)/'_construction-state').exists())

    def test_cached_authored_snapshot_and_resolved_wrapper_exact(self):
            with tempfile.TemporaryDirectory()as d:
                base=Path(d);version='v0.0.0-draft-structural';root=base/f.SLUG/f.SLUG/version;(root/'geoparquet').mkdir(parents=True)
                table=decode(page());path=root/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(table),path,compression='zstd');identity={'fixture':'explicit synthetic two-row cache'};audit={};captures={}
                for name,url in f.URLS.items():
                    body=('Explicit synthetic input: '+name).encode();captures[name]=(body,{'url':url,'final_url':url,'status':200,'bytes':len(body),'sha256':f.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00+00:00'})
                manifest=f.manifest_for(captures,identity,{'bytes':path.stat().st_size,'sha256':f.sha256_file(path)})
                for name,body in f.source_payloads(captures,identity,audit,manifest).items():f.write_private(root,name,body)
                ready={'approved':False,'version':version,'processing_fingerprint':f.digest(f.encode(identity)),'objects':{str(p.relative_to(root)):f.sha256_file(p)for p in root.rglob('*')if p.is_file()}};f.write_private(root,f.READY,f.encode(ready));storage=StagingStorageResource(local_dir=d)
                canonical=f.encode(manifest);f.write_private(root,'metadata/source/source_manifest.json',canonical)
                projected=f.provenance_projection(f.load_resolved_source_manifest(storage,f.SLUG,f.SLUG,version).metadata);f.write_private(root,'metadata/resolved_provenance.json',f.encode(projected))
                f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                for name in['metadata/source/source_manifest.json','metadata/resolved_provenance.json']:
                    original=(root/name).read_bytes();f.write_private(root,name,b'{"approved":true,"code_commit":"FORGED_SOURCE_WRAPPER"}\n')
                    with self.assertRaisesRegex(ValueError,'cached_source_snapshot_drift|cached_resolved_provenance_drift'):f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                    f.write_private(root,name,original)
                f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)

    def test_malformed_receipt_retains_consumed_body_before_parse(self):
            with tempfile.TemporaryDirectory()as d:
                r=Path(d);cap=r/'inputs';cap.mkdir();first=next(iter(f.URLS));(cap/(first+'.body')).write_bytes(b'original literal source body');(cap/(first+'.receipt.json')).write_bytes(b'{bad')
                with self.assertRaises(ValueError):f.acquire(StagingStorageResource(local_dir=str(r/'stage')),cap)
                self.assertEqual(next((r/'stage').glob('_raw-captures/*/*/*.body')).read_bytes(),b'original literal source body');self.assertEqual(next((r/'stage').glob('_acquisition-attempts/*/*/*.raw-receipt.body')).read_bytes(),b'{bad')

@unittest.skipUnless(os.environ.get('DIALYSIS_TEST_CAPTURE_DIR'),'Complete real source is explicit external opt-in')
class CompleteDialysisTests(unittest.TestCase):
    def test_complete7717_all15_fields_native_and_inactive_semantics(self):
        root=Path(os.environ['DIALYSIS_TEST_CAPTURE_DIR']);captures={n:((root/(n+'.body')).read_bytes(),f.strict_json((root/(n+'.receipt.json')).read_bytes()))for n in f.URLS};table,audit=f.inspect_captures(captures);self.assertEqual(table.num_rows,7717);self.assertEqual(table.num_columns,16);self.assertEqual(audit['literal_zero_coordinate_count'],15);self.assertEqual(audit['status_counts'],[('Open',7717)]);self.assertEqual(table['ZIP'].type,pa.int32());self.assertEqual(table['Phone'].type,pa.float64())
        with tempfile.TemporaryDirectory()as d:
            path=Path(d);(path/'geoparquet').mkdir();out=path/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(table),out,compression='zstd');f.validate_output(path,table);manifest=f.manifest_for(captures,{'explicit_test_identity':True},{'bytes':out.stat().st_size,'sha256':f.sha256_file(out)});f.validate_release_provenance(manifest)

if __name__=='__main__':unittest.main()
