"""Small explicit synthetic polygons; complete official capture is an external input."""
import copy,json,os,struct,tempfile,unittest,math
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
import httpx,shapely
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetKey,DefaultScheduleStatus
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_large_scale as f
from dagster_hifld.definitions import defs
from dagster_hifld.assets.epa_large_scale import epa_large_scale_source_integrity_schedule

def page():
    rings=[[[0.,0.],[0.,4.],[4.,4.],[4.,0.],[0.,0.]],[[1.,1.],[2.,1.],[2.,2.],[1.,2.],[1.,1.]],[[10.,0.],[10.,1.],[11.,1.],[11.,0.],[10.,0.]]]
    features=[{'attributes':{'Shape_Length':12.5,'Shape_Area':None,'OBJECTID':i+1,'EPAREGION':' Synthetic Region '+str(i+1)+' '},'geometry':{'rings':copy.deepcopy(rings)}}for i in range(2)]
    return {'displayFieldName':'EPAREGION','geometryType':'esriGeometryPolygon','spatialReference':{'wkid':4269,'latestWkid':4269},'fields':copy.deepcopy(f.PAGE_FIELDS),'features':features,'exceededTransferLimit':False}
def decode(p):return f.decode_page(p)[0]

class LargeScaleTests(unittest.TestCase):
    def test_all_original_columns_physical_declarations_and_exact_native_rings(self):
        p=page();t,a=f.decode_page(p);self.assertEqual(t.column_names,[d['name']for d in f.FIELD_DEFINITIONS]);self.assertEqual(t['OBJECTID'].type,pa.int64());self.assertFalse(t.schema.field('OBJECTID').nullable);self.assertEqual(t['Shape_Length'].type,pa.float64());self.assertEqual(t['Shape_Area'].to_pylist(),[None,None]);self.assertEqual(t['EPAREGION'][0].as_py(),' Synthetic Region 1 ');self.assertEqual(a,{'rings':6,'vertices':30})
        for row,record in zip(t.to_pylist(),p['features']):self.assertEqual(f.verify_original_rings(record['geometry'],shapely.from_wkb(row['Shape'])),(3,15))
        for d in f.FIELD_DEFINITIONS:self.assertEqual(t.schema.field(d['name']).metadata,{b'original_esri_field':f.encode(d)})
    def test_deterministic_native_parquet_and_original_Shape_name(self):
        t=decode(page())
        with tempfile.TemporaryDirectory()as d:
            r=Path(d);(r/'geoparquet').mkdir();p=r/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(t),p,compression='zstd');first=p.read_bytes();f.validate_output(r,t);pq.write_table(f.parquet_table(t),p,compression='zstd');self.assertEqual(first,p.read_bytes());geo=json.loads(pq.read_schema(p).metadata[b'geo']);self.assertEqual(geo['primary_column'],'Shape');self.assertEqual(geo['columns']['Shape']['crs']['id'],{'authority':'EPSG','code':4269})
    def test_missing_renamed_extra_duplicate_and_null_source_IDs(self):
        for change in[lambda p:p['features'][0]['attributes'].pop('EPAREGION'),lambda p:p['features'][0]['attributes'].update(extra='x'),lambda p:p['features'][0]['attributes'].update(OBJECTID=None),lambda p:p['features'][1]['attributes'].update(OBJECTID=1)]:
            p=page();change(p)
            with self.assertRaises(ValueError):decode(p)
    def test_source_types_bounds_widths_and_lossy_values(self):
        for n,v in[('OBJECTID',True),('OBJECTID',1.0),('OBJECTID',2**63),('Shape_Length',True),('Shape_Area',float('nan')),('Shape_Area',2**53+1),('EPAREGION',7),('EPAREGION','x'*51)]:
            p=page();p['features'][0]['attributes'][n]=v
            with self.assertRaises(ValueError,msg=n):decode(p)
    def test_transfer_schema_CRS_and_extended_geometry_drift(self):
        for change in[lambda p:p.update(exceededTransferLimit=True),lambda p:p.update(exceededTransferLimit=0),lambda p:p['fields'][0].update(type='esriFieldTypeInteger'),lambda p:p['fields'][-1].update(length=50.0),lambda p:p['spatialReference'].update(wkid=4269.0),lambda p:p['features'][0]['geometry'].update(curveRings=[]),lambda p:p['features'][0]['geometry']['rings'][0][1].append(5)]:
            p=page();change(p)
            with self.assertRaises(Exception):decode(p)
    def test_null_geometry_is_preserved_without_invention(self):
        p=page();p['features'][0]['geometry']=None;t=decode(p);self.assertIsNone(t['Shape'][0].as_py());self.assertTrue(t.schema.field('Shape').nullable)
    def test_driver_headers_before_large_features_are_derived_only(self):
        p=page();ring=[[math.cos(-i*2*math.pi/2000),math.sin(-i*2*math.pi/2000)]for i in range(2000)];ring.append(ring[0]);p['features'][0]['geometry']['rings']=[ring];p={**{'features':p['features']},**{k:v for k,v in p.items()if k!='features'}};raw=json.dumps(p).encode();t,a=f.decode_page(p);self.assertEqual(raw,json.dumps(p).encode());self.assertEqual(t.num_rows,2);self.assertEqual(a['rings'],4);self.assertEqual(a['vertices'],2016)
    def test_exact_complete_ten_ID_witness(self):
        self.assertEqual(f.id_roster(f.encode({'objectIdFieldName':'OBJECTID','objectIds':list(range(1,11))})),list(range(1,11)))
        for ids in[list(range(1,10)),[True]+list(range(2,11)),[1.0]+list(range(2,11)),[1]*10]:
            with self.assertRaises(ValueError):f.id_roster(f.encode({'objectIdFieldName':'OBJECTID','objectIds':ids}))
    def test_registered_exact_large_scale_asset_and_stopped_refresh(self):
        key=AssetKey(['ingest',f.SLUG,f.SLUG]);self.assertEqual(set(defs.resolve_job_def('epa_large_scale_source_integrity').asset_layer.executable_asset_keys),{key});self.assertEqual(epa_large_scale_source_integrity_schedule.default_status,DefaultScheduleStatus.STOPPED);self.assertTrue(f.BASE.endswith('/MapServer/3'));self.assertEqual(f.INITIAL_COUNT,10)

    def test_regular_descriptor_guards_and_permanent_empty_incident_stop(self):
            with tempfile.TemporaryDirectory()as d:
                r=Path(d);os.mkfifo(r/'fifo')
                with self.assertRaises(ValueError):f.bounded_input(r/'fifo',100)
                (r/'fifo').unlink();(r/'file').write_bytes(b'1234');os.link(r/'file',r/'hardlink')
                with self.assertRaises(ValueError):f.bounded_input(r/'file',4)
                incident=r/f.incident_directory('v1');incident.mkdir(parents=True)
                with self.assertRaisesRegex(ValueError,'permanent_observed'):f.refuse_observed_loss(StagingStorageResource(local_dir=d),'v1')

    def test_body_and_exact_badreceipt_retained_before_parse(self):
            with tempfile.TemporaryDirectory()as d:
                r=Path(d);c=r/'input';c.mkdir();n=next(iter(f.URLS));(c/(n+'.body')).write_bytes(b'literal original body');(c/(n+'.receipt.json')).write_bytes(b'{bad')
                with self.assertRaises(ValueError):f.acquire(StagingStorageResource(local_dir=str(r/'stage')),c)
                self.assertEqual(next((r/'stage').glob('_raw-captures/*/*/*.body')).read_bytes(),b'literal original body');self.assertEqual(next((r/'stage').glob('_acquisition-attempts/*/*/*.raw-receipt.body')).read_bytes(),b'{bad')

    def test_checked_private_write_and_duplicate_json(self):
            with tempfile.TemporaryDirectory()as d,mock.patch.object(f.os,'write',return_value=1):
                with self.assertRaises(ValueError):f.write_private(Path(d),'metadata/test.json',b'{}\n')
            with self.assertRaises(ValueError):f.strict_json(b'{"x":1,"x":2}')

    def test_partial_HTTPX_block_pending_bytes_physically_retained(self):
            delivered=bytes(range(64));original=Path.open
            class Stream(httpx.SyncByteStream):
                def __iter__(self):yield delivered
            class Short:
                def __init__(self,s):self.s=s
                def __enter__(self):return self
                def __exit__(self,*a):return self.s.__exit__(*a)
                def write(self,b):self.s.write(b[:7]);return len(b)
                def flush(self):return self.s.flush()
                def fileno(self):return self.s.fileno()
            def opening(path,*a,**kw):
                stream=original(path,*a,**kw);return Short(stream)if path.name=='wire.body'and a and a[0]=='xb'else stream
            with tempfile.TemporaryDirectory()as d:
                client=httpx.Client(transport=httpx.MockTransport(lambda req:httpx.Response(200,stream=Stream())))
                with mock.patch.object(f.httpx,'Client',return_value=client),mock.patch.object(Path,'open',opening):
                    with self.assertRaises(ValueError):f.acquire(StagingStorageResource(local_dir=d))
                self.assertEqual(next(Path(d).glob('_acquisition-attempts/*/*/*/wire.body')).read_bytes(),delivered[:7]);self.assertEqual(next(Path(d).glob('_acquisition-attempts/*/*/*/pending_wire.body')).read_bytes(),delivered);self.assertFalse((Path(d)/'_construction-state').exists())

    def test_original_manifest_input_order_survives_canonical_CAS_roundtrip(self):
            captures={}
            for name,url in f.URLS.items():
                body=('explicit synthetic input '+name).encode();captures[name]=(body,{'url':url,'final_url':url,'status':200,'bytes':len(body),'sha256':f.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00+00:00'})
            pin={'bytes':12,'sha256':'1'*64};identity={'explicit':'synthetic canonical-order regression'}
            original=f.manifest_for(captures,identity,pin);reversed_inputs=f.manifest_for(dict(reversed(list(captures.items()))),identity,pin)
            self.assertEqual(f.encode(original),f.encode(reversed_inputs));self.assertEqual([r['input_id'].split(':')[0]for r in original['inputs']],sorted(captures))

    def test_exact_ready_and_authored_source_wrappers_required(self):
            with tempfile.TemporaryDirectory()as d:
                base=Path(d);version='v0.0.0-draft-explicit-synthetic';root=base/f.SLUG/f.SLUG/version;(root/'geoparquet').mkdir(parents=True);table=decode(page());path=root/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(table),path,compression='zstd');identity={'fixture':'explicit synthetic two-row candidate'};audit={};captures={}
                for name,url in f.URLS.items():
                    body=('Explicit synthetic input '+name).encode();captures[name]=(body,{'url':url,'final_url':url,'status':200,'bytes':len(body),'sha256':f.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00+00:00'})
                manifest=f.manifest_for(captures,identity,{'bytes':path.stat().st_size,'sha256':f.sha256_file(path)})
                for name,body in f.source_payloads(captures,identity,audit,manifest).items():f.write_private(root,name,body)
                ready={'approved':False,'version':version,'processing_fingerprint':f.digest(f.encode(identity)),'capture_fingerprint':f.source_fingerprint(captures),'objects':{str(p.relative_to(root)):f.sha256_file(p)for p in root.rglob('*')if p.is_file()}};f.write_private(root,f.READY,f.encode(ready));storage=StagingStorageResource(local_dir=d)
                f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                quality={'quality_check_passed':True,'feature_count':2,'native_crs':'EPSG:4269'};dictionary={'schema_version':1,'columns':[{'name':'OBJECTID','source':'EPA'}]}
                f.write_private(root,'metadata/quality_manifest.json',f.encode(quality));f.write_private(root,'metadata/data_dictionary.json',f.encode(dictionary))
                with mock.patch.object(f,'load_resolved_source_manifest',return_value=SimpleNamespace(metadata={})),mock.patch.object(f,'summarize_staged_catalog',return_value=SimpleNamespace(quality_manifest=quality,data_dictionary=dictionary)):
                    f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                    for sidecar_name,field,value in [('metadata/quality_manifest.json','quality_check_passed',False),('metadata/data_dictionary.json','columns',[{'name':'TAMPERED'}])]:
                        original=f.candidate_bytes(root,sidecar_name);changed=json.loads(original);changed[field]=value;f.write_private(root,sidecar_name,f.encode(changed))
                        with self.assertRaisesRegex(ValueError,'cached_catalog_metadata_mismatch'):f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                        f.write_private(root,sidecar_name,original)
                (root/'metadata/quality_manifest.json').unlink();(root/'metadata/data_dictionary.json').unlink()
                bad=dict(ready);bad['approved']=0;f.write_private(root,f.READY,f.encode(bad))
                with self.assertRaisesRegex(ValueError,'candidate_ready_drift'):f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                f.write_private(root,f.READY,f.encode(ready));f.write_private(root,'metadata/source/source_manifest.json',b'{"approved":true}')
                with self.assertRaisesRegex(ValueError,'cached_source_snapshot_drift'):f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)

@unittest.skipUnless(os.environ.get('EPA_LARGE_SCALE_TEST_CAPTURE_DIR'),'Complete official input is explicit external artifact opt-in')
class CompleteLargeScaleTests(unittest.TestCase):
    def test_full_ten_original_native_large_scale_regions(self):
        root=Path(os.environ['EPA_LARGE_SCALE_TEST_CAPTURE_DIR']);captures={n:((root/(n+'.body')).read_bytes(),f.strict_json((root/(n+'.receipt.json')).read_bytes()))for n in f.URLS};t,a=f.inspect_captures(captures);self.assertEqual(t.num_rows,10);self.assertEqual(t.num_columns,5);self.assertEqual(a['rings'],106);self.assertEqual(a['vertices'],404974);self.assertEqual(a['source_CRS'],4269);self.assertEqual(a['service_display_CRS'],3857)
        with tempfile.TemporaryDirectory()as d:
            r=Path(d);(r/'geoparquet').mkdir();p=r/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(t),p,compression='zstd');f.validate_output(r,t);m=f.manifest_for(captures,{'explicit_test_identity':True},{'bytes':p.stat().st_size,'sha256':f.sha256_file(p)});f.validate_release_provenance(m)

if __name__=='__main__':unittest.main()
