"""Tiny explicitly synthetic polygons; full producer capture is external opt-in."""
import copy,json,os,struct,tempfile,unittest
from pathlib import Path
from unittest import mock
import httpx,shapely
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetKey,DefaultScheduleStatus
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_small_scale as f
from dagster_hifld.definitions import defs
from dagster_hifld.assets.epa_small_scale import epa_small_scale_source_integrity_schedule

def page():
    # Clockwise outer, counterclockwise hole and a separated outer: no repair/reprojection.
    rings=[[[0.,0.],[0.,4.],[4.,4.],[4.,0.],[0.,0.]],[[1.,1.],[2.,1.],[2.,2.],[1.,2.],[1.,1.]],[[10.,0.],[10.,1.],[11.,1.],[11.,0.],[10.,0.]]]
    features=[{'attributes':{'Shape_Length':12.5,'Shape_Area':None,'OBJECTID':i+1,'EPAREGION':' Synthetic Region '+str(i+1)+' '},'geometry':{'rings':copy.deepcopy(rings)}}for i in range(2)]
    return {'displayFieldName':'EPAREGION','geometryType':'esriGeometryPolygon','spatialReference':{'wkid':4269,'latestWkid':4269},'fields':copy.deepcopy(f.PAGE_FIELDS),'features':features,'exceededTransferLimit':False}
def decode(p):return f.decode_page(p)[0]

class SmallScaleTests(unittest.TestCase):
    def test_all_original_fields_types_nulls_ids_and_native_rings(self):
        p=page();table,audit=f.decode_page(p);self.assertEqual(table.column_names,[d['name']for d in f.FIELD_DEFINITIONS]);self.assertEqual(table['OBJECTID'].type,pa.int64());self.assertFalse(table.schema.field('OBJECTID').nullable);self.assertEqual(table['EPAREGION'].to_pylist(),[' Synthetic Region 1 ',' Synthetic Region 2 ']);self.assertEqual(table['Shape_Area'].to_pylist(),[None,None]);self.assertEqual(table['Shape_Length'].type,pa.float64());self.assertEqual(audit,{'rings':6,'vertices':30})
        for row,feature in zip(table.to_pylist(),p['features']):self.assertEqual(f.verify_original_rings(feature['geometry'],shapely.from_wkb(row['Shape'])),(3,15))
        for d in f.FIELD_DEFINITIONS:self.assertEqual(table.schema.field(d['name']).metadata,{b'original_esri_field':f.encode(d)})
    def test_native_parquet_closed_types_and_deterministic_bytes(self):
        table=decode(page())
        with tempfile.TemporaryDirectory()as d:
            root=Path(d);(root/'geoparquet').mkdir();path=root/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(table),path,compression='zstd');first=path.read_bytes();f.validate_output(root,table);pq.write_table(f.parquet_table(table),path,compression='zstd');self.assertEqual(first,path.read_bytes());geo=json.loads(pq.read_schema(path).metadata[b'geo']);self.assertEqual(geo['primary_column'],'Shape');self.assertEqual(geo['columns']['Shape']['crs']['id'],{'authority':'EPSG','code':4269})
    def test_missing_extra_duplicate_renamed_and_null_id_rejected(self):
        for mutate in[lambda p:p['features'][0]['attributes'].pop('EPAREGION'),lambda p:p['features'][0]['attributes'].update(extra='x'),lambda p:p['features'][0]['attributes'].update(OBJECTID=None),lambda p:p['features'][1]['attributes'].update(OBJECTID=1)]:
            p=page();mutate(p)
            with self.assertRaises(ValueError):decode(p)
    def test_physical_bool_width_string_double_and_nullable_drift(self):
        for n,v in[('OBJECTID',True),('OBJECTID',1.0),('OBJECTID',2**63),('Shape_Length',True),('Shape_Area',float('nan')),('Shape_Area',2**53+1),('EPAREGION',7),('EPAREGION','x'*51)]:
            p=page();p['features'][0]['attributes'][n]=v
            with self.assertRaises(ValueError):decode(p)
    def test_transfer_schema_type_CRS_and_geometry_extension_drift(self):
        for mutate in[lambda p:p.update(exceededTransferLimit=True),lambda p:p.update(exceededTransferLimit=0),lambda p:p['fields'][0].update(type='esriFieldTypeInteger'),lambda p:p['fields'][-1].update(length=50.0),lambda p:p['spatialReference'].update(wkid=4269.0),lambda p:p['features'][0]['geometry'].update(curveRings=[]),lambda p:p['features'][0]['geometry']['rings'][0][1].append(5)]:
            p=page();mutate(p)
            with self.assertRaises((ValueError,RuntimeError)):decode(p)
    def test_null_geometry_preserved_not_filled(self):
        p=page();p['features'][0]['geometry']=None;table=decode(p);self.assertIsNone(table['Shape'][0].as_py());self.assertTrue(table.schema.field('Shape').nullable)
    def test_GDAL_headers_precede_large_features_independent_of_key_order(self):
        p=page();p['features'][0]['attributes']['EPAREGION']=' '*(40);p=dict(sorted(p.items()));table=decode(p);self.assertEqual(table.num_rows,2);self.assertEqual(table['EPAREGION'][0].as_py(),' '*40)
    def test_all_ten_original_ID_witnesses_strict_and_complete(self):
        good=f.encode({'objectIdFieldName':'OBJECTID','objectIds':list(range(1,11))});self.assertEqual(f.id_roster(good),list(range(1,11)))
        for ids in[list(range(1,10)),[True]+list(range(2,11)),[1.0]+list(range(2,11)),[1]*10]:
            with self.assertRaises(ValueError):f.id_roster(f.encode({'objectIdFieldName':'OBJECTID','objectIds':ids}))
    def test_exact_asset_stopped_monthly_and_small_scale_endpoint(self):
        self.assertEqual(set(defs.resolve_job_def('epa_small_scale_source_integrity').asset_layer.executable_asset_keys),{AssetKey(['ingest',f.SLUG,f.SLUG])});self.assertEqual(epa_small_scale_source_integrity_schedule.default_status,DefaultScheduleStatus.STOPPED);self.assertTrue(f.BASE.endswith('/MapServer/2'));self.assertEqual(f.INITIAL_COUNT,10)

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
                    canonical=f.encode(manifest);f.write_private(root,'metadata/source/source_manifest.json',canonical);f.write_private(root,'metadata/source/version_'+f.digest(canonical)+'_source_manifest.json',canonical)
                    projected=f.provenance_projection(f.load_resolved_source_manifest(storage,f.SLUG,f.SLUG,version).metadata);f.write_private(root,'metadata/resolved_provenance.json',f.encode(projected))
                    from dagster_hifld.catalog import summarize_staged_catalog
                    resolved=f.load_resolved_source_manifest(storage,f.SLUG,f.SLUG,version)
                    summary=summarize_staged_catalog(storage,f.SLUG,f.SLUG,version,f.SLUG,source_metadata=resolved.metadata)
                    quality='metadata/quality_manifest.json';dictionary='metadata/data_dictionary.json'
                    f.write_private(root,quality,f.encode(summary.quality_manifest));f.write_private(root,dictionary,f.encode(summary.data_dictionary))
                    f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                    for name in(quality,dictionary):
                        original=(root/name).read_bytes();forged=json.loads(original);forged['tampered_for_regression']=True;f.write_private(root,name,f.encode(forged))
                        with self.assertRaisesRegex(ValueError,'cached_catalog_metadata_mismatch'):f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                        f.write_private(root,name,original)
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

@unittest.skipUnless(os.environ.get('EPA_SMALL_SCALE_TEST_CAPTURE_DIR'),'Complete source is explicit external artifact opt-in')
class CompleteSmallScaleTests(unittest.TestCase):
    def test_complete_ten_original_native_regions(self):
        root=Path(os.environ['EPA_SMALL_SCALE_TEST_CAPTURE_DIR']);captures={n:((root/(n+'.body')).read_bytes(),f.strict_json((root/(n+'.receipt.json')).read_bytes()))for n in f.URLS};table,audit=f.inspect_captures(captures);self.assertEqual(table.num_rows,10);self.assertEqual(table.num_columns,5);self.assertEqual(audit['rings'],106);self.assertEqual(audit['vertices'],404974);self.assertEqual(audit['source_CRS'],4269);self.assertEqual(audit['service_display_CRS'],3857)
        with tempfile.TemporaryDirectory()as d:
            path=Path(d);(path/'geoparquet').mkdir();out=path/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(table),out,compression='zstd');f.validate_output(path,table);manifest=f.manifest_for(captures,{'explicit_test_identity':True},{'bytes':out.stat().st_size,'sha256':f.sha256_file(out)});f.validate_release_provenance(manifest)

if __name__=='__main__':unittest.main()

