"""Small, explicit synthetic SEMS fixtures; complete official input is opt-in."""
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
from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import epa_sems as f
from dagster_hifld.definitions import defs
from dagster_hifld.assets.epa_sems import epa_sems_source_integrity_schedule


def page():
    features=[]
    for i in range(2):
        attrs={d['name']:None for d in f.FIELD_DEFINITIONS}
        attrs.update(OBJECTID=2**31+i,REGISTRY_ID='000000000001',PRIMARY_NAME=' Literal Site ',FIPS_CODE='00001',POSTAL_CODE='00123',LATITUDE83=42.5,LONGITUDE83=-73.5,ACCURACY_VALUE=1,CREATE_DATE=1715731200000,UPDATE_DATE=1718630742123,LAST_REPORTED_DATE=None,PGM_SYS_ID='0000123456',PGM_SYS_ACRNM='SEMS',INTEREST_TYPE='SUPERFUND (NON-NPL)'if i else'SUPERFUND NPL',PGM_REPORT_URL='no data yet',EPA_REGION_CODE='01',KEY_FIELD='SEMS0000123456')
        features.append({'attributes':attrs,'geometry':{'x':-73.50000000001,'y':42.50000000001}})
    return {'objectIdFieldName':'OBJECTID','geometryType':'esriGeometryPoint','spatialReference':{'wkid':4269,'latestWkid':4269},'fields':copy.deepcopy(f.PAGE_FIELDS),'features':features,'exceededTransferLimit':False}


def decode(p):return f.decode_pages([p],[2**31,2**31+1])


class SEMSTests(unittest.TestCase):
    def test_native_identifiers_dates_types_nulls_and_original_geometry(self):
        t=decode(page());self.assertEqual(t.num_rows,2);self.assertEqual(t.num_columns,33)
        self.assertEqual(t['OBJECTID'].type,pa.int64());self.assertEqual(t['ACCURACY_VALUE'].type,pa.int32());self.assertEqual(t['LATITUDE83'].type,pa.float64());self.assertEqual(t['CREATE_DATE'].type,pa.int64())
        self.assertEqual(t['CREATE_DATE'].to_pylist(),[1715731200000]*2);self.assertEqual(t['UPDATE_DATE'][0].as_py(),1718630742123);self.assertEqual(t['LAST_REPORTED_DATE'].null_count,2)
        self.assertEqual(t['REGISTRY_ID'].to_pylist(),['000000000001']*2);self.assertEqual(t['EPA_REGION_CODE'][0].as_py(),'01');self.assertEqual(t['PRIMARY_NAME'][0].as_py(),' Literal Site ')
        self.assertEqual(t['geometry'][0].as_py(),struct.pack('<BIdd',1,1,-73.50000000001,42.50000000001));self.assertNotEqual(t['LONGITUDE83'][0].as_py(),-73.50000000001)
        for d in f.FIELD_DEFINITIONS:self.assertEqual(t.schema.field(d['name']).metadata,{b'original_esri_field':f.encode(d)});self.assertEqual(t.schema.field(d['name']).nullable,d['nullable'])
    def test_typed_deterministic_native_parquet(self):
        t=decode(page())
        with tempfile.TemporaryDirectory()as d:
            r=Path(d);(r/'geoparquet').mkdir();p=r/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(t),p,compression='zstd');first=p.read_bytes();f.validate_output(r,t);pq.write_table(f.parquet_table(t),p,compression='zstd');self.assertEqual(first,p.read_bytes());self.assertEqual(json.loads(t.schema.field('CREATE_DATE').metadata[b'original_esri_field'])['type'],'esriFieldTypeDate');self.assertEqual(json.loads(pq.read_schema(p).metadata[b'geo'])['columns']['geometry']['crs'],f.NATIVE_CRS_JSON)
    def test_duplicate_extra_missing_ids_and_source_columns(self):
        for change in[lambda p:p['features'][0]['attributes'].pop('KEY_FIELD'),lambda p:p['features'][0]['attributes'].update(new='x'),lambda p:p['features'][1]['attributes'].update(OBJECTID=2**31),lambda p:p['features'][1]['attributes'].update(OBJECTID=9),lambda p:p['features'].pop()]:
            p=page();change(p)
            with self.assertRaises(ValueError):decode(p)
    def test_source_physical_bounds_no_bools_or_lossy_numbers(self):
        for key,value in[('OBJECTID',True),('OBJECTID',2**63),('ACCURACY_VALUE',2**31),('ACCURACY_VALUE',1.0),('CREATE_DATE',1.5),('CREATE_DATE',True),('CREATE_DATE',None),('CREATE_DATE',2**63),('UPDATE_DATE','2024-05-15'),('REGISTRY_ID','x'*13),('REGISTRY_ID',1),('LATITUDE83',float('nan')),('LATITUDE83',2**53+1),('FIPS_CODE','123456')]:
            p=page();p['features'][0]['attributes'][key]=value
            with self.assertRaises(ValueError,msg=key):decode(p)
    def test_page_metadata_geometry_CRS_and_json_type_drift(self):
        for change in[lambda p:p.update(exceededTransferLimit=0),lambda p:p.update(exceededTransferLimit=True),lambda p:p['fields'][0].update(nullable=0),lambda p:p['spatialReference'].update(wkid=4269.0),lambda p:p['features'][0]['geometry'].update(x=float('nan')),lambda p:p['features'][0]['geometry'].update(z=1)]:
            p=page();change(p)
            with self.assertRaises(ValueError):decode(p)
    def test_complete_id_targeted_requests_unfiltered_and_bounded(self):
        ids=list(range(1201));urls=f.capture_urls(ids);pages=[u for n,u in urls.items()if n.startswith('page_')];self.assertEqual(len(pages),7)
        for name,url in urls.items():self.assertEqual(f.expected_url(name,url),url)
        self.assertIn('objectIds=1000%2C',urls['page_01000']);self.assertIn('outFields=%2A',pages[0]);self.assertNotIn('where=',pages[0]);self.assertEqual(f.id_roster(f.encode({'objectIdFieldName':'OBJECTID','objectIds':[2,1]}),2),[1,2])
        for values in[[True,2],[1.0,2],[1,1]]:
            with self.assertRaises(ValueError):f.id_roster(f.encode({'objectIdFieldName':'OBJECTID','objectIds':values}),2)
    def test_concrete_official_distribution_provenance_and_strict_receipt(self):
        n='count_before';body=b'explicit synthetic';r={'url':f.URLS[n],'final_url':f.URLS[n],'status':200,'bytes':len(body),'sha256':f.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00+00:00'}
        item=f.validate_receipt(n,body,r);self.assertEqual(item['delegation_evidence']['portal_owner'],'EPA_GEO');self.assertEqual(item['role'],'official_distributor')
        for key,value in[('status',200.0),('bytes',True),('complete_body',1),('observed_at','2026-10-19')]:
            bad=dict(r);bad[key]=value
            with self.assertRaises(ValueError):f.validate_receipt(n,body,bad)
    def test_registration_explicit_asset_normal_graph_and_stopped_refresh(self):
        key=AssetKey(['ingest',f.SLUG,f.SLUG]);self.assertEqual(set(defs.resolve_job_def('epa_sems_source_integrity').asset_layer.executable_asset_keys),{key});self.assertEqual(epa_sems_source_integrity_schedule.default_status,DefaultScheduleStatus.STOPPED)
        keys=set(defs.resolve_asset_graph().get_all_asset_keys());self.assertIn(key,keys);self.assertIn(AssetKey(['publish','catalog']),keys);self.assertIn(AssetKey(['publish','verify']),keys);self.assertIn(AssetKey(['publish','promote']),keys)
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


    def test_checked_private_READY_failure_precedes_any_outside_seal(self):
        p=page();ids=[2**31,2**31+1];captures={}
        with mock.patch.object(f,'INITIAL_COUNT',2):
            for name,url in f.capture_urls(ids).items():
                value={'catalog':(f.SERVICE+' '+f.ITEM).encode(),'epa_frs':b'explicit synthetic EPA landing fixture',
                    'epa_item':f.encode({'id':f.ITEM,'owner':'EPA_GEO','orgId':'cJ9YHowT8TU7DUyn','url':f.SERVICE}),
                    'epa_service':f.encode({'serviceItemId':f.ITEM,'layers':[{'id':0}]}),
                    'epa_layer':f.encode({'id':0,'name':'FRS_INTERESTS_SEMS','fields':f.FIELD_DEFINITIONS,'objectIdField':'OBJECTID','geometryType':'esriGeometryPoint','hasZ':False,'hasM':False,'extent':{'spatialReference':p['spatialReference']},'maxRecordCount':2000,'editingInfo':{'lastEditDate':123},'dateFieldsTimeReference':{'timeZone':'UTC','timeZoneIANA':'Etc/UTC','respectsDaylightSaving':False}}),
                    'count_before':f.encode({'count':2}),'ids_before':f.encode({'objectIdFieldName':'OBJECTID','objectIds':ids})}
                value['layer_after']=value['epa_layer'];value['count_after']=value['count_before'];value['ids_after']=value['ids_before'];value['page_00000']=json.dumps(p).encode()
                body=value[name];captures[name]=(body,{'url':url,'final_url':url,'status':200,'bytes':len(body),'sha256':f.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00+00:00'})
            t,a=f.inspect_captures(captures);self.assertEqual(t.num_rows,2);self.assertEqual(dict(a['interest_types']),{'SUPERFUND NPL':1,'SUPERFUND (NON-NPL)':1})
            bad=copy.deepcopy(captures);layer=f.strict_json(bad['layer_after'][0]);layer['editingInfo']['lastEditDate']=124;body=f.encode(layer);r=dict(bad['layer_after'][1]);r.update(bytes=len(body),sha256=f.digest(body));bad['layer_after']=(body,r)
            with self.assertRaisesRegex(ValueError,'source_changed_during_extraction'):f.inspect_captures(bad)
            with tempfile.TemporaryDirectory()as d:
                original=f.write_private
                def failing(root,name,body):
                    if name==f.READY:raise f.SEMSQuarantineError('explicit synthetic READY write fault')
                    return original(root,name,body)
                with mock.patch.object(f,'acquire',return_value=captures),mock.patch.object(f,'write_private',side_effect=failing):
                    with self.assertRaisesRegex(ValueError,'READY write fault'):f.stage_epa_sems(StagingStorageResource(local_dir=d))
                self.assertFalse((Path(d)/'_construction-state').exists());self.assertFalse((Path(d)/f.SLUG).exists())

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
            bad=dict(ready);bad['approved']=0;f.write_private(root,f.READY,f.encode(bad))
            with self.assertRaisesRegex(ValueError,'candidate_ready_drift'):f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
            f.write_private(root,f.READY,f.encode(ready));f.write_private(root,'metadata/source/source_manifest.json',b'{"approved":true}')
            with self.assertRaisesRegex(ValueError,'cached_source_snapshot_drift'):f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)

    def test_cached_catalog_is_canonical_and_required_snapshots_cannot_disappear(self):
        with tempfile.TemporaryDirectory()as d:
            base=Path(d);version='v0.0.0-draft-explicit-catalog-synthetic';root=base/f.SLUG/f.SLUG/version
            (root/'geoparquet').mkdir(parents=True);table=decode(page())
            parquet=root/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(table),parquet,compression='zstd')
            identity={'fixture':'explicit synthetic two-row canonical catalog'};audit={};captures={}
            for name,url in f.URLS.items():
                body=('Explicit synthetic input '+name).encode()
                captures[name]=(body,{'url':url,'final_url':url,'status':200,'bytes':len(body),'sha256':f.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00+00:00'})
            manifest=f.manifest_for(captures,identity,{'bytes':parquet.stat().st_size,'sha256':f.sha256_file(parquet)})
            for name,body in f.source_payloads(captures,identity,audit,manifest).items():f.write_private(root,name,body)
            ready={'approved':False,'version':version,'processing_fingerprint':f.digest(f.encode(identity)),
                'capture_fingerprint':f.source_fingerprint(captures),'objects':{str(p.relative_to(root)):f.sha256_file(p)for p in root.rglob('*')if p.is_file()}}
            f.write_private(root,f.READY,f.encode(ready));storage=StagingStorageResource(local_dir=d)

            # Reproduce the registered catalog lifecycle using the same production
            # snapshot, summarizer and writer functions as publish/catalog.
            snapshot_source_metadata(storage,f.SLUG,f.SLUG,version)
            resolved=load_resolved_source_manifest(storage,f.SLUG,f.SLUG,version)
            summary=summarize_staged_catalog(storage,f.SLUG,f.SLUG,version,f.SLUG,source_metadata=resolved.metadata)
            write_catalog_metadata(storage,f.SLUG,f.SLUG,version,summary.quality_manifest,summary.data_dictionary)
            projection_path=root/'metadata/resolved_provenance.json'
            self.assertTrue(projection_path.is_file())
            f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)

            quality_path=root/'metadata/quality_manifest.json';quality_original=quality_path.read_bytes()
            quality=json.loads(quality_original);quality['quality_check_passed']=not quality['quality_check_passed']
            f.write_private(root,'metadata/quality_manifest.json',f.encode(quality))
            with self.assertRaisesRegex(ValueError,'cached_catalog_sidecar_semantics_invalid'):
                f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
            f.write_private(root,'metadata/quality_manifest.json',quality_original)

            dictionary_path=root/'metadata/data_dictionary.json';dictionary_original=dictionary_path.read_bytes()
            dictionary=json.loads(dictionary_original);dictionary['columns'][0]['name']='TAMPERED_COLUMN'
            f.write_private(root,'metadata/data_dictionary.json',f.encode(dictionary))
            with self.assertRaisesRegex(ValueError,'cached_catalog_sidecar_semantics_invalid'):
                f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
            f.write_private(root,'metadata/data_dictionary.json',dictionary_original)

            projection_path.unlink()
            with self.assertRaisesRegex(ValueError,'cached_catalog_sidecar_semantics_invalid'):
                f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
            projection=provenance_projection(resolved.metadata)
            f.write_private(root,'metadata/resolved_provenance.json',f.encode(projection))

            source_snapshot=root/('metadata/source/version_'+f.digest(f.encode(manifest))+'_source_manifest.json')
            source_snapshot.unlink()
            with self.assertRaisesRegex(ValueError,'cached_source_manifest_snapshot_incomplete'):
                f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)


@unittest.skipUnless(os.environ.get('EPA_SEMS_TEST_CAPTURE_DIR'),'Complete official real capture is explicit external input')
class CompleteSEMS(unittest.TestCase):
    def test_complete_official_full_interest_view(self):
        r=Path(os.environ['EPA_SEMS_TEST_CAPTURE_DIR']);ids=f.id_roster((r/'ids_before.body').read_bytes(),f.INITIAL_COUNT);c={n:((r/(n+'.body')).read_bytes(),f.strict_json((r/(n+'.receipt.json')).read_bytes()))for n in f.capture_urls(ids)};t,a=f.inspect_captures(c)
        self.assertEqual(t.num_rows,14242);self.assertEqual(t.num_columns,33);self.assertEqual(sum(v for k,v in a['interest_types']),14242);self.assertTrue(dict(a['interest_types'])['SUPERFUND (NON-NPL)']);self.assertEqual(t['CREATE_DATE'].type,pa.int64())

if __name__=='__main__':unittest.main()
