"""Tiny explicit synthetic event fixtures; complete official source opt-in."""
import copy
import json
import os
import struct
import subprocess
import sys
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
from dagster_hifld.assets.noaa_significant_volcanic import (
    noaa_significant_volcanic_events_complete,
    noaa_significant_volcanic_events_drift,
    significant_volcanic_source_integrity_schedule,
)


def page():
    features=[]
    for i in range(2):
        attrs={d['name']:None for d in f.FIELD_DEFINITIONS};attrs.update(YEAR=-4360 if i==0 else None,MO=None,DAY=4 if i else None,VEI=None,VOL_ID=101.0,NUM='0101-06=',NAME=' literal name ',COMMENTS='<p> 001 literal & text </p>',TIME_ERUPT='D1',SIG_ID=42+i,TSU_ID=2800+i,HAZ_EVENT_ID=1,OBJECTID=7000+i,ID=10106,COUNTRY='Explicit synthetic country')
        features.append({'attributes':attrs,'geometry':{'x':1669681.0424083103,'y':4543887.574736076}})
    return {'objectIdFieldName':'OBJECTID','geometryType':'esriGeometryPoint','spatialReference':{'wkid':102100,'latestWkid':3857},'fields':copy.deepcopy(f.PAGE_FIELDS),'features':features,'exceededTransferLimit':False}


def decode(p):return f.decode_pages([p],[7000,7001])


def synthetic_capture_bodies(ids):
    """Full synthetic retained capture {name: (body, receipt)} for one roster."""
    ids=list(ids);n=len(ids)
    pages=f.derive_pages(ids);urls=f.capture_urls(pages);bodies={}
    for name in urls:
        if name in ('count_before','count_after'):body=f.encode({'count':n})
        elif name in ('ids_before','ids_after'):body=f.encode({'objectIdFieldName':'OBJECTID','objectIds':list(ids)})
        elif name=='ncei':body=b'https://www.ncei.noaa.gov/maps/hazards/?layers=0 explicit synthetic producer page'
        elif name=='hazards_viewer':body=b'./assets/index-Cyvq2L9n.js explicit synthetic viewer page'
        elif name=='hazards_js':body=b'e991eccebfa5425e9180d884af76f16a explicit synthetic viewer javascript'
        elif name=='volcano_item':body=f.encode({'id':'e991eccebfa5425e9180d884af76f16a','owner':'ncei_noaa','url':f.SERVICE})
        elif name=='volcano_service':body=f.encode({'layers':[{'id':0}]})
        elif name in ('volcano_layer','layer_after'):
            body=f.encode({'id':0,'fields':copy.deepcopy(f.FIELD_DEFINITIONS),'geometryType':'esriGeometryPoint','objectIdField':'OBJECTID','hasZ':False,'hasM':False,'maxRecordCount':f.PAGE_SIZE,
                'extent':{'xmin':0,'ymin':0,'xmax':1,'ymax':1,'spatialReference':{'wkid':102100,'latestWkid':3857}},'editingInfo':{'creationDate':-719836801000,'lastEditDate':-719836801000}})
        else:
            index=int(name.rsplit('_',1)[1]);chunk=ids[index*f.PAGE_SIZE:(index+1)*f.PAGE_SIZE]
            features=[]
            for oid in chunk:
                attrs={d['name']:None for d in f.FIELD_DEFINITIONS};attrs.update(YEAR=-4360 if oid==ids[0] else None,MO=None,DAY=4 if oid==ids[0] else None,VEI=None,VOL_ID=101.0,NUM='0101-06=',NAME=' literal name ',COMMENTS='<p> 001 literal & text </p>',TIME_ERUPT='D1',SIG_ID=42,TSU_ID=2800,HAZ_EVENT_ID=1,OBJECTID=oid,ID=10106,COUNTRY='Explicit synthetic country')
                features.append({'attributes':attrs,'geometry':{'x':1669681.0424083103,'y':4543887.574736076}})
            body=f.encode({'objectIdFieldName':'OBJECTID','geometryType':'esriGeometryPoint','spatialReference':{'wkid':102100,'latestWkid':3857},'fields':copy.deepcopy(f.PAGE_FIELDS),'features':features,'exceededTransferLimit':False})
        bodies[name]=body
    return {name:(bodies[name],{'url':urls[name],'final_url':urls[name],'status':200,'bytes':len(bodies[name]),'sha256':f.digest(bodies[name]),'complete_body':True,'observed_at':'2026-10-19T00:00:00+00:00'})for name in urls}


def write_capture_dir(root,ids):
    for name,(body,receipt)in synthetic_capture_bodies(ids).items():
        (root/(name+'.body')).write_bytes(body);(root/(name+'.receipt.json')).write_bytes(f.encode(receipt))


class SignificantVolcanicTests(unittest.TestCase):
    def test_official_distribution_receipt_has_concrete_delegation(self):
        body=b'explicit synthetic receipt fixture';name='count_before';receipt={'url':f.PRE_PAGE_URLS[name],'final_url':f.PRE_PAGE_URLS[name],'status':200,'bytes':len(body),'sha256':f.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00+00:00'}
        item=f.validate_receipt(name,body,receipt,f.PRE_PAGE_URLS[name]);self.assertEqual(item['role'],'official_distributor');self.assertEqual(item['delegation_evidence']['hosted_service_url'],f.SERVICE);self.assertEqual(item['delegation_evidence']['portal_owner'],'ncei_noaa')
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
    def test_registered_one_asset_checks_and_stopped_monthly_refresh(self):
        self.assertEqual(set(defs.resolve_job_def('significant_volcanic_source_integrity').asset_layer.executable_asset_keys),{AssetKey(['ingest',f.SLUG,f.SLUG])})
        check_names={key.name for definition in defs.asset_checks for key in definition.check_keys}
        self.assertIn('noaa_significant_volcanic_events_complete',check_names);self.assertIn('noaa_significant_volcanic_events_drift',check_names)
        self.assertEqual(significant_volcanic_source_integrity_schedule.default_status,DefaultScheduleStatus.STOPPED)
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
        with tempfile.TemporaryDirectory()as d,tempfile.TemporaryDirectory()as cap:
            client=httpx.Client(transport=httpx.MockTransport(lambda req:httpx.Response(200,stream=Stream())))
            with mock.patch.object(f.httpx,'Client',return_value=client),mock.patch.object(f,'MAX_BODY_BYTES',36),mock.patch.dict(os.environ,{'HIFLD_CAPTURE_ROOT':cap}):
                with self.assertRaises(ValueError):f.acquire(StagingStorageResource(local_dir=d))
            wire=list(Path(cap).glob(f'{f.SLUG}/{f.SLUG}/*/*/wire.body'));self.assertEqual(len(wire),1);self.assertEqual(wire[0].read_bytes(),b'x'*37)
    def test_canonical_CRS_is_independent_of_real_import_order(self):
        code="""import sys,json,hashlib,struct
from pyproj import CRS
pinned=json.loads(sys.argv[2])
if sys.argv[1]=='proj-first':assert CRS.from_json_dict(pinned).to_epsg()==3857
from dagster_hifld.sources import significant_volcanic_events as source
if sys.argv[1]=='source-first':assert CRS.from_json_dict(pinned).to_epsg()==3857
import pyarrow as pa,pyarrow.parquet as pq
table=pa.table({'geometry':pa.array([struct.pack('<BIdd',1,1,0.,0.),struct.pack('<BIdd',1,1,1.,2.)],type=pa.binary())})
output=source.parquet_table(table);assert json.loads(output.schema.metadata[b'geo'])['columns']['geometry']['crs']==pinned
sink=pa.BufferOutputStream();pq.write_table(output,sink,compression='zstd')
print(json.dumps({'metadata_sha256':hashlib.sha256(output.schema.metadata[b'geo']).hexdigest(),'parquet_sha256':hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()}))"""
        observations=[]
        for order in['proj-first','source-first']:
            completed=subprocess.run([sys.executable,'-c',code,order,json.dumps(f.NATIVE_CRS_JSON)],check=True,capture_output=True,text=True,timeout=30)
            observations.append(json.loads(completed.stdout))
        self.assertEqual(observations[0],observations[1])
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
            with tempfile.TemporaryDirectory()as d,tempfile.TemporaryDirectory()as cap:
                client=httpx.Client(transport=httpx.MockTransport(lambda req:httpx.Response(200,stream=Stream())))
                with mock.patch.object(f.httpx,'Client',return_value=client),mock.patch.object(Path,'open',opening),mock.patch.dict(os.environ,{'HIFLD_CAPTURE_ROOT':cap}):
                    with self.assertRaises(ValueError):f.acquire(StagingStorageResource(local_dir=d))
                prefix=Path(cap)/f.SLUG/f.SLUG
                pending=list(prefix.glob('*/*/pending_'+target));self.assertEqual(len(pending),1);self.assertEqual(pending[0].read_bytes(),delivered)
                physical=next(prefix.glob('*/*/'+target));self.assertEqual(physical.read_bytes(),delivered[:7])
                transfer=json.loads(next(prefix.glob('*/*/transfer.json')).read_bytes());self.assertFalse(transfer['complete']);self.assertFalse(transfer['independent_physical_readback_matches_acknowledged_sinks']);self.assertEqual(transfer['physical_'+target.split('.')[0]]['bytes'],7)
                self.assertFalse((Path(d)/'_construction-state').exists())
    def test_retained_replay_dir_roundtrips_through_acquire(self):
        with tempfile.TemporaryDirectory()as d,tempfile.TemporaryDirectory()as cap,tempfile.TemporaryDirectory()as retained:
            write_capture_dir(Path(retained),[7000,7001])
            with mock.patch.dict(os.environ,{'HIFLD_CAPTURE_ROOT':cap}):
                captures=f.acquire(StagingStorageResource(local_dir=d),capture_dir=retained)
            self.assertEqual(set(captures),set(f.capture_urls(f.derive_pages([7000,7001]))))
            self.assertTrue((Path(cap)/f.SLUG/f.SLUG).exists())
    def test_retained_replay_roster_drift_and_missing_file_refused(self):
        with tempfile.TemporaryDirectory()as d,tempfile.TemporaryDirectory()as cap,tempfile.TemporaryDirectory()as retained:
            write_capture_dir(Path(retained),[7000,7001])
            (Path(retained)/'extra.body').write_bytes(b'unbound retained artifact')
            with mock.patch.dict(os.environ,{'HIFLD_CAPTURE_ROOT':cap}):
                with self.assertRaisesRegex(ValueError,'retained_capture_roster_drift'):
                    f.acquire(StagingStorageResource(local_dir=d),capture_dir=retained)
            (Path(retained)/'extra.body').unlink()
            (Path(retained)/'ncei.body').unlink()
            with mock.patch.dict(os.environ,{'HIFLD_CAPTURE_ROOT':cap}):
                with self.assertRaisesRegex(ValueError,'retained_capture_roster_drift'):
                    f.acquire(StagingStorageResource(local_dir=d),capture_dir=retained)
    def test_upstream_edit_passes_completeness_and_drift_check_reports(self):
        # One feature added by the producer: completeness is the capture-time
        # witness, so the capture stays complete; the registered drift check
        # has no approved baseline and reports an explicit skip (WARN).
        ids=list(range(7000,7600))
        table,audit=f.inspect_captures(synthetic_capture_bodies(ids))
        candidate={'dataset_slug':f.SLUG,'file_slug':f.SLUG,'feature_count':table.num_rows,
            'source_reported_count':audit['source_reported_count'],'source_id_sha256':audit['source_id_sha256'],'source_population_complete':True}
        self.assertTrue(noaa_significant_volcanic_events_complete(candidate).passed)
        edited=ids+[7600]
        table2,audit2=f.inspect_captures(synthetic_capture_bodies(edited))
        candidate2={'dataset_slug':f.SLUG,'file_slug':f.SLUG,'feature_count':table2.num_rows,
            'source_reported_count':audit2['source_reported_count'],'source_id_sha256':audit2['source_id_sha256'],'source_population_complete':True}
        self.assertEqual(table2.num_rows,len(ids)+1);self.assertTrue(noaa_significant_volcanic_events_complete(candidate2).passed)
        drift=noaa_significant_volcanic_events_drift(candidate2)
        self.assertEqual(drift.severity.name,'WARN');self.assertEqual(drift.metadata['comparison_status'],'skipped_no_approved_baseline');self.assertFalse(drift.passed)
        self.assertNotEqual(candidate['source_id_sha256'],candidate2['source_id_sha256'])
    def test_observed_count_outside_plausibility_band_quarantined(self):
        for n in(f.COUNT_FLOOR-1,f.COUNT_CEIL+1):
            with self.assertRaisesRegex(ValueError,'observed_count_outside_plausibility_band'):
                f.inspect_captures(synthetic_capture_bodies(list(range(7000,7000+n))))
    def test_before_after_witness_drift_quarantined(self):
        def mutated(captures,name,body):
            receipt=dict(captures[name][1]);receipt['bytes']=len(body);receipt['sha256']=f.digest(body)
            captures[name]=(body,receipt);return captures
        ids=list(range(7000,7600))
        captures=mutated(synthetic_capture_bodies(ids),'count_after',f.encode({'count':len(ids)+1}))
        with self.assertRaisesRegex(ValueError,'source_count_or_id_roster_drift|invalid_complete_OID_witness'):f.inspect_captures(captures)
        captures=mutated(synthetic_capture_bodies(ids),'ids_after',f.encode({'objectIdFieldName':'OBJECTID','objectIds':ids[:-1]+[999999]}))
        with self.assertRaisesRegex(ValueError,'source_count_or_id_roster_drift'):f.inspect_captures(captures)
    def test_pages_derived_from_observed_roster_cover_exact_slices(self):
        ids=list(range(7000,7000+2*f.PAGE_SIZE+37))
        captures=synthetic_capture_bodies(ids)
        pages=f.derive_pages(ids)
        self.assertEqual(sorted(pages),[f'page_{i:04d}'for i in range((len(ids)+f.PAGE_SIZE-1)//f.PAGE_SIZE)])
        table,audit=f.inspect_captures(captures)
        self.assertEqual(table.num_rows,len(ids));self.assertEqual(audit['source_reported_count'],len(ids))
    def test_cached_authored_snapshot_and_resolved_wrapper_exact(self):
        with tempfile.TemporaryDirectory()as d:
            base=Path(d);version='v0.0.0-draft-structural';root=base/f.SLUG/f.SLUG/version;(root/'geoparquet').mkdir(parents=True)
            table=decode(page());path=root/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(table),path,compression='zstd');identity={'fixture':'explicit synthetic two-row cache'};audit={}
            captures=synthetic_capture_bodies([7000,7001])
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
        with tempfile.TemporaryDirectory()as d,tempfile.TemporaryDirectory()as cap,tempfile.TemporaryDirectory()as retained:
            write_capture_dir(Path(retained),[7000,7001])
            (Path(retained)/'ncei.receipt.json').write_bytes(b'{bad')
            with mock.patch.dict(os.environ,{'HIFLD_CAPTURE_ROOT':cap}):
                with self.assertRaises(ValueError):f.acquire(StagingStorageResource(local_dir=d),capture_dir=retained)
            self.assertEqual(next(Path(d).glob('_raw-captures/*/*/*.body')).read_bytes(),b'https://www.ncei.noaa.gov/maps/hazards/?layers=0 explicit synthetic producer page')
            failure=next(Path(cap).glob(f'{f.SLUG}/{f.SLUG}/*/failure.json'))
            self.assertTrue(failure.read_bytes().startswith(b'{"approved":false'))


@unittest.skipUnless(os.environ.get('SIGNIFICANT_VOLCANIC_TEST_CAPTURE_DIR'),'Complete real source is explicit external opt-in')
class CompleteSignificantVolcanicTests(unittest.TestCase):
    def test_complete_observed_all59attrs_and_native_geometry(self):
        r=Path(os.environ['SIGNIFICANT_VOLCANIC_TEST_CAPTURE_DIR'])
        count=f.strict_json((r/'count_before.body').read_bytes())['count']
        names=f.capture_urls(f.derive_pages(f.id_roster((r/'ids_before.body').read_bytes(),count)))
        c={n:((r/(n+'.body')).read_bytes(),f.strict_json((r/(n+'.receipt.json')).read_bytes()))for n in names}
        t,a=f.inspect_captures(c);self.assertEqual(t.num_columns,60);self.assertEqual(a['source_reported_count'],t.num_rows);self.assertTrue(f.COUNT_FLOOR<=a['source_reported_count']<=f.COUNT_CEIL);self.assertEqual(t['YEAR'].type,pa.int16())
        with tempfile.TemporaryDirectory()as d:
            p=Path(d);(p/'geoparquet').mkdir();pq.write_table(f.parquet_table(t),p/'geoparquet'/f'{f.SLUG}.parquet',compression='zstd');f.validate_output(p,t)
            path=p/'geoparquet'/f'{f.SLUG}.parquet';manifest=f.manifest_for(c,{'explicit_test_identity':True},{'bytes':path.stat().st_size,'sha256':f.sha256_file(path)});f.validate_release_provenance(manifest);self.assertTrue(all(x.get('delegation_evidence')for x in manifest['inputs']if x['role']=='official_distributor'))

if __name__=='__main__':unittest.main()
