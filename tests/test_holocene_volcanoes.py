"""Tiny structural sources by default; complete dated Smithsonian captures opt in."""
import copy
import json
import os
import re
import shutil
import stat
import struct
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import holocene_volcanoes as f


def population():
    rows=[]
    for i in range(2):
        props={d['name']:None for d in f.FIELD_DEFINITIONS[:-1]}
        props.update(Volcano_Number=210010+i,Volcano_Name=' 0001 literal ',Volcanic_Landform='',Last_Eruption_Year=-8300 if i else None,Latitude=50.17,Longitude=6.85,Elevation=-150 if i else None,Region='Explicit synthetic region')
        rows.append({'type':'Feature','id':'synthetic-original-'+str(i),'geometry':{'type':'Point','coordinates':[6.85,50.17]},'geometry_name':'GeoLocation','properties':props})
    return {'type':'FeatureCollection','totalFeatures':2,'numberMatched':2,'numberReturned':2,'crs':{'type':'name','properties':{'name':'urn:ogc:def:crs:EPSG::4326'}},'features':rows}


def decode(data):return f.decode_population(json.dumps(data).encode(),expected_count=2)


class HoloceneStructuralTests(unittest.TestCase):
    def test_explicit_registration_and_stopped_refresh(self):
        from dagster_hifld.definitions import defs
        from dagster_hifld.assets.smithsonian_holocene import holocene_source_integrity_schedule
        from dagster import DefaultScheduleStatus, AssetKey
        self.assertEqual(set(defs.resolve_job_def("holocene_source_integrity").asset_layer.executable_asset_keys),{AssetKey(["ingest",f.SLUG,f.SLUG])})
        self.assertEqual(holocene_source_integrity_schedule.default_status,DefaultScheduleStatus.STOPPED)

    def test_exact_types_ids_literals_null_and_native_point(self):
        t,a=decode(population())
        self.assertEqual(t['id'].to_pylist(),['synthetic-original-0','synthetic-original-1'])
        self.assertEqual(t['Volcano_Number'].type,pa.int32());self.assertEqual(t['Elevation'].type,pa.int16())
        self.assertEqual(t['Latitude'].type,pa.decimal128(38,9));self.assertEqual(t['Latitude'][0].as_py(),Decimal('50.17'))
        self.assertEqual(t['Volcano_Name'][0].as_py(),' 0001 literal ');self.assertEqual(t['Volcanic_Landform'][0].as_py(),'')
        self.assertIsNone(t['Elevation'][0].as_py());self.assertEqual(t['Last_Eruption_Year'][1].as_py(),-8300)
        self.assertEqual(t['GeoLocation'][0].as_py(),struct.pack('<BIdd',1,1,6.85,50.17))
        self.assertFalse(t.schema.field('Volcano_Number').nullable);self.assertTrue(t.schema.field('Elevation').nullable)
    def test_exact_decimal_parquet_roundtrip_and_drift(self):
        t,_=decode(population())
        with tempfile.TemporaryDirectory()as d:
            p=Path(d);(p/'geoparquet').mkdir();pq.write_table(f.parquet_table(t),p/'geoparquet'/f'{f.SLUG}.parquet',compression='zstd');f.validate_output(p,t)
            changed=t.set_column(11,t.schema.field('Longitude'),pa.array([Decimal('7'),Decimal('7')],type=pa.decimal128(38,9)))
            with self.assertRaises(ValueError):f.validate_output(p,changed)
    def test_missing_extra_fields_and_duplicate_ids_rejected(self):
        for mutate in [lambda p:p['features'][0]['properties'].pop('Country'),lambda p:p['features'][0]['properties'].update(extra='x'),lambda p:p['features'][1].update(id=p['features'][0]['id']),lambda p:p['features'][1]['properties'].update(Volcano_Number=210010)]:
            p=population();mutate(p)
            with self.assertRaises(ValueError):decode(p)
    def test_physical_types_and_nullability_rejected(self):
        for name,value in [('Volcano_Number',True),('Volcano_Number',210010.0),('Elevation',32768),('Elevation',False),('Last_Eruption_Year',-2**31-1),('Volcanic_Landform',None),('Country',3)]:
            p=population();p['features'][0]['properties'][name]=value
            with self.assertRaises(ValueError,msg=name):decode(p)
    def test_decimal_bound_and_geometry_drift(self):
        for value in [0.1234567891,float('nan')]:
            p=population();p['features'][0]['properties']['Latitude']=value
            with self.assertRaises(ValueError):decode(p)
        for mutate in [lambda p:p['features'][0]['geometry'].update(type='LineString'),lambda p:p['features'][0]['geometry'].update(coordinates=[6.86,50.17]),lambda p:p['crs']['properties'].update(name='EPSG:3857'),lambda p:p.update(numberMatched=True),lambda p:p['features'].pop()]:
            p=population();mutate(p)
            with self.assertRaises(ValueError):decode(p)
    def test_duplicate_json_and_xml_entities_rejected(self):
        with self.assertRaises(ValueError):f.decimal_json(b'{"x":1,"x":2}')
        with self.assertRaises(ValueError):f.declarations(b'<!DOCTYPE a><a/>')
    def test_bounded_regular_input_and_staging_specials(self):
        with tempfile.TemporaryDirectory()as d:
            root=Path(d);fifo=root/'fifo';os.mkfifo(fifo)
            with self.assertRaises(ValueError):f.bounded_input(fifo,1024)
            with self.assertRaises(ValueError):f.local_only(StagingStorageResource(local_dir=d))
            fifo.unlink();p=root/'p';p.write_bytes(b'1234')
            with self.assertRaises(ValueError):f.bounded_input(p,3)
            os.link(p,root/'link')
            with self.assertRaises(ValueError):f.bounded_input(p,4)
    def test_short_private_write_rejected(self):
        with tempfile.TemporaryDirectory()as d,mock.patch.object(f.os,'write',return_value=1):
            with self.assertRaisesRegex(ValueError,'incomplete_private_write'):f.write_private(Path(d),'metadata/test.json',b'{}\n')
    def test_live_consumed_wire_retained_before_oversize(self):
        class Stream(httpx.SyncByteStream):
            def __iter__(self):yield b'x'*37
        with tempfile.TemporaryDirectory()as d:
            s=StagingStorageResource(local_dir=d)
            client=httpx.Client(transport=httpx.MockTransport(lambda req:httpx.Response(200,stream=Stream())))
            with mock.patch.object(f.httpx,'Client',return_value=client),mock.patch.object(f,'MAX_BODY_BYTES',36):
                with self.assertRaises(ValueError):f.acquire(s)
            wire=list(Path(d).glob('_acquisition-attempts/*/*/*/wire.body'))
            self.assertEqual(len(wire),1);self.assertEqual(wire[0].read_bytes(),b'x'*37)
    def test_malformed_receipt_retains_body_and_receipt_before_parse(self):
        with tempfile.TemporaryDirectory()as d:
            root=Path(d);cap=root/'inputs';cap.mkdir();first=next(iter(f.URLS));(cap/(first+'.body')).write_bytes(b'literal first body');(cap/(first+'.receipt.json')).write_bytes(b'{malformed')
            s=StagingStorageResource(local_dir=str(root/'stage'))
            with self.assertRaises(ValueError):f.acquire(s,cap)
            self.assertEqual(next((root/'stage').glob('_raw-captures/*/*/*.body')).read_bytes(),b'literal first body')
            self.assertEqual(next((root/'stage').glob('_acquisition-attempts/*/*/*.raw-receipt.body')).read_bytes(),b'{malformed')


@unittest.skipUnless(os.environ.get('HOLOCENE_TEST_CAPTURE_DIR'),'Complete real source is an explicit external artifact opt-in')
class HoloceneCompleteSourceTests(unittest.TestCase):
    def test_complete_original_source(self):
        r=Path(os.environ['HOLOCENE_TEST_CAPTURE_DIR']);c={n:((r/(n+'.body')).read_bytes(),f.strict_json((r/(n+'.receipt.json')).read_bytes()))for n in f.URLS};t,a=f.inspect_captures(c)
        self.assertEqual(t.num_rows,1214);self.assertEqual(len(set(t['Volcano_Number'].to_pylist())),1214);self.assertEqual(len(set(t['id'].to_pylist())),1214);self.assertEqual(a['changed_server_feature_ids'],1214)
        self.assertEqual(a['historical_reference_source_id_roster_sha256'],f.HISTORICAL_REFERENCE_SOURCE_ID_SHA256)
        self.assertNotEqual(a['observed_source_id_roster_sha256'],a['historical_reference_source_id_roster_sha256'])
        self.assertNotEqual(a['observed_source_id_roster_sha256'],a['after_source_id_roster_sha256'])
        with tempfile.TemporaryDirectory()as d:
            p=Path(d);(p/'geoparquet').mkdir();pq.write_table(f.parquet_table(t),p/'geoparquet'/f'{f.SLUG}.parquet',compression='zstd');f.validate_output(p,t)

    def test_cache_reuses_first_context_when_only_unstable_ids_change(self):
        captured=Path(os.environ['HOLOCENE_TEST_CAPTURE_DIR'])
        with tempfile.TemporaryDirectory()as d:
            root=Path(d);variant=root/'variant-capture';shutil.copytree(captured,variant)
            for name,label in [('wfs_all','first'),('wfs_all_after','second')]:
                body_path=variant/(name+'.body');body=body_path.read_bytes();index=0
                def replace_id(match):
                    nonlocal index
                    value=f'synthetic-{label}-{index:04d}'.encode();index+=1
                    return match.group(1)+b'"'+value+b'"'
                mutated=re.sub(rb'("id"\s*:\s*)"(?:\\.|[^"\\])*"',replace_id,body)
                self.assertEqual(index,1214)
                body_path.write_bytes(mutated)
                receipt_path=variant/(name+'.receipt.json');receipt=json.loads(receipt_path.read_text(encoding='utf-8'))
                receipt['sha256']=f.digest(mutated);receipt['bytes']=len(mutated)
                receipt_path.write_text(json.dumps(receipt,sort_keys=True,separators=(',',':'))+'\n',encoding='utf-8')

            storage=StagingStorageResource(use_local=True,local_dir=str(root/'staging'),prefix='',backend='local')
            first=f.stage_holocene(storage,capture_dir=captured)
            candidate=Path(storage.local_dir)/first['dataset_slug']/first['file_slug']/first['version']
            before={str(p.relative_to(candidate)):f.sha256_file(p)for p in candidate.rglob('*')if p.is_file()}
            state=Path(storage.local_dir)/f.construction_record_key(first['version'])
            state_hash=f.sha256_file(state)
            second=f.stage_holocene(storage,capture_dir=variant)
            after={str(p.relative_to(candidate)):f.sha256_file(p)for p in candidate.rglob('*')if p.is_file()}
            self.assertFalse(first['reused']);self.assertTrue(second['reused']);self.assertEqual(first['version'],second['version'])
            self.assertEqual(before,after);self.assertEqual(f.sha256_file(state),state_hash)
            self.assertEqual(len(list((Path(storage.local_dir)/'_quarantine'/f.SLUG).glob('**/*'))),0)
            cached=json.loads((candidate/'metadata'/'holocene_source_audit.json').read_text(encoding='utf-8'))
            self.assertEqual(cached['all_original_feature_ids'],f.inspect_captures({n:((captured/(n+'.body')).read_bytes(),f.strict_json((captured/(n+'.receipt.json')).read_bytes()))for n in f.URLS})[1]['all_original_feature_ids'])

if __name__=='__main__':unittest.main()
