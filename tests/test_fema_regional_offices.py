"""Complete captured source tests; test fixtures never approve real releases."""
import base64
import copy
import gzip
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import httpx
import pyarrow as pa
from dagster_hifld import construction_state
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import fema_regional_offices as s

FIXTURE=Path(__file__).parent/'fixtures/fema-regional-offices-complete.json.gz'

def unpack(root):
    values=json.loads(gzip.decompress(FIXTURE.read_bytes()))
    for name,body in values.items():(root/name).write_bytes(base64.b64decode(body))

def fixture():
    values=json.loads(gzip.decompress(FIXTURE.read_bytes()));result={}
    for name in values:
        if name.endswith('.body'):
            key=name[:-5];result[key]=(base64.b64decode(values[name]),json.loads(base64.b64decode(values[key+'.receipt.json'])))
    return result

def mutate(captures,name,change):
    body,receipt=captures[name];value=json.loads(body);change(value);body=s.encode(value);captures[name]=(body,receipt|{'sha256':s.digest(body),'bytes':len(body)})

class FemaOfficeTests(unittest.TestCase):
    def stage(self,storage,root):
        # State/cache behavior unit boundary; actual registered driver separately
        # exercises unpatched reachable-Git validation at a published commit.
        with patch.object(s,'validate_construction_code_identity'):
            return s.stage_fema_regional_offices(storage,capture_dir=root)

    def test_complete_original_fourteen_fields_values_native_points(self):
        captures=fixture();table,audit=s.inspect_captures(captures);self.assertEqual(table.num_rows,14);self.assertEqual(table.column_names,list(s.FIELDS));self.assertEqual(table['region_number'].type,pa.int16());self.assertEqual(table['region_objectid'].type,pa.int16())
        self.assertEqual(audit['office_type_counts'],{'area':3,'national':1,'regional':10});rows=table.to_pylist();wire=json.loads(captures['page_00000'][0])['features']
        for actual,raw in zip(rows,wire):
            self.assertEqual({k:v for k,v in actual.items() if k!='geometry'},raw['attributes']);self.assertEqual(actual['geometry'],s.shapely.Point(raw['geometry']['x'],raw['geometry']['y']).wkb)
        self.assertEqual(rows[2]['address1'],'546 Bonney Loop ');self.assertIn('00918',table['zip'].to_pylist());self.assertIn('0002',table['zip4'].to_pylist());self.assertNotIn('id',table.column_names)

    def test_full_baseline_formats_and_separate_ids(self):
        c=fixture();table,_=s.inspect_captures(c);report=s.baseline_comparison(c,table)
        self.assertEqual(set(report['formats']),{'geoparquet','geopackage','geojson','shapefile'})
        for name,summary in report['formats'].items():
            self.assertEqual(summary['rows'],14);self.assertEqual(summary['geometry_WKB_exact_with_current_source'],14)
            self.assertEqual(len(summary['diagnostic_attribute_differences']),0 if name=='geojson' else 1)
        ids=[r['id'] for r in report['formats']['geoparquet']['published_identifiers']];self.assertLess(len(set(ids)),14);self.assertFalse(report['approved']);self.assertEqual(report['status'],'blocked')

    def test_display_only_renderer_exclusion_and_real_drift(self):
        c=fixture();original=s.inspect_captures(c)
        for name in ('metadata_before','metadata_after'):mutate(c,name,lambda d:d.update(drawingInfo={'synthetic_display_only':True}))
        table,audit=s.inspect_captures(c);self.assertTrue(table.equals(original[0],check_metadata=True));self.assertEqual(audit,original[1])
        mutate(c,'metadata_after',lambda d:d.update(maxRecordCount=9999))
        with self.assertRaisesRegex(ValueError,'source_changed'):s.inspect_captures(c)

    def test_missing_duplicate_unrequested_limit_schema_geometry_drift(self):
        changes=[lambda d:d['features'].pop(),lambda d:d['features'].append(d['features'][0]),lambda d:d['features'][0]['attributes'].update(objectid=999),lambda d:d.update(exceededTransferLimit=True),lambda d:d['features'][0]['attributes'].update(renamed=1),lambda d:d.update(spatialReference={'latestWkid':3857}),lambda d:d['features'][0]['geometry'].update(z=1)]
        for change in changes:
            c=fixture();mutate(c,'page_00000',change)
            with self.subTest(change=change),self.assertRaises(ValueError):s.inspect_captures(c)
        for name,change in [('count_after',lambda d:d.update(count=10)),('ids_after',lambda d:d.update(objectIds=[1,2])),('metadata_before',lambda d:d['fields'][6].update(type='esriFieldTypeInteger')),('authority_service',lambda d:d['layers'][0].update(geometryType='esriGeometryPolygon'))]:
            c=fixture();mutate(c,name,change)
            with self.subTest(name=name),self.assertRaises(ValueError):s.inspect_captures(c)

    def test_json_metadata_boolean_integer_float_drift_never_compares_equal(self):
        # Reviewer actual reproducer: False==0 must NOT make typed metadata equal.
        cases=[lambda d:d['fields'][0].update(nullable=0),lambda d:d['fields'][0].update(nullable=0.0),lambda d:d['fields'][0].update(editable=0),lambda d:d['fields'][1].update(length=40.0),lambda d:d.update(id=False),lambda d:d.update(id=0.0),lambda d:d['extent']['spatialReference'].update(latestWkid=4326.0),lambda d:d.update(maxRecordCount=2000.0),lambda d:d.update(minScale=False)]
        for change in cases:
            c=fixture();mutate(c,'metadata_after',change)
            with self.subTest(change=change),self.assertRaises(ValueError):s.inspect_captures(c)

    def test_original_refresh_semantic_json_types_are_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);c=fixture();identity={'files':{}};pin={'bytes':1,'sha256':'a'*64}
            for name,(body,receipt) in c.items():s.immutable(storage,f'_raw-captures/{s.SLUG}/{s.digest(body)}/{name}.body',body)
            with patch.object(s,'validate_construction_code_identity'):
                s.original_bundle(storage,'v1',c,identity,s.manifest_for(c,identity,pin),create=True)
                for name in ('metadata_before','metadata_after'):mutate(c,name,lambda d:d.update(minScale=False))
                s.inspect_captures(c) # Consistent new semantics may form a NEW draft version.
                with self.assertRaisesRegex(ValueError,'original_source_semantics_changed'):s.original_bundle(storage,'v1',c,identity,s.manifest_for(c,identity,pin))

    def test_physical_values_no_coercion(self):
        for field,value in [('objectid',True),('region_number',32768),('zip',123),('name','x'*41),('objectid',None)]:
            with self.subTest(field=field),self.assertRaises(ValueError):s.physical_value(next(f for f in s.FIELD_DEFINITIONS if f['name']==field),value)
        self.assertEqual(s.physical_value(s.FIELD_DEFINITIONS[8],'  literal  '),'  literal  ')

    def test_native_containers_and_metadata_forgery(self):
        table,_=s.inspect_captures(fixture())
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for f in ('candidate','geoparquet','geopackage'):(root/f).mkdir()
            for name in ('candidate/typed-source.parquet',f'geoparquet/{s.SLUG}.parquet'):s.pq.write_table(s.expected_parquet(table),root/name,compression='zstd')
            s.write_gpkg(table,root/'geopackage'/f'{s.SLUG}.gpkg');s.verify_outputs(root,table)
            with s.sqlite3.connect(root/'geopackage'/f'{s.SLUG}.gpkg') as db:db.execute('UPDATE gpkg_contents SET srs_id=3857')
            with self.assertRaisesRegex(ValueError,'full_container'):s.verify_outputs(root,table)

    def test_failed_receipt_body_first_and_exact_bad_bytes(self):
        for mode in ('malformed','unreadable'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);inputs=root/'inputs';inputs.mkdir();unpack(inputs);receipt=inputs/'authority_root.receipt.json';body=(inputs/'authority_root.body').read_bytes();bad=b'{not JSON\n'
                if mode=='malformed':receipt.write_bytes(bad)
                else:receipt.chmod(0)
                storage=StagingStorageResource(local_dir=str(root/'staging'))
                try:
                    with self.assertRaises((ValueError,PermissionError)):s.acquire(storage,inputs)
                finally:receipt.chmod(0o600)
                depot=root/'staging'/'_raw-captures'/s.SLUG/s.digest(body);self.assertEqual((depot/'authority_root.body').read_bytes(),body)
                attempted=json.loads(next((root/'staging'/'_quarantine').rglob('attempted-inputs.json')).read_bytes())['objects']['authority_root']
                self.assertEqual(attempted['raw_receipt_read'],mode=='malformed')
                if mode=='malformed':self.assertEqual((root/'staging'/attempted['raw_receipt_key']).read_bytes(),bad)
                self.assertFalse((root/'staging'/'_construction-state').exists())

    def test_local_guard_precedes_acquisition_and_relative_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);inputs=root/'inputs';inputs.mkdir();unpack(inputs);before=Path.cwd()
            try:
                os.chdir(root)
                result=self.stage(StagingStorageResource(local_dir='relative/staging'),inputs);self.assertEqual(result['feature_count'],14)
            finally:os.chdir(before)
            for storage in (PublishedStorageResource(local_dir=tmp),StagingStorageResource(local_dir=str(root/'..'/'bad')),StagingStorageResource(local_dir=tmp,bucket='production')):
                with patch.object(s,'fetch_one') as fetch,self.assertRaises(ValueError):s.stage_fema_regional_offices(storage)
                fetch.assert_not_called()

    def test_cache_reuse_renderer_receipt_new_observation_preserves_original(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);inputs=root/'inputs';inputs.mkdir();unpack(inputs);storage=StagingStorageResource(local_dir=str(root/'staging'));first=self.stage(storage,inputs);target=root/'staging'/s.SLUG/s.SLUG/first['version'];before={str(p.relative_to(target)):p.read_bytes() for p in target.rglob('*') if p.is_file()}
            alias=root/'staging'/s.construction_record_key(first['version']);original=alias.read_bytes()
            for name in ('metadata_before','metadata_after'):
                path=inputs/(name+'.body');d=json.loads(path.read_bytes());d['drawingInfo']={'new_display':1};body=s.encode(d);path.write_bytes(body);p=inputs/(name+'.receipt.json');rec=json.loads(p.read_bytes());rec.update(sha256=s.digest(body),bytes=len(body),observed_at='2026-10-19T13:00:00+00:00');p.write_bytes(s.encode(rec))
            second=self.stage(storage,inputs);self.assertTrue(second['reused']);self.assertEqual(second['version'],first['version']);self.assertEqual(alias.read_bytes(),original);self.assertEqual(before,{str(p.relative_to(target)):p.read_bytes() for p in target.rglob('*') if p.is_file()})

    def test_missing_all_originals_durable_incident_prevents_cold_rebootstrap(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);inputs=root/'inputs';inputs.mkdir();unpack(inputs);storage=StagingStorageResource(local_dir=str(root/'staging'));first=self.stage(storage,inputs);target=root/'staging'/s.SLUG/s.SLUG/first['version'];state=root/'staging'/s.construction_record_key(first['version']);backup={p:p.read_bytes() for p in state.parent.rglob('*') if p.is_file()}
            for p in backup:p.unlink()
            with self.assertRaisesRegex(ValueError,'invalid_candidate_detached_intact:observed_original_trust_loss'):self.stage(storage,inputs)
            self.assertFalse(target.exists());incident=root/'staging'/s.incident_directory(first['version']);self.assertTrue(any(incident.rglob('*')))
            for _ in range(2):
                with self.assertRaisesRegex(ValueError,'permanent_observed_original_trust_loss'):self.stage(storage,inputs)
            self.assertFalse(state.exists());self.assertTrue(any((root/'staging'/'_quarantine').rglob('typed-source.parquet')))

    def test_incident_write_failure_preserves_candidate_and_partial_journal_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);inputs=root/'inputs';inputs.mkdir();unpack(inputs);storage=StagingStorageResource(local_dir=str(root/'staging'));first=self.stage(storage,inputs);target=root/'staging'/s.SLUG/s.SLUG/first['version'];alias=root/'staging'/s.construction_record_key(first['version']);original=alias.read_bytes();alias.unlink();before={str(p.relative_to(target)):p.read_bytes() for p in target.rglob('*') if p.is_file()}
            with patch.object(s,'record_observed_loss',side_effect=OSError('durability unavailable')),self.assertRaisesRegex(s.IncidentWriteError,'candidate_preserved'):self.stage(storage,inputs)
            self.assertEqual(before,{str(p.relative_to(target)):p.read_bytes() for p in target.rglob('*') if p.is_file()})
            directory=root/'staging'/s.incident_directory(first['version']);directory.mkdir();(directory/'.pending-interrupted').write_bytes(b'partial')
            alias.write_bytes(original)
            with self.assertRaisesRegex(ValueError,'permanent_observed_original_trust_loss'):self.stage(storage,inputs)
            self.assertTrue(target.exists())

    def test_private_failure_creates_no_first_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);inputs=root/'inputs';inputs.mkdir();unpack(inputs);storage=StagingStorageResource(local_dir=str(root/'staging'))
            with patch.object(s,'write_gpkg',side_effect=OSError('private writer failed')),self.assertRaisesRegex(ValueError,'private writer failed'):self.stage(storage,inputs)
            self.assertFalse((root/'staging'/'_construction-state').exists());self.assertFalse(list((root/'staging').glob(s.SLUG+'/'+s.SLUG+'/v*')))

    def test_encoded_transport_and_partial_error_retains_read_body(self):
        real=httpx.Client;body=b'{"complete":true}';encoded=gzip.compress(body)
        def good(request):return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(encoded))},stream=httpx.ByteStream(encoded))
        with patch.object(s.httpx,'Client',side_effect=lambda **kw:real(transport=httpx.MockTransport(good),**kw)):
            data,receipt=s.fetch_one(s.BASE);self.assertEqual(data,body);self.assertEqual(receipt['wire_bytes_downloaded'],len(encoded))
        def short(request):return httpx.Response(200,headers={'Content-Length':'999'},stream=httpx.ByteStream(body))
        with patch.object(s.httpx,'Client',side_effect=lambda **kw:real(transport=httpx.MockTransport(short),**kw)),self.assertRaises(s.FemaOfficesFetchError) as caught:s.fetch_one(s.BASE)
        self.assertEqual(caught.exception.body,body);self.assertFalse(caught.exception.receipt['complete_body'])

if __name__=='__main__':unittest.main()
