"""Small default fixtures and opt-in complete-real-source tests; no release approval.

Unit state boundaries patch only Git identity. The registered runner separately
uses a published commit and real Git/state verification before first construction.
"""
import copy
import fcntl
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import struct
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import shapely
from dagster_hifld.sources import epa_regions as s

FIXTURE=Path(__file__).parent/'fixtures/epa-regions'

def unpack(root):
    original=Path(os.environ['HIFLD_EPA_REGIONS_REAL_CAPTURE_DIR'])
    manifest=json.loads((FIXTURE/'manifest.json').read_bytes())
    for name,pin in manifest['original_files'].items():
        if Path(name).name!=name:raise ValueError('fixture_nonliteral_file')
        body=(original/name).read_bytes()
        if len(body)!=pin['bytes']or hashlib.sha256(body).hexdigest()!=pin['sha256']:raise ValueError('complete_original_capture_mismatch')
        (root/name).write_bytes(body)

def transport_fixture(root):
    # Bounded structural fixture only; it never represents complete source data.
    body=b'structural HTTP body; not a source capture or release'
    (root/'official_application.body').write_bytes(body)
    receipt={'url':s.URLS['official_application'],'final_url':s.URLS['official_application'],'status':200,'bytes':len(body),'sha256':s.digest(body),'observed_at':'2026-10-19T00:00:00+00:00','complete_body':True,'wire_bytes_downloaded':len(body),'content_length':str(len(body)),'content_encoding':None}
    (root/'official_application.receipt.json').write_bytes(s.encode(receipt))

def stage(storage,inputs):
    with patch.object(s,'validate_construction_code_identity'):
        return s.stage_epa_regions(storage,capture_dir=inputs)

def caps(inputs):
    return {p.name[:-5]:(p.read_bytes(),json.loads(p.with_name(p.name[:-5]+'.receipt.json').read_bytes()))for p in inputs.glob('*.body')}

def mutate(captures,name,change):
    body,receipt=captures[name];value=json.loads(body);change(value);body=s.encode(value)
    captures[name]=(body,receipt|{'bytes':len(body),'sha256':s.digest(body)})

@unittest.skipUnless(os.environ.get('HIFLD_EPA_REGIONS_REAL_CAPTURE_DIR'),'opt-in complete captured real source; set HIFLD_EPA_REGIONS_REAL_CAPTURE_DIR to verified artifact restoration')
class EpaRegionsOriginalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.base=Path(cls.temp.name);cls.inputs=cls.base/'inputs';cls.inputs.mkdir();unpack(cls.inputs)
        cls.captures=caps(cls.inputs);cls.table,cls.audit=s.inspect_captures(cls.captures);cls.original=cls.base/'original';cls.output=stage(s.StagingStorageResource(local_dir=str(cls.original)),cls.inputs);cls.version=cls.output['version'];cls.prefix=f'{s.SLUG}/{s.SLUG}/{cls.version}'
    @classmethod
    def tearDownClass(cls):cls.temp.cleanup()
    def setUp(self):
        self.tempdir=tempfile.TemporaryDirectory();self.addCleanup(self.tempdir.cleanup);self.root=Path(self.tempdir.name);self.path=self.root/'staging';shutil.copytree(self.original,self.path);self.storage=s.StagingStorageResource(local_dir=str(self.path));self.candidate=self.path/self.prefix;self.prior=self.path/'prior.bin';self.prior.write_bytes(b'prior fixture stays unchanged')
    def refresh(self):return stage(self.storage,self.inputs)
    def record(self):return self.path/s.construction_record_key(self.version)
    def pins(self,root):return {str(p.relative_to(root)):s.sha256_file(p)for p in root.rglob('*')if p.is_file()}
    def test_all_real60_attributes_int32_nullability_widths_and_full_native_rings(self):
        self.assertEqual(self.table.num_rows,10);self.assertEqual(self.table.column_names,list(s.FIELDS));self.assertEqual(self.table['FID'].type,pa.int32());self.assertEqual(self.table['OBJECTID'].type,pa.int32());self.assertFalse(self.table.schema.field('FID').nullable);self.assertTrue(self.table.schema.field('OBJECTID').nullable)
        original=json.loads(self.captures['page_00000'][0])['features'];actual=pq.read_table(self.candidate/'geoparquet/epa-regions.parquet').to_pylist();coordinates=0;rings=0
        for raw,row in zip(original,actual):
            self.assertEqual({k:v for k,v in row.items()if k!='geometry'},raw['attributes'])
            self.assertEqual(type(row['FID']),int);self.assertEqual(type(row['Shape_Area']),float)
            before=[];after=[]
            for ring in raw['geometry']['rings']:before.append(b''.join(struct.pack('<dd',*xy)for xy in ring));coordinates+=len(ring);rings+=1
            for poly in shapely.get_parts(shapely.from_wkb(row['geometry'])):
                for ring in [poly.exterior,*poly.interiors]:after.append(b''.join(struct.pack('<dd',*xy)for xy in ring.coords))
            self.assertCountEqual(before,after)
        self.assertEqual((rings,coordinates),(106,404106));self.assertNotIn('id',actual[0]);self.assertNotIn('source_file',actual[0]);s.verify_outputs(self.candidate,self.table)
        with sqlite3.connect(self.candidate/'geopackage/epa-regions.gpkg')as db:
            fields={r[1]:r for r in db.execute('PRAGMA table_info(epa_regions)')}
            self.assertEqual(fields['FID'][2],'MEDIUMINT');self.assertEqual(fields['FID'][3],1);self.assertEqual(fields['OBJECTID'][2],'MEDIUMINT');self.assertEqual(fields['EPAREGION'][2],'TEXT(50)')
        lineage=[json.loads(x)for x in(self.candidate/'metadata/source_record_lineage.jsonl').read_bytes().splitlines()];self.assertEqual(len(lineage),10);self.assertEqual(lineage[0]['original_FID'],1);self.assertEqual(lineage[0]['original_EPAREGION'],'EPA Region 2')
    def test_idempotent_reuse_keeps_original_candidate_witnesses_and_raw(self):
        before=self.pins(self.candidate);state=self.pins(self.path/'_construction-state');raw=self.pins(self.path/'_raw-captures');self.assertTrue(self.refresh()['reused']);self.assertEqual(before,self.pins(self.candidate));self.assertEqual(state,self.pins(self.path/'_construction-state'));self.assertEqual(raw,self.pins(self.path/'_raw-captures'))
    def test_declaration_json_types_and_before_after_metadata_drift_fail(self):
        changes=[lambda m:m['fields'][0].update(nullable=0),lambda m:m['fields'][0].update(nullable=0.0),lambda m:m['fields'][2].update(length=50.0),lambda m:m.update(id=False),lambda m:m['extent']['spatialReference'].update(latestWkid=3857.0),lambda m:m.update(maxRecordCount=float(m['maxRecordCount'])),lambda m:m.update(minScale=False)]
        for change in changes:
            c=copy.deepcopy(self.captures);mutate(c,'metadata_after',change)
            with self.subTest(change=change),self.assertRaises(ValueError):s.inspect_captures(c)
    def test_count_ids_authority_or_field_drift_fail_before_any_candidate(self):
        cases=[('count_after',lambda x:x.update(count=9)),('count_after',lambda x:x.update(count=10.0)),('ids_after',lambda x:x['objectIds'].pop()),('ids_before',lambda x:x['objectIds'].__setitem__(0,True)),('source_item',lambda x:x.update(owner='unverified_other_account')),('official_webmap',lambda x:x.clear()),('source_service',lambda x:x['layers'][0].update(name='Other layer'))]
        for name,change in cases:
            c=copy.deepcopy(self.captures);mutate(c,name,change)
            with self.subTest(name=name),self.assertRaises(ValueError):s.inspect_captures(c)
    def test_missing_duplicate_unrequested_or_type_changed_source_ids_fail(self):
        changes=[lambda x:x['features'].pop(),lambda x:x['features'].append(x['features'][0]),lambda x:x['features'][0]['attributes'].update(FID=999),lambda x:x['features'][0]['attributes'].update(FID=True),lambda x:x['features'][0]['attributes'].update(OBJECTID=2),lambda x:x['features'][0]['attributes'].update(renamed=1),lambda x:x.update(exceededTransferLimit=True),lambda x:x.update(exceededTransferLimit=1)]
        for change in changes:
            c=copy.deepcopy(self.captures);mutate(c,'page_00000',change)
            with self.subTest(change=change),self.assertRaises(ValueError):s.inspect_captures(c)
    def test_new_valid_source_precision_or_coordinate_changes_quarantine(self):
        for change in [lambda x:x['features'][0]['attributes'].update(Shape_Leng=40.5),lambda x:x['features'][0]['geometry']['rings'][0][1].__setitem__(0,x['features'][0]['geometry']['rings'][0][1][0]+0.0001)]:
            c=copy.deepcopy(self.captures);mutate(c,'page_00000',change)
            with self.assertRaisesRegex(ValueError,'snapshot|invalid'):s.inspect_captures(c)
    def test_numeric_and_literal_string_physical_boundaries(self):
        for index,value in [(0,True),(0,2**31),(1,1.0),(2,123),(3,float('nan')),(3,2**53+1)]:
            with self.subTest(index=index),self.assertRaises(ValueError):s.physical_value(s.FIELD_DEFINITIONS[index],value)
        self.assertEqual(s.physical_value(s.FIELD_DEFINITIONS[2],' original '),' original ')
    def test_unknown_metadata_whole_candidate_preserved_and_original_rebuild(self):
        (self.candidate/'metadata/unknown.json').write_bytes(b'{"approved":true}');before=self.pins(self.candidate)
        with self.assertRaisesRegex(ValueError,'invalid_candidate_detached_intact'):self.refresh()
        self.assertFalse(self.candidate.exists());q=next(p for p in (self.path/'_quarantine'/s.SLUG).glob('invalid-candidate-*') if p.is_dir());self.assertEqual(before,self.pins(q));self.assertFalse(self.refresh()['reused']);self.assertEqual(self.prior.read_bytes(),b'prior fixture stays unchanged')
    def test_missing_all_witnesses_negative_journal_survives_cold_retries(self):
        record=self.record();backup={p.relative_to(record.parent):p.read_bytes()for p in record.parent.rglob('*')if p.is_file()};shutil.rmtree(record.parent)
        with self.assertRaisesRegex(ValueError,'observed_original_trust_loss'):self.refresh()
        self.assertFalse(self.candidate.exists());incident=self.path/s.incident_directory(self.version);self.assertTrue(incident.exists())
        for _ in range(2):
            with self.assertRaisesRegex(ValueError,'permanent_observed_original_trust_loss'):self.refresh()
        for name,body in backup.items():p=record.parent/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
        with self.assertRaisesRegex(ValueError,'permanent_observed_original_trust_loss'):self.refresh()
        incident.rename(self.root/'operator-retained-incident');self.assertFalse(self.refresh()['reused']);self.assertTrue(self.refresh()['reused']);self.assertEqual(self.prior.read_bytes(),b'prior fixture stays unchanged')
    def test_empty_partial_incident_and_failed_journal_preserve_candidate(self):
        original=self.record().read_bytes();self.record().unlink();before=self.pins(self.candidate)
        with patch.object(s,'record_observed_loss',side_effect=OSError('synthetic_journal_failure')),self.assertRaisesRegex(s.IncidentWriteError,'candidate_preserved'):self.refresh()
        self.assertEqual(before,self.pins(self.candidate));self.record().write_bytes(original);directory=self.path/s.incident_directory(self.version);directory.mkdir()
        for _ in range(2):
            with self.assertRaisesRegex(ValueError,'permanent_observed_original_trust_loss'):self.refresh()
            self.assertEqual(before,self.pins(self.candidate))
    def test_original_alias_coforge_fails_and_negative_incident_retained(self):
        record=self.record();value=json.loads(record.read_bytes());value['payload']['manifest']['code_commit']='0'*40;record.write_bytes(s.encode(value))
        with self.assertRaisesRegex(ValueError,'observed_original_trust_loss:construction_noncanonical_record_body'):self.refresh()
        self.assertFalse(self.candidate.exists())
        with self.assertRaisesRegex(ValueError,'permanent_observed_original_trust_loss'):self.refresh()
    def test_complete_private_failure_leaves_no_trust_or_candidate(self):
        out=self.root/'new-staging';storage=s.StagingStorageResource(local_dir=str(out))
        with patch.object(s,'write_gpkg',side_effect=OSError('synthetic_private_write_failure')),self.assertRaisesRegex(ValueError,'private_write_failure'):stage(storage,self.inputs)
        self.assertFalse((out/'_construction-state').exists());self.assertFalse(list(out.glob(s.SLUG+'/'+s.SLUG+'/v*')));self.assertEqual(len(list((out/'_raw-captures').glob('*/*/*.body'))),30)
    def test_original_claims_validate_before_any_first_seal(self):
        manifest=json.loads((self.candidate/'metadata/source_manifest.json').read_bytes());identity=s.processing_identity()
        for field,value in [('title','forged title'),('code_commit','0'*40)]:
            target=self.root/('preseal-'+field);storage=s.StagingStorageResource(local_dir=str(target));s.local_only(storage);bad=copy.deepcopy(manifest);bad[field]=value
            with self.assertRaisesRegex(ValueError,'preseal_full_original_manifest|construction_invalid_commit'):s.original_bundle(storage,'vtest',self.captures,identity,bad,create=True)
            self.assertFalse((target/'_construction-state').exists())
    def test_common_lock_covers_cache_verification(self):
        original=s.reconcile_cache
        def verify(*args,**kwargs):
            lock=self.candidate.parent/('.'+self.version+'.lock')
            with lock.open('a+b')as other:
                with self.assertRaises(BlockingIOError):fcntl.flock(other,fcntl.LOCK_EX|fcntl.LOCK_NB)
            return original(*args,**kwargs)
        with patch.object(s,'reconcile_cache',side_effect=verify):self.assertTrue(self.refresh()['reused'])

class EpaRegionsStructuralTests(unittest.TestCase):
    def test_small_real_schema_and_strict_json_types(self):
        meta=json.loads((FIXTURE/'metadata.json').read_bytes());s.validate_metadata(meta)
        for change in [lambda m:m.update(id=False),lambda m:m.update(maxRecordCount=float(m['maxRecordCount'])),lambda m:m['fields'][0].update(nullable=0),lambda m:m['fields'][2].update(length=50.0),lambda m:m['extent']['spatialReference'].update(latestWkid=3857.0)]:
            changed=copy.deepcopy(meta);change(changed)
            with self.assertRaises(ValueError):s.validate_metadata(changed)
    def test_small_full_count_and_identifier_witnesses(self):
        self.assertEqual(json.loads((FIXTURE/'count.json').read_bytes()),{'count':10});ids=json.loads((FIXTURE/'ids.json').read_bytes());self.assertEqual(s.source_ids(ids),list(range(1,11)))
        for value in (True,1.0,None):
            bad=copy.deepcopy(ids);bad['objectIds'][bad['objectIds'].index(1)]=value
            with self.assertRaises(ValueError):s.source_ids(bad)
    def test_literal_types_and_source_promotion_policy(self):
        self.assertEqual(s.physical_value(s.FIELD_DEFINITIONS[2],' original '),' original ')
        for index,value in [(0,True),(0,2**31),(1,1.0),(3,2**53+1),(3,float('nan'))]:
            with self.assertRaises(ValueError):s.physical_value(s.FIELD_DEFINITIONS[index],value)
        self.assertEqual(s.INITIAL_COUNT,10);self.assertEqual(len(s.FIELD_DEFINITIONS),6)

class EpaRegionsTransportTests(unittest.TestCase):
    def test_complete_body_and_exact_bad_receipt_preserved_before_parse(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);inputs=root/'inputs';inputs.mkdir();transport_fixture(inputs);name='official_application';original=(inputs/(name+'.body')).read_bytes();bad=b'{malformed receipt\n';(inputs/(name+'.receipt.json')).write_bytes(bad);storage=s.StagingStorageResource(local_dir=str(root/'staging'))
            with self.assertRaises(ValueError):s.acquire(storage,inputs)
            depot=root/'staging/_raw-captures'/s.SLUG/s.digest(original);self.assertEqual((depot/(name+'.body')).read_bytes(),original);self.assertEqual(next(depot.glob('*.raw-receipt-*.body')).read_bytes(),bad);self.assertFalse((root/'staging/_construction-state').exists())
    def test_genuine_unreadable_first_receipt_keeps_body_only_diagnostic(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);inputs=root/'inputs';inputs.mkdir();transport_fixture(inputs);receipt=inputs/'official_application.receipt.json';receipt.chmod(0);original=(inputs/'official_application.body').read_bytes()
            try:
                with self.assertRaises((ValueError,PermissionError)):s.acquire(s.StagingStorageResource(local_dir=str(root/'staging')),inputs)
            finally:receipt.chmod(0o600)
            body=root/'staging/_raw-captures'/s.SLUG/s.digest(original)/'official_application.body';self.assertEqual(body.read_bytes(),original)
            attempted=json.loads(next((root/'staging/_quarantine').rglob('attempted-inputs.json')).read_bytes());self.assertFalse(attempted['objects']['official_application']['raw_receipt_read'])
    def test_real_HTTPX_over_limit_consumed_bytes_are_preserved(self):
        with tempfile.TemporaryDirectory()as tmp:
            body=b'x'*37;real=httpx.Client
            def response(request):return httpx.Response(200,headers={'Content-Length':'37'},stream=httpx.ByteStream(body))
            storage=s.StagingStorageResource(local_dir=tmp)
            with patch.object(s,'MAX_RESPONSE_BYTES',36),patch.object(s.httpx,'Client',side_effect=lambda **kw:real(transport=httpx.MockTransport(response),**kw)),self.assertRaisesRegex(ValueError,'bounded_response_size_exceeded'):s.acquire(storage)
            self.assertEqual(next(Path(tmp).glob('_raw-captures/*/*/official_application.body')).read_bytes(),body);receipt=json.loads(next(Path(tmp).glob('_quarantine/**/official_application.receipt.json')).read_bytes());self.assertFalse(receipt['complete_body']);self.assertEqual(receipt['bytes'],37)
    def test_floor_before_next_read_keeps_every_already_consumed_chunk(self):
        with tempfile.TemporaryDirectory()as tmp:
            real=httpx.Client;seen=[];first=b'x'*65536
            class Stream(httpx.SyncByteStream):
                def __iter__(self):
                    seen.append(1);yield first
                    seen.append(2);yield b'y'*65536
            def response(request):return httpx.Response(200,stream=Stream())
            free=[SimpleNamespace(free=s.SPACE_FLOOR+1)]*2+[SimpleNamespace(free=s.SPACE_FLOOR-1)]*10
            with patch.object(s.shutil,'disk_usage',side_effect=free),patch.object(s.httpx,'Client',side_effect=lambda **kw:real(transport=httpx.MockTransport(response),**kw)),self.assertRaisesRegex(ValueError,'disk_floor_before_next_read'):s.acquire(s.StagingStorageResource(local_dir=tmp))
            self.assertEqual(seen,[1]);self.assertEqual(next(Path(tmp).glob('_raw-captures/*/*/official_application.body')).read_bytes(),first)
    def test_encoded_wire_completeness_and_unsafe_storage_refusal(self):
        raw=b'{"complete":true}';encoded=gzip.compress(raw);real=httpx.Client
        def response(request):return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(encoded))},stream=httpx.ByteStream(encoded))
        with patch.object(s.httpx,'Client',side_effect=lambda **kw:real(transport=httpx.MockTransport(response),**kw)):
            body,receipt=s.fetch_one(s.BASE);self.assertEqual(body,raw);self.assertEqual(receipt['wire_bytes_downloaded'],len(encoded))
        for storage in [s.PublishedStorageResource(local_dir='/tmp'),s.StagingStorageResource(local_dir='/tmp',bucket='production'),s.StagingStorageResource(local_dir='/tmp',prefix='alias')]:
            with patch.object(s,'fetch_one')as fetch,self.assertRaises(ValueError):s.stage_epa_regions(storage)
            fetch.assert_not_called()

if __name__=='__main__':unittest.main()
