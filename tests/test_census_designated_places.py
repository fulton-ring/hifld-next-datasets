"""Behavior fixtures are implementation tests, never dataset acceptance."""
import gzip
import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import fcntl
import httpx
import pyarrow as pa
import shapely
from dagster_hifld.sources import census_designated_places as c
from dagster_hifld.resources import StagingStorageResource, PublishedStorageResource

class CensusCDPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        polygon=shapely.to_wkb(shapely.MultiPolygon([shapely.Polygon([(0,0),(1,0),(1,1),(0,0)])]))
        rows=[]
        for state,count in c.STATE_COUNTS.items():
            for n in range(count):
                geoid=state+str(n).zfill(5)
                rows.append({'OBJECTID':len(rows)+1,'PLACENS':str(len(rows)).zfill(8),'GEOID':geoid,'GEOIDFQ':'1600000US'+geoid,'NAMELSAD':'Fixture CDP','CLASSFP':'U1','FUNCSTAT':'S','ALAND':1.,'AWATER':0.,'INTPTLAT':'+00.0000000','INTPTLON':'-000.0000000','SHAPE':polygon})
        cls.table=pa.Table.from_pylist(rows,schema=pa.schema(zip(c.FIELDS,c.TYPES)))
        cls.metadata={'crs':'EPSG:4269','geometry_type':'MultiPolygon','fid_column':'OBJECTID'}

    def test_complete_fixture_and_leading_identifiers(self):
        c.validate_table(self.table,self.metadata)
        self.assertEqual(self.table['GEOID'][0].as_py(),'0100000')
        self.assertIn('66',c.STATE_COUNTS);self.assertIn('72',c.STATE_COUNTS);self.assertIn('78',c.STATE_COUNTS)

    def test_incomplete_or_unexpected_schema_and_physical_type(self):
        variants=[self.table.slice(0,c.COUNT-1),self.table.rename_columns(['renamed',*c.FIELDS[1:]]),self.table.append_column('extra',pa.array([1]*c.COUNT)),self.table.set_column(0,'OBJECTID',self.table['OBJECTID'].cast(pa.int32()))]
        for table in variants:
            with self.subTest(schema=str(table.schema)),self.assertRaises(c.CDPQuarantineError):c.validate_table(table,self.metadata)

    def test_missing_duplicate_or_changed_source_id(self):
        for field,value in [('OBJECTID',2),('GEOID','0100001'),('PLACENS','1'),('GEOIDFQ','0100000'),('CLASSFP','C1')]:
            values=self.table[field].to_pylist();values[0]=value
            table=self.table.set_column(self.table.schema.get_field_index(field),field,pa.array(values,type=self.table[field].type))
            with self.subTest(field=field),self.assertRaises(c.CDPQuarantineError):c.validate_table(table,self.metadata)

    def test_crs_null_geometry_and_coverage_drift(self):
        with self.assertRaises(c.CDPQuarantineError):c.validate_table(self.table,self.metadata|{'crs':'EPSG:4326'})
        values=self.table['SHAPE'].to_pylist();values[0]=None
        table=self.table.set_column(11,'SHAPE',pa.array(values,type=pa.binary()))
        with self.assertRaises(c.CDPQuarantineError):c.validate_table(table,self.metadata)
        with patch.object(c,'STATE_COUNTS',{'01':c.COUNT}),self.assertRaises(c.CDPQuarantineError):c.validate_table(self.table,self.metadata)

    def test_local_only_and_duplicate_json(self):
        with self.assertRaises(c.CDPQuarantineError):c.local_only(PublishedStorageResource(local_dir='/tmp/never-write-cdp',use_local=True))
        with self.assertRaises(c.CDPQuarantineError):c.strict_json(b'{"approved":false,"approved":true}')

    def test_lock_covers_lookup_validation_and_install(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);prefix='dataset/file/version'
            with c.candidate_lock(base,prefix) as target:
                descriptor=os.open(target.parent/('.'+target.name+'.lock'),os.O_RDWR)
                try:
                    with self.assertRaises(BlockingIOError):fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
                finally:os.close(descriptor)
            with c.candidate_lock(base,prefix):pass

    def test_gzip_transport_compares_encoded_wire_length(self):
        body=b'complete-fixture-original';encoded=gzip.compress(body);real_client=httpx.Client
        def handler(request):
            self.assertEqual(request.headers['accept-encoding'],'identity')
            return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(encoded))},stream=httpx.ByteStream(encoded))
        with tempfile.TemporaryDirectory() as tmp,patch.object(c,'BYTES',len(body)),patch.object(c,'SHA256',hashlib.sha256(body).hexdigest()),patch.object(c.httpx,'Client',side_effect=lambda **kwargs:real_client(transport=httpx.MockTransport(handler),**kwargs)):
            storage=StagingStorageResource(local_dir=tmp,use_local=True)
            path,receipt=c.acquire(storage)
            self.assertEqual(path.read_bytes(),body);self.assertEqual(receipt['wire_bytes_downloaded'],len(encoded));self.assertTrue(receipt['complete_body'])

    def test_short_or_failed_transport_preserves_partial(self):
        body=b'incomplete';real_client=httpx.Client
        def handler(request):return httpx.Response(200,headers={'Content-Length':str(len(body)+1)},stream=httpx.ByteStream(body))
        with tempfile.TemporaryDirectory() as tmp,patch.object(c,'BYTES',len(body)),patch.object(c,'SHA256',hashlib.sha256(body).hexdigest()),patch.object(c.httpx,'Client',side_effect=lambda **kwargs:real_client(transport=httpx.MockTransport(handler),**kwargs)):
            storage=StagingStorageResource(local_dir=tmp,use_local=True)
            with self.assertRaises(c.CDPQuarantineError):c.acquire(storage)
            partials=list(Path(tmp).rglob('*.partial'));self.assertEqual(len(partials),1);self.assertEqual(partials[0].read_bytes(),body)
            self.assertEqual(len(list((Path(tmp)/'_quarantine').rglob('*.json'))),1)

    def test_missing_first_binding_is_not_recreated(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp,use_local=True)
            marker=f'_source-provenance/{c.SLUG}/v0.0.0-draft/bundle.json.first-construction.json'
            c.immutable(storage,marker,b'{}')
            with self.assertRaisesRegex(c.CDPQuarantineError,'missing_original_binding_after_first_construction'):
                c.construction_bundle(storage,'v0.0.0-draft',Path(tmp)/'not-read',{}, {'files':{}},{},create=True)
            self.assertFalse(storage.object_exists(marker.removesuffix('.first-construction.json')))

    def test_first_construction_rejects_mismatched_identity_before_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp,use_local=True)
            head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(c.__file__).parents[3],text=True).strip()
            identity={'code_identity_scheme':'git-commit-blobs-v1','files':{'src/dagster_hifld/construction_provenance.py':'0'*64}}
            manifest={'code_commit':head,'code_identity_scheme':identity['code_identity_scheme']}
            with self.assertRaisesRegex(ValueError,'construction_processing_blob_hash_mismatch'):
                c.construction_bundle(storage,'v0.0.0-draft',Path(tmp)/'not-read',{},identity,manifest,create=True)
            self.assertEqual(list(Path(tmp).rglob('*.json')),[])

    def test_real_full_original_when_explicitly_requested(self):
        path=os.environ.get('CDP_TEST_ARCHIVE')
        if not path:self.skipTest('set CDP_TEST_ARCHIVE to retained complete original ZIP')
        table,audit=c.inspect_archive(Path(path));self.assertEqual(table.num_rows,12884);self.assertEqual(audit['invalid_geometries'],0)
        self.assertEqual(table['GEOID'][0].as_py(),'0100100');self.assertEqual(table['PLACENS'][0].as_py(),'02582661')

if __name__=='__main__':unittest.main()
