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
from dagster_hifld.sources import census_latest
from dagster_hifld.sources import census_designated_places as c
from dagster_hifld.resources import StagingStorageResource, PublishedStorageResource

TIGER_ROOT='https://www2.census.gov/geo/tiger/'
TGRGDB25_DIR=TIGER_ROOT+'TGRGDB25/'
TGRGDB24_DIR=TIGER_ROOT+'TGRGDB24/'
ZIP_URL=TGRGDB25_DIR+'tlgdb_2025_a_us_substategeo.gdb.zip'
ZIP_URL_24=TGRGDB24_DIR+'tlgdb_2024_a_us_substategeo.gdb.zip'
ANNUAL_LISTING='<a href="TGRGDB25/">25</a>'
ANNUAL_LISTING_BOTH='<a href="TGRGDB25/">25</a><a href="TGRGDB24/">24</a>'
DIR25_LISTING='<a href="tlgdb_2025_a_us_substategeo.gdb.zip">us</a>'
DIR24_LISTING='<a href="tlgdb_2024_a_us_substategeo.gdb.zip">us</a>'
# 54 state/territory FIPS codes (incl. territories 66/72/78) spanning a plausible
# national CDP population inside the module's count range.
FIXTURE_STATES=['01','02','04','05','06','08','09','10','11','12','13','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','44','45','46','47','48','49','50','51','53','54','55','56','66','72','78']
FIXTURE_TOTAL=12884

def discovery_transport(body_handler,annual_listing=ANNUAL_LISTING):
    listings={TIGER_ROOT:annual_listing,TGRGDB25_DIR:DIR25_LISTING,TGRGDB24_DIR:DIR24_LISTING}
    def respond(request):
        url=str(request.url)
        if url in listings:return httpx.Response(200,stream=httpx.ByteStream(listings[url].encode()),request=request)
        if url==ZIP_URL:return body_handler(request)
        if url==ZIP_URL_24:return httpx.Response(200,stream=httpx.ByteStream(b'older-edition-bytes'),request=request)
        return httpx.Response(404,stream=httpx.ByteStream(b''),request=request)
    return httpx.MockTransport(respond)

class CensusCDPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        polygon=shapely.to_wkb(shapely.MultiPolygon([shapely.Polygon([(0,0),(1,0),(1,1),(0,0)])]))
        base=FIXTURE_TOTAL//len(FIXTURE_STATES);rem=FIXTURE_TOTAL%len(FIXTURE_STATES)
        counts={s:(base+(1 if i<rem else 0)) for i,s in enumerate(FIXTURE_STATES)}
        rows=[]
        for state,count in counts.items():
            for n in range(count):
                geoid=state+str(n).zfill(5)
                rows.append({'OBJECTID':len(rows)+1,'PLACENS':str(len(rows)).zfill(8),'GEOID':geoid,'GEOIDFQ':'1600000US'+geoid,'NAMELSAD':'Fixture CDP','CLASSFP':'U1','FUNCSTAT':'S','ALAND':1.,'AWATER':0.,'INTPTLAT':'+00.0000000','INTPTLON':'-000.0000000','SHAPE':polygon})
        cls.table=pa.Table.from_pylist(rows,schema=pa.schema(zip(c.FIELDS,c.TYPES)))
        cls.metadata={'crs':'EPSG:4269','geometry_type':'MultiPolygon','fid_column':'OBJECTID'}

    def test_complete_fixture_and_leading_identifiers(self):
        c.validate_table(self.table,self.metadata)
        self.assertEqual(self.table['GEOID'][0].as_py(),'0100000')
        self.assertEqual(self.table.num_rows,FIXTURE_TOTAL)
        self.assertTrue(c.FEATURE_COUNT_RANGE[0]<=self.table.num_rows<=c.FEATURE_COUNT_RANGE[1])

    def test_incomplete_or_unexpected_schema_and_physical_type(self):
        n=self.table.num_rows
        variants=[self.table.slice(0,c.FEATURE_COUNT_RANGE[0]-1),self.table.rename_columns(['renamed',*c.FIELDS[1:]]),self.table.append_column('extra',pa.array([1]*n)),self.table.set_column(0,'OBJECTID',self.table['OBJECTID'].cast(pa.int32()))]
        for table in variants:
            with self.subTest(schema=str(table.schema)),self.assertRaises(c.CDPQuarantineError):c.validate_table(table,self.metadata)

    def test_missing_duplicate_or_changed_source_id(self):
        for field,value in [('OBJECTID',2),('GEOID','0100001'),('PLACENS','1'),('GEOIDFQ','0100000'),('CLASSFP','C1')]:
            values=self.table[field].to_pylist();values[0]=value
            table=self.table.set_column(self.table.schema.get_field_index(field),field,pa.array(values,type=self.table[field].type))
            with self.subTest(field=field),self.assertRaises(c.CDPQuarantineError):c.validate_table(table,self.metadata)

    def test_crs_null_geometry_and_population_range_drift(self):
        with self.assertRaises(c.CDPQuarantineError):c.validate_table(self.table,self.metadata|{'crs':'EPSG:4326'})
        values=self.table['SHAPE'].to_pylist();values[0]=None
        table=self.table.set_column(11,'SHAPE',pa.array(values,type=pa.binary()))
        with self.assertRaises(c.CDPQuarantineError):c.validate_table(table,self.metadata)
        with patch.object(c,'FEATURE_COUNT_RANGE',(FIXTURE_TOTAL+1,10**9)),self.assertRaises(c.CDPQuarantineError):c.validate_table(self.table,self.metadata)

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
        body=b'complete-fixture-original';encoded=gzip.compress(body,mtime=0);real_client=httpx.Client
        def body_handler(request):
            self.assertEqual(request.headers['accept-encoding'],'identity')
            return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(encoded))},stream=httpx.ByteStream(encoded),request=request)
        with tempfile.TemporaryDirectory() as tmp,patch.object(c.httpx,'Client',side_effect=lambda **kwargs:real_client(transport=discovery_transport(body_handler),**kwargs)):
            storage=StagingStorageResource(local_dir=tmp,use_local=True)
            path,receipt,release=c.acquire(storage)
            self.assertEqual(path.read_bytes(),body);self.assertEqual(receipt['wire_bytes_downloaded'],len(encoded));self.assertTrue(receipt['complete_body'])
            self.assertEqual(release.vintage,2025);self.assertEqual(path.name,'original.gdb.zip');self.assertEqual(receipt['url'],ZIP_URL)

    def test_newer_published_vintage_selected_over_older(self):
        body=b'complete-fixture-original';real_client=httpx.Client
        fetched_zip=[]
        def body_handler(request):
            fetched_zip.append(str(request.url))
            return httpx.Response(200,stream=httpx.ByteStream(body),request=request)
        with tempfile.TemporaryDirectory() as tmp,patch.object(c.httpx,'Client',side_effect=lambda **kwargs:real_client(transport=discovery_transport(body_handler,annual_listing=ANNUAL_LISTING_BOTH),**kwargs)):
            storage=StagingStorageResource(local_dir=tmp,use_local=True)
            path,receipt,release=c.acquire(storage)
            self.assertEqual(release.vintage,2025);self.assertEqual(receipt['url'],ZIP_URL)
            # Only the newer (2025) archive is fetched; the older edition is never downloaded.
            self.assertEqual(fetched_zip,[ZIP_URL])

    def test_short_or_failed_transport_preserves_partial(self):
        body=b'incomplete';real_client=httpx.Client
        def body_handler(request):return httpx.Response(200,headers={'Content-Length':str(len(body)+1)},stream=httpx.ByteStream(body),request=request)
        with tempfile.TemporaryDirectory() as tmp,patch.object(c.httpx,'Client',side_effect=lambda **kwargs:real_client(transport=discovery_transport(body_handler),**kwargs)):
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
                c.construction_bundle(storage,'v0.0.0-draft',Path(tmp)/'not-read',{}, {'files':{}},{},'_raw-captures/census-designated-places/deadbeef/original.gdb.zip',create=True)
            self.assertFalse(storage.object_exists(marker.removesuffix('.first-construction.json')))

    def test_first_construction_rejects_mismatched_identity_before_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp,use_local=True)
            head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(c.__file__).parents[3],text=True).strip()
            identity={'code_identity_scheme':'git-commit-blobs-v1','files':{'src/dagster_hifld/construction_provenance.py':'0'*64}}
            manifest={'code_commit':head,'code_identity_scheme':identity['code_identity_scheme']}
            with self.assertRaisesRegex(ValueError,'construction_processing_blob_hash_mismatch'):
                c.construction_bundle(storage,'v0.0.0-draft',Path(tmp)/'not-read',{},identity,manifest,'_raw-captures/census-designated-places/deadbeef/original.gdb.zip',create=True)
            self.assertTrue(Path(tmp).is_dir())
            self.assertEqual(list(Path(tmp).rglob('*.json')),[])

    def test_real_full_original_when_explicitly_requested(self):
        path=os.environ.get('CDP_TEST_ARCHIVE');receipt_path=os.environ.get('CDP_TEST_RECEIPT')
        if not path or not receipt_path:self.skipTest('set CDP_TEST_ARCHIVE and CDP_TEST_RECEIPT to a retained complete original capture')
        receipt=c.strict_json(Path(receipt_path).read_bytes())
        release=census_latest.geodatabase_release_from_receipt(receipt,c.GEODATABASE_PRODUCT)
        table,audit=c.inspect_archive(Path(path),release)
        self.assertTrue(c.FEATURE_COUNT_RANGE[0]<=table.num_rows<=c.FEATURE_COUNT_RANGE[1]);self.assertEqual(audit['invalid_geometries'],0)
        self.assertEqual(table['GEOID'][0].as_py(),'0100100');self.assertEqual(table['PLACENS'][0].as_py(),'02582661')
        self.assertTrue(set(table['CLASSFP'].to_pylist())<= {'U1','U2','M2'})

if __name__=='__main__':unittest.main()
