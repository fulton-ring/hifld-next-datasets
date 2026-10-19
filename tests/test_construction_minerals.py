"""Complete preserved candidate fixtures; no source/contract acceptance is granted."""
import copy
import json
import os
import shutil
import struct
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import pyarrow as pa
from dagster_hifld import construction_state
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import construction_minerals as s

ARCHIVE=Path(__file__).parents[1]/'docs/dataset-sources/construction-minerals-operations/evidence/complete-candidate-captures.tar.gz'

class MineralsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.package_tmp=tempfile.TemporaryDirectory();cls.package=Path(cls.package_tmp.name)
        with tarfile.open(ARCHIVE,'r:gz') as archive:
            for m in archive:
                if not m.isfile() or Path(m.name).name!=m.name:raise ValueError('unsafe test archive')
                (cls.package/m.name).write_bytes(archive.extractfile(m).read())
    @classmethod
    def tearDownClass(cls):cls.package_tmp.cleanup()
    def captures(self):return {n:((self.package/(n+'.body')).read_bytes(),s.strict_json((self.package/(n+'.receipt.json')).read_bytes())) for n in s.PINS}
    def stage(self,storage):return s.stage_construction_minerals(storage,capture_dir=str(self.package),package_id=s.PACKAGE)
    def state(self,root,result):return Path(root)/s.construction_record_key(result['version'])

    def test_full766_native_values_types_ids_geometry_and_conflicting_metadata(self):
        captures=self.captures();table,ids,audit=s.inspect_captures(captures)
        self.assertEqual(table.num_rows,766);self.assertEqual(ids.num_rows,766);self.assertEqual(audit['states'],46)
        self.assertEqual(table['MINOP2X020'].type,pa.decimal128(3,0));self.assertEqual(table['LATITUDE'].type,pa.decimal128(9,3));self.assertEqual(table['LONGITUDE'].type,pa.decimal128(11,3))
        self.assertEqual(table['source_fid'].to_pylist(),list(range(766)));self.assertEqual(table['source_shp_record_number'].to_pylist(),list(range(1,767)))
        self.assertEqual(table['SITE_NAME'][0].as_py(),'Nichols Mine');self.assertIn('WGS_1984',audit['native_crs_claim']);self.assertIn('North American',audit['contradictory_original_USGS_metadata_datum'])
        raw=captures['minop2x020.shp'][0]
        for i,wkb in enumerate(table['geometry'].to_pylist()):
            point=s.shapely.from_wkb(wkb);self.assertEqual((point.x,point.y),struct.unpack_from('<dd',raw,100+i*28+12))
        self.assertEqual(ids['published_shapefile_fid'].to_pylist(),[int(x)-1 for x in ids['published_geopackage_storage_fid'].to_pylist()])
        self.assertNotEqual(table['source_fid'].to_pylist(),ids['published_shapefile_fid'].to_pylist())

    def test_explicit_package_required_and_remote_or_linked_storage_rejected_before_acquire(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError,'explicit_unapproved'):s.acquire(StagingStorageResource(local_dir=tmp),None,None)
            for resource in [PublishedStorageResource(local_dir=tmp),StagingStorageResource(bucket='forbidden',use_local=False),StagingStorageResource(local_dir=tmp+'/../unsafe')]:
                with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):s.stage_construction_minerals(resource,capture_dir=str(self.package),package_id=s.PACKAGE)
                acquire.assert_not_called()
            root=Path(tmp)/'linked';root.symlink_to(self.package,target_is_directory=True)
            with self.assertRaisesRegex(ValueError,'aliased'):self.stage(StagingStorageResource(local_dir=str(root)))

    def test_missing_renamed_extra_types_duplicate_ids_and_record_counts_fail(self):
        original=self.captures()['minop2x020.dbf'][0]
        changes=[]
        a=bytearray(original);a[32:43]=b'RENAMED\0\0\0\0';changes.append(a)
        a=bytearray(original);a[48]=4;changes.append(a)
        a=bytearray(original);a[321+182+1:321+182+4]=b'  1';changes.append(a)
        a=bytearray(original);a[321]=ord('*');changes.append(a)
        changes.extend([original[:-2],original+b'x'])
        for body in changes:
            with self.subTest(case=len(body)),self.assertRaises(ValueError):s.decode_dbf(body)

    def test_raw_physical_decimal_scale_and_character_bytes_not_coerced(self):
        original=self.captures()['minop2x020.dbf'][0]
        a=bytearray(original);a[321+1+3]=0xE9
        with self.assertRaisesRegex(ValueError,'nonASCII'):s.decode_dbf(a)
        a=bytearray(original);lat=321+1+3+21+46+51+14+16;a[lat:lat+9]=b'   36.828';self.assertEqual(s.decode_dbf(a)[0]['LATITUDE'].as_tuple().exponent,-3)
        a[lat:lat+9]=b'     36.8'
        with self.assertRaisesRegex(ValueError,'scale'):s.decode_dbf(a)

    def test_native_geometry_errors_missing_records_wrong_index_and_nonfinite_fail(self):
        c=self.captures();body=c['minop2x020.shp'][0];index=c['minop2x020.shx'][0]
        for which in ['truncated','index','nan','extra']:
            b=bytearray(body);i=bytearray(index)
            if which=='truncated':b=b[:-28]
            elif which=='index':struct.pack_into('>i',i,100,51)
            elif which=='nan':struct.pack_into('<d',b,112,float('nan'))
            else:b+=b'junk'
            with self.subTest(which=which),self.assertRaises(ValueError):s.decode_points(b,i)

    def test_pinned_drift_incomplete_receipts_and_extra_package_quarantine(self):
        with tempfile.TemporaryDirectory() as tmp:
            package=Path(tmp)/'input';shutil.copytree(self.package,package);storage=StagingStorageResource(local_dir=tmp+'/stage')
            target=package/'minop2x020.shp.body';target.write_bytes(target.read_bytes()[:-1])
            with self.assertRaisesRegex(ValueError,'capture_url_status_length'):s.acquire(storage,str(package),s.PACKAGE)
            self.assertTrue(list((Path(tmp)/'stage/_quarantine').rglob('minop2x020.shp.body')))
            target.write_bytes((self.package/target.name).read_bytes());(package/'extra').write_text('drift')
            with self.assertRaisesRegex(ValueError,'extra_explicit_package_roster'):s.acquire(storage,str(package),s.PACKAGE)

    def test_complete_reuse_and_published_id_or_ready_coforge_quarantines_whole_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);result=self.stage(storage);self.assertFalse(result['reused']);self.assertTrue(self.stage(storage)['reused'])
            root=Path(tmp)/s.SLUG/s.SLUG/result['version'];target=root/'candidate/published-identifiers.parquet';target.write_bytes(b'forged');ready=s.strict_json((root/s.READY).read_bytes());ready['objects']['candidate/published-identifiers.parquet']=s.digest(b'forged');(root/s.READY).write_bytes(s.encode(ready))
            with self.assertRaisesRegex(ValueError,'detached_intact'):self.stage(storage)
            self.assertFalse(root.exists());self.assertTrue(list((Path(tmp)/'_quarantine').rglob('published-identifiers.parquet')))
            # Trusted original state reconstructs original bytes, not forged cache.
            self.assertFalse(self.stage(storage)['reused'])

    def test_native_parquet_metadata_forgery_with_coforged_ready_does_not_rebase(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);result=self.stage(storage);root=Path(tmp)/s.SLUG/s.SLUG/result['version'];target=root/f'geoparquet/{s.SLUG}.parquet'
            table=s.pq.read_table(target);target.write_bytes(s.parquet_bytes(table.replace_schema_metadata({b'geo':b'{}'})));ready=s.strict_json((root/s.READY).read_bytes());ready['objects'][str(target.relative_to(root))]=s.sha256_file(target);(root/s.READY).write_bytes(s.encode(ready))
            before=self.state(tmp,result).read_bytes()
            with self.assertRaisesRegex(ValueError,'detached_intact'):self.stage(storage)
            self.assertEqual(self.state(tmp,result).read_bytes(),before)

    def test_original_receipt_coforge_and_repeated_missing_state_require_exact_restore(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);result=self.stage(storage);alias=self.state(tmp,result);original=alias.read_bytes();bundle=json.loads(original)
            receipt=bundle['payload']['captures']['minop2x020.dbf']['receipt'];receipt['observed_at']='2026-10-19T00:00:00+00:00'
            for item in bundle['payload']['manifest']['inputs']:
                if item['input_id'].startswith('minop2x020.dbf:'):item['retrieved_at']=receipt['observed_at']
            alias.write_bytes(s.canonical_construction_bundle(result['version'],bundle['processing_fingerprint'],bundle['payload']))
            for _ in range(2):
                with self.assertRaisesRegex(ValueError,'construction_marker_mismatch'):self.stage(storage)
            alias.write_bytes(original);self.assertFalse(self.stage(storage)['reused'])
            alias.unlink()
            for _ in range(2):
                with self.assertRaisesRegex(ValueError,'construction_missing_original_state'):self.stage(storage)
            alias.write_bytes(original);self.assertFalse(self.stage(storage)['reused'])

    def test_first_marker_failure_leaves_CAS_and_repeated_retry_cannot_bootstrap(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);real=construction_state._install_once
            def interrupt(root,key,body):
                if key.endswith('.first-construction.json'):raise OSError('injected marker fault')
                return real(root,key,body)
            with patch.object(construction_state,'_install_once',side_effect=interrupt),self.assertRaisesRegex(ValueError,'state_io_failed'):self.stage(storage)
            self.assertEqual(len(list((Path(tmp)/'_construction-state').rglob('*.evidence/*.json'))),1)
            for _ in range(2):
                with self.assertRaisesRegex(ValueError,'missing_original_state'):self.stage(storage)
            self.assertFalse(list((Path(tmp)/s.SLUG).rglob(s.READY)))

    def test_invalid_git_claim_and_private_writer_failure_never_seal(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp)
            with patch.object(s,'verify_outputs',side_effect=ValueError('private verification failed')),self.assertRaisesRegex(ValueError,'private verification failed'):self.stage(storage)
            self.assertFalse((Path(tmp)/'_construction-state').exists())
            real=s.manifest_for
            def wrong(*args,**kwargs):return real(*args,**kwargs)|{'code_commit':'0'*40}
            with patch.object(s,'manifest_for',side_effect=wrong),self.assertRaisesRegex(ValueError,'construction_invalid_commit'):self.stage(storage)
            self.assertFalse((Path(tmp)/'_construction-state').exists())

    def test_unknown_catalog_metadata_whole_quarantine_and_original_raw_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);result=self.stage(storage);root=Path(tmp)/s.SLUG/s.SLUG/result['version'];raw={str(p):s.sha256_file(p) for p in (Path(tmp)/'_raw-captures').rglob('*') if p.is_file()}
            (root/'metadata/extra.json').write_bytes(b'{}')
            with self.assertRaisesRegex(ValueError,'unknown_missing_or_linked'):self.stage(storage)
            self.assertTrue(all(s.sha256_file(Path(p))==sha for p,sha in raw.items()))
