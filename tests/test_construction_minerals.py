"""Complete USGS successor candidate fixtures; no source/contract acceptance is granted."""
import json
import os
import shutil
import struct
import tarfile
import tempfile
import unittest
import io
import zipfile
from pathlib import Path
from unittest.mock import patch
import pyarrow as pa
from dagster_hifld import construction_state
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import construction_minerals as s

_CAPTURE_ARCHIVE_ENV='HIFLD_CONSTRUCTION_MINERALS_TEST_CAPTURE_ARCHIVE'
ARCHIVE=Path(os.environ[_CAPTURE_ARCHIVE_ENV]) if os.environ.get(_CAPTURE_ARCHIVE_ENV) else None

@unittest.skipUnless(os.environ.get(_CAPTURE_ARCHIVE_ENV), 'complete original construction-minerals captures opt-in required; see docs/dataset-sources/construction-minerals-operations/README.md')
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

    def test_full765_usgs_native_values_types_ids_geometry_successor_proof_and_published_ids(self):
        captures=self.captures();table,ids,audit,comparison=s.inspect_captures(captures)
        self.assertEqual(table.num_rows,765);self.assertEqual(ids.num_rows,766);self.assertEqual(audit['states'],46)
        self.assertEqual(table['id'].type,pa.decimal128(8,0));self.assertEqual(table['LATITUDE'].type,pa.decimal128(10,5));self.assertEqual(table['LONGITUDE'].type,pa.decimal128(10,5))
        self.assertEqual(table['source_shp_record_number'].to_pylist(),[fid+1 for fid in table['source_fid'].to_pylist()])
        self.assertEqual(len(set(table['id'].to_pylist())),765)
        self.assertIn('Milby Mine',table['SITE_NAME'].to_pylist());self.assertIn('Kershaw Quarry',table['SITE_NAME'].to_pylist())
        self.assertEqual(audit['commodities'],dict(zip(s.CONSTRUCTION_COMMODITIES,(58,115,262,154,62,24,74,16))))
        self.assertIn('North_American_1927',audit['native_crs_claim']);self.assertIn('North American',audit['contradictory_original_USGS_metadata_datum']);self.assertIn('WGS 1984',audit['atlas_preserved_prj_claim'])
        members=s.decode_mineplant_zip(captures['usgs_mineplant_zip'][0]);raw=members['mineplant.shp']
        for i,fid in enumerate(table['source_fid'].to_pylist()):
            point=s.shapely.from_wkb(table['geometry'][i].as_py())
            self.assertEqual((point.x,point.y),struct.unpack_from('<dd',raw,100+fid*28+12))
            self.assertEqual(point.y,float(table['LATITUDE'][i].as_py()));self.assertEqual(point.x,float(table['LONGITUDE'][i].as_py()))
        self.assertEqual(comparison['atlas_records'],766);self.assertEqual(comparison['usgs_successor_construction_records'],765);self.assertEqual(comparison['unmatched_atlas_records'],[])
        self.assertEqual([case['atlas_MINOP2X020_pair'] for case in comparison['atlas_duplicate_collapsed_cases']],[['539','540']])
        self.assertEqual(comparison['atlas_duplicate_collapsed_cases'][0]['usgs_mineplant_id'],'2928')
        self.assertEqual([case['atlas_MINOP2X020'] for case in comparison['company_width_truncation_cases']],['126'])
        self.assertIn('Centex Construction Products Inc)',comparison['company_width_truncation_cases'][0]['atlas_COMPANY_NA'])
        self.assertEqual(ids['published_shapefile_fid'].to_pylist(),[int(x)-1 for x in ids['published_geopackage_storage_fid'].to_pylist()])
        self.assertNotEqual(table['source_fid'].to_pylist(),ids['published_shapefile_fid'].to_pylist())

    def test_explicit_package_required_and_remote_or_linked_storage_rejected_before_acquire(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError,'explicit_unapproved'):s.acquire(StagingStorageResource(local_dir=tmp),None,None)
            with self.assertRaisesRegex(ValueError,'explicit_unapproved'):s.acquire(StagingStorageResource(local_dir=tmp),str(self.package),'usgs-mrdata-mineplant-2003-v1-wrong-selection-id')
            for resource in [PublishedStorageResource(local_dir=tmp),StagingStorageResource(bucket='forbidden',use_local=False),StagingStorageResource(local_dir=tmp+'/../unsafe')]:
                with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):s.stage_construction_minerals(resource,capture_dir=str(self.package),package_id=s.PACKAGE)
                acquire.assert_not_called()
            root=Path(tmp)/'linked';root.symlink_to(self.package,target_is_directory=True)
            with self.assertRaisesRegex(ValueError,'aliased'):self.stage(StagingStorageResource(local_dir=str(root)))

    def test_successor_fidelity_drift_fails_closed(self):
        captures=self.captures()
        real=s.decode_dbf
        def renamed(body):
            rows=real(body);rows[0]['SITE_NAME']='Renamed Facility';return rows
        with patch.object(s,'decode_dbf',side_effect=renamed),self.assertRaisesRegex(ValueError,'usgs_successor_does_not_contain_complete_assigned_dataset'):s.inspect_captures(captures)

    def test_usgs_package_roster_member_size_and_txt_crosscheck_failures(self):
        captures=self.captures();members=s.decode_mineplant_zip(captures['usgs_mineplant_zip'][0])
        def zipped(changed):
            stream=io.BytesIO()
            with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as archive:
                for name,body in changed.items():archive.writestr(name,body)
            return stream.getvalue()
        with self.assertRaisesRegex(ValueError,'member_roster'):s.decode_mineplant_zip(zipped({**members,'rogue.txt':b'x'}))
        with self.assertRaisesRegex(ValueError,'member_roster'):s.decode_mineplant_zip(zipped({('mineplant.prj' if k=='mineplant.txt' else k):v for k,v in members.items()}))
        with self.assertRaisesRegex(ValueError,'member_roster'):s.decode_mineplant_zip(zipped({**members,'mineplant.dbf':members['mineplant.dbf'][:-2]}))
        rows=s.decode_mineplant_dbf(members['mineplant.dbf']);txt=members['mineplant.txt']
        mutated=txt.replace(b'Milby Mine',b'Milby Mine X',1)
        self.assertNotEqual(mutated,txt)
        with self.assertRaisesRegex(ValueError,'txt_dbf_value_drift'):s.crosscheck_mineplant_txt(mutated,rows)

    def test_missing_renamed_extra_types_duplicate_ids_and_record_counts_fail(self):
        original=s.decode_mineplant_zip(self.captures()['usgs_mineplant_zip'][0])['mineplant.dbf']
        changes=[]
        a=bytearray(original);a[32:40]=b'IDENT\0\0';changes.append(a)
        a=bytearray(original);struct.pack_into('<I',a,4,6784);changes.append(a)
        a=bytearray(original);a[321]=ord('*');changes.append(a)
        a=bytearray(original);struct.pack_into('<8s',a,321+1,b'      29');changes.append(a)
        changes.extend([original[:-2],original+b'x'])
        for body in changes:
            with self.subTest(case=len(body)),self.assertRaises(ValueError):s.decode_mineplant_dbf(body)

    def test_raw_physical_decimal_scale_and_character_bytes_not_coerced(self):
        original=s.decode_mineplant_zip(self.captures()['usgs_mineplant_zip'][0])['mineplant.dbf']
        a=bytearray(original);a[321+1+8]=0xE9
        with self.assertRaisesRegex(ValueError,'nonASCII'):s.decode_mineplant_dbf(a)
        lat=321+1+8+30+50+50+40+40
        a=bytearray(original);a[lat:lat+10]=b'  1.234567'
        with self.assertRaisesRegex(ValueError,'scale'):s.decode_mineplant_dbf(a)
        a=bytearray(original);a[lat:lat+10]=b'      36.8'
        rows=s.decode_mineplant_dbf(a);self.assertEqual(rows[0]['LATITUDE'].as_tuple().exponent,-1);self.assertEqual(rows[0]['LATITUDE'],s.Decimal('36.8'))

    def test_native_geometry_errors_missing_records_wrong_index_and_nonfinite_fail(self):
        members=s.decode_mineplant_zip(self.captures()['usgs_mineplant_zip'][0]);body=members['mineplant.shp'];index=members['mineplant.shx']
        for which in ['truncated','index','nan','extra']:
            b=bytearray(body);i=bytearray(index)
            if which=='truncated':b=b[:-28]
            elif which=='index':struct.pack_into('>i',i,100,51)
            elif which=='nan':struct.pack_into('<d',b,112,float('nan'))
            else:b+=b'junk'
            with self.subTest(which=which),self.assertRaises(ValueError):s.decode_points(b,i,s.MINEPLANT_RECORDS)

    def test_bounded_input_reads_exact_limit_and_rejects_oversize_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'body';path.write_bytes(b'1234');self.assertEqual(s.bounded_input(path,4),b'1234')
            with self.assertRaises(ValueError):s.bounded_input(path,3)
            linked=Path(tmp)/'linked';linked.symlink_to(path)
            with self.assertRaises(OSError):s.bounded_input(linked,4)
            hard=Path(tmp)/'hard';os.link(path,hard)
            with self.assertRaises(ValueError):s.bounded_input(path,4)

    @unittest.skipIf(os.geteuid()==0,'Real POSIX unreadable-receipt test needs an ordinary user')
    def test_unreadable_receipt_preserves_already_read_body_without_inventing_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            package=Path(tmp)/'input';shutil.copytree(self.package,package);storage=StagingStorageResource(local_dir=tmp+'/stage');name='legacy_metadata';body=(package/(name+'.body')).read_bytes();receipt=package/(name+'.receipt.json');original=receipt.read_bytes();receipt.chmod(0)
            try:
                with self.assertRaisesRegex(ValueError,'Permission denied'):s.acquire(storage,str(package),s.PACKAGE)
            finally:receipt.chmod(0o600)
            self.assertEqual(receipt.read_bytes(),original);bodykey=f'_raw-captures/{s.SLUG}/{s.digest(body)}/{name}.body';self.assertEqual(storage.read_key(bodykey),body)
            attempted=next((Path(tmp)/'stage/_quarantine').rglob('attempted-inputs.json'));record=s.strict_json(attempted.read_bytes())['objects'][name];self.assertFalse(record['raw_receipt_read']);self.assertNotIn('raw_receipt_key',record)
            self.assertFalse((Path(tmp)/'stage/_construction-state').exists())

    def test_first_malformed_or_invalid_receipt_preserves_full_attempt_before_parse(self):
        with tempfile.TemporaryDirectory() as tmp:
            package=Path(tmp)/'input';shutil.copytree(self.package,package);storage=StagingStorageResource(local_dir=tmp+'/stage');name='legacy_metadata';body=(package/(name+'.body')).read_bytes()
            for raw in (b'{ malformed receipt',s.encode({'url':'untrusted','status':403})):
                (package/(name+'.receipt.json')).write_bytes(raw)
                with self.assertRaises(ValueError):s.acquire(storage,str(package),s.PACKAGE)
                bodykey=f'_raw-captures/{s.SLUG}/{s.digest(body)}/{name}.body';receiptkey=f'_raw-captures/{s.SLUG}/{s.digest(body)}/{name}.raw-receipt-{s.digest(raw)}.body'
                self.assertEqual(storage.read_key(bodykey),body);self.assertEqual(storage.read_key(receiptkey),raw)
                attempts=list((Path(tmp)/'stage/_quarantine').rglob('attempted-inputs.json'));self.assertTrue(attempts)
                self.assertTrue(any(s.strict_json(p.read_bytes())['objects'].get(name,{}).get('raw_receipt_key')==receiptkey for p in attempts))
            self.assertEqual(len(list((Path(tmp)/'stage/_raw-captures').rglob(name+'.body'))),1)
            self.assertFalse((Path(tmp)/'stage/_construction-state').exists())

    def test_pinned_drift_incomplete_receipts_and_extra_package_quarantine(self):
        with tempfile.TemporaryDirectory() as tmp:
            package=Path(tmp)/'input';shutil.copytree(self.package,package);storage=StagingStorageResource(local_dir=tmp+'/stage')
            target=package/'usgs_mineplant_zip.body';target.write_bytes(target.read_bytes()[:-1])
            with self.assertRaisesRegex(ValueError,'capture_url_status_length'):s.acquire(storage,str(package),s.PACKAGE)
            self.assertTrue(list((Path(tmp)/'stage/_quarantine').rglob('usgs_mineplant_zip.body')))
            target.write_bytes((self.package/target.name).read_bytes());(package/'extra').write_text('drift')
            with self.assertRaisesRegex(ValueError,'extra_explicit_package_roster'):s.acquire(storage,str(package),s.PACKAGE)

    def test_complete_reuse_and_published_id_or_ready_coforge_quarantines_whole_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);result=self.stage(storage);self.assertFalse(result['reused']);self.assertEqual(result['feature_count'],765);self.assertTrue(self.stage(storage)['reused'])
            root=Path(tmp)/s.SLUG/s.SLUG/result['version']
            self.assertFalse((root/'shapefile').exists());self.assertTrue((root/f'geoparquet/{s.SLUG}.parquet').is_file())
            from dagster_hifld.definitions import _iter_staged_version_paths
            self.assertIn((s.SLUG,s.SLUG,result['version']),list(_iter_staged_version_paths(storage)))
            target=root/'candidate/published-identifiers.parquet';target.write_bytes(b'forged');ready=s.strict_json((root/s.READY).read_bytes());ready['objects']['candidate/published-identifiers.parquet']=s.digest(b'forged');(root/s.READY).write_bytes(s.encode(ready))
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
            receipt=bundle['payload']['captures']['usgs_mineplant_zip']['receipt'];receipt['observed_at']='2026-11-10T00:00:00+00:00'
            for item in bundle['payload']['manifest']['inputs']:
                if item['input_id'].startswith('usgs_mineplant_zip:'):item['retrieved_at']=receipt['observed_at']
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

    def test_rogue_shapefile_output_object_is_quarantined(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);result=self.stage(storage);root=Path(tmp)/s.SLUG/s.SLUG/result['version']
            (root/'shapefile').mkdir();(root/'shapefile/mineplant.shp').write_bytes(b'rogue')
            with self.assertRaisesRegex(ValueError,'unknown_missing_or_linked'):self.stage(storage)
            self.assertFalse(root.exists());self.assertTrue(list((Path(tmp)/'_quarantine').rglob('mineplant.shp')))

    def test_unknown_catalog_metadata_whole_quarantine_and_original_raw_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);result=self.stage(storage);root=Path(tmp)/s.SLUG/s.SLUG/result['version'];raw={str(p):s.sha256_file(p) for p in (Path(tmp)/'_raw-captures').rglob('*') if p.is_file()}
            (root/'metadata/extra.json').write_bytes(b'{}')
            with self.assertRaisesRegex(ValueError,'unknown_missing_or_linked'):self.stage(storage)
            self.assertTrue(all(s.sha256_file(Path(p))==sha for p,sha in raw.items()))
