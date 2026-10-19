"""Small default mechanics; full Census capture tests require an explicit opt-in."""
import copy
import fcntl
import gzip
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import shapely
from dagster_hifld.sources import elementary_school_districts as f

INPUTS=Path(os.environ['ELSD_TEST_CAPTURE_DIR']).absolute() if os.environ.get('ELSD_TEST_CAPTURE_DIR') else None

class ElementaryTransportTests(unittest.TestCase):
    def fetch(self,headers):
        original=httpx.Client;raw=b'complete bounded encoded input';encoded=gzip.compress(raw)
        transport=httpx.MockTransport(lambda request:httpx.Response(200,headers={'Content-Encoding':'gzip',**headers(len(encoded))},stream=httpx.ByteStream(encoded)))
        with tempfile.TemporaryDirectory()as tmp,patch.object(f.httpx,'Client',lambda **kwargs:original(transport=transport,**kwargs)):
            return f.acquire(f.StagingStorageResource(use_local=True,local_dir=tmp))
    def test_gzip_wire_length_is_not_decoded_length(self):
        caps=self.fetch(lambda n:{'Content-Length':str(n)})
        self.assertEqual(len(caps),26)
        for body,r in caps.values():
            self.assertEqual(body,b'complete bounded encoded input')
            self.assertEqual(int(r['content_length']),r['wire_bytes_downloaded'])
            self.assertNotEqual(r['bytes'],r['wire_bytes_downloaded'])
    def test_incomplete_wire_fails_and_retains_attempt(self):
        with self.assertRaisesRegex(ValueError,'non200_or_incomplete_http_response'):self.fetch(lambda n:{'Content-Length':str(n+1)})
    def test_chunked_complete_input(self):
        self.assertEqual(len(self.fetch(lambda n:{})),26)
    def test_malformed_local_receipt_retains_exact_body_and_receipt(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);source=root/'inputs';source.mkdir();body=b'original attempted body';bad=b'{bad exact receipt\n'
            (source/'census2026_elsd.body').write_bytes(body);(source/'census2026_elsd.receipt.json').write_bytes(bad)
            staging=root/'staging';storage=f.StagingStorageResource(use_local=True,local_dir=str(staging))
            with self.assertRaises(ValueError):f.acquire(storage,source)
            path=next((staging/'_quarantine').rglob('census2026_elsd.body'))
            self.assertEqual(path.read_bytes(),body);r=json.loads(path.with_suffix('.receipt.json').read_bytes())
            self.assertFalse(r['complete_body']);self.assertEqual((staging/r['original_receipt_key']).read_bytes(),bad)
            self.assertEqual((source/'census2026_elsd.receipt.json').read_bytes(),bad)
    def test_live_partial_stream_retains_three_attempts(self):
        class Broken(httpx.SyncByteStream):
            def __iter__(self):
                yield b'partial original bytes'
                raise httpx.ReadError('test interrupted stream')
        original=httpx.Client
        transport=httpx.MockTransport(lambda request:httpx.Response(200,stream=Broken()))
        with tempfile.TemporaryDirectory()as tmp,patch.object(f.httpx,'Client',lambda **kwargs:original(transport=transport,**kwargs)):
            with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):f.acquire(f.StagingStorageResource(local_dir=tmp))
            bodies=list(Path(tmp).glob('_quarantine/**/census2026_elsd.body'));self.assertEqual(len(bodies),3)
            self.assertTrue(all(p.read_bytes()==b'partial original bytes'for p in bodies))
    def test_published_cloud_prefixed_and_aliased_storage_refused(self):
        for storage in [f.PublishedStorageResource(local_dir='/tmp'),f.StagingStorageResource(bucket='never'),f.StagingStorageResource(local_dir='/tmp',prefix='unexpected')]:
            with self.assertRaises(ValueError):f.local_only(storage)
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);(root/'real').mkdir();(root/'alias').symlink_to(root/'real')
            with self.assertRaises(ValueError):f.local_only(f.StagingStorageResource(local_dir=str(root/'alias')))

class ElementarySmallFixtureTests(unittest.TestCase):
    """Synthetic rows exercise physical fidelity without standing in for Census acceptance."""
    @staticmethod
    def properties():
        return {'STATEFP':'04','ELSDLEA':'00001','GEOID':'0400001','GEOIDFQ':'9500000US0400001',
                'NAME':' Leading test district','LSAD':'00','LOGRADE':'PK','HIGRADE':'08',
                'MTFCC':'G5400','SDTYP':None,'FUNCSTAT':'E','ALAND':99999999999999,
                'AWATER':0,'INTPTLAT':'+34.1000000','INTPTLON':'-112.1000000'}

    @classmethod
    def dbf(cls):
        header=33+32*len(f.DBF_FIELDS);width=1+sum(v[2]for v in f.DBF_FIELDS)
        body=bytearray(header+width+1);body[0]=3;body[4:8]=(1).to_bytes(4,'little')
        body[8:10]=header.to_bytes(2,'little');body[10:12]=width.to_bytes(2,'little')
        values=cls.properties();cursor=header;body[cursor]=32;cursor+=1
        for index,(name,kind,size,scale)in enumerate(f.DBF_FIELDS):
            offset=32+index*32;body[offset:offset+len(name)]=name.encode()
            body[offset+11]=ord(kind);body[offset+16]=size;body[offset+17]=scale
            value=values[name];text=''if value is None else str(value)
            raw=text.encode().rjust(size,b' ')if kind=='N'else text.encode().ljust(size,b' ')
            assert len(raw)==size
            body[cursor:cursor+size]=raw;cursor+=size
        body[header-1]=13;body[-1]=26
        return bytes(body)

    @classmethod
    def table(cls):
        polygon=shapely.Polygon([(0,0),(0,1),(1,1),(1,0),(0,0)])
        rows=[]
        for index,geom in enumerate([polygon,shapely.MultiPolygon([polygon])]):
            row={**cls.properties(),'OGC_FID':index,'source_archive':'tl_2026_04_elsd',
                 'ELSDLEA':f'{index+1:05}','GEOID':f'04{index+1:05}',
                 'GEOIDFQ':f'9500000US04{index+1:05}','wkb_geometry':shapely.to_wkb(geom)}
            rows.append(row)
        return pa.Table.from_arrays([pa.array([row[name]for row in rows],type=kind)
                                     for name,kind in zip(f.FIELDS,f.TYPES)],names=f.FIELDS)

    def test_physical_dbf_strings_nulls_and_integer_literals(self):
        body=self.dbf();self.assertEqual(f.dbf_definition(body)['row_count'],1)
        self.assertEqual(f.decode_dbf_rows(body),[self.properties()])
        values=f.decode_dbf_rows(body)[0]
        self.assertEqual(values['ELSDLEA'],'00001');self.assertIsNone(values['SDTYP'])
        self.assertIs(type(values['ALAND']),int);self.assertEqual(values['ALAND'],99999999999999)
        self.assertTrue(values['NAME'].startswith(' '))

    def test_dbf_truncation_deleted_row_renamed_field_and_decimal_are_rejected(self):
        original=self.dbf();header=int.from_bytes(original[8:10],'little')
        changes=[original[:-2]]
        deleted=bytearray(original);deleted[header]=42;changes.append(bytes(deleted))
        renamed=bytearray(original);renamed[32]=ord('X');changes.append(bytes(renamed))
        fractional=bytearray(original);offset=header+1+sum(v[2]for v in f.DBF_FIELDS[:11])
        fractional[offset:offset+14]=b'           1.5';changes.append(bytes(fractional))
        for body in changes:
            with self.subTest(body=body[:40]),self.assertRaises(ValueError):f.decode_dbf_rows(body)

    def test_small_native_formats_keep_types_fields_nulls_and_polygon_bytes(self):
        table=self.table();expected=table.drop(['source_archive'])
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);f.write_native_gpkg(table,root/'tiny.gpkg')
            metadata,actual=pyogrio.read_arrow(root/'tiny.gpkg',layer=f.LAYER)
            self.assertEqual(metadata['crs'],'EPSG:4269')
            self.assertEqual(tuple(actual.column_names),f.SOURCE_FIELDS)
            self.assertEqual(tuple(v.type for v in actual.schema),f.TYPES[1:])
            self.assertEqual(actual.to_pylist(),expected.to_pylist())
            native=f.expected_parquet_table(table);pq.write_table(native,root/'tiny.parquet')
            self.assertTrue(pq.read_table(root/'tiny.parquet').equals(native,check_metadata=True))
            self.assertEqual(native['ALAND'].type,pa.int64());self.assertEqual(native['SDTYP'].null_count,2)
            self.assertNotIn('source_archive',native.column_names)
            self.assertEqual(shapely.get_type_id(shapely.from_wkb(native['wkb_geometry'].to_pylist())).tolist(),[3,6])

    def test_tiny_fixture_cannot_pass_complete_population_or_changed_schema(self):
        table=self.table()
        for changed in [table,table.drop(['GEOID']),table.rename_columns([
                'RENAMED_GEOID'if name=='GEOID'else name for name in table.column_names]),
                table.set_column(table.schema.get_field_index('ALAND'),'ALAND',pa.array([1.,2.]))]:
            with self.assertRaisesRegex(ValueError,'native_schema_physical_types_or_completeness_drift'):
                f.validate_table(changed)

    def test_lineage_is_separate_from_terminal_schema_and_retains_local_fids(self):
        table=self.table();records=[json.loads(line)for line in f.lineage_sidecar(table).splitlines()]
        self.assertEqual([r['original_file_local_OGC_FID']for r in records],[0,1])
        self.assertEqual([r['original_identifiers']['GEOID']for r in records],['0400001','0400002'])
        self.assertTrue(all(r['source_archive']=='tl_2026_04_elsd.zip'for r in records))
        self.assertNotIn('source_archive',f.expected_parquet_table(table).column_names)

    def test_observed_loss_journal_keeps_original_claim_and_rejects_rebinding(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);candidate=root/'synthetic-candidate';(candidate/'metadata').mkdir(parents=True)
            manifest={'synthetic_test_context':'original'}
            (candidate/'metadata/source_manifest.json').write_bytes(f.encode(manifest))
            storage=f.StagingStorageResource(local_dir=tmp);fingerprint='a'*64
            self.assertTrue(f.observed_trust_loss(storage,'vtest',fingerprint,candidate=candidate))
            journal=root/f'_construction-incidents/{f.SLUG}/vtest/incident.json';body=journal.read_bytes()
            for _ in range(2):self.assertTrue(f.observed_trust_loss(storage,'vtest',fingerprint))
            self.assertEqual(journal.read_bytes(),body)
            self.assertTrue(f.observed_trust_loss(storage,'vtest',fingerprint,restored_manifest=manifest))
            with self.assertRaisesRegex(ValueError,'restored_original_manifest_differs'):
                f.observed_trust_loss(storage,'vtest',fingerprint,restored_manifest={'synthetic_test_context':'new'})
            for key,value in [('schema_version',True),('schema_version',1.0),('approved',0)]:
                altered=json.loads(body);altered[key]=value;journal.write_bytes(f.encode(altered))
                with self.assertRaises(ValueError):f.observed_trust_loss(storage,'vtest',fingerprint)
            journal.write_bytes(body)

    def test_partial_journal_blocks_repeated_retry_and_preserves_candidate(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);candidate=root/'synthetic-candidate';(candidate/'metadata').mkdir(parents=True)
            manifest=f.encode({'synthetic_test_context':'original'})
            path=candidate/'metadata/source_manifest.json';path.write_bytes(manifest)
            storage=f.StagingStorageResource(local_dir=tmp)
            with patch.object(f,'immutable',side_effect=OSError('synthetic_partial_write')):
                with self.assertRaises(OSError):f.observed_trust_loss(storage,'vtest','a'*64,candidate=candidate)
            for _ in range(2):
                with self.assertRaisesRegex(ValueError,'incomplete_observed_trust_loss_journal'):
                    f.observed_trust_loss(storage,'vtest','a'*64)
                self.assertEqual(path.read_bytes(),manifest)

    def test_private_descriptor_short_and_false_counts_are_rejected(self):
        real=os.write;body=b'{"synthetic_private_metadata":"complete"}\n'
        for lie in (False,True):
            with self.subTest(false_count=lie),tempfile.TemporaryDirectory()as tmp:
                def short(descriptor,value):
                    count=real(descriptor,value[:17]);return len(value)if lie else count
                with patch.object(f.os,'write',side_effect=short),self.assertRaisesRegex(ValueError,'incomplete_private_write'):
                    f.write_private(Path(tmp),'metadata/test.json',body)
                self.assertEqual((Path(tmp)/'metadata/test.json').read_bytes(),body[:17])

    def test_candidate_fifo_and_oversize_are_rejected_before_bulk_read(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);(root/'metadata').mkdir();path=root/'metadata/test.json'
            os.mkfifo(path)
            with self.assertRaises(ValueError):f.candidate_bytes(root,'metadata/test.json')
            with patch.object(f,'acquire')as acquire:
                with self.assertRaisesRegex(ValueError,'staging_alias_or_special_file_refused'):
                    f.stage_elementary_districts(f.StagingStorageResource(local_dir=tmp))
                acquire.assert_not_called()
            path.unlink()
            with path.open('wb')as stream:stream.truncate(2_000_001)
            real=os.fdopen;reads=[]
            class Guard:
                def __init__(self,stream):self.stream=stream
                def __enter__(self):return self
                def __exit__(self,*args):self.stream.close()
                def fileno(self):return self.stream.fileno()
                def read(self,*args):reads.append(args);raise AssertionError('oversized data read before size rejection')
            with patch.object(f.os,'fdopen',side_effect=lambda *args,**kwargs:Guard(real(*args,**kwargs))):
                with self.assertRaisesRegex(ValueError,'unsafe_or_oversized_input'):f.candidate_bytes(root,'metadata/test.json')
            self.assertEqual(reads,[])

    def test_private_aliases_and_unsafe_keys_preserve_original_outside_bytes(self):
        for kind in ('symlink','hardlink','ancestor'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory()as tmp:
                root=Path(tmp);private=root/'private';outside=root/'outside';private.mkdir();outside.mkdir()
                original=outside/'test.json';original.write_bytes(b'original outside fixture')
                if kind=='ancestor':(private/'metadata').symlink_to(outside,target_is_directory=True)
                else:
                    (private/'metadata').mkdir();target=private/'metadata/test.json'
                    if kind=='symlink':target.symlink_to(original)
                    else:os.link(original,target)
                with self.assertRaises((ValueError,OSError)):f.write_private(private,'metadata/test.json',b'changed')
                self.assertEqual(original.read_bytes(),b'original outside fixture')
                for key in ('../outside/test.json','/outside/test.json','metadata/../test.json','metadata\\test.json'):
                    with self.assertRaises(ValueError):f.candidate_bytes(private,key)

    def test_candidate_roster_refuses_special_files_without_opening_them(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);os.mkfifo(root/'fifo')
            with self.assertRaisesRegex(ValueError,'candidate_alias_or_special_file_refused'):f.candidate_roster(root)


@unittest.skipUnless(INPUTS, 'Set ELSD_TEST_CAPTURE_DIR to the complete external Census capture for real-population tests.')
class ElementaryOriginalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory();cls.base=Path(cls.temp.name)
        cls.original=cls.base/'original';storage=f.StagingStorageResource(local_dir=str(cls.original))
        cls.output=f.stage_elementary_districts(storage,capture_dir=INPUTS)
        cls.prefix=f'{f.SLUG}/{f.SLUG}/{cls.output["version"]}'
        cls.caps={n:((INPUTS/(n+'.body')).read_bytes(),f.strict_json((INPUTS/(n+'.receipt.json')).read_bytes()))for n in f.URLS}
        cls.table,cls.audit=f.inspect_archives({n:INPUTS/(n+'.body')for n in f.PINS},cls.caps)
        cls.identity=f.processing_identity()
    @classmethod
    def tearDownClass(cls):cls.temp.cleanup()
    def setUp(self):
        self.tempdir=tempfile.TemporaryDirectory();self.addCleanup(self.tempdir.cleanup)
        self.path=Path(self.tempdir.name)/'staging';shutil.copytree(self.original,self.path)
        self.storage=f.StagingStorageResource(local_dir=str(self.path));self.root=self.path/self.prefix
        self.prior=self.path/'prior-accepted';self.prior.write_bytes(b'accepted bytes stay unchanged')
    def refresh(self):return f.stage_elementary_districts(self.storage,capture_dir=INPUTS)
    def repin(self,name):
        path=self.root/f.READY;j=json.loads(path.read_bytes());j['objects'][name]=f.sha256_file(self.root/name);path.write_bytes(f.encode(j))
    def record(self):
        return next(p for p in(self.path/'_construction-state'/f.SLUG).glob('*.json')if not p.name.endswith('.first-construction.json'))
    def reject(self,reason):
        with self.assertRaisesRegex(ValueError,reason):self.refresh()
        self.assertFalse(self.root.exists())
        self.assertTrue(list((self.path/'_quarantine'/f.SLUG).glob('invalid-candidate-*')))
        self.assertEqual(self.prior.read_bytes(),b'accepted bytes stay unchanged')
    def test_complete_2030_records_all_ids_native_types_nulls_geometry_and_reuse(self):
        self.assertEqual(self.table.num_rows,2030);self.assertEqual(len(self.audit),25)
        self.assertEqual(self.table['SDTYP'].null_count,1988)
        self.assertEqual(len(set(self.table['GEOID'].to_pylist())),2030)
        self.assertEqual(tuple(x.type for x in self.table.schema),f.TYPES)
        self.assertEqual(self.table['ELSDLEA'].type,pa.string())
        self.assertEqual(self.table['ALAND'].type,pa.int64())
        f.reconcile_outputs(self.root,self.table);self.assertTrue(self.refresh()['reused'])
    def test_terminal_schema_preserves_original_fields_and_complete_lineage_sidecar(self):
        parquet=pq.read_table(self.root/'geoparquet'/f'{f.SLUG}.parquet')
        _,gpkg=pyogrio.read_arrow(self.root/'geopackage'/f'{f.SLUG}.gpkg',layer=f.LAYER)
        self.assertEqual(tuple(parquet.column_names),f.SOURCE_FIELDS)
        self.assertEqual(tuple(gpkg.column_names),f.SOURCE_FIELDS)
        self.assertNotIn('source_archive',parquet.column_names)
        self.assertEqual(parquet['OGC_FID'].to_pylist(),self.table['OGC_FID'].to_pylist())
        self.assertLess(len(set(parquet['OGC_FID'].to_pylist())),2030)
        lineage=[json.loads(b)for b in(self.root/'metadata/source_record_lineage.jsonl').read_bytes().splitlines()]
        self.assertEqual(len(lineage),2030)
        self.assertEqual(len({r['original_identifiers']['GEOID']for r in lineage}),2030)
        for record,row in zip(lineage,self.table.to_pylist()):
            self.assertEqual(record['source_archive'],row['source_archive']+'.zip')
            self.assertEqual(record['source_member'],row['source_archive']+'.dbf')
            self.assertEqual(record['original_file_local_OGC_FID'],row['OGC_FID'])
            self.assertEqual(record['original_identifiers'],{n:row[n]for n in ['STATEFP','ELSDLEA','GEOID','GEOIDFQ']})
            self.assertEqual(record['original_input_sha256'],f.PINS[row['source_archive']]['sha256'])
        name='metadata/source_record_lineage.jsonl'
        records=lineage.copy();records[0]=dict(records[0]);records[0]['source_archive']='invented.zip'
        (self.root/name).write_bytes(b''.join(f.encode(r)for r in records));self.repin(name);self.reject('source_owned_bytes')

    def test_missing_renamed_retyped_ids_duplicate_coverage_and_unexpected_fields(self):
        for table in [self.table.drop(['GEOID']),self.table.rename_columns(['wrong']+self.table.column_names[1:]),self.table.slice(1),
                      self.table.set_column(4,'GEOID',pa.array(['0000000']*2030)),self.table.append_column('extra',pa.array([1]*2030)),
                      self.table.set_column(13,'ALAND',self.table['ALAND'].cast(pa.float64()))]:
            with self.subTest(columns=table.column_names),self.assertRaises(ValueError):f.validate_table(table)
    def test_complete_attribute_and_geometry_drift_detected(self):
        values=self.table['NAME'].to_pylist();values[0]='changed'
        with self.assertRaisesRegex(ValueError,'attribute_values'):f.validate_table(self.table.set_column(6,'NAME',pa.array(values)))
        geoms=self.table['wkb_geometry'].to_pylist();geoms[0]=geoms[1]
        with self.assertRaisesRegex(ValueError,'attribute_values'):f.validate_table(self.table.set_column(17,'wkb_geometry',pa.array(geoms,type=pa.binary())))
    def test_receipt_url_status_bool_length_and_time_fail_before_candidate(self):
        for change in [{'status':True},{'bytes':True},{'final_url':'https://example.invalid/'},{'complete_body':1},{'observed_at':'bad'}]:
            caps=copy.deepcopy(self.caps);caps['tl_2026_04_elsd'][1].update(change)
            with self.subTest(change=change),self.assertRaises(ValueError):f.manifest_for(caps,self.identity)
    def test_changed_truncated_missing_publication_and_directory_roster(self):
        for name in ['tl_2026_04_elsd','census2026_elsd']:
            caps=copy.deepcopy(self.caps);body,receipt=caps[name]
            body=body[:-1]if name!='census2026_elsd'else body.replace(b'tl_2026_04_elsd.zip',b'tl_2026_99_elsd.zip')
            receipt.update(bytes=len(body),sha256=f.digest(body));caps[name]=(body,receipt)
            with self.assertRaises(ValueError):f.inspect_archives({n:INPUTS/(n+'.body')for n in f.PINS},caps)
        caps=dict(self.caps);caps.pop('tl_2026_04_elsd')
        with self.assertRaises(ValueError):f.manifest_for(caps,self.identity)
    def test_physical_dbf_leading_characters_preserved_and_schema_drift_refused(self):
        with zipfile.ZipFile(INPUTS/'tl_2026_04_elsd.body')as z:body=bytearray(z.read('tl_2026_04_elsd.dbf'))
        h=int.from_bytes(body[8:10],'little');cursor=h+1
        for name,kind,width,_ in f.DBF_FIELDS:
            if name=='NAME':body[cursor:cursor+width]=b' leading original name'+b' '*(width-22);break
            cursor+=width
        self.assertEqual(f.decode_dbf_rows(bytes(body))[0]['NAME'],' leading original name')
        body[32+11]=ord('N')
        with self.assertRaisesRegex(ValueError,'physical_declarations'):f.dbf_definition(bytes(body))
    def test_forged_native_container_and_source_reports_cannot_self_repin(self):
        name=f'geopackage/{f.SLUG}.gpkg'
        with sqlite3.connect(self.root/name)as db:db.execute('UPDATE gpkg_contents SET srs_id=3857')
        self.repin(name);self.reject('full_container')
    def test_false_approval_placeholder_is_quarantined(self):
        name='metadata/baseline_comparison.json';j=json.loads((self.root/name).read_bytes());j['approved']=True
        (self.root/name).write_bytes(f.encode(j));self.repin(name);self.reject('source_owned_bytes')
    def test_missing_ready_preserves_entire_invalid_candidate(self):
        (self.root/f.READY).unlink();before={str(p.relative_to(self.root)):f.sha256_file(p)for p in self.root.rglob('*')if p.is_file()}
        self.reject('invalid_cache_moved_intact')
        q=next((self.path/'_quarantine'/f.SLUG).glob('invalid-candidate-*'))
        self.assertEqual(before,{str(p.relative_to(q)):f.sha256_file(p)for p in q.rglob('*')if p.is_file()})
    def test_missing_primary_three_retries_cannot_rebase_trusted_restore_recovers(self):
        record=self.record();original=record.read_bytes();marker=Path(str(record)+'.first-construction.json')
        saved=marker.read_bytes();evidence=self.path/json.loads(saved)['original_bundle_evidence_key'];saved_evidence=evidence.read_bytes()
        record.unlink();self.reject('construction_missing_original_state')
        for _ in range(2):
            with self.assertRaisesRegex(ValueError,'construction_missing_original_state'):self.refresh()
            self.assertFalse(self.root.exists());self.assertFalse(record.exists())
            self.assertEqual(marker.read_bytes(),saved);self.assertEqual(evidence.read_bytes(),saved_evidence)
        record.write_bytes(original);self.assertFalse(self.refresh()['reused']);self.assertTrue(self.refresh()['reused'])
    def test_observed_whole_history_loss_stays_refused_until_exact_trusted_restore(self):
        folder=self.record().parent
        saved={p.relative_to(folder):p.read_bytes()for p in folder.rglob('*')if p.is_file()}
        original_manifest=(self.root/'metadata/source_manifest.json').read_bytes()
        raw={p.relative_to(self.path):f.sha256_file(p)for p in(self.path/'_raw-captures').rglob('*')if p.is_file()}
        shutil.rmtree(folder);self.reject('construction_missing_original_state')
        incident=self.path/f'_construction-incidents/{f.SLUG}/{self.output["version"]}/incident.json'
        body=incident.read_bytes();self.assertEqual(json.loads(body)['observed_cached_manifest_sha256'],f.digest(original_manifest))
        for _ in range(2):
            with self.assertRaisesRegex(ValueError,'construction_missing_original_state'):self.refresh()
            self.assertFalse(self.root.exists());self.assertFalse(folder.exists());self.assertEqual(incident.read_bytes(),body)
        for name,value in saved.items():path=folder/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(value)
        self.assertFalse(self.refresh()['reused']);self.assertTrue(self.refresh()['reused'])
        self.assertEqual((self.root/'metadata/source_manifest.json').read_bytes(),original_manifest)
        self.assertEqual(raw,{p.relative_to(self.path):f.sha256_file(p)for p in(self.path/'_raw-captures').rglob('*')if p.is_file()})
        self.assertEqual(self.prior.read_bytes(),b'accepted bytes stay unchanged')
    def test_partial_incident_write_retains_untrusted_candidate_and_refuses_retry(self):
        shutil.rmtree(self.record().parent)
        before={str(p.relative_to(self.root)):f.sha256_file(p)for p in self.root.rglob('*')if p.is_file()}
        original=f.immutable
        def fail_incident(storage,key,body):
            if key.startswith('_construction-incidents/'):raise OSError('synthetic_incident_write_failure')
            return original(storage,key,body)
        with patch.object(f,'immutable',side_effect=fail_incident):
            with self.assertRaisesRegex(ValueError,'trust_loss_journal_failed_candidate_retained_untrusted'):self.refresh()
        for _ in range(2):
            with self.assertRaisesRegex(ValueError,'incomplete_observed_trust_loss_journal'):self.refresh()
            self.assertTrue(self.root.exists())
            self.assertEqual(before,{str(p.relative_to(self.root)):f.sha256_file(p)for p in self.root.rglob('*')if p.is_file()})
        self.assertEqual(self.prior.read_bytes(),b'accepted bytes stay unchanged')
    def test_relative_root_observed_incident_preserves_exact_restored_context(self):
        import os
        previous=Path.cwd()
        try:
            os.chdir(self.path.parent)
            self.storage=f.StagingStorageResource(local_dir=self.path.name)
            self.test_observed_whole_history_loss_stays_refused_until_exact_trusted_restore()
        finally:os.chdir(previous)
    def test_incident_canonical_schema_types_and_claim_are_strict(self):
        fingerprint=f.digest(f.encode(self.identity))
        f.observed_trust_loss(self.storage,self.output['version'],fingerprint,candidate=self.root)
        path=self.path/f'_construction-incidents/{f.SLUG}/{self.output["version"]}/incident.json';original=path.read_bytes()
        for key,value in [('schema_version',True),('schema_version',1.0),('approved',0),('observed_cached_manifest_sha256','wrong')]:
            bad=json.loads(original);bad[key]=value;path.write_bytes(f.encode(bad))
            with self.assertRaises(ValueError):f.observed_trust_loss(self.storage,self.output['version'],fingerprint)
        path.write_bytes(original)
        with self.assertRaisesRegex(ValueError,'restored_original_manifest_differs'):f.observed_trust_loss(self.storage,self.output['version'],fingerprint,restored_manifest={'synthetic':'different'})
    def test_marker_bool_or_float_version_fails_without_self_repair(self):
        marker=Path(str(self.record())+'.first-construction.json');original=marker.read_bytes()
        for value in [True,1.0]:
            with self.subTest(schema_version=value):
                j=json.loads(original);j['schema_version']=value;marker.write_bytes(f.encode(j));self.reject('construction')
                with self.assertRaisesRegex(ValueError,'construction'):self.refresh()
                marker.write_bytes(original);self.assertFalse(self.refresh()['reused'])
    def test_original_and_candidate_coforge_cannot_change_construction_claim(self):
        record=self.record();j=json.loads(record.read_bytes());j['payload']['manifest']['code_commit']='0'*40
        record.write_bytes(f.encode(j));name='metadata/source_manifest.json';j=json.loads((self.root/name).read_bytes());j['code_commit']='0'*40
        (self.root/name).write_bytes(f.encode(j));self.repin(name);self.reject('construction')
    def test_common_lock_covers_full_cache_verification(self):
        original=f.reconcile_cache
        def check(*a,**kw):
            with(self.path/'_locks'/(f.SLUG+'.lock')).open('a+b')as other:
                with self.assertRaises(BlockingIOError):fcntl.flock(other,fcntl.LOCK_EX|fcntl.LOCK_NB)
            return original(*a,**kw)
        with patch.object(f,'reconcile_cache',side_effect=check):self.assertTrue(self.refresh()['reused'])
    def test_failed_private_output_leaves_no_candidate_or_trust_state(self):
        with tempfile.TemporaryDirectory()as tmp,patch.object(f,'reconcile_outputs',side_effect=ValueError('test_private_failure')):
            with self.assertRaisesRegex(ValueError,'test_private_failure'):f.stage_elementary_districts(f.StagingStorageResource(local_dir=tmp),capture_dir=INPUTS)
            self.assertFalse([p for p in Path(tmp).glob('_construction-state/**/*')if p.is_file()])
            self.assertFalse(list(Path(tmp).glob(f'{f.SLUG}/{f.SLUG}/v*')))
            self.assertEqual(len(list(Path(tmp).glob('_raw-captures/**/*.body'))),26)
    def test_invalid_first_code_and_manifest_claims_create_no_trust_records(self):
        manifest=f.manifest_for(self.caps,self.identity)
        for key,value in [('code_commit','0'*40),('title','forged original title')]:
            bad=copy.deepcopy(manifest);bad[key]=value
            with tempfile.TemporaryDirectory()as tmp:
                storage=f.StagingStorageResource(local_dir=tmp)
                with self.assertRaisesRegex(ValueError,'construction_invalid_commit|preseal_original_manifest_semantic_drift'):
                    f.original_bundle(storage,'_construction-state/test/first.json','vtest',self.identity,create=(self.caps,bad))
                self.assertFalse([p for p in Path(tmp).glob('_construction-state/**/*')if p.is_file()])

    def test_real_private_short_and_false_write_counts_cannot_create_first_state(self):
        real=os.write
        for lie in (False,True):
            with self.subTest(false_count=lie),tempfile.TemporaryDirectory()as tmp:
                def short(descriptor,body):
                    path=os.readlink('/proc/self/fd/'+str(descriptor))
                    if '.private-elsd-'in path and path.endswith('/metadata/source_manifest.json'):
                        count=real(descriptor,body[:17]);return len(body)if lie else count
                    return real(descriptor,body)
                with patch.object(f.os,'write',side_effect=short):
                    with self.assertRaisesRegex(ValueError,'incomplete_private_write'):
                        f.stage_elementary_districts(f.StagingStorageResource(local_dir=tmp),capture_dir=INPUTS)
                self.assertFalse(list(Path(tmp).glob('_construction-state/**/*')))
                self.assertFalse(list(Path(tmp).glob(f'{f.SLUG}/{f.SLUG}/v*')))

    def test_complete_private_metadata_checked_before_and_after_first_seal(self):
        for after_seal in (False,True):
            with self.subTest(after_seal=after_seal),tempfile.TemporaryDirectory()as tmp:
                base=Path(tmp)
                def corrupt():
                    private=next(base.glob(f'{f.SLUG}/{f.SLUG}/.private-elsd-*'))
                    (private/'metadata/source_manifest.json').write_bytes(b'{broken metadata\n')
                if after_seal:
                    original=f.original_bundle
                    def hook(*args,**kwargs):
                        value=original(*args,**kwargs)
                        if kwargs.get('create')is not None:corrupt()
                        return value
                    target='original_bundle'
                else:
                    original=f.write_private
                    def hook(root,name,body):
                        original(root,name,body)
                        if name==f.READY:corrupt()
                    target='write_private'
                with patch.object(f,target,side_effect=hook):
                    with self.assertRaisesRegex(ValueError,'private_candidate_source_bytes_drift'):
                        f.stage_elementary_districts(f.StagingStorageResource(local_dir=tmp),capture_dir=INPUTS)
                records=[p for p in base.glob('_construction-state/**/*')if p.is_file()]
                self.assertEqual(len(records),3 if after_seal else 0)
                self.assertFalse(list(base.glob(f'{f.SLUG}/{f.SLUG}/v*')))

    def test_real_cached_fifo_is_refused_before_acquisition_without_watchdog(self):
        ready=self.root/f.READY;body=ready.read_bytes();ready.unlink();os.mkfifo(ready)
        try:
            with patch.object(f,'acquire')as acquire:
                with self.assertRaisesRegex(ValueError,'staging_alias_or_special_file_refused'):self.refresh()
                acquire.assert_not_called()
            self.assertTrue(self.root.is_dir());self.assertEqual(self.prior.read_bytes(),b'accepted bytes stay unchanged')
        finally:ready.unlink();ready.write_bytes(body)

    def test_real_oversized_cached_metadata_is_quarantined_intact(self):
        path=self.root/'metadata/source_manifest.json'
        with path.open('wb')as stream:stream.truncate(2_000_001)
        self.reject('unsafe_or_oversized_input')
        quarantined=next((self.path/'_quarantine'/f.SLUG).glob('invalid-candidate-*'))
        self.assertEqual((quarantined/'metadata/source_manifest.json').stat().st_size,2_000_001)

if __name__=='__main__':unittest.main()
