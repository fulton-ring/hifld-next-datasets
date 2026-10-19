"""Faithful draft mechanics and failure isolation; fixtures do not grant acceptance."""
import copy
import gzip
import threading
import json
import io
import os
import struct
import shutil
import zipfile
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from dagster import DefaultScheduleStatus

from dagster_hifld.assets.fws import fws_cbrs_buffer_draft_refresh_schedule, fws_cbrs_buffer_zone
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import cbrs_buffer as s

CAPTURE=Path(os.environ.get('HIFLD_CBRS_CAPTURE_DIR','/complete-cbrs-capture-directory-not-configured'))
REAL_INPUTS=unittest.skipUnless(os.environ.get('HIFLD_CBRS_CAPTURE_DIR'),'Full real source requires preserved external capture directory; no live refetch.')
FIXTURE=Path(__file__).parent/'fixtures/cbrs_buffer/structural_source.json'

class CBRSTests(unittest.TestCase):
    @staticmethod
    def structural_captures(storage=None):
        # Deliberately incomplete bodies: they exercise provenance mechanics only
        # and cannot pass the real source archive pin or establish acceptance.
        bodies={'official_zip':b'PK\x03\x04SYNTHETIC-STRUCTURAL-ONLY',
                'fws_data':b'SYNTHETIC supporting-page fixture',
                'fws_download':b'SYNTHETIC /sites/default/files/documents/2024-12/20241219-cbrs-boundaries.zip',
                'fws_buffer_metadata':b'SYNTHETIC 20241219 CBRS_Buffer_Zone metadata identity fixture'}
        captures={name:(body,{'requested_url':s.URLS[name],'final_url':s.URLS[name],
            'status':200,'byte_length':len(body),'sha256':s.sha(body),
            'observed_at':'2026-10-19T01:02:03+00:00'}) for name,body in bodies.items()}
        if storage is not None:
            for name,(body,receipt) in captures.items():
                s.immutable(storage,f'_raw-captures/{s.SLUG}/{s.sha(body)}/{name}.body',body)
        return captures

    @staticmethod
    def structural_archive():
        body=io.BytesIO()
        with zipfile.ZipFile(body,'w',compression=zipfile.ZIP_DEFLATED) as archive:
            for name in sorted(s.MEMBERS):
                value=b'SYNTHETIC-STRUCTURAL-MEMBER-ONLY'
                if name==s.STEM+'.dbf':value=b'\0'*4+struct.pack('<I',1145)+b'\0'*24
                archive.writestr(name,value)
        return body.getvalue()

    @staticmethod
    def atomic_fixture(name='geopackage/test.gpkg',body=b'fixture'):
        # Only atomic installation mechanics are synthetic in these fixtures.
        manifest={'synthetic_fixture':True}
        payloads={name:body,'metadata/source_manifest.json':s.encode(manifest),
                  'metadata/processing_identity.json':s.encode({'files':{}})}
        payloads.update({f'raw/captures/{n}.body':b'fixture' for n in s.URLS})
        payloads.update({f'raw/captures/{n}.receipt.json':b'{}' for n in s.URLS})
        return payloads,{'version':'v0.0.0-fixture'},manifest

    @REAL_INPUTS
    def test_complete_original_release(self):
        with tempfile.TemporaryDirectory() as d:
            rows,audit,members=s.inspect_archive((CAPTURE/'official_zip.body').read_bytes(),Path(d))
        self.assertEqual(len(rows),1145);self.assertEqual(len(members),25)
        self.assertEqual(audit['invalid_geometry_count'],0)
        self.assertEqual(audit['coverage']['BUFF_DIST'],{20.0:1138,0.0:7})
        self.assertEqual(len(audit['zero_buffer_width_fids']),7)
        self.assertEqual({r['properties']['__source_fid'] for r in rows},{str(i) for i in range(1145)})

    def test_registry_schedule_and_published_rejected_before_acquisition(self):
        self.assertEqual(sum(a is fws_cbrs_buffer_zone for a in hifld_ingest_assets),1)
        self.assertEqual(fws_cbrs_buffer_draft_refresh_schedule.default_status,DefaultScheduleStatus.STOPPED)
        self.assertEqual(fws_cbrs_buffer_draft_refresh_schedule.cron_schedule,'0 12 1 * *')
        for storage in [PublishedStorageResource(local_dir='/tmp/never'),StagingStorageResource(bucket='never',use_local=False)]:
            with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):
                s.stage_cbrs_buffer_candidate(storage,capture_dir=CAPTURE)
            acquire.assert_not_called()

    def test_lexical_root_and_ancestor_alias_rejected_before_acquisition(self):
        with tempfile.TemporaryDirectory() as d:
            target=Path(d)/'target';target.mkdir();alias=Path(d)/'staging-alias';alias.symlink_to(target,target_is_directory=True)
            for root in [alias,alias/'not-created']:
                with self.subTest(root=root),patch.object(s,'acquire') as acquire,self.assertRaisesRegex(ValueError,'linked_lexical_staging_root_or_ancestor'):
                    s.stage_cbrs_buffer_candidate(StagingStorageResource(local_dir=str(root)),capture_dir=CAPTURE)
                acquire.assert_not_called();self.assertEqual(list(target.iterdir()),[])
            # Source-local capture API also refuses before opening any body.
            with patch.object(Path,'open',side_effect=AssertionError('must reject before source reads')),self.assertRaisesRegex(ValueError,'linked_lexical_staging_root_or_ancestor'):
                s.acquire(StagingStorageResource(local_dir=str(alias)),CAPTURE)

    def test_relative_root_allowed_and_space_reserve_precedes_acquisition(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as d:
            root=Path(d)/'relative-staging';storage=StagingStorageResource(local_dir=os.path.relpath(root,Path.cwd()))
            s.require_local(storage);self.assertFalse(root.exists())
            low=type('DiskUsage',(),{'free':s.LOCAL_FREE_FLOOR-1})()
            with patch.object(s.shutil,'disk_usage',return_value=low),patch.object(s,'acquire') as acquire,self.assertRaisesRegex(ValueError,'local_space_guard_before_source_read'):
                s.stage_cbrs_buffer_candidate(storage,capture_dir=CAPTURE)
            acquire.assert_not_called();self.assertFalse(root.exists())

    def test_all_receipts_preflight_strict_timestamp_url_pins(self):
        captures=self.structural_captures()
        self.assertEqual(len(s.provenance(captures)['inputs']),4)
        for name in s.URLS:
            for field,value in [('requested_url','https://example.org/substitute'),('final_url','https://example.org/redirect'),('observed_at','not-a-date'),('observed_at','2026-10-19T00:00:00'),('observed_at','2026-10-19T00:00:00+00:99'),('observed_at','2026-10-19T00:00:00+99:00'),('observed_at',None),('byte_length',True),('sha256','0'*64),('status',True)]:
                changed={n:(body,dict(receipt)) for n,(body,receipt) in captures.items()};changed[name][1][field]=value
                with self.subTest(name=name,field=field,value=value),tempfile.TemporaryDirectory() as d,patch.object(s,'acquire',return_value=changed),self.assertRaises((ValueError,TypeError)):
                    try:s.stage_cbrs_buffer_candidate(StagingStorageResource(local_dir=d),capture_dir=CAPTURE)
                    finally:self.assertFalse(Path(d,s.SLUG).exists())

    def test_strict_json_error_array_nested_duplicate_nonfinite(self):
        for body in [b'[]',b'{"error":{}}',b'{"a":1,"a":2}',b'{"a":{"b":1,"b":2}}',b'{"a":NaN}']:
            with self.assertRaises(ValueError):s.strict_json(body)

    def test_bad_receipt_exact_bytes_retained_and_no_candidate(self):
        with tempfile.TemporaryDirectory() as d:
            capture=Path(d,'capture');capture.mkdir();body=b'SYNTHETIC complete diagnostic response'
            (capture/'official_zip.body').write_bytes(body);bad=b'{"malformed":'
            (capture/'official_zip.receipt.json').write_bytes(bad);stage=Path(d,'stage')
            with self.assertRaises(ValueError):s.acquire(StagingStorageResource(local_dir=str(stage)),capture)
            self.assertTrue(any(p.read_bytes()==bad for p in stage.rglob('original-receipt.body')))
            self.assertTrue(any(p.read_bytes()==body for p in stage.rglob('response.body')))
            self.assertFalse((stage/s.SLUG).exists())

    def test_partial_each_attempt_retained(self):
        original_client=s.httpx.Client
        class Partial(httpx.SyncByteStream):
            def __init__(self,block):self.block=block
            def __iter__(self):yield self.block;raise httpx.ReadError('synthetic reset')
        for block,headers in [(b'partial',{}),(gzip.compress(b'SYNTHETIC gzip response')[:18],{'Content-Encoding':'gzip'})]:
            def response(request):return httpx.Response(200,headers=headers,stream=Partial(block))
            def client(**kw):return original_client(transport=httpx.MockTransport(response),**kw)
            with self.subTest(headers=headers),tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',side_effect=client) as c:
                with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):s.acquire(StagingStorageResource(local_dir=d),None)
                self.assertEqual(c.call_count,3)
                wires=list(Path(d).rglob('wire.body'));self.assertEqual(len(wires),3)
                self.assertTrue(all(p.read_bytes()==block for p in wires))
                for p in Path(d).rglob('receipt.json'):
                    receipt=json.loads(p.read_bytes())
                    if 'transfer' in receipt:
                        self.assertFalse(receipt['transfer']['complete'])
                        self.assertEqual(receipt['wire_evidence']['bytes'],len(block))
                self.assertEqual(len(list(Path(d).rglob('response.body'))),3)
                self.assertFalse(Path(d,s.SLUG).exists())

    def test_wire_overflow_keeps_entire_delivered_block(self):
        original_client=s.httpx.Client;block=b'SYNTHETIC-'+b'x'*58
        def response(request):return httpx.Response(200,stream=httpx.ByteStream(block))
        def client(**kw):return original_client(transport=httpx.MockTransport(response),**kw)
        with tempfile.TemporaryDirectory() as d,patch.object(s,'MAX_BYTES',36),patch.object(s.httpx,'Client',side_effect=client):
            with self.assertRaisesRegex(ValueError,'wire_limit_exceeded'):s.acquire(StagingStorageResource(local_dir=d),None)
            wires=list(Path(d).rglob('wire.body'));self.assertEqual(len(wires),3)
            self.assertTrue(all(p.read_bytes()==block for p in wires))
            self.assertFalse(Path(d,s.SLUG).exists())

    def test_atomic_interrupt_and_oserror_never_expose_version(self):
        for failure in [OSError('disk'),KeyboardInterrupt()]:
            with self.subTest(failure=type(failure).__name__),tempfile.TemporaryDirectory() as d:
                storage=StagingStorageResource(local_dir=d)
                target='cbrs-buffer-zone-1/cbrs-buffer-zone-1/v0.0.0-draft-fixture'
                payloads,ready,manifest=self.atomic_fixture()
                with patch.object(s,'original_provenance_bundle',return_value=({},manifest)),patch.object(s.os,'rename',side_effect=failure),self.assertRaises(type(failure)):
                    s.commit_complete_candidate(storage,target,payloads,ready)
                self.assertFalse(Path(d,target).exists())
                self.assertFalse(list(Path(d).rglob('.candidate-build-*')))

    def test_failed_private_payload_or_ready_write_cannot_establish_first_binding(self):
        for failed_key in ['geopackage/test.gpkg','metadata/cbrs_buffer_candidate_ready.json']:
            for failure in [OSError('private write failed'),KeyboardInterrupt()]:
                with self.subTest(key=failed_key,failure=type(failure).__name__),tempfile.TemporaryDirectory() as d:
                    storage=StagingStorageResource(local_dir=d);payloads,ready,_=self.atomic_fixture()
                    original=s.immutable
                    def write(resource,key,body):
                        if key==failed_key:raise failure
                        return original(resource,key,body)
                    with patch.object(s,'immutable',side_effect=write),patch.object(s,'original_provenance_bundle') as binding,self.assertRaises(type(failure)):
                        s.commit_complete_candidate(storage,s.SLUG+'/'+s.SLUG+'/v0.0.0-fixture',payloads,ready)
                    binding.assert_not_called()
                    self.assertFalse(Path(d,'_source-provenance').exists())
                    self.assertFalse(list(Path(d).rglob('.candidate-build-*')))
                    self.assertFalse(Path(d,s.SLUG,s.SLUG,'v0.0.0-fixture').exists())

    @REAL_INPUTS
    def test_authentic_complete_source_first_native_writer_failure_has_no_binding(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d)
            with patch.object(s,'write_native_geopackage',side_effect=OSError('first native writer failed')),self.assertRaisesRegex(ValueError,'first native writer failed'):
                s.stage_cbrs_buffer_candidate(storage,capture_dir=CAPTURE)
            self.assertFalse(Path(d,'_source-provenance').exists())
            self.assertFalse(list(Path(d,s.SLUG).rglob('cbrs_buffer_candidate_ready.json')))
            self.assertTrue(any(s.sha256_file(p)==s.PIN for p in Path(d,'_raw-captures').rglob('official_zip.body')))

    def test_archive_pin_and_zip_roster_changes_fail_before_decode(self):
        for changed in [b'PK\x03\x04SYNTHETIC unknown archive',b'error',b'']:
            with tempfile.TemporaryDirectory() as d,patch.object(s.fiona,'open') as opened,self.assertRaisesRegex(ValueError,'changed_truncated'):
                s.inspect_archive(changed,Path(d))
            opened.assert_not_called()

    def test_synthetic_pin_still_rejects_missing_zip_member_before_native_decode(self):
        output=io.BytesIO()
        with zipfile.ZipFile(output,'w') as archive:archive.writestr('unexpected.txt',b'SYNTHETIC only')
        body=output.getvalue()
        with tempfile.TemporaryDirectory() as d,patch.object(s,'PIN',s.sha(body)),patch.object(s,'ARCHIVE_BYTES',len(body)),patch.object(s.fiona,'open') as native,self.assertRaisesRegex(ValueError,'unexpected_missing_duplicate_archive_member'):
            s.inspect_archive(body,Path(d))
        native.assert_not_called()

    def test_missing_extra_type_id_and_geometry_drift(self):
        body=self.structural_archive();fixture=json.loads(FIXTURE.read_text())
        rows=[{'properties':{**fixture['properties'],'__source_fid':str(i),'BUFF_DIST':20.0 if i<1138 else 0.0},'geometry':fixture['geometry']} for i in range(1145)]
        class Row(dict):
            def __init__(self,r):
                super().__init__(properties={k:v for k,v in r['properties'].items() if k!='__source_fid'},geometry={'type':'Polygon','coordinates':[[[-70.0,40.0],[-70.0,41.0],[-69.0,40.0],[-70.0,40.0]]]})
                self.id=r['properties']['__source_fid']
        class Stream:
            schema={'properties':s.NATIVE_SCHEMA,'geometry':'Polygon'};crs='EPSG:4269'
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def __iter__(self):return iter(self.rows)
        for mode in ['missing','extra','type','null','id','geometry','coverage']:
            mutated=[Row(r) for r in rows]
            if mode=='missing':del mutated[0]['properties']['BUFF_DIST']
            if mode=='extra':mutated[0]['properties']['surprise']=1
            if mode=='type':mutated[0]['properties']['BUFF_DIST']=20
            if mode=='null':mutated[0]['properties']['BUFF_DIST']=None
            if mode=='id':mutated[0].id=mutated[1].id
            if mode=='geometry':mutated[0]['geometry']=None
            if mode=='coverage':mutated[0]['properties']['BUFF_DIST']=10.0
            fake=Stream();fake.rows=mutated
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as d,patch.object(s,'PIN',s.sha(body)),patch.object(s,'ARCHIVE_BYTES',len(body)),patch.object(s.fiona,'open',return_value=fake),self.assertRaises(ValueError):
                s.inspect_archive(body,Path(d))

    def test_atomic_target_does_not_replace_existing_release(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);prefix='a/a/v0.0.0-fixture'
            payloads,ready,manifest=self.atomic_fixture('geopackage/a.gpkg',b'prior')
            with patch.object(s,'original_provenance_bundle',return_value=({},manifest)):
                s.commit_complete_candidate(storage,prefix,payloads,ready)
            with self.assertRaises(ValueError):s.commit_complete_candidate(storage,prefix,{'geopackage/a.gpkg':b'new'}, {})
            self.assertEqual(Path(d,prefix,'geopackage/a.gpkg').read_bytes(),b'prior')

    def test_every_auxiliary_geopackage_byte_is_bound(self):
        rows=[{'type':'Feature','properties':{'BUFF_DIST':20.0,'Shape_Leng':1.0,'Shape_Area':0.5,'__source_fid':'0'},
               'geometry':{'type':'Polygon','coordinates':[[[-70.0,40.0],[-70.0,41.0],[-69.0,40.0],[-70.0,40.0]]]}}]
        mutations=["UPDATE gpkg_contents SET srs_id=4326", "UPDATE gpkg_spatial_ref_sys SET definition='contradictory' WHERE srs_id=4269",
                   "UPDATE gpkg_spatial_ref_sys SET organization_coordsys_id=4326 WHERE srs_id=4269",
                   "CREATE TABLE unreviewed_metadata(value TEXT)", "UPDATE gpkg_contents SET min_x=-180",
                   "UPDATE rtree_cbrs-buffer-zone-1_geom SET minx=-180"]
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'original.gpkg';s.write_native_geopackage(rows,path);original=path.read_bytes()
            s.verify_canonical_geopackage(path,rows)
            for sql in mutations:
                with self.subTest(sql=sql):
                    path.write_bytes(original)
                    with sqlite3.connect(path) as db:
                        if sql.startswith('UPDATE rtree_'):
                            table=db.execute("SELECT name FROM sqlite_master WHERE name LIKE 'rtree_%_geom' AND type='table'").fetchone()[0]
                            db.execute('UPDATE "'+table+'" SET minx=-180')
                        else:db.execute(sql)
                    with self.assertRaisesRegex(ValueError,'container_bytes_disagree'):s.verify_canonical_geopackage(path,rows)
            path.write_bytes(original);data=bytearray(original);data[-1]^=1;path.write_bytes(data)
            with self.assertRaisesRegex(ValueError,'container_bytes_disagree'):s.verify_canonical_geopackage(path,rows)

    def test_rejected_cache_moves_intact_out_of_discovery(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);prefix=s.SLUG+'/'+s.SLUG+'/v0.0.0-fixture'
            root=Path(d)/prefix;(root/'geopackage').mkdir(parents=True);(root/'geopackage/corrupted.gpkg').write_bytes(b'corrupt')
            (root/'raw').mkdir();(root/'raw/original.zip').write_bytes(b'preserved original')
            q=s.quarantine_rejected_cache(storage,prefix,'test')
            self.assertFalse(root.exists());destination=Path(d)/q/root.name
            self.assertEqual((destination/'geopackage/corrupted.gpkg').read_bytes(),b'corrupt')
            self.assertEqual((destination/'raw/original.zip').read_bytes(),b'preserved original')

    def test_complete_fidelity_report_sidecars_wrappers_and_blocker_are_bound(self):
        rows=[{'type':'Feature','properties':{'BUFF_DIST':20.0,'Shape_Leng':1.0,'Shape_Area':0.5,'__source_fid':'0'},
               'geometry':{'type':'Polygon','coordinates':[[[-70.0,40.0],[-70.0,41.0],[-69.0,40.0],[-70.0,40.0]]]}}]
        records=b''.join(s.encode(r) for r in rows);fingerprint='synthetic-contract-fixture-not-real-source-approval'
        expected=s.fidelity_payloads(records,fingerprint)
        self.assertEqual(len(expected),7)
        report=json.loads(expected['metadata/source_fidelity/baseline_comparison.json'])
        self.assertFalse(report['baseline']['approved']);self.assertEqual(report['populations']['candidate']['rows'],1)
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            for name,body in expected.items():(root/name).parent.mkdir(parents=True,exist_ok=True);(root/name).write_bytes(body)
            s.validate_fidelity_payloads(root,records,fingerprint)
            for name,body in expected.items():
                with self.subTest(name=name):
                    (root/name).write_bytes(body+b'forged claim')
                    with self.assertRaisesRegex(ValueError,'cached_source_fidelity_evidence'):s.validate_fidelity_payloads(root,records,fingerprint)
                    (root/name).write_bytes(body)
            # Independent fresh replay must derive the entire same report and all sidecars.
            s._FIDELITY_PAYLOAD_CACHE.clear();self.assertEqual(s.fidelity_payloads(records,fingerprint),expected)

    def test_compressed_wire_length_and_incomplete_wire_response(self):
        original_client=s.httpx.Client
        plain=b'complete decoded producer response'*100
        compressed=gzip.compress(plain)
        def response(request):
            self.assertEqual(request.headers['Accept-Encoding'],'identity')
            return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(compressed))},stream=httpx.ByteStream(compressed))
        def client(**kw):return original_client(transport=httpx.MockTransport(response),**kw)
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',side_effect=client):
            captures=s.acquire(StagingStorageResource(local_dir=d),None)
            self.assertEqual(len(captures),4)
            for body,receipt in captures.values():
                self.assertEqual(body,plain);self.assertEqual(receipt['received_wire_bytes'],len(compressed))
                self.assertNotEqual(receipt['received_wire_bytes'],receipt['byte_length'])
        def truncated(request):return httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(compressed))},stream=httpx.ByteStream(compressed[:-4]))
        def badclient(**kw):return original_client(transport=httpx.MockTransport(truncated),**kw)
        with tempfile.TemporaryDirectory() as d,patch.object(s.httpx,'Client',side_effect=badclient),self.assertRaisesRegex(ValueError,'compressed_stream_incomplete'):
            try:s.acquire(StagingStorageResource(local_dir=d),None)
            finally:self.assertEqual(len(list(Path(d).rglob('report.json'))),3)

    def test_original_provenance_pin_retains_first_commit_and_receipts(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d)
            captures=self.structural_captures(storage);manifest=s.provenance(captures)
            manifest['processing_fingerprint']='test-fixture-not-acceptance'
            processing_files=s.processing_identity()['files']
            version='v0.0.0-fixture';prefix=s.SLUG+'/'+s.SLUG+'/'+version
            with s.candidate_lock(storage,prefix):
                with self.assertRaisesRegex(ValueError,'missing_original_construction'):
                    s.original_provenance_bundle(storage,version,captures,manifest,processing_files)
                original,first=s.original_provenance_bundle(storage,version,captures,manifest,processing_files,create=True)
            key=f'_source-provenance/{s.SLUG}/{version}/bundle.json';pin=storage.read_key(key)
            changed={n:(body,{**receipt,'observed_at':'2026-10-20T01:02:03+00:00'}) for n,(body,receipt) in captures.items()}
            # Real construction may precede later docs-only HEADs and observations.
            later=s.provenance(changed);later['processing_fingerprint']=manifest['processing_fingerprint']
            later['code_commit']='a'*40 # Explicit hypothetical later context; never changes the pinned original.
            with s.candidate_lock(storage,prefix):
                reused,replayed=s.original_provenance_bundle(storage,version,changed,later,processing_files)
                rebuilt,reconstructed=s.original_provenance_bundle(storage,version,changed,later,processing_files,create=True)
            # Co-forging both records cannot make a nonexistent or wrong-code Git identity valid.
            bundle=s.strict_json(pin)
            for commit in ['0'*40,'6969d05028352eefd20348a738a829034af9882f']:
                bundle['manifest']['code_commit']=commit;Path(d,key).write_bytes(s.encode(bundle))
                with self.assertRaisesRegex(ValueError,'construction_'):
                    s.original_provenance_bundle(storage,version,captures,manifest,processing_files)
            Path(d,key).write_bytes(pin)
            self.assertEqual(first,manifest);self.assertEqual(replayed,first);self.assertEqual(reconstructed,first)
            self.assertEqual(reused,original);self.assertEqual(rebuilt,original)
            self.assertEqual(storage.read_key(key),pin)
            self.assertNotEqual(replayed['code_commit'],'a'*40)
            self.assertTrue(all(reused[n][1]['observed_at']!=changed[n][1]['observed_at'] for n in s.URLS))

    def test_first_marker_storage_fault_retains_orphan_and_blocks_new_context(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);captures=self.structural_captures(storage)
            manifest=s.provenance(captures);manifest['processing_fingerprint']='partial-anchor-fixture-not-acceptance'
            files=s.processing_identity()['files'];version='v0.0.0-orphan-fixture';prefix=s.SLUG+'/'+s.SLUG+'/'+version
            original=StagingStorageResource.write_key_if_unchanged
            def fail_marker(resource,key,body,expected):
                if key.endswith('.first-construction.json'):raise OSError('first marker storage failure')
                return original(resource,key,body,expected)
            with patch.object(StagingStorageResource,'write_key_if_unchanged',new=fail_marker),s.candidate_lock(storage,prefix),self.assertRaisesRegex(OSError,'first marker storage failure'):
                s.original_provenance_bundle(storage,version,captures,manifest,files,create=True)
            key=Path(d,f'_source-provenance/{s.SLUG}/{version}/bundle.json')
            evidence=list(key.with_name(key.name+'.evidence').glob('*.json'));self.assertEqual(len(evidence),1)
            body=evidence[0].read_bytes();self.assertEqual(s.strict_json(body)['manifest'],manifest)
            self.assertFalse(key.exists());self.assertFalse(key.with_name(key.name+'.first-construction.json').exists())
            changed={n:(b,{**receipt,'observed_at':'2026-10-20T01:02:03+00:00'}) for n,(b,receipt) in captures.items()}
            later=s.provenance(changed);later['processing_fingerprint']=manifest['processing_fingerprint']
            for retry in [1,2]:
                with self.subTest(retry=retry),s.candidate_lock(storage,prefix),self.assertRaisesRegex(ValueError,'orphaned_original_construction_evidence_without_history'):
                    s.original_provenance_bundle(storage,version,changed,later,files,create=True)
                self.assertFalse(key.exists());self.assertEqual(evidence[0].read_bytes(),body)

    def test_first_construction_marker_blocks_missing_alias_and_marker_on_repeated_calls(self):
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);captures=self.structural_captures(storage)
            manifest=s.provenance(captures);manifest['processing_fingerprint']='marker-fixture-not-acceptance'
            files=s.processing_identity()['files'];version='v0.0.0-marker-fixture'
            prefix=s.SLUG+'/'+s.SLUG+'/'+version
            with s.candidate_lock(storage,prefix):
                s.original_provenance_bundle(storage,version,captures,manifest,files,create=True)
            key=Path(d,f'_source-provenance/{s.SLUG}/{version}/bundle.json')
            marker=key.with_name(key.name+'.first-construction.json')
            original=key.read_bytes();first=marker.read_bytes();claim=s.strict_json(first)
            evidence=Path(d,claim['original_bundle_evidence_key']);self.assertEqual(evidence.read_bytes(),original)
            changed={n:(body,{**receipt,'observed_at':'2026-10-20T01:02:03+00:00'}) for n,(body,receipt) in captures.items()}
            later=s.provenance(changed);later['processing_fingerprint']=manifest['processing_fingerprint']
            key.unlink()
            for retry in [1,2]:
                with self.subTest(mode='missing_alias',retry=retry),s.candidate_lock(storage,prefix),self.assertRaisesRegex(ValueError,'missing_original_construction_bundle_after_first_construction'):
                    s.original_provenance_bundle(storage,version,changed,later,files,create=True)
                self.assertFalse(key.exists());self.assertEqual(marker.read_bytes(),first)
            key.write_bytes(original) # Explicit restoration of saved authentic original, never inferred repair.
            for mode,body in [('missing',None),('invalid',b'{}'),('malformed',b'{'),
                              ('boolean_schema_version',s.encode({**claim,'schema_version':True})),
                              ('floating_schema_version',s.encode({**claim,'schema_version':1.0})),
                              ('noncanonical_bytes',first+b'\n')]:
                if body is None:marker.unlink()
                else:marker.write_bytes(body)
                for retry in [1,2]:
                    with self.subTest(mode=mode,retry=retry),s.candidate_lock(storage,prefix),self.assertRaises(ValueError):
                        s.original_provenance_bundle(storage,version,changed,later,files,create=True)
                    self.assertEqual(key.read_bytes(),original)
                    self.assertEqual(marker.read_bytes() if marker.exists() else None,body)
                marker.write_bytes(first)
            with s.candidate_lock(storage,prefix):
                replay,replayed=s.original_provenance_bundle(storage,version,changed,later,files,create=True)
            self.assertEqual(replayed,manifest);self.assertEqual(replay,captures)
            self.assertEqual(evidence.read_bytes(),original);self.assertEqual(marker.read_bytes(),first)
            for mode,body in [('missing_CAS',None),('changed_CAS',b'forged evidence bytes')]:
                if body is None:evidence.unlink()
                else:evidence.write_bytes(body)
                for retry in [1,2]:
                    with self.subTest(mode=mode,retry=retry),s.candidate_lock(storage,prefix),self.assertRaisesRegex(ValueError,'missing_or_changed_original_construction_evidence'):
                        s.original_provenance_bundle(storage,version,changed,later,files,create=True)
                    self.assertEqual(key.read_bytes(),original);self.assertEqual(marker.read_bytes(),first)
                evidence.write_bytes(original) # Explicit restoration, never automatic evidence repair.

    def test_relative_root_observed_loss_is_canonical_refusal_not_authority(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as d:
            storage=StagingStorageResource(local_dir=os.path.relpath(Path(d)/'staging',Path.cwd()))
            candidate=Path(d)/'candidate';(candidate/'metadata').mkdir(parents=True)
            manifest={'synthetic_fixture':True};(candidate/'metadata/source_manifest.json').write_bytes(s.encode(manifest))
            version='v0.0.0-synthetic-loss';fingerprint='a'*64
            self.assertTrue(s.observed_trust_loss(storage,version,fingerprint,candidate=candidate))
            path=Path(d)/'staging/_construction-incidents'/s.SLUG/version/'incident.json';original=path.read_bytes()
            self.assertTrue(s.observed_trust_loss(storage,version,fingerprint,restored_manifest=manifest))
            with self.assertRaisesRegex(ValueError,'restored_original_manifest_differs'):
                s.observed_trust_loss(storage,version,fingerprint,restored_manifest={'synthetic_fixture':False})
            claim=s.strict_json(original)
            for body in [s.encode({**claim,'schema_version':True}),s.encode({**claim,'schema_version':1.0}),original+b'\n']:
                path.write_bytes(body)
                for retry in [1,2]:
                    with self.subTest(body=body,retry=retry),self.assertRaisesRegex(ValueError,'invalid_observed_trust_loss_journal'):
                        s.observed_trust_loss(storage,version,fingerprint)
                    self.assertEqual(path.read_bytes(),body)
            path.write_bytes(original)
            self.assertTrue(s.observed_trust_loss(storage,version,fingerprint,restored_manifest=manifest))

    def test_failed_observed_loss_journal_preserves_untrusted_candidate_on_retries(self):
        captures=self.structural_captures();identity={'synthetic':True,'files':{}}
        fingerprint=s.sha(s.encode(identity));capture_pin=s.sha(s.encode({n:s.sha(body)for n,(body,_)in captures.items()}))
        version='v0.0.0-draft-cbrs20241219-'+capture_pin[:16]+'-'+fingerprint[:16]
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);candidate=Path(d)/s.SLUG/s.SLUG/version
            (candidate/'metadata').mkdir(parents=True);before=b'SYNTHETIC untrusted candidate bytes'
            (candidate/'metadata/source_manifest.json').write_bytes(before);original=s.immutable
            def fault(resource,key,body):
                if key.endswith('/incident.json'):raise OSError('injected journal commit failure')
                return original(resource,key,body)
            with patch.object(s,'acquire',return_value=captures),patch.object(s,'inspect_archive',return_value=([],{},{})),patch.object(s,'processing_identity',return_value=identity):
                with patch.object(s,'immutable',side_effect=fault),self.assertRaisesRegex(ValueError,'trust_loss_journal_failed_candidate_retained_untrusted'):
                    s.stage_cbrs_buffer_candidate(storage)
                for retry in [1,2]:
                    with self.subTest(retry=retry),self.assertRaisesRegex(ValueError,'incomplete_observed_trust_loss_journal'):
                        s.stage_cbrs_buffer_candidate(storage)
                    self.assertEqual((candidate/'metadata/source_manifest.json').read_bytes(),before)
                self.assertFalse(list(Path(d,'_quarantine',s.SLUG,'rejected-cache').rglob(version)))
                self.assertTrue((Path(d)/'_construction-incidents'/s.SLUG/version).is_dir())

    def test_synthetic_whole_witness_loss_refuses_retry_and_restores_exact_original(self):
        # Only acquisition/decoding use tiny SYNTHETIC inputs. Native writes,
        # complete reports, Git/state/locks and atomic installation are real.
        fixture=json.loads(FIXTURE.read_text());rows=[{'type':'Feature','properties':fixture['properties'],'geometry':fixture['geometry']}]
        audit={'bounds':[-70.,40.,-69.,41.],'invalid_geometry_count':0}
        members={name:b'SYNTHETIC structural member' for name in s.MEMBERS}
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);captures=self.structural_captures(storage)
            with patch.object(s,'acquire',return_value=captures),patch.object(s,'inspect_archive',return_value=(rows,audit,members)):
                first=s.stage_cbrs_buffer_candidate(storage)
            version=first['version'];root=Path(d)/s.SLUG/s.SLUG/version
            manifest=(root/'metadata/source_manifest.json').read_bytes();ready=s.strict_json((root/'metadata/cbrs_buffer_candidate_ready.json').read_bytes())
            folder=Path(d)/'_source-provenance'/s.SLUG/version;trusted=Path(d)/'trusted-original';shutil.move(str(folder),str(trusted))
            later={n:(body,{**receipt,'observed_at':'2026-10-20T01:02:03+00:00'})for n,(body,receipt)in captures.items()}
            with patch.object(s,'acquire',return_value=later),patch.object(s,'inspect_archive',return_value=(rows,audit,members)):
                for retry in [1,2,3]:
                    with self.subTest(retry=retry),self.assertRaisesRegex(ValueError,'missing_original_construction'):
                        s.stage_cbrs_buffer_candidate(storage)
                    self.assertFalse(root.exists());self.assertFalse(folder.exists())
                incident=Path(d)/'_construction-incidents'/s.SLUG/version/'incident.json';refusal=incident.read_bytes()
                shutil.copytree(trusted,folder) # Explicit saved-original restoration, never inferred trust.
                rebuilt=s.stage_cbrs_buffer_candidate(storage)
            self.assertEqual(rebuilt['feature_count'],1);self.assertEqual((root/'metadata/source_manifest.json').read_bytes(),manifest)
            self.assertTrue(all(s.sha256_file(root/name)==pin for name,pin in ready['objects'].items()))
            self.assertEqual(incident.read_bytes(),refusal)

    def test_invalid_lookup_quarantine_and_concurrent_valid_winner_share_lock(self):
        # These are explicitly synthetic implementation fixtures, not dataset approval.
        captures={'official_zip':(b'synthetic-body',{})};identity={'fixture':True,'files':{}}
        fp=s.sha(s.encode(identity));cp=s.sha(s.encode({'official_zip':s.sha(b'synthetic-body')}))
        prefix=s.SLUG+'/'+s.SLUG+'/v0.0.0-draft-cbrs20241219-'+cp[:16]+'-'+fp[:16]
        held=threading.Event();release=threading.Event();started=threading.Event();winner=threading.Event();errors=[]
        original_q=s.quarantine_rejected_cache_held;original_diag=s.quarantine
        with tempfile.TemporaryDirectory() as d:
            storage=StagingStorageResource(local_dir=d);target=Path(d)/prefix
            (target/'geopackage').mkdir(parents=True);(target/'geopackage/incomplete.gpkg').write_bytes(b'old incomplete')
            def pause_q(*args):
                held.set();self.assertTrue(release.wait(10));return original_q(*args)
            def diagnostic(*args):
                self.assertTrue(winner.wait(10));return original_diag(*args)
            def source():
                try:s.stage_cbrs_buffer_candidate(storage)
                except Exception as e:errors.append(str(e))
            def install():
                payloads,ready,manifest=self.atomic_fixture('geopackage/winner.gpkg',b'valid winner fixture')
                started.set()
                with patch.object(s,'original_provenance_bundle',return_value=({},manifest)):
                    s.commit_complete_candidate(storage,prefix,payloads,ready)
                winner.set()
            with patch.object(s,'acquire',return_value=captures),patch.object(s,'provenance',return_value={}),patch.object(s,'inspect_archive',return_value=([],{},{})),patch.object(s,'processing_identity',return_value=identity),patch.object(s,'quarantine_rejected_cache_held',side_effect=pause_q),patch.object(s,'quarantine',side_effect=diagnostic):
                a=threading.Thread(target=source);a.start();self.assertTrue(held.wait(10))
                b=threading.Thread(target=install);b.start();self.assertTrue(started.wait(10))
                self.assertFalse(winner.wait(.15));release.set();a.join(10);b.join(10)
                self.assertFalse(a.is_alive());self.assertFalse(b.is_alive())
            self.assertTrue(winner.is_set());self.assertTrue(any('missing_original_construction' in x for x in errors))
            self.assertEqual((target/'geopackage/winner.gpkg').read_bytes(),b'valid winner fixture')
            moved=list(Path(d,'_quarantine',s.SLUG,'rejected-cache').rglob('incomplete.gpkg'))
            self.assertEqual(len(moved),1);self.assertEqual(moved[0].read_bytes(),b'old incomplete')

if __name__=='__main__':unittest.main()
