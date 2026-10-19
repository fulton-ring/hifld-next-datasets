"""Complete real-input audit and explicitly synthetic two-row lifecycle fixtures.

Synthetic fixtures test implementation behavior; they do not approve Census data.
"""
import copy
import hashlib
import gzip
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry import MultiPolygon, Polygon

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import county_subdivisions as source


@contextmanager
def synthetic_population():
    with tempfile.TemporaryDirectory(prefix='cousub-unit-') as tmp:
        root=Path(tmp);body=b'EXPLICIT_SYNTHETIC_COMPLETE_2ROW_INPUT';raw=root/'source.bin';raw.write_bytes(body)
        receipt={'url':source.URL,'final_url':source.URL,'status':200,'observed_at':'2026-10-19T12:00:00+00:00','bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'complete_body':True}
        receipt_path=root/'receipt.json';receipt_path.write_bytes(source.encode(receipt))
        rows=[]
        for i in [1,2]:
            geoid='010019000'+str(i)
            rows.append({'OBJECTID':i,'COUSUBNS':'0000000'+str(i),'GEOID':geoid,'GEOIDFQ':'0600000US'+geoid,'NAMELSAD':'Fixture '+str(i),'CLASSFP':'Z5','FUNCSTAT':'S','ALAND':float(i*100),'AWATER':0.0,'INTPTLAT':'+32.0000000','INTPTLON':'-86.0000000','SHAPE':shapely.to_wkb(MultiPolygon([Polygon([(i,0),(i+1,0),(i+1,1),(i,1),(i,0)])]))})
        schema=pa.schema([pa.field(n,t) for n,t in zip(source.FIELDS,(pa.int64(),*source.TYPES[1:]))]);table=pa.Table.from_pylist(rows,schema=schema)
        @contextmanager
        def reader(_):
            yield {'crs':'EPSG:4269','geometry_type':'MultiPolygon','fid_column':'OBJECTID'},pa.RecordBatchReader.from_batches(table.schema,table.to_batches(max_chunksize=1))
        physical={'members':{},'native_layer_definition':'EXPLICIT_SYNTHETIC','physical_fields':{}}
        repo=Path(source.__file__).parents[3];f='src/dagster_hifld/construction_state.py';identity={'files':{f:source.sha256_file(repo/f)},'test_scope':'synthetic two-row behavior, not source acceptance'}
        storage=StagingStorageResource(local_dir=str(root/'staging'))
        with patch.multiple(source,BYTES=len(body),SHA256=hashlib.sha256(body).hexdigest(),COUNT=2,STATE_COUNTS={'01':2}),patch.object(source,'source_reader',reader),patch.object(source,'native_definition',return_value=physical),patch.object(source,'processing_identity',return_value=identity):
            yield root,storage,raw,receipt_path,table


class CountySubdivisionTests(unittest.TestCase):
    def stage(self,storage,raw,receipt):return source.stage_county_subdivisions_candidate(storage,archive_path=raw,receipt_path=receipt)

    def test_actual_httpx_gzip_wire_length_and_lossless_decoded_capture(self):
        body=b'EXPLICIT_SYNTHETIC_GZIP_TRANSPORT_BEHAVIOR'*100;compressed=gzip.compress(body,mtime=0);original_client=httpx.Client
        transport=httpx.MockTransport(lambda request:httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(compressed))},stream=httpx.ByteStream(compressed)))
        with tempfile.TemporaryDirectory() as tmp,patch.multiple(source,BYTES=len(body),SHA256=hashlib.sha256(body).hexdigest(),RAW_KEY='_raw-captures/synthetic/original.zip'),patch.object(source.httpx,'Client',side_effect=lambda **kwargs:original_client(transport=transport,**kwargs)):
            storage=StagingStorageResource(local_dir=str(Path(tmp)/'staging'));path,receipt,pointer=source.acquire(storage)
            self.assertEqual(path.read_bytes(),body);self.assertEqual(receipt['wire_bytes_downloaded'],len(compressed));self.assertEqual(receipt['bytes'],len(body));self.assertTrue(receipt['complete_body']);self.assertEqual(pointer['kind'],'owned_content_addressed_capture')

    def test_actual_httpx_truncated_wire_retains_partial_and_blocks(self):
        body=b'EXPLICIT_SYNTHETIC_SHORT_ENCODED_TRANSPORT'*100;compressed=gzip.compress(body,mtime=0);original_client=httpx.Client
        transport=httpx.MockTransport(lambda request:httpx.Response(200,headers={'Content-Encoding':'gzip','Content-Length':str(len(compressed)+7)},stream=httpx.ByteStream(compressed)))
        with tempfile.TemporaryDirectory() as tmp,patch.multiple(source,BYTES=len(body),SHA256=hashlib.sha256(body).hexdigest(),RAW_KEY='_raw-captures/synthetic/original.zip'),patch.object(source.httpx,'Client',side_effect=lambda **kwargs:original_client(transport=transport,**kwargs)):
            storage=StagingStorageResource(local_dir=str(Path(tmp)/'staging'))
            with self.assertRaisesRegex(ValueError,'encoded_transport_truncation'):source.acquire(storage)
            self.assertFalse((Path(storage.local_dir)/source.RAW_KEY).exists());partials=list(Path(storage.local_dir).rglob('*.partial'));self.assertEqual(len(partials),1);self.assertEqual(partials[0].read_bytes(),body)
            reports=list((Path(storage.local_dir)/'_quarantine').rglob('*.json'));self.assertEqual(len(reports),1);self.assertFalse(source.strict_json(reports[0].read_bytes())['receipt']['complete_body'])

    def test_complete_real_population_schema_and_physical_width(self):
        raw=os.environ.get('HIFLD_COUSUB_TEST_ARCHIVE')
        if not raw:self.skipTest('Set HIFLD_COUSUB_TEST_ARCHIVE for complete preserved real Census source audit')
        path=Path(raw);receipt=source.strict_json(Path(os.environ['HIFLD_COUSUB_TEST_RECEIPT']).read_bytes());source.validate_receipt(path,receipt);audit=source.source_audit(path)
        self.assertEqual(audit['rows'],36492);self.assertEqual(len(audit['state_counts']),56);self.assertEqual(audit['native_wkb_bytes'],489108707);self.assertEqual(audit['physical_fields']['OBJECTID']['Length'],'4');self.assertEqual(audit['native_attributes'],11)
        self.assertEqual(audit['state_counts']['60'],16);self.assertEqual(audit['state_counts']['69'],12)

    def test_native_width_values_geometry_refresh_and_no_raw_copy(self):
        with synthetic_population() as (_,storage,raw,receipt,_):
            first=self.stage(storage,raw,receipt);second=self.stage(storage,raw,receipt);self.assertFalse(first['reused']);self.assertTrue(second['reused'])
            root=Path(storage.local_dir)/source.SLUG/source.SLUG/first['version'];t=pq.read_table(root/'geoparquet'/f'{source.SLUG}.parquet');self.assertEqual(t.schema.field('OBJECTID').type,pa.int32());self.assertEqual(t['COUSUBNS'].to_pylist(),['00000001','00000002']);self.assertFalse((Path(storage.local_dir)/source.RAW_KEY).exists());self.assertEqual(source.strict_json((root/'raw/source_capture.json').read_bytes())['kind'],'external_preserved_read_only_capture')

    def test_source_drift_preserves_original_and_prior_output(self):
        with synthetic_population() as (root,storage,raw,receipt,_):
            prior=root/'prior';prior.write_bytes(b'accepted fixture');raw.write_bytes(b'changed source')
            with self.assertRaisesRegex(ValueError,'rejected_after_observation'):self.stage(storage,raw,receipt)
            self.assertEqual(raw.read_bytes(),b'changed source');self.assertEqual(prior.read_bytes(),b'accepted fixture');self.assertFalse((Path(storage.local_dir)/source.SLUG).exists())

    def test_missing_state_quarantines_and_second_attempt_never_bootstraps(self):
        with synthetic_population() as (_,storage,raw,receipt,_):
            r=self.stage(storage,raw,receipt);base=Path(storage.local_dir);key=f'_construction-state/{source.SLUG}/{r["version"]}.json';body=(base/key).read_bytes();(base/key).unlink()
            with self.assertRaisesRegex(ValueError,'whole_candidate_detached'):self.stage(storage,raw,receipt)
            with self.assertRaisesRegex(ValueError,'construction_missing_original_state'):self.stage(storage,raw,receipt)
            (base/key).write_bytes(body);restored=self.stage(storage,raw,receipt);self.assertFalse(restored['reused']);self.assertEqual(restored['version'],r['version'])

    def test_private_metadata_failure_leaves_no_first_state(self):
        with synthetic_population() as (_,storage,raw,receipt,_):
            with patch.object(source,'source_payloads',side_effect=ValueError('fixture metadata failure')):
                with self.assertRaisesRegex(ValueError,'fixture metadata failure'):self.stage(storage,raw,receipt)
            self.assertFalse((Path(storage.local_dir)/'_construction-state').exists())
            self.assertFalse(list((Path(storage.local_dir)/source.SLUG/source.SLUG).glob('v*')))
            self.assertTrue(self.stage(storage,raw,receipt))

    def test_external_malformed_or_invalid_receipts_preserved_before_validation(self):
        for invalid in [b'{NOT JSON',b'[]',b'{}']:
            with self.subTest(invalid=invalid),synthetic_population() as (root,storage,raw,receipt,_):
                original=raw.read_bytes();receipt.write_bytes(invalid)
                with self.assertRaisesRegex(ValueError,'rejected_after_observation'):self.stage(storage,raw,receipt)
                observations=list((Path(storage.local_dir)/'_raw-untrusted-observations').rglob('receipt.bin'));self.assertEqual(len(observations),1);self.assertEqual(observations[0].read_bytes(),invalid)
                diagnostic=source.strict_json(observations[0].with_name('observation.json').read_bytes());self.assertTrue(diagnostic['complete_receipt_descriptor_read']);self.assertEqual(diagnostic['archive_path'],str(raw));self.assertEqual(raw.read_bytes(),original)
                self.assertFalse((Path(storage.local_dir)/'_construction-state').exists());self.assertFalse((Path(storage.local_dir)/'_raw-capture-references').exists())

    def test_external_receipt_read_bounded_and_alias_refused(self):
        with synthetic_population() as (root,storage,raw,receipt,_):
            receipt.write_bytes(b'x'*(source.MAX_RECEIPT_BYTES+97))
            with self.assertRaisesRegex(ValueError,'rejected_after_observation'):self.stage(storage,raw,receipt)
            observed=next((Path(storage.local_dir)/'_raw-untrusted-observations').rglob('receipt.bin'));self.assertEqual(observed.stat().st_size,source.MAX_RECEIPT_BYTES+1)
            self.assertFalse(source.strict_json(observed.with_name('observation.json').read_bytes())['complete_receipt_descriptor_read'])
            link=root/'receipt-link';link.symlink_to(receipt)
            with self.assertRaisesRegex(ValueError,'alias'):self.stage(storage,raw,link)

    def test_partial_cas_only_blocks_without_rewriting(self):
        with synthetic_population() as (_,storage,raw,receipt,_):
            first=self.stage(storage,raw,receipt);base=Path(storage.local_dir);key=base/f'_construction-state/{source.SLUG}/{first["version"]}.json';original=key.read_bytes();key.unlink();Path(str(key)+'.first-construction.json').unlink()
            with self.assertRaises(ValueError):self.stage(storage,raw,receipt)
            with self.assertRaises(ValueError):self.stage(storage,raw,receipt)
            self.assertFalse(key.exists());self.assertTrue(any(p.read_bytes()==original for p in Path(str(key)+'.evidence').glob('*.json')))

    def test_self_repinned_report_and_native_metadata_still_quarantine(self):
        for kind in ['report','container','unknown','ready_sha','ready_extra']:
            with self.subTest(kind=kind),synthetic_population() as (_,storage,raw,receipt,_):
                r=self.stage(storage,raw,receipt);base=Path(storage.local_dir);root=base/source.SLUG/source.SLUG/r['version'];ready=source.strict_json((root/source.READY).read_bytes())
                if kind=='report':name='metadata/geopackage_source_fidelity.json';(root/name).write_bytes(source.encode({'status':'forged'}))
                elif kind=='container':
                    name=f'geopackage/{source.SLUG}.gpkg'
                    with sqlite3.connect(root/name) as db:db.execute("UPDATE gpkg_spatial_ref_sys SET organization='forged' WHERE srs_id=4269")
                elif kind=='unknown':name='metadata/unknown.json';(root/name).write_bytes(b'{}')
                elif kind=='ready_sha':ready['source_sha256']='0'*64
                else:ready['unreviewed_extra_field']=True
                if kind not in ('ready_sha','ready_extra'):ready['objects'][name]=source.sha256_file(root/name)
                (root/source.READY).write_bytes(source.encode(ready))
                with self.assertRaisesRegex(ValueError,'whole_candidate_detached'):self.stage(storage,raw,receipt)
                self.assertFalse(root.exists());self.assertTrue(list((base/'_quarantine'/source.SLUG).glob('invalid-*')))

    def test_root_ancestor_alias_and_prefix_reject_before_outside_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);outside=root/'outside';outside.mkdir();alias=root/'alias';alias.symlink_to(outside,target_is_directory=True)
            for path in [alias,alias/'child']:
                with self.assertRaises(ValueError):source.acquire(StagingStorageResource(local_dir=str(path)))
            with self.assertRaises(ValueError):source.acquire(StagingStorageResource(local_dir=str(outside),prefix='redirect'))
            self.assertEqual(list(outside.iterdir()),[])
            with self.assertRaises(ValueError):source.acquire(PublishedStorageResource(local_dir=str(outside)))

    def test_external_capture_alias_rejected(self):
        with synthetic_population() as (root,storage,raw,receipt,_):
            alias=root/'raw-alias';alias.symlink_to(raw)
            with self.assertRaisesRegex(ValueError,'alias'):self.stage(storage,alias,receipt)
            self.assertEqual(raw.read_bytes(),b'EXPLICIT_SYNTHETIC_COMPLETE_2ROW_INPUT')

    def test_missing_ids_unexpected_fields_type_geometry_and_coverage_drift(self):
        with synthetic_population() as (_,_,_,_,table):
            batch=source.native_batch(table.to_batches()[0]);cases=[batch.remove_column(0),batch.append_column('EXTRA',pa.array([1,2])),batch.set_column(0,'OBJECTID',pa.array([1,1],type=pa.int32())),batch.set_column(1,'COUSUBNS',pa.array(['00000001',None])),batch.set_column(2,'GEOID',pa.array(['0200190001','0200190002'])),batch.set_column(11,'SHAPE',pa.array([None,None],type=pa.binary()))]
            for changed in cases:
                with self.subTest(schema=str(changed.schema)),self.assertRaises((ValueError,TypeError)):source.scan([changed])
            with self.assertRaises((ValueError,pa.ArrowInvalid)):source.native_batch(table.set_column(0,'OBJECTID',pa.array([2**40,2],type=pa.int64())).to_batches()[0])

    def test_newer_receipt_keeps_original_binding_and_history(self):
        with synthetic_population() as (_,storage,raw,receipt,_):
            r=self.stage(storage,raw,receipt);base=Path(storage.local_dir);root=base/source.SLUG/source.SLUG/r['version'];original=(root/'metadata/source_manifest.json').read_bytes();d=source.strict_json(receipt.read_bytes());d['observed_at']='2026-10-20T12:00:00+00:00';receipt.write_bytes(source.encode(d));self.assertTrue(self.stage(storage,raw,receipt)['reused']);self.assertEqual((root/'metadata/source_manifest.json').read_bytes(),original);self.assertEqual(len(list((base/'_raw-capture-references'/source.SLUG/source.SHA256).glob('receipt-*.json'))),2)


if __name__=='__main__':unittest.main()
