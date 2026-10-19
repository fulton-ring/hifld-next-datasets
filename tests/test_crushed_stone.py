"""Complete historical preservation data; fixtures grant no acceptance."""
import json
import os
import struct
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_hifld import construction_state
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import crushed_stone as s

REAL_CAPTURE_DIR=os.environ.get('HIFLD_CRUSHED_STONE_REAL_CAPTURE_DIR')
def captures():
    if not REAL_CAPTURE_DIR:raise ValueError('explicit_complete_real_capture_dir_required')
    root=Path(REAL_CAPTURE_DIR);result={}
    for name in s.PINS:
        result[name]=((root/(name+'.body')).read_bytes(),json.loads((root/(name+'.receipt.json')).read_bytes()))
    return result

def unpack(root):
    for name,(body,receipt) in captures().items():
        (root/(name+'.body')).write_bytes(body);(root/(name+'.receipt.json')).write_bytes(s.encode(receipt))

def synthetic_binary_fixture():
    """Generate bounded synthetic bytes from a small explicit structural seed."""
    seed=json.loads((Path(__file__).parent/'fixtures/crushed-stone-structural.json').read_bytes());count=seed['row_count'];width=2082;header=385
    body=bytearray(header+count*width+1);body[0]=3;body[29]=87;struct.pack_into('<IHH',body,4,count,header,width)
    for ordinal,(name,kind,size,scale) in enumerate(s.DBF_FIELDS):
        off=32+ordinal*32;body[off:off+len(name)]=name.encode('ascii');body[off+11]=ord(kind);body[off+16]=size;body[off+17]=scale
    body[header-1]=13;body[-1]=26;points=[]
    for ordinal in range(count):
        x=seed['x_origin']+ordinal*seed['step'];y=seed['y_origin']+ordinal*seed['step'];points.append((x,y));row=dict(seed['sample'],CRSTONX020=ordinal+1,LATITUDE=f'{y:.4f}',LONGITUDE=f'{x:.4f}');off=header+ordinal*width;body[off]=32;off+=1
        for name,kind,size,scale in s.DBF_FIELDS:
            raw=str(row[name]).encode('ascii');raw=raw.ljust(size,b' ') if kind=='C' else raw.rjust(size,b' ');body[off:off+size]=raw;off+=size
    shp=bytearray(100+28*count);shx=bytearray(100+8*count)
    for value in (shp,shx):
        struct.pack_into('>i',value,0,9994);struct.pack_into('>i',value,24,len(value)//2);struct.pack_into('<ii',value,28,1000,1);struct.pack_into('<4d',value,36,points[0][0],points[0][1],points[-1][0],points[-1][1])
    for ordinal,(x,y) in enumerate(points):
        off=100+ordinal*28;struct.pack_into('>ii',shp,off,ordinal+1,10);struct.pack_into('<idd',shp,off+8,1,x,y);struct.pack_into('>ii',shx,100+ordinal*8,off//2,10)
    return bytes(body),bytes(shp),bytes(shx)

class StructuralCrushedStoneTests(unittest.TestCase):
    def test_synthetic_exact_decimal_leading_zero_and_literal_text(self):
        dbf,shp,shx=synthetic_binary_fixture();rows=s.decode_dbf(dbf);geometry=s.decode_points(shp,shx);self.assertEqual(len(rows),2324);self.assertEqual(len(geometry),2324);self.assertEqual(rows[0]['CRSTONX020'],Decimal('1'));self.assertEqual(rows[0]['LATITUDE'],Decimal('20.0000'));self.assertEqual(rows[0]['LATITUDE'].as_tuple().exponent,-4);self.assertEqual(rows[0]['FIPS'],'01117');self.assertEqual(rows[0]['STATE_FIPS'],'01');self.assertEqual(rows[0]['COMP_NAME'],' Synthetic company');self.assertEqual(geometry[0],s.shapely.Point(10,20).wkb)

    def test_synthetic_DBF_header_ID_precision_and_coverage_fail_closed(self):
        original,_,_=synthetic_binary_fixture()
        for label in ('renamed','width','scale','count','duplicate','deleted','incomplete'):
            body=bytearray(original)
            if label=='renamed':body[32]=ord('X')
            elif label=='width':body[48]=12
            elif label=='scale':body[49]=1
            elif label=='count':struct.pack_into('<I',body,4,2323)
            elif label=='duplicate':body[2468:2479]=body[386:397]
            elif label=='deleted':body[385]=42
            else:body=body[:-30]
            with self.subTest(label=label),self.assertRaises(ValueError):s.decode_dbf(bytes(body))

    def test_synthetic_native_SHP_SHX_type_ID_coordinate_and_index_failures(self):
        _,original,index=synthetic_binary_fixture()
        for label in ('record','type','coordinate','index','extra'):
            body=bytearray(original);idx=bytearray(index)
            if label=='record':struct.pack_into('>i',body,100,99)
            elif label=='type':struct.pack_into('<i',body,108,3)
            elif label=='coordinate':struct.pack_into('<d',body,112,float('nan'))
            elif label=='index':struct.pack_into('>i',idx,100,0)
            else:body+=b'0'
            with self.subTest(label=label),self.assertRaises(ValueError):s.decode_points(bytes(body),bytes(idx))

    def test_explicit_selection_and_storage_guard_before_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp)
            with self.assertRaisesRegex(ValueError,'explicit_unapproved_preservation_package_selection'):s.stage_crushed_stone(storage)
            with self.assertRaisesRegex(ValueError,'explicit_isolated_local_staging'):s.stage_crushed_stone(PublishedStorageResource(local_dir=tmp),capture_dir=tmp,package_id=s.PACKAGE)

    def test_bounded_descriptor_and_body_first_bad_receipt_diagnostics(self):
        for mode in ('malformed','unreadable'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);inputs=root/'input';inputs.mkdir();body=b'SYNTHETIC public structural input, not real data';url='https://example.com/synthetic-structural';(inputs/'synthetic.body').write_bytes(body);receipt=inputs/'synthetic.receipt.json';receipt.write_bytes(b'{ bad metadata')
                if mode=='unreadable':receipt.chmod(0)
                storage=StagingStorageResource(local_dir=str(root/'staging'));pin={'url':url,'bytes':len(body),'sha256':s.digest(body)}
                try:
                    with patch.object(s,'PINS',{'synthetic':pin}),self.assertRaises(ValueError):s.acquire(storage,inputs,s.PACKAGE)
                finally:receipt.chmod(0o600)
                self.assertEqual((root/'staging'/f'_raw-captures/{s.SLUG}/{s.digest(body)}/synthetic.body').read_bytes(),body);attempt=json.loads(next((root/'staging'/'_quarantine').rglob('attempted-inputs.json')).read_bytes())['objects']['synthetic'];self.assertEqual(attempt['raw_receipt_read'],mode=='malformed')

    def test_partial_negative_incident_blocks_even_without_original_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);directory=Path(tmp)/s.incident_directory('v-synthetic');directory.mkdir(parents=True);(directory/'.pending-structural-fixture').write_bytes(b'partial')
            with self.assertRaisesRegex(ValueError,'permanent_observed_original_trust_loss'):s.refuse_observed_loss(storage,'v-synthetic')

@unittest.skipUnless(REAL_CAPTURE_DIR,'complete real captures are checksummed artifacts; set HIFLD_CRUSHED_STONE_REAL_CAPTURE_DIR for full2324 replay')
class RealCapturedCrushedStoneTests(unittest.TestCase):
    def stage(self,storage,root):
        # Unit state-fault boundary only. Actual registered runner separately
        # validates unpatched Git at a public portable processing commit.
        with patch.object(s,'validate_construction_code_identity'):
            return s.stage_crushed_stone(storage,capture_dir=root,package_id=s.PACKAGE)


    def test_raw_archive_never_claimed_as_canonical_shapefile(self):
        c=captures();table,published,audit=s.inspect_captures(c);manifest=s.manifest_for(c,{'files':{}},{'sha256':'a'*64,'bytes':1});payloads=s.source_payloads(c,{'files':{}},audit,manifest,table,published)
        self.assertFalse(any(name.startswith(('shapefile/','unknown/','geopackage/')) for name in payloads))
        for ext in ('dbf','prj','shp','shp.xml','shx'):self.assertEqual(payloads['raw/archive/crstonx020.'+ext],c['crstonx020.'+ext][0])
        self.assertEqual(payloads['raw/archive/preservation-package.zip'],s.preservation_transport_zip(c))

    def test_all2324_original11attributes_decimal_widths_FIPS_native_geometry(self):
        c=captures();table,published,audit=s.inspect_captures(c);self.assertEqual(table.num_rows,2324);self.assertEqual(table.column_names,s.SOURCE_NAMES);self.assertEqual(table['CRSTONX020'].type,pa.decimal128(11,0));self.assertEqual(table['LATITUDE'].type,pa.decimal128(19,4));self.assertNotIn('source_fid',table.column_names);self.assertNotIn('id',table.column_names)
        self.assertEqual(table['CRSTONX020'].to_pylist(),[Decimal(i) for i in range(1,2325)]);self.assertEqual(table['LATITUDE'][0].as_py(),Decimal('33.1690'));self.assertEqual(table['FIPS'][0].as_py(),'01117');self.assertEqual(table['STATE_FIPS'][0].as_py(),'01')
        dbf=c['crstonx020.dbf'][0];shp=c['crstonx020.shp'][0]
        # Separate manual fixed-offset binary audit, not an assertion against
        # the decoder's own output. All producer values and point doubles.
        for ordinal,row in enumerate(table.to_pylist()):
            raw=dbf[385+ordinal*2082:385+(ordinal+1)*2082];offset=1
            for name,kind,width,scale in s.DBF_FIELDS:
                value=raw[offset:offset+width];offset+=width;expected=value.decode('ascii').rstrip(' ') if kind=='C' else Decimal(value.decode('ascii').strip());self.assertEqual(row[name],expected)
            x,y=struct.unpack_from('<dd',shp,100+ordinal*28+12);self.assertEqual(row['geometry'],s.shapely.Point(x,y).wkb)
        self.assertEqual(audit['native_crs'],'EPSG:4269');self.assertEqual(audit['commodities'],{'CRUSHED STONE':2324});self.assertEqual(len(audit['states']),48)

    def test_all_six_published_candidates_five_open_formats_wrong_family_ids_preserved(self):
        c=captures();table,published,audit=s.inspect_captures(c);self.assertEqual(set(published['complete_formats']),{'geoparquet','geojson','geopackage','shapefile','file_geodatabase'})
        for name,entry in published['complete_formats'].items():
            self.assertEqual(entry['rows'],766);self.assertEqual(len(entry['original_identifier_values']),766);self.assertEqual(entry['attribute_values_compared'],6894);self.assertIn('MINOP2X020',entry['original_identifier_columns']);self.assertNotIn('CRSTONX020',entry['original_identifier_columns'])
            if name!='geojson':self.assertEqual(entry['native_geometry_WKB_exact_with_published_parquet'],766)
        self.assertFalse(published['approved']);self.assertEqual(table.num_rows,2324);self.assertEqual(sum(published['published_commodities'].values()),766);self.assertNotIn('CRUSHED STONE',published['published_commodities'])

    def test_descriptor_width_scale_name_missing_ID_and_record_loss(self):
        original=captures()['crstonx020.dbf'][0]
        for label in ('rename','width','scale','count','deleted','duplicate','incomplete','encoding'):
            body=bytearray(original)
            if label=='rename':body[32:42]=b'RENAMED_ID'
            elif label=='width':body[32+16]=12
            elif label=='scale':body[32+17]=1
            elif label=='count':struct.pack_into('<I',body,4,2323)
            elif label=='deleted':body[385]=42
            elif label=='duplicate':body[385+2082+1:385+2082+12]=body[386:397]
            elif label=='incomplete':body=body[:-25]
            else:body[385+12]=233
            with self.subTest(label=label),self.assertRaises(ValueError):s.decode_dbf(bytes(body))

    def test_complete_SHP_SHX_original_header_ID_type_geometry_coverage(self):
        c=captures();original=c['crstonx020.shp'][0];index=c['crstonx020.shx'][0]
        for label in ('record','type','coordinate','trailing','index','bounds'):
            body=bytearray(original);idx=bytearray(index)
            if label=='record':struct.pack_into('>i',body,100,3)
            elif label=='type':struct.pack_into('<i',body,108,3)
            elif label=='coordinate':struct.pack_into('<d',body,112,float('nan'))
            elif label=='trailing':body.extend(b'0')
            elif label=='index':struct.pack_into('>i',idx,100,0)
            else:struct.pack_into('<d',body,36,-999)
            with self.subTest(label=label),self.assertRaises(ValueError):s.decode_points(bytes(body),bytes(idx))

    def test_original_package_master_checksum_and_authority_exact_pins(self):
        c=captures()
        for name in ('legacy_metadata','stanford_publicxml','crstonx020.prj','baseline_file_geodatabase'):
            mutated=dict(c);body,receipt=mutated[name];body+=b'changed';mutated[name]=(body,receipt|{'bytes':len(body),'sha256':s.digest(body)})
            with self.subTest(name=name),self.assertRaisesRegex(ValueError,'pinned_package_drift'):s.inspect_captures(mutated)

    def test_no_automatic_archive_acquisition_or_remote_storage(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp)
            for root,package in [(None,s.PACKAGE),(tmp,None),(tmp,'unapproved-other-package')]:
                with self.assertRaisesRegex(ValueError,'explicit_unapproved_preservation_package_selection'):s.stage_crushed_stone(storage,capture_dir=root,package_id=package)
            for bad in [PublishedStorageResource(local_dir=tmp),StagingStorageResource(local_dir=tmp,bucket='production')]:
                with self.assertRaisesRegex(ValueError,'explicit_isolated_local_staging'):s.stage_crushed_stone(bad,capture_dir=tmp,package_id=s.PACKAGE)

    def test_raw_body_before_malformed_or_unreadable_receipt(self):
        for label in ('malformed','unreadable'):
            with self.subTest(label=label),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);inputs=root/'input';inputs.mkdir();unpack(inputs);receipt=inputs/'legacy_metadata.receipt.json';body=(inputs/'legacy_metadata.body').read_bytes();bad=b'not-json\n'
                if label=='malformed':receipt.write_bytes(bad)
                else:receipt.chmod(0)
                storage=StagingStorageResource(local_dir=str(root/'staging'))
                try:
                    with self.assertRaises(ValueError):s.acquire(storage,inputs,s.PACKAGE)
                finally:receipt.chmod(0o600)
                depot=root/'staging'/f'_raw-captures/{s.SLUG}/{s.digest(body)}/legacy_metadata.body';self.assertEqual(depot.read_bytes(),body)
                attempt=json.loads(next((root/'staging'/'_quarantine').rglob('attempted-inputs.json')).read_bytes())['objects']['legacy_metadata'];self.assertEqual(attempt['raw_receipt_read'],label=='malformed')
                if label=='malformed':self.assertEqual((root/'staging'/attempt['raw_receipt_key']).read_bytes(),bad)
                self.assertFalse((root/'staging'/'_construction-state').exists())

    def test_native_parquet_all_values_types_metadata_bytes_and_baseline_ids(self):
        table,published,_=s.inspect_captures(captures())
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'candidate').mkdir();(root/'geoparquet').mkdir();(root/'metadata').mkdir();body=s.parquet_bytes(s.expected_parquet(table))
            for name in ('candidate/typed-source.parquet',f'geoparquet/{s.SLUG}.parquet'):(root/name).write_bytes(body)
            (root/'metadata/published-identifiers.json').write_bytes(s.encode(published));s.verify_outputs(root,table,published);self.assertTrue(pq.read_table(root/'candidate/typed-source.parquet').equals(s.expected_parquet(table),check_metadata=True));(root/'metadata/published-identifiers.json').write_bytes(b'{}')
            with self.assertRaisesRegex(ValueError,'separate_published_identifiers'):s.verify_outputs(root,table,published)

    def test_relative_local_staging_and_source_only_columns_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);inputs=root/'input';inputs.mkdir();unpack(inputs);before=Path.cwd()
            try:
                os.chdir(root);storage=StagingStorageResource(local_dir='relative/staging');first=self.stage(storage,inputs);again=self.stage(storage,inputs);self.assertEqual(first['feature_count'],2324);self.assertTrue(again['reused']);self.assertEqual(first['version'],again['version'])
                target=root/'relative/staging'/s.SLUG/s.SLUG/first['version'];self.assertTrue((target/'metadata/source-record-lineage.json').exists());self.assertEqual(pq.read_table(target/'candidate/typed-source.parquet').column_names,s.SOURCE_NAMES)
            finally:os.chdir(before)

    def test_original_receipt_forgery_and_all_witness_loss_permanent_incident(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);inputs=root/'input';inputs.mkdir();unpack(inputs);storage=StagingStorageResource(local_dir=str(root/'staging'));first=self.stage(storage,inputs);target=root/'staging'/s.SLUG/s.SLUG/first['version'];alias=root/'staging'/s.construction_record_key(first['version']);original=alias.read_bytes();forged=json.loads(original);forged['payload']['captures']['crstonx020.dbf']['receipt']['observed_at']='2026-10-19T23:04:58+00:00';alias.write_bytes(s.canonical_construction_bundle(first['version'],forged['processing_fingerprint'],forged['payload']))
            with self.assertRaisesRegex(ValueError,'invalid_candidate_detached_intact:observed_original_trust_loss'):self.stage(storage,inputs)
            self.assertFalse(target.exists())
            for p in list(alias.parent.rglob('*')):
                if p.is_file() and 'incident-' not in str(p):p.unlink()
            for _ in range(2):
                with self.assertRaisesRegex(ValueError,'permanent_observed_original_trust_loss'):self.stage(storage,inputs)
            self.assertFalse(alias.exists());self.assertFalse(target.exists())

    def test_private_or_negative_journal_failure_preserves_remaining_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);inputs=root/'input';inputs.mkdir();unpack(inputs);storage=StagingStorageResource(local_dir=str(root/'staging'))
            with patch.object(s,'verify_outputs',side_effect=OSError('private candidate verification fault')),self.assertRaisesRegex(ValueError,'private candidate verification fault'):self.stage(storage,inputs)
            self.assertFalse((root/'staging'/'_construction-state').exists())
            first=self.stage(storage,inputs);target=root/'staging'/s.SLUG/s.SLUG/first['version'];alias=root/'staging'/s.construction_record_key(first['version']);alias.unlink();before={str(p.relative_to(target)):s.sha256_file(p) for p in target.rglob('*') if p.is_file()}
            with patch.object(s,'record_observed_loss',side_effect=OSError('negative journal unavailable')),self.assertRaisesRegex(s.IncidentWriteError,'candidate_preserved'):self.stage(storage,inputs)
            self.assertEqual(before,{str(p.relative_to(target)):s.sha256_file(p) for p in target.rglob('*') if p.is_file()})

if __name__=='__main__':unittest.main()
