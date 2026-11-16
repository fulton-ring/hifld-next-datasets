"""Complete historical USGS 2003 survey candidate fixtures; no source/contract acceptance is granted."""
import io
import json
import os
import shutil
import struct
import tarfile
import tempfile
import unittest
import zipfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_hifld import construction_state
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import crushed_stone as s
from dagster_hifld.sources import usgs_mineplant as m

_CAPTURE_ARCHIVE_ENV='HIFLD_CRUSHED_STONE_TEST_CAPTURE_ARCHIVE'
ARCHIVE=Path(os.environ[_CAPTURE_ARCHIVE_ENV]) if os.environ.get(_CAPTURE_ARCHIVE_ENV) else None

STATE_NAMES=['Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','Delaware','Florida','Georgia','Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky','Louisiana','Maine','Maryland','Massachusetts','Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire','New Jersey','New Mexico','New York','North Carolina','North Dakota','Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island','South Carolina','South Dakota','Tennessee','Texas','Utah','Vermont','Virginia','Washington','West Virginia','Wisconsin','Wyoming']


def synthetic_mineplant_members():
    """Generate the complete bounded synthetic package from a structural seed."""
    seed=json.loads((Path(__file__).parent/'fixtures/crushed-stone-structural.json').read_bytes())
    count=seed['row_count'];header,width=321,244;lo,hi=seed['crushed_source_fid_range'];offset=seed['crushed_id_range'][0]-lo
    dbf=bytearray(header+count*width);dbf[0]=3;struct.pack_into('<IHH',dbf,4,count,header,width)
    for ordinal,(name,kind,size,scale) in enumerate(m.MINEPLANT_DBF_FIELDS):
        off=32+ordinal*32;dbf[off:off+len(name)]=name.encode('ascii');dbf[off+11]=ord(kind);dbf[off+16]=size;dbf[off+17]=scale
    dbf[header-1]=13
    rows=[]
    for fid in range(count):
        x=seed['x_origin']+fid*seed['step'];y=seed['y_origin']+fid*seed['step'];crushed=lo<=fid<=hi
        row={'id':fid+offset if crushed else fid+1,'COMMODITY':seed['crushed_commodity'] if crushed else seed['other_commodity'],
            'SITE_NAME':seed['sample']['SITE_NAME'],'COMPANY_NA':seed['sample']['COMPANY_NA'],'STATE_LOCA':STATE_NAMES[fid%len(STATE_NAMES)],
            'COUNTY':seed['sample']['COUNTY'],'LATITUDE':f'{y:.5f}','LONGITUDE':f'{x:.5f}','PLANT_MIN':seed['sample']['PLANT_MIN']}
        rows.append(row);rec=bytearray(width);rec[0:1]=b' ';off=1
        for name,kind,size,scale in m.MINEPLANT_DBF_FIELDS:
            raw=(row[name] if kind=='C' else str(row[name])).encode('ascii')
            raw=raw.ljust(size,b' ') if kind=='C' else raw.rjust(size,b' ')
            assert len(raw)==size,(name,row[name])
            rec[off:off+size]=raw;off+=size
        dbf[header+fid*width:header+(fid+1)*width]=rec
    xs=[seed['x_origin']+fid*seed['step'] for fid in range(count)];ys=[seed['y_origin']+fid*seed['step'] for fid in range(count)]
    shp=bytearray(100+28*count);shx=bytearray(100+8*count)
    for value in (shp,shx):
        struct.pack_into('>i',value,0,9994);struct.pack_into('>i',value,24,len(value)//2);struct.pack_into('<ii',value,28,1000,1)
        struct.pack_into('<4d',value,36,min(xs),min(ys),max(xs),max(ys))
    for fid,(x,y) in enumerate(zip(xs,ys)):
        off=100+fid*28;struct.pack_into('>ii',shp,off,fid+1,10);struct.pack_into('<idd',shp,off+8,1,x,y);struct.pack_into('>ii',shx,100+fid*8,off//2,10)
    lines=['\t'.join(['ID','COMMODITY','SITE_NAME','COMPANY_NA','STATE_LOCA','COUNTY','LATITUDE','LONGITUDE','PLANT_MIN'])]
    for row in rows:
        lines.append('\t'.join([str(row['id']),row['COMMODITY'],row['SITE_NAME'],row['COMPANY_NA'],row['STATE_LOCA'],row['COUNTY'],row['LATITUDE'],row['LONGITUDE'],row['PLANT_MIN']]))
    target=m.MINEPLANT_MEMBERS['mineplant.txt'];base=sum(len(line)+1 for line in lines);extra,rem=divmod(target-base,len(lines))
    if extra<0:raise ValueError('synthetic txt exceeds pinned member size')
    txt=('\n'.join(line+' '*(extra+(rem if index==len(lines)-1 else 0)) for index,line in enumerate(lines))+'\n').encode('ascii')
    assert len(txt)==target
    prj=(m.MINEPLANT_PRJ+'\n').encode('ascii')
    assert len(prj)==m.MINEPLANT_MEMBERS['mineplant.prj']
    return {'mineplant.dbf':bytes(dbf),'mineplant.shp':bytes(shp),'mineplant.shx':bytes(shx),'mineplant.txt':txt,'mineplant.prj':prj}


def synthetic_zip(members):
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as archive:
        for name,body in members.items():archive.writestr(name,body)
    return stream.getvalue()


class StructuralCrushedStoneTests(unittest.TestCase):
    def test_closed_ready_wrapper_binds_package_captures_types_and_object_hashes(self):
        c={'synthetic':(b'explicit synthetic raw bytes',{})};identity={'files':{'synthetic':'a'*64}};objects={'candidate/synthetic.parquet':'b'*64}
        ready=s.ready_metadata('v-synthetic',c,identity,objects)
        self.assertEqual(set(ready),{'version','approved','processing_fingerprint','capture_fingerprint','package_id','objects'})
        self.assertIs(ready['approved'],False);self.assertEqual(ready['package_id'],s.PACKAGE)
        self.assertEqual(ready['capture_fingerprint'],s.digest(s.encode({'synthetic':s.digest(c['synthetic'][0])})))
        variants=[ready|{'approved':0},ready|{'approved':None},ready|{'baseline_approved':True},ready|{'package_id':'WRONG'},ready|{'capture_fingerprint':'0'*64},ready|{'processing_fingerprint':'0'*64},ready|{'version':False},ready|{'objects':objects|{'invented':'c'*64}}]
        s.validate_ready_metadata(s.encode(ready),'v-synthetic',c,identity,objects)
        for bad in [s.encode(v) for v in variants]+[s.encode(ready)+b'\n',b'null',b'{ malformed']:
            with self.subTest(body=bad),self.assertRaisesRegex(ValueError,'ready_identity_or_closed_source_inventory_drift'):
                s.validate_ready_metadata(bad,'v-synthetic',c,identity,objects)

    def test_synthetic_package_decodes_exact_decimals_subset_and_geometry(self):
        members=synthetic_mineplant_members()
        self.assertEqual({name:len(body) for name,body in members.items()},m.MINEPLANT_MEMBERS)
        decoded=s.decode_mineplant_zip(synthetic_zip(members));self.assertEqual(decoded,members)
        rows=s.decode_mineplant_dbf(members['mineplant.dbf']);self.assertEqual(len(rows),m.MINEPLANT_RECORDS)
        s.crosscheck_mineplant_txt(members['mineplant.txt'],rows)
        geometry=s.decode_points(members['mineplant.shp'],members['mineplant.shx'],m.MINEPLANT_RECORDS)
        s.verify_mineplant_geometry_values(rows,geometry)
        subset=s.crushed_rows(rows)
        self.assertEqual(len(subset),s.CRUSHED_ROWS)
        self.assertEqual([int(row['id']) for row in subset],list(range(*s.CRUSHED_IDS,(s.CRUSHED_IDS[1]+1))))
        self.assertEqual([row['source_fid'] for row in subset],list(range(s.CRUSHED_SOURCE_FIDS[0],s.CRUSHED_SOURCE_FIDS[1]+1)))
        self.assertEqual(len({row['STATE_LOCA'] for row in subset}),len(STATE_NAMES))
        first=subset[0]
        self.assertEqual((first['id'],first['COMMODITY']),(s.CRUSHED_IDS[0],s.CRUSHED_COMMODITY))
        self.assertEqual(first['LATITUDE'],Decimal('34.01940'));self.assertEqual(first['LATITUDE'].as_tuple().exponent,-5)
        self.assertEqual(geometry[first['source_fid']],s.shapely.Point(-86.4494,34.0194).wkb)

    def test_synthetic_subset_count_id_record_and_state_pins_fail_closed(self):
        members=synthetic_mineplant_members();rows=s.decode_mineplant_dbf(members['mineplant.dbf'])
        with patch.object(s,'CRUSHED_ROWS',s.CRUSHED_ROWS-1),self.assertRaisesRegex(ValueError,'crushed_subset_completeness_drift'):s.crushed_rows(rows)
        with patch.object(s,'CRUSHED_IDS',(s.CRUSHED_IDS[0],s.CRUSHED_IDS[1]-1)),self.assertRaisesRegex(ValueError,'crushed_subset_id_roster_drift'):s.crushed_rows(rows)
        with patch.object(s,'CRUSHED_SOURCE_FIDS',(s.CRUSHED_SOURCE_FIDS[0],s.CRUSHED_SOURCE_FIDS[1]-1)),self.assertRaisesRegex(ValueError,'crushed_subset_source_record_roster_drift'):s.crushed_rows(rows)
        with patch.object(s,'CRUSHED_STATES',len(STATE_NAMES)+1),self.assertRaisesRegex(ValueError,'crushed_subset_state_coverage_drift'):s.crushed_rows(rows)
        commodity=m.MINEPLANT_DBF_FIELDS[1];lo=s.CRUSHED_SOURCE_FIDS[0]
        raw=bytearray(members['mineplant.dbf']);base=321+lo*244+1+8;raw[base:base+15]=b'Sand and Gravel'
        changed=s.decode_mineplant_dbf(bytes(raw))
        with self.assertRaisesRegex(ValueError,'crushed_subset_completeness_drift'):s.crushed_rows(changed)

    def test_synthetic_DBF_header_ID_precision_and_coverage_fail_closed(self):
        members=synthetic_mineplant_members();original=members['mineplant.dbf']
        for label in ('renamed','width','scale','count','duplicate','deleted','incomplete','nonASCII','scale_drift'):
            body=bytearray(original)
            if label=='renamed':body[32]=ord('X')
            elif label=='width':body[32+16]=12
            elif label=='scale':body[32+17]=1
            elif label=='count':struct.pack_into('<I',body,4,6784)
            elif label=='duplicate':struct.pack_into('<8s',body,321+1,b'      29')
            elif label=='deleted':body[321]=ord('*')
            elif label=='incomplete':body=body[:-30]
            elif label=='nonASCII':body[321+1+8]=0xE9
            else:body[321+1+8+30+50+50+40+40:321+1+8+30+50+50+40+40+10]=b'  1.234567'
            if label=='scale_drift':
                with self.subTest(label=label),self.assertRaisesRegex(ValueError,'declared_DBF_scale_drift'):s.decode_mineplant_dbf(bytes(body))
                continue
            with self.subTest(label=label),self.assertRaises(ValueError):s.decode_mineplant_dbf(bytes(body))
        integer=bytearray(original);lat=321+1+8+30+50+50+40+40;integer[lat:lat+10]=b'        33'
        rows=s.decode_mineplant_dbf(bytes(integer));self.assertEqual(rows[0]['LATITUDE'],Decimal('33'));self.assertEqual(rows[0]['LATITUDE'].as_tuple().exponent,0)

    def test_synthetic_native_SHP_SHX_type_ID_coordinate_and_index_failures(self):
        members=synthetic_mineplant_members();original=members['mineplant.shp'];index=members['mineplant.shx']
        for label in ('record','type','coordinate','index','extra','bounds'):
            body=bytearray(original);idx=bytearray(index)
            if label=='record':struct.pack_into('>i',body,100,99)
            elif label=='type':struct.pack_into('<i',body,108,3)
            elif label=='coordinate':struct.pack_into('<d',body,112,float('nan'))
            elif label=='index':struct.pack_into('>i',idx,100,0)
            elif label=='extra':body+=b'0'
            else:struct.pack_into('<d',body,36,-999)
            with self.subTest(label=label),self.assertRaises(ValueError):s.decode_points(bytes(body),bytes(idx),m.MINEPLANT_RECORDS)

    def test_synthetic_zip_member_roster_size_and_txt_crosscheck_failures(self):
        members=synthetic_mineplant_members()
        with self.assertRaisesRegex(ValueError,'member_roster'):s.decode_mineplant_zip(synthetic_zip({**members,'rogue.txt':b'x'}))
        with self.assertRaisesRegex(ValueError,'member_roster'):s.decode_mineplant_zip(synthetic_zip({('mineplant.prj' if k=='mineplant.txt' else k):v for k,v in members.items()}))
        with self.assertRaisesRegex(ValueError,'member_roster'):s.decode_mineplant_zip(synthetic_zip({**members,'mineplant.dbf':members['mineplant.dbf'][:-2]}))
        rows=s.decode_mineplant_dbf(members['mineplant.dbf'])
        mutated=members['mineplant.txt'].replace(b'Synthetic quarry',b'Synthetic quarry X',1)
        self.assertNotEqual(mutated,members['mineplant.txt'])
        with self.assertRaisesRegex(ValueError,'txt_dbf_value_drift'):s.crosscheck_mineplant_txt(mutated,rows)
        truncated=b''.join(members['mineplant.txt'].splitlines(keepends=True)[:-1])
        with self.assertRaisesRegex(ValueError,'txt_roster_or_header'):s.crosscheck_mineplant_txt(truncated,rows)

    def test_explicit_selection_and_storage_guard_before_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp)
            with self.assertRaisesRegex(ValueError,'explicit_unapproved_usgs_successor_package_selection'):s.stage_crushed_stone(storage)
            with self.assertRaisesRegex(ValueError,'explicit_unapproved_usgs_successor_package_selection'):s.stage_crushed_stone(storage,capture_dir=tmp,package_id='usgs-mrdata-mineplant-2003-crushed-stone-v1-wrong-selection-id')
            with self.assertRaisesRegex(ValueError,'explicit_unapproved_usgs_successor_package_selection'):s.stage_crushed_stone(storage,capture_dir=tmp)
            with self.assertRaisesRegex(ValueError,'explicit_isolated_local_staging'):s.stage_crushed_stone(PublishedStorageResource(local_dir=tmp),capture_dir=tmp,package_id=s.PACKAGE)

    def test_bounded_input_reads_exact_limit_and_rejects_oversize_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'body';path.write_bytes(b'1234');self.assertEqual(s.bounded_input(path,4),b'1234')
            with self.assertRaises(ValueError):s.bounded_input(path,3)
            linked=Path(tmp)/'linked';linked.symlink_to(path)
            with self.assertRaises(OSError):s.bounded_input(linked,4)
            hard=Path(tmp)/'hard';os.link(path,hard)
            with self.assertRaises(ValueError):s.bounded_input(path,4)

    def test_body_first_bad_receipt_diagnostics_without_validated_provenance(self):
        for mode in ('malformed','unreadable'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);inputs=root/'input';inputs.mkdir();body=b'SYNTHETIC public structural input, not real data';url='https://example.com/synthetic-structural';(inputs/'synthetic.body').write_bytes(body);receipt=inputs/'synthetic.receipt.json';receipt.write_bytes(b'{ bad metadata')
                if mode=='unreadable':receipt.chmod(0)
                storage=StagingStorageResource(local_dir=str(root/'staging'));pin={'url':url,'bytes':len(body),'sha256':s.digest(body)}
                try:
                    with patch.object(s,'PINS',{'synthetic':pin}),self.assertRaises(ValueError):s.acquire(storage,inputs,s.PACKAGE)
                finally:receipt.chmod(0o600)
                self.assertEqual((root/'staging'/f'_raw-captures/{s.SLUG}/{s.digest(body)}/synthetic.body').read_bytes(),body);attempt=json.loads(next((root/'staging'/'_quarantine').rglob('attempted-inputs.json')).read_bytes())['objects']['synthetic'];self.assertEqual(attempt['raw_receipt_read'],mode=='malformed');self.assertFalse((root/'staging'/'_construction-state').exists())

    def test_partial_negative_incident_blocks_even_without_original_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);directory=Path(tmp)/s.incident_directory('v-synthetic');directory.mkdir(parents=True);(directory/'.pending-structural-fixture').write_bytes(b'partial')
            with self.assertRaisesRegex(ValueError,'permanent_observed_original_trust_loss'):s.refuse_observed_loss(storage,'v-synthetic')


@unittest.skipUnless(ARCHIVE,'complete original crushed-stone captures opt-in required; see docs/dataset-sources/crushed-stone-operations/README.md')
class MineplantCapturedCrushedStoneTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.package_tmp=tempfile.TemporaryDirectory();cls.package=Path(cls.package_tmp.name)
        with tarfile.open(ARCHIVE,'r:gz') as archive:
            for member in archive:
                if not member.isfile() or Path(member.name).name!=member.name:raise ValueError('unsafe test archive')
                if member.name not in {name+suffix for name in s.PINS for suffix in ('.body','.receipt.json')}:continue
                (cls.package/member.name).write_bytes(archive.extractfile(member).read())
    @classmethod
    def tearDownClass(cls):cls.package_tmp.cleanup()
    def captures(self):return {n:((self.package/(n+'.body')).read_bytes(),s.strict_json((self.package/(n+'.receipt.json')).read_bytes())) for n in s.PINS}
    def stage(self,storage):return s.stage_crushed_stone(storage,capture_dir=str(self.package),package_id=s.PACKAGE)
    def state(self,root,result):return Path(root)/s.construction_record_key(result['version'])

    def test_full2190_usgs_native_values_types_ids_geometry_and_published_family(self):
        captures=self.captures();table,published,audit=s.inspect_captures(captures)
        self.assertEqual(table.num_rows,s.CRUSHED_ROWS);self.assertEqual(table.column_names,s.SOURCE_NAMES)
        self.assertEqual(table['id'].type,pa.decimal128(8,0));self.assertEqual(table['LATITUDE'].type,pa.decimal128(10,5));self.assertEqual(table['LONGITUDE'].type,pa.decimal128(10,5))
        self.assertEqual(table['source_shp_record_number'].to_pylist(),[fid+1 for fid in table['source_fid'].to_pylist()])
        self.assertEqual(table['id'].to_pylist(),[Decimal(value) for value in range(s.CRUSHED_IDS[0],s.CRUSHED_IDS[1]+1)])
        self.assertEqual(len(set(table['id'].to_pylist())),s.CRUSHED_ROWS)
        self.assertIn('Oneonta Quarry',table['SITE_NAME'].to_pylist())
        self.assertEqual(audit['commodities'],{s.CRUSHED_COMMODITY:s.CRUSHED_ROWS});self.assertEqual(len(audit['states']),s.CRUSHED_STATES)
        self.assertIn('North_American_1927',audit['native_crs_claim']);self.assertIn('North American Datum of 1983',audit['contradictory_retired_layer_metadata_datum'])
        self.assertEqual(audit['retired_layer_reference']['documented_points'],2324);self.assertEqual(audit['survey_reference']['rows'],s.CRUSHED_ROWS)
        members=s.decode_mineplant_zip(captures['usgs_mineplant_zip'][0]);raw=members['mineplant.shp']
        for index,fid in enumerate(table['source_fid'].to_pylist()):
            point=s.shapely.from_wkb(table['geometry'][index].as_py())
            self.assertEqual((point.x,point.y),struct.unpack_from('<dd',raw,100+fid*28+12))
            self.assertEqual(point.y,float(table['LATITUDE'][index].as_py()));self.assertEqual(point.x,float(table['LONGITUDE'][index].as_py()))
        self.assertEqual(set(published['complete_formats']),{'geoparquet','geojson','geopackage','shapefile','file_geodatabase'})
        for name,entry in published['complete_formats'].items():
            self.assertEqual(entry['rows'],766);self.assertEqual(entry['attribute_values_compared'],6894);self.assertIn('MINOP2X020',entry['original_identifier_columns'])
        self.assertEqual(sum(published['published_commodities'].values()),766);self.assertNotIn('Crushed Stone',published['published_commodities'])
        self.assertFalse(published['approved']);self.assertFalse(audit['approved'])

    def test_explicit_package_required_and_remote_or_linked_storage_rejected_before_acquire(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError,'explicit_unapproved'):s.acquire(StagingStorageResource(local_dir=tmp),None,None)
            with self.assertRaisesRegex(ValueError,'explicit_unapproved'):s.acquire(StagingStorageResource(local_dir=tmp),str(self.package),'usgs-mrdata-mineplant-2003-crushed-stone-v1-wrong-selection-id')
            for resource in [PublishedStorageResource(local_dir=tmp),StagingStorageResource(bucket='forbidden',use_local=False),StagingStorageResource(local_dir=tmp+'/../unsafe')]:
                with patch.object(s,'acquire') as acquire,self.assertRaises(ValueError):s.stage_crushed_stone(resource,capture_dir=str(self.package),package_id=s.PACKAGE)
                acquire.assert_not_called()
            root=Path(tmp)/'linked';root.symlink_to(self.package,target_is_directory=True)
            with self.assertRaisesRegex(ValueError,'aliased'):self.stage(StagingStorageResource(local_dir=str(root)))

    def test_pinned_drift_incomplete_receipts_and_extra_package_quarantine(self):
        with tempfile.TemporaryDirectory() as tmp:
            package=Path(tmp)/'input';shutil.copytree(self.package,package);storage=StagingStorageResource(local_dir=tmp+'/stage')
            target=package/'usgs_mineplant_zip.body';target.write_bytes(target.read_bytes()[:-1])
            with self.assertRaisesRegex(ValueError,'capture_url_status_length'):s.acquire(storage,str(package),s.PACKAGE)
            self.assertTrue(list((Path(tmp)/'stage/_quarantine').rglob('usgs_mineplant_zip.body')))
            target.write_bytes((self.package/target.name).read_bytes());(package/'extra').write_text('drift')
            with self.assertRaisesRegex(ValueError,'extra_explicit_package_roster'):s.acquire(storage,str(package),s.PACKAGE)

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

    def test_complete_reuse_and_published_id_or_ready_coforge_quarantines_whole_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp);result=self.stage(storage);self.assertFalse(result['reused']);self.assertEqual(result['feature_count'],s.CRUSHED_ROWS);self.assertTrue(self.stage(storage)['reused'])
            root=Path(tmp)/s.SLUG/s.SLUG/result['version']
            self.assertFalse((root/'shapefile').exists());self.assertTrue((root/f'geoparquet/{s.SLUG}.parquet').is_file())
            from dagster_hifld.definitions import _iter_staged_version_paths
            self.assertIn((s.SLUG,s.SLUG,result['version']),list(_iter_staged_version_paths(storage)))
            target=root/'metadata/published-identifiers.json';target.write_bytes(b'forged');ready=s.strict_json((root/s.READY).read_bytes());ready['objects']['metadata/published-identifiers.json']=s.digest(b'forged');(root/s.READY).write_bytes(s.encode(ready))
            with self.assertRaisesRegex(ValueError,'detached_intact'):self.stage(storage)
            self.assertFalse(root.exists());self.assertTrue(list((Path(tmp)/'_quarantine').rglob('published-identifiers.json')))
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

if __name__=='__main__':unittest.main()
