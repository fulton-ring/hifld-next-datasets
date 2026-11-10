"""Small, explicit synthetic NBI fixtures; the complete official input is opt-in."""
import hashlib
import json
import os
import struct
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetKey, DefaultScheduleStatus
from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import national_bridge_inventory as f
from dagster_hifld.definitions import defs
from dagster_hifld.assets.national_bridge_inventory import nbi_source_integrity_schedule


def record(state=b'113',structure=b'0001           ',lat=b'38554600',lon=b'077070110',features=b'STREET OVER CREEK',location=b'0.1 KM S OF JUNCTION'):
    payload=bytearray(b' '*445)
    payload[0:3]=state;payload[3:18]=structure.ljust(15)[:15];payload[37:61]=features.ljust(24)[:24];payload[80:105]=location.ljust(25)[:25]
    payload[129:137]=lat;payload[137:146]=lon;payload[389:390]=b'1';payload[433:434]=b'1';payload[434:435]=b'7'
    payload[435:445]=b'0000010000'
    return bytes(payload)

def state_body(*records):
    return b''.join(r+b'\r\n' for r in records)

def expected_for(body,item1,rows=1,short=0):
    return (hashlib.sha256(body).hexdigest(),len(body),rows,'CRLF',item1,short)

def receipt(url,body,**over):
    r={'url':url,'final_url':url,'status':200,'bytes':len(body),'sha256':f.digest(body),'complete_body':True,
        'observed_at':'2026-11-10T00:00:00+00:00','server_last_modified':'Thu, 12 Jun 2025 16:53:24 GMT'}
    r.update(over);return r


class PatchedContract:
    """Two synthetic state files plus matching URL/pin mocks for inspect_captures."""
    def __init__(self):
        self.dc=state_body(record())
        self.tx=state_body(record(state=b'486',structure=b'0002           '),record(state=b'486',structure=b'1103(RAMP C)'))
        self.bodies={'landing':b'explicit synthetic landing','files_page':b'links nbiState=DC25 nbiState=TX25',
            'format':b'explicit synthetic record format page','crosswalk':b'explicit synthetic data crosswalk',
            'disclaimer':b'explicit synthetic download disclaimer','state_DC':self.dc,'state_TX':self.tx}
        self.urls={'landing':f.LANDING,'files_page':f.FILES_PAGE,'format':f.FORMAT_PAGE,'crosswalk':f.CROSSWALK_URL,
            'disclaimer':f.DISCLAIMER,'state_DC':'https://www.fhwa.dot.gov/bridge/nbi/2025/DC25.txt',
            'state_TX':'https://www.fhwa.dot.gov/bridge/nbi/2025/TX25.txt'}
        self.expected={'DC':expected_for(self.dc,'113'),'TX':expected_for(self.tx,'486',rows=2)}
        ids=sorted([('113','0001           '),('486','0002           '),('486','1103(RAMP C)   ')])
        self.roster_sha=f.digest(('\n'.join(f'{s}|{n}'for s,n in ids)+'\n').encode())
    def pins(self):
        return {'EXPECTED_FILES':self.expected,'URLS':self.urls,'TOKENS':('DC','TX'),'EXPECTED_TOTAL_COUNT':3,
            'EXPECTED_ID_ROSTER_SHA256':self.roster_sha,'EXPECTED_COMPOSITE_DUPLICATE_COUNT':0,'EXPECTED_DUPLICATE_KEYS':[],
            'EXPECTED_ZERO_COORDINATE_COUNT':0,'EXPECTED_OUT_OF_ENVELOPE_COUNT':0,'EXPECTED_BLANK_COORDINATE_COUNT':0,
            'EXPECTED_PARTIAL_COORDINATE_COUNT':0,'EXPECTED_SIGNED_COORDINATE_COUNT':0,'EXPECTED_WHITESPACE_COORDINATE_COUNT':0,'EXPECTED_SHORT_RECORD_COUNT':0,
            'EXPECTED_BLANK_STRUCTURE_NUMBER_COUNT':0,'EXPECTED_NON_ASCII_BYTES':{}}
    def captures(self):
        return {n:(body,receipt(self.urls[n],body))for n,body in self.bodies.items()}


class NBIParseTests(unittest.TestCase):
    def test_layout_covers_official_record_exactly_once(self):
        self.assertEqual(len(f.LAYOUT),124);covered=[]
        for name,start,end,declared in f.LAYOUT:
            if declared!='-':self.assertEqual(end-start+1,int(declared.split('/')[0]))
            covered.extend(range(start,end+1))
        self.assertEqual(sorted(covered),list(range(1,446)))
        self.assertEqual(len({n for n,_,_,_ in f.LAYOUT}),124)
    def test_parse_preserves_exact_fixed_width_bytes(self):
        body=state_body(record())
        arrays,ids,stats=f.parse_state_file(body,'DC',expected_for(body,'113'))
        table=pa.Table.from_arrays(arrays,schema=f.build_schema())
        self.assertEqual(table.num_rows,1);self.assertEqual(table.num_columns,125)
        self.assertEqual(table['STRUCTURE_NUMBER_008'].to_pylist(),['0001           '])
        self.assertEqual(table['LATITUDE_016'].to_pylist(),['38554600']);self.assertEqual(table['LONGITUDE_017'].to_pylist(),['077070110'])
        self.assertEqual(table['KILOMETERPOINT_011'].to_pylist(),['       '])
        self.assertEqual(table['WASHINGTON_HQ_USE_RESERVED'].to_pylist(),[' '*41])
        self.assertEqual(table['DECK_AREA'].to_pylist(),['0000010000'])
        self.assertEqual(table['geometry'].to_pylist(),[struct.pack('<BIdd',1,1,-77.116972,38.929444)])
        self.assertEqual(json.loads(table.schema.field('LATITUDE_016').metadata[b'nbi_ascii_layout'])['start'],130)
    def test_official_crosswalk_conversion_and_hemisphere_rules(self):
        cases=[(b'38554600',b'077070110',b'113',(38.929444,-77.116972)),
            (b'00385444',b'770301400',b'113',(0.648456,-770.503889)),
            (b'00000039',b'-00000007',b'243',(0.000108,-0.000019)),
            (b'00000000',b'000000000',b'113',(0.0,0.0)),
            (b' 3612109',b'008017241',b'374',(4.022525,-8.036781)),
            (b'13190000',b'144301230',b'066',(13.316667,144.503417)),
            (b'14033000',b'170120000',b'060',(-14.058333,-170.2))]
        for lat,lon,state,expect in cases:
            point,category,signed,padded,zero=f.geometry_for(record(state=state,lat=lat,lon=lon),state.decode())
            self.assertEqual((struct.unpack('<d',point[13:21])[0],struct.unpack('<d',point[5:13])[0]),expect,(lat,lon,state))
        point,category,signed,padded,zero=f.geometry_for(record(lat=b' 3612109'),'NC')
        self.assertTrue(padded)
        point,category,signed,padded,zero=f.geometry_for(record(lat=b'        ',lon=b'         '),'DC')
        self.assertIsNone(point);self.assertEqual(category,'blank')
        point,category,signed,padded,zero=f.geometry_for(record(lat=b'        '),'DC')
        self.assertIsNone(point);self.assertEqual(category,'partial')
    def test_signed_value_contradicting_documented_hemisphere_fails_closed(self):
        with self.assertRaisesRegex(ValueError,'sign_contradicts'):f.geometry_for(record(lon=b'+077070110'),'DC')
        with self.assertRaisesRegex(ValueError,'sign_contradicts'):f.geometry_for(record(state=b'066',lat=b'13190000',lon=b'-144301230'),'GU')
    def test_malformed_coordinates_and_line_handling(self):
        with self.assertRaises(ValueError):f.geometry_for(record(lat=b'38554A00',lon=b'077070110'),'DC')
        with self.assertRaises(ValueError):f.coordinate_decimal(b'3855 460','DC')
        with self.assertRaises(ValueError):f.split_state_body(b'38554600','DC')
        with self.assertRaises(ValueError):f.split_state_body(record(),'DC')
        with self.assertRaises(ValueError):f.split_state_body(record()[:300]+b'\r\n','DC')
    def test_producer_short_tail_record_keeps_available_items(self):
        body=state_body(record()[:421])
        arrays,ids,stats=f.parse_state_file(body,'DC',expected_for(body,'113',short=1))
        table=pa.Table.from_arrays(arrays,schema=f.build_schema())
        self.assertEqual(stats['short'],1)
        self.assertEqual(table['STRUCTURE_NUMBER_008'].to_pylist(),['0001           '])
        self.assertIsNone(table['BRIDGE_CONDITION'][0].as_py());self.assertIsNone(table['DECK_AREA'][0].as_py())
        self.assertEqual(table['FEDERAL_AGENCY_INDICATOR'][0].as_py(),'1')
    def test_non_ascii_bytes_are_pinned_and_preserved(self):
        body=state_body(record(features=b'A\xa5ASCO BRIDGE'))
        with mock.patch.object(f,'EXPECTED_NON_ASCII_BYTES',{'PR':1}):
            arrays,ids,stats=f.parse_state_file(body,'PR',expected_for(body,'113'))
            table=pa.Table.from_arrays(arrays,schema=f.build_schema())
            self.assertEqual(stats['non_ascii_bytes'],1)
            self.assertEqual(table['FEATURES_INTERSECTED_006A'].to_pylist(),['A\xa5ASCO BRIDGE           '])
        with mock.patch.object(f,'EXPECTED_NON_ASCII_BYTES',{}):
            with self.assertRaisesRegex(ValueError,'non_ascii'):f.parse_state_file(body,'PR',expected_for(body,'113'))


class NBIContractTests(unittest.TestCase):
    def setUp(self):
        self.contract=PatchedContract();self.pins=self.contract.pins()
        self.originals={name:getattr(f,name)for name in self.pins}
        for name,value in self.pins.items():setattr(f,name,value)
    def tearDown(self):
        for name,value in self.originals.items():setattr(f,name,value)
    def test_inspect_binds_complete_synthetic_population(self):
        table,audit=f.inspect_captures(self.contract.captures())
        self.assertEqual(table.num_rows,3);self.assertEqual(audit['count'],3)
        self.assertEqual(audit['state_rows'],{'DC':1,'TX':2})
        self.assertEqual(audit['composite_ID_duplicate_keys'],[])
        self.assertEqual(table['STRUCTURE_NUMBER_008'].to_pylist(),['0001           ','0002           ','1103(RAMP C)   '])
    def test_state_file_bytes_roster_and_population_drift(self):
        good=self.contract.captures()
        bad=dict(good);changed=state_body(record(structure=b'0999           '))
        bad['state_DC']=(changed,receipt(self.contract.urls['state_DC'],changed))
        with self.assertRaisesRegex(ValueError,'state_file_bytes_drift'):f.inspect_captures(bad)
        bad=dict(good);page=good['files_page'][0].replace(b'nbiState=DC25',b'nbiState=XX25')
        bad['files_page']=(page,receipt(self.contract.urls['files_page'],page))
        with self.assertRaisesRegex(ValueError,'producer_state_file_roster_drift'):f.inspect_captures(bad)
        bad=dict(good);del bad['state_TX']
        with self.assertRaisesRegex(ValueError,'capture_roster_drift'):f.inspect_captures(bad)
        extended=good['state_TX'][0]+record(state=b'486',structure=b'0003           ')+b'\r\n'
        bad=dict(good);bad['state_TX']=(extended,receipt(self.contract.urls['state_TX'],extended))
        rebind=dict(self.contract.expected);rebind['TX']=expected_for(extended,'486',rows=3)
        with mock.patch.object(f,'EXPECTED_FILES',rebind):
            with self.assertRaisesRegex(ValueError,'complete_count_drift'):f.inspect_captures(bad)
        duplicated=good['state_TX'][0]+record(state=b'486',structure=b'1103(RAMP C)')+b'\r\n'
        bad=dict(good);bad['state_TX']=(duplicated,receipt(self.contract.urls['state_TX'],duplicated))
        rebind=dict(self.contract.expected);rebind['TX']=expected_for(duplicated,'486',rows=3)
        with mock.patch.object(f,'EXPECTED_FILES',rebind):
            with self.assertRaisesRegex(ValueError,'composite_ID_duplicate_drift'):f.inspect_captures(bad)
    def test_receipt_strictness_and_first_party_provenance(self):
        n='state_DC';body=self.contract.dc
        item=f.validate_receipt(n,body,receipt(self.contract.urls[n],body))
        self.assertEqual(item['role'],'producer_publication');self.assertIn('Federal Highway Administration',item['producer'])
        self.assertEqual(item['release_date'],'2025-06-12')
        self.assertEqual(item['delegation_evidence']['producer_program_url'],f.PROGRAM)
        self.assertEqual(item['server_last_modified'],'Thu, 12 Jun 2025 16:53:24 GMT')
        for key,value in[('status',200.0),('bytes',True),('complete_body',1),('observed_at','2026-11-10'),('url',self.contract.urls[n]+'x'),('server_last_modified','')]:
            bad=receipt(self.contract.urls[n],body);bad[key]=value
            with self.assertRaises(ValueError,msg=key):f.validate_receipt(n,body,bad)
    def test_registration_explicit_asset_and_stopped_refresh(self):
        key=AssetKey(['ingest',f.SLUG,f.SLUG])
        self.assertEqual(set(defs.resolve_job_def('nbi_source_integrity').asset_layer.executable_asset_keys),{key})
        self.assertEqual(nbi_source_integrity_schedule.default_status,DefaultScheduleStatus.STOPPED)
        keys=set(defs.resolve_asset_graph().get_all_asset_keys())
        self.assertIn(key,keys);self.assertIn(AssetKey(['publish','catalog']),keys)
        self.assertIn(AssetKey(['publish','verify']),keys);self.assertIn(AssetKey(['publish','promote']),keys)
    def test_manifest_input_order_survives_canonical_roundtrip(self):
        captures=self.contract.captures();pin={'bytes':12,'sha256':'1'*64};identity={'explicit':'synthetic identity'}
        original=f.manifest_for(captures,identity,pin)
        self.assertEqual([r['input_id'].split(':')[0]for r in original['inputs']],sorted(captures))
        self.assertEqual(f.encode(original),f.encode(f.manifest_for(dict(reversed(list(captures.items()))),identity,pin)))
        self.assertEqual(original['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]['path'],'geoparquet/'+f.SLUG+'.parquet')
    def test_checked_private_READY_failure_precedes_any_outside_seal(self):
        captures=self.contract.captures()
        with tempfile.TemporaryDirectory()as d:
            original=f.write_private
            def failing(root,name,body):
                if name==f.READY:raise f.NBIQuarantineError('explicit synthetic READY write fault')
                return original(root,name,body)
            with mock.patch.object(f,'acquire',return_value=captures),mock.patch.object(f,'write_private',side_effect=failing):
                with self.assertRaisesRegex(ValueError,'READY write fault'):f.stage_national_bridge_inventory(StagingStorageResource(local_dir=d))
            self.assertFalse((Path(d)/'_construction-state').exists());self.assertFalse((Path(d)/f.SLUG).exists())
    def test_exact_ready_and_cached_catalog_sidecars(self):
        captures=self.contract.captures()
        with tempfile.TemporaryDirectory()as d:
            base=Path(d);version='v0.0.0-draft-explicit-synthetic';root=base/f.SLUG/f.SLUG/version
            (root/'geoparquet').mkdir(parents=True)
            table,audit=f.inspect_captures(captures)
            parquet=root/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(table),parquet,compression='zstd')
            identity={'fixture':'explicit synthetic candidate'};manifest=f.manifest_for(captures,identity,{'bytes':parquet.stat().st_size,'sha256':f.sha256_file(parquet)})
            for name,body in f.source_payloads(captures,identity,audit,manifest).items():f.write_private(root,name,body)
            ready={'approved':False,'version':version,'processing_fingerprint':f.digest(f.encode(identity)),
                'capture_fingerprint':f.source_fingerprint(captures),'objects':{str(p.relative_to(root)):f.sha256_file(p)for p in root.rglob('*')if p.is_file()}}
            f.write_private(root,f.READY,f.encode(ready));storage=StagingStorageResource(local_dir=d)
            f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
            bad=dict(ready);bad['approved']=0;f.write_private(root,f.READY,f.encode(bad))
            with self.assertRaisesRegex(ValueError,'candidate_ready_drift'):f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
            f.write_private(root,f.READY,f.encode(ready))
            f.write_private(root,'metadata/source/source_manifest.json',b'{"approved":true}')
            with self.assertRaisesRegex(ValueError,'cached_source_snapshot_drift'):f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
            f.write_private(root,'metadata/source/source_manifest.json',f.encode(manifest))
            snapshot_source_metadata(storage,f.SLUG,f.SLUG,version)
            resolved=load_resolved_source_manifest(storage,f.SLUG,f.SLUG,version)
            summary=summarize_staged_catalog(storage,f.SLUG,f.SLUG,version,f.SLUG,source_metadata=resolved.metadata)
            write_catalog_metadata(storage,f.SLUG,f.SLUG,version,summary.quality_manifest,summary.data_dictionary)
            f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
            self.assertTrue((root/'metadata/resolved_provenance.json').is_file())
            dictionary_path=root/'metadata/data_dictionary.json';dictionary_original=dictionary_path.read_bytes()
            dictionary=json.loads(dictionary_original);dictionary['columns'][0]['name']='TAMPERED_COLUMN'
            f.write_private(root,'metadata/data_dictionary.json',f.encode(dictionary))
            with self.assertRaisesRegex(ValueError,'cached_catalog_sidecar_semantics_invalid'):f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
            f.write_private(root,'metadata/data_dictionary.json',dictionary_original)
            f.validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)


@unittest.skipUnless(os.environ.get('HIFLD_NBI_TEST_CAPTURE_DIR'),'Complete official real capture is explicit external input')
class CompleteNBI(unittest.TestCase):
    def test_complete_official_2025_ascii_release(self):
        root=Path(os.environ['HIFLD_NBI_TEST_CAPTURE_DIR'])
        captures={n:(f.bounded_input(root/(n+'.body'),f.MAX_BODY_BYTES),f.strict_json(f.bounded_input(root/(n+'.receipt.json'),65536)))for n in f.URLS}
        table,audit=f.inspect_captures(captures)
        self.assertEqual(table.num_rows,624193);self.assertEqual(table.num_columns,125)
        self.assertEqual(audit['state_rows']['DC'],261);self.assertEqual(audit['non_ascii_bytes'],{'NY':5,'PR':2})

if __name__=='__main__':unittest.main()
