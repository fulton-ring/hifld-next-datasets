"""Small, explicit synthetic SNBI fixtures; the complete official input is opt-in."""
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
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import national_bridge_inventory as f
from dagster_hifld.definitions import defs
from dagster_hifld.assets.national_bridge_inventory import nbi_source_integrity_schedule

SCHEMA={'$id':'synthetic.schema.json','title':'Synthetic SNBI','type':'array','items':{'required':['BL01','BID01'],'properties':{
    'BL01':{'description':'State Code','type':'integer'},'BID01':{'description':'Bridge Number','type':'string'},
    'BID02':{'description':'Bridge Name','type':'string'},'BL05':{'description':'Latitude','type':'number'},
    'BL06':{'description':'Longitude','type':'number'},'BG01':{'description':'NBIS Bridge Length','type':'number'},
    'BC01':{'description':'Deck Condition Rating','type':'string'},
    'Features':{'description':'Features','type':'array','items':{'type':'object','required':['BF01'],'properties':{
        'BF01':{'description':'Feature Type','type':'string'},'BF03':{'description':'Feature Name','type':'string'},
        'BH08':{'description':'Lanes on Highway','type':'integer'},'Routes':{'type':'array','items':{'type':'object','required':['BRT01'],'properties':{
            'BRT01':{'description':'Route Designation','type':'string'},'BRT02':{'description':'Route Number','type':'string'}}}}}}}}}}

def record(state=11,number='0001',name='Chain Bridge',lat=38.929444,lon=-77.116972,features=None):
    return {'BL01':state,'BID01':number,'BID02':name,'BL05':lat,'BL06':lon,'BG01':1383.0,'BC01':'7',
        'Features':features if features is not None else [{'BF01':'H01','BF03':'Chain Bridge Road','BH08':3.0,'Routes':[{'BF01':'H01','BRT01':'R01','BRT02':'0'}]}]}

def state_body(*records):
    return json.dumps(list(records),separators=(',',':')).encode()

def release_page(rows):
    cells=[]
    for name,href,count in rows:
        links=''
        if href:
            links=f'<td><a href="{href}">JSON</a></td>'
        cells.append(f'<tr><th scope="row" class="row">{name}</th><td class="txtright">{count:,}</td>{links}</tr>')
    total=sum(count for _,_,count in rows)
    cells.append(f'<tr><th scope="row" class="row">Total: All Hwy Bridges</th><td class="txtright">{total:,}</td></tr>')
    return ('<table>'+''.join(cells)+'</table>').encode()

def receipt(url,body,**over):
    value={'url':url,'final_url':url,'status':200,'bytes':len(body),'sha256':f.digest(body),'complete_body':True,
        'observed_at':'2026-11-17T00:00:00+00:00','server_last_modified':'Wed, 07 Oct 2026 12:00:00 GMT'}
    value.update(over);return value

class Contract:
    def __init__(self,tx_records=None,dc_records=None):
        self.index=b'<a href="nbi_data_2025.cfm">2025</a><a href="nbi_data_2026.cfm">2026</a>'
        self.release=release_page([('District of Columbia','2026/JSON/DC26.json',len(dc_records or [record()])),
            ('Texas','2026/JSON/TX26.json',len(tx_records or [record(state=48,number='0002'),record(state=48,number='1103(RAMP C)')])),
            ('Massachusetts','2026/JSON/MS26.json',1),('Guam',None,0)])
        self.schema_page=b'<a href="SNBI.Schema.v04.json">schema</a>'
        self.schema=json.dumps(SCHEMA,separators=(',',':')).encode()
        self.bodies={'index':self.index,'release':self.release,'schema_page':self.schema_page,'schema':self.schema,
            'state_DC':state_body(*(dc_records or [record()])),
            'state_TX':state_body(*(tx_records or [record(state=48,number='0002'),record(state=48,number='1103(RAMP C)')])),
            'state_MA':state_body(record(state=25,number='0003',name='Berkshire Bridge'))}
    def urls(self):
        base='https://www.fhwa.dot.gov/bridge/nbi/2026/JSON/'
        return {'index':f.INDEX_URL,'release':'https://www.fhwa.dot.gov/bridge/nbi/nbi_data_2026.cfm','schema_page':f.SCHEMA_PAGE_URL,
            'schema':'https://www.fhwa.dot.gov/bridge/snbi/SNBI.Schema.v04.json','state_DC':base+'DC26.json','state_TX':base+'TX26.json','state_MA':base+'MA26.json'}
    def captures(self):
        urls=self.urls();return {name:(body,receipt(urls[name],body))for name,body in self.bodies.items()}


class NBIParseTests(unittest.TestCase):
    def test_discover_release_uses_newest_year(self):
        year,url=f.discover_release(b'<a href="nbi_data_2024.cfm">2024</a><a href="nbi_data_2026.cfm">2026</a>')
        self.assertEqual((year,url),(2026,'https://www.fhwa.dot.gov/bridge/nbi/nbi_data_2026.cfm'))
    def test_release_page_roster_total_and_massachusetts_link_correction(self):
        with mock.patch.object(f,'MIN_STATES',1):
            release=f.parse_release_page(Contract().release,2026,'https://www.fhwa.dot.gov/bridge/nbi/nbi_data_2026.cfm')
        self.assertEqual(release['total'],4);self.assertEqual(set(release['states']),{'DC','TX','MA'})
        self.assertEqual(release['states']['MA']['url'],'https://www.fhwa.dot.gov/bridge/nbi/2026/JSON/MA26.json')
        self.assertEqual(release['link_corrections']['Massachusetts'],{'page_token':'MS','used_token':'MA'})
    def test_parse_preserves_native_types_and_nested_arrays(self):
        table,ids,stats=f.parse_state_file(state_body(record()),'DC',SCHEMA)
        self.assertEqual(table.num_rows,1);self.assertEqual(table.num_columns,10)
        self.assertEqual(table['BL01'].to_pylist(),[11]);self.assertEqual(table['BID01'].to_pylist(),['0001'])
        self.assertEqual(table['BG01'].to_pylist(),[1383.0]);self.assertEqual(table['DATA_YEAR'].to_pylist(),[None])
        self.assertEqual(table['Features'].to_pylist(),['[{"BF01":"H01","BF03":"Chain Bridge Road","BH08":3.0,"Routes":[{"BF01":"H01","BRT01":"R01","BRT02":"0"}]}]'])
        self.assertEqual(table['geometry'].to_pylist(),[struct.pack('<BIdd',1,1,-77.116972,38.929444)])
        self.assertEqual(ids,[(11,'0001')]);self.assertEqual(stats['nested_counts'],{'Features':1});self.assertEqual(stats['producer_extra_counts'],{'DATA_YEAR':0})
    def test_known_producer_extra_data_year_is_preserved_as_served(self):
        table,ids,stats=f.parse_state_file(state_body(record(),{**record(number='0002'),'DATA_YEAR':'8/4/2026'}),'CT',SCHEMA)
        self.assertEqual(table['DATA_YEAR'].to_pylist(),[None,'8/4/2026'])
        self.assertEqual(stats['producer_extra_counts'],{'DATA_YEAR':1})
    def test_geometry_categories_and_out_of_envelope_values(self):
        body=state_body(record(lat=None,lon=None),record(number='0002',lat=None),record(number='0003',lat=0.0,lon=0.0),record(number='0004',lat=95.0,lon=200.0))
        table,ids,stats=f.parse_state_file(body,'DC',SCHEMA)
        self.assertEqual(table['geometry'].to_pylist()[:3],[None,None,struct.pack('<BIdd',1,1,0.0,0.0)])
        self.assertEqual((stats['blank'],stats['partial'],stats['zero'],stats['out_of_envelope']),(1,1,1,1))
    def test_top_level_type_and_required_fields_fail_closed(self):
        with self.assertRaisesRegex(ValueError,'snbi_field_type_drift'):f.parse_state_file(state_body(record(number=1)),'DC',SCHEMA)
        bad=record();del bad['BID01']
        with self.assertRaisesRegex(ValueError,'state_record_required_field_missing'):f.parse_state_file(state_body(bad),'DC',SCHEMA)
        bad=record();bad['RENAMED']='x'
        with self.assertRaisesRegex(ValueError,'state_record_unknown_field'):f.parse_state_file(state_body(bad),'DC',SCHEMA)


class NBIContractTests(unittest.TestCase):
    def setUp(self):
        self.contract=Contract();self.patch=mock.patch.object(f,'MIN_STATES',1);self.patch.start()
    def tearDown(self):self.patch.stop()
    def test_inspect_binds_complete_population(self):
        table,audit=f.inspect_captures(self.contract.captures())
        self.assertEqual(table.num_rows,4);self.assertEqual(audit['count'],4);self.assertEqual(audit['release_year'],2026)
        self.assertEqual(audit['state_rows'],{'DC':1,'TX':2,'MA':1});self.assertEqual(audit['composite_ID_duplicate_count'],0)
        self.assertEqual(audit['field_count'],8);self.assertEqual(audit['release_page_total'],4)
        self.assertEqual(audit['release_page_count_drift'],{});self.assertEqual(audit['release_page_total_drift'],0)
        self.assertEqual(audit['producer_extra_field_counts'],{'DATA_YEAR':0})
    def test_duplicate_composite_ids_are_counted_not_merged(self):
        tx=[record(state=48,number='0002'),record(state=48,number='0002')]
        table,audit=f.inspect_captures(Contract(tx_records=tx).captures())
        self.assertEqual(table.num_rows,4);self.assertEqual(audit['composite_ID_duplicate_count'],1)
        self.assertEqual(audit['composite_ID_duplicate_keys'],[[48,'0002']])
    def test_release_page_counts_are_advisory_and_recorded(self):
        captures=self.contract.captures();bad=dict(captures);body=state_body(record(),record(number='0002'))
        bad['state_DC']=(body,receipt(captures['state_DC'][1]['url'],body))
        table,audit=f.inspect_captures(bad)
        self.assertEqual(table.num_rows,5);self.assertEqual(audit['count'],5)
        self.assertEqual(audit['release_page_count_drift'],{'DC':1});self.assertEqual(audit['release_page_total_drift'],1)
        bad=dict(captures);del bad['state_TX']
        with self.assertRaisesRegex(ValueError,'capture_roster_drift'):f.inspect_captures(bad)
    def test_receipt_strictness_and_first_party_provenance(self):
        captures=self.contract.captures();url=captures['state_DC'][1]['url'];body=captures['state_DC'][0]
        item=f.validate_receipt('state_DC',body,receipt(url,body),self.contract.urls())
        self.assertEqual(item['role'],'producer_publication');self.assertIn('Federal Highway Administration',item['producer'])
        self.assertEqual(item['release_date'],'2026-10-07');self.assertEqual(item['delegation_evidence']['release_index_url'],f.INDEX_URL)
        for key,value in[('status',200.0),('bytes',True),('complete_body',1),('observed_at','2026-11-10'),('url',url+'x'),('server_last_modified','')]:
            bad=receipt(url,body);bad[key]=value
            with self.assertRaises(ValueError,msg=key):f.validate_receipt('state_DC',body,bad,self.contract.urls())
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
        self.assertIn('2026',original['title'])
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
    def test_complete_official_snbi_release(self):
        root=Path(os.environ['HIFLD_NBI_TEST_CAPTURE_DIR'])
        names=('index','release','schema_page','schema')
        captures={name:(f.bounded_input(root/(name+'.body'),f.MAX_BODY_BYTES),f.strict_json(f.bounded_input(root/(name+'.receipt.json'),65536)))for name in names}
        _,release,_=f.context_from_captures(captures)
        for token in release['states']:
            captures['state_'+token]=(f.bounded_input(root/('state_'+token+'.body'),f.MAX_BODY_BYTES),f.strict_json(f.bounded_input(root/('state_'+token+'.receipt.json'),65536)))
        table,audit=f.inspect_captures(captures)
        self.assertGreaterEqual(table.num_rows,600000);self.assertEqual(audit['release_year'],2026)

if __name__=='__main__':unittest.main()
