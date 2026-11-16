"""Two-state roster and separately cached NAD83 geocoding contracts."""
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.sources import childcare_roster_pilot as s
from dagster_hifld.resources import StagingStorageResource, PublishedStorageResource
from dagster_hifld.release_approval import ReleaseApprovalError, require_release_approval


class PilotTests(unittest.TestCase):
    def rows(self):
        return [dict(_socrata_row_id='row-one',wacompassid='001abc',famlinkid='001',
            facilitytypegeneric='CHILD CARE CENTER',latestoperatingstatus='Active',
            physciallatitude='0.0000',physicallongitude='0.0000',physicalstreetaddress='4600 Silver Hill Rd',
            physicalcity='Suitland',physicalstate='WA',physicalzip='98501'),
            dict(_socrata_row_id='row-two',wacompassid='001abc',facilitytypegeneric='SCHOOL-AGE PROGRAM',
                 latestoperatingstatus='Not Active',physciallatitude='47',physicallongitude='-122')]

    def test_registered_assets_and_checks_are_manual(self):
        from dagster_hifld.assets import childcare_roster_pilot as assets
        self.assertEqual(len(assets.INGEST_ASSETS),2)
        self.assertEqual(len(assets.ASSET_CHECKS),2)
        self.assertEqual(assets.SCHEDULES,[])
        self.assertTrue(assets.pilot_complete({'feature_count':1,'roster_count':3,'outcome_count':3}))
        self.assertFalse(assets.pilot_complete({'feature_count':1,'roster_count':3,'outcome_count':2}))

    def test_eligibility_keeps_original_coordinates_and_withheld_rows(self):
        row=self.rows()[0]
        self.assertEqual(s.classify_location('wa',row)[0],'eligible')
        self.assertEqual(s.classify_location('wa',self.rows()[1])[0],'agency_location_preserved')
        for changes,status in [({'physicalstreetaddress':'P.O. Box 12'},'po_box'),
                               ({'physicalcity':''},'incomplete_address'),
                               ({'latestoperatingstatus':'Not Active'},'inactive')]:
            self.assertEqual(s.classify_location('wa',{**row,**changes})[0],status)
        ny=dict(facility_id='0001',program_type='DCC',address_omitted='Y',latitude='42',longitude='-73')
        self.assertEqual(s.classify_location('ny',ny)[0],'withheld')
        self.assertEqual(row['physciallatitude'],'0.0000')

    def test_batch_contract_accounts_all_ids_and_retains_match_payload(self):
        request=[['technical-1','1 Main St','Albany','NY','01234'],['technical-2','2 Main St','Albany','NY','01234'],['technical-3','3 Main St','Albany','NY','01234']]
        out=io.StringIO();csv.writer(out).writerows([
            ['technical-3','3 MAIN','Tie'],['technical-1','1 MAIN','Match','Non_Exact','1 MAIN','-73.1,42.1','123','L'],
            ['technical-2','2 MAIN','No_Match']])
        result=s.parse_batch(out.getvalue().encode(),request)
        self.assertEqual(set(result),{r[0] for r in request})
        self.assertEqual(result['technical-1']['coordinates'],[-73.1,42.1])
        self.assertEqual(result['technical-1']['response_fields'][3],'Non_Exact')
        for invalid in [out.getvalue().replace('technical-2','foreign'),out.getvalue()+out.getvalue(),
                        out.getvalue().replace('-73.1,42.1','nan,42.1'),out.getvalue().replace('-73.1,42.1','0,0'),
                        out.getvalue().replace('Match,Non_Exact','Unexpected,Non_Exact')]:
            with self.subTest(invalid=invalid),self.assertRaises(ValueError):s.parse_batch(invalid.encode(),request)
        with self.assertRaises(ValueError):s.parse_batch(b'',request)

    def test_native_schema_count_id_and_category_drift(self):
        meta={'columns':[{'fieldName':k,'dataTypeName':'text'} for k in self.rows()[0] if k!='_socrata_row_id']}
        audit=s.validate_rows('wa',self.rows(),meta,2)
        self.assertEqual(audit['duplicate_native_id_count'],1)
        self.assertEqual(audit['source_feature_count'],2)
        for rows,count in [([self.rows()[0]],2),([{**self.rows()[0],'wacompassid':None}],1),
                           ([{**self.rows()[0],'_socrata_row_id':''}],1),
                           ([{**self.rows()[0],'unexpected':'x'}],1),
                           ([{**self.rows()[0],'wacompassid':12}],1),
                           ([{**self.rows()[0],'facilitytypegeneric':'FAMILY HOME'}],1)]:
            with self.subTest(rows=rows),self.assertRaises(ValueError):s.validate_rows('wa',rows,meta,count)

    def test_freeze_replay_tamper_and_http_quarantine(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); calls=[]
            def handler(request):
                calls.append(str(request.url));return httpx.Response(200,stream=httpx.ByteStream(b'{"hello": "world"}'))
            with httpx.Client(transport=httpx.MockTransport(handler)) as client:
                body,receipt=s.frozen_get(client,'https://data.wa.gov/test',root,'item')
                body2,receipt2=s.frozen_get(client,'https://data.wa.gov/test',root,'item')
                self.assertEqual((body,receipt),(body2,receipt2));self.assertEqual(len(calls),1)
                (root/'item.body').write_bytes(b'tamper')
                with self.assertRaises(ValueError):s.frozen_get(client,'https://data.wa.gov/test',root,'item')
        for mode in ['status','partial']:
            with tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);storage=StagingStorageResource(use_local=True,local_dir=str(root/'staging'),prefix='')
                def handler(request):
                    return httpx.Response(503 if mode=='status' else 200,stream=httpx.ByteStream(b'{}'),
                                          headers={'content-length':'10'} if mode=='partial' else {})
                real=httpx.Client
                with patch.object(s.httpx,'Client',side_effect=lambda **kw:real(transport=httpx.MockTransport(handler),**kw)):
                    with self.assertRaises(ValueError):s.acquire_roster(storage,'wa',str(root/'captures'))
                self.assertTrue(list((root/'staging'/'_quarantine').rglob('failure.json')))

    def test_native_acquisition_success_replay_and_witness_drift(self):
        rows=self.rows()
        fields=sorted(set().union(*(set(r) for r in rows))-{'_socrata_row_id'})
        columns=[{'fieldName':k,'dataTypeName':'text'} for k in fields]
        meta={'id':'was8-3ni8','attribution':'DCYF','owner':{'displayName':'DCYF Open Data'},
              'rowsUpdatedAt':1793994541,'columns':columns,
              'metadata':{'custom_fields':{'Temporal':{'Posting Frequency':'Monthly'}}}}
        counts=[{'facilitytypegeneric':'CHILD CARE CENTER','n':'1'},
                {'facilitytypegeneric':'SCHOOL-AGE PROGRAM','n':'1'},
                {'facilitytypegeneric':'FAMILY HOME','n':'7'}]
        ids=[{k:r[k] for k in ('_socrata_row_id','wacompassid')} for r in rows]
        for mode in ['good','count','id','schema']:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);storage=StagingStorageResource(use_local=True,local_dir=str(root/'staging'),prefix='');calls=[]
                def handler(request):
                    calls.append(str(request.url));n=len(calls)
                    body=meta if n in (1,7) else counts if n in (2,6) else rows if n==4 else ids
                    body=json.loads(json.dumps(body))
                    if mode=='count' and n==6:body[0]['n']='2'
                    if mode=='id' and n==5:body[0]['wacompassid']='changed'
                    if mode=='schema' and n==7:body['columns'][0]['dataTypeName']='number'
                    return httpx.Response(200,stream=httpx.ByteStream(s.canonical(body)))
                spec={**s.SOURCES['wa'],'schema':s.sha(s.canonical([(k,'text') for k in fields]))}
                real=httpx.Client
                with patch.dict(s.SOURCES,{'wa':spec}),patch.object(s.httpx,'Client',side_effect=lambda **kw:real(transport=httpx.MockTransport(handler),**kw)):
                    if mode!='good':
                        with self.assertRaises(ValueError):s.acquire_roster(storage,'wa',str(root/'captures'))
                        self.assertTrue(list((root/'staging'/'_quarantine').rglob('failure.json')))
                    else:
                        first=s.acquire_roster(storage,'wa',str(root/'captures'))
                        second=s.acquire_roster(storage,'wa',str(root/'captures'))
                        self.assertEqual(first['rows'],second['rows']);self.assertEqual(len(calls),7)
                        self.assertEqual(first['audit']['excluded_category_count'],7)
                        (Path(first['capture_root'])/'roster.body').write_bytes(b'tampered')
                        with self.assertRaises(ValueError):s.acquire_roster(storage,'wa',str(root/'captures'))

    def test_all_unmatched_checkpoint_keeps_deferred_and_quarantines_bad_ids(self):
        for status in ['No_Match','Tie','bad-id']:
            with self.subTest(status=status),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);storage=StagingStorageResource(use_local=True,local_dir=str(root/'staging'),prefix='')
                rows=[self.rows()[0],{**self.rows()[0],'_socrata_row_id':'another-row'}]
                artifact={'state':'wa','rows':rows,'source_record_sha256':s.sha(s.canonical(rows)),
                          'audit':{'source_feature_count':2},'captures':{},'metadata':{'columns':[]}}
                def handler(request):
                    if request.method=='POST':
                        body=('foreign,INPUT,No_Match\n' if status=='bad-id' else 'technical-00000000,INPUT,'+status+'\n').encode()
                    else:body=s.canonical({'benchmarks':[{'id':'4','benchmarkName':'Public_AR_Current'}]})
                    return httpx.Response(200,stream=httpx.ByteStream(body))
                real=httpx.Client
                with patch.object(s.httpx,'Client',side_effect=lambda **kw:real(transport=httpx.MockTransport(handler),**kw)):
                    with self.assertRaises(ValueError):s.geocode_roster(storage,artifact,str(root/'geocoder'),1)
                if status=='bad-id':
                    self.assertTrue(list((root/'staging'/'_quarantine').rglob('failure.json')))
                else:
                    paths=list((root/'geocoder').rglob('complete-outcomes.json'))
                    self.assertEqual(len(paths),1)
                    self.assertEqual([r['status'] for r in json.loads(paths[0].read_bytes())],[status,'deferred'])
                    self.assertEqual(json.loads(paths[0].with_name('complete-roster.json').read_bytes()),rows)

    def test_geocoding_stage_ledger_cache_catalog_and_refusal(self):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);storage=StagingStorageResource(use_local=True,local_dir=str(root/'staging'),prefix='')
            rows=self.rows(); rowbody=s.canonical(rows)
            artifact={'state':'wa','rows':rows,'source_record_sha256':s.sha(rowbody),
                      'audit':{'source_feature_count':2,'duplicate_native_id_count':1},'captures':{},
                      'metadata':{'rowsUpdatedAt':1793994541,'columns':[]}}
            # Tests exercise construction with real identity of this committed test module.
            calls=[]
            def handler(request):
                calls.append(str(request.url))
                if request.method=='POST':
                    return httpx.Response(200,stream=httpx.ByteStream(b'"technical-00000000","INPUT","Match","Exact","MATCHED","-122.1,47.1","123","L"\n'))
                return httpx.Response(200,stream=httpx.ByteStream(s.canonical({'benchmarks':[{'id':'4','benchmarkName':'Public_AR_Current'}]})))
            real=httpx.Client
            with patch.object(s.httpx,'Client',side_effect=lambda **kw:real(transport=httpx.MockTransport(handler),**kw)):
                first=s.geocode_roster(storage,artifact,str(root/'geocoder'),1)
                second=s.geocode_roster(storage,artifact,str(root/'geocoder'),1)
            self.assertEqual(len(calls),2);self.assertTrue(second['reused'])
            prefix=first['prefix'];key=prefix+'/geoparquet/'+s.FILES['wa']+'.parquet'
            table=pq.read_table(root/'staging'/key)
            self.assertEqual(table.num_rows,1)
            ledger=json.loads(storage.read_key(prefix+'/metadata/geocoding_outcomes.json'))
            self.assertEqual([r['status'] for r in ledger],['Match','agency_location_preserved'])
            self.assertEqual(json.loads(table['source_json'][0].as_py())['wacompassid'],'001abc')
            geo=json.loads(table.schema.metadata[b'geo']);self.assertEqual(CRS.from_json_dict(geo['columns']['geometry']['crs']).to_epsg(),4269)
            summary=summarize_staged_catalog(storage,s.DATASET,s.FILES['wa'],first['version'],'pilot')
            self.assertEqual(summary.quality_manifest['feature_count'],1)
            self.assertEqual(summary.quality_manifest['null_geometry_count'],0)
            self.assertTrue(summary.quality_manifest['quality_check_passed'])
            write_catalog_metadata(storage,s.DATASET,s.FILES['wa'],first['version'],summary.quality_manifest,summary.data_dictionary)
            destination=PublishedStorageResource(use_local=True,local_dir=str(root/'published'),prefix='')
            (root/'published').mkdir();(root/'published'/'existing').write_bytes(b'keep')
            with patch.dict('os.environ',{},clear=True),self.assertRaises(ReleaseApprovalError):require_release_approval(storage,destination,[(key,key)])
            self.assertEqual((root/'published'/'existing').read_bytes(),b'keep')
            (root/'staging'/key).write_bytes(b'tamper')
            with patch.object(s.httpx,'Client',side_effect=lambda **kw:real(transport=httpx.MockTransport(handler),**kw)):
                with self.assertRaises(ValueError):s.geocode_roster(storage,artifact,str(root/'geocoder'),1)


if __name__=='__main__':unittest.main()
