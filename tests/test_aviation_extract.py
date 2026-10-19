import copy
import datetime
import hashlib
import io
import json
from pathlib import Path
import tempfile
import subprocess
import unittest
from unittest import mock

import pyarrow.parquet as pq
from dagster import build_schedule_context, DefaultScheduleStatus, materialize
from dagster_hifld import aviation_extract as a
from dagster_hifld.assets.faa import _stage_aviation, faa_aviation_draft_refresh_schedule, faa_aviation_facilities
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class AviationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.capture = self.root/'capture'; self.capture.mkdir()
        self.storage = StagingStorageResource(local_dir=str(self.root/'stage'))
        fields = json.loads((Path(__file__).parents[1]/'docs/dataset-sources/aviation-facilities/draft-contract.json').read_text())['source_fields']
        attrs = {f['name']: ('000001' if f['type']=='esriFieldTypeString' else 1.25 if f['type']=='esriFieldTypeDouble' else 7) for f in fields}
        first = dict(attrs, OBJECTID=2**53+1, SITE_NO='00001.00', ARPT_ID='A001')
        second = dict(attrs, OBJECTID=2**53+2, SITE_NO='00001.00', ARPT_ID='A002', LAT_DEG=None)
        layer = {'fields':fields,'geometryType':'esriGeometryPoint','objectIdField':'OBJECTID',
            'extent':{'spatialReference':{'wkid':102100,'latestWkid':3857}},
            'editingInfo':{'dataLastEditDate':1790959621805}}
        ids=[first['OBJECTID'],second['OBJECTID']]
        self.data={'item':{'id':a.ITEM_ID,'owner':'USDOT_BTS','url':a.SERVICE.rsplit('/',1)[0],'licenseInfo':'Synthetic unrestricted fixture license','accessInformation':'Synthetic FAA producer / BTS distributor fixture attribution'},
            'bts_layer':layer,'bts_layer_after':copy.deepcopy(layer),
            'bts_count':{'count':2},'bts_count_after':{'count':2},
            'bts_ids':{'objectIds':ids},'bts_ids_after':{'objectIds':ids},
            'page-000':{'fields':[{'name':f['name'],'type':f['type']} for f in fields],
                'spatialReference':layer['extent']['spatialReference'],
                'features':[{'attributes':first,'geometry':{'x':1.25,'y':2.5}},
                            {'attributes':second,'geometry':{'x':3.5,'y':4.75}}]}}
        self.write()

    def write(self):
        urls=a.expected_requests([2**53+1,2**53+2])
        for name,value in self.data.items():
            body=json.dumps(value).encode();(self.capture/(name+'.body')).write_bytes(body)
            receipt={'url':urls[name],'final_url':urls[name],'bytes':len(body),
                'sha256':hashlib.sha256(body).hexdigest(),'status':200,'complete_body':True,
                'observed_at':'2026-10-19T00:00:00+00:00'}
            (self.capture/(name+'.receipt.json')).write_text(json.dumps(receipt))

    def test_processing_provenance_binds_actual_worktree_commit(self):
        expected=subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[1],text=True).strip()
        identity=a.processing_identity()
        self.assertEqual(identity['code_commit'],expected)
        self.assertEqual(len(identity['installed_pipeline_sha256']),64)

    def test_full_native_types_ids_duplicates_and_idempotency(self):
        result=_stage_aviation(self.capture,self.storage); again=_stage_aviation(self.capture,self.storage)
        self.assertEqual(result,again)
        root=self.root/'stage'/a.SLUG/a.SLUG/result['version']
        t=pq.read_table(root/'draft_candidate/candidate.parquet')
        self.assertEqual(t.num_rows,2);self.assertEqual(t['OBJECTID'].to_pylist(),[2**53+1,2**53+2])
        self.assertEqual(t['SITE_NO'].to_pylist(),['00001.00','00001.00'])
        self.assertEqual(str(t.schema.field('LAT_DEG').type),'int32');self.assertEqual(t['LAT_DEG'].to_pylist(),[7,None])
        audit=a.verify_typed_geopackage(root/'draft_candidate/candidate.parquet',root/'geopackage'/f'{a.SLUG}.gpkg')
        self.assertTrue(audit['all_native_geometry_wkb_equal'])
        self.assertFalse(result['promotion_allowed'])
        manifest=json.loads((root/'metadata/source_manifest.json').read_bytes())
        a.validate_release_provenance(manifest)
        self.assertEqual(len(manifest['inputs']),8)
        for item in manifest['inputs']:
            self.assertEqual(item['observed_final_url'],item['retrieval_url'])
            self.assertEqual(item['sha256'],self.storage.sha256_key(item['raw_key']))
        self.assertEqual(manifest['processor'],'HIFLD Next')

    def test_completeness_and_drift_quarantine_preserves_original(self):
        result=_stage_aviation(self.capture,self.storage)
        before={str(p.relative_to(self.root/'stage')):hashlib.sha256(p.read_bytes()).hexdigest() for p in (self.root/'stage').rglob('*') if p.is_file()}
        self.data['page-000']['features'][0]['attributes'].pop('ARPT_ID');self.write()
        with self.assertRaisesRegex(ValueError,'Missing/extra'): _stage_aviation(self.capture,self.storage)
        for name,digest in before.items():self.assertEqual(hashlib.sha256((self.root/'stage'/name).read_bytes()).hexdigest(),digest)
        self.assertTrue(list((self.root/'stage/quarantine').rglob('*.body')))

    def test_count_ids_schema_edit_crs_and_transfer_fail_closed(self):
        original=copy.deepcopy(self.data)
        cases=[('bts_count_after','count',1),('bts_ids_after','objectIds',[1,2]),
               ('bts_layer_after','editingInfo',{'dataLastEditDate':1}),
               ('page-000','exceededTransferLimit',True)]
        for name,key,value in cases:
            self.data=copy.deepcopy(original);self.data[name][key]=value;self.write()
            with self.assertRaises(ValueError):a.inspect_capture(self.capture)
        self.data=copy.deepcopy(original);self.data['bts_layer']['fields'][0]['type']='esriFieldTypeDouble'
        self.data['bts_layer_after']=copy.deepcopy(self.data['bts_layer']);self.write()
        with self.assertRaisesRegex(ValueError,'schema drift'):a.inspect_capture(self.capture)

    def test_all_replayed_http200_application_errors_quarantine_raw(self):
        original=copy.deepcopy(self.data)
        for name in original:
            with self.subTest(name=name):
                self.data=copy.deepcopy(original)
                self.data[name]['error']={'code':500,'message':'Synthetic upstream failure'}
                self.write()  # Deliberately repin receipt SHA/length: status200 is insufficient.
                with self.assertRaisesRegex(ValueError,'application error'):
                    _stage_aviation(self.capture,self.storage)
        retained=list((self.root/'stage/quarantine').rglob('*.body'))
        self.assertTrue(retained)
        self.assertFalse(list((self.root/'stage').glob('aviation-facilities/aviation-facilities/*/draft_candidate/candidate.parquet')))
        self.assertTrue(any(b'Synthetic upstream failure' in p.read_bytes() for p in retained))

    def test_fractional_boolean_overflow_and_duplicate_oid(self):
        for value in (True,1.5,2**63,None):
            self.data['bts_ids']['objectIds']=[value,2];self.write()
            with self.assertRaises(ValueError):a.inspect_capture(self.capture)

    def test_integer_to_double_precision_loss_and_duplicate_native_oid_rejected(self):
        self.data['page-000']['features'][0]['attributes']['LAT_SEC']=2**53+1
        self.write()
        with self.assertRaisesRegex(ValueError,'Invalid double'):a.inspect_capture(self.capture)
        self.data['page-000']['features'][0]['attributes']['LAT_SEC']=1.25
        self.data['page-000']['features'][1]['attributes']['OBJECTID']=2**53+1
        self.write()
        with self.assertRaisesRegex(ValueError,'Duplicate'):a.inspect_capture(self.capture)

    def test_receipts_bind_original_final_url_status_size_and_hash(self):
        p=self.capture/'page-000.receipt.json';original=p.read_bytes()
        for key,value in [('url','https://mirror.example/query'),('final_url','https://mirror.example/query'),('status',403),('bytes',1),('sha256','0'*64),('complete_body',None)]:
            receipt=json.loads(original);receipt[key]=value;p.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError,'receipt'):a.inspect_capture(self.capture)
        p.write_bytes(original.replace(b'"status": 200',b'"status": 403, "status": 200'))
        with self.assertRaisesRegex(ValueError,'Duplicate JSON'):a.inspect_capture(self.capture)

    def test_native_integer_coordinate_precision_refused(self):
        with self.assertRaisesRegex(ValueError,'precision'):a._exact_point({'x':2**53+1,'y':1})
        for value in (True,float('nan'),float('inf')):
            with self.assertRaises(ValueError):a._exact_point({'x':value,'y':1})

    def test_published_and_remote_storage_rejected_before_input_or_fetch(self):
        for storage in (PublishedStorageResource(local_dir=str(self.root/'output')),StagingStorageResource(use_local=False,bucket='unused')):
            with mock.patch.object(a,'_capture_json') as fetch:
                with self.assertRaises(ValueError):_stage_aviation(self.root/'missing',storage)
                with self.assertRaises(ValueError):a.stage_aviation_capture(self.root/'missing',storage,'v0.0.20-draft')
                fetch.assert_not_called()

    def test_actual_asset_rejects_published_storage_before_live_capture(self):
        with mock.patch.object(a,'capture_live') as capture:
            result=materialize([faa_aviation_facilities],
                resources={'staging_storage':PublishedStorageResource(local_dir=str(self.root/'published'))},
                raise_on_error=False)
        self.assertFalse(result.success);capture.assert_not_called()
        self.assertFalse((self.root/'published').exists())

    def test_refresh_schedule_stopped_local_and_source_only(self):
        self.assertEqual(faa_aviation_draft_refresh_schedule.default_status,DefaultScheduleStatus.STOPPED)
        context=build_schedule_context(scheduled_execution_time=datetime.datetime(2026,10,19,tzinfo=datetime.timezone.utc),resources={'staging_storage':self.storage})
        self.assertEqual(faa_aviation_draft_refresh_schedule.evaluate_tick(context).run_requests[0].run_key,'aviation-draft-2026-10-19')
        context=build_schedule_context(scheduled_execution_time=datetime.datetime(2026,10,19,tzinfo=datetime.timezone.utc),resources={'staging_storage':PublishedStorageResource(local_dir=str(self.root/'output'))})
        self.assertIn('isolated',faa_aviation_draft_refresh_schedule.evaluate_tick(context).skip_message)

    def test_partial_network_attempt_and_error_receipt_retained(self):
        class Broken:
            status=200;headers={}
            def geturl(self):return a.ITEM_URL
            def read(self,n):raise __import__('http.client').client.IncompleteRead(b'partial',10)
            def close(self):pass
        dest=self.root/'failed';dest.mkdir()
        with mock.patch.object(a.urllib.request,'urlopen',return_value=Broken()):
            with self.assertRaisesRegex(ValueError,'bounded attempts'):a._capture_json(dest,'item',a.ITEM_URL,attempts=2)
        self.assertEqual((dest/'attempts/item/000.body').read_bytes(),b'partial')
        receipt=json.loads((dest/'attempts/item/001.receipt.json').read_bytes())
        self.assertIn('IncompleteRead',receipt['error']);self.assertEqual(receipt['final_url'],a.ITEM_URL)
