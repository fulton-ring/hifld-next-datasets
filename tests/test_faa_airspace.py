import copy
import hashlib
import io
import urllib.error
from unittest import mock
import json
import pathlib
import tempfile
import unittest

from dagster_hifld.faa_airspace import stage_candidate, ingest_candidate
from dagster_hifld.resources import StagingStorageResource


class FaaCandidateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=pathlib.Path(self.tmp.name);self.capture=self.root/'capture';self.capture.mkdir()
        self.storage=StagingStorageResource(local_dir=str(self.root/'staging'))
        fields=[{'name':k,'type':v,'nullable':False} for k,v in [('OBJECTID','esriFieldTypeOID'),('GLOBAL_ID','esriFieldTypeString'),('GlobalID','esriFieldTypeGlobalID'),('UPPER_VAL','esriFieldTypeDouble')]]
        attrs={'OBJECTID':7,'GLOBAL_ID':'001234567890123456789','GlobalID':'{a}','UPPER_VAL':1.25}
        polygon={'type':'Polygon','coordinates':[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}
        self.data={'layer':{'fields':fields},'item':{'owner':'AeronauticalInformationServices_FAA','description':'Effective test release'},'count':{'count':1},'ids':{'objectIds':[7]},'source-native':{'objectIdFieldName':'OBJECTID','geometryType':'esriGeometryPolygon','fields':fields,'features':[{'attributes':attrs,'geometry':{'rings':polygon['coordinates']}}], 'spatialReference':{'wkid':4269}},'source-geojson':{'features':[{'properties':attrs,'geometry':polygon,'id':7}]},'page-end':{'features':[]}}
    def write(self):
        for k,v in self.data.items():
            body=json.dumps(v).encode();(self.capture/(k+'.body')).write_bytes(body)
            (self.capture/(k+'.json')).write_text(json.dumps({'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'status':200}))
    def run_candidate(self):
        self.write();return stage_candidate(self.storage,self.capture)
    def test_all_identifiers_and_physical_types_preserved_idempotent_quarantine(self):
        import pyarrow.parquet as pq
        result=self.run_candidate();again=stage_candidate(self.storage,self.capture)
        self.assertEqual(result,again);self.assertFalse(result['promotion_allowed'])
        p=self.root/'staging'/result['quarantine_prefix']/'candidate/airspace.parquet';t=pq.read_table(p)
        self.assertEqual(t['GLOBAL_ID'].to_pylist(),['001234567890123456789']);self.assertEqual(str(t.schema.field('UPPER_VAL').type),'double');self.assertFalse(t.schema.field('GLOBAL_ID').nullable)
        self.assertEqual(t['__source_geojson_feature_id'].to_pylist(),[7])
        self.assertEqual((p.parent.parent/'raw/source-native.json').read_bytes(),(self.capture/'source-native.body').read_bytes())
        self.assertFalse(list((self.root/'staging').glob('**/v*/geojson/*')))
    def test_missing_identifier_rejected(self):
        for f in self.data['source-native']['features']:f['attributes'].pop('GLOBAL_ID')
        with self.assertRaisesRegex(ValueError,'schema mismatch'):self.run_candidate()
    def test_incomplete_ids_rejected(self):
        self.data['ids']['objectIds']=[8]
        with self.assertRaisesRegex(ValueError,'ID set mismatch'):self.run_candidate()
    def test_transfer_limit_rejected(self):
        self.data['source-native']['exceededTransferLimit']=True
        with self.assertRaisesRegex(ValueError,'pagination'):self.run_candidate()
    def test_integer_fraction_not_truncated(self):
        self.data['source-native']['features'][0]['attributes']['OBJECTID']=7.5
        self.data['source-geojson']['features'][0]['properties']['OBJECTID']=7.5
        self.data['ids']['objectIds']=[7.5]
        with self.assertRaisesRegex(ValueError,'exact int64'):self.run_candidate()
    def test_new_field_retained_only_in_quarantine(self):
        self.data['layer']['fields'].append({'name':'EXTRA_ID','type':'esriFieldTypeString','nullable':False})
        self.data['source-native']['features'][0]['attributes']['EXTRA_ID']='00001'
        result=self.run_candidate();m=json.loads((self.root/'staging'/result['quarantine_prefix']/'metadata/provenance.json').read_text())
        self.assertIn('EXTRA_ID',m['candidate_columns']);self.assertFalse(m['promotion_allowed'])
    def test_checksum_change_rejected(self):
        self.write();(self.capture/'ids.body').write_text('{}')
        with self.assertRaisesRegex(ValueError,'checksum'):stage_candidate(self.storage,self.capture)
    def test_remote_storage_forbidden(self):
        with self.assertRaisesRegex(ValueError,'isolated local'):stage_candidate(StagingStorageResource(use_local=False,bucket='not-used'),self.capture)

    def test_invalid_geometry_preserved_unrepaired_and_flagged(self):
        self.data['source-geojson']['features'][0]['geometry']['coordinates']=[[[0,0],[1,1],[1,0],[0,1],[0,0]]]
        result=self.run_candidate();m=json.loads((self.root/'staging'/result['quarantine_prefix']/'metadata/provenance.json').read_text())
        self.assertEqual(m['invalid_geometry_count'],0);self.assertEqual(m['diagnostic_server_geojson_invalid_geometry_count'],1);self.assertFalse(m['promotion_allowed'])

    def test_geojson_feature_ids_reject_lossy_or_null_values(self):
        for value in (7.5, True, None, '7', 2**63, -(2**63)-1):
            with self.subTest(value=value):
                self.data['source-geojson']['features'][0]['id']=value
                with self.assertRaisesRegex(ValueError,'GeoJSON Feature.id'):
                    self.run_candidate()

    def test_duplicate_geojson_feature_ids_rejected(self):
        native=copy.deepcopy(self.data['source-native']['features'][0])
        native['attributes'].update(OBJECTID=8,GLOBAL_ID='00008',GlobalID='{b}')
        self.data['source-native']['features'].append(native)
        feature=copy.deepcopy(self.data['source-geojson']['features'][0])
        feature['properties']=native['attributes'];feature['id']=7
        self.data['source-geojson']['features'].append(feature)
        self.data['ids']['objectIds']=[7,8];self.data['count']['count']=2
        with self.assertRaisesRegex(ValueError,'duplicate GeoJSON Feature.id'):
            self.run_candidate()

    def test_rejected_drift_retains_raw_and_previous_candidate(self):
        result=self.run_candidate()
        previous=self.storage.read_key(result['quarantine_prefix']+'/candidate/airspace.parquet')
        self.data['source-native']['features'][0]['attributes'].pop('GLOBAL_ID')
        with self.assertRaisesRegex(ValueError,'schema mismatch'):
            self.run_candidate()
        self.assertEqual(previous,self.storage.read_key(result['quarantine_prefix']+'/candidate/airspace.parquet'))
        rejected=(self.capture/'source-native.body').read_bytes()
        archives=list((self.root/'staging').glob('**/capture-*/quarantine/raw/source-native.body'))
        self.assertTrue(any(p.read_bytes()==rejected for p in archives))
        failures=list((self.root/'staging').glob('**/metadata/failures/*.json'))
        self.assertTrue(any('schema mismatch' in json.loads(p.read_text())['reason'] for p in failures))

    def test_default_live_http_error_body_survives_temporary_cleanup(self):
        error_body=b'{"error":{"code":403,"message":"provider denied"}}'
        error=urllib.error.HTTPError('https://www.arcgis.com/test',403,'Forbidden',{},io.BytesIO(error_body))
        with mock.patch('dagster_hifld.faa_airspace.urllib.request.urlopen',side_effect=error):
            with self.assertRaisesRegex(ValueError,'upstream failure'):
                ingest_candidate(self.storage)
        bodies=list((self.root/'staging').glob('**/capture-*/quarantine/raw/item.body'))
        self.assertEqual([p.read_bytes() for p in bodies],[error_body])
        receipt=json.loads(next((self.root/'staging').glob('**/capture-*/quarantine/raw/item.json')).read_text())
        self.assertEqual(receipt['status'],403)
        self.assertTrue(list((self.root/'staging').glob('**/metadata/failures/*.json')))

    def test_default_live_transport_failure_receipt_survives(self):
        with mock.patch('dagster_hifld.faa_airspace.urllib.request.urlopen',side_effect=urllib.error.URLError('network interrupted')):
            with self.assertRaises(urllib.error.URLError):
                ingest_candidate(self.storage)
        receipt=json.loads(next((self.root/'staging').glob('**/capture-*/quarantine/raw/item.json')).read_text())
        self.assertEqual(receipt['error_type'],'URLError')
        self.assertTrue(list((self.root/'staging').glob('**/metadata/failures/*.json')))

    def test_unsupported_physical_type_evidence_survives(self):
        self.data['layer']['fields'][-1]['type']='esriFieldTypeBlob'
        with self.assertRaisesRegex(ValueError,'unsupported source physical type'):
            self.run_candidate()
        self.assertTrue(list((self.root/'staging').glob('**/quarantine/raw/layer.body')))
        self.assertTrue(list((self.root/'staging').glob('**/metadata/failures/*.json')))

    def test_native_geometry_validity_not_broken_export_controls_candidate(self):
        self.data['source-native']['features'][0]['geometry']['rings']=[[[0,0],[1,1],[1,0],[0,1],[0,0]]]
        with self.assertRaisesRegex(ValueError,'invalid native geometry'):
            self.run_candidate()
        self.assertTrue(list((self.root/'staging').glob('**/metadata/failures/*.json')))


class FaaDraftScheduleTests(unittest.TestCase):
    def test_schedule_stopped_and_local_guarded(self):
        import datetime
        from dagster import build_schedule_context, DefaultScheduleStatus
        from dagster_hifld.assets.faa import faa_airspace_draft_refresh_schedule
        self.assertEqual(faa_airspace_draft_refresh_schedule.default_status,DefaultScheduleStatus.STOPPED)
        self.assertEqual(faa_airspace_draft_refresh_schedule.execution_timezone,'America/New_York')
        clock=datetime.datetime(2026,10,19,6,tzinfo=datetime.timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            context=build_schedule_context(resources={'staging_storage':StagingStorageResource(local_dir=tmp)},scheduled_execution_time=clock)
            result=faa_airspace_draft_refresh_schedule.evaluate_tick(context)
            self.assertEqual(len(result.run_requests),1)
            self.assertEqual(result.run_requests[0].run_key,'faa-airspace-draft-2026-10-19')
        context=build_schedule_context(resources={'staging_storage':StagingStorageResource(use_local=False,bucket='no-network-call')},scheduled_execution_time=clock)
        result=faa_airspace_draft_refresh_schedule.evaluate_tick(context)
        self.assertFalse(result.run_requests)
        self.assertIn('local quarantine',result.skip_message)


class FaaPartialCaptureTests(unittest.TestCase):
    def test_partial_live_body_and_error_survive_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp)
            response=mock.MagicMock()
            response.status=200
            response.__enter__.return_value=response
            response.read.side_effect=[b'{"partial":',TimeoutError('interrupted body')]
            with mock.patch('dagster_hifld.faa_airspace.urllib.request.urlopen',return_value=response):
                with self.assertRaisesRegex(ValueError,'incomplete bounded response'):
                    ingest_candidate(storage)
            body=next(pathlib.Path(tmp).glob('**/quarantine/raw/item.body'))
            receipt=json.loads(next(pathlib.Path(tmp).glob('**/quarantine/raw/item.json')).read_text())
            self.assertEqual(body.read_bytes(),b'{"partial":')
            self.assertIn('interrupted body',receipt['read_error'])
            self.assertTrue(list(pathlib.Path(tmp).glob('**/metadata/failures/*.json')))
