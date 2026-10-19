import copy
import hashlib
import io
import urllib.error
from unittest import mock
import json
import pathlib
import tempfile
import unittest

from dagster_hifld.faa_airspace import stage_candidate, ingest_candidate, retain_capture
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class FaaCandidateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=pathlib.Path(self.tmp.name);self.capture=self.root/'capture';self.capture.mkdir()
        self.storage=StagingStorageResource(local_dir=str(self.root/'staging'))
        fields=[{'name':k,'type':v,'nullable':False} for k,v in [('OBJECTID','esriFieldTypeOID'),('GLOBAL_ID','esriFieldTypeString'),('GlobalID','esriFieldTypeGlobalID'),('UPPER_VAL','esriFieldTypeDouble')]]
        attrs={'OBJECTID':7,'GLOBAL_ID':'001234567890123456789','GlobalID':'{a}','UPPER_VAL':1.25}
        polygon={'type':'Polygon','coordinates':[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}
        self.data={'layer':{'id':0,'serviceItemId':'67885972e4e940b2aa6d74024901c561','fields':fields},'item':{'id':'67885972e4e940b2aa6d74024901c561','orgId':'ssFJjBXIUyZDrSYZ','url':'https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/Boundary_Airspace/FeatureServer','owner':'AeronauticalInformationServices_FAA','description':'Effective test release'},'count':{'count':1},'ids':{'objectIds':[7]},'source-native':{'objectIdFieldName':'OBJECTID','geometryType':'esriGeometryPolygon','fields':fields,'features':[{'attributes':attrs,'geometry':{'rings':polygon['coordinates']}}], 'spatialReference':{'wkid':4269}},'source-geojson':{'features':[{'properties':attrs,'geometry':polygon,'id':7}]},'page-end':{'features':[]}}
    def write(self):
        from dagster_hifld.faa_airspace import capture_urls
        urls=capture_urls(self.data["count"]["count"])
        for k,v in self.data.items():
            body=json.dumps(v).encode();(self.capture/(k+'.body')).write_bytes(body)
            (self.capture/(k+'.json')).write_text(json.dumps({'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'status':200,'url':urls[k],'final_url':urls[k],'checked_at':'2026-10-19T00:00:00+00:00'}))
    def run_candidate(self):
        self.write();return stage_candidate(self.storage,self.capture)
    def test_all_identifiers_and_physical_types_preserved_idempotent_quarantine(self):
        import pyarrow.parquet as pq
        result=self.run_candidate();again=stage_candidate(self.storage,self.capture)
        self.assertEqual(result,again);self.assertFalse(result['promotion_allowed'])
        p=self.root/'staging'/result['staging_prefix']/'candidate/airspace.parquet';t=pq.read_table(p)
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
        result=self.run_candidate();m=json.loads((self.root/'staging'/result['staging_prefix']/'metadata/provenance.json').read_text())
        self.assertIn('EXTRA_ID',m['candidate_columns']);self.assertFalse(m['promotion_allowed'])
    def test_checksum_change_rejected(self):
        self.write();(self.capture/'ids.body').write_text('{}')
        with self.assertRaisesRegex(ValueError,'checksum'):stage_candidate(self.storage,self.capture)
    def test_remote_storage_forbidden(self):
        with self.assertRaisesRegex(ValueError,'isolated local'):stage_candidate(StagingStorageResource(use_local=False,bucket='not-used'),self.capture)

    def test_invalid_geometry_preserved_unrepaired_and_flagged(self):
        self.data['source-geojson']['features'][0]['geometry']['coordinates']=[[[0,0],[1,1],[1,0],[0,1],[0,0]]]
        result=self.run_candidate();m=json.loads((self.root/'staging'/result['staging_prefix']/'metadata/provenance.json').read_text())
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
        previous=self.storage.read_key(result['staging_prefix']+'/candidate/airspace.parquet')
        self.data['source-native']['features'][0]['attributes'].pop('GLOBAL_ID')
        with self.assertRaisesRegex(ValueError,'schema mismatch'):
            self.run_candidate()
        self.assertEqual(previous,self.storage.read_key(result['staging_prefix']+'/candidate/airspace.parquet'))
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


class FaaBoundaryRegressionTests(unittest.TestCase):
    def test_published_resource_rejected_before_capture_or_any_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);prior=root/'accepted.json';prior.write_bytes(b'accepted-byte-exact')
            published=PublishedStorageResource(local_dir=tmp)
            before={str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()}
            with mock.patch('dagster_hifld.faa_airspace.capture') as capture_call:
                for action in (lambda:ingest_candidate(published), lambda:stage_candidate(published,root/'missing'),lambda:retain_capture(published,root/'missing')):
                    with self.assertRaisesRegex(ValueError,'isolated local'):
                        action()
                capture_call.assert_not_called()
            self.assertEqual(before,{str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()})

    def test_normal_asset_and_schedule_reject_published_resource(self):
        import datetime
        from dagster import materialize, build_schedule_context
        from dagster_hifld.assets.faa import faa_airspace_boundaries, faa_airspace_draft_refresh_schedule
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);(root/'prior').write_bytes(b'preserve')
            published=PublishedStorageResource(local_dir=tmp)
            before={str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()}
            with mock.patch('dagster_hifld.faa_airspace.capture') as capture_call:
                result=materialize([faa_airspace_boundaries],resources={'staging_storage':published},raise_on_error=False)
                self.assertFalse(result.success)
                context=build_schedule_context(resources={'staging_storage':published},scheduled_execution_time=datetime.datetime(2026,10,19,tzinfo=datetime.timezone.utc))
                tick=faa_airspace_draft_refresh_schedule.evaluate_tick(context)
                self.assertFalse(tick.run_requests);self.assertIn('local quarantine',tick.skip_message)
                capture_call.assert_not_called()
            self.assertEqual(before,{str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()})


class FaaAmbiguousJsonTests(unittest.TestCase):
    setUp = FaaCandidateTests.setUp
    write = FaaCandidateTests.write
    def test_duplicate_json_members_rejected_and_retained(self):
        self.write()
        for name,old,new in [('source-geojson','"id": 7','"id": 6, "id": 7'),('source-native','"OBJECTID": 7','"OBJECTID": 6, "OBJECTID": 7'),('item','"owner":','"owner": "earlier", "owner":'),('layer','"name": "OBJECTID"','"name": "earlier", "name": "OBJECTID"')]:
            with self.subTest(name=name):
                self.write()
                path=self.capture/(name+'.body');body=path.read_bytes();self.assertIn(old.encode(),body)
                body=body.replace(old.encode(),new.encode(),1);path.write_bytes(body)
                receipt=self.capture/(name+'.json');m=json.loads(receipt.read_text());m.update(bytes=len(body),sha256=hashlib.sha256(body).hexdigest());receipt.write_text(json.dumps(m))
                with self.assertRaisesRegex(ValueError,'duplicate JSON member'):
                    stage_candidate(self.storage,self.capture)
                archives=list((self.root/'staging').glob('**/quarantine/raw/'+name+'.body'))
                self.assertTrue(any(p.read_bytes()==body for p in archives))
                self.assertTrue(list((self.root/'staging').glob('**/metadata/failures/*.json')))


class FaaCanonicalIntegrationTests(unittest.TestCase):
    setUp = FaaCandidateTests.setUp
    write = FaaCandidateTests.write

    def test_ingest_emits_discoverable_semver_and_valid_actual_provenance(self):
        from dagster_hifld.definitions import _iter_staged_version_paths
        from dagster_hifld.release_approval import validate_release_provenance
        from dagster_hifld.partitions import SEMVER_VERSION_RE
        self.write()
        out=ingest_candidate(self.storage,self.capture)
        self.assertRegex(out['version'],SEMVER_VERSION_RE)
        self.assertEqual(list(_iter_staged_version_paths(self.storage)),[(out['dataset_slug'],out['file_slug'],out['version'])])
        prefix=out['staging_prefix']
        manifest=json.loads(self.storage.read_key(prefix+'/metadata/source_manifest.json'))
        validate_release_provenance(manifest)
        self.assertFalse(manifest['source_accepted'])
        self.assertIsNone(manifest['approved_contract'])
        self.assertEqual(len(manifest['inputs']),7)
        for item in manifest['inputs']:
            self.assertEqual(hashlib.sha256(self.storage.read_key(item['raw_key'])).hexdigest(),item['sha256'])
            self.assertEqual(self.storage.read_key(item['receipt_key']),(self.capture/(item['input_id']+'.json')).read_bytes())
        self.assertTrue(self.storage.object_exists(prefix+'/geopackage/airspace.gpkg'))
        with mock.patch('dagster_hifld.faa_airspace.verifier_implementation_sha256',return_value='a'*64):
            changed=ingest_candidate(self.storage,self.capture)
        self.assertNotEqual(out['version'],changed['version'])
        self.assertTrue(self.storage.object_exists(prefix+'/geopackage/airspace.gpkg'))

    def test_unverified_or_wrong_replay_origin_is_durable_quarantine(self):
        from dagster_hifld.faa_airspace import LAYER_URL
        for change in ('missing','mirror','wrong_layer','wrong_scope','requested_mirror'):
            with self.subTest(change=change):
                self.write()
                path=self.capture/'source-native.json';receipt=json.loads(path.read_text())
                if change=='missing':receipt.pop('final_url')
                if change=='mirror':receipt['final_url']=receipt['final_url'].replace('services6.arcgis.com','example.org')
                if change=='wrong_layer':receipt['final_url']=receipt['final_url'].replace('/0/query','/1/query')
                if change=='wrong_scope':receipt['final_url']=receipt['final_url'].replace('where=1%3D1','where=OBJECTID%3D7')
                if change=='requested_mirror':receipt['url']=receipt['url'].replace('services6.arcgis.com','example.org')
                path.write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError,'FAA .*origin|FAA observed final URL'):
                    ingest_candidate(self.storage,self.capture)
                self.assertFalse(list((self.root/'staging').glob('**/geopackage/*')))
                archives=list((self.root/'staging').glob('**/quarantine/raw/source-native.json'))
                self.assertTrue(any(p.read_bytes()==path.read_bytes() for p in archives))
        self.assertEqual(len(list((self.root/'staging').glob('**/metadata/failures/*.json'))),5)

    def test_registered_refresh_feeds_normal_graph_without_bridge(self):
        from dagster import DagsterInstance, Definitions, AssetSelection, define_asset_job
        from dagster_hifld.definitions import defs
        from dagster_hifld.assets import hifld_dataset_assets
        from dagster_hifld.checks import hifld_asset_checks
        from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
        self.write()
        published=PublishedStorageResource(local_dir=str(self.root/'published'))
        published.write_key('previous',b'keep')
        resources={'staging_storage':self.storage,'published_storage':published}
        with DagsterInstance.ephemeral() as instance:
            run=defs.resolve_job_def('faa_airspace_draft_refresh').execute_in_process(instance=instance,resources=resources,run_config={'ops':{'ingest__airspace_boundaries_1__airspace_boundaries_1':{'config':{'capture_dir':str(self.capture)}}}})
            self.assertTrue(run.success)
            out=run.output_for_node('ingest__airspace_boundaries_1__airspace_boundaries_1')
            self.assertIn(out['partition_key'],instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))
            selection=AssetSelection.keys(['publish','catalog'],['publish','formats','geopackage'],['publish','formats','geoparquet'],['publish','formats','pmtiles'],['publish','formats','shapefile_zip'],['publish','verify'],['publish','promote'])
            downstream=Definitions(assets=hifld_dataset_assets,asset_checks=hifld_asset_checks,resources=resources,jobs=[define_asset_job('faa_no_bridge',selection=selection)])
            run=downstream.resolve_job_def('faa_no_bridge').execute_in_process(instance=instance,partition_key=out['partition_key'],raise_on_error=False)
            errors=run.get_step_failure_events()
            self.assertEqual([e.step_key for e in errors],['publish__verify'])
            self.assertIn('missing_independent_release_approval_registry',str(errors[0].event_specific_data.error))
            self.assertEqual(published.list_prefix(),['previous'])
            self.assertEqual(published.read_key('previous'),b'keep')
            quality=json.loads(self.storage.read_key(out['staging_prefix']+'/metadata/quality_manifest.json'))
            self.assertEqual(quality['feature_count'],1)
            self.assertEqual(quality['invalid_geometry_count'],0)
