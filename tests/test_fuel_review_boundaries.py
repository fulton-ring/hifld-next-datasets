"""Independent-review regressions: no published bypass or lost failure evidence."""
import hashlib
import http.client
import io
import json
import pathlib
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

from dagster import DagsterInstance
from dagster_hifld.assets.fuel_stations import (
    SLUG, SERVICE, _capture_json, build_candidate, inspect_capture,
    stage_fuel_capture, bts_alternative_fueling_stations, UNSUPPORTED_OUTPUT_FORMATS,
)
from dagster_hifld.definitions import defs
from dagster_hifld.assets.publish import _write_and_publish_pmtiles
from dagster_hifld.promotion import promote_immutable_objects
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from tests.test_fuel_stations import fixture, write


class Response(io.BytesIO):
    status = 200
    headers = {'Date': 'Mon, 19 Oct 2026 00:00:00 GMT'}


class FuelReviewBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=pathlib.Path(self.temp.name);self.capture=self.root/'capture';self.capture.mkdir()
        self.layer,self.page=fixture(self.capture)
        self.staging=StagingStorageResource(local_dir=str(self.root/'staging'),use_local=True)

    def registered_run(self, resource, config):
        job=defs.get_repository_def().get_job('fuel_stations_draft_refresh_job')
        with DagsterInstance.ephemeral() as instance:
            return job.execute_in_process(instance=instance,resources={'staging_storage':resource},
                run_config={'ops':{bts_alternative_fueling_stations.op.name:{'config':config}}},raise_on_error=False)

    def test_published_resource_rejected_before_helper_or_registered_job_capture_or_write(self):
        published=PublishedStorageResource(local_dir=str(self.root/'published'),use_local=True)
        with self.assertRaisesRegex(ValueError,'published storage'):
            stage_fuel_capture(self.capture,published,'v0.0.0-test')
        with patch('dagster_hifld.assets.fuel_stations.urllib.request.urlopen') as network:
            result=self.registered_run(published,{'capture_dir':'','version':'v0.0.0-test'})
        self.assertFalse(result.success);network.assert_not_called()
        self.assertFalse((self.root/'published').exists())

    def test_all_count_and_id_witness_types_cardinality_and_uniqueness_are_strict(self):
        for name,value in [('bts_count',True),('bts_count',1.0),('bts_count_after',True),('bts_count_after',1.0),
                ('bts_ids',[True]),('bts_ids',[9999999999.0]),('bts_ids_after',[9999999999,9999999999]),
                ('bts_ids_after',[True]),('bts_ids_after',[9999999999.0]),('bts_ids_after',['9999999999'])]:
            fixture(self.capture)
            write(self.capture,name,{'count':value} if 'count' in name else {'objectIds':value})
            with self.subTest(name=name,value=value),self.assertRaises(ValueError):inspect_capture(self.capture)

    def test_duplicate_json_identifier_retained_in_blocked_quarantine(self):
        path=self.capture/'page-000.body';raw=path.read_bytes().replace(b'"id": 7',b'"id": 6, "id": 7')
        path.write_bytes(raw)
        (self.capture/'page-000.receipt.json').write_text(json.dumps({'status':200,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}))
        with self.assertRaisesRegex(ValueError,'Duplicate JSON member'):
            stage_fuel_capture(self.capture,self.staging,'v0.0.0-duplicate')
        saved=list((self.root/'staging/quarantine').rglob('page-000.body'))
        self.assertEqual(len(saved),1);self.assertEqual(saved[0].read_bytes(),raw)
        status=list((self.root/'staging/quarantine').rglob('status.json'))[0]
        self.assertEqual(json.loads(status.read_text())['state'],'blocked')
        self.assertFalse((self.root/'staging'/SLUG).exists())

    def test_schema_rejection_keeps_raw_and_previous_candidate(self):
        old=self.root/'previous';build_candidate(self.capture,old);before=(old/'candidate.parquet').read_bytes()
        with self.assertRaisesRegex(ValueError,'schema drift'):
            stage_fuel_capture(self.capture,self.staging,'v0.0.0-drift')
        self.assertEqual((old/'candidate.parquet').read_bytes(),before)
        self.assertEqual(len(list((self.root/'staging/quarantine').rglob('page-000.body'))),1)
        self.assertFalse((self.root/'staging'/SLUG).exists())

    def test_integer_geometry_precision_loss_is_blocked_and_raw_retained(self):
        self.page['features'][0]['geometry']['x'] = 2**53 + 1
        write(self.capture, 'page-000', self.page)
        with self.assertRaisesRegex(ValueError, 'geometry would lose source numeric precision'):
            stage_fuel_capture(self.capture, self.staging, 'v0.0.0-geometry-precision')
        raw = next((self.root/'staging/quarantine').rglob('page-000.body'))
        self.assertIn(str(2**53 + 1).encode(), raw.read_bytes())
        self.assertFalse((self.root/'staging'/SLUG).exists())

    def test_every_data_receipt_requires_official_request_and_observed_final_scope(self):
        names = ['bts_layer','bts_layer_after','bts_count','bts_count_after','bts_ids','bts_ids_after','page-000']
        previous = self.root/'previous'
        build_candidate(self.capture, previous)
        saved = (previous/'candidate.parquet').read_bytes()
        for name in names:
            for key in ('url', 'final_url'):
                fixture(self.capture)
                path = self.capture/(name+'.receipt.json')
                receipt = json.loads(path.read_text())
                receipt[key] = receipt[key].replace('services.arcgis.com', 'mirror.example.com')
                path.write_text(json.dumps(receipt))
                with self.subTest(name=name,key=key),self.assertRaisesRegex(ValueError,'origin/scope'):
                    stage_fuel_capture(self.capture,self.staging,'v0.0.0-origin')
                self.assertTrue(any(p.read_bytes()==path.read_bytes() for p in (self.root/'staging/quarantine').rglob(path.name)))
        for change in ('missing', 'layer', 'extra_projection', 'duplicate_filter'):
            fixture(self.capture)
            path = self.capture/'page-000.receipt.json'
            receipt = json.loads(path.read_text())
            if change == 'missing': del receipt['final_url']
            elif change == 'layer': receipt['final_url'] = receipt['final_url'].replace('FeatureServer/0','FeatureServer/1')
            elif change == 'extra_projection': receipt['final_url'] += '&outSR=4326'
            else: receipt['final_url'] += '&where=1%3D1'
            path.write_text(json.dumps(receipt))
            with self.subTest(change=change),self.assertRaises(ValueError):
                stage_fuel_capture(self.capture,self.staging,'v0.0.0-origin')
        self.assertEqual((previous/'candidate.parquet').read_bytes(),saved)
        self.assertFalse((self.root/'staging'/SLUG).exists())

    def test_pmtiles_policy_skips_before_generation_or_existing_reuse_and_gate_preserves(self):
        version='v0.0.0-policy'
        prefix=f'{SLUG}/{SLUG}/{version}'
        self.staging.write(SLUG,SLUG,version,'metadata/source_manifest.json',json.dumps({
            'source_url':SERVICE,'inputs':[{'input_id':SERVICE}],
            'unsupported_output_formats':UNSUPPORTED_OUTPUT_FORMATS}).encode())
        diagnostic=self.staging.write(SLUG,SLUG,version,'pmtiles/diagnostic.pmtiles',b'diagnostic-only')
        published=PublishedStorageResource(local_dir=str(self.root/'published'),use_local=True)
        old=published.write_key('previous/keep.bin',b'preserve prior release')
        before=published.sha256_key(old)
        with patch('dagster_hifld.assets.publish.process_layer_chunked') as convert,patch('dagster_hifld.assets.publish._prepare_format_publish') as reuse:
            outputs=_write_and_publish_pmtiles(self.staging,SLUG,SLUG,version)
        convert.assert_not_called();reuse.assert_not_called()
        self.assertEqual(len(outputs),1)
        self.assertEqual(outputs[0].path, '')
        self.assertIn('unsupported_output_format:',outputs[0].source_metadata['skip_reason'])
        with patch.dict('os.environ',{},clear=True):
            with self.assertRaisesRegex(ReleaseApprovalError,'missing_independent_release_approval_registry'):
                promote_immutable_objects(self.staging,published,[(diagnostic,diagnostic)],version_prefix=prefix)
        self.assertEqual(published.sha256_key(old),before)
        self.assertFalse(published.object_exists(diagnostic))
        self.assertEqual(self.staging.read_key(diagnostic),b'diagnostic-only')

    def test_bounded_retry_keeps_transport_and_http_error_bytes_before_success(self):
        target=self.root/'download';target.mkdir()
        error=urllib.error.HTTPError(SERVICE,503,'busy',{},io.BytesIO(b'{"error":{"code":503}}'))
        with patch('dagster_hifld.assets.fuel_stations.urllib.request.urlopen',side_effect=[urllib.error.URLError('reset'),error,Response(b'{"count":1}')]) as network:
            self.assertEqual(_capture_json(target,'count',SERVICE),{'count':1})
        self.assertEqual(network.call_count,3)
        self.assertEqual((target/'attempts/count/000.body').read_bytes(),b'')
        self.assertEqual((target/'attempts/count/001.body').read_bytes(),b'{"error":{"code":503}}')
        self.assertEqual((target/'count.body').read_bytes(),b'{"count":1}')

    def test_partial_body_retained_for_each_failed_attempt(self):
        class Partial(Response):
            def read(self, size=-1):raise http.client.IncompleteRead(b'{"incomplete":',100)
        target=self.root/'partial';target.mkdir()
        with patch('dagster_hifld.assets.fuel_stations.urllib.request.urlopen',side_effect=lambda *a,**k:Partial()):
            with self.assertRaisesRegex(ValueError,'3 bounded attempts'):_capture_json(target,'page',SERVICE)
        bodies=list((target/'attempts/page').glob('*.body'));self.assertEqual(len(bodies),3)
        self.assertTrue(all(p.read_bytes()==b'{"incomplete":' for p in bodies))
        self.assertFalse(json.loads((target/'attempts/page/000.receipt.json').read_text())['complete_body'])

    def test_valid_json_with_short_declared_http_body_is_not_complete(self):
        target=self.root/'short';target.mkdir()
        class Short(Response):
            headers={'Content-Length':'999'}
        with patch('dagster_hifld.assets.fuel_stations.urllib.request.urlopen',side_effect=lambda *a,**k:Short(b'{"count":1}')):
            with self.assertRaisesRegex(ValueError,'declared length'):_capture_json(target,'count',SERVICE)
        self.assertFalse((target/'count.body').exists())
        self.assertEqual(len(list((target/'attempts/count').glob('*.body'))),3)

    def test_live_registered_failure_keeps_prior_responses_and_failed_later_page(self):
        original=build_candidate(self.capture,self.root/'previous');before=(self.root/'previous/candidate.parquet').read_bytes()
        def network(url, **kwargs):
            if 'returnCountOnly' in url:return Response(b'{"count":1}')
            if 'returnIdsOnly' in url:return Response(b'{"objectIds":[9999999999]}')
            if '/query?' not in url:return Response(json.dumps(self.layer).encode())
            return Response(b'{"error":{"code":503,"message":"busy"}}')
        with patch('dagster_hifld.assets.fuel_stations.urllib.request.urlopen',side_effect=network) as fetch:
            result=self.registered_run(self.staging,{'capture_dir':'','version':'v0.0.0-live-failure'})
        self.assertFalse(result.success);self.assertEqual(fetch.call_count,6)
        quarantine=self.root/'staging/quarantine'
        self.assertEqual(len(list(quarantine.rglob('bts_layer.body'))),1)
        self.assertEqual(len(list(quarantine.rglob('page-000/*.body'))),3)
        self.assertTrue(any(json.loads(p.read_text())['state']=='blocked' for p in quarantine.rglob('status.json')))
        self.assertEqual((self.root/'previous/candidate.parquet').read_bytes(),before)
        self.assertFalse((self.root/'staging'/SLUG).exists())

    def test_processing_fingerprint_change_rejects_reuse_and_keeps_original_provenance(self):
        destination=self.root/'candidate';first=build_candidate(self.capture,destination)
        before=(destination/'provenance.json').read_bytes()
        self.assertEqual(len(first['processing']['installed_pipeline_sha256']),64)
        self.assertTrue(first['processing']['code_commit'])
        changed={**first['processing'],'builder_fingerprint':'0'*64}
        with patch('dagster_hifld.assets.fuel_stations.processing_identity',return_value=changed):
            with self.assertRaises(FileExistsError):build_candidate(self.capture,destination)
        self.assertEqual((destination/'provenance.json').read_bytes(),before)
        self.assertEqual(build_candidate(self.capture,destination),first)

    def test_failed_candidate_binds_processing_and_preserves_partial_on_changed_builder(self):
        self.page['features'][0]['attributes']['latitude'] = 2**53 + 1
        write(self.capture, 'page-000', self.page)
        destination = self.root/'failed-candidate'
        with self.assertRaises(Exception): build_candidate(self.capture, destination)
        marker = json.loads((destination/'processing-request.json').read_text())
        partial = destination/'candidate.parquet.partial'
        before = partial.read_bytes()
        changed = {**marker['processing'], 'builder_fingerprint': '0'*64}
        with patch('dagster_hifld.assets.fuel_stations.processing_identity', return_value=changed):
            with self.assertRaisesRegex(FileExistsError, 'processing request changed'):
                build_candidate(self.capture, destination)
        self.assertEqual(partial.read_bytes(), before)


if __name__=='__main__':unittest.main()
