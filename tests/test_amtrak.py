"""Draft BTS adapter failures must quarantine before any promotion is possible."""
import copy
import hashlib
import json
import pathlib
import tempfile
import sqlite3
import unittest
import datetime
import http.client
import io
import os
import urllib.parse
import zipfile
from unittest.mock import patch

from dagster import DefaultScheduleStatus, Definitions, DagsterInstance, build_schedule_context, materialize
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.amtrak_extract import build_candidate, inspect_capture, stage_amtrak_capture, bts_amtrak_stations, verify_typed_geopackage, SLUG
from dagster_hifld.amtrak_extract import amtrak_draft_refresh_job, amtrak_draft_refresh_schedule
from dagster_hifld.release_approval import validate_release_provenance


def write(root, name, value):
    body = json.dumps(value, sort_keys=True).encode()
    (root / (name + '.body')).write_bytes(body)
    from dagster_hifld.amtrak_extract import SERVICE, ITEM_URL
    if name.startswith('bts_item'): url=ITEM_URL
    elif name.startswith('bts_layer'): url=SERVICE+'?f=pjson'
    else:
        query={'f':'json','where':'1=1'}
        if name.startswith('bts_count'): query['returnCountOnly']='true'
        elif name.startswith('bts_ids'): query['returnIdsOnly']='true'
        else: query={'f':'json','where':'OBJECTID >= 9999999999 AND OBJECTID <= 9999999999','outFields':'*','returnGeometry':'true','orderByFields':'OBJECTID ASC','resultRecordCount':2000}
        url=SERVICE+'/query?'+urllib.parse.urlencode(query)
    (root / (name + '.receipt.json')).write_text(json.dumps({'status': 200, 'complete_body': True, 'url':url,'final_url':url,'observed_at':'2026-10-19T00:00:00+00:00','bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}))


def fixture(root):
    fields = [{'name': n, 'type': t, 'nullable': nullable} for n, t, nullable in [
        ('OBJECTID', 'esriFieldTypeOID', False), ('Code', 'esriFieldTypeString', True),
        ('zip', 'esriFieldTypeString', True), ('updated_at', 'esriFieldTypeDate', True),
        ('latitude', 'esriFieldTypeDouble', True)]]
    sr = {'wkid': 4326, 'latestWkid': 4326}
    layer = {'fields': fields, 'editingInfo': {'lastEditDate': 1, 'dataLastEditDate': 1}, 'serviceItemId': '1ed62a9f46304679aaa396bed4c8565a', 'geometryType': 'esriGeometryPoint', 'objectIdField': 'OBJECTID', 'hasZ': False, 'hasM': False, 'extent': {'spatialReference': sr}}
    page = {'fields': fields, 'spatialReference': sr, 'features': [{'attributes': {'OBJECTID': 9999999999, 'Code': '007', 'zip': '00123', 'updated_at': 1792320841494, 'latitude': 38.5}, 'geometry': {'x': -100.123456789, 'y': 40.25}}]}
    item={'id':'1ed62a9f46304679aaa396bed4c8565a','owner':'USDOT_BTS','orgId':'xOi1kZaI0eWDREZv','url':'https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Amtrak_Stations/FeatureServer','access':'public','description':'synthetic fixture, not acceptance',
          'licenseInfo':'Explicit synthetic test license; no real dataset acceptance',
          'accessInformation':'Synthetic Amtrak producer/BTS distributor attribution for behavior test only'}
    for suffix in ('', '_after'):
        write(root, 'bts_item' + suffix, item)
        write(root, 'bts_layer' + suffix, layer)
        write(root, 'bts_count' + suffix, {'count': 1})
        write(root, 'bts_ids' + suffix, {'objectIds': [9999999999]})
    write(root, 'page-000', page)
    return layer, page


class AmtrakDraftTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.layer, self.page = fixture(self.root)

    def test_lossless_types_ids_geometry_and_idempotency(self):
        audit = build_candidate(self.root, self.root / 'candidate')
        table = pq.read_table(self.root / 'candidate/candidate.parquet')
        row = table.to_pylist()[0]
        self.assertEqual(row['OBJECTID'], 9999999999)
        self.assertEqual(row['Code'], '007')
        self.assertEqual(row['zip'], '00123')
        self.assertEqual(row['updated_at'], 1792320841494)
        self.assertEqual(str(table.schema.field('Code').type), 'string')
        self.assertEqual(str(table.schema.field('OBJECTID').type), 'int64')
        self.assertEqual(tuple(from_wkb(row['geometry']).coords)[0], (-100.123456789, 40.25))
        self.assertFalse(audit['promotion_allowed'])
        self.assertEqual(build_candidate(self.root, self.root / 'candidate'), audit)

    def test_rejects_incomplete_and_changed_census(self):
        for doc, value in [('bts_count_after', {'count': 2}), ('bts_ids_after', {'objectIds': [2]})]:
            fixture(self.root); write(self.root, doc, value)
            with self.subTest(doc=doc), self.assertRaises(ValueError): inspect_capture(self.root)

    def test_rejects_missing_renamed_extra_and_mistyped_ids(self):
        for change in ('missing', 'renamed', 'extra', 'float', 'null', 'overflow'):
            _, page = fixture(self.root); attrs = page['features'][0]['attributes']
            if change == 'missing': del attrs['Code']
            if change == 'renamed': attrs['station_id'] = attrs.pop('Code')
            if change == 'extra': attrs['unknown_id'] = 8
            if change == 'float': attrs['Code'] = 7.0
            if change == 'null': attrs['Code'] = None
            if change == 'overflow': attrs['Code'] = 2**32
            write(self.root, 'page-000', page)
            with self.subTest(change=change), self.assertRaises(ValueError): inspect_capture(self.root)

    def test_rejects_duplicate_ids_truncation_crs_and_geometry(self):
        for change in ('duplicate', 'truncated', 'crs', 'null_geometry', 'extra_dimension', 'error'):
            _, page = fixture(self.root)
            if change == 'duplicate': page['features'].append(copy.deepcopy(page['features'][0]))
            if change == 'truncated': page['exceededTransferLimit'] = True
            if change == 'crs': page['spatialReference'] = {'wkid': 4326}
            if change == 'null_geometry': page['features'][0]['geometry'] = None
            if change == 'extra_dimension': page['features'][0]['geometry']['z'] = 1
            if change == 'error': page = {'error': {'code': 400}}
            write(self.root, 'page-000', page)
            with self.subTest(change=change), self.assertRaises(ValueError): inspect_capture(self.root)

    def test_rejects_source_edit_or_schema_change(self):
        for kind in ('edit', 'schema'):
            layer, _ = fixture(self.root)
            if kind == 'edit': layer['editingInfo']['lastEditDate'] = 2
            else: layer['fields'][1]['type'] = 'esriFieldTypeInteger'
            write(self.root, 'bts_layer_after', layer)
            with self.subTest(kind=kind), self.assertRaises(ValueError): inspect_capture(self.root)

    def test_standard_asset_stages_raw_candidate_and_provenance(self):
        signature = self.layer['fields']
        digest = hashlib.sha256(json.dumps(signature,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        storage = StagingStorageResource(local_dir=str(self.root/'staging'), use_local=True)
        with patch('dagster_hifld.amtrak_extract.PROPOSED_SCHEMA_SHA256', digest), DagsterInstance.ephemeral() as instance:
            result=materialize([bts_amtrak_stations],instance=instance,resources={'staging_storage':storage},run_config={'ops':{bts_amtrak_stations.op.name:{'config':{'capture_dir':str(self.root),'version':'v0.0.0-synthetic'}}}})
        self.assertTrue(result.success)
        root=self.root/'staging'/SLUG/SLUG/'v0.0.0-synthetic'
        self.assertEqual((root/'raw_arcgis/page-000.body').read_bytes(),(self.root/'page-000.body').read_bytes())
        source=json.loads((root/'metadata/source_manifest.json').read_bytes())
        validate_release_provenance(source)
        self.assertEqual(source['schema_version'], 1)
        self.assertEqual(source['processor'], 'HIFLD Next')
        self.assertEqual(len(source['inputs']),9)
        self.assertTrue(source['inputs'][0]['input_id'])
        self.assertFalse(source['promotion_allowed'])
        self.assertEqual(pq.read_table(root/'draft_candidate/candidate.parquet').num_rows,1)
        bridge = root / 'geopackage' / (SLUG + '.gpkg')
        self.assertEqual(verify_typed_geopackage(root/'draft_candidate/candidate.parquet', bridge)['rows'], 1)
        with sqlite3.connect(bridge) as connection:
            # Only this synthetic tamper fixture bypasses spatial maintenance.
            for (trigger,) in connection.execute('SELECT name FROM sqlite_master WHERE type="trigger"').fetchall():
                connection.execute(f'DROP TRIGGER "{trigger}"')
            connection.execute(f'UPDATE "{SLUG}" SET zip=?', ('BAD',))
        with self.assertRaisesRegex(ValueError, 'value/geometry changed'):
            verify_typed_geopackage(root/'draft_candidate/candidate.parquet', bridge)
        geojson=json.loads((root/'geojson'/(SLUG+'.geojson')).read_bytes())
        self.assertEqual(geojson['features'][0]['properties']['zip'],'00123')
        self.assertEqual(geojson['features'][0]['properties']['OBJECTID'],9999999999)

    def test_unreviewed_schema_drift_and_remote_staging_are_rejected(self):
        storage=StagingStorageResource(local_dir=str(self.root/'staging'),use_local=True)
        with self.assertRaisesRegex(ValueError,'schema drift'): stage_amtrak_capture(self.root,storage,'v0.0.0-test')
        self.assertTrue((self.root/'staging/quarantine').exists())
        self.assertFalse((self.root/'staging'/SLUG).exists())
        remote=StagingStorageResource(bucket='not-used',use_local=False)
        with self.assertRaisesRegex(ValueError,'local staging'): stage_amtrak_capture(self.root,remote,'v0.0.0-test')

    def test_refresh_schedule_is_stopped_and_selects_only_source_with_dated_version(self):
        definitions = Definitions(assets=[bts_amtrak_stations],
            jobs=[amtrak_draft_refresh_job], schedules=[amtrak_draft_refresh_schedule],
            resources={'staging_storage': StagingStorageResource(local_dir=str(self.root/'staging'), use_local=True)})
        job = definitions.resolve_job_def('amtrak_draft_refresh_job')
        self.assertEqual(set(job.graph.node_names()), {bts_amtrak_stations.op.name})
        self.assertEqual(amtrak_draft_refresh_schedule.default_status, DefaultScheduleStatus.STOPPED)
        instant = datetime.datetime(2026, 10, 19, 13, tzinfo=datetime.timezone.utc)
        with build_schedule_context(scheduled_execution_time=instant, resources={'staging_storage':StagingStorageResource(local_dir=str(self.root/'schedule'),use_local=True)}) as context:
            requests = amtrak_draft_refresh_schedule.evaluate_tick(context).run_requests
        self.assertEqual(len(requests), 1)
        config = requests[0].run_config['ops'][bts_amtrak_stations.op.name]['config']
        self.assertEqual(config, {'capture_dir': '', 'version': 'v0.0.0-research-20261019T130000Z'})
        self.assertEqual(requests[0].tags['hifld_promotion_allowed'], 'false')

    def test_source_asset_rejects_cloud_before_any_live_capture(self):
        remote = StagingStorageResource(bucket='not-used', use_local=False)
        with patch('dagster_hifld.amtrak_extract.capture_live') as fetch, DagsterInstance.ephemeral() as instance:
            result = materialize([bts_amtrak_stations], instance=instance,
                resources={'staging_storage': remote}, raise_on_error=False,
                run_config={'ops': {bts_amtrak_stations.op.name: {'config': {'capture_dir': '', 'version': 'v0.0.0-test'}}}})
        self.assertFalse(result.success)
        fetch.assert_not_called()

    def test_rejects_byte_tampering_and_preserves_candidate(self):
        build_candidate(self.root, self.root / 'candidate')
        target = self.root / 'candidate/candidate.parquet'
        before = target.read_bytes()
        self.page['features'][0]['attributes']['zip'] = '00001'
        write(self.root, 'page-000', self.page)
        with self.assertRaises(FileExistsError): build_candidate(self.root, self.root / 'candidate')
        self.assertEqual(before, target.read_bytes())
        path = self.root / 'page-000.body'; path.write_bytes(path.read_bytes() + b' ')
        with self.assertRaisesRegex(ValueError, 'receipt/checksum'): inspect_capture(self.root)

    def test_guards_published_storage_before_fetch_or_write(self):
        from dagster_hifld.amtrak_extract import capture_live
        storage = PublishedStorageResource(local_dir=str(self.root/'never-written'), use_local=True)
        with patch('dagster_hifld.amtrak_extract.urllib.request.urlopen') as fetch:
            with self.assertRaisesRegex(ValueError, 'published storage'):
                capture_live(self.root/'capture', storage)
            with self.assertRaisesRegex(ValueError, 'published storage'):
                stage_amtrak_capture(self.root, storage, 'v0.0.0-test')
        fetch.assert_not_called()
        self.assertFalse((self.root/'never-written').exists())
        self.assertFalse((self.root/'capture').exists())

    def test_partial_read_is_durable_before_retry_exhaustion(self):
        from dagster_hifld.amtrak_extract import _capture_json
        class Partial:
            status = 200
            headers = {}
            def geturl(self): return 'https://example.invalid'
            def read(self, size):
                raise http.client.IncompleteRead(b'raw partial bytes', 100)
            def close(self): pass
        with patch('dagster_hifld.amtrak_extract.urllib.request.urlopen', return_value=Partial()):
            with self.assertRaisesRegex(ValueError, '2 bounded attempts'):
                _capture_json(self.root, 'broken', 'https://example.invalid', attempts=2)
        for number in range(2):
            self.assertEqual((self.root/f'attempts/broken/{number:03d}.body').read_bytes(), b'raw partial bytes')
            receipt=json.loads((self.root/f'attempts/broken/{number:03d}.receipt.json').read_bytes())
            self.assertFalse(receipt['complete_body'])
            self.assertEqual(receipt['bytes'], 17)
        self.assertFalse((self.root/'broken.body').exists())

    def test_rejects_duplicate_json_and_false_terminal_receipt(self):
        from dagster_hifld.amtrak_extract import _strict_json
        with self.assertRaisesRegex(ValueError, 'Duplicate JSON'):
            _strict_json(b'{"OBJECTID":1,"OBJECTID":2}')
        receipt_path=self.root/'page-000.receipt.json'
        receipt=json.loads(receipt_path.read_bytes());receipt['complete_body']=False
        receipt_path.write_text(json.dumps(receipt))
        with self.assertRaisesRegex(ValueError, 'receipt/checksum'):
            inspect_capture(self.root)

    def test_changed_official_owner_and_geographic_domain_quarantine(self):
        item=json.loads((self.root/'bts_item.body').read_bytes());item['owner']='third_party'
        write(self.root,'bts_item',item)
        with self.assertRaisesRegex(ValueError, 'official item'):
            inspect_capture(self.root)
        _,page=fixture(self.root);page['features'][0]['geometry']['x']=500
        write(self.root,'page-000',page)
        with self.assertRaisesRegex(ValueError, 'geographic domain'):
            inspect_capture(self.root)

    def test_remote_schedule_returns_skip_without_fetch(self):
        with patch.dict(os.environ, {'HIFLD_STAGING_BUCKET':'never-contact'}):
            with build_schedule_context(scheduled_execution_time=datetime.datetime.now(datetime.timezone.utc), resources={'staging_storage':StagingStorageResource(local_dir=str(self.root/'schedule'),use_local=True)}) as context:
                result=amtrak_draft_refresh_schedule.evaluate_tick(context)
        self.assertFalse(result.run_requests)
        self.assertIn('local staging', result.skip_message)

    def test_published_schedule_and_missing_or_redirected_receipts_fail_closed(self):
        with build_schedule_context(scheduled_execution_time=datetime.datetime.now(datetime.timezone.utc),
                resources={'staging_storage':PublishedStorageResource(local_dir=str(self.root/'forbidden'),use_local=True)}) as context:
            result=amtrak_draft_refresh_schedule.evaluate_tick(context)
        self.assertFalse(result.run_requests)
        self.assertIn('published storage', result.skip_message)
        for final in (None, 'https://mirror.invalid/query', 'https://services.arcgis.com/other/layer'):
            fixture(self.root)
            path=self.root/'page-000.receipt.json';receipt=json.loads(path.read_bytes())
            if final is None: receipt.pop('final_url')
            else: receipt['final_url']=final
            path.write_text(json.dumps(receipt))
            with self.subTest(final=final), self.assertRaisesRegex(ValueError, 'final response URL'):
                inspect_capture(self.root)

    def test_large_integer_coordinate_cannot_round_into_a_candidate(self):
        self.page['features'][0]['geometry']['x']=2**53+1
        write(self.root,'page-000',self.page)
        with self.assertRaisesRegex(ValueError, 'geographic domain'):
            build_candidate(self.root,self.root/'must-not-exist')
        self.assertFalse((self.root/'must-not-exist/candidate.parquet').exists())

    def test_semantic_provenance_tamper_and_numeric_rounding_cannot_reuse(self):
        destination=self.root/'candidate'
        build_candidate(self.root,destination)
        original=(destination/'candidate.parquet').read_bytes()
        provenance=json.loads((destination/'provenance.json').read_bytes())
        provenance['producer']='unrelated mirror'
        (destination/'provenance.json').write_text(json.dumps(provenance))
        with self.assertRaises(FileExistsError):
            build_candidate(self.root,destination)
        self.assertEqual((destination/'candidate.parquet').read_bytes(), original)
        self.page['features'][0]['attributes']['latitude']=2**53+1
        write(self.root,'page-000',self.page)
        with self.assertRaisesRegex(ValueError, 'would round'):
            build_candidate(self.root,self.root/'numeric-rejection')
        self.assertFalse((self.root/'numeric-rejection/candidate.parquet').exists())

    def test_effective_dependencies_native_runtimes_and_lock_identity_prevent_reuse(self):
        from dagster_hifld.amtrak_extract import processing_identity
        target=self.root/'identity-candidate'
        original=build_candidate(self.root,target)
        expected=(target/'candidate.parquet').read_bytes()
        patches=[
            patch('dagster_hifld.amtrak_extract.shapely.__version__','changed-effective-shapely'),
            patch('dagster_hifld.amtrak_extract.shapely.geos_version_string','changed-native-geos'),
            patch('dagster_hifld.amtrak_extract.pyproj.__version__','changed-effective-pyproj'),
            patch('dagster_hifld.amtrak_extract.pyproj.proj_version_str','changed-native-proj'),
            patch('dagster_hifld.amtrak_extract.platform.python_version',return_value='different-python'),
            patch('dagster_hifld.amtrak_extract._project_identities',return_value={'uv.lock':{'sha256':'f'*64}}),
            patch('dagster_hifld.amtrak_extract.importlib.metadata.version',return_value='different-installed-distribution'),
        ]
        for replacement in patches:
            with replacement:
                self.assertNotEqual(processing_identity()['builder_fingerprint'],original['processing']['builder_fingerprint'])
                with self.assertRaisesRegex(FileExistsError,'version collision'):
                    build_candidate(self.root,target)
            self.assertEqual((target/'candidate.parquet').read_bytes(),expected)
            self.assertEqual(json.loads((target/'provenance.json').read_bytes()),original)
        self.assertEqual(build_candidate(self.root,target),original)


if __name__ == '__main__': unittest.main()


class AmtrakActualReplayReviewRegression(unittest.TestCase):
    """Use the complete committed1,036-row capture, never a substitute fixture."""
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root=pathlib.Path(self.temporary.name)
        self.capture=self.root/'capture'
        self.capture.mkdir()
        archive=pathlib.Path(__file__).resolve().parents[1]/'docs/dataset-sources/amtrak-stations/evidence/complete-source-20261019.zip'
        with zipfile.ZipFile(archive) as raw:
            raw.extractall(self.capture)

    def test_actual_manifest_passes_shared_validation_and_catalog_keeps_every_input(self):
        from dagster_hifld.catalog import summarize_staged_catalog
        from dagster_hifld.source_manifest import load_resolved_source_manifest
        storage=StagingStorageResource(local_dir=str(self.root/'staging'),use_local=True)
        version='v0.0.0-actual-review-provenance'
        stage_amtrak_capture(self.capture,storage,version)
        staged=pathlib.Path(storage.local_dir)/SLUG/SLUG/version
        manifest=json.loads((staged/'metadata/source_manifest.json').read_bytes())
        validate_release_provenance(manifest)
        self.assertEqual(manifest['schema_version'],1)
        self.assertEqual(manifest['processor'],'HIFLD Next')
        self.assertEqual(manifest['complete_rows'],1036)
        bodies=[p for p in (staged/'raw_arcgis').rglob('*.body')]
        self.assertEqual(len(manifest['inputs']),len(bodies))
        self.assertEqual(len(bodies),18)
        self.assertEqual({i['input_id'] for i in manifest['inputs']},{str(p.relative_to(staged)) for p in bodies})
        for item in manifest['inputs']:
            raw=staged/item['raw_path']
            receipt=json.loads((staged/item['receipt_path']).read_bytes())
            self.assertEqual(item['sha256'],hashlib.sha256(raw.read_bytes()).hexdigest())
            self.assertEqual(item['bytes'],raw.stat().st_size)
            self.assertEqual(item['retrieval_url'],receipt['final_url'])
            self.assertEqual(item['canonical_url'],receipt['url'])
            self.assertEqual(item['retrieved_at'],receipt['observed_at'])
            self.assertEqual(item['role'],'official_distributor')
            self.assertTrue(item['delegation_evidence'])
            self.assertIn('release_date_unknown_reason',item)
        resolved=load_resolved_source_manifest(storage,SLUG,SLUG,version)
        summary=summarize_staged_catalog(storage,SLUG,SLUG,version,SLUG,source_metadata=resolved.metadata)
        self.assertEqual(summary.data_dictionary['inputs'],manifest['inputs'])
        self.assertEqual(summary.quality_manifest['feature_count'],1036)
        self.assertFalse(manifest['promotion_allowed'])
        self.assertNotIn('acceptance_decision',manifest)

    def test_all_eight_authentic_witnesses_error_or_nonobject_are_quarantined(self):
        names=['bts_item','bts_item_after','bts_layer','bts_layer_after',
               'bts_count','bts_count_after','bts_ids','bts_ids_after']
        for name in names:
            body=self.capture/(name+'.body')
            receipt_path=self.capture/(name+'.receipt.json')
            original,original_receipt=body.read_bytes(),receipt_path.read_bytes()
            for kind in ('error','array','null'):
                value=json.loads(original)
                if kind=='error':value['error']={'code':500,'message':'Injected replay regression; original authentic fields retained'}
                elif kind=='array':value=[value]
                else:value=None
                changed=json.dumps(value).encode()
                body.write_bytes(changed)
                receipt=json.loads(original_receipt)
                receipt.update(bytes=len(changed),sha256=hashlib.sha256(changed).hexdigest())
                receipt_path.write_text(json.dumps(receipt))
                storage=StagingStorageResource(local_dir=str(self.root/'failures'/name/kind),use_local=True)
                version='v0.0.0-review-'+name.replace('_','-')+'-'+kind
                with self.subTest(name=name,kind=kind), self.assertRaisesRegex(ValueError,'ArcGIS application error or nonobject'):
                    stage_amtrak_capture(self.capture,storage,version)
                stage=pathlib.Path(storage.local_dir)
                self.assertFalse((stage/SLUG).exists())
                copies=list((stage/'quarantine').rglob(name+'.body'))
                self.assertEqual(len(copies),1)
                self.assertEqual(copies[0].read_bytes(),changed)
                retained_receipts=list((stage/'quarantine').rglob(name+'.receipt.json'))
                self.assertEqual(retained_receipts[0].read_bytes(),receipt_path.read_bytes())
                status=json.loads(next((stage/'quarantine').rglob('status.json')).read_bytes())
                self.assertEqual(status['state'],'blocked')
                self.assertTrue(status['raw_preserved'])
                body.write_bytes(original)
                receipt_path.write_bytes(original_receipt)
