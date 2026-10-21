import copy
import datetime
import hashlib
import io
import json
from pathlib import Path
import tempfile
import subprocess
import shutil
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
        fields = json.loads((Path(__file__).parent/'fixtures/aviation-source-fields.json').read_text())
        attrs = {f['name']: ('000001' if f['type']=='esriFieldTypeString' else 1.25 if f['type']=='esriFieldTypeDouble' else 7) for f in fields}
        first = dict(attrs, OBJECTID=2**53+1, SITE_NO='00001.00', ARPT_ID='A001')
        second = dict(attrs, OBJECTID=2**53+2, SITE_NO='00001.00', ARPT_ID='A002', LAT_DEG=None)
        layer = {'spatialReference':{'wkid':4326,'latestWkid':4326},'fields':fields,'geometryType':'esriGeometryPoint','objectIdField':'OBJECTID',
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

    def test_processing_identity_uses_validated_non_git_package_identity(self):
        packaged = self.root / 'packaged-runtime'
        (packaged / 'src').mkdir(parents=True)
        shutil.copytree(Path(__file__).parents[1] / 'src/dagster_hifld',
                        packaged / 'src/dagster_hifld')
        for name in ('pyproject.toml', 'uv.lock'):
            shutil.copy2(Path(__file__).parents[1] / name, packaged / name)

        identity = a.processing_identity(packaged)
        self.assertEqual(identity['code_identity_scheme'], 'sha256-packaged-dagster-hifld-v1')
        self.assertEqual(identity['working_tree_status'], 'unknown')
        self.assertIsNone(identity['working_tree_modified'])
        self.assertRegex(identity['code_commit'], r'^packaged-sha256:[0-9a-f]{64}$')
        self.assertNotEqual(identity['code_identity_scheme'], 'git-commit-blobs-v1')
        with (packaged / 'src/dagster_hifld/aviation_extract.py').open('ab') as stream:
            stream.write(b'\n# changed after identity construction\n')
        with self.assertRaisesRegex(ValueError, 'construction_packaged_file_hash_mismatch'):
            from dagster_hifld.construction_provenance import validate_construction_code_identity
            validate_construction_code_identity(
                packaged, identity['code_commit'], identity['files'], identity=identity,
            )

    def test_native_dependency_change_separates_versions_without_retrieval_misattribution(self):
        original = _stage_aviation(self.capture,self.storage)
        root = self.root/'stage'/a.SLUG/a.SLUG/original['version']
        previous = {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in root.rglob('*') if p.is_file()}
        old_provenance=json.loads((root/'draft_candidate/provenance.json').read_bytes())
        old_fingerprint=a.processing_identity()['builder_fingerprint']
        for module,name in ((a.shapely,'geos_version_string'),(a.pyproj,'proj_version_str')):
            with self.subTest(dependency=name),mock.patch.object(module,name,'synthetic-changed-native-version'):
                self.assertNotEqual(a.processing_identity()['builder_fingerprint'],old_fingerprint)
        with mock.patch.object(a.shapely,'geos_version_string','synthetic-changed-native-version'):
            with self.assertRaisesRegex(FileExistsError,'Immutable candidate'):
                a.build_candidate(self.capture,root/'draft_candidate')
            changed=_stage_aviation(self.capture,self.storage)
        self.assertNotEqual(changed['version'],original['version'])
        newer=self.root/'stage'/a.SLUG/a.SLUG/changed['version']
        provenance=json.loads((newer/'draft_candidate/provenance.json').read_bytes())
        self.assertEqual(provenance['original_retrieval_receipts'],old_provenance['original_retrieval_receipts'])
        self.assertNotEqual(provenance['processing']['builder_fingerprint'],old_provenance['processing']['builder_fingerprint'])
        self.assertEqual(previous,{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
                                  for p in root.rglob('*') if p.is_file()})
        self.assertEqual(_stage_aviation(self.capture,self.storage),original)

    def test_installed_distributions_and_manifest_hashes_are_in_processing_identity(self):
        identity=a.processing_identity()
        self.assertTrue(identity['installed_distributions'])
        self.assertTrue(identity['versions']['python'])
        for name in ('geos','proj','gdal_pyogrio','gdal_fiona'):
            self.assertTrue(identity['versions'][name])
        for name in ('uv.lock','pyproject.toml'):
            expected=hashlib.sha256((Path(__file__).parents[1]/name).read_bytes()).hexdigest()
            self.assertEqual(identity['dependency_manifests'][name]['sha256'],expected)
        class ChangedDistribution:
            metadata={'Name':'synthetic-version-change'}
            version='999'
        with mock.patch.object(a.importlib.metadata,'distributions',return_value=[ChangedDistribution()]):
            self.assertNotEqual(a.processing_identity()['builder_fingerprint'],identity['builder_fingerprint'])

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

    def test_conflicting_or_extra_returned_crs_witnesses_quarantine_raw(self):
        original=copy.deepcopy(self.data)
        cases=[{'wkid':4326,'latestWkid':3857},{'wkid':999999,'latestWkid':3857},
               {'wkid':102100}, {'latestWkid':3857}, {},
               {'wkid':True,'latestWkid':3857}, {'wkid':102100,'latestWkid':True},
               {'wkid':102100,'latestWkid':3857,'wkt':'GEOGCS["unapproved"]'},
               {'wkid':102100,'latestWkid':3857,'vcsWkid':5703},
               {'wkid':102100,'latestWkid':3857,'latestVcsWkid':5703}]
        for reference in cases:
            with self.subTest(reference=reference):
                self.data=copy.deepcopy(original)
                for name in ('bts_layer','bts_layer_after'):
                    self.data[name]['extent']['spatialReference']=copy.deepcopy(reference)
                self.data['page-000']['spatialReference']=copy.deepcopy(reference);self.write()
                with self.assertRaisesRegex(ValueError,'CRS authority'):_stage_aviation(self.capture,self.storage)
        self.assertTrue(list((self.root/'stage/quarantine').rglob('*.body')))
        self.assertFalse(list((self.root/'stage').glob('aviation-facilities/aviation-facilities/*/draft_candidate/candidate.parquet')))
        self.data=copy.deepcopy(original)
        for name in ('bts_layer','bts_layer_after'):self.data[name]['extent']['spatialReference']['wkid']=3857
        self.data['page-000']['spatialReference']['wkid']=3857;self.write()
        self.assertEqual(a.inspect_capture(self.capture)['rows'],2)

    def test_observed_service_crs_and_extent_geometry_identity_are_pinned_stable(self):
        original=copy.deepcopy(self.data)
        cases=[('extent',{'spatialReference':{'wkid':102100,'latestWkid':3857},'xmin':1}),
               ('spatialReference',{'wkid':3857,'latestWkid':3857}),
               ('hasZ',True),('hasM',0),('objectIdField','RENAMED'),('geometryType','esriGeometryPolygon')]
        for key,value in cases:
            with self.subTest(key=key):
                self.data=copy.deepcopy(original);self.data['bts_layer_after'][key]=value;self.write()
                with self.assertRaises(ValueError):a.inspect_capture(self.capture)
        self.data=copy.deepcopy(original)
        for name in ('bts_layer','bts_layer_after'):self.data[name]['spatialReference']={'wkid':3857,'latestWkid':3857}
        self.write()
        with self.assertRaisesRegex(ValueError,'discrepancy changed'):a.inspect_capture(self.capture)

    def test_every_original_retrieval_time_requires_valid_aware_iso_and_is_preserved(self):
        original=(self.capture/'item.receipt.json').read_bytes()
        bad=['not-a-date','','2026-10-19','2026-10-19T00:00:00','2026-99-19T00:00:00+00:00','2026-10-19T00:00:00+00:99','2026-10-19T00:00:00-05:60','2026-10-19T00:00:00+24:00',True,None,123]
        for value in bad:
            with self.subTest(value=value):
                receipt=json.loads(original);receipt['observed_at']=value
                (self.capture/'item.receipt.json').write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError,'observed_at'):_stage_aviation(self.capture,self.storage)
        (self.capture/'item.receipt.json').write_bytes(original)
        for name in self.data:
            path=self.capture/(name+'.receipt.json');body=path.read_bytes();receipt=json.loads(body);receipt.pop('observed_at')
            path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError,'observed_at'):a.inspect_capture(self.capture)
            path.write_bytes(body)
        for value in ('2026-10-19T05:30:00+05:30','2026-10-18T19:00:00-05:00','2026-10-19T00:00:00Z'):
            receipt=json.loads(original);receipt['observed_at']=value
            (self.capture/'item.receipt.json').write_text(json.dumps(receipt))
            result=_stage_aviation(self.capture,self.storage)
            manifest=json.loads((self.root/'stage'/a.SLUG/a.SLUG/result['version']/'metadata/source_manifest.json').read_bytes())
            self.assertEqual(next(i for i in manifest['inputs'] if i['input_id'].endswith(':item'))['retrieved_at'],value)
        attempts=self.capture/'attempts';attempts.mkdir();(attempts/'failed.receipt.json').write_text(json.dumps({'observed_at':'2026-10-19','error':'retained upstream failure'}))
        with self.assertRaisesRegex(ValueError,'observed_at'):a.inspect_capture(self.capture)

    def test_all_consumed_metadata_preflight_before_any_canonical_candidate(self):
        good=_stage_aviation(self.capture,self.storage)
        prior=self.root/'stage'/a.SLUG/a.SLUG/good['version']
        inventory={str(p.relative_to(prior)):hashlib.sha256(p.read_bytes()).hexdigest() for p in prior.rglob('*') if p.is_file()}
        original=copy.deepcopy(self.data)
        cases=['headers-null','headers-list','date-list','extra-receipt','license','attribution','invalid-manifest']
        for number,case in enumerate(cases):
            with self.subTest(case=case):
                self.data=copy.deepcopy(original);self.write()
                extra=self.capture/'page-retained-diagnostic.receipt.json'
                if extra.exists():extra.unlink()
                if case in ('headers-null','headers-list','date-list'):
                    p=self.capture/'item.receipt.json';receipt=json.loads(p.read_bytes())
                    receipt['date' if case=='date-list' else 'headers']=[] if case!='headers-null' else None
                    p.write_text(json.dumps(receipt))
                elif case=='extra-receipt':
                    extra.write_text(json.dumps({'observed_at':'2026-10-19T00:00:00+00:00','headers':None}))
                elif case in ('license','attribution'):
                    self.data['item']['licenseInfo' if case=='license' else 'accessInformation']=''
                    self.write()
                storage=StagingStorageResource(local_dir=str(self.root/f'bad-{number}'))
                identity=a.processing_identity()
                if case=='invalid-manifest':identity['code_commit']=None
                with mock.patch.object(a,'processing_identity',return_value=identity):
                    with self.assertRaises((ValueError,RuntimeError)):_stage_aviation(self.capture,storage)
                    direct=self.root/f'direct-{number}'
                    with self.assertRaises((ValueError,RuntimeError)):a.build_candidate(self.capture,direct)
                self.assertFalse(list((self.root/f'bad-{number}').glob('aviation-facilities/aviation-facilities/*')))
                self.assertFalse(direct.exists())
                self.assertTrue(list((self.root/f'bad-{number}'/'quarantine').rglob('*.receipt.json')))
                self.assertEqual(inventory,{str(p.relative_to(prior)):hashlib.sha256(p.read_bytes()).hexdigest() for p in prior.rglob('*') if p.is_file()})

    def test_failed_attempts_are_inventory_only_not_successful_input_claims(self):
        attempts=self.capture/'attempts';attempts.mkdir()
        (attempts/'failed.body').write_bytes(b'original partial bytes')
        (attempts/'failed.receipt.json').write_text(json.dumps({'observed_at':'2026-10-19T00:00:00+00:00','headers':None,'error':'retained failure','status':503}))
        result=_stage_aviation(self.capture,self.storage)
        root=self.root/'stage'/a.SLUG/a.SLUG/result['version']
        provenance=json.loads((root/'draft_candidate/provenance.json').read_bytes())
        self.assertTrue(any(r['name']=='attempts/failed.body' for r in provenance['raw_files']))
        self.assertFalse(any('failed' in r['name'] for r in provenance['original_retrieval_receipts']))
        manifest=json.loads((root/'metadata/source_manifest.json').read_bytes())
        self.assertEqual(len(manifest['inputs']),8)

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
