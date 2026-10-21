"""Source-native property/type/presence preservation and fail-closed behavior."""
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock,patch
from dagster import Definitions,DefaultScheduleStatus,build_schedule_context,validate_run_config
from dagster_hifld.assets.fdic_locations import fdic_locations_official,fdic_locations_full_quality,fdic_locations_draft_job,fdic_locations_draft_schedule
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import fdic_locations as s

FIXTURE=Path(__file__).parent/'fixtures/fdic_locations/representative_native_records.json'

def rows():return list(json.loads(FIXTURE.read_text())['records'].values())

class FdicLocationsTests(unittest.TestCase):
    def test_real_structural_records_preserve_every_primitive_presence_and_null(self):
        original=rows();collection,quality=s.point_collection(original);decoded=s.strict_json(s.encode(collection))
        self.assertEqual(s.encode([f['properties']for f in decoded['features']]),s.encode(original))
        variants=json.loads(FIXTURE.read_text())['records']
        self.assertIs(type(variants['leading_zero_string_ZIP']['ZIP']),str)
        self.assertNotIn('CBSA_METRO_NAME',variants['integer_variant_and_ABSENT'])
        self.assertIsNone(variants['explicit_null_date']['ACQDATE'])
        self.assertIs(type(variants['integer_zero_coordinates']['LATITUDE']),int)
        self.assertFalse(quality['quality_check_passed']);self.assertGreater(quality['zero_coordinate_pairs'],0)
        self.assertIsNone(quality['producer_declared_CRS'])
        self.assertTrue(all('id'not in f for f in decoded['features']))

    def test_uninterpretable_coordinates_retain_row_and_original_literal_value(self):
        original=[{'ID':'synthetic','UNINUM':1,'LATITUDE':'unknown','LONGITUDE':None}]
        collection,quality=s.point_collection(original)
        self.assertEqual(collection['features'][0]['properties'],original[0]);self.assertIsNone(collection['features'][0]['geometry'])
        self.assertEqual(quality['uninterpretable_coordinates'],1);self.assertFalse(quality['quality_check_passed'])

    def test_boolean_coordinate_is_not_integer_and_never_filled(self):
        collection,quality=s.point_collection([{'LATITUDE':False,'LONGITUDE':0}])
        self.assertIsNone(collection['features'][0]['geometry']);self.assertIs(collection['features'][0]['properties']['LATITUDE'],False)
        self.assertEqual(quality['uninterpretable_coordinates'],1)

    def test_real_out_of_range_coordinates_are_not_swapped_or_repaired(self):
        cases=json.loads(FIXTURE.read_text())['records']
        original=[row for name,row in cases.items()if name.startswith('uninterpretable_literal_coordinates_')]
        self.assertEqual(len(original),2)
        collection,quality=s.point_collection(original)
        self.assertEqual(quality['uninterpretable_coordinates'],2)
        self.assertTrue(all(feature['geometry']is None for feature in collection['features']))
        self.assertEqual(s.encode([feature['properties']for feature in collection['features']]),s.encode(original))

    def test_duplicate_JSON_nonfinite_and_API_error_fail_closed(self):
        for bad in [b'{"ID":"a","ID":"b"}',b'{"x":NaN}',b'{"error":"upstream"}']:
            with self.subTest(bad=bad),self.assertRaises(ValueError):s.strict_json(bad)

    def test_output_byte_guard_rejects_repin_of_native_integer_to_string(self):
        collection,_=s.point_collection(rows());body=s.encode(collection)
        with tempfile.TemporaryDirectory()as temporary:
            storage=StagingStorageResource(local_dir=temporary);root=Path(temporary)/'synthetic';root.mkdir();(root/'geojson').mkdir();(root/'metadata').mkdir()
            expected={s.GEOJSON_KEY:body,'metadata/source_manifest.json':s.encode({'synthetic':True})}
            for name,payload in expected.items():(root/name).write_bytes(payload)
            ready={'version':'v0.0.0-synthetic','processing_fingerprint':'a'*64,'approved':False,'objects':{name:{'sha256':s.digest(payload),'bytes':len(payload)}for name,payload in expected.items()}}
            (root/s.READY).write_bytes(s.encode(ready));s.reconcile_cache(storage,'synthetic',expected,'v0.0.0-synthetic','a'*64)
            changed=s.strict_json(body);changed['features'][0]['properties']['UNINUM']=str(changed['features'][0]['properties']['UNINUM']);bad=s.encode(changed)
            (root/s.GEOJSON_KEY).write_bytes(bad);ready['objects'][s.GEOJSON_KEY]={'sha256':s.digest(bad),'bytes':len(bad)};(root/s.READY).write_bytes(s.encode(ready))
            with self.assertRaisesRegex(ValueError,'repinned_noncanonical_or_changed_ready_receipt'):s.reconcile_cache(storage,'synthetic',expected,'v0.0.0-synthetic','a'*64)

    def test_postcatalog_cache_recomputes_full_canonical_quality_and_dictionary(self):
        with tempfile.TemporaryDirectory()as temporary:
            storage=StagingStorageResource(local_dir=temporary);root=Path(temporary)/'synthetic';(root/'metadata').mkdir(parents=True);(root/'candidate').mkdir()
            expected={'candidate/payload.json':s.encode({'native':True}),'metadata/source_manifest.json':s.encode({'schema_version':1})}
            for name,payload in expected.items():(root/name).write_bytes(payload)
            quality={'version':'v1','quality_check_passed':False,'feature_count':3,'nested':{'bounds':[1,2,3,4]}}
            dictionary={'schema_version':1,'columns':[{'name':'BANK_ID','types':{'string':3},'presence':{'absent':0,'null':1}}]}
            for name,value in [('metadata/quality_manifest.json',quality),('metadata/data_dictionary.json',dictionary)]:
                (root/name).write_bytes(s.encode(value))
            version='v0.0.0-synthetic';fingerprint='a'*64
            ready={'version':version,'processing_fingerprint':fingerprint,'approved':False,'objects':{name:{'sha256':s.digest(payload),'bytes':len(payload)}for name,payload in expected.items()}}
            (root/s.READY).write_bytes(s.encode(ready))
            summary=Mock(quality_manifest=quality,data_dictionary=dictionary)
            with patch.object(s,'load_resolved_source_manifest',return_value=Mock(metadata={})),patch.object(s,'summarize_staged_catalog',return_value=summary):
                s.reconcile_cache(storage,'synthetic',expected,version,fingerprint)
                altered=json.loads((root/'metadata/data_dictionary.json').read_bytes())
                altered['columns'][0]['presence']['absent']=9
                (root/'metadata/data_dictionary.json').write_bytes(s.encode(altered))
                with self.assertRaisesRegex(ValueError,'catalog_sidecar_semantic_drift:metadata/data_dictionary.json'):
                    s.reconcile_cache(storage,'synthetic',expected,version,fingerprint)

    def test_canonical_ready_rejects_false_as_integer(self):
        with tempfile.TemporaryDirectory()as temporary:
            storage=StagingStorageResource(local_dir=temporary);root=Path(temporary)/'synthetic';(root/'metadata').mkdir(parents=True)
            (root/s.READY).write_bytes(s.encode({'version':'v0.0.0-synthetic','processing_fingerprint':'a'*64,'approved':0,'objects':{}}))
            with self.assertRaisesRegex(ValueError,'repinned_noncanonical'):s.reconcile_cache(storage,'synthetic',{},'v0.0.0-synthetic','a'*64)

    def test_PublishedStorage_remote_and_linked_roots_reject_before_acquire(self):
        with patch.object(s,'acquire',side_effect=AssertionError('must not read source'))as acquire:
            for resource in [PublishedStorageResource(local_dir='/tmp/unused-FDIC'),StagingStorageResource(bucket='no-write'),StagingStorageResource(local_dir='/tmp/unused-FDIC',prefix='unsafe')]:
                with self.subTest(resource=type(resource).__name__),self.assertRaises(ValueError):s.stage_fdic_locations(resource,'unused')
            acquire.assert_not_called()
        with tempfile.TemporaryDirectory()as temporary:
            target=Path(temporary)/'target';target.mkdir();link=Path(temporary)/'alias';link.symlink_to(target,target_is_directory=True)
            with self.assertRaises(OSError):s.local_root(StagingStorageResource(local_dir=str(link)))

    def test_oversized_regular_file_rejected_before_body_read(self):
        with tempfile.TemporaryDirectory()as temporary:
            p=Path(temporary)/'oversize';p.write_bytes(b'x'*65)
            with patch.object(Path,'read_bytes',side_effect=AssertionError('unbounded read forbidden')),self.assertRaisesRegex(ValueError,'source_capture_size_limit'):
                s.bounded_regular_read(p,64)

    def test_stream_cap_preserves_complete_delivered_chunk_and_floor_precedes_read(self):
        with tempfile.TemporaryDirectory()as temporary:
            storage=StagingStorageResource(local_dir=temporary);response=Mock(raw=io.BytesIO(b'x'*37));p=Path(temporary)/'capture'
            with self.assertRaisesRegex(ValueError,'size_drift'):s.persist_response_stream(storage,response,p,36)
            self.assertEqual(p.read_bytes(),b'x'*37)
            response=Mock(raw=Mock());q=Path(temporary)/'not-read'
            with patch.object(s,'local_root',side_effect=ValueError('space floor')),self.assertRaises(ValueError):s.persist_response_stream(storage,response,q,36)
            response.raw.read.assert_not_called()

    def test_relative_root_partial_observed_loss_refuses_bootstrap(self):
        with tempfile.TemporaryDirectory(dir=Path.cwd())as temporary:
            storage=StagingStorageResource(local_dir=os.path.relpath(Path(temporary)/'staging',Path.cwd()));candidate=Path(temporary)/'synthetic';(candidate/'metadata').mkdir(parents=True);(candidate/'metadata/source_manifest.json').write_bytes(b'{"synthetic":true}')
            with patch.object(s,'immutable',side_effect=OSError('injected journal body write')),self.assertRaises(OSError):s.observed_trust_loss(storage,'v0.0.0-synthetic','a'*64,candidate=candidate)
            for retry in [1,2]:
                with self.subTest(retry=retry),self.assertRaisesRegex(ValueError,'incomplete_observed_trust_loss_journal'):s.observed_trust_loss(storage,'v0.0.0-synthetic','a'*64)
            self.assertTrue(candidate.exists())

    def test_stopped_source_preservation_schedule_has_valid_real_op_config(self):
        self.assertEqual(fdic_locations_draft_schedule.default_status,DefaultScheduleStatus.STOPPED)
        defs=Definitions(assets=[fdic_locations_official],asset_checks=[fdic_locations_full_quality],jobs=[fdic_locations_draft_job],resources={'staging_storage':StagingStorageResource(local_dir='/tmp/not-materialized-fdic-structural')})
        with patch.dict(os.environ,{'HIFLD_FDIC_LOCATIONS_CAPTURE_DIR':'/tmp/explicit-preserved-captures'}):
            request=fdic_locations_draft_schedule.evaluate_tick(build_schedule_context()).run_requests[0];validate_run_config(defs.resolve_job_def('fdic_locations_draft_reconstruction'),request.run_config)
            self.assertFalse(defs.resolve_job_def('fdic_locations_draft_reconstruction').asset_layer.asset_graph.asset_check_keys)

    @unittest.skipUnless(os.environ.get('HIFLD_FDIC_LOCATIONS_TEST_CAPTURE_DIR'),'Complete real data needs explicit preserved artifact directory, no live refetch.')
    def test_complete_real_typed_source_and_all_baseline_rows(self):
        captures=s.acquire(os.environ['HIFLD_FDIC_LOCATIONS_TEST_CAPTURE_DIR']);records,audit,index=s.decode_native(captures);collection,quality=s.point_collection(records)
        self.assertEqual(len(records),78028);self.assertEqual(quality['zero_coordinate_pairs'],803)
        self.assertEqual(s.encode([f['properties']for f in s.strict_json(s.encode(collection))['features']]),s.encode(records))
        report,lineage=s.full_comparison(captures,records,quality);self.assertEqual(report['populations']['baseline'],79542);self.assertFalse(report['approved']);self.assertTrue(lineage)

if __name__=='__main__':unittest.main()
