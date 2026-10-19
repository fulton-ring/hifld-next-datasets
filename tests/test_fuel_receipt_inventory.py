"""Canonical provenance consumes pinned successful inputs, not raw diagnostics."""
import copy
import hashlib
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

from dagster_hifld.assets.fuel_stations import SLUG, _release_inputs, build_candidate, inspect_capture, stage_fuel_capture
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.release_approval import ReleaseApprovalError
from tests.test_fuel_stations import fixture, write


class FuelReceiptInventoryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=pathlib.Path(self.temp.name);self.capture=self.root/'capture';self.capture.mkdir()
        self.layer,self.page=fixture(self.capture)
        self.staging=StagingStorageResource(local_dir=str(self.root/'staging'),use_local=True)
        signature=[{k:f[k] for k in ('name','type','nullable')} for f in self.layer['fields']]
        self.schema=hashlib.sha256(json.dumps(signature,sort_keys=True,separators=(',',':')).encode()).hexdigest()

    def test_extra_null_headers_receipt_and_malformed_diagnostics_are_pinned_untrusted(self):
        extras={'page-retained-diagnostic.receipt.json':b'{"observed_at":"2026-10-19T00:00:00Z","headers":null}',
            'page-retained-diagnostic.body':b'partial invalid upstream response',
            'bts-failed.receipt.json':b'{malformed failed response receipt',
            'attempts/page-000/000.body':b'{"error":',
            'attempts/page-000/000.receipt.json':b'{"headers":null}'}
        for name,raw in extras.items():
            path=self.capture/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
        audit=inspect_capture(self.capture)
        self.assertEqual({r['name'] for r in audit['untrusted_diagnostic_files']},set(extras))
        self.assertEqual(len(audit['canonical_retrieval_receipts']),8)
        self.assertEqual(len(_release_inputs(self.capture,audit)),8)
        destination=self.root/'candidate';result=build_candidate(self.capture,destination)
        self.assertEqual(result['rows'],1)
        self.assertEqual(len(result['original_retrieval_receipts']),8)
        self.assertFalse(any(x['name'] in extras for x in result['original_retrieval_receipts']))
        self.assertTrue((destination/'candidate.parquet').exists());self.assertTrue((destination/'provenance.json').exists())
        for name,raw in extras.items():
            record=next(x for x in result['raw_files'] if x['name']==name)
            self.assertEqual(record['sha256'],hashlib.sha256(raw).hexdigest());self.assertEqual(record['bytes'],len(raw))
        with patch('dagster_hifld.assets.fuel_stations.PROPOSED_SCHEMA_SHA256',self.schema):
            stage_fuel_capture(self.capture,self.staging,'v0.0.0-diagnostics')
        staged=self.root/'staging'/SLUG/SLUG/'v0.0.0-diagnostics'
        manifest=json.loads((staged/'metadata/source_manifest.json').read_bytes())
        self.assertEqual(len(manifest['inputs']),8)
        for name,raw in extras.items():self.assertEqual((staged/'raw_arcgis'/name).read_bytes(),raw)
        previous=(destination/'candidate.parquet').read_bytes();provenance=(destination/'provenance.json').read_bytes()
        (self.capture/'page-retained-diagnostic.receipt.json').write_bytes(b'changed failed diagnostic')
        with self.assertRaisesRegex(FileExistsError,'processing request changed'):build_candidate(self.capture,destination)
        self.assertEqual((destination/'candidate.parquet').read_bytes(),previous)
        self.assertEqual((destination/'provenance.json').read_bytes(),provenance)

    def test_null_optional_headers_on_canonical_receipt_cannot_cause_late_failure(self):
        path=self.capture/'page-000.receipt.json';receipt=json.loads(path.read_text());receipt['headers']=None
        path.write_text(json.dumps(receipt))
        result=build_candidate(self.capture,self.root/'candidate')
        self.assertEqual(len(result['original_retrieval_receipts']),8)
        self.assertIsNone(next(x for x in result['original_retrieval_receipts'] if x['name']==path.name)['http_date'])
        self.assertTrue((self.root/'candidate/provenance.json').is_file())

    def test_invalid_canonical_receipt_fails_before_any_destination_or_version(self):
        previous=self.root/'prior';build_candidate(self.capture,previous)
        saved=(previous/'candidate.parquet').read_bytes()
        path=self.capture/'page-000.receipt.json';receipt=json.loads(path.read_text());del receipt['observed_at'];path.write_text(json.dumps(receipt));raw=path.read_bytes()
        with self.assertRaises(ValueError):build_candidate(self.capture,self.root/'rejected')
        self.assertFalse((self.root/'rejected').exists())
        with self.assertRaises(ValueError):stage_fuel_capture(self.capture,self.staging,'v0.0.0-reject')
        self.assertFalse((self.root/'staging'/SLUG).exists())
        retained=list((self.root/'staging/quarantine').rglob('page-000.receipt.json'))
        self.assertEqual(len(retained),1);self.assertEqual(retained[0].read_bytes(),raw)
        self.assertEqual((previous/'candidate.parquet').read_bytes(),saved)

    def test_captured_license_and_attribution_fail_before_outputs_with_quarantine(self):
        for field in ('licenseInfo','accessInformation'):
            fixture(self.capture)
            value=json.loads((self.capture/'bts_item.body').read_bytes());value[field]='';write(self.capture,'bts_item',value)
            destination=self.root/('rejected-'+field)
            with self.subTest(field=field),self.assertRaises(ValueError):build_candidate(self.capture,destination)
            self.assertFalse(destination.exists())
            with self.assertRaises(ValueError):stage_fuel_capture(self.capture,self.staging,'v0.0.0-'+field)
            self.assertFalse((self.root/'staging'/SLUG).exists())
            self.assertTrue(any(p.read_bytes()==(self.capture/'bts_item.body').read_bytes()
                for p in (self.root/'staging/quarantine').rglob('bts_item.body')))

    def test_shared_standard_input_rights_preflight_runs_before_any_canonical_write(self):
        audit=inspect_capture(self.capture);inputs=_release_inputs(self.capture,audit)
        for field in ('license','attribution'):
            invalid=copy.deepcopy(inputs);invalid[0][field]=''
            destination=self.root/('bad-'+field)
            with patch('dagster_hifld.assets.fuel_stations._release_inputs',return_value=invalid):
                with self.assertRaises(ReleaseApprovalError):build_candidate(self.capture,destination)
                with patch('dagster_hifld.assets.fuel_stations.PROPOSED_SCHEMA_SHA256',self.schema):
                    with self.assertRaises(ReleaseApprovalError):stage_fuel_capture(self.capture,self.staging,'v0.0.0-'+field)
            self.assertFalse(destination.exists());self.assertFalse((self.root/'staging'/SLUG).exists())
            self.assertTrue((self.root/'staging/quarantine').exists())


if __name__=='__main__':unittest.main()
