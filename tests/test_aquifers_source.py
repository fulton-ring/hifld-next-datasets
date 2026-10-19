"""Draft source fidelity/denial behavior; fixtures confer no release approval."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from dagster import AssetKey, DefaultScheduleStatus, ScheduleExecutionError, build_schedule_context

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usgs import usgs_aquifers, usgs_aquifers_draft_refresh_schedule
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import aquifers as source

CAPTURES = Path('/workspace/work/hifld-coordinator/19')


class AquifersTests(unittest.TestCase):
    def test_receipt_strict_json_and_official_identity(self):
        for body in (b'[]', b'{"error":{}}', b'{"a":1,"a":2}', b'{"x":{"a":1,"a":2}}'):
            with self.assertRaises(ValueError):
                source.strict_json(body)
        receipt = dict(requested_url=source.URL, final_url=source.URL, status=200, bytes=1,
                       sha256=source.sha(b'x'), fetched_at='2026-10-19T00:00:00+00:00')
        source.validate_receipt(b'x', receipt)
        for field, value in [('final_url','https://example.org/substitute.zip'), ('requested_url','https://example.org/source.zip'),
                             ('status',True), ('bytes',True), ('fetched_at','2026-10-19T00:00:00'), ('sha256','0'*64)]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                source.validate_receipt(b'x', {**receipt, field:value})

    def test_registry_and_stopped_local_schedule(self):
        self.assertEqual(sum(a is usgs_aquifers for a in hifld_ingest_assets), 1)
        self.assertEqual(usgs_aquifers.key, AssetKey(['ingest','aquifers','aquifers']))
        self.assertEqual(usgs_aquifers_draft_refresh_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(usgs_aquifers_draft_refresh_schedule.cron_schedule, '0 7 1 * *')
        for storage in [PublishedStorageResource(use_local=True, local_dir='/tmp/never'),
                        StagingStorageResource(use_local=False, bucket='never'),
                        StagingStorageResource(use_local=True, bucket='still-remote', local_dir='/tmp/never')]:
            with self.subTest(storage=type(storage).__name__), patch.object(source,'acquire') as fetch:
                with self.assertRaises(ValueError):
                    source.stage_aquifers_candidate(storage)
                fetch.assert_not_called()
            with build_schedule_context(resources={'staging_storage': storage}) as context:
                with self.assertRaises(ScheduleExecutionError) as caught:
                    usgs_aquifers_draft_refresh_schedule.evaluate_tick(context)
                self.assertIsInstance(caught.exception.__cause__, source.AquifersQuarantineError)

    def test_bounded_partial_failures_are_all_durable(self):
        class Response:
            status_code = 200
            url = source.URL
            headers = {}
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def iter_bytes(self):
                yield b'partial-real-network-chunk'
                raise httpx.ReadError('connection reset')
        class Client:
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def stream(self,*args): return Response()
        with tempfile.TemporaryDirectory() as tmp, patch.object(source.httpx,'Client',return_value=Client()) as mocked:
            storage=StagingStorageResource(local_dir=tmp)
            with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):
                source.acquire(storage,None,None)
            self.assertEqual(mocked.call_count,3)
            bodies=list(Path(tmp).rglob('response.body'))
            self.assertEqual(len(bodies),3)
            self.assertTrue(all(p.read_bytes()==b'partial-real-network-chunk' for p in bodies))
            self.assertFalse(Path(tmp,'aquifers').exists())

    @unittest.skipUnless(CAPTURES.joinpath('zip.body').exists(), 'optional complete real capture not installed')
    def test_real_archive_native_fidelity_no_repair(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows,audit,members=source.inspect_archive(CAPTURES.joinpath('zip.body').read_bytes(),Path(tmp))
            self.assertEqual(len(rows),4637)
            self.assertEqual(audit['invalid_geometry_count'],20)
            self.assertEqual(audit['nulls']['ROCK_NAME'],1627)
            self.assertEqual(len(members),7)
            self.assertEqual(rows[0]['properties']['OBJECTID_1'],1)
            self.assertEqual(rows[-1]['properties']['OBJECTID_1'],4637)
            self.assertEqual(audit['schema']['properties'],source.SCHEMA)
            with self.assertRaisesRegex(ValueError,'changed_truncated'):
                source.inspect_archive(CAPTURES.joinpath('zip.body').read_bytes()[:-1],Path(tmp))

    @unittest.skipUnless(CAPTURES.joinpath('zip.body').exists(), 'optional complete real capture not installed')
    def test_immutable_reuse_and_changed_dependency_namespace(self):
        identity=source.processing_identity()
        with tempfile.TemporaryDirectory() as tmp, patch.object(source,'processing_identity',return_value=identity):
            storage=StagingStorageResource(local_dir=tmp)
            kwargs=dict(archive_path=CAPTURES/'zip.body',receipt_path=CAPTURES/'zip.json')
            first=source.stage_aquifers_candidate(storage,**kwargs)
            prefix=Path(tmp,'aquifers','aquifers',first['version'])
            before={str(p.relative_to(prefix)):source.sha(p.read_bytes()) for p in prefix.rglob('*') if p.is_file()}
            repeated=source.stage_aquifers_candidate(storage,**kwargs)
            self.assertEqual(first,repeated)
            self.assertEqual(before,{str(p.relative_to(prefix)):source.sha(p.read_bytes()) for p in prefix.rglob('*') if p.is_file()})
            changed=copy.deepcopy(identity);changed['gdal']='proposed-new-runtime'
            with patch.object(source,'processing_identity',return_value=changed):
                other=source.stage_aquifers_candidate(storage,**kwargs)
            self.assertNotEqual(first['version'],other['version'])
            self.assertEqual(before,{str(p.relative_to(prefix)):source.sha(p.read_bytes()) for p in prefix.rglob('*') if p.is_file()})
            manifest=json.loads(prefix.joinpath('metadata/source_manifest.json').read_text())
            source.validate_release_provenance(manifest)
            marker=prefix/'metadata/aquifers_candidate_ready.json';ready=json.loads(marker.read_text())
            del ready['objects']['raw/members/us_aquifers.dbf'];marker.write_text(json.dumps(ready))
            with self.assertRaisesRegex(ValueError,'inventory_conflict'):
                source.stage_aquifers_candidate(storage,**kwargs)
