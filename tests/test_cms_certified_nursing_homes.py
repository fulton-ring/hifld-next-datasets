"""CMS original CSV lexical fidelity and fail-closed draft boundaries."""
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.sources import cms_certified_nursing_homes as source


class CMSTests(unittest.TestCase):
    def fixture(self):
        fields = ['CMS Certification Number (CCN)', 'Latitude', 'Longitude', 'Processing Date', 'Provider Name']
        rows = [['015009', '32.123456', '-86.123456', '2026-10-01', ' A, B '],
                ['015010', '33.0', '-87.0', '2026-10-01', '']]
        out = io.StringIO(newline='')
        writer = csv.writer(out); writer.writerow(fields); writer.writerows(rows)
        return out.getvalue().encode(), fields, rows

    def pins(self, body, fields):
        return patch.multiple(source, EXPECTED_COUNT=2, EXPECTED_CSV_SHA256=source.sha(body),
                              EXPECTED_SCHEMA_SHA256=source.sha(source.canonical(fields)))

    def test_csv_strings_and_proposed_geometry_preserved(self):
        body, fields, rows = self.fixture()
        with self.pins(body, fields):
            table, audit = source.parse_csv(body)
        self.assertEqual(table.column_names, fields + ['geometry'])
        self.assertEqual([[table[f][i].as_py() for f in fields] for i in range(2)], rows)
        self.assertTrue(all(pa.types.is_string(table[f].type) for f in fields))
        self.assertFalse(audit['crs_accepted'])
        self.assertFalse(audit['mapping_approved'])

    def test_incomplete_changed_duplicate_and_invalid_csv_fail(self):
        body, fields, rows = self.fixture()
        variants = [body[:-10], body.replace(b'015010', b'015009'), body.replace(b'015009', b'15009'),
                    body.replace(b'32.123456', b'NaN'), body.replace(b'2026-10-01', b'2026-11-01'),
                    body.replace(b'Latitude', b'LATITUDE'), body.replace(b' A, B ', b' A, C ')]
        for changed in variants:
            with self.subTest(changed=changed[:60]), self.pins(body, fields), self.assertRaises(ValueError):
                source.parse_csv(changed)

    def test_metadata_identity_distribution_and_edition_fail_closed(self):
        meta = {'identifier':'4pq5-n9py','title':'Provider Information','accessLevel':'public',
                'publisher':{'data':{'name':source.PUBLISHER}},'modified':'2026-10-01',
                'released':'2026-10-28','nextUpdateDate':'2026-12-02',
                'distribution':[{'data':{'downloadURL':source.CSV_URL,'mediaType':'text/csv'}}]}
        source.validate_metadata(meta)
        for key, value in [('identifier','other'),('modified','2026-11-01'),('license','approved')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                source.validate_metadata({**meta,key:value})
        changed=json.loads(json.dumps(meta)); changed['distribution'][0]['data']['downloadURL']='https://h-o.st/file.csv'
        with self.assertRaises(ValueError): source.validate_metadata(changed)

    def test_asset_wiring_completeness_and_stopped_schedule(self):
        from dagster import DefaultScheduleStatus
        from dagster_hifld.assets import cms_certified_nursing_homes as assets
        self.assertEqual(assets.cms_certified_nursing_homes.key.path,
                         ['ingest',source.DATASET_SLUG,source.FILE_SLUG])
        self.assertEqual(assets.SCHEDULES[0].default_status, DefaultScheduleStatus.STOPPED)
        result=assets.cms_certified_nursing_homes_complete({'feature_count':source.EXPECTED_COUNT})
        self.assertTrue(result.passed)
        self.assertFalse(assets.cms_certified_nursing_homes_complete({'feature_count':1}).passed)
        for gate in source.GATES: self.assertFalse(result.metadata[gate].value)

    def metadata(self):
        return {'identifier':'4pq5-n9py','title':'Provider Information','accessLevel':'public',
                'publisher':{'data':{'name':source.PUBLISHER}},'modified':'2026-10-01',
                'released':'2026-10-28','nextUpdateDate':'2026-12-02',
                'distribution':[{'data':{'downloadURL':source.CSV_URL,'mediaType':'text/csv'}}]}

    def acquire(self, storage, root, handler):
        import httpx
        real_client = httpx.Client
        with patch.object(source.httpx, 'Client', side_effect=lambda **kwargs:
                          real_client(transport=httpx.MockTransport(handler), **kwargs)):
            return source.capture_cms_certified_nursing_homes(storage,version='vfixture',capture_directory=str(root/'capture'))

    def test_capture_cache_tamper_and_publish_refusal(self):
        import httpx
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
        from dagster_hifld.release_approval import ReleaseApprovalError, require_release_approval
        body,fields,rows=self.fixture()
        def handler(request):
            return httpx.Response(200,stream=httpx.ByteStream(body if str(request.url)==source.CSV_URL else source.canonical(self.metadata())))
        with tempfile.TemporaryDirectory() as tmp, self.pins(body,fields):
            root=Path(tmp); staging=StagingStorageResource(use_local=True,local_dir=str(root/'staging'),prefix='')
            first=self.acquire(staging,root,handler)
            second=self.acquire(staging,root,handler)
            self.assertFalse(first['reused']); self.assertTrue(second['reused'])
            self.assertEqual(first['version'],second['version'])
            key=f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}/geoparquet/{source.FILE_SLUG}.parquet"
            table=pq.read_table(pa.BufferReader(staging.read_key(key)))
            self.assertEqual(table[fields[0]].to_pylist(),['015009','015010'])
            destination=PublishedStorageResource(use_local=True,local_dir=str(root/'published'),prefix='')
            (root/'published').mkdir(); (root/'published'/'sentinel').write_bytes(b'existing')
            with patch.dict('os.environ',{},clear=True), self.assertRaises(ReleaseApprovalError):
                require_release_approval(staging,destination,[(key,key)])
            self.assertEqual(list((root/'published').iterdir()),[root/'published'/'sentinel'])
            self.assertEqual((root/'published'/'sentinel').read_bytes(),b'existing')
            (root/'staging'/key).write_bytes(b'tampered')
            with self.assertRaisesRegex(ValueError,'cached_candidate_object_integrity_failed'):
                self.acquire(staging,root,handler)

    def test_status_partial_and_drift_are_quarantined_without_ready_candidate(self):
        import httpx
        from dagster_hifld.resources import StagingStorageResource
        body,fields,_=self.fixture()
        for failure in ('status','partial','drift','after'):
            calls=[]
            def handler(request):
                calls.append(str(request.url))
                if str(request.url)==source.CSV_URL:
                    if failure=='status': return httpx.Response(503,stream=httpx.ByteStream(b'failure'))
                    if failure=='partial': return httpx.Response(200,stream=httpx.ByteStream(body),headers={'content-length':str(len(body)+5)})
                    return httpx.Response(200,stream=httpx.ByteStream(body.replace(b'015010',b'015009') if failure=='drift' else body))
                meta=self.metadata()
                if failure=='after' and len(calls)==3: meta['modified']='2026-11-01'
                return httpx.Response(200,stream=httpx.ByteStream(source.canonical(meta)))
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp, self.pins(body,fields):
                root=Path(tmp); storage=StagingStorageResource(use_local=True,local_dir=str(root/'staging'),prefix='')
                with self.assertRaises(ValueError): self.acquire(storage,root,handler)
                self.assertTrue(list((root/'staging'/'_quarantine').rglob('failure.json')))
                self.assertFalse(list((root/'staging').rglob('candidate_ready.json')))
