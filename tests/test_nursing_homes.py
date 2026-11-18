"""CMS nursing-home CSV fidelity, capture-time completeness and fail-closed draft boundaries."""
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.sources import nursing_homes as source

CSV_URL = 'https://data.cms.gov/provider-data/sites/default/files/resources/fixture/NH_ProviderInfo.csv'
FIELDS = ['CMS Certification Number (CCN)', 'Provider Name', 'Provider Address', 'City/Town',
          'State', 'ZIP Code', 'Latitude', 'Longitude', 'Geocoding Footnote', 'Processing Date']
ROWS = [['015009', ' A, B ', '1 MAIN ST', 'TOWN', 'AL', '35653', '32.123456', '-86.123456', '', '2026-10-01'],
        ['015010', 'C', '2 MAIN ST', 'TOWN', 'AL', '35653', '33.0', '-87.0', '22', '2026-10-01']]


def fixture_body(rows=None):
    out = io.StringIO(newline='')
    writer = csv.writer(out)
    writer.writerow(FIELDS)
    writer.writerows(ROWS if rows is None else rows)
    return out.getvalue().encode()


def metadata(**changes):
    value = {'identifier': '4pq5-n9py', 'title': 'Provider Information', 'accessLevel': 'public',
             'publisher': {'data': {'name': source.PUBLISHER}}, 'modified': '2026-10-01',
             'released': '2026-10-28', 'nextUpdateDate': '2026-12-02',
             'distribution': [{'identifier': 'dist-fixture',
                               'data': {'downloadURL': CSV_URL, 'mediaType': 'text/csv'}}]}
    value.update(changes)
    return value


class NursingHomesTests(unittest.TestCase):
    def test_csv_strings_proposed_geometry_and_capture_witness_preserved(self):
        table, audit, ids = source.parse_csv(fixture_body(), source_count=2, source_modified='2026-10-01')
        self.assertEqual(table.column_names, FIELDS + ['geometry'])
        self.assertEqual([[table[f][i].as_py() for f in FIELDS] for i in range(2)], ROWS)
        self.assertTrue(all(pa.types.is_string(table[f].type) for f in FIELDS))
        self.assertEqual(ids, ['015009', '015010'])
        self.assertTrue(audit['source_population_complete'])
        self.assertEqual(audit['source_count_witness'], 2)
        self.assertEqual(audit['source_coordinate_count'], 2)
        self.assertEqual(audit['source_coordinate_share'], 1.0)
        self.assertEqual(audit['source_state_codes'], ['AL'])
        self.assertEqual(audit['geocoding_footnote_counts'], {'': 1, '22': 1})
        self.assertFalse(audit['crs_accepted'])
        self.assertFalse(audit['mapping_approved'])
        self.assertFalse(hasattr(source, 'EXPECTED_COUNT'))

    def test_incomplete_changed_duplicate_and_invalid_csv_fail(self):
        body = fixture_body()
        variants = [
            (body[:-10], 2),
            (body.replace(b'015010', b'015009'), 2),
            (body.replace(b'015009', b'15009'), 2),
            (body.replace(b'32.123456', b'NaN'), 2),
            (body.replace(b'2026-10-01', b'2026-11-01'), 2),
            (body.replace(b'Latitude', b'LATITUDE'), 2),
            (body.replace(b'Provider Name', b'CMS Certification Number (CCN)'), 2),
            (body.replace(b'Latitude,', b''), 2),
            (body.replace(b'015009', b''), 2),
            (body.replace(b'32.123456', b'91.0'), 2),
            (body.replace(b'32.123456', b'0.0').replace(b'-86.123456', b'0.0'), 2),
            (body, 1),
        ]
        for changed, count in variants:
            with self.subTest(changed=changed[:60], count=count), self.assertRaises(ValueError):
                source.parse_csv(changed, source_count=count, source_modified='2026-10-01')

    def test_metadata_identity_distribution_and_edition_fail_closed(self):
        descriptor = source.validate_metadata(metadata())
        self.assertEqual(descriptor['download_url'], CSV_URL)
        self.assertEqual(descriptor['distribution_id'], 'dist-fixture')
        for key, value in [('identifier', 'other'), ('modified', 'not-a-date'),
                           ('license', 'approved')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                source.validate_metadata(metadata(**{key: value}))
        for distribution in (None, [], [{'identifier': 'x', 'data': {'downloadURL': 'https://h-o.st/file.csv',
                                                                     'mediaType': 'text/csv'}}],
                             [{'identifier': 'x', 'data': {'downloadURL': CSV_URL, 'mediaType': 'text/plain'}}]):
            with self.subTest(distribution=distribution), self.assertRaises(ValueError):
                source.validate_metadata(metadata(distribution=distribution))

    def acquire(self, storage, root, handler):
        import httpx
        real_client = httpx.Client
        with patch.object(source.httpx, 'Client', side_effect=lambda **kwargs:
                          real_client(transport=httpx.MockTransport(handler), **kwargs)):
            return source.capture_nursing_homes(storage, version='vfixture',
                                                capture_directory=str(root / 'capture'))

    def handler(self, body=None):
        import httpx
        body = fixture_body() if body is None else body
        def handle(request):
            url = str(request.url)
            if url == source.METADATA_URL:
                return httpx.Response(200, stream=httpx.ByteStream(source.canonical(metadata())))
            if url.startswith(source.DATASTORE_URL):
                query = parse_qs(urlsplit(url).query)
                offset, limit = int(query['offset'][0]), int(query['limit'][0])
                ids = [row[0] for row in ROWS]
                document = {'count': len(ids), 'results': [[ccn] for ccn in ids[offset:offset + limit]],
                            'query': {'limit': limit, 'offset': offset,
                                      'resources': [{'id': 'dist-fixture'}]}}
                return httpx.Response(200, stream=httpx.ByteStream(source.canonical(document)))
            if url == CSV_URL:
                return httpx.Response(200, stream=httpx.ByteStream(body))
            raise AssertionError(url)
        return handle

    def test_capture_cache_tamper_and_publish_refusal(self):
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
        from dagster_hifld.release_approval import ReleaseApprovalError, require_release_approval
        body = fixture_body()
        handler = self.handler(body)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            staging = StagingStorageResource(use_local=True, local_dir=str(root / 'staging'), prefix='')
            first = self.acquire(staging, root, handler)
            second = self.acquire(staging, root, handler)
            self.assertFalse(first['reused'])
            self.assertTrue(second['reused'])
            self.assertEqual(first['version'], second['version'])
            self.assertTrue(first['source_population_complete'])
            self.assertEqual(first['source_count_witness'], 2)
            key = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}/geoparquet/{source.FILE_SLUG}.parquet"
            table = pq.read_table(pa.BufferReader(staging.read_key(key)))
            self.assertEqual(table[FIELDS[0]].to_pylist(), ['015009', '015010'])
            destination = PublishedStorageResource(use_local=True, local_dir=str(root / 'published'), prefix='')
            (root / 'published').mkdir()
            (root / 'published' / 'sentinel').write_bytes(b'existing')
            with patch.dict('os.environ', {}, clear=True), self.assertRaises(ReleaseApprovalError):
                require_release_approval(staging, destination, [(key, key)])
            self.assertEqual(list((root / 'published').iterdir()), [root / 'published' / 'sentinel'])
            self.assertEqual((root / 'published' / 'sentinel').read_bytes(), b'existing')
            (root / 'staging' / key).write_bytes(b'tampered')
            with self.assertRaisesRegex(ValueError, 'cached_candidate_object_integrity_failed'):
                self.acquire(staging, root, handler)

    def test_status_partial_roster_and_metadata_drift_are_quarantined(self):
        import httpx
        from dagster_hifld.resources import StagingStorageResource
        body = fixture_body()
        for failure in ('status', 'partial', 'roster', 'after'):
            calls = {'metadata': 0}
            def handler(request, failure=failure, calls=calls):
                url = str(request.url)
                if url == source.METADATA_URL:
                    calls['metadata'] += 1
                    value = metadata()
                    if failure == 'after' and calls['metadata'] == 2:
                        value['description'] = 'changed during capture'
                    return httpx.Response(200, stream=httpx.ByteStream(source.canonical(value)))
                if url.startswith(source.DATASTORE_URL):
                    query = parse_qs(urlsplit(url).query)
                    offset, limit = int(query['offset'][0]), int(query['limit'][0])
                    ids = [row[0] for row in ROWS]
                    document = {'count': len(ids), 'results': [[ccn] for ccn in ids[offset:offset + limit]],
                                'query': {'limit': limit, 'offset': offset,
                                          'resources': [{'id': 'dist-fixture'}]}}
                    return httpx.Response(200, stream=httpx.ByteStream(source.canonical(document)))
                if url == CSV_URL:
                    if failure == 'status':
                        return httpx.Response(503, stream=httpx.ByteStream(b'failure'))
                    if failure == 'partial':
                        return httpx.Response(200, stream=httpx.ByteStream(body),
                                              headers={'content-length': str(len(body) + 5)})
                    if failure == 'roster':
                        return httpx.Response(200, stream=httpx.ByteStream(body.replace(b'015010', b'015011')))
                    return httpx.Response(200, stream=httpx.ByteStream(body))
                raise AssertionError(url)
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                storage = StagingStorageResource(use_local=True, local_dir=str(root / 'staging'), prefix='')
                with self.assertRaises(ValueError):
                    self.acquire(storage, root, handler)
                self.assertTrue(list((root / 'staging' / '_quarantine').rglob('failure.json')))
                self.assertFalse(list((root / 'staging').rglob('candidate_ready.json')))

    def test_asset_wiring_completeness_and_stopped_schedule(self):
        from dagster import AssetCheckSeverity, DefaultScheduleStatus
        from dagster_hifld.assets import nursing_homes as assets
        self.assertEqual(assets.nursing_homes.key.path, ['ingest', source.DATASET_SLUG, source.FILE_SLUG])
        self.assertEqual(assets.SCHEDULES[0].default_status, DefaultScheduleStatus.STOPPED)
        candidate = {'source_population_complete': True, 'source_id_roster_complete': True,
                     'source_count_witness': 2, 'feature_count': 2, 'source_coordinate_count': 2,
                     'source_id_sha256': 'abc'}
        result = assets.nursing_homes_population_complete(candidate)
        self.assertTrue(result.passed)
        self.assertFalse(assets.nursing_homes_population_complete({**candidate, 'feature_count': 1}).passed)
        self.assertFalse(assets.nursing_homes_population_complete({**candidate, 'source_coordinate_count': 1}).passed)
        for gate in source.GATES:
            self.assertFalse(result.metadata[gate].value)
        drift = assets.nursing_homes_approved_baseline_drift(candidate)
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertEqual(drift.metadata['comparison_status'].value, 'skipped_no_approved_baseline')


if __name__ == '__main__':
    unittest.main()
