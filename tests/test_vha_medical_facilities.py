"""Regression checks for the official anonymous VA locator population."""
import copy
import hashlib
import io
import json
import tempfile
import unittest
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import vha_medical_facilities as va
from dagster_hifld.sources import capture_common


def record(identifier, kind='health', status='NORMAL'):
    attrs = {name: None for name in va.ATTRIBUTE_NAMES}
    attrs.update(id=identifier, uniqueId=identifier.split('_', 1)[1],
                 facilityType='va_health_facility' if kind == 'health' else 'vet_center',
                 name='Clínica VA', classification=None, lat=18.25, long=-66.125,
                 mobile=False, visn='08', operatingStatus={'code': status},
                 access={'health': [], 'effectiveDate': ''}, address=[], feedback=[],
                 hours={}, phone={'main': '555-0100'}, services={'lastUpdated': '2026-11-15'},
                 operationalHoursSpecialInstructions=['Call first'])
    return {'id': identifier, 'type': 'facility', 'attributes': attrs}


def captures():
    populations = {'health': [record('vha_001'), record('vha_001AA', status='COMING_SOON')],
                   'vet_center': [record('vc_001', 'vet_center')]}
    result = {}
    for sweep in ('before', 'after'):
        for kind, rows in populations.items():
            doc = {'data': rows, 'meta': {'pagination': {
                'currentPage': 1, 'prevPage': None, 'nextPage': None,
                'totalPages': 1, 'totalEntries': len(rows)}}}
            name = f'{sweep}_{kind}_0001'
            body = va.canonical(doc)
            receipt = {'method': 'POST', 'url': va.SEARCH_URL, 'final_url': va.SEARCH_URL,
                       'observed_at': '2026-11-15T00:00:00+00:00', 'status': 200,
                       'complete_body': True, 'request_json': va.search_params(kind, 1),
                       'bytes': len(body), 'sha256': va.sha(body), 'wire_bytes': len(body),
                       'wire_sha256': va.sha(body), 'content_encoding': 'identity'}
            result[name] = (body, receipt, body)
    return result


@contextmanager
def fixture_pins():
    with ExitStack() as stack:
        stack.enter_context(patch.object(va, 'EXPECTED_COUNTS', {'health': 2, 'vet_center': 1}))
        stack.enter_context(patch.object(va, 'EXPECTED_IDS_SHA256', ''))
        stack.enter_context(patch.object(va, 'EXPECTED_GEOMETRY_SHA256', ''))
        yield


class VhaMedicalFacilitiesTests(unittest.TestCase):
    def test_complete_two_sweeps_native_ids_selection_and_dates(self):
        with fixture_pins():
            rows, audit = va.validate_pages(captures())
        self.assertEqual([r['id'] for r in rows], ['vc_001', 'vha_001'])
        self.assertEqual(audit['source_count'], 3)
        self.assertEqual(audit['excluded_status_ids'], {'vha_001AA': 'COMING_SOON'})
        self.assertIsNone(audit['source_release_date'])
        self.assertIn('not coordinate', audit['date_semantics'])

    def test_incomplete_duplicate_changed_and_filtered_pages_refuse(self):
        for failure in ('missing', 'duplicate', 'mid_capture', 'mobile', 'type', 'field', 'date', 'id', 'coordinate', 'pagination'):
            with self.subTest(failure=failure), fixture_pins():
                docs = captures()
                if failure == 'missing':
                    docs.pop('after_health_0001')
                else:
                    doc = json.loads(docs['before_health_0001'][0])
                    row = doc['data'][0]
                    if failure == 'duplicate': doc['data'][1] = copy.deepcopy(row)
                    if failure == 'mid_capture': row['attributes']['name'] = 'Changed'
                    if failure == 'mobile': row['attributes']['mobile'] = True
                    if failure == 'type': row['attributes']['facilityType'] = 'va_benefits_facility'
                    if failure == 'field': row['attributes']['unexpected'] = 'new'
                    if failure == 'date': row['attributes']['services']['lastUpdated'] = 'yesterday'
                    if failure == 'id': row['attributes']['uniqueId'] = 1
                    if failure == 'coordinate': row['attributes']['lat'] = '18.25'
                    if failure == 'pagination': doc['meta']['pagination']['totalEntries'] = 4
                    body = va.canonical(doc)
                    receipt = docs['before_health_0001'][1]
                    receipt.update(bytes=len(body), sha256=va.sha(body), wire_bytes=len(body), wire_sha256=va.sha(body))
                    docs['before_health_0001'] = (body, receipt, body)
                with self.assertRaises(ValueError):
                    va.validate_pages(docs)

    def test_parquet_preserves_scalars_complex_json_and_every_coordinate(self):
        with fixture_pins():
            rows, _ = va.validate_pages(captures())
            body = va.geoparquet(rows)
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.column_names, ['id', 'type', *[n for n in va.ATTRIBUTE_NAMES if n != 'id'], 'source_record_json', 'geometry'])
        for source, actual in zip(rows, table.to_pylist()):
            self.assertEqual(json.loads(actual['source_record_json']), source)
            for key, value in source['attributes'].items():
                self.assertEqual(json.loads(actual[key]) if key in va.JSON_FIELDS else actual[key], value)
            geometry = from_wkb(actual['geometry'])
            self.assertEqual((geometry.x, geometry.y), (source['attributes']['long'], source['attributes']['lat']))
        self.assertEqual(table.column('uniqueId').to_pylist(), ['001', '001'])

    def test_normal_anonymous_bootstrap_uses_session_without_recording_credentials(self):
        requests = []
        def respond(request):
            requests.append(request)
            if request.method == 'HEAD':
                return httpx.Response(204, headers={'X-CSRF-Token': 'ephemeral', 'set-cookie': 'session=temporary; Path=/; Secure'})
            self.assertEqual(request.headers['X-CSRF-Token'], 'ephemeral')
            self.assertIn('session=temporary', request.headers.get('cookie', ''))
            return httpx.Response(200, stream=httpx.ByteStream(b'{"data":[]}'))
        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            va.bootstrap_session(client)
            with tempfile.TemporaryDirectory() as tmp:
                _, receipt, _ = va.capture_page(client, Path(tmp), 'before_health_0001', 'health', 1)
                self.assertNotIn('ephemeral', json.dumps(receipt))
                self.assertNotIn('temporary', json.dumps(receipt))
        self.assertEqual(requests[0].url, httpx.URL(va.CSRF_URL))

    def test_source_failures_quarantine_and_no_candidate_is_written(self):
        with fixture_pins(), tempfile.TemporaryDirectory() as tmp:
            fixture = captures(); fixture.pop('after_health_0001')
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / 'stage'))
            with patch.object(va, 'acquire', return_value=fixture):
                with self.assertRaises(ValueError):
                    va.capture_vha_medical_facilities(storage, capture_directory=str(Path(tmp) / 'captures'))
            files = [str(p.relative_to(Path(tmp) / 'stage')) for p in (Path(tmp) / 'stage').rglob('*') if p.is_file()]
            self.assertTrue(files)
            self.assertTrue(all(p.startswith('_quarantine/') for p in files))

    def test_null_coordinates_remain_null_geometry_without_losing_record(self):
        with fixture_pins():
            fixture = captures()
            for name in ('before_health_0001', 'after_health_0001'):
                doc = json.loads(fixture[name][0])
                doc['data'][0]['attributes']['lat'] = None
                body = va.canonical(doc)
                receipt = fixture[name][1]
                receipt.update(bytes=len(body), sha256=va.sha(body), wire_bytes=len(body), wire_sha256=va.sha(body))
                fixture[name] = (body, receipt, body)
            rows, audit = va.validate_pages(fixture)
            actual = pq.read_table(io.BytesIO(va.geoparquet(rows))).to_pylist()
        self.assertEqual(audit['null_geometry_count'], 1)
        self.assertIsNone(actual[1]['geometry'])
        self.assertIsNone(json.loads(actual[1]['source_record_json'])['attributes']['lat'])
        self.assertEqual(actual[1]['long'], -66.125)

    def test_cache_replay_keeps_original_receipts_and_refuses_tampered_objects(self):
        identity = {'code_commit': 'a' * 40, 'files': {'x.py': 'b' * 64}}
        with fixture_pins(), tempfile.TemporaryDirectory() as tmp, \
                patch.object(va, 'processing_identity', return_value=identity), \
                patch.object(va, 'validate_construction_code_identity'), \
                patch.object(capture_common, 'validate_construction_code_identity'):
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / 'stage'))
            with patch.object(va, 'acquire', return_value=captures()):
                first = va.capture_vha_medical_facilities(storage, capture_directory=str(Path(tmp) / 'capture'))
            second = va.capture_vha_medical_facilities(storage, capture_directory=str(Path(tmp) / 'replay'),
                offline_capture_directory=first['capture_directory'])
            self.assertTrue(second['reused'])
            self.assertEqual(first['version'], second['version'])
            parquet = Path(storage.local_dir) / first['candidate_ready'].replace(
                'metadata/candidate_ready.json', f'geoparquet/{va.FILE_SLUG}.parquet')
            parquet.write_bytes(b'tampered')
            with self.assertRaisesRegex(ValueError, 'cached_candidate_object_integrity_failed'):
                va.capture_vha_medical_facilities(storage, capture_directory=str(Path(tmp) / 'replay'),
                    offline_capture_directory=first['capture_directory'])
            captured = Path(first['capture_directory']) / 'before_health_0001.wire'
            captured.write_bytes(b'tampered')
            with self.assertRaisesRegex(ValueError, 'capture_roster_artifact_mismatch'):
                va.load_capture(Path(first['capture_directory']))

    def test_duplicate_json_keys_are_refused_even_when_values_match(self):
        with fixture_pins():
            fixture = captures()
            name = 'before_health_0001'
            body, receipt, _ = fixture[name]
            body = body.replace(b'"uniqueId":"001"', b'"uniqueId":"001","uniqueId":"001"')
            receipt.update(bytes=len(body), sha256=va.sha(body), wire_bytes=len(body), wire_sha256=va.sha(body))
            fixture[name] = (body, receipt, body)
            with self.assertRaisesRegex(ValueError, 'duplicate_json_key'):
                va.validate_pages(fixture)

    def test_transport_errors_keep_failure_witnesses_without_session_headers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            client = httpx.Client(transport=httpx.MockTransport(lambda request:
                httpx.Response(503, stream=httpx.ByteStream(b'unavailable'))))
            with self.assertRaisesRegex(ValueError, 'incomplete_source_capture'):
                va.capture_page(client, root, 'before_health_0001', 'health', 1)
            receipt = json.loads((root / 'before_health_0001.receipt.json').read_bytes())
            self.assertEqual(receipt['status'], 503)
            self.assertEqual((root / 'before_health_0001.wire').read_bytes(), b'unavailable')

    def test_stopped_registered_asset_and_complete_check(self):
        from dagster_hifld.assets.vha_medical_facilities import (
            INGEST_ASSETS, ASSET_CHECKS, SCHEDULES, vha_medical_facilities_complete)
        from dagster import DefaultScheduleStatus
        self.assertEqual(len(INGEST_ASSETS), 1)
        self.assertEqual(len(ASSET_CHECKS), 1)
        self.assertEqual(SCHEDULES[0].default_status, DefaultScheduleStatus.STOPPED)
        self.assertTrue(vha_medical_facilities_complete({'source_population_complete': True, 'feature_count': 2, 'release_accepted': False}).passed)
        self.assertFalse(vha_medical_facilities_complete({'source_population_complete': False, 'feature_count': 2}).passed)


if __name__ == '__main__':
    unittest.main()
