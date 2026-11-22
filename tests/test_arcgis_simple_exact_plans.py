"""Real transport coverage for native policies bound to the shared ID owner."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode

import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import ems_stations as ems
from dagster_hifld.sources import fema_nss_shelter_locations as fema
from dagster_hifld.sources import fuds_division_boundaries as divisions
from dagster_hifld.sources import fuds_irm_property_point as irm
from dagster_hifld.sources import trails_usgs as trails
from dagster_hifld.sources import usace_offices as offices
from tests.test_ems_stations import EMSStationTests
from tests.test_fema_nss_shelter_locations import synthetic_layer, data_elements_body, full_capture_bodies
from tests.test_fuds_division_boundaries import _fixtures as division_documents
from tests.test_trails_usgs import _captures as trail_captures
from tests.test_usace_offices import live_layer, corpus as office_corpus

_REAL_CLIENT = httpx.Client


def client_factory(handler):
    return lambda *args, **kwargs: _REAL_CLIENT(*args, transport=httpx.MockTransport(handler), **kwargs)


def response(body):
    return httpx.Response(200, stream=httpx.ByteStream(body))


class SimpleExactPlanTransportTests(unittest.TestCase):
    def test_shelter_native_id_requests_and_refused_page_prefix(self):
        for refused in (False, True):
            with self.subTest(refused=refused), tempfile.TemporaryDirectory() as tmp, patch.object(fema, 'PAGE_SIZE', 1):
                docs = full_capture_bodies(synthetic_layer(hasZ=False, hasM=False), data_elements_body())
                by_url = {url + ('?' + urlencode(params) if params else ''): value
                          for name, (url, params, value) in docs.items() if not name.startswith('page_')}
                rows = docs['page_000'][2]['features']
                seen = []

                def serve(request):
                    query = request.url.params
                    if query.get('outFields') == '*':
                        wanted = list(map(int, query['objectIds'].split(',')))
                        seen.append(wanted)
                        self.assertEqual(query['returnZ'], 'true')
                        self.assertEqual(query['returnM'], 'true')
                        self.assertEqual(query['outSR'], '4326')
                        page_rows = [row for row in rows if row['attributes']['objectid'] in wanted]
                        if refused and wanted == [102]:
                            page_rows = [rows[0]]
                        value = {'features': page_rows, 'spatialReference': {'wkid': 4326}, 'exceededTransferLimit': False}
                    else:
                        value = by_url[str(request.url)]
                    return response(json.dumps(value).encode())

                storage = StagingStorageResource(local_dir=tmp + '/stage', use_local=True)
                with patch.object(fema.httpx, 'Client', side_effect=client_factory(serve)):
                    if refused:
                        with self.assertRaisesRegex(ValueError, '^source_page_id_slice_mismatch$'):
                            fema.capture_fema_nss_shelter_locations(storage, version='bad', capture_directory=tmp + '/capture')
                        root = next(Path(tmp + '/capture').rglob('page_001.body')).parent
                        self.assertFalse((root / 'layer_after.body').exists())
                        raw_keys = storage.list_prefix('_quarantine/' + fema.DATASET_SLUG + '/')
                        self.assertTrue(any(key.endswith('/raw/page_001.body') for key in raw_keys))
                    else:
                        result = fema.capture_fema_nss_shelter_locations(storage, version='good', capture_directory=tmp + '/capture')
                        self.assertEqual(result['feature_count'], 2)
                        self.assertFalse(result['release_accepted'])
                self.assertEqual(seen, [[101], [102]])

    def test_division_offsets_and_native_order_refusal(self):
        for refused in (False, True):
            with self.subTest(refused=refused), tempfile.TemporaryDirectory() as tmp, patch.object(divisions, 'PAGE_SIZE', 1):
                docs = division_documents(bad_ids=refused)
                offsets = []

                def serve(request):
                    query = request.url.params
                    if request.url.path == httpx.URL(divisions.SERVICE_URL).path:
                        value = docs['service_before']
                    elif request.url.path == httpx.URL(divisions.LAYER_URL).path:
                        value = docs['layer_before']
                    elif query.get('returnCountOnly'):
                        value = docs['count_before']
                    elif query.get('returnIdsOnly'):
                        value = docs['ids_before']
                    else:
                        offset = int(query['resultOffset'])
                        offsets.append(offset)
                        self.assertEqual(query['orderByFields'], 'OBJECTID ASC')
                        self.assertEqual(query['returnZ'], 'true')
                        self.assertEqual(query['returnM'], 'true')
                        value = docs[f'page_{offset:03d}']
                    return response(json.dumps(value).encode())

                storage = StagingStorageResource(local_dir=tmp + '/stage', use_local=True)
                with patch.object(divisions.httpx, 'Client', side_effect=client_factory(serve)):
                    if refused:
                        with self.assertRaisesRegex(ValueError, '^source_page_id_slice_mismatch$'):
                            divisions.capture_fuds_division_boundaries(storage, version='bad', capture_directory=tmp + '/capture')
                        self.assertEqual(offsets, [0])
                        self.assertFalse((Path(tmp) / 'capture' / 'bad' / 'layer_after.body').exists())
                    else:
                        result = divisions.capture_fuds_division_boundaries(storage, version='good', capture_directory=tmp + '/capture')
                        self.assertEqual(result['feature_count'], 2)
                        self.assertEqual(offsets, [0, 1])

    def test_ems_requests_keep_native_body_files_ids_and_closing_order(self):
        bodies, schema_hash, id_hash, row_hash = EMSStationTests()._fixture()
        names = ['layer', 'count_before', 'ids_before', 'page_000', 'page_001', 'ids_after', 'count_after']
        seen = []
        with tempfile.TemporaryDirectory() as tmp, patch.object(ems, 'PAGE_SIZE', 2), patch.object(ems, 'COUNT', 3):
            def serve(request):
                name = names[len(seen)]
                seen.append(name)
                offset = int(name[5:]) * 2 if name.startswith('page_') else None
                self.assertEqual(str(request.url), ems._url(name, offset))
                return response(bodies[name])

            with patch.object(ems.httpx, 'Client', side_effect=client_factory(serve)), patch.object(
                    ems, 'default_capture_dir', return_value=Path(tmp)):
                captured, receipts = ems._capture(None)
            self.assertEqual(seen, names)
            self.assertEqual([receipt['capture'] for receipt in receipts], names)
            with patch.object(ems, 'SCHEMA_SHA256', schema_hash), patch.object(ems, 'IDS_SHA256', id_hash), patch.object(ems, 'ROWS_SHA256', row_hash):
                features, _layer, quality = ems.validate(captured)
            self.assertEqual(quality['feature_count'], 3)
            self.assertEqual([row['attributes']['objectid'] for row in features], sorted(json.loads(bodies['ids_before'])['objectIds']))

    def test_irm_deferred_error_and_malformed_pages_still_capture_closing_brackets(self):
        for malformed in (False, True):
            with self.subTest(malformed=malformed), tempfile.TemporaryDirectory() as tmp, patch.object(irm, 'PAGE_SIZE', 2):
                service = {'serviceItemId': 'df5f8412d041469692ff4846c7131a56', 'mapName': 'FUDS Data',
                           'copyrightText': 'US Army Corps of Engineers', 'layers': [
                               {'id': 5, 'name': 'Interim Risk Management (IRM)', 'type': 'Group Layer'},
                               {'id': irm.LAYER_ID, 'name': irm.LAYER_NAME, 'type': 'Feature Layer', 'parentLayerId': 5},
                               {'id': 7, 'name': 'IRM Project Boundary'}, {'id': 8, 'name': 'IRM Property Boundary'}]}
                layer = {'id': irm.LAYER_ID, 'name': irm.LAYER_NAME, 'geometryType': 'esriGeometryPoint',
                         'spatialReference': {'latestWkid': 4326}, 'advancedQueryCapabilities': {'supportsPagination': True},
                         'fields': [{'name': 'OBJECTID', 'type': 'esriFieldTypeOID'}]}
                requests = []

                def serve(request):
                    requests.append(str(request.url))
                    query = request.url.params
                    if request.url.path == httpx.URL(irm.SERVICE_URL).path:
                        value = service
                    elif request.url.path == httpx.URL(irm.LAYER_URL).path:
                        value = layer
                    elif query.get('returnIdsOnly'):
                        value = {'objectIds': [1, 2, 3]}
                    elif query.get('returnCountOnly'):
                        value = {'count': 3}
                    elif query.get('objectIds') == '1,2':
                        return response(b'{broken' if malformed else b'{"error":{"code":500}}')
                    else:
                        value = {'features': [{'attributes': {'OBJECTID': 3}, 'geometry': {'x': 1, 'y': 1}}]}
                    return response(json.dumps(value).encode())

                storage = StagingStorageResource(local_dir=tmp + '/stage', use_local=True)
                error = json.JSONDecodeError if malformed else ValueError
                pattern = 'Expecting property name' if malformed else '^source_page_error_or_truncated$'
                with patch.object(irm.httpx, 'Client', side_effect=client_factory(serve)), self.assertRaisesRegex(error, pattern):
                    irm.capture_fuds_irm_property_point(storage, version='bad', capture_directory=tmp + '/capture')
                root = next(Path(tmp + '/capture').glob('bad-*'))
                for name in ('layer_after', 'service_after', 'count_after', 'ids_after'):
                    self.assertTrue((root / (name + '.body')).is_file())
                self.assertEqual(len(requests), 11)
                self.assertTrue(storage.list_prefix('_quarantine/' + irm.DATASET_SLUG + '/'))

    def test_trails_deferred_refusal_keeps_all_native_closing_requests(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(trails, 'BATCH_SIZE', 2):
            captures = trail_captures([1, 3, 9])
            bodies = {receipt['url']: body for body, receipt in captures.values()}
            page_url = captures['page_00000'][1]['url']
            bodies[page_url] = b'{"error":{"code":500}}'
            seen = []

            def serve(request):
                seen.append(str(request.url))
                return response(bodies[str(request.url)])

            storage = StagingStorageResource(local_dir=tmp + '/stage', use_local=True)
            with patch.object(trails.httpx, 'Client', side_effect=client_factory(serve)), self.assertRaisesRegex(
                    ValueError, '^trails_page_geometry_type_drift:page_00000$'):
                trails.capture_trails(storage, version='bad', capture_directory=tmp + '/capture')
            root = next(Path(tmp + '/capture').rglob('capture_manifest.json')).parent
            for name in ('count_after', 'ids_after', 'layer_after', 'service_after'):
                self.assertTrue((root / (name + '.body')).is_file())
            expected_after = [captures[name][1]['url'] for name in ('count_after', 'ids_after', 'layer_after', 'service_after')]
            self.assertEqual(seen[-4:], expected_after)

    def test_offices_native_page_and_closing_schema_refusals_stop_at_original_boundary(self):
        for stage in ('page', 'closing'):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as tmp:
                before, after = live_layer(), live_layer()
                if stage == 'closing':
                    after['fields'][0]['type'] = 'esriFieldTypeString'
                responses = office_corpus(before, after)
                if stage == 'page':
                    page_url = next(url for url in responses if 'objectIds=' in url)
                    page = json.loads(responses[page_url][0])
                    page['fields'][0]['type'] = 'esriFieldTypeString'
                    responses[page_url] = [json.dumps(page).encode()]
                seen = []

                def serve(request):
                    seen.append(str(request.url))
                    return response(responses[str(request.url)].pop(0))

                storage = StagingStorageResource(local_dir=tmp, use_local=True)
                pattern = '^query_field_declaration_drift$' if stage == 'page' else '^source_field_schema_drift$'
                with patch.object(offices.httpx, 'Client', side_effect=client_factory(serve)), self.assertRaisesRegex(
                        offices.USACEOfficesSourceError, pattern):
                    offices.stage_usace_offices(storage, version='bad')
                root = Path(tmp) / 'captures' / 'bad'
                self.assertTrue((root / 'features_00000.decoded').is_file())
                self.assertFalse((root / 'count_after.decoded').exists())
                self.assertFalse((root / 'ids_after.decoded').exists())
                self.assertEqual(len(seen), 4 if stage == 'page' else 5)
