"""Failure boundaries of sources using shared ordered GET acquisition."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs

import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import current_dart, ems_stations


class RemainingGetAcquisitionTests(unittest.TestCase):
    def test_ems_refuses_invalid_discovery_before_any_page_or_closing_witness(self):
        urls = []

        def respond(request):
            urls.append(str(request.url))
            query = parse_qs(request.url.query.decode())
            if query.get('returnCountOnly') == ['true']:
                document = {'count': 'invalid'}
            elif query.get('returnIdsOnly') == ['true']:
                document = {'objectIds': [7]}
            else:
                document = {'objectIdField': 'OBJECTID'}
            return httpx.Response(200, json=document)

        client_class = httpx.Client
        with patch.object(ems_stations.httpx, 'Client',
                          side_effect=lambda **kwargs: client_class(
                              transport=httpx.MockTransport(respond), **kwargs)):
            with self.assertRaisesRegex(ems_stations.EMSQuarantineError,
                                        'preflight_count_and_id_mismatch'):
                ems_stations._capture(None)
        self.assertEqual(urls, [ems_stations._url(name)
                                for name in ('layer', 'count_before', 'ids_before')])

    def test_dart_terminal_refusals_preserve_each_native_attempt_and_stop_sequence(self):
        urls = []

        def respond(request):
            urls.append(str(request.url))
            return httpx.Response(503, content=b'refused native response')

        client_class = httpx.Client
        with tempfile.TemporaryDirectory() as temporary:
            storage = StagingStorageResource(local_dir=str(Path(temporary) / 'stage'))
            with patch.object(current_dart.httpx, 'Client',
                              side_effect=lambda **kwargs: client_class(
                                  transport=httpx.MockTransport(respond), **kwargs)), \
                    patch.object(current_dart, 'quarantine', wraps=current_dart.quarantine) as quarantined:
                with self.assertRaisesRegex(current_dart.DartQuarantineError, 'bounded_fetch_exhausted'):
                    current_dart.acquire(storage, None)
            self.assertEqual(urls, [next(iter(current_dart.URLS.values()))] * 3)
            self.assertEqual(quarantined.call_count, 3)
            self.assertEqual([call.args[2]['attempt'] for call in quarantined.call_args_list], [1, 2, 3])
            self.assertTrue(all(call.args[1] == b'refused native response'
                                and call.args[2]['partial'] is True
                                for call in quarantined.call_args_list))
            self.assertTrue(any((Path(temporary) / 'stage').rglob('*.json')))
