"""Native NTM page and closing-witness failure custody contracts."""
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from test_national_transit_map_routes import _fixture_factory
from test_national_transit_map_stops import (
    _capture_patches,
    _fake_capture_get,
    _metadata_bodies,
    _stop_row,
    _tiny_layer,
)

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import national_transit_map_routes as routes
from dagster_hifld.sources import national_transit_map_stops as stops


class ClosingWitnessFailureTests(unittest.TestCase):
    def test_routes_closing_refusal_keeps_complete_pages_and_stops_closing(self):
        factory, _, _, _, schema, ids, _, _ = _fixture_factory()
        native = routes.capture_get
        observed = []

        def capture(client, url, root, name, **kwargs):
            observed.append(name)
            if name == 'layer_after':
                raise ValueError('closing_refused')
            return native(client, url, root, name, **kwargs)

        with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
            stack.enter_context(patch.object(routes, 'EXPECTED_SOURCE_COUNT', 2))
            stack.enter_context(patch.object(routes, 'EXPECTED_SOURCE_SCHEMA_SHA256', schema))
            stack.enter_context(patch.object(routes, 'EXPECTED_SOURCE_ID_SHA256', ids))
            stack.enter_context(patch.object(routes, 'PAGE_SIZE', 1))
            stack.enter_context(patch.object(routes.httpx, 'Client', side_effect=lambda **kw: factory()))
            stack.enter_context(patch.object(routes, 'capture_get', side_effect=capture))
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend='local')
            with self.assertRaisesRegex(ValueError, 'closing_refused'):
                routes.capture_national_transit_map_routes(
                    storage, capture_directory=str(Path(tmp) / 'captures'))
            self.assertLess(observed.index('page_001'), observed.index('layer_after'))
            self.assertEqual(observed[-1], 'layer_after')
            root = next((Path(tmp) / 'captures').iterdir())
            self.assertTrue((root / 'page_000.body').is_file())
            self.assertTrue((root / 'page_001.wire').is_file())
            self.assertTrue((root / 'page_001.receipt.json').is_file())

    def test_stops_closing_error_preserves_pages_and_does_not_fetch_later_witnesses(self):
        layer, fields = _tiny_layer(1)
        bodies = _metadata_bodies(layer, 2, [1, 2])
        for index, oid in enumerate((1, 2)):
            bodies[f'page_{index:03d}'] = {
                'spatialReference': {'wkid': 4326},
                'features': [_stop_row(fields, oid, -73.0, 42.0, 17.5)],
            }
        bodies['layer_after'] = {'error': {'code': 400}}
        observed = []
        with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
            stack.enter_context(patch.object(stops, 'RETRY_BACKOFF_SECONDS', (0,)))
            for item in _capture_patches(2, 1, _fake_capture_get(observed, bodies)):
                stack.enter_context(item)
            storage = StagingStorageResource(local_dir=str(Path(tmp) / 'staging'),
                                             use_local=True, backend='local')
            with self.assertRaisesRegex(ValueError, 'source_response_error:layer_after'):
                stops.capture_national_transit_map_stops(
                    storage, version='synthetic', capture_directory=str(Path(tmp) / 'capture'))
            closing_index = observed.index('layer_after')
            self.assertEqual(observed[closing_index - 2:closing_index], ['page_000', 'page_001'])
            self.assertEqual(observed[closing_index:], ['layer_after'] * stops.MAX_REQUEST_ATTEMPTS)
            root = Path(tmp) / 'capture/synthetic'
            self.assertTrue((root / 'page_001.body').is_file())
            self.assertTrue((root / 'page_001.wire').is_file())
            self.assertTrue((root / 'page_001.receipt.json').is_file())
