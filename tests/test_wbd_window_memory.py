"""WBD window results retain real artifacts without retaining their body objects."""
import gc
import hashlib
import json
import tempfile
import unittest
import weakref
from dataclasses import replace
from functools import partial
from pathlib import Path
from unittest.mock import patch

import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import wbd_hu_family as family
from dagster_hifld.sources import wbd_12_digit_hu as source
from dagster_hifld.sources.arcgis_capture import RetainedCaptureView
from dagster_hifld.sources.capture_common import capture_get, quarantine_incomplete_capture


class TrackedBody(bytearray):
    pass


class WbdWindowMemoryTests(unittest.TestCase):
    def test_recursive_windows_release_real_body_objects_before_return_and_during_merge(self):
        refs = []
        peak = 0
        read_bytes = Path.read_bytes

        def track(path):
            nonlocal peak
            value = read_bytes(path)
            if path.name.startswith('page_') and path.suffix == '.body':
                value = TrackedBody(value)
                refs.append(weakref.ref(value))
                peak = max(peak, sum(ref() is not None for ref in refs))
            return value

        def serve(request):
            count = int(request.url.params['resultRecordCount'])
            oid = int(request.url.params['where'].split('>')[1]) + 1
            doc = ({'error': {'code': 500}} if count > 1 else
                   {'features': [{'attributes': {'objectid': oid}}], 'payload': 'x' * 1_000_000})
            return httpx.Response(200, stream=httpx.ByteStream(json.dumps(doc).encode()))

        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(serve)) as client:
            root = Path(tmp)
            spec = replace(source._spec(), page_size=16, capture_max_bytes=100_000_000,
                           max_bisection_depth=100, attrs_parts=False, single_refusal_retries=0)
            with patch.object(Path, 'read_bytes', track):
                parts, refusals = family.capture_page_window(spec, client, list(range(1, 17)), 0, root,
                    capture=partial(capture_get, max_bytes=2_000_000))
                gc.collect()
                self.assertLessEqual(peak, 2)
                self.assertFalse(any(ref() is not None for ref in refs))
                self.assertEqual(len(parts), 31)
                self.assertEqual(len(refusals), 15)
                combined = RetainedCaptureView(root)
                combined.update(parts)
                gc.collect()
                self.assertLessEqual(peak, 2)
                self.assertFalse(any(ref() is not None for ref in refs))
                for name in combined:
                    body, receipt = combined[name]
                    self.assertEqual(hashlib.sha256(body).hexdigest(), receipt['sha256'])
                    del body
                gc.collect()
                self.assertLessEqual(peak, 2)
                self.assertFalse(any(ref() is not None for ref in refs))

    def test_refusal_and_attrs_bytes_survive_lazy_quarantine_and_symlink_refuses(self):
        refusal = b'{"error":{"code":500}}'
        attributes = b'{"features":[{"attributes":{"objectid":11}}]}'

        def serve(request):
            body = attributes if request.url.params.get('returnGeometry') == 'false' else refusal
            return httpx.Response(200, stream=httpx.ByteStream(body))

        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(serve)) as client:
            base = Path(tmp)
            root = base / 'capture'
            root.mkdir()
            spec = replace(source._spec(), page_size=1, single_refusal_retries=0, attrs_parts=True)
            parts, refusals = family.capture_page_window(spec, client, [11], 0, root,
                capture=partial(capture_get, max_bytes=1000))
            self.assertEqual(set(parts), {'page_0000_refusal', 'page_0000_attrs'})
            self.assertEqual(refusals, ['page_0000_refusal'])
            self.assertEqual(parts['page_0000_refusal'][0], refusal)
            self.assertEqual(parts['page_0000_attrs'][0], attributes)
            storage = StagingStorageResource(local_dir=str(base / 'staging'), use_local=True)
            prefix = quarantine_incomplete_capture(storage, root, 'wbd-test', 'failed', parts,
                                                   'source_page_window_refused_after_bisection')
            for name in parts:
                for suffix in ('body', 'wire'):
                    self.assertEqual(storage.read_key(prefix + '/raw/' + name + '.' + suffix),
                                     (root / (name + '.' + suffix)).read_bytes())
                self.assertEqual(json.loads(storage.read_key(prefix + '/raw/' + name + '.receipt.json')),
                                 parts[name][1])
            (root / 'page_0000_attrs.body').unlink()
            forged = base / 'forged.body'
            forged.write_bytes(b'forged attributes')
            (root / 'page_0000_attrs.body').symlink_to(forged)
            with self.assertRaisesRegex(ValueError, '^retained_capture_symlink_rejected$'):
                parts['page_0000_attrs']
