"""Nested captures cannot weaken a budget covering the same retained directory."""
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path

import httpx

from dagster_hifld.http_capture import capture_retention_budget
from dagster_hifld.sources.capture_common import capture_get


class SameRootBudgetTests(unittest.TestCase):
    def test_inner_same_root_larger_limit_cannot_override_outer_limit(self):
        for other_root_scope in (False, True):
            with self.subTest(intervening_scope=other_root_scope), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                retained = root / 'capture'
                retained.mkdir()
                other = root / 'other'
                other.mkdir()
                with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(
                        200, stream=httpx.ByteStream(b'payload')))) as client:
                    with self.assertRaisesRegex(ValueError, 'source_capture_budget_exceeded'):
                        with capture_retention_budget(retained, 1):
                            scope = capture_retention_budget(other, 100000) if other_root_scope else nullcontext()
                            with scope:
                                with capture_retention_budget(retained / '.', 100000):
                                    capture_get(client, 'https://producer.test/query', retained, 'page', max_bytes=10000)
                self.assertLessEqual(sum(p.stat().st_size for p in retained.iterdir()
                                         if p.name.endswith(('.body', '.wire'))), 1)
                self.assertTrue((retained / 'page.receipt.json').is_file())

    def test_same_root_refusal_latches_outer_scope_even_if_inner_error_is_caught(self):
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, stream=httpx.ByteStream(b'payload')))) as client:
            root = Path(tmp)
            with self.assertRaisesRegex(ValueError, 'source_capture_budget_exceeded'):
                with capture_retention_budget(root, 100000):
                    try:
                        with capture_retention_budget(root, 1):
                            capture_get(client, 'https://producer.test/query', root, 'page', max_bytes=10000)
                    except ValueError:
                        pass
