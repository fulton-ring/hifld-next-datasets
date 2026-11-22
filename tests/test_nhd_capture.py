"""Failure contracts at the shared NHD acquisition boundary."""
import hashlib
import json
import re
import tempfile
import unittest
import weakref
import gc
from pathlib import Path
from unittest.mock import Mock

from dagster_hifld.sources import nhd_capture


class CaptureError(ValueError):
    pass


def page(ids):
    return json.dumps({"features": [{"attributes": {"OBJECTID": oid}} for oid in ids]}).encode()


class NhdCaptureTests(unittest.TestCase):
    def test_id_windows_retain_paths_and_release_every_acquired_body(self):
        from dagster_hifld.sources.arcgis_capture import RetainedCaptureView
        class Body(bytearray):
            pass
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            refs = []
            def fetch(client, url, root, name, **kwargs):
                gc.collect()
                self.assertLessEqual(sum(ref() is not None for ref in refs), 1)
                body = Body(b'{"features":[],"exceededTransferLimit":false}')
                refs.append(weakref.ref(body))
                (root / f"{name}.body").write_bytes(body)
                return body, {"url": url}
            pages, refused = nhd_capture.capture_id_windows(
                None, list(range(1, 11)), root, [0], page_size=1, attempts=1,
                base_delay=0, max_body_bytes=1000, window_url=lambda ids: str(ids[0]),
                budget_check=lambda budget: None, budget_guard=lambda budget: None,
                failed_receipt_error_code=lambda *args: None,
                preserve_failed_attempt=lambda *args: None, producer_error_doc=lambda body: False,
                fetch=fetch, sleep=lambda *args: None)
            gc.collect()
            self.assertIsInstance(pages, RetainedCaptureView)
            self.assertEqual(sum(ref() is not None for ref in refs), 0)
            self.assertEqual(len(pages), 10)
            self.assertEqual(refused, {})
            self.assertEqual(pages["page_0000001"][0], b'{"features":[],"exceededTransferLimit":false}')

    def test_corrupt_retained_capture_remains_untouched_in_offline_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "page.body").write_bytes(b"corrupt")
            discard, fetch = Mock(), Mock()
            with self.assertRaisesRegex(CaptureError, "receipt_mismatch"):
                nhd_capture.retained_capture(
                    None, root, "page", "url", read_capture=Mock(side_effect=CaptureError("receipt_mismatch")),
                    discard=discard, fetch=fetch, error_type=CaptureError)
            discard.assert_not_called()
            fetch.assert_not_called()
            self.assertEqual((root / "page.body").read_bytes(), b"corrupt")

    def test_invalid_retained_capture_is_witnessed_before_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "page.body").write_bytes(b"corrupt")
            operations = []

            def discard(root, name):
                operations.append("witness")
                (root / f"{name}.body").rename(root / f"{name}.error.body")

            def fetch(*args):
                self.assertTrue((root / "page.error.body").exists())
                operations.append("fetch")
                return b"fresh", {}

            result = nhd_capture.retained_capture(
                object(), root, "page", "url", read_capture=Mock(side_effect=CaptureError()),
                discard=discard, fetch=fetch, error_type=CaptureError)
            self.assertEqual(result, (b"fresh", {}))
            self.assertEqual(operations, ["witness", "fetch"])

    def test_strict_resume_rejects_checkpoint_digest_and_wrong_roster_slice(self):
        for retained_ids, checkpoint_sha in [([1, 2], "stale"), ([2, 3], None)]:
            with self.subTest(ids=retained_ids), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                body = page(retained_ids)
                (root / "page_0_2.body").write_bytes(body)
                witness = Mock()
                gaps = nhd_capture.plan_segment_gaps(
                    root, [1, 2], load_state=lambda root: {"segments": [{
                        "name": "page_0_2", "body_sha256": checkpoint_sha or hashlib.sha256(body).hexdigest()}]},
                    read_capture=lambda root, name: (body, {}), witness=witness,
                    name_pattern=re.compile(r"page_\d+_\d+"), parse_name=lambda name: (0, 2),
                    segment_name=lambda offset, count: f"page_{offset}_{count}",
                    producer_error=lambda body: False, error_type=CaptureError,
                    chunk_size=10, strict_resume=True)
                self.assertEqual(gaps, [(0, 2)])
                witness.assert_called_once_with(root, "page_0_2")

    def run_segments(self, root, fetch, *, roster=None):
        state = {"segments": [], "complete": True}
        witness, save = Mock(), Mock()
        nhd_capture.fetch_segments(
            object(), root, roster or [1, 2], state,
            plan_gaps=lambda root, ids: [(0, len(ids))],
            segment_name=lambda offset, count: f"page_{offset}_{count}",
            segment_query=lambda offset, count: (offset, count), capture_retry=fetch,
            witness=witness, producer_error=lambda body: "error" in json.loads(body),
            save_state=save, error_type=CaptureError, probe_attempts=2, doc_attempts=4,
            probe_delay=.1, doc_delay=.2, max_body_bytes=1234, strict_resume=True)
        return state, witness, save

    def test_producer_refusal_is_witnessed_then_bisected_with_bounded_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            calls = []

            def fetch(client, window, root, name, **kwargs):
                calls.append((window, kwargs))
                if window == (0, 2):
                    return b'{"error": {"code": 500}}', {}
                return page([window[0] + 1]), {"observed_at": "then"}

            state, witness, _ = self.run_segments(Path(directory), fetch)
            self.assertEqual([entry[0] for entry in calls], [(0, 2), (0, 1), (1, 1)])
            self.assertEqual([entry["name"] for entry in state["segments"]], ["page_0_1", "page_1_1"])
            self.assertFalse(state["complete"])
            witness.assert_called_once_with(Path(directory), "page_0_2")
            self.assertEqual([kwargs["max_bytes"] for _, kwargs in calls], [1234] * 3)
            self.assertEqual([kwargs["attempts"] for _, kwargs in calls], [2, 4, 4])

    def test_incomplete_transport_bisects_without_accepting_the_parent(self):
        with tempfile.TemporaryDirectory() as directory:
            def fetch(client, window, root, name, **kwargs):
                if window == (0, 2):
                    raise ValueError("incomplete_source_capture:reset")
                return page([window[0] + 1]), {"observed_at": "then"}

            state, _, _ = self.run_segments(Path(directory), fetch)
            self.assertEqual([entry["count"] for entry in state["segments"]], [1, 1])

    def test_invalid_population_is_witnessed_and_never_checkpointed(self):
        for body, message in [(page([1]), "source_page_row_count_incomplete"),
                              (page([2, 1]), "source_page_id_slice_incomplete_or_reordered"),
                              (b"[]", "source_page_error")]:
            with self.subTest(body=body), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                state = {"segments": []}
                witness, save = Mock(), Mock()
                with self.assertRaisesRegex(CaptureError, message):
                    nhd_capture.fetch_segments(
                        object(), root, [1, 2], state, plan_gaps=lambda *args: [(0, 2)],
                        segment_name=lambda *args: "page", segment_query=lambda *args: "url",
                        capture_retry=lambda *args, **kwargs: (body, {}), witness=witness,
                        producer_error=lambda body: False, save_state=save, error_type=CaptureError,
                        probe_attempts=1, doc_attempts=1, probe_delay=0, doc_delay=0,
                        max_body_bytes=1000, strict_resume=True)
                self.assertEqual(state["segments"], [])
                witness.assert_called_once_with(root, "page")
                save.assert_not_called()

    def test_single_unservable_feature_keeps_refusal_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            witness = Mock()
            with self.assertRaisesRegex(CaptureError, "source_single_feature_unservable"):
                nhd_capture.fetch_segments(
                    object(), Path(directory), [1], {"segments": []}, plan_gaps=lambda *args: [(0, 1)],
                    segment_name=lambda *args: "page", segment_query=lambda *args: "url",
                    capture_retry=lambda *args, **kwargs: (b'{"error": {}}', {}), witness=witness,
                    producer_error=lambda body: True, save_state=Mock(), error_type=CaptureError,
                    probe_attempts=1, doc_attempts=1, probe_delay=0, doc_delay=0,
                    max_body_bytes=1000, strict_resume=True)
            witness.assert_called_once_with(Path(directory), "page")

    def test_nonempty_tail_is_refused_without_checkpointing(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(CaptureError, "source_tail_page_not_empty"):
                nhd_capture.capture_tail(
                    object(), Path(directory), "url", 2,
                    retained_or_fetch=lambda *args, **kwargs: (page([3]), {}),
                    reject_error_response=Mock(), error_type=CaptureError)


if __name__ == "__main__":
    unittest.main()
