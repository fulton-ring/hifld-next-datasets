"""Safety checks for the local registered-gate proof CLI."""

from pathlib import Path
import tempfile
import unittest

from scripts.verification.registered_gate_proof import (
    is_exact_approval_failure,
    validate_run_root,
)


class RegisteredGateProofSafetyTests(unittest.TestCase):
    def test_rejects_nonempty_run_root_before_writing(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            candidate = base / "candidate"
            candidate.mkdir()
            run_root = base / "run"
            run_root.mkdir()
            marker = run_root / "keep.txt"
            marker.write_text("preserve", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "must be empty"):
                validate_run_root(run_root, candidate)

            self.assertEqual(marker.read_text(encoding="utf-8"), "preserve")

    def test_rejects_run_root_overlapping_candidate(self):
        with tempfile.TemporaryDirectory() as temporary:
            candidate = Path(temporary) / "candidate"
            candidate.mkdir()

            with self.assertRaisesRegex(ValueError, "non-overlapping"):
                validate_run_root(candidate / "run", candidate)

    def test_approval_failure_requires_the_exact_missing_approval_reason(self):
        self.assertTrue(is_exact_approval_failure({
            "step_key": "publish__verify",
            "error": "missing_or_ambiguous_exact_object_approval:object",
        }))
        self.assertFalse(is_exact_approval_failure({
            "step_key": "publish__verify",
            "error": "unrelated verification failure",
        }))
        self.assertFalse(is_exact_approval_failure({
            "step_key": "publish__promote",
            "error": "missing_or_ambiguous_exact_object_approval:object",
        }))


if __name__ == "__main__":
    unittest.main()
