"""Safety checks for the local registered-gate proof CLI."""

from pathlib import Path
import tempfile
import unittest

from scripts.verification.registered_gate_proof import (
    ensure_release_metadata_prerequisite,
    is_exact_approval_failure,
    validate_gate_selection,
    validate_run_root,
)


class RegisteredGateProofSafetyTests(unittest.TestCase):
    def test_gate_selection_is_explicit_and_does_not_expand_to_release_metadata(self):
        validate_gate_selection(["publish/verify", "publish/promote"])
        for selected in (
            ["publish/verify"],
            ["publish/verify", "publish/promote", "publish/release_metadata"],
        ):
            with self.subTest(selected=selected), self.assertRaisesRegex(
                AssertionError, "select only verify and promote"
            ):
                validate_gate_selection(selected)

    def test_synthetic_release_metadata_is_valid_and_limited_to_proof_copy(self):
        from dagster_hifld.portolan.release import read_staged_release
        from dagster_hifld.resources import StagingStorageResource

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            staging = root / "staging"
            prefix = "synthetic/points/v0.0.0-draft"
            candidate = staging / prefix
            candidate.mkdir(parents=True)
            (candidate / "metadata.txt").write_text("candidate bytes", encoding="utf-8")
            source_copy = root / "source-candidate"
            source_copy.mkdir()
            (source_copy / "metadata.txt").write_text("candidate bytes", encoding="utf-8")

            result = ensure_release_metadata_prerequisite(candidate, prefix)
            document, destination_prefix = read_staged_release(
                StagingStorageResource(
                    use_local=True, local_dir=str(staging), prefix="", backend="local"
                ),
                prefix,
            )

            self.assertEqual(result["kind"], "synthetic_gate_only")
            self.assertEqual(document["version"], "0.0.0-gate-proof")
            self.assertEqual(document["id"], "hifld/synthetic/points/0.0.0-gate-proof")
            self.assertEqual(destination_prefix, "synthetic/points/0.0.0-gate-proof")
            self.assertEqual((candidate / "metadata.txt").read_text(encoding="utf-8"), "candidate bytes")
            self.assertFalse((source_copy / "collection.json").exists())

    def test_existing_release_metadata_is_validated_and_never_overwritten(self):
        import json

        with tempfile.TemporaryDirectory() as temporary:
            staging = Path(temporary) / "staging"
            prefix = "synthetic/points/v0.0.0-draft"
            candidate = staging / prefix
            candidate.mkdir(parents=True)
            collection = candidate / "collection.json"
            original = b'{"type":"FeatureCollection","version":"0.0.0"}\n'
            collection.write_bytes(original)

            with self.assertRaisesRegex(ValueError, "Staged Collection"):
                ensure_release_metadata_prerequisite(candidate, prefix)

            self.assertEqual(collection.read_bytes(), original)

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
