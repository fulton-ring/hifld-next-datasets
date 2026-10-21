"""Focused checks for the reusable shared FRS helper's packaged runtime path."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from dagster_hifld.construction_provenance import (
    ConstructionCodeIdentityError,
    validate_construction_code_identity,
)
from dagster_hifld.sources.epa_frs_common import processing_identity


class EpaFrsPackagedIdentityTests(unittest.TestCase):
    def test_processing_identity_and_validator_work_without_git(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package = root / "src/dagster_hifld"
            package.mkdir(parents=True)
            (package / "source.py").write_text("candidate transform = 1\n")
            (root / "pyproject.toml").write_text("[project]\nname = 'fixture'\n")
            (root / "uv.lock").write_text("version = 1\n")
            self.assertFalse((root / ".git").exists())

            identity = processing_identity(root)
            self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
            self.assertEqual(identity["working_tree_status"], "unknown")
            self.assertRegex(identity["code_commit"], r"^packaged-sha256:[0-9a-f]{64}$")
            validate_construction_code_identity(
                root, identity["code_commit"], identity["files"], identity=identity,
            )

            (package / "source.py").write_text("changed transform\n")
            with self.assertRaisesRegex(ConstructionCodeIdentityError, "construction_packaged_file_hash_mismatch"):
                validate_construction_code_identity(
                    root, identity["code_commit"], identity["files"], identity=identity,
                )
