"""Trusted CI rejects stale and unrelated pull request checkouts."""

import importlib.util
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PATH = Path(__file__).parents[1] / "dataset_pr_identity.py"


class DatasetPRIdentityTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(PATH.is_file(), "trusted exact-head verifier is missing")
        spec = importlib.util.spec_from_file_location("dataset_pr_identity", PATH)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.pr = {
            "number": 698,
            "state": "open",
            "draft": False,
            "head": {
                "sha": "a" * 40,
                "repo": {"full_name": "fulton-ring/hifld-next-datasets"},
            },
            "base": {
                "sha": "b" * 40,
                "ref": "main",
                "repo": {"full_name": "fulton-ring/hifld-next-datasets"},
            },
        }

    def check(self):
        return self.module.validate(
            self.pr, "fulton-ring/hifld-next-datasets", "698", "a" * 40, "b" * 40
        )

    def test_accepts_exact_open_same_repository_pair(self):
        self.assertEqual(
            self.check(),
            {"pr_number": "698", "head_sha": "a" * 40, "base_sha": "b" * 40},
        )

    def test_rejects_stale_head_or_base(self):
        for side in ("head", "base"):
            with self.subTest(side=side):
                original = self.pr[side]["sha"]
                self.pr[side]["sha"] = "c" * 40
                with self.assertRaises(ValueError):
                    self.check()
                self.pr[side]["sha"] = original

    def test_rejects_fork_closed_wrong_base_and_wrong_number(self):
        for section, key, value in (
            (self.pr["head"]["repo"], "full_name", "attacker/fork"),
            (self.pr, "state", "closed"),
            (self.pr["base"], "ref", "other"),
            (self.pr, "number", 699),
        ):
            with self.subTest(key=key):
                original = section[key]
                section[key] = value
                with self.assertRaises(ValueError):
                    self.check()
                section[key] = original

    def test_rejects_noncanonical_or_injected_inputs(self):
        for number, head, base in [
            ("698\nfoo", "a" * 40, "b" * 40),
            ("698", "../main", "b" * 40),
            ("698", "a" * 40, "B" * 40),
        ]:
            with (
                self.subTest(number=number, head=head, base=base),
                self.assertRaises(ValueError),
            ):
                self.module.validate(
                    self.pr, "fulton-ring/hifld-next-datasets", number, head, base
                )

    def test_cli_refuses_workflow_revision_different_from_base_before_api_call(self):
        environment = {
            "GITHUB_REPOSITORY": "fulton-ring/hifld-next-datasets",
            "PR_NUMBER": "698",
            "HEAD_SHA": "a" * 40,
            "BASE_SHA": "b" * 40,
            "GITHUB_SHA": "c" * 40,
            "GH_TOKEN": "test-only-placeholder",
        }
        with (
            patch.dict(os.environ, environment, clear=True),
            patch.object(
                self.module,
                "urlopen",
                side_effect=AssertionError("API must not be called"),
            ),
            self.assertRaisesRegex(ValueError, "workflow snapshot"),
        ):
            self.module.main()

    def test_cli_emits_only_validated_identity_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            environment = {
                "GITHUB_REPOSITORY": "fulton-ring/hifld-next-datasets",
                "PR_NUMBER": "698",
                "HEAD_SHA": "a" * 40,
                "BASE_SHA": "b" * 40,
                "GITHUB_SHA": "b" * 40,
                "GH_TOKEN": "test-only-placeholder",
                "GITHUB_OUTPUT": str(output),
            }
            response = io.BytesIO(json.dumps(self.pr).encode())
            stdout = io.StringIO()
            with (
                patch.dict(os.environ, environment, clear=True),
                patch.object(self.module, "urlopen", return_value=response) as api,
                patch("sys.stdout", stdout),
            ):
                self.module.main()
            self.assertEqual(
                output.read_text(),
                "pr_number=698\nhead_sha=" + "a" * 40 + "\nbase_sha=" + "b" * 40 + "\n",
            )
            self.assertNotIn(environment["GH_TOKEN"], stdout.getvalue())
            request = api.call_args.args[0]
            self.assertEqual(
                request.full_url,
                "https://api.github.com/repos/fulton-ring/hifld-next-datasets/pulls/698",
            )
