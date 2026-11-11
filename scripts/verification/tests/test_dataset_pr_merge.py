"""Exercise the trusted workflow's merge with real, divergent Git histories."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).parents[3] / ".github/workflows/verify-dataset-pr.yml"


def merge_step():
    lines = WORKFLOW.read_text().splitlines()
    start = lines.index("      - name: Prepare exact prospective merge")
    start = lines.index("        run: |", start) + 1
    body = []
    for line in lines[start:]:
        if line and not line.startswith("          "):
            break
        body.append(line[10:])
    return "\n".join(body)


class DatasetPRMergeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = root = Path(self.temporary.name)

        def git(*args):
            return subprocess.check_output(
                ["git", "-C", str(root), *args], text=True, stderr=subprocess.PIPE
            ).strip()

        def commit(message):
            git("add", ".")
            git("commit", "-qm", message)
            return git("rev-parse", "HEAD")

        git("init", "-q")
        git("config", "user.name", "Fixture")
        git("config", "user.email", "fixture@example.invalid")
        (root / "tests").mkdir()
        (root / "tests/__init__.py").write_text("")
        (root / "pipeline.py").write_text("existing = True\n")
        common = commit("common")
        (root / "pipeline.py").write_text("existing = True\nnew_base_api = True\n")
        (root / "tests/test_baseline.py").write_text(
            "import unittest, pipeline\n"
            "class Baseline(unittest.TestCase):\n"
            " def test_new_base_api(self): self.assertTrue(pipeline.new_base_api)\n"
        )
        base = commit("base adds unrelated API and regression")
        git("checkout", "--detach", common)
        (root / "feature.py").write_text("proposed = True\n")
        (root / "tests/test_proposed.py").write_text(
            "import unittest, feature\n"
            "class Proposed(unittest.TestCase):\n"
            " def test_proposed(self): self.assertTrue(feature.proposed)\n"
        )
        head = commit("PR changes another module")
        self.git, self.commit = git, commit
        self.common, self.base, self.head = common, base, head

    def test_exact_pair_merges_source_and_tests_without_changing_requested_identity(self):
        root, git, commit = self.root, self.git, self.commit
        common, base, head = self.common, self.base, self.head
        environment = {**os.environ, "HEAD_SHA": head, "BASE_SHA": base}
        command = ["bash", "-e", "-o", "pipefail", "-c", merge_step()]
        subprocess.run(command, cwd=root, env=environment, check=True, capture_output=True)
        merged = git("rev-parse", "HEAD")
        self.assertEqual(git("show", "-s", "--format=%P"), f"{head} {base}")
        self.assertEqual(
            git("show", "HEAD:pipeline.py"), (root / "pipeline.py").read_text().strip()
        )
        self.assertNotEqual(merged, head)

        proposed_tests = root / "proposed-tests"
        shutil.copytree(root / "tests", proposed_tests)
        shutil.rmtree(root / "tests")
        git("restore", "--source", base, "--worktree", "--", "tests")
        for tests in (root / "tests", proposed_tests):
            subprocess.run(
                [sys.executable, "-m", "unittest", "discover", "-s", str(tests)],
                cwd=root,
                check=True,
                capture_output=True,
                timeout=10,
            )

        # Identical input commits produce an identical local merge identity.
        git("reset", "--hard", head)
        subprocess.run(command, cwd=root, env=environment, check=True, capture_output=True)
        self.assertEqual(git("rev-parse", "HEAD"), merged)

        # Already integrated bases and fast-forwardable PRs use the same exact-pair path.
        for requested_head, requested_base in ((merged, base), (common, base)):
            with self.subTest(head=requested_head, base=requested_base):
                git("reset", "--hard", requested_head)
                subprocess.run(
                    command,
                    cwd=root,
                    env={**environment, "HEAD_SHA": requested_head, "BASE_SHA": requested_base},
                    check=True,
                    capture_output=True,
                )
                self.assertEqual(
                    git("show", "HEAD:pipeline.py"), "existing = True\nnew_base_api = True"
                )
                git("merge-base", "--is-ancestor", requested_head, "HEAD")
                git("merge-base", "--is-ancestor", requested_base, "HEAD")

        wrong_head = subprocess.run(
            command, cwd=root, env=environment, capture_output=True, check=False
        )
        self.assertNotEqual(wrong_head.returncode, 0)

        # Conflicts must fail rather than selecting either side silently.
        git("reset", "--hard", common)
        (root / "pipeline.py").write_text("existing = False\n")
        conflicting_head = commit("conflicting PR")
        (root / "pipeline.py").write_text("existing = None\n")
        conflicting_base = commit("conflicting base")
        git("reset", "--hard", conflicting_head)
        (root / "pipeline.py").write_text("existing = 123\n")
        conflicting_head = commit("divergent conflicting PR")
        result = subprocess.run(
            command,
            cwd=root,
            env={**environment, "HEAD_SHA": conflicting_head, "BASE_SHA": conflicting_base},
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
