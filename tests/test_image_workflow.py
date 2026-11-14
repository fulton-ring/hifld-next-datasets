import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


class ImageWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.safe_load(
            (ROOT / ".github/workflows/publish-images.yml").read_text()
        )

    def test_image_namespace_follows_repository_owner_and_is_lowercase(self):
        build = self.workflow["jobs"]["build-only"]
        step = next((step for step in build["steps"] if step.get("id") == "image"), None)
        self.assertIsNotNone(step, "workflow must resolve the current repository's image")
        self.assertEqual(build["outputs"]["image"], "${{ steps.image.outputs.image }}")
        for repository in (
            "fulton-ring/hifld-next-datasets",
            "Public-Environmental-Data-Partners/hifld-next-datasets",
        ):
            with self.subTest(repository=repository), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "output"
                subprocess.run(
                    ["bash", "-euo", "pipefail", "-c", step["run"]],
                    env={
                        **os.environ,
                        "GITHUB_REPOSITORY": repository,
                        "GITHUB_OUTPUT": str(output),
                    },
                    check=True,
                )
                self.assertIn(
                    f"image=ghcr.io/{repository.lower()}/dagster-user\n",
                    output.read_text(),
                )

    def test_only_publish_job_can_write_packages_and_both_builds_link_source(self):
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        publish = self.workflow["jobs"]["publish"]
        self.assertEqual(publish["permissions"]["packages"], "write")
        self.assertEqual(publish["if"], "github.event_name != 'pull_request' && !github.event.repository.fork")
        self.assertEqual(publish["env"]["DAGSTER_IMAGE"], "${{ needs.build-only.outputs.image }}")
        for job in self.workflow["jobs"].values():
            for step in job["steps"]:
                if step.get("uses", "").startswith("docker/build-push-action@"):
                    self.assertIn(
                        "org.opencontainers.image.source=${{ github.server_url }}/${{ github.repository }}",
                        step["with"]["labels"],
                    )
        self.assertNotIn("fulton-ring", (ROOT / "Dockerfile").read_text())

    def test_public_pull_gate_is_anonymous_and_blocks_latest_when_private(self):
        steps = self.workflow["jobs"]["publish"]["steps"]
        check = next((step for step in steps if step.get("id") == "public-pull"), None)
        self.assertIsNotNone(check, "workflow must verify anonymous pulls")
        latest = next(step for step in steps if step["name"] == "Promote main image to latest")
        self.assertLess(steps.index(check), steps.index(latest))
        self.assertEqual(latest["if"], "github.ref == 'refs/heads/main'")
        for status in (0, 1):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as tmp:
                docker = Path(tmp) / "docker"
                docker.write_text(
                    '#!/bin/sh\n'
                    'test "$1" = --config || exit 2\n'
                    'test -d "$2" || exit 2\n'
                    'test ! -f "$2/config.json" || exit 2\n'
                    'test "$3 $4" = "manifest inspect" || exit 2\n'
                    'test "$5" = "$DAGSTER_IMAGE:$GITHUB_SHA" || exit 2\n'
                    'exit "$PULL_STATUS"\n'
                )
                docker.chmod(0o755)
                result = subprocess.run(
                    ["bash", "-euo", "pipefail", "-c", check["run"]],
                    env={
                        **os.environ,
                        "PATH": f"{tmp}:{os.environ['PATH']}",
                        "DAGSTER_IMAGE": "ghcr.io/pedp/example/dagster-user",
                        "GITHUB_SHA": "abc123",
                        "PULL_STATUS": str(status),
                        "TMPDIR": tmp,
                    },
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, status, result.stderr)
                if status:
                    self.assertIn("::error::", result.stdout)
                    self.assertIn("public", result.stdout.lower())

    def test_package_summary_is_available_even_when_publishing_fails(self):
        steps = self.workflow["jobs"]["publish"]["steps"]
        summary = next((step for step in steps if step.get("id") == "package-summary"), None)
        self.assertIsNotNone(summary, "workflow must show the package and setup instructions")
        self.assertEqual(summary["if"], "always()")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "summary"
            subprocess.run(
                ["bash", "-euo", "pipefail", "-c", summary["run"]],
                env={
                    **os.environ,
                    "DAGSTER_IMAGE": "ghcr.io/pedp/example/dagster-user",
                    "GITHUB_REPOSITORY": "pedp/example",
                    "GITHUB_SERVER_URL": "https://github.com",
                    "GITHUB_SHA": "abc123",
                    "GITHUB_STEP_SUMMARY": str(path),
                },
                check=True,
            )
            self.assertIn("ghcr.io/pedp/example/dagster-user:abc123", path.read_text())
            self.assertIn("/pkgs/container/example%2Fdagster-user", path.read_text())
            self.assertIn("public", path.read_text().lower())


if __name__ == "__main__":
    unittest.main()
