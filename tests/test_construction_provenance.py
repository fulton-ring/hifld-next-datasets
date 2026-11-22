import hashlib
import importlib.metadata
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.construction_provenance import (
    ConstructionCodeIdentityError, validate_construction_code_identity,
)


def git(root, *args, input=None):
    result = subprocess.run(["git", "-C", str(root), *args], input=input,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    return result.stdout.decode().strip()


def fixture(root, object_format="sha1"):
    root.mkdir()
    git(root, "init", "-q", "--object-format=" + object_format)
    (root / "src").mkdir()
    for name, body in {"src/adapter.py": b"original_code = True\n", "pyproject.toml": b"[project]\nname='fixture'\n", "uv.lock": b"version = 1\n"}.items():
        (root / name).write_bytes(body)
    commit(root, "original construction")
    return git(root, "rev-parse", "HEAD"), {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in ["src/adapter.py", "uv.lock", "pyproject.toml"]}


def commit(root, message):
    git(root, "add", ".")
    git(root, "-c", "user.name=Synthetic fixture", "-c", "user.email=fixture@example.invalid", "commit", "-q", "-m", message)


class ConstructionProvenanceTests(unittest.TestCase):
    def assert_code(self, code, root, sha, files):
        with self.assertRaises(ConstructionCodeIdentityError) as result:
            validate_construction_code_identity(root, sha, files)
        self.assertEqual(result.exception.code, code)
        self.assertEqual(str(result.exception), code)

    def test_historical_docs_only_head_and_every_original_processing_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root)
            (root / "docs.md").write_text("later documentation only\n"); commit(root, "documentation")
            before = {name: (root / name).read_bytes() for name in files}
            validate_construction_code_identity(root, sha, files)
            validate_construction_code_identity(root, git(root, "rev-parse", "HEAD"), files)
            for name in files:
                wrong = dict(files); wrong[name] = "0" * 64
                self.assert_code("construction_processing_blob_hash_mismatch", root, sha, wrong)
            self.assertEqual(before, {name: (root / name).read_bytes() for name in files})

    def test_many_binary_blobs_use_bounded_git_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; fixture(root)
            files = {}
            for index in range(64):
                name = f"src/binary-{index}.py"
                body = bytes([index]) + b"\0\n blob header-looking bytes \n" * 20
                (root / name).write_bytes(body)
                files[name] = hashlib.sha256(body).hexdigest()
            commit(root, "many binary processing blobs")
            sha = git(root, "rev-parse", "HEAD")
            run = subprocess.run
            with patch("dagster_hifld.construction_provenance.subprocess.run", wraps=run) as calls:
                validate_construction_code_identity(root, sha, files)
            cat_file_calls = [call for call in calls.call_args_list
                              if "cat-file" in call.args[0]]
            self.assertLessEqual(len(cat_file_calls), 3)
            # Every claimed file is checked even at the end of a batch.
            files["src/binary-63.py"] = "0" * 64
            self.assert_code("construction_processing_blob_hash_mismatch", root, sha, files)

    def test_batch_protocol_corruption_and_changed_sizes_fail_closed(self):
        from dagster_hifld import construction_provenance as provenance
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root)
            original = provenance._git
            def corrupt(mode):
                def run(root, args, env, **kwargs):
                    result = original(root, args, env, **kwargs)
                    if args == ["cat-file", "--batch"]:
                        header, separator, bodies = result.stdout.partition(b"\n")
                        if mode == "short":
                            result.stdout = result.stdout[:-1]
                        elif mode == "extra":
                            result.stdout += b"unrequested content"
                        elif mode == "size":
                            result.stdout = header.rsplit(b" ", 1)[0] + b" 999999\n" + bodies
                        elif mode == "type":
                            result.stdout = header.replace(b" blob ", b" tree ") + separator + bodies
                        elif mode == "oid":
                            result.stdout = b"0" * 40 + header[40:] + separator + bodies
                        elif mode == "nonzero":
                            result.returncode = 1
                    return result
                return run
            for mode in ("short", "extra", "size", "type", "oid", "nonzero"):
                with self.subTest(mode=mode), patch.object(provenance, "_git", side_effect=corrupt(mode)):
                    self.assert_code("construction_git_read_failed", root, sha, files)

    def test_batch_chunks_and_limits_preserve_path_ordered_failures(self):
        from dagster_hifld import construction_provenance as provenance
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; fixture(root)
            files = {}
            for name, body in {"a.py": b"a" * 9, "b.py": b"b" * 9, "c.py": b"c" * 12}.items():
                (root / name).write_bytes(body)
                files[name] = hashlib.sha256(body).hexdigest()
            commit(root, "batch boundaries")
            sha = git(root, "rev-parse", "HEAD")
            original = provenance._git
            with patch.object(provenance, "_MAX_BLOB_BYTES", 16), patch.object(provenance, "_git", wraps=original) as calls:
                validate_construction_code_identity(root, sha, files)
            requests = [call.kwargs["input"] for call in calls.call_args_list
                        if call.args[1] == ["cat-file", "--batch"]]
            self.assertEqual(len(requests), 3)
            # Later oversize cannot hide an earlier wrong hash.
            wrong = dict(files); wrong["a.py"] = "0" * 64
            with patch.object(provenance, "_MAX_BLOB_BYTES", 10):
                self.assert_code("construction_processing_blob_hash_mismatch", root, sha, wrong)
                self.assert_code("construction_processing_blob_size_limit", root, sha, files)
            with patch.object(provenance, "_MAX_TOTAL_BLOB_BYTES", 18):
                self.assert_code("construction_processing_blob_size_limit", root, sha, files)

    def test_changed_source_commit_cannot_claim_the_recorded_old_blobs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root)
            (root / "src/adapter.py").write_text("changed_code = True\n"); commit(root, "changed implementation")
            self.assert_code("construction_processing_blob_hash_mismatch", root, git(root, "rev-parse", "HEAD"), files)
            # Current HEAD equality is intentionally not the trust boundary.
            validate_construction_code_identity(root, sha, files)

    def test_canonical_commit_and_mapping_types(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root)
            for value in [None, False, "HEAD", sha[:12], sha.upper(), "0" * 40, sha + "^{commit}", "f" * 64]:
                self.assert_code("construction_invalid_commit", root, value, files)
            self.assert_code("construction_commit_not_found", root, "f" * 40, files)
            for value in [None, {}, [], {"src/adapter.py": False}, {"src/adapter.py": "no-hash"}]:
                self.assert_code("construction_invalid_processing_files" if not isinstance(value, dict) or not value else "construction_invalid_processing_digest", root, sha, value)
            self.assert_code("construction_invalid_repository", root / "absent", sha, files)
            self.assert_code("construction_invalid_repository", root / "src", sha, files)

    def test_unreachable_commit_rejected_but_real_branch_reference_qualifies(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root)
            tree = git(root, "rev-parse", "HEAD^{tree}")
            orphan = git(root, "-c", "user.name=Synthetic fixture", "-c", "user.email=fixture@example.invalid", "commit-tree", tree, input=b"unreferenced construction\n")
            self.assert_code("construction_commit_not_reachable", root, orphan, files)
            git(root, "branch", "actual-original-construction", orphan)
            validate_construction_code_identity(root, orphan, files)

    def test_unsafe_missing_symlink_directory_and_submodule_are_not_regular_blobs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root)
            os.symlink("uv.lock", root / "symlink.lock"); commit(root, "committed symlink")
            sha = git(root, "rev-parse", "HEAD")
            for path in ["/uv.lock", "C:/uv.lock", "../uv.lock", "src/../uv.lock", "./uv.lock", "src//adapter.py", ".git/config", "uv.lock\n", "src\\adapter.py"]:
                self.assert_code("construction_unsafe_processing_path", root, sha, {path: "0" * 64})
            self.assert_code("construction_processing_blob_missing", root, sha, {"unknown.py": "0" * 64})
            for path in ["src", "symlink.lock"]:
                self.assert_code("construction_nonregular_processing_blob", root, sha, {path: "0" * 64})
            git(root, "update-index", "--add", "--cacheinfo", "160000," + sha + ",dependency")
            git(root, "-c", "user.name=Synthetic fixture", "-c", "user.email=fixture@example.invalid", "commit", "-q", "-m", "gitlink")
            self.assert_code("construction_nonregular_processing_blob", root, git(root, "rev-parse", "HEAD"), {"dependency": "0" * 64})

    def test_literal_paths_cannot_execute_shell_or_git_pathspecs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root)
            for name in ["source$(touch EXECUTED).py", ":(glob)literal.py", "-option.py"]:
                (root / name).write_bytes(b"literal content")
            commit(root, "literal filenames")
            files = {name: hashlib.sha256(b"literal content").hexdigest() for name in ["source$(touch EXECUTED).py", ":(glob)literal.py", "-option.py"]}
            validate_construction_code_identity(root, git(root, "rev-parse", "HEAD"), files)
            self.assertFalse((root / "EXECUTED").exists())

    def test_linked_worktree_and_repo_environment_redirection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root)
            linked = Path(directory) / "linked"
            git(root, "worktree", "add", "-q", "--detach", str(linked), sha)
            with patch.dict(os.environ, {"GIT_DIR": str(Path(directory) / "not-a-repo"), "GIT_WORK_TREE": "/unrelated"}):
                validate_construction_code_identity(linked, sha, files)

    def test_size_limits_fail_without_reading_or_modifying_other_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root)
            before = (root / "src/adapter.py").read_bytes()
            with patch("dagster_hifld.construction_provenance._MAX_BLOB_BYTES", 1):
                self.assert_code("construction_processing_blob_size_limit", root, sha, files)
            with patch("dagster_hifld.construction_provenance._MAX_FILES", 1):
                self.assert_code("construction_invalid_processing_files", root, sha, files)
            with patch("dagster_hifld.construction_provenance._MAX_TOTAL_PATH_BYTES", 1):
                self.assert_code("construction_processing_path_limit", root, sha, files)
            self.assertEqual(before, (root / "src/adapter.py").read_bytes())

    def test_git_unavailable_and_missing_blobs_fail_with_stable_reasons(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root)
            before = {name: (root / name).read_bytes() for name in files}
            with patch("dagster_hifld.construction_provenance.subprocess.run", side_effect=PermissionError("inaccessible Git")):
                self.assert_code("construction_git_read_failed", root, sha, files)
            oid = git(root, "rev-parse", sha + ":uv.lock")
            self.assert_code("construction_commit_not_found", root, oid, files)
            (root / ".git/objects" / oid[:2] / oid[2:]).unlink()
            self.assert_code("construction_git_read_failed", root, sha, files)
            self.assertEqual(before, {name: (root / name).read_bytes() for name in files})

    def test_replace_refs_do_not_substitute_other_code_for_committed_blobs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root)
            (root / "src/adapter.py").write_text("replacement_code = True\n"); commit(root, "replacement")
            replacement = git(root, "rev-parse", "HEAD")
            git(root, "replace", sha, replacement)
            validate_construction_code_identity(root, sha, files)
            changed = dict(files); changed["src/adapter.py"] = hashlib.sha256((root / "src/adapter.py").read_bytes()).hexdigest()
            self.assert_code("construction_processing_blob_hash_mismatch", root, sha, changed)

    def test_sha256_git_repository_requires_full_commit_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"; sha, files = fixture(root, object_format="sha256")
            validate_construction_code_identity(root, sha, files)
            self.assert_code("construction_invalid_commit", root, sha[:40], files)

    def test_versioned_packaged_identity_is_recomputed_without_git(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "src/dagster_hifld"
            package.mkdir(parents=True)
            (package / "runtime.py").write_bytes(b"runtime bytes\n")
            (root / "pyproject.toml").write_bytes(b"project metadata\n")
            (root / "uv.lock").write_bytes(b"locked dependencies\n")
            files = {
                path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted([*package.rglob("*"), root / "pyproject.toml", root / "uv.lock"])
                if path.is_file()
            }
            distributions = [
                {"name": distribution.metadata["Name"], "version": distribution.version}
                for distribution in importlib.metadata.distributions()
                if distribution.metadata.get("Name") and distribution.version
            ]
            distributions.sort(key=lambda item: (item["name"].casefold(), item["name"], item["version"]))
            identity = {
                "code_identity_scheme": "sha256-packaged-dagster-hifld-v1",
                "working_tree_status": "unknown", "files": files,
                "installed_distributions": distributions,
                "python": "3.12.0", "pyarrow": "1.0", "proj": "1.0", "geos": "1.0",
            }
            fields = ("code_identity_scheme", "working_tree_status", "files", "installed_distributions",
                      "python", "pyarrow", "proj", "geos")
            body = json.dumps({key: identity[key] for key in fields}, sort_keys=True,
                              separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
            code_commit = "packaged-sha256:" + hashlib.sha256(body).hexdigest()
            with patch("dagster_hifld.construction_provenance._packaged_runtime_versions", return_value={
                    "python": identity["python"], "pyarrow": identity["pyarrow"],
                    "proj": identity["proj"], "geos": identity["geos"],
            }):
                validate_construction_code_identity(root, code_commit, files, identity=identity)
            with patch("dagster_hifld.construction_provenance._packaged_runtime_versions", return_value={
                    "python": "changed", "pyarrow": identity["pyarrow"],
                    "proj": identity["proj"], "geos": identity["geos"],
            }), self.assertRaisesRegex(ConstructionCodeIdentityError, "construction_packaged_runtime_versions_changed"):
                validate_construction_code_identity(root, code_commit, files, identity=identity)
            self.assert_code("construction_invalid_commit", root, code_commit, files)

            (package / "runtime.py").write_bytes(b"changed runtime bytes\n")
            with patch("dagster_hifld.construction_provenance._packaged_runtime_versions", return_value={
                    "python": identity["python"], "pyarrow": identity["pyarrow"],
                    "proj": identity["proj"], "geos": identity["geos"],
            }), self.assertRaisesRegex(ConstructionCodeIdentityError, "construction_packaged_file_hash_mismatch"):
                validate_construction_code_identity(root, code_commit, files, identity=identity)
