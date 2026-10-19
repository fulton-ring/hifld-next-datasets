"""Reproduce read-only construction-code identity checks, without release approval."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

from dagster_hifld.construction_provenance import (
    ConstructionCodeIdentityError,
    validate_construction_code_identity,
)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args()
    root = args.repo_root.resolve(strict=True)
    work = args.work_dir.absolute()
    work.mkdir(parents=True, exist_ok=False)
    staged = work / "staged" / "original-source.body"
    published = work / "published" / "prior-release.body"
    for path, value in ((staged, b"preserved original source\n"),
                        (published, b"preserved prior release\n")):
        path.parent.mkdir()
        path.write_bytes(value)
    before = {str(path.relative_to(work)): digest(path) for path in (staged, published)}
    files = {str(path.relative_to(root)): digest(path)
             for path in sorted((root / "src" / "dagster_hifld").rglob("*.py"))}
    files.update({name: digest(root / name) for name in ("pyproject.toml", "uv.lock")})
    commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    validate_construction_code_identity(root, commit, files)
    failures = {}
    for name, claimed_commit, claimed_files in (
        ("zero_commit", "0" * len(commit), files),
        ("wrong_source_blob", commit, {**files, next(iter(files)): "0" * 64}),
        ("wrong_lock_blob", commit, {**files, "uv.lock": "0" * 64}),
        ("wrong_project_blob", commit, {**files, "pyproject.toml": "0" * 64}),
    ):
        try:
            validate_construction_code_identity(root, claimed_commit, claimed_files)
        except ConstructionCodeIdentityError as exc:
            failures[name] = exc.code
        else:
            raise AssertionError(f"{name} unexpectedly accepted")
    fixture = work / "git-fixture"
    fixture.mkdir()

    def git(*arguments: str) -> str:
        return subprocess.check_output(["git", "-C", str(fixture), *arguments],
                                       stderr=subprocess.DEVNULL, text=True).strip()

    git("init", "-q")
    git("config", "user.name", "Synthetic Construction Proof")
    git("config", "user.email", "synthetic@example.invalid")
    code = fixture / "source.py"
    code.write_bytes(b"value = 1\n")
    fixture_files = {"source.py": digest(code)}
    git("add", "source.py")
    git("commit", "-qm", "Original synthetic processing source")
    original_commit = git("rev-parse", "HEAD")
    (fixture / "notes.md").write_text("Later documentation only\n")
    git("add", "notes.md")
    git("commit", "-qm", "Synthetic later documentation")
    validate_construction_code_identity(fixture, original_commit, fixture_files)
    validate_construction_code_identity(fixture, git("rev-parse", "HEAD"), fixture_files)
    code.write_bytes(b"value = 2\n")
    git("add", "source.py")
    git("commit", "-qm", "Synthetic changed processing source")
    try:
        validate_construction_code_identity(fixture, git("rev-parse", "HEAD"), fixture_files)
    except ConstructionCodeIdentityError as exc:
        failures["changed_source_commit"] = exc.code
    else:
        raise AssertionError("changed source commit unexpectedly accepted")
    validate_construction_code_identity(fixture, original_commit, fixture_files)
    assert before == {str(path.relative_to(work)): digest(path) for path in (staged, published)}
    result = {
        "classification": "read-only actual Git/content proof; no dataset acceptance or release",
        "repo_root": str(root), "code_commit": commit, "processing_file_count": len(files),
        "processing_files": files, "all_recorded_blobs_verified": True,
        "historical_docs_only_commit_valid": original_commit,
        "historical_commit_valid_after_source_changed": True,
        "negative_cases": failures, "staged_and_published_bytes_preserved": before,
        "limits": "Callers must independently bind original identity and complete file roster to preserved capture evidence; this does not prove execution time or authority.",
    }
    (work / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "processing_files"}, indent=2))


if __name__ == "__main__":
    main()
