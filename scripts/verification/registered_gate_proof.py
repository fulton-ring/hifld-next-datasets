#!/usr/bin/env python3
"""Prove the registered publication gates against an isolated candidate copy.

This is a local evidence harness, not an acceptance or publication tool. It runs
the registered quality check, verifier, and promotion asset selection against
two copies of an already-cataloged candidate. A synthetic populated destination
is snapshotted before and after both runs and must remain byte-for-byte intact.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Any

from dagster import AssetSelection, DagsterInstance, define_asset_job

from dagster_hifld.assets.publish import publish_version, verify_release
from dagster_hifld.checks import publish_catalog_quality_manifest
from dagster_hifld.definitions import defs as registered_defs
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.portolan.release import read_staged_release
from dagster_hifld.release_approval import verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def inventory(root: Path) -> dict[str, dict[str, int | str]]:
    return {
        path.relative_to(root).as_posix(): {
            "bytes": path.stat().st_size,
            "sha256": file_sha256(path),
        }
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def inventory_sha256(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def validate_run_root(run_root: Path, candidate: Path) -> Path:
    """Require a new or empty root disjoint from the input candidate."""
    root = run_root.expanduser().resolve()
    source = candidate.expanduser().resolve()
    if root == source or root.is_relative_to(source) or source.is_relative_to(root):
        raise ValueError("run root and candidate must be separate, non-overlapping paths")
    if root.exists():
        if not root.is_dir() or any(root.iterdir()):
            raise ValueError(f"run root must be empty; refusing existing contents: {root}")
    else:
        root.mkdir(parents=True)
    return root


def validate_prefix(value: str) -> tuple[str, str, str]:
    prefix = PurePosixPath(value)
    if prefix.is_absolute() or len(prefix.parts) != 3 or any(p in {".", ".."} for p in prefix.parts):
        raise ValueError("staging prefix must be a relative dataset/file/version path")
    return tuple(prefix.parts)  # type: ignore[return-value]


def create_synthetic_published_fixture(root: Path) -> dict[str, dict[str, int | str]]:
    """Create an unmistakably synthetic, nonempty local PublishedStorage tree."""
    objects = {
        "SYNTHETIC_FIXTURE_ONLY/README.txt": (
            "Synthetic gate-test object; not an accepted HIFLD release.\n"
        ).encode(),
        "synthetic-baseline/metadata/catalog.json": json.dumps(
            {"fixture": "synthetic", "accepted": False}, sort_keys=True
        ).encode() + b"\n",
        "synthetic-baseline/data/object.bin": b"synthetic-prior-release-bytes\n",
    }
    for key, payload in objects.items():
        target = root / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
    snapshot = inventory(root)
    if not snapshot:
        raise AssertionError("synthetic PublishedStorage fixture must be nonempty")
    return snapshot


def event_record(event) -> dict[str, Any]:
    kind = event.event_type_value
    record: dict[str, Any] = {"event": kind, "step_key": event.step_key}
    if kind == "ASSET_CHECK_EVALUATION":
        data = event.event_specific_data
        spec = getattr(data, "asset_check_evaluation", data)
        record.update(
            check_asset_key=spec.asset_key.to_user_string(),
            check_name=spec.check_name,
            passed=spec.passed,
            metadata={str(key): str(value) for key, value in spec.metadata.items()},
        )
    if kind == "STEP_FAILURE":
        error = event.event_specific_data.error
        record["error"] = error.to_string() if error else "unknown step failure"
    return record


def step_started(case: dict[str, Any], suffix: str) -> bool:
    return any(step == suffix or step.endswith("__" + suffix) for step in case["step_starts"])


def validate_gate_selection(selected_assets: list[str]) -> None:
    """Keep this proof scoped to the explicitly selected gate assets."""
    expected = {"publish/verify", "publish/promote"}
    if set(selected_assets) != expected:
        raise AssertionError(
            "registered gate proof must select only verify and promote; "
            f"got {sorted(selected_assets)}"
        )


def is_exact_approval_failure(event: dict[str, Any]) -> bool:
    verify_step = event["step_key"] == "verify" or event["step_key"].endswith("__verify")
    return verify_step and "missing_or_ambiguous_exact_object_approval" in event.get("error", "")


def harness_checkout_head() -> str:
    """Return the checkout containing this script, never the caller's cwd head."""
    script_directory = Path(__file__).resolve().parent
    try:
        return subprocess.check_output(
            ["git", "-C", str(script_directory), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"


def run_gate_case(
    *,
    name: str,
    case_root: Path,
    prefix: str,
    published_dir: Path,
    approvals_path: Path | None,
    include_quality_check: bool = True,
) -> dict[str, Any]:
    staging_dir = case_root / "staging"
    candidate_dir = staging_dir.joinpath(*prefix.split("/"))
    home = case_root / "dagster-home"
    home.mkdir(parents=True)
    (home / "dagster.yaml").write_text("telemetry:\n  enabled: false\n", encoding="utf-8")

    os.environ["DAGSTER_HOME"] = str(home)
    os.environ.pop("HIFLD_STAGING_BUCKET", None)
    os.environ.pop("HIFLD_DATASETS_BUCKET", None)
    if approvals_path is None:
        os.environ.pop("HIFLD_RELEASE_APPROVALS", None)
    else:
        os.environ["HIFLD_RELEASE_APPROVALS"] = str(approvals_path)

    staging = StagingStorageResource(
        use_local=True, local_dir=str(staging_dir), prefix="", backend="local"
    )
    published = PublishedStorageResource(
        use_local=True, local_dir=str(published_dir), prefix="", backend="local"
    )
    partition = build_publish_partition_key(*prefix.split("/"))
    selection = AssetSelection.assets(verify_release, publish_version)
    if include_quality_check:
        selection = selection | AssetSelection.checks(publish_catalog_quality_manifest)
    job = define_asset_job(
        f"registered_gate_proof_{name.replace('-', '_')}",
        selection=selection,
    )
    harness_defs = type(registered_defs)(
        assets=registered_defs.assets,
        asset_checks=registered_defs.asset_checks,
        jobs=[job],
        resources={"staging_storage": staging, "published_storage": published},
    )
    job_def = harness_defs.resolve_job_def(job.name)
    selected_assets = sorted(
        key.to_user_string() for key in job_def.asset_layer.selected_asset_keys
    )
    validate_gate_selection(selected_assets)
    with DagsterInstance.from_config(str(home)) as instance:
        instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
        result = job_def.execute_in_process(
            instance=instance,
            partition_key=partition,
            raise_on_error=False,
        )
        events = [event_record(event) for event in result.all_events]

    check_events = [
        event for event in events
        if event["event"] == "ASSET_CHECK_EVALUATION"
        and event.get("check_name") == "quality_manifest"
    ]
    return {
        "case": name,
        "run_id": result.run_id,
        "run_success": result.success,
        "partition_key": partition,
        "candidate_copy": str(candidate_dir),
        "dagster_home": str(home),
        "persistent_run_database": str(home / "history" / "runs.db"),
        "selected_assets": selected_assets,
        "quality_check_evaluations": check_events,
        "step_starts": sorted(
            event["step_key"] for event in events if event["event"] == "STEP_START"
        ),
        "step_failures": [
            event for event in events if event["event"] == "STEP_FAILURE"
        ],
        "events": events,
    }


def copy_candidate(source: Path, case_root: Path, prefix: str) -> Path:
    destination = case_root / "staging" / Path(*prefix.split("/"))
    destination.parent.mkdir(parents=True)
    # Keep proof mutations isolated from canonical source objects. Hard links
    # change source inode link counts and invalidate the staging custody guard.
    shutil.copytree(source, destination, copy_function=shutil.copy2)
    return destination


def ensure_release_metadata_prerequisite(candidate: Path, prefix: str) -> dict[str, Any]:
    """Add a synthetic semver Collection to the isolated approval copy only.

    Current verification requires a staged release Collection before it can
    reach exact-object approval. This proof fixture does not materialize that
    asset or represent source, release, or acceptance evidence.
    """
    dataset, file_slug, _ = validate_prefix(prefix)
    collection_path = candidate / "collection.json"
    kind = "existing_candidate_collection"
    if not collection_path.exists():
        version = "0.0.0-gate-proof"
        document = {
            "type": "Collection",
            "stac_version": "1.1.0",
            "stac_extensions": [
                "https://stac-extensions.github.io/version/v1.2.0/schema.json"
            ],
            "id": f"hifld/{dataset}/{file_slug}/{version}",
            "version": version,
            "description": (
                "Synthetic registered-gate prerequisite only; no source claims, "
                "release acceptance, or publishable catalog."
            ),
            "license": "other",
            "links": [],
            "assets": {},
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        }
        payload = json.dumps(document, sort_keys=True, indent=2).encode() + b"\n"
        with collection_path.open("xb") as stream:
            stream.write(payload)
        kind = "synthetic_gate_only"

    # Use the production parser for the exact prerequisite, not an ad hoc
    # approximation of its semver/path validation.
    staging_root = candidate.parents[2]
    storage = StagingStorageResource(
        use_local=True, local_dir=str(staging_root), prefix="", backend="local"
    )
    document, destination_prefix = read_staged_release(storage, prefix)
    return {
        "kind": kind,
        "path": str(collection_path),
        "sha256": file_sha256(collection_path),
        "version": document["version"],
        "id": document["id"],
        "destination_prefix": destination_prefix,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True, help="already-cataloged candidate version directory")
    parser.add_argument("--staging-prefix", required=True, help="dataset/file/version relative staging key")
    parser.add_argument("--run-root", type=Path, required=True, help="new or empty isolated evidence directory")
    args = parser.parse_args(argv)

    candidate = args.candidate.expanduser().resolve()
    prefix_parts = validate_prefix(args.staging_prefix)
    prefix = "/".join(prefix_parts)
    if not candidate.is_dir():
        raise ValueError(f"candidate directory does not exist: {candidate}")
    if candidate.name != prefix_parts[2]:
        raise ValueError("candidate directory name must equal the version in --staging-prefix")
    quality_file = candidate / "metadata" / "quality_manifest.json"
    source_manifest = candidate / "metadata" / "source_manifest.json"
    if not quality_file.is_file() or not source_manifest.is_file():
        raise ValueError("candidate must contain metadata/quality_manifest.json and source_manifest.json")

    run_root = validate_run_root(args.run_root, candidate)
    original_candidate_inventory = inventory(candidate)
    if not original_candidate_inventory:
        raise ValueError("candidate contains no files")
    quality = json.loads(quality_file.read_text(encoding="utf-8"))
    if quality.get("quality_check_passed") is not False:
        raise ValueError("candidate must retain its actual failed quality manifest")

    source_manifest_data = json.loads(source_manifest.read_text(encoding="utf-8"))
    failed_root = run_root / "quality-failed"
    approval_root = run_root / "approval-refused"
    failed_candidate = copy_candidate(candidate, failed_root, prefix)
    failed_quality_path = failed_candidate / "metadata" / "quality_manifest.json"
    failed_quality = json.loads(failed_quality_path.read_text(encoding="utf-8"))
    if failed_quality.get("quality_check_passed") is not False:
        raise ValueError("source candidate must retain its actual failed quality manifest")
    failed_inventory = inventory(failed_candidate)
    changed = sorted(
        relative for relative in set(original_candidate_inventory) | set(failed_inventory)
        if original_candidate_inventory.get(relative) != failed_inventory.get(relative)
    )
    if changed:
        raise AssertionError(f"quality-failed case must preserve candidate exactly: {changed}")

    approval_candidate = copy_candidate(candidate, approval_root, prefix)
    copied_inventory = inventory(approval_candidate)
    if copied_inventory != original_candidate_inventory:
        raise AssertionError("approval case candidate copy differs from the supplied candidate")
    release_metadata = ensure_release_metadata_prerequisite(approval_candidate, prefix)
    approval_prepared_inventory = inventory(approval_candidate)
    changed_for_release_metadata = sorted(
        relative for relative in set(copied_inventory) | set(approval_prepared_inventory)
        if copied_inventory.get(relative) != approval_prepared_inventory.get(relative)
    )
    expected_metadata_change = (
        ["collection.json"] if release_metadata["kind"] == "synthetic_gate_only" else []
    )
    if changed_for_release_metadata != expected_metadata_change:
        raise AssertionError(
            "approval case may add only its isolated synthetic release Collection: "
            f"{changed_for_release_metadata}"
        )

    published_dir = run_root / "synthetic-published-storage"
    published_before = create_synthetic_published_fixture(published_dir)
    approvals_path = run_root / "empty-approval-registry.json"
    approvals_path.write_text(
        json.dumps({"schema_version": 1, "releases": []}, indent=2) + "\n",
        encoding="utf-8",
    )

    quality_case = run_gate_case(
        name="quality-failed",
        case_root=failed_root,
        prefix=prefix,
        published_dir=published_dir,
        approvals_path=None,
    )
    quality_checks = quality_case["quality_check_evaluations"]
    if len(quality_checks) != 1 or quality_checks[0]["passed"] is not False:
        raise AssertionError(f"registered quality check did not fail: {quality_checks}")
    if step_started(quality_case, "verify") or step_started(quality_case, "promote"):
        raise AssertionError("verify or promotion started after failed quality check")
    if quality_case["run_success"]:
        raise AssertionError("false-quality Dagster run unexpectedly succeeded")
    published_after_quality = inventory(published_dir)
    if published_after_quality != published_before:
        raise AssertionError("synthetic published inventory changed during false-quality run")

    approval_case = run_gate_case(
        name="approval-refused",
        case_root=approval_root,
        prefix=prefix,
        published_dir=published_dir,
        approvals_path=approvals_path,
        include_quality_check=False,
    )
    approval_checks = approval_case["quality_check_evaluations"]
    if approval_checks:
        raise AssertionError(f"approval-only graph unexpectedly selected quality check: {approval_checks}")
    if "publish/catalog" in approval_case["selected_assets"]:
        raise AssertionError("approval-only graph unexpectedly selected the catalog asset")
    if not step_started(approval_case, "verify"):
        raise AssertionError("registered verify step did not start for empty approval registry")
    if step_started(approval_case, "promote"):
        raise AssertionError("promotion started despite empty approval registry")
    verify_failures = [
        event for event in approval_case["step_failures"]
        if is_exact_approval_failure(event)
    ]
    if len(verify_failures) != 1:
        raise AssertionError(f"verify did not fail specifically on absent exact approval: {verify_failures}")
    if approval_case["run_success"]:
        raise AssertionError("empty-registry Dagster run unexpectedly succeeded")
    published_after_approval = inventory(published_dir)
    if published_after_approval != published_before:
        raise AssertionError("synthetic published inventory changed during empty-registry run")

    source_after = inventory(candidate)
    if source_after != original_candidate_inventory:
        raise AssertionError("input candidate was modified")
    result = {
        "purpose": "normal failed-quality refusal and separate approval-only refusal with quality checks omitted; no acceptance or production publication",
        "harness_checkout_head": harness_checkout_head(),
        "harness_script_sha256": file_sha256(Path(__file__).resolve()),
        "registered_pipeline_implementation_sha256": verifier_implementation_sha256(),
        "code_identity_scheme": source_manifest_data.get("code_identity_scheme"),
        "candidate": str(candidate),
        "staging_prefix": prefix,
        "candidate_inventory_sha256": inventory_sha256(original_candidate_inventory),
        "candidate_copy_inventory_sha256": inventory_sha256(copied_inventory),
        "approval_case_inventory_sha256": inventory_sha256(approval_prepared_inventory),
        "approval_case_release_metadata": release_metadata,
        "approval_case_release_metadata_changed_files": changed_for_release_metadata,
        "candidate_source_manifest_sha256": file_sha256(source_manifest),
        "candidate_source_code_commit": source_manifest_data.get("code_commit"),
        "quality_failed_candidate_changed_files": changed,
        "quality_failed_run": quality_case,
        "empty_approval_run": approval_case,
        "empty_approval_registry": str(approvals_path),
        "synthetic_published_fixture_object_count": len(published_before),
        "synthetic_published_fixture_inventory_before_sha256": inventory_sha256(published_before),
        "synthetic_published_fixture_inventory_after_quality_sha256": inventory_sha256(published_after_quality),
        "synthetic_published_fixture_inventory_after_approval_sha256": inventory_sha256(published_after_approval),
        "published_fixture_unchanged": published_before == published_after_quality == published_after_approval,
        "candidate_unchanged": source_after == original_candidate_inventory,
    }
    output = run_root / "registered-gate-proof.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "proof": str(output),
        "harness_checkout_head": result["harness_checkout_head"],
        "harness_script_sha256": result["harness_script_sha256"],
        "quality_failed_run_id": quality_case["run_id"],
        "empty_approval_run_id": approval_case["run_id"],
        "published_fixture_unchanged": result["published_fixture_unchanged"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, AssertionError, OSError, json.JSONDecodeError) as exc:
        print(f"registered gate proof refused/failed: {exc}", file=sys.stderr)
        raise SystemExit(2)
