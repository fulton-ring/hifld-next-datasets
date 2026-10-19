import copy
import json
import os
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

from dagster import DagsterInstance, Definitions, define_asset_job

from dagster_hifld.assets.publish import _copy_version_files, publish_version, verify_release
from dagster_hifld.checks import hifld_asset_checks
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.promotion import promote_immutable_objects, write_immutable_object
from dagster_hifld.release_approval import ReleaseApprovalError, _storage_identity, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import _merge_manifest_layers
from dagster_hifld.validation import ComparisonLimits, compare_full_rows, sha256_file


CONTRACT = {
    "id_fields": ["id"], "coverage_fields": ["state"], "geometry": True, "crs": "EPSG:4326",
    "columns": {"id": {"type": "string", "nullable": False},
                "state": {"type": "string", "nullable": False},
                "name": {"type": "string", "nullable": True}},
}
BASELINE = {"dataset_slug": "synthetic", "file_slug": "points", "version": "accepted-fixture",
            "catalog_url": "https://example.invalid/catalog.json", "catalog_sha256": "0" * 64,
            "data_objects": [{"url": "https://example.invalid/data", "sha256": "0" * 64, "bytes": 0}],
            "mapping_evidence": "SYNTHETIC ONLY; no dataset approval",
            "decision": {"id": "SYNTHETIC-BASELINE", "approved_by": "test-fixture", "approved_at": "2026-10-19"}}


def feature(key="001", state="PA", name="A", x=1):
    return {"type": "Feature", "properties": {"id": key, "state": state, "name": name},
            "geometry": {"type": "Point", "coordinates": [x, 2]}}


def stream(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def baseline_reference(path):
    value = copy.deepcopy(BASELINE)
    digest = sha256_file(path)
    value["data_objects"][0].update(sha256=digest, bytes=path.stat().st_size)
    value["comparison_input"] = {"adapter": "typed-geojson-feature-jsonl-v1", "sha256": digest, "bytes": path.stat().st_size,
                                 "derivation": {"source_sha256": [digest], "code_sha256": "identity-jsonl-fixture",
                                                "parameters": {"mode": "identity"}}}
    return value


def comparison(root, left, right, contract=None, limits=ComparisonLimits()):
    baseline, candidate = root / "baseline.jsonl", root / "candidate.jsonl"
    stream(baseline, left)
    stream(candidate, right)
    return compare_full_rows(baseline, candidate, contract or CONTRACT, root / "metadata",
                             baseline_sha256=sha256_file(baseline), candidate_sha256=sha256_file(candidate),
                             baseline_reference=baseline_reference(baseline), limits=limits)


def accepted_fixture(root):
    """Explicit synthetic acceptance owned by the test, outside storage roots."""
    source = StagingStorageResource(local_dir=str(root / "staging"), use_local=True)
    target = PublishedStorageResource(local_dir=str(root / "published"), use_local=True)
    prefix = "synthetic/points/vtest"
    version_dir = root / "staging" / prefix
    version_dir.mkdir(parents=True)
    stream(root / "baseline.jsonl", [feature()])
    stream(version_dir / "metadata/candidate.records.jsonl", [feature()])
    candidate = version_dir / "metadata/candidate.records.jsonl"
    report = compare_full_rows(root / "baseline.jsonl", candidate, CONTRACT, version_dir / "metadata",
                               baseline_sha256=sha256_file(root / "baseline.jsonl"), candidate_sha256=sha256_file(candidate),
                               baseline_reference=baseline_reference(root / "baseline.jsonl"))
    source.write("synthetic", "points", "vtest", "geojson/source.geojson",
                 json.dumps({"type": "FeatureCollection", "features": [feature()]}).encode())
    manifest = {"schema_version": 1, "processor": "HIFLD Next", "code_commit": "synthetic-only",
                "transformations": [], "inputs": [{"input_id": "fixture", "producer": "Synthetic Agency",
                "host": "example.invalid", "role": "producer_publication", "canonical_url": "https://example.invalid/data",
                "retrieval_url": "https://example.invalid/data", "release_date_unknown_reason": "synthetic",
                "retrieved_at": "2026-10-19T00:00:00Z", "sha256": sha256_file(candidate), "bytes": candidate.stat().st_size,
                "license": "synthetic", "attribution": "Synthetic Agency"}]}
    for name, value in (("source_manifest.json", manifest), ("quality_manifest.json", {"quality_check_passed": True}),
                        ("data_dictionary.json", {"agency": "Synthetic Agency"})):
        source.write("synthetic", "points", "vtest", f"metadata/{name}", json.dumps(value).encode())
    objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "vtest")}
    entry = {"destination": _storage_identity(target), "staged_version_prefix": prefix,
             "implementation_sha256": verifier_implementation_sha256(),
             "decision": {"id": "SYNTHETIC-ONLY", "approved_by": "test-fixture", "approved_at": "2026-10-19"},
             "baseline": report["baseline"], "contract_sha256": report["contract_sha256"],
             "report_sha256": objects[f"{prefix}/metadata/baseline_comparison.json"],
             "provenance_sha256": objects[f"{prefix}/metadata/source_manifest.json"],
             "staged_objects": objects, "published_objects": objects,
             "candidate_comparison_input": {"key": f"{prefix}/metadata/candidate.records.jsonl",
                 "sha256": sha256_file(candidate), "bytes": candidate.stat().st_size, "adapter": report["adapter"],
                 "derivation": {"code_sha256": "synthetic-fixture-builder", "parameters": {"mode": "reviewed_feature_collection_to_jsonl"},
                                "source_objects": {f"{prefix}/geojson/source.geojson": objects[f"{prefix}/geojson/source.geojson"]}}}}
    registry = root / "approvals.json"
    registry.write_text(json.dumps({"schema_version": 1, "releases": [entry]}))
    return source, target, registry


class FullPopulationTests(unittest.TestCase):
    def test_input_mutation_during_comparison_is_blocked(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            baseline, candidate = root / "baseline.jsonl", root / "candidate.jsonl"
            stream(baseline, [feature()])
            stream(candidate, [feature()])
            original = sha256_file
            calls = 0
            def mutate(path):
                nonlocal calls
                calls += 1
                result = original(path)
                if calls == 2:
                    stream(candidate, [feature(name="changed during read")])
                return result
            with patch("dagster_hifld.validation.sha256_file", side_effect=mutate):
                report = compare_full_rows(baseline, candidate, CONTRACT, root / "out",
                                           baseline_sha256=original(baseline), candidate_sha256=original(candidate),
                                           baseline_reference=baseline_reference(baseline))
            self.assertEqual(report["status"], "blocked")

    def test_multi_input_provenance_merge_preserves_identity_and_rejects_conflicts(self):
        one, two = {"input_id": "a", "producer": "Agency A"}, {"input_id": "b", "producer": "Agency B"}
        merged = _merge_manifest_layers([("dataset", "a", {"inputs": [one]}), ("version", "b", {"inputs": [two]})])
        self.assertEqual(merged.metadata["inputs"], [one, two])
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            _merge_manifest_layers([("dataset", "a", {"inputs": [one]}),
                                    ("version", "b", {"inputs": [{**one, "producer": "Mirror"}]})])

    def test_top_level_source_identifier_cannot_be_silently_dropped(self):
        with tempfile.TemporaryDirectory() as d:
            report = comparison(Path(d), [{**feature(), "id": "producer-id"}], [feature()])
            self.assertEqual(report["status"], "fail")

    def test_equal_rows_distinct_ids_and_hidden_coverage_loss(self):
        with tempfile.TemporaryDirectory() as d:
            report = comparison(Path(d), [feature("001", "PA"), feature("002", "VA")],
                                [feature("001", "PA"), feature("003", "PA")])
            self.assertEqual(report["status"], "needs_decision")
            self.assertEqual(report["differences"]["added"], 1)
            self.assertEqual(report["differences"]["removed"], 1)
            self.assertEqual(report["differences"]["coverage_buckets"], 2)

    def test_complete_geometry_and_attributes_after_sample_limit(self):
        with tempfile.TemporaryDirectory() as d:
            left = [feature(str(i)) for i in range(1200)]
            right = copy.deepcopy(left)
            right[-1] = feature("1199", name="changed", x=8)
            report = comparison(Path(d), left, right)
            self.assertEqual(report["differences"]["attributes"], 1)
            self.assertEqual(report["differences"]["geometry"], 1)

    def test_schema_extra_column_null_and_duplicate_keys_fail(self):
        for rows in ([feature(None)], [feature(), feature()], [{**feature(), "properties": {**feature()["properties"], "extra": "x"}}]):
            with self.subTest(rows=rows), tempfile.TemporaryDirectory() as d:
                self.assertEqual(comparison(Path(d), [feature()], rows)["status"], "fail")

    def test_leading_zero_whitespace_and_integer_type_preserved(self):
        for key in ("1", "001 ", 1, 9007199254740993):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as d:
                report = comparison(Path(d), [feature()], [feature(key)])
                self.assertNotEqual(report["status"], "pass")

    def test_decimal_precision_is_not_rounded_to_float(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            left, right = root / "left", root / "right"
            text = json.dumps(feature()).replace('"name": "A"', '"name": 1.00000000000000000001')
            left.write_text(text + "\n")
            right.write_text(text.replace("00001", "00002") + "\n")
            contract = copy.deepcopy(CONTRACT)
            contract["columns"]["name"]["type"] = "decimal"
            report = compare_full_rows(left, right, contract, root / "out", baseline_sha256=sha256_file(left),
                                       candidate_sha256=sha256_file(right), baseline_reference=baseline_reference(left))
            self.assertEqual(report["differences"]["attributes"], 1)

    def test_resource_exhaustion_and_missing_baseline_are_durable_blockers(self):
        for limits in (ComparisonLimits(max_record_bytes=10), ComparisonLimits(max_scratch_bytes=1)):
            with tempfile.TemporaryDirectory() as d:
                root = Path(d)
                report = comparison(root, [feature()], [feature()], limits=limits)
                self.assertEqual(report["status"], "blocked")
                self.assertTrue((root / "metadata/baseline_comparison.json").exists())
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            report = compare_full_rows(root / "missing", root / "missing2", CONTRACT, root / "out",
                                       baseline_sha256="0" * 64, candidate_sha256="0" * 64, baseline_reference=BASELINE)
            self.assertEqual(report["status"], "blocked")


class ReleaseBoundaryTests(unittest.TestCase):
    def test_storage_resource_subclass_cannot_bypass_release_gate(self):
        with tempfile.TemporaryDirectory() as d, patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
            source, target, _ = accepted_fixture(Path(d))
            target_alias = StagingStorageResource(local_dir=target.local_dir, use_local=True)
            key = "synthetic/points/vtest/geojson/source.geojson"
            with self.assertRaises(ReleaseApprovalError):
                promote_immutable_objects(source, target_alias, [(key, key)])

    def test_approval_inside_staging_and_stale_code_decisions_block(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry = accepted_fixture(root)
            candidate_registry = root / "staging/forged-approval.json"
            candidate_registry.write_bytes(registry.read_bytes())
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(candidate_registry)}):
                with self.assertRaisesRegex(ReleaseApprovalError, "outside_candidate"):
                    _copy_version_files(source, target, "synthetic", "points", "vtest")
            value = json.loads(registry.read_text())
            value["releases"][0]["implementation_sha256"] = "stale"
            registry.write_text(json.dumps(value))
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaisesRegex(ReleaseApprovalError, "implementation_changed"):
                    _copy_version_files(source, target, "synthetic", "points", "vtest")

    def test_absent_registry_blocks_direct_local_and_low_level_writes(self):
        with tempfile.TemporaryDirectory() as d, patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
            source, target, _ = accepted_fixture(Path(d))
            key = "synthetic/points/vtest/geojson/source.geojson"
            for action in (lambda: _copy_version_files(source, target, "synthetic", "points", "vtest"),
                           lambda: promote_immutable_objects(source, target, [(key, key)]),
                           lambda: write_immutable_object(target, key, b"forged")):
                with self.assertRaises(ReleaseApprovalError):
                    action()
            self.assertEqual(target.list_prefix(), [])

    def test_exact_approval_then_new_shard_or_mutated_bytes_preserves_previous_release(self):
        with tempfile.TemporaryDirectory() as d:
            source, target, registry = accepted_fixture(Path(d))
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                _copy_version_files(source, target, "synthetic", "points", "vtest")
                before = {key: target.object_snapshot(key) for key in target.list_prefix()}
                source.write("synthetic", "points", "vtest", "geojson/added.geojson", b"new shard")
                with self.assertRaises(ReleaseApprovalError):
                    _copy_version_files(source, target, "synthetic", "points", "vtest")
                self.assertEqual(before, {key: target.object_snapshot(key) for key in target.list_prefix()})
                source.delete_prefix("synthetic/points/vtest/geojson/added.geojson")
                source.write("synthetic", "points", "vtest", "geojson/source.geojson", b"changed bytes")
                with self.assertRaisesRegex(ReleaseApprovalError, "bytes_changed"):
                    _copy_version_files(source, target, "synthetic", "points", "vtest")
                self.assertEqual(before, {key: target.object_snapshot(key) for key in target.list_prefix()})

    def test_dagster_synthetic_verify_promote_and_failed_promotion_preservation(self):
        preserved = os.environ.get("HIFLD_VERIFICATION_EVIDENCE_DIR")
        with (nullcontext(preserved) if preserved else tempfile.TemporaryDirectory()) as d:
            root = Path(d)
            source, target, registry = accepted_fixture(root)
            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            partition = "synthetic/points/vtest"
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry), "DAGSTER_HOME": str(instance_dir)}), DagsterInstance.get() as instance:
                instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
                resources = {"staging_storage": source, "published_storage": target}
                assets = [verify_release, publish_version]
                checks = [check for check in hifld_asset_checks if next(iter(check.check_specs)).asset_key.path == ["publish", "verify"]]
                defs = Definitions(assets=assets, asset_checks=checks, resources=resources,
                                   jobs=[define_asset_job("synthetic_verification")])
                job = defs.resolve_job_def("synthetic_verification")
                success = job.execute_in_process(instance=instance, partition_key=partition)
                self.assertTrue(success.success)
                before = {key: target.object_snapshot(key) for key in target.list_prefix()}
                source.write("synthetic", "points", "vtest", "geojson/source.geojson", b"unapproved drift")
                failure = job.execute_in_process(instance=instance, partition_key=partition, raise_on_error=False)
                self.assertFalse(failure.success)
                self.assertEqual(before, {key: target.object_snapshot(key) for key in target.list_prefix()})
                self.assertTrue(source.object_exists("synthetic/points/vtest/metadata/baseline_comparison.json"))
                (root / "integration_result.json").write_text(json.dumps({
                    "classification": "synthetic integration; no real acceptance", "success_run_id": success.run_id,
                    "failed_drift_run_id": failure.run_id, "success": success.success, "drift_success": failure.success,
                    "accepted_objects_unchanged": True, "candidate_evidence_preserved": True,
                    "published_object_count": len(before), "partition": partition,
                    "dagster_home": str(instance_dir),
                }, indent=2) + "\n")


if __name__ == "__main__":
    unittest.main()
