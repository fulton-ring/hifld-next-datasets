import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.assets.publish import _copy_version_files, _write_and_publish_geoparquet
from dagster_hifld.source_manifest import snapshot_source_metadata
from test_full_verification import accepted_fixture

from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import prebuilt_output_keys, load_resolved_source_manifest


def policy():
    return {"prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
        "reason": "Synthetic native date32 must not be regenerated as text", "objects": [
            {"path": "geoparquet/native.parquet", "sha256": hashlib.sha256(b"pinned bytes").hexdigest(), "bytes": 12}]}}}}


class PrebuiltOutputTests(unittest.TestCase):
    def test_all_layer_bans_accumulate_and_empty_map_does_not_unban(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(use_local=True, local_dir=directory)
            storage.write_key("dataset/metadata/source_manifest.json", b'{"unsupported_output_formats":{"geoparquet":"Dataset whole ban"}}')
            storage.write_key("dataset/file/metadata/source_manifest.json", b'{"unsupported_output_formats":{"pmtiles":"File whole ban"}}')
            for value in [{}, {"unsupported_output_formats": {}}, {"unsupported_output_formats": {"shapefile_zip": "Version whole ban"}},
                          {"unsupported_output_formats": {"geoparquet": "Another explicit ban"}}]:
                storage.write_key("dataset/file/vdraft/metadata/source_manifest.json", json.dumps(value).encode())
                resolved = load_resolved_source_manifest(storage, "dataset", "file", "vdraft").metadata
                self.assertIn("geoparquet", resolved["unsupported_output_formats"])
                self.assertIn("pmtiles", resolved["unsupported_output_formats"])
            storage.write_key("dataset/file/vdraft/metadata/source_manifest.json", b'{"unsupported_output_formats":null}')
            with self.assertRaises(ValueError):
                load_resolved_source_manifest(storage, "dataset", "file", "vdraft")

    def test_cross_layer_ban_and_changed_prebuilt_policy_cannot_be_overridden(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(use_local=True, local_dir=directory)
            original = {"unsupported_output_formats": {"geoparquet": "Dataset whole ban"}}
            storage.write_key("dataset/metadata/source_manifest.json", json.dumps(original).encode())
            lower = {**policy(), "unsupported_output_formats": {"pmtiles": "Version whole ban"}}
            storage.write_key("dataset/file/vdraft/metadata/source_manifest.json", json.dumps(lower).encode())
            snapshot_source_metadata(storage, "dataset", "file", "vdraft")
            before = {key: storage.read_key(key) for key in storage.list_prefix()}
            with self.assertRaisesRegex(ValueError, "wholly banned"):
                _write_and_publish_geoparquet(storage, "dataset", "file", "vdraft")
            self.assertEqual(before, {key: storage.read_key(key) for key in storage.list_prefix()})
            self.assertTrue(any("dataset_" in key and "metadata/source/" in key for key in before))
            storage.write_key("dataset/metadata/source_manifest.json", json.dumps(policy()).encode())
            lower = copy.deepcopy(policy()); lower["prebuilt_output_policy"]["formats"]["geoparquet"]["reason"] = "Different lower-layer policy"
            storage.write_key("dataset/file/vdraft/metadata/source_manifest.json", json.dumps(lower).encode())
            with self.assertRaisesRegex(ValueError, "Conflicting inherited prebuilt"):
                load_resolved_source_manifest(storage, "dataset", "file", "vdraft")

    def test_exact_synthetic_approvals_do_not_erase_inherited_ban_at_writer_or_release(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            storage, target, registry = accepted_fixture(root)
            prefix = "synthetic/points/vtest"
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                _copy_version_files(storage, target, "synthetic", "points", "vtest")
                published_before = {key: target.object_snapshot(key) for key in target.list_prefix()}
                parent = b'{"unsupported_output_formats":{"geoparquet":"Inherited whole-format ban"}}'
                storage.write_key("synthetic/metadata/source_manifest.json", parent)
                manifest_key = prefix + "/metadata/source_manifest.json"
                manifest = json.loads(storage.read_key(manifest_key))
                manifest.update(policy(), unsupported_output_formats={"pmtiles": "Version visualization ban"})
                storage.write_key(manifest_key, json.dumps(manifest).encode())
                storage.write_key(prefix + "/geoparquet/native.parquet", b"pinned bytes")
                # Deliberately grant exact synthetic byte decisions only. The
                # inherited policy must remain a prerequisite, not be erased.
                approval = json.loads(registry.read_text()); entry = approval["releases"][0]
                objects = {key: storage.sha256_key(key) for key in storage.list_prefix(prefix)}
                entry.update(staged_objects=objects, published_objects=objects, provenance_sha256=objects[manifest_key])
                registry.write_text(json.dumps(approval))
                staged_before = {key: storage.read_key(key) for key in storage.list_prefix()}
                for call in [lambda: _write_and_publish_geoparquet(storage, "synthetic", "points", "vtest"),
                             lambda: _copy_version_files(storage, target, "synthetic", "points", "vtest")]:
                    with self.assertRaisesRegex(ValueError, "wholly banned"):
                        call()
                self.assertEqual(staged_before, {key: storage.read_key(key) for key in storage.list_prefix()})
                self.assertEqual(published_before, {key: target.object_snapshot(key) for key in target.list_prefix()})

    def test_policy_rejects_malformed_unqueried_entries_and_conflicting_bans(self):
        base = policy()
        mutations = [lambda p: p.update(prebuilt_output_policy=None),
                     lambda p: p["prebuilt_output_policy"].update(schema_version=True),
                     lambda p: p["prebuilt_output_policy"]["formats"].update(pmtiles={}),
                     lambda p: p["prebuilt_output_policy"]["formats"]["geoparquet"].update(reason=" "),
                     lambda p: p["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"][0].update(path="geoparquet/../native.parquet"),
                     lambda p: p["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"][0].update(bytes=True),
                     lambda p: p["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"].append(copy.deepcopy(p["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"][0])),
                     lambda p: p.update(unsupported_output_formats={"geoparquet": "Full format ban"})]
        for mutate in mutations:
            candidate = copy.deepcopy(base); mutate(candidate)
            with self.subTest(candidate=candidate), self.assertRaises(ValueError):
                validate_output_policy(candidate)

    def test_exact_roster_pins_missing_extra_and_modified_native_shards(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(use_local=True, local_dir=directory, prefix="physical-prefix")
            prefix = "dataset/file/vdraft"
            key = prefix + "/geoparquet/native.parquet"
            metadata = policy()
            storage.write_key("dataset/metadata/source_manifest.json", json.dumps(metadata).encode())
            storage.write_key(prefix + "/metadata/source_manifest.json", b'{"title":"Inherited policy fixture"}')
            metadata = load_resolved_source_manifest(storage, "dataset", "file", "vdraft").metadata
            self.assertEqual(metadata["prebuilt_output_policy"], policy()["prebuilt_output_policy"])
            with self.assertRaisesRegex(ValueError, "roster"):
                prebuilt_output_keys(metadata, storage, prefix, "geoparquet")
            storage.write_key(key, b"pinned bytes")
            self.assertEqual(prebuilt_output_keys(metadata, storage, prefix, "geoparquet"), ["physical-prefix/" + key])
            storage.write_key(prefix + "/geoparquet/extra.parquet", b"extra")
            with self.assertRaisesRegex(ValueError, "roster"):
                prebuilt_output_keys(metadata, storage, prefix, "geoparquet")
            (Path(directory) / "physical-prefix" / prefix / "geoparquet/extra.parquet").unlink()
            storage.write_key(key, b"altered data")
            with self.assertRaisesRegex(ValueError, "bytes changed"):
                prebuilt_output_keys(metadata, storage, prefix, "geoparquet")

    def test_actual_date32_registered_graph_keeps_all_gates_and_never_converts(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory) / "fixture"
            result = subprocess.run([sys.executable, "-m", "scripts.verification.prebuilt_date32_local", "--work-dir", str(work)],
                                    text=True, capture_output=True, timeout=180)
            self.assertEqual(result.returncode, 0, result.stderr[-18000:] + result.stdout[-3000:])
            evidence = json.loads((work / "prebuilt_result.json").read_text())
            self.assertEqual(evidence["generic_converter_calls"], 0)
            for name in ["date32_and_null_preserved", "failed_quality_blocked", "staged_mutation_blocked", "whole_format_ban_blocked"]:
                self.assertTrue(evidence[name])
