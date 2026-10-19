import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import prebuilt_output_keys, load_resolved_source_manifest


def policy():
    return {"prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
        "reason": "Synthetic native date32 must not be regenerated as text", "objects": [
            {"path": "geoparquet/native.parquet", "sha256": hashlib.sha256(b"pinned bytes").hexdigest(), "bytes": 12}]}}}}


class PrebuiltOutputTests(unittest.TestCase):
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
