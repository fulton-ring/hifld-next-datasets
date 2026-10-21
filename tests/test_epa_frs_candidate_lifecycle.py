"""Focused regression for the EPA FRS candidate version/prefix/cache boundary."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DagsterInstance, materialize

from dagster_hifld.assets.epa_power_generation import (
    epa_frs_eia_860_power_generation_facilities_by_source,
)
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.construction_provenance import (
    ConstructionCodeIdentityError,
    validate_construction_code_identity,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    content_derived_version,
    processing_identity,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)


class EpaFrsCandidateLifecycleTests(unittest.TestCase):
    def test_packaged_identity_needs_no_git_and_revalidates_installed_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            package = root / "src/dagster_hifld"
            package.mkdir(parents=True)
            (package / "source.py").write_text("candidate transform = 1\n")
            (root / "pyproject.toml").write_text("[project]\nname = 'fixture'\n")
            (root / "uv.lock").write_text("version = 1\n")
            self.assertFalse((root / ".git").exists())

            with patch("subprocess.check_output", side_effect=FileNotFoundError("git unavailable")), \
                 patch("subprocess.run", side_effect=FileNotFoundError("git unavailable")):
                identity = processing_identity(root)
                self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
                self.assertEqual(identity["working_tree_status"], "unknown")
                self.assertRegex(identity["code_commit"], r"^packaged-sha256:[0-9a-f]{64}$")
                validate_construction_code_identity(
                    root, identity["code_commit"], identity["files"], identity=identity,
                )

            (package / "source.py").write_text("mutated transform = 2\n")
            with self.assertRaisesRegex(ConstructionCodeIdentityError, "construction_packaged_file_hash_mismatch"):
                validate_construction_code_identity(
                    root, identity["code_commit"], identity["files"], identity=identity,
                )

    def test_content_version_prefix_seal_reuse_and_registered_partition_agree(self):
        repo = Path(__file__).resolve().parents[1]
        identity = processing_identity(repo)
        dataset = "epa-frs-eia-860-power-generation-facilities-by-source"
        file_slug = dataset
        record_sha = hashlib.sha256(b"synthetic complete record population").hexdigest()
        geometry_sha = hashlib.sha256(b"synthetic native point geometry").hexdigest()
        schema_sha = hashlib.sha256(b"synthetic pinned field profile").hexdigest()
        version = content_derived_version(record_sha, geometry_sha, schema_sha, identity)
        prefix = candidate_prefix(dataset, file_slug, version)
        state_key = f"_construction-state/{dataset}/{version}.json"
        fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            raw = b"synthetic native capture bytes"
            wire = b"synthetic wire bytes"
            receipt = {
                "complete_body": True, "status": 200,
                "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest(),
            }
            objects = {
                "geoparquet/native.parquet": b"synthetic parquet candidate",
                "metadata/candidate_ready.json": json.dumps({"approved": False, "version": version}).encode(),
                "metadata/source_manifest.json": b'{"approved":false,"fixture":true}',
                "raw/source.body": raw,
                "raw/source.wire": wire,
                "raw/source.receipt.json": json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode() + b"\n",
            }
            descriptors = {}
            for name, body in objects.items():
                write_immutable(storage, f"{prefix}/{name}", body)
                descriptors[name] = {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            payload = {
                "processing_identity": identity,
                "source_record_sha256": record_sha,
                "source_schema_sha256": schema_sha,
                "capture_receipts": {"source": receipt},
                "objects": descriptors,
            }
            seal_ready_candidate(storage, state_key, version, fingerprint, payload)
            reused = verify_cached_candidate(
                storage, repo, state_key, prefix, version, fingerprint, identity,
                record_sha, schema_sha, descriptors, {"source": receipt},
            )
            self.assertEqual(reused, {"version": version, "reused": True})
            self.assertEqual(set(storage.list_prefix(prefix)), {f"{prefix}/{name}" for name in objects})

            # A completed catalog must not invalidate an intact source cache.
            manifest = objects["metadata/source_manifest.json"]
            derived = {
                "metadata/quality_manifest.json": b'{"quality_check_passed":false}',
                "metadata/data_dictionary.json": b'{"fixture":true}',
                "metadata/resolved_provenance.json": b'{"fixture":true}',
                "metadata/source/source_manifest.json": manifest,
                "metadata/source/version_" + hashlib.sha256(manifest).hexdigest() + "_source_manifest.json": manifest,
            }
            for name, body in derived.items():
                write_immutable(storage, f"{prefix}/{name}", body)
            def reuse():
                return verify_cached_candidate(storage, repo, state_key, prefix, version,
                    fingerprint, identity, record_sha, schema_sha, descriptors, {"source": receipt})
            self.assertTrue(reuse()["reused"])
            rogue = Path(tmp) / prefix / "raw/unsealed.body"
            rogue.write_bytes(b"unsealed")
            with self.assertRaisesRegex(ValueError, "storage_roster_changed"):
                reuse()
            rogue.unlink()
            snapshot_path = Path(tmp) / prefix / "metadata/source/source_manifest.json"
            snapshot_path.write_bytes(b"changed snapshot")
            with self.assertRaisesRegex(ValueError, "catalog_source_snapshot_changed"):
                reuse()
            snapshot_path.write_bytes(manifest)
            source_path = Path(tmp) / prefix / "raw/source.body"
            source_path.write_bytes(b"mutated source")
            with self.assertRaisesRegex(ValueError, "object_integrity_failed"):
                reuse()
            source_path.unlink()
            with self.assertRaisesRegex(ValueError, "storage_roster_changed"):
                reuse()
            source_path.write_bytes(raw)
            self.assertTrue(reuse()["reused"])

            instance_dir = Path(tmp) / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            fake = {
                "version": version, "feature_count": 2328,
                "source_null_geometry_count": 0,
                "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            }
            with patch(
                "dagster_hifld.assets.epa_power_generation.capture_epa_power_generation",
                return_value=fake,
            ), DagsterInstance.from_config(str(instance_dir)) as instance:
                result = materialize(
                    [epa_frs_eia_860_power_generation_facilities_by_source],
                    resources={"staging_storage": storage}, instance=instance,
                )
                partition = build_publish_partition_key(dataset, file_slug, version)
                self.assertTrue(result.success)
                self.assertIn(partition, instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))
                self.assertEqual(prefix.rsplit("/", 1)[-1], partition.rsplit("/", 1)[-1])


if __name__ == "__main__":
    unittest.main()
