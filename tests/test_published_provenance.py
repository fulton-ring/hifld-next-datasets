import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from dagster_hifld.catalog import generate_data_dictionary, write_catalog_metadata
from dagster_hifld.assets.publish import _copy_version_files
from dagster_hifld.portolan.catalog import CatalogRecord, render_portolan_tree
from dagster_hifld.portolan.provenance import attach_published_provenance
from dagster_hifld.portolan.validation import PortolanValidationError, normalize_candidate_tree, validate_candidate_tree
from dagster_hifld.portolan.workflow import PortolanPublishRequest, prepare_portolan_release_bundle
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from test_full_verification import accepted_fixture
from test_provenance import provenance_fixture


class PublishedProvenanceTests(unittest.TestCase):
    def test_remote_dependency_guard_fails_before_any_cloud_operation(self):
        storage = PublishedStorageResource(use_local=False, bucket="synthetic-never-contacted")
        with self.assertRaisesRegex(RuntimeError, "local-only"):
            storage.write_key_with_dependencies_if_unchanged("_catalog/current.json", b"fixture", None, {})

    def test_actual_multi_input_catalog_publication_and_failed_provenance_preservation(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "roundtrip"
            result = subprocess.run([sys.executable, "-m", "scripts.verification.provenance_roundtrip_local", "--work-dir", str(root)],
                                    capture_output=True, text=True, timeout=180)
            self.assertEqual(result.returncode, 0, result.stderr[-15000:] + result.stdout[-3000:])
            outcome = json.loads((root / "roundtrip_result.json").read_text())
            self.assertEqual(outcome["source_input_count"], 2)
            self.assertEqual(outcome["metadata_assets_roundtripped"], outcome["sqlite_metadata_rows"])
            self.assertTrue(outcome["repinned_omitted_input_blocked"])
            self.assertTrue(outcome["removed_dependency_labels_blocked"])
            self.assertTrue(outcome["native_graph"]["invalid_provenance_blocked"])
            self.assertEqual(set(outcome["asset_dependency_negative_probes"]), {"wrong_metadata_href", "changed_data_after_plan", "sqlite_metadata_omission", "sqlite_object_bounds", "commit_boundary_dependency_race"})
            for result in outcome["asset_dependency_negative_probes"].values():
                self.assertTrue(result["blocked"])
                self.assertTrue(result["pointer_preserved"])
                self.assertTrue(result["plan_evidence_preserved"])
                self.assertEqual(result["prior_object_roster_preserved"], outcome["prior_published_objects_preserved"])
            self.assertTrue(outcome["actual_catalog_asset"]["exact_persisted_plan_unchanged"])
            self.assertTrue(outcome["actual_catalog_asset"]["legacy_surfaces_blocked"])

    def test_resolved_dataset_and_version_inputs_reach_dictionary_and_downloadable_manifest(self):
        with tempfile.TemporaryDirectory() as d:
            storage = StagingStorageResource(use_local=True, local_dir=d)
            parent = provenance_fixture()
            parent["transformations"] = [{"name": "upstream source preparation"}]
            version = copy.deepcopy(parent)
            version["inputs"][0].update(input_id="covariate", producer="Another Synthetic Agency")
            version.update(transformations=[], code_commit="downstream-fixture-code")
            storage.write_key("dataset/metadata/source_manifest.json", json.dumps(parent).encode())
            storage.write_key("dataset/file/v1.0.0/metadata/source_manifest.json", json.dumps(version).encode())
            snapshot_source_metadata(storage, "dataset", "file", "v1.0.0")
            resolved = load_resolved_source_manifest(storage, "dataset", "file", "v1.0.0")
            dictionary = generate_data_dictionary(pd.DataFrame({"id": [1]}), "fixture", source_metadata=resolved.metadata)
            self.assertEqual(dictionary["transformations"], [])
            self.assertEqual([item["input_id"] for item in dictionary["inputs"]], ["producer", "covariate"])
            write_catalog_metadata(storage, "dataset", "file", "v1.0.0", {"quality_check_passed": True}, dictionary)
            captured = json.loads(storage.read_key("dataset/file/v1.0.0/metadata/resolved_provenance.json"))
            self.assertEqual(captured["inputs"], dictionary["inputs"])
            self.assertEqual(captured["code_commit"], version["code_commit"])
            self.assertEqual([layer["code_commit"] for layer in captured["manifest_layers"]], [parent["code_commit"], version["code_commit"]])
            self.assertEqual(captured["manifest_layers"][0]["transformations"], parent["transformations"])
            for layer in captured["manifest_layers"]:
                self.assertEqual(storage.sha256_key("dataset/file/v1.0.0/" + layer["snapshot"]), layer["sha256"])

    def test_repinned_dictionary_input_loss_cannot_promote(self):
        with tempfile.TemporaryDirectory() as d:
            source, target, registry = accepted_fixture(Path(d))
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
                before = {key: target.object_snapshot(key) for key in target.list_prefix()}
                key = "synthetic/points/v1.0.0/metadata/data_dictionary.json"
                value = json.loads(source.read_key(key)); value["inputs"] = []
                source.write_key(key, json.dumps(value).encode())
                approvals = json.loads(registry.read_text())
                for name in ["staged_objects", "published_objects"]:
                    approvals["releases"][0][name][key] = source.sha256_key(key)
                registry.write_text(json.dumps(approvals))
                with self.assertRaisesRegex(ReleaseApprovalError, "resolved_provenance_metadata"):
                    _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
                self.assertEqual(before, {key: target.object_snapshot(key) for key in target.list_prefix()})

    def test_rendered_processor_roles_and_each_input_link_are_checked(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry = accepted_fixture(root)
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
            record = CatalogRecord("synthetic", "synthetic", "points", "v1.0.0", "Fixture", "Synthetic only", "spatial", 1, ())
            record = attach_published_provenance(record, target, "synthetic/points/v1.0.0", "https://example.invalid/data")
            tree = root / "tree"
            render_portolan_tree(tree, (record,), public_root="https://example.invalid/catalog")
            normalize_candidate_tree(tree)
            validate_candidate_tree(tree)
            path = tree / record.version_path / "collection.json"
            original = json.loads(path.read_text())
            wrong = copy.deepcopy(original)
            next(provider for provider in wrong["providers"] if provider["name"] == "HIFLD Next")["roles"] = ["producer", "host"]
            path.write_text(json.dumps(wrong))
            with self.assertRaisesRegex(PortolanValidationError, "processor/host"):
                validate_candidate_tree(tree)
            wrong = copy.deepcopy(original)
            wrong["links"] = [link for link in wrong["links"] if link.get("hifld:input_url_role") != "canonical_url"]
            path.write_text(json.dumps(wrong))
            with self.assertRaisesRegex(PortolanValidationError, "canonical_url"):
                validate_candidate_tree(tree)

    def test_linked_generated_metadata_cannot_overwrite_preserved_capture(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "stage"))
            original = root / "capture.gpkg"; original.write_bytes(b"original captured source bytes")
            metadata = root / "stage/dataset/file/v1.0.0/metadata"; metadata.mkdir(parents=True)
            alias = metadata / "resolved_provenance.json"; alias.symlink_to(original)
            with self.assertRaisesRegex(ValueError, "link"):
                write_catalog_metadata(storage, "dataset", "file", "v1.0.0", {"quality_check_passed": True}, provenance_fixture())
            self.assertEqual(original.read_bytes(), b"original captured source bytes")
            self.assertEqual(list(metadata.iterdir()), [alias])

    def test_prepared_catalog_refuses_linked_output_before_original_mutation(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, _ = accepted_fixture(root)
            original = root / "capture.bin"; original.write_bytes(b"preserved original")
            plan = root / "plan"; plan.mkdir()
            (plan / "bundle_plan.json").symlink_to(original)
            record = CatalogRecord("synthetic", "synthetic", "points", "v1.0.0", "Fixture", "Synthetic only", "spatial", 1, ())
            request = PortolanPublishRequest("synthetic", "synthetic", "points", "v1.0.0", "Fixture", "Synthetic only", "Agency")
            with self.assertRaisesRegex(ValueError, "link"):
                prepare_portolan_release_bundle((record,), request, target, plan)
            self.assertEqual(original.read_bytes(), b"preserved original")


if __name__ == "__main__":
    unittest.main()
