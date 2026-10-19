from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DagsterInstance, materialize

from dagster_hifld.assets import usgs_nldi as nldi
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource


class USGSNLDIAssetTests(unittest.TestCase):
    def test_registered_refresh_reuses_content_version_and_partition(self):
        feature = {"type": "Feature", "properties": {"id": "USGS-00123", "provider_id": "USGS-00123",
                   "nhdpv2_COMID": 123.0, "mainstem_uri": None},
                   "geometry": {"type": "Point", "coordinates": [-107.0, 35.0]}}
        payload = json.dumps({"type": "FeatureCollection", "name": "source", "features": [feature]}, separators=(",", ":")).encode()
        digest = hashlib.sha256(payload).hexdigest()
        published_at = "2026-07-16T20:14:57Z"
        asset_url = f"https://github.com/{nldi.REPOSITORY}/releases/download/v1.7/{nldi.PINNED_ASSET}"
        def make_capture(folder, download_count, retrieved):
            metadata = {"tag_name": nldi.PINNED_TAG, "published_at": nldi.PINNED_PUBLISHED_AT,
                        "draft": False, "prerelease": False, "author": {"login": "maintainer"},
                        "download_count": download_count,
                        "assets": [{"name": nldi.PINNED_ASSET, "size": len(payload), "digest": f"sha256:{digest}",
                                    "browser_download_url": asset_url}]}
            meta_bytes = json.dumps(metadata).encode()
            meta_sha = hashlib.sha256(meta_bytes).hexdigest()
            receipts = [
                {"request": {"url": nldi.LATEST_RELEASE_URL}, "final_url": nldi.LATEST_RELEASE_URL,
                 "status_code": 200, "complete": True, "decoded_bytes": len(meta_bytes), "decoded_sha256": meta_sha,
                 "wire_bytes": len(meta_bytes), "wire_sha256": meta_sha, "fetched_at": retrieved},
                {"request": {"url": asset_url}, "final_url": "https://release-assets.githubusercontent.com/release/asset",
                 "status_code": 200, "complete": True, "decoded_bytes": len(payload), "decoded_sha256": digest,
                 "wire_bytes": len(payload), "wire_sha256": digest, "fetched_at": retrieved},
            ]
            cap = Path(folder)
            (cap / "evidence").mkdir()
            (cap / "metadata").mkdir()
            (cap / "evidence" / "release-metadata-test.decoded").write_bytes(meta_bytes)
            (cap / "evidence" / "release-metadata-test.wire").write_bytes(meta_bytes)
            (cap / "evidence" / f"{nldi.PINNED_ASSET}.decoded").write_bytes(payload)
            (cap / "evidence" / f"{nldi.PINNED_ASSET}.wire").write_bytes(payload)
            (cap / "metadata" / "source_manifest.json").write_text(json.dumps({"source_observation": {"receipts": receipts}}))
            return meta_bytes, retrieved

        with tempfile.TemporaryDirectory() as capture_one, tempfile.TemporaryDirectory() as capture_two, tempfile.TemporaryDirectory() as root, \
             patch.object(nldi, "PINNED_ASSET_BYTES", len(payload)), \
             patch.object(nldi, "PINNED_ASSET_SHA256", digest), \
             patch.object(nldi, "PINNED_FEATURE_COUNT", 1), \
             patch.object(nldi, "MAX_CAPTURE_BYTES", len(payload)), \
             patch.object(nldi, "PINNED_PUBLISHED_AT", published_at):
            _, retrieved = make_capture(capture_one, 1, "2026-10-19T19:13:16+00:00")
            make_capture(capture_two, 2, "2026-10-20T19:13:16+00:00")
            staging = StagingStorageResource(local_dir=root, use_local=True)
            dagster_home = Path(root) / "dagster-home"
            dagster_home.mkdir()
            asset_name = "ingest__nhd__usgs_hydro_nldi_streamgage_monitoring_locations"
            with patch.dict(os.environ, {"DAGSTER_HOME": str(dagster_home)}), DagsterInstance.local_temp() as instance:
                first = materialize([nldi.usgs_nldi_streamgage_monitoring_locations],
                    resources={"staging_storage": staging}, instance=instance,
                    run_config={"ops": {asset_name: {"config": {"replay_capture_directory": capture_one}}}})
                result = first.output_for_node(asset_name)
                prefix = Path(root) / nldi.DATASET_SLUG / nldi.FILE_SLUG / result["version"]
                original_receipt_sha = hashlib.sha256((prefix / "metadata/release-metadata.receipt.json").read_bytes()).hexdigest()
                second = materialize([nldi.usgs_nldi_streamgage_monitoring_locations],
                    resources={"staging_storage": staging}, instance=instance,
                    run_config={"ops": {asset_name: {"config": {"replay_capture_directory": capture_two}}}})
                again = second.output_for_node(asset_name)
                partition = build_publish_partition_key(nldi.DATASET_SLUG, nldi.FILE_SLUG, result["version"])
                self.assertIn(partition, instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))
            self.assertEqual((prefix / nldi.ASSET_PATH).read_bytes(), payload)
            self.assertEqual((prefix / "evidence" / f"{nldi.PINNED_ASSET}.wire").read_bytes(), payload)
            manifest = json.loads((prefix / "metadata/source_manifest.json").read_text())
            observations = list((Path(root) / "_observations" / nldi.FILE_SLUG / result["version"]).iterdir())
            self.assertEqual(result["feature_count"], 1)
            self.assertTrue(again["reused"])
            self.assertEqual(result["version"], again["version"])
            self.assertTrue(result["version"].startswith("v"))
            self.assertEqual(len(observations), 2)
            self.assertEqual(hashlib.sha256((prefix / "metadata/release-metadata.receipt.json").read_bytes()).hexdigest(), original_receipt_sha)
            self.assertEqual(manifest["source_observation"]["id_examples"], ["USGS-00123"])
            self.assertEqual(manifest["source_observation"]["release_tag"], "v1.7")
            self.assertEqual(manifest["source_observation"]["publication_status"], "STOPPED")
            self.assertFalse(manifest["source_observation"]["agency_delegation_acceptance"])
            self.assertEqual(manifest["publisher"], "Internet of Water (maintainer/publisher of the ref_gages compilation)")
            self.assertEqual(manifest["literal_geojson_catalog"]["sha256"], digest)
            self.assertEqual(manifest["inputs"][1]["retrieved_at"], retrieved)
            self.assertTrue((Path(root) / "_construction-state" / nldi.DATASET_SLUG / nldi.FILE_SLUG / f"{result['version']}.json").is_file())

    def test_latest_release_drift_stops_before_asset_selection(self):
        with self.assertRaisesRegex(ValueError, "differs from pinned v1.7"):
            nldi._select_release({"tag_name": "v1.8", "published_at": "2026-08-01T00:00:00Z", "draft": False, "prerelease": False, "assets": []})


if __name__ == "__main__":
    unittest.main()
