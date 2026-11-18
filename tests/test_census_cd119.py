import unittest
import os
import tempfile
import json
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.assets.census_cd119 import (
    CD119_DATASET_SLUG,
    CD119_FILE_SLUG,
    CD119_SOURCE_FILES,
    CD119_SOURCE_GEOIDS,
    CD119_SOURCE_URLS,
    CensusCd119Config,
    census_119th_congressional_districts,
)
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILE_PAIRS
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.native_geoparquet_catalog import pinned_native_path, summarize_native_path
import dagster_hifld.sources.census_cd119 as source


class CensusCD119SourceTests(unittest.TestCase):
    def test_official_2025_source_roster_is_complete_and_unambiguous(self):
        self.assertEqual(len(CD119_SOURCE_GEOIDS), 56)
        self.assertEqual(len(set(CD119_SOURCE_GEOIDS)), 56)
        self.assertEqual(CD119_SOURCE_GEOIDS[0:3], ("01", "02", "04"))
        self.assertEqual(CD119_SOURCE_GEOIDS[-5:], ("60", "66", "69", "72", "78"))
        self.assertEqual(
            CD119_SOURCE_FILES,
            tuple(f"tl_2025_{geoid}_cd119.zip" for geoid in CD119_SOURCE_GEOIDS),
        )
        self.assertEqual(
            CD119_SOURCE_URLS,
            tuple(
                (
                    f"https://www2.census.gov/geo/tiger/TIGER2025/CD/{name}",
                    name,
                )
                for name in CD119_SOURCE_FILES
            ),
        )
        self.assertTrue(all("2024" not in url and "cd118" not in url for url, _ in CD119_SOURCE_URLS))

    def test_dataset_is_a_single_explicit_registered_ingest_asset(self):
        matching = [
            asset_def
            for asset_def in hifld_ingest_assets
            if next(iter(asset_def.keys)).path[-2:]
            == [CD119_DATASET_SLUG, CD119_FILE_SLUG]
        ]
        self.assertEqual(matching, [census_119th_congressional_districts])
        self.assertEqual(
            SUPPORTED_DATASET_FILE_PAIRS.count((CD119_DATASET_SLUG, CD119_FILE_SLUG)),
            1,
        )

    @unittest.skipUnless(
        Path("/tmp/hifld-native-reconstruction/387/capture/receipts.json").is_file(),
        "retained Census source capture is unavailable",
    )
    def test_retained_national_capture_passes_all_archive_and_feature_gates(self):
        root = source._capture_root()
        receipts, by_name = source._verify_receipts(root, replay=True)
        seen = set()
        total = 0
        for geoid in source.GEOIDS:
            name = f"tl_2025_{geoid}_cd119.zip"
            table = source._read_archive(root, name, by_name[name], geoid)
            total += table.num_rows
            for value in table["GEOID"].to_pylist():
                self.assertNotIn(value, seen)
                seen.add(value)
        self.assertEqual(len(receipts), 56)
        self.assertEqual(total, 444)
        self.assertEqual(len(seen), 444)
        self.assertEqual(len(source.SOURCE_PINS), 56)
        self.assertEqual(sum(source.COUNTS.values()), 444)

    @unittest.skipUnless(
        Path("/tmp/hifld-native-reconstruction/387/capture/receipts.json").is_file(),
        "retained Census source capture is unavailable",
    )
    def test_replay_staging_cache_and_prefix_are_stable(self):
        with tempfile.TemporaryDirectory(prefix="cd119-stage-test-") as temp:
            storage = StagingStorageResource(local_dir=temp, prefix="pinned-prefix", use_local=True)
            old_mode = os.environ.get("HIFLD_CD119_MODE")
            old_root = os.environ.get("HIFLD_CAPTURE_ROOT")
            os.environ["HIFLD_CD119_MODE"] = "replay"
            os.environ["HIFLD_CAPTURE_ROOT"] = "/tmp"
            try:
                first = source.stage_cd119_candidate(storage, CD119_DATASET_SLUG, CD119_FILE_SLUG)
                second = source.stage_cd119_candidate(storage, CD119_DATASET_SLUG, CD119_FILE_SLUG)
            finally:
                if old_mode is None:
                    os.environ.pop("HIFLD_CD119_MODE", None)
                else:
                    os.environ["HIFLD_CD119_MODE"] = old_mode
                if old_root is None:
                    os.environ.pop("HIFLD_CAPTURE_ROOT", None)
                else:
                    os.environ["HIFLD_CAPTURE_ROOT"] = old_root
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(first["candidate_identity"], second["candidate_identity"])
            self.assertEqual(first["source_feature_count"], 444)
            self.assertEqual(first["source_unique_geoid_count"], 444)
            self.assertEqual(first["prebuilt_geoparquet_sha256"], second["prebuilt_geoparquet_sha256"])
            self.assertEqual(first["source_manifest_key"], second["source_manifest_key"])
            manifest = json.loads(storage.read_key(first["source_manifest_key"]))
            self.assertIn("pinned-prefix/", first["source_manifest_key"])
            with pinned_native_path(
                storage,
                f"{CD119_DATASET_SLUG}/{CD119_FILE_SLUG}/{first['version']}",
                manifest,
            ) as native_path:
                quality, _ = summarize_native_path(Path(native_path), CD119_FILE_SLUG, manifest)
            self.assertEqual(quality["feature_count"], 444)
            self.assertEqual(quality["geometry_type"], "Mixed")
            self.assertEqual(quality["invalid_geometry_count"], 0)


class CensusCD119AcquisitionRootTests(unittest.TestCase):
    def test_explicit_acquisitions_directory_wins_and_shared_root_is_the_fallback(self):
        with tempfile.TemporaryDirectory() as temp:
            explicit = Path(temp) / "acquisitions"
            with patch.object(source, "GEOIDS", ()):
                acquisition = source._acquire_live(str(explicit))
                self.assertEqual(acquisition.parent, explicit.absolute())
                self.assertRegex(acquisition.name, r"cd119-[0-9a-f]{32}")
            with patch.object(source, "GEOIDS", ()), \
                    patch.dict(os.environ, {"HIFLD_CAPTURE_ROOT": str(temp)}):
                acquisition = source._acquire_live()
                self.assertEqual(
                    acquisition.parent,
                    Path(temp) / "hifld-native-reconstruction" / "387" / "acquisitions")

    def test_live_acquisition_parent_stays_inside_the_shared_root(self):
        # The retired per-dataset override must not select a path outside the
        # shared root when both variables are present.
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(source, "GEOIDS", ()), \
                    patch.dict(os.environ, {"HIFLD_CAPTURE_ROOT": str(temp),
                                            "HIFLD_CD119_ACQUISITIONS_DIR": "/elsewhere"}):
                acquisition = source._acquire_live()
                self.assertEqual(
                    acquisition.parent,
                    Path(temp) / "hifld-native-reconstruction" / "387" / "acquisitions")

    def test_stage_routes_the_explicit_acquisitions_directory_to_live_capture(self):
        with tempfile.TemporaryDirectory() as temp:
            storage = StagingStorageResource(local_dir=temp, use_local=True)
            with patch.object(source, "_mode", lambda: "live"), \
                    patch.object(source, "_acquire_live",
                                 return_value=Path(temp) / "acq") as acquire, \
                    patch.object(source, "_verify_receipts",
                                 side_effect=AssertionError("verify called")):
                with self.assertRaises(AssertionError):
                    source.stage_cd119_candidate(
                        storage, CD119_DATASET_SLUG, CD119_FILE_SLUG,
                        acquisitions_dir=str(temp))
            acquire.assert_called_once_with(str(temp))

    def test_asset_config_feeds_the_acquisitions_directory(self):
        self.assertEqual(CensusCd119Config().acquisitions_directory, "")


if __name__ == "__main__":
    unittest.main()
