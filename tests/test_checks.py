import tempfile
import unittest
from pathlib import Path

from dagster_hifld.checks import hifld_asset_checks
from dagster_hifld.checks.common import (
    BASELINE_VERSION,
    read_baseline_comparison_result,
    read_quality_check_result,
)
from dagster_hifld.resources import StagingStorageResource


class CheckTests(unittest.TestCase):
    def test_quality_checks_target_catalog_assets(self):
        for check_def in hifld_asset_checks:
            spec = next(iter(check_def.check_specs))
            self.assertEqual(spec.asset_key.path[0], "catalog")

    def test_manifest_backed_checks_cover_backfill_and_census_assets(self):
        asset_paths = {
            tuple(next(iter(check_def.check_specs)).asset_key.path)
            for check_def in hifld_asset_checks
        }

        self.assertIn(
            ("catalog", "119th-congressional-districts", "119th-congressional-districts"),
            asset_paths,
        )
        self.assertIn(
            ("catalog", "address-ranges", "address-ranges"),
            asset_paths,
        )
        self.assertIn(
            (
                "catalog",
                "agricultural-minerals-operations",
                "agricultural-minerals-operations",
            ),
            asset_paths,
        )
        self.assertIn(
            ("catalog", "2020-census-blocks-1", "tl_2024_01_tabblock20"),
            asset_paths,
        )

        census_checks = [
            path
            for path in asset_paths
            if path[1] == "2020-census-blocks-1"
        ]
        self.assertEqual(len(census_checks), 56)

    def test_read_quality_check_result_reads_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_dir = (
                Path(tmpdir)
                / "amtrak-stations"
                / "amtrak-stations"
                / "run_test"
                / "metadata"
            )
            metadata_dir.mkdir(parents=True)
            (metadata_dir / "quality_manifest.json").write_text(
                '{"quality_check_passed": true, "feature_count": 5}',
                encoding="utf-8",
            )
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = read_quality_check_result(
                resource,
                dataset_slug="amtrak-stations",
                file_slug="amtrak-stations",
                version="run_test",
            )

            self.assertTrue(result.passed)
            self.assertEqual(result.metadata["feature_count"].value, 5)

    def test_baseline_check_skips_when_partition_is_baseline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            result = read_baseline_comparison_result(
                resource,
                dataset_slug="foo",
                file_slug="bar",
                version=BASELINE_VERSION,
            )
            self.assertTrue(result.passed)
            self.assertEqual(result.metadata["reason"].value, "is_baseline")

    def test_baseline_check_no_baseline_when_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            result = read_baseline_comparison_result(
                resource,
                dataset_slug="foo",
                file_slug="bar",
                version="v999",
            )
            self.assertTrue(result.passed)
            self.assertEqual(result.metadata["reason"].value, "no_baseline")

    def test_baseline_check_passes_when_aligned(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cols = '{"columns": [{"name": "id"}, {"name": "geom", "type": "geometry"}]}'
            for ver in (BASELINE_VERSION, "v999"):
                meta = root / "ds" / "f" / ver / "metadata"
                meta.mkdir(parents=True)
                (meta / "quality_manifest.json").write_text(
                    '{"feature_count": 100}',
                    encoding="utf-8",
                )
                (meta / "data_dictionary.json").write_text(cols, encoding="utf-8")
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            result = read_baseline_comparison_result(
                resource, dataset_slug="ds", file_slug="f", version="v999"
            )
            self.assertTrue(result.passed)
            self.assertTrue(result.metadata["columns_ok"].value)
            self.assertTrue(result.metadata["rows_ok"].value)

    def test_baseline_check_fails_on_dropped_column(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            b = root / "ds" / "f" / BASELINE_VERSION / "metadata"
            b.mkdir(parents=True)
            (b / "quality_manifest.json").write_text(
                '{"feature_count": 100}', encoding="utf-8"
            )
            (b / "data_dictionary.json").write_text(
                '{"columns": [{"name": "id"}, {"name": "x"}]}',
                encoding="utf-8",
            )
            c = root / "ds" / "f" / "v999" / "metadata"
            c.mkdir(parents=True)
            (c / "quality_manifest.json").write_text(
                '{"feature_count": 100}', encoding="utf-8"
            )
            (c / "data_dictionary.json").write_text(
                '{"columns": [{"name": "id"}]}',
                encoding="utf-8",
            )
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            result = read_baseline_comparison_result(
                resource, dataset_slug="ds", file_slug="f", version="v999"
            )
            self.assertFalse(result.passed)
            self.assertFalse(result.metadata["columns_ok"].value)

    def test_baseline_check_fails_on_row_delta(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cols = '{"columns": [{"name": "id"}]}'
            b = root / "ds" / "f" / BASELINE_VERSION / "metadata"
            b.mkdir(parents=True)
            (b / "quality_manifest.json").write_text(
                '{"feature_count": 100}', encoding="utf-8"
            )
            (b / "data_dictionary.json").write_text(cols, encoding="utf-8")
            c = root / "ds" / "f" / "v999" / "metadata"
            c.mkdir(parents=True)
            (c / "quality_manifest.json").write_text(
                '{"feature_count": 200}', encoding="utf-8"
            )
            (c / "data_dictionary.json").write_text(cols, encoding="utf-8")
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            result = read_baseline_comparison_result(
                resource, dataset_slug="ds", file_slug="f", version="v999"
            )
            self.assertFalse(result.passed)
            self.assertFalse(result.metadata["rows_ok"].value)


if __name__ == "__main__":
    unittest.main()
