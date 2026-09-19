import tempfile
import unittest
from pathlib import Path

from dagster import AssetCheckSeverity, build_op_context
from dagster._core.execution.context.invocation import DirectAssetCheckExecutionContext
from dagster_hifld.checks import hifld_asset_checks
from dagster_hifld.checks import publish_catalog_quality_manifest
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
            self.assertEqual(spec.asset_key.path[:2], ["publish", "catalog"])

    def test_manifest_backed_checks_cover_backfill_and_census_assets(self):
        asset_paths = {
            tuple(next(iter(check_def.check_specs)).asset_key.path)
            for check_def in hifld_asset_checks
        }

        self.assertEqual(asset_paths, {("publish", "catalog")})
        self.assertEqual(len(hifld_asset_checks), 2)

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

    def test_publish_quality_check_reports_warning_severity_for_invalid_geometry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_dir = Path(tmpdir) / "nfhl" / "area" / "v1.0.0" / "metadata"
            metadata_dir.mkdir(parents=True)
            (metadata_dir / "quality_manifest.json").write_text(
                '{"quality_check_passed": false, "invalid_geometry_count": 40}',
                encoding="utf-8",
            )
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)
            context = DirectAssetCheckExecutionContext(
                build_op_context(partition_key="nfhl/area/v1.0.0")
            )

            result = publish_catalog_quality_manifest(context, resource)

            self.assertFalse(result.passed)
            self.assertEqual(result.severity, AssetCheckSeverity.WARN)
            self.assertEqual(result.metadata["invalid_geometry_count"].value, 40)

    def test_read_quality_check_result_passes_expected_all_null_geometry_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_dir = Path(tmpdir) / "trauma-levels" / "trauma-levels" / "v1.0.0" / "metadata"
            metadata_dir.mkdir(parents=True)
            (metadata_dir / "quality_manifest.json").write_text(
                '{"quality_check_passed": true, "feature_count": 221, "spatial_status": "all_null_geometry"}',
                encoding="utf-8",
            )
            resource = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = read_quality_check_result(
                resource,
                dataset_slug="trauma-levels",
                file_slug="trauma-levels",
                version="v1.0.0",
            )

            self.assertTrue(result.passed)
            self.assertEqual(result.metadata["spatial_status"].value, "all_null_geometry")

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
