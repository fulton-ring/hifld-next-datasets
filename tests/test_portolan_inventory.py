import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import JobDefinition

from dagster_hifld.portolan.build_inventory_catalog import inventory_catalog_job
from dagster_hifld.portolan.inventory import (
    _to_crs84_bbox,
    build_catalog_from_inventories,
)


class PortolanInventoryTests(unittest.TestCase):
    def test_clamps_only_floating_point_wgs84_bound_overflow(self):
        self.assertEqual(
            _to_crs84_bbox((-180.0, -10.0, 180.0000000001, 20.0), "OGC:CRS84"),
            (-180.0, -10.0, 180.0, 20.0),
        )

    def test_exposes_dagster_job_for_metadata_only_inventory_build(self):
        self.assertIsInstance(inventory_catalog_job, JobDefinition)
        with patch(
            "dagster_hifld.portolan.build_inventory_catalog.build_inventory_catalog_artifact",
            return_value={"version_count": 1, "asset_count": 1},
        ):
            result = inventory_catalog_job.execute_in_process(
                run_config={
                    "ops": {
                        "build_inventory_catalog": {
                            "config": {
                                "source_inventory": "source.json",
                                "published_copy_report": "target.jsonl",
                                "parquet_footer": "footer.json",
                                "source_bucket": "source",
                                "published_root": "https://example.test/published",
                                "output_dir": "output",
                                "metadata_cache": "cache",
                                "production_collections": "collections.json",
                            }
                        }
                    }
                }
            )
        self.assertTrue(result.success)

    def test_builds_generation_pinned_metadata_only_record(self):
        source_objects = [
            _object("dataset/file/v1.0.0/geoparquet/data.parquet", "101"),
            _object("dataset/metadata/source_manifest.json", "102"),
            _object("dataset/file/metadata/source_manifest.json", "103"),
            _object("dataset/file/v1.0.0/metadata/source_manifest.json", "104"),
            _object("dataset/file/v1.0.0/metadata/data_dictionary.json", "105"),
            _object("dataset/file/v1.0.0/metadata/quality_manifest.json", "106"),
        ]
        target_objects = [
            {
                **entry,
                "name": f"hifld/{entry['name']}",
                "generation": f"copy-{entry['generation']}",
            }
            for entry in source_objects
        ]
        metadata = {
            "dataset/metadata/source_manifest.json": {"title": "Dataset title"},
            "dataset/file/metadata/source_manifest.json": {"title": "File title"},
            "dataset/file/v1.0.0/metadata/source_manifest.json": {
                "title": "Version title",
                "description": "Source description",
                "publisher": "Source agency",
                "tags": {"theme": ["health"]},
            },
            "dataset/file/v1.0.0/metadata/data_dictionary.json": {
                "columns": [{"name": "geometry", "type": "geometry", "nullable": False}]
            },
            "dataset/file/v1.0.0/metadata/quality_manifest.json": {
                "description": None,
                "bounds": [-8575600.0, 4707060.0, -8575500.0, 4707160.0],
                "quality_check_passed": True,
                "invalid_geometry_count": 0,
                "null_geometry_count": 0,
            },
        }
        footer = [
            {
                "key": "dataset/file/v1.0.0/geoparquet/data.parquet",
                "generation": "101",
                "rows": 12,
                "primary_column": "geometry",
                "geometry_types": ["Point"],
                "crs": "OGC:CRS84",
                "bbox": [-77, 38, -76, 39],
            }
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_path = root / "source.json"
            target_path = root / "target.json"
            footer_path = root / "footer.json"
            source_path.write_text(json.dumps(source_objects))
            target_path.write_text(json.dumps(target_objects))
            footer_path.write_text(json.dumps(footer))
            result = build_catalog_from_inventories(
                source_path,
                target_path,
                footer_path,
                source_metadata=lambda item: metadata[item.name],
                published_root="https://storage.googleapis.com/hifld-next-portolan-published",
                collection_created_at="2026-02-13T02:52:43.921817",
                collection_updated_at="2026-02-13T02:52:43.921841",
                dataset_timestamps={
                    "dataset": (
                        "2026-08-17T22:47:38.047035",
                        "2026-08-17T22:47:38.047064",
                    )
                },
            )

        self.assertEqual(result.report.version_count, 1)
        record = result.records[0]
        self.assertEqual(record.title, "Version title")
        self.assertEqual(record.license_id, "CC-PDM-1.0")
        self.assertEqual(record.license_href, "../../../LICENSE.md")
        self.assertEqual(record.feature_count, 12)
        self.assertEqual(record.spatial_status, "spatial")
        self.assertIsNone(record.source_version_description)
        self.assertEqual(
            record.source_version_bounds,
            (-8575600.0, 4707060.0, -8575500.0, 4707160.0),
        )
        self.assertEqual(record.assets[0].storage_revision, "copy-101")
        self.assertEqual(
            record.assets[0].href,
            "https://storage.googleapis.com/hifld-next-portolan-published/hifld/dataset/file/v1.0.0/geoparquet/data.parquet",
        )
        self.assertIsNone(record.assets[0].sha256)
        self.assertEqual(
            record.assets[0].checksum_multihash,
            "d50110000102030405060708090a0b0c0d0e0f",
        )
        self.assertEqual(record.collection_created_at, "2026-02-13T02:52:43.921817")
        self.assertEqual(record.collection_updated_at, "2026-02-13T02:52:43.921841")
        self.assertEqual(record.dataset_created_at, "2026-08-17T22:47:38.047035")
        self.assertEqual(record.dataset_updated_at, "2026-08-17T22:47:38.047064")
        self.assertIsNone(record.created_at)
        self.assertIsNone(record.updated_at)


def _object(name: str, generation: str) -> dict[str, str]:
    return {
        "name": name,
        "generation": generation,
        "size": "3",
        "md5Hash": "AAECAwQFBgcICQoLDA0ODw==",
    }
