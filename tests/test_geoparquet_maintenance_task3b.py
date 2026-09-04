import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.maintenance import (
    audit_geoparquet,
    benchmark_tiles,
    replace_geoparquet,
)
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


def _parquet_bytes(value: int = 1) -> bytes:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "part.parquet"
        pq.write_table(pa.table({"id": [value], "geometry": [b"x"]}), path)
        return path.read_bytes()


def _layout(path: str, *, size: int = 0, sha256: str = "") -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "validation_status": "valid",
            "layers": [
                {
                    "layer": "roads",
                    "source_format": "geojson",
                    "validation_status": "valid",
                    "feature_count": 1,
                    "partition_strategy": "single_file",
                    "partition_columns": [],
                    "hive_partition_columns": {},
                    "outputs": [
                        {
                            "path": path,
                            "relative_path": "geoparquet/part.parquet",
                            "file_size_bytes": size,
                            "sha256": sha256,
                            "row_counts": [1],
                            "row_group_uncompressed_sizes": [151],
                        }
                    ],
                }
            ],
        }
    ).encode()


class Task3BTests(unittest.TestCase):
    def test_audit_reports_compliant_canonical_version(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            parquet = _parquet_bytes()
            storage.write_key("d/f/v/geoparquet/part.parquet", parquet)
            storage.write_key(
                "d/f/v/metadata/geoparquet_layout.json",
                _layout(
                    "d/f/v/geoparquet/part.parquet",
                    size=len(parquet),
                    sha256=__import__("hashlib").sha256(parquet).hexdigest(),
                ),
            )

            report = audit_geoparquet(storage)

            self.assertEqual(report["status"], "compliant")
            self.assertEqual(report["versions"][0]["dataset"], "d")

    def test_replace_dry_run_does_not_mutate_production(self):
        with (
            tempfile.TemporaryDirectory() as production_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            production = PublishedStorageResource(
                local_dir=production_dir, use_local=True
            )
            staging = StagingStorageResource(
                local_dir=staging_dir, use_local=True, prefix="stage"
            )
            old = _parquet_bytes()
            production.write_key("d/f/v/geoparquet/part.parquet", old)
            production.write_key(
                "d/f/v/metadata/geoparquet_layout.json",
                _layout(
                    "d/f/v/geoparquet/part.parquet",
                    size=len(old),
                    sha256=hashlib.sha256(old).hexdigest(),
                ),
            )
            staging.write_key(
                "_temporary/repack/run/d/f/v/geoparquet/part.parquet", old
            )
            staging.write_key(
                "_temporary/repack/run/d/f/v/metadata/geoparquet_layout.json",
                _layout(
                    "d/f/v/geoparquet/part.parquet",
                    size=len(old),
                    sha256=hashlib.sha256(old).hexdigest(),
                ),
            )

            report = replace_geoparquet(
                production, staging, "d", "f", "v", "run", apply=False
            )

            self.assertEqual(report["status"], "planned")
            self.assertEqual(
                production.read_bytes("d", "f", "v", "geoparquet/part.parquet"), old
            )

    def test_replace_apply_backups_and_promotes_only_canonical_objects(self):
        with (
            tempfile.TemporaryDirectory() as production_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            production = PublishedStorageResource(
                local_dir=production_dir, use_local=True
            )
            staging = StagingStorageResource(
                local_dir=staging_dir, use_local=True, prefix="stage"
            )
            old = _parquet_bytes()
            new = _parquet_bytes(2)
            production.write_key("d/f/v/geoparquet/old.parquet", old)
            production.write_key("d/f/v/pmtiles/tiles.pmtiles", b"keep")
            production.write_key(
                "d/f/v/metadata/geoparquet_layout.json",
                _layout(
                    "d/f/v/geoparquet/old.parquet",
                    size=len(old),
                    sha256=hashlib.sha256(old).hexdigest(),
                ),
            )
            staging.write_key("_temporary/repack/run/d/f/v/geoparquet/new.parquet", new)
            staging.write_key(
                "_temporary/repack/run/d/f/v/metadata/geoparquet_layout.json",
                _layout(
                    "d/f/v/geoparquet/new.parquet",
                    size=len(new),
                    sha256=hashlib.sha256(new).hexdigest(),
                ),
            )

            report = replace_geoparquet(
                production, staging, "d", "f", "v", "run", apply=True
            )

            self.assertEqual(report["status"], "promoted")
            self.assertFalse(production.object_exists("d/f/v/geoparquet/old.parquet"))
            self.assertTrue(production.object_exists("d/f/v/geoparquet/new.parquet"))
            self.assertTrue(production.object_exists("d/f/v/pmtiles/tiles.pmtiles"))
            self.assertTrue(
                production.object_exists(
                    "_rollback/geoparquet/run/d/f/v/geoparquet/old.parquet"
                )
            )

    def test_benchmark_sends_token_header_without_reporting_token(self):
        class Response:
            status_code = 204
            content = b""

        class Client:
            def get(self, url, *, headers, timeout):
                self.url = url
                self.headers = headers
                self.timeout = timeout
                return Response()

        client = Client()
        with patch.dict(os.environ, {"TILE_TOKEN": "secret-token"}):
            report = benchmark_tiles(
                "https://example.test/",
                "q1",
                "TILE_TOKEN",
                [(1, 2, 3)],
                repetitions=1,
                client=client,
                clock=iter([0.0, 0.1]).__next__,
            )

        self.assertEqual(report["status"], "compliant")
        self.assertEqual(
            client.url, "https://example.test/api/queries/q1/tiles/1/2/3.mvt"
        )
        self.assertEqual(client.headers["X-HIFLD-Query-Token"], "secret-token")
        self.assertNotIn("secret-token", json.dumps(report))

    def test_audit_file_only_filter_scans_root_then_filters_in_memory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            parquet = _parquet_bytes()
            storage.write_key("d/f/v/geoparquet/part.parquet", parquet)
            storage.write_key(
                "d/f/v/metadata/geoparquet_layout.json",
                _layout(
                    "d/f/v/geoparquet/part.parquet",
                    size=len(parquet),
                    sha256=hashlib.sha256(parquet).hexdigest(),
                ),
            )
            original_list = PublishedStorageResource.list_object_snapshots
            with patch.object(
                PublishedStorageResource,
                "list_object_snapshots",
                autospec=True,
                side_effect=lambda storage, key_prefix="": original_list(
                    storage, key_prefix
                ),
            ) as listing:
                report = audit_geoparquet(storage, file="f")
            self.assertEqual(report["status"], "compliant")
            listing.assert_called_once_with(storage, "")

    def test_audit_version_only_filter_scans_root_then_filters_in_memory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            parquet = _parquet_bytes()
            storage.write_key("d/f/v/geoparquet/part.parquet", parquet)
            storage.write_key(
                "d/f/v/metadata/geoparquet_layout.json",
                _layout(
                    "d/f/v/geoparquet/part.parquet",
                    size=len(parquet),
                    sha256=hashlib.sha256(parquet).hexdigest(),
                ),
            )
            original_list = PublishedStorageResource.list_object_snapshots
            with patch.object(
                PublishedStorageResource,
                "list_object_snapshots",
                autospec=True,
                side_effect=lambda storage, key_prefix="": original_list(
                    storage, key_prefix
                ),
            ) as listing:
                report = audit_geoparquet(storage, version="v")
            self.assertEqual(report["status"], "compliant")
            listing.assert_called_once_with(storage, "")

    def test_audit_does_not_use_read_bytes_for_parquet_objects(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            parquet = _parquet_bytes()
            storage.write_key("d/f/v/geoparquet/part.parquet", parquet)
            storage.write_key(
                "d/f/v/metadata/geoparquet_layout.json",
                _layout(
                    "d/f/v/geoparquet/part.parquet",
                    size=len(parquet),
                    sha256=hashlib.sha256(parquet).hexdigest(),
                ),
            )
            original_read = PublishedStorageResource.read_bytes
            with patch.object(
                PublishedStorageResource,
                "read_bytes",
                autospec=True,
                side_effect=lambda storage, dataset, file, version, key: original_read(
                    storage, dataset, file, version, key
                ),
            ) as read:
                report = audit_geoparquet(storage)
            self.assertEqual(report["status"], "compliant")
            self.assertEqual(read.call_count, 1)

    def test_replace_rejects_candidate_parquet_outside_canonical_directory(self):
        with (
            tempfile.TemporaryDirectory() as production_dir,
            tempfile.TemporaryDirectory() as staging_dir,
        ):
            production = PublishedStorageResource(
                local_dir=production_dir, use_local=True
            )
            staging = StagingStorageResource(
                local_dir=staging_dir, use_local=True, prefix="stage"
            )
            parquet = _parquet_bytes()
            staging.write_key(
                "_temporary/repack/run/d/f/v/geoparquet/part.parquet", parquet
            )
            staging.write_key(
                "_temporary/repack/run/d/f/v/metadata/geoparquet_layout.json",
                _layout(
                    "d/f/v/geoparquet/part.parquet",
                    size=len(parquet),
                    sha256=hashlib.sha256(parquet).hexdigest(),
                ),
            )
            staging.write_key("_temporary/repack/run/d/f/v/stray.parquet", parquet)
            report = replace_geoparquet(production, staging, "d", "f", "v", "run")
            self.assertEqual(report["status"], "blocked")
            self.assertIn("outside", " ".join(report["errors"]).lower())

    def test_benchmark_rejects_non_mvt_success_and_hides_request_errors(self):
        class Client:
            def get(self, url, *, headers, timeout):
                raise RuntimeError("request failed secret-token")

        with patch.dict(os.environ, {"TILE_TOKEN": "secret-token"}):
            report = benchmark_tiles(
                "https://example.test",
                "q1",
                "TILE_TOKEN",
                [(1, 2, 3)],
                repetitions=1,
                client=Client(),
                clock=iter([0.0]).__next__,
            )
        self.assertEqual(report["status"], "violation")
        self.assertNotIn("secret-token", json.dumps(report))


if __name__ == "__main__":
    unittest.main()
