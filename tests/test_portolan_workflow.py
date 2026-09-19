import hashlib
import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import JobDefinition
from shapely import Point

from dagster_hifld.assets.publish import _write_and_publish_shapefile_zip
from dagster_hifld.assets.portolan import publish_portolan_catalog
from dagster_hifld.partitions import PUBLISH_PARTITIONS
from dagster_hifld.portolan.catalog import CatalogRecord
from dagster_hifld.portolan.workflow import (
    PortolanPublishRequest,
    _ensure_absolute_self_link,
    _promote_source_metadata,
    _promote_staged_data,
    _publish_catalog,
    _record_assets,
    columns_from_dictionary,
    execute_portolan_manifest,
    inspect_geoparquet,
    load_publish_requests,
    manifest_tags,
    normalize_stac_datetime,
    portolan_publish_job,
)
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class PortolanWorkflowTests(unittest.TestCase):
    def test_terminal_portolan_asset_is_partitioned_with_promotion(self):
        self.assertIs(publish_portolan_catalog.partitions_def, PUBLISH_PARTITIONS)

    def test_terminal_asset_publishes_selected_promoted_partition_only(self):
        context = SimpleNamespace(partition_key="dataset/file/v1.0.0")
        staging = StagingStorageResource(local_dir="/tmp/staging", use_local=True, prefix="hifld")
        published = PublishedStorageResource(local_dir="/tmp/published", use_local=True, prefix="hifld")
        with (
            patch.dict(
                "os.environ",
                {
                    "HIFLD_PORTOLAN_ENABLED": "1",
                    "HIFLD_PORTOLAN_PUBLIC_ROOT": "https://example.test/catalog",
                    "HIFLD_PORTOLAN_COLLECTION_TITLE": "HIFLD Next",
                },
            ),
            patch(
                "dagster_hifld.portolan.workflow.publish_portolan_record",
                return_value="generation-1",
            ) as publish,
        ):
            result = publish_portolan_catalog.node_def.compute_fn.decorated_fn(
                context, staging, published
            )
        self.assertEqual(result.value["catalog_generation"], "generation-1")
        args, kwargs = publish.call_args
        request = args[0]
        self.assertEqual(
            (request.collection_slug, request.dataset_slug, request.file_slug, request.version),
            ("hifld", "dataset", "file", "v1.0.0"),
        )
        self.assertEqual(request.public_root, "https://example.test/catalog")
        self.assertEqual(request.collection_title, "HIFLD Next")
        self.assertFalse(request.archive_public_domain)
        self.assertTrue(kwargs["catalog_only"])

    def test_terminal_asset_opts_into_archived_hifld_status_when_configured(self):
        context = SimpleNamespace(partition_key="dataset/file/v1.0.0")
        staging = StagingStorageResource(local_dir="/tmp/staging", use_local=True, prefix="hifld")
        published = PublishedStorageResource(local_dir="/tmp/published", use_local=True, prefix="hifld")
        with (
            patch.dict("os.environ", {"HIFLD_PORTOLAN_ENABLED": "1", "HIFLD_PORTOLAN_ARCHIVE_PUBLIC_DOMAIN": "1"}),
            patch("dagster_hifld.portolan.workflow.publish_portolan_record", return_value="generation-1") as publish,
        ):
            publish_portolan_catalog.node_def.compute_fn.decorated_fn(
                context, staging, published
            )
        request = publish.call_args.args[0]
        self.assertEqual(
            request.resolved_license, ("CC-PDM-1.0", "../../../LICENSE.md")
        )

    def test_incremental_catalog_publication_keeps_existing_parent_links(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            request = PortolanPublishRequest(
                "hifld", "first", "file", "v1.0.0", "First", "First", "Agency",
                public_root="https://example.test/catalog",
            )
            first = CatalogRecord(
                "hifld", "first", "file", "v1.0.0", "First", "First", "non_spatial_source", 0, (),
                collection_title="HIFLD Next",
            )
            second = CatalogRecord(
                "hifld", "second", "file", "v1.0.0", "Second", "Second", "non_spatial_source", 0, (),
                collection_title="HIFLD",
            )
            _publish_catalog((first,), request, published)
            _publish_catalog((second,), request, published)

            collection = json.loads(published.read_key("hifld/catalog.json"))
            child_hrefs = {
                link["href"]
                for link in collection["links"]
                if link["rel"] == "child"
            }
            self.assertEqual(
                child_hrefs,
                {
                    "https://example.test/catalog/hifld/first/catalog.json",
                    "https://example.test/catalog/hifld/second/catalog.json",
                },
            )
            with sqlite3.connect(Path(tmpdir) / "_catalog/catalog.sqlite") as conn:
                root_title = conn.execute(
                    "SELECT root_title FROM catalog_metadata"
                ).fetchone()[0]
            self.assertEqual(root_title, "HIFLD")
            root_catalog = json.loads(published.read_key("catalog.json"))
            self.assertEqual(root_catalog["title"], "HIFLD")

    def test_root_catalog_ignores_data_prefix(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(
                local_dir=tmpdir, use_local=True, prefix="hifld"
            )
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1.0.0", "File", "File", "Agency",
                public_root="https://example.test/catalog",
            )
            record = CatalogRecord(
                "hifld", "dataset", "file", "v1.0.0", "File", "File",
                "non_spatial_source", 0, (), collection_title="HIFLD Next",
            )
            _publish_catalog((record,), request, published)
            self.assertTrue((Path(tmpdir) / "catalog.json").is_file())
            self.assertTrue((Path(tmpdir) / "hifld/catalog.json").is_file())
            self.assertFalse((Path(tmpdir) / "hifld/hifld/catalog.json").exists())

    def test_incremental_version_publication_keeps_prior_version_and_latest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1.0.0", "File", "File", "Agency",
                public_root="https://example.test/catalog",
            )
            for version in ("v1.9.0", "v1.10.0", "v1.0.0"):
                record = CatalogRecord(
                    "hifld", "dataset", "file", version, "File", "File",
                    "non_spatial_source", 0, (), collection_title="HIFLD Next",
                )
                _publish_catalog((record,), request, published)

            file_catalog = json.loads(
                published.read_key("hifld/dataset/file/catalog.json")
            )
            links = file_catalog["links"]
            child_hrefs = {link["href"] for link in links if link["rel"] == "child"}
            latest = [link["href"] for link in links if link["rel"] == "latest-version"]
            self.assertEqual(len(child_hrefs), 3)
            self.assertEqual(
                latest,
                ["https://example.test/catalog/hifld/dataset/file/v1.10.0/collection.json"],
            )
            with sqlite3.connect(Path(tmpdir) / "_catalog/catalog.sqlite") as conn:
                projected_latest = conn.execute(
                    "SELECT latest_version FROM files WHERE file_path = ?",
                    ("hifld/dataset/file",),
                ).fetchone()[0]
            self.assertEqual(projected_latest, "v1.10.0")

    def test_exposes_real_dagster_job_and_optional_license_request(self):
        self.assertIsInstance(portolan_publish_job, JobDefinition)
        request = PortolanPublishRequest.from_mapping(
            {
                "collection_slug": "hifld",
                "dataset_slug": "dataset",
                "file_slug": "file",
                "version": "v1.0.0",
                "title": "Fixture",
                "description": "Authored fixture",
                "publisher": "Agency",
            }
        )
        self.assertEqual(request.provider, "Agency")
        self.assertIsNone(request.license_href)
        self.assertEqual(request.resolved_license, ("other", None))

    def test_archived_status_requires_opt_in_and_dataset_license_wins(self):
        archive = PortolanPublishRequest(
            "hifld", "dataset", "file", "v1.0.0", "Title", "Description", "Agency",
            archive_public_domain=True,
        )
        self.assertEqual(
            archive.resolved_license,
            ("CC-PDM-1.0", "../../../LICENSE.md"),
        )
        authored = replace(archive, license_href="metadata/source/LICENSE.md")
        self.assertEqual(
            authored.resolved_license,
            ("other", "metadata/source/LICENSE.md"),
        )

    def test_requests_need_identity_not_handwritten_metadata(self):
        request = PortolanPublishRequest.from_mapping(
            {
                "collection_slug": "hifld",
                "dataset_slug": "dataset",
                "file_slug": "file",
                "version": "v1.0.0",
            }
        )
        self.assertEqual(
            (request.title, request.description, request.provider), ("", "", "")
        )

    def test_manifest_tags_preserve_all_group_names_and_values(self):
        self.assertEqual(
            manifest_tags(
                {
                    "tags": {
                        "categories": ["Agriculture", "Mining"],
                        "inventory_name": "source-name",
                        "empty": [],
                    }
                }
            ),
            (
                ("categories", "Agriculture"),
                ("categories", "Mining"),
                ("inventory_name", "source-name"),
            ),
        )

    def test_loads_all_records_from_fixture_manifest(self):
        records = load_publish_requests(
            {
                "records": [
                    {
                        "collection_slug": "hifld",
                        "dataset_slug": "one",
                        "file_slug": "file",
                        "version": "v1.0.0",
                        "title": "One",
                        "description": "First",
                        "publisher": "Agency",
                    },
                    {
                        "collection_slug": "hifld",
                        "dataset_slug": "two",
                        "file_slug": "file",
                        "version": "v1.0.0",
                        "title": "Two",
                        "description": "Second",
                        "publisher": "Agency",
                    },
                ]
            }
        )
        self.assertEqual([record.dataset_slug for record in records], ["one", "two"])

    def test_executes_inspectable_dagster_run_from_manifest_path(self):
        record = {
            "collection_slug": "hifld",
            "dataset_slug": "one",
            "file_slug": "file",
            "version": "v1.0.0",
            "title": "One",
            "description": "First",
            "publisher": "Agency",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"records": [record]}))
            with patch(
                "dagster_hifld.portolan.workflow.publish_portolan_manifest",
                return_value=("generation",),
            ):
                result = execute_portolan_manifest(manifest, root / "dagster")
            self.assertTrue(result.success)
            self.assertTrue(result.run_id)

    def test_inspection_computes_bounds_when_geo_footer_omits_them(self):
        geo = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "crs": "OGC:CRS84"}},
        }
        table = pa.table(
            {"OBJECTID": [1, 2], "geometry": [Point(-77, 38).wkb, Point(-76, 39).wkb]}
        ).replace_schema_metadata({b"geo": json.dumps(geo).encode()})
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            output = root / "geoparquet/data.parquet"
            output.parent.mkdir()
            pq.write_table(table, output)
            facts = inspect_geoparquet(root)
        self.assertEqual(facts.native_bbox, (-77.0, 38.0, -76.0, 39.0))
        self.assertEqual(facts.crs84_bbox, facts.native_bbox)

    def test_existing_authored_shapefile_zip_is_never_repacked(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            key = storage.write(
                "dataset", "file", "v1.0.0", "shapefile/file.zip", b"pinned-original"
            )
            outputs = _write_and_publish_shapefile_zip(
                storage, "dataset", "file", "v1.0.0"
            )
            self.assertEqual(storage.read_key(key), b"pinned-original")
            self.assertEqual([output.path for output in outputs], [key])

    def test_asset_scan_excludes_preexisting_version_stac(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = PublishedStorageResource(
                local_dir=tmpdir, use_local=True, prefix="hifld"
            )
            storage.write("dataset", "file", "v1.0.0", "pmtiles/file.pmtiles", b"tiles")
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1.0.0", "File", "Description", "Agency"
            )
            before = _record_assets(storage, request)
            storage.write("dataset", "file", "v1.0.0", "collection.json", b"{}")
            assets = _record_assets(storage, request)
        self.assertEqual([asset.format_key for asset in assets], ["pmtiles"])
        self.assertEqual(
            [asset.key for asset in assets], [asset.key for asset in before]
        )

    def test_asset_scan_ignores_legacy_docs_without_verified_hash(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = PublishedStorageResource(
                local_dir=tmpdir, use_local=True, prefix="hifld"
            )
            storage.write("dataset", "file", "v1.0.0", "AGENTS.md", b"legacy docs")
            storage.write("dataset", "file", "v1.0.0", "README.md", b"legacy docs")
            storage.write("dataset", "file", "v1.0.0", "pmtiles/file.pmtiles", b"tiles")
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1.0.0", "File", "Description", "Agency"
            )
            original_snapshot = PublishedStorageResource.object_snapshot

            def snapshot_without_doc_hash(self, key):
                snapshot = original_snapshot(self, key)
                if snapshot is not None and key.endswith(".md"):
                    return replace(snapshot, sha256=None)
                return snapshot

            with patch.object(
                PublishedStorageResource, "object_snapshot", snapshot_without_doc_hash
            ):
                assets = _record_assets(storage, request)

        self.assertEqual([asset.format_key for asset in assets], ["pmtiles"])

    def test_asset_scan_streams_hash_when_object_has_no_sha_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = PublishedStorageResource(
                local_dir=tmpdir, use_local=True, prefix="hifld"
            )
            storage.write("dataset", "file", "v1.0.0", "pmtiles/file.pmtiles", b"tiles")
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1.0.0", "File", "Description", "Agency"
            )
            original_snapshot = PublishedStorageResource.object_snapshot

            def snapshot_without_hash(self, key):
                snapshot = original_snapshot(self, key)
                return replace(snapshot, sha256=None) if snapshot is not None else None

            with (
                patch.object(PublishedStorageResource, "object_snapshot", snapshot_without_hash),
                patch.object(PublishedStorageResource, "read_key", side_effect=AssertionError("do not buffer data assets")),
            ):
                assets = _record_assets(storage, request)

        self.assertEqual(assets[0].sha256, hashlib.sha256(b"tiles").hexdigest())

    def test_catalog_only_promotes_exact_source_metadata_without_data_assets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            staging = StagingStorageResource(
                local_dir=str(root / "staging"), use_local=True, prefix="hifld"
            )
            published = PublishedStorageResource(
                local_dir=str(root / "published"), use_local=True, prefix="hifld"
            )
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1.0.0", "File", "Description", "Agency"
            )
            expected = {
                "data_dictionary.json": b'{"columns":[]}',
                "quality_manifest.json": b'{"quality_check_passed":true}',
                "source_manifest.json": b'{"source":"production"}',
            }
            for filename, contents in expected.items():
                staging.write(
                    "dataset",
                    "file",
                    "v1.0.0",
                    f"metadata/source/{filename}",
                    contents,
                )

            _promote_source_metadata(staging, published, request)

            for filename, contents in expected.items():
                self.assertEqual(
                    published.read_bytes(
                        "dataset",
                        "file",
                        "v1.0.0",
                        f"metadata/source/{filename}",
                    ),
                    contents,
                )
            self.assertEqual(
                published.list_keys("dataset", "file", "v1.0.0"),
                [
                    f"hifld/dataset/file/v1.0.0/metadata/source/{filename}"
                    for filename in sorted(expected)
                ],
            )

    def test_catalog_only_can_promote_every_staged_data_format_byte_for_byte(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            staging = StagingStorageResource(
                local_dir=str(root / "staging"), use_local=True, prefix="hifld"
            )
            published = PublishedStorageResource(
                local_dir=str(root / "published"), use_local=True, prefix="hifld"
            )
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1.1.0", "", "", ""
            )
            expected = {
                "geopackage/file.gpkg": b"original-gpkg",
                "geoparquet/file.parquet": b"original-parquet",
                "pmtiles/file.pmtiles": b"original-pmtiles",
                "shapefile/file.zip": b"original-shapefile",
            }
            for relative_path, contents in expected.items():
                staging.write("dataset", "file", "v1.1.0", relative_path, contents)

            _promote_staged_data(staging, published, request)

            for relative_path, contents in expected.items():
                self.assertEqual(
                    published.read_bytes("dataset", "file", "v1.1.0", relative_path),
                    contents,
                )

    def test_published_stac_document_gets_absolute_self_link(self):
        document = {"links": [{"rel": "root", "href": "../../catalog.json"}]}

        result = _ensure_absolute_self_link(
            document,
            "hifld/hospitals-3/hospitals-3/v1.1.0/collection.json",
            "http://localhost:8333/hifld-local-published",
        )

        self.assertEqual(
            result["links"],
            [
                {"rel": "root", "href": "../../catalog.json"},
                {
                    "rel": "self",
                    "href": "http://localhost:8333/hifld-local-published/hifld/hospitals-3/hospitals-3/v1.1.0/collection.json",
                    "type": "application/json",
                },
            ],
        )

    def test_dictionary_defines_schema_independently_of_parquet_columns(self):
        enriched = columns_from_dictionary(
            {
                "columns": [
                    {
                        "name": "OBJECTID",
                        "type": "integer",
                        "nullable": False,
                        "description": "Stable identifier",
                        "exampleValues": [1, 2],
                        "possibleValues": [1, 2],
                        "min": 1,
                        "max": 2,
                        "length": 2,
                        "numNullValues": 0,
                        "numUniqueValues": 2,
                    },
                    {"name": "ID", "type": "string", "nullable": False},
                ]
            },
        )
        self.assertEqual(enriched[0].description, "Stable identifier")
        self.assertEqual(enriched[0].example_values, (1, 2))
        self.assertEqual(enriched[0].unique_count, 2)
        self.assertEqual([column.name for column in enriched], ["OBJECTID", "ID"])
        self.assertEqual(enriched[0].data_type, "integer")
        self.assertFalse(enriched[1].nullable)

    def test_empty_dictionary_does_not_fall_back_to_physical_schema(self):
        self.assertEqual(columns_from_dictionary({"columns": []}), ())

    def test_authored_iso_date_normalizes_to_rfc3339(self):
        self.assertEqual(normalize_stac_datetime("2017-06-12"), "2017-06-12T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
