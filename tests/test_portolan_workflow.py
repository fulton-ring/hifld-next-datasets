import gzip
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

from dagster_hifld.assets.portolan import publish_portolan_catalog
from dagster_hifld.assets.publish import _write_and_publish_shapefile_zip
from dagster_hifld.partitions import PUBLISH_PARTITIONS
from dagster_hifld.portolan.catalog import CatalogRecord
from dagster_hifld.portolan.release import ReleasePointer
from dagster_hifld.portolan.validation import PortolanValidationError
from dagster_hifld.portolan.workflow import (
    GeoParquetFacts,
    PortolanPublishRequest,
    _ensure_absolute_self_link,
    _prepare_portolan_record,
    _promote_source_metadata,
    _promote_staged_data,
    _publish_catalog,
    _record_assets,
    _version_metadata_path,
    _write_default_pmtiles_style,
    columns_from_dictionary,
    execute_portolan_manifest,
    inspect_geoparquet,
    load_publish_requests,
    manifest_tags,
    normalize_stac_datetime,
    portolan_publish_job,
    publish_portolan_record,
    rollback_portolan_release,
)
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


def _pmtiles_archive(layer_id: str = "roads") -> bytes:
    payload = gzip.compress(
        json.dumps({"vector_layers": [{"id": layer_id}]}).encode("utf-8")
    )
    header = bytearray(127)
    header[:7] = b"PMTiles"
    header[7] = 3
    header[24:32] = (127).to_bytes(8, "little")
    header[32:40] = len(payload).to_bytes(8, "little")
    header[97] = 2
    return bytes(header) + payload


class PortolanWorkflowTests(unittest.TestCase):
    def test_release_pointer_stays_selected_when_candidate_validation_rejects(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            request = PortolanPublishRequest(
                "hifld",
                "dataset",
                "file",
                "v1.0.0",
                "File",
                "Description",
                "Agency",
                public_root="https://example.test/catalog",
            )
            record = CatalogRecord(
                "hifld",
                "dataset",
                "file",
                "v1.0.0",
                "File",
                "Description",
                "non_spatial_source",
                0,
                (),
                collection_title="HIFLD Next",
            )
            _publish_catalog((record,), request, published, use_release_pointer=True)
            before = published.read_key("_catalog/current.json")

            with (
                patch(
                    "dagster_hifld.portolan.workflow.validate_candidate_tree",
                    side_effect=PortolanValidationError("invalid candidate"),
                ),
                self.assertRaisesRegex(PortolanValidationError, "invalid candidate"),
            ):
                _publish_catalog(
                    (record,), request, published, use_release_pointer=True
                )

            self.assertEqual(published.read_key("_catalog/current.json"), before)

    def test_release_publication_commits_pointer_after_uploading_a_complete_bundle(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            request = PortolanPublishRequest(
                "hifld",
                "dataset",
                "file",
                "v1.0.0",
                "File",
                "Description",
                "Agency",
                public_root="https://example.test/catalog",
            )
            record = CatalogRecord(
                "hifld",
                "dataset",
                "file",
                "v1.0.0",
                "File",
                "Description",
                "non_spatial_source",
                0,
                (),
                collection_title="HIFLD Next",
            )

            generation = _publish_catalog(
                (record,), request, published, use_release_pointer=True
            )
            pointer = ReleasePointer.parse(published.read_key("_catalog/current.json"))
            self.assertEqual(pointer.generation, generation)
            self.assertEqual(pointer.root_key, f"releases/{generation}/catalog.json")
            self.assertTrue(published.object_exists(pointer.catalog_key))
            self.assertTrue(published.object_exists(pointer.root_key))
            root_catalog = json.loads(published.read_key(pointer.root_key))
            root_links = {
                link["rel"]: link["href"]
                for link in root_catalog["links"]
                if link["rel"] in {"root", "self"}
            }
            expected_root = (
                f"https://example.test/catalog/releases/{generation}/catalog.json"
            )
            self.assertEqual(root_links, {"root": expected_root, "self": expected_root})

    def test_release_rollback_conditionally_restores_a_complete_prior_generation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            request = PortolanPublishRequest(
                "hifld",
                "dataset",
                "file",
                "v1.0.0",
                "File",
                "Description",
                "Agency",
                public_root="https://example.test/catalog",
            )
            record = CatalogRecord(
                "hifld",
                "dataset",
                "file",
                "v1.0.0",
                "File",
                "Description",
                "non_spatial_source",
                0,
                (),
                collection_title="HIFLD Next",
            )

            first_generation = _publish_catalog(
                (record,), request, published, use_release_pointer=True
            )
            second_generation = _publish_catalog(
                (record,), request, published, use_release_pointer=True
            )
            current_snapshot = published.object_snapshot("_catalog/current.json")
            self.assertIsNotNone(current_snapshot)
            self.assertNotEqual(first_generation, second_generation)

            second_root = json.loads(
                published.read_key(f"releases/{second_generation}/catalog.json")
            )
            second_hrefs = [link["href"] for link in second_root["links"]]
            self.assertIn(
                f"https://example.test/catalog/releases/{second_generation}/hifld/catalog.json",
                second_hrefs,
            )
            self.assertFalse(
                any(f"/releases/{first_generation}/" in href for href in second_hrefs)
            )

            restored = rollback_portolan_release(
                first_generation,
                published=published,
                expected_snapshot=current_snapshot,
            )

            self.assertEqual(restored.generation, first_generation)
            self.assertEqual(
                ReleasePointer.parse(
                    published.read_key("_catalog/current.json")
                ).generation,
                first_generation,
            )

    def test_terminal_portolan_asset_is_partitioned_with_promotion(self):
        self.assertIs(publish_portolan_catalog.partitions_def, PUBLISH_PARTITIONS)

    def test_terminal_asset_publishes_selected_promoted_partition_only(self):
        context = SimpleNamespace(partition_key="dataset/file/v1.0.0")
        staging = StagingStorageResource(
            local_dir="/tmp/staging", use_local=True, prefix="hifld"
        )
        published = PublishedStorageResource(
            local_dir="/tmp/published", use_local=True, prefix="hifld"
        )
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
            (
                request.collection_slug,
                request.dataset_slug,
                request.file_slug,
                request.version,
            ),
            ("hifld", "dataset", "file", "v1.0.0"),
        )
        self.assertEqual(request.public_root, "https://example.test/catalog")
        self.assertEqual(request.collection_title, "HIFLD Next")
        self.assertTrue(request.archive_public_domain)
        self.assertEqual(
            request.resolved_license, ("CC-PDM-1.0", "../../../LICENSE.md")
        )
        self.assertTrue(kwargs["catalog_only"])
        self.assertTrue(kwargs["use_release_pointer"])

    def test_terminal_asset_opts_into_archived_hifld_status_when_configured(self):
        context = SimpleNamespace(partition_key="dataset/file/v1.0.0")
        staging = StagingStorageResource(
            local_dir="/tmp/staging", use_local=True, prefix="hifld"
        )
        published = PublishedStorageResource(
            local_dir="/tmp/published", use_local=True, prefix="hifld"
        )
        with (
            patch.dict(
                "os.environ",
                {
                    "HIFLD_PORTOLAN_ENABLED": "1",
                    "HIFLD_PORTOLAN_ARCHIVE_PUBLIC_DOMAIN": "1",
                },
            ),
            patch(
                "dagster_hifld.portolan.workflow.publish_portolan_record",
                return_value="generation-1",
            ) as publish,
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
                "hifld",
                "first",
                "file",
                "v1.0.0",
                "First",
                "First",
                "Agency",
                public_root="https://example.test/catalog",
            )
            first = CatalogRecord(
                "hifld",
                "first",
                "file",
                "v1.0.0",
                "First",
                "First",
                "non_spatial_source",
                0,
                (),
                collection_title="HIFLD Next",
            )
            second = CatalogRecord(
                "hifld",
                "second",
                "file",
                "v1.0.0",
                "Second",
                "Second",
                "non_spatial_source",
                0,
                (),
                collection_title="HIFLD",
            )
            _publish_catalog((first,), request, published)
            _publish_catalog((second,), request, published)

            collection = json.loads(published.read_key("hifld/catalog.json"))
            child_hrefs = {
                link["href"] for link in collection["links"] if link["rel"] == "child"
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
            self.assertEqual(root_title, "HIFLD Next")
            root_catalog = json.loads(published.read_key("catalog.json"))
            self.assertEqual(root_catalog["title"], "HIFLD Next")
            self.assertEqual(collection["title"], "HIFLD")

    def test_root_catalog_ignores_data_prefix(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(
                local_dir=tmpdir, use_local=True, prefix="hifld"
            )
            request = PortolanPublishRequest(
                "hifld",
                "dataset",
                "file",
                "v1.0.0",
                "File",
                "File",
                "Agency",
                public_root="https://example.test/catalog",
            )
            record = CatalogRecord(
                "hifld",
                "dataset",
                "file",
                "v1.0.0",
                "File",
                "File",
                "non_spatial_source",
                0,
                (),
                collection_title="HIFLD Next",
            )
            _publish_catalog((record,), request, published)
            self.assertTrue((Path(tmpdir) / "catalog.json").is_file())
            self.assertTrue((Path(tmpdir) / "hifld/catalog.json").is_file())
            self.assertFalse((Path(tmpdir) / "hifld/hifld/catalog.json").exists())

    def test_incremental_version_publication_keeps_prior_version_and_latest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            published = PublishedStorageResource(local_dir=tmpdir, use_local=True)
            request = PortolanPublishRequest(
                "hifld",
                "dataset",
                "file",
                "v1.0.0",
                "File",
                "File",
                "Agency",
                public_root="https://example.test/catalog",
            )
            for version in ("v1.9.0", "v1.10.0", "v1.0.0"):
                record = CatalogRecord(
                    "hifld",
                    "dataset",
                    "file",
                    version,
                    "File",
                    "File",
                    "non_spatial_source",
                    0,
                    (),
                    collection_title="HIFLD Next",
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
                [
                    "https://example.test/catalog/hifld/dataset/file/v1.10.0/collection.json"
                ],
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
            "hifld",
            "dataset",
            "file",
            "v1.0.0",
            "Title",
            "Description",
            "Agency",
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

    def test_request_uses_configured_public_root_when_manifest_omits_it(self):
        with patch.dict(
            "os.environ",
            {
                "HIFLD_PORTOLAN_PUBLIC_ROOT": "https://catalog.example.test",
                "HIFLD_PORTOLAN_STORAGE_SLUG": "gcs-catalog",
            },
        ):
            request = PortolanPublishRequest.from_mapping(
                {
                    "collection_slug": "hifld",
                    "dataset_slug": "dataset",
                    "file_slug": "file",
                    "version": "v1.0.0",
                }
            )

        self.assertEqual(request.public_root, "https://catalog.example.test")
        self.assertEqual(request.storage_slug, "gcs-catalog")

    def test_version_metadata_path_uses_copied_production_layout_when_needed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata = (
                Path(tmpdir) / "hifld/dataset/file/v1.0.0/metadata/data_dictionary.json"
            )
            metadata.parent.mkdir(parents=True)
            metadata.write_text("{}", encoding="utf-8")
            storage = StagingStorageResource(
                local_dir=tmpdir, use_local=True, prefix="hifld"
            )
            request = PortolanPublishRequest(
                "hifld",
                "dataset",
                "file",
                "v1.0.0",
                "",
                "",
                "",
            )

            path = _version_metadata_path(storage, request, "data_dictionary.json")

        self.assertEqual(path, "metadata/data_dictionary.json")

    def test_catalog_only_record_publication_does_not_promote_source_objects(self):
        request = PortolanPublishRequest(
            "hifld", "dataset", "file", "v1.0.0", "", "", ""
        )
        staging = StagingStorageResource(local_dir="/tmp/staging", use_local=True)
        published = PublishedStorageResource(local_dir="/tmp/published", use_local=True)
        record = CatalogRecord(
            "hifld",
            "dataset",
            "file",
            "v1.0.0",
            "Title",
            "Description",
            "spatial",
            0,
            (),
        )
        with (
            patch(
                "dagster_hifld.portolan.workflow._prepare_portolan_record",
                return_value=record,
            ) as prepare,
            patch(
                "dagster_hifld.portolan.workflow._publish_catalog",
                return_value="generation",
            ),
        ):
            publish_portolan_record(
                request, staging=staging, published=published, catalog_only=True
            )

        self.assertFalse(prepare.call_args.kwargs["convert"])
        self.assertFalse(prepare.call_args.kwargs["promote"])

    def test_catalog_only_record_preserves_authored_version_note_and_bounds(self):
        note = (
            "Updated hospital bed counts.\n\n"
            "Update support provided by [Niyam IT](https://niyamit.com).\n\n"
            "![Niyam IT icon](https://niyamit.com/favicon.ico)"
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            staging = StagingStorageResource(
                local_dir=str(root / "staging"), use_local=True, prefix="hifld"
            )
            published = PublishedStorageResource(
                local_dir=str(root / "published"), use_local=True, prefix="hifld"
            )
            request = PortolanPublishRequest(
                "hifld", "hospitals-3", "hospitals-3", "v1.1.0", "", "", ""
            )
            staging.write_key(
                "metadata/source/collections.json",
                b'[{"slug":"hifld","name":"HIFLD","description":"Catalog"}]',
            )
            staging.write_key(
                "hospitals-3/metadata/source/source_manifest.json",
                b'{"title":"Hospitals"}',
            )
            staging.write_key(
                "hospitals-3/hospitals-3/metadata/source/source_manifest.json",
                b'{"title":"Hospitals"}',
            )
            staging.write(
                "hospitals-3",
                "hospitals-3",
                "v1.1.0",
                "metadata/source/source_manifest.json",
                b"{}",
            )
            staging.write(
                "hospitals-3",
                "hospitals-3",
                "v1.1.0",
                "metadata/source/data_dictionary.json",
                b'{"title":"Hospitals","description":"Hospital locations","columns":[]}',
            )
            staging.write(
                "hospitals-3",
                "hospitals-3",
                "v1.1.0",
                "metadata/source/quality_manifest.json",
                json.dumps(
                    {
                        "feature_count": 1,
                        "quality_check_passed": True,
                        "description": note,
                        "bounds": [-77.1, 37.9, -75.9, 39.1],
                    }
                ).encode(),
            )
            facts = GeoParquetFacts(
                1,
                "OGC:CRS84",
                "geometry",
                "Point",
                None,
                (-77.1, 37.9, -75.9, 39.1),
                (-77.1, 37.9, -75.9, 39.1),
                (),
            )
            with (
                patch(
                    "dagster_hifld.portolan.workflow.inspect_geoparquet",
                    return_value=facts,
                ),
                patch(
                    "dagster_hifld.portolan.workflow.render_geoparquet_thumbnail",
                    return_value=None,
                ),
            ):
                record = _prepare_portolan_record(
                    request, staging, published, convert=False, promote=False
                )

        self.assertEqual(record.collection_title, "HIFLD")
        self.assertEqual(record.source_version_description, note)
        self.assertEqual(record.source_version_bounds, (-77.1, 37.9, -75.9, 39.1))

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
            storage.write(
                "dataset", "file", "v1.0.0", "pmtiles/file.pmtiles", _pmtiles_archive()
            )
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1.0.0", "File", "Description", "Agency"
            )
            before = _record_assets(storage, request)
            storage.write("dataset", "file", "v1.0.0", "collection.json", b"{}")
            assets = _record_assets(storage, request)
        self.assertEqual([asset.format_key for asset in assets], ["pmtiles"])
        self.assertEqual(assets[0].pmtiles_layers, ("roads",))
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
            storage.write(
                "dataset", "file", "v1.0.0", "pmtiles/file.pmtiles", _pmtiles_archive()
            )
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
            archive = _pmtiles_archive()
            storage.write("dataset", "file", "v1.0.0", "pmtiles/file.pmtiles", archive)
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1.0.0", "File", "Description", "Agency"
            )
            original_snapshot = PublishedStorageResource.object_snapshot

            def snapshot_without_hash(self, key):
                snapshot = original_snapshot(self, key)
                return replace(snapshot, sha256=None) if snapshot is not None else None

            with (
                patch.object(
                    PublishedStorageResource, "object_snapshot", snapshot_without_hash
                ),
                patch.object(
                    PublishedStorageResource,
                    "read_key",
                    side_effect=AssertionError("do not buffer data assets"),
                ),
            ):
                assets = _record_assets(storage, request)

        self.assertEqual(assets[0].sha256, hashlib.sha256(archive).hexdigest())

    def test_default_style_uses_verified_pmtiles_layer_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = PublishedStorageResource(
                local_dir=tmpdir, use_local=True, prefix="hifld"
            )
            storage.write(
                "dataset", "file", "v1.0.0", "pmtiles/file.pmtiles", _pmtiles_archive()
            )
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1.0.0", "File", "Description", "Agency"
            )
            pmtiles_assets = _record_assets(storage, request)

            self.assertTrue(
                _write_default_pmtiles_style(storage, request, pmtiles_assets)
            )
            assets = _record_assets(storage, request)
            style = next(asset for asset in assets if asset.format_key == "styles")
            document = json.loads(storage.read_key(style.object_key or ""))

        self.assertEqual(style.roles, ("style", "default"))
        self.assertEqual(style.media_type, "application/vnd.mapbox.style+json")
        self.assertEqual(
            document["sources"]["pmtiles-0"]["url"],
            "pmtiles://http://localhost:8333/hifld-local-published/hifld/dataset/file/v1.0.0/pmtiles/file.pmtiles",
        )
        self.assertEqual(document["layers"][0]["source-layer"], "roads")

    def test_asset_scan_marks_generated_thumbnail_as_a_thumbnail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = PublishedStorageResource(
                local_dir=tmpdir, use_local=True, prefix="hifld"
            )
            storage.write(
                "dataset", "file", "v1.0.0", "thumbnail/thumbnail.png", b"png"
            )
            request = PortolanPublishRequest(
                "hifld", "dataset", "file", "v1.0.0", "File", "Description", "Agency"
            )

            assets = _record_assets(storage, request)

        self.assertEqual(len(assets), 1)
        self.assertEqual(assets[0].format_key, "thumbnail")
        self.assertEqual(assets[0].media_type, "image/png")
        self.assertEqual(assets[0].roles, ("thumbnail",))

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
