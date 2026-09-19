import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from dagster_hifld.portolan.catalog import (
    CATALOG_APPLICATION_ID,
    CATALOG_SCHEMA_VERSION,
    AssetRecord,
    CatalogRecord,
    ColumnRecord,
    build_catalog_sqlite,
    render_portolan_tree,
    update_catalog_sqlite,
)


class PortolanCatalogTests(unittest.TestCase):
    def test_builds_normalized_sqlite_with_full_path_identity_and_latest(self):
        record = CatalogRecord(
            collection_slug="hifld",
            dataset_slug="example-dataset",
            file_slug="example-file",
            version_label="v1.0.0",
            title="Example file",
            description="A documented example.",
            spatial_status="spatial",
            feature_count=2,
            assets=(
                AssetRecord(
                    key="geoparquet",
                    format_key="geoparquet",
                    title="GeoParquet",
                    href="hifld/example-dataset/example-file/v1.0.0/geoparquet/data.parquet",
                    media_type="application/vnd.apache.parquet",
                    size_bytes=3,
                    sha256="a" * 64,
                ),
            ),
            tags=("example", "boundaries"),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Path(tmpdir) / "catalog.sqlite"
            build_catalog_sqlite(database, (record,), catalog_generation="generation-1")
            connection = sqlite3.connect(database)
            self.assertEqual(
                connection.execute("PRAGMA application_id").fetchone()[0],
                CATALOG_APPLICATION_ID,
            )
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0],
                CATALOG_SCHEMA_VERSION,
            )
            self.assertEqual(
                connection.execute("SELECT file_path FROM files").fetchone()[0],
                "hifld/example-dataset/example-file",
            )
            self.assertEqual(
                connection.execute("SELECT is_latest FROM versions").fetchone()[0], 1
            )
            self.assertEqual(
                connection.execute(
                    "SELECT catalog_generation FROM catalog_metadata"
                ).fetchone()[0],
                "generation-1",
            )
            connection.close()

    def test_renders_stac_tree_and_does_not_link_unprefixed_asset(self):
        record = CatalogRecord(
            collection_slug="hifld",
            dataset_slug="example-dataset",
            file_slug="example-file",
            version_label="v1.0.0",
            title="Example file",
            description="A documented example.",
            spatial_status="non_spatial_source",
            feature_count=2,
            assets=(
                AssetRecord(
                    key="parquet",
                    format_key="parquet",
                    title="Parquet",
                    href="hifld/example-dataset/example-file/v1.0.0/parquet/data.parquet",
                    media_type="application/vnd.apache.parquet",
                    size_bytes=3,
                    sha256="b" * 64,
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            render_portolan_tree(Path(tmpdir), (record,))
            collection = (
                Path(tmpdir)
                / "hifld/example-dataset/example-file/v1.0.0/collection.json"
            )
            self.assertIn('"type": "Collection"', collection.read_text())
            self.assertIn(
                "hifld/example-dataset/example-file/v1.0.0/parquet/data.parquet",
                collection.read_text(),
            )
            self.assertTrue((Path(tmpdir) / "catalog.json").is_file())
            self.assertTrue((Path(tmpdir) / "hifld/README.md").is_file())

    def test_rendered_stac_uses_absolute_internal_links_when_public_root_is_set(self):
        record = CatalogRecord(
            "hifld",
            "dataset",
            "file",
            "v1.0.0",
            "File",
            "Description",
            "non_spatial_source",
            0,
            (
                AssetRecord(
                    "parquet",
                    "parquet",
                    "Parquet",
                    "hifld/dataset/file/v1.0.0/parquet/data.parquet",
                    "application/vnd.apache.parquet",
                    3,
                    "a" * 64,
                ),
            ),
            source_url="https://source.example.test/data",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            render_portolan_tree(root, (record,), public_root="http://seaweed.test:8333/bucket")
            documents = tuple(
                json.loads(path.read_text())
                for path in root.rglob("*.json")
                if path.name in {"catalog.json", "collection.json"}
            )

        for document in documents:
            for link in document["links"]:
                self.assertTrue(
                    link["href"].startswith(("http://seaweed.test:8333/bucket/", "https://source.example.test/")),
                    link,
                )
        collection = next(document for document in documents if document["type"] == "Collection")
        self.assertEqual(
            collection["assets"]["parquet"]["href"],
            "http://seaweed.test:8333/bucket/hifld/dataset/file/v1.0.0/parquet/data.parquet",
        )

    def test_sqlite_leaves_missing_file_and_version_timestamps_null(self):
        record = CatalogRecord(
            "hifld", "dataset", "file", "v1", "File", "Description", "non_spatial_source", 0, ()
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Path(tmpdir) / "catalog.sqlite"
            build_catalog_sqlite(database, (record,))
            connection = sqlite3.connect(database)
            self.assertEqual(
                connection.execute("SELECT created_at, updated_at FROM files").fetchone(),
                (None, None),
            )
            self.assertEqual(
                connection.execute("SELECT created_at, updated_at FROM versions").fetchone(),
                (None, None),
            )
            connection.close()

    def test_renders_portolan_navigation_and_pmtiles_discovery_links(self):
        record = CatalogRecord(
            "hifld",
            "example-dataset",
            "example-file",
            "v1.0.0",
            "Example file",
            "A documented example.",
            "non_spatial_source",
            0,
            (
                AssetRecord(
                    "pmtiles",
                    "pmtiles",
                    "Example map tiles",
                    "hifld/example-dataset/example-file/v1.0.0/pmtiles/data.pmtiles",
                    "application/vnd.pmtiles",
                    3,
                    "a" * 64,
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            render_portolan_tree(root, (record,))
            root_document = json.loads((root / "catalog.json").read_text())
            collection_document = json.loads(
                (
                    root
                    / "hifld/example-dataset/example-file/v1.0.0/collection.json"
                ).read_text()
            )
            self.assertIn(
                "https://schemas.portolan-sdi.org/portolan/v0.2.0/schema.json",
                root_document["stac_extensions"],
            )
            self.assertEqual(
                {link["rel"] for link in root_document["links"]},
                {"root", "self", "agents", "describedby", "child"},
            )
            self.assertEqual(
                next(
                    link["title"]
                    for link in root_document["links"]
                    if link["rel"] == "child"
                ),
                "hifld",
            )
            self.assertEqual(
                collection_document["extent"]["spatial"]["bbox"],
                [[-180.0, -90.0, 180.0, 90.0]],
            )
            self.assertIn(
                "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json",
                collection_document["stac_extensions"],
            )
            pmtiles = next(
                link
                for link in collection_document["links"]
                if link["rel"] == "pmtiles"
            )
            self.assertEqual(pmtiles["type"], "application/vnd.pmtiles")

    def test_renders_source_md5_multihash_without_fabricating_sha256(self):
        record = CatalogRecord(
            "hifld",
            "example-dataset",
            "example-file",
            "v1.0.0",
            "Example file",
            "A documented example.",
            "non_spatial_source",
            0,
            (
                AssetRecord(
                    key="parquet",
                    format_key="parquet",
                    title="Parquet",
                    href="https://example.test/hifld/data.parquet",
                    media_type="application/vnd.apache.parquet",
                    size_bytes=3,
                    sha256=None,
                    checksum_multihash="d50110000102030405060708090a0b0c0d0e0f",
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            database = root / "catalog.sqlite"
            build_catalog_sqlite(database, (record,))
            connection = sqlite3.connect(database)
            self.assertEqual(
                connection.execute(
                    "SELECT sha256, checksum_multihash FROM assets"
                ).fetchone(),
                (None, "d50110000102030405060708090a0b0c0d0e0f"),
            )
            connection.close()
            render_portolan_tree(root, (record,))
            document = json.loads(
                (
                    root
                    / "hifld/example-dataset/example-file/v1.0.0/collection.json"
                ).read_text()
            )
            self.assertEqual(
                document["assets"]["parquet"]["file:checksum"],
                "d50110000102030405060708090a0b0c0d0e0f",
            )

    def test_version_stac_preserves_provider_schema_and_unknown_license(self):
        record = CatalogRecord(
            "hifld",
            "dataset",
            "file",
            "v1.0.0",
            "File",
            "Description",
            "spatial",
            1,
            (),
            provider="Authoritative Agency",
            columns=(
                ColumnRecord(
                    "OBJECTID",
                    "int64",
                    0,
                    False,
                    description="Stable source identifier",
                    null_count=0,
                    unique_count=2,
                    min_value="1",
                    max_value="2",
                    example_values=(1, 2),
                    possible_values=(1, 2),
                    length=2,
                ),
                ColumnRecord("geometry", "binary", 1, True, is_geometry=True),
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            render_portolan_tree(Path(tmpdir), (record,))
            path = Path(tmpdir) / "hifld/dataset/file/v1.0.0/collection.json"
            document = json.loads(path.read_text())
            self.assertEqual(document["license"], "other")
            self.assertNotIn("license", {link["rel"] for link in document["links"]})
            self.assertEqual(document["providers"], [{"name": "Authoritative Agency"}])
            self.assertEqual(document["table:columns"][0]["name"], "OBJECTID")
            self.assertEqual(
                document["table:columns"][0]["description"],
                "Stable source identifier",
            )
            self.assertEqual(document["table:columns"][0]["exampleValues"], [1, 2])
            self.assertEqual(document["table:columns"][0]["numUniqueValues"], 2)
            self.assertEqual(document["table:columns"][1]["is_geometry"], True)

    def test_archived_hifld_record_renders_public_domain_mark_and_notice(self):
        record = CatalogRecord(
            "hifld", "dataset", "file", "v1.0.0", "File", "Description",
            "non_spatial_source", 0, (), license_id="CC-PDM-1.0",
            license_href="../../../LICENSE.md",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            render_portolan_tree(root, (record,), public_root="https://example.test")
            document = json.loads(
                (root / "hifld/dataset/file/v1.0.0/collection.json").read_text()
            )
            self.assertEqual(document["license"], "CC-PDM-1.0")
            self.assertIn(
                {
                    "rel": "license",
                    "href": "https://example.test/hifld/LICENSE.md",
                    "type": "text/markdown",
                },
                document["links"],
            )
            notice = (root / "hifld/LICENSE.md").read_text()
            self.assertIn("Public Domain Mark 1.0", notice)
            self.assertIn("archived HIFLD Open", notice)
            self.assertIn("not a new license grant", notice)

    def test_incremental_update_preserves_an_unrelated_file(self):
        def record(dataset: str) -> CatalogRecord:
            return CatalogRecord(
                "hifld",
                dataset,
                "file",
                "v1",
                dataset,
                "description",
                "non_spatial_source",
                1,
                (),
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            database = Path(tmpdir) / "catalog.sqlite"
            build_catalog_sqlite(database, (record("first"),), catalog_generation="one")
            update_catalog_sqlite(
                database, (record("second"),), catalog_generation="two"
            )
            connection = sqlite3.connect(database)
            self.assertEqual(
                connection.execute("SELECT count(*) FROM files").fetchone()[0], 2
            )
            self.assertEqual(
                connection.execute(
                    "SELECT catalog_generation FROM catalog_metadata"
                ).fetchone()[0],
                "two",
            )

    def test_incremental_update_replaces_authoritative_metadata_and_tags(self):
        fixture = CatalogRecord(
            "hifld",
            "dataset",
            "file",
            "v1",
            "Fixture file",
            "Fixture file description",
            "non_spatial_source",
            1,
            (),
            collection_title="Fixture collection",
            collection_description="Fixture collection description",
            dataset_title="Fixture dataset",
            dataset_description="Fixture dataset description",
            dataset_tags=(("theme", "old"),),
            file_tags=(("status", "retired"),),
        )
        authoritative = CatalogRecord(
            "hifld",
            "dataset",
            "file",
            "v1",
            "Authoritative file",
            "Authoritative file description",
            "non_spatial_source",
            1,
            (),
            collection_title="Authoritative collection",
            collection_description="Authoritative collection description",
            dataset_title="Authoritative dataset",
            dataset_description="Authoritative dataset description",
            dataset_tags=(("theme", "new"), ("theme", "current")),
            file_tags=(("status", "active"),),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Path(tmpdir) / "catalog.sqlite"
            build_catalog_sqlite(database, (fixture,))
            update_catalog_sqlite(database, (authoritative,))
            connection = sqlite3.connect(database)
            self.assertEqual(
                connection.execute(
                    "SELECT title, description FROM collections"
                ).fetchone(),
                ("Authoritative collection", "Authoritative collection description"),
            )
            self.assertEqual(
                connection.execute(
                    "SELECT title, description FROM datasets"
                ).fetchone(),
                ("Authoritative dataset", "Authoritative dataset description"),
            )
            self.assertEqual(
                connection.execute("SELECT title, description FROM files").fetchone(),
                ("Authoritative file", "Authoritative file description"),
            )
            self.assertEqual(
                connection.execute(
                    "SELECT entity_path, tag_key, tag_value FROM tags "
                    "ORDER BY entity_path, tag_key, tag_value"
                ).fetchall(),
                [
                    ("hifld/dataset", "theme", "current"),
                    ("hifld/dataset", "theme", "new"),
                    ("hifld/dataset/file", "status", "active"),
                ],
            )

    def test_parent_catalogs_preserve_grouped_source_tags(self):
        record = CatalogRecord(
            "hifld",
            "dataset",
            "file",
            "v1",
            "File",
            "File description",
            "non_spatial_source",
            1,
            (),
            collection_description="Authoritative collection description",
            dataset_title="Dataset title",
            dataset_description="Dataset description",
            tags=("searchable",),
            dataset_tags=(("theme", "energy"), ("theme", "infrastructure")),
            file_tags=(("format", "tabular"),),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            render_portolan_tree(root, (record,))
            collection = json.loads((root / "hifld/catalog.json").read_text())
            dataset = json.loads((root / "hifld/dataset/catalog.json").read_text())
            file = json.loads((root / "hifld/dataset/file/catalog.json").read_text())
            version = json.loads(
                (root / "hifld/dataset/file/v1/collection.json").read_text()
            )
            self.assertEqual(collection["title"], "hifld")
            self.assertEqual(
                collection["description"], "Authoritative collection description"
            )
            self.assertEqual(dataset["title"], "Dataset title")
            self.assertEqual(dataset["description"], "Dataset description")
            self.assertEqual(
                dataset["hifld:tags"], {"theme": ["energy", "infrastructure"]}
            )
            self.assertEqual(file["hifld:tags"], {"format": "tabular"})
            self.assertEqual(dataset["keywords"], ["searchable"])
            self.assertEqual(file["keywords"], ["searchable"])
            self.assertEqual(version["keywords"], ["searchable"])

    def test_root_metadata_comes_from_the_single_authoritative_collection(self):
        fixture = CatalogRecord(
            "hifld",
            "dataset",
            "file",
            "v1",
            "Fixture file",
            "Fixture description",
            "non_spatial_source",
            1,
            (),
            collection_title="Fixture root",
            collection_description="Fixture root description",
        )
        authoritative = CatalogRecord(
            "hifld",
            "dataset",
            "file",
            "v1",
            "File fallback must not appear",
            "File description fallback must not appear",
            "non_spatial_source",
            1,
            (),
            collection_title="Authoritative root",
            collection_description="Authoritative root description",
            collection_created_at="2020-01-01T00:00:00Z",
            collection_updated_at="2024-01-01T00:00:00Z",
            dataset_title="",
            dataset_description="",
            dataset_created_at="2021-01-01T00:00:00Z",
            dataset_updated_at="2023-01-01T00:00:00Z",
            created_at="2022-01-01T00:00:00Z",
            updated_at="2025-01-01T00:00:00Z",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            database = root / "catalog.sqlite"
            build_catalog_sqlite(database, (fixture,))
            update_catalog_sqlite(database, (authoritative,))
            render_portolan_tree(root, (authoritative,))
            connection = sqlite3.connect(database)
            self.assertEqual(
                connection.execute(
                    "SELECT root_title FROM catalog_metadata"
                ).fetchone()[0],
                "Authoritative root",
            )
            root_catalog = json.loads((root / "catalog.json").read_text())
            dataset_catalog = json.loads(
                (root / "hifld/dataset/catalog.json").read_text()
            )
            self.assertEqual(root_catalog["title"], "Authoritative root")
            self.assertEqual(
                root_catalog["description"], "Authoritative root description"
            )
            self.assertEqual(dataset_catalog["title"], "hifld/dataset")
            self.assertEqual(dataset_catalog["description"], "Catalog of published data.")
            self.assertEqual(
                root_catalog["hifld:created_at"], "2020-01-01T00:00:00Z"
            )
            self.assertEqual(
                root_catalog["hifld:updated_at"], "2024-01-01T00:00:00Z"
            )
            self.assertEqual(
                dataset_catalog["hifld:created_at"], "2021-01-01T00:00:00Z"
            )
            self.assertEqual(
                dataset_catalog["hifld:updated_at"], "2023-01-01T00:00:00Z"
            )
            self.assertEqual(
                connection.execute(
                    "SELECT created_at, updated_at FROM collections"
                ).fetchone(),
                ("2020-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
            )
            self.assertEqual(
                connection.execute(
                    "SELECT created_at, updated_at FROM datasets"
                ).fetchone(),
                ("2021-01-01T00:00:00Z", "2023-01-01T00:00:00Z"),
            )
            self.assertIn(
                "Authoritative root description", (root / "README.md").read_text()
            )

    def test_formats_may_share_generic_media_type(self):
        assets = tuple(
            AssetRecord(
                name,
                name,
                name,
                f"hifld/d/f/v/{name}/data",
                "application/octet-stream",
                1,
                "a" * 64,
            )
            for name in ("file_geodatabase", "shapefile")
        )
        record = CatalogRecord("hifld", "d", "f", "v", "F", "D", "spatial", 1, assets)
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Path(tmpdir) / "catalog.sqlite"
            build_catalog_sqlite(database, (record,))
            connection = sqlite3.connect(database)
            self.assertEqual(
                connection.execute("SELECT count(*) FROM formats").fetchone()[0], 2
            )

    def test_incremental_update_prunes_formats_no_longer_referenced(self):
        asset = AssetRecord(
            "bad", "collection.json", "Bad", "bad", "application/json", 1, "a" * 64
        )
        old = CatalogRecord("hifld", "d", "f", "v", "F", "D", "spatial", 1, (asset,))
        clean = CatalogRecord("hifld", "d", "f", "v", "F", "D", "spatial", 1, ())
        with tempfile.TemporaryDirectory() as tmpdir:
            database = Path(tmpdir) / "catalog.sqlite"
            build_catalog_sqlite(database, (old,))
            update_catalog_sqlite(database, (clean,))
            connection = sqlite3.connect(database)
            self.assertEqual(
                connection.execute("SELECT count(*) FROM formats").fetchone()[0], 0
            )
