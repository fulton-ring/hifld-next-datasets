from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from ops.acceptance.bootstrap import (
    DEFAULT_BUCKET,
    _asset_href,
    _sha256,
    load_manifest,
    load_metadata_sources,
)


class LocalFixtureBootstrapTests(unittest.TestCase):
    def test_local_fixture_manifest_is_real_multiformat_inventory(self) -> None:
        fixtures = load_manifest()

        self.assertEqual(DEFAULT_BUCKET, "hifld-local-staging")
        self.assertEqual(
            {fixture.format for fixture in fixtures},
            {
                "file_geodatabase",
                "geojson",
                "geopackage",
                "geoparquet",
                "pmtiles",
                "shapefile",
            },
        )
        self.assertGreaterEqual(
            {fixture.geometry for fixture in fixtures}, {"point", "line", "polygon"}
        )
        raw = json.loads(
            Path("ops/acceptance/manifest.json").read_text(encoding="utf-8")
        )
        self.assertTrue(
            all(
                not ({"title", "description", "publisher", "tags"} & record.keys())
                for record in raw["records"]
            )
        )
        self.assertTrue(
            all(
                fixture.destination_key.startswith(
                    f"hifld/{fixture.dataset_slug}/{fixture.file_slug}/{fixture.version}/{fixture.format}/"
                )
                for fixture in fixtures
            )
        )

    def test_cache_name_preserves_source_extension(self) -> None:
        by_format = {fixture.format: fixture for fixture in load_manifest()}

        self.assertTrue(by_format["file_geodatabase"].cache_name.endswith(".zip"))
        self.assertTrue(by_format["shapefile"].cache_name.endswith(".zip"))
        self.assertTrue(by_format["geojson"].cache_name.endswith(".geojson"))
        self.assertTrue(by_format["geopackage"].cache_name.endswith(".gpkg"))
        self.assertTrue(by_format["geoparquet"].cache_name.endswith(".parquet"))
        self.assertTrue(by_format["pmtiles"].cache_name.endswith(".pmtiles"))

    def test_manifest_rejects_destination_outside_collection_prefix(self) -> None:
        source = json.loads(
            Path("ops/acceptance/manifest.json").read_text(encoding="utf-8")
        )
        source["records"][0]["destination_key"] = "production-shaped/path.gpkg"
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "manifest.json"
            manifest.write_text(json.dumps(source), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "canonical collection prefix"):
                load_manifest(manifest)

    def test_fixture_content_type_matches_original_format(self) -> None:
        self.assertEqual(
            {fixture.format: fixture.content_type for fixture in load_manifest()},
            {
                "file_geodatabase": "application/zip",
                "geojson": "application/geo+json",
                "geopackage": "application/geopackage+sqlite3",
                "geoparquet": "application/vnd.apache.parquet",
                "pmtiles": "application/vnd.pmtiles",
                "shapefile": "application/zip",
            },
        )

    def test_staging_asset_href_is_absolute_and_checksum_is_multihash(self) -> None:
        fixture = load_manifest()[0]
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.bin"
            source.write_bytes(b"source")
            self.assertEqual(
                _asset_href(DEFAULT_BUCKET, fixture),
                f"http://localhost:8333/{DEFAULT_BUCKET}/{fixture.destination_key}",
            )
            self.assertRegex(_sha256(source), r"^1220[0-9a-f]{64}$")

    def test_metadata_inventory_pins_version_dataset_and_file_manifests(self) -> None:
        sources = load_metadata_sources()
        self.assertEqual(len(sources), 27)
        by_version: dict[tuple[str, str, str], set[str]] = {}
        for source in sources:
            if source.scope == "version":
                assert source.file_slug is not None
                assert source.version is not None
                identity = (source.dataset_slug, source.file_slug, source.version)
                by_version.setdefault(identity, set()).add(source.filename)
                self.assertEqual(
                    source.destination_key,
                    f"hifld/{source.dataset_slug}/{source.file_slug}/{source.version}/metadata/source/{source.filename}",
                )
        expected = {
            "data_dictionary.json",
            "quality_manifest.json",
            "source_manifest.json",
        }
        self.assertEqual(len(by_version), 6)
        self.assertTrue(all(filenames == expected for filenames in by_version.values()))
        scoped = {
            (source.scope, source.dataset_slug, source.file_slug) for source in sources
        }
        self.assertEqual(
            {identity for identity in scoped if identity[0] == "dataset"},
            {
                ("dataset", "agricultural-minerals-operations", None),
                ("dataset", "uscg-sectors", None),
                ("dataset", "uniform-hazard-ground-motion", None),
                ("dataset", "hospitals-3", None),
            },
        )
        self.assertEqual(
            len({identity for identity in scoped if identity[0] == "file"}), 5
        )

    def test_hospitals_inventory_contains_all_production_versions_and_formats(self) -> None:
        fixtures = [
            fixture for fixture in load_manifest() if fixture.dataset_slug == "hospitals-3"
        ]
        by_version: dict[str, set[str]] = {}
        for fixture in fixtures:
            by_version.setdefault(fixture.version, set()).add(fixture.format)
        self.assertEqual(
            by_version,
            {
                "v1.0.0": {
                    "file_geodatabase",
                    "geojson",
                    "geopackage",
                    "geoparquet",
                    "pmtiles",
                    "shapefile",
                },
                "v1.1.0": {"geopackage", "geoparquet", "pmtiles", "shapefile"},
            },
        )

    def test_collection_export_is_exactly_pinned(self) -> None:
        manifest = json.loads(
            Path("ops/acceptance/manifest.json").read_text(encoding="utf-8")
        )
        source = manifest["collection_source"]
        content = Path("ops/acceptance/collection-source.json").read_bytes()

        self.assertEqual(
            source["url"], "https://hifld.publicenvirodata.org/api/collections"
        )
        self.assertEqual(
            source["destination_key"], "hifld/metadata/source/collections.json"
        )
        self.assertEqual(hashlib.sha256(content).hexdigest(), source["sha256"])
        self.assertEqual(json.loads(content)[0]["name"], "HIFLD")
