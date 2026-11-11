"""Real storage and approval-boundary checks for staged STAC release versions."""

import json
import hashlib
import os
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from dagster import DagsterInstance, Definitions, define_asset_job

from dagster_hifld.assets.publish import _copy_version_files
from dagster_hifld.assets.publish import verify_release, publish_version, run_local_version_pipeline
from dagster_hifld.checks import hifld_asset_checks
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.portolan.catalog import CatalogRecord, render_portolan_tree, version_sort_key
from dagster_hifld.portolan.release import ReleasePointer, require_current_baseline
from dagster_hifld.release_approval import ReleaseApprovalError
from test_full_verification import accepted_fixture


PREFIX = "synthetic/points/vtest"


def select_version(source, registry, version="1.5.0"):
    """Synthetic maintainer decision; deliberately outside both storage roots."""
    document = {
        "type": "Collection", "stac_version": "1.1.0",
        "stac_extensions": ["https://stac-extensions.github.io/version/v1.2.0/schema.json"],
        "id": f"hifld/synthetic/points/v{version}", "version": version,
        "description": "Synthetic only", "license": "other", "links": [],
        "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[None, None]]}},
        "assets": {},
    }
    source.write_key(f"{PREFIX}/collection.json", json.dumps(document).encode())
    value = json.loads(registry.read_text())
    entry = value["releases"][0]
    entry["staged_objects"] = {key: source.sha256_key(key) for key in source.list_prefix(PREFIX)}
    entry["published_objects"] = {
        key.replace(PREFIX + "/", f"synthetic/points/v{version}/", 1): digest
        for key, digest in entry["staged_objects"].items()
    }
    registry.write_text(json.dumps(value))
    return document


class StacReleaseVersionTests(unittest.TestCase):
    def test_renderer_writes_standard_version_without_directory_prefix(self):
        record = CatalogRecord("hifld", "synthetic", "points", "v1.5.0", "Synthetic", "Data", "spatial", 0, ())
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            render_portolan_tree(root, (record,))
            document = json.loads((root / "hifld/synthetic/points/v1.5.0/collection.json").read_text())
        self.assertEqual(document.get("version"), "1.5.0")
        self.assertNotIn("release_version", document)

    def test_latest_version_sorts_numeric_prerelease_identifiers(self):
        labels = ["v1.5.0-rc.10", "v1.5.0-rc.2", "v1.4.9"]
        self.assertEqual(max(labels, key=version_sort_key), "v1.5.0-rc.10")

    def test_preparation_writes_final_stac_without_publishing_or_self_approval(self):
        import geopandas as gpd
        from shapely import Point
        from dagster_hifld.portolan.workflow import PortolanPublishRequest, prepare_staged_release
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry = accepted_fixture(root, candidate_version="vtest")
            path = root / "staging" / PREFIX / "geoparquet/data.parquet"
            path.parent.mkdir()
            gpd.GeoDataFrame({"id": ["001"], "geometry": [Point(1, 2)]}, crs="EPSG:4326").to_parquet(path)
            key = f"{PREFIX}/metadata/data_dictionary.json"
            dictionary = json.loads(source.read_key(key))
            dictionary["columns"] = [{"name": "id", "type": "string", "nullable": False},
                                     {"name": "geometry", "type": "geometry", "nullable": True}]
            source.write_key(key, json.dumps(dictionary).encode())
            source.write_key(f"{PREFIX}/metadata/quality_manifest.json",
                             json.dumps({"quality_check_passed": True, "feature_count": 1, "invalid_geometry_count": 0}).encode())
            request = PortolanPublishRequest("hifld", "synthetic", "points", "v1.5.0", "Synthetic", "Fixture", "Agency",
                                             public_root="https://example.test/catalog")
            style_key = f"{PREFIX}/styles/maplibre.json"
            source.write_key(style_key, json.dumps({"version": 8, "sources": {"data": {
                "type": "vector", "url": "pmtiles://https://example.test/catalog/synthetic/points/vtest/pmtiles/data.pmtiles"
            }}, "layers": []}).encode())
            before = registry.read_bytes()
            document = prepare_staged_release(request, source, target, staged_version="vtest")
            self.assertEqual(document["version"], "1.5.0")
            self.assertEqual(document["id"], "hifld/synthetic/points/v1.5.0")
            self.assertEqual(next(link["href"] for link in document["links"] if link["rel"] == "self"),
                             "https://example.test/catalog/synthetic/points/v1.5.0/collection.json")
            self.assertEqual(json.loads(source.read_key(f"{PREFIX}/collection.json")), document)
            for asset in document["assets"].values():
                self.assertIn("/synthetic/points/v1.5.0/", asset["href"])
                self.assertNotIn("/vtest/", asset["href"])
            self.assertEqual(target.list_prefix(), [])
            self.assertEqual(registry.read_bytes(), before)
            document = prepare_staged_release(replace(request, version="v2.0.0"), source, target, staged_version="vtest")
            self.assertEqual(document["version"], "2.0.0")
            style = json.loads(source.read_key(style_key))
            self.assertEqual(style["sources"]["data"]["url"],
                             "pmtiles://https://example.test/catalog/synthetic/points/v2.0.0/pmtiles/data.pmtiles")

    def test_approved_override_changes_destination_and_retry_reuses_bytes(self):
        with tempfile.TemporaryDirectory() as d:
            source, target, registry = accepted_fixture(Path(d), candidate_version="vtest")
            select_version(source, registry)
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                copied = _copy_version_files(source, target, "synthetic", "points", "vtest")
                before = {key: target.object_snapshot(key) for key in copied}
                self.assertEqual(copied, _copy_version_files(source, target, "synthetic", "points", "vtest"))
            self.assertTrue(copied)
            self.assertTrue(all(key.startswith("synthetic/points/v1.5.0/") for key in copied))
            self.assertEqual(before, {key: target.object_snapshot(key) for key in copied})
            self.assertEqual(target.list_prefix(PREFIX), [])
            self.assertEqual(json.loads(target.read_key("synthetic/points/v1.5.0/collection.json"))["version"], "1.5.0")

    def test_registered_verify_check_and_promote_follow_candidate_to_release_mapping(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry = accepted_fixture(root, candidate_version="vtest")
            select_version(source, registry)
            checks = [check for check in hifld_asset_checks
                      if next(iter(check.check_specs)).asset_key.path == ["publish", "verify"]]
            definitions = Definitions(assets=[verify_release, publish_version], asset_checks=checks,
                resources={"staging_storage": source, "published_storage": target},
                jobs=[define_asset_job("semantic_release")])
            (root / "dagster").mkdir()
            (root / "dagster/dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            with DagsterInstance.local_temp(str(root / "dagster")) as instance:
                instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [PREFIX])
                with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                    result = definitions.resolve_job_def("semantic_release").execute_in_process(instance=instance, partition_key=PREFIX)
            self.assertTrue(result.success)
            self.assertEqual(result.output_for_node("publish__promote")["version"], "v1.5.0")
            self.assertTrue(target.object_exists("synthetic/points/v1.5.0/collection.json"))
            self.assertEqual(target.list_prefix(PREFIX), [])

    def test_missing_stac_cannot_fall_back_to_partition_or_request_version(self):
        with tempfile.TemporaryDirectory() as d:
            source, target, registry = accepted_fixture(Path(d), candidate_version="vtest")
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                source.delete_prefix(f"{PREFIX}/collection.json")
                with self.assertRaisesRegex(ValueError, "staged.*Collection"):
                    _copy_version_files(source, target, "synthetic", "points", "vtest")
            self.assertEqual(target.list_prefix(), [])

    def test_invalid_or_inconsistent_staged_versions_are_rejected(self):
        for change in ({"version": "v1.5.0"}, {"version": "1.5"}, {"version": "01.5.0"},
                       {"version": "1.5.0-01"}, {"version": "../bad"}, {"version": None},
                       {"id": "hifld/synthetic/points/v2.0.0"}):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as d:
                source, target, registry = accepted_fixture(Path(d), candidate_version="vtest")
                document = select_version(source, registry)
                document.update(change)
                source.write_key(f"{PREFIX}/collection.json", json.dumps(document).encode())
                with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                    with self.assertRaises(ValueError):
                        _copy_version_files(source, target, "synthetic", "points", "vtest")
                self.assertEqual(target.list_prefix(), [])

    def test_relabeling_after_approval_cannot_reuse_old_approval(self):
        with tempfile.TemporaryDirectory() as d:
            source, target, registry = accepted_fixture(Path(d), candidate_version="vtest")
            document = select_version(source, registry)
            document.update(version="2.0.0", id="hifld/synthetic/points/v2.0.0")
            source.write_key(f"{PREFIX}/collection.json", json.dumps(document).encode())
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaises(ReleaseApprovalError):
                    _copy_version_files(source, target, "synthetic", "points", "vtest")
            self.assertEqual(target.list_prefix(), [])

    def test_conflicting_destination_does_not_overwrite_accepted_bytes(self):
        with tempfile.TemporaryDirectory() as d:
            source, target, registry = accepted_fixture(Path(d), candidate_version="vtest")
            select_version(source, registry)
            target.write_key("synthetic/points/v1.5.0/geojson/source.geojson", b"accepted")
            before = {key: target.object_snapshot(key) for key in target.list_prefix()}
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaisesRegex(ValueError, "immutable"):
                    _copy_version_files(source, target, "synthetic", "points", "vtest")
            self.assertEqual(before, {key: target.object_snapshot(key) for key in target.list_prefix()})

    def test_matching_selected_legacy_baseline_allows_first_semantic_release(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            _, target, _ = accepted_fixture(root, candidate_version="vtest")
            database = root / "legacy.sqlite"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE files (file_path TEXT, latest_version TEXT)")
                connection.execute("INSERT INTO files VALUES ('hifld/synthetic/points', 'v20260214')")
            body = database.read_bytes()
            generation = "1f5bd380-a67f-48a8-b5e2-c80aa85f0e63"
            key = f"releases/{generation}/_catalog/catalog.sqlite"
            target.write_key(key, body)
            pointer = ReleasePointer(generation, key, f"releases/{generation}/catalog.json",
                                     hashlib.sha256(body).hexdigest(), len(body), "2026-10-21T00:00:00Z")
            target.write_key("_catalog/current.json", pointer.to_bytes())
            document = {"id": "hifld/synthetic/points/v1.5.0", "version": "1.5.0"}
            require_current_baseline(target, document, {"version": "v20260214"})
            with self.assertRaisesRegex(ValueError, "baseline.*changed"):
                require_current_baseline(target, document, {"version": "v20260213"})

    def test_local_pipeline_reports_the_promoted_release_inventory(self):
        with tempfile.TemporaryDirectory() as d:
            source, target, registry = accepted_fixture(Path(d), candidate_version="vtest")
            select_version(source, registry)

            def approve_prepared_fixture(*args):
                # Synthetic maintainer approval of the actual generated inventory.
                select_version(source, registry)
                return _copy_version_files(*args)

            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}), \
                 patch("dagster_hifld.assets.publish._copy_version_files", side_effect=approve_prepared_fixture), \
                 patch("dagster_hifld.assets.publish._write_and_publish_pmtiles", return_value=[]):
                result = run_local_version_pipeline(source, target, "synthetic", "points", "vtest")
            paths = [output.path for output in result["outputs"] if output.path]
            self.assertTrue(paths)
            self.assertTrue(all(path.startswith("synthetic/points/v1.5.0/") for path in paths), paths)
            self.assertTrue(all(target.object_exists(path) for path in paths))
            self.assertIn("metadata", {output.format_type for output in result["outputs"]})

    def test_changed_accepted_baseline_blocks_promotion(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry = accepted_fixture(root, candidate_version="vtest")
            select_version(source, registry)
            database = root / "current.sqlite"
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE files (file_path TEXT, latest_version TEXT)")
                connection.execute("INSERT INTO files VALUES ('hifld/synthetic/points', 'v1.4.9')")
            body = database.read_bytes()
            generation = "1f5bd380-a67f-48a8-b5e2-c80aa85f0e63"
            key = f"releases/{generation}/_catalog/catalog.sqlite"
            target.write_key(key, body)
            pointer = ReleasePointer(generation, key, f"releases/{generation}/catalog.json",
                                     hashlib.sha256(body).hexdigest(), len(body), "2026-10-21T00:00:00Z")
            target.write_key("_catalog/current.json", pointer.to_bytes())
            before = {key: target.object_snapshot(key) for key in target.list_prefix()}
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaisesRegex(ValueError, "baseline.*changed"):
                    _copy_version_files(source, target, "synthetic", "points", "vtest")
            self.assertEqual(before, {key: target.object_snapshot(key) for key in target.list_prefix()})


if __name__ == "__main__":
    unittest.main()
