"""Focused integrity tests for the exact USACE FUDS district adapter."""
import unittest
import hashlib
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from dagster import DagsterInstance, materialize

from dagster_hifld.assets.fuds_district_boundaries import (
    usace_fuds_program_district_boundaries,
    usace_fuds_program_district_boundaries_complete,
)
from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.fuds_district_boundaries import (
    EXPECTED_SCHEMA_SHA256, DATASET_SLUG, FILE_SLUG, _canonical, _esri_polygon,
    _field_contract, capture_fuds_district_boundaries,
)
from dagster_hifld.source_manifest import load_resolved_source_manifest


class FudsDistrictBoundaryIntegrityTests(unittest.TestCase):
    def test_polygon_rings_preserve_shell_and_hole(self):
        # Esri clockwise shell, counter-clockwise hole.
        geometry = {"rings": [
            [[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]],
            [[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]],
        ]}
        polygon = _esri_polygon(geometry)
        self.assertEqual(polygon.geom_type, "MultiPolygon")
        self.assertEqual(len(polygon.geoms), 1)
        self.assertEqual(len(polygon.geoms[0].interiors), 1)

    def test_ring_dimensions_drift_fails_before_z_loss(self):
        with self.assertRaisesRegex(ValueError, "dimensions_invalid"):
            _esri_polygon({"rings": [[[0, 0, 5], [0, 1, 5], [1, 1, 5], [0, 0, 5]]]})

    def test_schema_drift_is_quarantinable(self):
        layer = {"fields": [{"name": "OBJECTID", "type": "esriFieldTypeOID"},
                            {"name": "SHAPE", "type": "esriFieldTypeGeometry"}]}
        with self.assertRaisesRegex(ValueError, "source_schema_drift"):
            _field_contract(layer)

    def test_pinned_schema_fingerprint_is_sha256(self):
        self.assertEqual(len(EXPECTED_SCHEMA_SHA256), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in EXPECTED_SCHEMA_SHA256))

    def test_tiny_capture_seal_catalog_and_cache(self):
        fields = [{"name": "OBJECTID", "type": "esriFieldTypeOID"},
                  {"name": "SHAPE", "type": "esriFieldTypeGeometry"}]
        plain = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f}
                 for f in fields]
        schema_sha = hashlib.sha256(json.dumps(plain, sort_keys=True, separators=(",", ":"),
                                      ensure_ascii=False).encode()).hexdigest()
        layer = {"id": 9, "name": "FUDS Program District Boundaries",
                 "geometryType": "esriGeometryPolygon", "fields": fields,
                 "extent": {"spatialReference": {"latestWkid": 4326}},
                 "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True}}
        service = {"currentVersion": 11.5}
        page = {"spatialReference": {"latestWkid": 4326}, "features": [{
            "attributes": {"OBJECTID": 1}, "geometry": {"rings": [[
                [0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]},
        }]}

        def fake_capture(_client, url, root, name):
            if name.startswith("service_"):
                doc = service
            elif name.startswith("layer_"):
                doc = layer
            elif name == "count_before" or name == "count_after":
                doc = {"count": 1}
            elif name == "ids_before" or name == "ids_after":
                doc = {"objectIds": [1]}
            else:
                doc = page
            body = _canonical(doc)
            (root / f"{name}.wire").write_bytes(body)
            receipt = {"url": url, "final_url": url, "observed_at": "2026-10-19T00:00:00Z",
                       "status": 200, "complete_body": True, "bytes": len(body),
                       "sha256": hashlib.sha256(body).hexdigest(), "wire_bytes": len(body),
                       "wire_sha256": hashlib.sha256(body).hexdigest(),
                       "content_encoding": "identity"}
            return body, receipt

        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            with patch("dagster_hifld.sources.fuds_district_boundaries.EXPECTED_SCHEMA_SHA256", schema_sha), \
                 patch("dagster_hifld.sources.fuds_district_boundaries.EXPECTED_COUNT", 1), \
                 patch("dagster_hifld.sources.fuds_district_boundaries.capture_get", fake_capture), \
                 patch("dagster_hifld.sources.fuds_district_boundaries.validate_construction_code_identity"), \
                 patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                first = capture_fuds_district_boundaries(storage, version="fixture-v1",
                                                         capture_directory=f"{directory}/captures")
                self.assertFalse(first["candidate_approved"])
                manifest = load_resolved_source_manifest(storage, DATASET_SLUG, FILE_SLUG,
                                                         first["version"]).metadata
                summary = summarize_staged_catalog(storage, DATASET_SLUG, FILE_SLUG,
                                                  first["version"], DATASET_SLUG,
                                                  source_metadata=manifest)
                self.assertEqual(summary.quality_manifest["feature_count"], 1)
                write_catalog_metadata(storage, DATASET_SLUG, FILE_SLUG, first["version"],
                                       summary.quality_manifest, summary.data_dictionary)
                second = capture_fuds_district_boundaries(storage, version="fixture-v2",
                                                          capture_directory=f"{directory}/captures")
                self.assertTrue(second["reused"])

    def _tiny_layer_fixture(self):
        fields = [{"name": "OBJECTID", "type": "esriFieldTypeOID"},
                  {"name": "SHAPE", "type": "esriFieldTypeGeometry"}]
        plain = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f}
                 for f in fields]
        schema_sha = hashlib.sha256(json.dumps(plain, sort_keys=True, separators=(",", ":"),
                                      ensure_ascii=False).encode()).hexdigest()
        layer = {"id": 9, "name": "FUDS Program District Boundaries",
                 "geometryType": "esriGeometryPolygon", "fields": fields,
                 "extent": {"spatialReference": {"latestWkid": 4326}},
                 "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True}}
        service = {"currentVersion": 11.5}
        page = {"spatialReference": {"latestWkid": 4326}, "features": [{
            "attributes": {"OBJECTID": 1}, "geometry": {"rings": [[
                [0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]},
        }]}
        return schema_sha, layer, service, page

    def _quarantine_case(self, directory, mutate, expected_regex, expected_reason,
                         captured_names):
        """Run a capture whose fake responses are mutated by `mutate`; assert quarantine."""
        schema_sha, layer, service, page = self._tiny_layer_fixture()
        storage = StagingStorageResource(local_dir=directory, use_local=True)

        def fake_capture(_client, url, root, name):
            if name.startswith("service_"):
                doc = service
            elif name.startswith("layer_"):
                doc = layer
            elif name == "count_before" or name == "count_after":
                doc = {"count": 1}
            elif name == "ids_before" or name == "ids_after":
                doc = {"objectIds": [1]}
            else:
                doc = page
            mutate(name, doc)
            body = _canonical(doc)
            (root / f"{name}.wire").write_bytes(body)
            receipt = {"url": url, "final_url": url, "observed_at": "2026-10-19T00:00:00Z",
                       "status": 200, "complete_body": True, "bytes": len(body),
                       "sha256": hashlib.sha256(body).hexdigest(), "wire_bytes": len(body),
                       "wire_sha256": hashlib.sha256(body).hexdigest(),
                       "content_encoding": "identity"}
            return body, receipt

        with patch("dagster_hifld.sources.fuds_district_boundaries.EXPECTED_SCHEMA_SHA256", schema_sha), \
             patch("dagster_hifld.sources.fuds_district_boundaries.EXPECTED_COUNT", 1), \
             patch("dagster_hifld.sources.fuds_district_boundaries.capture_get", fake_capture), \
             patch("dagster_hifld.sources.fuds_district_boundaries.validate_construction_code_identity"), \
             patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
            captures_dir = Path(directory) / "captures"
            captures_dir.mkdir(parents=True, exist_ok=True)
            with self.assertRaisesRegex(ValueError, expected_regex):
                capture_fuds_district_boundaries(storage, version="drift-v1",
                                                 capture_directory=str(captures_dir))
            quarantines = list((Path(directory) / "_quarantine").rglob("failure.json"))
            self.assertEqual(len(quarantines), 1, "incomplete captures must quarantine evidence")
            failure = json.loads(quarantines[0].read_text())
            self.assertEqual(failure["reason_code"], expected_reason)
            # Every request made before the failure is retained in the quarantine.
            for name in captured_names:
                self.assertIn(name, failure["captured_names"])
                self.assertTrue((quarantines[0].parents[1] / "raw" / f"{name}.body").exists())

    def test_incomplete_page_response_is_quarantined(self):
        self._quarantine_case(
            self.enterContext(tempfile.TemporaryDirectory()),
            mutate=lambda name, doc: doc.update(
                {"features": doc["features"][:-1]}) if name.startswith("page") else None,
            expected_regex="source_page_incomplete",
            expected_reason="ValueError",
            captured_names=["service_before", "layer_before", "count_before",
                            "ids_before", "page_000"],
        )

    def test_id_slice_drift_is_quarantined(self):
        self._quarantine_case(
            self.enterContext(tempfile.TemporaryDirectory()),
            mutate=lambda name, doc: (
                doc["objectIds"].append(999) if name == "ids_before" else None),
            expected_regex="source_initial_id_roster_invalid",
            expected_reason="ValueError",
            captured_names=["service_before", "layer_before", "count_before", "ids_before"],
        )

    def test_changed_during_capture_is_quarantined(self):
        self._quarantine_case(
            self.enterContext(tempfile.TemporaryDirectory()),
            mutate=lambda name, doc: (
                doc["objectIds"].append(999) if name == "ids_after" else None),
            expected_regex="source_changed_during_capture",
            expected_reason="ValueError",
            captured_names=["service_before", "layer_before", "count_before",
                            "ids_before", "page_000", "layer_after", "service_after",
                            "count_after", "ids_after"],
        )

    def test_dagster_materialization_records_publish_partition(self):
        schema_sha, layer, service, page = self._tiny_layer_fixture()
        storage = StagingStorageResource(
            local_dir=self.enterContext(tempfile.TemporaryDirectory()), use_local=True)

        def fake_capture(_client, url, root, name):
            if name.startswith("service_"):
                doc = service
            elif name.startswith("layer_"):
                doc = layer
            elif name == "count_before" or name == "count_after":
                doc = {"count": 1}
            elif name == "ids_before" or name == "ids_after":
                doc = {"objectIds": [1]}
            else:
                doc = page
            body = _canonical(doc)
            (root / f"{name}.wire").write_bytes(body)
            receipt = {"url": url, "final_url": url, "observed_at": "2026-10-19T00:00:00Z",
                       "status": 200, "complete_body": True, "bytes": len(body),
                       "sha256": hashlib.sha256(body).hexdigest(), "wire_bytes": len(body),
                       "wire_sha256": hashlib.sha256(body).hexdigest(),
                       "content_encoding": "identity"}
            return body, receipt

        instance_dir = Path(self.enterContext(tempfile.TemporaryDirectory())) / "dagster"
        instance_dir.mkdir()
        (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
        with patch("dagster_hifld.sources.fuds_district_boundaries.EXPECTED_SCHEMA_SHA256", schema_sha), \
             patch("dagster_hifld.sources.fuds_district_boundaries.EXPECTED_COUNT", 1), \
             patch("dagster_hifld.sources.fuds_district_boundaries.capture_get", fake_capture), \
             patch("dagster_hifld.sources.fuds_district_boundaries.validate_construction_code_identity"), \
             patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"), \
             DagsterInstance.from_config(str(instance_dir)) as instance:
            result = materialize(
                [usace_fuds_program_district_boundaries],
                resources={"staging_storage": storage}, instance=instance,
            )
            self.assertTrue(result.success)
            self.assertEqual(len(result.get_asset_materialization_events()), 1)
            partition = instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME)
            self.assertEqual(len(partition), 1)
            self.assertIn(DATASET_SLUG, partition[0])
            # materialize does not auto-run unselected checks, so verify the
            # blocking check's pure logic directly against the candidate result.
            check_result = usace_fuds_program_district_boundaries_complete(
                {"feature_count": 13, "candidate_approved": False})
            self.assertTrue(check_result.passed)
            self.assertFalse(usace_fuds_program_district_boundaries_complete(
                {"feature_count": 1, "candidate_approved": False}).passed)


if __name__ == "__main__":
    unittest.main()
