"""Focused integrity tests for the exact USACE FUDS district adapter."""
import unittest
import hashlib
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
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


if __name__ == "__main__":
    unittest.main()
