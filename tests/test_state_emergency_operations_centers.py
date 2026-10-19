"""Focused source-fidelity and draft approval behavior for FEMA State EOC."""
import unittest
import tempfile
from pathlib import Path

import shapely
from dagster import AssetCheckSeverity
from dagster_hifld.resources import StagingStorageResource

from dagster_hifld.assets.state_emergency_operations_centers import source_quality_check_result

from dagster_hifld.sources.state_emergency_operations_centers import (
    EXPECTED_FIELDS,
    EOCSourceError,
    _table,
    _validate_rows,
    _validate_source_layer_metadata,
    _local_staging,
)


class StateEocSourceTests(unittest.TestCase):
  def test_source_crs_and_physical_schema_drift_are_rejected(self):
    fields = [{"name": name, "type": field_type, "nullable": nullable, "length": length}
              for name, field_type, nullable, length in EXPECTED_FIELDS]
    wgs84 = {"wkid": 4326, "latestWkid": 4326}
    metadata = {"fields": fields, "objectIdField": "objectid", "geometryType": "esriGeometryPoint",
                "sourceSpatialReference": wgs84, "extent": {"spatialReference": wgs84}}
    _validate_source_layer_metadata(metadata)

    changed_crs = {**metadata, "extent": {"spatialReference": {"wkid": 3857, "latestWkid": 3857}}}
    with self.assertRaisesRegex(EOCSourceError, "source_coordinate_reference_changed"):
      _validate_source_layer_metadata(changed_crs)

    changed_schema = {**metadata, "fields": [*fields[:-1], {**fields[-1], "length": 50}]}
    with self.assertRaisesRegex(EOCSourceError, "source_physical_schema_contract_changed"):
      _validate_source_layer_metadata(changed_schema)

  def test_explicit_local_staging_path_outside_worker_scratch_is_supported(self):
    with tempfile.TemporaryDirectory() as root:
      storage_path = Path(root) / "stage"
      capture_path = Path(root) / "captures"
      storage = StagingStorageResource(bucket="", use_local=True, local_dir=str(storage_path))
      self.assertEqual(_local_staging(storage, str(capture_path)), capture_path)
      self.assertTrue(storage_path.is_dir())
      self.assertTrue(capture_path.is_dir())

  def test_native_candidate_preserves_source_values_and_flags_zero_point(self):
    fields = [
        {"name": "objectid", "type": "esriFieldTypeOID", "nullable": False},
        *[{"name": name, "type": "esriFieldTypeString", "nullable": True, "length": 255} for name in (
            "name", "city", "state", "fax", "website", "traffic")],
        {"name": "eoc_id", "type": "esriFieldTypeSmallInteger", "nullable": True},
        *[{"name": name, "type": "esriFieldTypeString", "nullable": True,
           "length": 50 if name in ("zip", "telephone") else 100} for name in (
            "address1", "address2", "statename", "zip", "telephone")],
        {"name": "latitude", "type": "esriFieldTypeDouble", "nullable": True},
        {"name": "longitude", "type": "esriFieldTypeDouble", "nullable": True},
    ]
    attrs = {
        "objectid": 1, "name": "Synthetic Center", "city": "Sample", "state": "ZZ",
        "fax": "555-0100", "website": "https://example.invalid", "traffic": "",
        "eoc_id": 77, "address1": "1 Example Way", "address2": "", "statename": "Sample",
        "zip": "01234", "telephone": "555-0101", "latitude": 0.0, "longitude": 0.0,
    }
    feature = {"attributes": attrs, "geometry": {"x": 0.0, "y": 0.0, "spatialReference": {"wkid": 4326}}}

    self.assertEqual(_validate_rows([feature], tuple(f["name"] for f in fields), "objectid", [1]), [])
    table, quality = _table({"fields": fields}, [feature])

    self.assertEqual(table.num_rows, 1)  # Quality findings never filter source rows.
    self.assertEqual(table.column("eoc_id").type.bit_width, 16)
    self.assertEqual(table.column("zip").to_pylist(), ["01234"])
    point = shapely.from_wkb(table.column("geometry")[0].as_py())
    self.assertEqual((point.x, point.y), (0.0, 0.0))
    self.assertEqual(quality, ["source_zero_coordinate"])
    failed = source_quality_check_result({"version": "synthetic", "rows": 1,
                                          "quality_passed": False,
                                          "quality_errors": quality})
    self.assertFalse(failed.passed)
    self.assertEqual(failed.severity, AssetCheckSeverity.ERROR)
