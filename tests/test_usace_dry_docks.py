import json
import shutil
import struct
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely import from_wkb, get_coordinates

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_dry_docks import (
    usace_dry_docks_complete, usace_dry_docks_draft_schedule, usace_dry_docks_usace_ienc,
)
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.sources.epa_frs_common import processing_identity
from dagster_hifld.sources import usace_dry_docks as source


class UsaceDryDocksTests(unittest.TestCase):
    def test_asset_is_registered_and_schedule_is_stopped(self):
        self.assertIn(usace_dry_docks_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_dry_docks_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        result = usace_dry_docks_complete({"feature_count": 2, "source_population_complete": True})
        self.assertTrue(result.passed)
        self.assertFalse(result.metadata["release_accepted"].value)
        self.assertFalse(result.metadata["rights_accepted"].value)
        self.assertFalse(result.metadata["baseline_comparison_accepted"].value)
        self.assertFalse(usace_dry_docks_complete({
            "feature_count": 1, "source_population_complete": True,
        }).passed)

    def test_native_polygon_wkb_preserves_ring_and_vertex_order(self):
        exterior = [[0, 0], [0, 4], [4, 4], [4, 0], [0, 0]]
        wkb = source._polygon_wkb({"rings": [exterior]})
        self.assertEqual(struct.unpack_from("<I", wkb, 5)[0], 1)
        self.assertEqual(get_coordinates(from_wkb(wkb).exterior).tolist(), exterior)

    def test_polygon_rejects_non_xy_nonfinite_open_and_rewound_rings(self):
        ring = [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]
        cases = [
            ({"rings": [[*ring[:-1], [0, 0, 5], ring[-1]]]}, "source_polygon_coordinate_not_finite_xy"),
            ({"rings": [[*ring[:-1], [float("nan"), 0], ring[-1]]]}, "source_polygon_coordinate_not_finite_xy"),
            ({"rings": [ring[:-1]]}, "source_polygon_ring_invalid_or_open"),
            ({"rings": [list(reversed(ring))]}, "source_exterior_ring_winding_drift"),
            ({"rings": [ring], "hasM": True}, "source_polygon_not_xy"),
        ]
        for geometry, error in cases:
            with self.subTest(error=error):
                with self.assertRaisesRegex(ValueError, error):
                    source._polygon_wkb(geometry)

    def test_parquet_preserves_native_fields_values_and_polygon_wkb(self):
        ring1 = [[-84.69451419999996, 39.11002610000003], [-84.69419449999998, 39.10964780000006],
                 [-84.69437689999995, 39.109559800000056], [-84.69468809999995, 39.10993910000008],
                 [-84.69451419999996, 39.11002610000003]]
        ring2 = [[-84.69766069999997, 39.108345100000065], [-84.69750049999999, 39.10844210000005],
                 [-84.69715039999994, 39.10809340000003], [-84.69732869999996, 39.10799640000005],
                 [-84.69766069999997, 39.108345100000065]]
        def attrs(object_id, name, feature_id):
            values = {field["name"]: None for field in source.ATTRIBUTE_FIELDS}
            values.update(OBJECTID=object_id, Object_Name=name, Horizontal_Length="", Horizontal_Width="",
                          Horizontal_Clearance="", Depth_Value_1="", Scale_Minimum=18750,
                          Information="", Source_Date="20191010",
                          Source_Indication="US,U3,SURVY,LRL_Markland_Pool_Field_Check_IENC-20-001",
                          Source_Dataset="U37OH439.000", Feature_ID=feature_id)
            return values
        features = [
            {"attributes": attrs(1, "McGinnis", "U3 1589847816 00001"), "geometry": {"rings": [ring1]}},
            {"attributes": attrs(2, "C&B Marine Services", "U3 1392960548 00001"), "geometry": {"rings": [ring2]}},
        ]
        data = source._parquet_bytes(source.EXPECTED_FIELD_DECLARATIONS, features)
        table = pq.read_table(pa.BufferReader(data))
        self.assertEqual(table.column_names, [field["name"] for field in source.ATTRIBUTE_FIELDS] + ["geometry"])
        self.assertEqual(table["Object_Name"].to_pylist(), ["McGinnis", "C&B Marine Services"])
        self.assertEqual(table["Horizontal_Length"].to_pylist(), ["", ""])
        self.assertEqual(table["OBJECTID"].to_pylist(), [1, 2])
        self.assertEqual(table.schema.field("OBJECTID").type, pa.int64())
        self.assertEqual(table.schema.field("Scale_Minimum").type, pa.int32())
        self.assertEqual(table.schema.field("Object_Name").type, pa.string())
        self.assertEqual(table["Feature_ID"].to_pylist(), ["U3 1589847816 00001", "U3 1392960548 00001"])
        self.assertEqual(table["geometry"][0].as_py(), source._polygon_wkb({"rings": [ring1]}))

    def test_retained_capture_is_complete_and_matches_pinned_native_snapshot(self):
        capture_root = Path("/tmp/58-dry-docks-capture-20261021")
        captures = source._read_replay(capture_root)
        fields, features, audit = source._validate(captures)
        self.assertEqual(len(features), 2)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_record_sha256"], source.EXPECTED_RECORD_SHA256)
        self.assertEqual(audit["source_geometry_sha256"], source.EXPECTED_GEOMETRY_SHA256)
        self.assertEqual(audit["source_schema_sha256"], source.EXPECTED_SCHEMA_SHA256)
        self.assertEqual(audit["source_polygon_vertex_count"], 10)
        self.assertEqual(audit["source_coverage_scope"].startswith("Complete observed DRY_DOCK_AREA"), True)

    def test_packaged_code_identity_validates_without_git_metadata(self):
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            packaged_root = Path(tmp) / "runtime"
            packaged_root.mkdir()
            shutil.copytree(repo_root / "src/dagster_hifld", packaged_root / "src/dagster_hifld")
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copy2(repo_root / name, packaged_root / name)
            self.assertFalse((packaged_root / ".git").exists())
            identity = processing_identity(packaged_root)
            self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
            self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
            validate_construction_code_identity(
                packaged_root, identity["code_commit"], identity["files"], identity=identity,
            )


if __name__ == "__main__":
    unittest.main()
