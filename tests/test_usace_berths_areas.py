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
from dagster_hifld.assets.usace_berths_areas import (
    usace_berths_areas_complete, usace_berths_areas_draft_schedule,
    usace_berths_areas_usace_ienc,
)
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.sources.epa_frs_common import processing_identity
from dagster_hifld.sources import usace_berths_areas as source


class UsaceBerthsAreasTests(unittest.TestCase):
    def test_asset_is_registered_and_refresh_is_stopped_with_acceptance_false(self):
        self.assertIn(usace_berths_areas_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_berths_areas_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        result = usace_berths_areas_complete({"feature_count": 1108, "source_population_complete": True})
        self.assertTrue(result.passed)
        self.assertFalse(result.metadata["release_accepted"].value)
        self.assertFalse(result.metadata["rights_accepted"].value)

    def test_native_polygon_wkb_preserves_ring_and_vertex_order(self):
        exterior = [[0, 0], [0, 4], [4, 4], [4, 0], [0, 0]]
        interior = [[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]
        wkb = source._polygon_wkb({"rings": [exterior, interior]})
        self.assertEqual(struct.unpack_from("<I", wkb, 5)[0], 2)
        polygon = from_wkb(wkb)
        self.assertEqual(get_coordinates(polygon.exterior).tolist(), exterior)
        self.assertEqual(get_coordinates(polygon.interiors[0]).tolist(), interior)

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

    def test_parquet_retains_all_attributes_duplicate_feature_ids_and_polygon_wkb(self):
        first = {field["name"]: None for field in source.ATTRIBUTE_FIELDS}
        first.update(OBJECTID=1, Feature_ID="duplicate")
        second = dict(first, OBJECTID=2)
        ring = [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]
        features = [
            {"attributes": first, "geometry": {"rings": [ring]}},
            {"attributes": second, "geometry": {"rings": [ring]}},
        ]
        data = source._parquet_bytes(source.EXPECTED_FIELD_DECLARATIONS, features)
        table = pq.read_table(pa.BufferReader(data))
        self.assertEqual(table.column_names, [field["name"] for field in source.ATTRIBUTE_FIELDS] + ["geometry"])
        self.assertEqual(table["Feature_ID"].to_pylist(), ["duplicate", "duplicate"])
        self.assertEqual(table["OBJECTID"].to_pylist(), [1, 2])
        self.assertEqual(table["geometry"][0].as_py(), source._polygon_wkb({"rings": [ring]}))

    def test_capture_root_version_traversal_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = source.StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_usace_berths_areas(storage, version="../outside")

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
