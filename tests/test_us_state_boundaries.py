"""Focused state boundaries candidate gates: synthetic behavior, no source approval."""
import hashlib
import io
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from pyproj import CRS
from shapely.geometry import MultiPolygon, Polygon

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILE_PAIRS
from dagster_hifld.assets.us_state_boundaries import us_state_boundaries_source_complete
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import census_latest
from dagster_hifld.sources.us_state_boundaries import (
    DATASET_SLUG,
    EXPECTED_FIELDS,
    EXPECTED_GEOIDS,
    FILE_SLUG,
    STRING_FIELDS,
    _capture,
    _geoparquet,
    _inspect_archive,
    _inspect_archive_data,
    candidate_version,
)


def _release(vintage=2026):
    source_file = f"tl_{vintage}_us_state.zip"
    return census_latest.ShapefileRelease(
        vintage=vintage, layer="STATE", source_file=source_file,
        source_url=f"https://www2.census.gov/geo/tiger/TIGER{vintage}/STATE/{source_file}",
        directory_url=f"https://www2.census.gov/geo/tiger/TIGER{vintage}/STATE/",
        listing_sha256="0" * 64, listing_bytes=1, latest_checked_at="2026-11-13T00:00:00+00:00",
        annual_listing_sha256="1" * 64,
    )


class RegistrationTests(unittest.TestCase):
    def test_explicit_source_check_job_and_stopped_schedule_are_wired(self):
        with patch.dict(os.environ, {"HIFLD_SHADOW_MANUAL_ONLY": "1"}):
            from dagster_hifld.definitions import defs

            graph = defs.resolve_asset_graph()
            keys = {tuple(key.path) for key in graph.get_all_asset_keys()}
            self.assertIn(("ingest", DATASET_SLUG, FILE_SLUG), keys)
            self.assertIn(("publish", "catalog"), keys)
            self.assertIn(("publish", "promote"), keys)
            from dagster_hifld.assets.ingest_registry import hifld_ingest_assets

            self.assertEqual(
                sum(any(tuple(key.path) == ("ingest", DATASET_SLUG, FILE_SLUG) for key in asset.keys)
                    for asset in hifld_ingest_assets),
                1,
            )
            self.assertIsNotNone(defs.resolve_job_def("us_state_boundaries_draft_refresh"))
            from dagster_hifld.assets.us_state_boundaries import us_state_boundaries_draft_schedule

            self.assertEqual(us_state_boundaries_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


class UsStateBoundariesTests(unittest.TestCase):
    def _archive(self, names):
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w") as archive:
            for name in names:
                archive.writestr(name, b"fixture")
        return sink.getvalue()

    def _synthetic_frame(self, overrides=None):
        geoids = ["01", "02"]
        attributes = {}
        for name in EXPECTED_FIELDS:
            if name in ("ALAND", "AWATER"):
                attributes[name] = [10, 20]
            elif name == "GEOID":
                attributes[name] = geoids
            elif name == "GEOIDFQ":
                attributes[name] = ["0400000US" + geoid for geoid in geoids]
            elif name == "STATENS":
                attributes[name] = ["01700001", "01700002"]
            elif name == "MTFCC":
                attributes[name] = ["G4000", "G4000"]
            elif name == "FUNCSTAT":
                attributes[name] = ["A", "A"]
            elif name == "REGION":
                attributes[name] = ["1", "1"]
            elif name == "DIVISION":
                attributes[name] = ["0", "0"]
            elif name == "LSAD":
                attributes[name] = ["00", "00"]
            else:
                attributes[name] = [name + "1", name + "2"]
        if overrides:
            for name, values in overrides.items():
                attributes[name] = values
        polygons = [
            Polygon([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)]),
            MultiPolygon([
                Polygon([(3, 0), (4, 0), (4, 1), (3, 1), (3, 0)]),
                Polygon([(5, 0), (6, 0), (6, 1), (5, 1), (5, 0)]),
            ]),
        ]
        return gpd.GeoDataFrame(attributes, geometry=polygons, crs="EPSG:4269")

    @staticmethod
    def _synthetic_pins(frame):
        ids = sorted(frame["GEOID"].astype(str).tolist())
        return {
            "EXPECTED_GEOIDS": ids,
            "EXPECTED_REGION": set(frame["REGION"].astype(str)),
            "EXPECTED_DIVISION": set(frame["DIVISION"].astype(str)),
            "EXPECTED_LSAD": set(frame["LSAD"].astype(str)),
        }

    def _inspect_with_frame(self, frame, pins=None):
        members = [
            "tl_2026_us_state.cpg", "tl_2026_us_state.dbf", "tl_2026_us_state.prj",
            "tl_2026_us_state.shp", "tl_2026_us_state.shp.ea.iso.xml",
            "tl_2026_us_state.shp.iso.xml", "tl_2026_us_state.shx",
        ]
        if pins is None:
            pins = self._synthetic_pins(self._synthetic_frame())
        with patch("dagster_hifld.sources.us_state_boundaries.gpd") as stub, \
                patch.dict("dagster_hifld.sources.us_state_boundaries.__dict__", pins):
            stub.read_file.return_value = frame
            return _inspect_archive(self._archive(members), _release())

    def test_candidate_version_is_bound_to_resolved_vintage_and_code_identity(self):
        identity = {
            "code_commit": "packaged-sha256:" + "a" * 64,
            "code_identity_scheme": "sha256-packaged-dagster-hifld-v1",
            "files": {"src/dagster_hifld/example.py": "b" * 64},
            "working_tree_status": "unknown",
            "installed_distributions": [{"name": "dagster", "version": "1.11.0"}],
            "python": "3.12.14", "pyarrow": "23.0.1", "proj": "9.5.1", "geos": "3.13.1",
        }
        first = candidate_version(_release(), "c" * 64, identity)
        self.assertEqual(first, candidate_version(_release(), "c" * 64, identity))
        self.assertNotEqual(first, candidate_version(_release(2027), "c" * 64, identity))
        self.assertNotEqual(first, candidate_version(_release(), "d" * 64, identity))

    def test_state_pair_is_registered_for_derived_downstream_assets(self):
        self.assertIn((DATASET_SLUG, FILE_SLUG), SUPPORTED_DATASET_FILE_PAIRS)

    def test_rejects_incomplete_tiger_archive_member_roster(self):
        with self.assertRaisesRegex(ValueError, "tiger_archive_member_roster_drift"):
            _inspect_archive(self._archive(["tl_2026_us_state.shp"]), _release())

    def test_rejects_archive_path_traversal_before_extraction(self):
        names = [
            "../tl_2026_us_state.cpg", "tl_2026_us_state.dbf", "tl_2026_us_state.prj",
            "tl_2026_us_state.shp", "tl_2026_us_state.shp.ea.iso.xml",
            "tl_2026_us_state.shp.iso.xml", "tl_2026_us_state.shx",
        ]
        with self.assertRaisesRegex(ValueError, "tiger_archive_unsafe_member"):
            _inspect_archive(self._archive(names), _release())

    def test_inspect_rejects_missing_renamed_and_unexpected_columns(self):
        frame = self._synthetic_frame({"STUSPS": ["AL", "AK"], "EXTRA": ["x", "y"]})
        frame = frame.drop(columns=["NAME"])
        with self.assertRaisesRegex(ValueError, "tiger_source_schema_or_crs_drift"):
            self._inspect_with_frame(frame)

    def test_inspect_rejects_missing_id_and_bad_id_roster_and_structure(self):
        missing = self._synthetic_frame({"GEOID": ["01", None]})
        with self.assertRaisesRegex(ValueError, "tiger_source_population_or_id_drift"):
            self._inspect_with_frame(missing)
        renamed = self._synthetic_frame({"GEOID": ["01", "03"]})
        with self.assertRaisesRegex(ValueError, "tiger_source_geoid_roster_drift"):
            self._inspect_with_frame(renamed)
        structured = self._synthetic_frame({"GEOID": ["01", "002"]})
        pins = self._synthetic_pins(structured)
        with self.assertRaisesRegex(ValueError, "tiger_source_geoid_structure_drift"):
            self._inspect_with_frame(structured, pins)

    def test_inspect_rejects_geoidfq_and_statens_drift(self):
        fq = self._synthetic_frame({"GEOIDFQ": ["0400000US01", "0400000US02X"]})
        with self.assertRaisesRegex(ValueError, "tiger_source_geoidfq_structure_drift"):
            self._inspect_with_frame(fq)
        statens = self._synthetic_frame({"STATENS": ["1700001", "01700002"]})
        with self.assertRaisesRegex(ValueError, "tiger_source_statens_structure_drift"):
            self._inspect_with_frame(statens)

    def test_inspect_rejects_type_null_geometry_and_coverage_drift(self):
        typed = self._synthetic_frame({"ALAND": [10.5, 20.5]})
        typed["ALAND"] = typed["ALAND"].astype("float64")
        with self.assertRaisesRegex(ValueError, "tiger_source_area_physical_type_drift"):
            self._inspect_with_frame(typed)
        strings = self._synthetic_frame({"NAME": [1, 2]})
        with self.assertRaisesRegex(ValueError, "tiger_source_attribute_physical_type_drift"):
            self._inspect_with_frame(strings)
        classification = self._synthetic_frame({"MTFCC": ["G4001", "G4000"]})
        with self.assertRaisesRegex(ValueError, "tiger_source_classification_drift:MTFCC"):
            self._inspect_with_frame(classification)
        invalid = self._synthetic_frame()
        invalid.geometry = [Polygon([(0, 0), (1, 1), (0, 0)]), MultiPolygon([
            Polygon([(3, 0), (4, 0), (4, 1), (3, 1), (3, 0)]),
            Polygon([(5, 0), (6, 0), (6, 1), (5, 1), (5, 0)]),
        ])]
        with self.assertRaisesRegex(ValueError, "tiger_source_geometry_quality_failure"):
            self._inspect_with_frame(invalid)
        coverage = self._synthetic_frame()
        coverage.geometry = [
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
            Polygon([(200, 0), (201, 0), (201, 1), (200, 1), (200, 0)]),
        ]
        with self.assertRaisesRegex(ValueError, "tiger_source_coverage_extent_drift"):
            self._inspect_with_frame(coverage)

    def test_capture_failure_quarantines_evidence_and_raises(self):
        with tempfile.TemporaryDirectory(prefix="state-quarantine-") as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with patch("dagster_hifld.sources.census_tiger_common.httpx.Client",
                       side_effect=OSError("network_down")):
                with self.assertRaisesRegex(ValueError, "official_tiger_source_capture_failed"):
                    _capture(None, storage, "vtest")
            quarantine_keys = sorted(
                str(path.relative_to(Path(tmp) / "staging"))
                for path in (Path(tmp) / "staging").rglob("*") if path.is_file()
            )
            self.assertTrue(any("_quarantine/us-state-boundaries-1/" in key for key in quarantine_keys))
            self.assertTrue(any(key.endswith("raw/source.body") for key in quarantine_keys))
            self.assertTrue(any(key.endswith("metadata/failure.json") for key in quarantine_keys))

    def test_complete_source_gate_keeps_release_acceptance_false(self):
        result = us_state_boundaries_source_complete({
            "feature_count": 56,
            "source_population_complete": True,
            "source_archive_bytes": 1000,
            "source_archive_sha256": "a" * 64,
            "source_id_sha256": "b" * 64,
            "source_vintage": "2026",
        })
        self.assertTrue(result.passed)
        self.assertFalse(result.metadata["release_accepted"].value)
        self.assertFalse(result.metadata["rights_accepted"].value)
        failed = us_state_boundaries_source_complete({
            "feature_count": 0, "source_population_complete": True,
            "source_archive_bytes": 1000, "source_archive_sha256": "a" * 64,
            "source_id_sha256": "b" * 64, "source_vintage": "2026",
        })
        self.assertFalse(failed.passed)

    def test_native_geoparquet_output_preserves_leading_zeros_and_mixed_polygons(self):
        frame = self._synthetic_frame({"INTPTLAT": ["+32.0000000", "+64.0000000"],
                                       "INTPTLON": ["-086.000000", "-150.0000000"]})
        output = _geoparquet(frame)
        table = pq.read_table(pa.BufferReader(output))
        self.assertEqual(table.column_names, EXPECTED_FIELDS + ["geometry"])
        self.assertEqual(table["GEOID"].type, pa.string())
        self.assertEqual(table["GEOID"].to_pylist(), ["01", "02"])
        self.assertEqual(table["ALAND"].type, pa.int64())
        self.assertEqual(table["INTPTLAT"].to_pylist()[0], "+32.0000000")
        geometries = shapely.from_wkb(table["geometry"].to_pylist())
        self.assertEqual([geometry.geom_type for geometry in geometries], ["Polygon", "MultiPolygon"])
        self.assertTrue(all(shapely.equals_exact(a, b, tolerance=0.0) for a, b in zip(frame.geometry, geometries)))
        self.assertEqual(set(table.schema.metadata), {b"geo"})
        geo_metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo_metadata["version"], "1.1.0")
        self.assertEqual(geo_metadata["primary_column"], "geometry")
        column = geo_metadata["columns"]["geometry"]
        self.assertEqual(column["encoding"], "WKB")
        self.assertEqual(column["geometry_types"], ["MultiPolygon", "Polygon"])
        self.assertEqual(column["crs"], CRS.from_epsg(4269).to_json_dict())


if __name__ == "__main__":
    unittest.main()
