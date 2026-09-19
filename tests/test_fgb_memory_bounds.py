import asyncio
import math
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import fiona
import geopandas as gpd
from shapely.geometry import Polygon, mapping

from dagster_hifld.conversion import (
    PMTilesGenerationError,
    _estimate_feature_size_bytes,
    process_layer_chunked,
)
from dagster_hifld.resources import StagingStorageResource


def _polygon(vertex_count: int, offset: float = 0.0) -> Polygon:
    coordinates = [
        (
            offset + math.cos(2 * math.pi * index / vertex_count) / 1000,
            math.sin(2 * math.pi * index / vertex_count) / 1000,
        )
        for index in range(vertex_count)
    ]
    coordinates.append(coordinates[0])
    return Polygon(coordinates)


def _write_geopackage(path: Path, vertex_counts: list[int]) -> None:
    schema = {"geometry": "Polygon", "properties": {"name": "str"}}
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer="features",
        schema=schema,
        crs="EPSG:4326",
    ) as destination:
        for index, vertex_count in enumerate(vertex_counts):
            destination.write(
                {
                    "type": "Feature",
                    "id": str(index),
                    "properties": {"name": f"feature-{index}"},
                    "geometry": mapping(_polygon(vertex_count, index / 100_000)),
                }
            )


class FlatGeobufMemoryBoundsTests(unittest.TestCase):
    def test_estimator_fails_closed_when_complete_serialization_is_impossible(self):
        cyclic_feature = {"type": "Feature", "id": "cyclic"}
        cyclic_feature["properties"] = cyclic_feature

        with self.assertRaisesRegex(ValueError, "feature 'cyclic'"):
            _estimate_feature_size_bytes(cyclic_feature)

    def test_estimator_counts_complete_fiona_geometry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.gpkg"
            _write_geopackage(source, [10, 100_000])

            with fiona.open(source, layer="features") as collection:
                small, large = list(collection)

            self.assertGreater(
                _estimate_feature_size_bytes(large),
                _estimate_feature_size_bytes(small) * 1_000,
            )
            nested_feature = {
                "type": "Feature",
                "properties": large.properties,
                "geometry": large.geometry,
            }
            self.assertGreater(
                _estimate_feature_size_bytes(nested_feature),
                _estimate_feature_size_bytes(small) * 1_000,
            )

    def test_fgb_chunks_are_bounded_by_geometry_bytes_and_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.gpkg"
            vertex_counts = [10_000] * 12 + [10] * 10_005
            _write_geopackage(source, vertex_counts)
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            chunks: list[tuple[int, int]] = []

            def capture_fgb(frame: gpd.GeoDataFrame, path: Path, **kwargs: str) -> None:
                del path, kwargs
                feature_bytes = sum(
                    _estimate_feature_size_bytes(feature)
                    for feature in frame.iterfeatures()
                )
                chunks.append((len(frame), feature_bytes))

            with (
                patch.object(gpd.GeoDataFrame, "to_file", new=capture_fgb),
                patch(
                    "dagster_hifld.conversion._create_and_upload_pmtiles",
                    new=AsyncMock(return_value="published.pmtiles"),
                ),
            ):
                result = asyncio.run(
                    process_layer_chunked(
                        file_path=source,
                        format_type="geopackage",
                        layer_name="features",
                        layer_filename="features",
                        dest_folder="output",
                        dest_storage=storage,
                        work_dir=Path(tmpdir) / "work",
                        fgb_chunk_size_mb=1,
                        skip_parquet=True,
                    )
                )

            self.assertEqual(result["feature_count"], len(vertex_counts))
            self.assertEqual(sum(rows for rows, _ in chunks), len(vertex_counts))
            self.assertTrue(all(rows <= 10_000 for rows, _ in chunks))
            self.assertTrue(all(size <= 1024 * 1024 for _, size in chunks))

    def test_single_feature_larger_than_fgb_budget_fails_clearly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.gpkg"
            _write_geopackage(source, [100_000])
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            with self.assertRaisesRegex(
                PMTilesGenerationError,
                r"Feature .* exceeds.*FlatGeobuf.*budget.*bytes",
            ):
                asyncio.run(
                    process_layer_chunked(
                        file_path=source,
                        format_type="geopackage",
                        layer_name="features",
                        layer_filename="features",
                        dest_folder="output",
                        dest_storage=storage,
                        work_dir=Path(tmpdir) / "work",
                        fgb_chunk_size_mb=1,
                        skip_parquet=True,
                    )
                )


if __name__ == "__main__":
    unittest.main()
