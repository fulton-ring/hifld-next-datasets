import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch
import zipfile

import geopandas as gpd
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from shapely.geometry import Point

from dagster_hifld.conversion import (
    DEFAULT_GEOPARQUET_AGGREGATE_BUFFER_BYTES,
    DEFAULT_GEOPARQUET_MAX_ROW_GROUP_BYTES,
    DEFAULT_GEOPARQUET_TARGET_FILE_BYTES,
    DEFAULT_GEOPARQUET_WRITE_BUFFER_BYTES,
    DEFAULT_LARGE_GEOPARQUET_THRESHOLD_BYTES,
    GeoParquetWritePolicy,
    ShapefileZipPolicy,
    _PreflightHistogramStore,
    _StorageAdapter,
    _allocate_feature_bytes,
    _allocate_semantic_hive_keys,
    _build_tippecanoe_cmd,
    _canonicalize_geoparquet_batch_metadata,
    _create_and_upload_pmtiles,
    _detect_format_from_path,
    _discover_staged_formats,
    _estimate_feature_size_bytes,
    _geodataframe_to_geoparquet_arrow,
    _hilbert_like_key,
    _layer_output_namespace,
    _policy_s2_levels,
    _row_group_uncompressed_sizes,
    _s2_cells_for_point,
    _select_s2_level,
    geoparquet_policy_for,
    _to_wgs84,
    _write_geodataframe_parquet,
    process_layer_partitioned_geoparquet,
    process_layer_chunked,
    process_staged_dataset_version,
    write_geopackage_chunked,
    write_shapefile_zip,
)
from dagster_hifld.resources import StagingStorageResource


class ConversionTests(unittest.TestCase):
    @staticmethod
    def _write_shapefile(path: Path, name: str) -> Path:
        path.mkdir(parents=True, exist_ok=True)
        shapefile = path / f"{name}.shp"
        gpd.GeoDataFrame(
            {"name": [name]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326",
        ).to_file(shapefile)
        return shapefile

    def test_to_wgs84_handles_crs_to_epsg_failure(self):
        class BadCRS:
            def to_epsg(self):
                raise RuntimeError("bad crs")

        gdf = Mock()
        converted = Mock()
        gdf.crs = BadCRS()
        gdf.to_crs.return_value = converted

        result = _to_wgs84(gdf)

        self.assertIs(result, converted)
        gdf.to_crs.assert_called_once_with("EPSG:4326")

    def test_storage_adapter_roundtrips_logical_upload_and_qualified_reads(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.bin"
            source.write_bytes(b"payload")
            resource = StagingStorageResource(
                local_dir=tmpdir,
                prefix="tenant",
                use_local=True,
            )
            adapter = _StorageAdapter(resource)
            logical_key = "tenant/file/v1/geoparquet/file.parquet"

            qualified_key = asyncio.run(adapter.upload_file(source, logical_key))

            self.assertEqual(
                qualified_key,
                "tenant/tenant/file/v1/geoparquet/file.parquet",
            )
            self.assertEqual(adapter.qualify_key(logical_key), qualified_key)
            self.assertEqual(adapter.logical_key(qualified_key), logical_key)
            self.assertEqual(
                asyncio.run(adapter.list_files("tenant/file/v1/geoparquet")),
                [qualified_key],
            )
            self.assertTrue(asyncio.run(adapter.file_exists(qualified_key)))
            self.assertEqual(asyncio.run(adapter.read_bytes(qualified_key)), b"payload")
            downloaded = Path(tmpdir) / "downloaded.bin"
            asyncio.run(adapter.download_file(qualified_key, downloaded))
            self.assertEqual(downloaded.read_bytes(), b"payload")
            self.assertEqual(
                adapter.path_to_storage_uri(qualified_key),
                str(Path(tmpdir).resolve() / qualified_key),
            )

    def test_process_staged_dataset_version_uses_single_asyncio_run(self):
        staging_storage = Mock()
        published_storage = Mock()
        keys = ["dataset/file/v1/shapefile/example.shp"]

        with patch(
            "dagster_hifld.conversion._process_staged_dataset_version_async",
            new=Mock(return_value="sentinel-coro"),
        ) as async_impl, patch(
            "dagster_hifld.conversion.asyncio.run",
            return_value={"success": True, "layers": []},
        ) as asyncio_run:
            result = process_staged_dataset_version(
                staging_storage=staging_storage,
                published_storage=published_storage,
                keys=keys,
            )

        self.assertEqual(result, {"success": True, "layers": []})
        self.assertEqual(asyncio_run.call_count, 1)
        async_impl.assert_called_once_with(
            staging_storage=staging_storage,
            published_storage=published_storage,
            keys=keys,
        )

    def test_detect_format_treats_gdb_zip_as_file_geodatabase(self):
        self.assertEqual(
            _detect_format_from_path("dataset/file/v1.0.0/file_geodatabase/source.gdb.zip"),
            "file_geodatabase",
        )

    def test_discover_staged_formats_extracts_zipped_file_geodatabase(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            fgdb_dir = version_dir / "file_geodatabase"
            fgdb_dir.mkdir()
            zip_path = fgdb_dir / "source.gdb.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("source.gdb/gdb", b"")
                zf.writestr("source.gdb/a00000001.gdbtable", b"")

            processed = _discover_staged_formats(version_dir)

            self.assertIn("file_geodatabase", processed)
            self.assertEqual(processed["file_geodatabase"]["format_type"], "file_geodatabase")
            self.assertEqual(processed["file_geodatabase"]["data_file"].name, "source.gdb")
            self.assertTrue(processed["file_geodatabase"]["data_file"].is_dir())

    def test_discover_staged_formats_reads_canonical_shapefile_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            shapefile = self._write_shapefile(version_dir / "shapefile", "source")

            processed = _discover_staged_formats(version_dir)

            self.assertEqual(processed["shapefile"]["format_type"], "shapefile")
            self.assertEqual(processed["shapefile"]["data_file"], shapefile)

    def test_discover_staged_formats_reads_nested_case_insensitive_canonical_sources(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            expected = {
                "geopackage": version_dir / "geopackage" / "nested" / "SOURCE.GPKG",
                "shapefile": version_dir / "shapefile" / "nested" / "SOURCE.SHP",
                "geojson": version_dir / "geojson" / "nested" / "SOURCE.JSON",
            }
            geodatabase = (
                version_dir
                / "file_geodatabase"
                / "nested"
                / "SOURCE.GDB"
            )
            for path in expected.values():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"source")
            geodatabase.mkdir(parents=True)
            (geodatabase / "a00000001.gdbtable").write_bytes(b"gdb")

            processed = _discover_staged_formats(version_dir)

            self.assertEqual(set(processed), set(expected) | {"file_geodatabase"})
            self.assertEqual(processed["geopackage"]["data_file"], expected["geopackage"])
            self.assertEqual(processed["file_geodatabase"]["data_file"], geodatabase)
            self.assertEqual(processed["shapefile"]["data_file"], expected["shapefile"])
            self.assertEqual(processed["geojson"]["data_file"], expected["geojson"])

    def test_discover_staged_formats_rejects_ambiguous_nested_canonical_shapefiles(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            shapefile_dir = Path(tmpdir) / "shapefile"
            for name in ("first", "second"):
                path = shapefile_dir / name / f"{name}.shp"
                path.parent.mkdir(parents=True)
                path.write_bytes(b"shape")

            with self.assertRaisesRegex(ValueError, "multiple canonical shapefile"):
                _discover_staged_formats(Path(tmpdir))

    def test_canonical_shapefile_wins_over_ambiguous_legacy_unknown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            canonical = self._write_shapefile(version_dir / "shapefile", "canonical")
            unknown = version_dir / "unknown"
            self._write_shapefile(unknown, "legacy-a")
            self._write_shapefile(unknown, "legacy-b")

            processed = _discover_staged_formats(version_dir)

            self.assertEqual(processed["shapefile"]["data_file"], canonical)

    def test_discover_staged_formats_reads_one_complete_readable_legacy_shapefile(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            unknown = version_dir / "unknown"
            shapefile = self._write_shapefile(unknown, "legacy")

            processed = _discover_staged_formats(version_dir)

            self.assertEqual(processed["shapefile"]["data_file"], shapefile)

    def test_discover_staged_formats_rejects_multiple_legacy_shapefiles(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            unknown = version_dir / "unknown"
            self._write_shapefile(unknown, "legacy-a")
            self._write_shapefile(unknown, "legacy-b")

            with self.assertRaisesRegex(ValueError, "multiple Shapefile datasets"):
                _discover_staged_formats(version_dir)

    def test_discover_staged_formats_rejects_incomplete_legacy_unknown_contents(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            unknown = version_dir / "unknown"
            unknown.mkdir()
            (unknown / "legacy.shp").write_bytes(b"not a shapefile")
            (unknown / "notes.txt").write_text("ambiguous", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "complete readable Shapefile"):
                _discover_staged_formats(version_dir)

    def test_discover_staged_formats_ignores_non_shapefile_legacy_unknown_contents(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            unknown = version_dir / "unknown"
            unknown.mkdir()
            (unknown / "notes.txt").write_text("ambiguous", encoding="utf-8")

            try:
                processed = _discover_staged_formats(version_dir)
            except ValueError as exc:
                self.fail(f"CSV-only unknown contents should not raise: {exc}")
            self.assertEqual(processed, {})

    def test_discover_staged_formats_ignores_derived_output_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            version_dir = Path(tmpdir)
            geoparquet = version_dir / "geoparquet"
            pmtiles = version_dir / "pmtiles"
            geoparquet.mkdir()
            pmtiles.mkdir()
            (geoparquet / "source.parquet").write_bytes(b"parquet")
            (pmtiles / "source.pmtiles").write_bytes(b"pmtiles")

            self.assertEqual(_discover_staged_formats(version_dir), {})

    def test_process_layer_chunked_creates_nested_work_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir) / "nested" / "geoparquet"

            with patch(
                "dagster_hifld.conversion.fiona.open",
                side_effect=RuntimeError("stop before real I/O"),
            ):
                result = asyncio.run(
                    process_layer_chunked(
                        file_path=Path("source.gpkg"),
                        format_type="geopackage",
                        layer_name=None,
                        layer_filename="source",
                        dest_folder="dataset/file/v1",
                        dest_storage=Mock(),
                        work_dir=work_dir,
                    )
                )

            self.assertIn("error", result)
            self.assertTrue((work_dir / "geoparquet").is_dir())
            self.assertTrue((work_dir / "pmtiles").is_dir())

    def test_process_layer_chunked_applies_large_geojson_config_to_parquet_reads(self):
        import dagster_hifld.conversion as conversion_module

        env_calls = []

        class FakeEnv:
            def __init__(self, **kwargs):
                env_calls.append(kwargs)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(conversion_module.fiona, "Env", FakeEnv), patch(
                "dagster_hifld.conversion.fiona.open",
                side_effect=RuntimeError("stop before real I/O"),
            ):
                result = asyncio.run(
                    process_layer_chunked(
                        file_path=Path("source.geojson"),
                        format_type="geojson",
                        layer_name=None,
                        layer_filename="source",
                        dest_folder="dataset/file/v1",
                        dest_storage=Mock(),
                        work_dir=Path(tmpdir),
                        skip_pmtiles=True,
                    )
                )

        self.assertIn("error", result)
        self.assertIn({"OGR_GEOJSON_MAX_OBJ_SIZE": "0"}, env_calls)

    def test_large_geojson_context_applies_pyogrio_gdal_config(self):
        import pyogrio
        from dagster_hifld.gdal import with_large_geojson_support

        calls = []

        class FakeEnv:
            def __init__(self, **kwargs):
                calls.append(("fiona", kwargs))

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch("dagster_hifld.gdal.fiona.Env", FakeEnv), patch.object(
            pyogrio,
            "get_gdal_config_option",
            return_value="12",
        ) as get_config, patch.object(
            pyogrio,
            "set_gdal_config_options",
        ) as set_config:
            with with_large_geojson_support():
                pass

        get_config.assert_called_once_with("OGR_GEOJSON_MAX_OBJ_SIZE")
        set_config.assert_any_call({"OGR_GEOJSON_MAX_OBJ_SIZE": "0"})
        set_config.assert_any_call({"OGR_GEOJSON_MAX_OBJ_SIZE": "12"})
        self.assertIn(("fiona", {"OGR_GEOJSON_MAX_OBJ_SIZE": "0"}), calls)

    def test_process_layer_chunked_applies_large_geojson_config_to_pmtiles_reads(self):
        import dagster_hifld.conversion as conversion_module

        env_calls = []

        class FakeEnv:
            def __init__(self, **kwargs):
                env_calls.append(kwargs)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(conversion_module.fiona, "Env", FakeEnv), patch(
                "dagster_hifld.conversion.fiona.open",
                side_effect=RuntimeError("stop before real I/O"),
            ):
                result = asyncio.run(
                    process_layer_chunked(
                        file_path=Path("source.geojson"),
                        format_type="geojson",
                        layer_name=None,
                        layer_filename="source",
                        dest_folder="dataset/file/v1",
                        dest_storage=Mock(),
                        work_dir=Path(tmpdir),
                        skip_parquet=True,
                    )
                )

        self.assertIn("error", result)
        self.assertIn({"OGR_GEOJSON_MAX_OBJ_SIZE": "0"}, env_calls)

    def test_write_geopackage_chunked_applies_large_geojson_config(self):
        import dagster_hifld.conversion as conversion_module

        env_calls = []

        class FakeEnv:
            def __init__(self, **kwargs):
                env_calls.append(kwargs)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(conversion_module.fiona, "Env", FakeEnv), patch(
                "dagster_hifld.conversion.fiona.open",
                side_effect=RuntimeError("stop before real I/O"),
            ):
                with self.assertRaisesRegex(RuntimeError, "stop before real I/O"):
                    asyncio.run(
                        write_geopackage_chunked(
                            file_path=Path("source.geojson"),
                            format_type="geojson",
                            layer_name=None,
                            output_gpkg=Path(tmpdir) / "source.gpkg",
                        )
                    )

        self.assertIn({"OGR_GEOJSON_MAX_OBJ_SIZE": "0"}, env_calls)

    def test_write_geodataframe_parquet_retries_without_unsupported_schema_version(self):
        gdf = gpd.GeoDataFrame(
            {"name": ["A"]},
            geometry=[Point(0, 0)],
            crs="EPSG:4326",
        )
        calls = []

        def fake_to_parquet(path, **kwargs):
            calls.append(kwargs.copy())
            if "schema_version" in kwargs:
                raise TypeError("__cinit__() got an unexpected keyword argument 'schema_version'")

        gdf.to_parquet = fake_to_parquet

        _write_geodataframe_parquet(gdf, Path("out.parquet"), row_group_size=100)

        self.assertIn("schema_version", calls[0])
        self.assertNotIn("schema_version", calls[-1])
        self.assertEqual(calls[-1]["compression"], "zstd")
        self.assertEqual(calls[-1]["row_group_size"], 100)
        self.assertFalse(calls[-1]["index"])

    def test_process_layer_chunked_uses_shared_geoparquet_writer(self):
        features = [
            {
                "type": "Feature",
                "properties": {"name": "A"},
                "geometry": {"type": "Point", "coordinates": [0, 0]},
            }
        ]

        class FakeCollection:
            crs = "EPSG:4326"

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                return iter(features)

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("dagster_hifld.conversion.fiona.open", return_value=FakeCollection()), patch(
                "dagster_hifld.conversion._write_geodataframe_parquet"
            ) as writer, patch("dagster_hifld.conversion._upload_geoparquet_files", return_value=[]):
                writer.side_effect = lambda _gdf, path, **_kwargs: path.write_bytes(b"parquet")
                result = asyncio.run(
                    process_layer_chunked(
                        file_path=Path("source.gpkg"),
                        format_type="geopackage",
                        layer_name=None,
                        layer_filename="source",
                        dest_folder="dataset/file/v1",
                        dest_storage=Mock(),
                        work_dir=Path(tmpdir),
                        skip_pmtiles=True,
                    )
                )

        self.assertGreaterEqual(writer.call_count, 1)
        self.assertEqual(result["feature_count"], 1)

    def test_partitioned_geoparquet_writer_uploads_hive_paths_without_chunk_suffixes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gdf = gpd.GeoDataFrame(
                {"statefp": ["06", "12"], "name": ["A", "B"]},
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            )
            gdf.to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(force_admin_columns=("statefp",)),
                )
            )

            self.assertEqual(result["partitioning"], "admin")
            self.assertEqual(result["partition_columns"], ["statefp"])
            hive_key = result["hive_partition_columns"]["statefp"]
            self.assertIn(
                f"dataset/file/v1.0.0/geoparquet/layer-source/{hive_key}=v-06/part-000.parquet",
                result["geoparquet_paths"],
            )
            self.assertIn(
                f"dataset/file/v1.0.0/geoparquet/layer-source/{hive_key}=v-12/part-000.parquet",
                result["geoparquet_paths"],
            )
            self.assertFalse(any("-0.zstd.parquet" in path for path in result["geoparquet_paths"]))

    def test_partitioned_geoparquet_writer_derives_nested_huc_prefixes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gdf = gpd.GeoDataFrame(
                {
                    "huc12": ["010100020101", "010100030101"],
                    "name": ["A", "B"],
                },
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            )
            gdf.to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(
                        derived_huc_column="huc12",
                        derived_huc_partition_columns=("huc2", "huc4", "huc6"),
                    ),
                )
            )

            self.assertEqual(result["partitioning"], "derived_huc")
            self.assertEqual(result["partition_columns"], ["huc2", "huc4", "huc6"])
            hive_keys = result["hive_partition_columns"]
            self.assertIn(
                "dataset/file/v1.0.0/geoparquet/layer-source/"
                f"{hive_keys['huc2']}=v-01/{hive_keys['huc4']}=v-0101/"
                f"{hive_keys['huc6']}=v-010100/part-000.parquet",
                result["geoparquet_paths"],
            )
            self.assertFalse(any("huc12=" in path for path in result["geoparquet_paths"]))

    def test_geoparquet_policy_registry_uses_coarser_wbd_huc_partitions(self):
        wbd12 = geoparquet_policy_for("wbd", "12-digit-hu-subwatershed")
        self.assertEqual(wbd12.derived_huc_column, "huc12")
        self.assertEqual(wbd12.derived_huc_partition_columns, ("huc2",))

        wbd14 = geoparquet_policy_for("wbd", "14-digit-hu")
        self.assertEqual(wbd14, GeoParquetWritePolicy())

    def test_geoparquet_policy_registry_derives_nfhl_and_nhd_prefixes(self):
        nfhl = geoparquet_policy_for("nfhl", "national-flood-hazard-layer-line-nfhl-1")
        self.assertEqual(nfhl.derived_prefix_column, "DFIRM_ID")
        self.assertEqual(nfhl.derived_prefix_partitions, (("state_fips", 2),))
        self.assertLessEqual(nfhl.max_row_group_rows, 50_000)

        nhd = geoparquet_policy_for("nhd", "flowline-large-scale-2")
        self.assertEqual(nhd.derived_prefix_column, "REACHCODE")
        self.assertEqual(nhd.derived_prefix_partitions, (("huc2", 2),))

    def test_partitioned_geoparquet_writer_derives_generic_prefix_partition(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gdf = gpd.GeoDataFrame(
                {
                    "DFIRM_ID": ["29001C", "30001C"],
                    "name": ["A", "B"],
                },
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            )
            gdf.to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(
                        derived_prefix_column="DFIRM_ID",
                        derived_prefix_partitions=(("state_fips", 2),),
                    ),
                )
            )

            self.assertEqual(result["partitioning"], "derived_prefix")
            self.assertEqual(result["partition_columns"], ["state_fips"])
            hive_key = result["hive_partition_columns"]["state_fips"]
            self.assertIn(
                f"dataset/file/v1.0.0/geoparquet/layer-source/{hive_key}=v-29/part-000.parquet",
                result["geoparquet_paths"],
            )

    def test_streaming_geoparquet_writer_keeps_schema_stable_for_sparse_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gdf = gpd.GeoDataFrame(
                {
                    "statefp": ["06"] * 27,
                    "sometimes_null": [None] * 26 + ["present"],
                },
                geometry=[Point(float(i), 0) for i in range(27)],
                crs="EPSG:4326",
            )
            gdf.to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(
                        force_admin_columns=("statefp",),
                        target_row_group_bytes=1,
                    ),
                )
            )

            self.assertNotIn("error", result)
            self.assertEqual(result["feature_count"], 27)
            self.assertEqual(result["partitioning"], "admin")
            hive_key = result["hive_partition_columns"]["statefp"]
            self.assertEqual(
                result["geoparquet_paths"],
                [
                    "dataset/file/v1.0.0/geoparquet/"
                    f"layer-source/{hive_key}=v-06/part-000.parquet"
                ],
            )

    def test_streaming_geoparquet_writer_keeps_small_layers_unpartitioned(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gdf = gpd.GeoDataFrame(
                {"statefp": ["06", "12"], "name": ["A", "B"]},
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            )
            gdf.to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(candidate_admin_columns=("statefp",)),
                )
            )

            self.assertEqual(result["partitioning"], "single_file")
            self.assertEqual(result["partition_columns"], [])
            self.assertEqual(
                result["geoparquet_paths"],
                ["dataset/file/v1.0.0/geoparquet/source.parquet"],
            )

    def test_geoparquet_policy_defaults_use_bounded_layout_budgets(self):
        policy = GeoParquetWritePolicy()

        self.assertEqual(DEFAULT_LARGE_GEOPARQUET_THRESHOLD_BYTES, 2 * 1024**3)
        self.assertEqual(DEFAULT_GEOPARQUET_TARGET_FILE_BYTES, 2 * 1024**3)
        self.assertEqual(DEFAULT_GEOPARQUET_WRITE_BUFFER_BYTES, 112 * 1024**2)
        self.assertEqual(DEFAULT_GEOPARQUET_MAX_ROW_GROUP_BYTES, 128 * 1024**2)
        self.assertEqual(DEFAULT_GEOPARQUET_AGGREGATE_BUFFER_BYTES, 512 * 1024**2)
        self.assertEqual(policy.s2_candidate_levels, tuple(range(2, 17)))

    def test_streaming_writer_resolves_forced_admin_column_case_insensitively(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"STATEFP": ["06", "12"]},
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(force_admin_columns=("statefp",)),
                )
            )

            self.assertNotIn("error", result)
            self.assertEqual(result["partition_columns"], ["STATEFP"])
            hive_key = result["hive_partition_columns"]["STATEFP"]
            self.assertTrue(
                any(
                    f"geoparquet/layer-source/{hive_key}=v-06/" in path
                    for path in result["geoparquet_paths"]
                )
            )

    def test_streaming_writer_uses_first_present_forced_admin_alternative(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"COUNTYFP": ["001", "003"]},
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(
                        force_admin_columns=("STATEFP", "countyfp")
                    ),
                )
            )

            self.assertNotIn("error", result)
            self.assertEqual(result["partition_columns"], ["COUNTYFP"])

    def test_streaming_writer_rejects_when_no_forced_admin_alternative_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"name": ["A"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(
                        force_admin_columns=("STATEFP", "COUNTYFP")
                    ),
                )
            )

            self.assertIn("force_admin_columns alternatives", result["error"])
            self.assertIn("'STATEFP', 'COUNTYFP'", result["error"])

    def test_semantic_partition_paths_are_injective_and_hive_reader_preserves_values(
        self,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            expected = ["a/b", "a-b", None, "__null__", "06", "12"]
            gpd.GeoDataFrame(
                {"STATEFP": expected},
                geometry=[Point(index, index) for index in range(len(expected))],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(force_admin_columns=("statefp",)),
                )
            )

            self.assertNotIn("error", result)
            partition_segments = {
                Path(path).parent.name for path in result["geoparquet_paths"]
            }
            hive_key = result["hive_partition_columns"]["STATEFP"]
            self.assertEqual(len(partition_segments), len(expected))
            self.assertIn(f"{hive_key}=v-a%2Fb", partition_segments)
            self.assertIn(f"{hive_key}=v-a-b", partition_segments)
            self.assertIn(f"{hive_key}=n", partition_segments)
            self.assertIn(f"{hive_key}=v-__null__", partition_segments)

            dataset = ds.dataset(
                Path(tmpdir) / "work" / "geoparquet" / "layer-source",
                format="parquet",
                partitioning="hive",
            )
            table = dataset.to_table()
            self.assertIn("STATEFP", table.column_names)
            self.assertIn(hive_key, table.column_names)
            self.assertCountEqual(table.column("STATEFP").to_pylist(), expected)
            self.assertIn("06", table.column("STATEFP").to_pylist())

    def test_semantic_hive_keys_avoid_source_and_normalized_name_collisions(self):
        source_columns = {"STATEFP", "partition_statefp", "a-b", "a_b"}

        mapping = _allocate_semantic_hive_keys(
            ["STATEFP", "a-b", "a_b"], source_columns
        )

        self.assertEqual(len({value.casefold() for value in mapping.values()}), 3)
        self.assertTrue(
            all(
                value.casefold()
                not in {column.casefold() for column in source_columns}
                for value in mapping.values()
            )
        )
        self.assertNotEqual(mapping["a-b"], mapping["a_b"])

    def test_hive_reader_preserves_source_column_that_matches_synthetic_key_prefix(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            expected_columns = {
                "STATEFP": ["06", "12"],
                "partition_statefp": ["original-a", "original-b"],
                "a-b": ["dash-a", "dash-b"],
                "a_b": ["underscore-a", "underscore-b"],
            }
            gpd.GeoDataFrame(
                expected_columns,
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(force_admin_columns=("statefp",)),
                )
            )

            hive_key = result["hive_partition_columns"]["STATEFP"]
            self.assertNotIn(
                hive_key.casefold(),
                {name.casefold() for name in expected_columns},
            )
            self.assertEqual(
                result["layout"]["hive_partition_columns"]["STATEFP"], hive_key
            )
            dataset = ds.dataset(
                Path(tmpdir) / "work" / "geoparquet" / "layer-source",
                format="parquet",
                partitioning="hive",
            )
            table = dataset.to_table()
            for column, expected in expected_columns.items():
                self.assertCountEqual(table.column(column).to_pylist(), expected)

    def test_feature_byte_accounting_uses_utf8_and_allocates_exact_batch_total(self):
        ascii_feature = {"type": "Feature", "properties": {"name": "a"}}
        unicode_feature = {"type": "Feature", "properties": {"name": "é"}}

        self.assertEqual(
            _estimate_feature_size_bytes(unicode_feature)
            - _estimate_feature_size_bytes(ascii_feature),
            1,
        )
        allocations = _allocate_feature_bytes([1, 2, 10], 17)
        self.assertEqual(sum(allocations), 17)
        self.assertTrue(all(allocation >= 1 for allocation in allocations))

    def test_streaming_writer_rejects_missing_configured_source_column(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"name": ["A"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(derived_prefix_column="DFIRM_ID"),
                )
            )

            self.assertIn("Configured derived_prefix_column 'DFIRM_ID'", result["error"])

    def test_select_s2_level_uses_coarsest_bin_under_target_and_caps_at_16(self):
        histograms = {
            2: {"a": 300},
            3: {"a": 120, "b": 180},
            4: {"a": 80, "b": 90},
        }
        self.assertEqual(_select_s2_level(histograms, 100, (2, 3, 4)), 4)
        self.assertEqual(_select_s2_level(histograms, 10, (2, 3, 4)), 4)

    def test_legacy_s2_policy_fields_still_customize_candidate_levels(self):
        policy = GeoParquetWritePolicy(s2_parent_candidates=(5, 6), s2_fine_level=14)

        self.assertEqual(_policy_s2_levels(policy), (5, 6))
        self.assertEqual(policy.s2_fine_level, 14)

    def test_preflight_adds_s2_for_large_total_but_keeps_semantic_partition(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"STATEFP": ["06", "06", "12"]},
                geometry=[Point(0, 0), Point(0.1, 0.1), Point(30, 30)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            common = dict(
                file_path=source,
                format_type="geojson",
                layer_name=None,
                layer_filename="source",
                dest_folder="dataset/file/v1.0.0/",
                dest_storage=_StorageAdapter(storage),
            )

            unpartitioned = asyncio.run(
                process_layer_partitioned_geoparquet(
                    **common,
                    work_dir=Path(tmpdir) / "work-total",
                    policy=GeoParquetWritePolicy(
                        large_dataset_threshold_bytes=1,
                        target_file_size_bytes=10**9,
                    ),
                )
            )
            semantic = asyncio.run(
                process_layer_partitioned_geoparquet(
                    **common,
                    work_dir=Path(tmpdir) / "work-semantic",
                    policy=GeoParquetWritePolicy(
                        force_admin_columns=("statefp",),
                        large_dataset_threshold_bytes=1,
                        target_file_size_bytes=10**9,
                    ),
                )
            )

            self.assertEqual(unpartitioned["partitioning"], "s2")
            self.assertEqual(unpartitioned["chosen_s2_level"], 2)
            self.assertEqual(semantic["partitioning"], "admin")
            self.assertEqual(semantic["partition_columns"], ["STATEFP"])
            self.assertIsNone(semantic["chosen_s2_level"])

    def test_preflight_calculates_s2_cell_once_per_feature_for_all_candidate_levels(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"name": ["a", "b"]},
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            with patch(
                "dagster_hifld.conversion._s2_cells_for_point",
                wraps=_s2_cells_for_point,
            ) as calculate_cells:
                asyncio.run(
                    process_layer_partitioned_geoparquet(
                        file_path=source,
                        format_type="geojson",
                        layer_name=None,
                        layer_filename="source",
                        dest_folder="dataset/file/v1.0.0/",
                        dest_storage=_StorageAdapter(storage),
                        work_dir=Path(tmpdir) / "work",
                        policy=GeoParquetWritePolicy(force_s2=True),
                    )
                )

            self.assertEqual(calculate_cells.call_count, 2)

    def test_preflight_does_not_store_unused_standalone_s2_histograms(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"STATEFP": ["06", "12"]},
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)
            captured: list[tuple[str, str, int, str, int, int]] = []
            original_add_many = _PreflightHistogramStore.add_many

            def capture_add_many(store, rows):
                materialized = list(rows)
                captured.extend(materialized)
                original_add_many(store, materialized)

            with patch.object(_PreflightHistogramStore, "add_many", capture_add_many):
                asyncio.run(
                    process_layer_partitioned_geoparquet(
                        file_path=source,
                        format_type="geojson",
                        layer_name=None,
                        layer_filename="source",
                        dest_folder="dataset/file/v1.0.0/",
                        dest_storage=_StorageAdapter(storage),
                        work_dir=Path(tmpdir) / "work",
                        policy=GeoParquetWritePolicy(
                            force_admin_columns=("STATEFP",),
                            large_dataset_threshold_bytes=1,
                        ),
                    )
                )

            self.assertNotIn("s2", {row[0] for row in captured})
            self.assertNotIn("semantic_s2", {row[0] for row in captured})

    def test_dense_level_16_cell_rolls_files_and_excludes_internal_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"name": ["c", "a", "b"]},
                geometry=[Point(0, 0), Point(0, 0), Point(0, 0)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(
                        force_s2=True,
                        target_file_size_bytes=1,
                        write_buffer_bytes=1,
                        aggregate_buffer_bytes=1,
                    ),
                )
            )

            self.assertEqual(result["chosen_s2_level"], 16)
            self.assertEqual(len(result["geoparquet_paths"]), 3)
            for output in result["layout"]["outputs"]:
                parquet_path = Path(tmpdir) / output["path"]
                columns = pq.ParquetFile(parquet_path).schema_arrow.names
                self.assertNotIn("s2_cell", columns)
                self.assertNotIn("s2_parent_cell", columns)
                self.assertNotIn("hilbert_cell", columns)

    def test_force_s2_hive_reader_preserves_source_internal_like_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            expected_columns = {
                "s2_parent_cell": ["original-parent-a", "original-parent-b"],
                "s2_cell": ["original-cell-a", "original-cell-b"],
                "hilbert_cell": ["original-hilbert-a", "original-hilbert-b"],
            }
            gpd.GeoDataFrame(
                expected_columns,
                geometry=[Point(0, 0), Point(1, 1)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(force_s2=True),
                )
            )

            hive_key = result["hive_partition_columns"]["s2_parent_cell"]
            self.assertNotIn(
                hive_key.casefold(),
                {column.casefold() for column in expected_columns},
            )
            dataset = ds.dataset(
                Path(tmpdir) / "work" / "geoparquet" / "layer-source",
                format="parquet",
                partitioning="hive",
            )
            table = dataset.to_table()
            self.assertIn(hive_key, table.column_names)
            for column, expected in expected_columns.items():
                self.assertCountEqual(table.column(column).to_pylist(), expected)

    def test_dense_s2_cell_rolls_at_target_file_size_with_large_memory_budgets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"name": ["a", "b", "c"]},
                geometry=[Point(0, 0), Point(0, 0), Point(0, 0)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(
                        force_s2=True,
                        target_file_size_bytes=1,
                        write_buffer_bytes=10**9,
                        aggregate_buffer_bytes=10**9,
                    ),
                )
            )

            self.assertEqual(result["chosen_s2_level"], 16)
            self.assertEqual(len(result["geoparquet_paths"]), 3)
            self.assertTrue(result["geoparquet_paths"][0].endswith("part-000.parquet"))
            self.assertTrue(result["geoparquet_paths"][1].endswith("part-001.parquet"))
            self.assertTrue(result["geoparquet_paths"][2].endswith("part-002.parquet"))

    def test_partitioned_layer_namespace_is_non_hive_and_collision_proof(self):
        slash_name = _layer_output_namespace("roads/east")
        dash_name = _layer_output_namespace("roads-east")

        self.assertNotEqual(slash_name, dash_name)
        self.assertNotIn("/", slash_name)
        self.assertNotIn("=", slash_name)

    def test_writer_sorts_each_buffer_spatially_and_keeps_bbox_covering_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            points = [Point(120, 20), Point(-120, -20), Point(30, -10), Point(-30, 10)]
            gpd.GeoDataFrame(
                {"name": ["d", "a", "c", "b"]}, geometry=points, crs="EPSG:4326"
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(),
                )
            )

            output_path = Path(tmpdir) / result["geoparquet_paths"][0]
            written = gpd.read_parquet(output_path)
            keys = [_hilbert_like_key(point.x, point.y) for point in written.geometry]
            self.assertEqual(keys, sorted(keys))
            geo = json.loads(pq.ParquetFile(output_path).schema_arrow.metadata[b"geo"])
            self.assertIn("covering", geo["columns"][geo["primary_column"]])
            self.assertEqual(
                result["layout"]["outputs"][0]["relative_path"],
                "geoparquet/source.parquet",
            )

    def test_batch_metadata_canonicalization_preserves_covering_and_schema(self):
        first = _geodataframe_to_geoparquet_arrow(
            gpd.GeoDataFrame(
                {"name": ["a"]},
                geometry=[Point(0, 0)],
                crs="EPSG:4326",
            )
        )
        second = _geodataframe_to_geoparquet_arrow(
            gpd.GeoDataFrame(
                {"name": ["b"]},
                geometry=[Point(10, 20)],
                crs="EPSG:4326",
            )
        )

        first_canonical = _canonicalize_geoparquet_batch_metadata(first)
        second_canonical = _canonicalize_geoparquet_batch_metadata(second)
        first_geo = json.loads(first_canonical.schema.metadata[b"geo"])

        self.assertTrue(first_canonical.schema.equals(second_canonical.schema))
        self.assertNotIn("bbox", first_geo["columns"]["geometry"])
        self.assertIn("covering", first_geo["columns"]["geometry"])

    def test_row_group_hard_limit_rewrites_smaller_files_before_upload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"name": ["a", "b", "c"]},
                geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            def simulated_sizes(path):
                return [200] if pq.ParquetFile(path).metadata.num_rows > 1 else [50]

            with patch(
                "dagster_hifld.conversion._row_group_uncompressed_sizes",
                side_effect=simulated_sizes,
            ):
                result = asyncio.run(
                    process_layer_partitioned_geoparquet(
                        file_path=source,
                        format_type="geojson",
                        layer_name=None,
                        layer_filename="source",
                        dest_folder="dataset/file/v1.0.0/",
                        dest_storage=_StorageAdapter(storage),
                        work_dir=Path(tmpdir) / "work",
                        policy=GeoParquetWritePolicy(max_row_group_bytes=100),
                    )
                )

            self.assertNotIn("error", result)
            self.assertEqual(result["feature_count"], 3)
            self.assertEqual(len(result["geoparquet_paths"]), 1)
            self.assertEqual(result["layout"]["outputs"][0]["row_counts"], [1, 1, 1])

    def test_singleton_oversize_fails_without_uploading_parquet(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"name": ["oversize"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            with patch(
                "dagster_hifld.conversion._row_group_uncompressed_sizes",
                return_value=[200],
            ):
                result = asyncio.run(
                    process_layer_partitioned_geoparquet(
                        file_path=source,
                        format_type="geojson",
                        layer_name=None,
                        layer_filename="source",
                        dest_folder="dataset/file/v1.0.0/",
                        dest_storage=_StorageAdapter(storage),
                        work_dir=Path(tmpdir) / "work",
                        policy=GeoParquetWritePolicy(max_row_group_bytes=100),
                    )
                )

            self.assertIn("single feature", result["error"].lower())
            self.assertEqual(
                list((Path(tmpdir) / "dataset/file/v1.0.0").rglob("*.parquet")), []
            )

    def test_aggregate_buffer_budget_forces_partition_flushes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"STATEFP": ["06", "12", "06", "12"]},
                geometry=[Point(0, 0), Point(10, 10), Point(1, 1), Point(11, 11)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(
                        force_admin_columns=("statefp",),
                        write_buffer_bytes=10**9,
                        aggregate_buffer_bytes=1,
                    ),
                )
            )

            self.assertEqual(len(result["geoparquet_paths"]), 2)
            hive_key = result["hive_partition_columns"]["STATEFP"]
            self.assertTrue(
                any(
                    f"{hive_key}=v-06/part-000.parquet" in path
                    for path in result["geoparquet_paths"]
                )
            )
            self.assertTrue(
                any(
                    f"{hive_key}=v-12/part-000.parquet" in path
                    for path in result["geoparquet_paths"]
                )
            )
            self.assertCountEqual(
                [output["row_counts"] for output in result["layout"]["outputs"]],
                [[1, 1], [1, 1]],
            )

    def test_multiple_buffer_flushes_append_row_groups_to_one_partition_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"STATEFP": ["06", "06", "06"]},
                geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(
                        force_admin_columns=("statefp",),
                        write_buffer_bytes=1,
                        aggregate_buffer_bytes=10**9,
                        target_file_size_bytes=10**9,
                    ),
                )
            )

            self.assertNotIn("error", result)
            self.assertEqual(len(result["geoparquet_paths"]), 1)
            output = result["layout"]["outputs"][0]
            self.assertEqual(output["row_counts"], [1, 1, 1])
            parquet_path = Path(tmpdir) / output["path"]
            self.assertEqual(pq.ParquetFile(parquet_path).metadata.num_row_groups, 3)

    def test_preflight_and_write_feature_count_mismatch_fails_before_upload(self):
        features = [
            {
                "type": "Feature",
                "properties": {"name": name},
                "geometry": {"type": "Point", "coordinates": [index, index]},
            }
            for index, name in enumerate(("a", "b"))
        ]

        class FakeCollection:
            crs = "EPSG:4326"
            schema = {"properties": {"name": "str"}, "geometry": "Point"}

            def __init__(self, collection_features):
                self.collection_features = collection_features

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                return iter(self.collection_features)

        storage = Mock()
        storage.upload_file = AsyncMock()
        with tempfile.TemporaryDirectory() as tmpdir, patch(
            "dagster_hifld.conversion.fiona.open",
            side_effect=[FakeCollection(features), FakeCollection(features[:1])],
        ):
            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=Path("source.geojson"),
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=storage,
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(preflight_chunk_rows=1),
                )
            )

        self.assertIn("feature count validation failed", result["error"])
        storage.upload_file.assert_not_called()

    def test_derived_source_column_resolution_preserves_source_spelling(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"DFIRM_ID": ["29001C"]}, geometry=[Point(0, 0)], crs="EPSG:4326"
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(
                        derived_prefix_column="dfirm_id",
                        derived_prefix_partitions=(("state_fips", 2),),
                    ),
                )
            )

            self.assertNotIn("error", result)
            self.assertEqual(result["partitioning"], "derived_prefix")
            hive_key = result["hive_partition_columns"]["state_fips"]
            self.assertIn(
                f"{hive_key}=v-29", result["geoparquet_paths"][0]
            )

    def test_spatial_sort_keeps_features_with_null_geometry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"name": ["missing", "point"]},
                geometry=[None, Point(1, 1)],
                crs="EPSG:4326",
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            result = asyncio.run(
                process_layer_partitioned_geoparquet(
                    file_path=source,
                    format_type="geojson",
                    layer_name=None,
                    layer_filename="source",
                    dest_folder="dataset/file/v1.0.0/",
                    dest_storage=_StorageAdapter(storage),
                    work_dir=Path(tmpdir) / "work",
                    policy=GeoParquetWritePolicy(),
                )
            )

            self.assertNotIn("error", result)
            self.assertEqual(result["feature_count"], 2)

    def test_manifest_hashing_does_not_read_whole_parquet_into_memory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "source.geojson"
            gpd.GeoDataFrame(
                {"name": ["point"]}, geometry=[Point(1, 1)], crs="EPSG:4326"
            ).to_file(source, driver="GeoJSON")
            storage = StagingStorageResource(local_dir=tmpdir, use_local=True)

            with patch.object(Path, "read_bytes", side_effect=AssertionError("whole-file read")):
                result = asyncio.run(
                    process_layer_partitioned_geoparquet(
                        file_path=source,
                        format_type="geojson",
                        layer_name=None,
                        layer_filename="source",
                        dest_folder="dataset/file/v1.0.0/",
                        dest_storage=_StorageAdapter(storage),
                        work_dir=Path(tmpdir) / "work",
                        policy=GeoParquetWritePolicy(),
                    )
                )

            self.assertNotIn("error", result)
            self.assertEqual(len(result["layout"]["outputs"][0]["sha256"]), 64)

    def test_preflight_histograms_spill_many_distinct_bins_to_disk(self):
        store = _PreflightHistogramStore()
        try:
            for batch_start in range(0, 1_000, 100):
                store.add_many(
                    (
                        "candidate",
                        "STATEFP",
                        -1,
                        str(value),
                        10,
                        1,
                    )
                    for value in range(batch_start, batch_start + 100)
                )

            self.assertEqual(store.cardinality("candidate", "STATEFP"), 1_000)
            self.assertEqual(store.max_bytes("candidate", "STATEFP"), 10)
            self.assertFalse(
                any(isinstance(value, (dict, set)) for value in vars(store).values())
            )
            self.assertTrue(store.database_path.exists())
        finally:
            database_path = store.database_path
            store.close()

        self.assertFalse(database_path.exists())

    def test_preflight_s2_rollup_preserves_prefixed_bins_and_aggregates_counts(self):
        import s2sphere

        fine_cell = s2sphere.CellId.from_lat_lng(
            s2sphere.LatLng.from_degrees(38.9, -77.0)
        ).parent(16)
        parent_cell = fine_cell.parent(2).id()
        store = _PreflightHistogramStore()
        try:
            store.add_many(
                [
                    ("semantic_s2", "", 16, f'["v"]|{fine_cell.id()}', 40, 2),
                    ("semantic_s2", "", 16, f'["v"]|{fine_cell.next().id()}', 60, 3),
                ]
            )
            store.roll_up_s2("semantic_s2", "", (2, 16))
            self.assertEqual(store.max_bytes("semantic_s2", "", 2), 100)
            row = store._require_connection().execute(
                "SELECT bin, row_count FROM histogram WHERE kind = 'semantic_s2' AND level = 2"
            ).fetchone()
            self.assertEqual(row, (f'["v"]|{parent_cell}', 5))
        finally:
            store.close()

    def test_write_shapefile_zip_skips_plain_dataframe(self):
        result = write_shapefile_zip(
            pd.DataFrame({"name": ["A"]}),
            Path("unused"),
            "source",
            ShapefileZipPolicy(),
        )

        self.assertFalse(result.created)
        self.assertEqual(result.reason, "non_spatial_source")

    def test_tippecanoe_command_uses_scratch_temp_directory_and_parallel_reads(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pmtiles_path = Path(tmpdir) / "pmtiles" / "layer.pmtiles"
            fgb_path = Path(tmpdir) / "pmtiles" / "chunk-0.fgb"

            cmd = _build_tippecanoe_cmd(pmtiles_path, "layer", [fgb_path])

            self.assertIn("--read-parallel", cmd)
            self.assertIn(f"--temporary-directory={pmtiles_path.parent}", cmd)

    def test_pmtiles_creation_streams_tippecanoe_logs_without_capture_output(self):
        class FakeStorage:
            async def upload_file(self, local_path, remote_path):
                return remote_path

        with tempfile.TemporaryDirectory() as tmpdir:
            pmtiles_path = Path(tmpdir) / "pmtiles" / "layer.pmtiles"
            fgb_path = Path(tmpdir) / "pmtiles" / "chunk-0.fgb"
            fgb_path.parent.mkdir(parents=True)
            fgb_path.write_bytes(b"fgb")
            pmtiles_path.write_bytes(b"pmtiles")

            proc = Mock()
            proc.wait.return_value = 0
            with patch("dagster_hifld.conversion.subprocess.Popen", return_value=proc) as popen:
                result = asyncio.run(
                    _create_and_upload_pmtiles(
                        dest_storage=FakeStorage(),
                        fgb_files=[fgb_path],
                        pmtiles_path=pmtiles_path,
                        dest_folder="dataset/file/v1.0.0/",
                        layer_filename="layer",
                    )
                )

            self.assertEqual(result, "dataset/file/v1.0.0/pmtiles/layer.pmtiles")
            _cmd, kwargs = popen.call_args
            self.assertNotIn("capture_output", kwargs)
            self.assertEqual(kwargs["stdout"], None)
            self.assertEqual(kwargs["stderr"], None)
            self.assertEqual(kwargs["cwd"], str(pmtiles_path.parent))
            self.assertEqual(kwargs["env"]["TMPDIR"], str(pmtiles_path.parent))


if __name__ == "__main__":
    unittest.main()
