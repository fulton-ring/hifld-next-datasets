import asyncio
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import zipfile

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from dagster_hifld.conversion import (
    GeoParquetWritePolicy,
    ShapefileZipPolicy,
    _StorageAdapter,
    _build_tippecanoe_cmd,
    _create_and_upload_pmtiles,
    _detect_format_from_path,
    _discover_staged_formats,
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
            self.assertIn(
                "dataset/file/v1.0.0/geoparquet/statefp=06/part-000.parquet",
                result["geoparquet_paths"],
            )
            self.assertIn(
                "dataset/file/v1.0.0/geoparquet/statefp=12/part-000.parquet",
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
            self.assertIn(
                "dataset/file/v1.0.0/geoparquet/huc2=01/huc4=0101/huc6=010100/part-000.parquet",
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
            self.assertIn(
                "dataset/file/v1.0.0/geoparquet/state_fips=29/part-000.parquet",
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
            self.assertEqual(
                result["geoparquet_paths"],
                ["dataset/file/v1.0.0/geoparquet/statefp=06/part-000.parquet"],
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
                return None

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
