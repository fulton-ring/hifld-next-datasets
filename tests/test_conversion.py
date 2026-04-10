import unittest
from unittest.mock import Mock, patch

from dagster_hifld.conversion import _to_wgs84, process_staged_dataset_version


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


if __name__ == "__main__":
    unittest.main()
