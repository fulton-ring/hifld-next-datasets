"""Historical and excluded-mirror acquisition must stop before side effects."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from dagster import DagsterInstance, materialize

from dagster_hifld.assets.ornl_power_plants import eia_power_plants_historical
from dagster_hifld.assets import dhs_hifld
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import (
    eia_biodiesel_plants,
    bts_ipcd,
    eia_ethanol_plants,
    eia_natural_gas_pipelines,
    hydrocarbon_gas_liquid_pipelines,
    natural_gas_trading_hubs,
    ornl_power_plants,
    ferrous_metal_processing_plants,
)


CAPTURES = (
    eia_biodiesel_plants.capture_eia_biodiesel_plants,
    eia_ethanol_plants.capture_eia_ethanol_plants,
    eia_natural_gas_pipelines.capture_eia_natural_gas_pipelines,
    hydrocarbon_gas_liquid_pipelines.capture_hgl_pipelines,
    natural_gas_trading_hubs.capture_natural_gas_trading_hubs,
    ornl_power_plants.capture_power_plants,
    ferrous_metal_processing_plants.capture_ferrous_metal_processing_plants,
    bts_ipcd.capture_bts_ipcd,
)


class HistoricalRefreshGuardTests(unittest.TestCase):
    def test_dormant_pasda_asset_refuses_before_context_download_or_storage(self):
        compute = dhs_hifld.dhs_hifld_above_ground_lng_storage_facilities.op.compute_fn.decorated_fn
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory)/'existing-release.parquet'
            marker.write_bytes(b'existing immutable release')
            storage = StagingStorageResource(local_dir=directory)
            with patch.object(dhs_hifld, 'build_version_id', side_effect=AssertionError('context accessed'), create=True) as version, patch.object(dhs_hifld, 'download_convert_and_stage', side_effect=AssertionError('download attempted'), create=True) as download, patch('httpx.Client') as client:
                with self.assertRaisesRegex(ValueError, 'source_policy_excluded_mirror'):
                    compute(object(), storage)
                version.assert_not_called()
                download.assert_not_called()
                client.assert_not_called()
            self.assertEqual(list(Path(directory).iterdir()), [marker])
            self.assertEqual(marker.read_bytes(), b'existing immutable release')

    def test_bts_explicit_retained_replay_refuses_before_loading_capture(self):
        with patch.object(bts_ipcd, '_load_retained_capture', side_effect=AssertionError('replay accessed')) as replay, patch('httpx.Client') as client:
            with self.assertRaisesRegex(ValueError, 'historical_source_not_maintained'):
                bts_ipcd.capture_bts_ipcd(object(), version='v20261116', capture_directory='missing-retained-capture', reuse_retained_capture=True)
            replay.assert_not_called()
            client.assert_not_called()

    def test_ferrous_sealed_replay_refuses_before_loading_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            retained = root/'capture'
            retained.mkdir()
            (retained/'capture_manifest.json').write_text('{"complete": true}')
            storage = StagingStorageResource(local_dir=str(root/'untouched-stage'))
            with patch.object(ferrous_metal_processing_plants, '_read_replay', side_effect=AssertionError('replay accessed')) as replay, patch('httpx.Client') as client:
                with self.assertRaisesRegex(ValueError, 'historical_source_not_maintained'):
                    ferrous_metal_processing_plants.capture_ferrous_metal_processing_plants(storage, version='v20261116', capture_directory=str(retained))
                replay.assert_not_called()
                client.assert_not_called()
            self.assertFalse((root/'untouched-stage').exists())

    def test_historical_entrypoints_refuse_before_resource_access(self):
        for capture in CAPTURES:
            with self.subTest(capture=capture.__name__):
                with self.assertRaisesRegex(ValueError, "historical_source_not_maintained"):
                    capture(object(), version="v20261116")

    def test_live_and_replay_requests_leave_existing_storage_untouched(self):
        for capture in CAPTURES:
            for replay in (None, "missing-retained-capture"):
                with self.subTest(capture=capture.__name__, replay=replay):
                    with tempfile.TemporaryDirectory() as directory:
                        root = Path(directory)
                        marker = root / "existing-release.parquet"
                        marker.write_bytes(b"existing immutable release")
                        storage = StagingStorageResource(local_dir=directory)
                        with patch("httpx.Client") as client:
                            with self.assertRaisesRegex(ValueError, "historical_source_not_maintained"):
                                capture(storage, version="v20261116", capture_directory=str(root / replay) if replay else None)
                            client.assert_not_called()
                        self.assertEqual(list(root.iterdir()), [marker])
                        self.assertEqual(marker.read_bytes(), b"existing immutable release")

    def test_registered_ingest_cannot_stage_or_register_a_historical_refresh(self):
        with tempfile.TemporaryDirectory() as directory, DagsterInstance.ephemeral() as instance:
            with patch("httpx.Client") as client:
                result = materialize(
                    [eia_power_plants_historical],
                    resources={"staging_storage": StagingStorageResource(local_dir=directory)},
                    instance=instance,
                    raise_on_error=False,
                )
                client.assert_not_called()
            self.assertFalse(result.success)
            errors = result.get_step_failure_events()
            self.assertEqual(len(errors), 1)
            self.assertIn("historical_source_not_maintained", errors[0].event_specific_data.error.to_string())
            self.assertEqual(instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME), [])
            self.assertEqual(list(Path(directory).iterdir()), [])
