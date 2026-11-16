"""Historical EIA captures must not run through maintained-refresh entrypoints."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from dagster import DagsterInstance, materialize

from dagster_hifld.assets.ornl_power_plants import eia_power_plants_historical
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import (
    eia_biodiesel_plants,
    eia_ethanol_plants,
    eia_natural_gas_pipelines,
    hydrocarbon_gas_liquid_pipelines,
    natural_gas_trading_hubs,
    ornl_power_plants,
)


CAPTURES = (
    eia_biodiesel_plants.capture_eia_biodiesel_plants,
    eia_ethanol_plants.capture_eia_ethanol_plants,
    eia_natural_gas_pipelines.capture_eia_natural_gas_pipelines,
    hydrocarbon_gas_liquid_pipelines.capture_hgl_pipelines,
    natural_gas_trading_hubs.capture_natural_gas_trading_hubs,
    ornl_power_plants.capture_power_plants,
)


class HistoricalRefreshGuardTests(unittest.TestCase):
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
                                capture(storage, version="v20261116", capture_directory=replay)
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
