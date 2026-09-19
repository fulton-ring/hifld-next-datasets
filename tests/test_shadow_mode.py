import os
import unittest
from unittest.mock import patch

from dagster_hifld import definitions


class ShadowModeTests(unittest.TestCase):
    def test_shadow_step_resources_are_overridden_only_in_manual_shadow_mode(self):
        shadow_env = {
            "HIFLD_SHADOW_MANUAL_ONLY": "1",
            "HIFLD_SHADOW_STEP_CPU_REQUEST": "500m",
            "HIFLD_SHADOW_STEP_MEMORY_REQUEST": "2Gi",
            "HIFLD_SHADOW_STEP_EPHEMERAL_STORAGE_REQUEST": "5Gi",
            "HIFLD_SHADOW_STEP_CPU_LIMIT": "2",
            "HIFLD_SHADOW_STEP_MEMORY_LIMIT": "4Gi",
            "HIFLD_SHADOW_STEP_EPHEMERAL_STORAGE_LIMIT": "10Gi",
            "HIFLD_SHADOW_STEP_SCRATCH_STORAGE": "20Gi",
        }
        with patch.dict(os.environ, shadow_env, clear=True):
            resources = definitions._step_resources()

        self.assertEqual(
            resources,
            {
                "requests": {
                    "cpu": "500m",
                    "memory": "2Gi",
                    "ephemeral-storage": "5Gi",
                },
                "limits": {
                    "cpu": "2",
                    "memory": "4Gi",
                    "ephemeral-storage": "10Gi",
                },
            },
        )

    def test_production_step_resources_ignore_shadow_overrides(self):
        with patch.dict(
            os.environ,
            {
                "HIFLD_SHADOW_MANUAL_ONLY": "0",
                "HIFLD_SHADOW_STEP_CPU_REQUEST": "500m",
            },
            clear=True,
        ):
            self.assertEqual(
                definitions._step_resources()["requests"]["cpu"],
                "4",
            )

    def test_manual_shadow_mode_omits_discovery_sensor(self):
        with patch.dict(os.environ, {"HIFLD_SHADOW_MANUAL_ONLY": "1"}):
            self.assertEqual(definitions._configured_sensors(), [])

    def test_normal_mode_keeps_discovery_sensor(self):
        with patch.dict(os.environ, {"HIFLD_SHADOW_MANUAL_ONLY": "0"}):
            self.assertEqual(
                definitions._configured_sensors(),
                [definitions.version_discovery_sensor],
            )
