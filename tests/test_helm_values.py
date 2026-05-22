import unittest
from pathlib import Path

import yaml


class HelmValuesTests(unittest.TestCase):
    def setUp(self):
        values_path = Path(__file__).resolve().parents[1] / "helm" / "values.yaml"
        self.values = yaml.safe_load(values_path.read_text(encoding="utf-8"))

    def test_launched_runs_do_not_inherit_user_code_server_resources(self):
        deployment = self.values["dagster-user-deployments"]["deployments"][0]

        self.assertFalse(deployment["includeConfigInLaunchedRuns"]["enabled"])
        self.assertEqual(
            deployment["annotations"]["cluster-autoscaler.kubernetes.io/safe-to-evict"],
            "false",
        )
        self.assertEqual(
            deployment["deploymentStrategy"],
            {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1},
            },
        )
        self.assertEqual(deployment["resources"]["requests"]["memory"], "1Gi")
        self.assertEqual(deployment["resources"]["requests"]["ephemeral-storage"], "10Gi")
        self.assertEqual(deployment["resources"]["limits"]["ephemeral-storage"], "10Gi")

    def test_run_launcher_resources_are_lightweight_orchestrators(self):
        launcher = self.values["runLauncher"]["config"]["k8sRunLauncher"]
        resources = launcher["resources"]

        self.assertEqual(resources["requests"]["cpu"], "500m")
        self.assertEqual(resources["requests"]["memory"], "2Gi")
        self.assertEqual(resources["requests"]["ephemeral-storage"], "10Gi")
        self.assertEqual(resources["limits"]["cpu"], "2")
        self.assertEqual(resources["limits"]["memory"], "4Gi")
        self.assertEqual(resources["limits"]["ephemeral-storage"], "10Gi")

    def test_run_launcher_does_not_mount_heavy_scratch_volume(self):
        launcher = self.values["runLauncher"]["config"]["k8sRunLauncher"]
        k8s_config = launcher["runK8sConfig"]

        self.assertNotIn("containerConfig", k8s_config)
        self.assertNotIn("podSpecConfig", k8s_config)
        self.assertEqual(k8s_config["jobSpecConfig"]["ttlSecondsAfterFinished"], 120)
        self.assertEqual(
            k8s_config["podTemplateSpecMetadata"]["annotations"][
                "cluster-autoscaler.kubernetes.io/safe-to-evict"
            ],
            "false",
        )

    def test_run_queue_limits_large_publish_concurrency(self):
        coordinator = self.values["dagsterDaemon"]["runCoordinator"]["config"]["queuedRunCoordinator"]

        self.assertEqual(coordinator["maxConcurrentRuns"], 8)

    def test_daemon_waits_for_user_code_service_before_starting(self):
        daemon = self.values["dagsterDaemon"]
        init_containers = daemon["extraPrependedInitContainers"]

        wait_container = next(
            container
            for container in init_containers
            if container["name"] == "wait-for-hifld-user-code"
        )
        self.assertEqual(wait_container["image"], "busybox:1.28")
        self.assertIn("nc -z hifld 4000", wait_container["command"][-1])


if __name__ == "__main__":
    unittest.main()
