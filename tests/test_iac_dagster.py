import unittest
from pathlib import Path
import re


class DagsterIaCTests(unittest.TestCase):
    def test_hifld_user_code_has_pod_disruption_budget(self):
        dagster_tf = (
            Path(__file__).resolve().parents[2]
            / "hifld-next-iac"
            / "environments"
            / "prod"
            / "dagster.tf"
        ).read_text(encoding="utf-8")

        self.assertIn('resource "kubernetes_pod_disruption_budget_v1" "dagster_hifld_user_code"', dagster_tf)
        self.assertIn('max_unavailable = "0"', dagster_tf)
        self.assertRegex(dagster_tf, re.compile(r'deployment\s+=\s+"hifld"'))
        self.assertRegex(dagster_tf, re.compile(r'component\s+=\s+"user-deployments"'))


if __name__ == "__main__":
    unittest.main()
