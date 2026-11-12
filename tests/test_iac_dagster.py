import os
import unittest
from pathlib import Path
import re


IAC_ROOT = Path(os.environ.get(
    "HIFLD_IAC_CHECKOUT", str(Path(__file__).resolve().parents[2] / "hifld-next-iac")
))
DAGSTER_TF = IAC_ROOT / "environments/prod/dagster.tf"


@unittest.skipUnless(
    os.environ.get("HIFLD_IAC_CHECKOUT") or DAGSTER_TF.is_file(),
    "External IaC integration: set HIFLD_IAC_CHECKOUT to the trusted IaC checkout",
)
class DagsterIaCTests(unittest.TestCase):
    def test_hifld_user_code_has_pod_disruption_budget(self):
        dagster_tf = DAGSTER_TF.read_text(encoding="utf-8")

        self.assertIn('resource "kubernetes_pod_disruption_budget_v1" "dagster_hifld_user_code"', dagster_tf)
        self.assertIn('max_unavailable = "0"', dagster_tf)
        self.assertRegex(dagster_tf, re.compile(r'deployment\s+=\s+"hifld"'))
        self.assertRegex(dagster_tf, re.compile(r'component\s+=\s+"user-deployments"'))


if __name__ == "__main__":
    unittest.main()
