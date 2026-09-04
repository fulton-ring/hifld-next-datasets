import json
import unittest
from pathlib import Path


class LifecyclePolicyTests(unittest.TestCase):
    def test_staging_policy_only_expires_temporary_objects_after_seven_days(self):
        policy = self._load("staging.json")

        self.assertEqual(
            policy,
            {
                "rule": [
                    {
                        "action": {"type": "Delete"},
                        "condition": {
                            "age": 7,
                            "matchesPrefix": ["_temporary/"],
                        },
                    }
                ]
            },
        )

    def test_production_policy_only_expires_rollback_objects_after_seven_days(self):
        policy = self._load("production.json")

        self.assertEqual(
            policy,
            {
                "rule": [
                    {
                        "action": {"type": "Delete"},
                        "condition": {
                            "age": 7,
                            "matchesPrefix": ["_rollback/"],
                        },
                    }
                ]
            },
        )

    @staticmethod
    def _load(filename: str) -> dict[str, object]:
        path = Path(__file__).resolve().parents[1] / "ops/gcs-lifecycle" / filename
        return json.loads(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
