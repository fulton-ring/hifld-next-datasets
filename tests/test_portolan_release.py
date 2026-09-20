import json
import unittest

from dagster_hifld.portolan.release import ReleasePointer


class ReleasePointerTests(unittest.TestCase):
    def test_round_trips_a_generation_scoped_catalog_commit(self):
        pointer = ReleasePointer(
            generation="1f5bd380-a67f-48a8-b5e2-c80aa85f0e63",
            catalog_key="releases/1f5bd380-a67f-48a8-b5e2-c80aa85f0e63/_catalog/catalog.sqlite",
            root_key="releases/1f5bd380-a67f-48a8-b5e2-c80aa85f0e63/catalog.json",
            sha256="a" * 64,
            size_bytes=123,
            published_at="2026-09-19T20:00:00Z",
        )

        result = ReleasePointer.parse(pointer.to_bytes())

        self.assertEqual(result, pointer)
        self.assertEqual(json.loads(pointer.to_bytes())["protocol_version"], 1)

    def test_rejects_a_catalog_key_outside_its_release(self):
        payload = {
            "protocol_version": 1,
            "generation": "1f5bd380-a67f-48a8-b5e2-c80aa85f0e63",
            "catalog_key": "_catalog/catalog.sqlite",
            "root_key": "releases/1f5bd380-a67f-48a8-b5e2-c80aa85f0e63/catalog.json",
            "sha256": "a" * 64,
            "size_bytes": 123,
            "published_at": "2026-09-19T20:00:00Z",
        }

        with self.assertRaisesRegex(ValueError, "release"):
            ReleasePointer.parse(json.dumps(payload).encode())


if __name__ == "__main__":
    unittest.main()
