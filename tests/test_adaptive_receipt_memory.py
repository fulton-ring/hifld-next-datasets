"""Receipt iteration must not retain a population of capture bodies."""
import importlib
import unittest
import weakref
from collections.abc import Mapping


class TrackedBody:
    pass


class FreshBodyMapping(Mapping):
    def __init__(self):
        self.refs = []
        self.peak_live_bodies = 0

    def __iter__(self):
        return iter(f"page_{i}" for i in range(8))

    def __len__(self):
        return 8

    def __getitem__(self, name):
        body = TrackedBody()
        self.refs.append(weakref.ref(body))
        self.peak_live_bodies = max(self.peak_live_bodies,
                                    sum(ref() is not None for ref in self.refs))
        return body, {"bytes": 1, "wire_bytes": 1}


class ReceiptIterationMemoryTests(unittest.TestCase):
    def test_each_offset_adapter_streams_receipts_without_collecting_bodies(self):
        for name in ("narn_rail_lines", "narn_rail_nodes", "railroad_bridges",
                     "runways", "gnis_crossings", "gnis_streams_mouth"):
            with self.subTest(adapter=name):
                source = importlib.import_module("dagster_hifld.sources." + name)
                captures = FreshBodyMapping()
                receipts = list(source._page_receipts(captures))
                self.assertEqual(len(receipts), 8)
                self.assertLessEqual(captures.peak_live_bodies, 2)


if __name__ == "__main__":
    unittest.main()
