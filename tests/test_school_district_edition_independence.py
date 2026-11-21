"""No school-district module may depend on a pinned TIGER/Line edition.

The three school adapters must resolve the newest published national school
geodatabase at capture time, and neither the shared config nor any adapter may
carry an edition literal (a TGRGDBxx directory or a tlgdb_xxxx filename).
"""
from __future__ import annotations

import dataclasses
import re
import unittest
from pathlib import Path

from dagster_hifld.sources import census_tiger_common as shared
from dagster_hifld.sources import school_district_administrative_areas as administrative
from dagster_hifld.sources import secondary_school_districts as secondary
from dagster_hifld.sources import unified_school_districts as unified

EDITION_LITERAL = re.compile(r"TGRGDB\d{2}|tlgdb_\d{4}")
ADAPTERS = (administrative, secondary, unified)


class SchoolDistrictEditionIndependenceTests(unittest.TestCase):
    def test_shared_config_default_is_not_a_pinned_edition(self):
        fields = {f.name: f for f in dataclasses.fields(shared.SchoolDistrictLayerConfig)}
        self.assertEqual(fields["source_url"].default, "")
        self.assertEqual(fields["source_vintage"].default, "")
        self.assertIsNone(EDITION_LITERAL.search(
            Path(shared.__file__).read_text(encoding="utf-8")))
        self.assertFalse(hasattr(shared, "SOURCE_URL"))
        self.assertFalse(hasattr(shared, "SOURCE_HOST"))

    def test_each_school_adapter_resolves_the_edition_at_capture_time(self):
        for module in ADAPTERS:
            with self.subTest(module=module.__name__):
                config = module.CONFIG
                self.assertEqual(config.source_url, "")
                self.assertEqual(config.source_vintage, "")
                self.assertEqual(config.source_host, "www2.census.gov")
                self.assertTrue(config.discover_current)
                self.assertIsNone(config.expected_count)
                low, high = config.feature_count_range
                self.assertTrue(low < high)
                self.assertIsNone(EDITION_LITERAL.search(
                    Path(module.__file__).read_text(encoding="utf-8")), module.__name__)


if __name__ == "__main__":
    unittest.main()
