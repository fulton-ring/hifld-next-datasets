"""Tiny complete source capture, immutable cache, catalog, and drift fixtures."""
import unittest

from test_bts_intermodal_family import BtsIntermodalLayerTests


class BtsRailTofcCofcTests(BtsIntermodalLayerTests):
    LAYER = "rail_tofc_cofc"


if __name__ == "__main__":
    unittest.main()
