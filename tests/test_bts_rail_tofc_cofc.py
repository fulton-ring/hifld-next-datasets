"""Tiny complete source capture, immutable cache, catalog, and drift fixtures."""
import unittest

import bts_intermodal_family_support as family_support


class BtsRailTofcCofcTests(family_support.BtsIntermodalLayerTests):
    LAYER = "rail_tofc_cofc"


if __name__ == "__main__":
    unittest.main()
