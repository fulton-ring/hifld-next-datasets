"""Tiny complete source capture, immutable cache, catalog, and drift fixtures."""
import unittest

import bts_intermodal_family_support as family_support


class BtsMarineRoroTests(family_support.BtsIntermodalLayerTests):
    LAYER = "marine_roro"


if __name__ == "__main__":
    unittest.main()
