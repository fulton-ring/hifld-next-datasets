"""Tiny complete source capture, immutable cache, catalog, and drift fixtures."""
import unittest

from test_bts_intermodal_family import BtsIntermodalLayerTests


class BtsMarineRoroTests(BtsIntermodalLayerTests):
    LAYER = "marine_roro"


if __name__ == "__main__":
    unittest.main()
