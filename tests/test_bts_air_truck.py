"""Tiny complete source capture, immutable cache, catalog, and drift fixtures."""
import unittest

from test_bts_intermodal_family import BtsIntermodalLayerTests


class BtsAirTruckTests(BtsIntermodalLayerTests):
    LAYER = "air_truck"


if __name__ == "__main__":
    unittest.main()
