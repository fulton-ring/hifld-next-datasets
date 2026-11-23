"""Tiny complete source capture, immutable cache, catalog, and drift fixtures."""
import unittest

import bts_intermodal_family_support as family_support


class BtsAirTruckTests(family_support.BtsIntermodalLayerTests):
    LAYER = "air_truck"


if __name__ == "__main__":
    unittest.main()
