"""Focused tests for the USGS GNIS Historical Hydrographic Points draft pipeline."""
import unittest

from dagster_hifld.sources import gnis_historical_hydrographic_points as src

import gnis_historical_points_support as support
from gnis_historical_points_support import GnisLayerTestSpec

SPEC = GnisLayerTestSpec(
    source=src,
    featureclasses=("Lake", "Reservoir", "Spring", "Stream"),
    dataset_slug="historical-hydrographic-points-1",
    layer_id=13,
    layer_name="Historical Hydrographic Points",
)


class GnisHistoricalHydrographicPointsTests(support.GnisHistoricalPointsLayerTests):
    spec = SPEC


if __name__ == "__main__":
    unittest.main()
