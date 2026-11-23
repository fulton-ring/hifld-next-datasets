"""Focused tests for the USGS GNIS Historical Cultural-Political Points draft pipeline."""
import unittest

from dagster_hifld.sources import gnis_historical_cultural_political_points as src

import gnis_historical_points_support as support
from gnis_historical_points_support import (
    FIXTURE_OBJECTIDS,
    JSONStream,
    GnisLayerTestSpec,
    _fixture_rows as shared_fixture_rows,
    make_responder as shared_make_responder,
)

SPEC = GnisLayerTestSpec(
    source=src,
    featureclasses=("Census", "Crossing", "Military", "Populated Place"),
    dataset_slug="historical-cultural-political-points-1",
    layer_id=12,
    layer_name="Historical Cultural-Political Points",
)


def _fixture_rows():
    return shared_fixture_rows(SPEC.featureclasses)


def make_responder(rows, **kwargs):
    return shared_make_responder(src, rows, **kwargs)


class GnisHistoricalCulturalPoliticalPointsTests(support.GnisHistoricalPointsLayerTests):
    spec = SPEC


if __name__ == "__main__":
    unittest.main()
