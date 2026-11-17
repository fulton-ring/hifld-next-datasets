"""FEMA National Flood Hazard Layer (NFHL) MapServer family.

``layers.LAYERS`` declares every layer the family builds; ``capture`` pages a
layer by OBJECTID windows between producer witnesses; ``convert`` streams
validated pages into GeoParquet; ``candidate`` stages the unapproved,
content-versioned candidate through the shared candidate contract.
"""
from dagster_hifld.sources.nfhl.candidate import capture_nfhl_layer
from dagster_hifld.sources.nfhl.capture import CaptureSettings
from dagster_hifld.sources.nfhl.layers import LAYERS, LAYERS_BY_SLUG, NfhlLayer

__all__ = ["LAYERS", "LAYERS_BY_SLUG", "CaptureSettings", "NfhlLayer", "capture_nfhl_layer"]
