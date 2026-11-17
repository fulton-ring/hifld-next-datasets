"""EPA Regions Large Scale from EPA_GEO's maintained 2023 regions service; release unapproved.

Large and Small Scale read the same single-resolution layer as EPA Regions, so
their outputs are identical apart from the dataset slug. The retired
EPA_Locations/MapServer layers and the personal-account EPA_Region layer are not read.
"""
from dagster_hifld.sources.epa_regions_2023 import (
    LARGE_SCALE_IDENTITY,
    EpaRegions2023Error,
    acquire,
    inspect_captures,
    decode_page,
    parquet_table,
    stage_epa_regions_2023,
    validate_output,
)
SLUG=LARGE_SCALE_IDENTITY.slug

def stage_epa_large_scale(storage,*,capture_dir=None):
    return stage_epa_regions_2023(storage,capture_dir=capture_dir,dataset=LARGE_SCALE_IDENTITY)
