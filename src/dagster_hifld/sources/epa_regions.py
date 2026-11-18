"""EPA Regions from EPA_GEO's maintained 2023 regions service; release unapproved.

Large and Small Scale read the same single-resolution layer as EPA Regions, so
their outputs are identical apart from the dataset slug. The retired
EPA_Locations/MapServer layers and the personal-account EPA_Region layer are not read.
"""
from dagster_hifld.sources.epa_regions_common import (
    REGIONS_IDENTITY,
    EpaRegions2023Error,
    acquire,
    inspect_captures,
    decode_page,
    parquet_table,
    stage_epa_regions_common,
    validate_output,
)
SLUG=REGIONS_IDENTITY.slug

def stage_epa_regions(storage,*,capture_dir=None):
    return stage_epa_regions_common(storage,capture_dir=capture_dir,dataset=REGIONS_IDENTITY)
