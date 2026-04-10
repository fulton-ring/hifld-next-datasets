"""Explicit ingest asset registry.

Keep source-specific ingest assets explicit. Downstream catalog/publish/check
assets are derived automatically from this registry.
"""

from dagster_hifld.assets.noaa import (
    noaa_12nm_territorial_sea,
    noaa_200nm_eez_and_maritime_boundaries,
    noaa_24nm_contiguous_zone,
)
from dagster_hifld.assets.census_bureau import census_bureau_aiannh_areas
from dagster_hifld.assets.census_blocks_2020 import census_blocks_2020_ingest_assets
from dagster_hifld.assets.red_cross import red_cross_american_red_cross_divisions
from dagster_hifld.assets.faa import faa_airspace_boundaries, faa_aviation_facilities
from dagster_hifld.assets.bts import bts_amtrak_stations
from dagster_hifld.assets.dhs_hifld import dhs_hifld_above_ground_lng_storage_facilities
from dagster_hifld.assets.usace import usace_administrative_areas_usace_ienc

hifld_ingest_assets = [
    noaa_12nm_territorial_sea,
    census_bureau_aiannh_areas,
    *census_blocks_2020_ingest_assets,
    noaa_200nm_eez_and_maritime_boundaries,
    noaa_24nm_contiguous_zone,
    red_cross_american_red_cross_divisions,
    faa_airspace_boundaries,
    faa_aviation_facilities,
    bts_amtrak_stations,
    dhs_hifld_above_ground_lng_storage_facilities,
    usace_administrative_areas_usace_ienc,
]
