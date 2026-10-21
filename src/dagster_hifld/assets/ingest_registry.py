"""Explicit ingest asset registry.

Keep source-specific ingest assets explicit. Downstream catalog/publish/check
assets are derived automatically from this registry.
"""

from dagster_hifld.assets.noaa import (
    noaa_12nm_territorial_sea,
    noaa_200nm_eez_and_maritime_boundaries,
)
from dagster_hifld.assets.noaa_contiguous import noaa_24nm_contiguous_zone
from dagster_hifld.assets.census_bureau import census_bureau_aiannh_areas
from dagster_hifld.assets.census_primary_roads import census_bureau_primary_roads
from dagster_hifld.assets.census_blocks_2020 import census_blocks_2020_ingest_assets
from dagster_hifld.assets.red_cross import red_cross_american_red_cross_divisions
from dagster_hifld.assets.faa import faa_airspace_boundaries, faa_aviation_facilities
from dagster_hifld.assets.bts import bts_amtrak_stations
from dagster_hifld.assets.fuel_stations import bts_alternative_fueling_stations
from dagster_hifld.assets.dhs_hifld import dhs_hifld_above_ground_lng_storage_facilities
from dagster_hifld.assets.usace import usace_administrative_areas_usace_ienc
from dagster_hifld.assets.usace_berths_areas import usace_berths_areas_usace_ienc
from dagster_hifld.assets.quaternary_fault_lines import quaternary_fault_lines_usgs
from dagster_hifld.assets.usace_pylon_areas import usace_pylon_areas_usace_ienc
from dagster_hifld.assets.usace_pylon_locations import usace_pylon_locations_usace_ienc
from dagster_hifld.assets.usace_recommended_tracks import usace_recommended_tracks_ienc
from dagster_hifld.assets.usace_overhead_pipeline_lines import usace_overhead_pipeline_lines_usace_ienc
from dagster_hifld.assets.usace_obstruction_lines import usace_obstruction_lines_usace_ienc
from dagster_hifld.assets.secondary_school_districts import census_secondary_school_districts
from dagster_hifld.assets.principal_ports import principal_ports_usace
from dagster_hifld.assets.unified_school_districts import census_unified_school_districts
from dagster_hifld.assets.puerto_rico_subbarrios import puerto_rico_subbarrios_census
from dagster_hifld.assets.usace_caution_areas import usace_caution_areas_usace_ienc

hifld_ingest_assets = [
    noaa_12nm_territorial_sea,
    census_bureau_aiannh_areas,
    census_bureau_primary_roads,
    *census_blocks_2020_ingest_assets,
    noaa_200nm_eez_and_maritime_boundaries,
    noaa_24nm_contiguous_zone,
    red_cross_american_red_cross_divisions,
    faa_airspace_boundaries,
    faa_aviation_facilities,
    bts_amtrak_stations,
    bts_alternative_fueling_stations,
    dhs_hifld_above_ground_lng_storage_facilities,
    usace_administrative_areas_usace_ienc,
    usace_berths_areas_usace_ienc,
    quaternary_fault_lines_usgs,
    usace_pylon_areas_usace_ienc,
    usace_pylon_locations_usace_ienc,
    usace_recommended_tracks_ienc,
    usace_overhead_pipeline_lines_usace_ienc,
    usace_obstruction_lines_usace_ienc,
    census_secondary_school_districts,
    principal_ports_usace,
    census_unified_school_districts,
    puerto_rico_subbarrios_census,
    usace_caution_areas_usace_ienc,
]
