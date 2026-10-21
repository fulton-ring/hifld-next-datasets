"""Explicit ingest asset registry.

Keep source-specific ingest assets explicit. Downstream catalog/publish/check
assets are derived automatically from this registry.
"""

from dagster_hifld.assets.usgs import usgs_aquifers
from dagster_hifld.assets.noaa_current_dart import noaa_current_dart
from dagster_hifld.assets.crushed_stone import usgs_crushed_stone_preservation
from dagster_hifld.assets.noaa import (
    noaa_12nm_territorial_sea,
    noaa_200nm_eez_and_maritime_boundaries,
)
from dagster_hifld.assets.noaa_contiguous import noaa_24nm_contiguous_zone
from dagster_hifld.assets.census_bureau import census_bureau_aiannh_areas
from dagster_hifld.assets.census_primary_roads import census_bureau_primary_roads
from dagster_hifld.assets.census_block_groups import census_bureau_block_groups
from dagster_hifld.assets.census_tracts import census_bureau_tracts
from dagster_hifld.assets.census_blocks_2020 import census_blocks_2020_ingest_assets
from dagster_hifld.assets.red_cross import red_cross_american_red_cross_divisions
from dagster_hifld.assets.arc_regions import arc_regions
from dagster_hifld.assets.red_cross_headquarters import red_cross_headquarters
from dagster_hifld.assets.faa import faa_airspace_boundaries, faa_aviation_facilities
from dagster_hifld.assets.bts import bts_amtrak_stations
from dagster_hifld.assets.fuel_stations import bts_alternative_fueling_stations
from dagster_hifld.assets.dhs_hifld import dhs_hifld_above_ground_lng_storage_facilities
from dagster_hifld.assets.usace import usace_administrative_areas_usace_ienc
from dagster_hifld.assets.usace import usace_navigation_marks_areas_usace_ienc
from dagster_hifld.assets.bsee_ocs_platforms import bsee_ocs_platforms
from dagster_hifld.assets.proposed_critical_habitat import proposed_critical_habitat_features
from dagster_hifld.assets.usace_berths_areas import usace_berths_areas_usace_ienc
from dagster_hifld.assets.quaternary_fault_lines import quaternary_fault_lines_usgs
from dagster_hifld.assets.usace_pylon_areas import usace_pylon_areas_usace_ienc
from dagster_hifld.assets.us_county_boundaries import us_county_boundaries_census
from dagster_hifld.assets.usace_pylon_locations import usace_pylon_locations_usace_ienc
from dagster_hifld.assets.usace_recommended_tracks import usace_recommended_tracks_ienc
from dagster_hifld.assets.usace_overhead_pipeline_lines import usace_overhead_pipeline_lines_usace_ienc
from dagster_hifld.assets.usace_overhead_cables import usace_overhead_cables_usace_ienc
from dagster_hifld.assets.usace_dam_lines import usace_dam_lines_usace_ienc
from dagster_hifld.assets.usace_obstruction_lines import usace_obstruction_lines_usace_ienc
from dagster_hifld.assets.secondary_school_districts import census_secondary_school_districts
from dagster_hifld.assets.principal_ports import principal_ports_usace
from dagster_hifld.assets.unified_school_districts import census_unified_school_districts
from dagster_hifld.assets.usace_pontoon_areas import usace_pontoon_areas_usace_ienc
from dagster_hifld.assets.puerto_rico_subbarrios import puerto_rico_subbarrios_census
from dagster_hifld.assets.usace_caution_areas import usace_caution_areas_usace_ienc
from dagster_hifld.assets.nces_public_schools import nces_public_schools
from dagster_hifld.assets.usace_pile_locations import usace_pile_locations_usace_ienc
from dagster_hifld.assets.usace_pipeline_areas import usace_pipeline_areas_usace_ienc
from dagster_hifld.assets.usace_data_quality_areas import usace_data_quality_areas_usace_ienc
from dagster_hifld.assets.plate_boundaries import plate_boundaries_usgs
from dagster_hifld.assets.ornl_power_plants import eia_power_plants_historical
from dagster_hifld.assets.nces_private_schools import nces_private_schools_2023_24
from dagster_hifld.assets.gcca_warehouse_directory import gcca_warehouses_current
from dagster_hifld.assets.coastlines_usace_ienc import coastlines_usace_ienc
from dagster_hifld.assets.usace_airport_area import usace_airport_area_ienc
from dagster_hifld.assets.campus_preservation import ornl_campus_historical_preservation

from dagster_hifld.assets.ibc import ibc_canada_us_border
from dagster_hifld.assets.fws import fws_cbrs_buffer_zone
from dagster_hifld.assets.fws_map_panels import fws_cbrs_map_panels
from dagster_hifld.assets.fws_prohibitions import fws_cbrs_prohibitions
from dagster_hifld.assets.fws_units import fws_cbrs_units
from dagster_hifld.assets.census_designated_places import census_designated_places
from dagster_hifld.assets.state_capitols import state_capitols_usgs
from dagster_hifld.assets.colleges_universities import nces_colleges_universities
from dagster_hifld.assets.combined_statistical_areas import census_combined_statistical_areas
from dagster_hifld.assets.census_consolidated_cities import census_consolidated_cities
from dagster_hifld.assets.usace_conveyor_areas import usace_conveyor_areas_usace_ienc
from dagster_hifld.assets.construction_minerals import construction_minerals_preservation_candidate
from dagster_hifld.assets.census_county_subdivisions import census_county_subdivisions
from dagster_hifld.assets.usace_crane_locations import usace_crane_locations_ienc
from dagster_hifld.assets.courthouses_usgs import courthouses_usgs
from dagster_hifld.assets.usace_data_coverage_areas import usace_data_coverage_areas_usace_ienc

hifld_ingest_assets = [
    noaa_current_dart,
    usgs_crushed_stone_preservation,
    nces_colleges_universities,
    usgs_aquifers,
    noaa_12nm_territorial_sea,
    census_bureau_aiannh_areas,
    census_bureau_primary_roads,
    census_bureau_block_groups,
    census_bureau_tracts,
    usace_pontoon_areas_usace_ienc,
    *census_blocks_2020_ingest_assets,
    noaa_200nm_eez_and_maritime_boundaries,
    noaa_24nm_contiguous_zone,
    red_cross_american_red_cross_divisions,
    arc_regions,
    red_cross_headquarters,
    faa_airspace_boundaries,
    faa_aviation_facilities,
    bts_amtrak_stations,
    bts_alternative_fueling_stations,
    dhs_hifld_above_ground_lng_storage_facilities,
    usace_administrative_areas_usace_ienc,
    bsee_ocs_platforms,
    proposed_critical_habitat_features,
    usace_navigation_marks_areas_usace_ienc,
    usace_berths_areas_usace_ienc,
    quaternary_fault_lines_usgs,
    usace_pylon_areas_usace_ienc,
    us_county_boundaries_census,
    usace_pylon_locations_usace_ienc,
    usace_recommended_tracks_ienc,
    usace_overhead_pipeline_lines_usace_ienc,
    usace_overhead_cables_usace_ienc,
    usace_dam_lines_usace_ienc,
    usace_obstruction_lines_usace_ienc,
    census_secondary_school_districts,
    principal_ports_usace,
    census_unified_school_districts,
    puerto_rico_subbarrios_census,
    usace_caution_areas_usace_ienc,
    usace_pile_locations_usace_ienc,
    usace_pipeline_areas_usace_ienc,
    usace_data_quality_areas_usace_ienc,
    plate_boundaries_usgs,
    eia_power_plants_historical,
    nces_private_schools_2023_24,
    nces_public_schools,
    gcca_warehouses_current,
    coastlines_usace_ienc,
    usace_airport_area_ienc,
    ibc_canada_us_border,
    fws_cbrs_buffer_zone,
    fws_cbrs_map_panels,
    fws_cbrs_prohibitions,
    fws_cbrs_units,
    census_designated_places,
    state_capitols_usgs,
    ornl_campus_historical_preservation,
    census_combined_statistical_areas,
    census_consolidated_cities,
    usace_conveyor_areas_usace_ienc,
    construction_minerals_preservation_candidate,
    census_county_subdivisions,
    usace_crane_locations_ienc,
    courthouses_usgs,
    usace_data_coverage_areas_usace_ienc,
]
