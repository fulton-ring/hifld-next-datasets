"""Explicit ingest asset registry.

Keep source-specific ingest assets explicit. Downstream catalog/publish/check
assets are derived automatically from this registry.
"""

from dagster_hifld.assets.smithsonian_holocene import smithsonian_holocene_volcanoes
from dagster_hifld.assets.usgs import usgs_aquifers
from dagster_hifld.assets.ems_stations import usgs_ems_stations
from dagster_hifld.assets.fema_regional_offices import fema_regional_offices
from dagster_hifld.assets.epa_power_generation import epa_frs_eia_860_power_generation_facilities_by_source
from dagster_hifld.assets.epa_icis_wastewater import epa_icis_wastewater_treatment_plants
from dagster_hifld.assets.fuds_irm_property_point import fuds_irm_property_point_ingest
from dagster_hifld.assets.fuds_projects_points import usace_fuds_projects_points
from dagster_hifld.assets.fuds_district_boundaries import usace_fuds_program_district_boundaries
from dagster_hifld.assets.epa_small_scale import epa_small_scale_regions
from dagster_hifld.assets.noaa_current_dart import noaa_current_dart
from dagster_hifld.assets.crushed_stone import usgs_crushed_stone_preservation
from dagster_hifld.assets.noaa import (
    noaa_12nm_territorial_sea,
    noaa_200nm_eez_and_maritime_boundaries,
)
from dagster_hifld.assets.noaa_contiguous import noaa_24nm_contiguous_zone
from dagster_hifld.assets.bts_ferry_routes import bts_ferry_routes
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
from dagster_hifld.assets.bts_rail_tofc_cofc import bts_intermodal_freight_rail_tofc_cofc
from dagster_hifld.assets.bts_marine_roro import bts_intermodal_freight_marine_roll_on_roll_off
from dagster_hifld.assets.national_transit_map_stops import national_transit_map_stops
from dagster_hifld.assets.fuel_stations import bts_alternative_fueling_stations
from dagster_hifld.assets.dhs_hifld import dhs_hifld_above_ground_lng_storage_facilities
from dagster_hifld.assets.usace import usace_administrative_areas_usace_ienc
from dagster_hifld.assets.fdic_locations import fdic_locations_official
from dagster_hifld.assets.fema_regions import fema_regions_official
from dagster_hifld.assets.ferc_regions import ferc_historical_preservation
from dagster_hifld.assets.fuds_division_boundaries import fuds_program_division_boundaries
from dagster_hifld.assets.epa_frs_naics_powerplants import epa_frs_naics_powerplants
from dagster_hifld.assets.epa_regions import epa_regions
from dagster_hifld.assets.usace import usace_navigation_marks_areas_usace_ienc
from dagster_hifld.assets.bsee_ocs_platforms import bsee_ocs_platforms
from dagster_hifld.assets.proposed_critical_habitat import proposed_critical_habitat_features
from dagster_hifld.assets.usace_berths_areas import usace_berths_areas_usace_ienc
from dagster_hifld.assets.usace_dry_docks import usace_dry_docks_usace_ienc
from dagster_hifld.assets.quaternary_fault_lines import quaternary_fault_lines_usgs
from dagster_hifld.assets.usace_pylon_areas import usace_pylon_areas_usace_ienc
from dagster_hifld.assets.us_county_boundaries import us_county_boundaries_census
from dagster_hifld.assets.us_state_boundaries import us_state_boundaries_census
from dagster_hifld.assets.usace_pylon_locations import usace_pylon_locations_usace_ienc
from dagster_hifld.assets.usace_distance_marks import usace_distance_marks_usace_ienc
from dagster_hifld.assets.usace_recommended_tracks import usace_recommended_tracks_ienc
from dagster_hifld.assets.usace_overhead_pipeline_lines import usace_overhead_pipeline_lines_usace_ienc
from dagster_hifld.assets.usace_overhead_cables import usace_overhead_cables_usace_ienc
from dagster_hifld.assets.usace_dam_lines import usace_dam_lines_usace_ienc
from dagster_hifld.assets.usace_obstruction_lines import usace_obstruction_lines_usace_ienc
from dagster_hifld.assets.usace_mooring_facility_lines import usace_mooring_facility_lines_usace_ienc
from dagster_hifld.assets.secondary_school_districts import census_secondary_school_districts
from dagster_hifld.assets.principal_ports import principal_ports_usace
from dagster_hifld.assets.unified_school_districts import census_unified_school_districts
from dagster_hifld.assets.usace_pontoon_areas import usace_pontoon_areas_usace_ienc
from dagster_hifld.assets.usace_hulke_areas import usace_hulke_areas_usace_ienc
from dagster_hifld.assets.puerto_rico_subbarrios import puerto_rico_subbarrios_census
from dagster_hifld.assets.usace_caution_areas import usace_caution_areas_usace_ienc
from dagster_hifld.assets.nces_public_schools import nces_public_schools
from dagster_hifld.assets.usace_pile_locations import usace_pile_locations_usace_ienc
from dagster_hifld.assets.usace_daymark_locations import usace_daymark_locations_usace_ienc
from dagster_hifld.assets.usace_dynamic_buoys import usace_dynamic_buoys_usace_ienc
from dagster_hifld.assets.usace_pipeline_areas import usace_pipeline_areas_usace_ienc
from dagster_hifld.assets.usace_data_quality_areas import usace_data_quality_areas_usace_ienc
from dagster_hifld.assets.usace_dumping_ground_areas import usace_dumping_ground_areas_usace_ienc
from dagster_hifld.assets.plate_boundaries import plate_boundaries_usgs
from dagster_hifld.assets.ornl_power_plants import eia_power_plants_historical
from dagster_hifld.assets.nces_private_schools import nces_private_schools_2023_24
from dagster_hifld.assets.gcca_warehouse_directory import gcca_warehouses_current
from dagster_hifld.assets.coastlines_usace_ienc import coastlines_usace_ienc
from dagster_hifld.assets.locks_usace_ienc import locks_usace_ienc
from dagster_hifld.assets.lock_basins_usace_ienc import lock_basins_usace_ienc_candidate
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
from dagster_hifld.assets.usace_floating_docks import usace_floating_docks_usace_ienc
from dagster_hifld.assets.floodwalls_usace_ienc import floodwalls_usace_ienc
from dagster_hifld.assets.harbor_facility_locations_usace_ienc import (
    harbor_facility_locations_usace_ienc,
)
from dagster_hifld.assets.hf_areas_usace_ienc import hf_areas_usace_ienc_candidate
from dagster_hifld.assets.ferrous_metal_processing_plants import (
    ferrous_metal_processing_plants_usgs,
)
from dagster_hifld.assets.ferrous_metal_mines import ferrous_metal_mines_candidate
from dagster_hifld.assets.courthouses_usgs import courthouses_usgs
from dagster_hifld.assets.usace_data_coverage_areas import usace_data_coverage_areas_usace_ienc
from dagster_hifld.assets.epa_rmp_facilities import epa_emergency_response_rmp_facilities
from dagster_hifld.assets.epa_frs_sic_powerplants import epa_frs_sic_powerplants
from dagster_hifld.assets.epa_rcra_tsd import epa_rcra_treatment_storage_and_disposal_facilities
from dagster_hifld.assets.docks import docks_usace_bts
from dagster_hifld.assets.usace_depth_areas import usace_depth_areas_usace_ienc
from dagster_hifld.assets.epa_facilities import epa_facilities
from dagster_hifld.assets.epa_facilities_labels import epa_facilities_labels
from dagster_hifld.assets.epa_frs_tri import epa_frs_toxic_release_inventory
from dagster_hifld.assets.epa_tsca import epa_frs_toxic_substances_control_act_tsca
from dagster_hifld.assets.census_elementary_districts import census_elementary_districts
from dagster_hifld.assets.electric_retail_service_territories import electric_retail_service_territories_preservation_candidate
from dagster_hifld.assets.epa_eez import epa_eez
from dagster_hifld.assets.epa_regions_with_eez import epa_regions_with_eez
from dagster_hifld.assets.epa_large_scale import epa_large_scale_regions
from dagster_hifld.assets.bts_air_truck import bts_intermodal_freight_air_to_truck

from dagster_hifld.assets.epa_sems import epa_sems_interests
from dagster_hifld.assets.historical_tsunami_events import noaa_historical_tsunami_events
from dagster_hifld.assets.bts_ferry_terminals import bts_ferry_terminals

hifld_ingest_assets = [
    bts_ferry_terminals,
    smithsonian_holocene_volcanoes,
    bts_ferry_routes,
    fdic_locations_official,
    fema_regions_official,
    ferc_historical_preservation,
    fuds_program_division_boundaries,
    fema_regional_offices,
    epa_sems_interests,
    epa_eez,
    epa_regions_with_eez,
    epa_large_scale_regions,
    census_elementary_districts,
    noaa_current_dart,
    usgs_crushed_stone_preservation,
    nces_colleges_universities,
    usgs_aquifers,
    usgs_ems_stations,
    noaa_historical_tsunami_events,
    noaa_12nm_territorial_sea,
    census_bureau_aiannh_areas,
    census_bureau_primary_roads,
    census_bureau_block_groups,
    census_bureau_tracts,
    usace_pontoon_areas_usace_ienc,
    usace_hulke_areas_usace_ienc,
    *census_blocks_2020_ingest_assets,
    noaa_200nm_eez_and_maritime_boundaries,
    noaa_24nm_contiguous_zone,
    red_cross_american_red_cross_divisions,
    arc_regions,
    red_cross_headquarters,
    faa_airspace_boundaries,
    faa_aviation_facilities,
    bts_amtrak_stations,
    bts_intermodal_freight_rail_tofc_cofc,
    bts_intermodal_freight_marine_roll_on_roll_off,
    national_transit_map_stops,
    bts_alternative_fueling_stations,
    dhs_hifld_above_ground_lng_storage_facilities,
    usace_administrative_areas_usace_ienc,
    epa_frs_naics_powerplants,
    epa_regions,
    bsee_ocs_platforms,
    proposed_critical_habitat_features,
    usace_navigation_marks_areas_usace_ienc,
    usace_berths_areas_usace_ienc,
    usace_dry_docks_usace_ienc,
    quaternary_fault_lines_usgs,
    usace_pylon_areas_usace_ienc,
    us_county_boundaries_census,
    us_state_boundaries_census,
    usace_pylon_locations_usace_ienc,
    usace_distance_marks_usace_ienc,
    usace_recommended_tracks_ienc,
    usace_overhead_pipeline_lines_usace_ienc,
    usace_overhead_cables_usace_ienc,
    usace_dam_lines_usace_ienc,
    usace_obstruction_lines_usace_ienc,
    usace_mooring_facility_lines_usace_ienc,
    census_secondary_school_districts,
    principal_ports_usace,
    census_unified_school_districts,
    puerto_rico_subbarrios_census,
    usace_caution_areas_usace_ienc,
    usace_pile_locations_usace_ienc,
    usace_daymark_locations_usace_ienc,
    usace_dynamic_buoys_usace_ienc,
    usace_pipeline_areas_usace_ienc,
    usace_data_quality_areas_usace_ienc,
    usace_dumping_ground_areas_usace_ienc,
    plate_boundaries_usgs,
    eia_power_plants_historical,
    epa_frs_eia_860_power_generation_facilities_by_source,
    epa_icis_wastewater_treatment_plants,
    fuds_irm_property_point_ingest,
    usace_fuds_projects_points,
    usace_fuds_program_district_boundaries,
    epa_small_scale_regions,
    nces_private_schools_2023_24,
    nces_public_schools,
    gcca_warehouses_current,
    coastlines_usace_ienc,
    locks_usace_ienc,
    lock_basins_usace_ienc_candidate,
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
    usace_floating_docks_usace_ienc,
    floodwalls_usace_ienc,
    harbor_facility_locations_usace_ienc,
    hf_areas_usace_ienc_candidate,
    ferrous_metal_processing_plants_usgs,
    ferrous_metal_mines_candidate,
    courthouses_usgs,
    usace_data_coverage_areas_usace_ienc,
    docks_usace_bts,
    usace_depth_areas_usace_ienc,
    electric_retail_service_territories_preservation_candidate,
    epa_emergency_response_rmp_facilities,
    epa_frs_sic_powerplants,
    epa_rcra_treatment_storage_and_disposal_facilities,
    epa_facilities,
    epa_facilities_labels,
    epa_frs_toxic_release_inventory,
    epa_frs_toxic_substances_control_act_tsca,
    bts_intermodal_freight_air_to_truck,
]
