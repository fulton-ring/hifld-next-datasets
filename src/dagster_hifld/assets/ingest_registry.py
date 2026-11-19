"""Ingest asset registry.

New datasets declare ``INGEST_ASSETS`` in their own asset module (see
``discovery.py``) rather than adding entries here. Downstream catalog/publish/check
assets are derived automatically from this registry.
"""

from dagster_hifld.assets.discovery import discovered, unique

from dagster_hifld.assets.noaa_current_dart import noaa_current_dart
from dagster_hifld.assets.noaa import (
    noaa_12nm_territorial_sea,
    noaa_200nm_eez_and_maritime_boundaries,
)
from dagster_hifld.assets.national_transit_map_stops import national_transit_map_stops
from dagster_hifld.assets.usace import usace_administrative_areas_usace_ienc
from dagster_hifld.assets.usace import usace_navigation_marks_areas_usace_ienc
from dagster_hifld.assets.usace_navigation_lights import usace_navigation_lights_usace_ienc
from dagster_hifld.assets.usace_berths_areas import usace_berths_areas_usace_ienc
from dagster_hifld.assets.usace_dry_docks import usace_dry_docks_usace_ienc
from dagster_hifld.assets.usace_distance_marks import usace_distance_marks_usace_ienc
from dagster_hifld.assets.usace_recommended_tracks import usace_recommended_tracks_ienc
from dagster_hifld.assets.usace_obstruction_lines import usace_obstruction_lines_usace_ienc
from dagster_hifld.assets.usace_mooring_facility_lines import usace_mooring_facility_lines_usace_ienc
from dagster_hifld.assets.small_craft_facility_areas_usace_ienc import small_craft_facility_areas_usace_ienc
from dagster_hifld.assets.coastlines_usace_ienc import coastlines_usace_ienc
from dagster_hifld.assets.locks_usace_ienc import locks_usace_ienc
from dagster_hifld.assets.lock_basins_usace_ienc import lock_basins_usace_ienc_candidate

from dagster_hifld.assets.ibc import ibc_canada_us_border
from dagster_hifld.assets.usace_crane_locations import usace_crane_locations_ienc
from dagster_hifld.assets.usace_floating_docks import usace_floating_docks_usace_ienc
from dagster_hifld.assets.building_locations_usace_ienc import building_locations_usace_ienc
from dagster_hifld.assets.epa_frs_sic_powerplants import epa_frs_sic_powerplants
from dagster_hifld.assets.census_elementary_districts import census_elementary_districts

from dagster_hifld.assets.bts_ferry_terminals import bts_ferry_terminals

hifld_ingest_assets = [
    bts_ferry_terminals,
    census_elementary_districts,
    noaa_current_dart,
    noaa_12nm_territorial_sea,
    noaa_200nm_eez_and_maritime_boundaries,
    national_transit_map_stops,
    usace_administrative_areas_usace_ienc,
    usace_navigation_marks_areas_usace_ienc,
    usace_navigation_lights_usace_ienc,
    usace_berths_areas_usace_ienc,
    usace_dry_docks_usace_ienc,
    usace_distance_marks_usace_ienc,
    usace_recommended_tracks_ienc,
    usace_obstruction_lines_usace_ienc,
    usace_mooring_facility_lines_usace_ienc,
    small_craft_facility_areas_usace_ienc,
    coastlines_usace_ienc,
    locks_usace_ienc,
    lock_basins_usace_ienc_candidate,
    ibc_canada_us_border,
    usace_crane_locations_ienc,
    usace_floating_docks_usace_ienc,
    epa_frs_sic_powerplants,
    building_locations_usace_ienc,
]

hifld_ingest_assets = unique(hifld_ingest_assets, discovered("INGEST_ASSETS"))
