"""NOAA dataset assets."""

from dagster import AssetKey, asset

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import StagingStorageResource

_NOAA_MAPSERVER_BASE = (
    "https://maritimeboundaries.noaa.gov/arcgis/rest/services/"
    "MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer"
)
_TERRITORIAL_SEA_URL = (
    f"{_NOAA_MAPSERVER_BASE}/1/query?where=1%3D1&outFields=*&f=geojson"
)
_TERRITORIAL_SEA_NAME = "12nm-territorial-sea.geojson"
_TERRITORIAL_SEA_DATASET_SLUG = "12nm-territorial-sea"
_TERRITORIAL_SEA_FILE_SLUG = "12nm-territorial-sea"
_EEZ_URL = f"{_NOAA_MAPSERVER_BASE}/3/query?where=1%3D1&outFields=*&f=geojson"
_EEZ_NAME = "200nm-eez-and-maritime-boundaries.geojson"
_EEZ_DATASET_SLUG = "200nm-eez-and-maritime-boundaries"
_EEZ_FILE_SLUG = "200nm-eez-and-maritime-boundaries"
_CONTIGUOUS_ZONE_URL = (
    f"{_NOAA_MAPSERVER_BASE}/2/query?where=1%3D1&outFields=*&f=geojson"
)
_CONTIGUOUS_ZONE_NAME = "24nm-contiguous-zone.geojson"
_CONTIGUOUS_ZONE_DATASET_SLUG = "24nm-contiguous-zone"
_CONTIGUOUS_ZONE_FILE_SLUG = "24nm-contiguous-zone"


@asset(
    key=AssetKey([_TERRITORIAL_SEA_DATASET_SLUG, _TERRITORIAL_SEA_FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description="12NM Territorial Sea (HIFLD inventory). Publisher: National Oceanic and Atmospheric Administration. NOAA depicts on nautical charts the limits of the 12 NM Territorial Sea, 24 NM Contiguous Zone, and 200 NM EEZ from the U.S. normal baseline. Source: maritimeboundaries.noaa.gov.",
)
def noaa_12nm_territorial_sea(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [(_TERRITORIAL_SEA_URL, _TERRITORIAL_SEA_NAME)],
        _TERRITORIAL_SEA_DATASET_SLUG,
        _TERRITORIAL_SEA_FILE_SLUG,
        version,
        staging_storage,
    )


@asset(
    key=AssetKey([_EEZ_DATASET_SLUG, _EEZ_FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description="200NM EEZ and Maritime Boundaries (HIFLD inventory). Publisher: NOAA. Outer limit of the U.S. Exclusive Economic Zone and maritime boundaries with adjacent countries; from the U.S. Baseline Committee. Source: maritimeboundaries.noaa.gov.",
)
def noaa_200nm_eez_and_maritime_boundaries(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [(_EEZ_URL, _EEZ_NAME)],
        _EEZ_DATASET_SLUG,
        _EEZ_FILE_SLUG,
        version,
        staging_storage,
    )


@asset(
    key=AssetKey([_CONTIGUOUS_ZONE_DATASET_SLUG, _CONTIGUOUS_ZONE_FILE_SLUG]),
    group_name="noaa",
    compute_kind="http",
    description="24NM Contiguous Zone (HIFLD inventory). Publisher: NOAA. Maritime limits from the U.S. normal baseline; part of NOAA OCS U.S. Maritime Limits & Boundaries. Source: maritimeboundaries.noaa.gov.",
)
def noaa_24nm_contiguous_zone(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [(_CONTIGUOUS_ZONE_URL, _CONTIGUOUS_ZONE_NAME)],
        _CONTIGUOUS_ZONE_DATASET_SLUG,
        _CONTIGUOUS_ZONE_FILE_SLUG,
        version,
        staging_storage,
    )
