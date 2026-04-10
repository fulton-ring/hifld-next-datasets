"""BTS dataset assets."""

from dagster import AssetKey, asset

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import StagingStorageResource

_DATASET_SLUG = "amtrak-stations"
_FILE_SLUG = "amtrak-stations"
_GROUP_NAME = "bts"
_DESCRIPTION = (
    "Amtrak Stations (HIFLD inventory). Publisher: Amtrak. Part of USDOT/BTS "
    "National Transportation Atlas Database (NTAD). Contains Amtrak intercity "
    "railroad passenger terminals in the United States. Source: BTS geodata.bts.gov."
)
_AMTRAK_BASE = (
    "https://geodata.bts.gov/api/v3/datasets/"
    "1ed62a9f46304679aaa396bed4c8565a_0/downloads/data"
)


@asset(
    key=AssetKey([_DATASET_SLUG, _FILE_SLUG]),
    group_name=_GROUP_NAME,
    compute_kind="http",
    description=_DESCRIPTION,
)
def bts_amtrak_stations(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [
            (f"{_AMTRAK_BASE}?format=fgdb&spatialRefId=4326", "Amtrak_Stations.gdb.zip"),
            (f"{_AMTRAK_BASE}?format=shp&spatialRefId=4326", "Amtrak_Stations.zip"),
        ],
        _DATASET_SLUG,
        _FILE_SLUG,
        version,
        staging_storage,
    )
