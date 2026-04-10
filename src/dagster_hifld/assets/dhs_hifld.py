"""DHS HIFLD dataset assets."""

from dagster import AssetKey, asset

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import StagingStorageResource

_DATASET_SLUG = "above-ground-lng-storage-facilities"
_FILE_SLUG = "above-ground-lng-storage-facilities"
_DESCRIPTION = (
    "Above Ground LNG Storage Facilities (HIFLD inventory). Publisher: "
    "Energy Information Administration / DOE. Above Ground Liquefied "
    "Natural Gas Storage (AG_LNG) facilities for peak shaving, agricultural, "
    "manufacturing, vehicular fuel, etc. CONUS, Alaska, Hawaii. Source: "
    "PASDA HIFLD/FEMA conus_clipped mirror."
)


@asset(
    key=AssetKey([_DATASET_SLUG, _FILE_SLUG]),
    group_name="dhs_hifld",
    compute_kind="http",
    description=_DESCRIPTION,
)
def dhs_hifld_above_ground_lng_storage_facilities(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [("https://www.pasda.psu.edu/download/hifld_fema/conus_clipped/Above_Ground_LNG_Storage_Facilities.zip", "Above_Ground_LNG_Storage_Facilities.zip")],
        _DATASET_SLUG,
        _FILE_SLUG,
        version,
        staging_storage,
    )
