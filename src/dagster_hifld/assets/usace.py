"""USACE dataset assets."""

from dagster import AssetKey, asset

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import StagingStorageResource

_DATASET_SLUG = "administrative-areas-usace-ienc"
_FILE_SLUG = "administrative-areas-usace-ienc"
_DESCRIPTION = (
    "Administrative Areas (USACE IENC) (HIFLD inventory). Publisher: "
    "Army Geospatial Center / US Army Corps of Engineers. Inland navigation "
    "system (rivers, lock chambers) in 22 states; IENC S-57 data in Esri "
    "File Geodatabase. Source: PASDA HIFLD/FEMA conus_clipped mirror."
)


@asset(
    key=AssetKey(["ingest", _DATASET_SLUG, _FILE_SLUG]),
    group_name="usace",
    compute_kind="http",
    description=_DESCRIPTION,
)
def usace_administrative_areas_usace_ienc(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [("https://www.pasda.psu.edu/download/hifld_fema/conus_clipped/Administrative_Areas__USACE_IENC_.zip", "Administrative_Areas__USACE_IENC_.zip")],
        _DATASET_SLUG,
        _FILE_SLUG,
        version,
        staging_storage,
    )
