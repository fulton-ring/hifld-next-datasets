"""American Red Cross dataset assets (sourced from Penn State PASDA)."""

from dagster import AssetKey, asset

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import StagingStorageResource

_DATASET_SLUG = "american-red-cross-divisions"
_FILE_SLUG = "american-red-cross-divisions"
_DESCRIPTION = (
    "American Red Cross Divisions (HIFLD inventory). Publisher: American "
    "Red Cross. Red Cross divisions (corporate structure with DVP, DDE, "
    "DDD). Counties make up chapters, chapters make up regions, regions "
    "make up divisions. Source: Penn State PASDA (HIFLD/FEMA conus_clipped mirror)."
)


@asset(
    key=AssetKey([_DATASET_SLUG, _FILE_SLUG]),
    group_name="red_cross",
    compute_kind="http",
    description=_DESCRIPTION,
)
def red_cross_american_red_cross_divisions(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [("https://www.pasda.psu.edu/download/hifld_fema/conus_clipped/American_Red_Cross_Divisions.zip", "American_Red_Cross_Divisions.zip")],
        _DATASET_SLUG,
        _FILE_SLUG,
        version,
        staging_storage,
    )
