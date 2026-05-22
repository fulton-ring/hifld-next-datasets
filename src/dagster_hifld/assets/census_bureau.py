"""Census Bureau dataset assets."""

from dagster import AssetKey, asset

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import StagingStorageResource

_DATASET_SLUG = "aiannh-areas-1"
_FILE_SLUG = "aiannh-areas-1"
_DESCRIPTION = (
    "American Indian / Alaska Native / Native Hawaiian (AIANNH) Areas "
    "(HIFLD inventory). Publisher: Census Bureau. Federal and state "
    "American Indian reservations, off-reservation trust lands; from "
    "TIGER/Line. Source: Census Bureau TIGER2023 AIANNH "
    "(tl_2023_us_aiannh.zip)."
)


@asset(
    key=AssetKey(["ingest", _DATASET_SLUG, _FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=_DESCRIPTION,
)
def census_bureau_aiannh_areas(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [("https://www2.census.gov/geo/tiger/TIGER2023/AIANNH/tl_2023_us_aiannh.zip", "tl_2023_us_aiannh.zip")],
        _DATASET_SLUG,
        _FILE_SLUG,
        version,
        staging_storage,
    )
