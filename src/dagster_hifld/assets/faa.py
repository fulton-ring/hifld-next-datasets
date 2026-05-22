"""FAA dataset assets."""

from dagster import AssetKey, asset

from dagster_hifld.download import build_version_id, download_convert_and_stage
from dagster_hifld.resources import StagingStorageResource

_AIRSPACE_DATASET_SLUG = "airspace-boundaries-1"
_AIRSPACE_FILE_SLUG = "airspace-boundaries-1"
_AIRSPACE_DESCRIPTION = (
    "Airspace Boundaries (HIFLD inventory). Publisher: FAA Aeronautical "
    "Information Services. Vector airspace boundary data published every eight "
    "weeks by USDOT/FAA. Source: FAA ADDS Open Data "
    "(adds-faa.opendata.arcgis.com)."
)
_AIRSPACE_BASE = (
    "https://adds-faa.opendata.arcgis.com/api/v3/datasets/"
    "67885972e4e940b2aa6d74024901c561_0/downloads/data"
)
_AVIATION_DATASET_SLUG = "aviation-facilities"
_AVIATION_FILE_SLUG = "aviation-facilities"
_AVIATION_DESCRIPTION = (
    "Aviation Facilities (HIFLD inventory). Publisher: FAA. Part of USDOT/BTS "
    "NTAD; updated every 28 days from FAA. Geographic point database of "
    "official/operational aerodromes in the U.S. and territories. Source: BTS "
    "geodata.bts.gov (FAA NASR-derived)."
)
_AVIATION_BASE = (
    "https://geodata.bts.gov/api/v3/datasets/"
    "1551114f78e34d8395fd77bf41cd8a80_0/downloads/data"
)


@asset(
    key=AssetKey(["ingest", _AIRSPACE_DATASET_SLUG, _AIRSPACE_FILE_SLUG]),
    group_name="faa",
    compute_kind="http",
    description=_AIRSPACE_DESCRIPTION,
)
def faa_airspace_boundaries(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [
            (f"{_AIRSPACE_BASE}?format=shp&spatialRefId=4326", "Airspace_Boundary.zip"),
            (f"{_AIRSPACE_BASE}?format=geojson&spatialRefId=4326", "Airspace_Boundary.geojson"),
        ],
        _AIRSPACE_DATASET_SLUG,
        _AIRSPACE_FILE_SLUG,
        version,
        staging_storage,
    )


@asset(
    key=AssetKey(["ingest", _AVIATION_DATASET_SLUG, _AVIATION_FILE_SLUG]),
    group_name="faa",
    compute_kind="http",
    description=_AVIATION_DESCRIPTION,
)
def faa_aviation_facilities(context, staging_storage: StagingStorageResource) -> dict:
    version = build_version_id(context)
    return download_convert_and_stage(
        [(f"{_AVIATION_BASE}?format=fgdb&spatialRefId=4326", "Aviation_Facilities.gdb.zip")],
        _AVIATION_DATASET_SLUG,
        _AVIATION_FILE_SLUG,
        version,
        staging_storage,
    )
