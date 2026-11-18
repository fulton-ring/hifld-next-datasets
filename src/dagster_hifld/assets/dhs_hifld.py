"""Parked legacy HIFLD definitions whose mirror acquisition is excluded."""

from dagster import AssetKey, asset

from dagster_hifld.resources import StagingStorageResource

_DATASET_SLUG = "above-ground-lng-storage-facilities"
_FILE_SLUG = "above-ground-lng-storage-facilities"
_DESCRIPTION = (
    "Parked Above Ground LNG Storage Facilities legacy definition. The PASDA "
    "HIFLD/FEMA mirror is excluded by source policy; acquisition is refused "
    "before context, download or storage access. No maintained producer route "
    "is established by this definition."
)


@asset(
    key=AssetKey(["ingest", _DATASET_SLUG, _FILE_SLUG]),
    group_name="dhs_hifld",
    compute_kind="http",
    description=_DESCRIPTION,
)
def dhs_hifld_above_ground_lng_storage_facilities(context, staging_storage: StagingStorageResource) -> dict:
    """Refuse the excluded PASDA mirror before any side effect."""
    raise ValueError(
        "source_policy_excluded_mirror: PASDA HIFLD/FEMA LNG mirror is not an authorized maintained producer source."
    )
