"""Census Bureau dataset assets."""

from pathlib import Path

from dagster import AssetKey, Config, asset

from dagster_hifld.sources.aiannh import stage_aiannh_candidate
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


class AiannhIngestConfig(Config):
    """Explicit source selection; local paths replay pinned preserved captures."""
    vintage: int = 2023
    archive_path: str | None = None
    code_commit: str | None = None


@asset(
    key=AssetKey(["ingest", _DATASET_SLUG, _FILE_SLUG]),
    group_name="census_bureau",
    compute_kind="http",
    description=_DESCRIPTION,
)
def census_bureau_aiannh_areas(context, staging_storage: StagingStorageResource,
                               config: AiannhIngestConfig) -> dict:
    result = stage_aiannh_candidate(
        staging_storage,
        vintage=config.vintage,
        archive_path=Path(config.archive_path) if config.archive_path else None,
        code_commit=config.code_commit,
    )
    context.add_output_metadata({key: result[key] for key in
                                 ('version', 'feature_count', 'source_sha256', 'candidate_sha256', 'status')})
    return result
