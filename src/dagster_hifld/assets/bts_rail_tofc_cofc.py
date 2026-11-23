"""BTS Intermodal Freight Facilities Rail TOFC/COFC native draft assets."""

from dagster_hifld.assets.bts_intermodal_family import build_bts_intermodal_layer
from dagster_hifld.sources import bts_rail_tofc_cofc as source
from dagster_hifld.sources.bts_rail_tofc_cofc import EXPECTED_SOURCE_COUNT

bts_intermodal_freight_rail_tofc_cofc, bts_intermodal_freight_rail_tofc_cofc_complete, \
    bts_rail_tofc_cofc_draft_job, bts_rail_tofc_cofc_draft_schedule = build_bts_intermodal_layer(
        source,
        source.capture_bts_rail_tofc_cofc,
        asset_name="bts_intermodal_freight_rail_tofc_cofc",
        check_name="bts_intermodal_freight_rail_tofc_cofc_complete",
        job_name="bts_rail_tofc_cofc_draft_refresh",
        schedule_name="bts_rail_tofc_cofc_draft_refresh_schedule",
        expected_count=lambda: EXPECTED_SOURCE_COUNT,
    )

INGEST_ASSETS = [bts_intermodal_freight_rail_tofc_cofc]
ASSET_CHECKS = [bts_intermodal_freight_rail_tofc_cofc_complete]
JOBS = [bts_rail_tofc_cofc_draft_job]
SCHEDULES = [bts_rail_tofc_cofc_draft_schedule]
