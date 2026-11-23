"""BTS Intermodal Freight Facilities Marine Roll-on/Roll-off native draft assets."""

from dagster_hifld.assets.bts_intermodal_family import build_bts_intermodal_layer
from dagster_hifld.sources import bts_marine_roro as source
from dagster_hifld.sources.bts_marine_roro import EXPECTED_SOURCE_COUNT

(bts_intermodal_freight_marine_roll_on_roll_off,
 bts_intermodal_freight_marine_roll_on_roll_off_complete,
 bts_marine_roro_draft_job,
 bts_marine_roro_draft_schedule) = build_bts_intermodal_layer(
     source,
     source.capture_bts_marine_roro,
     asset_name="bts_intermodal_freight_marine_roll_on_roll_off",
     check_name="bts_intermodal_freight_marine_roll_on_roll_off_complete",
     job_name="bts_marine_roro_draft_refresh",
     schedule_name="bts_marine_roro_draft_refresh_schedule",
     expected_count=lambda: EXPECTED_SOURCE_COUNT,
 )

INGEST_ASSETS = [bts_intermodal_freight_marine_roll_on_roll_off]
ASSET_CHECKS = [bts_intermodal_freight_marine_roll_on_roll_off_complete]
JOBS = [bts_marine_roro_draft_job]
SCHEDULES = [bts_marine_roro_draft_schedule]
