"""BTS Intermodal Freight Facilities Air-to-Truck native draft assets."""

from dagster_hifld.assets.bts_intermodal_family import build_bts_intermodal_layer
from dagster_hifld.sources import bts_air_truck as source
from dagster_hifld.sources.bts_air_truck import EXPECTED_SOURCE_COUNT

bts_intermodal_freight_air_to_truck, bts_intermodal_freight_air_to_truck_complete, \
    bts_air_truck_draft_job, bts_air_truck_draft_schedule = build_bts_intermodal_layer(
        source,
        source.capture_bts_air_truck,
        asset_name="bts_intermodal_freight_air_to_truck",
        check_name="bts_intermodal_freight_air_to_truck_complete",
        job_name="bts_air_truck_draft_refresh",
        schedule_name="bts_air_truck_draft_refresh_schedule",
        expected_count=lambda: EXPECTED_SOURCE_COUNT,
    )

INGEST_ASSETS = [bts_intermodal_freight_air_to_truck]
ASSET_CHECKS = [bts_intermodal_freight_air_to_truck_complete]
JOBS = [bts_air_truck_draft_job]
SCHEDULES = [bts_air_truck_draft_schedule]
