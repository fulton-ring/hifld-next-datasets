import os,json,sys
from pathlib import Path
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.assets.historical_tsunami_events import noaa_historical_tsunami_events
with DagsterInstance.get() as instance:
 result=defs.resolve_job_def('tsunami_events_draft_refresh').execute_in_process(instance=instance,raise_on_error=False)
 output={'run_id':result.run_id,'success':result.success,'output':result.output_for_node(noaa_historical_tsunami_events.op.name) if result.success else None,'errors':[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]}
 Path(sys.argv[1]).write_text(json.dumps(output,indent=2));assert result.success
