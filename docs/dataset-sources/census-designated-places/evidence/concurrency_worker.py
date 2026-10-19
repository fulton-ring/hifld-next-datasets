import os,json,sys
from pathlib import Path
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.assets.census_designated_places import census_designated_places
with DagsterInstance.get() as instance:
 result=defs.resolve_job_def('cdp_source_integrity').execute_in_process(instance=instance,raise_on_error=False)
 output={'run_id':result.run_id,'success':result.success,'output':result.output_for_node(census_designated_places.op.name) if result.success else None,'errors':[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]}
 Path(sys.argv[1]).write_text(json.dumps(output,indent=2));assert result.success
