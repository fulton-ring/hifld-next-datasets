"""Actual first-writer construction race in existing isolated draft runtime."""
import json,os,subprocess,sys,shutil
from pathlib import Path
p=Path('/workspace/work/hifld-coordinator/113');runtime=p/'runtime';repo=Path('/workspace/hifld-worker-worktrees/113');report=json.loads((runtime/'execution.json').read_text());version=report['output']['version'];root=runtime/'staging/historical-tsunami-event-locations-/historical-tsunami-event-locations-'/version
history=runtime/'closed-prior-draft-for-concurrency';assert not history.exists();os.rename(root,history)
# Preserve every original healthy draft byte in a closed local proof snapshot.
worker=p/'concurrency_worker.py';worker.write_text('''import os,json,sys
from pathlib import Path
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.assets.historical_tsunami_events import noaa_historical_tsunami_events
with DagsterInstance.get() as instance:
 result=defs.resolve_job_def('tsunami_events_draft_refresh').execute_in_process(instance=instance,raise_on_error=False)
 output={'run_id':result.run_id,'success':result.success,'output':result.output_for_node(noaa_historical_tsunami_events.op.name) if result.success else None,'errors':[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]}
 Path(sys.argv[1]).write_text(json.dumps(output,indent=2));assert result.success
''')
env=os.environ.copy()
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:env.pop(key,None)
env.update(PYTHONPATH=str(repo/'src'),DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1',HIFLD_TSUNAMI_EVENTS_CAPTURE_DIR=str(p/'captures'))
processes=[];logs=[]
for n in (1,2):
 log=(p/f'concurrent-{n}.log').open('wb');logs.append(log);processes.append(subprocess.Popen([sys.executable,str(worker),str(p/f'concurrent-{n}.json')],cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT))
for process in processes:assert process.wait()==0
for log in logs:log.close()
runs=[json.loads((p/f'concurrent-{n}.json').read_text()) for n in (1,2)];assert sorted(r['output']['reused'] for r in runs)==[False,True] and all(r['output']['version']==version for r in runs)
result={'scope':'Actual independent Dagster processes raced construction under same stable source lock and shared isolated instance/storage','runs':runs,'one_complete_first_writer_survives_and_waiter_reuses':True,'prior_healthy_draft_preserved':str(history)};p.joinpath('concurrency.json').write_text(json.dumps(result,indent=2));report['actual_concurrent_construction']=result;(runtime/'execution.json').write_text(json.dumps(report,indent=2));print(result)
