"""Real30-row Dagster regression probes for self-pinned cache and interrupted builds."""
import argparse
import json
import os
import shutil
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);args=parser.parse_args()
repo=Path(__file__).resolve().parents[1];runtime=args.runtime.resolve();assert not runtime.exists()
for name in ['dagster','cache-staging','interruption-staging','published']:(runtime/name).mkdir(parents=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
os.environ['DAGSTER_HOME']=str(runtime/'dagster')
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_hifld.assets.ibc import ibc_canada_us_border
from dagster_hifld.definitions import _iter_staged_version_paths
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import canada_us_border as source

capture=repo/'docs/dataset-sources/canada-and-us-border/evidence/captures'
published=PublishedStorageResource(local_dir=str(runtime/'published'))
published.write_key('prior-fixture.bin',b'prior-local-fixture-no-real-acceptance')
prior=published.read_key('prior-fixture.bin')
config={'ops':{ibc_canada_us_border.op.name:{'config':{'capture_dir':str(capture)}}}}
report={'approved':False,'runs':[]}
def job(storage):
 return Definitions(assets=[ibc_canada_us_border],resources={'staging_storage':storage},jobs=[define_asset_job('boundary_review_probe',selection=AssetSelection.assets(ibc_canada_us_border))]).resolve_job_def('boundary_review_probe')
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors});return errors
with DagsterInstance.get() as instance:
 storage=StagingStorageResource(local_dir=str(runtime/'cache-staging'))
 pipeline=job(storage);first=pipeline.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 record('real_source_before_cache_attack',first);assert first.success
 output=first.output_for_node(ibc_canada_us_border.op.name)
 root=runtime/'cache-staging'/source.SLUG/source.SLUG/output['version'];raw=(root/'raw'/source.ARCHIVE_NAME).read_bytes()
 path=root/'candidate/typed-source.parquet';table=pq.read_table(path)
 table=table.set_column(table.schema.get_field_index('SectionEng'),table.schema.field('SectionEng'),pa.array(['independent-cache-tamper-probe']*30))
 pq.write_table(table,path)
 marker=root/'metadata/canada_us_border_candidate_ready.json';ready=json.loads(marker.read_text());ready['objects']['candidate/typed-source.parquet']=source.sha(path.read_bytes());marker.write_text(json.dumps(ready))
 failed=pipeline.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 errors=record('repinned_all30_cache_values_rejected',failed)
 assert not failed.success and any('cached_typed_parquet_source_fidelity_failure' in error for error in errors)
 assert raw==(root/'raw'/source.ARCHIVE_NAME).read_bytes()
 assert list((runtime/'cache-staging/_quarantine').rglob('report.json'))
 assert published.read_key('prior-fixture.bin')==prior
 fresh=StagingStorageResource(local_dir=str(runtime/'interruption-staging'));pipeline=job(fresh)
 original=source.immutable
 def injected(storage,key,body):
  if Path(storage.local_dir).name.startswith('.candidate-build-') and key=='candidate/typed-source.parquet':raise OSError('injected real local staging interruption before Parquet write')
  return original(storage,key,body)
 with patch.object(source,'immutable',side_effect=injected):
  failed=pipeline.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 errors=record('interrupted_private_real_candidate_rejected',failed)
 assert not failed.success and any('injected real local staging interruption' in error for error in errors)
 assert list(_iter_staged_version_paths(fresh))==[]
 assert list((runtime/'interruption-staging/_raw-captures').rglob('ibc-zip.body'))
 assert list((runtime/'interruption-staging/_quarantine').rglob('report.json'))
 assert not list((runtime/'interruption-staging').rglob('.candidate-build-*'))
 assert published.read_key('prior-fixture.bin')==prior
report.update(cache_attack_is_not_release_approval_bypass=True,fresh_source_reconciliation_rejects_self_repinned_cache=True,
 interruption_leaves_no_sensor_discoverable_candidate=True,original_raw_preserved=True,prior_local_fixture_preserved=True)
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
