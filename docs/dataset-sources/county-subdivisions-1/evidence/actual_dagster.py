"""Actual isolated registered graph; complete real source and tiny labeled quality fixture."""
import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

parser=argparse.ArgumentParser();parser.add_argument('--resume',action='store_true');parser.add_argument('--acquisition-repair',action='store_true');parser.add_argument('--repo',type=Path,required=True);parser.add_argument('--runtime',type=Path,required=True);args=parser.parse_args();repo=args.repo;runtime=args.runtime
for name in ['dagster','staging','published']:(runtime/name).mkdir(parents=True,exist_ok=args.resume)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(key,None)
raw='/workspace/work/hifld-coordinator/34/runtime/staging/_raw-captures/census-designated-places/e2409cd4ddb1a9302b130096fbcf9925ca3a701f720b8991045602bcb99d6b35/original.gdb.zip';receipt='/workspace/work/hifld-coordinator/34/official2024_substategeo.receipt.json'
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1',HIFLD_COUSUB_ARCHIVE=raw,HIFLD_COUSUB_RECEIPT=receipt)
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.census_county_subdivisions import census_county_subdivisions
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import county_subdivisions as source

staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
if not args.resume:published.write_key('synthetic-previous-publication-fixture.bin',b'local behavior fixture only; not real accepted data')
else:assert published.read_key('synthetic-previous-publication-fixture.bin')==b'local behavior fixture only; not real accepted data'
def snapshot(root):return {str(p.relative_to(root)):source.sha256_file(p) for p in root.rglob('*') if p.is_file()}
prior=snapshot(runtime/'published');originalraw={'bytes':Path(raw).stat().st_size,'sha256':source.sha256_file(Path(raw))}
normalkeys=[k for k in normal.resolve_asset_graph().get_all_asset_keys() if k.path[0]=='publish']
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('county_subdivisions_actual_formats',selection=AssetSelection.assets(*normalkeys))])
report={'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'runtime':str(runtime),'module':source.__file__,'approved':False,'runs':[]}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,'materializations':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()],'checks':[{'name':e.check_name,'passed':e.passed} for e in result.get_asset_check_evaluations()]});(runtime/'execution-progress.json').write_text(json.dumps(report,indent=2)+'\n');return errors
with DagsterInstance.get() as instance:
 job=normal.resolve_job_def('county_subdivisions_draft_refresh');graph=defs.resolve_job_def('county_subdivisions_actual_formats')
 if not args.resume:
  if args.acquisition_repair:
   bad_receipt=runtime/'explicit-synthetic-malformed-receipt.bin';bad_receipt.write_bytes(b'{EXPLICIT_SYNTHETIC_MALFORMED_RECEIPT')
   os.environ['HIFLD_COUSUB_RECEIPT']=str(bad_receipt)
   rejected=job.execute_in_process(instance=instance,raise_on_error=False);errors=record('actual_synthetic_bad_receipt_full_original_source',rejected)
   assert not rejected.success and any('external_capture_receipt_rejected_after_observation' in e for e in errors)
   observed=list((runtime/'staging/_raw-untrusted-observations').rglob('receipt.bin'));assert len(observed)==1 and observed[0].read_bytes()==bad_receipt.read_bytes()
   diagnostic=json.loads(observed[0].with_name('observation.json').read_text());assert diagnostic['complete_receipt_descriptor_read'] and diagnostic['classification'].startswith('UNTRUSTED')
   assert not (runtime/'staging/_construction-state').exists() and snapshot(runtime/'published')==prior
   report['bad_receipt_observation']={'path':str(observed[0]),'bytes':observed[0].stat().st_size,'sha256':source.sha256_file(observed[0]),'diagnostic':diagnostic,'no_first_state':True,'no_source_validity_claim':True}
   os.environ['HIFLD_COUSUB_RECEIPT']=receipt
  first=job.execute_in_process(instance=instance,raise_on_error=False);record('complete_real_source_36492',first);assert first.success
  output=first.output_for_node(census_county_subdivisions.op.name);assert output['feature_count']==36492;prefix=build_publish_partition_key(source.SLUG,source.SLUG,output['version']);root=runtime/'staging'/prefix
  assert (source.SLUG,source.SLUG,output['version']) in list(_iter_staged_version_paths(staging));before=snapshot(root)
  refresh=job.execute_in_process(instance=instance,raise_on_error=False);record('complete_real_idempotent_refresh',refresh);assert refresh.success and refresh.output_for_node(census_county_subdivisions.op.name)['reused'] and snapshot(root)==before
  instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
  for index in ([1] if args.acquisition_repair else [1,2]):
   normalrun=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);errors=record('normal_catalog_missing_approval_'+str(index),normalrun)
   assert not normalrun.success and errors and all('missing_independent_release_approval_registry' in e for e in errors),errors
   assert snapshot(runtime/'published')==prior
   post=job.execute_in_process(instance=instance,raise_on_error=False);record('complete_real_post_catalog_refresh_'+str(index),post);assert post.success and post.output_for_node(census_county_subdivisions.op.name)['reused']
   assert snapshot(runtime/'published')==prior
 else:
  report=json.loads((runtime/'execution-progress.json').read_text());assert len(report['runs'])==6 and report['runs'][-1]['success']
  roots=[p for p in (runtime/'staging'/source.SLUG/source.SLUG).iterdir() if p.is_dir() and p.name.startswith('v0.')];assert len(roots)==1;root=roots[0];prefix=str(root.relative_to(runtime/'staging'));output={'dataset_slug':source.SLUG,'file_slug':source.SLUG,'version':root.name,'feature_count':36492,'reused':False}
  report['harness_resume']='Preserved all six completed runs; fixed dictionary geometry label assertion from SHAPE to generic catalog geometry; no source/candidate changes or repeated complete source runs.'
 audit=json.loads((root/'metadata/cousub_source_audit.json').read_text());source.verify_outputs(root,Path(raw),audit,canonical=False)
 q=json.loads((root/'metadata/quality_manifest.json').read_text());assert q['quality_check_passed'] and q['feature_count']==36492
 dictionary=json.loads((root/'metadata/data_dictionary.json').read_text());columns={c['name']:c for c in dictionary['columns']};assert set(source.FIELDS)-{'SHAPE'}==set(columns)-{'geometry','SHAPE'};assert set(columns)&{'geometry','SHAPE'}
 if not args.acquisition_repair:
  # Tiny explicit behavior partition: no partial substitute claimed as a dataset.
  badprefix=prefix+'-synthetic-2row-null-geometry';badroot=runtime/'staging'/badprefix;(badroot/'geopackage').mkdir(parents=True);(badroot/'metadata').mkdir()
  _,t=pyogrio.read_arrow(source.uri(Path(raw)),layer=source.LAYER,return_fids=True,max_features=2);b=source.native_batch(t.to_batches()[0]);t=pa.Table.from_batches([b]);geoms=t['SHAPE'].to_pylist();geoms[0]=None;t=t.set_column(t.schema.get_field_index('SHAPE'),t.schema.field('SHAPE'),pa.array(geoms,type=pa.binary()))
  pyogrio.write_arrow(t,badroot/'geopackage'/f'{source.SLUG}.gpkg',layer=source.LAYER,driver='GPKG',geometry_name='SHAPE',geometry_type='MultiPolygon',crs='EPSG:4269',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'SHAPE'})
  m=json.loads((root/'metadata/source_manifest.json').read_text());m['synthetic_fixture_scope']='two real-derived rows with deliberately NULL geometry; not complete source or accepted data';m.pop('prebuilt_output_policy',None);(badroot/'metadata/source_manifest.json').write_bytes(source.encode(m))
  instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix]);bad=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);record('synthetic_two_row_failed_quality',bad);assert not bad.success and any(not x.passed for x in bad.get_asset_check_evaluations())
 else:
  report['prior_quality_proof']='Old689seven-run graph and quality negative43beeb7f retained; catalog/publish/nativewriter functions unchanged, complete native gold byte parity separately verified.'
 assert snapshot(runtime/'published')==prior;assert originalraw=={'bytes':Path(raw).stat().st_size,'sha256':source.sha256_file(Path(raw))}
 report.update(output=output,source_audit=audit,quality=q,all_original_native_values_ids_types_nulls_wkb_verified=True,original_external_capture_preserved_read_only=True,prior_local_publication_fixture_unchanged=True,discovery_registered=True,candidate_snapshot=snapshot(root),terminal_parquet={'bytes':(root/'geoparquet'/f'{source.SLUG}.parquet').stat().st_size,'sha256':source.sha256_file(root/'geoparquet'/f'{source.SLUG}.parquet'),'native_oid_type':str(pq.read_schema(root/'geoparquet'/f'{source.SLUG}.parquet').field('OBJECTID').type)})
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'status':'PASS','runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
