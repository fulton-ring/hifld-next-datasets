"""Actual isolated registered pipeline proof for construction Git identity gates.

Complete retained original inputs; no real dataset acceptance or production write.
Earlier complete geometry/container/report negatives remain in frozen evidence.
"""
from __future__ import annotations
import argparse
import ast
import json
import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);args=parser.parse_args()
repo=Path(__file__).resolve().parents[1];runtime=args.runtime.resolve();assert not runtime.exists()
for name in ['dagster','staging','published']:(runtime/name).mkdir(parents=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
import fiona
import pyarrow.parquet as pq
import shapely
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.fws_units import fws_cbrs_units
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import cbrs_units as source
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.provenance import provenance_projection

staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
published.write_key('synthetic-accepted-fixture.bin',b'local-fixture-only-not-an-accepted-real-release')
def snapshot(storage):return {key:source.sha(storage.read_key(key)) for key in storage.list_prefix()}
prior=snapshot(published);keys=[key for key in normal.resolve_asset_graph().get_all_asset_keys() if key.path[0]=='publish']
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('cbrs_construction_graph',selection=AssetSelection.assets(*keys))])
report={'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'runtime':str(runtime),'module':source.__file__,'approved':False,'runs':[]}
config={'ops':{fws_cbrs_units.op.name:{'config':{'capture_dir':str(repo/'docs/dataset-sources/cbrs-units-2/evidence/captures')}}}}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,'materializations':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()],'checks':[{'name':e.check_name,'passed':e.passed} for e in result.get_asset_check_evaluations()]})
 (runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');return errors

def source_run(kind):
 result=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record(kind,result);assert result.success
 return result.output_for_node(fws_cbrs_units.op.name)

with patch('dagster_hifld.assets.publish.process_layer_partitioned_geoparquet',side_effect=AssertionError('no generic converter fallback for native prebuilt')) as converter,DagsterInstance.get() as instance:
 job=normal.resolve_job_def('fws_cbrs_units_draft_refresh');graph=defs.resolve_job_def('cbrs_construction_graph')
 output=source_run('real_complete_authenticated_source');report['output']=output;assert output['feature_count']==942
 prefix=build_publish_partition_key(source.SLUG,source.SLUG,output['version']);root=runtime/'staging'/prefix
 binding=runtime/'staging/_source-provenance'/source.SLUG/output['version']/'bundle.json';binding_original=binding.read_bytes()
 manifest=json.loads((root/'metadata/source_manifest.json').read_bytes());assert manifest==json.loads(binding_original)['manifest'] and manifest['code_commit']==report['code_head']
 source.validate_construction_code_identity(repo,manifest['code_commit'],source.processing_identity()['files'])
 before=source.candidate_inventory(root);assert source_run('real_authenticated_reuse')==output;assert source.candidate_inventory(root)==before
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
 for cycle in (1,2):
  normal_result=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);errors=record(f'normal_catalog_{cycle}_missing_real_registry',normal_result)
  assert not normal_result.success and errors and all('missing_independent_release_approval_registry' in e for e in errors)
  assert source_run(f'real_refresh_after_catalog_{cycle}')==output
  assert snapshot(published)==prior and converter.call_count==0
 expected_products={'geopackage/cbrs-units-2.gpkg':'0549479ec4e30738dff31c2b9585db1ae82131b37e993b84c5627f0fb96b9350','geoparquet/cbrs-units-2.parquet':'64f1bb9b98ff27b76bcfbeb8f6dc049815c0a828f022c87f261b1f797707520a','metadata/candidate.records.jsonl':'320b8cf705073f5042fa230dd02457776612e984a38c50d416c42acdbd8eebcc'}
 for name,pin in expected_products.items():assert source.sha((root/name).read_bytes())==pin
 rows=pq.read_table(root/'geoparquet'/f'{source.SLUG}.parquet').to_pylist();assert len(rows)==942
 assert rows[679]['Name']==' Brigantine' and rows[680]['Name']==' Island Beach'
 with fiona.open(root/'geopackage'/f'{source.SLUG}.gpkg') as stream:
  for row,feature in zip(rows,stream):
   assert {k:row[k] for k in source.SCHEMA}==dict(feature['properties']);assert shapely.geometry.shape(feature.geometry).wkb==row['geometry']
 report['native_products_unchanged_since_corrected4cdb']=expected_products
 report['construction_attacks']=[]
 for attack in ['coforged_zero_commit','coforged_reachable_wrong_source_commit','missing_external_binding','invalid_external_binding']:
  metadata={str(p.relative_to(root)):p.read_bytes() for p in (root/'metadata').rglob('*') if p.is_file()};raw={str(p.relative_to(root)):source.sha(p.read_bytes()) for p in (root/'raw').rglob('*') if p.is_file()}
  if attack.startswith('coforged_'):
   value=json.loads(metadata['metadata/source_manifest.json']);value['code_commit']='0'*40 if attack=='coforged_zero_commit' else 'f7669766e2cd9168f06f833b70c64fbbf7747edf'
   if attack!='coforged_zero_commit':assert subprocess.run(['git','merge-base','--is-ancestor',value['code_commit'],'HEAD'],cwd=repo).returncode==0
   forged=source.encode(value);(root/'metadata/source_manifest.json').write_bytes(forged)
   bundle=json.loads(binding_original);bundle['manifest']=value;binding.write_bytes(source.encode(bundle))
   for file in (root/'metadata/source').glob('*source_manifest.json'):
    if file.name=='source_manifest.json':file.write_bytes(forged)
    else:file.unlink()
   (root/'metadata/source'/('version_'+source.sha(forged)+'_source_manifest.json')).write_bytes(forged)
   resolved=provenance_projection(load_resolved_source_manifest(staging,source.SLUG,source.SLUG,output['version']).metadata);(root/'metadata/resolved_provenance.json').write_bytes(source.encode(resolved))
   readyfile=root/'metadata/cbrs_units_candidate_ready.json';ready=json.loads(readyfile.read_bytes());ready['objects']['metadata/source_manifest.json']=source.sha(forged);readyfile.write_bytes(source.encode(ready))
  elif attack=='missing_external_binding':binding.unlink()
  else:binding.write_bytes(b'{}\n')
  inventory=source.candidate_inventory(root);prior_quarantines=set((runtime/'staging/_quarantine'/source.SLUG/'versions').glob('*/candidate'))
  result=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);errors=record(attack,result)
  expected=('construction_invalid_commit' if attack=='coforged_zero_commit' else 'construction_processing_blob_hash_mismatch' if attack=='coforged_reachable_wrong_source_commit' else 'missing_original_construction_provenance_bundle' if attack=='missing_external_binding' else 'original_construction_bundle_roster_drift')
  assert not result.success and errors and any(expected in e for e in errors),errors
  assert not root.exists() and (source.SLUG,source.SLUG,output['version']) not in list(_iter_staged_version_paths(staging))
  retained=set((runtime/'staging/_quarantine'/source.SLUG/'versions').glob('*/candidate'))-prior_quarantines;assert len(retained)==1;retained=retained.pop()
  assert source.candidate_inventory(retained)==inventory
  assert raw=={str(p.relative_to(retained)):source.sha(p.read_bytes()) for p in (retained/'raw').rglob('*') if p.is_file()}
  assert json.loads((retained.parent/'report.json').read_bytes())['inventory']==inventory
  assert snapshot(published)==prior;binding.write_bytes(binding_original)
  report['construction_attacks'].append({'kind':attack,'run_id':result.run_id,'expected_error':expected,'rejected':True,'all_remaining_candidate_bytes_preserved':True,'raw_preserved':True,'not_discoverable':True,'prior_published_preserved':True})
  if attack=='coforged_zero_commit':
   # Rebuild from preserved complete original captures and the restored genuine
   # construction record; never copy the untrusted quarantined candidate back.
   assert source_run('complete_rebuild_from_original_inputs_after_forgery')==output
   for name,pin in expected_products.items():assert source.sha((root/name).read_bytes())==pin
   assert (root/'metadata/source_manifest.json').read_bytes()==metadata['metadata/source_manifest.json']
  else:
   os.rename(retained,root)
   for file in (root/'metadata').rglob('*'):
    if file.is_file() and str(file.relative_to(root)) not in metadata:file.unlink()
   for name,body in metadata.items():(root/name).write_bytes(body)
 assert source_run('real_refresh_after_restored_binding_negatives')==output
 # One bounded synthetic full version tests failed quality preserves prior data.
 badprefix=prefix+'-synthetic-invalid-geometry';badroot=runtime/'staging'/badprefix;shutil.copytree(root,badroot)
 gp=badroot/'geopackage'/f'{source.SLUG}.gpkg'
 with fiona.open(gp) as stream:schema=stream.schema;crs=stream.crs;features=[{'properties':dict(f['properties']),'geometry':f.geometry} for f in stream]
 features[0]['geometry']=None;gp.unlink()
 with fiona.open(gp,'w',driver='GPKG',layer=source.SLUG,schema=schema,crs=crs) as stream:stream.writerecords(features)
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix]);result=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);record('synthetic_failed_geometry_quality',result)
 assert not result.success and any(not e.passed for e in result.get_asset_check_evaluations())
 assert snapshot(published)==prior and converter.call_count==0
 assert binding.read_bytes()==binding_original
 report.update(generic_converter_calls=converter.call_count,source_rows=942,all_original_source_values_types_native_WKB_preserved=True,prior_published_preserved=True,raw_preserved=True,original_construction_history_preserved=True,source_discoverable_without_manual_copy=True,real_release_approved=False)
 # Recorded source decoding/writers are exactly the physical-name-corrected4cdb.
 old=ast.parse(subprocess.check_output(['git','show','4cdb6bcb80b6fb66ff8fffeb86ae760fa544de19:src/dagster_hifld/sources/cbrs_units.py'],cwd=repo,text=True));current=ast.parse((repo/'src/dagster_hifld/sources/cbrs_units.py').read_text());report['unchanged_data_writers']={}
 for name in ['decode_physical_dbf','inspect_archive','typed_table','derive_native_parquet_bytes','write_geopackage','source_verification_payloads']:
  left=ast.dump(next(v for v in old.body if isinstance(v,ast.FunctionDef) and v.name==name),include_attributes=False);right=ast.dump(next(v for v in current.body if isinstance(v,ast.FunctionDef) and v.name==name),include_attributes=False);assert left==right;report['unchanged_data_writers'][name]=source.sha(right.encode())
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'code_head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
