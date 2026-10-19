"""Actual isolated Dagster source and normal format/gate runs on all942real CBRS units."""
from __future__ import annotations
import argparse
import json
import os
import shutil
import subprocess
import sqlite3
import pyarrow as pa
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);parser.add_argument('--resume-after-integrity',action='store_true');parser.add_argument('--live-followup',action='store_true');args=parser.parse_args()
repo=Path(__file__).resolve().parents[1];capture=repo/'docs/dataset-sources/cbrs-units-2/evidence/captures'
runtime=args.runtime.resolve();assert runtime.is_dir() if args.resume_after_integrity else not runtime.exists()
for name in ['dagster','staging','published']:(runtime/name).mkdir(parents=True,exist_ok=args.resume_after_integrity)
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

staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True)
published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
fixture=b'local-fixture-only-not-an-accepted-real-release'
if args.resume_after_integrity:
 assert published.list_prefix()==['synthetic-accepted-fixture.bin'] and published.read_key('synthetic-accepted-fixture.bin')==fixture
else:published.write_key('synthetic-accepted-fixture.bin',fixture)
def snapshot(storage):return {key:source.sha(storage.read_key(key)) for key in storage.list_prefix()}
prior=snapshot(published)
normalkeys=[key for key in normal.resolve_asset_graph().get_all_asset_keys() if key.path[0]=='publish']
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},
 jobs=[define_asset_job('cbrs_source',selection=AssetSelection.assets(fws_cbrs_units)),define_asset_job('cbrs_graph',selection=AssetSelection.assets(*normalkeys))])
report=json.loads((runtime/'execution.json').read_bytes()) if args.resume_after_integrity else {'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'module':source.__file__,'runtime':str(runtime),'approved':False,'runs':[]}
config={'ops':{fws_cbrs_units.op.name:{'config':{'capture_dir':str(capture)}}}}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,
  'materializations':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()],
  'checks':[{'name':e.check_name,'passed':e.passed} for e in result.get_asset_check_evaluations()]})
 (runtime/'execution.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
 return errors
with patch('dagster_hifld.assets.publish.process_layer_partitioned_geoparquet',side_effect=AssertionError('generic regeneration must never run for pinned native source')) as converter, DagsterInstance.get() as instance:
 sourcejob=normal.resolve_job_def('fws_cbrs_units_draft_refresh');graph=defs.resolve_job_def('cbrs_graph')
 if not args.resume_after_integrity:
  first=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
  record('real_complete_source',first);assert first.success
  output=first.output_for_node(fws_cbrs_units.op.name);assert output['feature_count']==942
  prefix=build_publish_partition_key(source.SLUG,source.SLUG,output['version']);root=runtime/'staging'/prefix
  assert (source.SLUG,source.SLUG,output['version']) in list(_iter_staged_version_paths(staging))
  assert source.sha((root/'raw'/source.ARCHIVE_NAME).read_bytes())==source.PIN
  before={str(p.relative_to(root)):source.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
  # A policy reason/control change cannot self-authorize through new ready hashes.
  manifestfile=root/'metadata/source_manifest.json';manifest_original=manifestfile.read_bytes()
  readyfile=root/'metadata/cbrs_units_candidate_ready.json';ready_original=readyfile.read_bytes()
  policy=json.loads(manifest_original);policy['prebuilt_output_policy']['formats']['geoparquet']['reason']='Unreviewed replacement capability claim'
  manifestfile.write_bytes(source.encode(policy));ready=json.loads(ready_original)
  ready['objects']['metadata/source_manifest.json']=source.sha(manifestfile.read_bytes());readyfile.write_bytes(source.encode(ready))
  attacked=source.candidate_inventory(root)
  control=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
  control_errors=record('complete_source_cache_self_repin_prebuilt_policy_control',control)
  assert not control.success and any('cached_original_construction_manifest_or_receipts_changed' in e for e in control_errors)
  assert not root.exists() and (source.SLUG,source.SLUG,output['version']) not in list(_iter_staged_version_paths(staging))
  retained=list((runtime/'staging/_quarantine'/source.SLUG/'versions').glob('*/candidate'))
  assert len(retained)==1 and source.candidate_inventory(retained[0])==attacked
  os.rename(retained[0],root);manifestfile.write_bytes(manifest_original);readyfile.write_bytes(ready_original)
  assert snapshot(published)==prior
  report['prebuilt_policy_control_self_repin']={'run_id':control.run_id,'rejected':True,'not_discoverable':True,'all_remaining_bytes_preserved':True,'test_fixture_explicitly_restored':True}
  again=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
  record('real_complete_refresh',again);assert again.success and output==again.output_for_node(fws_cbrs_units.op.name)
  assert before=={str(p.relative_to(root)):source.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
  instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
  formats=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
  errors=record('normal_formats_missing_approval',formats)
  assert not formats.success and errors and all('missing_independent_release_approval_registry' in error for error in errors),errors
  assert snapshot(published)==prior
  assert converter.call_count==0
  positive_policy=json.loads((root/'metadata/source_manifest.json').read_bytes())['prebuilt_output_policy']
  assert positive_policy==json.loads((root/'metadata/data_dictionary.json').read_bytes())['prebuilt_output_policy']==json.loads((root/'metadata/resolved_provenance.json').read_bytes())['prebuilt_output_policy']
  expected_object=positive_policy['formats']['geoparquet']['objects'][0]
  assert source.sha((root/expected_object['path']).read_bytes())==expected_object['sha256']
  report['prebuilt_capability']={'policy':positive_policy,'dictionary_and_resolved_provenance_exact':True,'generic_converter_calls':converter.call_count,'native_artifact_sha256':expected_object['sha256']}
  post=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
  record('real_complete_post_formats_refresh',post);assert post.success and post.output_for_node(fws_cbrs_units.op.name)==output
  assert snapshot(published)==prior
  # The second normal catalog snapshots its previously generated dictionary/quality.
  second_formats=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
  second_errors=record('second_normal_formats_missing_approval',second_formats)
  assert not second_formats.success and second_errors and all('missing_independent_release_approval_registry' in e for e in second_errors)
  assert (root/'metadata/source/data_dictionary.json').exists() and (root/'metadata/source/quality_manifest.json').exists()
  second_refresh=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
  record('real_complete_post_second_catalog_refresh',second_refresh)
  assert second_refresh.success and second_refresh.output_for_node(fws_cbrs_units.op.name)==output
  assert snapshot(published)==prior
  # Bind all original construction claims and capture receipts outside mutable
  # candidates. Re-pin ready/snapshots just as the independent reviewer did.
  manifestfile=root/'metadata/source_manifest.json';readyfile=root/'metadata/cbrs_units_candidate_ready.json'
  binding=runtime/'staging/_source-provenance'/source.SLUG/output['version']/'bundle.json'
  binding_original=binding.read_bytes();report['construction_binding_probes']=[]
  for attack in ['forged_code_commit','forged_original_receipt','missing_external_binding']:
   saved={str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file() and (p.suffix=='.json')}
   if attack=='missing_external_binding':binding.unlink()
   else:
    value=json.loads(manifestfile.read_bytes())
    if attack=='forged_code_commit':value['code_commit']='0'*40
    else:
     receiptfile=root/'raw/captures/official_zip.receipt.json';receipt=json.loads(receiptfile.read_bytes());receipt['observed_at']='2020-01-01T00:00:00+00:00';receiptfile.write_bytes(source.encode(receipt))
     value['inputs'][0]['retrieved_at']=receipt['observed_at']
    manifestfile.write_bytes(source.encode(value))
    # Keep all existing snapshot names but alter their bytes too; the external
    # binding, not a candidate-local snapshot or ready self-pin, must reject.
    for snapshotfile in (root/'metadata/source').glob('*source_manifest.json'):
     snapshotfile.write_bytes(manifestfile.read_bytes())
     if snapshotfile.name.startswith('version_'):snapshotfile.rename(snapshotfile.with_name('version_'+source.sha(manifestfile.read_bytes())+'_source_manifest.json'))
    ready=json.loads(readyfile.read_bytes())
    for name in ready['objects']:ready['objects'][name]=source.sha((root/name).read_bytes())
    readyfile.write_bytes(source.encode(ready))
   attacked=source.candidate_inventory(root)
   failed=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
   errors=record('construction_binding_'+attack,failed)
   assert not failed.success and any(('cached_original_construction_manifest_or_receipts_changed' if attack!='missing_external_binding' else 'missing_original_construction_provenance_bundle') in e for e in errors),errors
   assert not root.exists() and (source.SLUG,source.SLUG,output['version']) not in list(_iter_staged_version_paths(staging))
   retained=[p for p in (runtime/'staging/_quarantine'/source.SLUG/'versions').glob('*/candidate') if p.exists()]
   assert len(retained)==1 and source.candidate_inventory(retained[0])==attacked
   assert snapshot(published)==prior
   report['construction_binding_probes'].append({'attack':attack,'run_id':failed.run_id,'rejected':True,'all_remaining_bytes_preserved':True,'prior_published_preserved':True,'not_discoverable':True})
   os.rename(retained[0],root)
   for file in root.rglob('*.json'):
    if str(file.relative_to(root)) not in saved:file.unlink()
   for name,body in saved.items():(root/name).write_bytes(body)
   binding.write_bytes(binding_original)
  restored=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
  record('real_refresh_after_construction_binding_attacks',restored);assert restored.success
  assert restored.output_for_node(fws_cbrs_units.op.name)==output
  rows=pq.read_table(root/'candidate/typed-source.parquet').to_pylist();assert len(rows)==942
  with fiona.open(root/'geopackage'/f'{source.SLUG}.gpkg') as f:gp={r['properties']['__source_fid']:r for r in f}
  for row in rows:
   expected=gp[row['__source_fid']];assert dict(expected['properties'])=={key:row[key].isoformat() if hasattr(row[key],'isoformat') else row[key] for key in source.SCHEMA}
   assert shapely.geometry.shape(expected.geometry).wkb==row['geometry']
  terminal=pq.read_table(root/'geoparquet'/f'{source.SLUG}.parquet')
  assert len(terminal)==942
  for name,kind in source.SCHEMA.items():
   assert str(terminal.schema.field(name).type)==('double' if kind.startswith('float') else 'date32[day]' if kind=='date' else 'string')
  assert set(terminal.column_names)-set(source.SCHEMA)-{'geometry'}==set()
  bysection={row['__source_fid']:row for row in rows}
  for row in terminal.to_pylist():
   expected=bysection[row['__source_fid']]
   assert {key:row[key] for key in source.SCHEMA}=={key:expected[key] for key in source.SCHEMA}
   assert row['geometry']==expected['geometry']
  assert bysection['679']['Name']==' Brigantine' and bysection['680']['Name']==' Island Beach'
  report['terminal_parquet']={'preserved_original_leading_names':{'679':' Brigantine','680':' Island Beach'},'rows':942,'all_source_values_types_wkb_exact':True,'extra_columns':[],'nullable_inference_requires_review':True}
  # Authentic complete-candidate self-repin attacks exercise the actual registered Dagster asset.
  readyfile=root/'metadata/cbrs_units_candidate_ready.json';ready_original=readyfile.read_bytes()
  evidence_original={n:(root/'metadata'/n).read_bytes() for n in ['geopackage_source_fidelity.json','baseline_comparison.json']}
  gpfile=root/'geopackage'/f'{source.SLUG}.gpkg';gp_original=gpfile.read_bytes()
  parquet=root/'candidate/typed-source.parquet';parquet_original=parquet.read_bytes()
  report['self_repin_probes']=[]
  for attack in ['gpkg_contents_srs','gpkg_spatial_ref_definition','gpkg_geometry_envelope','gpkg_rtree','parquet_geo_crs','fidelity_population','baseline_approval','missing_ready']:
   try:
    if attack in ['gpkg_contents_srs','gpkg_spatial_ref_definition','gpkg_rtree']:
     with sqlite3.connect(gpfile) as db:
      if attack=='gpkg_rtree':db.execute('UPDATE "rtree_'+source.SLUG+'_geom" SET maxx=0 WHERE id=1')
      else:db.execute("UPDATE gpkg_contents SET srs_id=4326" if attack=='gpkg_contents_srs' else "UPDATE gpkg_spatial_ref_sys SET definition='undefined' WHERE srs_id=4269")
     target=gpfile
    elif attack=='gpkg_geometry_envelope':
     with sqlite3.connect(gpfile) as db:
      for name in ['ST_IsEmpty','ST_MinX','ST_MaxX','ST_MinY','ST_MaxY']:db.create_function(name,1,lambda x:0)
      fid,blob=db.execute('SELECT fid,geom FROM "'+source.SLUG+'" LIMIT 1').fetchone()
      import struct
      assert (blob[3]>>1)&7
      altered=blob[:8]+struct.pack('<d' if blob[3]&1 else '>d',0.0)+blob[16:]
      db.execute('UPDATE "'+source.SLUG+'" SET geom=? WHERE fid=?',(altered,fid))
     target=gpfile
    elif attack=='parquet_geo_crs':
     table=pq.read_table(parquet);metadata=dict(table.schema.metadata);geo=json.loads(metadata[b'geo']);geo['columns']['geometry']['crs']={'type':'name','properties':{'name':'EPSG:3857'}}
     metadata[b'geo']=source.encode(geo);pq.write_table(table.replace_schema_metadata(metadata),parquet,compression='zstd');target=parquet
    elif attack=='fidelity_population':
     target=root/'metadata/geopackage_source_fidelity.json';value=json.loads(target.read_bytes())
     value['shared_verifier']['populations']['baseline']['rows']=1;value['shared_verifier']['retained_rows_compared']=1
     target.write_bytes(source.encode(value))
    elif attack=='baseline_approval':
     target=root/'metadata/baseline_comparison.json';target.write_bytes(source.encode({'schema_version':1,'status':'pass','approved':True,'acceptance':'approved'}))
    else:target=readyfile
    if attack=='missing_ready':readyfile.unlink()
    else:
     ready=json.loads(ready_original);ready['objects'][str(target.relative_to(root))]=source.sha(target.read_bytes());readyfile.write_bytes(source.encode(ready))
    attacked_inventory=source.candidate_inventory(root)
    negative=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
    errors=record('complete_source_cache_self_repin_'+attack,negative)
    assert not negative.success and errors
    assert not root.exists() and (source.SLUG,source.SLUG,output['version']) not in list(_iter_staged_version_paths(staging))
    retained=[p.parent for p in (runtime/'staging/_quarantine'/source.SLUG/'versions').glob('*/report.json') if json.loads(p.read_bytes())['original_prefix']==prefix and (p.parent/'candidate').exists()]
    assert len(retained)==1
    assert source.candidate_inventory(retained[0]/'candidate')==attacked_inventory
    assert json.loads((retained[0]/'report.json').read_bytes())['inventory']==attacked_inventory
    assert snapshot(published)==prior
    report['self_repin_probes'].append({'attack':attack,'run_id':negative.run_id,'rejected':True,'prior_published_preserved':True,'not_discoverable':True,'whole_remaining_candidate_bytes_preserved':True})
   finally:
    if not root.exists():
     retained=[p for p in (runtime/'staging/_quarantine'/source.SLUG/'versions').glob('*/candidate') if p.exists()]
     assert len(retained)==1;os.rename(retained[0],root)
    gpfile.write_bytes(gp_original);parquet.write_bytes(parquet_original);readyfile.write_bytes(ready_original)
    for name,body in evidence_original.items():(root/'metadata'/name).write_bytes(body)
  assert all(source.sha((root/key).read_bytes())==digest for key,digest in json.loads(ready_original)['objects'].items())
 else:
  # Continue only the captured complete negative phase. Do not rewrite outputs,
  # replay terminal formats or reset the existing published preservation fixture.
  attacks=[r for r in report['runs'] if r['kind'].startswith('complete_source_cache_self_repin_')]
  assert len(attacks)==8 and all(not r['success'] for r in attacks)
  assert len(report['self_repin_probes'])==7 and attacks[-1]['kind'].endswith('missing_ready')
  refreshed=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
  record('continuation_revalidate_restored_complete_source',refreshed);assert refreshed.success
  output=refreshed.output_for_node(fws_cbrs_units.op.name);assert output['feature_count']==942
  prefix=build_publish_partition_key(source.SLUG,source.SLUG,output['version']);root=runtime/'staging'/prefix
  assert (source.SLUG,source.SLUG,output['version']) in list(_iter_staged_version_paths(staging))
  remaining=source.candidate_inventory(root);remaining.pop('metadata/cbrs_units_candidate_ready.json')
  missing=[p for p in (runtime/'staging/_quarantine'/source.SLUG/'versions').glob('*/report.json')
           if json.loads(p.read_bytes())['reason']=='incomplete_existing_candidate_missing_ready']
  assert len(missing)==1 and json.loads(missing[0].read_bytes())['inventory']==remaining
  report['self_repin_probes'].append({'attack':'missing_ready','run_id':attacks[-1]['run_id'],
    'rejected':True,'not_discoverable':True,'whole_remaining_candidate_bytes_preserved':True,
    'prior_published_preserved':True,'test_candidate_explicitly_restored_and_freshly_revalidated':True})
  ready=json.loads((root/'metadata/cbrs_units_candidate_ready.json').read_bytes())
  assert all(source.sha((root/key).read_bytes())==digest for key,digest in ready['objects'].items())
  assert snapshot(published)==prior
  report['harness_continuation']={'reason':'Previous assertion incorrectly rejected a valid zero-byte differences sidecar after all eight attacks completed. SHA validation supports empty files.',
    'source_or_data_writer_change':False,'prior_runs_preserved':True,'terminal_formats_rebuilt':False}
 # Exercise the actual normal writer under native-object drift, without source
 # regeneration or re-signing controls. All data remain explicitly local fixtures.
 driftprefix=prefix+'-synthetic-prebuilt-drift';driftroot=runtime/'staging'/driftprefix
 shutil.copytree(root,driftroot);driftfile=driftroot/'geoparquet'/f'{source.SLUG}.parquet'
 drift_original=driftfile.read_bytes();instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[driftprefix])
 report['prebuilt_normal_drift_probes']=[]
 for attack in ['missing','extra','changed']:
  try:
   if attack=='missing':driftfile.unlink()
   elif attack=='extra':(driftfile.parent/'unexpected.parquet').write_bytes(drift_original)
   else:
    table=pq.read_table(driftfile);column=table.column_names.index('Name');names=table.column('Name').to_pylist();names[0]='Explicit synthetic mutation'
    pq.write_table(table.set_column(column,table.schema.field('Name'),pa.array(names,type=pa.string())),driftfile,compression='zstd')
   failed=graph.execute_in_process(instance=instance,partition_key=driftprefix,raise_on_error=False)
   errors=record('synthetic_prebuilt_normal_writer_'+attack,failed)
   assert not failed.success and any('Prebuilt output' in error for error in errors),errors
   assert converter.call_count==0 and snapshot(published)==prior
   report['prebuilt_normal_drift_probes'].append({'attack':attack,'run_id':failed.run_id,'rejected':True,'generic_converter_calls':0,'prior_published_fixture_preserved':True})
  finally:
   driftfile.write_bytes(drift_original);(driftfile.parent/'unexpected.parquet').unlink(missing_ok=True)
 # Corrupt a separate explicitly synthetic test partition, leaving the real candidate unchanged.
 badprefix=prefix+'-synthetic-invalid-geometry';badroot=runtime/'staging'/badprefix;shutil.copytree(root,badroot)
 gpfile=badroot/'geopackage'/f'{source.SLUG}.gpkg'
 with fiona.open(gpfile) as f:schema=f.schema;crs=f.crs;features=[{'properties':dict(r['properties']),'geometry':r.geometry} for r in f]
 features[0]['geometry']=None;gpfile.unlink()
 with fiona.open(gpfile,'w',driver='GPKG',layer=source.SLUG,schema=schema,crs=crs) as f:f.writerecords(features)
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix])
 bad=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False)
 record('synthetic_failed_geometry_quality_gate',bad);assert not bad.success
 assert any(not e.passed for e in bad.get_asset_check_evaluations())
 assert snapshot(published)==prior
 assert (root/'candidate/typed-source.parquet').read_bytes()==(root/'geoparquet'/f'{source.SLUG}.parquet').read_bytes()
 assert converter.call_count==0
 report['generic_converter_calls']=converter.call_count
 report.update(output=output,all_942_ids_values_types_wkb_preserved=True,source_discoverable_without_manual_copy=True,refresh_byte_idempotent=True,prior_published_fixture_preserved=True,
  quality=json.loads((root/'metadata/quality_manifest.json').read_text()),provenance_input_count=len(json.loads((root/'metadata/source_manifest.json').read_text())['inputs']))
 if args.live_followup:
  live=sourcejob.execute_in_process(instance=instance,run_config={'ops':{fws_cbrs_units.op.name:{'config':{'capture_dir':''}}}},raise_on_error=False)
  errors=record('live_all_official_inputs_source_refresh',live);assert live.success,errors
  liveoutput=live.output_for_node(fws_cbrs_units.op.name);assert liveoutput['feature_count']==942
  liveroot=runtime/'staging'/source.SLUG/source.SLUG/liveoutput['version']
  liveparquet=pq.read_table(liveroot/'geoparquet'/f'{source.SLUG}.parquet')
  assert liveparquet.equals(pq.read_table(root/'geoparquet'/f'{source.SLUG}.parquet'),check_metadata=True)
  observations={name:json.loads((liveroot/f'raw/captures/{name}.receipt.json').read_bytes()) for name in source.URLS}
  assert all(value['status']==200 for value in observations.values())
  assert (runtime/'staging/_source-provenance'/source.SLUG/liveoutput['version']/'bundle.json').is_file()
  assert snapshot(published)==prior and converter.call_count==0
  report['live_followup']={'run_id':live.run_id,'output':liveoutput,'observations':observations,'all_942_corrected_values_types_native_wkb_equal':True,'new_observations_preserved':True,'prior_published_preserved':True}
(runtime/'execution.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'code_head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']]}))
