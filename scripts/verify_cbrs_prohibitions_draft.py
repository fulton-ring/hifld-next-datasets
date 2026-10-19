"""Actual complete-source local Dagster graph; no real acceptance is invented."""
from __future__ import annotations
import argparse
import json
import os
import shutil
import sqlite3
import struct
import subprocess
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser()
parser.add_argument('--construction-only',action='store_true',help='Reuse historical container negatives; run the new construction-binding probes instead.')
parser.add_argument('--runtime',type=Path,required=True)
parser.add_argument('--archive-helper',type=Path,help='Optional guarded lossless archive helper for closed bad-cache trees.')
args=parser.parse_args();repo=Path(__file__).resolve().parents[1];capture=repo/'docs/dataset-sources/cbrs-prohibitions-1/evidence/captures'
runtime=args.runtime.resolve();assert not runtime.exists()
for name in ['dagster','staging','published']:(runtime/name).mkdir(parents=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
import fiona
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.fws_prohibitions import fws_cbrs_prohibitions
from dagster_hifld.assets import publish as publisher
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME,build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.sources import cbrs_prohibitions as source

staging=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True)
published=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
published.write_key('synthetic-accepted-fixture.bin',b'local-fixture-only-not-an-accepted-real-release')
def snapshot(storage):return {key:source.sha(storage.read_key(key)) for key in storage.list_prefix()}
prior=snapshot(published)
normalkeys=[key for key in normal.resolve_asset_graph().get_all_asset_keys() if key.path[0]=='publish']
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('cbrs_source',selection=AssetSelection.assets(fws_cbrs_prohibitions)),define_asset_job('cbrs_graph',selection=AssetSelection.assets(*normalkeys))])
report={'code_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'module':source.__file__,'runtime':str(runtime),'approved':False,'runs':[],'cache_negatives':{}}
config={'ops':{fws_cbrs_prohibitions.op.name:{'config':{'capture_dir':str(capture)}}}}
def save():(runtime/'execution.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors,'materializations':[e.asset_key.to_user_string() for e in result.get_asset_materialization_events()],'checks':[{'name':e.check_name,'passed':e.passed} for e in result.get_asset_check_evaluations()]});save();return errors

def forbid_generic(stack):
 return [stack.enter_context(patch.object(publisher,name,side_effect=AssertionError('Generic regeneration forbidden for native DATE source'))) for name in ['process_layer_partitioned_geoparquet','process_layer_chunked','_validate_reusable_geoparquet_layout']]

with DagsterInstance.get() as instance,ExitStack() as guards:
 generic=forbid_generic(guards);sourcejob=defs.resolve_job_def('cbrs_source');graph=defs.resolve_job_def('cbrs_graph')
 first=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('real_complete_source',first);assert first.success
 output=first.output_for_node(fws_cbrs_prohibitions.op.name);assert output['feature_count']==1693
 prefix=build_publish_partition_key(source.SLUG,source.SLUG,output['version']);root=runtime/'staging'/prefix;report['output']=output
 assert (source.SLUG,source.SLUG,output['version']) in list(_iter_staged_version_paths(staging))
 assert source.sha((root/'raw'/source.ARCHIVE_NAME).read_bytes())==source.PIN
 raw_before=source.tree_inventory(runtime/'staging/_raw-captures');before=source.tree_inventory(root)
 again=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('real_complete_refresh',again);assert again.success and output==again.output_for_node(fws_cbrs_prohibitions.op.name);assert before==source.tree_inventory(root)
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
 for cycle in [1,2]:
  formats=graph.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);errors=record(f'normal_catalog_cycle_{cycle}_missing_approval',formats)
  assert not formats.success and errors and all('missing_independent_release_approval_registry' in e for e in errors),errors
  gp_events=[e.event_specific_data.materialization.metadata for e in formats.get_asset_materialization_events() if e.asset_key.path==['publish','formats','geoparquet']]
  assert gp_events and gp_events[0]['generation_mode'].value=='preserved_prebuilt' and gp_events[0]['generic_regeneration'].value=='disabled'
  assert snapshot(published)==prior
  refresh=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record(f'complete_source_refresh_after_catalog_cycle_{cycle}',refresh);assert refresh.success and refresh.output_for_node(fws_cbrs_prohibitions.op.name)==output
  assert snapshot(published)==prior
 assert (root/'metadata/source/data_dictionary.json').exists() and (root/'metadata/source/quality_manifest.json').exists()
 native=root/'geoparquet'/f'{source.SLUG}.parquet';terminal=pq.read_table(native);rows=pq.read_table(root/'candidate/typed-source.parquet').to_pylist();assert len(rows)==len(terminal)==1693
 assert native.read_bytes()==(root/'candidate/typed-source.parquet').read_bytes()
 with fiona.open(root/'geopackage'/f'{source.SLUG}.gpkg') as f:gp={r['properties']['__source_fid']:r for r in f}
 byfid={r['__source_fid']:r for r in rows};assert set(byfid)==set(gp)=={str(i) for i in range(1693)}
 for row in terminal.to_pylist():
  expected=byfid[row['__source_fid']];assert {k:row[k] for k in source.SCHEMA}=={k:expected[k] for k in source.SCHEMA} and row['geometry']==expected['geometry']
  feature=gp[row['__source_fid']];assert dict(feature['properties'])=={k:row[k].isoformat() if hasattr(row[k],'isoformat') else row[k] for k in source.SCHEMA};assert shapely.geometry.shape(feature.geometry).wkb==row['geometry']
 for name,kind in source.SCHEMA.items():assert str(terminal.schema.field(name).type)==('double' if kind.startswith('float') else 'date32[day]' if kind=='date' else 'string')
 assert terminal.column('SU_Date').null_count==564 and terminal.column('FI_Date').null_count==0
 assert set(terminal.column_names)==set(source.SCHEMA)|{'geometry'}
 manifest=json.loads((root/'metadata/source_manifest.json').read_text());policy=manifest['prebuilt_output_policy'];assert 'geoparquet' not in manifest['unsupported_output_formats']
 assert policy['formats']['geoparquet']['objects']==[{'path':f'geoparquet/{source.SLUG}.parquet','sha256':source.sha(native.read_bytes()),'bytes':native.stat().st_size}]
 assert json.loads((root/'metadata/data_dictionary.json').read_text())['prebuilt_output_policy']==policy
 assert json.loads((root/'metadata/resolved_provenance.json').read_text())['prebuilt_output_policy']==policy
 report['native_prebuilt']={'rows':1693,'all_original_ids_values_physical_dates_types_nulls_native_WKB_exact':True,'source_fid_type':'string original shapefile intrinsic FID proxy; no invented OBJECTID or published-ID mapping','native_date32_columns':['FI_Date','SU_Date'],'SU_Date_nulls':564,'sha256':source.sha(native.read_bytes()),'policy':policy,'generic_regeneration_calls':0}
 seed=runtime/'immutable-post-catalog-seed';shutil.copytree(root,seed);good=source.tree_inventory(seed)
 modes={'false_fidelity_report':'cached_complete_source_fidelity_evidence_drift:metadata/geopackage_source_fidelity.json','false_accepted_placeholder':'cached_complete_source_fidelity_evidence_drift:metadata/baseline_comparison.json','unknown_SQL_identifiers':'cached_geopackage_auxiliary_metadata_or_header_drift:complete_container_bytes','geometry_envelope':'cached_geopackage_auxiliary_metadata_or_header_drift:complete_container_bytes','contents_SRS':'cached_geopackage_auxiliary_metadata_or_header_drift:complete_container_bytes','missing_ready_marker':'existing_candidate_missing_ready_marker'}
 if args.construction_only:modes={}
 modes.update({'forged_construction_commit':'cached_provenance_differs_from_original_construction_bundle','missing_construction_bundle':'missing_original_construction_bundle','forged_external_construction_commit':'original_construction_commit_not_in_repository'})
 bundle_path=runtime/'staging'/source.construction_bundle_key(output['version']);bundle_original=bundle_path.read_bytes()
 report['original_construction_bundle']={'path':str(bundle_path),'sha256':source.sha(bundle_original),'manifest':json.loads(bundle_original)['manifest']};save()
 for mode,expected_reason in modes.items():
  marker=root/'metadata/cbrs_prohibitions_candidate_ready.json';ready=json.loads(marker.read_text());gpfile=root/'geopackage'/f'{source.SLUG}.gpkg';changed=[]
  if mode=='forged_construction_commit':
   target=root/'metadata/source_manifest.json';doc=json.loads(target.read_text());doc['code_commit']='0'*40;target.write_bytes(source.encode(doc));changed=[target]
  elif mode=='missing_construction_bundle':bundle_path.unlink()
  elif mode=='forged_external_construction_commit':
   doc=json.loads(bundle_original);doc['manifest']['code_commit']='0'*40;bundle_path.write_bytes(source.encode(doc))
  elif mode=='false_fidelity_report':
   target=root/'metadata/geopackage_source_fidelity.json';doc=json.loads(target.read_text());doc['shared_verifier']['populations']['candidate']['rows']=1;doc['shared_verifier']['baseline']['approved']=True;target.write_bytes(source.encode(doc));changed=[target]
  elif mode=='false_accepted_placeholder':
   target=root/'metadata/baseline_comparison.json';doc=json.loads(target.read_text());doc.update(status='pass',approved=True,acceptance='approved');target.write_bytes(source.encode(doc));changed=[target]
  elif mode=='unknown_SQL_identifiers':
   with sqlite3.connect(gpfile) as db:db.execute('CREATE TABLE unapproved_extra_identifiers (OBJECTID INTEGER)');db.execute('INSERT INTO unapproved_extra_identifiers VALUES (90000001)')
   db.close();changed=[gpfile]
  elif mode=='geometry_envelope':
   with sqlite3.connect(gpfile) as db:blob=db.execute('SELECT geom FROM "'+source.SLUG+'" ORDER BY fid LIMIT 1').fetchone()[0]
   db.close();assert ((blob[3]>>1)&7)==1;data=bytearray(gpfile.read_bytes());offset=data.find(blob[:64]);assert offset>=0;data[offset+8:offset+16]=struct.pack('<d' if blob[3]&1 else '>d',0.0);gpfile.write_bytes(data);changed=[gpfile]
  elif mode=='contents_SRS':
   with sqlite3.connect(gpfile) as db:db.execute('UPDATE gpkg_contents SET srs_id=4326')
   db.close();changed=[gpfile]
  elif mode=='missing_ready_marker':marker.unlink()
  if mode!='missing_ready_marker':
   for file in changed:ready['objects'][str(file.relative_to(root))]=source.sha(file.read_bytes())
   marker.write_bytes(source.encode(ready))
  bad_inventory=source.tree_inventory(root);qroot=runtime/'staging/_quarantine'/source.SLUG;before_quarantine=set(qroot.glob('invalid-cache-*')) if qroot.exists() else set()
  failed=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False);errors=record('actual_complete_cache_'+mode,failed);assert not failed.success and any(expected_reason in e for e in errors)
  created=set(qroot.glob('invalid-cache-*'))-before_quarantine;assert len(created)==1;quarantine=created.pop();qr=json.loads((quarantine/'report.json').read_text());assert qr['reason']==expected_reason and qr['objects']==bad_inventory and source.tree_inventory(quarantine/'candidate')==bad_inventory
  assert not root.exists() and (source.SLUG,source.SLUG,output['version']) not in list(_iter_staged_version_paths(staging));assert snapshot(published)==prior and source.tree_inventory(runtime/'staging/_raw-captures')==raw_before
  archive_manifest=None
  if args.archive_helper:
   bundle='cbrs30-policy-'+mode.replace('_','-')+'-'+failed.run_id[:8]
   subprocess.run(['python',str(args.archive_helper),'--bundle',bundle,'--parent',str(quarantine),'candidate'],check=True)
   archive_manifest=json.loads((quarantine/'candidate.archived.json').read_text())['manifest']
  report['cache_negatives'][mode]={'run_id':failed.run_id,'expected_reason':expected_reason,'whole_bad_inventory':bad_inventory,'quarantine_report':str(quarantine/'report.json'),'archive_manifest':archive_manifest,'not_discoverable':True,'prior_and_raw_preserved':True};save();bundle_path.write_bytes(bundle_original);shutil.copytree(seed,root)
 restored=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('restored_complete_source_after_forensic_negatives',restored);assert restored.success and source.tree_inventory(root)==good
 saved=runtime/'closed-original-for-rebuild-proof';os.rename(root,saved)
 rebuilt=sourcejob.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('fresh_rebuild_preserves_first_construction_bundle',rebuilt);assert rebuilt.success
 assert bundle_path.read_bytes()==bundle_original
 original_ready=json.loads((saved/'metadata/cbrs_prohibitions_candidate_ready.json').read_text());rebuilt_ready=json.loads((root/'metadata/cbrs_prohibitions_candidate_ready.json').read_text());assert rebuilt_ready==original_ready
 assert all((root/name).read_bytes()==(saved/name).read_bytes() for name in original_ready['objects'])
 report['fresh_rebuild_preserves_original_manifest_receipts_native_bytes']=True;save();shutil.rmtree(root);os.rename(saved,root)
 # One explicitly synthetic partition exercises both policy and quality failures.
 badprefix=prefix+'-synthetic-negative-tests';badroot=runtime/'staging'/badprefix;shutil.copytree(root,badroot);instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[badprefix])
 badnative=badroot/'geoparquet'/f'{source.SLUG}.parquet';native_original=badnative.read_bytes();badnative.write_bytes(native_original+b'bad-staged-mutation')
 badpolicy=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);errors=record('synthetic_prebuilt_staged_mutation',badpolicy);assert not badpolicy.success and any('Prebuilt output bytes changed' in e for e in errors);assert snapshot(published)==prior;badnative.write_bytes(native_original)
 gpfile=badroot/'geopackage'/f'{source.SLUG}.gpkg'
 with fiona.open(gpfile) as f:schema=f.schema;crs=f.crs;features=[{'properties':dict(r['properties']),'geometry':r.geometry} for r in f]
 features[0]['geometry']=None;gpfile.unlink()
 with fiona.open(gpfile,'w',driver='GPKG',layer=source.SLUG,schema=schema,crs=crs) as f:f.writerecords(features)
 badquality=graph.execute_in_process(instance=instance,partition_key=badprefix,raise_on_error=False);record('synthetic_failed_geometry_quality',badquality);assert not badquality.success and any(not e.passed for e in badquality.get_asset_check_evaluations())
 assert all(mock.call_count==0 for mock in generic)
 assert snapshot(published)==prior and source.tree_inventory(runtime/'staging/_raw-captures')==raw_before
 assert source.tree_inventory(root)==good
 assert bundle_path.read_bytes()==bundle_original
 report.update(generic_converter_calls=0,all_real_source_rows_ids_schema_geometry_coverage_provenance_verified=True,prior_published_fixture_and_original_raw_preserved=True,two_normal_catalog_cycles_and_refresh_pass=True,quality=json.loads((root/'metadata/quality_manifest.json').read_text()),provenance_input_count=len(manifest['inputs']));save()
print(json.dumps({'code_head':report['code_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']],'generic_converter_calls':0}))
