"""Targeted actual complete-source guard repair verification; no production writes."""
import os,json,pathlib,shutil,sqlite3,subprocess,argparse
parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=pathlib.Path,required=True);args=parser.parse_args();runtime=args.runtime.resolve();assert not runtime.exists()
for n in ['dagster','staging','published']:(runtime/n).mkdir(parents=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for n in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(n,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import Definitions,AssetSelection,AssetKey,define_asset_job,DagsterInstance
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.assets.fws import fws_cbrs_buffer_zone as asset
from dagster_hifld.sources import cbrs_buffer as s
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
assert pathlib.Path(s.__file__).resolve().is_relative_to(pathlib.Path(__file__).resolve().parents[1]/'src')
repo=pathlib.Path(__file__).resolve().parents[1];capture=repo/'docs/dataset-sources/cbrs-buffer-zone-1/evidence/captures'
stage=StagingStorageResource(local_dir=str(runtime/'staging'),use_local=True);pub=PublishedStorageResource(local_dir=str(runtime/'published'),use_local=True)
pub.write_key('prior-local-fixture.bin',b'local-only fixture, no accepted real release claimed')
def inventory(root):return {str(p.relative_to(root)):s.sha256_file(p) for p in root.rglob('*') if p.is_file()}
prior=inventory(runtime/'published')
defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':stage,'published_storage':pub},jobs=[define_asset_job('repaired_source',selection=AssetSelection.assets(asset)),define_asset_job('repeated_catalog',selection=AssetSelection.keys(AssetKey(['publish','catalog'])))])
report={'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'module':s.__file__,'approved':False,'scope':'Targeted review repairs; prior full native physical/WKB/terminal proof unchanged.','runs':[],'rejected_caches':[]}
config={'ops':{asset.op.name:{'config':{'capture_dir':str(capture)}}}}
def record(kind,result):
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors});(runtime/'execution-in-progress.json').write_text(json.dumps(report,indent=2)+'\n');return errors
with DagsterInstance.get() as instance:
 job=defs.resolve_job_def('repaired_source');catjob=defs.resolve_job_def('repeated_catalog')
 source=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('complete_real_source',source);assert source.success
 output=source.output_for_node(asset.op.name);assert output['feature_count']==1145
 prefix=f'{s.SLUG}/{s.SLUG}/{output["version"]}';root=runtime/'staging'/prefix;gp=root/'geopackage'/f'{s.SLUG}.gpkg'
 assert s.sha256_file(gp)=='29edab0b125fc438930422b0f5ff395869d25fa4854077222632515423c79a56'
 assert s.sha256_file(root/'candidate/typed-source.parquet')==s.sha256_file(oldroot/'candidate/typed-source.parquet')
 instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
 for n in [1,2]:
  result=catjob.execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False);record('actual_catalog_'+str(n),result);assert result.success
 assert all((root/n).is_file() for n in ['metadata/source/data_dictionary.json','metadata/source/quality_manifest.json'])
 s._FIDELITY_PAYLOAD_CACHE.clear() # Independently replay all complete real-source evidence on cold reuse.
 before=inventory(root);reuse=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('reuse_after_second_actual_catalog',reuse);assert reuse.success and reuse.output_for_node(asset.op.name)==output and inventory(root)==before
 backup=runtime/'clean-full-candidate';shutil.copytree(root,backup)
 for mode in ['complete_report_and_wrappers_forgery','sidecar_only_forgery','missing_ready']:
  if not root.exists():shutil.copytree(backup,root)
  ready=root/'metadata/cbrs_buffer_candidate_ready.json'
  marker=json.loads(ready.read_text());changed=[]
  if mode=='missing_ready':ready.unlink()
  elif mode=='sidecar_only_forgery':
   name='metadata/source_fidelity/baseline_field_changes.jsonl';p=root/name;p.write_bytes(p.read_bytes()+b'{"field":"forged","retained_rows_compared":1,"changed":0}\n');changed.append(name)
  else:
   def forge(report):
    report['baseline']['approved']=True;report['populations']['baseline']['rows']=1;report['populations']['candidate']['rows']=1;report['retained_rows_compared']=1
   for name in ['metadata/source_fidelity/baseline_comparison.json','metadata/geopackage_source_fidelity.json','metadata/parquet_source_fidelity.json','metadata/baseline_comparison.json']:
    p=root/name;x=json.loads(p.read_text())
    if name.endswith('/baseline_comparison.json') and '/source_fidelity/' not in name:x.update(status='pass',approved=True,acceptance='approved')
    elif name.endswith('/geopackage_source_fidelity.json'):forge(x['shared_verifier'])
    elif name.endswith('/parquet_source_fidelity.json'):forge(x['shared_verifier']['shared_schema_geometry_validation_reference']);x['shared_verifier']['rows']=1
    else:forge(x)
    p.write_text(json.dumps(x));changed.append(name)
  if mode!='missing_ready':
   for name in changed:marker['objects'][name]=s.sha256_file(root/name)
   ready.write_text(json.dumps(marker))
  bad_inventory=inventory(root);before_q=set((runtime/'staging/_quarantine'/s.SLUG/'rejected-cache').glob('*')) if (runtime/'staging/_quarantine'/s.SLUG/'rejected-cache').exists() else set()
  result=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);errors=record('reject_self_repinned_'+mode,result);assert not result.success
  reason='incomplete_existing_candidate_missing_ready_receipt' if mode=='missing_ready' else 'cached_source_fidelity_evidence_disagrees_with_fresh_original'
  assert any(reason in e for e in errors),errors
  after_q=set((runtime/'staging/_quarantine'/s.SLUG/'rejected-cache').glob('*'));added=after_q-before_q;assert len(added)==1
  quarantined=next(iter(added))/root.name;assert inventory(quarantined)==bad_inventory
  assert not root.exists() and (s.SLUG,s.SLUG,output['version']) not in list(_iter_staged_version_paths(stage))
  assert s.sha256_file(quarantined/'raw'/s.ARCHIVE_NAME)==s.PIN and inventory(runtime/'published')==prior
  report['rejected_caches'].append({'mutation':mode,'reason':reason,'intact_location':str(quarantined),'complete_tree_pins_preserved':True,'files':len(bad_inventory),'no_normal_discovery_candidate':True,'rawZIP_preserved':True,'prior_published_fixture_preserved':True})
 shutil.copytree(backup,root)
 restored=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('restored_clean_full_candidate_reuse',restored);assert restored.success and inventory(root)==inventory(backup)
 assert inventory(runtime/'published')==prior
 report.update(output=output,second_catalog_and_refresh_success=True,complete_cold_process_evidence_rederivation_success=True,original_full_GPKG_and_typedParquet_payload_hashes_unchanged=True,prior_published_fixture_preserved=True,all_rejected_versions_preserved_intact_outside_discovery=True)
(runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'head':report['head'],'runs':[(x['kind'],x['run_id'],x['success']) for x in report['runs']]}))
