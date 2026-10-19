from pathlib import Path
import json,os,subprocess,shutil,sqlite3,struct
r=Path('/workspace/work/hifld-coordinator/30/repairs/cache-graph');resume=r.exists();r.mkdir(exist_ok=True)
for n in ['dagster','staging','published']:(r/n).mkdir(exist_ok=True)
(r/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for n in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS']:os.environ.pop(n,None)
os.environ.update(DAGSTER_HOME=str(r/'dagster'),HIFLD_STAGING_DIR=str(r/'staging'),HIFLD_DATASETS_DIR=str(r/'published'))
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld.sources import cbrs_prohibitions as s
from dagster_hifld.assets.fws_prohibitions import fws_cbrs_prohibitions
from dagster_hifld.definitions import defs as normal,_iter_staged_version_paths
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
repo=Path(s.__file__).parents[3];head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert head=='c84febb6bc6c6d047d2b0e6cc85f82c131e64e80'
staging=StagingStorageResource(local_dir=str(r/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(r/'published'),use_local=True);published.write_key('synthetic-accepted-fixture.bin',b'local fixture only; no accepted real data') if not published.object_exists('synthetic-accepted-fixture.bin') else None
prior=s.tree_inventory(r/'published');defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':staging,'published_storage':published},jobs=[define_asset_job('source30_cache_repair',selection=AssetSelection.assets(fws_cbrs_prohibitions))]);capture=repo/'docs/dataset-sources/cbrs-prohibitions-1/evidence/captures';config={'ops':{fws_cbrs_prohibitions.op.name:{'config':{'capture_dir':str(capture)}}}};report=json.loads((r/'cache-repair.json').read_text()) if resume else {'head':head,'approved':False,'runs':[],'probes':{}}
def save():(r/'cache-repair.json').write_text(json.dumps(report,indent=2)+'\n')
def record(kind,result):
 row={'kind':kind,'run_id':result.run_id,'success':result.success,'errors':[x.event_specific_data.error.to_string() for x in result.get_step_failure_events()]};report['runs'].append(row);save();return row
with DagsterInstance.get() as instance:
 job=defs.resolve_job_def('source30_cache_repair')
 if not resume:
  first=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('cold_real_1693_source',first);assert first.success
  output=first.output_for_node(fws_cbrs_prohibitions.op.name);assert output['feature_count']==1693;version=output['version'];target=r/'staging'/s.SLUG/s.SLUG/version;seed=r/'immutable-good-seed';shutil.copytree(target,seed)
  good=s.tree_inventory(seed);raw=s.tree_inventory(r/'staging/_raw-captures');report['output']=output;report['original_good_inventory']=good;save()
 else:
  output=report['output'];version=output['version'];target=r/'staging'/s.SLUG/s.SLUG/version;seed=r/'immutable-good-seed';good=report['original_good_inventory'];raw=s.tree_inventory(r/'staging/_raw-captures')
  assert s.tree_inventory(seed)==good and s.tree_inventory(target)==good
 for mode in ['false_fidelity_report','false_accepted_placeholder','false_shared_report','false_markdown','unknown_SQL_identifiers','geometry_envelope','contents_SRS','missing_ready_marker','unexpected_identifier_file']:
  if mode in report['probes']:continue
  marker=target/'metadata/cbrs_prohibitions_candidate_ready.json';gp=target/'geopackage'/f'{s.SLUG}.gpkg';ready=json.loads(marker.read_text());changed=[]
  if mode=='false_fidelity_report':
   file=target/'metadata/geopackage_source_fidelity.json';j=json.loads(file.read_text())
   def alter(x):
    if isinstance(x,dict):return {k:(True if k=='approved' else alter(v)) for k,v in x.items()}
    if isinstance(x,list):return [alter(v) for v in x]
    return 1 if type(x) is int and x==1693 else x
   file.write_bytes(s.encode(alter(j)));changed=[file]
  elif mode=='false_accepted_placeholder':
   file=target/'metadata/baseline_comparison.json';j=json.loads(file.read_text());j.update(status='pass',approved=True,acceptance='approved');file.write_bytes(s.encode(j));changed=[file]
  elif mode=='false_shared_report':
   file=target/'metadata/source-fidelity/baseline_comparison.json';j=json.loads(file.read_text());j['populations']['candidate']['rows']=1;file.write_bytes(s.encode(j));changed=[file]
  elif mode=='false_markdown':
   file=target/'metadata/source-fidelity/baseline_comparison.md';file.write_bytes(file.read_bytes()+b'\nFALSE source acceptance claim\n');changed=[file]
  elif mode=='unknown_SQL_identifiers':
   with sqlite3.connect(gp) as db:db.execute('CREATE TABLE unapproved_extra_identifiers (OBJECTID INTEGER)');db.execute('INSERT INTO unapproved_extra_identifiers VALUES (90000001)')
   db.close()
   changed=[gp]
  elif mode=='geometry_envelope':
   with sqlite3.connect(gp) as db:blob=db.execute('SELECT geom FROM "'+s.SLUG+'" ORDER BY fid LIMIT 1').fetchone()[0]
   db.close()
   assert ((blob[3]>>1)&7)==1;data=bytearray(gp.read_bytes());offset=data.find(blob[:64]);assert offset>=0;data[offset+8:offset+16]=struct.pack('<d' if blob[3]&1 else '>d',0.0);gp.write_bytes(data);changed=[gp]
  elif mode=='contents_SRS':
   with sqlite3.connect(gp) as db:db.execute('UPDATE gpkg_contents SET srs_id=4326')
   db.close()
   changed=[gp]
  elif mode=='missing_ready_marker':marker.unlink()
  else:
   file=target/'metadata/unapproved-identifiers.csv';file.write_bytes(b'OBJECTID\n90000001\n');changed=[file]
  if mode!='missing_ready_marker':
   for file in changed:
    key=str(file.relative_to(target))
    if key in ready['objects']:ready['objects'][key]=s.sha(file.read_bytes())
   marker.write_bytes(s.encode(ready))
  bad=s.tree_inventory(target);before=set((r/'staging/_quarantine'/s.SLUG).glob('invalid-cache-*')) if (r/'staging/_quarantine'/s.SLUG).exists() else set()
  negative=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);entry=record(mode,negative);assert not negative.success
  after=set((r/'staging/_quarantine'/s.SLUG).glob('invalid-cache-*'));created=after-before;assert len(created)==1;quarantine=created.pop()
  assert not target.exists() and (s.SLUG,s.SLUG,version) not in list(_iter_staged_version_paths(staging))
  assert s.tree_inventory(quarantine/'candidate')==bad
  qr=json.loads((quarantine/'report.json').read_text());assert qr['objects']==bad and qr['approved'] is False
  assert s.tree_inventory(r/'published')==prior and s.tree_inventory(r/'staging/_raw-captures')==raw
  bundle='cbrs30-c84-'+mode.replace('_','-')+'-'+negative.run_id[:8]
  subprocess.run(['python','/workspace/work/hifld-coordinator/scripts/archive_closed_fuel.py','--bundle',bundle,'--parent',str(quarantine),'candidate'],check=True)
  archive_root=Path('/workspace/work/hifld-coordinator/closed-generated-archives');manifest=archive_root/(bundle+'.manifest.json');assert manifest.exists() and (quarantine/'candidate.archived.json').exists()
  report['probes'][mode]={'run_id':negative.run_id,'rejected':True,'canonical_absent':True,'not_discoverable':True,'all_bad_bytes_inventory_preserved':True,'bad_inventory':bad,'quarantine_report':str(quarantine/'report.json'),'verified_archive_manifest':str(manifest),'prior_published_and_raw_depot_preserved':True};save()
  shutil.copytree(seed,target);assert s.tree_inventory(target)==good
 restored=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False);record('restored_complete_real_1693_cache',restored);assert restored.success and restored.output_for_node(fws_cbrs_prohibitions.op.name)==output
 assert s.tree_inventory(target)==good and s.tree_inventory(r/'published')==prior and s.tree_inventory(r/'staging/_raw-captures')==raw
 report['restored_positive']=True;report['all9_original_and_extended_negatives_rejected']=True;save()
print(json.dumps({'head':head,'runs':report['runs'],'all9negatives':True}))
