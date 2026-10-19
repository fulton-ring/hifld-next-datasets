"""Complete real-source construction-history guards; bounded replayable forensic deltas.

Run once on a new --runtime. Commit docs/scripts only, then use --later-docs to
check genuine historical provenance and cold reconstruction on that same runtime.
"""
import argparse,hashlib,json,os,pathlib,shutil,subprocess,tarfile
parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=pathlib.Path,required=True);parser.add_argument('--later-docs',action='store_true');args=parser.parse_args()
r=args.runtime.resolve();repo=pathlib.Path(__file__).resolve().parents[1];capture=repo/'docs/dataset-sources/cbrs-buffer-zone-1/evidence/captures'
if not args.later_docs:
 assert not r.exists()
 for n in ['dagster','staging','published']:(r/n).mkdir(parents=True)
 (r/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for n in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(n,None)
os.environ.update(DAGSTER_HOME=str(r/'dagster'),HIFLD_STAGING_DIR=str(r/'staging'),HIFLD_DATASETS_DIR=str(r/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs,_iter_staged_version_paths
from dagster_hifld.assets.fws import fws_cbrs_buffer_zone as asset
from dagster_hifld.sources import cbrs_buffer as s
from dagster_hifld.resources import StagingStorageResource
import fiona,pyarrow.parquet as pq,shapely
assert pathlib.Path(s.__file__).resolve().is_relative_to(repo/'src')
storage=StagingStorageResource(local_dir=str(r/'staging'))
def pins(root):return {str(p.relative_to(root)):{'bytes':p.stat().st_size,'sha256':s.sha256_file(p)} for p in root.rglob('*') if p.is_file()}
def head():return subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
def save(): (r/'construction-history-in-progress.json').write_bytes(s.encode(report))
def closed(root):
 for p in pathlib.Path('/proc').glob('[0-9]*'):
  for fd in list((p/'fd').glob('*'))+[p/'cwd']:
   try:t=str(fd.readlink())
   except OSError:continue
   assert t!=str(root) and not t.startswith(str(root)+'/'),str(fd)
def tar_pins(archive):
 result={}
 with tarfile.open(archive,'r:gz') as stream:
  for m in stream:
   if m.isdir():continue
   assert m.isfile() and m.name not in result
   with stream.extractfile(m) as body:result[m.name]={'bytes':m.size,'sha256':hashlib.file_digest(body,'sha256').hexdigest()}
 return result
def restore():
 assert not root.exists();root.mkdir(parents=True)
 assert s.sha256_file(base)==report['healthy_base_archive']['sha256'] and tar_pins(base)==basepins
 with tarfile.open(base,'r:gz') as stream:stream.extractall(root,filter='data')
 assert pins(root)==basepins

def reject_archive(parent):
 # A complete healthy base plus exact changed-file delta reproduces every bad
 # byte. Replay all base members and all delta members before removing the closed
 # generated clone; no source capture or trusted construction record is removed.
 closed(parent);before=pins(parent);expected={root.name+'/'+k:v for k,v in basepins.items()}
 changed={k:v for k,v in before.items() if expected.get(k)!=v}
 assert set(expected).issubset(before)
 archive=r/'rejected-deltas'/('rejected-'+parent.name+'.tar.gz');archive.parent.mkdir(exist_ok=True)
 with tarfile.open(archive,'w:gz',compresslevel=1) as stream:
  for k in changed:stream.add(parent/k,arcname=k,recursive=False)
 delta=tar_pins(archive);assert delta==changed
 replay={root.name+'/'+k:v for k,v in tar_pins(base).items()};replay.update(delta)
 assert replay==before and pins(parent)==before
 closed(parent)
 receipt={'base_archive':str(base),'base_archive_sha256':s.sha256_file(base),'delta_archive':str(archive),'delta_archive_sha256':s.sha256_file(archive),'candidate_name':root.name,'complete_replayed_bad_file_roster':before,'base_and_delta_full_member_SHA_replay_passed':True,'restore_steps':['Create an empty directory for the UUID quarantine entry.','Extract healthy base archive into its candidate_name subdirectory with tar data filter.','Extract delta archive into UUID directory, overwriting exact changed files.','Verify every file bytes and SHA256 against complete_replayed_bad_file_roster.']}
 pointer=parent.with_name(parent.name+'.archived.json');pointer.write_bytes(s.encode(receipt));shutil.rmtree(parent)
 return receipt

if args.later_docs:
 report=json.loads((r/'construction-history-execution.json').read_bytes());assert head()!=report['construction_head']
else:
 report={'construction_head':head(),'source_checkpoint':'1d8e49b','approved':False,'runs':[],'rejected':[],'scope':'Complete1145 authentic source; missing alias and missing/malformed marker repeated without restoration; synthetic output fixture grants no acceptance.'}
 (r/'published/prior-local-fixture').write_bytes(b'prior local preservation fixture, not accepted real data')
prior=pins(r/'published');rawbefore=pins(r/'staging/_raw-captures')
config={'ops':{asset.op.name:{'config':{'capture_dir':str(capture)}}}}
def execute(kind,success,reason=None):
 result=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
 errors=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
 report['runs'].append({'kind':kind,'run_id':result.run_id,'success':result.success,'errors':errors});save()
 assert result.success is success,(kind,errors)
 if reason:assert any(reason in e for e in errors),(kind,errors)
 assert pins(r/'published')==prior
 return result
with DagsterInstance.get() as instance:
 job=defs.resolve_job_def('fws_cbrs_buffer_draft_refresh')
 if not args.later_docs:
  result=execute('authentic_complete_first_construction',True);output=result.output_for_node(asset.op.name);report['output']=output
 else:output=report['output']
 prefix=f'{s.SLUG}/{s.SLUG}/{output["version"]}';root=r/'staging'/prefix
 key=r/'staging/_source-provenance'/s.SLUG/output['version']/'bundle.json';marker=key.with_name(key.name+'.first-construction.json')
 base=r/'healthy-candidate.tar.gz'
 if not args.later_docs:
  bundle=key.read_bytes();first=marker.read_bytes();report['original_bundle_sha256']=s.sha(bundle);report['original_marker_sha256']=s.sha(first)
  (r/'trusted-original-bundle.json').write_bytes(bundle);(r/'trusted-original-marker.json').write_bytes(first)
  assert json.loads(bundle)['manifest']['code_commit']==report['construction_head']
  assert json.loads(first)['original_bundle_sha256']==s.sha(bundle)
  rows=pq.read_table(root/'candidate/typed-source.parquet').to_pylist();assert len(rows)==1145
  with fiona.open(root/'geopackage'/f'{s.SLUG}.gpkg') as stream:gp={f['properties']['__source_fid']:f for f in stream}
  assert set(gp)=={str(i) for i in range(1145)}
  for row in rows:
   original=gp[row['__source_fid']];assert dict(original['properties'])=={n:row[n] for n in s.SCHEMA}
   assert shapely.geometry.shape(original.geometry).wkb==row['geometry']
  basepins=pins(root)
  with tarfile.open(base,'w:gz',compresslevel=1) as stream:
   for name in basepins:stream.add(root/name,arcname=name,recursive=False)
  assert tar_pins(base)==basepins and pins(root)==basepins
  report['healthy_base_archive']={'path':str(base),'sha256':s.sha256_file(base),'complete_file_roster':basepins}
  for mode in ['primary_missing','marker_missing','marker_malformed']:
   if not root.exists():restore()
   key.write_bytes(bundle);marker.write_bytes(first)
   if mode=='primary_missing':key.unlink();reason='missing_original_construction_bundle_after_first_construction'
   elif mode=='marker_missing':marker.unlink();reason='missing_or_invalid_first_construction_marker'
   else:marker.write_bytes(b'{');reason='Expecting property name'
   bad_primary=key.read_bytes() if key.exists() else None;bad_marker=marker.read_bytes() if marker.exists() else None
   qroot=r/'staging/_quarantine'/s.SLUG/'rejected-cache';before={p for p in qroot.glob('*') if p.is_dir()}
   for retry in [1,2]:
    execute(mode+'_retry_'+str(retry),False,reason)
    assert not root.exists() and (s.SLUG,s.SLUG,output['version']) not in list(_iter_staged_version_paths(storage))
    assert (key.read_bytes() if key.exists() else None)==bad_primary
    assert (marker.read_bytes() if marker.exists() else None)==bad_marker
    if retry==1:
     added={p for p in qroot.glob('*') if p.is_dir()}-before;assert len(added)==1
     receipt=reject_archive(next(iter(added)));report['rejected'].append({'mode':mode,'complete_bad_tree_preserved':receipt});save()
   key.write_bytes(bundle);marker.write_bytes(first) # Explicit saved-original restoration only.
  restore();execute('explicit_trusted_original_restoration_reuse',True)
 else:
  basepins=report['healthy_base_archive']['complete_file_roster'];bundle=(r/'trusted-original-bundle.json').read_bytes();first=(r/'trusted-original-marker.json').read_bytes()
  assert key.read_bytes()==bundle and marker.read_bytes()==first
  later=r/'later-observation-fixture';later.mkdir()
  for name in s.URLS:
   os.link(capture/(name+'.body'),later/(name+'.body'))
   receipt=json.loads((capture/(name+'.receipt.json')).read_bytes());receipt['observed_at']='2026-10-20T01:02:03+00:00';(later/(name+'.receipt.json')).write_bytes(s.encode(receipt))
  config={'ops':{asset.op.name:{'config':{'capture_dir':str(later)}}}}
  execute('genuine_later_docs_HEAD_original_context_reuse',True)
  assert pins(root)==basepins
  closed(root);assert tar_pins(base)==basepins and pins(root)==basepins
  shutil.rmtree(root) # Complete healthy bytes already archived and fully replayed.
  execute('cold_rebuild_original_context_with_later_observations',True)
  assert pins(root)==basepins
  report.update(later_docs_HEAD=head(),later_receipts_scope='Explicit simulated later observation receipts of unchanged complete real bodies; no new live claim',cold_rebuild_original_manifest_and_receipts_preserved=True)
 assert key.read_bytes()==bundle and marker.read_bytes()==first
 assert s.sha256_file(root/'geopackage'/f'{s.SLUG}.gpkg')=='29edab0b125fc438930422b0f5ff395869d25fa4854077222632515423c79a56'
 assert s.sha256_file(root/'candidate/typed-source.parquet')=='54b294ce12e616ade4b8fa8b00bd27cab9ce320a7ef4af7ce24807a7d4e1480e'
 now=pins(r/'staging/_raw-captures');assert all(now.get(k)==v for k,v in rawbefore.items())
 report.update(all1145_values_IDs_types_native_WKB_preserved=True,raw_originals_and_prior_local_fixture_preserved=True,original_binding_and_marker_preserved=True,no_baseline_contract_source_or_release_approval=True)
(r/'construction-history-execution.json').write_bytes(s.encode(report));print(json.dumps({'head':head(),'runs':[(x['kind'],x['run_id'],x['success']) for x in report['runs']]}))
