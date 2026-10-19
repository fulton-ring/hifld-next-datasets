"""Actual registered 845-row first-marker storage fault and exact recovery."""
import json,os,subprocess
from pathlib import Path
from datetime import datetime,timedelta
from unittest.mock import patch
rt=Path('/workspace/work/hifld-coordinator/89/complete-construction-actual');assert not rt.exists()
for name in ['dagster','staging','published']:(rt/name).mkdir(parents=True)
(rt/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for k in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','KUBERNETES_SERVICE_HOST','HIFLD_RELEASE_APPROVALS']:os.environ.pop(k,None)
os.environ.update(DAGSTER_HOME=str(rt/'dagster'),HIFLD_STAGING_DIR=str(rt/'staging'),HIFLD_DATASETS_DIR=str(rt/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance,Definitions,AssetSelection,define_asset_job
from dagster_hifld.definitions import defs as normal
from dagster_hifld.assets.bts_ferry_terminals import bts_ferry_terminals
from dagster_hifld.sources import ferry_terminals as s
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
repo=Path('/workspace/hifld-worker-worktrees/89');capture=repo/'docs/dataset-sources/ferry-terminals/evidence/captures'
(rt/'published/prior.bin').write_bytes(b'Synthetic prior fixture; no dataset acceptance.')
prior=(rt/'published/prior.bin').read_bytes();stage=StagingStorageResource(local_dir=str(rt/'staging'),use_local=True);published=PublishedStorageResource(local_dir=str(rt/'published'),use_local=True)
report={'source_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'approved':False,'classification':'Real complete845rows; injected first-marker write fault and explicitly synthetic later receipt timestamps; no production or dataset approval','runs':[]}
def config(path):return {'ops':{bts_ferry_terminals.op.name:{'config':{'capture_dir':str(path)}}}}
def record(label,r):
 errors=[e.event_specific_data.error.to_string() for e in r.get_step_failure_events()];report['runs'].append({'kind':label,'run_id':r.run_id,'success':r.success,'errors':errors});(rt/'execution-progress.json').write_bytes(s.encode(report));assert (rt/'published/prior.bin').read_bytes()==prior;return errors
original_write=s.immutable
calls=[]
def interrupt(storage,key,body):
 if key.startswith('_candidate-construction/') and key.endswith('.first-construction.json'):
  calls.append(key);raise OSError('injected actual first marker write interruption after preserved CAS')
 return original_write(storage,key,body)
with DagsterInstance.get() as inst:
 job=normal.resolve_job_def('bts_ferry_terminals_draft_refresh')
 with patch.object(s,'immutable',side_effect=interrupt):r=job.execute_in_process(instance=inst,run_config=config(capture),raise_on_error=False)
 errors=record('actual_first_marker_write_failure_after_full_private_native_build',r);assert not r.success and calls and any('first marker write interruption' in x for x in errors)
 evidence=list((rt/'staging/_candidate-construction'/s.SLUG).glob('*.json.evidence/*.json'));assert len(evidence)==1;evidence=evidence[0];body=evidence.read_bytes();bundle=json.loads(body);version=bundle['version'];root=rt/'staging'/s.SLUG/s.SLUG/version;alias=evidence.parent.parent/(version+'.json');marker=alias.with_name(alias.name+'.first-construction.json');assert not root.exists() and not alias.exists() and not marker.exists()
 newer=rt/'explicit-synthetic-later-receipts';newer.mkdir()
 for name in s.URLS:
  value=(capture/(name+'.body')).read_bytes();receipt=json.loads((capture/(name+'.receipt.json')).read_bytes());receipt['fetched_at']=(datetime.fromisoformat(receipt['fetched_at'])+timedelta(hours=3)).isoformat();(newer/(name+'.body')).write_bytes(value);(newer/(name+'.receipt.json')).write_bytes(s.encode(receipt))
 for attempt in [1,2]:
  with patch.object(s,'write_canonical_geopackage',wraps=s.write_canonical_geopackage) as writer:r=job.execute_in_process(instance=inst,run_config=config(newer),raise_on_error=False)
  errors=record(f'CAS_only_second_context_attempt_{attempt}',r);assert not r.success and writer.call_count==0 and any('partial_original_construction_evidence_requires_trusted_restore' in x for x in errors);assert evidence.read_bytes()==body and not root.exists() and not alias.exists() and not marker.exists()
 witness={'schema_version':1,'version':version,'processing_fingerprint':bundle['manifest']['processing_fingerprint'],'original_bundle_sha256':s.sha(body),'original_bundle_evidence_key':str(evidence.relative_to(rt/'staging'))}
 marker.write_bytes(s.encode(witness));alias.write_bytes(body)
 r=job.execute_in_process(instance=inst,run_config=config(newer),raise_on_error=False);record('explicit_exact_original_CAS_operator_restore_and_complete_reconstruction',r);assert r.success;output=r.output_for_node(bts_ferry_terminals.op.name);assert output['feature_count']==845;assert (root/'metadata/source_manifest.json').read_bytes()==s.encode(bundle['manifest'])
 reference=json.loads(Path('/workspace/work/hifld-coordinator/reviews/ferry-89-marker-runtime/execution.json').read_text())['native_byte_parity'];actual={name:s.sha((root/name).read_bytes()) for name in reference};assert actual==reference
 snapshot={str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
 r=job.execute_in_process(instance=inst,run_config=config(newer),raise_on_error=False);record('complete_native_refresh_retains_original_all9_receipts',r);assert r.success;assert snapshot=={str(p.relative_to(root)):s.sha(p.read_bytes()) for p in root.rglob('*') if p.is_file()}
 prefix=f'{s.SLUG}/{s.SLUG}/{version}';inst.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix]);keys=[k for k in normal.resolve_asset_graph().get_all_asset_keys() if k.path[0]=='publish'];defs=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,resources={'staging_storage':stage,'published_storage':published},jobs=[define_asset_job('complete_ferry_graph',selection=AssetSelection.assets(*keys))]);r=defs.resolve_job_def('complete_ferry_graph').execute_in_process(instance=inst,partition_key=prefix,raise_on_error=False);errors=record('normal_catalog_and_unapproved_promotion_after_original_recovery',r);assert not r.success and errors and all('missing_independent_release_approval_registry' in x for x in errors)
 report.update(output=output,native_byte_parity=actual,all845_rows_fields_types_native_geometry_unchanged=True,all9_original_manifest_receipts_preserved=True,first_partial_CAS_preserved_exact=True,repeat_no_bootstrap_before_writer=True,prior_fixture_preserved=True,operator_restore_scope='Explicit exact saved original witness records restored from retained fullCAS; no automatic trust repair')
(rt/'execution.json').write_bytes(s.encode(report));print(json.dumps({'head':report['source_head'],'runs':[(r['kind'],r['run_id'],r['success']) for r in report['runs']],'all_assertions_pass':True}))
