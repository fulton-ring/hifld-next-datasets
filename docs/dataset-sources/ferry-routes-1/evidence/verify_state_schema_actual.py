"""Actual registered complete964-source marker physical-type rejection and exact recovery."""
import json,os,subprocess
from pathlib import Path
R=Path('/workspace/work/hifld-coordinator/88');r=R/'state-schema-final-actual';capture=R/'capture'
if r.exists()and any(r.iterdir()):raise ValueError('requires empty isolated runtime')
for d in ['dagster','staging','published']:(r/d).mkdir(parents=True,exist_ok=True)
(r/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for k in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS','KUBERNETES_SERVICE_HOST']:os.environ.pop(k,None)
os.environ.update(DAGSTER_HOME=str(r/'dagster'),HIFLD_FERRY_ROUTES_CAPTURE_DIR=str(capture),HIFLD_STAGING_DIR=str(r/'staging'),HIFLD_DATASETS_DIR=str(r/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')
from dagster import DagsterInstance
from dagster_hifld.definitions import defs
from dagster_hifld.sources import ferry_routes as f
s=r/'staging';(r/'published/synthetic-accepted-fixture.bin').write_bytes(b'Preserve this prior fixture; no real dataset accepted.')
prior={str(p.relative_to(r/'published')):f.sha256_file(p)for p in(r/'published').rglob('*')if p.is_file()};report={'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(f.__file__).parents[3],text=True).strip(),'scope':'Actual registered full964 candidate and synthetic malformed marker types, not real producer drift or release acceptance','runs':[]}
def save():(r/'execution.json').write_text(json.dumps(report,indent=2)+'\n')
def run(instance,label,success):
 result=defs.resolve_job_def('ferry_routes_source_integrity').execute_in_process(instance=instance,raise_on_error=False);errors=[e.event_specific_data.error.to_string()for e in result.get_step_failure_events()];item={'label':label,'run_id':result.run_id,'success':result.success,'errors':errors};report['runs'].append(item);save();assert result.success is success,errors
 if success:item['output']=result.output_for_node('ingest__ferry_routes_1__ferry_routes_1');assert item['output']['feature_count']==964
 else:assert any('missing_or_invalid_first_construction_state'in e for e in errors)
 return result
with DagsterInstance.get()as instance:
 run(instance,'fresh_complete964_source',True);root=next((s/f.SLUG/f.SLUG).glob('v*'));primary=next((s/'_source-provenance').rglob('bundle.json'));original=primary.read_bytes();marker=next((s/'_construction-state').rglob('*.json'));mark=marker.read_bytes();state=json.loads(mark);evidence=s/state['original_bundle_evidence_key'];cas=evidence.read_bytes();assert cas==original
 raw={str(p.relative_to(s)):f.sha256_file(p)for p in(s/'_raw-captures').rglob('*')if p.is_file()}
 for value in [True,1.0]:
  before={str(p.relative_to(root)):f.sha256_file(p)for p in root.rglob('*')if p.is_file()};existing=set((s/'_quarantine'/f.SLUG).glob('invalid-candidate-*'))if(s/'_quarantine'/f.SLUG).exists()else set();badstate=dict(state);badstate['schema_version']=value;marker.write_bytes(f.encode(badstate));bad=marker.read_bytes();run(instance,'malformed_marker_'+type(value).__name__+'_rejects',False);assert marker.read_bytes()==bad and evidence.read_bytes()==cas and not root.exists()
  q=set((s/'_quarantine'/f.SLUG).glob('invalid-candidate-*'))-existing;assert len(q)==1;kept=next(iter(q));assert before=={str(p.relative_to(kept)):f.sha256_file(p)for p in kept.rglob('*')if p.is_file()}
  marker.write_bytes(mark);primary.parent.mkdir(parents=True,exist_ok=True);primary.write_bytes(original);run(instance,'exact_original_'+type(value).__name__+'_restoration_rebuild',True);assert marker.read_bytes()==mark and evidence.read_bytes()==cas
 run(instance,'valid_original_cache_reuse',True)
 assert raw=={str(p.relative_to(s)):f.sha256_file(p)for p in(s/'_raw-captures').rglob('*')if p.is_file()};assert prior=={str(p.relative_to(r/'published')):f.sha256_file(p)for p in(r/'published').rglob('*')if p.is_file()};assert marker.read_bytes()==mark and evidence.read_bytes()==cas
 report.update(all964_rows_native_payloads_verified=True,raw_objects_unchanged=True,prior_synthetic_accepted_fixture_unchanged=True,whole_invalid_candidates_retained=True,malformed_markers_preserved_until_explicit_restore=True,full_original_CAS_unchanged=True,valid_original_marker_exactly_restored=True,native_payload_hashes={str(p.relative_to(root)):f.sha256_file(p)for p in [root/'geopackage'/f'{f.SLUG}.gpkg',root/'geoparquet'/f'{f.SLUG}.parquet']});save()
print(json.dumps({'head':report['head'],'runs':[(x['label'],x['run_id'],x['success'])for x in report['runs']]}))
