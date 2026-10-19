"""Narrow actual184-row SRS/header/index cache rejection; no terminal rebuild."""
import os,json,subprocess,struct,argparse
from pathlib import Path
parser=argparse.ArgumentParser();parser.add_argument('--capture-dir',type=Path,required=True);parser.add_argument('--runtime-dir',type=Path,required=True);parser.add_argument('--allow-mutate-isolated-candidate',action='store_true',required=True);args=parser.parse_args()
p=args.capture_dir.resolve();runtime=args.runtime_dir.resolve()
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_CSA_CAPTURE_DIR=str(p),HIFLD_STAGING_DIR=str(runtime/'staging'),HIFLD_DATASETS_DIR=str(runtime/'published'))
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld.assets.combined_statistical_areas import census_combined_statistical_areas as source
from dagster_hifld.sources import combined_statistical_areas as c
stage=c.StagingStorageResource(local_dir=str(runtime/'staging'))
defs=Definitions(assets=[source],resources={'staging_storage':stage},jobs=[define_asset_job('csa_srs_header_index_guard',selection=AssetSelection.assets(source))])
job=defs.resolve_job_def('csa_srs_header_index_guard');record={'head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'module':c.__file__,'classification':'Narrow repaired actual full184 source/cache runs; previous terminal proof reused only after exact data hash equality','positive_runs':[],'negative_runs':[]}
prior={str(x.relative_to(runtime/'published')):c.sha256_file(x) for x in (runtime/'published').rglob('*') if x.is_file()};raw=runtime/'staging'/f'_raw-captures/{c.SLUG}/{c.SHA256}/original.gdb.zip';raw_pin=c.sha256_file(raw)
with DagsterInstance.get() as instance:
 for label in ['source','reuse']:
  run=job.execute_in_process(instance=instance,raise_on_error=False);assert run.success,[e.event_specific_data.error.to_string() for e in run.get_step_failure_events()]
  out=run.output_for_node(source.op.name);assert out['feature_count']==184
  record['positive_runs'].append({'label':label,'run_id':run.run_id,'success':True,'output':out})
 root=runtime/'staging'/c.SLUG/c.SLUG/out['version'];gpkg=root/'geopackage'/f'{c.SLUG}.gpkg';parquet=root/'candidate/typed-source.parquet';readyfile=root/'metadata/csa_candidate_ready.json'
 original=gpkg.read_bytes();ready_bytes=readyfile.read_bytes();record['source_payload_hashes']={f'geopackage/{c.SLUG}.gpkg':c.sha256_file(gpkg),'candidate/typed-source.parquet':c.sha256_file(parquet)}
 previous={'geopackage/'+c.SLUG+'.gpkg':'e42ee037b7e6009e6d4f515d43cd990d71c6950b4803e411194a37eee7f9704b','candidate/typed-source.parquet':'d0ed83f85ceefd7ce80e80f5134d3b1a1c52fa1744368b2ef2623bbd328091b3'};assert all(previous[k]==v for k,v in record['source_payload_hashes'].items())
 with c.sqlite3.connect(gpkg) as db:original_rows=db.execute(f'SELECT _gpkg_storage_fid,SHAPE FROM "{c.LAYER}" ORDER BY _gpkg_storage_fid').fetchall()
 original_geometry_payloads={fid:blob[8+{0:0,1:32,2:48,3:48,4:64}[(blob[3]>>1)&7]:] for fid,blob in original_rows}
 try:
  for case in ['definition','authority_code','header_srs','header_flags','header_envelope','rtree']:
   gpkg.write_bytes(original);readyfile.write_bytes(ready_bytes)
   with c.sqlite3.connect(gpkg) as db:
    if case=='definition':db.execute('UPDATE gpkg_spatial_ref_sys SET definition=? WHERE srs_id=4269',(c.pyproj.CRS(4326).to_wkt(version='WKT1_GDAL'),))
    elif case=='authority_code':db.execute('UPDATE gpkg_spatial_ref_sys SET organization_coordsys_id=4326 WHERE srs_id=4269')
    elif case=='rtree':db.execute(f'UPDATE "rtree_{c.LAYER}_SHAPE" SET minx=-999 WHERE id=1')
    else:
     with db.blobopen(c.LAYER,'SHAPE',1) as blob:
      if case=='header_srs':blob.seek(4);blob.write((4326).to_bytes(4,'little',signed=True))
      elif case=='header_flags':blob.seek(3);value=blob.read(1)[0];blob.seek(3);blob.write(bytes([value^32]))
      else:blob.seek(8);blob.write(struct.pack('<d',999.0))
   ready=json.loads(ready_bytes);ready['objects'][f'geopackage/{c.SLUG}.gpkg']=c.sha256_file(gpkg);readyfile.write_bytes(c.encode(ready))
   with c.sqlite3.connect(gpkg) as db:changed_rows=db.execute(f'SELECT _gpkg_storage_fid,SHAPE FROM "{c.LAYER}" ORDER BY _gpkg_storage_fid').fetchall()
   assert len(changed_rows)==184 and all(original_geometry_payloads[fid]==blob[8+{0:0,1:32,2:48,3:48,4:64}[(blob[3]>>1)&7]:] for fid,blob in changed_rows)
   tampered=c.sha256_file(gpkg)
   run=job.execute_in_process(instance=instance,raise_on_error=False);errors=[e.event_specific_data.error.to_string() for e in run.get_step_failure_events()]
   assert not run.success and errors and all('cached_geopackage_container_bytes_disagree_with_fresh_original' in e for e in errors),errors
   assert c.sha256_file(gpkg)==tampered
   record['negative_runs'].append({'case':case,'run_id':run.run_id,'success':False,'ready_checksum_repinned':True,'all184_wkb_payloads_unchanged':True,'failed_cache_did_not_overwrite_existing_candidate':True,'reason':'cached_geopackage_container_bytes_disagree_with_fresh_original'})
 finally:
  gpkg.write_bytes(original);readyfile.write_bytes(ready_bytes)
 restored=job.execute_in_process(instance=instance,raise_on_error=False);assert restored.success
 record['restored_run_id']=restored.run_id
 record['raw_original_unchanged']=c.sha256_file(raw)==raw_pin==c.SHA256
 record['prior_published_fixture_unchanged']=prior=={str(x.relative_to(runtime/'published')):c.sha256_file(x) for x in (runtime/'published').rglob('*') if x.is_file()}
 record['all_immutable_source_candidate_bytes_restored']=all(c.sha256_file(root/k)==pin for k,pin in json.loads(ready_bytes)['objects'].items()) and readyfile.read_bytes()==ready_bytes
 assert record['raw_original_unchanged'] and record['prior_published_fixture_unchanged'] and record['all_immutable_source_candidate_bytes_restored']
 record['prior_terminal_verification_reused_data_hashes_exact']=True
(p/'srs-fix-actual.json').write_bytes(c.encode(record));print(json.dumps(record,indent=2))
