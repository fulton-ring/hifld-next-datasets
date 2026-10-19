"""Authentic complete candidate mutations; no acceptance/promotion decisions."""
import os,json,argparse
from pathlib import Path
parser=argparse.ArgumentParser();parser.add_argument('--capture-dir',type=Path,required=True);parser.add_argument('--runtime-dir',type=Path,required=True);parser.add_argument('--allow-mutate-isolated-candidate',action='store_true',required=True);args=parser.parse_args()
p=args.capture_dir.resolve();runtime=args.runtime_dir.resolve()
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_CSA_CAPTURE_DIR=str(p))
from dagster import Definitions,AssetSelection,define_asset_job,DagsterInstance
from dagster_hifld.assets.combined_statistical_areas import census_combined_statistical_areas as source
from dagster_hifld.sources import combined_statistical_areas as c
report=json.loads((runtime/'materialization.json').read_text());root=runtime/'staging'/c.SLUG/c.SLUG/report['version'];stage=c.StagingStorageResource(local_dir=str(runtime/'staging'))
defs=Definitions(assets=[source],resources={'staging_storage':stage},jobs=[define_asset_job('csa_metadata_negative',selection=AssetSelection.assets(source))])
parquet=root/'candidate/typed-source.parquet';gpkg=root/'geopackage'/f'{c.SLUG}.gpkg';readyfile=root/'metadata/csa_candidate_ready.json'
original_parquet=parquet.read_bytes();original_gpkg=gpkg.read_bytes();original_ready=readyfile.read_bytes();table=c.pq.read_table(parquet)
prior={str(x.relative_to(runtime/'published')):c.sha256_file(x) for x in (runtime/'published').rglob('*') if x.is_file()};raw=runtime/'staging'/f'_raw-captures/{c.SLUG}/{c.SHA256}/original.gdb.zip';raw_pin=c.sha256_file(raw)
result={'source_code_checkpoint':'2c990c2','classification':'Actual full184-row metadata-only self-repin mutations, each rejected; no approved data','tests':[]}
mutations=[('wrong_crs','crs',c.pyproj.CRS(3857).to_json_dict()),('wrong_encoding','encoding','WKT'),('wrong_geometry_types','geometry_types',['Polygon']),('wrong_bounds','bbox',[0,0,1,1]),('wrong_primary','primary_column','invented'),('geometry_extension_metadata','extension',None),('wrong_gpkg_bounds','gpkg',None)]
with DagsterInstance.get() as instance:
 try:
  for label,field,value in mutations:
   parquet.write_bytes(original_parquet);gpkg.write_bytes(original_gpkg);readyfile.write_bytes(original_ready)
   ready=json.loads(original_ready)
   if field=='gpkg':
    with c.sqlite3.connect(gpkg) as db:db.execute('UPDATE gpkg_contents SET min_x=999')
    ready['objects'][f'geopackage/{c.SLUG}.gpkg']=c.sha256_file(gpkg)
   elif field=='extension':
    fields=list(table.schema);i=table.column_names.index('SHAPE');fields[i]=fields[i].with_metadata({b'ARROW:extension:name':b'geoarrow.wkt'})
    changed=c.pa.Table.from_arrays(table.columns,schema=c.pa.schema(fields,metadata=table.schema.metadata));c.pq.write_table(changed,parquet)
    ready['objects']['candidate/typed-source.parquet']=c.sha256_file(parquet)
   else:
    meta=dict(table.schema.metadata);geo=json.loads(meta[b'geo'])
    if field=='primary_column':geo[field]=value
    else:geo['columns']['SHAPE'][field]=value
    meta[b'geo']=c.encode(geo);c.pq.write_table(table.replace_schema_metadata(meta),parquet)
    ready['objects']['candidate/typed-source.parquet']=c.sha256_file(parquet)
   if field=='gpkg':
    _,observed=c.pyogrio.read_arrow(gpkg,layer=c.LAYER)
   else:observed=c.pq.read_table(parquet)
   assert c.table_values(observed)==c.table_values(table)
   readyfile.write_bytes(c.encode(ready))
   mutated_payload_pin=c.sha256_file(gpkg if field=='gpkg' else parquet)
   run=defs.resolve_job_def('csa_metadata_negative').execute_in_process(instance=instance,raise_on_error=False)
   errors=[e.event_specific_data.error.to_string() for e in run.get_step_failure_events()]
   assert not run.success and errors and all('cached_' in e and ('fresh_original' in e) for e in errors),errors
   assert mutated_payload_pin==c.sha256_file(gpkg if field=='gpkg' else parquet), 'failed refresh must preserve preexisting candidate bytes'
   result['tests'].append({'mutation':label,'run_id':run.run_id,'success':run.success,'expected_failure':'cached geometry/schema metadata disagrees with fresh original','ready_digest_repinned':True,'all184_rows_and_WKB_unmodified':True,'mutated_preexisting_candidate_preserved_on_failure':True})
 finally:
  parquet.write_bytes(original_parquet);gpkg.write_bytes(original_gpkg);readyfile.write_bytes(original_ready)
 result['raw_original_unchanged']=c.sha256_file(raw)==raw_pin==c.SHA256
 result['prior_published_fixture_unchanged']=prior=={str(x.relative_to(runtime/'published')):c.sha256_file(x) for x in (runtime/'published').rglob('*') if x.is_file()}
 result['original_candidate_restored_after_tests']=parquet.read_bytes()==original_parquet and gpkg.read_bytes()==original_gpkg and readyfile.read_bytes()==original_ready
 assert result['raw_original_unchanged'] and result['prior_published_fixture_unchanged'] and result['original_candidate_restored_after_tests']
 valid=defs.resolve_job_def('csa_metadata_negative').execute_in_process(instance=instance,raise_on_error=False)
 assert valid.success;result['restored_valid_refresh_run_id']=valid.run_id
(p/'review-fix-metadata-probes.json').write_bytes(c.encode(result));print(json.dumps(result,indent=2))
