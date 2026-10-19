"""Complete native vs published comparison; same-name GEOID pairing is diagnostic only."""
import pathlib,json,hashlib,gzip,collections,time
import pyarrow.parquet as pq
import pyogrio
from dagster_hifld.sources import county_subdivisions as source
p=pathlib.Path('/workspace/work/hifld-coordinator/45');raw=pathlib.Path('/workspace/work/hifld-coordinator/34/runtime/staging/_raw-captures/census-designated-places/'+source.SHA256+'/original.gdb.zip');current={};published={};currentids={n:[] for n in ['OBJECTID','COUSUBNS','GEOID','GEOIDFQ']};oldids={n:[] for n in ['OBJECTID','id','OID','COUSUBNS','GEOID','STATE','COUNTY','COUSUB']};nulls=collections.Counter();start=time.time()
with source.source_reader(raw) as (_,reader):
 for b in reader:
  for r in source.native_batch(b).to_pylist():
   r['native_wkb_sha256']=hashlib.sha256(r.pop('SHAPE')).hexdigest();assert r['GEOID'] not in current;current[r['GEOID']]=r
   for n in currentids:currentids[n].append(r[n])
f=pq.ParquetFile(p/'published-baseline.parquet')
for b in f.iter_batches(batch_size=512):
 for r in b.to_pylist():
  for n,v in r.items():nulls[n]+=v is None
  r['native_wkb_sha256']=hashlib.sha256(r.pop('geometry')).hexdigest();assert r['GEOID'] not in published;published[r['GEOID']]=r
  for n in oldids:oldids[n].append(r[n])
a=set(current);b=set(published);common=a&b;sharednames=(set(source.FIELDS)-{'SHAPE'})&set(f.schema_arrow.names);changes={n:sum(type(current[g][n])!=type(published[g][n]) or current[g][n]!=published[g][n] for g in common) for n in sorted(sharednames)};d={'approved':False,'classification':'Complete unapproved published candidate; diagnostic same-name GEOID value correspondence, NOT an accepted source ID or schema mapping','source_rows':len(current),'published_rows':len(published),'source_original_ids':{n:{'type':str(source.TYPES[source.FIELDS.index(n)]),'unique':len(set(v)),'nulls':sum(x is None for x in v),'ordered_values_sha256':hashlib.sha256(source.encode({'values':v})).hexdigest()} for n,v in currentids.items()},'published_original_ids':{n:{'type':str(f.schema_arrow.field(n).type),'unique':len(set(v)),'nulls':sum(x is None for x in v),'ordered_values_sha256':hashlib.sha256(source.encode({'values':v})).hexdigest()} for n,v in oldids.items()},'published_schema':str(f.schema_arrow),'published_nulls':dict(nulls),'source_only_GEOID_values':sorted(a-b),'published_only_GEOID_values':sorted(b-a),'diagnostic_shared_GEOID_values':len(common),'common_name_value_or_type_changes':changes,'exact_native_wkb_hash_pairs_under_diagnostic_GEOID':sum(current[g]['native_wkb_sha256']==published[g]['native_wkb_sha256'] for g in common),'source_crs':'EPSG:4269','published_crs':'EPSG:3857','geometry_mapping_or_tolerance_accepted':False,'source_only_fields':sorted(set(source.FIELDS)-set(f.schema_arrow.names)),'published_only_fields':sorted(set(f.schema_arrow.names)-set(source.FIELDS)),'seconds':time.time()-start}
(p/'baseline-comparison.json').write_text(json.dumps(d,indent=2)+'\n')
with (p/'complete-ID-censuses.json.gz').open('wb') as out,gzip.GzipFile(fileobj=out,mode='wb',mtime=0) as gz:gz.write(source.encode({'approved':False,'source':currentids,'published':oldids}))
print(json.dumps({k:v for k,v in d.items() if k in ['source_rows','published_rows','diagnostic_shared_GEOID_values','source_only_GEOID_values','published_only_GEOID_values','common_name_value_or_type_changes','exact_native_wkb_hash_pairs_under_diagnostic_GEOID']}))
