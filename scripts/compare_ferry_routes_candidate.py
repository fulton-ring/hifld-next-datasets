"""Complete diagnostic source/baseline comparison; never accepts an identifier mapping."""
import argparse
import collections
import json
import hashlib
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pyogrio
import shapely

from dagster_hifld.sources import ferry_routes as f

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--evidence-dir',type=Path,required=True)
parser.add_argument('--capture-dir',type=Path,required=True)
parser.add_argument('--output-dir',type=Path,required=True)
args=parser.parse_args();args.output_dir.mkdir(parents=True,exist_ok=True)
captures={name:((args.capture_dir/(name+'.body')).read_bytes(),json.loads((args.capture_dir/(name+'.receipt.json')).read_text()))for name in f.URLS}
source,audit=f.inspect_originals(captures)
archive=args.evidence_dir/'baseline-native-gdb.body'
with zipfile.ZipFile(archive)as z:folders={n.split('/')[0]for n in z.namelist()};assert len(folders)==1
meta,baseline=pyogrio.read_arrow('/vsizip/{'+str(archive.resolve())+'}/'+folders.pop(),return_fids=True)
parquet=pq.read_table(args.evidence_dir/'baseline-parquet.body')
sr={r['OBJECTID']:r for r in source.to_pylist()};br={r['OBJECTID']:r for r in baseline.to_pylist()}
assert len(sr)==source.num_rows and len(br)==baseline.num_rows
business_counts=collections.Counter((r['operator_id'],r['segment_id'])for r in sr.values())
parquet_by_business=collections.defaultdict(list)
for row in parquet.to_pylist():parquet_by_business[(row['operator_id'],row['segment_id'])].append(row)
changes=collections.Counter();diags=[];maxdelta=0.0
for oid,row in sr.items():
 old=br.get(oid);changed=[];equal=False;witness=[]
 if old is not None:
  for name in f.FIELDS[:-1]:
   target='SHAPE_Length'if name=='Shape__Length'else name
   if row[name]!=old[target]:changed.append({'source_field':name,'baseline_field':target,'source':row[name],'baseline':old[target]});changes[name]+=1
  a=shapely.from_wkb(row['wkb_geometry']);b=shapely.from_wkb(old['SHAPE']);equal=bool(shapely.equals_exact(a,b,0))
  ca=shapely.get_coordinates(a).tolist();cb=shapely.get_coordinates(b).tolist()
  delta=max(abs(x-y)for p,q in zip(ca,cb)for x,y in zip(p,q))if len(ca)==len(cb)else None
  if delta is not None:maxdelta=max(maxdelta,delta)
  business_same=(row['operator_id'],row['segment_id'])==(old['operator_id'],old['segment_id'])
 else:delta=None;business_same=False
 for candidate in parquet_by_business[(row['operator_id'],row['segment_id'])]:witness.append({'published_parquet_id':candidate['id'],'same_literal_as_source_OBJECTID':candidate['id']==oid})
 diags.append({'source_OBJECTID':oid,'published_native_OBJECTID':old['OBJECTID']if old else None,'diagnostic_same_literal_business_IDs':business_same,'published_parquet_witnesses':witness,'attribute_changes':changed,'native_geometry_exact':equal,'same_coordinate_count':delta is not None,'max_coordinate_component_delta_degrees':delta,'mapping_approved':False})
with (args.output_dir/'literal-id-diagnostics.jsonl').open('w')as stream:
 for row in diags:stream.write(json.dumps(row,sort_keys=True,separators=(',',':'))+'\n')
report={'approved':False,'comparison_kind':'Complete diagnostic same-literal OBJECTID/business-ID witnesses only; no accepted join or destructive schema mapping.',
 'source_rows':source.num_rows,'published_native_rows':baseline.num_rows,'published_parquet_rows':parquet.num_rows,
 'source_unique_business_id_pairs':len(business_counts),'source_duplicate_business_id_pair_occurrences':sum(n-1 for n in business_counts.values()),
 'source_attributes':len(f.FIELDS)-1,'published_native_attributes':baseline.num_columns-2,
 'source_OBJECTIDs':sorted(sr),'published_native_OBJECTIDs':sorted(br),'published_parquet_id_values':sorted(parquet['id'].to_pylist()),
 'source_original_schema':str(source.schema),'published_native_schema':str(baseline.schema),'published_parquet_schema':str(parquet.schema),
 'source_fields_removed':sorted(set(f.FIELDS[:-1])-set(baseline.column_names)),'baseline_fields_absent':sorted(set(baseline.column_names)-set(f.FIELDS)),
 'literal_OBJECTID_business_ID_witnesses':sum(x['diagnostic_same_literal_business_IDs']for x in diags),
 'exact_native_geometries':sum(x['native_geometry_exact']for x in diags),'max_native_coordinate_component_delta_degrees':maxdelta,
 'attribute_differences':dict(changes),'source_census_year':audit['census_year_counts'],'source_data_year':audit['data_year_counts'],
 'native_crs':meta['crs'],'published_parquet_declared_crs':json.loads(parquet.schema.metadata[b'geo'])['columns']['geometry']['crs'],
 'source_geometry':'Native Esri paths preserved as WKB MultiLineString','baseline_geometry':meta['geometry_type'],
 'required_decisions':['Producer item2022 versus actual layer2020 vintage conflict','Crosswalk inaccessible403, not unmatched','Exact original/published ID mapping including Parquet id','Shape__Length versus SHAPE_Length naming/value differences','Integer physical type/nullability and geometry precision compatibility','Native output column/format schema and exact release registry'],
 'complete_baseline_pin':{'gdb_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'parquet_sha256':hashlib.sha256((args.evidence_dir/'baseline-parquet.body').read_bytes()).hexdigest()}}
(args.output_dir/'comparison.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items()if k not in ['source_OBJECTIDs','published_native_OBJECTIDs','published_parquet_id_values','source_original_schema','published_native_schema','published_parquet_schema','published_parquet_declared_crs']}))
