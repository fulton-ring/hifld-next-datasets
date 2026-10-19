import json
from pathlib import Path
import pyarrow.parquet as pq,pyogrio,shapely,pyproj
from dagster_hifld.sources.combined_statistical_areas import *
import argparse
parser=argparse.ArgumentParser();parser.add_argument('--capture-dir',type=Path,required=True);args=parser.parse_args()
p=args.capture_dir
m,t,_=inspect_archive(p/'official2024_nationgeo.zip')
a={r['GEOID']:r for r in t.to_pylist()};b={r['GEOID']:r for r in pq.read_table(p/'baseline.parquet').to_pylist()}
geo=json.loads(pq.read_schema(p/'baseline.parquet').metadata[b'geo'])
changes=[]
for key in sorted(a):
 x,y=a[key],b[key];sg=shapely.from_wkb(x['SHAPE']);bg=shapely.from_wkb(y['geometry'])
 changes.append({'GEOID':key,'source_OBJECTID':x['OBJECTID'],'published_id':y['id'],'native_WKB_identical':x['SHAPE']==y['geometry'],'coordinate_sequence_exact':bool(shapely.equals_exact(sg,bg,0)),'topological_equals_in_declared_coordinate_values':bool(sg.equals(bg)),'fields_value_or_type_different':[k for k in FIELDS if k not in ('OBJECTID','SHAPE') and (type(x[k])!=type(y[k]) or x[k]!=y[k])]})
r={'approved':False,'status':'compatibility_review_required','classification':'Full184-to184 comparison; GEOID is a diagnostic join, not approved source/published-ID mapping. No tolerance or reprojection authorized.','source_sha256':SHA256,'published_sha256':sha256_file(p/'baseline.parquet'),'source_crs':'EPSG:4269','published_geo_metadata':geo,'source_schema':str(t.schema),'published_schema':str(pq.read_schema(p/'baseline.parquet')),'source_count':len(a),'published_count':len(b),'source_only_GEOIDs':sorted(set(a)-set(b)),'published_only_GEOIDs':sorted(set(b)-set(a)),'source_only_fields':sorted(set(t.column_names)-set(pq.read_schema(p/'baseline.parquet').names)),'published_only_fields':sorted(set(pq.read_schema(p/'baseline.parquet').names)-set(t.column_names)),'source_OBJECTID_equals_published_id':sum(x['source_OBJECTID']==x['published_id'] for x in changes),'native_WKB_identical':sum(x['native_WKB_identical'] for x in changes),'coordinate_sequence_exact':sum(x['coordinate_sequence_exact'] for x in changes),'topological_equals_raw_coordinate_values':sum(x['topological_equals_in_declared_coordinate_values'] for x in changes),'per_GEOID':changes}
(p/'comparison.json').write_bytes(encode(r));print({k:v for k,v in r.items() if k in ['source_count','published_count','native_WKB_identical','coordinate_sequence_exact','topological_equals_raw_coordinate_values','source_OBJECTID_equals_published_id']})
