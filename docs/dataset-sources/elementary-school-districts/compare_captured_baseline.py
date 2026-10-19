"""Full preserved baseline/source diagnostics; no approved join or schema mapping."""
import argparse,collections,json,hashlib
from pathlib import Path
import pyogrio,pyarrow.parquet as pq,shapely
from dagster_hifld.sources import elementary_school_districts as s
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source-inputs',type=Path,default=Path(__file__).resolve().parent/'evidence/inputs')
parser.add_argument('--baseline-root',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args();C=args.source_inputs;B=args.baseline_root
caps={n:((C/(n+'.body')).read_bytes(),s.strict_json((C/(n+'.receipt.json')).read_bytes()))for n in s.URLS}
table,_=s.inspect_archives({n:C/(n+'.body')for n in s.PINS},caps);rows=table.to_pylist()
gm,gt=pyogrio.read_arrow(next(B.glob('geopackage*.body')),return_fids=True);gr=gt.to_pylist()
pt=pq.read_table(next(B.glob('geoparquet*.body')));pr=pt.to_pylist()
def id_roster(rr,n):
 vals=[r[n]for r in rr];cc=collections.Counter(vals)
 return {'rows':len(vals),'unique':len(cc),'null_count':cc.get(None,0),'leading_zero_rows':sum(type(v)is str and v.startswith('0')for v in vals),'all_values_original_order':vals,'duplicates':[{'value':k,'count':v}for k,v in cc.items()if v>1]}
assert len({r['GEOID']for r in gr})==len(gr)and len({r['GEOID']for r in pr})==len(pr)
bm={r['GEOID']:r for r in gr};pm={r['GEOID']:r for r in pr};sm={r['GEOID']:r for r in rows}
pairs={'STATEFP':'STATE','ELSDLEA':'SDELM','ALAND':'AREALAND','AWATER':'AREAWATER','LSAD':'LSADC','NAME':'NAME','LOGRADE':'LOGRADE','HIGRADE':'HIGRADE','MTFCC':'MTFCC','SDTYP':'SDTYP','FUNCSTAT':'FUNCSTAT','INTPTLAT':'INTPTLAT','INTPTLON':'INTPTLON'}
inter=sorted(sm.keys()&bm.keys());diffs={};gp_pq_diffs=[];geometry=[]
for k in sorted(bm):
 b=bm[k];p=pm[k]
 for n in gt.column_names:
  if n=='fid':continue
  pn='geometry'if n==gm['geometry_name']else n
  if b[n]!=p[pn]:gp_pq_diffs.append({'GEOID':k,'column':n,'GPKG':hashlib.sha256(b[n]).hexdigest()if isinstance(b[n],bytes)else b[n],'Parquet':hashlib.sha256(p[pn]).hexdigest()if isinstance(p[pn],bytes)else p[pn]})
for k in inter:
 a=sm[k];b=bm[k]
 for n,bn in pairs.items():
  av=a[n];bv=b[bn]
  if type(av)is not type(bv)or av!=bv:diffs.setdefault(n,[]).append({'GEOID':k,'source':av,'source_type':type(av).__name__,'published':bv,'published_type':type(bv).__name__})
 ag=shapely.from_wkb(a['wkb_geometry']);bg=shapely.from_wkb(b[gm['geometry_name']])
 geometry.append({'GEOID':k,'source_archive':a['source_archive'],'source_OGC_FID':a['OGC_FID'],'source_native_WKB_sha256':hashlib.sha256(a['wkb_geometry']).hexdigest(),'source_geometry_type':ag.geom_type,'source_coordinates':int(shapely.get_num_coordinates(ag)),'source_bounds':list(ag.bounds),'baseline_OBJECTID':b['OBJECTID'],'baseline_OID':b['OID'],'baseline_native_WKB_sha256':hashlib.sha256(b[gm['geometry_name']]).hexdigest(),'baseline_geometry_type':bg.geom_type,'baseline_coordinates':int(shapely.get_num_coordinates(bg)),'baseline_bounds':list(bg.bounds),'comparison':'nativeCRS4269vs3857; native representations differ. No reprojection/join/tolerance approved or applied to candidate.'})
roster={'source':{n:id_roster(rows,n)for n in ['source_archive','OGC_FID','STATEFP','ELSDLEA','GEOID','GEOIDFQ']},'baseline_gpkg':{n:id_roster(gr,n)for n in ['fid','GEOID','SDELM','STATE','OID','OBJECTID','id']},'baseline_parquet':{n:id_roster(pr,n)for n in ['GEOID','OID','OBJECTID','id']}}
out={'scope':'Complete preserved native source2030 and historical baseline1971; diagnostic literalGEOID only, never an accepted join/mapping','source_rows':len(rows),'source_native_crs':'EPSG:4269','source_state_counts':s.STATE_COUNTS,'source_declared_DBF_fields':s.DBF_FIELDS,'source_Arrow_schema':str(table.drop(['source_archive']).schema),'source_native_records_sha256':s.record_hash(table),'baseline_rows':len(gr),'baseline_native_crs':gm['crs'],'baseline_state_counts':dict(collections.Counter(r['STATE']for r in gr)),'baseline_GPKG_schema':str(gt.schema),'baseline_Parquet_schema':str(pt.schema),'baseline_native_GPKG_Parquet_all_shared_content_differences':gp_pq_diffs,'source_only_GEOID':sorted(sm.keys()-bm.keys()),'baseline_only_GEOID':sorted(bm.keys()-sm.keys()),'intersection_count':len(inter),'proposed_unapproved_field_name_pairs':pairs,'source_only_fields':[n for n in s.SOURCE_FIELDS if n not in pairs and n!='GEOID'],'historical_fields_not_silently_invented_or_dropped':['AREALAND','AREAWATER','BASENAME','CENTLON','CENTLAT','OID','OBJECTID','id','fid','bbox'],'all_attribute_type_and_value_differences':diffs,'difference_counts':{n:len(v)for n,v in diffs.items()},'all_original_identifier_rosters':roster,'all_intersection_native_geometry_and_original_ID_pins':geometry,'publication_approval':False}
args.output.write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:out[k]for k in ['source_rows','baseline_rows','source_native_crs','baseline_native_crs','intersection_count','source_only_GEOID','baseline_only_GEOID','difference_counts']},indent=2))
print('baseline_GP_PQ_content_diffs',len(gp_pq_diffs))
