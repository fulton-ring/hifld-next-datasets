"""Offline full-input comparison; ECODE is a diagnostic alignment, never an approved join."""
import argparse,json,hashlib,collections,math,struct,sqlite3,zipfile
from pathlib import Path
import pyarrow.parquet as pq
import pyogrio,shapely
from pyproj import Transformer
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--capture-root',type=Path,default=Path(__file__).resolve().parent/'evidence')
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
R=args.capture_root;B=R/'baseline';D=R/'discovery'
j=lambda p:json.loads(p.read_bytes())
source=j(R/'full-features.json')
layer=j(D/'layer_before.body');rows=[x['attributes']for x in source]
fields=layer['fields'];assert len(source)==226 and len(fields)==19
ids=j(D/'ids_before.body')['objectIds'];assert all(type(x)is int for x in ids) and len(set(ids))==226
assert sorted(ids)==[x['OBJECTID']for x in rows]
original_page_rows=[]
for offset in range(0,226,100):
 page=j(D/('page_'+str(offset).zfill(4)+'.body'))
 assert not page.get('exceededTransferLimit') and 'error' not in page
 assert [f['attributes']['OBJECTID']for f in page['features']]==sorted(ids)[offset:offset+100]
 original_page_rows.extend(page['features'])
assert original_page_rows==source, 'derived feature list must match every original source response value'
assert all(type(x)is int for x in j(D/'ids_after.body')['objectIds'])
assert len(j(D/'ids_after.body')['objectIds'])==len(set(j(D/'ids_after.body')['objectIds']))==226
assert sorted(j(D/'ids_after.body')['objectIds'])==sorted(ids)
assert j(D/'count_before.body')['count']==j(D/'count_after.body')['count']==226
assert layer['fields']==j(D/'layer_after.body')['fields'] and layer['editingInfo']==j(D/'layer_after.body')['editingInfo']
assert all(set(r)=={f['name']for f in fields}for r in rows)
gpath=next(B.glob('geopackage*.body'));gmeta,gtable=pyogrio.read_arrow(gpath,return_fids=True);grows=gtable.to_pylist()
ppath=next(B.glob('geoparquet*.body'));ptable=pq.read_table(ppath);prows=ptable.to_pylist()
geo=j(next(B.glob('geojson*.body')))['features']
assert len(grows)==len(prows)==len(geo)==226
# Source declarations retained verbatim; no writer casts, coordinate transforms or ID replacements.
mapnames={'Region_Type':'Region_Typ','Chapter_Address':'Chapter_Ad','Chapter_Address_2':'Chapter__1','Chapter_City':'Chapter_Ci','Chapter_State':'Chapter_St','Chapter_Zip':'Chapter_Zi','Chapter_Phone':'Chapter_Ph','FEMA_Region':'FEMA_Regio'}
sm={x['attributes']['ECODE']:x for x in source};gm={x['ECODE']:x for x in grows};pm={x['ECODE']:x for x in prows}
assert len(sm)==len(gm)==len(pm)==226
inter=sorted(sm.keys()&gm.keys()&pm.keys())
attrdiff={};typediff={};idrows=[];geodiff=[];baseline_gp_parq_diffs=[]
for key in inter:
 s=sm[key];g=gm[key];p=pm[key];a=s['attributes']
 idrows.append({'ECODE':key,'source_OBJECTID':a['OBJECTID'],'baseline_GPKG_OBJECTID':g['OBJECTID'],'baseline_GPKG_fid':g['fid'],'baseline_GPKG_id':g['id'],'baseline_Parquet_OBJECTID':p['OBJECTID'],'baseline_Parquet_id':p['id']})
 for f in fields:
  name=f['name'];other=mapnames.get(name,name);sv=a[name];gv=g[other];pv=p[other]
  if sv!=gv:attrdiff.setdefault(name,[]).append({'ECODE':key,'source':sv,'baseline':gv})
  if type(sv)is not type(gv):typediff.setdefault(name,[]).append({'ECODE':key,'source_type':type(sv).__name__,'baseline_type':type(gv).__name__,'source':sv,'baseline':gv})
  if gv!=pv:baseline_gp_parq_diffs.append({'ECODE':key,'field':other,'GPKG':gv,'Parquet':pv})
 gg=shapely.from_wkb(g['geom']);pg=shapely.from_wkb(p['geometry'])
 if not shapely.equals_exact(gg,pg,0):baseline_gp_parq_diffs.append({'ECODE':key,'field':'native_geometry','GPKG':shapely.to_wkt(gg,rounding_precision=-1),'Parquet':shapely.to_wkt(pg,rounding_precision=-1)})
 if 'geometry'not in s or s['geometry']is None:
  geodiff.append({'ECODE':key,'source_geometry_member_present':'geometry'in s,'source_geometry':s.get('geometry'),'baseline_geometry':shapely.to_wkt(gg,rounding_precision=-1),'source_OBJECTID':a['OBJECTID']})
 else:
  sg=s['geometry'];assert set(sg)=={'x','y'}
  exact=struct.pack('<BIdd',1,1,sg['x'],sg['y'])==g['geom']
  if not exact:geodiff.append({'ECODE':key,'source_xy':[sg['x'],sg['y']],'baseline_xy':list(gg.coords[0]),'native_euclidean_distance':math.dist([sg['x'],sg['y']],gg.coords[0])})
# literal all source ID rosters, source strings/nulls and native type declarations.
def roster(rr,col):
 v=[r[col]for r in rr];c=collections.Counter(v)
 return {'row_count':len(v),'unique':len(c),'nulls':c.get(None,0),'leading_zero_count':sum(isinstance(x,str)and x.startswith('0')for x in v),'duplicates':[{'value':k,'occurrences':n}for k,n in c.items()if n>1],'all_values_in_original_order':v}
rosters={'source':{f:roster(rows,f)for f in ['OBJECTID','ECODE','RCODE','DCODE','Chapter_Zip']},'baseline_gpkg':{f:roster(grows,f)for f in ['fid','OBJECTID','id','ECODE']},'baseline_parquet':{f:roster(prows,f)for f in ['OBJECTID','id','ECODE']}}
# Read actual original GeoPackage physical column declarations, not inferred fiona casts.
con=sqlite3.connect(f'file:{gpath}?mode=ro',uri=True)
table=con.execute('select table_name from gpkg_contents').fetchone()[0]
sqlcols=[dict(zip(['cid','name','type','notnull','default','pk'],r))for r in con.execute('pragma table_info("'+table+'")')]
contents=con.execute('select * from gpkg_contents').fetchall();con.close()
# Baseline SHP/DBF complete original physical schema with no archive extraction.
zp=next(B.glob('shapefile*.body'))
with zipfile.ZipFile(zp)as z:
 names=z.namelist();dbfn=next(x for x in names if x.lower().endswith('.dbf'));dbf=z.read(dbfn)
 dbfcount=struct.unpack('<I',dbf[4:8])[0];hlen,rlen=struct.unpack('<HH',dbf[8:12]);dbfs=[]
 for off in range(32,hlen-1,32):
  f=dbf[off:off+32];dbfs.append({'name':f[:11].split(b'\0')[0].decode('ascii'),'type':chr(f[11]),'length':f[16],'decimals':f[17]})
 assert dbfcount==226 and len(dbf)>=hlen+dbfcount*rlen
 shapefilen=next(x for x in names if x.lower().endswith('.shp'));shp=z.read(shapefilen)
 shpcount=0;off=100
 while off<len(shp):
  _,words=struct.unpack('>II',shp[off:off+8]);off+=8+2*words;shpcount+=1
 assert off==len(shp)and shpcount==226
collection=j(B/'collection.body')
pins=[]
for name,a in collection['assets'].items():
 b=(B/(name+'.body')).read_bytes();assert len(b)==a['file:size']and a['file:checksum']=='d50110'+hashlib.md5(b).hexdigest()
 pins.append({'asset':name,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest(),'collection_checksum':a['file:checksum'],'verified':True})
out={'scope':'complete original source and complete historical baseline candidates; no authority, join, mapping or release approval','source':{'rows':226,'fields':fields,'native_spatial_reference':layer['extent']['spatialReference'],'point_rows':sum(bool(x.get('geometry'))for x in source),'absent_geometry_rows':[x for x in source if 'geometry'not in x],'coverage_counts':dict(collections.Counter(r['Chapter_State']for r in rows)),'region_type_counts':dict(collections.Counter(r['Region_Type']for r in rows)),'nulls':{f['name']:sum(r[f['name']]is None for r in rows)for f in fields},'python_physical_value_types':{f['name']:dict(collections.Counter(type(r[f['name']]).__name__ for r in rows))for f in fields}},'baseline':{'gpkg_rows':len(grows),'gpkg_crs':gmeta['crs'],'gpkg_sql_columns':sqlcols,'gpkg_arrow_schema':str(gtable.schema),'gpkg_native_empty_points':sum(shapely.from_wkb(r['geom']).is_empty for r in grows),'gpkg_native_null_geometry':sum(r['geom']is None for r in grows),'parquet_schema':str(ptable.schema),'gpkg_parquet_shared_content_diffs':baseline_gp_parq_diffs,'geojson_rows':len(geo),'shapefile_rows':shpcount,'dbf_rows':dbfcount,'dbf_fields':dbfs,'complete_asset_pins':pins},'comparison':{'diagnostic_key':'literal ECODE intersection only, NOT an approved source↔published ID join','source_only_ECODE':sorted(sm.keys()-gm.keys()),'baseline_only_ECODE':sorted(gm.keys()-sm.keys()),'intersection_count':len(inter),'proposed_name_mapping_unapproved':mapnames,'attribute_difference_counts':{k:len(v)for k,v in attrdiff.items()},'all_attribute_differences':attrdiff,'all_value_type_differences':typediff,'all_geometry_differences':geodiff,'geometry_difference_count':len(geodiff),'all_id_comparisons':idrows,'source_vs_baseline_OBJECTID_different_count':sum(x['source_OBJECTID']!=x['baseline_GPKG_OBJECTID']for x in idrows)},'identifier_rosters':rosters}
args.output.write_text(json.dumps(out,indent=2,ensure_ascii=False)+'\n')
print(json.dumps({'rows':226,'source_points':225,'source_absentgeometry':1,'baseline_points':226,'baseline_empty':out['baseline']['gpkg_native_empty_points'],'ECODE_intersection':len(inter),'ECODE_sourceonly':out['comparison']['source_only_ECODE'],'ECODE_baselineonly':out['comparison']['baseline_only_ECODE'],'OBJECTID_diffs':out['comparison']['source_vs_baseline_OBJECTID_different_count'],'attribute_diffs':out['comparison']['attribute_difference_counts'],'native_geometry_diffs':len(geodiff),'baselineGPParqdiffs':len(baseline_gp_parq_diffs)},indent=2))
