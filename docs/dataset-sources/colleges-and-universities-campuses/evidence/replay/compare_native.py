from pathlib import Path
import io,json,collections,zipfile,hashlib,struct
import fiona,pyarrow.parquet as pq,shapely
from fiona.io import ZipMemoryFile
from shapely.geometry import shape
import argparse, hashlib
from dagster_hifld.sources.campus_preservation_spec import INPUTS
parser=argparse.ArgumentParser(description='Read complete pinned original inputs; output diagnostics only, no accepted mapping/tolerance.')
parser.add_argument('--capture-dir',type=Path,default=Path(__file__).parents[1]/'captures')
parser.add_argument('--output-dir',type=Path,required=True)
args=parser.parse_args();W=args.capture_dir;O=args.output_dir;O.mkdir(parents=True,exist_ok=True)
for name in ['baseline','pasda-original-zip','pasda-hifld-zip']:
 body=(W/(name+'.body')).read_bytes();assert len(body)==INPUTS[name]['bytes'] and hashlib.sha256(body).hexdigest()==INPUTS[name]['sha256']
def sha(b):return hashlib.sha256(b).hexdigest()
def dbf_header(b):
 h=int.from_bytes(b[8:10],'little');return {'count':int.from_bytes(b[4:8],'little'),'record_bytes':int.from_bytes(b[10:12],'little'),'fields':[{'name':b[i:i+11].split(b'\0')[0].decode(),'type':chr(b[i+11]),'width':b[i+16],'decimals':b[i+17]} for i in range(32,h-1,32)]}
archives={};objects={}
for name in ['pasda-original-zip','pasda-hifld-zip']:
 b=(W/(name+'.body')).read_bytes();z=zipfile.ZipFile(io.BytesIO(b));assert z.testzip() is None;assert len(z.namelist())==len(set(z.namelist()));shp=next(n for n in z.namelist() if n.lower().endswith('.shp'));dbf=z.read(next(n for n in z.namelist() if n.lower().endswith('.dbf')));header=dbf_header(dbf);rows=[]
 with ZipMemoryFile(b) as mem:
  with mem.open(shp) as c:
   schema=c.schema;crs=c.crs.to_epsg();bounds=c.bounds
   for f in c: rows.append({'fid':f.id,'attributes':dict(f.properties),'geometry':shape(f.geometry)})
 assert len(rows)==header['count']==5898;assert len({r['fid'] for r in rows})==5898
 byid={r['attributes']['UNIQUEID']:r for r in rows};assert len(byid)==5898;assert all(type(x)is str for x in byid)
 objects[name]=byid;archives[name]={'bytes':len(b),'sha256':sha(b),'members':{x.filename:{'bytes':x.file_size,'sha256':sha(z.read(x))} for x in z.infolist()},'dbf':header,'schema':schema,'EPSG':crs,'bounds':bounds,'features':len(rows),'source_fid_unique':True,'UNIQUEID_unique_string':True,'state_counts':dict(sorted(collections.Counter(r['attributes']['STATE'] for r in rows).items())),'invalid_geometry_count':sum(not r['geometry'].is_valid for r in rows)}
t=pq.read_table(W/'baseline.body');rows=t.to_pylist();byid={r['UNIQUEID']:r for r in rows};assert len(byid)==len(rows)==5898
source=objects['pasda-hifld-zip'];shared=set(source)&set(byid);assert len(shared)==5898
fields=[n for n in t.column_names if n not in {'geometry','id','bbox'}];assert len(fields)==24
reports=[];diffcounts=collections.Counter();geomstats=collections.Counter();nullstats={};statecounts=collections.Counter();pubids=[]
for uid in sorted(shared):
 old=byid[uid];new=source[uid];a=new['attributes'];g=new['geometry'];p=shapely.from_wkb(old['geometry']);diff={k:{'published':old[k],'archive':a[k]} for k in fields if old[k]!=a[k]};diffcounts.update(diff);statecounts.update([a['STATE']]);pubids.append(old['id'])
 entry={'UNIQUEID':uid,'published_id':old['id'],'archive_original_fid':new['fid'],'attribute_differences':diff,'ordered_exact_geometry':p.equals_exact(g,0),'normalized_exact_geometry':p.equals_exact(g,0,normalize=True),'baseline_valid':p.is_valid,'archive_valid':g.is_valid,'baseline_type':p.geom_type,'archive_type':g.geom_type,'baseline_vertices':int(shapely.get_num_coordinates(p)),'archive_vertices':int(shapely.get_num_coordinates(g)),'exact_coordinate_multiset':collections.Counter(tuple(x) for x in shapely.get_coordinates(p))==collections.Counter(tuple(x) for x in shapely.get_coordinates(g))}
 reports.append(entry)
 for k in ['ordered_exact_geometry','normalized_exact_geometry','exact_coordinate_multiset']:
  if entry[k]:geomstats[k]+=1
 for k in ['baseline_valid','archive_valid']:
  if not entry[k]:geomstats[k+'_false']+=1
for name in fields:nullstats[name]={'published_nulls':sum(r[name]is None for r in rows),'archive_nulls':sum(r['attributes'][name]is None for r in source.values())}
result={'scope':'Complete historical archive candidates compared with complete unapproved published candidate; no maintained official source or acceptance inferred','baseline':{'rows':len(rows),'bytes':(W/'baseline.body').stat().st_size,'sha256':sha((W/'baseline.body').read_bytes()),'arrow_schema':str(t.schema),'geo':json.loads(t.schema.metadata[b'geo']),'published_id_unique':len(set(pubids))==len(pubids),'published_id_equals_archive_FID_all':all(str(r['published_id'])==r['archive_original_fid'] for r in reports)},'archives':archives,'comparison':{'diagnostic_join':'Exact original UNIQUEID string values only; sourceFID and publishedid retained separately, no substitute IDs generated','shared_original_UNIQUEIDs':len(shared),'all24_field_differences':dict(diffcounts),'null_stats':nullstats,'allgeometry_counts':dict(geomstats),'state_counts':dict(sorted(statecounts.items())),'source_extra_fields':sorted(set(next(iter(source.values()))['attributes'])-set(fields)),'all5898row_comparisons':reports},'tools':{'fiona':fiona.__version__,'gdal':fiona.__gdal_version__,'shapely':shapely.__version__}}
(O/'full-comparison.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'rows':5898,'diffcounts':dict(diffcounts),'geometry':dict(geomstats),'publishedid_equals_originalFID':result['baseline']['published_id_equals_archive_FID_all'],'states':len(statecounts),'source_extra':result['comparison']['source_extra_fields']}))
