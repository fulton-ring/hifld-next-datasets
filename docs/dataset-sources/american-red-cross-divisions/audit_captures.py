from pathlib import Path
import json,hashlib,collections
import pyarrow.parquet as pq,fiona,shapely
from fiona.io import MemoryFile
from shapely.geometry import shape
import argparse
parser=argparse.ArgumentParser();parser.add_argument('capture_directory',type=Path);parser.add_argument('--output',type=Path);args=parser.parse_args();W=args.capture_directory
for name,pin in json.loads((Path(__file__).parent/'evidence/capture-manifest.json').read_text()).items():
 b=(W/name).read_bytes();assert len(b)==pin['bytes'] and hashlib.sha256(b).hexdigest()==pin['sha256'],name
def strict(p):
 def unique(kv):
  d={}
  for k,v in kv:
   if k in d:raise ValueError('duplicate JSON '+k)
   d[k]=v
  return d
 return json.loads(p.read_bytes(),object_pairs_hook=unique,parse_constant=lambda v:(_ for _ in ()).throw(ValueError(v)))
for p in W.glob('*.receipt.json'):
 r=strict(p);b=p.with_name(p.name.replace('.receipt.json','.body')).read_bytes();assert len(b)==r['bytes'] and hashlib.sha256(b).hexdigest()==r['sha256']
layer=strict(W/'layer1.body');assert layer==strict(W/'after-layer1.body')
count=strict(W/'count.body')['count'];assert type(count)is int and count==strict(W/'after-count.body')['count']==6
ids=strict(W/'ids.body')['objectIds'];assert ids==strict(W/'after-ids.body')['objectIds']==list(range(1,7));assert strict(W/'end.body')['features']==[]
page=strict(W/'native-full.body');assert not page.get('error') and not page.get('exceededTransferLimit') and len(page['features'])==count
assert page['spatialReference']=={'wkid':102100,'latestWkid':3857}
fields=[f['name'] for f in layer['fields']];src={x['attributes']['OBJECTID']:x for x in page['features']};assert len(src)==count and list(src)==ids
for x in src.values():assert set(x['attributes'])==set(fields) and type(x['attributes']['OBJECTID'])is int and set(x['geometry'])=={'rings'}
geoms={}
with MemoryFile((W/'native-full.body').read_bytes(),ext='.json') as mem:
 with mem.open(driver='ESRIJSON') as c:
  assert c.crs.to_epsg()==3857
  for f in c:geoms[f.properties['OBJECTID']]=shape(f.geometry)
collection=strict(W/'baseline-collection.body');asset=next(a for a in collection['assets'].values() if a['type']=='application/vnd.apache.parquet')
assert asset['file:size']==(W/'baseline.body').stat().st_size
assert asset['file:checksum']=='d50110'+hashlib.md5((W/'baseline.body').read_bytes()).hexdigest()
t=pq.read_table(W/'baseline.body');old={x['OBJECTID']:x for x in t.to_pylist()};assert len(old)==6 and list(old)==ids
mapping={'OBJECTID':'OBJECTID','DCODE':'DCODE','Division':'Division','States':'States','Acres':'Acres','SQ_MI':'SQ_MI','Shape__Are':'Shape__Area','Shape__Len':'Shape__Length'}
geomrows=[];attrs=[]
for oid in ids:
 a=old[oid];b=src[oid]['attributes'];g=geoms[oid];p=shapely.from_wkb(a['geometry']);raw=src[oid]['geometry']['rings']
 rawcoords=collections.Counter(tuple(v) for ring in raw for v in ring);decoded=collections.Counter(tuple(v) for v in shapely.get_coordinates(g).tolist());assert rawcoords==decoded
 attrs.append({'OBJECTID':oid,'published_id':a['id'],'source_DCODE':b['DCODE'],'attribute_differences':{k:{'baseline':a[k],'source':b[v]} for k,v in mapping.items() if a[k]!=b[v]},'source_extra_Division_DCODE':b['Division_DCODE']})
 geomrows.append({'OBJECTID':oid,'baseline_type':p.geom_type,'source_type':g.geom_type,'baseline_valid':p.is_valid,'source_valid':g.is_valid,'source_validity':shapely.is_valid_reason(g),'baseline_vertices':int(shapely.get_num_coordinates(p)),'source_raw_vertices':sum(map(len,raw)),'source_raw_rings':len(raw),'all_raw_vertices_preserved_by_ESRIJSON_decode':True,'ordered_exact':p.equals_exact(g,0),'normalized_exact':p.equals_exact(g,0,normalize=True),'topological_exact':p.equals(g),'baseline_bounds':list(p.bounds),'source_bounds':list(g.bounds)})
result={'scope':'Complete candidate comparison only; no source/baseline/contract approval or pipeline claimed','baseline':{'rows':len(t),'bytes':(W/'baseline.body').stat().st_size,'sha256':hashlib.sha256((W/'baseline.body').read_bytes()).hexdigest(),'arrow_schema':str(t.schema),'geo_metadata':json.loads(t.schema.metadata[b'geo']),'all_id_equals_OBJECTID':all(x['id']==x['OBJECTID'] for x in old.values())},'source':{'rows':count,'fields':layer['fields'],'geometry_crs':page['spatialReference'],'stable_before_after_count_ids_schema_edit':True,'all_original_identifiers_retained':True,'complete_terminal_empty_page':True,'no_transfer_limit_or_error':True,'release_claim_conflict':{'item_title':strict(W/'item.body')['title'],'service_description':strict(W/'service.body')['description']},'row_states':{str(k):v['attributes']['States'] for k,v in src.items()}},'diagnostic_only_attribute_mapping':mapping,'all_row_attribute_comparisons':attrs,'all_row_geometry_comparisons':geomrows,'physical_type_migrations_requiring_review':{'Acres':'source esriFieldTypeDouble, published int64','SQ_MI':'source esriFieldTypeDouble, published int64','OBJECTID':'source esriFieldTypeOID, published int64','Shape__Area/Length':'source full names versus published truncated Shape__Are/Shape__Len','Division_DCODE':'source extra string absent published; no silent drop','id/bbox':'published extra fields absent source; no invented substitutions'},'tools':{'fiona':fiona.__version__,'gdal':fiona.__gdal_version__,'shapely':shapely.__version__}}
(args.output or W/'full-comparison-replay.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'rows':count,'attribute_differences':sum(len(x['attribute_differences']) for x in attrs),'exact_geometries':sum(x['ordered_exact'] for x in geomrows),'normalized_exact':sum(x['normalized_exact'] for x in geomrows),'topological_exact':sum(x['topological_exact'] for x in geomrows),'allrawverticespreserved':True,'sourceinvalid':sum(not x['source_valid'] for x in geomrows)}))
