"""Complete native and all row-format comparison; joins are diagnostic and unapproved."""
from pathlib import Path
import json,collections,hashlib,sqlite3,io,zipfile
import pyarrow.parquet as pq,pyogrio,shapely
from fiona.io import MemoryFile,ZipMemoryFile
import argparse
parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--capture-dir',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();W=args.capture_dir.resolve()
from dagster_hifld.sources.fema_regions import acquire
acquire(W) # Complete pinned inputs, bounded regular reads and original receipts before parsing.
P='federal-emergency-management-agency-fema-regions';baseline=pq.read_table(W/'baseline-parquet.body');old={r['objectid']:r for r in baseline.to_pylist()};result={'scope':'Complete producer-native query pages and every captured published format; diagnostic joins only, allmappings/schema/IDs/geometry acceptance pending','source':[],'published_parquet':{'rows':len(baseline),'schema':str(baseline.schema)}}
def rings(g):
 if g.geom_type=='Polygon':return [tuple(g.exterior.coords)]+[tuple(r.coords)for r in g.interiors]
 if g.geom_type=='MultiPolygon':return [r for p in g.geoms for r in rings(p)]
 raise ValueError(g.geom_type)
for oid in range(11,21):
 path=W/f'source-oid-{oid}.body';raw=path.read_bytes();v=json.loads(raw);assert len(v['features'])==1 and not v.get('exceededTransferLimit');f=v['features'][0];a=f['attributes'];assert a['objectid']==oid
 with MemoryFile(raw,ext='.json')as mem:
  with mem.open(driver='ESRIJSON')as layer:
   q=next(iter(layer));g=shapely.geometry.shape(q['geometry']);schema=dict(layer.schema);crs=str(layer.crs)
 rawrings=[tuple(tuple(c)for c in r)for r in f['geometry']['rings']];decoded=rings(g);same=collections.Counter(rawrings)==collections.Counter(decoded);assert same
 before=old[oid];bg=shapely.from_wkb(before['geometry']);mapping={'objectid':'objectid','region':'region','Shape__Area':'Shape__Are','Shape__Length':'Shape__Len','cnt_region':'cnt_region','website':'website'}
 diffs={n:{'source':a[n],'published':before[o]}for n,o in mapping.items()if a[n]!=before[o]}
 result['source'].append({'objectid':oid,'attributes':a,'published_id':before['id'],'native_CRS':crs,'source_schema':schema,'rings':len(rawrings),'vertices':sum(map(len,rawrings)),'ordered_ring_bag_exact':same,'valid':g.is_valid,'published_valid':bg.is_valid,'source_geometry_type':g.geom_type,'published_geometry_type':bg.geom_type,'geometry_exact':g.equals_exact(bg,0),'geometry_normalized_exact':g.equals_exact(bg,0,normalize=True),'native_WKB_sha256':hashlib.sha256(shapely.to_wkb(g)).hexdigest(),'published_WKB_sha256':hashlib.sha256(before['geometry']).hexdigest(),'attribute_differences':diffs,'bounds_source':g.bounds,'bounds_published':bg.bounds})
result['other_published_formats']={}
for kind in ['geopackage','geojson','shapefile']:
 path=next(W.glob('baseline-'+kind+'__*.body'))
 if kind=='shapefile':
  raw=path.read_bytes()
  with ZipMemoryFile(raw)as mem:
   with mem.open(next(n for n in zipfile.ZipFile(io.BytesIO(raw)).namelist()if n.endswith('.shp')))as c:records=[{'id':r.id,'attributes':dict(r.properties),'geometry':shapely.geometry.shape(r.geometry)}for r in c];schema=dict(c.schema);crs=str(c.crs)
 else:
  import fiona
  with fiona.open(path,driver='GPKG'if kind=='geopackage'else'GeoJSON')as c:records=[{'id':r.id,'attributes':dict(r.properties),'geometry':shapely.geometry.shape(r.geometry)}for r in c];schema=dict(c.schema);crs=str(c.crs)
 result['other_published_formats'][kind]={'rows':len(records),'schema':schema,'CRS':crs,'all_original_ids_attrs':[{'fid':r['id'],'attributes':r['attributes'],'vertices':int(shapely.get_num_coordinates(r['geometry'])),'geometry_WKB_sha256':hashlib.sha256(shapely.to_wkb(r['geometry'])).hexdigest()}for r in records]}
 if kind=='geopackage':
  with sqlite3.connect(path)as db:result['other_published_formats'][kind]['sqlite_columns']=db.execute('PRAGMA table_info("federal-emergency-management-agency-fema-regions")').fetchall()
for kind,fmt in result['other_published_formats'].items():
 rows=fmt.pop('all_original_ids_attrs');fmt['all_original_attributes_and_reader_identifiers']=rows
 fmt['reader_identifier_role']={'geojson':'Fiona reader-assigned record ordinal; NO persisted GeoJSON feature id or id property.','shapefile':'Shapefile record ordinal, distinct from original attributes.id.','geopackage':'GeoPackage storage primary key, distinct from attributes.id and source objectid.'}[kind]
 for row in rows:row['reader_identifier']=row.pop('fid')
result['all10_native_ordered_rings_exact']=all(r['ordered_ring_bag_exact']for r in result['source']);result['native_attribute_differences']=sum(bool(r['attribute_differences'])for r in result['source']);result['native_geometry_exact_count']=sum(r['geometry_exact']for r in result['source']);result['native_geometry_normalized_exact_count']=sum(r['geometry_normalized_exact']for r in result['source'])
args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items()if k not in ['source','other_published_formats','published_parquet']}));print([(r['objectid'],r['vertices'],r['valid'],r['geometry_exact'],r['attribute_differences'])for r in result['source']])
