import json,tempfile,zipfile,collections,hashlib
from pathlib import Path
import fiona,pyarrow.parquet as pq,shapely
from shapely.geometry import shape
r=Path(__file__).resolve().parent
with tempfile.TemporaryDirectory() as d:
 zipfile.ZipFile(r/'captures/official_zip.body').extractall(d)
 with fiona.open(Path(d,'20241219_CBRS_Polygons.shp')) as f:src=[{'fid':x.id,'properties':dict(x['properties']),'geometry':shape(x['geometry'])} for x in f]
b=pq.read_table(r/'published_candidate.body').to_pylist();a=json.loads((r/'native_page0000.body').read_text())['features']
source={x['properties']['Unit']:x for x in src};published={x['Unit']:x for x in b};api={x['attributes']['Unit']:x for x in a}
assert len(source)==len(published)==len(api)==len(src)==len(b)==len(a)==942
report={'approved':False,'join':'Unique exact Unit value, diagnostic only; no identifier crosswalk accepted','source_units':sorted(source),'published_units':sorted(published),'api_units':sorted(api),'source_only':sorted(source.keys()-published.keys()),'published_only':sorted(published.keys()-source.keys()),'by_unit':[]}
for unit,s in sorted(source.items()):
 p=published.get(unit);apirow=api.get(unit)
 differences={k:{'source':v,'published':p.get(k)} for k,v in s['properties'].items() if p is not None and v!=p.get(k)}
 pg=shapely.from_wkb(p['geometry']) if p else None
 ag=apirow['attributes'] if apirow else {}
 ad={k:{'source':v,'api':ag.get({'Shape_Leng':'Shape_Length'}.get(k,k))} for k,v in s['properties'].items() if v!=ag.get({'Shape_Leng':'Shape_Length'}.get(k,k))}
 report['by_unit'].append({'Unit':unit,'original_fid':s['fid'],'published_OBJECTID':p.get('OBJECTID') if p else None,'published_id':p.get('id') if p else None,'api_OBJECTID':ag.get('OBJECTID'),'attribute_differences':differences,'api_attribute_differences':ad,'native_geometry_type':s['geometry'].geom_type,'published_geometry_type':pg.geom_type if pg else None,'exact_wkb':pg.wkb==s['geometry'].wkb if pg else False,'topologically_equal_native_crs':pg.equals(s['geometry']) if pg else False})
report['summary']={'unique_Unit_942_each':True,'attribute_difference_counts':dict(collections.Counter(k for x in report['by_unit'] for k in x['attribute_differences'])),'topologically_equal_native_crs':sum(x['topologically_equal_native_crs'] for x in report['by_unit']),'native_fid_equal_published_OBJECTID':sum(x['original_fid']==str(x['published_OBJECTID']) for x in report['by_unit']),'native_fid_equal_published_id':sum(x['original_fid']==str(x['published_id']) for x in report['by_unit']),'api_OBJECTID_equal_published_OBJECTID':sum(x['api_OBJECTID']==x['published_OBJECTID'] for x in report['by_unit'])}
(r/'unit-key-comparison.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report['summary'],indent=2))
