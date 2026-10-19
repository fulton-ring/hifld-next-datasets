"""Replay COMPLETE source and all six published candidate files, without acceptance."""
from pathlib import Path
from collections import Counter
import hashlib,json,tempfile,zipfile
import fiona,pyarrow.parquet as pq,shapely
E=Path(__file__).resolve().parent;P=E/'published-candidate'
source=json.loads((E/'captures/features.body').read_bytes());native=source['features'];byid={f['attributes']['OBJECTID']:f for f in native};assert len(byid)==845
collection=json.loads((P/'collection.json').read_bytes());provider={};published={}
for key,asset in collection['assets'].items():
 n=key.split('-')[0];name=asset['href'].rsplit('/',1)[-1];body=(P/name).read_bytes();assert len(body)==asset['file:size'];assert asset['file:checksum']=='d50110'+hashlib.md5(body).hexdigest();provider[n]={'file':name,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'provider_size_md5_verified':True}
 if n=='geojson':
  gj=json.loads(body);gjrows=gj['features'];published[n]={'count':len(gjrows),'columns':list(gjrows[0]['properties']),'feature_ids':[f.get('id') for f in gjrows],'ids':[{k:f['properties'][k] for k in ('OBJECTID','TERMINAL_ID','OPERATOR_ID') if k in f['properties']} for f in gjrows]}
 if n=='geoparquet':
  table=pq.read_table(P/name);old=table.to_pylist();published[n]={'count':len(old),'schema':str(table.schema),'ids':[{k:r[k] for k in ('OBJECTID','TERMINAL_ID','OPERATOR_ID') if k in r} for r in old]}
 if n=='geopackage':
  with fiona.open(P/name) as f:published[n]={'count':len(f),'schema':dict(f.schema),'crs':f.crs.to_string(),'ids':[{'intrinsic_fid':r.id,**{k:r.properties[k] for k in ('OBJECTID','TERMINAL_ID','OPERATOR_ID') if k in r.properties}} for r in f]}
 if n in ('file_geodatabase','shapefile'):
  with zipfile.ZipFile(P/name) as z:
   assert z.testzip() is None;assert all(not Path(x).is_absolute() and '..' not in Path(x).parts for x in z.namelist())
   with tempfile.TemporaryDirectory() as tmp:
    z.extractall(tmp);path=next(Path(tmp).rglob('*.gdb' if n=='file_geodatabase' else '*.shp'))
    with fiona.open(path) as f:published[n]={'count':len(f),'schema':dict(f.schema),'crs':f.crs.to_string(),'ids':[{'intrinsic_fid':r.id,**{k:r.properties[k] for k in ('OBJECTID','TERMINAL_ID','OPERATOR_ID') if k in r.properties}} for r in f]}
assert all(v['count']==845 for v in published.values());common=[k for k in native[0]['attributes'] if k in old[0]];encode=lambda v:json.dumps(v,sort_keys=True,separators=(',',':'))
a=Counter(encode({k:f['attributes'][k] for k in common}) for f in native);b=Counter(encode({k:f[k] for k in common}) for f in old)
newcoords=Counter(tuple(f['geometry'][k] for k in ('x','y')) for f in native);oldcoords=Counter(tuple(shapely.from_wkb(f['geometry']).coords[0]) for f in old)
gjid={f['properties']['OBJECTID']:f for f in gjrows};assert set(gjid)==set(byid)
result={'approved':False,'classification':'unapproved_complete_published_candidate_diagnostic_no_mapping','source_count':len(native),'source_fields':source['fields'],
 'source_unique_identifiers':{k:len({f['attributes'][k] for f in native}) for k in ('OBJECTID','TERMINAL_ID','OPERATOR_ID')},
 'source_country':dict(Counter(f['attributes']['TERM_COUNTRY'] for f in native)),'source_census_years':dict(Counter(f['attributes']['CENSUS_YEAR'] for f in native)),'source_data_years':dict(Counter(f['attributes']['DATA_YEAR'] for f in native)),
 'all_six_complete_format_pins':provider,'published_formats':published,'common_attribute_columns':common,'complete_common_attribute_multiset_exact':a==b,'common_attribute_multiplicity_delta_rows':sum((a-b).values()),'native_exact_coordinate_multiset_matches':sum((newcoords&oldcoords).values()),
 'published_geojson_OBJECTID_population_exact':True,'all_source22_attributes_equal_published_geojson_by_original_objectid':all(byid[k]['attributes']==gjid[k]['properties'] for k in byid),
 'geojson_exact_point_coordinate_count':sum(tuple(byid[k]['geometry'][c] for c in ('x','y'))==tuple(gjid[k]['geometry']['coordinates']) for k in byid),
 'geojson_max_coordinate_absolute_difference':max(abs(byid[k]['geometry'][c]-gjid[k]['geometry']['coordinates'][i]) for k in byid for i,c in enumerate(('x','y'))),
 'source_declares_ncfo2020_compiled2024_10_16_but_portal_claims2022_compiled2025_10_27':True,
 'release_gate':'No source, baseline, schema/nullability/format/ID/coverage acceptance. No deduplication, invented composite IDs, country filtering, coordinate tolerance or stale metadata currency claim.'}
(E/'baseline-audit.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k not in ('source_fields','published_formats')}))
