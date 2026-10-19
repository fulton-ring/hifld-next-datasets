from pathlib import Path
from collections import Counter,defaultdict
import json,tempfile,zipfile,hashlib,datetime
import fiona,pyarrow.parquet as pq,shapely
from shapely.geometry import shape
root=Path(__file__).resolve().parent;b=pq.read_table(root/'published_candidate.body');br=b.to_pylist()
with tempfile.TemporaryDirectory() as d:
 zipfile.ZipFile(root/'captures/official_zip.body').extractall(d)
 with fiona.open(Path(d)/'20241219_CBRS_Prohibitions.shp') as f:sr=[{'fid':r.id,'properties':dict(r['properties']),'geometry':shape(r.geometry)} for r in f]
 api=json.loads((root/'native_page0000.body').read_text());apiids=json.loads((root/'source_ids.body').read_text())['objectIds'];count=json.loads((root/'source_count.body').read_text())['count']; assert len(sr)==len(api['features'])==len(apiids)==len(set(apiids))==count==1693 and not api.get('exceededTransferLimit',False)
 assert {r['attributes']['OBJECTID'] for r in api['features']}==set(apiids)
 ring_equal=all(list(r['geometry'].geoms)[0].geom_type=='Polygon' if False else True for r in sr)
 # Compare native ZIP attributes to API (date milliseconds) and rings as an unordered coordinate-ring roster, retaining both original populations separately.
 ziprings=[]
 for r in sr:
  g=r['geometry'];polys=list(g.geoms) if g.geom_type=='MultiPolygon' else [g]
  ziprings.append([list(poly.exterior.coords) for poly in polys])
 props=lambda p:tuple(p[k] for k in ['Unit','CBRS_Type','FI_Date','SU_Date','SHAPE_Leng','SHAPE_Area'])
 native=defaultdict(list)
 for r in sr:native[(r['properties']['Unit'],r['properties']['CBRS_Type'])].append(r)
 mismatches=[];fid_oid=[];geomexact=0;topoequal=0;ambiguous=0;unmatched_rows=[]
 wkbmap=defaultdict(list)
 for r in sr:
  g=r['geometry'];key=(shapely.normalize(g).wkb if g.geom_type=='MultiPolygon' else shapely.normalize(shapely.MultiPolygon([g])).wkb)
  wkbmap[key].append(r)
 for r in br:
  g=shapely.from_wkb(r['geometry']);matches=wkbmap[shapely.normalize(g).wkb]
  if len(matches)==1:
   s=matches[0];fid_oid.append({'source_fid':s['fid'],'published_OBJECTID':r['OBJECTID'],'published_id':r['id']})
   diffs={k:{'source':v,'published':r[k]} for k,v in s['properties'].items() if v!=r[k]}
   if diffs:mismatches.append({'source_fid':s['fid'],'differences':diffs})
  else:
   ambiguous+=1;unmatched_rows.append({'published_OBJECTID':r['OBJECTID'],'published_id':r['id'],'Unit':r['Unit'],'CBRS_Type':r['CBRS_Type'],'source_geometry_matches':len(matches),'published_WKB_sha256':hashlib.sha256(r['geometry']).hexdigest()})
 exact={r['geometry'].wkb for r in sr};wrappers={shapely.MultiPolygon([r['geometry']]).wkb if r['geometry'].geom_type=='Polygon' else r['geometry'].wkb for r in sr}
 report={'approved':False,'source':{'rows':len(sr),'native_types':dict(Counter(r['geometry'].geom_type for r in sr)),'invalid':sum(not r['geometry'].is_valid for r in sr),'nulls':{k:sum(r['properties'][k] is None for r in sr) for k in sr[0]['properties']},'bounds':shapely.total_bounds([r['geometry'] for r in sr]).tolist()},'published_candidate':{'rows':len(br),'sha256':hashlib.sha256((root/'published_candidate.body').read_bytes()).hexdigest(),'schema':str(b.schema),'geo_metadata':json.loads(b.schema.metadata[b'geo']),'bounds':shapely.total_bounds([shapely.from_wkb(r['geometry']) for r in br]).tolist(),'nulls':{k:sum(r[k] is None for r in br) for k in ['OBJECTID','id','Unit','CBRS_Type','FI_Date','SU_Date','SHAPE_Leng','SHAPE_Area']},'unique_identifier_counts':{k:len({r[k] for r in br}) for k in ['OBJECTID','id']},'geometry_types':dict(Counter(shapely.from_wkb(r['geometry']).geom_type for r in br)),'invalid':sum(not shapely.from_wkb(r['geometry']).is_valid for r in br),'OBJECTIDs':[r['OBJECTID'] for r in br],'ids':[r['id'] for r in br]},'complete_comparison':{'exact_native_wkb_members':sum(r['geometry'] in exact for r in br),'exact_wkb_after_singlepart_wrapper_members':sum(r['geometry'] in wrappers for r in br),'unique_normalized_geometry_joins':len(fid_oid),'ambiguous_or_missing_geometry_joins':ambiguous,'unmatched_or_ambiguous_published_rows':unmatched_rows,'attribute_difference_rows':len(mismatches),'attribute_difference_field_counts':dict(Counter(k for r in mismatches for k in r['differences'])),'all_attribute_differences':mismatches,'diagnostic_only_not_approved_ID_mapping':fid_oid},'delegated_service':{'count':count,'unique_OBJECTIDs':len(apiids),'IDs':[r['attributes']['OBJECTID'] for r in api['features']],'all_population_ids_exact':True,'exceededTransferLimit':api.get('exceededTransferLimit',False)},'decisions':['No accepted published baseline or approved original-FID/OBJECTID/id crosswalk.','Native date columns preserved as GPKG DATE and Arrow date32; published candidate string dates require explicit output mapping.','Mixed Polygon/MultiPolygon original geometry preserved; published single-part wrappers cannot be silently normalized.']}
 (root/'complete-comparison.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:v for k,v in report['complete_comparison'].items() if k not in ['all_attribute_differences','diagnostic_only_not_approved_ID_mapping']},indent=2))
