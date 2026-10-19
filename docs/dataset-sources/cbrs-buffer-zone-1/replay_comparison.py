import pathlib,json,tempfile,zipfile,collections,hashlib,argparse
from dagster_hifld.sources.cbrs_buffer import inspect_archive
import fiona,pyarrow.parquet as pq,shapely
from shapely.geometry import shape
parser=argparse.ArgumentParser();parser.add_argument('--native-api',type=pathlib.Path);parser.add_argument('--output',type=pathlib.Path);args=parser.parse_args()
p=pathlib.Path(__file__).parent/'evidence';baseline=pq.read_table(p/'published_candidate.body');source=json.loads(args.native_api.read_text()) if args.native_api else None
b=baseline.to_pylist();bg=[shapely.from_wkb(r['geometry']) for r in b]
r={'approved':False,'baseline':{'rows':len(b),'schema':str(baseline.schema),'sha256':hashlib.sha256((p/'published_candidate.body').read_bytes()).hexdigest(),'generation':json.loads((p/'published_candidate.receipt.json').read_text())['headers'].get('x-goog-generation'),'bounds':shapely.total_bounds(bg).tolist(),'invalid':sum(not g.is_valid for g in bg),'id_unique':{n:len({x[n] for x in b}) for n in ['id','OBJECTID']}}}
with tempfile.TemporaryDirectory() as d:
 inspect_archive((p/'captures/official_zip.body').read_bytes(),pathlib.Path(d))
 for n in ['Buffer_Zone','Polygons','Prohibitions']:
  with fiona.open(pathlib.Path(d)/f'20241219_CBRS_{n}.shp') as f:
   rows=list(f);gg=[shape(x['geometry']) for x in rows];w={g.wkb for g in gg};norm={shapely.normalize(g).wkb for g in gg}
   r[n]={'rows':len(rows),'schema':dict(f.schema),'crs':str(f.crs),'bounds':shapely.total_bounds(gg).tolist(),'invalid':sum(not g.is_valid for g in gg),'published_exact_wkb_matches':sum(g.wkb in w for g in bg),'published_normalized_wkb_matches':sum(shapely.normalize(g).wkb in norm for g in bg),'properties_first':dict(rows[0]['properties'])}
   if n=='Buffer_Zone' and source is not None:
    ring_witness=[]
    for z,s in zip(rows,source['features']):
     ring_witness.append(json.dumps(z['geometry']['coordinates'],separators=(',',':'))==json.dumps(s['geometry']['rings'],separators=(',',':')))
    r['Buffer_Zone']['api_source_rows']=len(source['features']);r['Buffer_Zone']['api_source_id_count']=len({x['attributes']['OBJECTID'] for x in source['features']});r['Buffer_Zone']['all_native_rings_equal_api_in_order']=all(ring_witness)
 r['decision']={'release_blocked':True,'reason':'Published candidate schema contains prohibition-area fields and no BUFF_DIST. Faithful buffer source has different feature grain, columns, IDs and geometry. No automatic replacement or ID crosswalk accepted.'}
if args.output:args.output.write_text(json.dumps(r,indent=2)+'\n')
assert r['baseline']['rows']==1206 and r['Buffer_Zone']['rows']==1145 and r['Buffer_Zone']['published_normalized_wkb_matches']==0 and r['Prohibitions']['published_exact_wkb_matches']==76
print(json.dumps(r,indent=2))
