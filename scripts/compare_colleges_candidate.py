"""Compare complete original NCES and published HIFLD candidates diagnostically.

Same literal ID strings are comparison witnesses, never an accepted ID mapping.
No coordinates, attributes, identifiers or original captures are modified.
"""
import argparse
import collections
import io
import json
import zipfile
import tempfile
from pathlib import Path
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

parser=argparse.ArgumentParser()
parser.add_argument('--evidence-dir',type=Path,required=True)
args=parser.parse_args();p=args.evidence_dir
outer=zipfile.ZipFile(p/'geocode2526.body');nested=outer.read('Shapefile.zip')
with tempfile.TemporaryDirectory() as temporary:
 archive=Path(temporary)/'native.zip';archive.write_bytes(nested)
 sm,source=pyogrio.read_arrow('/vsizip/{'+str(archive)+'}/EDGE_GEOCODE_POSTSECSCH_2526.shp',return_fids=True)
gz=zipfile.ZipFile(p/'baseline_native_gdb.body');gdb=gz.namelist()[0].split('/')[0]
bm,baseline=pyogrio.read_arrow('/vsizip/{'+str(p/'baseline_native_gdb.body')+'}/'+gdb,return_fids=True)
published=pq.read_table(p/'baseline_parquet.body')
source_rows=source.to_pylist();baseline_rows=baseline.to_pylist()
sids=[r['UNITID']for r in source_rows];bids=[r['IPEDSID']for r in baseline_rows]
assert len(set(sids))==len(sids) and len(set(bids))==len(bids)
sindex={r['UNITID']:r for r in source_rows};bindex={r['IPEDSID']:r for r in baseline_rows}
shared=sorted(sindex.keys()&bindex.keys());transform=pyproj.Transformer.from_crs(bm['crs'],sm['crs'],always_xy=True);geod=pyproj.Geod(ellps='WGS84')
distances=[];same_names=0;state_changes=[]
with (p/'literal-id-diagnostics.jsonl').open('w') as stream:
 for key in sorted(sindex.keys()|bindex.keys()):
  s=sindex.get(key);b=bindex.get(key)
  row={'comparison_only':True,'literal_id':key,'source_UNITID':s['UNITID']if s else None,'source_OGC_FID':s['OGC_FID']if s else None,'baseline_IPEDSID':b['IPEDSID']if b else None,'baseline_OBJECTID':b['OBJECTID']if b else None}
  if s and b:
   sg=shapely.from_wkb(s['wkb_geometry']);bg=shapely.from_wkb(b['SHAPE']);x,y=transform.transform(bg.x,bg.y);distance=abs(geod.inv(x,y,sg.x,sg.y)[2]);distances.append(distance)
   same_names+=s['NAME']==b['NAME'];row.update(diagnostic_geodesic_distance_metres=distance,baseline_crs=bm['crs'],source_crs=sm['crs'],source_name=s['NAME'],baseline_name=b['NAME'])
   if s['STATE']!=b['STATE']:state_changes.append({'literal_id':key,'source':s['STATE'],'baseline':b['STATE']})
  stream.write(json.dumps(row,sort_keys=True,allow_nan=False)+'\n')
historical=zipfile.ZipFile(p/'geocode2021.body');hname=next(n for n in historical.namelist()if n.endswith('Shapefile.zip'))
with tempfile.TemporaryDirectory() as temporary:
 archive=Path(temporary)/'historical.zip';archive.write_bytes(historical.read(hname));hm,ht=pyogrio.read_arrow('/vsizip/{'+str(archive)+'}/EDGE_GEOCODE_POSTSECSCH_2021.shp',return_fids=True)
hids=set(ht['UNITID'].to_pylist())
report={'approved':False,'classification':'Unapproved changed producer/year/schema/ID/coverage/CRS successor; no maintained ORNL continuation established.',
 'source_rows':source.num_rows,'historical_2020_2021_NCES_rows':ht.num_rows,'baseline_original_GDB_rows':baseline.num_rows,'baseline_published_Parquet_rows':published.num_rows,
 'source_original_fields':str(source.schema),'baseline_original_fields':str(baseline.schema),'baseline_published_fields':str(published.schema),
 'baseline_original_fid':'OBJECTID is intrinsic original published GDB FID, preserved in original ZIP and all diagnostic rows; absent from published Parquet; no replacement generated.',
 'literal_ID_diagnostics_only':{'shared':len(shared),'source_only':len(sindex.keys()-bindex.keys()),'baseline_only':len(bindex.keys()-sindex.keys()),'source_only_ids':sorted(sindex.keys()-bindex.keys()),'baseline_only_ids':sorted(bindex.keys()-sindex.keys()),'baseline_IDs_present_in_official_NCES2020_2021':len(hids&bindex.keys()),'baseline_IDs_absent_from_official_NCES2020_2021':sorted(bindex.keys()-hids)},
 'coverage':{'source':dict(collections.Counter(source['STATE'].to_pylist())),'baseline':dict(collections.Counter(baseline['STATE'].to_pylist())),'literal_id_state_changes':state_changes},
 'geometry_diagnostics_only':{'source_crs':sm['crs'],'baseline_crs':bm['crs'],'source_invalid':int((~shapely.is_valid(shapely.from_wkb(source['wkb_geometry'].to_pylist()))).sum()),'baseline_invalid':int((~shapely.is_valid(shapely.from_wkb(baseline['SHAPE'].to_pylist()))).sum()),'matched_ids_distance_le_1m':sum(x<=1 for x in distances),'matched_ids_distance_gt_100m':sum(x>100 for x in distances),'maximum_distance_m':max(distances)},
 'literal_same_NAME_values':same_names,'source_date':'School year2025–2026, exact release day unknown; embedded creation2026-03-05 is not publication date.',
 'release_prerequisites':['Approve NCES changed successor vs retaining ORNL enriched historical product','Approve complete raw/published schema and physical type mapping; no silent enrichment removal','Approve explicit UNITID/IPEDSID/OBJECTID/FID compatibility and migration','Approve coverage/year/CRS/geometry changes','Independent approved baseline/contract/release registry, with all shared verification gates satisfied']}
(p/'comparison.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({k:report[k]for k in ['source_rows','baseline_original_GDB_rows','geometry_diagnostics_only','literal_same_NAME_values']}))
