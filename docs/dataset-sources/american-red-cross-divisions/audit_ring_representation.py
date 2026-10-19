from pathlib import Path
import json,collections,hashlib,struct
import pyarrow.parquet as pq,shapely
import argparse
parser=argparse.ArgumentParser();parser.add_argument('capture_directory',type=Path);parser.add_argument('--output',type=Path);args=parser.parse_args();W=args.capture_directory
page=json.loads((W/'native-full.body').read_text());old={r['OBJECTID']:r for r in pq.read_table(W/'baseline.body').to_pylist()}
def key(ring):
 pts=[tuple(p) for p in ring];assert pts[0]==pts[-1];pts=pts[:-1]
 m=min(pts);a=[tuple(pts[i:]+pts[:i]) for i,p in enumerate(pts) if p==m];rev=list(reversed(pts));a.extend(tuple(rev[i:]+rev[:i]) for i,p in enumerate(rev) if p==m)
 return hashlib.sha256(b''.join(struct.pack('<dd',*p) for p in min(a))).hexdigest()
def rings(g):
 for p in shapely.get_parts(g):
  yield list(p.exterior.coords)
  for r in p.interiors:yield list(r.coords)
rows=[]
for f in page['features']:
 oid=f['attributes']['OBJECTID'];a=collections.Counter(key(r) for r in f['geometry']['rings']);g=shapely.from_wkb(old[oid]['geometry']);b=collections.Counter(key(r) for r in rings(g));rows.append({'OBJECTID':oid,'same_complete_original_ring_coordinate_multiset':a==b,'baseline_invalid_reason':shapely.is_valid_reason(g),'source_ring_count':sum(a.values()),'baseline_ring_count':sum(b.values()),'raw_coordinate_multiset_equal':collections.Counter(tuple(p) for r in f['geometry']['rings'] for p in r)==collections.Counter(tuple(p) for r in rings(g) for p in r)})
(args.output or W/'ring-representation-replay.json').write_text(json.dumps({'scope':'Diagnostic ring rotation/orientation/order only; no coordinate tolerance, geometry repair, or approved topology substitution','rows':rows},indent=2)+'\n');print(json.dumps(rows))
