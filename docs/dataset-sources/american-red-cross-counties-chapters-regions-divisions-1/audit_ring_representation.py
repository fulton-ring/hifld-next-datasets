"""Full native ring fidelity and bounded representation diagnostics; no approved mapping."""
from pathlib import Path
import json,collections,hashlib,argparse
import pyarrow.parquet as pq
from shapely import from_wkb
from shapely.geometry import shape
from fiona.io import MemoryFile
parser=argparse.ArgumentParser();parser.add_argument('capture_directory',type=Path);parser.add_argument('--comparison',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
raw=args.capture_directory;baseline={r['GeoID']:r for r in pq.read_table(raw/'baseline.body').to_pylist()};report={'source_native_ring_sequences_exact':0,'rows':0,'shared':0,'shared_literal_ring_sequences_equal':0,'seven_topological_differences':[]};bad=set(r['GeoID'] for r in json.loads(args.comparison.read_bytes())['comparison']['all_geometry_comparisons'] if not r['topological_exact'])
def rings(geom):
 if geom.geom_type=='Polygon':return [list(geom.exterior.coords)]+[list(x.coords) for x in geom.interiors]
 return [x for part in geom.geoms for x in rings(part)]
def counter(rs):return collections.Counter(tuple(tuple(p) for p in r) for r in rs)
def canonical(r):
 r=tuple(tuple(p) for p in r);r=r[:-1] if r and r[0]==r[-1] else r;m=min(r);options=[]
 for seq in [r,tuple(reversed(r))]:
  for i,p in enumerate(seq):
   if p==m:options.append(seq[i:]+seq[:i])
 return min(options)
for offset in [0,1000,2000,3000]:
 p=raw/f'native-{offset:04}.body';j=json.loads(p.read_bytes());byid={f['attributes']['OBJECTID']:f for f in j['features']}
 with MemoryFile(p.read_bytes(),ext='.json') as m:
  with m.open(driver='ESRIJSON') as stream:
   for f in stream:
    oid=f.properties['OBJECTID'];original=byid[oid];geoid=original['attributes']['GeoID'];g=shape(f.geometry);native=counter(original['geometry']['rings']);converted=counter(rings(g));assert native==converted,(oid,geoid);report['source_native_ring_sequences_exact']+=1;report['rows']+=1
    if geoid in baseline:
     old=from_wkb(baseline[geoid]['geometry']);eq=counter(rings(old))==native;report['shared']+=1;report['shared_literal_ring_sequences_equal']+=eq
     if geoid in bad:report['seven_topological_differences'].append({'GeoID':geoid,'baseline_invalid':not old.is_valid,'source_valid':g.is_valid,'same_literal_ring_coordinate_sequences':eq,'baseline_parts':len(old.geoms),'source_parts':len(g.geoms),'baseline_ring_count':sum(counter(rings(old)).values()),'source_ring_count':sum(native.values()),'canonical_ring_sequences_equal_ignoring_start_and_direction':collections.Counter(canonical(r) for r in rings(old))==collections.Counter(canonical(r) for r in original['geometry']['rings']),'all_vertices_equal':collections.Counter(tuple(p) for r in rings(old) for p in r)==collections.Counter(tuple(p) for r in original['geometry']['rings'] for p in r)})
args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
