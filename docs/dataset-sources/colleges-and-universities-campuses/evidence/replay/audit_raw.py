from pathlib import Path
import json,zipfile,io,struct,collections
from fiona.io import ZipMemoryFile
from shapely.geometry import shape
import shapely,pyarrow.parquet as pq
import argparse, hashlib
from dagster_hifld.sources.campus_preservation_spec import INPUTS
parser=argparse.ArgumentParser(description='Read complete pinned original inputs; output diagnostics only, no accepted mapping/tolerance.')
parser.add_argument('--capture-dir',type=Path,default=Path(__file__).parents[1]/'captures')
parser.add_argument('--output-dir',type=Path,required=True)
args=parser.parse_args();W=args.capture_dir;O=args.output_dir;O.mkdir(parents=True,exist_ok=True)
for name in ['baseline','pasda-original-zip','pasda-hifld-zip']:
 body=(W/(name+'.body')).read_bytes();assert len(body)==INPUTS[name]['bytes'] and hashlib.sha256(body).hexdigest()==INPUTS[name]['sha256']
out={}
for name in ['pasda-original-zip','pasda-hifld-zip']:
 b=(W/(name+'.body')).read_bytes();z=zipfile.ZipFile(io.BytesIO(b));shpname=next(n for n in z.namelist()if n.endswith('.shp'));s=z.read(shpname);assert len(s)==struct.unpack_from('>i',s,24)[0]*2;assert struct.unpack_from('<i',s,32)[0]==5;raw=[];off=100
 while off<len(s):
  number,length=struct.unpack_from('>ii',s,off);payload=s[off+8:off+8+length*2];assert len(payload)==length*2 and struct.unpack_from('<i',payload)[0]==5
  parts,points=struct.unpack_from('<ii',payload,36);starts=struct.unpack_from('<'+'i'*parts,payload,44);assert starts[0]==0 and list(starts)==sorted(set(starts));start=44+parts*4;coords=[struct.unpack_from('<dd',payload,start+i*16)for i in range(points)];assert start+points*16==len(payload)
  raw.append({'record_number':number,'points':points,'parts':parts,'coordinates':collections.Counter(coords)});off+=8+length*2
 assert off==len(s)and len(raw)==5898
 rows=[]
 with ZipMemoryFile(b)as m:
  with m.open(shpname)as c:
   for i,f in enumerate(c):
    g=shape(f.geometry);v=collections.Counter(tuple(p)for p in shapely.get_coordinates(g));r=raw[i];rows.append({'UNIQUEID':f.properties['UNIQUEID'],'original_fid':f.id,'shp_record_number':r['record_number'],'raw_points':r['points'],'raw_parts':r['parts'],'raw_coordinate_multiplicity_conserved':r['coordinates']==v})
 out[name]={'rows':len(rows),'all_raw_coordinate_multiplicities_conserved':all(r['raw_coordinate_multiplicity_conserved']for r in rows),'all_rows':rows}
t=pq.read_table(W/'baseline.body');rows=t.to_pylist();groups=collections.defaultdict(list)
for r in rows:groups[r['id']].append(r['UNIQUEID'])
duplicates={str(k):v for k,v in groups.items()if len(v)>1};out['published_ids']={'rows':len(rows),'unique':len(groups),'min':min(groups),'max':max(groups),'duplicate_groups':duplicates,'scope':'All duplicate original published IDs retained; no replacement ID generated; original UNIQUEID is unique but no acceptance of alternative output mapping'}
(O/'raw-native-and-published-id-audit.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({k:{a:b for a,b in v.items()if a!='all_rows'}for k,v in out.items()}))
