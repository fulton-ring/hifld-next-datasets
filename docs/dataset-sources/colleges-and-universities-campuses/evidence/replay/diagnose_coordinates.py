from pathlib import Path
import json,io,zipfile,collections,math
import pyarrow.parquet as pq,shapely,numpy as np
from shapely.ops import transform
from shapely.geometry import shape
from pyproj import Transformer
from fiona.io import ZipMemoryFile
import argparse, hashlib
from dagster_hifld.sources.campus_preservation_spec import INPUTS
parser=argparse.ArgumentParser(description='Read complete pinned original inputs; output diagnostics only, no accepted mapping/tolerance.')
parser.add_argument('--capture-dir',type=Path,default=Path(__file__).parents[1]/'captures')
parser.add_argument('--output-dir',type=Path,required=True)
args=parser.parse_args();W=args.capture_dir;O=args.output_dir;O.mkdir(parents=True,exist_ok=True)
for name in ['baseline','pasda-original-zip','pasda-hifld-zip']:
 body=(W/(name+'.body')).read_bytes();assert len(body)==INPUTS[name]['bytes'] and hashlib.sha256(body).hexdigest()==INPUTS[name]['sha256']
old={r['UNIQUEID']:r for r in pq.read_table(W/'baseline.body').to_pylist()};tr=Transformer.from_crs(4326,3857,always_xy=True);out={}
for name in ['pasda-original-zip','pasda-hifld-zip']:
 data=(W/(name+'.body')).read_bytes();z=zipfile.ZipFile(io.BytesIO(data));shp=next(n for n in z.namelist()if n.endswith('.shp'));rows=[]
 with ZipMemoryFile(data)as mem:
  with mem.open(shp)as c:
   crs=c.crs.to_epsg()
   for f in c:
    uid=f.properties['UNIQUEID'];g=shape(f.geometry);q=transform(tr.transform,g)if crs==4326 else g;q=shapely.MultiPolygon([q])if q.geom_type=='Polygon'else q;p=shapely.from_wkb(old[uid]['geometry']);a=shapely.get_coordinates(shapely.normalize(p));b=shapely.get_coordinates(shapely.normalize(q));same=len(a)==len(b)
    rows.append({'UNIQUEID':uid,'same_normalized_vertex_count':same,'normalized_order_Linf_meter_difference':float(np.max(np.abs(a-b)))if same else None,'max_bounds_meter_difference':max(abs(x-y)for x,y in zip(p.bounds,q.bounds))})
 out[name]={'scope':'Descriptive coordinate differences only; normalize changes ordering, not coordinates; native4326 diagnostic projection explicit; no tolerance accepted, no geometry repair or output substitution','all_rows':rows,'same_count_rows':sum(x['same_normalized_vertex_count']for x in rows),'max_normalized_order_difference_meters':max(x['normalized_order_Linf_meter_difference']for x in rows if x['same_normalized_vertex_count']),'max_bounds_difference_meters':max(x['max_bounds_meter_difference']for x in rows)}
(O/'coordinate-drift.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({k:{a:b for a,b in v.items()if a!='all_rows'}for k,v in out.items()}))
