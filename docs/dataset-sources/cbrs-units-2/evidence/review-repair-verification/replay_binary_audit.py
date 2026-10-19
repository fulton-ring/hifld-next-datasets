"""Independent binary DBF/SHP audit; no adapter decoder imports."""
import collections,hashlib,io,json,struct,zipfile
from pathlib import Path
import pyarrow.parquet as pq
import shapely
import pyproj
BASE=Path('/workspace/hifld-worker-worktrees/31/docs/dataset-sources/cbrs-units-2/evidence')
RUNTIME=Path('/workspace/work/hifld-coordinator/31/graph-review-repair')
body=(BASE/'captures/official_zip.body').read_bytes();assert len(body)==32280370 and hashlib.sha256(body).hexdigest()=='8765dd5c6d8d0404d321b28af33236956de2daa8f43b81d61191f8f49eafabd0'
with zipfile.ZipFile(io.BytesIO(body)) as z:
 assert len(z.namelist())==25 and len(set(z.namelist()))==25
 dbf=z.read('20241219_CBRS_Polygons.dbf');shp=z.read('20241219_CBRS_Polygons.shp');shx=z.read('20241219_CBRS_Polygons.shx');prj=z.read('20241219_CBRS_Polygons.prj')
 n,header,width=struct.unpack_from('<IHH',dbf,4);assert n==942
 fields=[];off=32
 while dbf[off]!=13:
  name=dbf[off:off+11].split(b'\x00')[0].decode();kind=chr(dbf[off+11]);length,decimal=dbf[off+16:off+18]
  fields.append((name,kind,length,decimal));off+=32
 assert fields==[('Unit','C',8,0),('Name','C',254,0),('Unit_Type','C',25,0),('Fast_Acres','F',13,11),('Wet_Acres','F',13,11),('Shore_Mile','F',13,11),('Shape_Leng','F',19,11),('Shape_Area','F',19,11)]
 assert pyproj.CRS(prj.decode()).equals(pyproj.CRS(4269),ignore_axis_order=True)
 records=[]
 for index in range(n):
  record=dbf[header+index*width:header+(index+1)*width];assert record[0]==32;cursor=1;values={}
  for name,kind,length,decimal in fields:
   raw=record[cursor:cursor+length];cursor+=length
   values[name]=raw.rstrip(b' ').decode('utf-8') if kind=='C' else float(raw)
  assert cursor==width;records.append(values)
 assert struct.unpack_from('>I',shp,0)[0]==9994 and struct.unpack_from('<II',shp,28)==(1000,5)
 assert struct.unpack_from('>I',shp,24)[0]*2==len(shp)
 assert (len(shx)-100)//8==942
 shapes=[];off=100
 while off<len(shp):
  rid,size=struct.unpack_from('>II',shp,off);assert rid==len(shapes)+1
  idxoff,idxsize=struct.unpack_from('>II',shx,100+8*len(shapes));assert idxoff*2==off and idxsize==size
  data=shp[off+8:off+8+size*2];assert struct.unpack_from('<I',data)[0]==5
  parts,points=struct.unpack_from('<II',data,36);starts=list(struct.unpack_from('<'+'I'*parts,data,44))+[points]
  coords=[struct.unpack_from('<dd',data,44+4*parts+16*p) for p in range(points)]
  rings=[tuple(coords[starts[p]:starts[p+1]]) for p in range(parts)]
  assert all(r[0]==r[-1] for r in rings)
  shapes.append(rings);off+=8+size*2
 assert len(shapes)==942 and off==len(shp)
 execution=json.loads((RUNTIME/'execution.json').read_bytes());version=execution.get('output',{}).get('version')
 root=RUNTIME/'staging/cbrs-units-2/cbrs-units-2'/version;table=pq.read_table(root/'geoparquet/cbrs-units-2.parquet');rows=table.to_pylist();assert len(rows)==942
 def rings_of(g):
  polygons=list(g.geoms) if g.geom_type=='MultiPolygon' else [g]
  return [tuple(poly.exterior.coords) for poly in polygons]+[tuple(r.coords) for poly in polygons for r in poly.interiors]
 geoms=[];attribute_mismatches=[]
 for index,row in enumerate(rows):
  assert row['__source_fid']==str(index)
  attribute_mismatches.extend({'fid':index,'field':k,'raw_decoded':v,'candidate':row[k]} for k,v in records[index].items() if row[k]!=v or type(row[k]) is not type(v))
  g=shapely.from_wkb(row['geometry']);geoms.append(g)
  assert collections.Counter(rings_of(g))==collections.Counter(shapes[index]),index
  assert g.is_valid and not g.is_empty
 assert all(not f.nullable for f in table.schema)
 geo=json.loads(table.schema.metadata[b'geo']);assert pyproj.CRS(geo['columns']['geometry']['crs']).equals(pyproj.CRS(4269))
 baseline=pq.read_table(BASE/'published_candidate.body');br=baseline.to_pylist();assert len(br)==942
 bg=json.loads(baseline.schema.metadata[b'geo']);assert pyproj.CRS(bg['columns']['geometry']['crs']).equals(pyproj.CRS(4269))
 byunit={r['Unit']:r for r in br};assert len(byunit)==942 and set(byunit)=={r['Unit'] for r in records}
 topo=sum(shapely.from_wkb(byunit[r['Unit']]['geometry']).equals(g) for r,g in zip(records,geoms))
 diffs={k:sum(r[k]!=byunit[r['Unit']][k] for r in records) for k,_,_,_ in fields}
 api=json.loads((BASE/'native_page0000.body').read_bytes());ids=json.loads((BASE/'source_ids.body').read_bytes())['objectIds'];assert len(api['features'])==len(ids)==len(set(ids))==942
 assert {r['attributes']['OBJECTID'] for r in api['features']}==set(ids)=={r['OBJECTID'] for r in br}
 assert topo==941
 print('ATTRIBUTE_MISMATCHES',attribute_mismatches,'RAW_BASELINE_DIFFS',diffs)
 output={'approved':False,'source_checkpoint':'4cdb6bcb80b6fb66ff8fffeb86ae760fa544de19','audit_role':'author replay of independently written reviewer binary audit; independent re-review remains pending','method':'Original ZIP binary DBF/F fields and SHP/SHX part-coordinate arrays parsed independently without adapter/Fiona decoder; all native ring coordinate arrays retained exactly; Unit-key baseline diagnostic joins are unapproved.', 'source_archive_sha256':hashlib.sha256(body).hexdigest(),'source_rows':942,'fields':fields,'source_fids':{'count':942,'range':[0,941]},'all_942_all_8_values_exact':not attribute_mismatches,'attribute_mismatches':attribute_mismatches,'all_native_coordinate_rings_exact':True,'coverage':dict(collections.Counter(r['Unit_Type'] for r in records)),'geometry_types':dict(collections.Counter(g.geom_type for g in geoms)),'nulls':0,'baseline_sha256':hashlib.sha256((BASE/'published_candidate.body').read_bytes()).hexdigest(),'baseline_rows':942,'baseline_crs':'EPSG:4269','topologically_equal':topo,'attribute_difference_counts':diffs,'delegated_API_complete_unique_OBJECTIDs':942,'published_OBJECTIDs_exact_API_ID_population':True,'native_parquet_sha256':hashlib.sha256((root/'geoparquet/cbrs-units-2.parquet').read_bytes()).hexdigest()}
 Path('/workspace/work/hifld-coordinator/31/graph-review-repair/original-binary-audit.json').write_text(json.dumps(output,indent=2)+'\n');print(json.dumps(output,indent=2))
