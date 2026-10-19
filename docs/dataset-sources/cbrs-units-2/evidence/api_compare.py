import json,tempfile,zipfile,collections,datetime,hashlib
from pathlib import Path
import fiona
from shapely.geometry import shape,mapping
root=Path(__file__).resolve().parent;api=json.loads((root/'native_page0000.body').read_text())
with tempfile.TemporaryDirectory() as d:
 zipfile.ZipFile(root/'captures/official_zip.body').extractall(d)
 with fiona.open(Path(d)/'20241219_CBRS_Polygons.shp') as f:rows=list(f)
 equal=0;geometry_differences=[];diff=collections.Counter();fids=[]
 for r,a in zip(rows,api['features']):
  g=shape(r.geometry);m=mapping(g);coords=m['coordinates'];rings=list(coords) if g.geom_type=='Polygon' else [ring for poly in coords for ring in poly]
  if json.dumps(rings,separators=(',',':'))==json.dumps(a['geometry']['rings'],separators=(',',':')):equal+=1
  else: geometry_differences.append({'ZIP_intrinsic_fid':r.id,'API_OBJECTID':a['attributes']['OBJECTID'],'Unit':r['properties']['Unit'],'Unit_Type':r['properties']['Unit_Type'],'ZIP_rings_sha256':hashlib.sha256(json.dumps(rings,separators=(',',':')).encode()).hexdigest(),'API_rings_sha256':hashlib.sha256(json.dumps(a['geometry']['rings'],separators=(',',':')).encode()).hexdigest(),'decision':'No substitution or coordinate repair; primary pipeline preserves original producer ZIP geometry.'})
  for k,v in r['properties'].items():
   ak={'Shape_Leng':'Shape_Length'}.get(k,k);av=a['attributes'][ak]
   if k in ['FI_Date','SU_Date'] and av is not None:av=datetime.datetime.fromtimestamp(av/1000,datetime.timezone.utc).date().isoformat()
   if v!=av:diff[k]+=1
  fids.append({'ZIP_intrinsic_fid':r.id,'API_OBJECTID':a['attributes']['OBJECTID']})
 report={'source_zip_sha256':hashlib.sha256((root/'captures/official_zip.body').read_bytes()).hexdigest(),'API_sha256':hashlib.sha256((root/'native_page0000.body').read_bytes()).hexdigest(),'all_942_native_coordinate_ring_arrays_equal_in_order':equal==942,'exact_ring_rows':equal,'geometry_difference_rows':geometry_differences,'native_API_attribute_difference_counts':dict(diff),'original_source_identifiers_preserved_separately':fids,'join_status':'diagnostic original-order witness only; no published ID mapping approved'}
 (root/'native-api-comparison.json').write_text(json.dumps(report,indent=2)+'\n');print(equal,dict(diff))
