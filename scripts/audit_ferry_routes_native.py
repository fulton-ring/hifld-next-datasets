"""Independent original JSON→literal WKB proof, without adapter expected-row helpers."""
import json,struct,collections,hashlib,argparse
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio

parser=argparse.ArgumentParser();parser.add_argument('--capture-dir',type=Path,default=Path(__file__).parents[1]/'docs/dataset-sources/ferry-routes-1/evidence/inputs');parser.add_argument('--runtime',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
C=args.capture_dir;schema=json.loads((C/'source_layer_before.body').read_text())['fields']
expected=[];vertices=0;leading=[]
for path in sorted(C.glob('source_page_*.body')):
 for feature in json.loads(path.read_text())['features']:
  row=dict(feature['attributes']);paths=feature['geometry']['paths'];body=bytearray(struct.pack('<BII',1,5,len(paths)))
  for points in paths:
   body.extend(struct.pack('<BII',1,2,len(points)));vertices+=len(points)
   for x,y in points:body.extend(struct.pack('<dd',x,y))
  for name,value in row.items():
   if isinstance(value,str)and value.startswith(' '):leading.append({'OBJECTID':row['OBJECTID'],'field':name,'value':value})
  row['wkb_geometry']=bytes(body);expected.append(row)
root=next((args.runtime/'staging/ferry-routes-1/ferry-routes-1').glob('v*'))
table=pq.read_table(root/'geoparquet/ferry-routes-1.parquet');meta,gpkg=pyogrio.read_arrow(root/'geopackage/ferry-routes-1.gpkg')
assert table.to_pylist()==expected and gpkg.to_pylist()==expected and meta['crs']=='EPSG:4326'
type_map={'esriFieldTypeOID':pa.int64(),'esriFieldTypeInteger':pa.int32(),'esriFieldTypeDouble':pa.float64(),'esriFieldTypeString':pa.string()}
for field in schema:
 name=field['name'];assert table[name].type==type_map[field['type']]and gpkg[name].type==type_map[field['type']]
 assert json.loads(table.schema.field(name).metadata[b'original_esri_field'])==field
 assert table.schema.field(name).nullable==field['nullable']
assert [r['OBJECTID']for r in expected]==list(range(1,965))and vertices==714082
report={'rows':len(expected),'fields':len(schema),'coordinate_pairs':vertices,'all_attributes_original_IDs_exact_in_GPKG_and_Parquet':True,'literal_independent_binary_WKB_exact_in_both_outputs':True,'all_source_Esri_field_metadata_and_nullability_preserved_in_Parquet':True,'numeric_string_types_preserved':True,'OID_int64_storage_proxy_unapproved':True,'WKB_sha256':hashlib.sha256(b''.join(r['wkb_geometry']for r in expected)).hexdigest(),'leading_character_values':leading,'operator_segment_unique_pairs':len(set((r['operator_id'],r['segment_id'])for r in expected)),'source_census_year':dict(collections.Counter(r['census_year']for r in expected)),'source_data_year':dict(collections.Counter(r['data_year']for r in expected)),'native_payloads':{str(p.relative_to(root)):{'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in [root/'geopackage/ferry-routes-1.gpkg',root/'geoparquet/ferry-routes-1.parquet']}}
args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
