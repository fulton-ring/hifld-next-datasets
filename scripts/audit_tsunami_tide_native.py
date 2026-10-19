"""Literal original JSON-to-Point WKB audit, independent of adapter functions."""
import argparse,collections,hashlib,json,struct
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
p=argparse.ArgumentParser();p.add_argument('--capture-dir',type=Path,required=True);p.add_argument('--runtime-dir',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();expected=[]
for path in sorted(a.capture_dir.glob('source_page_*.body')):
 for feature in json.loads(path.read_bytes())['features']:
  row=dict(feature['attributes']);g=feature['geometry'];row['wkb_geometry']=struct.pack('<BIdd',1,1,g['x'],g['y']);expected.append(row)
root=next((a.runtime_dir/'staging/tsunami-capable-tide-stations/tsunami-capable-tide-stations').glob('v*'));gp=root/'geopackage/tsunami-capable-tide-stations.gpkg';pp=root/'geoparquet/tsunami-capable-tide-stations.parquet';meta,gt=pyogrio.read_arrow(gp);pt=pq.read_table(pp);assert gt.to_pylist()==expected and pt.to_pylist()==expected and meta['crs']=='EPSG:3857';fields=json.loads((a.capture_dir/'source_layer_before.body').read_bytes())['fields'];types={'esriFieldTypeInteger':pa.int32(),'esriFieldTypeOID':pa.int64(),'esriFieldTypeDouble':pa.float64(),'esriFieldTypeString':pa.string()}
for field in fields:
 n=field['name'];assert pt[n].type==gt[n].type==types[field['type']];assert json.loads(pt.schema.field(n).metadata[b'original_esri_field'])==field;assert pt.schema.field(n).nullable==field['nullable']
report={'scope':'Author separate literal binary audit; independent native review still required','rows':len(expected),'original_fields':len(fields),'all_original_attribute_values_IDs_types_nulls_and_native_Point_WKB_exact_in_both_outputs':True,'all_Parquet_original_field_declarations_and_nullability_exact':True,'native_CRS':'EPSG:3857','original_identifier_occurrences':{n:[r[n]for r in expected]for n in ['STAT','STAT_OBJID','OID']},'original_null_counts':{n:sum(r[n]is None for r in expected)for n in expected[0]},'WKB_sha256':hashlib.sha256(b''.join(r['wkb_geometry']for r in expected)).hexdigest(),'native_payloads':{str(p.relative_to(root)):{'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in [gp,pp]}}
a.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'rows':len(expected),'fields':len(fields),'literal_native_WKB_exact':True,'payloads':report['native_payloads']}))
