"""Complete historical files, original identifiers and typed current-source diagnostics.

No column rename, numeric-string conversion, ID join or tolerance is approved.
"""
from pathlib import Path
from collections import Counter
import argparse,hashlib,json,tempfile,zipfile
import fiona,pyarrow.parquet as pq,pyogrio,shapely
from dagster_hifld.sources import tsunami_observations as source
parser=argparse.ArgumentParser();parser.add_argument('--evidence-dir',type=Path,required=True);args=parser.parse_args();E=args.evidence_dir;P=E/'published-candidate';C=E/'captures'
http=source.unpack_captures({n:(C/(n+'.zip')).read_bytes() for n in source.ARCHIVE_NAMES});source.validate_complete_bundle(http)
rows,audit,decodes=source.decode_rows(http);current={r['properties']['ID']:r for r in rows};assert len(current)==34495
collection=json.loads((P/'collection.json').read_bytes());pins={};formats={}
for key,asset in collection['assets'].items():
 name=key.split('-')[0];file=asset['href'].rsplit('/',1)[-1];body=(P/file).read_bytes();assert len(body)==asset['file:size'] and asset['file:checksum']=='d50110'+hashlib.md5(body).hexdigest();pins[name]={'file':file,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'provider_size_md5_verified':True}
 if name=='geojson':
  geojson=json.loads(body);records=geojson['features'];formats[name]={'rows':len(records),'columns':list(records[0]['properties']),'feature_ids':[r.get('id') for r in records],
    'id_columns':{k:[r['properties'].get(k) for r in records] for k in ('ID','source_ID','ObjectId','OBJECTID','TSEVENT_ID') if k in records[0]['properties']}}
 if name=='geoparquet':
  table=pq.read_table(P/file);old=table.to_pylist();formats[name]={'rows':len(old),'schema':str(table.schema),'geo_metadata':json.loads(table.schema.metadata[b'geo']),
   'id_columns':{k:[r[k] for r in old] for k in ('ID','source_ID','ObjectId','OBJECTID','TSEVENT_ID') if k in table.column_names}}
 if name=='geopackage':
  with fiona.open(P/file) as f:
   fields=list(f.schema['properties']);formats[name]={'rows':len(f),'schema':dict(f.schema),'crs':f.crs.to_string(),'intrinsic_fids':[r.id for r in f],
      'id_columns':{k:[] for k in ('ID','source_ID','ObjectId','OBJECTID','TSEVENT_ID') if k in fields}}
   for r in f:
    for k in formats[name]['id_columns']:formats[name]['id_columns'][k].append(r.properties[k])
 if name in ('file_geodatabase','shapefile'):
  with zipfile.ZipFile(P/file) as z:
   assert z.testzip() is None;assert all(not Path(n).is_absolute() and '..' not in Path(n).parts for n in z.namelist())
   with tempfile.TemporaryDirectory() as tmp:
    z.extractall(tmp);path=next(Path(tmp).rglob('*.gdb' if name=='file_geodatabase' else '*.shp'))
    with fiona.open(path) as f:
     ids=[];sourceids={k:[] for k in ('ID','source_ID','ObjectId','OBJECTID','TSEVENT_ID') if k in f.schema['properties']}
     for r in f:
      ids.append(r.id)
      for k in sourceids:sourceids[k].append(r.properties[k])
     fiona_schema=dict(f.schema)
     formats[name]={'rows':len(ids),'fiona_schema':fiona_schema,'crs':f.crs.to_string(),'intrinsic_fids':ids,'id_columns':sourceids}
     if name=='file_geodatabase':
      metadata,native=pyogrio.raw.read_arrow(path)
      geometry_name=metadata['geometry_name'];attributes=[n for n in native.column_names if n!=geometry_name]
      assert len(native)==32614 and len(attributes)==49
      formats[name].update(native_intrinsic_fid_column=metadata.get('fid_column'),native_geometry_column=metadata.get('geometry_name'),schema=str(native.schema),columns=attributes,source_physical_types={n:str(native.schema.field(n).type) for n in attributes},
       native_reader='pyogrio Arrow; full rows and physical declarations',fiona_omitted_physical_fields=sorted(set(attributes)-set(fiona_schema['properties'])))
      formats[name]['id_columns']={k:native.column(k).to_pylist() for k in ('ID','source_ID','ObjectId','OBJECTID','TSEVENT_ID') if k in attributes}
      formats[name]['all_physical_attribute_value_sha256']={n:hashlib.sha256(source.encode(native.column(n).to_pylist())).hexdigest() for n in attributes}
      formats[name]['native_float32_fields']={n:{'type':str(native.schema.field(n).type),'rows':len(native),'nulls':native.column(n).null_count,
       'all_native_values_sha256':formats[name]['all_physical_attribute_value_sha256'][n]} for n in ['PERIOD','RUNUP_HT','RUNUP_HORIZ','PRED_TRAV_HOURS']}
      assert formats[name]['fiona_omitted_physical_fields']==['PERIOD','PRED_TRAV_HOURS','RUNUP_HORIZ','RUNUP_HT']
     else:formats[name]['schema']=fiona_schema
assert all(v['rows']==32614 for v in formats.values())
old_by_source_id={r['source_ID']:r for r in old};assert len(old_by_source_id)==32614
common=set(current)&set(old_by_source_id);changed={}
for field in current[next(iter(current))]['properties']:
 if field not in old[0]:continue
 changed[field]={'source_native_type':source.KIND[next(f['type'] for f in source.NATIVE_FIELDS if f['name']==field)],
   'published_arrow_type':str(table.schema.field(field).type),
   'different_python_type_or_value':sum(type(current[k]['properties'][field]) is not type(old_by_source_id[k][field]) or current[k]['properties'][field]!=old_by_source_id[k][field] for k in common)}
report={'approved':False,'classification':'unapproved_complete_baseline_diagnostic_no_mapping_acceptance','source_count':34495,'historical_published_count':32614,
 'all_six_complete_format_pins':pins,'published_formats':formats,
 'id_columns_preserved':'SourceID/ObjectId/TSEVENT_ID and all original published Feature.id/source_ID/OBJECTID/ObjectId/intrinsicFIDs retained in full captures. No renamed ID or row index is substituted.',
 'diagnostic_unapproved_correspondence':{'current_ID_vs_published_source_ID_only':'Diagnostic value-set comparison; this is NOT an approved ID rename/join.','common':len(common),'only_current':len(set(current)-set(old_by_source_id)),'only_published':len(set(old_by_source_id)-set(current))},
 'common_field_type_and_value_changes':changed,
 'exact_wkb_matches_under_unapproved_diagnostic_correspondence':sum(shapely.geometry.shape(current[k]['geometry']).wkb==old_by_source_id[k]['geometry'] for k in common),
 'baseline_reader_diagnostic':{'file_geodatabase':'Fiona omitted four native Float32 columns; complete Arrow49attribute schema and whole-column value hashes are authoritative; all original FIDs retained separately. No cross-format physical-type mapping accepted.'},
 'source_audit':audit,'numeric_physical_decodes':audit['numeric_physical_decode'],
 'release_gate':'No source/baseline/currentID-vs-source_ID mapping/physicaltype/nullability/geometry/coverage/format acceptance. Originalwiredecimals retained; explicit declaredFloat32decode is not approval of historical string-to-numeric migration.'}
(E/'baseline-audit.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'source':34495,'baseline':32614,'diagnostic':report['diagnostic_unapproved_correspondence'],'exact_wkb':report['exact_wkb_matches_under_unapproved_diagnostic_correspondence'],'provider_pins':6,'approved':False}))
