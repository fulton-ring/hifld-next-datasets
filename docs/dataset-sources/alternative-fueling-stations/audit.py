"""Offline complete source/baseline comparison; raw captures stay outside Git.

Run from repository root with the locked environment and PYTHONPATH=src:
 uv run --frozen --offline python docs/dataset-sources/alternative-fueling-stations/audit.py CAPTURE_DIR OUTPUT_DIR
All rows stream through a disk-backed baseline index. No field is silently mapped.
"""
import collections
import datetime
import hashlib
import json
import pathlib
import sqlite3
import sys
import tempfile
from dagster_hifld.assets.fuel_stations import _strict_json, _page_paths

import pyarrow.parquet as pq
from shapely import from_wkb
from dagster_hifld.assets.fuel_stations import inspect_capture


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def run(root, output):
    output.mkdir(parents=True, exist_ok=True)
    audit = inspect_capture(root)
    baseline = root / 'baseline_parquet.body'
    receipt = _strict_json((root / 'baseline_parquet.receipt.json').read_text())
    assert hashlib.sha256(baseline.read_bytes()).hexdigest() == receipt['sha256']
    assert baseline.stat().st_size == receipt['bytes'] == 10134712
    table = pq.ParquetFile(baseline)
    geo = _strict_json(table.schema_arrow.metadata[b'geo'])
    assert geo['columns']['geometry']['crs']['id']['code'] == 3857
    source_fields = {f['name']:f for f in audit['fields']}
    published_fields = {f.name:str(f.type) for f in table.schema_arrow}
    common = sorted(set(source_fields) & set(published_fields))
    coverage_fields = ['country','state','fuel_type_code','status_code','access_code']
    coverage = {p:{k:collections.Counter() for k in coverage_fields} for p in ('source','published')}
    nulls = {p:collections.Counter() for p in ('source','published')}
    raw_changed = collections.Counter(); semantic_changed = collections.Counter(); examples = collections.defaultdict(list)
    semantic_dates = {n for n in common if source_fields[n]['type']=='esriFieldTypeDate'}
    source_ids=set(); baseline_ids=set(); all_source_ids=set(); geom_changed=0; exactgeom=0; max_delta=0; max_case=None; max_geometry_evidence=None
    raw_counts=collections.Counter(); row_dates=[]
    with tempfile.TemporaryDirectory() as temporary:
        db=sqlite3.connect(pathlib.Path(temporary)/'baseline.sqlite')
        db.execute('create table rows(id integer primary key, attributes text not null, x real not null, y real not null)')
        for batch in table.iter_batches(batch_size=1000):
            insert=[]
            for row in batch.to_pylist():
                identifier=row['id']; assert type(identifier) is int and identifier not in baseline_ids
                baseline_ids.add(identifier)
                geom=from_wkb(row.pop('geometry')); assert geom.geom_type=='Point' and geom.is_valid and not geom.is_empty
                row.pop('bbox')
                for name,value in row.items():
                    if value is None:nulls['published'][name]+=1
                for name in coverage_fields:coverage['published'][name][canonical(row[name])]+=1
                insert.append((identifier, canonical(row),geom.x,geom.y))
            db.executemany('insert into rows values (?,?,?,?)',insert)
        db.commit()
        for path in _page_paths(root):
            for feature in _strict_json(path.read_bytes())['features']:
                attrs=feature['attributes']; identifier=attrs['id'];source_ids.add(identifier);all_source_ids.add(attrs['OBJECTID'])
                if attrs['updated_at'] is not None:row_dates.append(attrs['updated_at'])
                for name,value in attrs.items():
                    raw_counts[(name,type(value).__name__)]+=1
                    if value is None:nulls['source'][name]+=1
                for name in coverage_fields:coverage['source'][name][canonical(attrs[name])]+=1
                found=db.execute('select attributes,x,y from rows where id=?',(identifier,)).fetchone()
                if not found:continue
                published=_strict_json(found[0])
                for name in common:
                    value=attrs[name]; previous=published[name]
                    if type(value) is not type(previous) or value != previous:raw_changed[name]+=1
                    comparable=value
                    if name in semantic_dates and value is not None:
                        comparable=datetime.datetime.fromtimestamp(value/1000,datetime.timezone.utc).isoformat()
                    if comparable != previous:
                        semantic_changed[name]+=1
                        if len(examples[name])<5:examples[name].append(identifier)
                x,y=feature['geometry']['x'],feature['geometry']['y'];delta=max(abs(x-found[1]),abs(y-found[2]))
                if delta==0:exactgeom+=1
                else:
                    geom_changed+=1
                    if delta>max_delta:
                        max_delta=delta;max_case=identifier
                        max_geometry_evidence={'id':identifier,'source_xy':[x,y],'published_xy':[found[1],found[2]],'source_location':{k:attrs[k] for k in ('station_name','city','state','country','latitude','longitude')},'published_location':{k:published[k] for k in ('station_name','city','state','country','latitude','longitude')}}
        db.close()
    source_proposal=[]
    arrow_type={'esriFieldTypeOID':'int64','esriFieldTypeInteger':'int32','esriFieldTypeDouble':'double','esriFieldTypeDate':'int64','esriFieldTypeString':'string'}
    for f in audit['fields']:
        n=f['name'];source_proposal.append({'name':n,'raw_declared_type':f['type'],'raw_nullable':f['nullable'],'proposed_candidate_type':arrow_type[f['type']], 'published_type':published_fields.get(n),'observed_json_types':{t:c for (field,t),c in raw_counts.items() if field==n},'null_count':nulls['source'][n],'mapping':'identity; no cast/rename/drop','approval':'unapproved'})
    result={'source_rows':len(source_ids),'published_rows':len(baseline_ids),'matched_station_ids':len(source_ids&baseline_ids),
      'added_station_ids':len(source_ids-baseline_ids),'removed_station_ids':len(baseline_ids-source_ids),'added_id_examples':sorted(source_ids-baseline_ids)[:20],'removed_id_examples':sorted(baseline_ids-source_ids)[:20],
      'source_id_set_sha256':hashlib.sha256(canonical(sorted(source_ids)).encode()).hexdigest(),'published_id_set_sha256':hashlib.sha256(canonical(sorted(baseline_ids)).encode()).hexdigest(),
      'identifier_preservation':{'source':['OBJECTID','id','federal_agency_id','ev_network_ids_station','ev_network_ids_posts'],'published':['id','federal_agency_id','ev_network_ids_station','ev_network_ids_posts'],'canonical_join':'id integer exact; source OBJECTID retained separately, not silently dropped'},
      'source_only_fields':sorted(set(source_fields)-set(published_fields)),'published_only_fields':sorted(set(published_fields)-set(source_fields)),
      'source_schema_proposal':source_proposal,'published_schema':[{'name':f.name,'type':str(f.type),'nullable':f.nullable} for f in table.schema_arrow],
      'common_fields_raw_value_or_python_type_changed':dict(sorted(raw_changed.items())),
      'common_fields_value_changed_diagnostic_only':dict(sorted(semantic_changed.items())),
      'diagnostic_transformations':'Compare epoch-ms dates as UTC ISO strings solely for diagnostics; numeric int/float equality solely diagnostic. Neither authorizes published casts.',
      'changed_value_id_examples':dict(examples),'coverage':{p:{k:dict(sorted(v.items())) for k,v in fs.items()} for p,fs in coverage.items()},
      'null_counts':{p:dict(sorted(v.items())) for p,v in nulls.items()},
      'geometry':{'crs':'EPSG:3857 native source and published candidate','compared_matched_ids':exactgeom+geom_changed,'exact_coordinate_matches':exactgeom,'changed_coordinates':geom_changed,'max_absolute_axis_delta_m':max_delta,'max_delta_station_id':max_case,'max_delta_evidence':max_geometry_evidence,'tolerance_applied':0,'interpretation':'Raw geometric drift counts include binary coordinate representation effects; no approved tolerance.'},
      'source_record_updated_at_range_epoch_ms':[min(row_dates),max(row_dates)],'acceptance':'unapproved source/schema/baseline proposal; changes require maintainer decision'}
    (output/'full-comparison.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    (output/'full-source-audit.json').write_text(json.dumps(audit,indent=2,sort_keys=True)+'\n')
    return {k:result[k] for k in ('source_rows','published_rows','matched_station_ids','added_station_ids','removed_station_ids','source_only_fields','published_only_fields','geometry')}


if __name__=='__main__':print(json.dumps(run(pathlib.Path(sys.argv[1]),pathlib.Path(sys.argv[2])),indent=2))
