"""Full streamed canonical typed candidate vs generated terminal Parquet audit.

Usage: ... audit_terminal.py STAGED_VERSION_ROOT OUTPUT_JSON
The accepted raw/source pins are checked by audit.py and source module first.
"""
import collections
import hashlib
import json
import pathlib
import sqlite3
import sys
import tempfile

import pyarrow.parquet as pq
from pyproj import CRS


def digest(path):
    value=hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda:source.read(1024*1024),b''):value.update(block)
    return value.hexdigest()


def main(root, output):
    candidate=root/'draft_candidate/candidate.parquet'; original=pq.ParquetFile(candidate)
    paths=sorted((root/'geoparquet').rglob('*.parquet'))
    if not paths:raise ValueError('No generated terminal Parquet')
    source_types={f.name:f.type for f in original.schema_arrow}
    expected_fields=set(source_types)-{'geometry'}
    rows=0; ids=set(); changes=collections.Counter(); nullable_differences={}; additional=set(); schema_changes={}; geometry_changes=0
    with tempfile.TemporaryDirectory() as temporary:
        database=sqlite3.connect(pathlib.Path(temporary)/'reference.sqlite')
        database.execute('create table rows(id integer primary key, attrs text, geometry blob)')
        for batch in original.iter_batches(batch_size=1000):
            insert=[]
            for row in batch.to_pylist():
                geometry=row.pop('geometry');insert.append((row['id'],json.dumps(row,separators=(',',':'),sort_keys=True),geometry))
            database.executemany('insert into rows values (?,?,?)',insert)
        database.commit()
        for path in paths:
            terminal=pq.ParquetFile(path)
            geo=json.loads(terminal.schema_arrow.metadata[b'geo']);geometry_name=geo['primary_column']
            if CRS.from_user_input(geo['columns'][geometry_name]['crs'])!=CRS.from_epsg(3857):raise ValueError('Unexpected terminal CRS; no reprojected equality inferred')
            names=set(terminal.schema_arrow.names)
            if not expected_fields<=names:raise ValueError('Terminal field omission')
            additional|=names-expected_fields-{geometry_name}
            for name in expected_fields:
                f=terminal.schema_arrow.field(name);old=original.schema_arrow.field(name)
                if f.type!=old.type:schema_changes[name]={'source':str(old.type),'terminal':str(f.type)}
                if f.nullable!=old.nullable:nullable_differences[name]={'source':old.nullable,'terminal':f.nullable}
            for batch in terminal.iter_batches(batch_size=1000):
                for row in batch.to_pylist():
                    identifier=row['id']
                    if identifier in ids:raise ValueError('Duplicate terminal ID')
                    ids.add(identifier)
                    found=database.execute('select attrs,geometry from rows where id=?',(identifier,)).fetchone()
                    if found is None:raise ValueError('Unexpected terminal ID')
                    reference=json.loads(found[0])
                    for name in expected_fields:
                        if type(row[name]) is not type(reference[name]) or row[name]!=reference[name]:changes[name]+=1
                    if row[geometry_name]!=found[1]:geometry_changes+=1
                    rows+=1
        if rows!=original.metadata.num_rows:raise ValueError('Incomplete terminal population')
    result={'rows':rows,'terminal_files':len(paths),'source_candidate_sha256':digest(candidate),'all_station_ids_unique_and_equal':True,'all_raw_source_columns_retained':True,'source_field_type_changes':schema_changes,'source_field_nullability_changes':nullable_differences,'typed_value_changes':dict(changes),'native_WKB_changes':geometry_changes,'additional_terminal_columns':sorted(additional),'terminal_pins':[{'relative_path':str(path.relative_to(root)),'bytes':path.stat().st_size,'sha256':digest(path)} for path in paths],'approval':'none; additional columns/nullability and baseline/schema/ID migration remain review decisions'}
    output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k!='terminal_pins'},indent=2))
    if schema_changes or changes or geometry_changes:raise ValueError('Terminal representation fidelity failed; retain artifacts in quarantine')


if __name__=='__main__':main(pathlib.Path(sys.argv[1]),pathlib.Path(sys.argv[2]))
