"""Compare every original attribute/type and native point with typed candidate.

Usage: uv run python .../audit_candidate.py CAPTURE_ROOT CANDIDATE_PARQUET OUTPUT
Integer-looking JSON tokens in source-declared Double fields must be exactly
representable numeric values. Original raw JSON tokens remain in raw storage.
"""
import json
import pathlib
import sys

import pyarrow.parquet as pq
from shapely import from_wkb
from dagster_hifld.assets.fuel_stations import _TYPES, _digest, _strict_json, inspect_capture


def main(capture, candidate, output):
    audit=inspect_capture(capture)
    parquet=pq.ParquetFile(candidate)
    source_types={f['name']:f['type'] for f in audit['fields']}
    for field in audit['fields']:
        actual=parquet.schema_arrow.field(field['name'])
        assert actual.type==_TYPES[field['type']] and actual.nullable==field['nullable']
    assert parquet.schema_arrow.names==list(source_types)+['geometry']
    rows=values=double_tokens=0
    ids=set();oids=set()
    for index,path in enumerate(sorted(capture.glob('page-*.body'))):
        raw=_strict_json(path.read_bytes())['features']
        generated=parquet.read_row_group(index).to_pylist()
        assert len(raw)==len(generated)
        for feature,row in zip(raw,generated,strict=True):
            geometry=from_wkb(row.pop('geometry'))
            assert row==feature['attributes']
            for name,value in row.items():
                original=feature['attributes'][name]
                if source_types[name]=='esriFieldTypeDouble' and type(original) is int:
                    assert type(value) is float and value==original
                    double_tokens+=1
                else:
                    assert type(value) is type(original)
            assert (geometry.x,geometry.y)==(feature['geometry']['x'],feature['geometry']['y'])
            assert geometry.is_valid
            ids.add(row['id']);oids.add(row['OBJECTID']);values+=len(row);rows+=1
    assert rows==audit['rows'] and len(ids)==rows and len(oids)==rows
    result={'rows_asserted':rows,'raw_attribute_values_and_declared_types_asserted':values,
        'declared_double_fields_with_integer_JSON_tokens':double_tokens,
        'json_token_policy':'Source-declared Double physical storage retains exact numeric value; original JSON bytes retained. No ID/string coercion.',
        'native_geometry_points_asserted':rows,'unique_station_ids':len(ids),'unique_OBJECTIDs':len(oids),
        'all_raw_source_fields_retained':True,'candidate_sha256':_digest(candidate),
        'parquet_schema':[{'name':f.name,'type':str(f.type),'nullable':f.nullable} for f in parquet.schema_arrow],
        'crs':_strict_json(parquet.schema_arrow.metadata[b'geo'])['columns']['geometry']['crs']['id'],
        'approval':'none; assertions do not approve source/schema/baseline'}
    output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='parquet_schema'},indent=2))


if __name__=='__main__':
    main(pathlib.Path(sys.argv[1]),pathlib.Path(sys.argv[2]),pathlib.Path(sys.argv[3]))
