"""Offline, complete audit of the unapproved ASR published candidate; no repairs."""
import argparse
import collections
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import shapely

ROOT = Path(__file__).resolve().parent

def analyze(path):
    data = path.read_bytes()
    table = pq.read_table(path)
    result = {
        'acceptance': 'unapproved published candidate; never used as accepted contract',
        'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
        'rows': table.num_rows,
        'arrow_schema': str(table.schema),
        'schema_metadata': {k.decode(): v.decode() for k, v in (table.schema.metadata or {}).items()},
        'columns': [],
    }
    for field in table.schema:
        col = table[field.name].to_pylist()
        encoded = [json.dumps(x, sort_keys=True, ensure_ascii=False, allow_nan=False).encode() for x in col] if field.name != 'geometry' else [bytes(x) if x is not None else b'NULL' for x in col]
        h = hashlib.sha256()
        for value in encoded:
            h.update(len(value).to_bytes(8, 'big')); h.update(value)
        stat = {'name': field.name, 'type': str(field.type), 'nullable': field.nullable, 'nulls': col.count(None), 'ordered_value_sha256': h.hexdigest()}
        if field.name in ['RegNum', 'UniqSysID', 'id']:
            counts = collections.Counter(x for x in col if x is not None)
            stat.update(distinct_nonnull=len(counts), duplicate_rows=sum(v - 1 for v in counts.values()), minimum=min(counts), maximum=max(counts))
        if field.name in ['LatDir', 'LonDir', 'CoordsType', 'StatusCode', 'LocState']:
            stat['most_common'] = [list(item) for item in collections.Counter(col).most_common(15)]
        result['columns'].append(stat)
    geometries = shapely.from_wkb(table['geometry'].to_numpy())
    valid = shapely.is_valid(geometries)
    coords = shapely.get_coordinates(geometries)
    lat = table['latdec'].to_pylist(); lon = table['londec'].to_pylist()
    pairs = [(i, la, lo) for i, (la, lo) in enumerate(zip(lat, lon)) if la is not None and lo is not None]
    result['geometry'] = {'types': {str(k):v for k,v in collections.Counter(shapely.get_type_id(geometries).tolist()).items()}, 'invalid': int((~valid).sum()), 'empty': int(shapely.is_empty(geometries).sum()), 'bounds': shapely.total_bounds(geometries).tolist(), 'coordinate_rows':len(coords)}
    result['diagnostics_not_repairs'] = {
        'nonnull_declared_latlon_pairs': len(pairs),
        'declared_latlon_exactly_equal_geometry_xy_rows': int(sum(la == coords[i, 1] and lo == coords[i, 0] for i, la, lo in pairs)),
        'declared_latitude_outside_90_rows': sum(abs(la) > 90 for _,la,_ in pairs),
        'declared_lat_direction_outside_NS_rows': sum(x not in ('N','S',None) for x in table['LatDir'].to_pylist()),
        'declared_lon_direction_outside_EW_rows': sum(x not in ('E','W',None) for x in table['LonDir'].to_pylist()),
        'declared_url_not_http_rows':sum(x is not None and not x.startswith(('https://','http://')) for x in table['url'].to_pylist()),
        'first_two_observed_rows': [{k:(bytes(v).hex() if k=='geometry' else v) for k,v in row.items()} for row in table.slice(0,2).to_pylist()],
        'interpretation': 'Name/value inconsistencies are observed on the complete published candidate. No alternate schema, positional repair, identifier substitution, or actual source contract is inferred.'
    }
    return result

def main():
    parser = argparse.ArgumentParser(); parser.add_argument('--record', action='store_true'); args=parser.parse_args()
    observed = analyze(ROOT / 'evidence/published-candidate.parquet')
    target=ROOT / 'evidence/published-analysis.json'
    if args.record: target.write_text(json.dumps(observed, indent=2, allow_nan=False)+'\n')
    else:
        expected=json.loads(target.read_text()); assert observed==expected,'Offline candidate audit differs from committed evidence'
    print(json.dumps({'rows':observed['rows'],'sha256':observed['sha256'],'geometry':observed['geometry'],'id_columns':[c for c in observed['columns'] if c['name'] in ['RegNum','UniqSysID','id']]},indent=2))
if __name__=='__main__': main()
