"""Complete offline mirror/published-candidate research audit; no acceptance policy."""
import collections
import hashlib
import json
from pathlib import Path
import sys

import pyarrow.parquet as pq
from pyproj import CRS, Transformer
from shapely import from_wkb
from shapely.geometry import Point

ROOT = Path(__file__).parent
EVIDENCE = ROOT / 'evidence'

def compare():
    manifest = json.loads((EVIDENCE / 'published-candidate.json').read_text())
    candidate = ROOT / manifest['capture']
    assert candidate.stat().st_size == manifest['bytes']
    assert hashlib.sha256(candidate.read_bytes()).hexdigest() == manifest['sha256']
    assert hashlib.sha256((EVIDENCE / 'published-candidate-stac.json').read_bytes()).hexdigest() == manifest['catalog_sha256']
    source_features = [f for p in sorted(EVIDENCE.glob('page-*.json')) for f in json.loads(p.read_text())['features']]
    raw = {r['attributes']['MINOP1X020']: r for r in source_features}
    table = pq.read_table(candidate)
    rows = table.to_pylist()
    published = {r['MINOP1X020']: r for r in rows}
    assert len(raw) == len(source_features) == len(published) == len(rows) == 236
    schema = json.loads((EVIDENCE / 'layer.json').read_text())['fields']
    source_columns = {f['name'] for f in schema}
    shared = sorted(source_columns & set(table.schema.names))
    attr_differences = []
    geometry_observations = []
    geo = json.loads(table.schema.metadata[b'geo'])
    crs = CRS.from_json_dict(geo['columns']['geometry']['crs'])
    # A diagnostic reprojection only; it cannot approve a CRS or coordinate tolerance.
    to_source = Transformer.from_crs(crs, CRS.from_epsg(4326), always_xy=True)
    for key in sorted(raw.keys() & published.keys()):
        for field in shared:
            if raw[key]['attributes'][field] != published[key][field]:
                attr_differences.append({'MINOP1X020': key, 'field': field, 'source': raw[key]['attributes'][field], 'published': published[key][field]})
        src = Point(raw[key]['geometry']['x'], raw[key]['geometry']['y'])
        pub = from_wkb(published[key]['geometry'])
        x, y = to_source.transform(pub.x, pub.y)
        diagnostic = Point(x, y)
        geometry_observations.append({'MINOP1X020': key, 'source_xy': list(src.coords[0]), 'published_xy': list(pub.coords[0]), 'published_to_source_diagnostic_xy': [x, y], 'diagnostic_exact_equal': src.equals_exact(diagnostic, 0), 'diagnostic_absolute_degree_deltas': [abs(x-src.x), abs(y-src.y)], 'source_valid': src.is_valid, 'published_valid': pub.is_valid})
    return {'acceptance': 'unapproved third-party mirror versus unapproved published baseline candidate', 'match_key_observation': 'MINOP1X020; observed unique within these two files only, not approved cross-release identity', 'source_rows': len(raw), 'published_rows': len(rows), 'source_schema': schema, 'published_schema': [{'name': f.name, 'type': str(f.type), 'nullable': f.nullable} for f in table.schema], 'source_only_ids': sorted(raw.keys()-published.keys()), 'published_only_ids': sorted(published.keys()-raw.keys()), 'source_only_columns': sorted(source_columns-set(table.schema.names)), 'published_only_columns': sorted(set(table.schema.names)-source_columns), 'shared_attribute_differences': attr_differences, 'source_state_counts': dict(collections.Counter(r['attributes']['STATE_LOCA'] for r in source_features)), 'published_state_counts': dict(collections.Counter(r['STATE_LOCA'] for r in rows)), 'source_crs': 'EPSG:4326', 'published_crs': crs.to_string(), 'geometry_observations': geometry_observations, 'limits': 'Every retained row compared. Reprojection is diagnostic, no geometry repair/tolerance/CRS mapping accepted. Exact input CRS/coordinates retained. No official lineage/currency/rights or national completeness inferred from equality. Physical integer widths, dropped mirror IDs and added bbox remain unresolved.'}

if __name__ == '__main__':
    result = compare()
    path = EVIDENCE / 'published-comparison.json'
    if '--record' in sys.argv:
        path.write_text(json.dumps(result, indent=2)+'\n')
    else:
        assert result == json.loads(path.read_text())
    print(json.dumps({k: v for k, v in result.items() if k not in ('source_schema','published_schema','geometry_observations','limits')},indent=2))
    print('All geometry diagnostic records:',len(result['geometry_observations']))
