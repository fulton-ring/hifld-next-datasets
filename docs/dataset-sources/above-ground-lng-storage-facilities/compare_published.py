"""Offline complete pinned PASDA/published-candidate comparison; never grants acceptance."""
import collections
import datetime
import hashlib
import json
from pathlib import Path
import sys

import fiona
import pyarrow.parquet as pq
from pyproj import CRS
from shapely import from_wkb
from shapely.geometry import shape

ROOT = Path(__file__).parent
EVIDENCE = ROOT / 'evidence'

def analyze():
    manifest = json.loads((EVIDENCE / 'published-candidate.json').read_text())
    candidate = ROOT / manifest['capture']
    assert candidate.stat().st_size == manifest['bytes']
    assert hashlib.sha256(candidate.read_bytes()).hexdigest() == manifest['sha256']
    assert hashlib.sha256((EVIDENCE / 'published-candidate-stac.json').read_bytes()).hexdigest() == manifest['catalog_sha256']
    with fiona.open('zip://' + str((EVIDENCE / 'pasda-zip.zip').resolve()) + '!Above_Ground_LNG_Storage_Facilities/Above_Ground_LNG_Storage_Facilities_pt.shp') as source:
        source_schema = dict(source.schema['properties'])
        source_crs = CRS(source.crs)
        raw = {row['properties']['AGLNGID']: {'properties': dict(row['properties']), 'geometry': shape(row['geometry'])} for row in source}
        assert len(raw) == len(source) == 13
    table = pq.read_table(candidate)
    published_rows = table.to_pylist()
    published = {row['AGLNGID']: row for row in published_rows}
    assert len(published) == len(published_rows) == 13
    geo = json.loads(table.schema.metadata[b'geo'])
    candidate_crs = CRS.from_json_dict(geo['columns']['geometry']['crs'])
    diffs = []
    geometry_diffs = []
    for key in sorted(raw.keys() & published.keys()):
        for col, value in raw[key]['properties'].items():
            other = published[key][col]
            if value != other:
                diffs.append({'id': key, 'column': col, 'raw_value': value, 'published_value': other, 'raw_python_type': type(value).__name__, 'published_python_type': type(other).__name__})
        if not raw[key]['geometry'].equals_exact(from_wkb(published[key]['geometry']), 0):
            geometry_diffs.append(key)
    return {
        'acceptance': 'unapproved candidate; observations only',
        'raw_count': len(raw), 'published_count': len(published),
        'raw_sha256': hashlib.sha256((EVIDENCE / 'pasda-zip.zip').read_bytes()).hexdigest(),
        'published_sha256': manifest['sha256'],
        'raw_schema': source_schema,
        'published_schema': [{'name': f.name, 'type': str(f.type), 'nullable': f.nullable} for f in table.schema],
        'raw_crs': source_crs.to_string(), 'published_crs': candidate_crs.to_string(),
        'same_crs': source_crs.equals(candidate_crs),
        'raw_only_ids': sorted(raw.keys() - published.keys()),
        'published_only_ids': sorted(published.keys() - raw.keys()),
        'unmatched_shared_attributes': diffs,
        'geometry_exact_coordinate_mismatch_ids': geometry_diffs,
        'extra_published_columns': sorted(set(table.schema.names) - set(source_schema) - {'geometry'}),
        'raw_state_counts': dict(collections.Counter(r['properties']['STATE'] for r in raw.values())),
        'published_state_counts': dict(collections.Counter(r['STATE'] for r in published_rows)),
        'limits': 'AGLNGID observed unique only in these snapshots; no cross-release stability or national completeness. Geometry compared at zero tolerance in common CRS, no repair/reprojection. Physical schema drift retained; nullability approval unknown.'
    }

if __name__ == '__main__':
    result = analyze()
    target = EVIDENCE / 'published-comparison.json'
    if '--record' in sys.argv:
        target.write_text(json.dumps(result, indent=2) + '\n')
    else:
        assert result == json.loads(target.read_text())
    print(json.dumps({k: v for k, v in result.items() if k not in ('raw_schema', 'published_schema', 'limits')}, indent=2))
