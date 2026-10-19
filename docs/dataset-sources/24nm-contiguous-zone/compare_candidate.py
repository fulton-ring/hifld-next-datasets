"""Replay complete research comparisons. Never approves a baseline or contract.

Usage: uv run --frozen python docs/dataset-sources/24nm-contiguous-zone/compare_candidate.py /path/to/24nm-contiguous-zone.parquet
The complete pinned Parquet is preserved outside Git; this script refuses another object.
"""
import collections
import gzip
import hashlib
import json
import pathlib
import sys

import pyarrow.parquet as pq
import shapely
from shapely.geometry import MultiLineString, shape

ROOT = pathlib.Path(__file__).parent
EVIDENCE = ROOT / 'evidence' / 'network-20261019'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(baseline_path):
    pin = json.loads((EVIDENCE / 'baseline-candidate.json').read_text())
    assert digest(baseline_path) == pin['sha256'], 'baseline checksum mismatch'
    assert baseline_path.stat().st_size == pin['bytes'], 'baseline size mismatch'
    source = json.loads(gzip.decompress((EVIDENCE / 'features-geojson.json.gz').read_bytes()))
    old = json.loads((ROOT / 'evidence' / 'all-features.json').read_text())
    table = pq.read_table(baseline_path)
    rows = table.to_pylist()
    fs = source['features']
    ids = [f['properties']['OBJECTID'] for f in fs]
    before = json.loads((EVIDENCE / 'ids-json.body').read_text())['objectIds']
    after = json.loads((EVIDENCE / 'ids-after.body').read_text())['objectIds']
    count = json.loads((EVIDENCE / 'count-json.body').read_text())['count']
    assert len(ids) == len(set(ids)) == len(before) == len(set(before)) == len(after) == len(set(after)) == count == 63
    assert None not in ids and set(ids) == set(before) == set(after)
    assert not source.get('exceededTransferLimit', False)
    src = {f['properties']['BOUND_ID']: f for f in fs}
    pub = {r['BOUND_ID']: r for r in rows}
    historical = {f['attributes']['BOUND_ID']: f for f in old['features']}
    assert len(src) == len(fs) and len(pub) == len(rows) and len(historical) == len(old['features'])
    shared = sorted(src.keys() & pub.keys())
    columns = sorted({k for f in fs for k in f['properties']})
    assert all(set(f['properties']) == set(columns) for f in fs), 'inconsistent source columns'
    fields = {}
    for col in columns:
        fields[col] = {
            'esri_declared_type': next(f['type'] for f in old['fields'] if f['name'] == col),
            'source_json_value_types': sorted({type(f['properties'][col]).__name__ for f in fs}),
            'candidate_arrow_type': str(table.schema.field(col).type) if col in table.column_names else None,
            'source_nulls': sum(f['properties'][col] is None for f in fs),
            'candidate_nulls': table.column(col).null_count if col in table.column_names else None,
            'unequal_values': [k for k in shared if src[k]['properties'][col] != pub[k].get(col)],
            'unequal_python_types': [k for k in shared if type(src[k]['properties'][col]) is not type(pub[k].get(col))],
            'changed_since_reviewed_capture': [k for k in sorted(src.keys() & historical.keys()) if src[k]['properties'][col] != historical[k]['attributes'][col]],
        }
    geometry = []
    source_shapes, candidate_shapes = [], []
    for key in shared:
        fresh = shape(src[key]['geometry'])
        baseline = shapely.from_wkb(pub[key]['geometry'])
        previous = MultiLineString(historical[key]['geometry']['paths'])
        # Comparison-only representation adaptation: do not change retained inputs or claim a mapping approval.
        as_multi = MultiLineString([fresh]) if fresh.geom_type == 'LineString' else fresh
        source_shapes.append(fresh)
        candidate_shapes.append(baseline)
        geometry.append({
            'BOUND_ID': key, 'OBJECTID': src[key]['properties']['OBJECTID'],
            'source_type': fresh.geom_type, 'candidate_type': baseline.geom_type,
            'source_valid': bool(fresh.is_valid), 'candidate_valid': bool(baseline.is_valid),
            'source_empty': bool(fresh.is_empty), 'candidate_empty': bool(baseline.is_empty),
            'exact_same_representation': fresh.equals_exact(baseline, 0),
            'exact_after_single_line_wrapper': as_multi.equals_exact(baseline, 0),
            'wkb_equal_after_single_line_wrapper': shapely.to_wkb(as_multi) == pub[key]['geometry'],
            'topologically_equal': fresh.equals(baseline),
            'hausdorff_distance_degrees': fresh.hausdorff_distance(baseline),
            'reviewed_raw_wkb_equal': shapely.to_wkb(previous) == pub[key]['geometry'],
            'bbox_equal': list(fresh.bounds) == [pub[key]['bbox'][n] for n in ['xmin', 'ymin', 'xmax', 'ymax']],
            'published_id_equals_SOURCE_OBJECTID': pub[key]['id'] == src[key]['properties']['OBJECTID'],
        })
    schema = pq.ParquetFile(baseline_path).schema
    report = {
        'acceptance': 'CANDIDATE_UNAPPROVED; comparison is research, not promotion authorization',
        'baseline_sha256': pin['sha256'], 'source_capture_sha256': digest(EVIDENCE / 'features-geojson.json.gz'),
        'extraction': {'features': len(fs), 'before_ids': len(before), 'after_ids': len(after), 'count': count, 'all_sets_equal': True, 'max_record_count': 1000, 'transfer_limit_exceeded': False, 'method': 'full GeoJSON response below advertised limit; independent IDs before/after and independent count; temporal transaction isolation not guaranteed'},
        'source_rows': len(fs), 'candidate_rows': len(rows),
        'source_only_keys': sorted(src.keys() - pub.keys()), 'candidate_only_keys': sorted(pub.keys() - src.keys()),
        'all_source_OBJECTIDs': sorted(ids), 'all_source_BOUND_IDs': sorted(src),
        'candidate_only_columns': sorted(set(table.column_names) - set(columns)),
        'source_only_columns': sorted(set(columns) - set(table.column_names)),
        'candidate_arrow_schema': str(table.schema), 'candidate_parquet_physical_schema': str(schema).split('\n', 1)[1],
        'candidate_geo_metadata': json.loads(table.schema.metadata[b'geo']),
        'source_crs': 'RFC7946 longitude/latitude; old Esri response explicitly wkid4326',
        'fields': fields, 'geometries': geometry,
        'source_bounds': list(shapely.total_bounds(source_shapes)),
        'candidate_bounds': list(shapely.total_bounds(candidate_shapes)),
        'source_regions': dict(sorted(collections.Counter(f['properties']['REGION'] for f in fs).items())),
        'candidate_regions': dict(sorted(collections.Counter(r['REGION'] for r in rows).items())),
        'schema_decisions': ['UNILATERAL is esriFieldTypeDouble but published int64; all observed values 0 does not approve future narrowing.', 'RULEID esriFieldTypeInteger and OBJECTID OID published int64; dates retained as epoch-millisecond int64, not timestamps.', 'Published extra id duplicates OBJECTID in all 63 rows; bbox and geometry extra fields need explicit mapping approval.', '57 fresh LineString geometries versus published MultiLineString; all exact WKB only after explicit single-part wrapping.'],
    }
    return report


if __name__ == '__main__':
    result = run(pathlib.Path(sys.argv[1]))
    print(json.dumps(result, indent=2, allow_nan=False))
