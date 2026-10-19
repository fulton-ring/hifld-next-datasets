"""Compare complete pinned research inputs; never write or approve dataset output.

Usage: uv run --frozen python <this-file> --baseline /path/to/preserved.parquet
All input checksums are checked before parsing. Output is a deterministic report.
"""
import argparse
import collections
import gzip
import hashlib
import json
import re
from pathlib import Path

import pyarrow.parquet as pq
from shapely import from_wkb
from shapely.geometry import shape

EVIDENCE = Path(__file__).parent / 'evidence'


def read(name):
    raw = (EVIDENCE / name).read_bytes()
    return json.loads(gzip.decompress(raw) if name.endswith('.gz') else raw)


def counts(values):
    return dict(sorted(collections.Counter(values).items()))


def paths(geometry):
    """Observe ordered coordinate parts; do not normalize, repair, round or publish."""
    if geometry.geom_type == 'LineString':
        return [list(geometry.coords)]
    if geometry.geom_type == 'MultiLineString':
        return [list(part.coords) for part in geometry.geoms]
    raise ValueError(f'Unexpected geometry: {geometry.geom_type}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    args = parser.parse_args()
    for name, fingerprint in read('comparison-inputs.json').items():
        raw = (EVIDENCE / name).read_bytes()
        assert len(raw) == fingerprint['bytes'], name
        assert hashlib.sha256(raw).hexdigest() == fingerprint['sha256'], name
    manifest = read('published-candidate.json')
    raw = args.baseline.read_bytes()
    assert len(raw) == manifest['bytes']
    assert hashlib.sha256(raw).hexdigest() == manifest['sha256']
    assert hashlib.sha256((EVIDENCE / manifest['catalog_capture']).read_bytes()).hexdigest() == manifest['catalog_sha256']
    table = pq.read_table(args.baseline)
    rows = table.to_pylist()
    source = read('all.json.gz')
    geojson = read('geojson.json.gz')
    layer = read('layer.body')
    for body in [source, geojson, layer]:
        assert 'error' not in body
    sf = source['features']
    gf = geojson['features']
    assert not source.get('exceededTransferLimit')
    assert len(sf) == len(gf) == read('count.json')['count'] == 96
    source_ids = [f['attributes']['OBJECTID'] for f in sf]
    baseline_ids = [r['OBJECTID'] for r in rows]
    projection_ids = [f['attributes']['OBJECTID'] for f in read('id-page.json')['features']]
    for ids in [source_ids, baseline_ids, projection_ids]:
        assert None not in ids and len(ids) == len(set(ids))
    assert set(source_ids) == set(projection_ids)
    by_source = {f['attributes']['OBJECTID']: f for f in sf}
    by_geo = {f['properties']['OBJECTID']: f for f in gf}
    by_base = {r['OBJECTID']: r for r in rows}
    assert set(by_geo) == set(by_source)
    shared = sorted(set(by_source) & set(by_base))
    fields = [f for f in layer['fields'] if f['type'] != 'esriFieldTypeGeometry']
    attribute_diffs = {}
    for f in fields:
        name = f['name']
        differences = [k for k in shared if by_source[k]['attributes'][name] != by_base[k].get(name)]
        type_pairs = counts(f"{type(by_source[k]['attributes'][name]).__name__}->{type(by_base[k].get(name)).__name__}" for k in shared)
        attribute_diffs[name] = {'value_difference_ids': differences, 'python_type_pairs': type_pairs,
                                'source_declared_type': f['type'],
                                'baseline_arrow_type': str(table.schema.field(name).type),
                                'source_declared_nullable': f.get('nullable'),
                                'baseline_declared_nullable': table.schema.field(name).nullable,
                                'source_nulls': sum(by_source[k]['attributes'][name] is None for k in shared),
                                'baseline_nulls': sum(by_base[k].get(name) is None for k in shared)}
    geometry_details = []
    for key in shared:
        g = from_wkb(by_base[key]['geometry'])
        s = shape(by_geo[key]['geometry'])
        esri_paths = [[tuple(x) for x in part] for part in by_source[key]['geometry']['paths']]
        bbox = by_base[key]['bbox']
        geometry_details.append({'OBJECTID': key, 'source_geojson_type': s.geom_type,
                                 'baseline_type': g.geom_type,
                                 'exact_same_geometry_type_and_coordinates': g.equals_exact(s, 0),
                                 'ordered_parts_coordinates_equal_esri': paths(g) == esri_paths,
                                 'ordered_parts_coordinates_equal_geojson': paths(g) == paths(s),
                                 'topological_equals': g.equals(s),
                                 'hausdorff_distance_degrees': g.hausdorff_distance(s),
                                 'bbox_matches_geometry': list(g.bounds) == [bbox[k] for k in ['xmin', 'ymin', 'xmax', 'ymax']],
                                 'baseline_valid_nonempty': g.is_valid and not g.is_empty,
                                 'source_valid_nonempty': s.is_valid and not s.is_empty})
    out = {
        'acceptance': 'unapproved_candidate_comparison_only',
        'producer_schema_org': [json.loads(x) for x in re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', gzip.decompress((EVIDENCE / 'inport-item.body.gz').read_bytes()).decode(), re.S)],
        'baseline_sha256': manifest['sha256'], 'baseline_generation': manifest['generation'],
        'baseline_rows': len(rows), 'source_rows': len(sf),
        'physical_schema': [{'name': f.name, 'type': str(f.type), 'nullable': f.nullable,
                             'metadata': {k.decode(): v.decode() for k, v in (f.metadata or {}).items()}} for f in table.schema],
        'baseline_geo_metadata': json.loads(table.schema.metadata[b'geo']),
        'source_fields': layer['fields'],
        'baseline_only_columns': sorted(set(table.column_names) - {f['name'] for f in layer['fields']}),
        'source_only_columns': sorted({f['name'] for f in layer['fields']} - set(table.column_names)),
        'source_ids': sorted(source_ids), 'baseline_ids': sorted(baseline_ids),
        'source_only_ids': sorted(set(source_ids) - set(baseline_ids)),
        'baseline_only_ids': sorted(set(baseline_ids) - set(source_ids)),
        'identifier_observations': {name: {'source_values_by_objectid': {str(k): by_source[k]['attributes'].get(name) for k in shared},
                                         'baseline_values_by_objectid': {str(k): by_base[k][name] for k in shared},
                                         'baseline_nulls': sum(r[name] is None for r in rows),
                                         'baseline_distinct_nonnull': len({r[name] for r in rows if r[name] is not None})}
                                    for name in ['OBJECTID', 'BOUND_ID', 'RULEID', 'id']},
        'baseline_id_equals_geojson_feature_id_every_row': all(by_base[k]['id'] == by_geo[k]['id'] and type(by_base[k]['id']) is type(by_geo[k]['id']) for k in shared),
        'source_geojson_properties_equal_esri_attributes': all(by_geo[k]['properties'] == by_source[k]['attributes'] for k in by_source),
        'baseline_id_equals_objectid_every_row': all(r['id'] == r['OBJECTID'] and type(r['id']) is type(r['OBJECTID']) for r in rows),
        'attribute_diffs': attribute_diffs,
        'coverage': {name: {'source': counts(f['attributes'][name] for f in sf),
                            'baseline': counts(r[name] for r in rows)} for name in ['REGION', 'FEAT_TYPE']},
        'geometry_method': 'Decode pinned WKB and retained GeoJSON/Esri paths; exact ordered parts and coordinate tuples, no normalization/rounding/repair/CRS transform. Topological equality is separate evidence, not schema compatibility approval.',
        'geometry': geometry_details,
        'geometry_summary': {name: sum(row[name] for row in geometry_details) for name in [
            'exact_same_geometry_type_and_coordinates', 'ordered_parts_coordinates_equal_esri',
            'ordered_parts_coordinates_equal_geojson', 'topological_equals', 'bbox_matches_geometry',
            'baseline_valid_nonempty', 'source_valid_nonempty']},
        'source_geometry_types': counts(row['source_geojson_type'] for row in geometry_details),
        'baseline_geometry_types': counts(row['baseline_type'] for row in geometry_details),
    }
    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
