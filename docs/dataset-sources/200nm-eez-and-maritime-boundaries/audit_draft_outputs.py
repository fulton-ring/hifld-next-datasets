"""Audit every real NOAA row/field/vertex across local draft outputs, without writes."""
import argparse
import collections
import json
from pathlib import Path

import fiona
import pyarrow.parquet as pq
from shapely import from_wkb
from shapely.geometry import shape


def parts(g):
    return [list(g.coords)] if g.geom_type == 'LineString' else [list(p.coords) for p in g.geoms]


def compare(source, records, name, geometry_column=None):
    indexed = {r['OBJECTID']: r for r in records}
    expected = {f['attributes']['OBJECTID']: f for f in source['features']}
    report = {'rows': len(records), 'unique_ids': len(indexed), 'ids_match': set(indexed) == set(expected),
              'fields': {}, 'geometry_coordinate_difference_ids': [], 'geometry_type_difference_ids': []}
    if len(indexed) != len(records) or set(indexed) != set(expected):
        raise ValueError(f'{name}: ID mismatch or duplication')
    for field in next(iter(expected.values()))['attributes']:
        value_diffs, type_diffs = [], []
        for key, original in expected.items():
            previous = original['attributes'][field]
            value = indexed[key].get(field)
            if value != previous:
                value_diffs.append({'OBJECTID': key, 'source': previous, 'output': value})
            if type(value) is not type(previous):
                type_diffs.append({'OBJECTID': key, 'source_type': type(previous).__name__, 'output_type': type(value).__name__})
        report['fields'][field] = {'value_difference_count': len(value_diffs), 'value_differences': value_diffs,
                                   'physical_python_type_difference_count': len(type_diffs), 'physical_type_differences': type_diffs}
    for key, original in expected.items():
        source_paths = [[tuple(v) for v in p] for p in original['geometry']['paths']]
        g = indexed[key]['__geometry__']
        if parts(g) != source_paths:
            report['geometry_coordinate_difference_ids'].append(key)
        expected_type = 'LineString' if len(source_paths) == 1 else 'MultiLineString'
        if g.geom_type != expected_type:
            report['geometry_type_difference_ids'].append(key)
    report['all_values_and_coordinates_preserved'] = not any(v['value_difference_count'] for v in report['fields'].values()) and not report['geometry_coordinate_difference_ids']
    report['all_physical_attribute_types_preserved'] = not any(v['physical_python_type_difference_count'] for v in report['fields'].values())
    return report


def audit(root):
    source = json.loads((root / 'raw/features.json').read_text())
    manifest = json.loads((root / 'metadata/source_manifest.json').read_text())
    outputs = {}
    for kind, files in [('native_candidate', root.glob('candidate/geoparquet/*.parquet')), ('terminal_geoparquet', root.glob('geoparquet/*.parquet'))]:
        records = []
        for path in files:
            table = pq.read_table(path)
            column = json.loads(table.schema.metadata[b'geo'])['primary_column']
            for row in table.to_pylist():
                row['__geometry__'] = from_wkb(row.pop(column)); records.append(row)
        if records:
            outputs[kind] = compare(source, records, kind)
            outputs[kind]['column_mapping'] = {'source_geometry': 'SHAPE', 'output_geometry': column}
            outputs[kind]['extra_columns'] = sorted(set(records[0]) - set(source['features'][0]['attributes']) - {'__geometry__'})
    for kind, files in [('geopackage', root.glob('geopackage/*.gpkg')), ('shapefile_zip', root.glob('shapefile/*.zip'))]:
        for path in files:
            with fiona.open('zip://' + str(path) if path.suffix == '.zip' else path) as dataset:
                records = [{**dict(f['properties']), '__geometry__': shape(f['geometry'])} for f in dataset]
                outputs[kind] = compare(source, records, kind)
                outputs[kind]['fiona_schema'] = dict(dataset.schema)
                outputs[kind]['quarantined_derivative'] = kind in manifest.get('unsupported_output_formats', {})
    for kind in ('native_candidate', 'geopackage'):
        assert outputs[kind]['all_values_and_coordinates_preserved'], kind
        assert outputs[kind]['all_physical_attribute_types_preserved'], kind
    return {'classification': 'Draft full real-input audit; no acceptance or publication approval',
            'rows': len(source['features']), 'outputs': outputs,
            'unsupported_output_formats': manifest.get('unsupported_output_formats', {}),
            'shapefile_skipped': not list(root.glob('shapefile/*.zip')),
            'pmtiles': 'Visualization derivative: tiling, quantization and tile feature repetition are not a lossless source geometry/row representation; not evaluated as a replacement baseline',
            'provenance_inputs': manifest['inputs'], 'baseline_accepted': False, 'contract_approved': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--staged-version', required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(audit(args.staged_version), indent=2, sort_keys=True))
