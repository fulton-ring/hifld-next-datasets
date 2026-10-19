"""Full-population field/type/value/geometry audit of isolated terminal formats."""
import hashlib
import json
import pathlib

import pyarrow.parquet as pq
import pyogrio
from pyproj import CRS
from shapely import from_wkb


def audit_formats(staging_root: pathlib.Path, candidate_prefix: str, version_prefix: str) -> dict:
    original = pq.read_table(staging_root / candidate_prefix / 'candidate/airspace.parquet')
    expected = {r['GLOBAL_ID']: r for r in original.to_pylist()}
    assert len(expected) == original.num_rows == 477
    result = {'expected_rows': 477, 'join_key': 'GLOBAL_ID', 'formats': {}, 'publication_approved': False}
    for format_name, pattern in [('geopackage', '*.gpkg'), ('geoparquet', '*.parquet')]:
        files = sorted((staging_root / version_prefix / format_name).rglob(pattern))
        assert len(files) == 1, (format_name, files)
        path = files[0]
        if format_name == 'geopackage':
            metadata, table = pyogrio.read_arrow(path)
            geometry_name = metadata['geometry_name']
            crs = CRS.from_user_input(metadata['crs'])
        else:
            table = pq.read_table(path)
            geo = json.loads(table.schema.metadata[b'geo'])
            geometry_name = geo['primary_column']
            crs = CRS.from_user_input(geo['columns'][geometry_name]['crs'])
        actual = {r['GLOBAL_ID']: r for r in table.to_pylist()}
        assert len(actual) == table.num_rows == 477 and set(actual) == set(expected)
        assert crs.to_epsg() == 4269
        changed = {}
        for name in original.column_names:
            if name == 'geometry':
                changed[name] = sum(not from_wkb(expected[key][name]).equals_exact(
                    from_wkb(actual[key][geometry_name]), 0, normalize=True) for key in expected)
            else:
                assert name in table.column_names, (format_name, name)
                assert original.schema.field(name).type == table.schema.field(name).type, (
                    format_name, name, original.schema.field(name).type, table.schema.field(name).type)
                changed[name] = sum(expected[key][name] != actual[key][name] for key in expected)
        assert not any(changed.values()), (format_name, changed)
        geometry_wkb_changes = sum(expected[key]['geometry'] != actual[key][geometry_name] for key in expected)
        assert geometry_wkb_changes == 0, (format_name, geometry_wkb_changes)
        nullable_changes={name: {'source': original.schema.field(name).nullable, 'output': table.schema.field(name).nullable} for name in original.column_names if name in table.column_names and original.schema.field(name).nullable != table.schema.field(name).nullable}
        result['formats'][format_name] = {
            'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'rows': table.num_rows, 'crs': crs.to_string(), 'geometry_column': geometry_name,
            'all_original_columns_retained': True, 'all_original_physical_types_retained': True,
            'all_original_values_and_geometries_retained': True, 'changed_counts': changed,
            'native_geometry_wkb_byte_changes': geometry_wkb_changes,
            'all_original_nullability_retained': not nullable_changes, 'nullability_changes_requiring_mapping_decision': nullable_changes,
            'additional_columns_requiring_mapping_decision': sorted(set(table.column_names) - set(original.column_names)),
            'schema': [{'name': field.name, 'type': str(field.type), 'nullable': field.nullable} for field in table.schema],
        }
    return result
