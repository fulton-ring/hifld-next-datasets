"""Compare complete pinned candidate and producer ZIPs; no acceptance or data writes.

Projection and area-code grouping are diagnostics only, never a published mapping.
Raw values, physical types, IDs and geometry hashes remain independently reported.
"""
import argparse
import collections
import hashlib
import gzip
import json
import pathlib
import zipfile

import fiona
import pyarrow.parquet as pq
import pyproj
from shapely import from_wkb
from shapely.geometry import shape
from shapely.ops import transform

ROOT = pathlib.Path(__file__).parent
E = ROOT / 'evidence'
PIN = '878e40fea860e5dcd2979e391c5b70fad0d5290e7f8b3785b5831b6eae40e1c4'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summarize(rows, geometry, schema, crs):
    keys = list(rows[0])
    return dict(count=len(rows), schema=schema, crs=crs,
                null_counts={k: sum(r[k] is None for r in rows) for k in keys},
                identifiers={k: dict(unique=len({r[k] for r in rows}),
                                    nulls=sum(r[k] is None for r in rows))
                             for k in keys if k in ('GEOID', 'GEOIDFQ', 'AIANNH', 'AIANNHCE', 'AIANNHNS', 'AIANNHFP1', 'AIANNHFP2', 'AIANNHFP3', 'OID', 'OBJECTID', 'id')},
                geometry_types=dict(collections.Counter(g.geom_type for g in geometry)),
                invalid_geometry=sum(not g.is_valid for g in geometry),
                empty_geometry=sum(g.is_empty for g in geometry),
                bounds=[min(g.bounds[i] for g in geometry) if i < 2 else max(g.bounds[i] for g in geometry) for i in range(4)])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=pathlib.Path, required=True)
    parser.add_argument('--archive-dir', type=pathlib.Path, required=True)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--rows-output', type=pathlib.Path, required=True)
    args = parser.parse_args()
    assert digest(args.baseline) == PIN, 'Published candidate checksum mismatch'
    table = pq.read_table(args.baseline)
    published = table.to_pylist()
    pgeom = [from_wkb(r['geometry']) for r in published]
    metadata = json.loads(table.schema.metadata[b'geo'])
    pcrs = pyproj.CRS(metadata['columns']['geometry']['crs'])
    pattrs = [{k: v for k, v in r.items() if k != 'geometry'} for r in published]
    report = dict(approved=False, baseline_sha256=PIN,
                  baseline=summarize(pattrs, pgeom, {f.name: str(f.type) for f in table.schema}, pcrs.to_string()),
                  baseline_categories={k: dict(collections.Counter(r[k] for r in pattrs)) for k in ('AIANNHCOMP', 'AIANNHCC', 'MTFCC')},
                  comparisons={}, limitations=[
                      'Candidate acceptance, row key stability and schema/column mapping remain unapproved.',
                      'Exact topology after CRS conversion is diagnostic; no tolerance, geometry repair, simplification or replacement is approved.',
                      'AIANNH/AIANNHCE grouping compares code membership only; matching code is not a verified identity or polygon coverage match.',
                      'The 163 published M rows have combined reservation/trust names; source R/T rows are components. No automated M to R/T mapping is authorized.'])
    pin_requests = json.loads((E / 'requests.json').read_text())
    for year in (2023, 2025, 2026):
        archive = args.archive_dir / f'zip{year}.body'
        expected = next(x['sha256'] for x in pin_requests if x['name'] == f'zip{year}')
        assert digest(archive) == expected, f'{year}: original ZIP checksum mismatch'
        with zipfile.ZipFile(archive) as z:
            assert z.testzip() is None, 'ZIP CRC mismatch'
        with fiona.open(f'/vsizip/{{{archive.resolve()}}}/tl_{year}_us_aiannh.shp') as src:
            schema = dict(src.schema)
            crs = src.crs.to_string()
            features = list(src)
        attrs = [dict(f['properties']) for f in features]
        geom = [shape(f['geometry']) for f in features]
        retained = json.loads((E / f'rows{year}.json').read_text())
        assert [r['properties'] for r in retained] == attrs
        assert [r['geometry_sha256'] for r in retained] == [hashlib.sha256(g.wkb).hexdigest() for g in geom]
        a = {r['GEOID']: (r, g) for r, g in zip(attrs, geom)}
        b = {r['GEOID']: (r, g) for r, g in zip(pattrs, pgeom)}
        assert len(a) == len(attrs) and len(b) == len(pattrs)
        shared = sorted(set(a) & set(b))
        cols = sorted(set(attrs[0]) & set(pattrs[0]))
        projector = pyproj.Transformer.from_crs(pcrs, crs, always_xy=True)
        projected = {k: transform(projector.transform, b[k][1]) for k in shared}
        valid = [k for k in shared if a[k][1].is_valid and projected[k].is_valid]
        source_codes = collections.defaultdict(list)
        pub_codes = collections.defaultdict(list)
        for r in attrs:
            source_codes[r['AIANNHCE']].append(r['GEOID'])
        for r in pattrs:
            pub_codes[r['AIANNH']].append(r['GEOID'])
        changed_grain = {k: {'source_component_geoids': sorted(source_codes[k]), 'published_geoids': sorted(pub_codes[k])}
                         for k in sorted(set(source_codes) & set(pub_codes)) if sorted(source_codes[k]) != sorted(pub_codes[k])}
        code_shared = sorted(set(source_codes) & set(pub_codes))
        grouped_values = {
            'method': 'Diagnostic AIANNHCE vs AIANNH code grouping; sum integer ALAND/AWATER versus parsed AREALAND/AREAWATER strings only in this report; raw types remain separate',
            'groups_compared': len(code_shared),
            'equal_summed_land': sum(sum(a[i][0]['ALAND'] for i in source_codes[k]) == int(next(r['AREALAND'] for r in pattrs if r['AIANNH'] == k)) for k in code_shared),
            'equal_summed_water': sum(sum(a[i][0]['AWATER'] for i in source_codes[k]) == int(next(r['AREAWATER'] for r in pattrs if r['AIANNH'] == k)) for k in code_shared),
            'equal_gnis_identifier_sets': sum({a[i][0]['AIANNHNS'] for i in source_codes[k]} == {r['AIANNHNS'] for r in pattrs if r['AIANNH'] == k} for k in code_shared),
        }
        report['comparisons'][str(year)] = dict(
            source_sha256=expected, source=summarize(attrs, geom, schema, crs),
            source_categories={k: dict(collections.Counter(r[k] for r in attrs)) for k in ('COMPTYP', 'CLASSFP', 'MTFCC')},
            exact_geoid_shared=len(shared), source_only_geoids=sorted(set(a)-set(b)), published_only_geoids=sorted(set(b)-set(a)),
            source_only_columns=sorted(set(attrs[0])-set(pattrs[0])), published_only_columns=sorted(set(pattrs[0])-set(attrs[0])),
            changed_shared_attribute_counts={k: sum(a[i][0][k] != b[i][0][k] for i in shared) for k in cols},
            geometry_comparison=dict(method='Reproject complete published EPSG:3857 polygons to source EPSG:4269 using pyproj always_xy; no rounding/repair',
                                     all_shared=len(shared), valid_pairs=len(valid), invalid_pairs_skipped=len(shared)-len(valid),
                                     identical_raw_wkb_sha256=sum(hashlib.sha256(a[k][1].wkb).digest() == hashlib.sha256(b[k][1].wkb).digest() for k in shared),
                                     topologically_equal_after_projection=sum(a[k][1].equals(projected[k]) for k in valid)),
            code_grouping_diagnostic=dict(source_unique_area_codes=len(source_codes), published_unique_area_codes=len(pub_codes),
                                          shared_area_codes=len(set(source_codes) & set(pub_codes)),
                                          source_only_area_codes=sorted(set(source_codes)-set(pub_codes)), published_only_area_codes=sorted(set(pub_codes)-set(source_codes)),
                                          changed_grain=changed_grain, grouped_attribute_diagnostic=grouped_values))
    retained_rows = [dict(properties=r, geometry_sha256=hashlib.sha256(g.wkb).hexdigest(), geometry_type=g.geom_type, valid=g.is_valid, empty=g.is_empty) for r, g in zip(pattrs, pgeom)]
    args.rows_output.write_bytes(gzip.compress((json.dumps(retained_rows, separators=(',', ':')) + '\n').encode(), mtime=0))
    report['baseline_rows_sha256'] = digest(args.rows_output)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: {f: v[f] for f in ('exact_geoid_shared', 'geometry_comparison', 'code_grouping_diagnostic')} for k, v in report['comparisons'].items()}, indent=2))


if __name__ == '__main__':
    main()
