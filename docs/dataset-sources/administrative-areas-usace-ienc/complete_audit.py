"""Complete candidate comparison, with raw pages preserved outside Git; no acceptance.

Use --capture to download, then replay without network. Runtime files must be kept.
No ID mapping or geometry repair is applied. GeoJSON is service-generated WGS84.
"""
import argparse
from collections import Counter
import datetime
import gzip
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import requests
import shapely

HERE = Path(__file__).parent
URL = 'https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/USACE_IENC_Master_Service/FeatureServer/56/query'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def request(params):
    r = requests.get(URL, params=params, timeout=120)
    r.raise_for_status()
    assert len(r.content) < 25_000_000, 'page exceeds bounded 25 MB'
    data = r.json()
    assert 'error' not in data, data.get('error')
    return r, data


def capture(root):
    root.mkdir(parents=True, exist_ok=True)
    assert not root.joinpath('geometry-pages.json').exists(), 'preserve existing full source; use a fresh --runtime directory'
    start, ids = request({'f':'json', 'where':'1=1', 'returnIdsOnly':'true'})
    count_response, count = request({'f':'json', 'where':'1=1', 'returnCountOnly':'true'})
    expected = sorted(ids['objectIds'])
    assert len(expected) == len(set(expected)) == count['count']
    receipts = []
    for offset in range(0, len(expected), 25):
        selected = expected[offset:offset + 25]
        r, data = request({'f':'geojson','objectIds':','.join(map(str,selected)),
                           'outFields':'*','returnGeometry':'true','outSR':4326})
        actual = sorted(f['properties']['OBJECTID'] for f in data['features'])
        assert actual == selected, (offset, actual, selected)
        assert not data.get('exceededTransferLimit', False)
        name = f'geometry-{offset:04}.geojson.gz'
        compressed = gzip.compress(r.content, mtime=0)
        root.joinpath(name).write_bytes(compressed)
        receipts.append({'file':name,'url':r.url,'checked_at':datetime.datetime.now(datetime.UTC).isoformat(),
                         'status':r.status_code,'bytes':len(r.content),'gzip_bytes':len(compressed),
                         'sha256_raw':digest(r.content),'sha256_gzip':digest(compressed),
                         'ids':selected,'count':len(actual),'exceededTransferLimit':False})
        print(offset,len(r.content),flush=True)
    end, after = request({'f':'json','where':'1=1','returnIdsOnly':'true'})
    endcount, aftercount = request({'f':'json','where':'1=1','returnCountOnly':'true'})
    assert sorted(after['objectIds']) == expected and aftercount == count
    for name, response in [('ids-before',start),('ids-after',end),('count-before',count_response),('count-after',endcount)]:
        root.joinpath(name+'.json').write_bytes(response.content)
    manifest = {'source_url':URL,'format':'service-generated GeoJSON; EPSG:4326; no precision or simplification parameters',
                'expected_count':len(expected),'expected_ids':expected,'ids_unchanged':True,'pages':receipts}
    root.joinpath('geometry-pages.json').write_text(json.dumps(manifest,indent=2)+'\n')


def geom_summary(geoms):
    bounds = [g.bounds for g in geoms if g is not None and not g.is_empty]
    return {'nulls':sum(g is None for g in geoms),'empty':sum(g is not None and g.is_empty for g in geoms),
            'invalid':sum(g is not None and not g.is_valid for g in geoms),
            'types':dict(Counter(g.geom_type if g is not None else None for g in geoms)),
            'bounds':[min(b[0] for b in bounds),min(b[1] for b in bounds),max(b[2] for b in bounds),max(b[3] for b in bounds)]}


def summarize(root, baseline, pin):
    manifest = json.loads(root.joinpath('geometry-pages.json').read_text())
    rows, geoms, inventory = [], [], []
    for entry in manifest['pages']:
        compressed = root.joinpath(entry['file']).read_bytes()
        assert digest(compressed) == entry['sha256_gzip']
        raw = gzip.decompress(compressed)
        assert digest(raw) == entry['sha256_raw']
        data = json.loads(raw)
        assert 'error' not in data and not data.get('exceededTransferLimit',False)
        assert sorted(f['properties']['OBJECTID'] for f in data['features']) == entry['ids']
        for feature in data['features']:
            rows.append(feature['properties'])
            g = shapely.from_geojson(json.dumps(feature['geometry'])) if feature['geometry'] else None
            geoms.append(g)
            inventory.append({'OBJECTID':rows[-1]['OBJECTID'], 'Feature_ID':rows[-1]['Feature_ID'],
                              'Source_Dataset':rows[-1]['Source_Dataset'],'geometry_sha256':digest(shapely.to_wkb(g)) if g is not None else None})
    ids = [row['OBJECTID'] for row in rows]
    assert len(ids) == len(set(ids)) == manifest['expected_count']
    assert sorted(ids) == manifest['expected_ids']
    for name in ['ids-before','ids-after']:
        assert sorted(json.loads(root.joinpath(name+'.json').read_text())['objectIds']) == sorted(ids)
    for name in ['count-before','count-after']:
        assert json.loads(root.joinpath(name+'.json').read_text())['count'] == len(rows)
    oldrows = []
    for e in json.loads(HERE.joinpath('evidence/pages.json').read_text()):
        oldrows.extend(f['attributes'] for f in json.loads(gzip.decompress(HERE.joinpath('evidence',e['file']).read_bytes()))['features'])
    assert {r['OBJECTID']:r for r in rows} == {r['OBJECTID']:r for r in oldrows}, 'attribute snapshot changed'
    rawbaseline = baseline.read_bytes()
    assert digest(rawbaseline) == pin['sha256'] and len(rawbaseline) == pin['bytes']
    table = pq.read_table(baseline)
    brow = table.to_pylist()
    bgeom = [shapely.from_wkb(r['geometry']) if r['geometry'] else None for r in brow]
    common = sorted(set(rows[0]) & set(table.column_names))
    fields = {}
    for name in sorted(set(rows[0]) | set(table.column_names)):
        a = [r.get(name) for r in rows] if name in rows[0] else None
        b = [r[name] for r in brow] if name in table.column_names else None
        if name in ['geometry','bbox']: continue
        fields[name] = {'source_types':sorted({type(v).__name__ for v in a}) if a is not None else None,
                        'published_type':str(table.schema.field(name).type) if b is not None else None,
                        'source_nulls':a.count(None) if a is not None else None,'published_nulls':b.count(None) if b is not None else None,
                        'source_distinct':len(set(a)) if a is not None else None,'published_distinct':len(set(b)) if b is not None else None,
                        'shared_distinct_values':len(set(a)&set(b)) if a is not None and b is not None else None,
                        'source_only_distinct_values':len(set(a)-set(b)) if a is not None and b is not None else None,
                        'published_only_distinct_values':len(set(b)-set(a)) if a is not None and b is not None else None}
    # Geometry hashes compare the complete multisets without inventing an ID join.
    a = Counter(digest(shapely.to_wkb(shapely.normalize(g))) for g in geoms if g is not None)
    b = Counter(digest(shapely.to_wkb(shapely.normalize(g))) for g in bgeom if g is not None)
    # Explicitly diagnostic precision grids address ArcGIS GeoJSON decimal formatting.
    # These derived copies never replace raw geometries or establish an approved tolerance.
    diagnostics = {}
    for grid in [1e-9, 1e-7]:
        ag = Counter(digest(shapely.to_wkb(shapely.normalize(shapely.set_precision(g, grid)))) for g in geoms if g is not None)
        bg = Counter(digest(shapely.to_wkb(shapely.normalize(shapely.set_precision(g, grid)))) for g in bgeom if g is not None)
        diagnostics[str(grid)] = {'grid_degrees':grid,'source_distinct_geometries':len(ag),
                                  'published_distinct_geometries':len(bg),'multiset_intersection':sum((ag&bg).values()),
                                  'source_unmatched_occurrences':sum((ag-bg).values()),'published_unmatched_occurrences':sum((bg-ag).values())}
    result = {'scope':'Unapproved published candidate vs complete candidate source; no accepted ID mapping, deduplication, geometry repair or promotion',
              'published_candidate':pin,'source_rows':len(rows),'published_rows':len(brow),
              'complete_source_attributes_equal_prior_2966_row_capture':True,
              'source_only_fields':sorted(set(rows[0])-set(table.column_names)),
              'published_only_fields':sorted(set(table.column_names)-set(rows[0])),
              'common_field_value_domains':fields,'source_geometry':geom_summary(geoms),'published_geometry':geom_summary(bgeom),
              'exact_normalized_geometry_multiset_intersection':sum((a&b).values()),
              'diagnostic_precision_grid_comparisons':diagnostics,
              'geometry_comparison_limit':'Exact normalized coordinate hashes only; no tolerance, repair or CRS substitution. Hash inequality does not prove topological inequality.',
              'source_chart_counts':dict(Counter(r['Source_Dataset'] for r in rows)),
              'published_unmapped_source_field_domains':{name:dict(Counter(r[name] for r in brow)) for name in ['Source_Dat','Source_Ind','Source_D_1'] if name in table.column_names},
              'id_join':'None accepted; shared OBJECTID domains are service row numbers, not verified cross-release identity',
              'full_source_runtime':str(root),'replay':'complete_audit.py --runtime <preserved directory> --baseline <pinned parquet>',
              'approval_required':True}
    HERE.joinpath('evidence/complete-comparison.json').write_text(json.dumps(result,indent=2)+'\n')
    HERE.joinpath('evidence/geometry-pages.json').write_text(json.dumps(manifest,indent=2)+'\n')
    HERE.joinpath('evidence/source-feature-geometry-hashes.json.gz').write_bytes(gzip.compress(json.dumps(inventory,separators=(',',':')).encode(),mtime=0))
    print(json.dumps({k:result[k] for k in ['source_rows','published_rows','source_geometry','published_geometry','exact_normalized_geometry_multiset_intersection']},indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--capture',action='store_true')
    parser.add_argument('--runtime',type=Path,default=Path('/workspace/hifld-worker-runtime/6/complete-source'))
    parser.add_argument('--baseline',type=Path,default=Path('/workspace/work/hifld-network-retry/data/administrative-areas-usace-ienc.parquet'))
    args = parser.parse_args()
    pin = json.loads(HERE.joinpath('evidence/published-candidate.json').read_text())
    if args.capture: capture(args.runtime)
    summarize(args.runtime,args.baseline,pin)
