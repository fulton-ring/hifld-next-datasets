"""Replay the bounded complete #22 evidence. Never fetch, approve or publish."""
from __future__ import annotations
import collections
import hashlib
import io
import json
from pathlib import Path
import tempfile
import xml.etree.ElementTree as ET
import zipfile
import fiona
import pyarrow.parquet as pq
import shapely
from pyproj import Transformer

ROOT = Path(__file__).resolve().parent
EVIDENCE = ROOT / 'evidence'
NS = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}


def workbook(name):
    with zipfile.ZipFile(EVIDENCE / (name + '.body')) as archive:
        assert archive.testzip() is None
        strings = [''.join(t.itertext()) for t in ET.fromstring(archive.read('xl/sharedStrings.xml')).findall('s:si', NS)]
        rows = []
        for row in ET.fromstring(archive.read('xl/worksheets/sheet1.xml')).findall('.//s:row', NS):
            values = {}
            for cell in row.findall('s:c', NS):
                val = cell.find('s:v', NS)
                value = val.text if val is not None else ''
                if cell.get('t') == 's':
                    value = strings[int(value)]
                values[''.join(c for c in cell.get('r') if c.isalpha())] = value
            rows.append(values)
        return rows


def replay():
    receipts = {}
    for path in sorted(EVIDENCE.glob('*.json')):
        receipt = json.loads(path.read_text())
        if 'sha256' not in receipt:
            continue
        raw = path.with_suffix('.body').read_bytes()
        assert hashlib.sha256(raw).hexdigest() == receipt['sha256'], path
        assert len(raw) == receipt['bytes'], path
        receipts[path.stem] = {k: receipt[k] for k in ('status', 'bytes', 'sha256', 'requested_url', 'observed_url', 'retrieved')}
    baseline = pq.read_table(EVIDENCE / 'baseline.body')
    published = baseline.to_pylist()
    with zipfile.ZipFile(EVIDENCE / 'eia_zip.body') as archive:
        assert archive.testzip() is None
        assert len(archive.namelist()) == len(set(archive.namelist())) == 8
        assert all('/' not in n and '\\' not in n and '..' not in n for n in archive.namelist())
        members = [{'name': i.filename, 'bytes': i.file_size, 'crc32': f'{i.CRC:08x}'} for i in archive.infolist()]
        with tempfile.TemporaryDirectory() as tmp:
            archive.extractall(tmp)
            with fiona.open(next(Path(tmp).glob('*.shp'))) as layer:
                source_schema = layer.schema
                source_crs = layer.crs.to_string()
                source = [{'fid': feature.id, 'properties': dict(feature.properties), 'geometry': dict(feature.geometry)} for feature in layer]
    # Numeric coordinates are a diagnostic witness, never a replacement ID or approved join.
    source_coords = {(f['properties']['Longitude'], f['properties']['Latitude']): f for f in source}
    baseline_coords = {(f['Longitude'], f['Latitude']): f for f in published}
    overlap = sorted(set(source_coords) & set(baseline_coords))
    shared = ['Company', 'State', 'PADD', 'Cap_Mmgal', 'Source', 'Period', 'Longitude', 'Latitude']
    differences = []
    transform = Transformer.from_crs(source_crs, 'EPSG:3857', always_xy=True)
    for coord in overlap:
        a, b = source_coords[coord], baseline_coords[coord]
        delta = {k: {'source': a['properties'][k], 'published': b[k]} for k in shared if a['properties'][k] != b[k] or type(a['properties'][k]) is not type(b[k])}
        geom = shapely.from_wkb(b['geometry'])
        xy = transform.transform(*a['geometry']['coordinates'])
        differences.append({'coordinate_witness': coord, 'source_OBJECTID': a['properties']['OBJECTID'], 'source_fiona_fid': a['fid'], 'attribute_differences': delta, 'transformed_source_minus_published_xy_diagnostic': [xy[0] - geom.x, xy[1] - geom.y]})
    combined = workbook('combined_xlsx')
    kinds = collections.Counter(r.get('G') for r in combined[1:] if r.get('G'))
    assert kinds['Biodiesel'] == 45
    for name in ('eia_service', 'eia_layer113', 'eia_query'):
        assert json.loads((EVIDENCE / (name + '.body')).read_text())['error']['code'] == 499
    atlas = json.loads((EVIDENCE / 'atlas_all.body').read_text())
    assert atlas['numberMatched'] == atlas['numberReturned'] == len(atlas['features'])
    assert all('next' != link.get('rel') for link in atlas['links'])
    return {
        'acceptance': 'UNAPPROVED; evidence only; no pipeline or current GIS replacement established',
        'receipts': receipts,
        'published_candidate': {'rows': len(published), 'schema': str(baseline.schema), 'periods': dict(collections.Counter(baseline['Period'].to_pylist())), 'geometry_types': dict(collections.Counter([g.geom_type for g in shapely.from_wkb(baseline['geometry'].to_pylist())])), 'bounds_native': shapely.total_bounds(shapely.from_wkb(baseline['geometry'].to_pylist())).tolist(), 'explicit_source_id_columns': [], 'warning': 'Company and Site individually repeat; no identity is inferred from physical row order or coordinates'},
        'official_historical_zip': {'rows': len(source), 'schema': source_schema, 'crs': source_crs, 'periods': dict(collections.Counter(f['properties']['Period'] for f in source)), 'OBJECTID_values': [f['properties']['OBJECTID'] for f in source], 'fiona_fid_values': [f['fid'] for f in source], 'members': members, 'full_features': source},
        'full_comparison': {'source_coordinate_witness_count': len(source_coords), 'published_coordinate_witness_count': len(baseline_coords), 'exact_numeric_coordinate_overlap': len(overlap), 'source_only_OBJECTIDs': [f['properties']['OBJECTID'] for c,f in source_coords.items() if c not in baseline_coords], 'published_only_complete_rows': [{k:v.hex() if isinstance(v,bytes) else v for k,v in f.items()} for c,f in baseline_coords.items() if c not in source_coords], 'source_only_columns': ['OBJECTID', 'Site_Name'], 'published_only_columns': ['Site', 'bbox'], 'paired_diagnostics': differences, 'approved_id_mapping': None, 'approved_geometry_tolerance': None},
        'official_capacity_workbooks': {'2025_columns': workbook('capacityxlsx')[2], '2026_columns': combined[0], '2026_complete_facility_types': dict(kinds), '2026_biodiesel_rows': [r for r in combined[1:] if r.get('G') == 'Biodiesel'], 'no_geometry_or_source_id': True},
        'official_atlas_complete_catalog': {'items': len(atlas['features']), 'biodiesel_or_biofuels_titles': [f['properties'].get('title') for f in atlas['features'] if any(t in f['properties'].get('title','').lower() for t in ('biodiesel','biofuel'))]},
        'blocker': 'Current authoritative complete point feed unavailable anonymously (application499); static official GIS is January2021, and current January2026 capacity-only successor lacks source coordinates and stable source identifiers. Need official current GIS access/publication or explicit historical/successor reconstruction decision; no guessed coordinate joins.'
    }

if __name__ == '__main__':
    report = replay()
    target = ROOT / 'comparison.json'
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    print(json.dumps({k:v for k,v in report.items() if k in ['published_candidate','official_atlas_complete_catalog','blocker']}, indent=2))
