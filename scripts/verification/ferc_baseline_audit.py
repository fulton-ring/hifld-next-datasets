"""Replay all five complete historical FERC candidates; never accepts a mapping.

Inputs are the externally retained baseline capture directory, its receipts and
the source capture directory. Large bodies/reports are artifacts, not fixtures.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import tempfile
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pyogrio
import shapely

from dagster_hifld.portolan.pmtiles import extract_vector_layer_ids
from dagster_hifld.sources.ferc_regions import inspect_source


def checksum(path, compressed=False):
    sha, md5, count = hashlib.sha256(), hashlib.md5(), 0
    with (gzip.open(path, 'rb') if compressed else path.open('rb')) as stream:
        while chunk := stream.read(65536):
            sha.update(chunk); md5.update(chunk); count += len(chunk)
    return {'bytes': count, 'sha256': sha.hexdigest(), 'md5': md5.hexdigest()}


def signature(table):
    return [{'name': f.name, 'type': str(f.type), 'nullable': f.nullable,
             'metadata': {k.decode(): v.decode() for k,v in (f.metadata or {}).items()}}
            for f in table.schema]


def records(table, geometry):
    result = []
    for row in table.to_pylist():
        body = row.pop(geometry)
        geom = shapely.from_wkb(body)
        result.append({'attributes': {k: str(v) if not isinstance(v, (str,int,float,bool,type(None),dict)) else v
                                      for k,v in row.items()},
                       'WKB_sha256': hashlib.sha256(body).hexdigest(),
                       'geometry_type': geom.geom_type, 'coordinates': int(shapely.get_num_coordinates(geom)),
                       'valid': bool(geom.is_valid), 'bounds': list(geom.bounds)})
    return result


def geojson_records(path):
    # Exact pinned historical writer: one feature per line, all properties
    # precede geometry. Retain top-level IDs separately; never let GDAL absorb
    # them into its file-local FID. GEOS parses all coordinates directly, avoiding
    # millions of temporary Python coordinate lists. This is not a JSON adapter
    # for unknown writers: differing layouts fail rather than discard members.
    headers, rows = [], []
    with gzip.open(path, 'rb') as stream:
        for line in stream:
            if b'"type": "Feature"' not in line:continue
            prefix, separator, tail = line.partition(b'"geometry": ')
            if not separator or not tail.startswith(b'{ "type":'):
                raise ValueError('unsupported_geojson_feature_layout')
            body=line.rstrip().rstrip(b',').rstrip()
            if not body.endswith(b'] } }'):
                raise ValueError('unsupported_geojson_feature_suffix')
            header=json.loads(prefix+b'"geometry": null}',parse_float=Decimal)
            if header.get('type')!='Feature':raise ValueError('invalid_feature_header')
            properties=header['properties']
            exact_literals={k:str(v)for k,v in properties.items()if isinstance(v,Decimal)}
            attrs={k:float(v)if isinstance(v,Decimal)else v for k,v in properties.items()}
            geom=shapely.from_geojson(body.decode())
            headers.append({k:v for k,v in header.items()if k not in ('properties','geometry')})
            rows.append({'attributes':attrs,'exact_JSON_decimal_literals':exact_literals,
                'WKB_sha256':hashlib.sha256(shapely.to_wkb(geom)).hexdigest(),
                'geometry_type':geom.geom_type,'coordinates':int(shapely.get_num_coordinates(geom)),
                'valid':bool(geom.is_valid),'bounds':list(geom.bounds)})
    if len(rows)!=5:raise ValueError('incomplete_feature_headers')
    return {'physical_schema':'JSON object properties; JSON numbers have no declared integer-width/float-width/nullability contract.',
        'literal_feature_headers':headers,'records':rows,
        'logical_property_types':{k:type(v).__name__ for k,v in rows[0]['attributes'].items()}}


def audit(baseline, source, output):
    collection = json.loads((baseline.parent/'discovery/baseline_collection.body').read_bytes())
    assets = list(collection['assets'].values())
    formats = {'parquet': 'parquet.body', 'pmtiles': 'pmtiles.body',
               'geojson': 'geojson.body.gz', 'geopackage': 'geopackage.body.gz',
               'shapefile': 'shapefile.body'}
    report = {'schema_version': 1, 'acceptance': False,
              'scope': 'Complete immutable candidates and full native-record diagnostics; no approved mapping/tolerance.',
              'captures': {}, 'formats': {}}
    for name, filename in formats.items():
        path = baseline/filename; actual = checksum(path, filename.endswith('.gz'))
        receipt = json.loads((baseline/(name+'.receipt.json')).read_bytes())
        assert receipt['complete_body'] is True and type(receipt['status']) is int and receipt['status']==200
        assert all(actual[k] == receipt[k] for k in actual)
        asset = next(a for a in assets if a['href'].split('/')[-1] == receipt['requested_url'].split('/')[-1])
        assert asset['file:size'] == actual['bytes']
        assert asset['file:checksum'] == 'd50110'+actual['md5']
        report['captures'][name] = {**actual, 'retained_path':str(path),
            'retained_sha256':checksum(path)['sha256'], 'retained_bytes':path.stat().st_size}
    native, original = inspect_source(source/'pasda_original.body')
    report['source'] = {'schema':signature(native),'records':records(native,'wkb_geometry'),
                        'invalid_objectids':original['invalid_objectids']}
    table = pq.read_table(baseline/'parquet.body')
    report['formats']['parquet'] = {'schema':signature(table),'records':records(table,'geometry')}
    with tempfile.TemporaryDirectory(prefix='ferc-baseline-audit-') as tmp:
        gp = Path(tmp)/'baseline.gpkg'
        with gzip.open(baseline/'geopackage.body.gz','rb') as src, gp.open('wb') as dst:shutil.copyfileobj(src,dst,65536)
        for name,path in [('geopackage',str(gp)),
                          ('shapefile','/vsizip/{'+str(baseline/'shapefile.body')+'}')]:
            layers=pyogrio.list_layers(path)
            if len(layers)!=1:raise ValueError('unexpected_layer_roster')
            meta, candidate=pyogrio.read_arrow(path,layer=layers[0][0],return_fids=True)
            report['formats'][name]={'schema':signature(candidate),'crs':meta['crs'],
                                    'records':records(candidate,meta['geometry_name']or'wkb_geometry')}
    report['formats']['geojson']=geojson_records(baseline/'geojson.body.gz')
    path=baseline/'pmtiles.body'
    with path.open('rb') as stream:
        def read(offset,length):stream.seek(offset);return stream.read(length)
        report['formats']['pmtiles']={'vector_layers':extract_vector_layer_ids(read,path.stat().st_size),
            'comparison_scope':'Complete byte pin and display-layer metadata only; generalized tiles are not a lossless original-geometry/row-ID representation.'}
    for name, item in report['formats'].items():
        if 'records' not in item:continue
        assert len(item['records'])==5
        assert sorted(r['attributes']['OBJECTID']for r in item['records'])==[1,2,3,4,5]
        published={r['attributes']['OBJECTID']:r for r in report['formats']['parquet']['records']}
        for row in item['records']:
            row['exact_WKB_equals_published_parquet']=row['WKB_sha256']==published[row['attributes']['OBJECTID']]['WKB_sha256']
            row['exact_WKB_equals_original']=row['WKB_sha256']==next(r['WKB_sha256']for r in report['source']['records']if r['attributes']['OBJECTID']==row['attributes']['OBJECTID'])
    output.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
    print(json.dumps({'report':str(output),'report_sha256':checksum(output)['sha256'],
                      'complete_capture_count':len(report['captures']),'full_record_formats':4,'accepted':False}))


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline',type=Path,required=True);p.add_argument('--source',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    audit(a.baseline,a.source,a.output)
