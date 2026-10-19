"""Draft FAA full-extraction adapter; outputs stay in a nonpublishable quarantine.

No accepted schema, baseline or migration exists. Preserve exact native and
GeoJSON bodies, declared schema and identifiers before building a typed candidate.
"""
from __future__ import annotations

import datetime
import hashlib
import io
import json
import math
import pathlib
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import shape

from dagster_hifld.resources import StagingStorageResource

SLUG = 'airspace-boundaries-1'
ITEM_URL = 'https://www.arcgis.com/sharing/rest/content/items/67885972e4e940b2aa6d74024901c561?f=pjson'
LAYER_URL = 'https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/Boundary_Airspace/FeatureServer/0'
MAX_BYTES = 100 * 1024 * 1024
NAMES = ('item', 'layer', 'count', 'ids', 'source-native', 'source-geojson', 'page-end')


def capture(directory: pathlib.Path) -> None:
    """Anonymous, bounded complete snapshot; changed transfer scope fails closed."""
    directory.mkdir(parents=True, exist_ok=True)
    urls = {'item': ITEM_URL, 'layer': LAYER_URL+'?f=pjson',
            'count': LAYER_URL+'/query?where=1%3D1&returnCountOnly=true&f=pjson',
            'ids': LAYER_URL+'/query?where=1%3D1&returnIdsOnly=true&f=pjson'}
    def fetch(name, url):
        with urllib.request.urlopen(url, timeout=60) as response:
            body = response.read(MAX_BYTES+1)
            if len(body) > MAX_BYTES:
                raise ValueError('FAA response exceeds bounded capture limit')
            parsed = json.loads(body)
            if response.status != 200 or 'error' in parsed:
                raise ValueError(f'FAA upstream failure: {name}')
            metadata = {'url': url, 'status': response.status, 'bytes': len(body),
                        'sha256': hashlib.sha256(body).hexdigest(),
                        'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
        (directory/(name+'.body')).write_bytes(body)
        (directory/(name+'.json')).write_text(json.dumps(metadata, sort_keys=True))
        return parsed
    for name,url in urls.items():
        fetch(name,url)
    count = json.loads((directory/'count.body').read_bytes())['count']
    layer = json.loads((directory/'layer.body').read_bytes())
    if not isinstance(count,int) or isinstance(count,bool) or not 0 < count <= min(layer['maxRecordCount'], 2000):
        raise ValueError('FAA count exceeds verified single-page scope; pagination adapter review required')
    base = LAYER_URL+'/query?where=1%3D1&outFields=%2A&returnGeometry=true&orderByFields=OBJECTID&resultOffset=0&resultRecordCount=2000'
    fetch('source-native',base+'&f=pjson')
    fetch('source-geojson',base+'&f=geojson&outSR=4326')
    fetch('page-end',LAYER_URL+f'/query?where=1%3D1&outFields=OBJECTID&orderByFields=OBJECTID&resultOffset={count}&resultRecordCount=2000&f=pjson')


def stage_candidate(storage: StagingStorageResource, directory: pathlib.Path) -> dict:
    """Replay checksum-pinned captures to a deterministic local quarantine only."""
    if not storage.use_local or storage.bucket:
        raise ValueError('Draft FAA adapter requires isolated local candidate storage')
    bodies = {}; metadata = {}
    for name in NAMES:
        body = (directory/(name+'.body')).read_bytes()
        m = json.loads((directory/(name+'.json')).read_text())
        if len(body)>MAX_BYTES or len(body)!=m['bytes'] or hashlib.sha256(body).hexdigest()!=m['sha256'] or m['status']!=200:
            raise ValueError(f'FAA checksum/status mismatch: {name}')
        parsed = json.loads(body)
        if 'error' in parsed:
            raise ValueError(f'FAA error response: {name}')
        bodies[name]=body;metadata[name]=m
    layer=json.loads(bodies['layer']);native=json.loads(bodies['source-native']);geo=json.loads(bodies['source-geojson'])
    fields=layer['fields'];count=json.loads(bodies['count'])['count'];ids=json.loads(bodies['ids'])['objectIds']
    rows=[f['attributes'] for f in native['features']];features=geo['features']
    if not rows or len(rows)!=count or len(features)!=count or len(ids)!=count or len(set(ids))!=count:
        raise ValueError('FAA incomplete or duplicate ID response')
    if native.get('exceededTransferLimit') or geo.get('exceededTransferLimit') or json.loads(bodies['page-end'])['features']:
        raise ValueError('FAA incomplete pagination')
    if {r['OBJECTID'] for r in rows}!=set(ids) or {f['properties']['OBJECTID'] for f in features}!=set(ids):
        raise ValueError('FAA independent ID set mismatch')
    if any(set(r)!={f['name'] for f in fields} for r in rows):
        raise ValueError('FAA all-column schema mismatch')
    by_id={f['properties']['OBJECTID']:f for f in features}
    if any(r!=by_id[r['OBJECTID']]['properties'] for r in rows):
        raise ValueError('FAA native/GeoJSON attribute mismatch')
    required={'OBJECTID':'esriFieldTypeOID','GLOBAL_ID':'esriFieldTypeString','GlobalID':'esriFieldTypeGlobalID'}
    declared={f['name']:f['type'] for f in fields}
    if any(declared.get(k)!=v for k,v in required.items()):
        raise ValueError('FAA identifier name/type drift')
    for key in required:
        values=[r[key] for r in rows]
        if any(v is None for v in values) or len(set(values))!=count:
            raise ValueError(f'FAA required identifier null/duplicate: {key}')
    types={'esriFieldTypeOID':pa.int64(),'esriFieldTypeString':pa.string(), 'esriFieldTypeGlobalID':pa.string(), 'esriFieldTypeDouble':pa.float64(), 'esriFieldTypeSmallInteger':pa.int16()}
    arrays={}
    for f in fields:
        values=[r[f['name']] for r in rows]
        if f['type'] not in types:
            raise ValueError('FAA unsupported source physical type')
        if not f['nullable'] and any(v is None for v in values):
            raise ValueError('FAA required column null')
        if any(v is not None and (isinstance(v,bool) or (not isinstance(v,str) if f['type'] in ('esriFieldTypeString','esriFieldTypeGlobalID') else not isinstance(v,(int,float)))) for v in values):
            raise ValueError('FAA source value/type drift')
        if f['type'] in ('esriFieldTypeOID','esriFieldTypeSmallInteger') and any(v is not None and type(v) is not int for v in values):
            raise ValueError('FAA integer source type drift')
        if any(isinstance(v,float) and not math.isfinite(v) for v in values):
            raise ValueError('FAA nonfinite numeric value')
        arrays[f['name']]=pa.array(values,type=types[f['type']],safe=True)
    geometries=[shape(by_id[r['OBJECTID']]['geometry']) for r in rows]
    if 'geometry' in arrays or '__source_geojson_feature_id' in arrays:
        raise ValueError('FAA candidate reserved column collision')
    arrays['geometry']=pa.array([g.wkb for g in geometries],type=pa.binary())
    # Preserve original GeoJSON foreign member id, never manufacture a row ID.
    arrays['__source_geojson_feature_id']=pa.array([by_id[r['OBJECTID']]['id'] for r in rows],type=pa.int64())
    table=pa.table(arrays);stream=io.BytesIO();pq.write_table(table,stream,compression='zstd');candidate=stream.getvalue()
    digest=hashlib.sha256(b''.join(bodies[n] for n in NAMES)+json.dumps(metadata,sort_keys=True).encode()).hexdigest()
    version='candidate-'+digest[:24];prefix=f'{SLUG}/{SLUG}/{version}/quarantine'
    manifest={'schema_version':1,'status':'needs_decision','promotion_allowed':False,'feature_count':count,'source_fields':fields,'candidate_columns':table.column_names,'source_native_crs':native['spatialReference'],'candidate_geometry_crs':'EPSG:4326 via producer outSR; original NAD83 Esri geometry retained raw','transformations':['GeoJSON geometry to WKB (no repair)','Explicit Esri physical types to Arrow; all fields retained','GeoJSON Feature.id retained as __source_geojson_feature_id (unapproved mapping)'],'candidate_sha256':hashlib.sha256(candidate).hexdigest(),'captures':metadata,'publisher':json.loads(bodies['item'])['owner'],'release_description':json.loads(bodies['item'])['description'],'cadence':'56 days per producer item; refresh requires separate scheduled deployment approval','baseline':None,'approved_contract':None,'complete':True,'invalid_geometry_count':sum(not g.is_valid for g in geometries),'empty_geometry_count':sum(g.is_empty for g in geometries),'quarantine_reason':'Unapproved baseline, schema/ID mapping and geometry/coverage migration; shared #417 exact release approval required before promotion'}
    outputs={**{f'raw/{name}.json':body for name,body in bodies.items()},'candidate/airspace.parquet':candidate,'candidate/airspace.geojson':bodies['source-geojson'],'metadata/provenance.json':json.dumps(manifest,sort_keys=True,indent=2).encode()}
    # Preflight every existing key before any write. Identical reruns are no-ops.
    for name,body in outputs.items():
        key=f'{prefix}/{name}'
        if storage.object_exists(key) and storage.read_key(key)!=body:
            raise ValueError('FAA immutable candidate collision')
    for name,body in outputs.items():
        key=f'{prefix}/{name}'
        if not storage.object_exists(key):
            storage.write_key_if_unchanged(key,body,None)
    return {'version':version,'feature_count':count,'quarantine_prefix':prefix,'candidate_sha256':manifest['candidate_sha256'],'promotion_allowed':False}
