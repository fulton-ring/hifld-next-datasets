"""Replay bounded historical captures without network or source mutation."""
import collections, gzip, hashlib, json, pathlib
P=pathlib.Path(__file__).parent/'evidence'
def read(n):
 p=P/n
 return json.loads(gzip.decompress(p.read_bytes()) if p.suffix=='.gz' else p.read_bytes())
layer=read('layer.body'); full=read('all.json.gz'); page=read('id-page.json'); geo=read('geojson.json.gz')
for d in (layer,full,page,geo): assert 'error' not in d
assert read('ids.json')['error']['code']==500
fs=full['features']; ids=[f['attributes']['OBJECTID'] for f in fs]; others=[f['attributes']['OBJECTID'] for f in page['features']]
assert len(ids)==len(set(ids))==read('count.json')['count']==len(geo['features'])==96
assert set(ids)==set(others) and None not in ids
assert not full.get('exceededTransferLimit') and not page.get('exceededTransferLimit')
summary={'count':len(ids),'unique_objectids':len(set(ids)),'independent_ordered_id_projection_matches':True,'returnIdsOnly':'HTTP 200 ArcGIS error 500; not successful','snapshot_consistency':'sequential requests, not an atomic snapshot','limit':layer['maxRecordCount'],'pagination_supported':layer['advancedQueryCapabilities']['supportsPagination'],'pagination':'one ordered ID page at offset 0 count 1000 covers count 96; full response below limit','regions':dict(collections.Counter(f['attributes']['REGION'] for f in fs)),'categories':dict(collections.Counter(f['attributes']['FEAT_TYPE'] for f in fs)),'fields':{},'publication_dates_ms':dict(collections.Counter(str(f['attributes']['PUB_DATE']) for f in fs)),'null_geometry':sum(f.get('geometry') is None for f in fs),'extent':layer['extent']}
for field in layer['fields']:
 n=field['name']; vals=[f['attributes'].get(n) for f in fs]
 summary['fields'][n]={'source_type':field['type'],'declared_nullable':field.get('nullable'),'observed_nulls':None if field['type']=='esriFieldTypeGeometry' else sum(v is None for v in vals),'observed_types':sorted(set(type(v).__name__ for v in vals))}
print(json.dumps(summary,indent=2))
