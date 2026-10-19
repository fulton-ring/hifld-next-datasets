"""Offline replay of bounded 2026-10-19 captures; never calls a live API."""
import hashlib,json,pathlib,collections
p=pathlib.Path(__file__).parent/'evidence'
for r in json.loads((p/'requests.json').read_text()):
 candidates=[p/(r['name']+s) for s in ('.txt','.json')]
 f=next(f for f in candidates if f.exists())
 assert hashlib.sha256(f.read_bytes()).hexdigest()==r['sha256']
layer=json.loads((p/'layer.json').read_text()); data=json.loads((p/'features.json').read_text()); ids=json.loads((p/'ids.json').read_text())['objectIds']; count=json.loads((p/'count.json').read_text())['count'];fs=data['features']; actual=[f['attributes']['OBJECTID'] for f in fs]
assert count==len(fs)==len(ids)==len(set(actual))
assert set(actual)==set(ids) and None not in actual
assert not data.get('exceededTransferLimit',False)
assert {f['name'] for f in layer['fields']}=={f['name'] for f in data['fields']}
summary={'count':count,'id_sets_equal':True,'exceededTransferLimit':data.get('exceededTransferLimit',False),'maxRecordCount':layer['maxRecordCount'],'supportsPagination':layer['advancedQueryCapabilities']['supportsPagination'],'geometry_type':data['geometryType'],'crs':data['spatialReference'],'extent':layer['extent'],'fields':{},'null_geometry':sum(f.get('geometry') is None for f in fs),'categories':dict(collections.Counter(f['attributes'].get('Category_of_Airport') for f in fs))}
for field in layer['fields']:
 n=field['name'];v=[f['attributes'][n] for f in fs];summary['fields'][n]={'type':field['type'],'schema_nullable':field.get('nullable'),'null_count':sum(x is None for x in v),'distinct':len(set(v))}
coordinates=[xy for f in fs for ring in f['geometry']['rings'] for xy in ring]
summary['coordinate_bounds']=[min(x[0] for x in coordinates),min(x[1] for x in coordinates),max(x[0] for x in coordinates),max(x[1] for x in coordinates)]
summary['out_of_range_coordinates']=sum(not(-180<=x[0]<=180 and -90<=x[1]<=90) for x in coordinates)
summary['empty_geometry']=sum(not f['geometry'].get('rings') for f in fs)
summary['unclosed_rings']=sum(r[0]!=r[-1] for f in fs for r in f['geometry']['rings'])
summary['source_date_distribution']=dict(collections.Counter(f['attributes']['Source_Date'] for f in fs))
summary['source_datasets']=dict(collections.Counter(f['attributes']['Source_Dataset'] for f in fs))
summary['geometry_validity']='Structural rings/bounds checked; OGC topology not evaluated. No conversion performed.'
print(json.dumps(summary,indent=2))
