"""Verify bounded evidence offline. This is research, never publication acceptance."""
import collections, hashlib, json, pathlib
P=pathlib.Path(__file__).parent/'evidence'
manifest=json.loads((P/'checksums.json').read_text())
for name,sha in manifest.items(): assert hashlib.sha256((P/name).read_bytes()).hexdigest()==sha,name
x=json.loads((P/'all-features.json').read_text()); c=json.loads((P/'count-recheck.json').read_text())
assert 'error' not in x and 'error' not in c
fs=x['features']; ids=[f['attributes']['OBJECTID'] for f in fs]
assert len(fs)==c['count']==63 and len(set(ids))==63 and None not in ids
assert not x.get('exceededTransferLimit')
print(json.dumps({'features':len(fs),'unique_OBJECTID':len(set(ids)),'independent_ids_reconciliation':'BLOCKED: ID query HTTP200 ArcGIS error', 'regions':dict(collections.Counter(f['attributes']['REGION'] for f in fs))},indent=2))
