"""Offline validation of the pinned research package, not ingestion acceptance."""
import gzip,hashlib,json
from pathlib import Path
P=Path(__file__).parent
for line in (P/'SHA256SUMS').read_text().splitlines():
    digest,name=line.split('  ',1)
    assert hashlib.sha256((P/name).read_bytes()).hexdigest()==digest,name
s=json.loads((P/'source.json').read_text());assert s['research_only'] and not s['implementation_approved'];assert s['published_contract']['approved'] is False
layer=json.loads((P/'evidence/layer.capture').read_text());assert [f['name'] for f in layer['fields']]==[f['name'] for f in s['published_contract']['columns']]
entries=json.loads((P/'evidence/pages.json').read_text());rows=[]
for i,e in enumerate(entries):
    raw=gzip.decompress((P/'evidence'/e['file']).read_bytes());assert hashlib.sha256(raw).hexdigest()==e['sha256_raw'];j=json.loads(raw);assert 'error' not in j
    assert e['count']==len(j['features'])==(100 if i<29 else 66)
    rows += [f['attributes'] for f in j['features']]
assert len(entries)==30 and not entries[-1]['exceededTransferLimit']
ids=[r['OBJECTID'] for r in rows];assert len(ids)==len(set(ids))==2966
for name in ['ids.capture','ids-after.capture']:
    assert set(ids)==set(json.loads((P/'evidence'/name).read_text())['objectIds'])
assert len({r['Feature_ID'] for r in rows})==2781
assert len({(r['Source_Dataset'],r['Feature_ID']) for r in rows})==2937
assert all(set(r)=={f['name'] for f in layer['fields']} for r in rows)
g=json.loads((P/'evidence/geometry-sample.capture').read_text());assert len(g['features'])==1 and g['features'][0]['geometry']['rings']
d=json.loads((P/'evidence/dagster-repositories.json').read_text());assert 'errors' not in d
repo=d['data']['repositoriesOrError'];assert repo['__typename']=='RepositoryConnection'
paths=[a['assetKey']['path'] for n in repo['nodes'] for a in n['assetNodes']]
assert any('administrative-areas-usace-ienc' in a for a in paths)
print('PASS: hashes, all raw attributes/count/IDs/pages, nonunique source IDs, proposed contract, geometry sample, Dagster repository loading. No geometry completeness or ingestion approval.')
