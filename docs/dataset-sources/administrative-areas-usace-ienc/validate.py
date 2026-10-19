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
full=json.loads((P/'evidence/complete-comparison.json').read_text())
assert full['source_rows']==2966 and full['published_rows']==3131
assert full['published_candidate']['acceptance']=='candidate only; not accepted'
assert full['source_geometry']['nulls']==full['source_geometry']['empty']==full['source_geometry']['invalid']==0
assert full['published_geometry']['nulls']==full['published_geometry']['empty']==full['published_geometry']['invalid']==0
assert full['exact_normalized_geometry_multiset_intersection']==0
for diagnostic in full['diagnostic_precision_grid_comparisons'].values():
    assert diagnostic['multiset_intersection']==2757
    assert diagnostic['source_unmatched_occurrences']==209 and diagnostic['published_unmatched_occurrences']==374
manifest=json.loads((P/'evidence/geometry-pages.json').read_text())
assert len(manifest['pages'])==119 and sum(e['count'] for e in manifest['pages'])==2966
assert manifest['ids_unchanged'] and sorted(i for e in manifest['pages'] for i in e['ids'])==manifest['expected_ids']
for e in manifest['pages']: assert e['status']==200 and not e['exceededTransferLimit']
audit=json.loads(gzip.decompress((P/'evidence/source-feature-geometry-hashes.json.gz').read_bytes()))
assert len(audit)==2966 and len({r['OBJECTID'] for r in audit})==2966
ready=json.loads((P/'evidence/readiness-retry.json').read_text())
assert ready['loaded_dataset_ingest'] and ready['webserver_stopped'] and ready['bucket_variables_unset'] and not ready['materialization']
print('PASS: committed hashes, complete raw attribute replay/IDs, proposed contract, bounded full geometry receipts/hash inventory/comparison, isolated Dagster loading. Complete raw geometry replay requires preserved runtime files; no ingestion approval.')
