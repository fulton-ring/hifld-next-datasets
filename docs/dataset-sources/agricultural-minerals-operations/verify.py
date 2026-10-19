"""Offline verification of bounded captured evidence; no network or ingestion."""
import collections
import hashlib
import json
from pathlib import Path

root = Path(__file__).parent
for entry in json.loads((root/'evidence/checksums.json').read_text()):
    assert hashlib.sha256((root/entry['path']).read_bytes()).hexdigest() == entry['sha256'], entry['path']
load = lambda name: json.loads((root/'evidence'/name).read_text())
ids = load('ids.json')['objectIds']
features = [f for path in sorted((root/'evidence').glob('page-*.json')) for f in json.loads(path.read_text())['features']]
assert len(ids) == len(set(ids)) == load('count.json')['count'] == len(features) == 236
assert collections.Counter(ids) == collections.Counter(f['attributes']['OBJECTID_1'] for f in features)
assert set(ids) == set(load('ids-after.json')['objectIds'])
schema = load('layer.json')['fields']
fields = {f['name'] for f in schema}
for path in (root/'evidence').glob('page-*.json'):
    assert not json.loads(path.read_text()).get('exceededTransferLimit', False)
for feature in features:
    for field in schema:
        value = feature['attributes'][field['name']]
        assert value is not None or field['nullable']
        if value is not None:
            expected = str if field['type'] == 'esriFieldTypeString' else (int, float) if field['type'] == 'esriFieldTypeDouble' else int
            assert isinstance(value, expected) and not isinstance(value, bool)
assert len({json.dumps(f, sort_keys=True) for f in features}) == 236
assert all(set(f['attributes']) == fields for f in features)
for name in ['OBJECTID_1','FID_1','OBJECTID','MINOP1X020']:
    values = [f['attributes'][name] for f in features]
    assert None not in values and len(set(values)) == 236
assert all(f.get('geometry') and -180 <= f['geometry']['x'] <= 180 and -90 <= f['geometry']['y'] <= 90 for f in features)
source = json.loads((root/'source.json').read_text())
assert source['published_contract']['approved'] is False
assert {f['name'] for f in source['published_contract']['published_columns']} == fields
repos = load('repositories.json')['data']['repositoriesOrError']
assert repos['__typename'] == 'RepositoryConnection' and repos['nodes'][0]['assetNodes']
print(f'PASS: checksums, 236 captured mirror rows/all IDs/schema/coordinates, unapproved proposal and {len(repos["nodes"][0]["assetNodes"])} locally loaded assets. No baseline comparison or ingestion acceptance.')
