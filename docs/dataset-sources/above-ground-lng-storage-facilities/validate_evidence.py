"""Offline structural and checksum validation of research captures."""
import gzip
import hashlib
import json
from pathlib import Path

root = Path(__file__).parent
for relative, expected in json.loads((root / 'evidence-checksums.json').read_text()).items():
    assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == expected, relative
for path in root.rglob('*.json'):
    json.loads(path.read_text())
for record in json.loads((root / 'evidence/requests.json').read_text()):
    if 'body' in record:
        body = (root / 'evidence' / record['body']).read_bytes()
        if record['body'].endswith('.gz'):
            body = gzip.decompress(body)
        assert len(body) == record['bytes'], record['name']
        assert hashlib.sha256(body).hexdigest() == record['sha256'], record['name']
source = json.loads((root / 'source.json').read_text())
archive = json.loads((root / 'evidence/archive-analysis.json').read_text())
contract = source['published_contract']
assert contract['approved'] is False and source['ingestion_approval'] is False
assert source['outcome'] == 'blocked'
columns = contract['published_columns']
assert len(columns) == 37
assert {c['name'] for c in columns} == set(archive['fiona_schema']['properties']) | {'geometry'}
for column in columns:
    assert {'name', 'type', 'nullable', 'meaning', 'id_role', 'approved'} <= column.keys()
assert archive['feature_count'] == archive['dbf_record_count'] == 13
assert archive['coverage']['STATE'] == {'PA': 13}
for name in ['mirror-layer', 'mirror-count', 'mirror-ids']:
    record = next(r for r in json.loads((root / 'evidence/requests.json').read_text()) if r['name'] == name)
    body = (root / 'evidence' / record['body']).read_bytes()
    response = json.loads(gzip.decompress(body) if record['body'].endswith('.gz') else body)
    assert response['error']['code'] == 400
readiness = json.loads((root / 'evidence/dagster-readiness.json').read_text())
assert readiness['server_info']['status'] == readiness['graphql']['status'] == 200
result = readiness['graphql']['body']['data']['repositoriesOrError']
assert result['__typename'] == 'RepositoryConnection'
assets = [node['assetKey']['path'] for repo in result['nodes'] for node in repo['assetNodes']]
assert ['ingest', source['dataset_id'], source['dataset_id']] in assets
print(f'PASS: JSON/checksums, complete unapproved 37-column proposal, rejected ArcGIS errors; Dagster loaded {len(assets)} asset keys')
