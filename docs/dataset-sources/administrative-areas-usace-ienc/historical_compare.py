"""Replay complete restored historical-service attributes; no ID mapping accepted."""
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path

P = Path(__file__).parent / 'evidence'
receipt = json.loads(P.joinpath('retry-old-attributes.json').read_text())
historical = []
for e in receipt['pages']:
    raw = gzip.decompress(P.joinpath(e['file']).read_bytes())
    assert hashlib.sha256(raw).hexdigest() == e['sha256_raw']
    data = json.loads(raw)
    assert 'error' not in data and len(data['features']) == e['count']
    assert data.get('exceededTransferLimit', False) == e['exceededTransferLimit']
    historical.extend(f['attributes'] for f in data['features'])
assert not receipt['pages'][-1]['exceededTransferLimit']
assert len(historical) == 2966
ids = [r['OBJECTID'] for r in historical]
assert len(set(ids)) == len(ids)
assert set(ids) == set(json.loads(P.joinpath('retry-old-ids.capture').read_text())['objectIds'])
assert len(ids) == json.loads(P.joinpath('retry-old-count.capture').read_text())['count']
master = []
for e in json.loads(P.joinpath('pages.json').read_text()):
    master.extend(f['attributes'] for f in json.loads(gzip.decompress(P.joinpath(e['file']).read_bytes()))['features'])
a = {r['OBJECTID']:r for r in historical}
b = {r['OBJECTID']:r for r in master}
common = sorted(set(historical[0]) & set(master[0]))
diff = [(k,f) for k in a for f in common if a[k][f] != b[k][f]]
assert len(diff) == receipt['differing_values'] == 532
assert len({k for k,f in diff}) == receipt['differing_objectid_rows'] == 302
noid = [k for k in common if k != 'OBJECTID']
assert Counter(tuple(r[k] for k in noid) for r in historical) == Counter(tuple(r[k] for k in noid) for r in master)
assert receipt['common_attribute_multiset_excluding_objectid_equal'] is True
assert receipt['all_common_attributes_equal_on_objectid'] is False
print('PASS: restored service complete2966 attributes; equal non-OID multisets,532 differences across302 OBJECTID joins. OBJECTID cross-service stability disproven.')
