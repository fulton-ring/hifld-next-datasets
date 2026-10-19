"""Verify committed bounded evidence offline; never approves publication.

The complete baseline comparison additionally needs the separately preserved Parquet;
run compare_candidate.py with its path. No live refetch is necessary for replay.
"""
import collections
import gzip
import hashlib
import json
import pathlib

P = pathlib.Path(__file__).parent / 'evidence'
N = P / 'network-20261019'
draft_manifest = json.loads((P / 'draft-checksums.json').read_text())
for name, sha in draft_manifest.items():
    assert hashlib.sha256((P / name).read_bytes()).hexdigest() == sha, name
for directory in [P, N]:
    manifest = json.loads((directory / 'checksums.json').read_text())
    for name, sha in manifest.items():
        assert hashlib.sha256((directory / name).read_bytes()).hexdigest() == sha, name
old = json.loads((P / 'all-features.json').read_text())
old_count = json.loads((P / 'count-recheck.json').read_text())
assert 'error' not in old and 'error' not in old_count
old_ids = [f['attributes']['OBJECTID'] for f in old['features']]
assert len(old_ids) == old_count['count'] == len(set(old_ids)) == 63
assert None not in old_ids and not old.get('exceededTransferLimit', False)
current = json.loads(gzip.decompress((N / 'features-geojson.json.gz').read_bytes()))
assert 'error' not in current and not current.get('exceededTransferLimit', False)
ids = [f['properties']['OBJECTID'] for f in current['features']]
before = json.loads((N / 'ids-json.body').read_text())['objectIds']
after = json.loads((N / 'ids-after.body').read_text())['objectIds']
count = json.loads((N / 'count-json.body').read_text())['count']
assert len(ids) == len(set(ids)) == len(before) == len(set(before)) == len(after) == len(set(after)) == count == 63
assert None not in ids and set(ids) == set(before) == set(after) == set(old_ids)
report = json.loads((N / 'candidate-comparison.json').read_text())
assert report['source_rows'] == report['candidate_rows'] == 63
assert not report['source_only_keys'] and not report['candidate_only_keys']
assert not report['source_only_columns']
assert len(report['geometries']) == 63
assert all(not v['unequal_values'] and not v['unequal_python_types'] and not v['changed_since_reviewed_capture'] for v in report['fields'].values())
assert all(v['wkb_equal_after_single_line_wrapper'] and v['reviewed_raw_wkb_equal'] and v['bbox_equal'] for v in report['geometries'])
# A real declared type difference must remain visible despite observed value equality.
assert report['fields']['UNILATERAL']['esri_declared_type'] == 'esriFieldTypeDouble'
assert report['fields']['UNILATERAL']['candidate_arrow_type'] == 'int64'
assert report['source_bounds'] == report['candidate_bounds']
assert report['source_regions'] == report['candidate_regions']
requests = json.loads((N / 'requests.json').read_text())
assert next(r for r in requests if r['name'] == 'inport')['status'] == 200
assert next(r for r in requests if r['name'] == 'crosswalk')['status'] == 403
readiness = json.loads((N / 'dagster-readiness.json').read_text())
assert readiness['repository']['data']['repositoriesOrError']['__typename'] == 'RepositoryConnection'
assert readiness['materialization'] is False
print(json.dumps({'features': len(ids), 'unique_OBJECTID': len(set(ids)), 'independent_ids_reconciliation': True, 'candidate_report_consistency': True, 'baseline_recomputed': False, 'acceptance': 'UNAPPROVED', 'regions': dict(collections.Counter(f['properties']['REGION'] for f in current['features']))}, indent=2))
