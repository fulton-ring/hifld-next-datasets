"""Offline retained-evidence verification; never fetches live URLs."""
import collections, hashlib, json, pathlib, struct
ROOT=pathlib.Path(__file__).parent
E=ROOT/'evidence'
manifest=json.loads((E/'checksums.json').read_text())
for name,digest in manifest.items():
    assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest()==digest,name
report=json.loads((E/'audit.json').read_text())
contract=json.loads((ROOT/'source.json').read_text())['published_contract']
assert contract['approved'] is False
for year in (2023,2025,2026):
    rows=json.loads((E/f'rows{year}.json').read_text()); audit=report[str(year)]
    dbf=(E/f'tl_{year}_us_aiannh.dbf').read_bytes()
    assert struct.unpack_from('<I',dbf,4)[0]==len(rows)==audit['count']
    assert set(rows[0]['properties'])==set(audit['schema']['properties'])
    assert {k:sum(r['properties'][k] is None for r in rows) for k in audit['schema']['properties']}==audit['null_counts']
    assert len({r['properties']['GEOID'] for r in rows})==len(rows)
    assert dict(collections.Counter(r['geometry_type'] for r in rows))==audit['geometry_types']
    for field,counts in audit['categories'].items():
        assert dict(collections.Counter(r['properties'][field] for r in rows))==counts
    assert len(json.loads((E/f'sample{year}.geojson').read_text())['features'])==2
assert {c['name'] for c in contract['columns']}==set(report['2023']['schema']['properties'])|{'geometry'}
graph=json.loads((E/'graphql.json').read_text())
assert not graph.get('errors')
assert graph['data']['repositoriesOrError']['__typename']=='RepositoryConnection'
assert any(n['assetKey']['path']==['ingest','aiannh-areas-1','aiannh-areas-1'] for n in graph['data']['assetNodes'])
print('PASS: retained checksums, complete row IDs/attributes/category counts, DBF counts, all-column proposal, and captured Dagster readiness; no ingestion acceptance')
