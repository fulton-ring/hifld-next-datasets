"""Offline full-capture audit; no writes outside evidence summary, no network."""
import collections
import gzip
import hashlib
import io
import json
import pathlib
import sys

import pyarrow.parquet as pq
from shapely import from_wkb
from shapely.geometry import shape

ROOT = pathlib.Path(__file__).parent
E = ROOT / 'evidence'
RUNTIME = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else pathlib.Path('/workspace/work/hifld-coordinator/10')

def load_bytes(name):
    path = E / (name + '.body.gz')
    return gzip.decompress(path.read_bytes()) if path.exists() else (RUNTIME / (name + '.body')).read_bytes()

def read(name):
    return json.loads(load_bytes(name))

def distribution(values):
    return dict(sorted(collections.Counter(str(v) for v in values).items()))

def id_audit(rows, key):
    values = [r[key] for r in rows]
    return {'null_count': sum(x is None for x in values), 'distinct_count': len(set(values)), 'types': distribution(type(v).__name__ for v in values), 'duplicate_nonnull_values': {str(k): n for k,n in collections.Counter(values).items() if k is not None and n > 1}}

def main():
    manifests = {p.stem: json.loads(p.read_text()) for p in E.glob('*.json') if 'sha256' in json.loads(p.read_text())}
    for name,m in manifests.items():
        b = load_bytes(name)
        assert len(b) == m['bytes'] and hashlib.sha256(b).hexdigest() == m['sha256'], name
    native,geo,layer = read('source-native'),read('source-geojson'),read('layer')
    assert 'error' not in native and 'error' not in geo
    nr = [f['attributes'] for f in native['features']]
    gr = [f['properties'] for f in geo['features']]
    count = read('count')['count']; ids=read('ids')['objectIds']
    assert len(nr)==len(gr)==count==len(set(ids))==477
    assert {r['OBJECTID'] for r in nr}=={r['OBJECTID'] for r in gr}==set(ids)
    assert not native.get('exceededTransferLimit') and not geo.get('exceededTransferLimit')
    assert not read('page-end')['features']
    assert nr==gr, 'Native and GeoJSON attributes differ'
    fields = [f['name'] for f in layer['fields']]
    assert all(set(r)==set(fields) for r in nr)
    t=pq.read_table(io.BytesIO(load_bytes('candidate-pinned'))); br=t.to_pylist()
    src={r['GLOBAL_ID']:r for r in nr};base={r['GLOBAL_ID']:r for r in br}
    assert len(src)==len(nr) and len(base)==len(br)
    common=sorted(src.keys() & base.keys())
    shared=sorted(set(fields)&set(t.column_names))
    differences={c:sum(src[k][c]!=base[k][c] for k in common) for c in shared}
    gmap={f['properties']['GLOBAL_ID']:shape(f['geometry']) for f in geo['features']}
    bmap={r['GLOBAL_ID']:from_wkb(r['geometry']) for r in br}
    changed=[k for k in common if not gmap[k].equals_exact(bmap[k],0,normalize=True)]
    equal_topology=[k for k in common if gmap[k].equals(bmap[k])]
    current_bounds=[min(g.bounds[0] for g in gmap.values()),min(g.bounds[1] for g in gmap.values()),max(g.bounds[2] for g in gmap.values()),max(g.bounds[3] for g in gmap.values())]
    result={'source_count':count,'candidate_count':len(br),'source_crs':native['spatialReference'],'comparison_crs':'EPSG:4326 (ArcGIS outSR transformation vs GeoParquet declared CRS)','max_record_count':layer['maxRecordCount'],'pagination_supported':layer['advancedQueryCapabilities']['supportsPagination'],'complete':True,'completeness_note':'Count, independent returnIdsOnly, native all-column/geometry and GeoJSON rows reconcile; terminal page empty; no exceededTransferLimit. Sequential observations, not atomic snapshot.','source_ids':{k:id_audit(nr,k) for k in ('OBJECTID','GLOBAL_ID','GlobalID','IDENT','ICAO_ID','ADHP_ID')},'candidate_ids':{k:id_audit(br,k) for k in ('OBJECTID','GLOBAL_ID','IDENT','ICAO_ID','ADHP_ID','id')},'source_columns':fields,'candidate_schema':[{ 'name':f.name,'type':str(f.type),'nullable':f.nullable} for f in t.schema],'source_only_columns':sorted(set(fields)-set(t.column_names)),'candidate_only_columns':sorted(set(t.column_names)-set(fields)-{'geometry'}),'join_key':'GLOBAL_ID (unique in each observed capture; longitudinal stability not established)','common_GLOBAL_ID_count':len(common),'source_only_GLOBAL_IDs':sorted(src.keys()-base.keys()),'candidate_only_GLOBAL_IDs':sorted(base.keys()-src.keys()),'shared_column_changed_counts':differences,'geometry_exact_normalized_changed_count':len(changed),'geometry_topologically_equal_count':len(equal_topology),'geometry_comparison_note':'Exact normalized GEOS coordinates compared for every common GLOBAL_ID; no snapping/tolerance. A difference alone does not isolate source edits from coordinate transformation/serialization.','source_geometry_types':distribution(g.geom_type for g in gmap.values()),'candidate_geometry_types':distribution(g.geom_type for g in bmap.values()),'source_invalid_geometry_count':sum(not g.is_valid for g in gmap.values()),'candidate_invalid_geometry_count':sum(not g.is_valid for g in bmap.values()),'source_empty_geometry_count':sum(g.is_empty for g in gmap.values()),'source_bounds':current_bounds,'candidate_bounds':[min(g.bounds[0] for g in bmap.values()),min(g.bounds[1] for g in bmap.values()),max(g.bounds[2] for g in bmap.values()),max(g.bounds[3] for g in bmap.values())],'source_TYPE_CODE_counts':distribution(r['TYPE_CODE'] for r in nr),'candidate_TYPE_CODE_counts':distribution(r['TYPE_CODE'] for r in br),'source_attribute_json_types':{c:distribution(type(r[c]).__name__ for r in nr) for c in fields},'source_null_counts':{c:sum(r[c] is None for r in nr) for c in fields},'source_to_geojson_attributes_exact':True,'accepted_baseline':None}
    out=E/'audit-summary.json';out.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k in ('source_count','candidate_count','common_GLOBAL_ID_count','source_only_columns','candidate_only_columns','geometry_exact_normalized_changed_count','geometry_topologically_equal_count','shared_column_changed_counts','source_bounds','candidate_bounds')},indent=2))

if __name__=='__main__': main()
