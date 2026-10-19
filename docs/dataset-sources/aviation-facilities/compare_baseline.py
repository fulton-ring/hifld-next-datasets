"""Complete pinned baseline vs full native source; diagnostics never acceptance."""
import collections
import hashlib
import json
from pathlib import Path
import sys
import pyarrow.parquet as pq
from shapely import from_wkb
from pyproj import CRS
from dagster_hifld import aviation_extract as a

root=Path(sys.argv[1]);capture=root/'capture-bound'
base=pq.read_table(root/'baseline.parquet');originals=[f for p in sorted(capture.glob('page-*.body')) for f in a._json(p)['features']]
source=[f['attributes'] for f in originals];old=base.to_pylist();fields=a.inspect_capture(capture)['fields'];declared={f['name']:f for f in fields}
assert len({r['ARPT_ID'] for r in source})==len(source) and len({r['ARPT_ID'] for r in old})==len(old)
before={r['ARPT_ID']:r for r in old};after={r['ARPT_ID']:r for r in source};geom={f['attributes']['ARPT_ID']:a._exact_point(f['geometry']) for f in originals}
shared=sorted(set(before)&set(after));common=sorted(set(base.column_names)&set(declared));counts=collections.Counter();examples={};exact_geometry=0;geometry_changed=[]
for identifier in shared:
    for key in common:
        x,y=before[identifier][key],after[identifier][key]
        if x!=y:
            counts[key]+=1
            if key not in examples:examples[key]={'ARPT_ID':identifier,'published':x,'source':y}
    prior=from_wkb(before[identifier]['geometry'])
    if prior.equals_exact(geom[identifier],0):exact_geometry+=1
    elif len(geometry_changed)<10:geometry_changed.append({'ARPT_ID':identifier,'published':list(prior.coords)[0],'source':list(geom[identifier].coords)[0]})
geo=json.loads(base.schema.metadata[b'geo']);native_crs=CRS.from_json_dict(geo['columns'][geo['primary_column']]['crs'])
result={'acceptance':None,'promotion_allowed':False,'comparison':'all rows/all common attributes, exact native geometry, full identifier census and physical schema',
    'diagnostic_join':'ARPT_ID strings currently unique in both; this is diagnostic correspondence, not an approved stable-key mapping',
    'baseline_pin':json.loads((root/'baseline-pin.json').read_bytes()),'baseline_rows':base.num_rows,'source_rows':len(source),
    'source_schema_fields':len(fields),'baseline_columns':len(base.column_names),'baseline_crs':native_crs.to_string(),'source_crs':'EPSG:3857',
    'ARPT_ID':{'matched':len(shared),'added':len(set(after)-set(before)),'removed':len(set(before)-set(after)),
        'added_all':sorted(set(after)-set(before)),'removed_all':sorted(set(before)-set(after))},
    'SITE_NO':{'source_unique':len({r['SITE_NO'] for r in source}),'baseline_unique':len({r['SITE_NO'] for r in old}),
        'source_duplicates':{k:v for k,v in collections.Counter(r['SITE_NO'] for r in source).items() if v>1},
        'baseline_duplicates':{k:v for k,v in collections.Counter(r['SITE_NO'] for r in old).items() if v>1}},
    'columns_added':sorted(set(declared)-set(base.column_names)),'columns_removed':sorted(set(base.column_names)-set(declared)-{'geometry','bbox'}),
    'derived_baseline_columns':['geometry','bbox'],'common_attribute_difference_counts':dict(counts),'common_attribute_difference_examples':examples,
    'physical_type_differences':[{'name':k,'published':str(base.schema.field(k).type),'source':str(a._TYPES[declared[k]['type']])} for k in common if base.schema.field(k).type!=a._TYPES[declared[k]['type']]],
    'schema_nullable_differences':[{'name':k,'published_nullable':base.schema.field(k).nullable,'source_nullable':declared[k]['nullable']} for k in common if base.schema.field(k).nullable!=declared[k]['nullable']],
    'shared_native_geometries_exact':exact_geometry,'shared_native_geometry_changed':len(shared)-exact_geometry,'geometry_changed_examples':geometry_changed,
    'source_bounds':a.inspect_capture(capture)['bounds'],'baseline_bounds':[min(from_wkb(r['geometry']).x for r in old),min(from_wkb(r['geometry']).y for r in old),max(from_wkb(r['geometry']).x for r in old),max(from_wkb(r['geometry']).y for r in old)],
    'source_STATE_CODE_counts':dict(collections.Counter(r['STATE_CODE'] for r in source)),'baseline_STATE_CODE_counts':dict(collections.Counter(r['STATE_CODE'] for r in old)),
    'source_effective_dates':dict(collections.Counter(r['EFF_DATE'] for r in source)),'baseline_effective_dates':dict(collections.Counter(r['EFF_DATE'] for r in old)),
    'blocker':'Changed population, source OBJECTID absent in published candidate, native int32 vs published int64 and all schema/mapping/coverage acceptance require an independent decision; no truncation/filter/replacement accepted.'}
(root/'baseline-comparison.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:result[k] for k in ['baseline_rows','source_rows','source_schema_fields','columns_added','columns_removed','shared_native_geometries_exact','shared_native_geometry_changed','physical_type_differences']},indent=2));print('ARPT_ID counts',{k:v for k,v in result['ARPT_ID'].items() if not k.endswith('_all')})
