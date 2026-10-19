"""Full diagnostic comparison, with no approved baseline or ID mapping implied."""
import argparse
import collections
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
from shapely import from_wkb


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--candidate',type=Path,required=True)
    parser.add_argument('--baseline',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    source=pq.read_table(args.candidate); baseline=pq.read_table(args.baseline)
    ss=source.to_pylist();bs=baseline.to_pylist()
    sd={r['Code']:r for r in ss};bd={r['Code']:r for r in bs}
    assert len(sd)==len(ss) and len(bd)==len(bs), 'Diagnostic Code join requires complete unique populations'
    shared=sorted(set(sd)&set(bd))
    attrs=sorted(set(source.column_names)&set(baseline.column_names)-{'geometry','bbox'})
    differences=[]
    for code in shared:
        left,right=sd[code],bd[code]
        changes={name:{'source':left[name],'published_candidate':right[name]} for name in attrs if left[name]!=right[name]}
        sg,bg=from_wkb(left['geometry']),from_wkb(right['geometry'])
        if sg.wkb!=bg.wkb:
            changes['geometry']={'source_wkb_hex':sg.wkb_hex,'published_candidate_wkb_hex':bg.wkb_hex,
                'coordinate_delta_degrees':[sg.x-bg.x,sg.y-bg.y],'meaning':'Exact diagnostic only; no tolerance is approved'}
        differences.append({'Code':code,'source_OBJECTID':left['OBJECTID'],'published_id':right['id'],'changes':changes})
    def profile(table,rows,id_name):
        values=[r[id_name] for r in rows]
        return {'rows':len(rows),'fields':[{'name':f.name,'type':str(f.type),'nullable':f.nullable} for f in table.schema],
            'geo_metadata':json.loads(table.schema.metadata[b'geo']),
            'identifier_column':id_name,'identifier_count':len(set(values)),'identifier_nulls':sum(v is None for v in values),
            'identifier_values_sha256':hashlib.sha256(json.dumps(sorted(values),separators=(',',':')).encode()).hexdigest(),
            'Code_count':len({r['Code'] for r in rows}),
            'coverage_by_State':dict(sorted(collections.Counter(r['State'] for r in rows).items())),
            'coverage_by_StnType':dict(sorted(collections.Counter(r['StnType'] for r in rows).items())),
            'invalid_geometry_count':sum(not from_wkb(r['geometry']).is_valid for r in rows)}
    report={'status':'needs_decision','baseline_acceptance':None,'column_or_ID_mapping_approved':False,
        'candidate_sha256':sha(args.candidate),'baseline_sha256':sha(args.baseline),
        'source':profile(source,ss,'OBJECTID'),'published_candidate':profile(baseline,bs,'id'),
        'source_only_columns':sorted(set(source.column_names)-set(baseline.column_names)),
        'published_only_columns':sorted(set(baseline.column_names)-set(source.column_names)),
        'diagnostic_Code_join':{'meaning':'Complete descriptive comparison using Code, never an approved publisher ID mapping',
            'shared':len(shared),'source_only':sorted(set(sd)-set(bd)),'published_only':sorted(set(bd)-set(sd)),
            'shared_attribute_differences':{name:sum(sd[c][name]!=bd[c][name] for c in shared) for name in attrs},
            'exact_geometry_differences':sum('geometry' in d['changes'] for d in differences),
            'OBJECTID_equals_published_id':sum(sd[c]['OBJECTID']==bd[c]['id'] for c in shared)},
        'complete_shared_Code_differences':differences}
    args.output.write_text(json.dumps(report,sort_keys=True,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report['diagnostic_Code_join'].items() if k!='shared_attribute_differences'}))


if __name__=='__main__':main()
