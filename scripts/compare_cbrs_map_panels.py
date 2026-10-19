import json,pathlib,collections,datetime
import pyarrow.parquet as pq,shapely
import argparse
parser=argparse.ArgumentParser();parser.add_argument('--output',type=pathlib.Path,required=True);args=parser.parse_args()
p=pathlib.Path(__file__).resolve().parents[1]/'docs/dataset-sources/cbrs-map-panels-1/evidence';src=json.loads((p/'captures/page0000.body').read_bytes())['features'];t=pq.read_table(p/'published_candidate.body');base=t.to_pylist();a={f['attributes']['Panel_No']:f for f in src};b={r['Panel_No']:r for r in base};both=set(a)&set(b)
renames={'Shape_Length':'Shape_Leng','Effective_Date':'Effective_','Historical_Link':'Historical'}
report={'approved':False,'source_rows':len(src),'baseline_rows':len(base),'source_fields':json.loads((p/'captures/page0000.body').read_bytes())['fields'],'baseline_schema':str(t.schema),'unique_panel_ids':[len(a),len(b)],'panel_intersection':len(both),'source_only':sorted(set(a)-set(b)),'baseline_only':sorted(set(b)-set(a)),'source_oid_unique':len({f['attributes']['OBJECTID'] for f in src}),'baseline_id_unique':len({r['id'] for r in base}),'oid_equal':sum(a[k]['attributes']['OBJECTID']==b[k]['OBJECTID'] for k in both),'published_id_equal_source_oid':sum(a[k]['attributes']['OBJECTID']==b[k]['id'] for k in both),'geometry_wkb_equal':sum(shapely.Polygon(a[k]['geometry']['rings'][0]).wkb==b[k]['geometry'] for k in both),'geometry_topological_equal':sum(shapely.Polygon(a[k]['geometry']['rings'][0]).equals(shapely.from_wkb(b[k]['geometry'])) for k in both),'field_differences':{}}
for name in src[0]['attributes']:
 old=renames.get(name,name)
 report['field_differences'][name]={'baseline_name':old,'same_typed_value':sum(type(a[k]['attributes'][name])is type(b[k][old]) and a[k]['attributes'][name]==b[k][old] for k in both),'source_nulls':sum(f['attributes'][name]is None for f in src),'baseline_nulls':sum(r[old]is None for r in base)}
report['date_diagnostic_only']={name:sum(datetime.datetime.fromtimestamp(a[k]['attributes'][name]/1000,datetime.timezone.utc).date().isoformat()==b[k][renames.get(name,name)] for k in both) for name in ['Map_Date','Effective_Date']}
report['source_states']=dict(collections.Counter(f['attributes']['State'] for f in src));report['baseline_states']=dict(collections.Counter(r['State'] for r in base))
args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
