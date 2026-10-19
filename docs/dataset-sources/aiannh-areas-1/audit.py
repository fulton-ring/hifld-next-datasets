"""Audit full downloaded Census archives without Dagster materialization."""
import argparse, collections, hashlib, json, pathlib, zipfile
import fiona
from shapely.geometry import shape, mapping
ROOT=pathlib.Path(__file__).parent
OUT=ROOT/'evidence'
parser=argparse.ArgumentParser()
parser.add_argument('--archive-dir',type=pathlib.Path,default=OUT)
args=parser.parse_args()
requests=json.loads((OUT/'requests.json').read_text())
reports={}
for year in (2023,2025,2026):
    raw=args.archive_dir/f'zip{year}.body'
    expected=next(r['sha256'] for r in requests if r['name']==f'zip{year}')
    assert hashlib.sha256(raw.read_bytes()).hexdigest()==expected, 'Archive checksum mismatch'
    with zipfile.ZipFile(raw) as z:
        bad=z.testzip()
        members=[dict(name=i.filename,bytes=i.file_size,crc=i.CRC) for i in z.infolist()]
        for i in z.infolist():
            if i.filename.endswith(('.dbf','.prj','.xml')):
                (OUT/i.filename).write_bytes(z.read(i.filename))
    rows=[]
    samples=[]
    with fiona.open('/vsizip/{'+str(raw.resolve())+'}/tl_'+str(year)+'_us_aiannh.shp') as src:
        schema=dict(src.schema); crs=src.crs.to_string(); bounds=src.bounds
        for f in src:
            p=dict(f['properties']); g=shape(f['geometry']) if f['geometry'] else None
            if len(samples)<2: samples.append(dict(type='Feature',properties=p,geometry=mapping(g) if g else None))
            rows.append(dict(properties=p,geometry_sha256=hashlib.sha256(g.wkb).hexdigest() if g else None,geometry_type=g.geom_type if g else None,valid=g.is_valid if g else False,empty=g.is_empty if g else True))
    (OUT/f'sample{year}.geojson').write_text(json.dumps(dict(type='FeatureCollection',features=samples))+'\n')
    (OUT/f'rows{year}.json').write_text(json.dumps(rows,separators=(',',':'))+'\n')
    reports[str(year)]=dict(zip_crc_failure=bad,members=members,count=len(rows),schema=schema,crs=crs,bounds=bounds,null_counts={k:sum(r['properties'][k] is None for r in rows) for k in schema['properties']},identifier_checks={k:dict(nulls=sum(r['properties'][k] is None for r in rows),unique=len({r['properties'][k] for r in rows}),duplicate_rows=len(rows)-len({r['properties'][k] for r in rows})) for k in schema['properties'] if k in ('GEOID','GEOIDFQ','AIANNHCE','AIANNHNS')},geometry_types=dict(collections.Counter(r['geometry_type'] for r in rows)),invalid=sum(not r['valid'] for r in rows),empty=sum(r['empty'] for r in rows),categories={k:dict(collections.Counter(r['properties'][k] for r in rows)) for k in ('CLASSFP','COMPTYP','LSAD','MTFCC','AIANNHR')})
    reports[str(year)]['release_date']={2023:'2023-11-22',2025:'2025-09-23',2026:'2026-10-14'}[year]
    reports[str(year)]['release_date_note']='Explicit release statement in captured official vintage landing page; distinct from ISO creation and directory modification.'
a=json.loads((OUT/'rows2023.json').read_text()); b=json.loads((OUT/'rows2026.json').read_text())
a={r['properties']['GEOID']:r for r in a}; b={r['properties']['GEOID']:r for r in b}; retained=set(a)&set(b)
reports['comparison_2023_to_2026']=dict(key='GEOID; observed uniqueness only, cross-release stability unapproved',added=sorted(set(b)-set(a)),removed=sorted(set(a)-set(b)),retained=len(retained),changed_geometry=sum(a[k]['geometry_sha256']!=b[k]['geometry_sha256'] for k in retained),changed_fields={f:sum(a[k]['properties'].get(f)!=b[k]['properties'].get(f) for k in retained) for f in reports['2023']['schema']['properties']})
(OUT/'audit.json').write_text(json.dumps(reports,indent=2)+'\n')
print(json.dumps(reports,indent=2))
