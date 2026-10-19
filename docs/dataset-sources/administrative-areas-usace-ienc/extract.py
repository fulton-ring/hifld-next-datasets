"""Bounded complete anonymous extraction and offline summary of a candidate, not approval."""
import gzip,hashlib,json,datetime
from pathlib import Path
import requests
P=Path(__file__).parent/'evidence'
URL='https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/USACE_IENC_Master_Service/FeatureServer/56/query'

def capture():
    entries=[]
    for offset in range(0,2966,100):
        params={'f':'json','where':'1=1','outFields':'*','returnGeometry':'false','resultOffset':offset,'resultRecordCount':100,'orderByFields':'OBJECTID ASC'}
        r=requests.get(URL,params=params,timeout=60);r.raise_for_status();j=r.json();assert 'error' not in j,j
        body=r.content; assert len(body)<3000000
        filename=f'page-{offset:04}.json.gz';(P/filename).write_bytes(gzip.compress(body,mtime=0))
        entries.append({'file':filename,'url':r.url,'checked_at':datetime.datetime.now(datetime.UTC).isoformat(),'status':r.status_code,'count':len(j['features']),'exceededTransferLimit':j.get('exceededTransferLimit',False),'sha256_raw':hashlib.sha256(body).hexdigest(),'sha256_gzip':hashlib.sha256((P/filename).read_bytes()).hexdigest()})
        (P/'pages.json').write_text(json.dumps(entries,indent=2)+'\n')
        print(offset,len(body),flush=True)

def summarize():
    from collections import Counter
    from shapely.geometry import Polygon,MultiPolygon
    rows=[];geometry_null=0;invalid=0;bounds=[]
    entries=json.loads((P/'pages.json').read_text())
    assert len(entries)==30
    assert all(e['count']==100 for e in entries[:-1]) and entries[-1]['count']==66
    assert not entries[-1]['exceededTransferLimit']
    for e in entries:
        raw=gzip.decompress((P/e['file']).read_bytes());assert hashlib.sha256(raw).hexdigest()==e['sha256_raw']
        j=json.loads(raw)
        for f in j['features']:
            rows.append(f['attributes']);g=f.get('geometry')
            if not g:geometry_null+=1;continue
            # ArcGIS rings are preserved; validity below tests each ring independently,
            # not polygon topology or hole assembly.
            for ring in g.get('rings',[]):
                poly=Polygon(ring);invalid+=not poly.is_valid;bounds.append(poly.bounds)
    expected=json.loads((P/'ids.capture').read_text())['objectIds'];ids=[x['OBJECTID'] for x in rows]
    assert len(ids)==len(set(ids))==len(expected)==2966
    assert set(ids)==set(expected)
    fields=json.loads((P/'layer.capture').read_text())['fields']
    s={'record_count':len(rows),'all_ids_reconciled':True,'objectid_nulls':ids.count(None),'objectid_duplicates':len(ids)-len(set(ids)),'geometry_extraction':'not extracted: first 100 unsimplified geometries exceed bounded 3 MB request; no full geometry/completeness claim', 'geometry_nulls':None,'invalid_individual_rings':None,'topology_validation':'not performed: complete attributes only; geometry sample retained separately','bounds':json.loads((P/'layer.capture').read_text())['extent'],'fields':{f['name']:{'nulls':sum(x.get(f['name']) is None for x in rows),'distinct':len({x.get(f['name']) for x in rows}),'observed_types':sorted({type(x.get(f['name'])).__name__ for x in rows}),'examples':[v for v in dict.fromkeys(x.get(f['name']) for x in rows)][:5]} for f in fields},'source_dataset_categories':dict(Counter(x['Source_Dataset'] for x in rows)),'nationality_categories':dict(Counter(x['Nationality'] for x in rows)),'unexpected_fields':sorted(set().union(*(set(x) for x in rows))-{f['name'] for f in fields})}
    (P/'extraction-summary.json').write_text(json.dumps(s,indent=2)+'\n')
if __name__=='__main__':
    import sys
    if '--offline' not in sys.argv:capture()
    summarize()
