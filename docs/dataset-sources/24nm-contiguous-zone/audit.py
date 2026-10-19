"""Bounded anonymous NOAA research capture; no ingestion or publication."""
import collections, datetime, hashlib, json, pathlib, urllib.parse, urllib.request
P=pathlib.Path(__file__).parent/'evidence'
BASE='https://maritimeboundaries.noaa.gov/arcgis/rest/services/MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer'
log=[]
def fetch(name,url):
    with urllib.request.urlopen(url,timeout=60) as r:
        data=r.read(); status=r.status; final=r.url
    (P/name).write_bytes(data)
    log.append(dict(file=name,url=url,final_url=final,status=status,checked_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),bytes=len(data),sha256=hashlib.sha256(data).hexdigest()))
    obj=json.loads(data)
    if 'error' in obj: raise RuntimeError(obj['error'])
    return obj

def query(name,**args):return fetch(name,BASE+'/2/query?'+urllib.parse.urlencode(dict(f='pjson',**args)))
service=fetch('service.json',BASE+'?f=pjson')
count=query('count.json',where='1=1',returnCountOnly='true')['count']
ids=query('ids-before.json',where='1=1',returnIdsOnly='true')['objectIds']
features=[]
for n in range(0,len(ids),100):
    page=query(f'page-{n//100}.json',objectIds=','.join(map(str,ids[n:n+100])),outFields='*',returnGeometry='true',outSR=4326)
    assert not page.get('exceededTransferLimit'), 'truncated page'
    features+=page['features']
after=query('ids-after.json',where='1=1',returnIdsOnly='true')['objectIds']
actual=[f['attributes']['OBJECTID'] for f in features]
assert count==len(ids)==len(actual)==len(set(actual)) and None not in actual
assert set(ids)==set(actual)==set(after)
summary=dict(count=count,object_ids=actual,ids_reconciled=True,transfer_limit=1000,request_batch_size=100,regions=dict(collections.Counter(f['attributes']['REGION'] for f in features)),feature_types=dict(collections.Counter(f['attributes']['FEAT_TYPE'] for f in features)),fields={k:dict(null_count=sum(f['attributes'][k] is None for f in features),types=sorted({type(f['attributes'][k]).__name__ for f in features})) for k in features[0]['attributes']},null_geometry=sum(not f.get('geometry') for f in features),pub_dates=sorted({str(f['attributes']['PUB_DATE']) for f in features}),approval_dates=sorted({str(f['attributes']['APPRV_DATE']) for f in features}))
(P/'extraction-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
(P/'requests.json').write_text(json.dumps(log,indent=2)+'\n')
print(json.dumps(summary,indent=2))
