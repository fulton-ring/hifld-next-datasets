"""Anonymous bounded mirror audit. Run with uv run --frozen python <this file>."""
import collections
import datetime
import hashlib
import json
from pathlib import Path
import requests

ROOT = Path(__file__).parent / 'evidence'
URL = 'https://services5.arcgis.com/HDRa0B57OVrv2E1q/arcgis/rest/services/Agricultural_Minerals_Operations/FeatureServer/0'

def main():
    log = []
    def get(name, url, params):
        response = requests.get(url, params=params, timeout=40)
        response.raise_for_status()
        value = response.json()
        if 'error' in value:
            raise RuntimeError(value['error'])
        (ROOT / (name + '.json')).write_bytes(response.content)
        log.append({'name': name, 'url': response.url, 'status': response.status_code, 'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'sha256': hashlib.sha256(response.content).hexdigest()})
        return value
    layer = get('layer', URL, {'f':'json'})
    count = get('count', URL+'/query', {'f':'json','where':'1=1','returnCountOnly':'true'})['count']
    ids = get('ids', URL+'/query', {'f':'json','where':'1=1','returnIdsOnly':'true'})['objectIds']
    assert len(ids) == len(set(ids)) == count and None not in ids
    features = []
    for i in range(0,len(ids),100):
        page=get('page-'+str(i//100),URL+'/query',{'f':'json','objectIds':','.join(map(str,sorted(ids)[i:i+100])),'outFields':'*','returnGeometry':'true','outSR':'4326'})
        assert not page.get('exceededTransferLimit',False)
        features.extend(page['features'])
    oid=layer['objectIdField']
    assert collections.Counter(f['attributes'][oid] for f in features)==collections.Counter(ids)
    final=get('ids-after',URL+'/query',{'f':'json','where':'1=1','returnIdsOnly':'true'})
    assert set(final['objectIds'])==set(ids)
    fields=[f['name'] for f in layer['fields']]
    assert all(set(f['attributes'])==set(fields) for f in features)
    attrs=[f['attributes'] for f in features]
    summary={'scope':'third-party mirror only; not archive identity or producer completeness','count':count,'pages':(len(ids)+99)//100,'maxRecordCount':layer['maxRecordCount'],'supportsPagination':layer['advancedQueryCapabilities']['supportsPagination'],'id_set_reconciled':True,'id_set_stable_during_fetch':True,'fields':layer['fields'],'geometry':layer['geometryType'],'crs':4326,'null_geometry':sum(f.get('geometry') is None for f in features),'invalid_coordinate_count':sum(not (-180<=f['geometry']['x']<=180 and -90<=f['geometry']['y']<=90) for f in features if f.get('geometry')),'nulls':{k:sum(a[k] is None for a in attrs) for k in fields},'identifier_profiles':{k:{'unique':len(set(a[k] for a in attrs)),'nulls':sum(a[k] is None for a in attrs)} for k in ['OBJECTID_1','FID_1','OBJECTID','MINOP1X020']},'states':dict(collections.Counter(a['STATE_LOCA'] for a in attrs)),'commodities':dict(collections.Counter(a['COMMODITY'] for a in attrs)),'plant_mine':dict(collections.Counter(a['PLANT_MIN'] for a in attrs))}
    (ROOT/'extraction.json').write_text(json.dumps(summary,indent=2)+'\n')
    (ROOT/'extraction-requests.json').write_text(json.dumps(log,indent=2)+'\n')
    print(json.dumps({k:v for k,v in summary.items() if k not in ['fields','states']}))

if __name__ == '__main__':
    main()
