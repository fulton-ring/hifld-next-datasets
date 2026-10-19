"""Anonymous, bounded source evidence capture; never materializes Dagster assets."""
import datetime, hashlib, json
from pathlib import Path
import requests
ROOT = Path(__file__).parent / 'evidence'
ROOT.mkdir(exist_ok=True)
LOG=[]
def get(name,url,params=None):
    row={'name':name,'url':url,'params':params,'checked_at':datetime.datetime.now(datetime.UTC).isoformat()}
    try:
        r=requests.get(url,params=params,timeout=45)
        row.update(status=r.status_code,final_url=r.url,redirects=[{'status':x.status_code,'url':x.url,'location':x.headers.get('Location')} for x in r.history])
        body=r.content
        if len(body)>2000000: raise ValueError('bounded capture exceeds 2 MB')
        (ROOT/(name+'.capture')).write_bytes(body)
        row.update(bytes=len(body),sha256=hashlib.sha256(body).hexdigest())
        try:
            data=r.json();row['arcgis_error']=data.get('error') if isinstance(data,dict) else None
        except ValueError:data=None
        return data
    except Exception as e: row['failure']=str(e)
    finally:
        LOG.append(row);(ROOT/'requests.json').write_text(json.dumps(LOG,indent=2)+'\n')
if __name__=='__main__':
    get('crosswalk','https://doi.org/10.3886/E241367V1')
    get('archive','https://www.datalumos.org/datalumos/project/238556/version/V1/view')
    get('search','https://www.arcgis.com/sharing/rest/search',{'f':'json','num':100,'q':'title:"Administrative Areas" IENC'})
    for name,item in [('master','6ee858a220a143a48c8d40a6cf0d3680'),('old','c04e3977dbae4f949425faa22cca0cf1'),('hifld','3d053589d0ad4e1a9df46d33382e39cb')]:
        get(name+'-item',f'https://www.arcgis.com/sharing/rest/content/items/{item}',{'f':'json'})
    get('owner','https://www.arcgis.com/sharing/rest/community/users/usace_agc_dkd',{'f':'json'})
    get('org','https://www.arcgis.com/sharing/rest/portals/n1YM8pTrFmm7L4hs',{'f':'json'})
    url='https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/USACE_IENC_Master_Service/FeatureServer'
    service=get('service',url,{'f':'json'})
    if service:
        layers=[x for x in service.get('layers',[]) if x['name'].upper()=='ADMINISTRATIVE_AREA']
        print('matching layers',layers)
        for layer in layers:
            layerurl=url+'/'+str(layer['id'])
            get('layer',layerurl,{'f':'json'})
            get('count',layerurl+'/query',{'f':'json','where':'1=1','returnCountOnly':'true'})
            get('ids',layerurl+'/query',{'f':'json','where':'1=1','returnIdsOnly':'true'})
            get('features',layerurl+'/query',{'f':'json','where':'1=1','outFields':'*','returnGeometry':'true','resultRecordCount':2000,'orderByFields':'OBJECTID ASC'})
    get('agc-landing','https://www.agc.army.mil/Missions/Navigation/Inland-Electronic-Navigational-Charts/')
    get('old-endpoint','https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/ADMINISTRATIVE_AREA/MapServer/0',{'f':'json'})
