"""Explicit live evidence retry. Pass a new scratch output directory; never overwrites pinned evidence."""
import argparse
import urllib.request,urllib.error,json,pathlib,hashlib,datetime,concurrent.futures
parser = argparse.ArgumentParser()
parser.add_argument('output_directory', type=pathlib.Path)
args = parser.parse_args()
p = args.output_directory
p.mkdir(parents=True, exist_ok=False)
records=[]
def get(pair):
 name,url=pair;r={'name':name,'url':url,'observed_at':datetime.datetime.now(datetime.UTC).isoformat()}
 try:
  with urllib.request.urlopen(url,timeout=45) as v:b=v.read();r.update(status=v.status,final_url=v.url,headers={k:v.headers[k] for k in ['Content-Type','cf-mitigated'] if k in v.headers})
 except urllib.error.HTTPError as e:b=e.read();r.update(status=e.code,final_url=e.url,headers={k:e.headers[k] for k in ['Content-Type','cf-mitigated'] if k in e.headers})
 except Exception as e:b=str(e).encode();r['error']=str(e)
 p.joinpath(name+'.body').write_bytes(b);r.update(bytes=len(b),sha256=hashlib.sha256(b).hexdigest());print(name,r.get('status'),len(b));return r
base='https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/AIRPORT_AREA/MapServer'
master='https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/USACE_IENC_Master_Service/FeatureServer/57'
urls={'historical-service':base+'?f=json','services':'https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=json','agc':'https://www.agc.army.mil/Missions/Echarts/','archive':'https://www.datalumos.org/datalumos/project/238564/version/V1/view','crosswalk':'https://doi.org/10.3886/E241367V1','master-geojson':master+'/query?f=geojson&where=1%3D1&outFields=*&returnGeometry=true&orderByFields=OBJECTID&resultRecordCount=2000'}
records+=list(concurrent.futures.ThreadPoolExecutor(6).map(get,urls.items()))
service=json.loads(p.joinpath('historical-service.body').read_bytes());print('layers',service.get('layers'));layer=next(x['id'] for x in service['layers'] if x['name']=='AIRPORT_AREA');url=base+'/'+str(layer)
records+=list(concurrent.futures.ThreadPoolExecutor(5).map(get,{'historical-layer':url+'?f=json','historical-count':url+'/query?f=json&where=1%3D1&returnCountOnly=true','historical-ids':url+'/query?f=json&where=1%3D1&returnIdsOnly=true','historical-features':url+'/query?f=json&where=1%3D1&outFields=*&returnGeometry=true&orderByFields=OBJECTID&resultRecordCount=2000','historical-geojson':url+'/query?f=geojson&where=1%3D1&outFields=*&returnGeometry=true&orderByFields=OBJECTID&resultRecordCount=2000'}.items()))
p.joinpath('requests.json').write_text(json.dumps(records,indent=2))
