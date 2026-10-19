import pathlib,json,httpx,hashlib,datetime,concurrent.futures
p=pathlib.Path('/workspace/work/hifld-coordinator/45');urls={'directory':'https://www2.census.gov/geo/tiger/TGRGDB24/','release':'https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html','metadata':'https://meta.geo.census.gov/data/existing/decennial/GEO/GPMB/TIGERline/Current_19115/series_tl_2023_cousub.shp.iso.xml','archive':'https://www.datalumos.org/datalumos/project/239081/version/V1/view','crosswalk':'https://doi.org/10.3886/E241367V1','root_catalog':'https://storage.googleapis.com/hifld-next-datasets-prod/catalog.json','inventory_catalog':'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/county-subdivisions-1/catalog.json','file_catalog':'https://storage.googleapis.com/hifld-next-datasets-prod/hifld/county-subdivisions-1/county-subdivisions-1/catalog.json'}
def fetch(k,u):
 try:
  with httpx.Client(follow_redirects=True,timeout=60,headers={'Accept-Encoding':'identity'}) as c:r=c.get(u)
  if len(r.content)>3000000:raise ValueError('boundeddiscoverylimit')
  (p/(k+'.body')).write_bytes(r.content);d={'url':u,'final_url':str(r.url),'status':r.status_code,'bytes':len(r.content),'sha256':hashlib.sha256(r.content).hexdigest(),'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'complete_body':True};(p/(k+'.receipt.json')).write_text(json.dumps(d,indent=2)+'\n');return k,r.status_code,len(r.content)
 except Exception as e:return k,type(e).__name__,str(e)
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as x:print(list(x.map(lambda kv:fetch(*kv),urls.items())))
