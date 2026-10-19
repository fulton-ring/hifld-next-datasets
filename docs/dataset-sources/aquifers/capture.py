"""Anonymous bounded capture of producer bytes and unapproved published candidate.

This writes evidence only to the supplied local directory, never staging/publish.
An unavailable metadata/crosswalk page is captured as an error, not an approval.
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import json
import urllib.error
import urllib.request
from pathlib import Path

URLS={
 'zip':('https://water.usgs.gov/GIS/dsdl/aquifers_us.zip',12_000_000),
 'landing':('https://www.usgs.gov/mission-areas/water-resources/science/principal-aquifers-united-states',2_000_000),
 'metadata':('https://water.usgs.gov/GIS/metadata/usgswrd/XML/aquifers_us.xml',2_000_000),
 'baseline':('https://storage.googleapis.com/hifld-next-datasets-prod/hifld/aquifers/aquifers/v1.0.0/geoparquet/aquifers.parquet?generation=1791506350105881',12_000_000),
 'crosswalk':('https://doi.org/10.3886/E241367V1',2_000_000),
 'sciencebase':('https://doi.org/10.5066/P9Y2HOUJ',2_000_000),
}


def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--directory',type=Path,required=True);args=parser.parse_args()
 args.directory.mkdir(parents=True,exist_ok=True)
 def capture(item):
  name,(url,limit)=item;body=b'';receipt={'requested_url':url,'fetched_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
  try:
   try:response=urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'HIFLDNext-reconstruction-evidence/1.0'}),timeout=60)
   except urllib.error.HTTPError as error:response=error
   with response:
    receipt.update(status=response.status,final_url=response.url,headers={key:value for key,value in response.headers.items() if key.lower() in {'content-type','content-length','last-modified','etag','x-goog-generation','x-goog-stored-content-length','x-goog-hash'}})
    while chunk:=response.read(64*1024):
     body+=chunk
     if len(body)>limit:raise ValueError('bounded_capture_size_limit')
  except Exception as error:receipt.update(error=type(error).__name__+':'+str(error),partial=True)
  receipt.update(bytes=len(body),sha256=hashlib.sha256(body).hexdigest())
  args.directory.joinpath(name+'.body').write_bytes(body);args.directory.joinpath(name+'.json').write_text(json.dumps(receipt,indent=2)+'\n')
  return {'name':name,'status':receipt.get('status'),'bytes':len(body),'sha256':receipt['sha256']}
 print(json.dumps(list(concurrent.futures.ThreadPoolExecutor(4).map(capture,URLS.items())),indent=2))


if __name__=='__main__':main()
