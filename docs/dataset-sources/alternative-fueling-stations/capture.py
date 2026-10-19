"""Anonymous public capture; never registers AFDC user or invents an API key.

Run with PYTHONPATH=src and the locked environment, supplying a NEW capture dir.
The complete feed is streamed in 2000-record native-CRS JSON pages, each <12MB.
"""
import hashlib
import json
import pathlib
import sys
import urllib.error
import urllib.request
from dagster_hifld.assets.fuel_stations import _strict_json

from dagster_hifld.assets.fuel_stations import capture_live


def fetch(root, name, url):
    try:
        with urllib.request.urlopen(url, timeout=90) as response:
            body=response.read(12_000_001); status=response.status; headers=dict(response.headers); final=response.url
    except urllib.error.HTTPError as error:
        body=error.read(12_000_001); status=error.code; headers=dict(error.headers); final=error.url
    except urllib.error.URLError as error:
        receipt={'url':url,'error':str(error)}
        (root/(name+'.receipt.json')).write_text(json.dumps(receipt,indent=2)+'\n');return receipt
    if len(body)>12_000_000:raise ValueError('Response exceeds bounded capture size')
    (root/(name+'.body')).write_bytes(body)
    receipt={'url':url,'final_url':final,'status':status,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'headers':{k:v for k,v in headers.items() if k.lower() not in ('set-cookie',)}}
    (root/(name+'.receipt.json')).write_text(json.dumps(receipt,indent=2)+'\n')
    return receipt


def main(root):
    capture_live(root)
    catalog='https://hifld.publicenvirodata.org/api/collections/hifld/datasets/alternative-fueling-stations'
    receipt=fetch(root,'baseline',catalog)
    for name in ('baseline_file','baseline_version'):
        document=_strict_json((root/(('baseline' if name=='baseline_file' else 'baseline_file')+'.body')).read_bytes())
        children=sorted({link['href'] for link in document['links'] if link['rel']=='child'})
        if len(children)!=1:raise ValueError('Ambiguous baseline catalog traversal')
        receipt=fetch(root,name,children[0])
    version=_strict_json((root/'baseline_version.body').read_bytes())
    assets=[a for a in version['assets'].values() if a.get('type')=='application/vnd.apache.parquet']
    if len(assets)!=1:raise ValueError('Ambiguous baseline candidate')
    asset=assets[0]; receipt=fetch(root,'baseline_parquet',asset['href'])
    if receipt['status']!=200 or receipt['bytes']!=asset['file:size']:raise ValueError('Incomplete baseline object')
    generation=next(v for k,v in receipt['headers'].items() if k.lower()=='x-goog-generation')
    pinned=fetch(root,'baseline_pinned',asset['href']+'?generation='+generation)
    if pinned['sha256']!=receipt['sha256']:raise ValueError('Generation pin changed baseline bytes')
    urls={'bts_item':'https://www.arcgis.com/sharing/rest/content/items/9010c04bcdf84c36abb74e98778af579?f=json',
      'bts_metadata':'https://www.arcgis.com/sharing/rest/content/items/9010c04bcdf84c36abb74e98778af579/info/metadata/metadata.xml',
      'afdc_download':'https://afdc.energy.gov/data_download',
      'bulk_form':'https://afdc.energy.gov/data_download/alt_fuel_stations_format',
      'nlr_docs':'https://developer.nlr.gov/docs/transportation/alt-fuel-stations-v1/all/',
      'nlr_api_no_key':'https://developer.nlr.gov/api/alt-fuel-stations/v1.json?limit=1',
      'crosswalk':'https://doi.org/10.3886/E241367V1','archive':'https://www.datalumos.org/datalumos/project/238569/version/V1/view'}
    for name,url in urls.items():fetch(root,name,url)
    print('Preserved full public capture; baseline/source/schema remain unapproved:',root)


if __name__=='__main__':main(pathlib.Path(sys.argv[1]))
