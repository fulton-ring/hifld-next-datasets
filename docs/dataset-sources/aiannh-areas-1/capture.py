"""Bounded anonymous candidate probes; preserves response bytes/errors, never credentials."""
import csv, datetime, hashlib, json, pathlib, subprocess
ROOT = pathlib.Path(__file__).parent
OUT = ROOT / 'evidence'
URLS = {
 'directory2026': 'https://www2.census.gov/geo/tiger/TIGER2026/AIANNH/',
 'zip2026': 'https://www2.census.gov/geo/tiger/TIGER2026/AIANNH/tl_2026_us_aiannh.zip',
 'landing2023': 'https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.2023.html',
 'landing2025': 'https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.2025.html',
 'crosswalk': 'https://doi.org/10.3886/E241367V1',
 'archive': 'https://www.datalumos.org/datalumos/project/238558/version/V1/view',
 'landing': 'https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html',
 'metadata2023': 'https://meta.geo.census.gov/data/existing/decennial/GEO/GPMB/TIGERline/Current_19115/series_tl_2023_aiannh.shp.iso.xml',
 'directory2023': 'https://www2.census.gov/geo/tiger/TIGER2023/AIANNH/',
 'zip2023': 'https://www2.census.gov/geo/tiger/TIGER2023/AIANNH/tl_2023_us_aiannh.zip',
 'directory2025': 'https://www2.census.gov/geo/tiger/TIGER2025/AIANNH/',
 'zip2025': 'https://www2.census.gov/geo/tiger/TIGER2025/AIANNH/tl_2025_us_aiannh.zip',
}
results=[]
for name,url in URLS.items():
    target=OUT/(name+'.body')
    args=['curl','--silent','--show-error','--location','--max-time','30','--max-filesize','20000000','--output',str(target),'--write-out','%{http_code}\n%{url_effective}\n%{size_download}',url]
    r=subprocess.run(args,capture_output=True,text=True)
    body=target.read_bytes() if target.exists() else b''
    results.append(dict(name=name,url=url,checked_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),command=args,exit_code=r.returncode,result=r.stdout,error=r.stderr,body_file=str(target.relative_to(ROOT)) if target.exists() else None,bytes=len(body),sha256=hashlib.sha256(body).hexdigest() if target.exists() else None))
with open('HIFLD_Open_Inventory_12112025.csv',newline='') as f:
    reader=csv.DictReader(f)
    row=next(r for r in reader if 'aiannh-areas-1' in r.values())
(OUT/'inventory-row.json').write_text(json.dumps(row,indent=2)+'\n')
(OUT/'requests.json').write_text(json.dumps(results,indent=2)+'\n')
print(json.dumps(results,indent=2))
