import httpx,json,pathlib,hashlib,datetime,os,shutil
p=pathlib.Path('/workspace/work/hifld-coordinator/34')
u='https://www2.census.gov/geo/tiger/TGRGDB24/tlgdb_2024_a_us_substategeo.gdb.zip';target=p/'official2024_substategeo.zip';size=736048475
if target.exists():raise SystemExit('already exists')
if shutil.disk_usage(p).free<size+650_000_000:raise SystemExit('insufficient guarded free space')
h=hashlib.sha256();n=0
with httpx.stream('GET',u,headers={'Accept-Encoding':'identity'},timeout=180) as r, open(target.with_suffix('.partial'),'xb') as f:
 if r.status_code!=200:raise ValueError(r.status_code)
 for b in r.iter_bytes(1024*1024):
  n+=len(b)
  if n>size or shutil.disk_usage(p).free<500_000_000:raise ValueError('size or capacity guard')
  f.write(b);h.update(b)
 receipt={'url':u,'final_url':str(r.url),'status':r.status_code,'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'bytes':n,'wire_bytes_downloaded':r.num_bytes_downloaded,'sha256':h.hexdigest(),'content_length':r.headers.get('content-length'),'last_modified':r.headers.get('last-modified'),'complete_body':n==size and r.num_bytes_downloaded==int(r.headers['content-length'])}
 p.joinpath('official2024_substategeo.receipt.json').write_text(json.dumps(receipt,indent=2));print(receipt)
 if not receipt['complete_body']:raise ValueError('incomplete')
 os.replace(target.with_suffix('.partial'),target)
