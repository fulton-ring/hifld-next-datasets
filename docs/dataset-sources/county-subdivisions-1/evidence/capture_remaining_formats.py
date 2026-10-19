"""Lossless full HTTP bodies, compressed without changing original byte hashes."""
import datetime
import hashlib
import json
import os
import pathlib
import shutil
import subprocess

import httpx

p=pathlib.Path('/workspace/work/hifld-coordinator/45');out=p/'published-format-captures';out.mkdir(exist_ok=True);assets=json.loads((p/'baseline-collection.json').read_text())['assets']
for key,a in assets.items():
 if key.startswith('geoparquet'):continue
 name=key.split('-')[0];target=out/(name+'.body.zst');receipt=out/(name+'.receipt.json')
 if target.exists():
  previous=json.loads(receipt.read_text());assert previous['complete_body'] and previous['lossless_full_replay_verified'] and previous['compressed_bytes']==target.stat().st_size and previous['compressed_sha256']==hashlib.file_digest(target.open('rb'),'sha256').hexdigest();print(json.dumps({'format':name,'status':'verified existing complete capture preserved'}),flush=True);continue
 if shutil.disk_usage(out).free<800000000:raise ValueError('capacity guard before large stream')
 partial=out/(name+'.partial.zst');h=hashlib.sha256();m=hashlib.md5();n=0;d={'url':a['href'],'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'complete_body':False,'classification':'complete historical published candidate only after full body/provider checksum verification'}
 with partial.open('xb') as compressed:
  z=subprocess.Popen(['zstd','-q','-6','-T1','-c'],stdin=subprocess.PIPE,stdout=compressed)
  try:
   with httpx.Client(timeout=180,follow_redirects=False,headers={'Accept-Encoding':'identity'}) as client,client.stream('GET',a['href']) as r:
    d.update(status=r.status_code,final_url=str(r.url),content_length=r.headers.get('content-length'),provider_hash=r.headers.get('x-goog-hash'),generation=r.headers.get('x-goog-generation'))
    if r.status_code!=200:raise ValueError('unexpected HTTP status')
    for block in r.iter_bytes(1024*1024):
     n+=len(block)
     if n>a['file:size'] or shutil.disk_usage(out).free<350000000:raise ValueError('original body size or global floor guard')
     h.update(block);m.update(block);z.stdin.write(block)
    d['wire_bytes_downloaded']=r.num_bytes_downloaded
   z.stdin.close();assert z.wait()==0
   d.update(bytes=n,sha256=h.hexdigest(),md5=m.hexdigest(),collection_checksum=a['file:checksum'])
   assert n==a['file:size'] and a['file:checksum']=='d50110'+m.hexdigest()
   assert d['content_length'] is None or int(d['content_length'])==d['wire_bytes_downloaded']
   # Independently decompress every original byte, not only zstd integrity test.
   replay=hashlib.sha256();size=0
   with subprocess.Popen(['zstd','-q','-d','-c',str(partial)],stdout=subprocess.PIPE) as reader:
    while block:=reader.stdout.read(1024*1024):size+=len(block);replay.update(block)
    assert reader.wait()==0
   assert size==n and replay.hexdigest()==h.hexdigest()
   os.replace(partial,target);d.update(complete_body=True,lossless_full_replay_verified=True,compression='zstd; original full HTTP body reproduced exactly',compressed_bytes=target.stat().st_size,compressed_sha256=hashlib.file_digest(target.open('rb'),'sha256').hexdigest());receipt.write_text(json.dumps(d,indent=2)+'\n');print(json.dumps({'format':name,'status':'PASS','bytes':n,'compressed_bytes':d['compressed_bytes'],'sha256':d['sha256']}),flush=True)
  except BaseException as exc:
   if z.stdin and not z.stdin.closed:z.stdin.close()
   z.wait();d.update(reason=str(exc),preserved_partial=str(partial),bytes=n,sha256=h.hexdigest());receipt.write_text(json.dumps(d,indent=2)+'\n');raise
