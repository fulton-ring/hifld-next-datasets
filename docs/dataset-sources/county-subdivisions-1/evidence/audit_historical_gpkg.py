"""Full historical native GPKG audit; bounded verified read-only temporary expansion.

Original full body remains in its checksum-verified lossless zstd capture.
"""
import collections
import gzip
import hashlib
import json
import os
import pathlib
import subprocess
import tempfile
import shutil

import pyarrow.parquet as pq
import pyogrio

p=pathlib.Path('/workspace/work/hifld-coordinator/45');captures=p/'published-format-captures';receipt=json.loads((captures/'geopackage.receipt.json').read_text());assert receipt['complete_body'] and receipt['lossless_full_replay_verified'];assert shutil.disk_usage(p).free>1026000000
scratch=tempfile.TemporaryDirectory(prefix='verified-historical-gpkg-',dir=p);path=pathlib.Path(scratch.name)/'historical.gpkg';fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
try:
 sha=hashlib.sha256();size=0
 with subprocess.Popen(['zstd','-q','-d','-c',str(captures/'geopackage.body.zst')],stdout=subprocess.PIPE) as process:
  while block:=process.stdout.read(1024*1024):
   assert shutil.disk_usage(p).free>350000000;sha.update(block);size+=len(block);view=memoryview(block)
   while view:view=view[os.write(fd,view):]
  assert process.wait()==0
 assert size==receipt['bytes'] and sha.hexdigest()==receipt['sha256'];os.fsync(fd);os.close(fd);fd=-1;os.chmod(path,0o400)
 info=pyogrio.read_info(path);baseline={}
 for batch in pq.ParquetFile(p/'published-baseline.parquet').iter_batches(batch_size=512):
  for r in batch.to_pylist():r['geometry_sha256']=hashlib.sha256(r.pop('geometry')).hexdigest();baseline[r['GEOID']]=r
 count=0;ids={};nulls=collections.Counter();changes=collections.Counter();columns={};types=None;fid=None;names=None
 with pyogrio.open_arrow(path,use_pyarrow=True,return_fids=True,batch_size=512) as (metadata,reader):
  types={f.name:str(f.type) for f in reader.schema};fid=metadata['fid_column'];names=reader.schema.names;geo=metadata['geometry_name'];columns={n:hashlib.sha256() for n in names};ids={n:[] for n in names if n in [fid,'OBJECTID','id','OID','COUSUBNS','GEOID','STATE','COUNTY','COUSUB']}
  for batch in reader:
   for row in batch.to_pylist():
    count+=1;old=baseline[row['GEOID']]
    for n,v in row.items():
     nulls[n]+=v is None;value=v.hex() if isinstance(v,bytes) else v;columns[n].update((json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)+'\n').encode())
     if n in ids:ids[n].append(v)
     if n in old:changes[n]+=type(v)!=type(old[n]) or v!=old[n]
    changes['native_geometry_wkb']+=hashlib.sha256(row[geo]).hexdigest()!=old['geometry_sha256']
 assert count==36498
 d={'approved':False,'classification':'Complete historical native GeoPackage format audit, no current-source mapping accepted','original_http_body_bytes':size,'original_http_body_sha256':sha.hexdigest(),'temporary_read_storage':'Verified read-only temporary expanded duplicate, removed after complete audit; original lossless compressed body remains preserved','native_layer':metadata['layer_name'] if 'layer_name' in metadata else info['layer_name'],'rows':count,'native_crs':metadata['crs'],'intrinsic_FID_column':fid,'native_geometry_column':geo,'native_types':types,'nulls':dict(nulls),'full_native_column_value_sha256':{n:h.hexdigest() for n,h in columns.items()},'value_or_type_changes_vs_same_historical_Parquet_GEOID':dict(changes),'original_ID_columns':{n:{'rows':len(v),'unique':len(set(v)),'ordered_values_sha256':hashlib.sha256((json.dumps(v,separators=(',',':'))+'\n').encode()).hexdigest()} for n,v in ids.items()}}
 (p/'historical-native-gpkg-audit.json').write_text(json.dumps(d,indent=2)+'\n')
 with (p/'complete-historical-gpkg-IDs.json.gz').open('wb') as out,gzip.GzipFile(fileobj=out,mode='wb',mtime=0) as z:z.write((json.dumps(ids,sort_keys=True,separators=(',',':'))+'\n').encode())
 print(json.dumps(d))
finally:
 if fd>=0:os.close(fd)
 scratch.cleanup()
