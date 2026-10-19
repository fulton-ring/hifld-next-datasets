"""Bounded incremental full historical GeoJSON feature/ID census, no source mapping."""
import collections
import gzip
import hashlib
import io
import json
import pathlib
import re
import subprocess

import pyarrow.parquet as pq

p=pathlib.Path('/workspace/work/hifld-coordinator/45');capt=p/'published-format-captures';receipt=json.loads((capt/'geojson.receipt.json').read_text());assert receipt['complete_body'] and receipt['lossless_full_replay_verified'];baseline={}
for batch in pq.ParquetFile(p/'published-baseline.parquet').iter_batches(batch_size=512):
 for row in batch.to_pylist():row.pop('geometry');row.pop('bbox');baseline[row['GEOID']]=row
ids={n:[] for n in ['OBJECTID','OID','GEOID','COUSUBNS']};topids=[];topabsent=0;count=0;changes=collections.Counter();roster=None;keys=None;decoder=json.JSONDecoder();prefix=None
with subprocess.Popen(['zstd','-q','-d','-c',str(capt/'geojson.body.zst')],stdout=subprocess.PIPE) as process:
 reader=io.TextIOWrapper(process.stdout,encoding='utf-8');buffer=reader.read(1024*1024);m=re.search(r'"features"\s*:\s*\[',buffer)
 assert m and len(buffer)<134217728;prefix=buffer[:m.end()-1];header=json.loads(prefix+'[]}');assert header['type']=='FeatureCollection';buffer=buffer[m.end():]
 while True:
  buffer=buffer.lstrip()
  if not buffer:
   buffer=reader.read(1024*1024);assert buffer
  if buffer.startswith(','):buffer=buffer[1:];continue
  if buffer.startswith(']'):
   tail=buffer+reader.read();assert len(tail)<134217728;json.loads(prefix+'['+tail);break
  try:feature,end=decoder.raw_decode(buffer)
  except json.JSONDecodeError:
   more=reader.read(1024*1024);assert more and len(buffer)<134217728;buffer+=more;continue
  buffer=buffer[end:];assert feature['type']=='Feature' and set(feature).issubset({'type','id','properties','geometry','bbox'});props=feature['properties'];count+=1
  if 'id' in feature:topids.append(feature['id'])
  else:topabsent+=1
  if keys is None:keys=sorted(props)
  assert sorted(props)==keys
  for n in ids:ids[n].append(props[n])
  old=baseline[props['GEOID']]
  for n in set(old)&set(props):changes[n]+=type(old[n])!=type(props[n]) or old[n]!=props[n]
 assert process.wait()==0
assert count==36498 and len(set(ids['GEOID']))==count
out={'approved':False,'classification':'Complete archival GeoJSON ID census, not source/current correspondence acceptance','rows':count,'properties':keys,'top_feature_id_present_rows':len(topids),'top_feature_id_absent_rows':topabsent,'top_feature_id_equals_published_parquet_id_when_present':all(v==baseline[g]['id'] for v,g in zip(topids,ids['GEOID'])) if not topabsent else None,'property_type_or_value_changes_vs_same_historical_Parquet_GEOID':dict(changes),'original_body_sha256':receipt['sha256'],'header_without_features':header,'source_current_mapping_approved':False}
(p/'historical-geojson-ID-audit.json').write_text(json.dumps(out,indent=2)+'\n')
with (p/'complete-historical-geojson-IDs.json.gz').open('wb') as f,gzip.GzipFile(fileobj=f,mode='wb',mtime=0) as z:z.write((json.dumps({'top_level_feature_id_values':topids,'top_level_feature_id_absent_rows':topabsent,'properties':ids},sort_keys=True,separators=(',',':'))+'\n').encode())
print(json.dumps(out))
