"""Complete PMTiles V3 directory/content/property audit; cartography, not raw geometry equivalence."""
from pathlib import Path
import json,struct,gzip,hashlib,collections
import argparse
parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--capture-dir',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();W=args.capture_dir.resolve()
from dagster_hifld.sources.fema_regions import acquire
acquire(W) # Complete pinned inputs, bounded regular reads and original receipts before parsing.
p=next(W.glob('baseline-pmtiles__*.body'));blob=p.read_bytes();assert blob[:8]==b'PMTiles\x03'
u=lambda off:struct.unpack_from('<Q',blob,off)[0]
header={'root_offset':u(8),'root_length':u(16),'metadata_offset':u(24),'metadata_length':u(32),'leaf_offset':u(40),'leaf_length':u(48),'tile_offset':u(56),'tile_length':u(64),'addressed_tiles':u(72),'tile_entries':u(80),'tile_contents':u(88),'internal_compression':blob[97],'tile_compression':blob[98],'tile_type':blob[99],'min_zoom':blob[100],'max_zoom':blob[101]}
def dec(data,compression,limit=16_000_000):
 if compression==1:out=data
 elif compression==2:
  import io
  with gzip.GzipFile(fileobj=io.BytesIO(data))as stream:out=stream.read(limit+1)
 else:raise ValueError('unsupported compression '+str(compression))
 assert len(out)<=limit;return out
def vi(data,pos):
 val=0;shift=0
 while True:
  assert pos<len(data)and shift<70;n=data[pos];pos+=1;val|=(n&127)<<shift
  if not n&128:return val,pos
  shift+=7
def directory(data):
 count,pos=vi(data,0);assert count<1_000_000;ids=[];last=0
 for _ in range(count):v,pos=vi(data,pos);last+=v;ids.append(last)
 runs=[];lengths=[];offsets=[]
 for target in [runs,lengths]:
  for _ in range(count):v,pos=vi(data,pos);target.append(v)
 for i in range(count):v,pos=vi(data,pos);offsets.append(offsets[i-1]+lengths[i-1]if v==0 and i else v-1)
 assert pos==len(data);return list(zip(ids,runs,lengths,offsets))
def fields(data):
 pos=0
 while pos<len(data):
  key,pos=vi(data,pos);number=key>>3;wire=key&7
  if wire==0:value,pos=vi(data,pos)
  elif wire==1:value=data[pos:pos+8];pos+=8
  elif wire==2:size,pos=vi(data,pos);value=data[pos:pos+size];assert len(value)==size;pos+=size
  elif wire==5:value=data[pos:pos+4];pos+=4
  else:raise ValueError('protobuf wire '+str(wire))
  yield number,wire,value
 assert pos==len(data)
def value(data):
 f=list(fields(data));assert len(f)==1;n,w,v=f[0]
 if n==1:return v.decode()
 if n==2:return struct.unpack('<f',v)[0]
 if n==3:return struct.unpack('<d',v)[0]
 if n in[4,5]:return v if n==5 or v<2**63 else v-2**64
 if n==6:return(v>>1)^-(v&1)
 if n==7:return bool(v)
 raise ValueError('unknown MVT value')
root=blob[header['root_offset']:header['root_offset']+header['root_length']];queue=directory(dec(root,header['internal_compression']));seenleaves=set();entries=[]
while queue:
 tile_id,run,length,offset=queue.pop()
 if run:entries.append((tile_id,run,length,offset));continue
 assert(offset,length)not in seenleaves;seenleaves.add((offset,length));assert 0<=offset and offset+length<=header['leaf_length']
 data=blob[header['leaf_offset']+offset:header['leaf_offset']+offset+length];queue.extend(directory(dec(data,header['internal_compression'])))
assert len(entries)==header['tile_entries']and sum(e[1]for e in entries)==header['addressed_tiles']
contents={(e[3],e[2])for e in entries};assert len(contents)==header['tile_contents'];layers={}
for offset,length in sorted(contents):
 assert offset>=0 and offset+length<=header['tile_length'];data=dec(blob[header['tile_offset']+offset:header['tile_offset']+offset+length],header['tile_compression'])
 for n,w,layer in fields(data):
  assert n==3 and w==2;parts=list(fields(layer));name=next(v.decode()for k,w,v in parts if k==1);keys=[v.decode()for k,w,v in parts if k==3];values=[value(v)for k,w,v in parts if k==4]
  audit=layers.setdefault(name,{'feature_occurrences':0,'feature_ids':set(),'properties':collections.defaultdict(set),'property_types':collections.defaultdict(set)})
  for feature in [v for k,w,v in parts if k==2]:
   audit['feature_occurrences']+=1
   for key,wire,val in fields(feature):
    if key==1:audit['feature_ids'].add(val)
    if key==2:
     tags=[];position=0
     while position<len(val):v,position=vi(val,position);tags.append(v)
     assert len(tags)%2==0
     for i in range(0,len(tags),2):
      k=keys[tags[i]];v=values[tags[i+1]];audit['properties'][k].add(v);audit['property_types'][k].add(type(v).__name__)
metadata=json.loads(dec(blob[header['metadata_offset']:header['metadata_offset']+header['metadata_length']],header['internal_compression']))
result={'scope':'Every complete PMTiles directory, unique tile-content payload and MVT property/ID decoded; quantized cartographic tile geometry is NOT a lossless raw polygon baseline','sha256':hashlib.sha256(blob).hexdigest(),'bytes':len(blob),'header':header,'metadata':metadata,'leaf_directories':len(seenleaves),'all_contents_decoded':len(contents),'layers':{n:{'feature_occurrences':a['feature_occurrences'],'feature_ids':sorted(a['feature_ids']),'properties':{k:sorted(v,key=str)for k,v in a['properties'].items()},'property_types':{k:sorted(v)for k,v in a['property_types'].items()}}for n,a in layers.items()}}
args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'contents':len(contents),'addressed_tiles':header['addressed_tiles'],'layers':[(n,v['feature_occurrences'],v['feature_ids'])for n,v in result['layers'].items()]}))
