"""Restore exact historical colleges evidence artifact without trusting archive paths."""
import argparse,hashlib,json,tarfile
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--archive',type=Path,required=True);p.add_argument('--destination',type=Path,required=True);p.add_argument('--capture-only',action='store_true');a=p.parse_args()
m=json.loads((Path(__file__).parents[1]/'docs/dataset-sources/colleges-and-universities/evidence/artifact-replay.json').read_bytes());pin=m['complete_evidence_archive'];assert a.archive.stat().st_size==pin['bytes']and hashlib.sha256(a.archive.read_bytes()).hexdigest()==pin['sha256'];assert not a.destination.exists();selected={}
with tarfile.open(a.archive,'r:gz')as tf:
 members=tf.getmembers();assert len(members)==len(pin['members'])and {x.name for x in members}==set(pin['members'])
 for member in members:
  path=Path(member.name);assert member.isfile()and not path.is_absolute()and'..'not in path.parts;body=tf.extractfile(member).read();expected=pin['members'][member.name];assert len(body)==expected['bytes']and hashlib.sha256(body).hexdigest()==expected['sha256']
  if not a.capture_only:selected[member.name]=body
  elif path.name in m['complete_source_capture']['files']:selected[path.name]=body
if a.capture_only:assert set(selected)==set(m['complete_source_capture']['files'])
a.destination.mkdir(parents=True)
for name,body in selected.items():path=a.destination/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
print(json.dumps({'approved':False,'verified_members':len(pin['members']),'restored_files':len(selected),'destination':str(a.destination)}))
