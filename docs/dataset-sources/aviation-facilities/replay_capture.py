"""Reconstruct the exact full anonymous capture offline, checking every raw hash."""
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import sys

root=Path(__file__).parent/'evidence';target=Path(sys.argv[1]);target.mkdir(parents=True,exist_ok=False)
records=json.loads((root/'capture-inventory.json').read_bytes())['inventory']
for record in records:
    if 'same_bytes_as' in record:continue
    output=target/record['path'];output.parent.mkdir(parents=True,exist_ok=True)
    body=gzip.decompress((root/record['gzip']).read_bytes()) if 'gzip' in record else (root/record['file']).read_bytes()
    assert len(body)==record['bytes'] and hashlib.sha256(body).hexdigest()==record['sha256']
    output.write_bytes(body)
for record in records:
    if 'same_bytes_as' not in record:continue
    output=target/record['path'];output.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(target/record['same_bytes_as'],output)
    assert output.stat().st_size==record['bytes'] and hashlib.sha256(output.read_bytes()).hexdigest()==record['sha256']
print(f'Restored {len(records)} original files at {target}; no live refetch or new retrieval timestamps')
