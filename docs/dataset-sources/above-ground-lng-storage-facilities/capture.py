"""Bounded anonymous evidence fetch; preserves response bytes and denies JSON errors as success."""
import datetime
import gzip
import hashlib
import json
from pathlib import Path
import sys
import urllib.error
import urllib.request

root = Path(__file__).parent / 'evidence'
records = json.loads((root / 'requests.json').read_text())
for name, url in zip(sys.argv[1::2], sys.argv[2::2]):
    row = {'name': name, 'url': url, 'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
    try:
        response = urllib.request.urlopen(url, timeout=35)
        status, final_url = response.status, response.url
        body = response.read(5_000_001)
    except urllib.error.HTTPError as error:
        status, final_url, body = error.code, error.url, error.read(100_000)
    except Exception as error:
        row['error'] = str(error)
        records.append(row)
        print(name, row['error'])
        continue
    if len(body) > 5_000_000:
        raise RuntimeError('bounded capture exceeded; do not treat truncated body as evidence')
    suffix = '.json' if body[:1] == b'{' else '.txt'
    filename = name + suffix
    if suffix == '.txt':
        filename += '.gz'
        row['body_encoding'] = 'gzip; checksum and bytes describe decompressed original'
    (root / filename).write_bytes(gzip.compress(body, mtime=0) if filename.endswith('.gz') else body)
    row.update(status=status, final_url=final_url, body=filename, bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
    try:
        parsed = json.loads(body)
        row['arcgis_error'] = parsed.get('error') if isinstance(parsed, dict) else None
    except ValueError:
        pass
    records.append(row)
    print(name, status, len(body), row.get('arcgis_error'))
(root / 'requests.json').write_text(json.dumps(records, indent=2) + '\n')
