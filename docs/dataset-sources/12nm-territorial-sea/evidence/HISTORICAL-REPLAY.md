# Replay the preserved 87-feature research capture

`historical-captures-20261019.tar.gz` contains exactly the ten successful
historical HTTP response bodies required by `verify_capture.py`, byte for byte.
They were collected during the original 2026-10-19 UTC audit, not refetched
for this follow-up. There are no credentials, headers, scripts, symlinks,
static-format downloads or unrelated datasets in the archive.

- Compressed size: **2,604,349 bytes**.
- Uncompressed response bodies: **7,674,177 bytes**.
- Archive SHA-256:
  `18fcd8e7ca75cea5fcc2690a262214bbeff908e0a0c139b15ed369ae351bcc4a`.
- The adjacent `.sha256` file identifies the same compressed bytes.
- `requests.json` retains each member's original URL, UTC retrieval time,
  byte length and SHA-256. The archive contains only regular files with
  these names: `geojson`, `count`, `count-final`, `ids`, `ids-after`,
  `page-0`, `page-25`, `ids-chunk-50`, `page-75`, `page-100-retry`.

The Git commit pins the archive and manifest. No NOAA connection is needed
to reproduce the audit. The failed responses and other original requests
remain described in `requests.json`; this archive intentionally contains
only the successful responses needed by this pilot's offline verifier.

Run the following from the repository root using its already provisioned uv
environment. It verifies the compressed checksum, rejects unexpected names,
duplicates and non-regular members, checks each member against the historical
manifest, and writes only verified bytes into a fresh temporary directory.
It deliberately does not call unrestricted `tar.extractall`.

```sh
capture_dir="$(mktemp -d "${TMPDIR:-/tmp}/noaa-12nm-replay.XXXXXX")"
python - "$capture_dir" <<'PY'
import hashlib
import json
import sys
import tarfile
from pathlib import Path

evidence = Path('docs/dataset-sources/12nm-territorial-sea/evidence')
archive = evidence / 'historical-captures-20261019.tar.gz'
expected_sha256 = '18fcd8e7ca75cea5fcc2690a262214bbeff908e0a0c139b15ed369ae351bcc4a'
if hashlib.sha256(archive.read_bytes()).hexdigest() != expected_sha256:
    raise SystemExit('Archive checksum mismatch')
expected = {'geojson', 'count', 'count-final', 'ids', 'ids-after',
            'page-0', 'page-25', 'ids-chunk-50', 'page-75', 'page-100-retry'}
manifest = {item['key']: item for item in json.loads((evidence / 'requests.json').read_text())}
destination = Path(sys.argv[1])
if not destination.is_dir() or any(destination.iterdir()):
    raise SystemExit('Destination must be a fresh empty directory')
with tarfile.open(archive, 'r:gz') as source:
    members = source.getmembers()
    if len(members) != 10 or {member.name for member in members} != expected:
        raise SystemExit('Unexpected or duplicate archive member')
    verified = {}
    for member in members:
        record = manifest[member.name]
        if not member.isfile() or member.size != record['bytes']:
            raise SystemExit('Member type or size mismatch')
        body = source.extractfile(member).read()
        if hashlib.sha256(body).hexdigest() != record['sha256']:
            raise SystemExit('Historical response checksum mismatch')
        verified[member.name] = body
    for name, body in verified.items():
        (destination / name).write_bytes(body)
print('Verified and restored all ten historical response bodies')
PY
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen --no-sync python docs/dataset-sources/12nm-territorial-sea/verify_capture.py "$capture_dir" > "$capture_dir/audit.json"
cmp docs/dataset-sources/12nm-territorial-sea/evidence/extraction-audit.json "$capture_dir/audit.json"
```

Expected result: ten verified response bodies; the existing audit reports
87 features, matching IDs, full attributes/coordinates, region counts and
geometry checks; `cmp` exits zero. Keep the temporary directory if examining
the capture, or delete that one directory after review. The replay writes no
application staging, published data or production services.

This preserves the original audit's limitations, including failed final ID
rechecks and the absence of an atomic source snapshot. It does not resolve
the unavailable crosswalk or published-baseline bytes, prove freshness beyond
the advertised 2013 revision, or approve an ingestion contract.
