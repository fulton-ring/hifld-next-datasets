"""Verify complete private issue114 artifacts; optionally extract to a fresh directory.

No network, dataset acceptance, overwrites or production writes. The trusted
small Git descriptor binds the private bundle AND its full member manifest.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

DESCRIPTOR = Path(__file__).resolve().parents[2] / 'docs/dataset-sources/historical-tsunami-observations/artifact-manifest.json'


def file_pin(path):
    h = hashlib.sha256()
    size = 0
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            size += len(block)
            h.update(block)
    return {'bytes': size, 'sha256': h.hexdigest()}


def safe_name(name):
    p = PurePosixPath(name)
    if not name or p.is_absolute() or '..' in p.parts or '\\' in name or str(p) != name:
        raise ValueError('Unsafe artifact member name')
    return p


def verify(bundle, manifest_path, descriptor):
    if file_pin(bundle) != {k: descriptor['bundle'][k] for k in ('bytes', 'sha256')}:
        raise ValueError('Bundle pin mismatch')
    if file_pin(manifest_path) != {k: descriptor['member_manifest'][k] for k in ('bytes', 'sha256')}:
        raise ValueError('Member manifest pin mismatch')
    manifest = json.loads(manifest_path.read_text())
    expected = manifest['members']
    if len(expected) != descriptor['member_count']:
        raise ValueError('Member count mismatch')
    seen = set()
    with tarfile.open(bundle, 'r:gz') as archive:
        for member in archive:
            safe_name(member.name)
            if not member.isfile() or member.name in seen or member.name not in expected:
                raise ValueError('Unexpected/duplicate/nonregular archive member')
            seen.add(member.name)
            pin = expected[member.name]
            if member.size != pin['bytes']:
                raise ValueError('Member length mismatch')
            h = hashlib.sha256()
            nested = manifest['nested_ZIP_members'].get(member.name)
            with tempfile.TemporaryFile() as spool, archive.extractfile(member) as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    h.update(block)
                    if nested is not None:
                        spool.write(block)
                if nested is not None:
                    spool.seek(0)
                    with zipfile.ZipFile(spool) as zipped:
                        infos = zipped.infolist()
                        if len(infos) != len(nested) or {i.filename for i in infos} != set(nested):
                            raise ValueError('Nested ZIP roster mismatch')
                        for info in infos:
                            safe_name(info.filename.rstrip('/'))
                            nh = hashlib.sha256()
                            with zipped.open(info) as original:
                                for block in iter(lambda: original.read(1024 * 1024), b''):
                                    nh.update(block)
                            np = nested[info.filename]
                            if info.file_size != np['bytes'] or nh.hexdigest() != np['sha256'] or f'{info.CRC:08x}' != np['zip_CRC32']:
                                raise ValueError('Nested ZIP pin mismatch')
            if h.hexdigest() != pin['sha256']:
                raise ValueError('Member checksum mismatch')
    if seen != set(expected):
        raise ValueError('Missing archive member')
    return manifest


def extract(bundle, manifest, destination, captures_only):
    if destination.exists() or destination.is_symlink():
        raise ValueError('Extraction requires a new destination')
    parent = destination.parent.resolve(strict=True)
    selected = {n for n in manifest['members'] if not captures_only or n in (
        'evidence/captures/source_http.zip', 'evidence/captures/authority_http.zip')}
    # No archive-controlled symlinks: verification rejects every nonregular entry.
    scratch = Path(tempfile.mkdtemp(prefix='.tsunami-replay-', dir=parent))
    try:
        with tarfile.open(bundle, 'r:gz') as archive:
            for member in archive:
                if member.name not in selected:
                    continue
                path = scratch.joinpath(*safe_name(member.name).parts)
                path.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, path.open('xb') as output:
                    shutil.copyfileobj(source, output, length=1024 * 1024)
                pin = manifest['members'][member.name]
                if file_pin(path) != {k: pin[k] for k in ('bytes', 'sha256')}:
                    raise ValueError('Extracted member checksum mismatch')
        if file_pin(bundle) != {k: manifest['bundle'][k] for k in ('bytes', 'sha256')}:
            raise ValueError('Bundle changed during extraction')
        scratch.rename(destination)
    except BaseException:
        shutil.rmtree(scratch)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--member-manifest', type=Path, required=True)
    parser.add_argument('--extract', type=Path)
    parser.add_argument('--captures-only', action='store_true')
    args = parser.parse_args()
    descriptor = json.loads(DESCRIPTOR.read_text())
    manifest = verify(args.bundle, args.member_manifest, descriptor)
    if args.extract:
        extract(args.bundle, manifest, args.extract, args.captures_only)
    print(json.dumps({'verified_outer_members': len(manifest['members']),
                      'bundle_sha256': descriptor['bundle']['sha256'],
                      'extracted': bool(args.extract), 'approved': False}))


if __name__ == '__main__':
    main()
