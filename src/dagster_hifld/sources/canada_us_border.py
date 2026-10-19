"""Local working draft for the original IBC 1.3 boundary publication.

This reconstructs the producer's static 2018 release, not a legal boundary survey. Byte pins and proposed fidelity checks do not approve an HIFLD ID
mapping, published baseline, schema migration, or release.
"""
from __future__ import annotations

import collections
import hashlib
import importlib.metadata
import io
import fcntl
import os
import json
import platform
import re
import sqlite3
import stat
import struct
import subprocess
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

import fiona
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import mapping, shape

from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import compare_full_rows, sha256_file

SLUG = 'canada-and-us-border'
URL = 'https://www.internationalboundarycommission.org/uploads/shapefile/us-canada-boundary-v1-3.zip'
LANDING = 'https://www.internationalboundarycommission.org/en/maps-coordinates/coordinates.php'
TERMS = 'https://www.internationalboundarycommission.org/en/privacy.php'
ABOUT = 'https://www.internationalboundarycommission.org/en/about/commission.php'
URLS = {'ibc-zip': URL, 'ibc-digital': LANDING, 'ibc-terms': TERMS, 'ibc-about': ABOUT}
ARCHIVE_NAME = 'us-canada-boundary-v1-3.zip'
PIN = 'eb327459528b87cbc27e55ccc6bfd6982562c75559823a00b6dc50c04abcaab1'
ARCHIVE_BYTES = 258764
MAX_BYTES = 1000000
MAX_EXPANDED_BYTES = 1000000
STEM = 'US-Canada_Boundary_v1.3'
MEMBERS = frozenset(STEM + suffix for suffix in ('.sbn', '.sbx', '.shp', '.shx', '.dbf', '.prj', '.cpg')) | frozenset(country + '_Metadata_v1.3.' + ext for country in ('Canada','UnitedStates') for ext in ('html','xml'))
NATIVE_SCHEMA = {'SectionNum':'int32:5','SectionEng':'str:150','SectionFre':'str:150','MaxScale':'str:50','Shape_Leng':'float:19.11'}
SCHEMA = {**NATIVE_SCHEMA, '__source_fid':'str:10'}
PROPOSAL = {'approved':False,'id_fields':['SectionNum','__source_fid'],'geometry':True,
 'coverage_fields':['SectionNum','MaxScale'],'crs':'EPSG:4269',
 'columns':{name:{'type':'integer' if kind.startswith('int') else 'decimal' if kind.startswith('float') else 'string','nullable':False} for name,kind in SCHEMA.items()}}


class BoundaryQuarantineError(ValueError):
    """Unrecognized/partial source retained outside the discoverable namespace."""


def encode(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def strict_json(body: bytes) -> dict:
    if len(body) > 128_000:
        raise BoundaryQuarantineError('metadata_size_limit')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise BoundaryQuarantineError('duplicate_json_member')
            result[key] = value
        return result
    result = json.loads(body, object_pairs_hook=pairs, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(result, dict) or 'error' in result:
        raise BoundaryQuarantineError('nonobject_or_error_json')
    return result


def require_local(storage: StagingStorageResource) -> None:
    if isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise BoundaryQuarantineError('draft_requires_explicit_isolated_local_staging')


def immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    before = storage.object_snapshot(key)
    if before is not None:
        if storage.read_key(key) != body or storage.object_snapshot(key) != before:
            raise BoundaryQuarantineError(f'immutable_staging_conflict:{key}')
    else:
        storage.write_key_if_unchanged(key, body, None)


def quarantine(storage: StagingStorageResource, body: bytes, receipt: dict, reason: str) -> str:
    prefix = f'_quarantine/{SLUG}/{sha(body)}/{uuid4().hex}'
    immutable(storage, f'{prefix}/response.body', body)
    immutable(storage, f'{prefix}/receipt.json', encode(receipt))
    immutable(storage, f'{prefix}/report.json', encode({'approved': False, 'status': 'quarantined', 'reason': reason}))
    return prefix


def validate_receipt(body: bytes, receipt: dict, url: str) -> None:
    if (receipt.get('url') != url or receipt.get('final_url') != url
            or type(receipt.get('status')) is not int or receipt['status'] != 200
            or type(receipt.get('bytes')) is not int or receipt['bytes'] != len(body)
            or receipt.get('sha256') != sha(body)):
        raise BoundaryQuarantineError('capture_receipt_url_status_hash_length_mismatch')
    timestamp=receipt.get('observed_at')
    if not isinstance(timestamp,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)',timestamp):
        raise BoundaryQuarantineError('capture_receipt_requires_strict_aware_iso_timestamp')
    moment = datetime.fromisoformat(timestamp)
    if moment.tzinfo is None:
        raise BoundaryQuarantineError('capture_receipt_requires_timezone')


def acquire(storage: StagingStorageResource, capture_dir: Path | None) -> dict:
    require_local(storage)
    captures = {}
    for name,url in URLS.items():
        if capture_dir is not None:
            body = b''; receipt_body=b''; receipt = {'capture_path':str(capture_dir),'url':url}
            try:
                with (capture_dir / (name + '.body')).open('rb') as f: body = f.read(MAX_BYTES + 1)
                receipt_body=(capture_dir / (name + '.receipt.json')).read_bytes()
                immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/receipts/{sha(receipt_body)}.body',receipt_body)
                receipt = strict_json(receipt_body)
                validate_receipt(body,receipt,url)
                if len(body)>MAX_BYTES: raise BoundaryQuarantineError('capture_size_limit')
            except (OSError,ValueError,TypeError) as exc:
                location=quarantine(storage,body,receipt,str(exc))
                immutable(storage,f'{location}/original-receipt.body',receipt_body)
                raise BoundaryQuarantineError(f'{exc}; retained:{location}') from exc
        else:
            for attempt in range(3):
                body=b'';receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
                try:
                    with httpx.Client(follow_redirects=False,timeout=45) as client:
                        with client.stream('GET',url) as response:
                            receipt.update(final_url=str(response.url),status=response.status_code)
                            for block in response.iter_bytes(chunk_size=65536):
                                body+=block
                                if len(body)>MAX_BYTES: raise BoundaryQuarantineError('response_size_limit')
                            receipt.update(bytes=len(body),sha256=sha(body))
                            validate_receipt(body,receipt,url)
                            if response.headers.get('content-length') is not None and int(response.headers['content-length'])!=len(body):
                                raise BoundaryQuarantineError('incomplete_content_length')
                    break
                except (httpx.HTTPError,OSError,ValueError,TypeError) as exc:
                    receipt.update(bytes=len(body),sha256=sha(body),partial=True)
                    location=quarantine(storage,body,receipt,str(exc))
                    if attempt==2: raise BoundaryQuarantineError(f'bounded_fetch_exhausted:{exc}; retained:{location}') from exc
        captures[name]=(body,receipt)
        immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/{name}.body',body)
        immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/{sha(encode(receipt))}.receipt.json',encode(receipt))
    return captures


def provenance(captures: dict) -> dict:
    # Validate the complete original capture bundle before any canonical candidate writes.
    for name,url in URLS.items(): validate_receipt(*captures[name],url)
    if b'/uploads/shapefile/us-canada-boundary-v1-3.zip' not in captures['ibc-digital'][0]:
        raise BoundaryQuarantineError('official_distribution_link_changed')
    inputs=[]
    for name,(body,receipt) in captures.items():
        inputs.append({'input_id':name+':'+sha(body),'producer':'International Boundary Commission',
            'host':'www.internationalboundarycommission.org',
            'role':'producer_publication',
            'canonical_url':URLS[name],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],
            'retrieved_at':receipt['observed_at'],'release_date':'2018-04-20' if name=='ibc-zip' else None,
            'release_date_unknown_reason':'Supporting producer webpage has no source publication date; fetch time is not release date.',
            'sha256':sha(body),'bytes':len(body),'license':'https://open.canada.ca/en/open-government-licence-canada' if name=='ibc-zip' else TERMS,
            'license_scope':'Dataset-specific embedded OGLCanada; compatibility with generic website restriction remains under review.' if name=='ibc-zip' else 'Supporting website material subject to website terms; no blanket dataset-licence applicability inferred.',
            'attribution':'International Boundary Commission, Canada / United States of America Boundary Dataset, edition 1.3, 2018-04-20. Copy of official work; not produced in affiliation with or endorsed by the Commission.',
            'lineage':'Producer-hosted distribution and supporting producer pages; embedded dataset-specific licence retained alongside website terms. No release approval.',
            'authority_evidence':LANDING})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Canada / United States of America Boundary Dataset 1.3 — working draft',
        'publisher':'International Boundary Commission','agency':'International Boundary Commission',
        'source_url':LANDING,'license':'https://open.canada.ca/en/open-government-licence-canada',
        'date_issued':'2018-04-20','inputs':inputs,
        'license_review_required':'Embedded dataset metadata specifies Open Government Licence Canada; generic website terms restrict commercial redistribution. Preserve both; maintainer must resolve before release.',
        'transformations':[{'operation':'decode_original_shapefile','preserved':'all five producer columns and physical types, native NAD83 coordinates, original FID in explicit proposed __source_fid, full original ZIP and all11members','repair_or_projection':False,'mapping_approval':'not established'}],
        'unsupported_output_formats':{'shapefile_zip':'The complete original producer ZIP is retained under raw/. Regenerated DBF/metadata/index archive is not a byte-identical producer release and the extra source-FID mapping is unapproved.',
            'pmtiles':'Tile reprojection/quantization changes native coordinates and no visualization mapping has been approved.'}}
    validate_release_provenance(manifest)
    return manifest


def processing_identity() -> dict:
    """Installed source, lock/project, Python, Python distributions and native engines."""
    root = Path(__file__).parents[3]
    sources = sorted(root.joinpath('src/dagster_hifld').rglob('*.py'))
    return {'python': platform.python_version(), 'implementation': platform.python_implementation(),
            'platform': platform.platform(),
            'distributions': sorted((d.metadata['Name'], d.version) for d in importlib.metadata.distributions()),
            'gdal': fiona.__gdal_version__, 'geos': shapely.geos_version_string, 'proj': pyproj.proj_version_str,
            'files': {str(p.relative_to(root)): sha256_file(p) for p in sources + [root / 'uv.lock', root / 'pyproject.toml']},
            'verifier_sha256': verifier_implementation_sha256()}


def inspect_archive(body: bytes, directory: Path) -> tuple[list[dict], dict, dict[str, bytes]]:
    if len(body) != ARCHIVE_BYTES or sha(body) != PIN or not body.startswith(b'PK\x03\x04'):
        raise BoundaryQuarantineError('changed_truncated_or_unknown_producer_archive')
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or set(names) != MEMBERS:
            raise BoundaryQuarantineError('unexpected_missing_duplicate_archive_member')
        if any(PurePosixPath(i.filename).name != i.filename or i.flag_bits & 1
               or stat.S_ISLNK(i.external_attr >> 16) for i in archive.infolist()):
            raise BoundaryQuarantineError('unsafe_encrypted_or_symlink_archive_member')
        if sum(i.file_size for i in archive.infolist()) > MAX_EXPANDED_BYTES:
            raise BoundaryQuarantineError('expanded_archive_size_limit')
        if archive.testzip() is not None:
            raise BoundaryQuarantineError('zip_crc_failure')
        members = {name: archive.read(name) for name in names}
    for name, data in members.items():
        directory.joinpath(name).write_bytes(data)
    with fiona.open(directory / f'{STEM}.shp') as source:
        schema, crs = dict(source.schema), source.crs
        rows = [{'type': 'Feature', 'properties': {**dict(r['properties']), '__source_fid':r.id},
                 'geometry': mapping(shape(r['geometry'])) if r['geometry'] else None} for r in source]
    if schema != {'properties': NATIVE_SCHEMA, 'geometry': 'LineString'} or not pyproj.CRS(crs).equals(pyproj.CRS(4269)):
        raise BoundaryQuarantineError('native_schema_or_crs_drift')
    if len(rows) != 30 or struct.unpack_from('<I', members[f'{STEM}.dbf'], 4)[0] != len(rows):
        raise BoundaryQuarantineError('incomplete_dbf_shp_population')
    ids = [r['properties'].get('SectionNum') for r in rows]
    if any(type(v) is not int for v in ids) or len(set(ids)) != len(rows):
        raise BoundaryQuarantineError('missing_invalid_or_duplicate_source_identifier')
    if set(ids) != set(range(30)) or {r['properties']['__source_fid'] for r in rows} != {str(i) for i in range(30)}:
        raise BoundaryQuarantineError('section_or_original_fid_population_drift')
    rows.sort(key=lambda r: r['properties']['SectionNum'])
    for row in rows:
        if set(row['properties']) != set(SCHEMA):
            raise BoundaryQuarantineError('unexpected_or_missing_source_columns')
        for name, kind in SCHEMA.items():
            value = row['properties'][name]
            expected = int if kind.startswith('int') else float if kind.startswith('float') else str
            if value is None:
                raise BoundaryQuarantineError('source_null_drift')
            if type(value) is not expected:
                raise BoundaryQuarantineError(f'source_type_or_null_drift:{name}')
            if isinstance(value, int) and not -(2**31) <= value < 2**31:
                raise BoundaryQuarantineError(f'source_integer_width_drift:{name}')
        if row['geometry'] is None:
            raise BoundaryQuarantineError('missing_native_geometry')
        g = shape(row['geometry'])
        if g.is_empty or g.geom_type != 'LineString':
            raise BoundaryQuarantineError('invalid_empty_or_changed_native_geometry')
    invalid = {r['properties']['SectionNum']: shapely.is_valid_reason(shape(r['geometry'])) for r in rows if not shape(r['geometry']).is_valid}
    if invalid: raise BoundaryQuarantineError('invalid_native_geometry')
    audit = {'invalid_geometry_count': len(invalid), 'native_invalid_geometries': invalid, 'source_quality': 'valid_native_geometry_no_repair', 'approved': False, 'status': 'producer_source_fidelity_pass', 'feature_count': len(rows),
             'schema': schema, 'crs': 'EPSG:4269', 'bounds': shapely.total_bounds([shape(r['geometry']) for r in rows]).tolist(),
             'nulls': {name: sum(r['properties'][name] is None for r in rows) for name in SCHEMA},
             'identifiers': {'SectionNum': {'unique': len(set(ids)), 'nulls': 0}},
             'coverage': {name: dict(collections.Counter(r['properties'][name] for r in rows)) for name in ('SectionNum', 'MaxScale')},
             'members': {name: {'bytes': len(data), 'sha256': sha(data)} for name, data in sorted(members.items())}}
    return rows, audit, members


def compare_derivative(source: Path, candidate: Path, output: Path) -> dict:
    pin = sha256_file(source)
    return compare_full_rows(source, candidate, PROPOSAL, output, baseline_sha256=pin,
        candidate_sha256=sha256_file(candidate), baseline_reference={
            'role': 'unapproved_producer_source_fidelity_reference', 'approved': False,
            'source_archive_sha256': PIN, 'comparison_input': {'adapter': 'typed-geojson-feature-jsonl-v1',
                'sha256': pin, 'bytes': source.stat().st_size}})


def typed_table(rows: list[dict], audit: dict) -> pa.Table:
    schema=pa.schema([pa.field(k,pa.int32() if v.startswith('int32') else pa.float64() if v.startswith('float') else pa.string(),nullable=False) for k,v in SCHEMA.items()]+[pa.field('geometry',pa.binary(),nullable=False)])
    table=pa.Table.from_pylist([{**r['properties'],'geometry':shape(r['geometry']).wkb} for r in rows],schema=schema)
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['LineString'],'crs':pyproj.CRS(4269).to_json_dict(),'bbox':audit['bounds']}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def local_candidate_path(storage: StagingStorageResource, prefix: str) -> Path:
    require_local(storage)
    root=Path(storage.local_dir).resolve()
    key=storage._ensure_prefixed(prefix)
    if Path(key).is_absolute() or any(part in {'.','..'} for part in Path(key).parts):
        raise BoundaryQuarantineError('unsafe_local_candidate_prefix')
    target=root/key
    current=root
    for part in target.relative_to(root).parts:
        current=current/part
        if current.is_symlink(): raise BoundaryQuarantineError('linked_candidate_path')
    return target



def write_canonical_geopackage(destination: Path, rows: list[dict]) -> None:
    """Deterministic native container; no coordinate, ID, field or type changes."""
    with fiona.open(destination, 'w', driver='GPKG', layer=SLUG, crs='EPSG:4269',
                    schema={'properties': SCHEMA, 'geometry': 'LineString'}) as stream:
        stream.writerecords(rows)
    with sqlite3.connect(destination) as db:
        db.execute("UPDATE gpkg_contents SET last_change='2018-04-20T00:00:00.000Z'")


def reconcile_cached_candidate(storage: StagingStorageResource,prefix: str,ready: dict,
                               rows: list[dict],audit: dict,captures: dict,identity: dict,members: dict) -> None:
    """Re-derive source-owned bytes/typed rows; a self-pinned ready receipt is no authority."""
    root=local_candidate_path(storage,prefix)
    # The normal graph adds only these derivative files. Source-owned directories remain exact.
    manifest_body=(root/'metadata/source_manifest.json').read_bytes()
    authored_snapshot='metadata/source/version_'+sha(manifest_body)+'_source_manifest.json'
    allowed_derivatives={f'geoparquet/{SLUG}.parquet','metadata/quality_manifest.json',
        'metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/resolved_provenance.json',
        'metadata/source/source_manifest.json','metadata/source/data_dictionary.json',
        'metadata/source/quality_manifest.json',authored_snapshot}
    for name in ['metadata/source/source_manifest.json',authored_snapshot]:
        if (root/name).exists() and (root/name).read_bytes()!=manifest_body:
            raise BoundaryQuarantineError('cached_authored_manifest_snapshot_differs')
    paths=list(root.rglob('*'))
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in paths):
        raise BoundaryQuarantineError('linked_candidate_object')
    actual={str(p.relative_to(root)) for p in paths if p.is_file()}
    expected=set(ready['objects'])|{'metadata/canada_us_border_candidate_ready.json'}
    if not expected.issubset(actual) or actual-expected-allowed_derivatives:
        raise BoundaryQuarantineError('existing_candidate_unrostered_or_missing_object')
    cached={name:((root/f'raw/captures/{name}.body').read_bytes(),strict_json((root/f'raw/captures/{name}.receipt.json').read_bytes())) for name in URLS}
    expected_manifest=provenance(cached)
    if any(cached[name][0]!=captures[name][0] for name in URLS):
        raise BoundaryQuarantineError('cached_capture_differs_from_fresh_source')
    manifest=strict_json((root/'metadata/source_manifest.json').read_bytes())
    validate_release_provenance(manifest)
    # code_commit identifies the original immutable construction, including later docs-only commits.
    for name,value in expected_manifest.items():
        if name!='code_commit' and manifest.get(name)!=value:
            raise BoundaryQuarantineError('cached_provenance_differs_from_original_capture')
    expected_bytes={'raw/'+ARCHIVE_NAME:captures['ibc-zip'][0],
        'raw/retrieval.json':encode(cached['ibc-zip'][1]),
        'metadata/candidate.records.jsonl':b''.join(encode(r) for r in rows),
        'metadata/processing_identity.json':encode(identity),
        'metadata/canada_us_border_source_audit.json':encode(audit),
        'metadata/proposed_source_contract.json':encode(PROPOSAL)}
    expected_bytes.update({f'raw/members/{name}':body for name,body in members.items()})
    if any((root/name).read_bytes()!=body for name,body in expected_bytes.items()):
        raise BoundaryQuarantineError('cached_source_or_normalized_records_differ')
    with fiona.open(root/'geopackage'/f'{SLUG}.gpkg') as stream:
        expected_schema={'properties':{k:('int32' if v.startswith('int32') else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()},'geometry':'LineString'}
        if dict(stream.schema)!=expected_schema or not pyproj.CRS(stream.crs).equals(pyproj.CRS(4269)):
            raise BoundaryQuarantineError('cached_geopackage_schema_or_crs_drift')
        decoded=[{'type':'Feature','properties':dict(r['properties']),'geometry':mapping(shape(r['geometry'])) if r['geometry'] else None} for r in stream]
    if b''.join(encode(r) for r in decoded)!=b''.join(encode(r) for r in rows):
        raise BoundaryQuarantineError('cached_geopackage_source_fidelity_failure')
    # Fiona selects one CRS declaration and ignores contradictory auxiliary SRS
    # metadata. Exact fresh reconstruction also binds every header, index and table.
    with tempfile.TemporaryDirectory(prefix='ibc-cache-native-container-') as tmp:
        expected=Path(tmp)/'expected.gpkg'
        write_canonical_geopackage(expected,rows)
        if sha256_file(root/'geopackage'/f'{SLUG}.gpkg')!=sha256_file(expected):
            raise BoundaryQuarantineError('cached_geopackage_canonical_container_differs')
    if not pq.read_table(root/'candidate/typed-source.parquet').equals(typed_table(rows,audit),check_metadata=True):
        raise BoundaryQuarantineError('cached_typed_parquet_source_fidelity_failure')


def commit_complete_candidate(storage: StagingStorageResource,prefix: str,payloads: dict,ready: dict) -> None:
    """Install one fully built local version atomically; partial builds are never discoverable."""
    target=local_candidate_path(storage,prefix)
    target.parent.mkdir(parents=True,exist_ok=True)
    # Cooperating refresh processes cannot replace an existing version or race its first commit.
    lock=target.parent/('.'+target.name+'.lock')
    descriptor=os.open(lock,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1: raise BoundaryQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX)
        if target.exists() or target.is_symlink():
            raise BoundaryQuarantineError('concurrent_or_incomplete_existing_candidate')
        with tempfile.TemporaryDirectory(prefix='.candidate-build-',dir=target.parent) as tmp:
            private=StagingStorageResource(local_dir=tmp,use_local=True)
            for name,value in payloads.items(): immutable(private,name,value)
            immutable(private,'metadata/canada_us_border_candidate_ready.json',encode(ready))
            os.rename(tmp,target)
    # Keep the tiny lock inode: unlinking permits a third process to bypass a waiter.




def quarantine_cached_candidate(storage: StagingStorageResource, prefix: str, reason: str) -> str | None:
    """Move a rejected local draft intact outside version discovery."""
    target=local_candidate_path(storage,prefix)
    lock=target.parent/('.'+target.name+'.lock')
    descriptor=os.open(lock,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:
            raise BoundaryQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX)
        if not target.exists():
            return None
        destination=Path(storage.local_dir)/'_quarantine'/SLUG/('invalid-cache-'+uuid4().hex)
        destination.mkdir(parents=True)
        os.rename(target,destination/'candidate')
        (destination/'report.json').write_bytes(encode({'approved':False,
            'status':'quarantined_invalid_draft_cache','original_prefix':prefix,'reason':reason}))
        return str(destination)


def stage_canada_us_border_candidate(storage: StagingStorageResource, *, capture_dir: Path | None = None) -> dict:
    require_local(storage)
    captures = acquire(storage,capture_dir)
    body,receipt = captures['ibc-zip']
    existing_prefix=None
    try:
        manifest=provenance(captures)
        with tempfile.TemporaryDirectory(prefix='canada_us_border-source-') as tmp:
            directory = Path(tmp)
            rows, audit, members = inspect_archive(body, directory)
            records = b''.join(encode(r) for r in rows)
            identity = processing_identity()
            fingerprint = sha(encode(identity))
            capture_fingerprint=sha(encode({name:sha(value[0]) for name,value in captures.items()}))
            version = f'v0.0.0-draft-ibc13-{capture_fingerprint[:16]}-{fingerprint[:16]}'
            prefix = f'{SLUG}/{SLUG}/{version}'
            ready_key = f'{prefix}/metadata/canada_us_border_candidate_ready.json'
            if storage.object_exists(ready_key):
                existing_prefix=prefix
                ready = strict_json(storage.read_key(ready_key))
                expected_names = {f'raw/{ARCHIVE_NAME}', 'raw/retrieval.json', f'geopackage/{SLUG}.gpkg', 'candidate/typed-source.parquet', 'metadata/candidate.records.jsonl', 'metadata/source_manifest.json', 'metadata/processing_identity.json', 'metadata/canada_us_border_source_audit.json', 'metadata/proposed_source_contract.json', 'metadata/geopackage_source_fidelity.json', 'metadata/parquet_source_fidelity.json', 'metadata/baseline_comparison.json'} | {f'raw/members/{name}' for name in MEMBERS} | {f'raw/captures/{name}{suffix}' for name in URLS for suffix in ('.body','.receipt.json')}
                if set(ready.get('objects', {})) != expected_names or ready.get('version') != version or ready.get('status') != 'working_draft_candidate_ready':
                    raise BoundaryQuarantineError('existing_candidate_inventory_conflict')
                if (ready.get('source_sha256') != PIN or ready.get('candidate_records_sha256') != sha(records)
                        or ready.get('processing_fingerprint') != fingerprint or ready.get('approved') is not False):
                    raise BoundaryQuarantineError('existing_candidate_identity_conflict')
                for name, expected in ready['objects'].items():
                    key = f'{prefix}/{name}'
                    snapshot = storage.object_snapshot(key)
                    if snapshot is None or sha(storage.read_key(key)) != expected or storage.object_snapshot(key) != snapshot:
                        raise BoundaryQuarantineError(f'existing_candidate_object_changed:{name}')
                reconcile_cached_candidate(storage,prefix,ready,rows,audit,captures,identity,members)
                return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version, 'feature_count': len(rows),
                        'status': 'working_draft_candidate_ready', 'processing_fingerprint': fingerprint}
            geopackage = directory / f'{SLUG}.gpkg'
            write_canonical_geopackage(geopackage,rows)
            source_records = directory / 'source.jsonl'
            source_records.write_bytes(records)
            decoded = directory / 'geopackage.jsonl'
            with fiona.open(geopackage) as source:
                if dict(source.schema) != {'properties': {k: ('int32' if v.startswith('int32') else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()}, 'geometry': 'LineString'}:
                    raise BoundaryQuarantineError(f'geopackage_physical_schema_loss:{source.schema}')
                if not pyproj.CRS(source.crs).equals(pyproj.CRS(4269)):
                    raise BoundaryQuarantineError('geopackage_native_crs_loss')
                decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': dict(r['properties']),
                    'geometry': mapping(shape(r['geometry']))}) for r in source))
            fidelity = compare_derivative(source_records, decoded, directory / 'comparison')
            if decoded.read_bytes() != records:
                raise BoundaryQuarantineError('geopackage_complete_fidelity_failure')
            table=typed_table(rows,audit)
            parquet = directory / 'typed-source.parquet'
            pq.write_table(table, parquet, compression='zstd')
            decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': {k:r[k] for k in SCHEMA},
                'geometry': mapping(shapely.from_wkb(r['geometry']))}) for r in pq.read_table(parquet).to_pylist()))
            parquet_fidelity = compare_derivative(source_records, decoded, directory / 'parquet-comparison')
            if decoded.read_bytes() != records or not pq.read_schema(parquet).equals(table.schema, check_metadata=True):
                raise BoundaryQuarantineError('typed_parquet_physical_or_full_row_fidelity_failure')
            commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=Path(__file__).parents[3], text=True).strip()
            manifest.update(code_commit=commit,processing_fingerprint=fingerprint)
            validate_release_provenance(manifest)
            payloads = {f'raw/{ARCHIVE_NAME}': body, 'raw/retrieval.json': encode(receipt),
                f'geopackage/{SLUG}.gpkg': geopackage.read_bytes(), 'candidate/typed-source.parquet': parquet.read_bytes(),
                'metadata/candidate.records.jsonl': records, 'metadata/source_manifest.json': encode(manifest),
                'metadata/processing_identity.json': encode(identity), 'metadata/canada_us_border_source_audit.json': encode(audit),
                'metadata/proposed_source_contract.json': encode(PROPOSAL),
                'metadata/geopackage_source_fidelity.json': encode({'canonical_full_records_byte_identical': True, 'shared_verifier': fidelity, 'known_invalid_native_geometry_release_blocked': False}), 'metadata/parquet_source_fidelity.json': encode({'canonical_full_records_byte_identical': True, 'shared_verifier': parquet_fidelity, 'known_invalid_native_geometry_release_blocked': False}),
                'metadata/baseline_comparison.json': encode({'schema_version':1, 'status':'blocked', 'approved':False,
                    'reason_codes':['unapproved_baseline_and_source_published_id_schema_crs_mapping'], 'acceptance':'not_established'})}
            payloads.update({f'raw/members/{name}': value for name,value in members.items()})
            payloads.update({f'raw/captures/{name}.body':value[0] for name,value in captures.items()})
            payloads.update({f'raw/captures/{name}.receipt.json':encode(value[1]) for name,value in captures.items()})
            ready = {'status':'working_draft_candidate_ready', 'approved':False, 'source_sha256':PIN,
                'candidate_records_sha256':sha(records), 'processing_fingerprint':fingerprint, 'version':version,
                'objects':{name:sha(value) for name,value in payloads.items()}}
            commit_complete_candidate(storage,prefix,payloads,ready)
            return {'dataset_slug':SLUG, 'file_slug':SLUG, 'version':version, 'feature_count':len(rows),
                    'status':ready['status'], 'processing_fingerprint':fingerprint}
    except (ValueError, TypeError, KeyError, OSError, RuntimeError, zipfile.BadZipFile, sqlite3.Error, fiona.errors.FionaError) as exc:
        retained=quarantine_cached_candidate(storage,existing_prefix,str(exc)) if existing_prefix else None
        location = quarantine(storage, body, receipt, str(exc))
        raise BoundaryQuarantineError(f'{exc}; retained:{location}; cached_candidate_retained:{retained}') from exc
