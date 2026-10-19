"""Faithful local draft of FWS CBRS Buffer Zone publication2024-12-19.

Not a definitive CBRS in/out determination. Published HIFLD candidate has a
different prohibition-area grain; no identity, ID, schema, or release approval.
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
from contextlib import contextmanager
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

SLUG = 'cbrs-buffer-zone-1'
URL = 'https://www.fws.gov/sites/default/files/documents/2024-12/20241219-cbrs-boundaries.zip'
LANDING = 'https://www.fws.gov/program/coastal-barrier-resources-act/maps-and-data'
DOWNLOAD = 'https://www.fws.gov/media/digital-coastal-barrier-resources-system-boundaries'
METADATA = 'https://www.fws.gov/sites/default/files/documents/2024-12/20241219-cbrs-buffer-zone-metadata.txt'
URLS = {'official_zip': URL, 'fws_data': LANDING, 'fws_download': DOWNLOAD, 'fws_buffer_metadata': METADATA}
ARCHIVE_NAME = '20241219-cbrs-boundaries.zip'
PIN = '8765dd5c6d8d0404d321b28af33236956de2daa8f43b81d61191f8f49eafabd0'
ARCHIVE_BYTES = 32280370
MAX_BYTES = 40_000_000
MAX_EXPANDED_BYTES = 60_000_000
STEM = '20241219_CBRS_Buffer_Zone'
MEMBERS = frozenset(['20241219_CBRS_Buffer_Zone.prj', '20241219_CBRS_Polygons.prj', '20241219_CBRS_Prohibitions.prj', '20241219_CBRS_Buffer_Zone.sbn', '20241219_CBRS_Polygons.sbn', '20241219_CBRS_Prohibitions.sbn', '20241219_CBRS_Buffer_Zone.sbx', '20241219_CBRS_Polygons.sbx', '20241219_CBRS_Prohibitions.sbx', '20241219_CBRS_Buffer_Zone.shp', '20241219_CBRS_Polygons.shp', '20241219_CBRS_Prohibitions.shp', '20241219_CBRS_Buffer_Zone.shx', '20241219_CBRS_Polygons.shx', '20241219_CBRS_Prohibitions.shx', '20241219-User-Guide-for-CBRS-data.pdf', '20241219_CBRS_Buffer_Zone.cpg', '20241219_CBRS_Polygons.cpg', '20241219_CBRS_Prohibitions.cpg', '20241219_CBRS_Buffer_Zone.dbf', '20241219_CBRS_Polygons.dbf', '20241219_CBRS_Prohibitions.dbf', '20241219_CBRS_Buffer_Zone.shp.xml', '20241219_CBRS_Polygons.shp.xml', '20241219_CBRS_Prohibitions.shp.xml'])
NATIVE_SCHEMA = {'BUFF_DIST':'float:19.11','Shape_Leng':'float:19.11','Shape_Area':'float:19.11'}
SCHEMA = {**NATIVE_SCHEMA, '__source_fid':'str:10'}
PROPOSAL = {'approved':False,'id_fields':['__source_fid'],'geometry':True,
 'coverage_fields':['BUFF_DIST'],'crs':'EPSG:4269',
 'columns':{name:{'type':'decimal' if kind.startswith('float') else 'string','nullable':False} for name,kind in SCHEMA.items()}}


class CBRSQuarantineError(ValueError):
    """Unrecognized/partial source retained outside the discoverable namespace."""


def encode(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def strict_json(body: bytes) -> dict:
    if len(body) > 128_000:
        raise CBRSQuarantineError('metadata_size_limit')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise CBRSQuarantineError('duplicate_json_member')
            result[key] = value
        return result
    result = json.loads(body, object_pairs_hook=pairs, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(result, dict) or 'error' in result:
        raise CBRSQuarantineError('nonobject_or_error_json')
    return result


def require_local(storage: StagingStorageResource) -> None:
    if isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise CBRSQuarantineError('draft_requires_explicit_isolated_local_staging')


def immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    before = storage.object_snapshot(key)
    if before is not None:
        if storage.read_key(key) != body or storage.object_snapshot(key) != before:
            raise CBRSQuarantineError(f'immutable_staging_conflict:{key}')
    else:
        storage.write_key_if_unchanged(key, body, None)


def quarantine(storage: StagingStorageResource, body: bytes, receipt: dict, reason: str) -> str:
    prefix = f'_quarantine/{SLUG}/{sha(body)}/{uuid4().hex}'
    immutable(storage, f'{prefix}/response.body', body)
    immutable(storage, f'{prefix}/receipt.json', encode(receipt))
    immutable(storage, f'{prefix}/report.json', encode({'approved': False, 'status': 'quarantined', 'reason': reason}))
    return prefix


def validate_receipt(body: bytes, receipt: dict, url: str) -> None:
    if (receipt.get('requested_url') != url or receipt.get('final_url') != url
            or type(receipt.get('status')) is not int or receipt['status'] != 200
            or type(receipt.get('byte_length')) is not int or receipt['byte_length'] != len(body)
            or receipt.get('sha256') != sha(body)):
        raise CBRSQuarantineError('capture_receipt_url_status_hash_length_mismatch')
    timestamp=receipt.get('observed_at')
    if not isinstance(timestamp,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)',timestamp):
        raise CBRSQuarantineError('capture_receipt_requires_strict_aware_iso_timestamp')
    moment = datetime.fromisoformat(timestamp)
    if moment.tzinfo is None:
        raise CBRSQuarantineError('capture_receipt_requires_timezone')


def acquire(storage: StagingStorageResource, capture_dir: Path | None) -> dict:
    require_local(storage)
    captures = {}
    for name,url in URLS.items():
        if capture_dir is not None:
            body = b''; receipt_body=b''; receipt = {'capture_path':str(capture_dir),'requested_url':url}
            try:
                with (capture_dir / (name + '.body')).open('rb') as f: body = f.read(MAX_BYTES + 1)
                receipt_body=(capture_dir / (name + '.receipt.json')).read_bytes()
                immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/receipts/{sha(receipt_body)}.body',receipt_body)
                receipt = strict_json(receipt_body)
                validate_receipt(body,receipt,url)
                if len(body)>MAX_BYTES: raise CBRSQuarantineError('capture_size_limit')
            except (OSError,ValueError,TypeError) as exc:
                location=quarantine(storage,body,receipt,str(exc))
                immutable(storage,f'{location}/original-receipt.body',receipt_body)
                raise CBRSQuarantineError(f'{exc}; retained:{location}') from exc
        else:
            for attempt in range(3):
                body=b'';receipt={'requested_url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
                try:
                    with httpx.Client(follow_redirects=False,timeout=45,headers={'Accept-Encoding':'identity'}) as client:
                        with client.stream('GET',url) as response:
                            receipt.update(final_url=str(response.url),status=response.status_code)
                            for block in response.iter_bytes(chunk_size=65536):
                                body+=block
                                if len(body)>MAX_BYTES: raise CBRSQuarantineError('response_size_limit')
                            receipt.update(byte_length=len(body),sha256=sha(body),received_wire_bytes=response.num_bytes_downloaded,content_encoding=response.headers.get('content-encoding'))
                            validate_receipt(body,receipt,url)
                            if response.headers.get('content-length') is not None and int(response.headers['content-length'])!=response.num_bytes_downloaded:
                                raise CBRSQuarantineError('incomplete_content_length')
                    break
                except (httpx.HTTPError,OSError,ValueError,TypeError) as exc:
                    receipt.update(byte_length=len(body),sha256=sha(body),partial=True)
                    location=quarantine(storage,body,receipt,str(exc))
                    if attempt==2: raise CBRSQuarantineError(f'bounded_fetch_exhausted:{exc}; retained:{location}') from exc
        captures[name]=(body,receipt)
        immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/{name}.body',body)
        immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/{sha(encode(receipt))}.receipt.json',encode(receipt))
    return captures


def provenance(captures: dict) -> dict:
    # Validate the complete original capture bundle before any canonical candidate writes.
    for name,url in URLS.items(): validate_receipt(*captures[name],url)
    if b'/sites/default/files/documents/2024-12/20241219-cbrs-boundaries.zip' not in captures['fws_download'][0]:
        raise CBRSQuarantineError('official_distribution_link_changed')
    if b'20241219' not in captures['fws_buffer_metadata'][0] or b'CBRS_Buffer_Zone' not in captures['fws_buffer_metadata'][0]:
        raise CBRSQuarantineError('official_metadata_identity_or_release_changed')
    inputs=[]
    for name,(body,receipt) in captures.items():
        inputs.append({'input_id':name+':'+sha(body),'producer':'U.S. Fish and Wildlife Service',
            'host':'www.fws.gov','role':'producer_publication',
            'canonical_url':URLS[name],'retrieval_url':receipt['requested_url'],'observed_final_url':receipt['final_url'],
            'retrieved_at':receipt['observed_at'],'release_date':'2024-12-19' if name in {'official_zip','fws_buffer_metadata'} else None,
            'release_date_unknown_reason':'Supporting official webpage has no dataset release date; fetch time is not release date.',
            'sha256':sha(body),'bytes':len(body),'license':METADATA,
            'license_scope':'Official metadata use constraints: acknowledge FWS, share derived products with FWS; no survey or definitive in/out determination. Redistribution acceptance remains pending.',
            'attribution':'U.S. Fish and Wildlife Service, CBRS Buffer Zone, publication2024-12-19.',
            'lineage':'Producer-hosted full distribution and official supporting pages; not a third-party archive or self-claimed delegation.',
            'authority_evidence':LANDING})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'CBRS Buffer Zone — faithful producer-source working draft',
        'publisher':'U.S. Fish and Wildlife Service','agency':'U.S. Fish and Wildlife Service',
        'source_url':LANDING,'license':METADATA,'date_issued':'2024-12-19','inputs':inputs,
        'source_anomalies':['Seven producer buffer polygons have BUFF_DIST=0.0 although metadata says all20feet. Values retained, no guessed correction; quality acceptance required.'],
        'license_review_required':'Preserve official use constraints; maintainer acceptance required before release.',
        'transformations':[{'operation':'decode_original_shapefile','preserved':'All three DBF columns and physical types, native NAD83 coordinates and original shapefile FID under proposed __source_fid. Complete ZIP and25members retained.',
            'repair_or_projection':False,'mapping_approval':'not established; published candidate is different CBRS Prohibitions grain'}],
        'unsupported_output_formats':{'shapefile_zip':'The full original producer archive is retained in raw/. Regenerated DBF loses the proposed explicit original source FID or renames it; no regenerated mapping approval.',
            'pmtiles':'Tile reprojection and quantization alter native geometry. No visualization mapping approved.'}}

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
        raise CBRSQuarantineError('changed_truncated_or_unknown_producer_archive')
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or set(names) != MEMBERS:
            raise CBRSQuarantineError('unexpected_missing_duplicate_archive_member')
        if any(PurePosixPath(i.filename).name != i.filename or i.flag_bits & 1
               or stat.S_ISLNK(i.external_attr >> 16) for i in archive.infolist()):
            raise CBRSQuarantineError('unsafe_encrypted_or_symlink_archive_member')
        if sum(i.file_size for i in archive.infolist()) > MAX_EXPANDED_BYTES:
            raise CBRSQuarantineError('expanded_archive_size_limit')
        if archive.testzip() is not None:
            raise CBRSQuarantineError('zip_crc_failure')
        members = {name: archive.read(name) for name in names}
    for name, data in members.items():
        directory.joinpath(name).write_bytes(data)
    with fiona.open(directory / f'{STEM}.shp') as source:
        schema, crs = dict(source.schema), source.crs
        rows = [{'type': 'Feature', 'properties': {**dict(r['properties']), '__source_fid':r.id},
                 'geometry': mapping(shape(r['geometry'])) if r['geometry'] else None} for r in source]
    if schema != {'properties': NATIVE_SCHEMA, 'geometry': 'Polygon'} or not pyproj.CRS(crs).equals(pyproj.CRS(4269)):
        raise CBRSQuarantineError('native_schema_or_crs_drift')
    if len(rows) != 1145 or struct.unpack_from('<I', members[f'{STEM}.dbf'], 4)[0] != len(rows):
        raise CBRSQuarantineError('incomplete_dbf_shp_population')
    ids = [r['properties']['__source_fid'] for r in rows]
    if set(ids) != {str(i) for i in range(1145)} or len(set(ids)) != len(rows):
        raise CBRSQuarantineError('original_fid_population_drift')
    rows.sort(key=lambda r: int(r['properties']['__source_fid']))
    for row in rows:
        if set(row['properties']) != set(SCHEMA):
            raise CBRSQuarantineError('unexpected_or_missing_source_columns')
        for name, kind in SCHEMA.items():
            value = row['properties'][name]
            expected = int if kind.startswith('int') else float if kind.startswith('float') else str
            if value is None:
                raise CBRSQuarantineError('source_null_drift')
            if type(value) is not expected:
                raise CBRSQuarantineError(f'source_type_or_null_drift:{name}')
            if isinstance(value, int) and not -(2**31) <= value < 2**31:
                raise CBRSQuarantineError(f'source_integer_width_drift:{name}')
        if row['geometry'] is None:
            raise CBRSQuarantineError('missing_native_geometry')
        g = shape(row['geometry'])
        if g.is_empty or g.geom_type != 'Polygon':
            raise CBRSQuarantineError('invalid_empty_or_changed_native_geometry')
    if collections.Counter(r['properties']['BUFF_DIST'] for r in rows) != {20.0:1138,0.0:7}:
        raise CBRSQuarantineError('buffer_width_coverage_drift')
    invalid = {r['properties']['__source_fid']: shapely.is_valid_reason(shape(r['geometry'])) for r in rows if not shape(r['geometry']).is_valid}
    if invalid: raise CBRSQuarantineError('invalid_native_geometry')
    audit = {'invalid_geometry_count': len(invalid), 'native_invalid_geometries': invalid, 'source_quality': 'blocked_known_seven_zero_width_buffer_attributes_preserved', 'zero_buffer_width_fids':[r['properties']['__source_fid'] for r in rows if r['properties']['BUFF_DIST']==0.0], 'approved': False, 'status': 'producer_source_fidelity_pass', 'feature_count': len(rows),
             'schema': schema, 'crs': 'EPSG:4269', 'bounds': shapely.total_bounds([shape(r['geometry']) for r in rows]).tolist(),
             'nulls': {name: sum(r['properties'][name] is None for r in rows) for name in SCHEMA},
             'identifiers': {'__source_fid': {'unique': len(set(ids)), 'nulls': 0}},
             'coverage': {name: dict(collections.Counter(r['properties'][name] for r in rows)) for name in ('BUFF_DIST',)},
             'members': {name: {'bytes': len(data), 'sha256': sha(data)} for name, data in sorted(members.items())}}
    return rows, audit, members


def compare_derivative(source: Path, candidate: Path, output: Path) -> dict:
    pin = sha256_file(source)
    return compare_full_rows(source, candidate, PROPOSAL, output, baseline_sha256=pin,
        candidate_sha256=sha256_file(candidate), baseline_reference={
            'role': 'unapproved_producer_source_fidelity_reference', 'approved': False,
            'source_archive_sha256': PIN, 'comparison_input': {'adapter': 'typed-geojson-feature-jsonl-v1',
                'sha256': pin, 'bytes': source.stat().st_size}})


_FIDELITY_PAYLOAD_CACHE: dict[tuple[str, str], dict[str, bytes]] = {}


def fidelity_payloads(records: bytes, fingerprint: str, *, completed: dict | None = None,
                      comparison_directory: Path | None = None) -> dict[str, bytes]:
    """Re-derive the complete report, both sidecars, wrappers and blocked placeholder.

    Cache only pure computed bytes in this process, keyed by freshly decoded source
    records and the effective installed processing identity. Ready receipts never
    supply this cache or authorize a source-owned report claim.
    """
    key=(sha(records),fingerprint)
    if key in _FIDELITY_PAYLOAD_CACHE:
        return dict(_FIDELITY_PAYLOAD_CACHE[key])
    if completed is None:
        with tempfile.TemporaryDirectory(prefix='cbrs-original-fidelity-') as tmp:
            directory=Path(tmp);left=directory/'original.jsonl';right=directory/'same-original.jsonl'
            left.write_bytes(records);right.write_bytes(records)
            report=compare_derivative(left,right,directory/'comparison')
            return fidelity_payloads(records,fingerprint,completed=report,comparison_directory=directory/'comparison')
    if comparison_directory is None or completed.get('status')!='pass':
        raise CBRSQuarantineError('fresh_original_fidelity_derivation_did_not_pass')
    parquet={'status':'pass','method':'complete_decoded_source_records_byte_equality_and_exact_arrow_schema',
        'rows':completed['populations']['candidate']['rows'],'source_records_sha256':sha(records),'shared_schema_geometry_validation_reference':completed}
    payloads={
        'metadata/source_fidelity/baseline_comparison.json':encode(completed),
        'metadata/source_fidelity/baseline_comparison.md':(comparison_directory/'baseline_comparison.md').read_bytes(),
        'metadata/source_fidelity/baseline_differences.jsonl':(comparison_directory/'baseline_differences.jsonl').read_bytes(),
        'metadata/source_fidelity/baseline_field_changes.jsonl':(comparison_directory/'baseline_field_changes.jsonl').read_bytes(),
        'metadata/geopackage_source_fidelity.json':encode({'shared_report_path':'source_fidelity/baseline_comparison.json','canonical_full_records_byte_identical':True,'shared_verifier':completed,'known_invalid_native_geometry_release_blocked':False}),
        'metadata/parquet_source_fidelity.json':encode({'shared_report_path':'source_fidelity/baseline_comparison.json','canonical_full_records_byte_identical':True,'shared_verifier':parquet,'known_invalid_native_geometry_release_blocked':False}),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'status':'blocked','approved':False,
            'reason_codes':['published_candidate_prohibitions_grain_differs_from_official_buffer_and_unapproved_fid_mapping'],'acceptance':'not_established'})}
    _FIDELITY_PAYLOAD_CACHE[key]=dict(payloads)
    return payloads


def validate_fidelity_payloads(root: Path, records: bytes, fingerprint: str) -> None:
    for name,expected in fidelity_payloads(records,fingerprint).items():
        if (root/name).read_bytes()!=expected:
            raise CBRSQuarantineError('cached_source_fidelity_evidence_disagrees_with_fresh_original:'+name)


def typed_table(rows: list[dict], audit: dict) -> pa.Table:
    schema=pa.schema([pa.field(k,pa.int32() if v.startswith('int32') else pa.float64() if v.startswith('float') else pa.string(),nullable=False) for k,v in SCHEMA.items()]+[pa.field('geometry',pa.binary(),nullable=False)])
    table=pa.Table.from_pylist([{**r['properties'],'geometry':shape(r['geometry']).wkb} for r in rows],schema=schema)
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Polygon'],'crs':pyproj.CRS(4269).to_json_dict(),'bbox':audit['bounds']}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def local_candidate_path(storage: StagingStorageResource, prefix: str, *, allow_version_link: bool = False) -> Path:
    require_local(storage)
    root=Path(storage.local_dir).resolve()
    key=storage._ensure_prefixed(prefix)
    if Path(key).is_absolute() or any(part in {'.','..'} for part in Path(key).parts):
        raise CBRSQuarantineError('unsafe_local_candidate_prefix')
    target=root/key
    current=root
    parts=target.relative_to(root).parts
    for index,part in enumerate(parts):
        current=current/part
        if current.is_symlink() and (not allow_version_link or index<len(parts)-1):
            raise CBRSQuarantineError('linked_candidate_path')
    return target


def write_native_geopackage(rows: list[dict], path: Path) -> None:
    """The original deterministic canonical writer, including SQLite metadata."""
    with fiona.open(path, 'w', driver='GPKG', layer=SLUG, crs='EPSG:4269',
                    schema={'properties': SCHEMA, 'geometry': 'Polygon'}) as destination:
        destination.writerecords(rows)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE gpkg_contents SET last_change='2024-12-19T00:00:00.000Z'")


def verify_canonical_geopackage(path: Path, rows: list[dict]) -> None:
    """Bind all immutable container bytes, including reader-invisible metadata."""
    with tempfile.TemporaryDirectory(prefix='cbrs-fresh-canonical-') as tmp:
        fresh=Path(tmp)/f'{SLUG}.gpkg'
        write_native_geopackage(rows,fresh)
        cached_gp=path
        if cached_gp.stat().st_size!=fresh.stat().st_size or sha256_file(cached_gp)!=sha256_file(fresh):
            raise CBRSQuarantineError('cached_geopackage_container_bytes_disagree_with_fresh_original')


@contextmanager
def candidate_lock(storage: StagingStorageResource, prefix: str):
    """Same permanent per-version lock used for lookup, invalidation and install."""
    target=local_candidate_path(storage,prefix,allow_version_link=True)
    target.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:raise CBRSQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX)
        yield target


def quarantine_rejected_cache(storage: StagingStorageResource, prefix: str, reason: str) -> str | None:
    with candidate_lock(storage,prefix):
        return quarantine_rejected_cache_held(storage,prefix,reason)


def quarantine_rejected_cache_held(storage: StagingStorageResource, prefix: str, reason: str) -> str | None:
    """Move the complete rejected version intact outside normal version discovery."""
    base=Path(storage.local_dir).resolve()
    target=base/storage._ensure_prefixed(prefix)
    # Check parents without following an alias. A linked version entry itself is
    # moved as an opaque entry, preserving it without reading its external target.
    current=base
    for part in target.relative_to(base).parts[:-1]:
        current=current/part
        if current.is_symlink():
            raise CBRSQuarantineError('cannot_quarantine_cache_through_linked_parent')
    if not target.exists() and not target.is_symlink():
        return None
    relative=f'_quarantine/{SLUG}/rejected-cache/{uuid4().hex}'
    location=base/relative
    current=base
    for part in Path(relative).parts:
        current=current/part
        if current.is_symlink():raise CBRSQuarantineError('linked_cache_quarantine_path')
    location.mkdir(parents=True,exist_ok=False)
    os.rename(target,location/target.name)
    immutable(storage,relative+'/report.json',encode({'approved':False,'status':'rejected_cache_quarantined',
        'original_prefix':prefix,'intact_version':target.name,'reason':reason}))
    return relative


def original_provenance_bundle(storage: StagingStorageResource, version: str,
                               captures: dict, manifest: dict, *, create: bool=False) -> tuple[dict,dict]:
    """Pin first construction outside the candidate, under the stable version lock.

    Later equal-body observations and docs-only commits cannot rewrite the original
    construction commit or receipts. An existing candidate must never seed a pin.
    """
    key=f'_source-provenance/{SLUG}/{version}/bundle.json'
    if not storage.object_exists(key):
        if not create:
            raise CBRSQuarantineError('missing_original_construction_provenance_bundle')
        bundle={'manifest':manifest,'captures':{name:{
            'body_key':f'_raw-captures/{SLUG}/{sha(body)}/{name}.body',
            'receipt':receipt} for name,(body,receipt) in captures.items()}}
        immutable(storage,key,encode(bundle))
    bundle=strict_json(storage.read_key(key))
    if set(bundle)!= {'manifest','captures'} or set(bundle['captures'])!=set(URLS):
        raise CBRSQuarantineError('original_construction_bundle_roster_drift')
    original={}
    for name in URLS:
        item=bundle['captures'][name]
        if set(item)!= {'body_key','receipt'}:
            raise CBRSQuarantineError('original_construction_capture_roster_drift')
        expected=f"_raw-captures/{SLUG}/{item['receipt']['sha256']}/{name}.body"
        if item['body_key']!=expected:
            raise CBRSQuarantineError('original_construction_capture_key_drift')
        body=storage.read_key(expected)
        if body!=captures[name][0]:
            raise CBRSQuarantineError('original_construction_capture_bytes_changed')
        original[name]=(body,item['receipt'])
    expected_manifest=provenance(original)
    expected_manifest['processing_fingerprint']=manifest['processing_fingerprint']
    actual=bundle['manifest']
    validate_release_provenance(actual)
    if set(actual)!=set(expected_manifest) or any(actual[k]!=v for k,v in expected_manifest.items() if k!='code_commit'):
        raise CBRSQuarantineError('original_construction_manifest_drift')
    return original,actual


def reconcile_cached_candidate(storage: StagingStorageResource,prefix: str,ready: dict,
                               rows: list[dict],audit: dict,captures: dict,identity: dict,members: dict) -> None:
    """Re-derive source-owned bytes/typed rows; a self-pinned ready receipt is no authority."""
    root=local_candidate_path(storage,prefix)
    # The normal graph adds only these derivative files. Source-owned directories remain exact.
    allowed_derivatives={f'geoparquet/{SLUG}.parquet','metadata/quality_manifest.json',
        'metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json',
        'metadata/source/data_dictionary.json','metadata/source/quality_manifest.json'}
    paths=list(root.rglob('*'))
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in paths):
        raise CBRSQuarantineError('linked_candidate_object')
    actual={str(p.relative_to(root)) for p in paths if p.is_file()}
    expected=set(ready['objects'])|{'metadata/cbrs_buffer_candidate_ready.json'}
    if not expected.issubset(actual) or actual-expected-allowed_derivatives:
        raise CBRSQuarantineError('existing_candidate_unrostered_or_missing_object')
    cached={name:((root/f'raw/captures/{name}.body').read_bytes(),strict_json((root/f'raw/captures/{name}.receipt.json').read_bytes())) for name in URLS}
    if cached!=captures:
        raise CBRSQuarantineError('cached_capture_differs_from_original_construction_bundle')
    manifest=strict_json((root/'metadata/source_manifest.json').read_bytes())
    validate_release_provenance(manifest)
    pinned=strict_json(storage.read_key(f'_source-provenance/{SLUG}/{ready["version"]}/bundle.json'))
    if manifest!=pinned['manifest']:
        raise CBRSQuarantineError('cached_original_construction_manifest_changed')
    snapshot=root/'metadata/source/source_manifest.json'
    if snapshot.exists():
        nested=strict_json(snapshot.read_bytes())
        validate_release_provenance(nested)
        if nested!=manifest:
            raise CBRSQuarantineError('cached_catalog_source_manifest_differs_from_authored_source')
    # Reader-selected CRS ignores contradictory auxiliary SRS tables, geometry
    # headers, envelopes and RTree metadata. Bind the entire immutable container
    # to an independently rebuilt original, not to the self-pinned ready receipt.
    verify_canonical_geopackage(root/'geopackage'/f'{SLUG}.gpkg',rows)
    expected_bytes={'raw/'+ARCHIVE_NAME:captures['official_zip'][0],
        'raw/retrieval.json':encode(cached['official_zip'][1]),
        'metadata/candidate.records.jsonl':b''.join(encode(r) for r in rows),
        'metadata/processing_identity.json':encode(identity),
        'metadata/cbrs_buffer_source_audit.json':encode(audit),
        'metadata/proposed_source_contract.json':encode(PROPOSAL)}
    expected_bytes.update({f'raw/members/{name}':body for name,body in members.items()})
    if any((root/name).read_bytes()!=body for name,body in expected_bytes.items()):
        raise CBRSQuarantineError('cached_source_or_normalized_records_differ')
    with fiona.open(root/'geopackage'/f'{SLUG}.gpkg') as stream:
        expected_schema={'properties':{k:('int32' if v.startswith('int32') else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()},'geometry':'Polygon'}
        if dict(stream.schema)!=expected_schema or not pyproj.CRS(stream.crs).equals(pyproj.CRS(4269)):
            raise CBRSQuarantineError('cached_geopackage_schema_or_crs_drift')
        decoded=[{'type':'Feature','properties':dict(r['properties']),'geometry':mapping(shape(r['geometry'])) if r['geometry'] else None} for r in stream]
    if b''.join(encode(r) for r in decoded)!=b''.join(encode(r) for r in rows):
        raise CBRSQuarantineError('cached_geopackage_source_fidelity_failure')
    if not pq.read_table(root/'candidate/typed-source.parquet').equals(typed_table(rows,audit),check_metadata=True):
        raise CBRSQuarantineError('cached_typed_parquet_source_fidelity_failure')
    validate_fidelity_payloads(root,b''.join(encode(r) for r in rows),sha(encode(identity)))


def commit_complete_candidate(storage: StagingStorageResource,prefix: str,payloads: dict,ready: dict) -> None:
    """Install one fully built local version atomically; partial builds are never discoverable."""
    target=local_candidate_path(storage,prefix)
    target.parent.mkdir(parents=True,exist_ok=True)
    # Cooperating refresh processes cannot replace an existing version or race its first commit.
    lock=target.parent/('.'+target.name+'.lock')
    descriptor=os.open(lock,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1: raise CBRSQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX)
        if target.exists() or target.is_symlink():
            raise CBRSQuarantineError('concurrent_or_incomplete_existing_candidate')
        with tempfile.TemporaryDirectory(prefix='.candidate-build-',dir=target.parent) as tmp:
            private=StagingStorageResource(local_dir=tmp,use_local=True)
            for name,value in payloads.items(): immutable(private,name,value)
            immutable(private,'metadata/cbrs_buffer_candidate_ready.json',encode(ready))
            os.rename(tmp,target)
    # Keep the tiny lock inode: unlinking permits a third process to bypass a waiter.



def stage_cbrs_buffer_candidate(storage: StagingStorageResource, *, capture_dir: Path | None = None) -> dict:
    require_local(storage)
    captures = acquire(storage,capture_dir)
    body,receipt = captures['official_zip']
    try:
        manifest=provenance(captures)
        with tempfile.TemporaryDirectory(prefix='cbrs_buffer-source-') as tmp:
            directory = Path(tmp)
            rows, audit, members = inspect_archive(body, directory)
            records = b''.join(encode(r) for r in rows)
            identity = processing_identity()
            fingerprint = sha(encode(identity))
            capture_fingerprint=sha(encode({name:sha(value[0]) for name,value in captures.items()}))
            version = f'v0.0.0-draft-cbrs20241219-{capture_fingerprint[:16]}-{fingerprint[:16]}'
            prefix = f'{SLUG}/{SLUG}/{version}'
            ready_key = f'{prefix}/metadata/cbrs_buffer_candidate_ready.json'
            manifest['processing_fingerprint']=fingerprint
            with candidate_lock(storage,prefix) as target:
                if target.exists() or target.is_symlink():
                    try:
                        if target.is_symlink():raise CBRSQuarantineError('linked_candidate_path')
                        if not storage.object_exists(ready_key):
                            raise CBRSQuarantineError('incomplete_existing_candidate_missing_ready_receipt')
                        captures,manifest=original_provenance_bundle(storage,version,captures,manifest)
                        ready = strict_json(storage.read_key(ready_key))
                        expected_names = {f'raw/{ARCHIVE_NAME}', 'raw/retrieval.json', f'geopackage/{SLUG}.gpkg', 'candidate/typed-source.parquet', 'metadata/candidate.records.jsonl', 'metadata/source_manifest.json', 'metadata/processing_identity.json', 'metadata/cbrs_buffer_source_audit.json', 'metadata/proposed_source_contract.json', 'metadata/geopackage_source_fidelity.json', 'metadata/parquet_source_fidelity.json', 'metadata/baseline_comparison.json'} | {'metadata/source_fidelity/baseline_comparison.json','metadata/source_fidelity/baseline_comparison.md','metadata/source_fidelity/baseline_differences.jsonl','metadata/source_fidelity/baseline_field_changes.jsonl'} | {f'raw/members/{name}' for name in MEMBERS} | {f'raw/captures/{name}{suffix}' for name in URLS for suffix in ('.body','.receipt.json')}
                        if set(ready.get('objects', {})) != expected_names or ready.get('version') != version or ready.get('status') != 'working_draft_candidate_ready':
                            raise CBRSQuarantineError('existing_candidate_inventory_conflict')
                        if (ready.get('source_sha256') != PIN or ready.get('candidate_records_sha256') != sha(records)
                                or ready.get('processing_fingerprint') != fingerprint or ready.get('approved') is not False):
                            raise CBRSQuarantineError('existing_candidate_identity_conflict')
                        for name, expected in ready['objects'].items():
                            key = f'{prefix}/{name}'
                            snapshot = storage.object_snapshot(key)
                            if snapshot is None or sha(storage.read_key(key)) != expected or storage.object_snapshot(key) != snapshot:
                                raise CBRSQuarantineError(f'existing_candidate_object_changed:{name}')
                        reconcile_cached_candidate(storage,prefix,ready,rows,audit,captures,identity,members)
                        return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version, 'feature_count': len(rows),
                                'status': 'working_draft_candidate_ready', 'processing_fingerprint': fingerprint}
                    except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error,fiona.errors.FionaError) as exc:
                        quarantine_rejected_cache_held(storage,prefix,str(exc))
                        raise
                captures,manifest=original_provenance_bundle(storage,version,captures,manifest,create=True)
                body,receipt=captures['official_zip']
            geopackage = directory / f'{SLUG}.gpkg'
            write_native_geopackage(rows,geopackage)
            source_records = directory / 'source.jsonl'
            source_records.write_bytes(records)
            decoded = directory / 'geopackage.jsonl'
            with fiona.open(geopackage) as source:
                if dict(source.schema) != {'properties': {k: ('int32' if v.startswith('int32') else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()}, 'geometry': 'Polygon'}:
                    raise CBRSQuarantineError(f'geopackage_physical_schema_loss:{source.schema}')
                if not pyproj.CRS(source.crs).equals(pyproj.CRS(4269)):
                    raise CBRSQuarantineError('geopackage_native_crs_loss')
                decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': dict(r['properties']),
                    'geometry': mapping(shape(r['geometry']))}) for r in source))
            fidelity = compare_derivative(source_records, decoded, directory / 'comparison')
            if decoded.read_bytes() != records:
                raise CBRSQuarantineError('geopackage_complete_fidelity_failure')
            table=typed_table(rows,audit)
            parquet = directory / 'typed-source.parquet'
            pq.write_table(table, parquet, compression='zstd')
            decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': {k:r[k] for k in SCHEMA},
                'geometry': mapping(shapely.from_wkb(r['geometry']))}) for r in pq.read_table(parquet).to_pylist()))
            if decoded.read_bytes() != records or not pq.read_schema(parquet).equals(table.schema, check_metadata=True):
                raise CBRSQuarantineError('typed_parquet_physical_or_full_row_fidelity_failure')
            evidence_payloads=fidelity_payloads(records,fingerprint,completed=fidelity,comparison_directory=directory/'comparison')
            validate_release_provenance(manifest)
            payloads = {f'raw/{ARCHIVE_NAME}': body, 'raw/retrieval.json': encode(receipt),
                f'geopackage/{SLUG}.gpkg': geopackage.read_bytes(), 'candidate/typed-source.parquet': parquet.read_bytes(),
                'metadata/candidate.records.jsonl': records, 'metadata/source_manifest.json': encode(manifest),
                'metadata/processing_identity.json': encode(identity), 'metadata/cbrs_buffer_source_audit.json': encode(audit),
                'metadata/proposed_source_contract.json': encode(PROPOSAL),
            }
            payloads.update(evidence_payloads)
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
        location = quarantine(storage, body, receipt, str(exc))
        raise CBRSQuarantineError(f'{exc}; retained:{location}') from exc
