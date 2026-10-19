"""Faithful local draft of FWS CBRS Prohibitions publication2024-12-19.

Not a definitive CBRS in/out determination. Native DBF dates and mixed geometry
are preserved; no source, baseline, ID, schema mapping, or release approval.
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
from contextlib import contextmanager
from datetime import date, datetime, timezone
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
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import compare_full_rows, sha256_file

SLUG = 'cbrs-prohibitions-1'
URL = 'https://www.fws.gov/sites/default/files/documents/2024-12/20241219-cbrs-boundaries.zip'
LANDING = 'https://www.fws.gov/program/coastal-barrier-resources-act/maps-and-data'
DOWNLOAD = 'https://www.fws.gov/media/digital-coastal-barrier-resources-system-boundaries'
METADATA = 'https://www.fws.gov/sites/default/files/documents/2024-12/20241219-cbrs-prohibitions-metadata.txt'
URLS = {'official_zip': URL, 'fws_data': LANDING, 'fws_download': DOWNLOAD, 'fws_prohibitions_metadata': METADATA}
FIDELITY_FILES = ('baseline_comparison.json','baseline_comparison.md','baseline_differences.jsonl','baseline_field_changes.jsonl')
ARCHIVE_NAME = '20241219-cbrs-boundaries.zip'
PIN = '8765dd5c6d8d0404d321b28af33236956de2daa8f43b81d61191f8f49eafabd0'
ARCHIVE_BYTES = 32280370
MAX_BYTES = 40_000_000
MAX_EXPANDED_BYTES = 60_000_000
STEM = '20241219_CBRS_Prohibitions'
MEMBERS = frozenset(['20241219_CBRS_Buffer_Zone.prj', '20241219_CBRS_Polygons.prj', '20241219_CBRS_Prohibitions.prj', '20241219_CBRS_Buffer_Zone.sbn', '20241219_CBRS_Polygons.sbn', '20241219_CBRS_Prohibitions.sbn', '20241219_CBRS_Buffer_Zone.sbx', '20241219_CBRS_Polygons.sbx', '20241219_CBRS_Prohibitions.sbx', '20241219_CBRS_Buffer_Zone.shp', '20241219_CBRS_Polygons.shp', '20241219_CBRS_Prohibitions.shp', '20241219_CBRS_Buffer_Zone.shx', '20241219_CBRS_Polygons.shx', '20241219_CBRS_Prohibitions.shx', '20241219-User-Guide-for-CBRS-data.pdf', '20241219_CBRS_Buffer_Zone.cpg', '20241219_CBRS_Polygons.cpg', '20241219_CBRS_Prohibitions.cpg', '20241219_CBRS_Buffer_Zone.dbf', '20241219_CBRS_Polygons.dbf', '20241219_CBRS_Prohibitions.dbf', '20241219_CBRS_Buffer_Zone.shp.xml', '20241219_CBRS_Polygons.shp.xml', '20241219_CBRS_Prohibitions.shp.xml'])
NATIVE_SCHEMA = {'Unit':'str:10','CBRS_Type':'str:30','FI_Date':'date','SU_Date':'date','SHAPE_Leng':'float:19.11','SHAPE_Area':'float:19.11'}
SCHEMA = {**NATIVE_SCHEMA, '__source_fid':'str:10'}
PROPOSAL = {'approved':False,'id_fields':['__source_fid'],'geometry':True,
 'coverage_fields':['CBRS_Type'],'crs':'EPSG:4269',
 'columns':{name:{'type':'decimal' if kind.startswith('float') else 'string','nullable':name=='SU_Date'} for name,kind in SCHEMA.items()}}


class CBRSQuarantineError(ValueError):
    """Unrecognized/partial source retained outside the discoverable namespace."""


class _CachedCandidateQuarantined(CBRSQuarantineError):
    """Complete invalid cache is already retained outside discovery."""


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
                body=b'';response=None;receipt={'requested_url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
                try:
                    with httpx.Client(headers={'Accept-Encoding':'identity'},follow_redirects=False,timeout=45) as client:
                        with client.stream('GET',url) as response:
                            receipt.update(final_url=str(response.url),status=response.status_code)
                            for block in response.iter_bytes():
                                body+=block[:MAX_BYTES+1-len(body)]
                                if len(body)>MAX_BYTES: raise CBRSQuarantineError('response_size_limit')
                            receipt.update(byte_length=len(body),sha256=sha(body),wire_byte_length=response.num_bytes_downloaded,content_encoding=response.headers.get('content-encoding','identity'),content_length_header=response.headers.get('content-length'))
                            validate_receipt(body,receipt,url)
                            if response.headers.get('content-length') is not None and int(response.headers['content-length'])!=response.num_bytes_downloaded:
                                raise CBRSQuarantineError('incomplete_content_length')
                    break
                except (httpx.HTTPError,OSError,ValueError,TypeError) as exc:
                    receipt.update(byte_length=len(body),sha256=sha(body),partial=True)
                    if response is not None:
                        receipt.update(wire_byte_length=getattr(response,'num_bytes_downloaded',None),content_encoding=response.headers.get('content-encoding','identity'),content_length_header=response.headers.get('content-length'))
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
    if b'20241219' not in captures['fws_prohibitions_metadata'][0] or b'CBRS_Prohibitions' not in captures['fws_prohibitions_metadata'][0]:
        raise CBRSQuarantineError('official_metadata_identity_or_release_changed')
    inputs=[]
    for name,(body,receipt) in captures.items():
        inputs.append({'input_id':name+':'+sha(body),'producer':'U.S. Fish and Wildlife Service',
            'host':'www.fws.gov','role':'producer_publication',
            'canonical_url':URLS[name],'retrieval_url':receipt['requested_url'],'observed_final_url':receipt['final_url'],
            'retrieved_at':receipt['observed_at'],'release_date':'2024-12-19' if name in {'official_zip','fws_prohibitions_metadata'} else None,
            'release_date_unknown_reason':'Supporting official webpage has no dataset release date; fetch time is not release date.',
            'sha256':sha(body),'bytes':len(body),'license':METADATA,
            'license_scope':'Official metadata use constraints: acknowledge FWS, share derived products with FWS; no survey or definitive in/out determination. Redistribution acceptance remains pending.',
            'attribution':'U.S. Fish and Wildlife Service, CBRS Prohibitions, publication2024-12-19.',
            'lineage':'Producer-hosted full distribution and official supporting pages; not a third-party archive or self-claimed delegation.',
            'authority_evidence':LANDING})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'CBRS Prohibitions — faithful producer-source working draft',
        'publisher':'U.S. Fish and Wildlife Service','agency':'U.S. Fish and Wildlife Service',
        'source_url':LANDING,'license':METADATA,'date_issued':'2024-12-19','inputs':inputs,
        'source_anomalies':[],
        'license_review_required':'Preserve official use constraints; maintainer acceptance required before release.',
        'transformations':[{'operation':'decode_original_shapefile','preserved':'All six DBF columns and physical types (including native date fields), native NAD83 coordinates and original shapefile FID under proposed __source_fid. Complete ZIP and25members retained.',
            'repair_or_projection':False,'mapping_approval':'not established; full published candidate compatibility and original FID/OBJECTID/id crosswalk remain unapproved'}],
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
    if len(rows) != 1693 or struct.unpack_from('<I', members[f'{STEM}.dbf'], 4)[0] != len(rows):
        raise CBRSQuarantineError('incomplete_dbf_shp_population')
    ids = [r['properties']['__source_fid'] for r in rows]
    if set(ids) != {str(i) for i in range(1693)} or len(set(ids)) != len(rows):
        raise CBRSQuarantineError('original_fid_population_drift')
    rows.sort(key=lambda r: int(r['properties']['__source_fid']))
    for row in rows:
        if set(row['properties']) != set(SCHEMA):
            raise CBRSQuarantineError('unexpected_or_missing_source_columns')
        for name, kind in SCHEMA.items():
            value = row['properties'][name]
            expected = int if kind.startswith('int') else float if kind.startswith('float') else str
            if value is None:
                if name=='SU_Date': continue
                raise CBRSQuarantineError('source_null_drift')
            if type(value) is not expected:
                raise CBRSQuarantineError(f'source_type_or_null_drift:{name}')
            if isinstance(value, int) and not -(2**31) <= value < 2**31:
                raise CBRSQuarantineError(f'source_integer_width_drift:{name}')
        if any(row['properties'][n] is not None and date.fromisoformat(row['properties'][n]).isoformat()!=row['properties'][n] for n in ('FI_Date','SU_Date')):
            raise CBRSQuarantineError('native_date_value_drift')
        if row['geometry'] is None:
            raise CBRSQuarantineError('missing_native_geometry')
        g = shape(row['geometry'])
        if g.is_empty or g.geom_type not in {'Polygon','MultiPolygon'}:
            raise CBRSQuarantineError('invalid_empty_or_changed_native_geometry')
    if collections.Counter(r['properties']['CBRS_Type'] for r in rows) != {'System Unit':1129,'Otherwise Protected Area':564}:
        raise CBRSQuarantineError('prohibition_type_coverage_drift')
    if sum(r['properties']['SU_Date'] is None for r in rows)!=564:
        raise CBRSQuarantineError('native_date_null_coverage_drift')
    invalid = {r['properties']['__source_fid']: shapely.is_valid_reason(shape(r['geometry'])) for r in rows if not shape(r['geometry']).is_valid}
    if invalid: raise CBRSQuarantineError('invalid_native_geometry')
    audit = {'invalid_geometry_count': len(invalid), 'native_invalid_geometries': invalid, 'source_quality': 'native_fidelity_pass_unapproved_release', 'approved': False, 'status': 'producer_source_fidelity_pass', 'feature_count': len(rows),
             'schema': schema, 'crs': 'EPSG:4269', 'bounds': shapely.total_bounds([shape(r['geometry']) for r in rows]).tolist(),
             'nulls': {name: sum(r['properties'][name] is None for r in rows) for name in SCHEMA},
             'identifiers': {'__source_fid': {'unique': len(set(ids)), 'nulls': 0}},
             'coverage': {name: dict(collections.Counter(r['properties'][name] for r in rows)) for name in ('CBRS_Type',)},
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
    schema=pa.schema([pa.field(k,pa.int32() if v.startswith('int32') else pa.float64() if v.startswith('float') else pa.date32() if v=='date' else pa.string(),nullable=k=='SU_Date') for k,v in SCHEMA.items()]+[pa.field('geometry',pa.binary(),nullable=False)])
    table=pa.Table.from_pylist([{**{k:date.fromisoformat(v) if SCHEMA[k]=='date' and v is not None else v for k,v in r['properties'].items()},'geometry':shape(r['geometry']).wkb} for r in rows],schema=schema)
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['MultiPolygon','Polygon'],'crs':pyproj.CRS(4269).to_json_dict(),'bbox':audit['bounds']}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def native_prebuilt_policy(parquet: Path) -> dict:
    """Describe an already-written native artifact; this is no release approval."""
    return {'schema_version':1,'formats':{'geoparquet':{
        'reason':'Native DBF DATE fields must retain Arrow date32 and source identifiers, physical types and WKB. Generic GeoPackage conversion currently casts dates to strings; only this exact complete native artifact may be reused without regeneration. Source/baseline/output mapping acceptance remains pending.',
        'objects':[{'path':f'geoparquet/{SLUG}.parquet','sha256':sha256_file(parquet),'bytes':parquet.stat().st_size}]}}}


def gpkg_metadata(path: Path) -> dict:
    with sqlite3.connect(path) as db:
        tables = {}
        for table in ('gpkg_contents','gpkg_geometry_columns','gpkg_spatial_ref_sys'):
            cursor=db.execute('SELECT * FROM '+table+' ORDER BY 1')
            columns=[x[0] for x in cursor.description]
            tables[table]=[{k:v for k,v in zip(columns,row) if k!='last_change'} for row in cursor]
        header_srs=[]
        for (blob,) in db.execute('SELECT geom FROM "'+SLUG+'" ORDER BY fid'):
            if not isinstance(blob,bytes) or blob[:2]!=b'GP' or len(blob)<8:
                raise CBRSQuarantineError('invalid_geopackage_geometry_header')
            header_srs.append(struct.unpack_from('<i' if blob[3]&1 else '>i',blob,4)[0])
        tables['geometry_header_srs']=header_srs
        return tables


def write_geopackage(path: Path, rows: list[dict]) -> None:
    """The original native data writer, including its fixed publication timestamp."""
    with fiona.open(path,'w',driver='GPKG',layer=SLUG,crs='EPSG:4269',
                    schema={'properties':SCHEMA,'geometry':'Unknown'}) as destination:
        destination.writerecords(rows)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE gpkg_contents SET last_change='2024-12-19T00:00:00.000Z'")


def validate_geopackage_metadata(path: Path, rows: list[dict]) -> None:
    """Rebuild the complete container; neither decoded WKB nor selected SQL tables suffice."""
    with tempfile.TemporaryDirectory(prefix='cbrs-prohibitions-gpkg-metadata-') as tmp:
        reference=Path(tmp)/'reference.gpkg';write_geopackage(reference,rows)
        if path.read_bytes()!=reference.read_bytes():
            raise CBRSQuarantineError('cached_geopackage_auxiliary_metadata_or_header_drift:complete_container_bytes')


def local_candidate_path(storage: StagingStorageResource, prefix: str) -> Path:
    require_local(storage)
    root=Path(storage.local_dir).resolve()
    key=storage._ensure_prefixed(prefix)
    if Path(key).is_absolute() or any(part in {'.','..'} for part in Path(key).parts):
        raise CBRSQuarantineError('unsafe_local_candidate_prefix')
    target=root/key
    current=root
    for part in target.relative_to(root).parts:
        current=current/part
        if current.is_symlink(): raise CBRSQuarantineError('linked_candidate_path')
    return target


def reconcile_cached_candidate(storage: StagingStorageResource,prefix: str,ready: dict,
                               rows: list[dict],audit: dict,captures: dict,identity: dict,members: dict) -> None:
    """Re-derive every source-owned byte; a self-pinned ready receipt is no authority."""
    root=local_candidate_path(storage,prefix)
    canonical_manifest=(root/'metadata/source_manifest.json').read_bytes()
    hashed_snapshot=f'metadata/source/version_{sha(canonical_manifest)}_source_manifest.json'
    allowed_derivatives={hashed_snapshot,'metadata/resolved_provenance.json','metadata/quality_manifest.json','metadata/data_dictionary.json',
        'metadata/geoparquet_layout.json','metadata/source/source_manifest.json',
        'metadata/source/data_dictionary.json','metadata/source/quality_manifest.json'}
    paths=list(root.rglob('*'))
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in paths):
        raise CBRSQuarantineError('linked_candidate_object')
    actual={str(p.relative_to(root)) for p in paths if p.is_file()}
    expected=set(ready['objects'])|{'metadata/cbrs_prohibitions_candidate_ready.json'}
    cached={name:((root/f'raw/captures/{name}.body').read_bytes(),strict_json((root/f'raw/captures/{name}.receipt.json').read_bytes())) for name in URLS}
    expected_manifest=provenance(cached)
    with tempfile.TemporaryDirectory(prefix='cbrs-prohibitions-policy-') as policy_tmp:
        fresh_native=Path(policy_tmp)/'native.parquet'
        pq.write_table(typed_table(rows,audit),fresh_native,compression='zstd')
        expected_manifest['prebuilt_output_policy']=native_prebuilt_policy(fresh_native)
    expected_manifest['processing_fingerprint']=sha(encode(identity))
    if any(cached[name][0]!=captures[name][0] for name in URLS):
        raise CBRSQuarantineError('cached_capture_differs_from_fresh_source')
    for name,(body,receipt) in cached.items():
        original_receipt=f'_raw-captures/{SLUG}/{sha(body)}/{sha(encode(receipt))}.receipt.json'
        if not storage.object_exists(original_receipt) or storage.read_key(original_receipt)!=encode(receipt):
            raise CBRSQuarantineError('cached_receipt_not_in_original_capture_depot')
    original_manifest,original_captures=load_construction_bundle(storage,ready['version'],expected_manifest,identity,captures)
    manifest=strict_json((root/'metadata/source_manifest.json').read_bytes())
    if manifest!=original_manifest or cached!=original_captures:
        raise CBRSQuarantineError('cached_provenance_differs_from_original_construction_bundle')
    validate_release_provenance(manifest)
    if not expected.issubset(actual) or actual-expected-allowed_derivatives:
        raise CBRSQuarantineError('existing_candidate_unrostered_or_missing_object')
    hashed_manifest=root/hashed_snapshot
    if hashed_manifest.exists() and hashed_manifest.read_bytes()!=canonical_manifest:
        raise CBRSQuarantineError('cached_hashed_provenance_snapshot_differs')
    snapshot_manifest=root/'metadata/source/source_manifest.json'
    if snapshot_manifest.exists() and strict_json(snapshot_manifest.read_bytes())!=manifest:
        raise CBRSQuarantineError('cached_source_provenance_snapshot_differs')
    if set(manifest)!=set(expected_manifest):raise CBRSQuarantineError('cached_provenance_field_roster_drift')
    for name,value in expected_manifest.items():
        if name!='code_commit' and manifest.get(name)!=value:
            raise CBRSQuarantineError('cached_provenance_differs_from_original_capture')
    resolved_file=root/'metadata/resolved_provenance.json'
    if resolved_file.exists():
        expected_resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,ready['version']).metadata)
        if strict_json(resolved_file.read_bytes())!=expected_resolved:
            raise CBRSQuarantineError('cached_resolved_provenance_differs')
    records=b''.join(encode(r) for r in rows)
    expected_bytes={'raw/'+ARCHIVE_NAME:captures['official_zip'][0],
        'raw/retrieval.json':encode(cached['official_zip'][1]),'metadata/candidate.records.jsonl':records,
        'metadata/processing_identity.json':encode(identity),'metadata/cbrs_prohibitions_source_audit.json':encode(audit),
        'metadata/proposed_source_contract.json':encode(PROPOSAL)}
    expected_bytes.update({f'raw/members/{name}':body for name,body in members.items()})
    if any((root/name).read_bytes()!=body for name,body in expected_bytes.items()):
        raise CBRSQuarantineError('cached_source_or_normalized_records_differ')
    validate_geopackage_metadata(root/'geopackage'/f'{SLUG}.gpkg',rows)
    with tempfile.TemporaryDirectory(prefix='cbrs-prohibitions-cache-replay-') as tmp:
        directory=Path(tmp);expected_parquet=directory/'expected.parquet'
        pq.write_table(typed_table(rows,audit),expected_parquet,compression='zstd')
        for candidate in [root/'candidate/typed-source.parquet',root/'geoparquet'/f'{SLUG}.parquet']:
            if candidate.read_bytes()!=expected_parquet.read_bytes():
                raise CBRSQuarantineError('cached_typed_parquet_complete_container_drift')
        # Cold construction decodes the actual GeoPackage and proves its complete
        # records equal these independently extracted originals. Complete fresh
        # container equality above binds that proof on reuse. Recompute the pure
        # comparator with distinct raw-source and clone paths, never a candidate
        # compared to itself or an accepted-baseline assertion.
        original=directory/'original.jsonl';clone=directory/'native-derivative.jsonl'
        original.write_bytes(records);clone.write_bytes(records)
        report=compare_derivative(original,clone,directory/'comparison')
        if report.get('status')!='pass':raise CBRSQuarantineError('fresh_source_fidelity_comparison_failed')
        for name,body in fidelity_payloads(report,rows,records,directory/'comparison').items():
            if (root/name).read_bytes()!=body:
                raise CBRSQuarantineError('cached_complete_source_fidelity_evidence_drift:'+name)


@contextmanager
def candidate_lock(storage: StagingStorageResource,prefix: str):
    """The same lock protects lookup, validation, quarantine and installation."""
    target=local_candidate_path(storage,prefix);target.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:raise CBRSQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX)
        yield target


def tree_inventory(root: Path) -> dict:
    inventory={}
    for path in sorted(root.rglob('*')):
        relative=str(path.relative_to(root))
        if path.is_symlink():inventory[relative]={'type':'symlink','target':os.readlink(path)}
        elif path.is_file():inventory[relative]={'sha256':sha256_file(path),'bytes':path.stat().st_size}
        elif path.is_dir():inventory[relative]={'type':'directory'}
        else:inventory[relative]={'type':'special','mode':path.lstat().st_mode}
    return inventory


def quarantine_cached_candidate_held(storage: StagingStorageResource,target: Path,prefix: str,reason: str) -> str:
    """Move the entire invalid version outside discovery while holding its lock."""
    inventory=tree_inventory(target)
    destination=Path(storage.local_dir)/'_quarantine'/SLUG/('invalid-cache-'+uuid4().hex)
    destination.mkdir(parents=True)
    os.rename(target,destination/'candidate')
    if tree_inventory(destination/'candidate')!=inventory:
        raise CBRSQuarantineError('quarantined_cache_inventory_changed_during_move:'+str(destination))
    (destination/'report.json').write_bytes(encode({'approved':False,'status':'quarantined_invalid_draft_cache',
        'original_prefix':prefix,'reason':reason,'objects':inventory}))
    return str(destination)


def fidelity_payloads(fidelity: dict,rows: list[dict],records: bytes,comparison: Path) -> dict[str,bytes]:
    parquet_fidelity={'status':'pass','method':'complete_decoded_source_records_byte_equality_and_exact_arrow_schema',
        'rows':len(rows),'source_records_sha256':sha(records),'shared_schema_geometry_validation_reference':fidelity}
    payloads={
        'metadata/geopackage_source_fidelity.json':encode({'canonical_full_records_byte_identical':True,'shared_verifier':fidelity,'known_invalid_native_geometry_release_blocked':False}),
        'metadata/parquet_source_fidelity.json':encode({'canonical_full_records_byte_identical':True,'shared_verifier':parquet_fidelity,'known_invalid_native_geometry_release_blocked':False}),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'status':'blocked','approved':False,
            'reason_codes':['unapproved_native_date_mapping_geometry_wrapper_and_original_fid_published_id_contract'],'acceptance':'not_established'})}
    payloads.update({f'metadata/source-fidelity/{name}':(comparison/name).read_bytes() for name in FIDELITY_FILES})
    return payloads


def construction_bundle_key(version: str) -> str:
    return f'_source-provenance/{SLUG}/{version}/bundle.json'


def validate_construction_commit(manifest: dict,identity: dict) -> None:
    """A construction commit must contain exactly the pinned processing files.

    Later documentation-only HEADs may differ. Neither the mutable candidate nor
    a malformed external bundle can invent a commit or a different processor.
    This local immutable depot is a preservation boundary, not a signature or a
    defense against an administrator replacing the whole depot and Git history.
    """
    commit=manifest.get('code_commit');repo=Path(__file__).parents[3]
    if not isinstance(commit,str) or not re.fullmatch('[0-9a-f]{40}',commit):
        raise CBRSQuarantineError('original_construction_commit_not_valid')
    try:
        subprocess.check_output(['git','cat-file','-e',commit+'^{commit}'],cwd=repo,stderr=subprocess.DEVNULL)
        for name,digest in identity['files'].items():
            if sha(subprocess.check_output(['git','show',f'{commit}:{name}'],cwd=repo,stderr=subprocess.DEVNULL))!=digest:
                raise CBRSQuarantineError('original_construction_commit_processing_files_differ')
    except subprocess.CalledProcessError as exc:
        raise CBRSQuarantineError('original_construction_commit_not_in_repository') from exc


def load_construction_bundle(storage: StagingStorageResource,version: str,
                             expected_manifest: dict,identity: dict,captures: dict) -> tuple[dict,dict]:
    """Read original construction only; cache reuse must never self-bootstrap it."""
    key=construction_bundle_key(version)
    path=local_candidate_path(storage,key)
    if not storage.object_exists(key):raise CBRSQuarantineError('missing_original_construction_bundle')
    if not path.is_file() or path.stat().st_nlink!=1:raise CBRSQuarantineError('linked_original_construction_bundle')
    bundle=strict_json(storage.read_key(key))
    if (set(bundle)!={'schema_version','version','manifest','captures'} or bundle['schema_version']!=1
            or bundle['version']!=version or not isinstance(bundle['captures'],dict)
            or set(bundle['captures'])!=set(URLS) or not isinstance(bundle['manifest'],dict)):
        raise CBRSQuarantineError('original_construction_bundle_roster_drift')
    original={}
    for name in URLS:
        item=bundle['captures'][name]
        if not isinstance(item,dict) or set(item)!={'body_key','receipt_key','receipt'}:
            raise CBRSQuarantineError('original_construction_capture_roster_drift')
        body=captures[name][0];receipt=item['receipt'];validate_receipt(body,receipt,URLS[name])
        body_key=f'_raw-captures/{SLUG}/{sha(body)}/{name}.body'
        receipt_key=f'_raw-captures/{SLUG}/{sha(body)}/{sha(encode(receipt))}.receipt.json'
        if item['body_key']!=body_key or item['receipt_key']!=receipt_key:
            raise CBRSQuarantineError('original_construction_capture_depot_key_drift')
        if storage.read_key(body_key)!=body or storage.read_key(receipt_key)!=encode(receipt):
            raise CBRSQuarantineError('original_construction_capture_depot_bytes_drift')
        original[name]=(body,receipt)
    manifest=bundle['manifest'];expected=provenance(original)
    expected.update(processing_fingerprint=sha(encode(identity)),prebuilt_output_policy=expected_manifest['prebuilt_output_policy'])
    if set(manifest)!=set(expected) or any(manifest.get(k)!=v for k,v in expected.items() if k!='code_commit'):
        raise CBRSQuarantineError('original_construction_manifest_drift')
    validate_release_provenance(manifest);validate_construction_commit(manifest,identity)
    return manifest,original


def bind_first_construction_held(storage: StagingStorageResource,version: str,
                                 payloads: dict,ready: dict,captures: dict,identity: dict) -> None:
    """Called only after a fresh full-source build and while holding version lock."""
    manifest=strict_json(payloads['metadata/source_manifest.json']);key=construction_bundle_key(version)
    local_candidate_path(storage,key)
    if not storage.object_exists(key):
        validate_construction_commit(manifest,identity)
        bundle={'schema_version':1,'version':version,'manifest':manifest,'captures':{name:{
            'body_key':f'_raw-captures/{SLUG}/{sha(body)}/{name}.body',
            'receipt_key':f'_raw-captures/{SLUG}/{sha(body)}/{sha(encode(receipt))}.receipt.json',
            'receipt':receipt} for name,(body,receipt) in captures.items()}}
        immutable(storage,key,encode(bundle))
    original,original_captures=load_construction_bundle(storage,version,manifest,identity,captures)
    payloads['metadata/source_manifest.json']=encode(original)
    payloads['raw/retrieval.json']=encode(original_captures['official_zip'][1])
    for name,(_,receipt) in original_captures.items():payloads[f'raw/captures/{name}.receipt.json']=encode(receipt)
    ready['objects']={name:sha(value) for name,value in payloads.items()}


def commit_complete_candidate(storage: StagingStorageResource,prefix: str,payloads: dict,ready: dict,
                              *,captures: dict | None = None,identity: dict | None = None) -> None:
    """Install one fully built local version atomically; partial builds are never discoverable."""
    with candidate_lock(storage,prefix) as target:
        if target.exists() or target.is_symlink():
            # A valid concurrent winner is never overwritten or quarantined.
            raise CBRSQuarantineError('concurrent_or_incomplete_existing_candidate')
        if captures is not None:
            bind_first_construction_held(storage,ready['version'],payloads,ready,captures,identity)
        with tempfile.TemporaryDirectory(prefix='.candidate-build-',dir=target.parent) as tmp:
            private=StagingStorageResource(local_dir=tmp,use_local=True)
            for name,value in payloads.items():immutable(private,name,value)
            immutable(private,'metadata/cbrs_prohibitions_candidate_ready.json',encode(ready))
            os.rename(tmp,target)



def stage_cbrs_prohibitions_candidate(storage: StagingStorageResource, *, capture_dir: Path | None = None) -> dict:
    require_local(storage)
    captures = acquire(storage,capture_dir)
    body,receipt = captures['official_zip']
    try:
        manifest=provenance(captures)
        with tempfile.TemporaryDirectory(prefix='cbrs_prohibitions-source-') as tmp:
            directory = Path(tmp)
            rows, audit, members = inspect_archive(body, directory)
            records = b''.join(encode(r) for r in rows)
            identity = processing_identity()
            fingerprint = sha(encode(identity))
            capture_fingerprint=sha(encode({name:sha(value[0]) for name,value in captures.items()}))
            version = f'v0.0.0-draft-cbrs20241219-{capture_fingerprint[:16]}-{fingerprint[:16]}'
            prefix = f'{SLUG}/{SLUG}/{version}'
            ready_key = f'{prefix}/metadata/cbrs_prohibitions_candidate_ready.json'
            with candidate_lock(storage,prefix) as target:
                if target.exists():
                    try:
                        if not storage.object_exists(ready_key):raise CBRSQuarantineError('existing_candidate_missing_ready_marker')
                        ready = strict_json(storage.read_key(ready_key))
                        expected_names = {f'raw/{ARCHIVE_NAME}', 'raw/retrieval.json', f'geopackage/{SLUG}.gpkg', 'candidate/typed-source.parquet', f'geoparquet/{SLUG}.parquet', 'metadata/candidate.records.jsonl', 'metadata/source_manifest.json', 'metadata/processing_identity.json', 'metadata/cbrs_prohibitions_source_audit.json', 'metadata/proposed_source_contract.json', 'metadata/geopackage_source_fidelity.json', 'metadata/parquet_source_fidelity.json', 'metadata/baseline_comparison.json'} | {f'metadata/source-fidelity/{name}' for name in FIDELITY_FILES} | {f'raw/members/{name}' for name in MEMBERS} | {f'raw/captures/{name}{suffix}' for name in URLS for suffix in ('.body','.receipt.json')}
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
                        retained=quarantine_cached_candidate_held(storage,target,prefix,str(exc))
                        raise _CachedCandidateQuarantined(f'{exc}; complete_cache_retained:{retained}') from exc
            geopackage = directory / f'{SLUG}.gpkg'
            write_geopackage(geopackage,rows)
            source_records = directory / 'source.jsonl'
            source_records.write_bytes(records)
            decoded = directory / 'geopackage.jsonl'
            with fiona.open(geopackage) as source:
                if dict(source.schema) != {'properties': {k: ('int32' if v.startswith('int32') else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()}, 'geometry': 'Unknown'}:
                    raise CBRSQuarantineError(f'geopackage_physical_schema_loss:{source.schema}')
                if not pyproj.CRS(source.crs).equals(pyproj.CRS(4269)):
                    raise CBRSQuarantineError('geopackage_native_crs_loss')
                decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': dict(r['properties']),
                    'geometry': mapping(shape(r['geometry']))}) for r in source))
            fidelity = compare_derivative(source_records, decoded, directory / 'comparison')
            if fidelity.get('status')!='pass' or decoded.read_bytes() != records:
                raise CBRSQuarantineError('geopackage_complete_fidelity_failure')
            table=typed_table(rows,audit)
            parquet = directory / 'typed-source.parquet'
            pq.write_table(table, parquet, compression='zstd')
            decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': {k:r[k].isoformat() if isinstance(r[k],date) else r[k] for k in SCHEMA},
                'geometry': mapping(shapely.from_wkb(r['geometry']))}) for r in pq.read_table(parquet).to_pylist()))
            if decoded.read_bytes() != records or not pq.read_schema(parquet).equals(table.schema, check_metadata=True):
                raise CBRSQuarantineError('typed_parquet_physical_or_full_row_fidelity_failure')
            commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=Path(__file__).parents[3], text=True).strip()
            manifest.update(code_commit=commit,processing_fingerprint=fingerprint,prebuilt_output_policy=native_prebuilt_policy(parquet))
            validate_release_provenance(manifest)
            payloads = {f'raw/{ARCHIVE_NAME}': body, 'raw/retrieval.json': encode(receipt),
                f'geopackage/{SLUG}.gpkg': geopackage.read_bytes(), 'candidate/typed-source.parquet': parquet.read_bytes(), f'geoparquet/{SLUG}.parquet':parquet.read_bytes(),
                'metadata/candidate.records.jsonl': records, 'metadata/source_manifest.json': encode(manifest),
                'metadata/processing_identity.json': encode(identity), 'metadata/cbrs_prohibitions_source_audit.json': encode(audit),
                'metadata/proposed_source_contract.json': encode(PROPOSAL),
            }
            payloads.update(fidelity_payloads(fidelity,rows,records,directory/'comparison'))
            payloads.update({f'raw/members/{name}': value for name,value in members.items()})
            payloads.update({f'raw/captures/{name}.body':value[0] for name,value in captures.items()})
            payloads.update({f'raw/captures/{name}.receipt.json':encode(value[1]) for name,value in captures.items()})
            ready = {'status':'working_draft_candidate_ready', 'approved':False, 'source_sha256':PIN,
                'candidate_records_sha256':sha(records), 'processing_fingerprint':fingerprint, 'version':version,
                'objects':{name:sha(value) for name,value in payloads.items()}}
            commit_complete_candidate(storage,prefix,payloads,ready,captures=captures,identity=identity)
            return {'dataset_slug':SLUG, 'file_slug':SLUG, 'version':version, 'feature_count':len(rows),
                    'status':ready['status'], 'processing_fingerprint':fingerprint}
    except _CachedCandidateQuarantined:
        raise
    except (ValueError, TypeError, KeyError, OSError, RuntimeError, zipfile.BadZipFile, sqlite3.Error, fiona.errors.FionaError) as exc:
        location = quarantine(storage, body, receipt, str(exc))
        raise CBRSQuarantineError(f'{exc}; retained:{location}') from exc
