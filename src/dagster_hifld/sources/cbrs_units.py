"""Faithful local draft of FWS CBRS Units publication2024-12-19.

Not a definitive CBRS in/out determination. Original DBF declarations, values and mixed geometry
are preserved; no source, baseline, ID, schema mapping, or release approval.
"""
from __future__ import annotations

import collections
import hashlib
import io
import fcntl
import os
import json
import platform
import re
import sqlite3
import stat
import struct
import tempfile
import zipfile
from datetime import datetime, timezone
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path, PurePosixPath
from uuid import uuid4

import fiona
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import mapping, shape

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest, prebuilt_output_keys
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import compare_full_rows, sha256_file
from dagster_hifld.sources.epa_frs_common import (STAGED_RELEASE_DERIVATIVE_NAMES,
    processing_identity as packaged_processing_identity, validate_staged_release_derivatives)

SLUG = 'cbrs-units-2'
URL = 'https://www.fws.gov/sites/default/files/documents/2024-12/20241219-cbrs-boundaries.zip'
LANDING = 'https://www.fws.gov/program/coastal-barrier-resources-act/maps-and-data'
DOWNLOAD = 'https://www.fws.gov/media/digital-coastal-barrier-resources-system-boundaries'
METADATA = 'https://www.fws.gov/sites/default/files/documents/2024-12/20241219-cbrs-polygons-metadata.txt'
URLS = {'official_zip': URL, 'fws_data': LANDING, 'fws_download': DOWNLOAD, 'fws_units_metadata': METADATA}
ARCHIVE_NAME = '20241219-cbrs-boundaries.zip'
PIN = '8765dd5c6d8d0404d321b28af33236956de2daa8f43b81d61191f8f49eafabd0'
ARCHIVE_BYTES = 32280370
MAX_BYTES = 40_000_000
MAX_EXPANDED_BYTES = 60_000_000
STEM = '20241219_CBRS_Polygons'
MEMBERS = frozenset(['20241219_CBRS_Buffer_Zone.prj', '20241219_CBRS_Polygons.prj', '20241219_CBRS_Prohibitions.prj', '20241219_CBRS_Buffer_Zone.sbn', '20241219_CBRS_Polygons.sbn', '20241219_CBRS_Prohibitions.sbn', '20241219_CBRS_Buffer_Zone.sbx', '20241219_CBRS_Polygons.sbx', '20241219_CBRS_Prohibitions.sbx', '20241219_CBRS_Buffer_Zone.shp', '20241219_CBRS_Polygons.shp', '20241219_CBRS_Prohibitions.shp', '20241219_CBRS_Buffer_Zone.shx', '20241219_CBRS_Polygons.shx', '20241219_CBRS_Prohibitions.shx', '20241219-User-Guide-for-CBRS-data.pdf', '20241219_CBRS_Buffer_Zone.cpg', '20241219_CBRS_Polygons.cpg', '20241219_CBRS_Prohibitions.cpg', '20241219_CBRS_Buffer_Zone.dbf', '20241219_CBRS_Polygons.dbf', '20241219_CBRS_Prohibitions.dbf', '20241219_CBRS_Buffer_Zone.shp.xml', '20241219_CBRS_Polygons.shp.xml', '20241219_CBRS_Prohibitions.shp.xml'])
NATIVE_SCHEMA = {'Unit':'str:8','Name':'str:254','Unit_Type':'str:25','Fast_Acres':'float:13.11','Wet_Acres':'float:13.11','Shore_Mile':'float:13.11','Shape_Leng':'float:19.11','Shape_Area':'float:19.11'}
SCHEMA = {**NATIVE_SCHEMA, '__source_fid':'str:10'}
PROPOSAL = {'approved':False,'id_fields':['__source_fid'],'geometry':True,
 'coverage_fields':['Unit_Type'],'crs':'EPSG:4269',
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
                            for block in response.iter_bytes():
                                body+=block
                                if len(body)>MAX_BYTES: raise CBRSQuarantineError('response_size_limit')
                            receipt.update(byte_length=len(body),sha256=sha(body))
                            validate_receipt(body,receipt,url)
                            wire=response.num_bytes_downloaded
                            receipt.update(wire_bytes_downloaded=wire,content_encoding=response.headers.get('content-encoding','identity'),response_content_length=response.headers.get('content-length'))
                            if wire>MAX_BYTES:
                                raise CBRSQuarantineError('wire_response_size_limit')
                            if response.headers.get('content-length') is not None and int(response.headers['content-length'])!=wire:
                                raise CBRSQuarantineError('incomplete_wire_content_length')
                    break
                except (httpx.HTTPError,OSError,ValueError,TypeError) as exc:
                    receipt.update(byte_length=len(body),sha256=sha(body),partial=True)
                    location=quarantine(storage,body,receipt,str(exc))
                    if attempt==2: raise CBRSQuarantineError(f'bounded_fetch_exhausted:{exc}; retained:{location}') from exc
        captures[name]=(body,receipt)
        immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/{name}.body',body)
        immutable(storage,f'_raw-captures/{SLUG}/{sha(body)}/{sha(encode(receipt))}.receipt.json',encode(receipt))
    return captures


def provenance(captures: dict, *, identity: dict | None = None,
               prebuilt_body: bytes | None = None) -> dict:
    # Validate the complete original capture bundle before any canonical candidate writes.
    identity = identity or processing_identity()
    for name,url in URLS.items(): validate_receipt(*captures[name],url)
    if b'/sites/default/files/documents/2024-12/20241219-cbrs-boundaries.zip' not in captures['fws_download'][0]:
        raise CBRSQuarantineError('official_distribution_link_changed')
    if b'20241219' not in captures['fws_units_metadata'][0] or b'CBRS_Polygons' not in captures['fws_units_metadata'][0]:
        raise CBRSQuarantineError('official_metadata_identity_or_release_changed')
    inputs=[]
    for name,(body,receipt) in captures.items():
        inputs.append({'input_id':name+':'+sha(body),'producer':'U.S. Fish and Wildlife Service',
            'host':'www.fws.gov','role':'producer_publication',
            'canonical_url':URLS[name],'retrieval_url':receipt['requested_url'],'observed_final_url':receipt['final_url'],
            'retrieved_at':receipt['observed_at'],'release_date':'2024-12-19' if name in {'official_zip','fws_units_metadata'} else None,
            'release_date_unknown_reason':'Supporting official webpage has no dataset release date; fetch time is not release date.',
            'sha256':sha(body),'bytes':len(body),'license':METADATA,
            'license_scope':'Official metadata use constraints: acknowledge FWS, share derived products with FWS; no survey or definitive in/out determination. Redistribution acceptance remains pending.',
            'attribution':'U.S. Fish and Wildlife Service, CBRS Units, publication2024-12-19.',
            'lineage':'Producer-hosted full distribution and official supporting pages; not a third-party archive or self-claimed delegation.',
            'authority_evidence':LANDING})
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':identity['code_commit'],
        'code_identity_scheme':identity['code_identity_scheme'],
        'title':'CBRS Units — faithful producer-source working draft',
        'publisher':'U.S. Fish and Wildlife Service','agency':'U.S. Fish and Wildlife Service',
        'source_url':LANDING,'license':METADATA,'date_issued':'2024-12-19','inputs':inputs,
        'source_anomalies':[],
        'license_review_required':'Preserve official use constraints; maintainer acceptance required before release.',
        'transformations':[{'operation':'decode_original_shapefile','preserved':'All eight DBF columns; character values decoded directly from original UTF-8 DBF with fixed-width right padding excluded and leading characters preserved; numeric values and original physical declarations/decimal bytes (retained in the raw ZIP), native NAD83 coordinates and original shapefile FID under proposed __source_fid. Complete ZIP and25members retained.',
            'repair_or_projection':False,'mapping_approval':'not established; full published candidate compatibility and original FID/OBJECTID/id crosswalk remain unapproved'}],
        'unsupported_output_formats':{'shapefile_zip':'The full original producer archive is retained in raw/. Regenerated DBF loses the proposed explicit original source FID or renames it; no regenerated mapping approval.',
            'pmtiles':'Tile reprojection and quantization alter native geometry. No visualization mapping approved.'}}

    if prebuilt_body is not None:
        manifest['prebuilt_output_policy']=native_prebuilt_policy(prebuilt_body)
    validate_output_policy(manifest)
    validate_release_provenance(manifest)
    return manifest



def native_prebuilt_policy(body: bytes) -> dict:
    """Capability pin, not source/contract/release acceptance or a format ban."""
    policy={'schema_version':1,'formats':{'geoparquet':{
        'reason':'Observed generic conversion adds bbox and changes field/nullability metadata. Preserve this complete native source-typed GeoParquet without generic regeneration; independent mapping and release approvals remain mandatory.',
        'objects':[{'path':f'geoparquet/{SLUG}.parquet','sha256':sha(body),'bytes':len(body)}]}}}
    validate_output_policy({'prebuilt_output_policy':policy})
    return policy

def processing_identity() -> dict:
    """Git or explicit installed-package identity plus native processing engines."""
    identity=packaged_processing_identity(Path(__file__).parents[3])
    return {**identity,'implementation':platform.python_implementation(),'platform':platform.platform(),
            'gdal':fiona.__gdal_version__,'verifier_sha256':verifier_implementation_sha256()}


def decode_physical_dbf(body: bytes, encoding: bytes) -> tuple[list[dict], list[dict]]:
    """Decode original physical values; C-field right padding is not source text.

    Fiona strips leading blanks from DBF character cells. Decode all character
    fields directly instead, and independently check the numeric representations.
    The pinned source has UTF-8 C fields and F numeric fields, and no DATE/OID.
    """
    if encoding != b'UTF-8' or len(body)<33:
        raise CBRSQuarantineError('physical_dbf_encoding_or_header_drift')
    count=struct.unpack_from('<I',body,4)[0]
    header,width=struct.unpack_from('<HH',body,8)
    declarations=[];offset=1
    if header<33 or (header-33)%32 or header+count*width>len(body):
        raise CBRSQuarantineError('physical_dbf_incomplete_header_or_records')
    for position in range(32,header-1,32):
        field=body[position:position+32]
        name=field[:11].split(b'\x00',1)[0].decode('ascii')
        kind=chr(field[11]);length=field[16];decimals=field[17]
        declarations.append({'name':name,'type':kind,'width':length,'decimals':decimals,'offset':offset})
        offset+=length
    expected=[(name,'C' if kind.startswith('str') else 'F',
               int(kind.split(':')[1].split('.')[0]),0 if kind.startswith('str') else int(kind.split('.')[1]))
              for name,kind in NATIVE_SCHEMA.items()]
    if ([(f['name'],f['type'],f['width'],f['decimals']) for f in declarations]!=expected
            or offset!=width or body[header-1]!=13 or body[header+count*width:] not in (b'',b'\x1a')):
        raise CBRSQuarantineError('physical_dbf_declarations_or_record_width_drift')
    records=[]
    for index in range(count):
        record=body[header+index*width:header+(index+1)*width]
        if record[0:1]!=b' ':
            raise CBRSQuarantineError('physical_dbf_deleted_or_invalid_record')
        decoded={}
        for field in declarations:
            value=record[field['offset']:field['offset']+field['width']]
            decoded[field['name']]=(value.rstrip(b' ').decode('utf-8',errors='strict')
                                    if field['type']=='C' else float(value.decode('ascii')))
        records.append(decoded)
    return records,declarations


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
    if len(rows) != 942 or struct.unpack_from('<I', members[f'{STEM}.dbf'], 4)[0] != len(rows):
        raise CBRSQuarantineError('incomplete_dbf_shp_population')
    ids = [r['properties']['__source_fid'] for r in rows]
    if set(ids) != {str(i) for i in range(942)} or len(set(ids)) != len(rows):
        raise CBRSQuarantineError('original_fid_population_drift')
    rows.sort(key=lambda r: int(r['properties']['__source_fid']))
    physical,declarations=decode_physical_dbf(members[f'{STEM}.dbf'],members[f'{STEM}.cpg'])
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
        original=physical[int(row['properties']['__source_fid'])]
        for name,value in original.items():
            driver=row['properties'][name]
            if driver!=(value.strip() if isinstance(value,str) else value):
                raise CBRSQuarantineError(f'physical_dbf_driver_value_drift:{name}')
            row['properties'][name]=value
        if row['geometry'] is None:
            raise CBRSQuarantineError('missing_native_geometry')
        g = shape(row['geometry'])
        if g.is_empty or g.geom_type not in {'Polygon','MultiPolygon'}:
            raise CBRSQuarantineError('invalid_empty_or_changed_native_geometry')
    if collections.Counter(r['properties']['Unit_Type'] for r in rows) != {'System Unit':592,'Otherwise Protected Area':350}:
        raise CBRSQuarantineError('unit_type_coverage_drift')
    invalid = {r['properties']['__source_fid']: shapely.is_valid_reason(shape(r['geometry'])) for r in rows if not shape(r['geometry']).is_valid}
    if invalid: raise CBRSQuarantineError('invalid_native_geometry')
    audit = {'invalid_geometry_count': len(invalid), 'native_invalid_geometries': invalid, 'source_quality': 'native_fidelity_pass_unapproved_release', 'approved': False, 'status': 'producer_source_fidelity_pass', 'feature_count': len(rows),
             'schema': schema, 'physical_dbf_fields':declarations, 'character_decoding':'UTF-8; remove fixed-width right-padding spaces only; preserve leading characters', 'crs': 'EPSG:4269', 'bounds': shapely.total_bounds([shape(r['geometry']) for r in rows]).tolist(),
             'nulls': {name: sum(r['properties'][name] is None for r in rows) for name in SCHEMA},
             'identifiers': {'__source_fid': {'unique': len(set(ids)), 'nulls': 0}},
             'coverage': {name: dict(collections.Counter(r['properties'][name] for r in rows)) for name in ('Unit_Type',)},
             'members': {name: {'bytes': len(data), 'sha256': sha(data)} for name, data in sorted(members.items())}}
    return rows, audit, members


def compare_derivative(source: Path, candidate: Path, output: Path) -> dict:
    pin = sha256_file(source)
    return compare_full_rows(source, candidate, PROPOSAL, output, baseline_sha256=pin,
        candidate_sha256=sha256_file(candidate), baseline_reference={
            'role': 'unapproved_producer_source_fidelity_reference', 'approved': False,
            'source_archive_sha256': PIN, 'comparison_input': {'adapter': 'typed-geojson-feature-jsonl-v1',
                'sha256': pin, 'bytes': source.stat().st_size}})



@lru_cache(maxsize=2)
def source_verification_payloads(records: bytes) -> dict[str, bytes]:
    """Recompute full comparison claims and sidecars from the complete original.

    Actual cached derivatives are independently decoded and proven byte-equal to
    this stream before these reports are trusted. The bounded memo is keyed by
    the original bytes, never by a ready receipt or cached report's self-pin.
    """
    with tempfile.TemporaryDirectory(prefix='cbrs-units-full-evidence-') as tmp:
        root=Path(tmp);original=root/'original.jsonl';original.write_bytes(records)
        derivative=root/'decoded-derivative.jsonl';derivative.write_bytes(records)
        fidelity=compare_derivative(original,derivative,root/'comparison')
        if fidelity.get('status')!='pass':
            raise CBRSQuarantineError('fresh_full_source_verification_failed')
        parquet_fidelity={'status':'pass','method':'complete_decoded_source_records_byte_equality_and_exact_arrow_schema',
            'rows':942,'source_records_sha256':sha(records),'shared_schema_geometry_validation_reference':fidelity}
        wrapper=lambda report: {'canonical_full_records_byte_identical':True,'shared_verifier':report,
                                'known_invalid_native_geometry_release_blocked':False}
        result={'metadata/geopackage_source_fidelity.json':encode(wrapper(fidelity)),
                'metadata/parquet_source_fidelity.json':encode(wrapper(parquet_fidelity)),
                'metadata/baseline_comparison.json':encode({'schema_version':1,'status':'blocked','approved':False,
                    'reason_codes':['unapproved_source_numeric_precision_geometry_wrapper_and_original_fid_published_id_contract'],
                    'acceptance':'not_established'})}
        for name in ('baseline_differences.jsonl','baseline_field_changes.jsonl'):
            result['metadata/'+name]=(root/'comparison'/name).read_bytes()
        return result


def validate_source_verification_payloads(root: Path, records: bytes) -> None:
    for name,body in source_verification_payloads(records).items():
        if (root/name).read_bytes()!=body:
            raise CBRSQuarantineError('cached_source_evidence_or_unapproved_baseline_claim_drift:'+name)

def typed_table(rows: list[dict], audit: dict) -> pa.Table:
    schema=pa.schema([pa.field(k,pa.float64() if v.startswith('float') else pa.string(),nullable=False) for k,v in SCHEMA.items()]+[pa.field('geometry',pa.binary(),nullable=False)])
    table=pa.Table.from_pylist([{**r['properties'],'geometry':shape(r['geometry']).wkb} for r in rows],schema=schema)
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['MultiPolygon','Polygon'],'crs':pyproj.CRS(4269).to_json_dict(),'bbox':audit['bounds']}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})



def derive_native_parquet_bytes(rows: list[dict], audit: dict) -> bytes:
    """Fresh canonical reference for policy pins; original materialization writer is unchanged."""
    output=pa.BufferOutputStream()
    pq.write_table(typed_table(rows,audit),output,compression='zstd')
    return output.getvalue().to_pybytes()

def write_geopackage(path: Path, rows: list[dict]) -> None:
    with fiona.open(path,'w',driver='GPKG',layer=SLUG,crs='EPSG:4269',
                    schema={'properties':SCHEMA,'geometry':'Unknown'}) as stream:
        stream.writerecords(rows)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE gpkg_contents SET last_change='2024-12-19T00:00:00.000Z'")


def validate_geopackage_metadata(path: Path, rows: list[dict]) -> None:
    # This is our immutable canonical cache, not an arbitrary equivalent container.
    # Full deterministic reconstruction binds SRS authorities/WKT, geometry flags,
    # all envelopes, RTree, indexes, triggers, auxiliary SQL tables and declarations.
    with tempfile.TemporaryDirectory(prefix='cbrs-units-gpkg-rederive-') as tmp:
        reference=Path(tmp)/'reference.gpkg'
        write_geopackage(reference,rows)
        if sha256_file(path)!=sha256_file(reference):
            raise CBRSQuarantineError('cached_geopackage_container_bytes_differ_from_original_reconstruction')


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


def same_captures(left: dict, right: dict) -> bool:
    return set(left)==set(right) and all(left[name][0]==right[name][0] and encode(left[name][1])==encode(right[name][1]) for name in left)


def construction_record_key(version: str) -> str:
    return f'_construction-state/{SLUG}/{version}.json'


def incident_directory(version: str) -> str:
    return f'_construction-state/{SLUG}/incident-{version}'


def state_present(storage: StagingStorageResource, version: str) -> bool:
    key=construction_record_key(version)
    return any(local_candidate_path(storage,value).exists()
               for value in (key,key+'.first-construction.json',key+'.evidence'))


def refuse_observed_loss(storage: StagingStorageResource, version: str) -> None:
    if local_candidate_path(storage,incident_directory(version)).exists():
        raise CBRSQuarantineError('permanent_observed_original_trust_loss_requires_explicit_reviewed_restore')


def record_observed_loss(storage: StagingStorageResource, version: str, fingerprint: str, reason: str) -> None:
    body=canonical_construction_bundle(version,fingerprint,{'approved':False,
        'kind':'observed_original_trust_loss','reason':reason,
        'operator_action':'Restore exact original witnesses and explicitly retain/review this incident; no automatic bootstrap.'})
    seal_first_construction(storage,incident_directory(version)+'/record.json',version,fingerprint,body)


class IncidentWriteError(Exception):
    """Failure to durably journal observed loss must preserve the candidate in place."""


def load_original_or_record_loss(storage,version,captures,manifest,processing_files,identity=None):
    try:
        return original_construction_bundle(storage,version,captures,manifest,processing_files,identity=identity)
    except (ValueError,TypeError,KeyError,OSError,RuntimeError) as loss:
        try:record_observed_loss(storage,version,manifest['processing_fingerprint'],str(loss))
        except Exception as write:
            raise IncidentWriteError('incident_write_failed_candidate_and_partial_state_preserved') from write
        raise CBRSQuarantineError('observed_original_trust_loss:'+str(loss)) from loss


def original_construction_bundle(storage: StagingStorageResource, version: str, captures: dict,
                                 manifest: dict, processing_files: dict, *,
                                 identity: dict | None=None, create: bool=False) -> tuple[dict,dict]:
    """Shared canonical CAS/marker/alias binding; caller owns the version lock.

    First sealing is permitted only after complete private-candidate validation.
    Legacy unbound records require explicit migration; cache bytes cannot bootstrap.
    """
    refuse_observed_loss(storage,version)
    identity=identity or processing_identity()
    legacy=f'_source-provenance/{SLUG}/{version}/bundle.json'
    if storage.object_exists(legacy):
        raise CBRSQuarantineError('legacy_unbound_provenance_requires_explicit_migration')
    fingerprint=manifest['processing_fingerprint'];key=construction_record_key(version)
    payload={'manifest':manifest,'captures':{name:{
        'body_key':f'_raw-captures/{SLUG}/{sha(body)}/{name}.body','receipt':receipt}
        for name,(body,receipt) in captures.items()}}
    # Validate source semantics, original receipt attribution and reachable code
    # BEFORE sealing any outside record, including the CAS-only failure path.
    def validate(bundle):
        if set(bundle)!={'manifest','captures'} or set(bundle['captures'])!=set(URLS):
            raise CBRSQuarantineError('original_construction_bundle_roster_drift')
        original={}
        for name in URLS:
            item=bundle['captures'][name]
            if set(item)!={'body_key','receipt'}:
                raise CBRSQuarantineError('original_construction_capture_roster_drift')
            expected=f"_raw-captures/{SLUG}/{item['receipt'].get('sha256')}/{name}.body"
            if item['body_key']!=expected:
                raise CBRSQuarantineError('original_construction_capture_key_drift')
            body=storage.read_key(expected)
            if body!=captures[name][0]:
                raise CBRSQuarantineError('original_construction_capture_bytes_changed')
            original[name]=(body,item['receipt'])
        expected=provenance(original,identity=identity)
        expected['processing_fingerprint']=fingerprint
        expected['prebuilt_output_policy']=manifest['prebuilt_output_policy']
        actual=bundle['manifest']
        validate_output_policy(actual);validate_release_provenance(actual)
        validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],processing_files,identity=identity)
        if encode({k:v for k,v in actual.items() if k!='code_commit'})!=encode({k:v for k,v in expected.items() if k!='code_commit'}):
            raise CBRSQuarantineError('original_construction_manifest_drift')
        return original,actual
    if create:
        validate(payload)
        envelope=canonical_construction_bundle(version,fingerprint,payload)
        body=seal_first_construction(storage,key,version,fingerprint,envelope)
    else:body=load_first_construction(storage,key,version,fingerprint)
    return validate(json.loads(body)['payload'])


def reconcile_cached_candidate(storage: StagingStorageResource,prefix: str,ready: dict,
                               rows: list[dict],audit: dict,captures: dict,identity: dict,members: dict,
                               *, root: Path | None=None, original_context: tuple | None=None) -> None:
    """Re-derive source-owned bytes/typed rows; a self-pinned ready receipt is no authority."""
    root=root if root is not None else local_candidate_path(storage,prefix)
    # The normal graph adds only these derivative files. Source-owned directories remain exact.
    allowed_derivatives={f'geoparquet/{SLUG}.parquet','metadata/quality_manifest.json',
        'metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json',
        'metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',
        'metadata/source/version_'+sha((root/'metadata/source_manifest.json').read_bytes())+'_source_manifest.json'}|STAGED_RELEASE_DERIVATIVE_NAMES
    paths=list(root.rglob('*'))
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in paths):
        raise CBRSQuarantineError('linked_candidate_object')
    actual={str(p.relative_to(root)) for p in paths if p.is_file()}
    expected=set(ready['objects'])|{'metadata/cbrs_units_candidate_ready.json'}
    if not expected.issubset(actual) or actual-expected-allowed_derivatives:
        raise CBRSQuarantineError('existing_candidate_unrostered_or_missing_object')
    try: validate_staged_release_derivatives(storage,prefix,{f'{prefix}/{name}' for name in actual})
    except (OSError,UnicodeError,ValueError,TypeError) as exc: raise CBRSQuarantineError('invalid_staged_release_derivatives') from exc
    cached={name:((root/f'raw/captures/{name}.body').read_bytes(),strict_json((root/f'raw/captures/{name}.receipt.json').read_bytes())) for name in URLS}
    expected_manifest=provenance(cached,identity=identity,prebuilt_body=derive_native_parquet_bytes(rows,audit))
    expected_manifest['processing_fingerprint']=sha(encode(identity))
    if any(cached[name][0]!=captures[name][0] for name in URLS):
        raise CBRSQuarantineError('cached_capture_differs_from_fresh_source')
    manifest=strict_json((root/'metadata/source_manifest.json').read_bytes())
    validate_output_policy(manifest)
    validate_release_provenance(manifest)
    original,anchored=original_context if original_context is not None else original_construction_bundle(storage,ready['version'],captures,expected_manifest,identity['files'],identity=identity)
    if encode(manifest)!=encode(anchored) or not same_captures(cached,original):
        raise CBRSQuarantineError('cached_original_construction_manifest_or_receipts_changed')
    if root==local_candidate_path(storage,prefix):
        prebuilt_output_keys(manifest,storage,prefix,'geoparquet')
    for relative in ('metadata/source/source_manifest.json',
                     'metadata/source/version_'+sha((root/'metadata/source_manifest.json').read_bytes())+'_source_manifest.json'):
        snapshot=root/relative
        if snapshot.exists() and snapshot.read_bytes()!=(root/'metadata/source_manifest.json').read_bytes():
            raise CBRSQuarantineError('cached_original_provenance_snapshot_drift')
    if (root/'metadata/resolved_provenance.json').exists():
        resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,ready['version']).metadata)
        if (resolved.get('inputs')!=manifest['inputs'] or resolved.get('processor')!=manifest['processor']
                or strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=resolved):
            raise CBRSQuarantineError('cached_resolved_provenance_drift')
    # code_commit identifies the original immutable construction, including later docs-only commits.
    for name,value in expected_manifest.items():
        if name!='code_commit' and manifest.get(name)!=value:
            raise CBRSQuarantineError('cached_provenance_differs_from_original_capture')
    expected_bytes={'raw/'+ARCHIVE_NAME:captures['official_zip'][0],
        'raw/retrieval.json':encode(cached['official_zip'][1]),
        'metadata/candidate.records.jsonl':b''.join(encode(r) for r in rows),
        'metadata/processing_identity.json':encode(identity),
        'metadata/cbrs_units_source_audit.json':encode(audit),
        'metadata/proposed_source_contract.json':encode(PROPOSAL)}
    expected_bytes.update({f'raw/members/{name}':body for name,body in members.items()})
    if any((root/name).read_bytes()!=body for name,body in expected_bytes.items()):
        raise CBRSQuarantineError('cached_source_or_normalized_records_differ')
    with fiona.open(root/'geopackage'/f'{SLUG}.gpkg') as stream:
        expected_schema={'properties':{k:('int32' if v.startswith('int32') else 'float' if v.startswith('float') else v) for k,v in SCHEMA.items()},'geometry':'Unknown'}
        if dict(stream.schema)!=expected_schema or not pyproj.CRS(stream.crs).equals(pyproj.CRS(4269)):
            raise CBRSQuarantineError('cached_geopackage_schema_or_crs_drift')
        decoded=[{'type':'Feature','properties':dict(r['properties']),'geometry':mapping(shape(r['geometry'])) if r['geometry'] else None} for r in stream]
    if b''.join(encode(r) for r in decoded)!=b''.join(encode(r) for r in rows):
        raise CBRSQuarantineError('cached_geopackage_source_fidelity_failure')
    for candidate in [root/'candidate/typed-source.parquet',root/'geoparquet'/f'{SLUG}.parquet']:
        if not pq.read_table(candidate).equals(typed_table(rows,audit),check_metadata=True):
            raise CBRSQuarantineError('cached_typed_parquet_source_fidelity_failure')
    validate_geopackage_metadata(root/'geopackage'/f'{SLUG}.gpkg',rows)
    validate_source_verification_payloads(root,b''.join(encode(r) for r in rows))



@contextmanager
def candidate_lock(storage: StagingStorageResource, prefix: str):
    """Same cooperating-writer lock for lookup, validation, detach and installation."""
    target=local_candidate_path(storage,prefix)
    target.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:
            raise CBRSQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX)
        yield target
    # Keep the tiny lock inode: unlinking permits a third process to bypass a waiter.


def write_private(root: Path, name: str, body: bytes) -> None:
    path=root/name;path.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    try:
        info=os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1:
            raise CBRSQuarantineError('private_write_alias_or_special_file_refused')
        if os.write(descriptor,body)!=len(body):
            raise CBRSQuarantineError('incomplete_private_write:'+name)
        os.fsync(descriptor)
    finally:os.close(descriptor)
    if path.read_bytes()!=body:
        raise CBRSQuarantineError('incomplete_private_write:'+name)


def commit_complete_candidate(storage: StagingStorageResource,prefix: str,payloads: dict,ready: dict,
                              *, validate_private, seal) -> None:
    """Complete closed private readback, then first seal, then atomic exposure.

    Both callbacks are required: production callers cannot omit semantic validation
    or silently create a discoverable unbound candidate. The common lock spans all.
    """
    with candidate_lock(storage,prefix) as target:
        refuse_observed_loss(storage,ready['version'])
        if target.exists() or target.is_symlink():
            raise CBRSQuarantineError('concurrent_or_incomplete_existing_candidate')
        with tempfile.TemporaryDirectory(prefix='.candidate-build-',dir=target.parent) as tmp:
            root=Path(tmp)
            for name,value in payloads.items():write_private(root,name,value)
            write_private(root,'metadata/cbrs_units_candidate_ready.json',encode(ready))
            expected={**payloads,'metadata/cbrs_units_candidate_ready.json':encode(ready)}
            def readback():
                if set(candidate_inventory(root))!=set(expected):
                    raise CBRSQuarantineError('private_candidate_closed_roster_drift')
                for name,body in expected.items():
                    path=root/name;info=path.lstat()
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or path.read_bytes()!=body:
                        raise CBRSQuarantineError('private_candidate_incomplete_or_changed:'+name)
                validate_private(root)
            readback()
            seal()
            readback()
            os.rename(tmp,target)


def candidate_inventory(root: Path) -> dict:
    inventory={}
    for path in sorted(root.rglob('*')):
        info=path.lstat();name=str(path.relative_to(root))
        if stat.S_ISLNK(info.st_mode):
            inventory[name]={'kind':'symlink','target':os.readlink(path)}
        elif stat.S_ISREG(info.st_mode):
            inventory[name]={'kind':'file','bytes':info.st_size,'sha256':sha256_file(path)}
        elif not stat.S_ISDIR(info.st_mode):
            inventory[name]={'kind':'unsupported','mode':info.st_mode}
    return inventory


def _quarantine_existing_candidate_locked(storage: StagingStorageResource, prefix: str,
                                         expected_inode: tuple[int,int], reason: str) -> str:
    """Caller holds candidate_lock through lookup, validation and this atomic move."""
    target=local_candidate_path(storage,prefix);info=target.lstat()
    if (info.st_dev,info.st_ino)!=expected_inode:
        raise CBRSQuarantineError('existing_candidate_changed_concurrently_not_quarantined')
    inventory=candidate_inventory(target)
    location=f'_quarantine/{SLUG}/versions/{uuid4().hex}'
    destination=Path(storage.local_dir)/storage._ensure_prefixed(location)
    destination.mkdir(parents=True,exist_ok=False)
    os.rename(target,destination/'candidate')
    immutable(storage,location+'/report.json',encode({'status':'invalid_draft_candidate_quarantined',
        'approved':False,'original_prefix':prefix,'reason':reason,'inventory':inventory}))
    return location




def reuse_existing_candidate(storage: StagingStorageResource,prefix: str,version: str,records: bytes,
                             fingerprint: str,rows: list[dict],audit: dict,captures: dict,
                             identity: dict,members: dict) -> dict | None:
    with candidate_lock(storage,prefix) as target:
        refuse_observed_loss(storage,version)
        manifest=provenance(captures,identity=identity,prebuilt_body=derive_native_parquet_bytes(rows,audit))
        manifest['processing_fingerprint']=fingerprint
        if not target.exists():
            if state_present(storage,version):
                load_original_or_record_loss(storage,version,captures,manifest,identity['files'],identity)
            return None
        info=target.lstat();inode=(info.st_dev,info.st_ino)
        ready_key=f'{prefix}/metadata/cbrs_units_candidate_ready.json'
        try:
            original_context=load_original_or_record_loss(storage,version,captures,manifest,identity['files'],identity)
            if not storage.object_exists(ready_key):
                raise CBRSQuarantineError('incomplete_existing_candidate_missing_ready')
            ready = strict_json(storage.read_key(ready_key))
            expected_names = {f'raw/{ARCHIVE_NAME}', 'raw/retrieval.json', f'geopackage/{SLUG}.gpkg', 'candidate/typed-source.parquet', f'geoparquet/{SLUG}.parquet', 'metadata/candidate.records.jsonl', 'metadata/source_manifest.json', 'metadata/processing_identity.json', 'metadata/cbrs_units_source_audit.json', 'metadata/proposed_source_contract.json', 'metadata/geopackage_source_fidelity.json', 'metadata/parquet_source_fidelity.json', 'metadata/baseline_comparison.json','metadata/baseline_differences.jsonl','metadata/baseline_field_changes.jsonl'} | {f'raw/members/{name}' for name in MEMBERS} | {f'raw/captures/{name}{suffix}' for name in URLS for suffix in ('.body','.receipt.json')}
            if set(ready.get('objects', {})) != expected_names or ready.get('version') != version or ready.get('status') != 'working_draft_candidate_ready':
                raise CBRSQuarantineError('existing_candidate_inventory_conflict')
            if set(ready)!={'status','approved','source_sha256','candidate_records_sha256','processing_fingerprint','version','objects'}:
                raise CBRSQuarantineError('existing_candidate_ready_roster_drift')
            if (ready.get('source_sha256') != PIN or ready.get('candidate_records_sha256') != sha(records)
                    or ready.get('processing_fingerprint') != fingerprint or ready.get('approved') is not False):
                raise CBRSQuarantineError('existing_candidate_identity_conflict')
            for name, expected in ready['objects'].items():
                key = f'{prefix}/{name}'
                snapshot = storage.object_snapshot(key)
                if snapshot is None or sha(storage.read_key(key)) != expected or storage.object_snapshot(key) != snapshot:
                    raise CBRSQuarantineError(f'existing_candidate_object_changed:{name}')
            reconcile_cached_candidate(storage,prefix,ready,rows,audit,captures,identity,members,original_context=original_context)
            return {'dataset_slug': SLUG, 'file_slug': SLUG, 'version': version, 'feature_count': len(rows),
                    'status': 'working_draft_candidate_ready', 'processing_fingerprint': fingerprint}

        except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error,fiona.errors.FionaError) as exc:
            retained=_quarantine_existing_candidate_locked(storage,prefix,inode,str(exc))
            raise CBRSQuarantineError(f'{exc}; candidate_quarantined:{retained}') from exc

def stage_cbrs_units_candidate(storage: StagingStorageResource, *, capture_dir: Path | None = None) -> dict:
    require_local(storage)
    captures = acquire(storage,capture_dir)
    body,receipt = captures['official_zip']
    identity=processing_identity()
    try:
        manifest=provenance(captures,identity=identity)
        with tempfile.TemporaryDirectory(prefix='cbrs_units-source-') as tmp:
            directory = Path(tmp)
            rows, audit, members = inspect_archive(body, directory)
            records = b''.join(encode(r) for r in rows)
            fingerprint = sha(encode(identity))
            capture_fingerprint=sha(encode({name:sha(value[0]) for name,value in captures.items()}))
            version = f'v0.0.0-draft-cbrs20241219-{capture_fingerprint[:16]}-{fingerprint[:16]}'
            prefix = f'{SLUG}/{SLUG}/{version}'
            expected_prebuilt=derive_native_parquet_bytes(rows,audit)
            manifest=provenance(captures,identity=identity,prebuilt_body=expected_prebuilt)
            cached=reuse_existing_candidate(storage,prefix,version,records,fingerprint,rows,audit,captures,identity,members)
            if cached is not None:return cached
            manifest['processing_fingerprint']=fingerprint
            with candidate_lock(storage,prefix):
                refuse_observed_loss(storage,version)
                if state_present(storage,version):
                    captures,manifest=load_original_or_record_loss(storage,version,captures,manifest,identity['files'],identity)
            body,receipt=captures['official_zip']
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
            if decoded.read_bytes() != records:
                raise CBRSQuarantineError('geopackage_complete_fidelity_failure')
            table=typed_table(rows,audit)
            parquet = directory / 'typed-source.parquet'
            pq.write_table(table, parquet, compression='zstd')
            decoded.write_bytes(b''.join(encode({'type': 'Feature', 'properties': {k:r[k] for k in SCHEMA},
                'geometry': mapping(shapely.from_wkb(r['geometry']))}) for r in pq.read_table(parquet).to_pylist()))
            if decoded.read_bytes() != records or not pq.read_schema(parquet).equals(table.schema, check_metadata=True):
                raise CBRSQuarantineError('typed_parquet_physical_or_full_row_fidelity_failure')
            if parquet.read_bytes()!=expected_prebuilt:
                raise CBRSQuarantineError('canonical_prebuilt_artifact_bytes_disagree_with_original_reconstruction')
            validate_release_provenance(manifest)
            payloads = {f'raw/{ARCHIVE_NAME}': body, 'raw/retrieval.json': encode(receipt),
                f'geopackage/{SLUG}.gpkg': geopackage.read_bytes(), 'candidate/typed-source.parquet': parquet.read_bytes(), f'geoparquet/{SLUG}.parquet':parquet.read_bytes(),
                'metadata/candidate.records.jsonl': records, 'metadata/source_manifest.json': encode(manifest),
                'metadata/processing_identity.json': encode(identity), 'metadata/cbrs_units_source_audit.json': encode(audit),
                'metadata/proposed_source_contract.json': encode(PROPOSAL),
            }
            payloads.update(source_verification_payloads(records))
            payloads.update({f'raw/members/{name}': value for name,value in members.items()})
            payloads.update({f'raw/captures/{name}.body':value[0] for name,value in captures.items()})
            payloads.update({f'raw/captures/{name}.receipt.json':encode(value[1]) for name,value in captures.items()})
            ready = {'status':'working_draft_candidate_ready', 'approved':False, 'source_sha256':PIN,
                'candidate_records_sha256':sha(records), 'processing_fingerprint':fingerprint, 'version':version,
                'objects':{name:sha(value) for name,value in payloads.items()}}
            def validate_private(root):
                reconcile_cached_candidate(storage,prefix,ready,rows,audit,captures,identity,members,
                    root=root,original_context=(captures,manifest))
            def seal():
                if state_present(storage,version):
                    original,first=load_original_or_record_loss(storage,version,captures,manifest,identity['files'],identity)
                    if not same_captures(original,captures) or encode(first)!=encode(manifest):
                        raise CBRSQuarantineError('concurrent_original_context_changed_retry_without_rebase')
                else:
                    original_construction_bundle(storage,version,captures,manifest,identity['files'],identity=identity,create=True)
            commit_complete_candidate(storage,prefix,payloads,ready,validate_private=validate_private,seal=seal)
            return {'dataset_slug':SLUG, 'file_slug':SLUG, 'version':version, 'feature_count':len(rows),
                    'status':ready['status'], 'processing_fingerprint':fingerprint}
    except (ValueError, TypeError, KeyError, OSError, RuntimeError, zipfile.BadZipFile, sqlite3.Error, fiona.errors.FionaError) as exc:
        location = quarantine(storage, body, receipt, str(exc))
        raise CBRSQuarantineError(f'{exc}; retained:{location}') from exc
