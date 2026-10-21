"""Local-only, unapproved 2022 HIFLD/ORNL campus preservation proposal.

PASDA is a third-party historical mirror, not a current producer feed. Both
native representations and the complete published candidate remain preserved.
No authority, selected release, ID/schema mapping or geometry repair is accepted.
"""
from __future__ import annotations

import collections
import fcntl
import hashlib
import io
import json
import os
import math
import os
import re
import shutil
import sqlite3
import stat
import struct
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from uuid import uuid4

import fiona
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from fiona.io import ZipMemoryFile
from shapely.geometry import shape

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.sources.epa_frs_common import processing_identity as packaged_processing_identity
from dagster_hifld.construction_state import (
    canonical_construction_bundle, load_first_construction, seal_first_construction,
)
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.sources.campus_preservation_spec import INPUTS, ARCHIVES, BASELINE_DUPLICATE_IDS
from dagster_hifld.validation import sha256_file
from dagster_hifld.sources.epa_frs_common import (STAGED_RELEASE_DERIVATIVE_NAMES,
    validate_staged_release_derivatives)

SLUG = 'colleges-and-universities-campuses'
TITLE = '2022 HIFLD/ORNL campus historical preservation candidate'
PRIMARY = 'pasda-hifld-zip'
READY = 'metadata/campus_preservation_ready.json'
LAYER = 'campus_historical_native'
CAPTURES = Path(os.environ.get('HIFLD_CAMPUS_CAPTURE_DIR',
    str(Path(__file__).parents[3] / 'docs/dataset-sources' / SLUG / 'evidence/captures')))


class CampusQuarantineError(ValueError):
    """Changed, incomplete or untrusted state is retained and cannot promote."""


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result: raise CampusQuarantineError('duplicate_json_member')
            result[key] = value
        return result
    value = json.loads(body, object_pairs_hook=unique,
                       parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if type(value) is not dict or 'error' in value:
        raise CampusQuarantineError('expected_nonerror_json_object')
    return value


def local_root(storage):
    # Reject publication/remote targets before source reads or any mutation.
    if (isinstance(storage, PublishedStorageResource) or not isinstance(storage, StagingStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise CampusQuarantineError('explicit_unprefixed_local_staging_required')
    path = Path(storage.local_dir)
    if '..' in path.parts: raise CampusQuarantineError('unsafe_local_root')
    path = path.absolute()
    descriptors = [os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)]
    try:
        for part in path.parts[1:]:
            try: os.mkdir(part, mode=0o700, dir_fd=descriptors[-1])
            except FileExistsError: pass
            descriptors.append(os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                       dir_fd=descriptors[-1]))
        if shutil.disk_usage(path).free < 300_000_000:
            raise CampusQuarantineError('local_space_guard')
    finally:
        for descriptor in reversed(descriptors): os.close(descriptor)
    return path


def safe_tree(path):
    if path.is_symlink(): raise CampusQuarantineError('linked_candidate_root')
    for item in path.rglob('*'):
        info = item.lstat()
        if item.is_symlink() or (not item.is_dir() and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1)):
            raise CampusQuarantineError('linked_or_special_candidate_object')


def checked_path(storage, key):
    root = local_root(storage); path = root/key
    if PurePosixPath(key).is_absolute() or '..' in PurePosixPath(key).parts:
        raise CampusQuarantineError('unsafe_local_object_key')
    for item in [path,*path.parents]:
        if item.is_symlink(): raise CampusQuarantineError('linked_local_object_ancestor')
    return path


def immutable(storage, key, body):
    local_root(storage)
    path = Path(storage.local_dir)/key
    for ancestor in [path, *path.parents]:
        if ancestor.is_symlink(): raise CampusQuarantineError('linked_capture_path')
    if path.exists() and (not path.is_file() or path.stat().st_nlink != 1):
        raise CampusQuarantineError('linked_capture_object')
    snapshot = storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key, body, None)
    elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
        raise CampusQuarantineError('immutable_capture_changed')


def retain_diagnostic(storage, pieces, reason):
    root = checked_path(storage,'_quarantine/'+SLUG+'/input-'+uuid4().hex)
    root.mkdir(parents=True)
    inventory = {}
    for name, body in pieces.items():
        path = root/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(body)
        inventory[name] = {'bytes':len(body), 'sha256':digest(body), 'trust':'untrusted_diagnostic'}
    (root/'incident.json').write_bytes(encode({'approved':False, 'reason':reason, 'objects':inventory}))
    return str(root)


def bounded_regular_read(path, limit):
    """Reject oversized/special inputs before allocation and bound raced growth."""
    observed = path.lstat()
    if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
        raise CampusQuarantineError('linked_or_special_source_capture')
    if observed.st_size > limit: raise CampusQuarantineError('source_capture_size_limit')
    descriptor = os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        current = os.fstat(descriptor)
        if (current.st_dev,current.st_ino,current.st_size,current.st_mtime_ns,current.st_nlink) != (observed.st_dev,observed.st_ino,observed.st_size,observed.st_mtime_ns,1):
            raise CampusQuarantineError('source_capture_changed_before_read')
        with os.fdopen(os.dup(descriptor),'rb') as stream: body = stream.read(limit+1)
        after = os.fstat(descriptor)
        if len(body) > limit: raise CampusQuarantineError('source_capture_size_limit')
        if (after.st_size,after.st_mtime_ns,after.st_nlink) != (current.st_size,current.st_mtime_ns,1) or len(body) != current.st_size:
            raise CampusQuarantineError('source_capture_changed_during_read')
        return body
    finally: os.close(descriptor)


def acquire(storage, capture_directory):
    local_root(storage)
    if capture_directory is None: raise CampusQuarantineError('explicit_preserved_capture_directory_required')
    root = Path(capture_directory).absolute(); pieces = {}; captures = {}
    if any(p.is_symlink() for p in [root, *root.parents]):
        raise CampusQuarantineError('linked_source_capture_directory')
    try:
        for name, pin in INPUTS.items():
            for suffix, limit in [('.body',16_000_000),('.receipt.json',128_000)]:
                path = root/(name+suffix)
                try: body = bounded_regular_read(path,limit)
                except CampusQuarantineError as exc:
                    if str(exc) == 'source_capture_size_limit':
                        pieces[name+suffix+'.oversize-diagnostic.json'] = encode({'trust':'untrusted_diagnostic',
                            'path':str(path),'observed_bytes':path.lstat().st_size,'limit':limit,
                            'body_captured':False,'original_preserved_in_place':True})
                    raise
                pieces[name+suffix] = body
            body = pieces[name+'.body']; receipt = strict_json(pieces[name+'.receipt.json'])
            validate_capture(name,body,receipt)
            captures[name] = (body, receipt)
        return captures
    except (OSError, ValueError, TypeError, KeyError, OverflowError) as exc:
        where = retain_diagnostic(storage, pieces, str(exc))
        raise CampusQuarantineError(str(exc)+'; retained:'+where) from exc


def validate_capture(name, body, receipt):
    pin = INPUTS[name]
    if (len(body) != pin['bytes'] or digest(body) != pin['sha256']
            or receipt.get('requested_url') != pin['url'] or receipt.get('observed_final_url') != pin['final_url']
            or type(receipt.get('status')) is not int or receipt['status'] != 200
            or type(receipt.get('bytes')) is not int or receipt['bytes'] != len(body)
            or receipt.get('sha256') != digest(body)):
        raise CampusQuarantineError('changed_or_incomplete_pinned_input:'+name)
    start = datetime.fromisoformat(receipt['started_at']); end = datetime.fromisoformat(receipt['observed_at'])
    if start.utcoffset() is None or end.utcoffset() is None or start > end:
        raise CampusQuarantineError('invalid_capture_observation_interval')


def processing_identity():
    root = Path(__file__).parents[3]
    identity = packaged_processing_identity(root)
    validate_construction_code_identity(root, identity['code_commit'], identity['files'], identity=identity)
    return identity


def dbf_table(body, expected):
    count = int.from_bytes(body[4:8], 'little'); header = int.from_bytes(body[8:10], 'little')
    size = int.from_bytes(body[10:12], 'little')
    fields = [{'name':body[i:i+11].split(b'\0')[0].decode(), 'type':chr(body[i+11]),
               'width':body[i+16], 'decimals':body[i+17]} for i in range(32,header-1,32)]
    if (count != 5898 or fields != expected or body[header-1] != 13
            or size != 1+sum(f['width'] for f in fields)
            or len(body) not in [header+count*size,header+count*size+1]
            or body[header+count*size:] not in [b'',b'\x1a']):
        raise CampusQuarantineError('native_dbf_census_schema_or_record_drift')
    rows = []
    for i in range(count):
        record = body[header+i*size:header+(i+1)*size]
        if record[0] != 32: raise CampusQuarantineError('deleted_or_invalid_native_record')
        row = {}; offset = 1
        for field in fields:
            raw = record[offset:offset+field['width']]; offset += field['width']; text = raw.decode('utf-8').strip()
            if field['type'] == 'C': value = raw.decode('utf-8').rstrip(' ') or None
            elif field['type'] == 'D': value = date(int(text[:4]),int(text[4:6]),int(text[6:])) if text not in ['', '00000000'] else None
            elif field['type'] in ['N','F']:
                value = None if not text or text == '*'*field['width'] else float(text) if field['decimals'] else int(text)
            else: raise CampusQuarantineError('unsupported_native_dbf_type')
            if isinstance(value,float) and not math.isfinite(value): raise CampusQuarantineError('nonfinite_native_dbf_value')
            row[field['name']] = value
        rows.append(row)
    return rows


def decode_archive(name, body):
    spec = ARCHIVES[name]; archive = zipfile.ZipFile(io.BytesIO(body))
    if archive.testzip() is not None or len(archive.namelist()) != len(set(archive.namelist())):
        raise CampusQuarantineError('archive_crc_or_duplicate_member')
    members = {i.filename:archive.read(i) for i in archive.infolist()}
    if ({k:{'bytes':len(v),'sha256':digest(v)} for k,v in members.items()} != spec['members']
            or any(PurePosixPath(k).is_absolute() or '..' in PurePosixPath(k).parts for k in members)):
        raise CampusQuarantineError('complete_original_archive_roster_drift')
    dbf = next(v for k,v in members.items() if k.lower().endswith('.dbf'))
    rows = dbf_table(dbf, spec['dbf_fields'])
    shpname = next(k for k in members if k.lower().endswith('.shp')); shp = members[shpname]
    if len(shp) != struct.unpack_from('>i',shp,24)[0]*2 or struct.unpack_from('<i',shp,32)[0] != 5:
        raise CampusQuarantineError('native_shapefile_header_drift')
    raw_coordinates = []; offset = 100
    while offset < len(shp):
        number, words = struct.unpack_from('>ii',shp,offset); record = shp[offset+8:offset+8+words*2]
        if number != len(raw_coordinates)+1 or struct.unpack_from('<i',record)[0] != 5:
            raise CampusQuarantineError('native_shape_record_or_type_drift')
        parts, points = struct.unpack_from('<ii',record,36); starts = struct.unpack_from('<'+'i'*parts,record,44)
        if not parts or starts[0] != 0 or list(starts) != sorted(set(starts)):
            raise CampusQuarantineError('native_shape_ring_roster_drift')
        begin = 44+4*parts
        if begin+16*points != len(record): raise CampusQuarantineError('native_shape_length_drift')
        raw_coordinates.append(collections.Counter(struct.unpack_from('<dd',record,begin+i*16) for i in range(points)))
        offset += 8+words*2
    if offset != len(shp) or len(raw_coordinates) != len(rows): raise CampusQuarantineError('native_shape_census_drift')
    geometries = []; ids = []
    with ZipMemoryFile(body) as memory:
        with memory.open(shpname) as source:
            if source.crs.to_epsg() != spec['crs'] or len(source) != 5898:
                raise CampusQuarantineError('native_crs_or_full_count_drift')
            for i, feature in enumerate(source):
                if dict(feature.properties) != {k:(v.isoformat() if isinstance(v,date) else v) for k,v in rows[i].items()}:
                    raise CampusQuarantineError('native_dbf_raw_and_reader_values_disagree')
                geometry = shape(feature.geometry)
                if raw_coordinates[i] != collections.Counter(tuple(p) for p in shapely.get_coordinates(geometry)):
                    raise CampusQuarantineError('native_geometry_coordinate_multiplicity_changed')
                ids.append(feature.id); geometries.append(geometry)
    uids = [r['UNIQUEID'] for r in rows]
    invalid = sorted(uid for uid,g in zip(uids,geometries) if not g.is_valid)
    if (len(set(uids)) != 5898 or any(type(v) is not str or not v for v in uids)
            or len(set(ids)) != 5898 or invalid != spec['invalid_UNIQUEIDs']
            or dict(collections.Counter(r['STATE'] for r in rows)) != spec['state_counts']):
        raise CampusQuarantineError('source_ids_coverage_or_quality_drift')
    arrays = [pa.array(ids,type=pa.string())]; schema = [pa.field('__source_fid',pa.string())]
    for field in spec['dbf_fields']:
        kind = pa.string() if field['type']=='C' else pa.date32() if field['type']=='D' else pa.float64() if field['decimals'] else pa.int64() if field['width']>9 else pa.int32()
        arrays.append(pa.array([r[field['name']] for r in rows],type=kind)); schema.append(pa.field(field['name'],kind))
    arrays.append(pa.array(shapely.to_wkb(geometries).tolist(),type=pa.binary())); schema.append(pa.field('geometry',pa.binary()))
    table = pa.Table.from_arrays(arrays,schema=pa.schema(schema))
    audit = {'rows':5898,'native_crs':'EPSG:'+str(spec['crs']),'physical_dbf_fields':spec['dbf_fields'],
             'invalid_UNIQUEIDs':invalid,'invalid_geometry_count':len(invalid),'geometry_repaired':False,
             'state_counts':spec['state_counts'],'all_native_coordinate_multiplicities_retained':True,
             'source_fid_scope':'Original file-local reader FID; proposed additional column, never a substitute published identifier',
             'null_counts':{n:table[n].null_count for n in table.column_names},'members':spec['members']}
    return table, audit


def geotable(table, crs):
    geometries = shapely.from_wkb(table['geometry'].to_pylist())
    geo = {'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB',
           'geometry_types':sorted(set(shapely.get_type_id(geometries).tolist())),
           'crs':pyproj.CRS(crs).to_json_dict(),'bbox':shapely.total_bounds(geometries).tolist()}}}
    geo['columns']['geometry']['geometry_types'] = sorted({g.geom_type for g in geometries})
    return table.replace_schema_metadata({b'geo':encode(geo)})


def parquet_bytes(table):
    stream = pa.BufferOutputStream(); pq.write_table(table,stream,compression='zstd')
    return stream.getvalue().to_pybytes()


def write_geopackage(table, path):
    pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='geometry',geometry_type='Unknown',
                       crs='EPSG:3857',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'geometry'})
    with sqlite3.connect(path) as db: db.execute("UPDATE gpkg_contents SET last_change='2022-07-18T00:00:00.000Z'")


def baseline_report(body, tables):
    baseline = pq.read_table(pa.BufferReader(body)); old = baseline.to_pylist()
    if len(old) != 5898: raise CampusQuarantineError('published_candidate_census_drift')
    byid = {r['UNIQUEID']:r for r in old}
    if len(byid) != 5898: raise CampusQuarantineError('published_UNIQUEID_missing_or_duplicate')
    groups = collections.defaultdict(list)
    for row in old: groups[str(row['id'])].append(row['UNIQUEID'])
    duplicates = {k:v for k,v in groups.items() if len(v)>1}
    if duplicates != BASELINE_DUPLICATE_IDS: raise CampusQuarantineError('published_original_duplicate_ID_roster_drift')
    comparisons = []
    for row in tables[PRIMARY].to_pylist():
        before = byid[row['UNIQUEID']]
        fields = [n for n in baseline.column_names if n not in ['geometry','id','bbox']]
        differences = [n for n in fields if before[n] != (row[n].isoformat() if isinstance(row[n],date) else row[n])]
        if differences: raise CampusQuarantineError('preserved_attribute_relationship_drift')
        comparisons.append({'UNIQUEID':row['UNIQUEID'],'published_id':before['id'],'native_file_fid':row['__source_fid'],
                            'all24_attribute_values_match':True,'published_WKB_sha256':digest(before['geometry']),
                            'native_WKB_sha256':digest(row['geometry'])})
    return {'schema_version':1,'approved':False,'status':'blocked','acceptance':'not_established',
            'reason_codes':['source_historical_release_selection_unapproved','published_identifier_mapping_unapproved',
                            'source_types_schema_geometry_precision_unapproved','source_15_invalid_polygons_preserved'],
            'populations':{'published_rows':5898,'native_rows':5898},'published_duplicate_ids':duplicates,
            'comparison_scope':'Complete values and original IDs, diagnostic original UNIQUEID join only; no geometry reprojection/repair or approved mapping',
            'all_rows':comparisons}


def manifest_for(captures, identity, parquet):
    if set(captures) != set(INPUTS): raise CampusQuarantineError('missing_extra_complete_input_roster')
    inputs = []
    for name in INPUTS:
        body, receipt = captures[name]
        validate_capture(name,body,receipt)
        mirror = name in ARCHIVES
        producer = 'ORNL/HIFLD attribution in preserved original metadata; exact selected release unverified' if mirror or name=='baseline' else 'Oak Ridge National Laboratory' if name=='ornl-geospatial-data-sets' else 'DHS HIFLD federal catalog' if name=='data-gov-raw' else 'HIFLD historical catalog, prior HIFLD Next processing retained'
        inputs.append({'input_id':name+':'+digest(body),'producer':producer,'host':urlsplit(receipt['requested_url']).hostname,
                       'role':'mirror' if mirror else 'archive' if name in ['baseline','baseline-collection'] else 'producer_publication',
                       'input_kind':'third_party_historical_dataset_mirror' if mirror else 'unapproved_published_candidate' if name=='baseline' else 'provenance_reference_only',
                       'authority_acceptance':False,
                       'canonical_url':receipt['requested_url'],'retrieval_url':receipt['requested_url'],
                       'observed_final_url':receipt['observed_final_url'],'retrieved_at':receipt['observed_at'],
                       'http_status':200,'bytes':len(body),'sha256':digest(body),
                       'license':'Original archived FGDC claims public domain/public use; exact source and rights review unresolved.' if mirror else 'Preserved provenance/candidate reference; no additional rights or authority inferred.',
                       'attribution':'Original ORNL/HIFLD credit and PASDA mirror role retained; HIFLD Next is reconstruction processor only.',
                       'release_date_unknown_reason':'Proposed historical preservation candidate: archived FGDC says2022-07-18; exact selected-release bytes/delegation not accepted. Observation time is not release time.'})
    manifest = {'schema_version':1,'processor':'HIFLD Next','approved':False,'title':TITLE,
                'code_commit':identity['code_commit'],'code_identity_scheme':identity['code_identity_scheme'],
                'processing_fingerprint':digest(encode(identity)),'inputs':inputs,
                'source_selection_status':'Unapproved historical preservation proposal; PASDA is not a maintained producer feed',
                'transformations':[{'operation':'decode_both_complete_original_archives','parameters':{'native_CRSs':['EPSG:4326','EPSG:3857'],
                    'all_native_fields_and_file_FIDs':'retained, additional __source_fid mapping unapproved','DBF_dates':'native date32, no approved migration to published strings',
                    'geometry':'native original WKB, all15invalid polygons retained; no repair, projection, polygon wrapping, buffers or hulls'}},
                    {'operation':'local_historical_candidate_construction','parameters':{'proposed_canonical_input':PRIMARY,'selection_approved':False,
                    'complete_published_candidate':'retained losslessly with duplicateoriginalids, never an accepted baseline','refresh':'Explicit preserved input reconstruction/integrity only; no network or autonomous archive polling'}}],
                'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Retain every native field/date/width interpretation and original file FID; published schema not accepted.',
                    'objects':[{'path':'geoparquet/'+SLUG+'.parquet','sha256':digest(parquet),'bytes':len(parquet)}]}}},
                'unsupported_output_formats':{'shapefile_zip':'Both original shapefile packages retained; additional FID/output mapping not approved.',
                    'pmtiles':'Geometry quantization/repair and published identifier mapping are unapproved.'}}
    validate_release_provenance(manifest)
    return manifest


def source_payloads(captures, identity, audits, report, manifest, tables):
    values = {'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),
              'metadata/campus_source_audit.json':encode({'approved':False,'source_selection_approved':False,'archives':audits}),
              'metadata/baseline_comparison.json':encode(report),
              'candidate/native_wgs84.parquet':parquet_bytes(geotable(tables['pasda-original-zip'],4326)),
              'geoparquet/'+SLUG+'.parquet':parquet_bytes(geotable(tables[PRIMARY],3857))}
    for name,(body,receipt) in captures.items():
        values['raw/captures/'+name+'.body'] = body
        values['raw/captures/'+name+'.receipt.json'] = encode(receipt)
    return values


def reconcile_native(root, table):
    meta, observed = pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    if meta['crs'] != 'EPSG:3857' or not observed.equals(table):
        raise CampusQuarantineError('fresh_native_values_types_ids_or_WKB_disagree')
    with tempfile.TemporaryDirectory(prefix='campus-native-comparison-') as temporary:
        fresh = Path(temporary)/'fresh.gpkg'; write_geopackage(table,fresh)
        if fresh.read_bytes() != (root/'geopackage'/f'{SLUG}.gpkg').read_bytes():
            raise CampusQuarantineError('fresh_native_complete_GPKG_container_disagrees')


@contextmanager
def version_lock(storage, version):
    folder = checked_path(storage,'_locks'); folder.mkdir(exist_ok=True)
    if folder.is_symlink(): raise CampusQuarantineError('linked_version_lock_directory')
    descriptor = os.open(folder/(SLUG+'-'+version+'.lock'),os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1: raise CampusQuarantineError('linked_version_lock')
        fcntl.flock(descriptor,fcntl.LOCK_EX); local_root(storage); yield
    finally: os.close(descriptor)


def original_context(storage, key, version, fp, captures, identity, proposed):
    raw = load_first_construction(storage,key,version,fp)
    payload = json.loads(raw)['payload']
    if set(payload) != {'manifest','captures'} or set(payload['captures']) != set(INPUTS):
        raise CampusQuarantineError('original_construction_roster_drift')
    original = {}
    for name, claim in payload['captures'].items():
        expected = f'_raw-captures/{SLUG}/{INPUTS[name]["sha256"]}/{name}.body'
        if set(claim) != {'body_key','receipt'} or claim['body_key'] != expected:
            raise CampusQuarantineError('original_construction_body_key_drift')
        body = storage.read_key(expected)
        if body != captures[name][0]: raise CampusQuarantineError('original_construction_input_bytes_changed')
        original[name] = (body,claim['receipt'])
    expected = manifest_for(original,identity,parquet_bytes(geotable(proposed,3857)))
    manifest = payload['manifest']; validate_release_provenance(manifest)
    if set(manifest) != set(expected) or any(manifest[k] != v for k,v in expected.items() if k != 'code_commit'):
        raise CampusQuarantineError('original_construction_full_manifest_drift')
    validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
    return original,manifest


def observed_trust_loss(storage, version, fp, *, candidate=None, restored_manifest=None):
    """Remember an OBSERVED missing/invalid witness before detaching its cache.

    This is a refusal journal, not authority to recreate the original context.
    Exact trusted outside records must be restored; a cached manifest is merely
    an additional observed claim against which that restoration is checked.
    Unobserved simultaneous loss of every outside record cannot be detected.
    """
    key = '_construction-incidents/'+SLUG+'/'+version+'/incident.json'
    folder = checked_path(storage,'_construction-incidents/'+SLUG+'/'+version)
    path = checked_path(storage,key)
    if not folder.exists() and candidate is None: return False
    if not folder.exists():
        # Persist refusal existence first. Even an interrupted body write leaves
        # a fail-closed incident; never reconstruct authority from this marker.
        folder.mkdir(parents=True)
        descriptor = os.open(folder.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
        claim = None; manifest_path = candidate/'metadata/source_manifest.json'
        try:
            descriptor = os.open(manifest_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            try:
                info = os.fstat(descriptor)
                if stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= 2_000_000:
                    with os.fdopen(os.dup(descriptor),'rb') as stream: claim = digest(stream.read())
            finally: os.close(descriptor)
        except FileNotFoundError: pass
        immutable(storage,key,encode({'schema_version':1,'version':version,'processing_fingerprint':fp,
            'status':'observed_original_construction_trust_loss','approved':False,
            'observed_cached_manifest_sha256':claim}))
    try: body = bounded_regular_read(path,128_000)
    except FileNotFoundError as exc: raise CampusQuarantineError('incomplete_observed_trust_loss_journal') from exc
    value = strict_json(body); claim = value.get('observed_cached_manifest_sha256')
    if claim is not None and (type(claim) is not str or re.fullmatch('[0-9a-f]{64}',claim) is None):
        raise CampusQuarantineError('invalid_observed_trust_loss_claim')
    expected = {'schema_version':1,'version':version,'processing_fingerprint':fp,
        'status':'observed_original_construction_trust_loss','approved':False,'observed_cached_manifest_sha256':claim}
    if body != encode(expected): raise CampusQuarantineError('invalid_observed_trust_loss_journal')
    if restored_manifest is not None and claim is not None and digest(encode(restored_manifest)) != claim:
        raise CampusQuarantineError('restored_original_manifest_differs_from_observed_candidate_claim')
    return True


def reconcile_cache(storage, prefix, expected, table, version, fp):
    root = checked_path(storage,prefix); safe_tree(root)
    ready = strict_json((root/READY).read_bytes())
    owned = set(expected)|{'geopackage/'+SLUG+'.gpkg'}
    if (set(ready) != {'version','processing_fingerprint','approved','objects'} or ready['version'] != version
            or ready['processing_fingerprint'] != fp or ready['approved'] is not False or set(ready['objects']) != owned):
        raise CampusQuarantineError('ready_identity_or_closed_roster_drift')
    for name, body in expected.items():
        if (root/name).read_bytes() != body: raise CampusQuarantineError('source_owned_full_bytes_drift:'+name)
    for name,pin in ready['objects'].items():
        if pin != {'sha256':sha256_file(root/name),'bytes':(root/name).stat().st_size}:
            raise CampusQuarantineError('source_owned_object_pin_drift')
    canonical_ready = {'version':version,'processing_fingerprint':fp,'approved':False,
        'objects':{name:{'sha256':sha256_file(root/name),'bytes':(root/name).stat().st_size} for name in owned}}
    if (root/READY).read_bytes() != encode(canonical_ready):
        raise CampusQuarantineError('noncanonical_or_changed_ready_receipt')
    manifest = strict_json(expected['metadata/source_manifest.json'])
    snapshot = 'metadata/source/version_'+digest(expected['metadata/source_manifest.json'])+'_source_manifest.json'
    derivatives = {'metadata/source/source_manifest.json',snapshot,'metadata/source/data_dictionary.json','metadata/source/quality_manifest.json',
                   'metadata/data_dictionary.json','metadata/quality_manifest.json','metadata/geoparquet_layout.json','metadata/resolved_provenance.json'} | STAGED_RELEASE_DERIVATIVE_NAMES
    actual = {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    if actual-(owned|{READY}|derivatives) or not (owned|{READY}).issubset(actual):
        raise CampusQuarantineError('unrostered_or_missing_candidate_object')
    try: validate_staged_release_derivatives(storage,prefix,{f'{prefix}/{name}' for name in actual})
    except (OSError,UnicodeError,ValueError,TypeError) as exc: raise CampusQuarantineError('invalid_staged_release_derivatives') from exc
    for name in ['metadata/source/source_manifest.json',snapshot]:
        if (root/name).exists() and (root/name).read_bytes() != expected['metadata/source_manifest.json']:
            raise CampusQuarantineError('authored_manifest_snapshot_changed')
    if (root/'metadata/resolved_provenance.json').exists():
        resolved = provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata)
        if resolved.get('inputs') != manifest['inputs'] or strict_json((root/'metadata/resolved_provenance.json').read_bytes()) != resolved:
            raise CampusQuarantineError('resolved_provenance_changed')
    reconcile_native(root,table)


def stage_campus_preservation(storage, capture_directory):
    root = local_root(storage); captures = acquire(storage,capture_directory)
    identity = processing_identity(); fp = digest(encode(identity))
    core = digest(encode({name:INPUTS[name]['sha256'] for name in INPUTS}))
    version = 'v0.0.0-draft-campus2022-'+core[:12]+'-'+fp[:16]
    prefix = SLUG+'/'+SLUG+'/'+version; target = checked_path(storage,prefix)
    key = '_construction-state/'+SLUG+'/'+version+'.json'
    for name,(body,receipt) in captures.items():
        raw = f'_raw-captures/{SLUG}/{digest(body)}/{name}.body'
        immutable(storage,raw,body); immutable(storage,raw+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
    tables = {}; audits = {}
    for name in ARCHIVES: tables[name],audits[name] = decode_archive(name,captures[name][0])
    report = baseline_report(captures['baseline'][0],tables)
    parquet = parquet_bytes(geotable(tables[PRIMARY],3857)); manifest = manifest_for(captures,identity,parquet)
    with version_lock(storage,version):
        state = root/key
        has_history = state.exists() or state.with_name(state.name+'.first-construction.json').exists() or state.with_name(state.name+'.evidence').exists()
        if target.exists():
            can_detach = True
            try:
                try:
                    captures,manifest = original_context(storage,key,version,fp,captures,identity,tables[PRIMARY])
                    observed_trust_loss(storage,version,fp,restored_manifest=manifest)
                except (ValueError,OSError,TypeError,KeyError,RuntimeError):
                    # Commit the refusal outside the candidate BEFORE detach.
                    # Otherwise total witness loss followed by quarantine could
                    # look like first use on the next call and reset receipts.
                    try: observed_trust_loss(storage,version,fp,candidate=target)
                    except (ValueError,OSError,TypeError,KeyError,RuntimeError):
                        can_detach = False
                        raise
                    raise
                expected = source_payloads(captures,identity,audits,report,manifest,tables)
                reconcile_cache(storage,prefix,expected,tables[PRIMARY],version,fp)
            except (ValueError,OSError,TypeError,KeyError,RuntimeError,sqlite3.Error) as exc:
                if not can_detach:
                    raise CampusQuarantineError('trust_loss_journal_failed_candidate_retained_untrusted:'+str(exc)) from exc
                quarantine = checked_path(storage,'_quarantine/'+SLUG+'/invalid-candidate-'+uuid4().hex)
                quarantine.parent.mkdir(parents=True,exist_ok=True); os.rename(target,quarantine)
                raise CampusQuarantineError('invalid_candidate_preserved_outside_discovery:'+str(quarantine)+':'+str(exc)) from exc
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':len(tables[PRIMARY]),'reused':True,
                    'source_invalid_geometry_count':audits[PRIMARY]['invalid_geometry_count'],'source_selection_approved':False}
        if has_history or observed_trust_loss(storage,version,fp):
            captures,manifest = original_context(storage,key,version,fp,captures,identity,tables[PRIMARY])
            observed_trust_loss(storage,version,fp,restored_manifest=manifest)
        else:
            validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
        expected = source_payloads(captures,identity,audits,report,manifest,tables)
        # Provenance and actual Git identity are checked before ANY candidate write.
        validate_release_provenance(manifest)
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'],identity=identity)
        building = checked_path(storage,'_building/'+SLUG); building.mkdir(parents=True,exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix='candidate-',dir=building))
        try:
            for name,body in expected.items():
                path = temporary/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes(body)
            (temporary/'geopackage').mkdir(); write_geopackage(tables[PRIMARY],temporary/'geopackage'/f'{SLUG}.gpkg')
            reconcile_native(temporary,tables[PRIMARY])
            objects = {str(p.relative_to(temporary)):{'sha256':sha256_file(p),'bytes':p.stat().st_size} for p in temporary.rglob('*') if p.is_file()}
            (temporary/READY).write_bytes(encode({'version':version,'processing_fingerprint':fp,'approved':False,'objects':objects}))
            bundle = canonical_construction_bundle(version,fp,{'manifest':manifest,'captures':{
                n:{'body_key':f'_raw-captures/{SLUG}/{INPUTS[n]["sha256"]}/{n}.body','receipt':r} for n,(_,r) in captures.items()}})
            # Late seal: a failed private payload/native validation/ready write cannot establish history.
            original = seal_first_construction(storage,key,version,fp,bundle)
            if original != bundle: raise CampusQuarantineError('concurrent_original_context_differs')
            target.parent.mkdir(parents=True,exist_ok=True); os.rename(temporary,target)
        except BaseException:
            if temporary.exists():
                quarantine = checked_path(storage,'_quarantine/'+SLUG+'/failed-private-'+uuid4().hex)
                quarantine.parent.mkdir(parents=True,exist_ok=True); os.rename(temporary,quarantine)
            raise
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':len(tables[PRIMARY]),'reused':False,
            'source_invalid_geometry_count':audits[PRIMARY]['invalid_geometry_count'],'source_selection_approved':False}
