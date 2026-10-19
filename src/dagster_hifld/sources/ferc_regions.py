"""Explicit pinned historical FERC preservation; no current-feed or acceptance claim.

The complete PASDA original retains five CONUS regions and two invalid polygons.
Only local explicitly selected captures are read. No HTTP feed or automatic
replacement selection is implemented. All release decisions remain external.
"""
from __future__ import annotations

import collections
import fcntl
import hashlib
import importlib.metadata
import io
import json
import os
import platform
import re
import shutil
import sqlite3
import stat
import struct
import subprocess
import tempfile
import zipfile
from datetime import datetime
from decimal import Decimal, DecimalException
from pathlib import Path, PurePosixPath
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.sources.ferc_regions_spec import SPEC
from dagster_hifld.validation import sha256_file

SLUG='ferc-regions'
LAYER='FERC_Regions'
READY='metadata/ferc_candidate_ready.json'
INPUTS=SPEC['inputs']
LICENSE='Captured historical PASDA metadata claims None (Public Use), with HSIP ORNL/LANL/INL/NGA credit; applicability/rights acceptance remains unresolved.'

class FercPreservationError(ValueError):
    """Incomplete, changed or untrusted input/candidate remains outside discovery."""


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise FercPreservationError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise FercPreservationError('metadata_must_be_object_without_error')
    return value


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise FercPreservationError('immutable_capture_changed:'+key)


def processing_identity():
    root=Path(__file__).parents[3]
    return {'python':platform.python_version(),'implementation':platform.python_implementation(),'platform':platform.platform(),
        'distributions':sorted((d.metadata['Name'],d.version) for d in importlib.metadata.distributions()),
        'gdal':pyogrio.__gdal_version_string__,'geos':shapely.geos_version_string,'proj':pyproj.proj_version_str,
        'files':{str(p.relative_to(root)):sha256_file(p) for p in sorted(root.joinpath('src/dagster_hifld').rglob('*.py'))+[root/'uv.lock',root/'pyproject.toml']},
        'verifier_sha256':verifier_implementation_sha256()}


def observed_trust_loss(storage,version,fingerprint,*,candidate=None,restored_manifest=None):
    """Persist refusal before detaching an observed cache with lost trust.

    This is not construction authority: recovery still needs the exact original
    outside witnesses. The cached manifest hash only checks that restoration.
    Loss of every witness before any observation cannot be detected here.
    """
    key=f'_construction-incidents/{SLUG}/{version}/incident.json'
    folder=Path(storage.local_dir)/f'_construction-incidents/{SLUG}/{version}'
    path=Path(storage.local_dir)/key
    if not folder.exists() and candidate is None:return False
    if not folder.exists():
        # Commit refusal existence before the body. Interrupted writes must not
        # make a later refresh appear to be an unobserved first construction.
        folder.mkdir(parents=True)
        descriptor=os.open(folder.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
        claim=None;manifest_path=candidate/'metadata/source_manifest.json'
        try:
            descriptor=os.open(manifest_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            try:
                info=os.fstat(descriptor)
                if stat.S_ISREG(info.st_mode)and info.st_nlink==1 and info.st_size<=2_000_000:
                    with os.fdopen(os.dup(descriptor),'rb')as stream:observed=stream.read(2_000_001)
                    after=os.fstat(descriptor)
                    if(len(observed)>2_000_000 or len(observed)!=info.st_size
                       or(info.st_size,info.st_mtime_ns)!=(after.st_size,after.st_mtime_ns)):
                        raise FercPreservationError('observed_manifest_changed_during_bounded_read')
                    claim=digest(observed)
            finally:os.close(descriptor)
        except FileNotFoundError:pass
        immutable(storage,key,encode({'schema_version':1,'version':version,'processing_fingerprint':fingerprint,
            'status':'observed_original_construction_trust_loss','approved':False,'observed_cached_manifest_sha256':claim}))
    try:descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    except FileNotFoundError as exc:raise FercPreservationError('incomplete_observed_trust_loss_journal')from exc
    try:
        info=os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1 or info.st_size>128_000:raise FercPreservationError('invalid_observed_trust_loss_journal_file')
        with os.fdopen(os.dup(descriptor),'rb')as stream:body=stream.read(128_001)
    finally:os.close(descriptor)
    value=strict_json(body);claim=value.get('observed_cached_manifest_sha256')
    if claim is not None and(type(claim)is not str or re.fullmatch('[0-9a-f]{64}',claim)is None):raise FercPreservationError('invalid_observed_trust_loss_claim')
    expected={'schema_version':1,'version':version,'processing_fingerprint':fingerprint,
        'status':'observed_original_construction_trust_loss','approved':False,'observed_cached_manifest_sha256':claim}
    if body!=encode(expected):raise FercPreservationError('invalid_observed_trust_loss_journal')
    if restored_manifest is not None and claim is not None and digest(encode(restored_manifest))!=claim:raise FercPreservationError('restored_original_manifest_differs_from_observed_candidate_claim')
    return True



def safe_local_root(value):
    """Keep lexical roots: resolving first would conceal ancestor aliases."""
    path=Path(value)
    if '..' in path.parts:raise FercPreservationError('parent_traversal_root_refused')
    path=path.absolute()
    for parent in [*reversed(path.parents),path]:
        try:info=parent.lstat()
        except FileNotFoundError:continue
        if not stat.S_ISDIR(info.st_mode):raise FercPreservationError('root_or_ancestor_not_directory_or_alias')
    return path


def local_only(storage):
    if (not isinstance(storage,StagingStorageResource)or isinstance(storage,PublishedStorageResource)
        or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise FercPreservationError('draft_requires_explicit_isolated_local_staging')
    root=safe_local_root(storage.local_dir)
    ancestor=next(p for p in [root,*root.parents]if p.exists())
    if shutil.disk_usage(ancestor).free<300_000_000:raise FercPreservationError('local_space_guard')
    if root.exists():
        for path in root.rglob('*'):
            info=path.lstat()
            if stat.S_ISLNK(info.st_mode)or(stat.S_ISREG(info.st_mode)and info.st_nlink!=1):raise FercPreservationError('staging_alias_refused')
            if not(stat.S_ISDIR(info.st_mode)or stat.S_ISREG(info.st_mode)):raise FercPreservationError('staging_special_file_refused')
    return root


def bounded_read(path,limit):
    """Reject aliases, FIFO/devices and oversized files before allocation."""
    path=Path(path)
    safe_local_root(path.parent)
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        before=os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode)or before.st_nlink!=1:raise FercPreservationError('capture_not_single_regular_file')
        if before.st_size>limit:raise FercPreservationError('capture_size_limit_before_read')
        with os.fdopen(os.dup(descriptor),'rb')as stream:body=stream.read(limit+1)
        after=os.fstat(descriptor)
        if len(body)>limit or len(body)!=before.st_size or (before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):
            raise FercPreservationError('capture_changed_during_bounded_read')
        return body
    finally:os.close(descriptor)


def quarantine(storage,captures,reason):
    """Reference retained immutable raw bytes; never multiply large failed bodies."""
    key=f'_quarantine/{SLUG}/{uuid4().hex}/reason.json'
    entries={}
    for name,(body,receipt)in captures.items():
        body_key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body'
        immutable(storage,body_key,body)
        entries[name]={'body_key':body_key,'sha256':digest(body),'bytes':len(body),'attempted_receipt':receipt}
    immutable(storage,key,encode({'approved':False,'reason':reason,'retained_captures':entries}))
    return key


def acquire(storage,capture_dir):
    base=local_only(storage)
    if not capture_dir:raise FercPreservationError('explicit_pinned_capture_directory_required_no_network_feed')
    directory=safe_local_root(capture_dir);captures={};attempts=[]
    try:
        for name,pin in INPUTS.items():
            attempts.append({'name':name,'original_body_path':str(directory/(name+'.body')),
                'original_receipt_path':str(directory/(name+'.receipt.json')),'local_originals_preserved_in_place':True})
            body=bounded_read(directory/(name+'.body'),50_000_000 if name=='pasda_original'else 2_000_000)
            raw_key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body'
            # The complete attempted binary exists before a receipt is opened.
            if storage.object_snapshot(raw_key)is None and shutil.disk_usage(next(p for p in [base,*base.parents]if p.exists())).free<302_000_000+len(body):
                raise FercPreservationError('space_guard_before_new_raw_capture_original_preserved_in_place')
            immutable(storage,raw_key,body)
            diagnostic={'body_key':raw_key,'sha256':digest(body),'bytes':len(body),'receipt_validated':False}
            captures[name]=(body,diagnostic)
            raw_receipt=bounded_read(directory/(name+'.receipt.json'),65_536)
            immutable(storage,raw_key+'.receipt-original-'+digest(raw_receipt)+'.bin',raw_receipt)
            diagnostic['original_receipt_key']=raw_key+'.receipt-original-'+digest(raw_receipt)+'.bin'
            receipt=strict_json(raw_receipt)
            validate_receipt(name,body,receipt)
            if len(body)!=pin['bytes']or digest(body)!=pin['sha256']:raise FercPreservationError('pinned_historical_input_changed:'+name)
            captures[name]=(body,receipt)
        return captures
    except(ValueError,OSError,TypeError,KeyError)as exc:
        where=quarantine(storage,captures,{'reason':str(exc),'attempted_local_originals':attempts})
        raise FercPreservationError(f'{exc};retained:{where}')from exc


def validate_receipt(name,body,receipt):
    pin=INPUTS[name];url=pin['url']
    requested=receipt.get('requested_url',receipt.get('url'))
    final=receipt.get('observed_final_url',receipt.get('final_url'))
    if(requested!=url or final!=url or type(receipt.get('status'))is not int or receipt['status']!=200
       or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body)or receipt.get('sha256')!=digest(body)
       or receipt.get('complete_body')is not True):raise FercPreservationError('receipt_identity_status_completeness_or_pin_mismatch:'+name)
    if name=='pasda_original':producer='Pennsylvania Spatial Data Access (PASDA), historical third-party mirror';role='mirror';host='www.pasda.psu.edu'
    elif name=='ferc_boundary_map':producer='Federal Energy Regulatory Commission';role='producer_publication';host='www.ferc.gov'
    else:producer='DHS HIFLD, record distributed through Data.gov';role='producer_publication';host='catalog.data.gov'
    return {'input_id':name+':'+digest(body),'producer':producer,'host':host,'role':role,
        'canonical_url':url,'retrieval_url':requested,'observed_final_url':final,
        'retrieved_at':receipt.get('observed_at'),'http_status':receipt['status'],'bytes':len(body),'sha256':digest(body),
        'license':LICENSE,'attribution':'Historical HIFLD/HSIP ORNL/LANL/INL/NGA credits; FERC office-jurisdiction PDF and exact DHS product record are contextual evidence, not delegation of the mirror.',
        'release_date_unknown_reason':'No exact producer release day established. Source validation2022-06-03, PASDA metadata update2022-12-10, archive directory2025 timestamp and retrieval dates are not release dates.'}


def dbf_rows(body):
    if len(body)<33:raise FercPreservationError('short_dbf')
    count,header,width=struct.unpack('<IHH',body[4:12])
    fields=[]
    for offset in range(32,header-1,32):
        field=body[offset:offset+32]
        fields.append([field[:11].split(b'\0')[0].decode('ascii'),chr(field[11]),field[16],field[17]])
    if(count!=5 or header!=32+32*len(fields)+1 or header>len(body)or body[header-1]!=13
       or width!=1+sum(f[2]for f in fields)or encode(fields)!=encode(SPEC['dbf_fields'])
       or len(body)not in(header+count*width,header+count*width+1)):
        raise FercPreservationError('dbf_row_count_physical_schema_or_length_drift')
    result=[]
    for index in range(count):
        cursor=header+index*width
        if body[cursor]!=32:raise FercPreservationError('deleted_dbf_record')
        cursor+=1;row={}
        for name,kind,size,decimals in fields:
            raw=body[cursor:cursor+size];cursor+=size
            if kind=='C':value=raw.rstrip(b' \0').decode('utf8')or None
            elif kind=='D':value=datetime.strptime(raw.decode('ascii'),'%Y%m%d').date().isoformat()
            else:value=float(raw)if decimals else int(raw)
            row[name]=value
        result.append(row)
    return result


def field_signatures(table):
    return[{'name':f.name,'type':str(f.type),'nullable':f.nullable,
             'width':(f.metadata or{}).get(b'GDAL:OGR:width',b'').decode()or None}for f in table.schema]


def inspect_source(path):
    """All DBF values and ordered SHP ring bytes, independent of GDAL decoding."""
    if sha256_file(path)!=INPUTS['pasda_original']['sha256']:raise FercPreservationError('original_zip_pin_changed')
    with zipfile.ZipFile(path)as archive:
        infos=archive.infolist()
        if len(infos)!=len(SPEC['members'])or {i.filename for i in infos}!=set(SPEC['members']):raise FercPreservationError('zip_member_roster_drift')
        if any(PurePosixPath(i.filename).name!=i.filename or i.flag_bits&1 or stat.S_ISLNK(i.external_attr>>16)for i in infos)or sum(i.file_size for i in infos)>65_000_000:
            raise FercPreservationError('unsafe_or_oversized_zip_members')
        for info in infos:
            pin=SPEC['members'][info.filename]
            if info.file_size!=pin['bytes']or digest(archive.read(info.filename))!=pin['sha256']:raise FercPreservationError('original_member_bytes_drift:'+info.filename)
        raw_dbf=archive.read('FERC_Regions.dbf');raw_shp=archive.read('FERC_Regions.shp')
        if archive.read('FERC_Regions.prj').decode()!=SPEC['source_prj']:raise FercPreservationError('native_crs_declaration_drift')
    meta,table=pyogrio.read_arrow('/vsizip/{'+str(path)+'}/FERC_Regions.shp',return_fids=True)
    if meta['crs']!='EPSG:4326'or meta['fid_column']!='OGC_FID'or encode(field_signatures(table))!=encode(SPEC['arrow_fields']):
        raise FercPreservationError('native_crs_physical_schema_FID_or_nullable_drift')
    original=dbf_rows(raw_dbf);rows=table.to_pylist();position=100;coordinates=0;rings=0;invalid=[]
    if len(rows)!=5:raise FercPreservationError('complete_five_region_population_drift')
    for index,row in enumerate(rows):
        wkb=row.pop('wkb_geometry')
        for name in ('SOURCEDATE','VAL_DATE'):row[name]=row[name].isoformat()
        if encode(row)!=encode(SPEC['rows'][index])or encode({k:v for k,v in row.items()if k!='OGC_FID'})!=encode(original[index]):
            raise FercPreservationError('all_original_DBF_values_or_file_FID_drift')
        record_number,words=struct.unpack('>ii',raw_shp[position:position+8]);content=raw_shp[position+8:position+8+2*words];position+=8+2*words
        if record_number!=index+1 or struct.unpack('<i',content[:4])[0]!=5:raise FercPreservationError('native_shape_record_identity_or_type_drift')
        parts,points=struct.unpack('<ii',content[36:44]);starts=struct.unpack('<'+'i'*parts,content[44:44+4*parts]);offset=44+4*parts
        if offset+16*points!=len(content):raise FercPreservationError('partial_shape_coordinates')
        raw_rings=collections.Counter(digest(content[offset+16*a:offset+16*b])for a,b in zip(starts,[*starts[1:],points]))
        geometry=shapely.from_wkb(wkb);decoded_rings=collections.Counter()
        for polygon in [geometry]if geometry.geom_type=='Polygon'else geometry.geoms:
            for ring in [polygon.exterior,*polygon.interiors]:decoded_rings[digest(np.asarray(ring.coords,dtype='<f8').tobytes())]+=1
        expected=SPEC['records'][index]
        if(raw_rings!=decoded_rings or int(shapely.get_num_coordinates(geometry))!=points
           or digest(wkb)!=expected['source_WKB_sha256']or parts!=expected['rings']or points!=expected['coords']):
            raise FercPreservationError('all_native_ordered_ring_bytes_or_WKB_drift')
        coordinates+=points;rings+=parts
        if not bool(shapely.is_valid(geometry)):invalid.append(row['OBJECTID'])
    if position!=len(raw_shp)or coordinates!=SPEC['coords']or rings!=SPEC['rings']or invalid!=SPEC['invalid_objectids']:
        raise FercPreservationError('population_native_geometry_or_invalid_identity_drift')
    geometry=shapely.from_wkb(table['wkb_geometry'].to_pylist())
    if shapely.total_bounds(geometry).tolist()!=SPEC['bounds']or any(shapely.is_empty(geometry)):
        raise FercPreservationError('CONUS_extent_or_empty_geometry_drift')
    exact=exact_numeric_table(table,raw_dbf)
    return exact,{'approved':False,'feature_count':5,'invalid_objectids':invalid,'coordinates':coordinates,'rings':rings,
        'source_extent_CONUS_only':SPEC['bounds'],'original_DBF_and_ordered_SHP_rings_exact':True,
        'original_file_FID_and_OBJECTID_and_ID_retained_separately':True,'original_member_pins':SPEC['members'],
        'original_DBF_declarations':SPEC['dbf_fields'],'exact_numeric_literals':numeric_literals(raw_dbf),
        'physical_schema_policy':'Original DBF N24,15 text retained exactly as Decimal128(38,15); declared width/scale also retained. GDAL doubles are diagnostic only. This draft interpretation is unapproved.',
        'proposed_arrow_schema':field_signatures(exact)}


def numeric_literals(body):
    count,header,width=struct.unpack('<IHH',body[4:12]);result=[]
    for index in range(count):
        cursor=header+index*width+1;row={}
        for name,kind,size,decimals in SPEC['dbf_fields']:
            raw=body[cursor:cursor+size];cursor+=size
            if kind=='N'and decimals:row[name]=raw.decode('ascii')
        result.append(row)
    return result


def exact_numeric_table(table,raw_dbf):
    literals=numeric_literals(raw_dbf)
    for name in ('SHAPE__Are','SHAPE__Len'):
        index=table.schema.get_field_index(name);original=table.schema.field(index)
        field=pa.field(name,pa.decimal128(38,15),nullable=original.nullable,
            metadata={**(original.metadata or{}),b'original_DBF_physical_declaration':encode({'type':'N','width':24,'decimals':15}),
                b'draft_numeric_policy':b'Exact literal decimal text; GDAL double is diagnostic, not canonical. Interpretation not accepted.'})
        try:
            texts=[row[name].strip()for row in literals]
            if any(re.fullmatch(r'-?\d+(?:\.\d+)?',text)is None for text in texts):raise ValueError('non_decimal_literal')
            decoded=[Decimal(text)for text in texts]
            values=pa.array(decoded,type=field.type)
            if any(value.as_py()!=decoded[i]for i,value in enumerate(values)):raise ValueError('rounded_decimal')
        except(ValueError,DecimalException,pa.ArrowException)as exc:
            raise FercPreservationError('exact_DBF_decimal_not_representable_without_rounding')from exc
        table=table.set_column(index,field,values)
    return table


def expected_parquet_table(table):
    geo={'version':'1.1.0','primary_column':'wkb_geometry','columns':{'wkb_geometry':{
        'encoding':'WKB','geometry_types':['MultiPolygon'],'crs':pyproj.CRS(4326).to_json_dict(),'bbox':SPEC['bounds']}}}
    return table.replace_schema_metadata({b'geo':encode(geo),b'original_DBF_physical_schema':encode(SPEC['dbf_fields']),
        b'draft_numeric_interpretation':b'Unapproved Decimal128(38,15) preserves exact N24,d15 literal values; raw declarations/formatting remain separately retained.'})


def parquet_bytes(table):
    sink=pa.BufferOutputStream()
    pq.write_table(expected_parquet_table(table),sink,compression='zstd',version='2.6')
    return sink.getvalue().to_pybytes()


def manifest_for(captures,identity,table):
    if set(captures)!=set(INPUTS):raise FercPreservationError('complete_input_roster_required')
    body=parquet_bytes(table)
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'FERC Regions — pinned historical PASDA preservation draft (CONUS only)',
        'publisher':'Pennsylvania Spatial Data Access (PASDA), historical third-party mirror',
        'agency':'Historical HIFLD/HSIP credits ORNL/LANL/INL/NGA; FERC jurisdiction contextual only',
        'source_url':INPUTS['pasda_original']['url'],'license':LICENSE,'processing_fingerprint':digest(encode(identity)),
        'source_selection_approved':False,'baseline_approved':False,'contract_approved':False,'rights_approved':False,
        'scope':'The complete captured mirror contains five CONUS polygons only. No Alaska/Hawaii/national completeness claim or current maintained producer-feed claim.',
        'inputs':[validate_receipt(n,*captures[n])for n in INPUTS],
        'transformations':[{'operation':'decode_complete_pinned_original_shapefile','parameters':{
            'identifiers':'Every original OGC_FID, OBJECTID and literal ID retained separately; historical published source_ID is not substituted.',
            'attributes':'Original DBF declarations/widths/Date32/int32 retained; exact N24,15 literal text represented as proposed Decimal128(38,15) instead of diagnostic GDAL doubles. No baseline mapping approved.',
            'geometry':'Original EPSG4326 WKB and every ordered ring/coordinate retained, including two invalid geometries; no repair/reprojection/simplification.',
            'capture':'Explicit local pinned files only; no automatic PASDA refresh or current-feed selection.'}},
            {'operation':'deterministic_native_exact_decimal_geoparquet','parameters':{
            'DBF_numeric_policy':'Literal N24,15 text decoded exactly to Decimal128(38,15); original declarations/formatting retained in raw/audit. Width24 plus requested scale15 cannot represent large area values without shortened fractional text. No double/REAL conversion accepted.',
            'container':'GeoPackage is wholly unsupported: SQLite REAL would lose literal decimal precision; no migration approved.'}}],
        'unsupported_output_formats':{'geopackage':'Exact DBF decimal text/scale cannot be silently converted to SQLite REAL/INTEGER; no decimal migration approved.',
            'shapefile_zip':'Original complete archive retained raw; no rewritten DBF/date/name mapping approved.',
            'pmtiles':'Projection/quantization changes original geometry, and source has two invalid polygons; no visualization exception approved.'},
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{
            'reason':'Preserve exact original physical IDs/types/Date32/names/native geometry without generic bbox/schema additions.',
            'objects':[{'path':f'geoparquet/{SLUG}.parquet','sha256':digest(body),'bytes':len(body)}]}}}}
    validate_release_provenance(manifest)
    return manifest


def reconcile_outputs(root,table):
    path=root/'geoparquet'/f'{SLUG}.parquet'
    if not pq.read_table(path).equals(expected_parquet_table(table),check_metadata=True):raise FercPreservationError('native_parquet_full_schema_values_FIDs_exact_decimals_or_WKB_drift')
    if path.read_bytes()!=parquet_bytes(table):raise FercPreservationError('deterministic_parquet_bytes_drift')
    if (root/'geopackage').exists():raise FercPreservationError('geopackage_decimal_conversion_not_supported')


def source_payloads(captures,identity,table,audit,manifest):
    baseline={'approved':False,'status':'blocked','complete_original_records':5,
        'diagnostic_records':SPEC['records'],
        'mapping_decisions_required':['ID to source_ID (collection labels ID)','OBJECTIDint32 to int64','Date32 to ISOstrings','exact DBF Decimal128 to publisheddouble',
            'SHAPE__Are/SHAPE__Len to full names','all5 original WKB differ','original OGC_FID missing from old output'],
        'full_original_published_candidates_preserved_separately':True}
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),
        'metadata/ferc_source_audit.json':encode(audit),'metadata/baseline_comparison.json':encode(baseline)}
    lineage=[]
    for row in SPEC['rows']:
        lineage.append(encode({'original_archive':'FERC_Regions_.zip','original_member':'FERC_Regions.dbf',
            'original_input_sha256':INPUTS['pasda_original']['sha256'],'original_file_local_OGC_FID':row['OGC_FID'],
            'original_identifiers':{k:row[k]for k in ['OBJECTID','ID','NAME']}}))
    values['metadata/source_record_lineage.jsonl']=b''.join(lineage)
    for name,(body,receipt)in captures.items():
        # Complete raw originals are retained once outside candidate discovery;
        # candidate references are checked against the immutable first bundle.
        values['raw/captures/'+name+'.reference.json']=encode({'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',
            'sha256':digest(body),'bytes':len(body),'original_receipt':receipt})
    return values


def validate_captures(captures):
    if set(captures)!=set(INPUTS):raise FercPreservationError('original_capture_roster_drift')
    for name,(body,receipt)in captures.items():
        pin=INPUTS[name]
        if len(body)!=pin['bytes']or digest(body)!=pin['sha256']:raise FercPreservationError('original_input_pin_drift:'+name)
        validate_receipt(name,body,receipt)


def original_bundle(storage,key,version,identity,table,*,create=None):
    fingerprint=digest(encode(identity))
    if create is None:body=load_first_construction(storage,key,version,fingerprint)
    else:
        captures,manifest=create;validate_captures(captures)
        expected=manifest_for(captures,identity,table)
        if encode({k:v for k,v in expected.items()if k!='code_commit'})!=encode({k:v for k,v in manifest.items()if k!='code_commit'}):
            raise FercPreservationError('preseal_complete_manifest_semantic_drift')
        validate_release_provenance(manifest)
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        payload={'manifest':manifest,'captures':{n:{'body_key':f'_raw-captures/{SLUG}/{digest(b)}/{n}.body','receipt':r}for n,(b,r)in captures.items()}}
        body=seal_first_construction(storage,key,version,fingerprint,canonical_construction_bundle(version,fingerprint,payload))
    payload=strict_json(body)['payload']
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(INPUTS):raise FercPreservationError('original_bundle_roster_drift')
    original={}
    for name,item in payload['captures'].items():
        expected=f"_raw-captures/{SLUG}/{item['receipt']['sha256']}/{name}.body"
        if set(item)!={'body_key','receipt'}or item['body_key']!=expected:raise FercPreservationError('original_raw_reference_drift')
        original[name]=(bounded_read(Path(storage.local_dir)/expected,50_000_000),item['receipt'])
    validate_captures(original)
    manifest=payload['manifest'];expected=manifest_for(original,identity,table)
    if encode({k:v for k,v in expected.items()if k!='code_commit'})!=encode({k:v for k,v in manifest.items()if k!='code_commit'}):raise FercPreservationError('original_manifest_semantic_drift')
    validate_release_provenance(manifest);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
    return original,manifest


def reconcile_cache(storage,prefix,table,audit,captures,identity,manifest,*,expected_version=None):
    root=Path(storage.local_dir)/prefix
    ready=strict_json(bounded_read(root/READY,128_000));values=source_payloads(captures,identity,table,audit,manifest)
    expected_names={*values,f'geoparquet/{SLUG}.parquet'}
    if (set(ready)!={'version','approved','processing_fingerprint','objects'}or ready.get('version')!=(expected_version or prefix.rsplit('/',1)[1])or ready.get('approved')is not False
        or ready.get('processing_fingerprint')!=digest(encode(identity))or set(ready.get('objects',{}))!=expected_names):raise FercPreservationError('ready_identity_or_closed_roster_drift')
    snapshot='metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'
    derivatives={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json',
        'metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    actual={str(p.relative_to(root))for p in root.rglob('*')if p.is_file()}
    if actual-(expected_names|{READY}|derivatives)or not(expected_names|{READY}).issubset(actual):raise FercPreservationError('unknown_or_missing_candidate_object')
    for name,body in values.items():
        if bounded_read(root/name,2_000_000)!=body:raise FercPreservationError('source_owned_metadata_or_raw_reference_drift:'+name)
    for name,pin in ready['objects'].items():
        if type(pin)is not str or sha256_file(root/name)!=pin:raise FercPreservationError('source_owned_bytes_drift:'+name)
    for name in ['metadata/source/source_manifest.json',snapshot]:
        if(root/name).exists()and bounded_read(root/name,2_000_000)!=encode(manifest):raise FercPreservationError('source_snapshot_drift')
    if(root/'metadata/resolved_provenance.json').exists():
        resolved=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,ready['version']).metadata)
        if encode(resolved.get('inputs'))!=encode(manifest['inputs'])or bounded_read(root/'metadata/resolved_provenance.json',2_000_000)!=encode(resolved):
            # Generic writer pretty-prints this JSON; compare canonical bytes.
            stored=strict_json(bounded_read(root/'metadata/resolved_provenance.json',2_000_000))
            if encode(resolved.get('inputs'))!=encode(manifest['inputs'])or encode(stored)!=encode(resolved):raise FercPreservationError('resolved_provenance_drift')
    reconcile_outputs(root,table)


def stage_ferc_preservation(storage,capture_dir):
    base=local_only(storage);captures=acquire(storage,capture_dir)
    try:
        identity=processing_identity();fingerprint=digest(encode(identity))
        version='v0.0.0-draft-ferc-historical-'+fingerprint[:16];prefix=f'{SLUG}/{SLUG}/{version}';root=base/prefix
        raw_path=base/f"_raw-captures/{SLUG}/{INPUTS['pasda_original']['sha256']}/pasda_original.body"
        table,audit=inspect_source(raw_path);manifest=manifest_for(captures,identity,table)
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        record=f'_construction-state/{SLUG}/{fingerprint}.json';record_path=base/record
        locks=base/'_locks';locks.mkdir(exist_ok=True)
        fd=os.open(locks/(SLUG+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'r+b')as lock:
            if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode)or os.fstat(lock.fileno()).st_nlink!=1:raise FercPreservationError('aliased_common_lock')
            fcntl.flock(lock,fcntl.LOCK_EX)
            has_history=record_path.exists()or Path(str(record_path)+'.first-construction.json').exists()or Path(str(record_path)+'.evidence').exists()
            if root.exists():
                can_detach=True
                try:
                    try:
                        captures,manifest=original_bundle(storage,record,version,identity,table)
                        observed_trust_loss(storage,version,fingerprint,restored_manifest=manifest)
                    except(ValueError,TypeError,KeyError,OSError,RuntimeError):
                        try:observed_trust_loss(storage,version,fingerprint,candidate=root)
                        except(ValueError,TypeError,KeyError,OSError,RuntimeError):can_detach=False;raise
                        raise
                    reconcile_cache(storage,prefix,table,audit,captures,identity,manifest)
                except(ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error)as exc:
                    if not can_detach:raise FercPreservationError('journal_failed_candidate_retained_untrusted:'+str(exc))from exc
                    target=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);target.parent.mkdir(parents=True,exist_ok=True);os.rename(root,target)
                    raise FercPreservationError('invalid_candidate_moved_intact:'+str(target)+':'+str(exc))from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':5,'source_invalid_geometry_count':2,'approved':False,'reused':True}
            if has_history or observed_trust_loss(storage,version,fingerprint):
                captures,manifest=original_bundle(storage,record,version,identity,table)
                observed_trust_loss(storage,version,fingerprint,restored_manifest=manifest)
            if shutil.disk_usage(base).free<500_000_000:raise FercPreservationError('native_build_requires_200MB_scratch_plus_300MB_floor')
            root.parent.mkdir(parents=True,exist_ok=True)
            private_parent=base/'_private-construction'/SLUG;private_parent.mkdir(parents=True,exist_ok=True)
            with tempfile.TemporaryDirectory(prefix='.private-ferc-',dir=private_parent)as temporary:
                private=Path(temporary);values=source_payloads(captures,identity,table,audit,manifest)
                for name,body in values.items():
                    path=private/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
                (private/'geoparquet').mkdir()
                (private/'geoparquet'/f'{SLUG}.parquet').write_bytes(parquet_bytes(table))
                roster={str(p.relative_to(private)):sha256_file(p)for p in private.rglob('*')if p.is_file()}
                (private/READY).write_bytes(encode({'version':version,'approved':False,'processing_fingerprint':fingerprint,'objects':roster}))
                reconcile_cache(storage,str(private.relative_to(base)),table,audit,captures,identity,manifest,expected_version=version)
                if (private/'metadata/source_manifest.json').read_bytes()!=encode(manifest):raise FercPreservationError('private_original_manifest_drift_before_seal')
                if any(sha256_file(private/name)!=pin for name,pin in roster.items()):raise FercPreservationError('private_bytes_changed_before_seal')
                original,original_manifest=original_bundle(storage,record,version,identity,table,create=(captures,manifest))
                if encode(original_manifest)!=encode(manifest)or any(original[n][0]!=captures[n][0]or encode(original[n][1])!=encode(captures[n][1])for n in INPUTS):raise FercPreservationError('first_construction_concurrent_winner_differs')
                if root.exists():raise FercPreservationError('concurrent_candidate_winner_preserved')
                os.rename(private,root)
            return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':5,'source_invalid_geometry_count':2,'approved':False,'reused':False}
    except(ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error)as exc:
        where=quarantine(storage,captures,str(exc));raise FercPreservationError(f'{exc};retained:{where}')from exc
