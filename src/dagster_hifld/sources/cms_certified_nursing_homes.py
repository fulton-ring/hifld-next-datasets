"""CMS Provider Information CSV; separate, unapproved certified-provider draft."""
from __future__ import annotations

import csv
import io
import json
import math
import re
from pathlib import Path
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.esri_common import canonical, sha

DATASET_SLUG = 'cms-certified-nursing-homes'
FILE_SLUG = 'provider-information'
PUBLISHER = 'Centers for Medicare & Medicaid Services (CMS)'
METADATA_URL = 'https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/4pq5-n9py?show-reference-ids'
CSV_URL = 'https://data.cms.gov/provider-data/sites/default/files/resources/0970c657767e40f8521187d02ba8d0ed_1792181744/NH_ProviderInfo_Oct2026.csv'
EXPECTED_COUNT = 14686
EXPECTED_CSV_SHA256 = 'ab70309ae6f0ee86b19fe9acb61a6d2953f970d3b09e80bb9b090e4b19ce35a2'
EXPECTED_SCHEMA_SHA256 = 'c254c071314268316889da3d10af5515fc7343b5660a53a15fa2721386fe3705'
CCN = 'CMS Certification Number (CCN)'
GATES = {name: False for name in ('source_release_accepted', 'freshness_accepted',
    'schema_contract_accepted', 'id_contract_accepted', 'rights_accepted', 'crs_accepted',
    'coverage_accepted', 'mapping_approved', 'release_accepted', 'quality_check_passed')}
CRS_NOTE = ('Proposed EPSG:4326 mapping of original Longitude/Latitude text to Point WKB; '
            'source datum and positional accuracy are unverified. No geocoding, reprojection or repair.')
SCOPE = ('Current CMS certified nursing-home providers, not all licensed nursing homes or assisted care. '
         'Observed coverage: 50 states, DC, Puerto Rico and Guam. CCN identifies a provider, not a timeless physical site. '
         'Separate from historical ORNL/HIFLD nursing homes: no HIFLD ID or enrichment parity claimed.')


def validate_metadata(meta: dict) -> None:
    """Pin producer, original distribution and currently reviewed edition."""
    if (meta.get('identifier') != '4pq5-n9py' or meta.get('title') != 'Provider Information'
            or meta.get('accessLevel') != 'public'
            or meta.get('publisher', {}).get('data', {}).get('name') != PUBLISHER
            or meta.get('modified') != '2026-10-01' or meta.get('released') != '2026-10-28'
            or meta.get('nextUpdateDate') != '2026-12-02' or meta.get('license') is not None):
        raise ValueError('CMS_source_identity_edition_or_rights_drift')
    distributions = meta.get('distribution')
    if (not isinstance(distributions, list) or len(distributions) != 1
            or distributions[0].get('data', {}).get('downloadURL') != CSV_URL
            or distributions[0].get('data', {}).get('mediaType') != 'text/csv'):
        raise ValueError('CMS_original_distribution_drift')


def parse_csv(body: bytes) -> tuple[pa.Table, dict]:
    """Preserve all source lexical values; add a proposed point column only."""
    reader = csv.reader(io.StringIO(body.decode('utf-8-sig'), newline=''), strict=True)
    fields = next(reader)
    if (len(fields) != len(set(fields)) or 'geometry' in fields
            or sha(canonical(fields)) != EXPECTED_SCHEMA_SHA256):
        raise ValueError('CMS_original_CSV_schema_drift')
    rows = list(reader)
    if len(rows) != EXPECTED_COUNT or any(len(row) != len(fields) for row in rows):
        raise ValueError('CMS_CSV_population_incomplete')
    ids, points = [], []
    for row in rows:
        attrs = dict(zip(fields, row, strict=True))
        ccn = attrs[CCN]
        if not re.fullmatch(r'[0-9A-Z]{6}', ccn):
            raise ValueError('CMS_CCN_contract_drift')
        ids.append(ccn)
        x, y = float(attrs['Longitude']), float(attrs['Latitude'])
        if (not math.isfinite(x) or not math.isfinite(y) or not -180 <= x <= 180
                or not -90 <= y <= 90 or (x, y) == (0, 0)):
            raise ValueError('CMS_coordinate_contract_drift')
        if attrs['Processing Date'] != '2026-10-01':
            raise ValueError('CMS_processing_date_drift')
        points.append(Point(x, y).wkb)
    if len(set(ids)) != EXPECTED_COUNT:
        raise ValueError('CMS_duplicate_CCN')
    if sha(body) != EXPECTED_CSV_SHA256:
        raise ValueError('CMS_unreviewed_complete_CSV_baseline_drift')
    geo = {'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{
        'encoding':'WKB','geometry_types':['Point'],'crs':CRS.from_epsg(4326).to_json_dict(),
        'edges':'planar'}}}
    arrays = [pa.array([row[i] for row in rows], type=pa.string()) for i in range(len(fields))]
    table = pa.Table.from_arrays(arrays + [pa.array(points, type=pa.binary())],
                                names=fields + ['geometry'])
    table = table.replace_schema_metadata({b'geo':canonical(geo), b'hifld:proposed_crs':CRS_NOTE.encode()})
    audit = {**GATES, 'source_feature_count':len(rows), 'unique_source_id_count':len(set(ids)),
        'source_fields':fields, 'source_csv_sha256':sha(body), 'source_record_sha256':sha(canonical(rows)),
        'source_schema_sha256':sha(canonical(fields)), 'source_id_sha256':sha(canonical(ids)),
        'source_geometry_sha256':sha(b''.join(points)), 'source_null_geometry_count':0,
        'source_invalid_geometry_count':0, 'source_crs':None, 'proposed_crs_epsg':4326,
        'geometry_mapping':CRS_NOTE, 'coverage_note':SCOPE, 'source_modified':'2026-10-01',
        'source_released':'2026-10-28','source_next_update':'2026-12-02',
        'source_license':None, 'source_license_note':'CMS metadata has no license field; rights remain unknown.'}
    return table, audit


def capture_cms_certified_nursing_homes(storage: StagingStorageResource, *, version: str,
                                        capture_directory: str) -> dict:
    """Acquire current metadata before/after the full original CSV; seal a draft."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or not storage.use_local or storage.bucket or storage.prefix or not storage.local_dir):
        raise ValueError('explicit_local_staging_required')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', version) or not capture_directory:
        raise ValueError('version_and_capture_directory_required')
    root = Path(capture_directory).absolute() / uuid4().hex
    for path in (root, Path(storage.local_dir).absolute()):
        if '..' in path.parts or any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError('unsafe_local_directory')
    root.mkdir(parents=True)
    captures = {}
    try:
        with httpx.Client(timeout=90, follow_redirects=False) as client:
            for name, url, budget in [('metadata_before', METADATA_URL, 2_000_000),
                                      ('providers', CSV_URL, 12_000_000),
                                      ('metadata_after', METADATA_URL, 2_000_000)]:
                captures[name] = capture_get(client, url, root, name, max_bytes=budget)
                if name.startswith('metadata'):
                    validate_metadata(json.loads(captures[name][0]))
        if json.loads(captures['metadata_before'][0]) != json.loads(captures['metadata_after'][0]):
            raise ValueError('CMS_metadata_changed_during_capture')
        table, audit = parse_csv(captures['providers'][0])
    except Exception as exc:
        # capture_get also retains partial/status-failed requests before raising.
        failed = {p.name.removesuffix('.receipt.json'):
                  ((root / (p.name.removesuffix('.receipt.json') + '.body')).read_bytes(), json.loads(p.read_bytes()))
                  for p in root.glob('*.receipt.json')}
        quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, failed, type(exc).__name__)
        raise
    repo = Path(__file__).parents[3]
    identity = processing_identity(repo)
    validate_construction_code_identity(repo, identity['code_commit'], identity['files'], identity=identity)
    version = content_derived_version(audit['source_record_sha256'], audit['source_geometry_sha256'],
                                      audit['source_schema_sha256'], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f'_construction-state/{DATASET_SLUG}/{version}.json'
    fingerprint = sha(canonical(identity))
    result = {**GATES, 'dataset_slug':DATASET_SLUG,'file_slug':FILE_SLUG,'version':version,
              'feature_count':EXPECTED_COUNT, 'capture_directory':str(root),
              'candidate_ready':f'{prefix}/metadata/candidate_ready.json'}
    if storage.object_snapshot(state_key) is not None:
        payload = json.loads(load_first_construction(storage, state_key, version, fingerprint))['payload']
        for name, (body, _) in captures.items():
            if storage.read_key(f'{prefix}/raw/{name}.body') != body:
                raise ValueError('cached_source_evidence_changed:' + name)
        verify_cached_candidate(storage, repo, state_key, prefix, version, fingerprint, identity,
            audit['source_record_sha256'], audit['source_schema_sha256'], payload['objects'], payload['capture_receipts'])
        return {**result,'reused':True}
    sink = pa.BufferOutputStream(); pq.write_table(table, sink, compression='zstd')
    parquet = sink.getvalue().to_pybytes()
    parquet_name = f'geoparquet/{FILE_SLUG}.parquet'
    descriptor = {'bytes':len(parquet),'sha256':sha(parquet)}
    manifest = {'schema_version':1,'approved':False,'title':'CMS certified nursing homes: Provider Information',
        'description':SCOPE + ' ' + CRS_NOTE,'publisher':PUBLISHER,'agency':PUBLISHER,'source_url':METADATA_URL,
        'license':'Unknown; absent from CMS metadata. Rights acceptance pending.',
        'source_modified':'2026-10-01','date_issued':'2026-10-28','date_modified':'2026-10-01',
        'processor':'HIFLD Next','code_commit':identity['code_commit'],
        'inputs':[{'input_id':name,'producer':PUBLISHER,'host':'data.cms.gov','role':'producer_publication',
            'canonical_url':METADATA_URL,'retrieval_url':r['url'],'observed_final_url':r['final_url'],
            'retrieved_at':r['observed_at'],'http_status':r['status'],'bytes':r['bytes'],'sha256':r['sha256'],
            'license':'Unknown; license field absent from CMS metadata.',
            'attribution':PUBLISHER + '; public access does not establish redistribution acceptance.',
            'release_date':'2026-10-28'} for name, (_,r) in captures.items()],
        'transformations':[{'operation':'original_csv_to_proposed_point_geoparquet','parameters':{
            'attributes':'All original headers and lexical values retained as strings; blanks are empty strings.',
            'identity':'Original CCN retained, including leading zeros. No replacement IDs.',
            'geometry':CRS_NOTE,'scope':SCOPE}}],
        'lineage':'CMS dataset 4pq5-n9py, current original distribution 0. ' + SCOPE,
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{
            'reason':'Lexical CMS CSV preservation with explicitly proposed unaccepted geometry mapping.',
            'objects':[{'path':parquet_name,**descriptor}]}}},
        'unsupported_output_formats':{'geopackage':'No approved mapping.','pmtiles':'No approved display transformation.'}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    outputs = {parquet_name:parquet,'metadata/source_manifest.json':canonical(manifest)+b'\n',
               'metadata/source_audit.json':canonical(audit)+b'\n',
               'metadata/processing_identity.json':canonical(identity)+b'\n',
               'metadata/candidate_ready.json':canonical({**GATES,'approved':False,'version':version,
                   'objects':{parquet_name:descriptor},'source_record_sha256':audit['source_record_sha256']})+b'\n'}
    for name, (body, receipt) in captures.items():
        outputs.update({f'raw/{name}.body':body, f'raw/{name}.wire':(root/f'{name}.wire').read_bytes(),
                        f'raw/{name}.receipt.json':canonical(receipt)+b'\n'})
    objects = {name:{'bytes':len(body),'sha256':sha(body)} for name,body in outputs.items()}
    for name,body in outputs.items(): write_immutable(storage,f'{prefix}/{name}',body)
    for name,desc in objects.items():
        snapshot=storage.object_snapshot(f'{prefix}/{name}')
        if snapshot is None or snapshot.size != desc['bytes'] or storage.sha256_key(f'{prefix}/{name}') != desc['sha256']:
            raise ValueError('candidate_physical_readback_failed:' + name)
    seal_ready_candidate(storage,state_key,version,fingerprint,{'processing_identity':identity,
        'source_record_sha256':audit['source_record_sha256'],'source_schema_sha256':audit['source_schema_sha256'],
        'capture_receipts':{n:r for n,(_,r) in captures.items()},'objects':objects})
    verify_cached_candidate(storage,repo,state_key,prefix,version,fingerprint,identity,
        audit['source_record_sha256'],audit['source_schema_sha256'],objects,{n:r for n,(_,r) in captures.items()})
    return {**result,'reused':False}
