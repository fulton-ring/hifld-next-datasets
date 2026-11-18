"""CMS Provider Information national nursing-home roster; unapproved draft candidate."""
from __future__ import annotations

import csv
import io
import json
import math
import re
from collections import Counter
from pathlib import Path
from urllib.parse import urlencode, urlsplit
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
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.esri_common import canonical, sha

DATASET_SLUG = FILE_SLUG = 'nursing-homes'
PUBLISHER = 'Centers for Medicare & Medicaid Services (CMS)'
METADATA_URL = 'https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/4pq5-n9py?show-reference-ids'
DATASTORE_URL = 'https://data.cms.gov/provider-data/api/1/datastore/query/4pq5-n9py/0'
DATASTORE_PAGE_SIZE = 1500
DATASTORE_CCN_FIELD = 'cms_certification_number_ccn'
CCN = 'CMS Certification Number (CCN)'
REQUIRED_FIELDS = frozenset({
    CCN, 'Provider Name', 'Provider Address', 'City/Town', 'State', 'ZIP Code',
    'Latitude', 'Longitude', 'Processing Date',
})
GATES = {name: False for name in ('source_release_accepted', 'freshness_accepted',
    'schema_contract_accepted', 'id_contract_accepted', 'rights_accepted', 'crs_accepted',
    'coverage_accepted', 'mapping_approved', 'release_accepted', 'quality_check_passed')}
CRS_NOTE = ('Proposed EPSG:4326 mapping of original Longitude/Latitude text to Point WKB; '
            'source datum and positional accuracy are unverified. No geocoding, reprojection or repair.')
SCOPE = ('Current Medicare- and Medicaid-certified nursing-home providers from CMS Provider Information, '
         'not all licensed nursing homes or assisted care. Observed coverage: 50 states, DC, Puerto Rico and Guam. '
         'CCN identifies a provider, not a timeless physical site. Separate from the historical ORNL/HIFLD '
         'nursing-homes layer: no HIFLD ID or enrichment parity claimed.')


def validate_metadata(meta: dict) -> dict:
    """Validate the producer identity and return the current distribution descriptor."""
    if (meta.get('identifier') != '4pq5-n9py' or meta.get('title') != 'Provider Information'
            or meta.get('accessLevel') != 'public'
            or meta.get('publisher', {}).get('data', {}).get('name') != PUBLISHER
            or meta.get('license') is not None):
        raise ValueError('CMS_source_identity_or_rights_drift')
    dates = {key: meta.get(key) for key in ('modified', 'released', 'nextUpdateDate')}
    if any(not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value)
           for value in dates.values()):
        raise ValueError('CMS_release_date_drift')
    distributions = meta.get('distribution')
    if not isinstance(distributions, list) or len(distributions) != 1:
        raise ValueError('CMS_original_distribution_drift')
    distribution = distributions[0]
    identifier = distribution.get('identifier')
    data = distribution.get('data')
    url = data.get('downloadURL') if isinstance(data, dict) else None
    parts = urlsplit(url) if isinstance(url, str) else None
    if (not isinstance(identifier, str) or not identifier or not isinstance(data, dict)
            or data.get('mediaType') != 'text/csv' or parts is None
            or parts.scheme != 'https' or parts.hostname != 'data.cms.gov'
            or not parts.path.endswith('.csv')):
        raise ValueError('CMS_original_distribution_drift')
    return {'download_url': url, 'distribution_id': identifier, **dates}


def _roster_url(offset: int) -> str:
    return DATASTORE_URL + '?' + urlencode({
        'limit': DATASTORE_PAGE_SIZE, 'offset': offset, 'properties[]': DATASTORE_CCN_FIELD,
        'count': 'true', 'schema': 'false', 'keys': 'false',
    })


def _capture_roster(client: httpx.Client, root: Path, captures: dict, prefix: str,
                    distribution_id: str) -> tuple[list[str], int]:
    """Page the producer datastore's native CCN roster and its own count witness."""
    ids: list[str] = []
    expected = None
    offset = 0
    while True:
        name = f'{prefix}_{offset:06d}'
        body, receipt = capture_get(client, _roster_url(offset), root, name, max_bytes=2_000_000)
        captures[name] = (body, receipt)
        document = json.loads(body)
        count, results = document.get('count'), document.get('results')
        query = document.get('query') if isinstance(document.get('query'), dict) else {}
        resources = query.get('resources')
        if (type(count) is not int or count < 1 or not isinstance(results, list)
                or query.get('offset') != offset or query.get('limit') != DATASTORE_PAGE_SIZE
                or not isinstance(resources, list) or len(resources) != 1
                or resources[0].get('id') != distribution_id):
            raise ValueError('CMS_datastore_count_or_resource_drift')
        if expected is None:
            expected = count
            if expected > 1_000_000:
                raise ValueError('CMS_population_limit')
        elif count != expected:
            raise ValueError('CMS_datastore_count_changed')
        for item in results:
            if (not isinstance(item, list) or len(item) != 1 or not isinstance(item[0], str)
                    or not re.fullmatch(r'[0-9A-Z]{6}', item[0])):
                raise ValueError('CMS_datastore_id_contract_drift')
            ids.append(item[0])
        offset += DATASTORE_PAGE_SIZE
        if offset >= expected:
            break
    if len(ids) != expected or len(set(ids)) != expected:
        raise ValueError('CMS_datastore_id_roster_incomplete')
    return ids, expected


def parse_csv(body: bytes, *, source_count: int, source_modified: str) -> tuple[pa.Table, dict, list[str]]:
    """Preserve all source lexical values; add a proposed point column only."""
    reader = csv.reader(io.StringIO(body.decode('utf-8-sig'), newline=''), strict=True)
    fields = next(reader)
    if (len(fields) != len(set(fields)) or 'geometry' in fields
            or not REQUIRED_FIELDS <= set(fields)):
        raise ValueError('CMS_original_CSV_schema_drift')
    rows = list(reader)
    if (not rows or len(rows) != source_count
            or any(len(row) != len(fields) for row in rows)):
        raise ValueError('CMS_CSV_population_incomplete')
    ids, points, states, footnotes = [], [], set(), Counter()
    for row in rows:
        attrs = dict(zip(fields, row, strict=True))
        ccn = attrs[CCN]
        if not re.fullmatch(r'[0-9A-Z]{6}', ccn):
            raise ValueError('CMS_CCN_contract_drift')
        ids.append(ccn)
        try:
            x, y = float(attrs['Longitude']), float(attrs['Latitude'])
        except ValueError:
            raise ValueError('CMS_coordinate_contract_drift') from None
        if (not math.isfinite(x) or not math.isfinite(y) or not -180 <= x <= 180
                or not -90 <= y <= 90 or (x, y) == (0, 0)):
            raise ValueError('CMS_coordinate_contract_drift')
        if attrs['Processing Date'] != source_modified:
            raise ValueError('CMS_processing_date_drift')
        if not re.fullmatch(r'[A-Z]{2}', attrs['State']):
            raise ValueError('CMS_state_contract_drift')
        states.add(attrs['State'])
        footnotes[str(attrs.get('Geocoding Footnote') or '')] += 1
        points.append(Point(x, y).wkb)
    if len(set(ids)) != len(ids):
        raise ValueError('CMS_duplicate_CCN')
    geo = {'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{
        'encoding':'WKB','geometry_types':['Point'],'crs':CRS.from_epsg(4326).to_json_dict(),
        'edges':'planar'}}}
    arrays = [pa.array([row[i] for row in rows], type=pa.string()) for i in range(len(fields))]
    table = pa.Table.from_arrays(arrays + [pa.array(points, type=pa.binary())],
                                names=fields + ['geometry'])
    table = table.replace_schema_metadata({b'geo':canonical(geo), b'hifld:proposed_crs':CRS_NOTE.encode()})
    audit = {**GATES, 'source_feature_count':len(rows), 'unique_source_id_count':len(set(ids)),
        'source_population_complete':True, 'source_count_witness':source_count,
        'source_id_roster_complete':True, 'source_fields':fields,
        'source_csv_sha256':sha(body), 'source_record_sha256':sha(canonical(rows)),
        'source_schema_sha256':sha(canonical(fields)),
        'source_id_sha256':sha(canonical(sorted(ids))),
        'source_geometry_sha256':sha(b''.join(points)), 'source_null_geometry_count':0,
        'source_invalid_geometry_count':0, 'source_crs':None, 'proposed_crs_epsg':4326,
        'geometry_mapping':CRS_NOTE, 'coverage_note':SCOPE,
        'source_state_codes':sorted(states), 'geocoding_required':False,
        'source_coordinate_count':len(points), 'source_coordinate_share':1.0,
        'geocoding_footnote_counts':dict(sorted(footnotes.items())),
        'source_modified':source_modified, 'source_license':None,
        'source_license_note':'CMS metadata has no license field; rights remain unknown.'}
    return table, audit, ids


def capture_nursing_homes(storage: StagingStorageResource, *, version: str,
                          capture_directory: str) -> dict:
    """Acquire metadata, the producer count/CCN roster and the full CSV; seal a draft."""
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
            captures['metadata_before'] = capture_get(client, METADATA_URL, root, 'metadata_before',
                                                      max_bytes=2_000_000)
            descriptor = validate_metadata(json.loads(captures['metadata_before'][0]))
            before_ids, before_count = _capture_roster(
                client, root, captures, 'ccn_before', descriptor['distribution_id'])
            captures['providers'] = capture_get(client, descriptor['download_url'], root, 'providers',
                                                max_bytes=32_000_000)
            after_ids, after_count = _capture_roster(
                client, root, captures, 'ccn_after', descriptor['distribution_id'])
            captures['metadata_after'] = capture_get(client, METADATA_URL, root, 'metadata_after',
                                                     max_bytes=2_000_000)
        if json.loads(captures['metadata_before'][0]) != json.loads(captures['metadata_after'][0]):
            raise ValueError('CMS_metadata_changed_during_capture')
        if before_ids != after_ids or before_count != after_count:
            raise ValueError('CMS_source_id_roster_changed_during_capture')
        table, audit, ids = parse_csv(captures['providers'][0], source_count=before_count,
                                      source_modified=descriptor['modified'])
        if sorted(ids) != sorted(before_ids):
            raise ValueError('CMS_CSV_id_roster_mismatch')
        audit.update({'source_released':descriptor['released'],
                      'source_next_update':descriptor['nextUpdateDate']})
    except Exception as exc:
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
              'feature_count':audit['source_feature_count'],
              'source_population_complete':audit['source_population_complete'],
              'source_count_witness':audit['source_count_witness'],
              'source_id_roster_complete':audit['source_id_roster_complete'],
              'source_coordinate_count':audit['source_coordinate_count'],
              'source_coordinate_share':audit['source_coordinate_share'],
              'source_id_sha256':audit['source_id_sha256'],
              'capture_directory':str(root),
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
    parquet_descriptor = {'bytes':len(parquet),'sha256':sha(parquet)}
    manifest = {'schema_version':1,'approved':False,'title':'Nursing Homes: CMS Provider Information',
        'description':SCOPE + ' ' + CRS_NOTE,'publisher':PUBLISHER,'agency':PUBLISHER,'source_url':METADATA_URL,
        'license':'Unknown; absent from CMS metadata. Rights acceptance pending.',
        'source_modified':audit['source_modified'],'date_issued':audit['source_released'],
        'date_modified':audit['source_modified'],
        'processor':'HIFLD Next','code_commit':identity['code_commit'],
        'inputs':[{'input_id':name,'producer':PUBLISHER,'host':'data.cms.gov','role':'producer_publication',
            'canonical_url':METADATA_URL,'retrieval_url':r['url'],'observed_final_url':r['final_url'],
            'retrieved_at':r['observed_at'],'http_status':r['status'],'bytes':r['bytes'],'sha256':r['sha256'],
            'license':'Unknown; license field absent from CMS metadata.',
            'attribution':PUBLISHER + '; public access does not establish redistribution acceptance.',
            'release_date':audit['source_released']} for name, (_,r) in captures.items()],
        'transformations':[{'operation':'original_csv_to_proposed_point_geoparquet','parameters':{
            'attributes':'All original headers and lexical values retained as strings; blanks are empty strings.',
            'identity':'Original CCN retained, including leading zeros. No replacement IDs.',
            'geometry':CRS_NOTE,'scope':SCOPE,
            'completeness':'Captured rows equal the producer datastore count and exact CCN roster before and after the CSV capture.'}}],
        'lineage':'CMS dataset 4pq5-n9py, current original distribution. ' + SCOPE,
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{
            'reason':'Lexical CMS CSV preservation with explicitly proposed unaccepted geometry mapping.',
            'objects':[{'path':parquet_name,**parquet_descriptor}]}}},
        'unsupported_output_formats':{'geopackage':'No approved mapping.','pmtiles':'No approved display transformation.'}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    outputs = {parquet_name:parquet,'metadata/source_manifest.json':canonical(manifest)+b'\n',
               'metadata/source_audit.json':canonical(audit)+b'\n',
               'metadata/processing_identity.json':canonical(identity)+b'\n',
               'metadata/candidate_ready.json':canonical({**GATES,'approved':False,'version':version,
                   'source_population_complete':True,'source_count_witness':audit['source_count_witness'],
                   'source_coordinate_share':audit['source_coordinate_share'],
                   'objects':{parquet_name:parquet_descriptor},'source_record_sha256':audit['source_record_sha256']})+b'\n'}
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
