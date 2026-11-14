"""Local draft for NOAA's US/Canada land-boundary line layer (producer MapServer layer 4).

The boundary's legal definition authority is the International Boundary
Commission; NOAA's Office of Coast Survey publishes this depiction layer inside
the U.S. Maritime Limits & Boundaries service. The source field expectations
below are extraction guards, not an accepted published contract. Final release
remains subject to the shared #417 gate.
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path

from dagster import (AssetKey, AssetSelection, Config, DefaultScheduleStatus, Output,
                     ScheduleDefinition, asset, define_asset_job)

from dagster_hifld.noaa_extract import fetch_noaa_layer, stage_noaa_candidate
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource

DATASET = FILE = 'us-canada-land-boundary'
LAYER_URL = ('https://maritimeboundaries.noaa.gov/arcgis/rest/services/'
             'MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer/4')
EXPECTED_FIELDS = {
    'OBJECTID': 'esriFieldTypeOID', 'BOUND_ID': 'esriFieldTypeString',
    'REGION': 'esriFieldTypeString', 'FEAT_TYPE': 'esriFieldTypeString',
    'PUB_DATE': 'esriFieldTypeDate', 'APPRV_DATE': 'esriFieldTypeDate',
    'LEGAL_AUTH': 'esriFieldTypeString', 'AOR': 'esriFieldTypeString',
    'NOTE': 'esriFieldTypeString', 'SUPP_INFO': 'esriFieldTypeString',
    'UNILATERAL': 'esriFieldTypeDouble', 'RULEID': 'esriFieldTypeInteger',
    'SHAPE': 'esriFieldTypeGeometry',
}

# Research snapshot drift guards; changing them requires review, not implicit adoption.
# 2026-11-14 anonymous extraction: 14 LineString segments, OBJECTIDs 979-1210,
# REGION US-Canada, FEAT_TYPE Land Boundary, BOUND_ID B0093-B0113, bounds
# (-141.00198, 41.676561, -67.283004, 69.646615).
PROPOSED_FINGERPRINTS = {
    'schema': '0cd5c0563b87a395fb872c67760a73f437864d14f137a6df5e45355862278e9c',
    'identifiers': '3a869eb2262e613ac9c226687ccfe9ff70b578edfd34f844a6c758bd56fe3e12',
    'geometry': '4a35bf7378af76dc8a91a32e1b982d9c4d5250001c6ce32a7cc3ebc8bd65c588',
    'coverage': '53ce65c01dc16b777c24a4fe0f65012e060925195d11aec5f1a76ac8a7bb4536',
}


class LandBoundaryDraftConfig(Config):
    """A checksummed complete source receipt; empty selects bounded live capture."""
    capture_dir: str = ''


def read_land_boundary_receipt(directory: Path) -> tuple[dict[str, bytes], dict]:
    """Read every pinned raw body; reject missing or mutated receipt bytes."""
    receipt = json.loads((directory / 'receipt.json').read_text())
    if type(receipt.get('schema_version')) is not int or receipt['schema_version'] != 1 or receipt.get('layer_url') != LAYER_URL:
        raise ValueError('wrong NOAA capture receipt identity')
    bodies = {}
    for name, pin in receipt['objects'].items():
        if Path(name).name != name:
            raise ValueError('raw receipt path must be a filename')
        body = (directory / name).read_bytes()
        if hashlib.sha256(body).hexdigest() != pin['sha256'] or len(body) != pin['bytes']:
            raise ValueError(f'raw capture checksum/size mismatch: {name}')
        bodies[name] = body
    required = {name + '.json' for name in ['layer', 'features', 'count_before', 'count_after', 'ids_before', 'ids_after', 'geojson']}
    if not required.issubset(bodies):
        raise ValueError(f'incomplete source receipt: {sorted(required - bodies.keys())}')
    receipt_items = receipt.get('receipts', [])
    expected_names = {Path(name).stem for name in required}
    if len(receipt_items) != len(expected_names) or {r.get('capture') for r in receipt_items} != expected_names:
        raise ValueError('missing, duplicate or extra raw retrieval receipts')
    for item in receipt_items:
        body = bodies[item['capture'] + '.json']
        if item.get('sha256') != hashlib.sha256(body).hexdigest() or item.get('bytes') != len(body):
            raise ValueError('retrieval receipt does not match pinned raw body')
        if not item.get('url', '').startswith((LAYER_URL + '?', LAYER_URL + '/query?')):
            raise ValueError('retrieval receipt is not from the exact producer layer')
        if datetime.fromisoformat(item['fetched_at']).tzinfo is None:
            raise ValueError('retrieval receipt requires timezone-aware fetch timestamp')
    return {Path(name).stem: body for name, body in bodies.items()}, receipt


@asset(
    key=AssetKey(['ingest', DATASET, FILE]), group_name='noaa', compute_kind='http',
    description=('Local draft NOAA producer extraction of the US/Canada international '
                 'land-boundary lines (Maritime Limits & Boundaries MapServer layer 4; '
                 '14 polyline segments; REGION US-Canada, FEAT_TYPE Land Boundary). '
                 'Boundary definition authority is the International Boundary Commission; '
                 'this is NOAA\'s depiction layer. Preserves native IDs and declared field '
                 'types. Candidate schema is unapproved.'),
)
def us_canada_land_boundary(context, config: LandBoundaryDraftConfig,
                            staging_storage: StagingStorageResource) -> Output[dict]:
    # Production enablement needs an accepted source/schema/release decision.
    if not staging_storage.use_local or staging_storage.bucket:
        raise ValueError('unapproved NOAA draft requires isolated local staging')
    if config.capture_dir:
        bodies, receipt = read_land_boundary_receipt(Path(config.capture_dir))
        receipts = receipt['receipts']
    else:
        bodies, receipts = fetch_noaa_layer(LAYER_URL, attempts=3)
    report = stage_noaa_candidate(
        staging=staging_storage, dataset_slug=DATASET, file_slug=FILE,
        expected_layer_url=LAYER_URL,
        captures=bodies, receipts=receipts, expected_fields=EXPECTED_FIELDS,
        expected_fingerprints=PROPOSED_FINGERPRINTS, expected_count=14,
        code_commit=context.run_tags.get('hifld/source_code_commit'),
    )
    partition = build_publish_partition_key(DATASET, FILE, report['version'])
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
    return Output(report, metadata={
        'version': report['version'], 'candidate_sha256': report['candidate_sha256'],
        'feature_count': report['feature_count'], 'promotion_approved': False,
        'draft_partition': partition, 'refresh_cadence': 'weekly stopped schedule; manual local draft runs only',
    })


# Declared cadence only. The schedule is stopped; local-only ingestion and the
# independent release boundary remain enforced when a human runs this job.
us_canada_land_boundary_draft_refresh_job = define_asset_job(
    name='us_canada_land_boundary_draft_refresh',
    selection=AssetSelection.assets(us_canada_land_boundary),
    tags={'hifld/draft_only': 'true'},
    description='Source-only local NOAA US/Canada land-boundary draft refresh; never selects publication.',
)
us_canada_land_boundary_draft_refresh_schedule = ScheduleDefinition(
    name='us_canada_land_boundary_draft_weekly',
    job=us_canada_land_boundary_draft_refresh_job,
    cron_schedule='0 10 * * 1',
    execution_timezone='America/New_York',
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [us_canada_land_boundary]
JOBS = [us_canada_land_boundary_draft_refresh_job]
SCHEDULES = [us_canada_land_boundary_draft_refresh_schedule]
