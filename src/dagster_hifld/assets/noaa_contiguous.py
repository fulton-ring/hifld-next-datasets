"""Reviewable local draft for NOAA's 24NM contiguous-zone line dataset.

The source field expectations below are extraction guards, not an accepted
published contract. Final release remains subject to the shared #417 gate.
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path

from dagster import AssetKey, Config, Output, asset

from dagster_hifld.noaa_extract import fetch_noaa_layer, stage_noaa_candidate
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource

DATASET = FILE = '24nm-contiguous-zone'
LAYER_URL = ('https://maritimeboundaries.noaa.gov/arcgis/rest/services/'
             'MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer/2')
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
PROPOSED_FINGERPRINTS = {
    'schema': '0cd5c0563b87a395fb872c67760a73f437864d14f137a6df5e45355862278e9c',
    'identifiers': 'cd331b6b846dcb6be348ead5350b39364adeb9092a6a51ebea4eef85f6ef6959',
    'geometry': '0556c7ca8cfe6ae5ae81954e86dcf5eae97d64bae2bd6b70e7af5b3ed8936d4c',
    'coverage': '5e01b9ccf140cf309279f431ad994ef63474fdc0109e14f86604a2250ad4e2e8',
}


class ContiguousDraftConfig(Config):
    """A checksummed complete source receipt; empty selects bounded live capture."""
    capture_dir: str = ''


def read_contiguous_receipt(directory: Path) -> tuple[dict[str, bytes], dict]:
    """Read every pinned raw body; reject missing or mutated receipt bytes."""
    receipt = json.loads((directory / 'receipt.json').read_text())
    if receipt.get('schema_version') != 1 or receipt.get('layer_url') != LAYER_URL:
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
    description=('Local draft NOAA producer extraction for 24NM contiguous-zone boundary lines; '
                 'preserves native IDs and declared field types. Candidate schema is unapproved.'),
)
def noaa_24nm_contiguous_zone(context, config: ContiguousDraftConfig,
                              staging_storage: StagingStorageResource) -> Output[dict]:
    # Production enablement needs an accepted source/schema/release decision.
    if not staging_storage.use_local or staging_storage.bucket:
        raise ValueError('unapproved NOAA draft requires isolated local staging')
    if config.capture_dir:
        bodies, receipt = read_contiguous_receipt(Path(config.capture_dir))
        receipts = receipt['receipts']
    else:
        bodies, receipts = fetch_noaa_layer(LAYER_URL, attempts=3)
    report = stage_noaa_candidate(
        staging=staging_storage, dataset_slug=DATASET, file_slug=FILE,
        captures=bodies, receipts=receipts, expected_fields=EXPECTED_FIELDS,
        expected_fingerprints=PROPOSED_FINGERPRINTS, expected_count=63,
        code_commit=context.run_tags.get('hifld/source_code_commit'),
    )
    partition = build_publish_partition_key(DATASET, FILE, report['version'])
    context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
    return Output(report, metadata={
        'version': report['version'], 'candidate_sha256': report['candidate_sha256'],
        'feature_count': report['feature_count'], 'promotion_approved': False,
        'draft_partition': partition, 'refresh_cadence': 'monthly proposal; manual draft runs only',
    })
