"""Real complete pinned NOAA US/Canada land-boundary draft; never authors approval."""
import copy
import gzip
import hashlib
import json
import os
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (AssetKey, AssetSelection, DagsterInstance, DefaultScheduleStatus,
                     Definitions, define_asset_job, materialize)
from shapely import from_wkb
from shapely.geometry import MultiLineString

import dagster_hifld.assets.us_canada_land_boundary as source_module
from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets.publish import (
    publish_geopackage, publish_geoparquet, publish_pmtiles, publish_shapefile_zip,
    publish_version, verify_release,
)
from dagster_hifld.definitions import defs as registered_defs
from dagster_hifld.noaa_extract import noaa_capture_urls, stage_noaa_candidate
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

EVIDENCE = Path(os.environ.get('HIFLD_US_CANADA_LAND_BOUNDARY_CAPTURE_EVIDENCE_DIR', '.'))


def synthetic_snapshot():
    """Layer-4-shaped extraction snapshot; synthetic bytes grant no acceptance."""
    attributes = lambda object_id: {'OBJECTID': object_id, 'BOUND_ID': 'B00' + str(object_id),
                                    'REGION': 'US-Canada', 'FEAT_TYPE': 'Land Boundary',
                                    'PUB_DATE': None, 'APPRV_DATE': None,
                                    'LEGAL_AUTH': 'International Boundary Commission',
                                    'AOR': 'NOAA/OCS', 'NOTE': 'US/Canada boundary - synthetic segment',
                                    'SUPP_INFO': 'http://www.nauticalcharts.noaa.gov/csdl/mb_suppinfo/SI033.docx',
                                    'UNILATERAL': 0.0, 'RULEID': 1}
    features = {'geometryType': 'esriGeometryPolyline', 'spatialReference': {'wkid': 4326},
                'features': [{'attributes': attributes(object_id),
                              'geometry': {'paths': [[[10.0 * object_id, 40.0 + object_id], [10.5 * object_id, 41.0 + object_id]]]}}
                             for object_id in [2, 1]]}
    layer = {'geometryType': 'esriGeometryPolyline', 'maxRecordCount': 1000,
             'fields': [{'name': name, 'type': kind} for name, kind in source_module.EXPECTED_FIELDS.items()]}
    geo = {'type': 'FeatureCollection', 'features': [
        {'type': 'Feature', 'id': f['attributes']['OBJECTID'], 'properties': f['attributes'],
         'geometry': {'type': 'LineString', 'coordinates': f['geometry']['paths'][0]}} for f in features['features']]}
    ids = {'objectIdFieldName': 'OBJECTID', 'objectIds': [1, 2]}
    objects = {'layer': layer, 'features': features, 'geojson': geo, 'ids_before': ids, 'ids_after': ids,
               'count_before': {'count': 2}, 'count_after': {'count': 2}}
    captures = {name: json.dumps(value).encode() for name, value in objects.items()}
    from dagster_hifld.noaa_extract import noaa_reference_fingerprints
    kwargs = dict(expected_fields=source_module.EXPECTED_FIELDS,
                  expected_fingerprints=noaa_reference_fingerprints(layer, features),
                  expected_count=2, expected_layer_url=source_module.LAYER_URL)
    return captures, kwargs


def synthetic_receipt(captures, directory):
    directory.mkdir(parents=True, exist_ok=True)
    urls = noaa_capture_urls(source_module.LAYER_URL)
    receipt = {'schema_version': 1, 'layer_url': source_module.LAYER_URL, 'objects': {}, 'receipts': []}
    for name, url in urls.items():
        body = captures[name]
        (directory / (name + '.json')).write_bytes(body)
        pin = {'sha256': hashlib.sha256(body).hexdigest(), 'bytes': len(body)}
        receipt['objects'][name + '.json'] = pin
        receipt['receipts'].append(dict(pin, capture=name, url=url, final_url=url, http_status=200,
                                        fetched_at='2026-11-14T08:00:00+00:00'))
    (directory / 'receipt.json').write_text(json.dumps(receipt))
    return receipt


def real_receipt(directory):
    """Complete retained live extraction with observed request/redirect receipts."""
    live = os.environ.get('HIFLD_US_CANADA_LAND_BOUNDARY_LIVE_RECEIPT')
    if live:
        captures, receipt = source_module.read_land_boundary_receipt(Path(live))
        directory.mkdir(parents=True)
        for name, body in captures.items():
            (directory / (name + '.json')).write_bytes(body)
        (directory / 'receipt.json').write_text(json.dumps(receipt))
        return captures, receipt
    if not os.environ.get('HIFLD_US_CANADA_LAND_BOUNDARY_CAPTURE_EVIDENCE_DIR'):
        raise unittest.SkipTest('Complete real inputs are external checksummed artifacts; set HIFLD_US_CANADA_LAND_BOUNDARY_CAPTURE_EVIDENCE_DIR or HIFLD_US_CANADA_LAND_BOUNDARY_LIVE_RECEIPT')
    receipt = json.loads((EVIDENCE / 'receipt.json').read_text())
    captures = {Path(name).stem: gzip.decompress((EVIDENCE / ('capture-' + Path(name).stem + '.json.gz')).read_bytes())
                if (EVIDENCE / ('capture-' + Path(name).stem + '.json.gz')).exists() else (EVIDENCE / name).read_bytes()
                for name in receipt['objects']}
    directory.mkdir(parents=True)
    for name, raw in captures.items():
        (directory / (name + '.json')).write_bytes(raw)
    (directory / 'receipt.json').write_text(json.dumps(receipt))
    return captures, receipt


class USCanadaLandBoundaryTests(unittest.TestCase):
    def test_normal_repository_registers_exactly_one_source_specific_asset(self):
        key = source_module.us_canada_land_boundary.key
        registered = [asset for asset in registered_defs.assets if key in asset.keys]
        self.assertEqual(len(registered), 1)
        self.assertIs(registered[0], source_module.us_canada_land_boundary)
        self.assertIn(key, registered_defs.resolve_asset_graph().get_all_asset_keys())

    def test_weekly_cadence_is_stopped_and_selects_only_the_local_guarded_source(self):
        schedule = source_module.us_canada_land_boundary_draft_refresh_schedule
        job = source_module.us_canada_land_boundary_draft_refresh_job
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.cron_schedule, '0 10 * * 1')
        self.assertEqual(schedule.execution_timezone, 'America/New_York')
        self.assertEqual(job.selection.resolve(registered_defs.resolve_asset_graph()),
                         {source_module.us_canada_land_boundary.key})
        self.assertEqual(job.tags['hifld/draft_only'], 'true')

    def test_capture_urls_are_exact_official_layer4_requests(self):
        urls = noaa_capture_urls(source_module.LAYER_URL)
        self.assertEqual(set(urls), {'layer', 'count_before', 'features', 'ids_before', 'ids_after', 'count_after', 'geojson'})
        for name, url in urls.items():
            self.assertTrue(url.startswith(source_module.LAYER_URL), name)
            self.assertIn('maritimeboundaries.noaa.gov', url)
        self.assertTrue(urls['layer'].endswith('/4?f=pjson'))
        for name, url in urls.items():
            if name != 'layer':
                self.assertIn('/MapServer/4/query?', url, name)
        with self.assertRaisesRegex(ValueError, 'exact official'):
            noaa_capture_urls(source_module.LAYER_URL.replace('/MapServer/4', '/MapServer/5'))
        with self.assertRaisesRegex(ValueError, 'exact official'):
            noaa_capture_urls(source_module.LAYER_URL + '?f=pjson')
        with self.assertRaisesRegex(ValueError, 'exact official'):
            noaa_capture_urls(source_module.LAYER_URL.replace('maritimeboundaries.noaa.gov', 'example.com'))

    def test_receipt_rejects_wrong_identity_and_mutation(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'receipt'
            p.mkdir()
            captures, kwargs = synthetic_snapshot()
            synthetic_receipt(captures, p)
            original = json.loads((p / 'receipt.json').read_text())
            source_module.read_land_boundary_receipt(p)
            for invalid_version in (True, 1.0, '1'):
                (p / 'receipt.json').write_text(json.dumps(dict(original, schema_version=invalid_version)))
                with self.assertRaisesRegex(ValueError, 'identity'):
                    source_module.read_land_boundary_receipt(p)
            (p / 'receipt.json').write_text(json.dumps(dict(original, layer_url=source_module.LAYER_URL.replace('/4', '/2'))))
            with self.assertRaisesRegex(ValueError, 'identity'):
                source_module.read_land_boundary_receipt(p)
            (p / 'receipt.json').write_text(json.dumps(original))
            (p / 'features.json').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'checksum'):
                source_module.read_land_boundary_receipt(p)

    def test_remote_or_published_storage_is_rejected_before_any_fetch(self):
        for storage in (PublishedStorageResource(local_dir='/nowhere', use_local=True),
                        StagingStorageResource(local_dir='/nowhere', use_local=False, bucket='hifld-staging')):
            with tempfile.TemporaryDirectory() as d:
                config = {'ops': {source_module.us_canada_land_boundary.op.name: {'config': {'capture_dir': ''}}}}
                result = materialize(
                    [source_module.us_canada_land_boundary],
                    resources={'staging_storage': storage},
                    run_config=config, raise_on_error=False,
                )
                self.assertFalse(result.success)
                errors = [event.event_specific_data.error.to_string() for event in result.all_events if event.is_step_failure]
                self.assertTrue(any('isolated local storage' in error or 'isolated local staging' in error for error in errors), errors)

    def test_synthetic_snapshot_stages_and_drift_quarantines_without_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            captures, kwargs = synthetic_snapshot()
            storage = StagingStorageResource(local_dir=str(p / 'staging'), use_local=True)
            receipt = synthetic_receipt(captures, p / 'receipt')
            valid = stage_noaa_candidate(staging=storage, dataset_slug=source_module.DATASET,
                                         file_slug=source_module.FILE, captures=captures,
                                         receipts=receipt['receipts'], **kwargs)
            before = (p / 'staging' / valid['candidate_key']).read_bytes()
            self.assertEqual(valid['feature_count'], 2)
            self.assertEqual(valid['status'], 'draft_candidate_unapproved')
            self.assertFalse(valid['promotion_approved'])
            changed = copy.deepcopy(captures)
            full = json.loads(changed['features'])
            full['features'][0]['geometry']['paths'][0][0][0] += 0.01
            changed['features'] = json.dumps(full).encode()
            changed_receipts = copy.deepcopy(receipt['receipts'])
            for item in changed_receipts:
                if item['capture'] == 'features':
                    item['sha256'] = hashlib.sha256(changed['features']).hexdigest()
                    item['bytes'] = len(changed['features'])
            with self.assertRaisesRegex(ValueError, 'geometry drift'):
                stage_noaa_candidate(staging=storage, dataset_slug=source_module.DATASET,
                                     file_slug=source_module.FILE, captures=changed,
                                     receipts=changed_receipts, **kwargs)
            self.assertEqual(before, (p / 'staging' / valid['candidate_key']).read_bytes())
            quarantine = list((p / 'staging').rglob('quarantine.json'))
            self.assertEqual(len(quarantine), 1)
            self.assertFalse(json.loads(quarantine[0].read_text())['promotion_approved'])

    def test_actual_complete_capture_graph_and_unapproved_promotion_preserves_release(self):
        preserved = os.environ.get('HIFLD_US_CANADA_LAND_BOUNDARY_EVIDENCE_DIR')
        with (nullcontext(preserved) if preserved else tempfile.TemporaryDirectory()) as d:
            p = Path(d)
            captures, receipt = real_receipt(p / 'receipt')
            home = p / 'dagster'
            home.mkdir(parents=True)
            (home / 'dagster.yaml').write_text('telemetry:\n  enabled: false\n')
            staging = StagingStorageResource(local_dir=str(p / 'staging'), use_local=True)
            published = PublishedStorageResource(local_dir=str(p / 'published'), use_local=True)
            sentinel = p / 'published/previous-accepted-release/sentinel'
            sentinel.parent.mkdir(parents=True)
            sentinel.write_bytes(b'previous release unchanged')
            before = {str(f.relative_to(p / 'published')): hashlib.sha256(f.read_bytes()).hexdigest()
                      for f in (p / 'published').rglob('*') if f.is_file()}
            formats = [publish_catalog, publish_geopackage, publish_geoparquet, publish_pmtiles, publish_shapefile_zip]
            defs = Definitions(assets=registered_defs.assets, asset_checks=registered_defs.asset_checks,
                               resources={'staging_storage': staging, 'published_storage': published}, jobs=[
                    define_asset_job('source', selection=AssetSelection.assets(source_module.us_canada_land_boundary)),
                    define_asset_job('formats', selection=AssetSelection.assets(*formats)),
                    define_asset_job('release', selection=AssetSelection.assets(verify_release, publish_version)),
                ])
            with patch.dict(os.environ, {'DAGSTER_HOME': str(home), 'HIFLD_RELEASE_APPROVALS': ''}), DagsterInstance.get() as instance:
                config = {'ops': {source_module.us_canada_land_boundary.op.name: {'config': {'capture_dir': str(p / 'receipt')}}}}
                tags = {'hifld/source_code_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()}
                first = defs.resolve_job_def('source').execute_in_process(instance=instance, run_config=config, tags=tags)
                report = first.output_for_node(source_module.us_canada_land_boundary.op.name)
                later_receipt = json.loads((p / 'receipt/receipt.json').read_text())
                for item in later_receipt['receipts']:
                    item['fetched_at'] = '2026-11-14T09:00:00+00:00'
                (p / 'receipt/receipt.json').write_text(json.dumps(later_receipt))
                second = defs.resolve_job_def('source').execute_in_process(instance=instance, run_config=config, tags=tags)
                self.assertEqual(report, second.output_for_node(source_module.us_canada_land_boundary.op.name))
                partition = f'{source_module.DATASET}/{source_module.FILE}/{report["version"]}'
                self.assertIn(partition, instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))
                table = pq.read_table(p / 'staging' / report['candidate_key'])
                self.assertEqual(table.num_rows, 14)
                self.assertEqual(table.schema.field('UNILATERAL').type, pa.float64())
                self.assertEqual(table.schema.field('PUB_DATE').type, pa.int64())
                self.assertEqual(table.schema.field('RULEID').type, pa.int32())
                self.assertEqual(set(table.column_names), set(source_module.EXPECTED_FIELDS))
                src = {f['attributes']['OBJECTID']: f for f in json.loads(captures['features'])['features']}
                for row in table.to_pylist():
                    original = src[row['OBJECTID']]
                    geometry = from_wkb(row['SHAPE'])
                    self.assertTrue(geometry.equals(MultiLineString(original['geometry']['paths'])))
                    for name, value in original['attributes'].items():
                        self.assertEqual(row[name], value)
                version_dir = p / 'staging' / source_module.DATASET / source_module.FILE / report['version']
                for name, raw in captures.items():
                    self.assertEqual((version_dir / 'raw' / (name + '.json')).read_bytes(), raw)
                provenance = json.loads((version_dir / 'metadata/source_manifest.json').read_text())
                validate_release_provenance(provenance)
                self.assertEqual(len(provenance['inputs']), 7)
                self.assertEqual({r['input_id'] for r in provenance['inputs']}, set(captures))
                formats_result = defs.resolve_job_def('formats').execute_in_process(instance=instance, partition_key=partition)
                self.assertTrue(formats_result.success)
                quality = json.loads((version_dir / 'metadata/quality_manifest.json').read_text())
                self.assertEqual(quality['feature_count'], 14)
                self.assertTrue(quality['quality_check_passed'])
                self.assertFalse(list((version_dir / 'shapefile').glob('*.zip')))
                self.assertEqual(formats_result.output_for_node(publish_shapefile_zip.op.name)['files'], [])
                self.assertIn('"NOTE": 59', provenance['unsupported_output_formats']['shapefile_zip'])
                dictionary = json.loads((version_dir / 'metadata/data_dictionary.json').read_text())
                date_column = next(c for c in dictionary['columns'] if c['name'] == 'PUB_DATE')
                self.assertEqual(date_column['type'], 'integer')
                self.assertTrue(date_column['nullable'])
                self.assertEqual(len(dictionary['inputs']), 7)
                terminal = list((version_dir / 'geoparquet').rglob('*.parquet'))
                self.assertTrue(terminal)
                terminal_rows = [r for f in terminal for r in pq.read_table(f).to_pylist()]
                self.assertEqual(len(terminal_rows), 14)
                for row in terminal_rows:
                    original = src[row['OBJECTID']]
                    for name, value in original['attributes'].items():
                        self.assertEqual(row[name], value)
                    geometry_value = row.get('SHAPE', row.get('geometry'))
                    terminal_geometry = from_wkb(geometry_value)
                    as_multi = MultiLineString([terminal_geometry]) if terminal_geometry.geom_type == 'LineString' else terminal_geometry
                    self.assertTrue(as_multi.equals_exact(MultiLineString(original['geometry']['paths']), 0))
                    self.assertEqual(list(terminal_geometry.bounds), [row['bbox'][n] for n in ['xmin', 'ymin', 'xmax', 'ymax']])
                terminal_schema = pq.read_table(terminal[0]).schema
                self.assertEqual(terminal_schema.field('RULEID').type, pa.int32())
                gpkg = next((version_dir / 'geopackage').glob('*.gpkg'))
                with sqlite3.connect(gpkg) as connection:
                    columns = {row[1]: row[2] for row in connection.execute(f'PRAGMA table_info("{source_module.FILE}")')}
                self.assertEqual(columns['RULEID'], 'MEDIUMINT')
                for name in ['OBJECTID', 'PUB_DATE', 'APPRV_DATE']:
                    self.assertEqual(terminal_schema.field(name).type, pa.int64())
                self.assertEqual(set(row['REGION'] for row in terminal_rows), set(report['coverage']['REGION']))
                terminal_type = terminal_schema.field('UNILATERAL').type
                self.assertEqual(terminal_type, pa.float64())
                failure = defs.resolve_job_def('release').execute_in_process(instance=instance, partition_key=partition, raise_on_error=False)
                self.assertFalse(failure.success)
                errors = [event.event_specific_data.error.to_string() for event in failure.all_events if event.is_step_failure]
                self.assertTrue(any('missing_independent_release_approval_registry' in error for error in errors), errors)
                after = {str(f.relative_to(p / 'published')): hashlib.sha256(f.read_bytes()).hexdigest()
                         for f in (p / 'published').rglob('*') if f.is_file()}
                self.assertEqual(before, after)
                self.assertTrue((version_dir / 'metadata/noaa_extraction.json').is_file())
                result = {'classification': 'working local draft; complete retained real source bytes; no dataset acceptance',
                          'source_code_commit': tags['hifld/source_code_commit'],
                          'module': source_module.__file__, 'asset_collection': 'dagster_hifld.definitions.defs.assets',
                          'normal_registered_asset_count': len(registered_defs.resolve_asset_graph().get_all_asset_keys()),
                          'source_run_id': first.run_id, 'idempotent_run_id': second.run_id,
                          'formats_run_id': formats_result.run_id, 'failed_release_run_id': failure.run_id,
                          'source_rows': table.num_rows, 'terminal_rows': len(terminal_rows),
                          'partition': partition, 'candidate_sha256': report['candidate_sha256'],
                          'full_attributes_ids_geometries_checked': True, 'coverage': report['coverage'],
                          'quality_check_passed': quality['quality_check_passed'], 'raw_bodies_preserved': 7,
                          'provenance_inputs': len(provenance['inputs']), 'published_objects_unchanged': True,
                          'release_failure': 'missing_independent_release_approval_registry', 'promotion_approved': False,
                          'state_storage': str(p)}
                (p / 'integration_result.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    unittest.main()
