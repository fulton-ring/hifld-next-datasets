"""Real complete pinned NOAA draft integration; never authors a release approval."""
import copy
import gzip
import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetSelection, DagsterInstance, Definitions, define_asset_job
from shapely import from_wkb
from shapely.geometry import MultiLineString

import dagster_hifld.assets.noaa_contiguous as source_module
from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets.publish import (
    publish_geopackage, publish_geoparquet, publish_pmtiles, publish_shapefile_zip,
    publish_version, verify_release,
)
from dagster_hifld.definitions import defs as registered_defs
from dagster_hifld.noaa_extract import stage_noaa_candidate
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

EVIDENCE = Path(__file__).resolve().parents[1] / 'docs/dataset-sources/24nm-contiguous-zone/evidence'


def real_receipt(directory):
    """Composite observed captures, explicitly not a simultaneous live snapshot."""
    live = os.environ.get('HIFLD_NOAA_CONTIGUOUS_LIVE_RECEIPT')
    if live:
        captures, receipt = source_module.read_contiguous_receipt(Path(live))
        directory.mkdir(parents=True)
        for name, body in captures.items():
            (directory / (name + '.json')).write_bytes(body)
        (directory / 'receipt.json').write_text(json.dumps(receipt))
        return captures, receipt
    new = EVIDENCE / 'network-20261019'
    captures = {
        'layer': (EVIDENCE / 'layer.txt').read_bytes(),
        'features': (EVIDENCE / 'all-features.json').read_bytes(),
        'geojson': gzip.decompress((new / 'features-geojson.json.gz').read_bytes()),
        'ids_before': (new / 'ids-json.body').read_bytes(),
        'ids_after': (new / 'ids-after.body').read_bytes(),
        'count_before': (new / 'count-json.body').read_bytes(),
        'count_after': (EVIDENCE / 'count-recheck.json').read_bytes(),
    }
    directory.mkdir(parents=True)
    requests = {r['name']: r for r in json.loads((new / 'requests.json').read_text())}
    source = json.loads((EVIDENCE.parent / 'source.json').read_text())
    names = {'geojson': 'features-geojson', 'ids_before': 'ids-json', 'ids_after': 'ids-after', 'count_before': 'count-json'}
    receipts = []
    for name, raw in captures.items():
        (directory / (name + '.json')).write_bytes(raw)
        req = requests.get(names.get(name, ''))
        url = req['url'] if req else source_module.LAYER_URL + ('?f=pjson' if name == 'layer' else '/query?where=1%3D1&' + ('outFields=*&f=pjson' if name == 'features' else 'returnCountOnly=true&f=pjson'))
        receipts.append({'capture': name, 'url': url, 'http_status': 200,
            'fetched_at': req['checked_at'] if req else '2026-10-19T04:19:06.917231+00:00',
            'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw),
            'receipt_note': 'Complete composite observed research captures; historical Esri plus equal fresh GeoJSON, not a simultaneous live snapshot or accepted release.'})
    receipt = {'schema_version': 1, 'layer_url': source_module.LAYER_URL,
               'objects': {name + '.json': {'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)} for name, raw in captures.items()}, 'receipts': receipts}
    (directory / 'receipt.json').write_text(json.dumps(receipt))
    return captures, receipt


class ContiguousDraftTests(unittest.TestCase):
    def test_normal_repository_registers_exactly_one_source_specific_contiguous_asset(self):
        key = source_module.noaa_24nm_contiguous_zone.key
        registered = [asset for asset in registered_defs.assets if key in asset.keys]
        self.assertEqual(len(registered), 1)
        self.assertIs(registered[0], source_module.noaa_24nm_contiguous_zone)
        self.assertIn(key, registered_defs.resolve_asset_graph().get_all_asset_keys())

    def test_receipt_rejects_wrong_identity_and_mutation(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'receipt'
            real_receipt(p)
            source_module.read_contiguous_receipt(p)
            (p / 'features.json').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'checksum'):
                source_module.read_contiguous_receipt(p)

    def test_geometry_drift_quarantines_without_overwriting_good_candidate(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            captures, receipt = real_receipt(p / 'receipt')
            storage = StagingStorageResource(local_dir=str(p / 'staging'), use_local=True)
            kwargs = dict(staging=storage, dataset_slug=source_module.DATASET, file_slug=source_module.FILE,
                          receipts=receipt['receipts'], expected_fields=source_module.EXPECTED_FIELDS,
                          expected_fingerprints=source_module.PROPOSED_FINGERPRINTS, expected_count=63)
            valid = stage_noaa_candidate(captures=captures, **kwargs)
            before = (p / 'staging' / valid['candidate_key']).read_bytes()
            changed = copy.deepcopy(captures)
            full = json.loads(changed['features']);full['features'][0]['geometry']['paths'][0][0][0] += 0.01
            changed['features'] = json.dumps(full).encode()
            changed_receipts = copy.deepcopy(receipt['receipts'])
            for item in changed_receipts:
                if item['capture'] == 'features':
                    item['sha256'] = hashlib.sha256(changed['features']).hexdigest()
                    item['bytes'] = len(changed['features'])
            kwargs['receipts'] = changed_receipts
            with self.assertRaisesRegex(ValueError, 'geometry drift'):
                stage_noaa_candidate(captures=changed, **kwargs)
            self.assertEqual(before, (p / 'staging' / valid['candidate_key']).read_bytes())
            quarantine = list((p / 'staging').rglob('quarantine.json'))
            self.assertEqual(len(quarantine), 1)
            self.assertFalse(json.loads(quarantine[0].read_text())['promotion_approved'])

    def test_actual_complete_capture_graph_and_unapproved_promotion_preserves_release(self):
        preserved = os.environ.get('HIFLD_NOAA_CONTIGUOUS_EVIDENCE_DIR')
        with (nullcontext(preserved) if preserved else tempfile.TemporaryDirectory()) as d:
            p = Path(d)
            captures, receipt = real_receipt(p / 'receipt')
            home = p / 'dagster';home.mkdir(parents=True)
            (home / 'dagster.yaml').write_text('telemetry:\n  enabled: false\n')
            staging = StagingStorageResource(local_dir=str(p / 'staging'), use_local=True)
            published = PublishedStorageResource(local_dir=str(p / 'published'), use_local=True)
            sentinel = p / 'published/previous-accepted-release/sentinel';sentinel.parent.mkdir(parents=True);sentinel.write_bytes(b'previous release unchanged')
            before = {str(f.relative_to(p / 'published')): hashlib.sha256(f.read_bytes()).hexdigest() for f in (p / 'published').rglob('*') if f.is_file()}
            formats = [publish_catalog, publish_geopackage, publish_geoparquet, publish_pmtiles, publish_shapefile_zip]
            # Execute selections from the actual registered repository asset collection.
            # A missing import/list registration must fail this integration too.
            defs = Definitions(assets=registered_defs.assets, asset_checks=registered_defs.asset_checks,
                resources={'staging_storage': staging, 'published_storage': published}, jobs=[
                    define_asset_job('source', selection=AssetSelection.assets(source_module.noaa_24nm_contiguous_zone)),
                    define_asset_job('formats', selection=AssetSelection.assets(*formats)),
                    define_asset_job('release', selection=AssetSelection.assets(verify_release, publish_version)),
                ])
            with patch.dict(os.environ, {'DAGSTER_HOME': str(home), 'HIFLD_RELEASE_APPROVALS': ''}), DagsterInstance.get() as instance:
                config = {'ops': {source_module.noaa_24nm_contiguous_zone.op.name: {'config': {'capture_dir': str(p / 'receipt')}}}}
                tags = {'hifld/source_code_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()}
                first = defs.resolve_job_def('source').execute_in_process(instance=instance, run_config=config, tags=tags)
                report = first.output_for_node(source_module.noaa_24nm_contiguous_zone.op.name)
                later_receipt = json.loads((p / 'receipt/receipt.json').read_text())
                for item in later_receipt['receipts']:
                    item['fetched_at'] = '2026-10-20T00:00:00+00:00'
                (p / 'receipt/receipt.json').write_text(json.dumps(later_receipt))
                second = defs.resolve_job_def('source').execute_in_process(instance=instance, run_config=config, tags=tags)
                self.assertEqual(report, second.output_for_node(source_module.noaa_24nm_contiguous_zone.op.name))
                partition = f"{source_module.DATASET}/{source_module.FILE}/{report['version']}"
                self.assertIn(partition, instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))
                table = pq.read_table(p / 'staging' / report['candidate_key'])
                self.assertEqual(table.num_rows, 63)
                self.assertEqual(table.schema.field('UNILATERAL').type, pa.float64())
                self.assertEqual(table.schema.field('PUB_DATE').type, pa.int64())
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
                self.assertEqual(quality['feature_count'], 63)
                self.assertTrue(quality['quality_check_passed'])
                self.assertFalse(list((version_dir / 'shapefile').glob('*.zip')))
                self.assertEqual(formats_result.output_for_node(publish_shapefile_zip.op.name)['files'], [])
                self.assertIn('\"NOTE\": 43', provenance['unsupported_output_formats']['shapefile_zip'])
                dictionary = json.loads((version_dir / 'metadata/data_dictionary.json').read_text())
                date_column = next(c for c in dictionary['columns'] if c['name'] == 'PUB_DATE')
                self.assertEqual(date_column['type'], 'integer')
                self.assertTrue(date_column['nullable'])
                self.assertEqual(len(dictionary['inputs']), 7)
                # Inspect the actual normal graph's terminal GeoParquet, all rows/attributes/geometries.
                terminal = list((version_dir / 'geoparquet').rglob('*.parquet'))
                self.assertTrue(terminal)
                terminal_rows = [r for f in terminal for r in pq.read_table(f).to_pylist()]
                self.assertEqual(len(terminal_rows), 63)
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
                for name in ['OBJECTID', 'RULEID', 'PUB_DATE', 'APPRV_DATE']:
                    self.assertEqual(terminal_schema.field(name).type, pa.int64())
                self.assertEqual(set(row['REGION'] for row in terminal_rows), set(report['coverage']['REGION']))
                terminal_type = terminal_schema.field('UNILATERAL').type
                self.assertEqual(terminal_type, pa.float64())
                failure = defs.resolve_job_def('release').execute_in_process(instance=instance, partition_key=partition, raise_on_error=False)
                self.assertFalse(failure.success)
                errors = [event.event_specific_data.error.to_string() for event in failure.all_events if event.is_step_failure]
                self.assertTrue(any('missing_independent_release_approval_registry' in error for error in errors), errors)
                after = {str(f.relative_to(p / 'published')): hashlib.sha256(f.read_bytes()).hexdigest() for f in (p / 'published').rglob('*') if f.is_file()}
                self.assertEqual(before, after)
                self.assertTrue((version_dir / 'metadata/noaa_extraction.json').is_file())
                result = {'classification': 'working local draft; ' + ('complete fresh live source capture' if os.environ.get('HIFLD_NOAA_CONTIGUOUS_LIVE_RECEIPT') else 'complete real composite observed captures') + '; no dataset acceptance',
                    'source_code_commit': tags['hifld/source_code_commit'],
                    'module': source_module.__file__, 'asset_collection': 'dagster_hifld.definitions.defs.assets',
                    'normal_registered_asset_count': len(registered_defs.resolve_asset_graph().get_all_asset_keys()), 'source_run_id': first.run_id, 'idempotent_run_id': second.run_id,
                    'formats_run_id': formats_result.run_id, 'failed_release_run_id': failure.run_id,
                    'source_rows': table.num_rows, 'terminal_rows': len(terminal_rows), 'terminal_UNILATERAL_type': str(terminal_type),
                    'partition': partition, 'candidate_sha256': report['candidate_sha256'],
                    'full_attributes_ids_geometries_checked': True, 'coverage': report['coverage'],
                    'quality_check_passed': quality['quality_check_passed'], 'raw_bodies_preserved': 7,
                    'provenance_inputs': len(provenance['inputs']), 'published_objects_unchanged': True,
                    'release_failure': 'missing_independent_release_approval_registry', 'promotion_approved': False,
                    'terminal_paths': [str(f) for f in terminal], 'terminal_schema': str(terminal_schema), 'state_storage': str(p)}
                (p / 'integration_result.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    unittest.main()
