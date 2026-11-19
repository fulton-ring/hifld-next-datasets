"""Faithful Generalized Units draft: synthetic structural checks plus drift quarantine.

Synthetic captures exercise the complete pipeline structurally; they grant no
source approval. Real-complete checks additionally require
HIFLD_GENERALIZED_UNITS_TEST_CAPTURE_DIR with the expanded roster and wire evidence.
"""
import collections
import copy
import gzip
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
import subprocess
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import httpx
from shapely.geometry import shape
from dagster import DefaultScheduleStatus, AssetCheckSeverity, materialize, DagsterInstance
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.assets.generalized_units import fws_generalized_units, fws_generalized_units_draft_refresh_schedule
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import generalized_units as s


def setUpModule():
    global identity_patch
    identity = s.packaged_processing_identity(Path(s.__file__).parents[3])
    identity['files'] = {'pyproject.toml': s.sha(subprocess.check_output(['git', 'show', 'HEAD:pyproject.toml']))}
    identity_patch = patch.object(s, 'processing_identity', return_value=identity)
    identity_patch.start()


def tearDownModule():
    identity_patch.stop()

_CAPTURE_ENV = 'HIFLD_GENERALIZED_UNITS_TEST_CAPTURE_DIR'
CAPTURE = Path(os.environ[_CAPTURE_ENV]) if os.environ.get(_CAPTURE_ENV) else None

LANDING = ('<html>FWS CBRS Mapper <a href="https://gis1.wim.usgs.gov/server/rest/services/'
           'CBRSMapper/CoastalBarrierResourcesSystem/MapServer">REST service</a> '
           'item a60c25b4b2e44969b3164742901058a2</html>').encode()
ITEM = {'id': 'a60c25b4b2e44969b3164742901058a2', 'title': 'FWS HQ ES Coastal Barrier Resources System',
        'owner': 'richard_easterbrook@fws.gov_fws', 'type': 'Map Service',
        'url': 'https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer'}


def synthetic_page(objectids=None, *, mutate=None):
    objectids = objectids or [11, 12, 13]
    shell = [[0.0, 0.0], [4.0, 0.0], [4.0, -4.0], [0.0, -4.0], [0.0, 0.0]]  # Clockwise shell.
    hole_a = [[1.0, -1.0], [1.0, -3.0], [3.0, -3.0], [3.0, -1.0], [1.0, -1.0]]  # Counter-clockwise hole.
    shell_b = [[10.0, 0.0], [13.0, 0.0], [13.0, -3.0], [10.0, -3.0], [10.0, 0.0]]
    hole_b = [[10.5, -0.5], [10.5, -2.5], [12.5, -2.5], [12.5, -0.5], [10.5, -0.5]]
    geometries = [ [shell], [shell, hole_a], [shell, hole_a, shell_b, hole_b] ]
    unit_types = ['System Unit', 'Otherwise Protected Area', 'System Unit']
    features = []
    for objectid, geometry, unit_type in zip(objectids, geometries, unit_types):
        attributes = {'OBJECTID': objectid, 'Unit': 'SYN-%02d' % objectid, 'Name': 'Synthetic Unit %03d' % objectid,
                      'Unit_Type': unit_type, 'Fast_Acres': 85, 'Wet_Acres': 0, 'Shore_Mile': 3 if objectid % 2 else 3.25,
                      'Shape_Length': 16.0, 'Shape_Area': 21.5}
        features.append({'attributes': attributes, 'geometry': {'rings': copy.deepcopy(geometry)}})
    page = {'objectIdFieldName': 'OBJECTID', 'spatialReference': {'wkid': 4269, 'latestWkid': 4269},
            'fields': [{'name': name, 'type': 'esriFieldTypeOID' if name == 'OBJECTID' else
                        'esriFieldTypeString' if name in ('Unit', 'Name', 'Unit_Type') else
                        'esriFieldTypeSingle' if name in ('Fast_Acres', 'Wet_Acres', 'Shore_Mile') else
                        'esriFieldTypeDouble', 'alias': name}
                       for name in s.SCHEMA],
            'features': features}
    if mutate:
        mutate(page)
    return page


def synthetic_layer():
    return {'id': s.LAYER_ID, 'name': s.LAYER_NAME, 'geometryType': 'esriGeometryPolygon',
            'sourceSpatialReference': {'wkid': 4269},
            'fields': [{'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID', 'domain': None},
                       {'name': 'Shape', 'type': 'esriFieldTypeGeometry', 'alias': 'Shape', 'domain': None},
                       {'name': 'Unit', 'type': 'esriFieldTypeString', 'alias': 'Unit', 'length': 8, 'domain': None},
                       {'name': 'Name', 'type': 'esriFieldTypeString', 'alias': 'Name', 'length': 254, 'domain': None},
                       {'name': 'Unit_Type', 'type': 'esriFieldTypeString', 'alias': 'Unit_Type', 'length': 25, 'domain': None},
                       {'name': 'Fast_Acres', 'type': 'esriFieldTypeSingle', 'alias': 'Fast_Acres', 'domain': None},
                       {'name': 'Wet_Acres', 'type': 'esriFieldTypeSingle', 'alias': 'Wet_Acres', 'domain': None},
                       {'name': 'Shore_Mile', 'type': 'esriFieldTypeSingle', 'alias': 'Shore_Mile', 'domain': None},
                       {'name': 'Shape_Length', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Length', 'domain': None},
                       {'name': 'Shape_Area', 'type': 'esriFieldTypeDouble', 'alias': 'Shape_Area', 'domain': None}]}


def receipt(url, body=None):
    base = {'requested_url': url, 'final_url': url, 'status': 200,
            'observed_at': '2026-10-21T00:00:00.000000Z', 'attempt': 1}
    if body is not None:
        base['byte_length'] = len(body)
        base['sha256'] = s.sha(body)
        base.update(bytes=len(body), complete_body=True, wire_bytes=len(body), wire_sha256=s.sha(body))
    return base


def synthetic_captures(objectids=None, *, mutate=None):
    page = synthetic_page(objectids, mutate=mutate)
    body = s.encode(page)
    ids = {'objectIdFieldName': 'OBJECTID',
           'objectIds': sorted(f['attributes']['OBJECTID'] for f in page['features'])}
    count_body = s.encode({'count': len(ids['objectIds'])})
    ids_body = s.encode(ids)
    layer_body = s.encode(synthetic_layer())
    service_body = s.encode({'serviceDescription': 'FWS delegated CBRS'})
    item_body = s.encode(ITEM)
    captures = {'service_before': (service_body, receipt(s.capture_roster([])['service_before'], service_body)),
            'service_after': (service_body, receipt(s.capture_roster([])['service_after'], service_body)),
            'layer_after': (layer_body, receipt(s.capture_roster([])['layer_after'], layer_body)),
            'fws_data': (LANDING, receipt(s.capture_roster([11, 12, 13])['fws_data'], LANDING)),
            'official_item': (item_body, receipt(s.capture_roster([11, 12, 13])['official_item'], item_body)),
            'layer': (layer_body, receipt(s.capture_roster([11, 12, 13])['layer'], layer_body)),
            'count': (count_body, receipt(s.capture_roster([11, 12, 13])['count'], count_body)),
            'ids': (ids_body, receipt(s.capture_roster([11, 12, 13])['ids'], ids_body)),
            'page0000': (body, receipt(s.capture_roster([11, 12, 13])['page0000'], body)),
            'count_after': (count_body, receipt(s.capture_roster([11, 12, 13])['count_after'], count_body)),
            'ids_after': (ids_body, receipt(s.capture_roster([11, 12, 13])['ids_after'], ids_body))}
    return {name: (body, metadata, body) for name, (body, metadata) in captures.items()}


def stage_with_synthetic(storage, captures=None):
    captures = captures if captures is not None else synthetic_captures()
    with patch.object(s, 'acquire', return_value=captures):
        return s.stage_generalized_units_candidate(storage)


class StructuralTests(unittest.TestCase):
    def test_existing_nonnullable_output_contract_is_preserved(self):
        rows, audit = s.inspect_source(synthetic_captures())
        self.assertTrue(all(not column['nullable'] for column in s.PROPOSAL['columns'].values()))
        self.assertTrue(all(not field.nullable for field in s.typed_table(rows, audit).schema))
        captures = synthetic_captures(mutate=lambda page: page['features'][0]['attributes'].update(Name=None))
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(s.CBRSQuarantineError, 'nullability_drift'):
                stage_with_synthetic(StagingStorageResource(local_dir=tmp), captures)
            self.assertTrue(list(Path(tmp, '_quarantine', s.SLUG).glob('*/*/report.json')))

    def test_added_upstream_row_passes_without_snapshot_overrides(self):
        captures = synthetic_captures()
        page = json.loads(captures['page0000'][0])
        added = copy.deepcopy(page['features'][0])
        added['attributes'].update(OBJECTID=14, Unit='NEW', Unit_Type='New designation')
        page['features'].append(added)
        for name, value in [('page0000', page), ('count', {'count': 4}),
                            ('count_after', {'count': 4}),
                            ('ids', {'objectIdFieldName': 'OBJECTID', 'objectIds': [11, 12, 13, 14]}),
                            ('ids_after', {'objectIdFieldName': 'OBJECTID', 'objectIds': [14, 13, 12, 11]})]:
            body = s.encode(value)
            captures[name] = (body, receipt(s.capture_roster([11, 12, 13])[name], body), body)
        for name, url in s.capture_roster([11, 12, 13, 14]).items():
            captures[name][1].update(requested_url=url, final_url=url)
        with tempfile.TemporaryDirectory() as tmp:
            store = StagingStorageResource(local_dir=tmp)
            before = stage_with_synthetic(store)
            after = stage_with_synthetic(store, captures)
            self.assertEqual(after['feature_count'], 4)
            self.assertNotEqual(after['version'], before['version'])
            self.assertEqual(stage_with_synthetic(store, captures), after)
            drift = approved_baseline_drift(after, {**before, 'release_accepted': True})
            self.assertFalse(drift.passed)
            self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
            self.assertEqual(drift.metadata['feature_count_delta'].value, 1)
            self.assertFalse(approved_baseline_drift(after).passed)
            root = Path(tmp, s.SLUG, s.SLUG, after['version'])
            self.assertEqual(pq.read_table(root / 'candidate/typed-source.parquet').num_rows, 4)
            self.assertFalse(json.loads((root / f'metadata/{s.SLUG}_candidate_ready.json').read_bytes())['approved'])

    def test_esri_ring_conversion_without_repair(self):
        page = synthetic_page()
        rings = page['features'][2]['geometry']['rings']
        hole = rings[1]
        geometry = s._esri_polygons(rings)
        self.assertEqual(geometry['type'], 'MultiPolygon')
        self.assertTrue(shape(geometry).is_valid)
        # Counter-clockwise hole nests inside the smallest containing shell.
        outer = shape(geometry)
        self.assertEqual(len(outer.geoms), 2)
        holes = [list(p.interiors) for p in outer.geoms]
        self.assertEqual([len(i) for i in holes], [1, 1])
        with self.assertRaisesRegex(s.CBRSQuarantineError, 'esri_shell_missing'):
            s._esri_polygons([hole])  # Only a counter-clockwise hole.
        with self.assertRaisesRegex(s.CBRSQuarantineError, 'geometry_layout_drift'):
            s._esri_polygons([[[0.0, 0.0], [1.0, 1.0]]])  # Unclosed ring.

    def test_complete_synthetic_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = StagingStorageResource(local_dir=tmp)
            result = stage_with_synthetic(store)
            self.assertEqual(result['feature_count'], 3)
            self.assertEqual(result['status'], 'working_draft_candidate_ready')
            root = Path(tmp, s.SLUG, s.SLUG, result['version'])
            ready = json.loads((root / f'metadata/{s.SLUG}_candidate_ready.json').read_bytes())
            self.assertFalse(ready['approved'])
            self.assertEqual(ready['source_sha256'], s.sha(synthetic_captures()['page0000'][0]))
            for name in ready['objects']:
                self.assertTrue((root / name).is_file(), name)
            table = pq.read_table(root / 'candidate/typed-source.parquet')
            self.assertEqual(table.num_rows, 3)
            self.assertEqual(table.schema.field('Fast_Acres').type, __import__('pyarrow').float64())
            self.assertEqual(table.schema.field('OBJECTID').type, __import__('pyarrow').int64())
            # Declared-Single JSON integers adapted to float64; raw capture keeps originals.
            self.assertEqual(table.column('Fast_Acres').to_pylist(), [85.0, 85.0, 85.0])
            self.assertEqual(table.column('Shore_Mile').to_pylist(), [3.0, 3.25, 3.0])
            audit = json.loads((root / f'metadata/{s.SLUG}_source_audit.json').read_bytes())
            self.assertEqual(audit['geometry_types'], {'Polygon': 2, 'MultiPolygon': 1})
            self.assertEqual(audit['invalid_geometry_count'], 0)
            self.assertEqual(audit['coverage'], {'System Unit': 2, 'Otherwise Protected Area': 1})
            manifest = json.loads((root / 'metadata/source_manifest.json').read_bytes())
            self.assertFalse(manifest['approved'])
            self.assertEqual(len(manifest['inputs']), len(s.capture_roster([11, 12, 13])))

    def test_registered_catalog_metadata_then_cache_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = StagingStorageResource(local_dir=tmp)
            result = stage_with_synthetic(store)
            version = result['version']
            snapshot_source_metadata(store, s.SLUG, s.SLUG, version)
            resolved = load_resolved_source_manifest(store, s.SLUG, s.SLUG, version)
            summary = summarize_staged_catalog(store, s.SLUG, s.SLUG, version, s.SLUG,
                                               source_metadata=resolved.metadata)
            write_catalog_metadata(store, s.SLUG, s.SLUG, version,
                                   summary.quality_manifest, summary.data_dictionary)
            root = Path(tmp, s.SLUG, s.SLUG, version)
            for relative in ('metadata/quality_manifest.json', 'metadata/data_dictionary.json',
                             'metadata/resolved_provenance.json', 'metadata/source/source_manifest.json'):
                self.assertTrue((root / relative).is_file(), relative)
            digest = s.sha((root / 'metadata/source_manifest.json').read_bytes())
            self.assertTrue((root / 'metadata/source' / f'version_{digest}_source_manifest.json').is_file())
            self.assertEqual(stage_with_synthetic(store), result)

    def test_candidate_refuses_remote_storage(self):
        with self.assertRaisesRegex(s.CBRSQuarantineError, 'draft_requires_explicit_isolated_local_staging'):
            s.stage_generalized_units_candidate(PublishedStorageResource(bucket='example'))


@unittest.skipUnless(CAPTURE is not None, 'complete original captures opt-in required; see docs/dataset-sources/generalized-units-1/README.md')
class RealCaptureTests(unittest.TestCase):
    def captures(self):
        ids = json.loads((CAPTURE / 'ids.body').read_bytes())['objectIds']
        return {name: ((CAPTURE / (name + '.body')).read_bytes(),
                       json.loads((CAPTURE / (name + '.receipt.json')).read_bytes()),
                       (CAPTURE / (name + '.wire')).read_bytes()) for name in s.capture_roster(sorted(ids))}

    def test_complete_original(self):
        rows, audit = s.inspect_source(self.captures())
        self.assertEqual(len(rows), 942)
        self.assertEqual(audit['feature_count'], 942)
        self.assertEqual(audit['invalid_geometry_count'], 0)
        self.assertEqual(audit['coverage'], {'System Unit': 592, 'Otherwise Protected Area': 350})
        self.assertEqual(len({r['properties']['OBJECTID'] for r in rows}), 942)
        self.assertEqual(len(s.provenance(self.captures(), s.processing_identity())['inputs']), len(s.capture_roster([11, 12, 13])))


class DriftTests(unittest.TestCase):
    def mutate_page(self, mode):
        def mutate(page):
            if mode == 'fields':
                page['fields'][0]['name'] = 'newOID'
            elif mode == 'field_type':
                page['fields'][0]['type'] = 'esriFieldTypeString'
            elif mode == 'type':
                page['features'][0]['attributes']['Fast_Acres'] = '85 acres'
            elif mode == 'id':
                page['features'][0]['attributes']['OBJECTID'] = page['features'][1]['attributes']['OBJECTID']
            elif mode == 'geometry':
                page['features'][0]['geometry']['rings'] = []
            elif mode == 'hole_uncontained':
                page['features'][1]['geometry']['rings'][1] = [[50.0, 50.0], [50.0, 52.0], [52.0, 52.0], [50.0, 52.0], [50.0, 50.0]]
            elif mode == 'column':
                page['features'][0]['attributes']['surprise'] = 1
            elif mode == 'crs':
                page['spatialReference']['wkid'] = 3857
            elif mode == 'string_width':
                page['features'][0]['attributes']['Unit'] = 'TOOLONGUNIT'
        return mutate

    def test_drift_negatives(self):
        for mode in ['count', 'ids', 'partial', 'fields', 'field_type', 'type', 'id', 'geometry',
                     'column', 'crs', 'string_width', 'hole_uncontained']:
            with self.subTest(mode=mode):
                captures = synthetic_captures(mutate=None)
                if mode == 'count':
                    body = s.encode({'count': 2})
                    captures['count_after'] = (body, receipt(s.capture_roster([11, 12, 13])['count_after'], body), body)
                elif mode == 'ids':
                    after = json.loads(captures['ids_after'][0]); after['objectIds'].pop()
                    body = s.encode(after)
                    captures['ids_after'] = (body, receipt(s.capture_roster([11, 12, 13])['ids_after'], body), body)
                elif mode == 'partial':
                    captures = synthetic_captures(mutate=lambda p: p.update(exceededTransferLimit=True))
                else:
                    captures = synthetic_captures(mutate=self.mutate_page(mode))
                with tempfile.TemporaryDirectory() as tmp:
                    store = StagingStorageResource(local_dir=tmp)
                    with patch.object(s, 'acquire', return_value=captures), \
                            self.assertRaises(ValueError):
                        s.stage_generalized_units_candidate(store)
                    self.assertFalse(Path(tmp, s.SLUG, s.SLUG).exists())
                    quarantined = list(Path(tmp, '_quarantine', s.SLUG).glob('*/*/report.json'))
                    self.assertTrue(quarantined, mode)

    def test_receipt_and_provenance_preflight(self):
        for name in s.capture_roster([11, 12, 13]):
            for field, value in [('requested_url', 'https://example.com'), ('final_url', 'https://example.com'),
                                 ('status', True), ('byte_length', True), ('sha256', '0' * 64),
                                 ('wire_bytes', 1), ('wire_sha256', '0' * 64), ('complete_body', False),
                                 ('observed_at', '2026-10-21T00:00:00+00:99'), ('observed_at', '2026-10-21')]:
                captures = synthetic_captures(); captures[name][1][field] = value
                with self.subTest(name=name, field=field), tempfile.TemporaryDirectory() as tmp, \
                        patch.object(s, 'acquire', return_value=captures), self.assertRaises(ValueError):
                    try:
                        stage_with_synthetic(StagingStorageResource(local_dir=tmp), captures)
                    finally:
                        self.assertFalse(Path(tmp, s.SLUG).exists())
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(s, 'validate_release_provenance', side_effect=ValueError('injected malformed provenance')), \
                self.assertRaises(ValueError):
            try:
                stage_with_synthetic(StagingStorageResource(local_dir=tmp))
            finally:
                self.assertFalse(Path(tmp, s.SLUG).exists())

    def test_delegation_and_layer_identity_drift(self):
        captures = synthetic_captures()
        body = b'<html>no service link here</html>'
        captures['fws_data'] = (body, receipt(s.capture_roster([11, 12, 13])['fws_data'], body), body)
        with self.assertRaisesRegex(ValueError, 'official_delegation_changed'):
            s.provenance(captures, s.processing_identity())
        captures = synthetic_captures()
        item = dict(ITEM); item['url'] = 'https://example.com/MapServer'
        body = s.encode(item)
        captures['official_item'] = (body, receipt(s.capture_roster([11, 12, 13])['official_item'], body), body)
        with self.assertRaisesRegex(ValueError, 'delegated_service_changed'):
            s.provenance(captures, s.processing_identity())
        captures = synthetic_captures(mutate=lambda p: p.update({'fields': p['fields']}))
        layer = synthetic_layer(); layer['name'] = 'Renamed Units'
        body = s.encode(layer)
        captures['layer'] = (body, receipt(s.capture_roster([11, 12, 13])['layer'], body), body)
        with tempfile.TemporaryDirectory() as tmp, patch.object(s, 'acquire', return_value=captures), \
                self.assertRaisesRegex(ValueError, 'layer_identity_or_geometry_drift|mid_capture_metadata_drift'):
            s.stage_generalized_units_candidate(StagingStorageResource(local_dir=tmp))


class CandidateIntegrityTests(unittest.TestCase):
    def test_atomic_interrupt_never_discoverable_and_prior_preserved(self):
        for failure in [OSError('disk'), KeyboardInterrupt()]:
            with tempfile.TemporaryDirectory() as tmp, patch.object(s.os, 'rename', side_effect=failure), \
                    self.assertRaises(type(failure)):
                try:
                    s.commit_complete_candidate(StagingStorageResource(local_dir=tmp), 'a/a/v0.0.0-fixture',
                                                {'geopackage/a.gpkg': b'candidate'}, {})
                finally:
                    self.assertFalse(Path(tmp, 'a/a/v0.0.0-fixture').exists())
        with tempfile.TemporaryDirectory() as tmp:
            store = StagingStorageResource(local_dir=tmp)
            s.commit_complete_candidate(store, 'a/a/v0.0.0-fixture', {'geopackage/a.gpkg': b'prior'}, {})
            with self.assertRaises(ValueError):
                s.commit_complete_candidate(store, 'a/a/v0.0.0-fixture', {'geopackage/a.gpkg': b'new'}, {})
            self.assertEqual(Path(tmp, 'a/a/v0.0.0-fixture/geopackage/a.gpkg').read_bytes(), b'prior')

    def test_invalid_cached_candidate_self_repin_quarantined(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = StagingStorageResource(local_dir=tmp)
            result = stage_with_synthetic(store)
            root = Path(tmp, s.SLUG, s.SLUG, result['version'])
            readyfile = root / f'metadata/{s.SLUG}_candidate_ready.json'
            pristine = Path(tmp, 'pristine'); shutil.copytree(root, pristine)
            key = f'geopackage/{s.SLUG}.gpkg'
            with sqlite3.connect(root / key) as db:
                db.execute(f"UPDATE \"{s.SLUG}\" SET Unit='MUTATED' WHERE OBJECTID=11")
            inventory = json.loads(readyfile.read_bytes())
            inventory['objects'][key] = s.sha((root / key).read_bytes())
            readyfile.write_bytes(s.encode(inventory))
            with self.assertRaises(ValueError):
                stage_with_synthetic(store)
            self.assertFalse(root.exists())
            retained = list(Path(tmp, '_quarantine', s.SLUG).glob('invalid-cache-*/candidate/geopackage/*.gpkg'))
            self.assertTrue(retained)
            shutil.copytree(pristine, root)
            self.assertEqual(stage_with_synthetic(store), result)

    def test_concurrent_install_winner_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            install = s.commit_complete_candidate
            def competing(store, prefix, payloads, ready):
                install(store, prefix, payloads, ready)
                raise s.CBRSQuarantineError('simulated_concurrent_install_winner')
            with patch.object(s, 'commit_complete_candidate', side_effect=competing), \
                    self.assertRaisesRegex(ValueError, 'simulated_concurrent_install_winner'):
                stage_with_synthetic(storage)


class WiringTests(unittest.TestCase):
    def test_materialized_asset_registers_dynamic_partition_and_warn_check(self):
        from dagster_hifld.assets.generalized_units import generalized_units_approved_baseline_drift
        from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
        with tempfile.TemporaryDirectory() as tmp, DagsterInstance.ephemeral() as instance, \
                patch.object(s, 'acquire', return_value=synthetic_captures()):
            result = materialize([fws_generalized_units, generalized_units_approved_baseline_drift],
                resources={'staging_storage': StagingStorageResource(local_dir=tmp)}, instance=instance)
            self.assertTrue(result.success)
            candidate = result.output_for_node(fws_generalized_units.op.name)
            self.assertIn(build_publish_partition_key(candidate['dataset_slug'], candidate['file_slug'], candidate['version']),
                          instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))
            checks = result.get_asset_check_evaluations()
            self.assertEqual(len(checks), 1)
            self.assertFalse(checks[0].passed)
            self.assertEqual(checks[0].severity, AssetCheckSeverity.WARN)

    def test_ingest_asset_registered_with_partition_and_schedule(self):
        keys = {str(next(iter(asset.keys))) for asset in hifld_ingest_assets}
        self.assertIn(str(fws_generalized_units.key), keys)
        self.assertEqual(fws_generalized_units_draft_refresh_schedule.cron_schedule, '0 16 1 * *')
        self.assertIs(fws_generalized_units_draft_refresh_schedule.default_status, DefaultScheduleStatus.STOPPED)


class AcquisitionTests(unittest.TestCase):
    def test_missing_replay_is_quarantined(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = StagingStorageResource(local_dir=str(Path(tmp, 'staging')))
            with self.assertRaises(s.CBRSQuarantineError):
                s.acquire(store, Path(tmp, 'missing'))
            self.assertTrue(list(Path(store.local_dir, '_quarantine', s.SLUG).glob('*/*/report.json')))

    def test_dynamic_exact_id_pages_live_transport_and_replay(self):
        captures = synthetic_captures()
        page = json.loads(captures.pop('page0000')[0])
        with patch.object(s, 'PAGE_SIZE', 2):
            urls = s.capture_roster([11, 12, 13])
            for index, start in enumerate((0, 2)):
                value = {**page, 'features': page['features'][start:start + 2]}
                captures[f'page{index:04d}'] = (s.encode(value), {})
            by_url = {url: captures[name][0] for name, url in urls.items()}
            requests = []
            def handle(request):
                requests.append(str(request.url))
                if str(request.url) == urls['page0001']:
                    return httpx.Response(200, headers={'Content-Encoding': 'gzip'},
                        stream=httpx.ByteStream(gzip.compress(by_url[str(request.url)], mtime=0)))
                return httpx.Response(200, stream=httpx.ByteStream(by_url[str(request.url)]))
            client_type = httpx.Client
            with tempfile.TemporaryDirectory() as tmp, patch.object(s.httpx, 'Client',
                    side_effect=lambda **kwargs: client_type(transport=httpx.MockTransport(handle))):
                store = StagingStorageResource(local_dir=str(Path(tmp, 'staging')))
                acquired = s.acquire(store, None)
                self.assertEqual(requests, list(urls.values()))
                self.assertEqual(len(s.inspect_source(acquired)[0]), 3)
                replay = Path(tmp, 'replay'); replay.mkdir()
                for path in Path(store.local_dir, '_raw-captures', s.SLUG).glob('*/*'):
                    shutil.copyfile(path, replay / path.name)
                self.assertEqual(s.acquire(store, replay), acquired)
                result = stage_with_synthetic(store, acquired)
                self.assertEqual(stage_with_synthetic(store, acquired), result)
                root = Path(store.local_dir, s.SLUG, s.SLUG, result['version'])
                self.assertEqual((root / 'raw/captures/page0001.wire').read_bytes(), acquired['page0001'][2])
                self.assertNotEqual(acquired['page0001'][2], acquired['page0001'][0])
                fresh = copy.deepcopy(acquired)
                for value in fresh.values():
                    value[1]['observed_at'] = '2026-11-19T12:00:00Z'
                self.assertEqual(stage_with_synthetic(store, fresh), result)
                key = 'raw/captures/page0001.receipt.json'
                forged = json.loads((root / key).read_bytes())
                forged.update(wire_bytes=1, wire_sha256='0' * 64)
                (root / key).write_bytes(s.encode(forged))
                ready_path = root / f'metadata/{s.SLUG}_candidate_ready.json'
                ready = json.loads(ready_path.read_bytes())
                ready['objects'][key] = s.sha((root / key).read_bytes())
                ready_path.write_bytes(s.encode(ready))
                with self.assertRaisesRegex(s.CBRSQuarantineError, 'retained_capture_receipt_invalid'):
                    stage_with_synthetic(store, acquired)
                self.assertFalse(root.exists())
                self.assertTrue(list(Path(store.local_dir, '_quarantine', s.SLUG).glob('invalid-cache-*/candidate/raw/captures/page0001.receipt.json')))
                (replay / 'page0001.wire').write_bytes(b'truncated')
                with self.assertRaisesRegex(s.CBRSQuarantineError, 'retained_capture_receipt_invalid'):
                    s.acquire(store, replay)
                self.assertTrue(list(Path(store.local_dir, '_quarantine', s.SLUG).glob('*/*/report.json')))

    def test_torn_witness_during_acquisition_quarantines_evidence(self):
        captures = synthetic_captures()
        urls = s.capture_roster([11, 12, 13])
        seen = collections.Counter()
        by_url = {url: captures[name][0] for name, url in urls.items()}
        def handle(request):
            url = str(request.url)
            seen[url] += 1
            body = s.encode({'count': 4}) if url == urls['count_after'] and seen[url] == 2 else by_url[url]
            return httpx.Response(200, stream=httpx.ByteStream(body))
        client = httpx.Client(transport=httpx.MockTransport(handle))
        with tempfile.TemporaryDirectory() as tmp, patch.object(s.httpx, 'Client', return_value=client):
            with self.assertRaisesRegex(s.CBRSQuarantineError, 'source_count_or_id_roster_drift'):
                s.acquire(StagingStorageResource(local_dir=tmp), None)
            self.assertTrue(list(Path(tmp, '_quarantine', s.SLUG).glob('*/*/evidence/count_after.body')))
            self.assertFalse(Path(tmp, s.SLUG).exists())


if __name__ == '__main__':
    unittest.main()
