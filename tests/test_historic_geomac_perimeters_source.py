"""Offline gate tests with a mocked pyogrio/zip roster grant no approval.

pyogrio cannot write OpenFileGDB fixtures, so the synthetic scenario mocks
pyogrio layer reads and the archive zip while every hash, receipt, census,
candidate, provenance, cache-reconcile and quarantine path runs for real.
Real complete captures are exercised opt-in via
HIFLD_GEOMAC_PERIMETERS_TEST_CAPTURE_DIR (see
docs/dataset-sources/historic-perimeters-combined-2000-2018-geomac/README.md).
"""
import contextlib
import json
import os
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import shapely
from shapely.geometry import MultiPolygon, Polygon, mapping

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.historic_geomac_perimeters import (
    historic_geomac_perimeters as historic_geomac_perimeters_asset,
    historic_geomac_perimeters_draft_schedule,
)
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import historic_geomac_perimeters as s

_CAPTURE_ENV = 'HIFLD_GEOMAC_PERIMETERS_TEST_CAPTURE_DIR'
CAPTURE = Path(os.environ[_CAPTURE_ENV]) if os.environ.get(_CAPTURE_ENV) else None

OBSERVED_AT = '2026-11-12T01:50:41+00:00'
FIELD_NAMES = [name for name, _ in s.EXPECTED_FIELDS]


def _square(x: float, y: float) -> MultiPolygon:
    return MultiPolygon([Polygon([(x, y), (x + 1, y), (x + 1, y + 1), (x, y + 1), (x, y)])])


class _Scenario:
    """Synthetic offline roster: real identity pins, small pinned census."""

    def __init__(self, root: Path):
        self.root = root
        self.capture = root / 'capture'
        self.capture.mkdir(parents=True)
        self.features = 6
        self.top_dir = s.GDB_TOP_DIR
        self.members = {'a.tdb': b'x' * 100, 'b.tdb': b'y' * 100, 'c.tdb': b'z' * 100}
        self.arrow_names = ['OBJECTID', *FIELD_NAMES, 'shape']
        self.per_year_observed = {'US_HIST_FIRE_PERIM_2000_DD83': 4, 'US_HIST_FIRE_PERIM_2001_DD83': 2}
        self.columns = {
            'OBJECTID': list(range(1, self.features + 1)),
            'agency': ['USFS', 'BLM\r\n', 'USFS', 'USFS', 'USFS', 'USFS'],
            'comments': [None, 'c2', 'c3', None, 'c5', 'c6'],
            'mapmethod': ['m1', 'm2', 'm3', 'm4', 'm5', 'm6'],
            'datecurrent': ['2002-04-26T00:00:00Z', None, '2010-05-05T00:00:00Z',
                            '2010-05-05T00:00:00Z', '2010-05-05T00:00:00Z', '2010-05-05T00:00:00Z'],
            'uniquefireidentifier': ['A', 'B', 'C', 'A', 'D', 'E'],
            'fireyear': [2000, 2000, 2001, 2001, 2002, 2002],
            'incidentname': ['i1', 'i2', 'i3', 'i4', 'i5', 'i6'],
            'pooownerunit': [None, None, None, None, None, None],
            'perimeterdatetime': ['1712-08-01T00:00:00Z', '2018-02-12T17:00:00Z', None,
                                  '2010-05-05T00:00:00Z', '2010-05-05T00:00:00Z', '2010-05-05T00:00:00Z'],
            'gisacres': [1.5, 2.5, 3.5, 4.5, 5.5, 6.5],
            'complexname': [None, None, None, None, None, None],
            'firecode': ['f1', 'f2', 'f3', 'f4', 'f5', 'f6'],
            'complexparentirwinid': [None, None, None, None, None, None],
            'pooresponsibleunit': ['p1', 'p2', 'p3', 'p4', 'p5', 'p6'],
            'state': ['AZ', 'AZ', 'CA', 'CA', 'WA', 'WA'],
            'inciwebid': [None, None, None, None, None, None],
            'localincidentidentifier': [None, None, None, None, None, None],
            'irwinid': [None, None, None, None, None, None],
            'incomplex': ['n', 'n', 'n', 'n', 'n', 'n'],
            'complexfirecode': [None, None, None, None, None, None],
            'shape_Length': [4.0, 4.0, 4.0, 4.0, 4.0, 4.0],
            'shape_Area': [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            'shape': [shapely.to_wkb(_square(i, i)) for i in range(self.features)],
        }
        self.nulls = {name: sum(1 for value in values if value is None)
                      for name, values in self.columns.items() if name != 'shape'}
        self.zip_bytes = self._write_archive()
        self.pins = self._pins()
        self._write_captures()

    def _write_archive(self) -> bytes:
        buffer_root = self.root / 'build'
        buffer_root.mkdir(exist_ok=True)
        for name, body in self.members.items():
            (buffer_root / name).write_bytes(body)
        archive = self.root / 'archive.zip'
        archive.unlink(missing_ok=True)
        with zipfile.ZipFile(archive, 'w') as zipped:
            for name in self.members:
                zipped.write(buffer_root / name, f'{self.top_dir}/{name}')
        shutil.rmtree(buffer_root)
        return archive.read_bytes()

    def _pins(self) -> dict:
        return {
            'ARCHIVE_BYTES': len(self.zip_bytes),
            'ARCHIVE_SHA256': s.sha(self.zip_bytes),
            'ZIP_MEMBERS': len(self.members),
            'ZIP_UNCOMPRESSED_BYTES': sum(len(body) for body in self.members.values()),
            'COMBINED_FEATURES': self.features,
            'EXPECTED_NULLS': dict(self.nulls),
            'EXPECTED_BOUNDS': [0.0, 0.0, float(self.features), float(self.features)],
            'EXPECTED_INVALID_GEOMETRY_COUNT': 0,
            'EXPECTED_VERTICES': 5 * self.features,
            'EXPECTED_UNIQUE_FIRE_IDS': 5,
            'EXPECTED_UNIQUE_FIRE_ID_DUPLICATES': 1,
            'EXPECTED_FIREYEAR_RANGE': (2000, 2002),
            'EXPECTED_DATECURRENT_RANGE': ('2002-04-26T00:00:00Z', '2010-05-05T00:00:00Z'),
            'EXPECTED_PERIMETERDATETIME_RANGE': ('1712-08-01T00:00:00Z', '2018-02-12T17:00:00Z'),
            'SPOT_OBJECTIDS': (1, 2, 3, 4, 5),
            'PER_YEAR_LAYERS': dict(self.per_year_observed),
            'PER_YEAR_TOTAL': sum(self.per_year_observed.values()),
        }

    def control_docs(self) -> dict:
        return {
            'gdb_item': {'id': s.GDB_ITEM_ID, 'owner': s.OWNER, 'orgId': s.ORG,
                         'name': s.ARCHIVE_NAME, 'title': 'Historic_Geomac_Perimeters_All_Years_2000_2018_gdb',
                         'type': 'File Geodatabase', 'size': len(self.zip_bytes)},
            'service_item': {'id': s.SERVICE_ITEM_ID, 'owner': s.OWNER, 'orgId': s.ORG,
                             'title': 'Historic Perimeters Combined 2000-2018 GeoMAC',
                             'type': 'Feature Service', 'url': s.SERVICE_URL},
            'layer': {'name': s.COMBINED_LAYER, 'geometryType': 'esriGeometryPolygon',
                      'serviceItemId': s.SERVICE_ITEM_ID,
                      'extent': {'xmin': 1.0, 'ymin': 2.0, 'xmax': 3.0, 'ymax': 4.0,
                                 'spatialReference': {'wkid': 102100, 'latestWkid': 3857}},
                      'fields': [dict(field) for field in s.SERVICE_FIELDS]},
            'count': {'count': self.features},
        }

    def spotcheck(self) -> dict:
        features = []
        for oid in self.pins['SPOT_OBJECTIDS']:
            geometry = shapely.from_wkb(self.columns['shape'][oid - 1])
            rings = [[list(point) for point in mapping(part)['coordinates'][0]]
                     for part in geometry.geoms]
            features.append({'attributes': {'OBJECTID': oid,
                                            'uniquefireidentifier': self.columns['uniquefireidentifier'][oid - 1],
                                            'fireyear': self.columns['fireyear'][oid - 1],
                                            'gisacres': self.columns['gisacres'][oid - 1]},
                             'geometry': {'rings': rings}})
        return {'objectIdFieldName': 'OBJECTID', 'spatialReference': {'wkid': 4269},
                'features': features}

    def bodies(self) -> dict:
        return {**{name: s.encode(doc) for name, doc in self.control_docs().items()},
                'spotcheck': s.encode(self.spotcheck()), 'archive': self.zip_bytes}

    def rewrite_capture(self, name: str, body: bytes) -> None:
        """Replace one capture body and re-pin its receipt like a real capture."""
        (self.capture / f'{name}.body').write_bytes(body)
        receipt = {'requested_url': s.URLS[name], 'status': 200, 'byte_length': len(body),
                   'sha256': s.sha(body), 'observed_at': OBSERVED_AT,
                   'final_url': 'https://www.arcgis.com/itemdata/synthetic'
                                if name == 'archive' else s.URLS[name]}
        (self.capture / f'{name}.receipt.json').write_bytes(s.encode(receipt))

    def _write_captures(self) -> None:
        for name, body in self.bodies().items():
            self.rewrite_capture(name, body)

    def _arrow_table(self) -> pa.Table:
        fields, columns = [], []
        arrow_types = {'int64': pa.int64(), 'int32': pa.int32(), 'double': pa.float64(),
                       'string': pa.string(), 'binary': pa.binary()}
        for name in self.arrow_names:
            kind = 'int64' if name == 'OBJECTID' else 'binary' if name == 'shape' \
                else dict(s.EXPECTED_FIELDS).get(name, 'string')
            kind = 'string' if kind == 'timestamp' else kind
            arrow_type = arrow_types[kind]
            fields.append(pa.field(name, arrow_type))
            columns.append(pa.array(self.columns.get(name, [1] * self.features), type=arrow_type))
        return pa.Table.from_arrays(columns, schema=pa.schema(fields))

    def patches(self):
        def read_info(path, layer=None, **kwargs):
            if layer == s.COMBINED_LAYER:
                dtypes = []
                for field_name, kind in s.EXPECTED_FIELDS:
                    if kind == 'timestamp':
                        dtypes.append('datetime64[ms]')
                    elif kind == 'int32':
                        dtypes.append('int32')
                    elif kind == 'double':
                        dtypes.append('float64')
                    else:
                        dtypes.append('object')
                return {'features': self.features, 'crs': 'EPSG:4269',
                        'geometry_type': 'MultiPolygon', 'fid_column': 'OBJECTID',
                        'geometry_name': 'shape', 'driver': 'OpenFileGDB',
                        'fields': np.array(FIELD_NAMES), 'dtypes': np.array(dtypes),
                        'total_bounds': np.array(self.pins['EXPECTED_BOUNDS'])}
            for name, count in self.per_year_observed.items():
                if layer == name:
                    return {'features': count}
            raise AssertionError('unexpected layer ' + str(layer))

        def read_arrow(path, layer=None, **kwargs):
            assert layer == s.COMBINED_LAYER and kwargs.get('return_fids') and kwargs.get('datetime_as_string')
            return ({'fid_column': 'OBJECTID', 'geometry_name': 'shape'}, self._arrow_table())

        layers = [(s.COMBINED_LAYER, 'MultiPolygon')]
        layers += [(name, 'MultiPolygon') for name in self.per_year_observed]
        items = [patch.object(s, name, value) for name, value in self.pins.items()]
        items += [patch.object(s.pyogrio, 'list_layers', return_value=layers),
                  patch.object(s.pyogrio, 'read_info', read_info),
                  patch.object(s.pyogrio, 'read_arrow', read_arrow)]
        return items


@contextlib.contextmanager
def scenario(mutate=None):
    with tempfile.TemporaryDirectory() as tmp:
        case = _Scenario(Path(tmp))
        if mutate is not None:
            mutate(case)
        with contextlib.ExitStack() as stack:
            for item in case.patches():
                stack.enter_context(item)
            staging = Path(tmp) / 'staging'
            staging.mkdir()
            yield case, StagingStorageResource(local_dir=str(staging))


class GeoMACMechanicsTests(unittest.TestCase):
    def test_registry_schedule_and_remote_rejected(self):
        self.assertIn(historic_geomac_perimeters_asset, hifld_ingest_assets)
        self.assertEqual(historic_geomac_perimeters_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        for storage in [PublishedStorageResource(local_dir='/tmp/no-write'),
                        StagingStorageResource(bucket='never', use_local=False)]:
            with patch.object(s, 'acquire') as acquire, self.assertRaises(ValueError):
                s.stage_historic_geomac_perimeters_candidate(storage, capture_dir=None)
            acquire.assert_not_called()

    def test_organize_polygons_must_stay_default(self):
        with scenario() as (case, store), patch.dict(os.environ, {'OGR_ORGANIZE_POLYGONS': 'SKIP'}):
            with self.assertRaisesRegex(ValueError, 'polygon_organization'):
                s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture)

    def test_replay_build_and_idempotent_reuse(self):
        with scenario() as (case, store):
            result = s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture)
            self.assertFalse(result['reused'])
            self.assertEqual(result['feature_count'], case.features)
            self.assertEqual(result['source_invalid_geometry_count'], 0)
            self.assertEqual(result['source_unique_id_count'], case.features)
            self.assertTrue(result['release_blocked'])
            self.assertFalse(result['release_accepted'])
            root = Path(store.local_dir) / s.SLUG / s.SLUG / result['version']
            for relative in (f'geoparquet/{s.SLUG}.parquet', 'metadata/source_manifest.json',
                             'metadata/source_audit.json', 'metadata/processing_identity.json',
                             'metadata/source_capture_manifest.json', 'metadata/candidate_ready.json',
                             'raw/captures/archive.body', 'raw/captures/archive.receipt.json'):
                self.assertTrue((root / relative).is_file(), relative)
            replay = s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture)
            self.assertEqual({**result, 'reused': True}, replay)
            self.assertEqual(len(list((Path(store.local_dir) / s.SLUG / s.SLUG).glob('v*'))), 1)

    def test_registered_catalog_metadata_then_replay(self):
        with scenario() as (case, store):
            result = s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture)
            version = result['version']
            snapshot_source_metadata(store, s.SLUG, s.SLUG, version)
            resolved = load_resolved_source_manifest(store, s.SLUG, s.SLUG, version)
            summary = summarize_staged_catalog(store, s.SLUG, s.SLUG, version, s.SLUG,
                                               source_metadata=resolved.metadata)
            write_catalog_metadata(store, s.SLUG, s.SLUG, version,
                                   summary.quality_manifest, summary.data_dictionary)
            root = Path(store.local_dir) / s.SLUG / s.SLUG / version
            for relative in ('metadata/quality_manifest.json', 'metadata/data_dictionary.json',
                             'metadata/resolved_provenance.json', 'metadata/source/source_manifest.json'):
                self.assertTrue((root / relative).is_file(), relative)
            self.assertEqual({**result, 'reused': True},
                             s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture))

    def test_receipt_preflight_rejects_forgeries(self):
        for member in ('archive', 'count'):
            for field, value in [('requested_url', 'https://example.com'), ('status', True),
                                 ('byte_length', True), ('sha256', '0' * 64),
                                 ('observed_at', '2026-11-12'), ('final_url', 'http://evil/')]:
                with scenario() as (case, store), self.subTest(member=member, field=field):
                    receipt_path = case.capture / f'{member}.receipt.json'
                    receipt = json.loads(receipt_path.read_bytes())
                    receipt[field] = value
                    receipt_path.write_bytes(s.encode(receipt))
                    with self.assertRaises(ValueError):
                        s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture)
                    self.assertFalse((Path(store.local_dir) / s.SLUG).exists())

    def test_control_identity_and_census_drift(self):
        def mutate(case, mode):
            def rewrite(name, doc):
                case.rewrite_capture(name, s.encode(doc) if not isinstance(doc, bytes) else doc)
            if mode == 'gdb_owner':
                doc = case.control_docs()['gdb_item']; doc['owner'] = 'other'; rewrite('gdb_item', doc)
            elif mode == 'service_url':
                doc = case.control_docs()['service_item']; doc['url'] = 'https://example.com'; rewrite('service_item', doc)
            elif mode == 'layer_item':
                doc = case.control_docs()['layer']; doc['serviceItemId'] = 'other'; rewrite('layer', doc)
            elif mode == 'service_fields':
                doc = case.control_docs()['layer']; doc['fields'][0]['name'] = 'renamed'; rewrite('layer', doc)
            elif mode == 'service_crs':
                doc = case.control_docs()['layer']; doc['extent']['spatialReference'] = {'wkid': 4269}; rewrite('layer', doc)
            elif mode == 'count':
                rewrite('count', s.encode({'count': case.features - 1}))
            elif mode == 'archive_sha':
                case.pins['ARCHIVE_SHA256'] = '0' * 64
            elif mode == 'zip_member':
                case.pins['ZIP_MEMBERS'] = 2
            elif mode == 'zip_uncompressed':
                case.pins['ZIP_UNCOMPRESSED_BYTES'] = 299
            elif mode == 'top_dir':
                case.top_dir = 'Other.gdb'
                case.zip_bytes = case._write_archive()
                case.pins = case._pins()
                case.rewrite_capture('archive', case.zip_bytes)
            elif mode == 'field_roster':
                case.arrow_names.insert(-1, 'surprise')
            elif mode == 'objectid':
                case.columns['OBJECTID'] = [1, 1, 3, 4, 5, 6]
            elif mode == 'nulls':
                case.columns['state'][0] = None
            elif mode == 'bounds':
                case.pins['EXPECTED_BOUNDS'] = [0.0, 0.0, 12.0, 11.0]
            elif mode == 'invalid':
                case.pins['EXPECTED_INVALID_GEOMETRY_COUNT'] = 1
            elif mode == 'vertices':
                case.pins['EXPECTED_VERTICES'] = 5 * case.features + 1
            elif mode == 'ufi':
                case.pins['EXPECTED_UNIQUE_FIRE_IDS'] = 6
            elif mode == 'peryear':
                case.per_year_observed['US_HIST_FIRE_PERIM_2000_DD83'] = 5
            elif mode == 'fireyear':
                case.pins['EXPECTED_FIREYEAR_RANGE'] = (2000, 2003)
            elif mode == 'spot_deviation':
                spot = case.spotcheck()
                spot['features'][0]['geometry']['rings'][0][0][0] += 1.0
                case.rewrite_capture('spotcheck', s.encode(spot))
            elif mode == 'spot_attribute':
                spot = case.spotcheck()
                spot['features'][0]['attributes']['gisacres'] = 99.0
                case.rewrite_capture('spotcheck', s.encode(spot))
            elif mode == 'spot_crs':
                spot = case.spotcheck()
                spot['spatialReference'] = {'wkid': 3857}
                case.rewrite_capture('spotcheck', s.encode(spot))
        modes = ['gdb_owner', 'service_url', 'layer_item', 'service_fields', 'service_crs', 'count',
                 'archive_sha', 'zip_member', 'zip_uncompressed', 'top_dir', 'field_roster', 'objectid',
                 'nulls', 'bounds', 'invalid', 'vertices', 'ufi', 'peryear', 'fireyear',
                 'spot_deviation', 'spot_attribute', 'spot_crs']
        for mode in modes:
            with self.subTest(mode=mode):
                def mutation(case, mode=mode):
                    mutate(case, mode)
                with scenario(mutate=mutation) as (case, store):
                    with self.assertRaises(ValueError):
                        s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture)
                    self.assertFalse((Path(store.local_dir) / s.SLUG).exists())

    def test_atomic_install_and_prior_preserved(self):
        for failure in [OSError('disk'), KeyboardInterrupt()]:
            with tempfile.TemporaryDirectory() as tmp, patch.object(s.os, 'rename', side_effect=failure), \
                    self.assertRaises(type(failure)):
                try:
                    s.commit_complete_candidate(StagingStorageResource(local_dir=tmp), 'a/a/v0-fixture',
                                               {'geoparquet/a.parquet': b'candidate'}, {})
                finally:
                    self.assertFalse(Path(tmp, 'a/a/v0-fixture').exists())
        with tempfile.TemporaryDirectory() as tmp:
            store = StagingStorageResource(local_dir=tmp)
            s.commit_complete_candidate(store, 'a/a/v0-fixture', {'geoparquet/a.parquet': b'prior'}, {})
            with self.assertRaises(ValueError):
                s.commit_complete_candidate(store, 'a/a/v0-fixture', {'geoparquet/a.parquet': b'new'}, {})
            self.assertEqual((Path(tmp, 'a/a/v0-fixture/geoparquet/a.parquet')).read_bytes(), b'prior')

    def test_invalid_cache_self_repin_rejected(self):
        with scenario() as (case, store):
            result = s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture)
            root = Path(store.local_dir) / s.SLUG / s.SLUG / result['version']
            pristine = Path(store.local_dir) / 'pristine'
            shutil.copytree(root, pristine)
            ready_file = root / 'metadata/candidate_ready.json'
            ready_bytes = ready_file.read_bytes()
            parquet = root / 'geoparquet' / f'{s.SLUG}.parquet'
            original_parquet = parquet.read_bytes()
            for mode in ('tampered_parquet', 'tampered_archive', 'forged_approval', 'missing_ready'):
                if not root.exists():
                    shutil.copytree(pristine, root)
                ready = json.loads(ready_file.read_bytes())
                if mode == 'tampered_parquet':
                    parquet.write_bytes(original_parquet + b'trailing')
                    ready['objects'][f'geoparquet/{s.SLUG}.parquet'] = s.sha(parquet.read_bytes())
                elif mode == 'tampered_archive':
                    raw = root / 'raw/captures/archive.body'
                    raw.write_bytes(raw.read_bytes()[:-1] + b'!')
                    ready['objects']['raw/captures/archive.body'] = s.sha(raw.read_bytes())
                elif mode == 'forged_approval':
                    ready['approved'] = True
                else:
                    ready_file.unlink()
                if mode != 'missing_ready':
                    ready_file.write_bytes(s.encode(ready))
                with self.subTest(mode=mode), self.assertRaises(ValueError):
                    s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture)
                self.assertFalse(root.exists())
                self.assertTrue(list((Path(store.local_dir) / '_quarantine' / s.SLUG).glob('invalid-cache-*')))
            shutil.copytree(pristine, root)
            self.assertEqual({**result, 'reused': True},
                             s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture))

    def test_concurrent_install_winner_is_preserved(self):
        with scenario() as (case, store):
            install = s.commit_complete_candidate

            def competing(storage, prefix, payloads, ready):
                install(storage, prefix, payloads, ready)
                raise s.GeoMACQuarantineError('simulated_concurrent_install_winner')
            with patch.object(s, 'commit_complete_candidate', side_effect=competing), \
                    self.assertRaisesRegex(ValueError, 'simulated_concurrent_install_winner'):
                s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture)
            versions = list((Path(store.local_dir) / s.SLUG / s.SLUG).glob('v*'))
            self.assertEqual(len(versions), 1)
            before = {str(p.relative_to(versions[0])): s.sha(p.read_bytes())
                      for p in versions[0].rglob('*') if p.is_file()}
            result = s.stage_historic_geomac_perimeters_candidate(store, capture_dir=case.capture)
            self.assertEqual(result['feature_count'], case.features)
            self.assertEqual({str(p.relative_to(versions[0])): s.sha(p.read_bytes())
                               for p in versions[0].rglob('*') if p.is_file()}, before)


@unittest.skipUnless(os.environ.get(_CAPTURE_ENV), 'complete original NIFC captures opt-in required')
class GeoMACRealCaptureTests(unittest.TestCase):
    def test_real_capture_replay_and_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = StagingStorageResource(local_dir=tmp)
            result = s.stage_historic_geomac_perimeters_candidate(store, capture_dir=CAPTURE)
            self.assertEqual(result['feature_count'], 23776)
            self.assertEqual(result['source_invalid_geometry_count'], 881)
            self.assertEqual(result['source_vertices'], 13400285)
            self.assertTrue(result['release_blocked'])
            replay = s.stage_historic_geomac_perimeters_candidate(store, capture_dir=CAPTURE)
            self.assertEqual({**result, 'reused': True}, replay)


if __name__ == '__main__':
    unittest.main()
