"""Offline gates with a mocked GDB read grant no approval.

pyogrio cannot author OpenFileGDB curved fixtures, so the synthetic scenario
mocks the geodatabase layer reads while every zip census, receipt validation,
attribute/geometry census, real GDAL curve linearization, candidate build,
provenance, cache-reconcile and quarantine path runs for real. Complete real
captures are exercised opt-in via HIFLD_MIRTA_TEST_CAPTURE_DIR.
"""
import base64
import contextlib
import hashlib
import json
import os
import ssl
import struct
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import mirta_dod_sites as s

_CAPTURE_ENV = 'HIFLD_MIRTA_TEST_CAPTURE_DIR'
CAPTURE = Path(os.environ[_CAPTURE_ENV]) if os.environ.get(_CAPTURE_ENV) else None

OBSERVED_AT = '2026-11-12T01:50:41+00:00'
# Independently recorded on 2026-11-12 from the live DISDI archive capture.
RECORDED_CANDIDATE_GEOMETRY_SHA256 = '7184add233e971ed55124ac39a94caebdbfcde65a89d890aca50cef3f8c2c5a4'


def _polygon_wkb(ring: list[tuple[float, float]], order: int = 1) -> bytes:
    endian = '<' if order == 1 else '>'
    body = bytes([order]) + struct.pack(endian + 'I', 3) + struct.pack(endian + 'I', 1)
    body += struct.pack(endian + 'I', len(ring))
    body += b''.join(struct.pack(endian + 'dd', x, y) for x, y in ring)
    return body


def _bow_tie_wkb() -> bytes:
    ring = [(0.0, 0.0), (2.0, 2.0), (2.0, 0.0), (0.0, 2.0), (0.0, 0.0)]
    return b'\x01' + struct.pack('<I', 6) + struct.pack('<I', 1) + _polygon_wkb(ring)


def _square_wkb(x: float, y: float) -> bytes:
    ring = [(x, y), (x + 1, y), (x + 1, y + 1), (x, y + 1), (x, y)]
    return b'\x01' + struct.pack('<I', 6) + struct.pack('<I', 1) + _polygon_wkb(ring)


def _structural_curve_wkb() -> bytes:
    """MultiSurface -> CurvePolygon -> CompoundCurve -> LineString (no arcs)."""
    ring = [(0.2, 0.2), (0.8, 0.2), (0.8, 0.8), (0.2, 0.8), (0.2, 0.2)]
    line = b'\x01' + struct.pack('<I', 2) + struct.pack('<I', len(ring))
    line += b''.join(struct.pack('<dd', x, y) for x, y in ring)
    compound = b'\x01' + struct.pack('<I', 9) + struct.pack('<I', 1) + line
    curve_poly = b'\x01' + struct.pack('<I', 10) + struct.pack('<I', 1) + compound
    return b'\x01' + struct.pack('<I', 12) + struct.pack('<I', 1) + curve_poly


def _arc_curve_wkb() -> bytes:
    """MultiSurface -> CurvePolygon -> CompoundCurve -> one CircularString."""
    points = [(1.5, 1.5), (2.0, 2.4), (2.5, 1.5), (3.0, 0.6), (3.5, 1.5)]
    circular = b'\x01' + struct.pack('<I', 8) + struct.pack('<I', len(points))
    circular += b''.join(struct.pack('<dd', x, y) for x, y in points)
    compound = b'\x01' + struct.pack('<I', 9) + struct.pack('<I', 1) + circular
    curve_poly = b'\x01' + struct.pack('<I', 10) + struct.pack('<I', 1) + compound
    return b'\x01' + struct.pack('<I', 12) + struct.pack('<I', 1) + curve_poly


class _Scenario:
    """Synthetic offline roster: real receipts and zip, mocked GDB reads."""

    FEATURES = 6
    GUID = '{0D629AB9-BEAD-4263-8D09-663887EA1100}'

    def __init__(self, root: Path):
        self.root = root
        self.capture = root / 'capture'
        self.capture.mkdir(parents=True)
        self.frozen_widths = dict(s.EXPECTED_FIELD_WIDTHS)
        self.frozen_aliases = dict(s.EXPECTED_FIELD_ALIASES)
        self.zip_members = {'a00000001.gdbtable': b'x' * 100, 'a00000001.gdbtablx': b'y' * 100,
                            'timestamps': b'z' * 100}
        self.zip_bytes = self._write_archive()
        self.objectid = [1, 2, 3, 4, 5, 6]
        self.geometries = [_square_wkb(0.0, 0.0), _bow_tie_wkb(), _square_wkb(2.0, 2.0),
                           _square_wkb(3.0, 3.0), _structural_curve_wkb(), _arc_curve_wkb()]
        self.linearized_curved = {w: self._vessel_linearized(w) for w in self.geometries[4:]}
        self.pins = self._pins()
        self._write_captures()

    def _write_archive(self) -> bytes:
        archive = self.root / 'archive.zip'
        with zipfile.ZipFile(archive, 'w') as zipped:
            zipped.writestr(zipfile.ZipInfo(f'{s.GDB_TOP_DIR}/'), b'')
            for name, body in self.zip_members.items():
                zipped.writestr(f'{s.GDB_TOP_DIR}/{name}', body)
        self.zip_entry_names = [f'{s.GDB_TOP_DIR}/'] + [f'{s.GDB_TOP_DIR}/{name}'
                                                        for name in self.zip_members]
        return archive.read_bytes()

    @staticmethod
    def _vessel_linearized(curved: bytes) -> bytes:
        import pyogrio
        pyogrio.set_gdal_config_options({'OGR_ARC_STEPSIZE': s.ARC_STEP_DEGREES})
        try:
            table = pa.table({'OBJECTID': pa.array([1], type=pa.int64()),
                              'SHAPE': pa.array([curved], type=pa.binary())})
            with tempfile.TemporaryDirectory() as tmp:
                vessel = Path(tmp) / 'vessel.shp'
                pyogrio.write_arrow(table, str(vessel), layer='vessel', driver='ESRI Shapefile',
                                    geometry_name='SHAPE', geometry_type='MultiPolygon',
                                    layer_options={'FID': 'OBJECTID', 'GEOMETRY_NAME': 'SHAPE'})
                _, readback = pyogrio.read_arrow(str(vessel), return_fids=True)
                name = next(n for n in readback.schema.names
                            if str(readback.schema.field(n).type) == 'binary')
                wkb = readback[name].to_pylist()[0]
                code = struct.unpack('<I', wkb[1:5])[0]
                return s.wrap_multi_polygon(wkb) if code == 3 else wkb
        finally:
            pyogrio.set_gdal_config_options({'OGR_ARC_STEPSIZE': None})

    def _pins(self) -> dict:
        kept = [w for w in self.geometries if struct.unpack('<I', w[1:5])[0] == 6]
        curved = [w for w in self.geometries if struct.unpack('<I', w[1:5])[0] == 12]
        subtype_census: dict[int, int] = {}
        source_vertices = 0
        for wkb in self.geometries:
            count, codes, _ = s.walk_wkb(wkb)
            source_vertices += count
            for code in codes:
                subtype_census[code] = subtype_census.get(code, 0) + 1
        candidate_vertices = sum(s.walk_wkb(w)[0] for w in kept)
        candidate_vertices += sum(s.walk_wkb(self.linearized_curved[w])[0] for w in curved)
        return {
            'ARCHIVE_BYTES': len(self.zip_bytes),
            'ARCHIVE_SHA256': s.sha(self.zip_bytes),
            'ZIP_MEMBERS': len(self.zip_entry_names),
            'ZIP_UNCOMPRESSED_BYTES': 300,
            'BOUNDARY_FEATURES': self.FEATURES,
            'POINT_FEATURES': 7,
            'POINT_BOUNDS': [-1.0, -2.0, 3.0, 4.0],
            'EXPECTED_BOUNDS': [0.0, 0.0, 4.0, 4.0],
            'EXPECTED_NULLS': {'countryName': 0, 'featureDescription': 0, 'featureName': 0,
                               'isCui': 0, 'isFirrmaSite': 0, 'isJointBase': 0, 'mediaId': 0,
                               'mirtaLocationsIdpk': 0, 'sdsId': 1, 'siteName': 0,
                               'siteOperationalStatus': 0, 'siteReportingComponent': 0,
                               'stateNameCode': 0},
            'EXPECTED_CONSTANT_VALUES': {'countryName': 'usa', 'featureDescription': 'na',
                                         'isCui': 'no', 'mediaId': 'na',
                                         'mirtaLocationsIdpk': ' '},
            'EXPECTED_VALUE_CENSUSES': {
                'isFirrmaSite': {'yes': 1, 'no': 5},
                'isJointBase': {'no': 6},
                'siteOperationalStatus': {'act': 5, 'clsd': 1},
                'siteReportingComponent': {'usa': 4, 'usn': 2},
                'stateNameCode': {'ca': 3, 'tx': 3},
            },
            'EXPECTED_OBJECTID_MIN': 1,
            'EXPECTED_OBJECTID_MAX': 6,
            'EXPECTED_SDS_ID_NULLS': 1,
            'EXPECTED_SDS_ID_NON_NULL': 5,
            'EXPECTED_SDS_ID_UNIQUE': 4,
            'EXPECTED_SDS_ID_DUPLICATE_ROWS': 1,
            'EXPECTED_SDS_ID_SHARED_GROUPS': 1,
            'EXPECTED_SDS_ID_CONFLICTING_SITE_GROUPS': 0,
            'EXPECTED_FEATURE_NAME_SITE_NAME_EQUAL': self.FEATURES,
            'EXPECTED_SOURCE_WKB_SUBTYPES': subtype_census,
            'EXPECTED_SOURCE_VERTICES': source_vertices,
            'EXPECTED_CURVED_FEATURES': len(curved),
            'EXPECTED_LINEAR_FEATURES': len(kept),
            'EXPECTED_CANDIDATE_VERTICES': candidate_vertices,
            'EXPECTED_INVALID_GEOMETRY_COUNT': 1,
            'EXPECTED_KEPT_INVALID_GEOMETRY_COUNT': 1,
            'EXPECTED_LINEARIZED_INVALID_GEOMETRY_COUNT': 0,
            'EXPECTED_EMPTY_GEOMETRY_COUNT': 0,
            'EXPECTED_NULL_GEOMETRY_COUNT': 0,
        }

    def columns(self) -> dict:
        return {
            'OBJECTID': self.objectid,
            'countryName': ['usa'] * self.FEATURES,
            'featureDescription': ['na'] * self.FEATURES,
            'featureName': ['Site A', 'Bowtie Site', 'Site C', 'Site D', 'Curve Site', 'Arc Site'],
            'isCui': ['no'] * self.FEATURES,
            'isFirrmaSite': ['no', 'no', 'no', 'no', 'no', 'yes'],
            'isJointBase': ['no'] * self.FEATURES,
            'mediaId': ['na'] * self.FEATURES,
            'mirtaLocationsIdpk': [' '] * self.FEATURES,
            'sdsId': [self.GUID, self.GUID, None, 'C28FC90D-1A11-4B2F-9E4F-000000000001',
                      'C28FC90D-1A11-4B2F-9E4F-000000000002', None],
            'siteName': ['Site A', 'Bowtie Site', 'Site C', 'Site D', 'Curve Site', 'Arc Site'],
            'siteOperationalStatus': ['act', 'act', 'act', 'act', 'act', 'clsd'],
            'siteReportingComponent': ['usa', 'usa', 'usa', 'usa', 'usn', 'usn'],
            'stateNameCode': ['ca', 'ca', 'ca', 'tx', 'tx', 'tx'],
            'SHAPE_Length': [4.0, 8.0, 4.0, 4.0, 4.0, 4.0],
            'SHAPE_Area': [1.0, -2.0, 1.0, 1.0, 1.0, 1.0],
            'SHAPE': self.geometries,
        }

    def _arrow_table(self) -> pa.Table:
        columns = self.columns()
        fields, arrays = [], []
        for name in ['OBJECTID', *s.EXPECTED_FIELDS, 'SHAPE_Length', 'SHAPE_Area', 'SHAPE']:
            kind = ('int64' if name == 'OBJECTID' else 'binary' if name == 'SHAPE'
                    else 'double' if name.startswith('SHAPE_') else 'string')
            arrow_type = {'int64': pa.int64(), 'binary': pa.binary(),
                          'double': pa.float64(), 'string': pa.string()}[kind]
            metadata = None
            if name in self.frozen_widths:
                entries = []
                width = self.frozen_widths[name]
                if width is not None:
                    entries.append((b'GDAL:OGR:width', str(width).encode()))
                entries.append((b'GDAL:OGR:alternative_name', self.frozen_aliases[name].encode()))
                metadata = dict(entries)
            fields.append(pa.field(name, arrow_type, metadata=metadata))
            arrays.append(pa.array(columns[name], type=arrow_type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def rewrite_capture(self, name: str, body: bytes) -> None:
        (self.capture / f'{name}.body').write_bytes(body)
        receipt = {'requested_url': s.URLS[name], 'status': 200, 'byte_length': len(body),
                   'sha256': s.sha(body), 'observed_at': OBSERVED_AT,
                   'final_url': s.URLS[name]}
        (self.capture / f'{name}.receipt.json').write_bytes(s.encode(receipt))

    def landing_body(self) -> bytes:
        return (b'<html><body><a href="../disdi/Downloads/installations_ranges.zip">zip</a>'
                b'<a href="../disdi/Downloads/release_notes.pdf">notes</a>'
                b'<p>Military Installations, Ranges and Training Areas</p>'
                b'<a href="https://geospatial.sec.usace.army.mil/x">viewer</a>'
                b'<a href="http://www.Data.gov">data.gov</a></body></html>')

    def bodies(self) -> dict:
        return {'landing': self.landing_body(), 'release_notes': b'%PDF-1.4 synthetic notes',
                'archive': self.zip_bytes}

    def _write_captures(self) -> None:
        for name, body in self.bodies().items():
            self.rewrite_capture(name, body)

    def patches(self):
        real_read_info, real_read_arrow = s.pyogrio.read_info, s.pyogrio.read_arrow

        def read_info(path, layer=None, **kwargs):
            if str(path).endswith(s.GDB_TOP_DIR):
                if layer == s.BOUNDARY_LAYER:
                    return {'features': self.FEATURES, 'crs': 'EPSG:4326',
                            'geometry_type': 'MultiPolygon', 'fid_column': 'OBJECTID',
                            'geometry_name': 'SHAPE', 'driver': 'OpenFileGDB',
                            'fields': [*s.EXPECTED_FIELDS, 'SHAPE_Length', 'SHAPE_Area'],
                            'dtypes': ['object'] * len(s.EXPECTED_FIELDS) + ['float64'] * 2,
                            'total_bounds': self.pins['EXPECTED_BOUNDS']}
                if layer == s.POINT_LAYER:
                    return {'features': self.pins['POINT_FEATURES'], 'crs': 'EPSG:4326',
                            'geometry_type': 'Point', 'fid_column': 'OBJECTID',
                            'geometry_name': 'SHAPE', 'driver': 'OpenFileGDB',
                            'fields': list(s.POINT_FIELDS),
                            'total_bounds': self.pins['POINT_BOUNDS']}
                raise AssertionError('unexpected gdb layer ' + str(layer))
            return real_read_info(path, layer=layer, **kwargs)

        def read_arrow(path, layer=None, **kwargs):
            if not str(path).endswith(s.GDB_TOP_DIR):
                return real_read_arrow(path, layer=layer, **kwargs)
            assert layer == s.BOUNDARY_LAYER and kwargs.get('return_fids')
            return {'fid_column': 'OBJECTID', 'geometry_name': 'SHAPE'}, self._arrow_table()

        items = [patch.object(s, name, value) for name, value in self.pins.items()]
        items += [patch.object(s.pyogrio, 'list_layers',
                               return_value=[(s.BOUNDARY_LAYER, 'MultiPolygon'),
                                             (s.POINT_LAYER, 'Point')]),
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


class MirtaMechanicsTests(unittest.TestCase):
    def test_tls_context_loads_pinned_intermediate_and_detects_tamper(self):
        context = s.tls_context()
        self.assertIsInstance(context, ssl.SSLContext)
        self.assertIn('TrustID Server CA O1', s.TRUSTID_INTERMEDIATE_PEM)
        der = base64.b64decode(''.join(line for line in s.TRUSTID_INTERMEDIATE_PEM.splitlines()
                                       if '-----' not in line))
        self.assertEqual(hashlib.sha256(der).hexdigest(), s.PINNED_INTERMEDIATE_SHA256)
        with patch.object(s, 'PINNED_INTERMEDIATE_SHA256', '0' * 64):
            with self.assertRaisesRegex(ValueError, 'pinned_tls_intermediate_digest_drift'):
                s.tls_context()

    def test_walk_wkb_counts_and_rejects_trailing_bytes(self):
        for wkb in (_square_wkb(0.0, 0.0), _structural_curve_wkb(), _arc_curve_wkb()):
            count, codes, end = s.walk_wkb(wkb)
            self.assertEqual(end, len(wkb))
            self.assertGreater(count, 0)
        with self.assertRaisesRegex(ValueError, 'wkb_trailing_bytes'):
            s.walk_wkb(_square_wkb(0.0, 0.0) + b'\x00')

    def test_wrap_multi_polygon_and_type_guard(self):
        wrapped = s.wrap_multi_polygon(_polygon_wkb([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]))
        self.assertEqual(shapely.from_wkb(wrapped).geom_type, 'MultiPolygon')
        with self.assertRaisesRegex(ValueError, 'wrap_requires_polygon_wkb'):
            s.wrap_multi_polygon(_square_wkb(0.0, 0.0))

    def test_remote_and_published_storage_rejected(self):
        with self.assertRaisesRegex(ValueError, 'draft_requires_explicit_isolated_local_staging'):
            s.require_local(PublishedStorageResource(local_dir='/tmp/x', use_local=True))
        with self.assertRaisesRegex(ValueError, 'draft_requires_explicit_isolated_local_staging'):
            s.require_local(StagingStorageResource(bucket='some-bucket', use_local=False))


class MirtaScenarioTests(unittest.TestCase):
    @staticmethod
    def stage(case, storage, **kwargs):
        return s.stage_mirta_boundaries(storage, **kwargs)

    def test_complete_candidate_build_is_release_blocked_and_reusable(self):
        with scenario() as (case, storage):
            result = self.stage(case, storage, capture_dir=case.capture)
            self.assertFalse(result['reused'])
            self.assertEqual(result['feature_count'], case.FEATURES)
            self.assertTrue(result['release_blocked'])
            prefix = f"{result['dataset_slug']}/{result['file_slug']}/{result['version']}"
            root = Path(storage.local_dir) / prefix
            self.assertIn('geoparquet/mirta-dod-sites-boundaries.parquet',
                          {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()})
            manifest = json.loads((root / 'metadata/source_manifest.json').read_bytes())
            self.assertFalse(manifest['approved'])
            self.assertEqual(len(manifest['inputs']), 3)
            audit = json.loads((root / 'metadata/source_audit.json').read_bytes())
            self.assertEqual(audit['linearization']['kept_exact_features'], 4)
            self.assertEqual(audit['linearization']['linearized_features'], 2)
            parquet = pq.read_table(root / 'geoparquet' / 'mirta-dod-sites-boundaries.parquet')
            self.assertEqual(parquet.num_rows, case.FEATURES)
            geometries = shapely.from_wkb(parquet['shape'].to_pylist())
            self.assertTrue(all(g.geom_type == 'MultiPolygon' for g in geometries))
            self.assertTrue(all(g.is_valid or i == 1 for i, g in enumerate(geometries)))
            arc = geometries[5]
            self.assertEqual(arc.bounds[0], 1.5)
            self.assertEqual(arc.bounds[2], 3.5)
            second = self.stage(case, storage, capture_dir=case.capture)
            self.assertTrue(second['reused'])
            self.assertEqual(second['version'], result['version'])

    def test_prebuilt_native_catalog_reconciles(self):
        with scenario() as (case, storage):
            result = self.stage(case, storage, capture_dir=case.capture)
            from dagster_hifld.catalog import summarize_staged_catalog
            from dagster_hifld.source_manifest import load_resolved_source_manifest
            resolved = load_resolved_source_manifest(storage, result['dataset_slug'],
                                                     result['file_slug'], result['version'])
            summary = summarize_staged_catalog(storage, result['dataset_slug'],
                                               result['file_slug'], result['version'],
                                               result['dataset_slug'],
                                               source_metadata=resolved.metadata)
            self.assertEqual(summary.quality_manifest['feature_count'], case.FEATURES)

    def test_archive_pin_drift_is_quarantined(self):
        def mutate(case):
            case.rewrite_capture('archive', case.zip_bytes + b'\x00')
        with scenario(mutate) as (case, storage):
            with self.assertRaisesRegex(ValueError, 'pinned_archive_content_drift'):
                self.stage(case, storage, capture_dir=case.capture)
            quarantined = list((Path(storage.local_dir) / '_quarantine' / s.SLUG).glob('*/response.body'))
            self.assertEqual(len(quarantined), 1)

    def test_landing_marker_drift_is_quarantined(self):
        def mutate(case):
            case.rewrite_capture('landing', b'<html>no markers here</html>')
        with scenario(mutate) as (case, storage):
            with self.assertRaisesRegex(ValueError, 'landing_marker_drift'):
                self.stage(case, storage, capture_dir=case.capture)

    def test_zip_member_census_drift_is_quarantined(self):
        def mutate(case):
            case.pins['ZIP_MEMBERS'] = case.pins['ZIP_MEMBERS'] + 1
        with scenario(mutate) as (case, storage):
            with self.assertRaisesRegex(ValueError, 'zip_member_census_drift'):
                self.stage(case, storage, capture_dir=case.capture)

    def test_boundary_layer_census_drift_is_quarantined(self):
        def mutate(case):
            case.pins['BOUNDARY_FEATURES'] = case.FEATURES + 1
        with scenario(mutate) as (case, storage):
            with self.assertRaisesRegex(ValueError, 'boundary_layer_identity_or_schema_changed'):
                self.stage(case, storage, capture_dir=case.capture)

    def test_sibling_point_layer_census_drift_is_quarantined(self):
        def mutate(case):
            case.pins['POINT_FEATURES'] = 8
        with scenario(mutate) as (case, storage):
            with self.assertRaisesRegex(ValueError, 'sibling_point_layer_census_drift'):
                self.stage(case, storage, capture_dir=case.capture)

    def test_null_census_drift_is_quarantined(self):
        def mutate(case):
            case.pins['EXPECTED_NULLS'] = dict(case.pins['EXPECTED_NULLS'])
            case.pins['EXPECTED_NULLS']['sdsId'] = 2
        with scenario(mutate) as (case, storage):
            with self.assertRaisesRegex(ValueError, 'null_census_drift'):
                self.stage(case, storage, capture_dir=case.capture)

    def test_value_census_drift_is_quarantined(self):
        def mutate(case):
            case.pins['EXPECTED_VALUE_CENSUSES'] = {name: dict(values)
                                                    for name, values in case.pins['EXPECTED_VALUE_CENSUSES'].items()}
            case.pins['EXPECTED_VALUE_CENSUSES']['stateNameCode']['ca'] = 4
        with scenario(mutate) as (case, storage):
            with self.assertRaisesRegex(ValueError, 'value_census_drift:stateNameCode'):
                self.stage(case, storage, capture_dir=case.capture)

    def test_sds_census_drift_is_quarantined(self):
        def mutate(case):
            case.pins['EXPECTED_SDS_ID_SHARED_GROUPS'] = 2
        with scenario(mutate) as (case, storage):
            with self.assertRaisesRegex(ValueError, 'sds_identifier_census_drift'):
                self.stage(case, storage, capture_dir=case.capture)

    def test_source_geometry_census_drift_is_quarantined(self):
        def mutate(case):
            case.pins['EXPECTED_CURVED_FEATURES'] = 3
        with scenario(mutate) as (case, storage):
            with self.assertRaisesRegex(ValueError, 'source_geometry_census_drift'):
                self.stage(case, storage, capture_dir=case.capture)

    def test_field_width_drift_is_quarantined(self):
        def mutate(case):
            case.frozen_widths['siteName'] = 101
        with scenario(mutate) as (case, storage):
            with self.assertRaisesRegex(ValueError, 'field_width_or_alias_drift:siteName'):
                self.stage(case, storage, capture_dir=case.capture)

    def test_field_alias_drift_is_quarantined(self):
        def mutate(case):
            case.frozen_aliases['countryName'] = 'Wrong Alias'
        with scenario(mutate) as (case, storage):
            with self.assertRaisesRegex(ValueError, 'field_width_or_alias_drift:countryName'):
                self.stage(case, storage, capture_dir=case.capture)

    def test_tampered_cached_candidate_is_quarantined_and_moved(self):
        with scenario() as (case, storage):
            result = self.stage(case, storage, capture_dir=case.capture)
            prefix = f"{result['dataset_slug']}/{result['file_slug']}/{result['version']}"
            parquet = (Path(storage.local_dir) / prefix / 'geoparquet'
                       / 'mirta-dod-sites-boundaries.parquet')
            body = bytearray(parquet.read_bytes())
            body[-1] ^= 0x01
            parquet.write_bytes(bytes(body))
            with self.assertRaisesRegex(ValueError, 'existing_candidate_object_changed'):
                self.stage(case, storage, capture_dir=case.capture)
            moved = list((Path(storage.local_dir) / '_quarantine' / s.SLUG).glob('invalid-cache-*'))
            self.assertEqual(len(moved), 1)


@unittest.skipUnless(CAPTURE, 'Complete real source is an explicit external artifact opt-in')
class MirtaCompleteSourceTests(unittest.TestCase):
    def test_complete_original_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = StagingStorageResource(local_dir=str(Path(tmp) / 'staging'))
            result = s.stage_mirta_boundaries(staging, capture_dir=CAPTURE)
            self.assertEqual(result['feature_count'], 825)
            self.assertFalse(result['reused'])
            prefix = f"{result['dataset_slug']}/{result['file_slug']}/{result['version']}"
            root = Path(staging.local_dir) / prefix
            parquet = pq.read_table(root / 'geoparquet' / 'mirta-dod-sites-boundaries.parquet')
            self.assertEqual(parquet.schema.names,
                             ['OBJECTID', *s.EXPECTED_FIELDS, 'SHAPE_Length', 'SHAPE_Area', 'shape'])
            audit = json.loads((root / 'metadata/source_audit.json').read_bytes())
            self.assertEqual(audit['source_wkb_subtype_census'], s.EXPECTED_SOURCE_WKB_SUBTYPES)
            self.assertEqual(audit['source_vertices'], s.EXPECTED_SOURCE_VERTICES)
            self.assertEqual(audit['linearization']['linearized_features'], s.EXPECTED_CURVED_FEATURES)
            self.assertEqual(audit['linearization']['kept_exact_features'], s.EXPECTED_LINEAR_FEATURES)
            self.assertEqual(audit['linearization']['candidate_geometry_sha256'],
                             RECORDED_CANDIDATE_GEOMETRY_SHA256)
            second = s.stage_mirta_boundaries(staging, capture_dir=CAPTURE)
            self.assertTrue(second['reused'])
            self.assertEqual(second['version'], result['version'])


if __name__ == '__main__':
    unittest.main()
