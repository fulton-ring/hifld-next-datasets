"""Offline native FileGDB lifecycle tests; no dataset acceptance is implied."""
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyogrio
import shapely

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import census_designated_places as cdp
from dagster_hifld.sources import census_latest
from dagster_hifld.sources import county_subdivisions as county


ROOT = census_latest.ROOT
DIRECTORY = ROOT + 'TGRGDB26/'
FILENAME = 'tlgdb_2026_us_substategeo.gdb.zip'
URL = DIRECTORY + FILENAME
ANNUAL = b'<a href="TGRGDB26/">26</a>'
LISTING = ('<a href="' + FILENAME + '">archive</a>').encode()


def native_archive(root):
    gdb = root / FILENAME[:-4]
    geometry = shapely.to_wkb(shapely.MultiPolygon([
        shapely.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])]))
    for module in (cdp, county):
        rows = []
        for i in (1, 2):
            geoid = f'01000{i:02d}' if module is cdp else f'010019000{i}'
            rows.append(dict(zip(module.FIELDS[1:], [
                f'{i:08d}', geoid,
                ('1600000US' if module is cdp else '0600000US') + geoid,
                'Synthetic fixture', 'U1' if module is cdp else 'Z5', 'S',
                1., 0., '+00.0', '-000.0', geometry])))
        table = pa.Table.from_pylist(rows, schema=pa.schema(zip(module.FIELDS[1:], module.TYPES[1:])))
        pyogrio.write_arrow(table, gdb, layer=module.LAYER, driver='OpenFileGDB',
                            geometry_name='SHAPE', geometry_type='MultiPolygon',
                            crs='EPSG:4269', layer_options={'GEOMETRY_NAME': 'SHAPE'})
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, 'w') as archive:
        for path in sorted(gdb.iterdir()):
            archive.writestr(gdb.name + '/' + path.name, path.read_bytes())
    return sink.getvalue()


def stage(module, storage, **kwargs):
    function = module.stage_cdp_candidate if module is cdp else module.stage_county_subdivisions_candidate
    return function(storage, **kwargs)


def snapshot(root):
    return {str(path.relative_to(root)): path.read_bytes()
            for path in root.rglob('*') if path.is_file()}


class SubstateConstructionTests(unittest.TestCase):
    def test_discovery_acquisition_construction_replay_and_live_cache(self):
        client_type = httpx.Client
        for module in (cdp, county):
            with self.subTest(module=module.SLUG), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                body = native_archive(root)
                listings = {ROOT: ANNUAL, DIRECTORY: LISTING}
                storage = StagingStorageResource(local_dir=str(root / 'stage'))
                def respond(request):
                    return httpx.Response(200, stream=httpx.ByteStream({
                        **listings, URL: body}[str(request.url)]))
                with patch.object(module, 'FEATURE_COUNT_RANGE', (2, 3)), patch.object(
                        module.httpx, 'Client', side_effect=lambda **kwargs: client_type(
                            transport=httpx.MockTransport(respond), **kwargs)):
                    first = stage(module, storage)
                    target = Path(storage.local_dir) / module.SLUG / module.SLUG / first['version']
                    before = snapshot(target)
                    receipt_path = target / 'raw/retrieval.json'
                    receipt = json.loads(receipt_path.read_bytes())
                    release = census_latest.geodatabase_release_from_receipt(receipt, 'substategeo')
                    self.assertEqual(release.listing_sha256, module.digest(LISTING))
                    self.assertEqual(release.annual_listing_sha256, module.digest(ANNUAL))
                    self.assertEqual(release.listing_bytes, len(LISTING))
                    self.assertEqual(json.loads(before['metadata/source_manifest.json'])['source_release'],
                                     release.as_metadata())
                    listings[ROOT] += b'\n<!-- unrelated annual index update -->'
                    listings[DIRECTORY] += b'\n<!-- unrelated directory update -->'
                    refresh = stage(module, storage)
                    self.assertEqual(first['version'], refresh['version'])
                    self.assertTrue(refresh['reused'])
                    self.assertEqual(snapshot(target), before)
                    archive = Path(storage.local_dir) / module.RAW_KEY_PREFIX / receipt['sha256'] / 'original.gdb.zip'
                    with patch.object(module.httpx, 'Client', side_effect=AssertionError('replay fetched HTTP')):
                        replay = stage(module, storage, archive_path=archive, receipt_path=receipt_path)
                    self.assertTrue(replay['reused'])
                    refresh = stage(module, storage)
                    self.assertEqual(first['version'], refresh['version'])
                    self.assertTrue(refresh['reused'])
                    self.assertEqual(snapshot(target), before)

    def test_runtime_change_preserves_previous_candidate_and_construction(self):
        for module in (cdp, county):
            with self.subTest(module=module.SLUG), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                raw = root / 'original.zip'
                raw.write_bytes(native_archive(root))
                receipt_path = root / 'receipt.json'
                receipt_path.write_bytes(module.encode({
                    'url': URL, 'final_url': URL, 'status': 200, 'complete_body': True,
                    'observed_at': '2026-11-19T12:00:00+00:00',
                    'bytes': raw.stat().st_size, 'sha256': module.sha256_file(raw)}))
                storage = StagingStorageResource(local_dir=str(root / 'stage'))
                with patch.object(module, 'FEATURE_COUNT_RANGE', (2, 3)):
                    identity = module.processing_identity()
                    first = stage(module, storage, archive_path=raw, receipt_path=receipt_path)
                    old = Path(storage.local_dir) / module.SLUG / module.SLUG / first['version']
                    before = snapshot(old)
                    construction_root = Path(storage.local_dir) / (
                        '_source-provenance' if module is cdp else '_construction-state')
                    construction = snapshot(construction_root)
                    with patch.object(module.pyogrio, '__gdal_version_string__', 'different-build'):
                        changed = module.processing_identity()
                        self.assertEqual(identity['code_commit'], changed['code_commit'])
                        self.assertNotEqual(identity, changed)
                        second = stage(module, storage, archive_path=raw, receipt_path=receipt_path)
                        self.assertNotEqual(first['version'], second['version'])
                        self.assertFalse(second['reused'])
                        self.assertTrue(stage(module, storage, archive_path=raw,
                                              receipt_path=receipt_path)['reused'])
                    self.assertEqual(snapshot(old), before)
                    after = snapshot(construction_root)
                    self.assertEqual({name: after[name] for name in construction}, construction)
                    self.assertFalse((Path(storage.local_dir) / '_quarantine').exists())
                    self.assertTrue(stage(module, storage, archive_path=raw,
                                          receipt_path=receipt_path)['reused'])
