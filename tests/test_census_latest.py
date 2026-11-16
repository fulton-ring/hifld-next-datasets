"""Annual producer discovery must be explicit and fail closed."""
import tempfile
import unittest
import io
import json
import struct
import zipfile
from pathlib import Path
from unittest.mock import patch
import geopandas as gpd
from shapely.geometry import LineString
import httpx
from dagster_hifld.sources import census_latest

ROOT = 'https://www2.census.gov/geo/tiger/'

class CensusLatestTests(unittest.TestCase):
    def resolve(self, root, files):
        def respond(request):
            body = root if str(request.url) == ROOT else files
            return httpx.Response(200, stream=httpx.ByteStream(body.encode()), request=request)
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(respond)) as client:
            release = census_latest.discover_primary_roads(client, Path(tmp))
            self.assertTrue((Path(tmp) / 'annual-listing.receipt.json').exists())
            return release

    def test_latest_actual_published_edition_and_evidence(self):
        release = self.resolve('<a href="TIGER2026/">2026</a><a href="TIGER2027/">2027</a>', '<a href="tl_2027_us_primaryroads.zip">roads</a>')
        self.assertEqual(release['edition'], '2027')
        self.assertEqual(release['source_url'], ROOT + 'TIGER2027/PRIMARYROADS/tl_2027_us_primaryroads.zip')
        self.assertEqual(len(release['listing_sha256']), 64)

    def test_latest_incomplete_does_not_silently_fall_back(self):
        with self.assertRaisesRegex(ValueError, 'published_archive_missing_or_ambiguous'):
            self.resolve('<a href="TIGER2026/">2026</a><a href="TIGER2027/">2027</a>', '<a href="tl_2026_us_primaryroads.zip">old</a>')

    def test_lookalikes_and_external_links_are_not_annual_releases(self):
        with self.assertRaisesRegex(ValueError, 'annual_editions_missing'):
            self.resolve('<a href="TIGER2027preview/">preview</a><a href="https://elsewhere.test/TIGER2028/">external</a>', '')

    def test_duplicate_target_is_ambiguous(self):
        with self.assertRaisesRegex(ValueError, 'published_archive_missing_or_ambiguous'):
            self.resolve('<a href="TIGER2026/">2026</a>', '<a href="tl_2026_us_primaryroads.zip">a</a><a href="tl_2026_us_primaryroads.zip">b</a>')

    def test_version_changes_for_revision_and_edition_but_not_check_time(self):
        version = census_latest.release_version('2026', 'a'*64, 'code')
        self.assertEqual(version, census_latest.release_version('2026', 'a'*64, 'code'))
        self.assertNotEqual(version, census_latest.release_version('2026', 'b'*64, 'code'))
        self.assertNotEqual(version, census_latest.release_version('2027', 'a'*64, 'code'))

class CurrentRoadsTests(unittest.TestCase):
    @staticmethod
    def tiny_archive(directory, count=1):
        stem = 'tl_2027_us_primaryroads'
        frame = gpd.GeoDataFrame({'LINEARID':[f'{i:022d}' for i in range(count)], 'FULLNAME':['Road']*count, 'RTTYP':['I']*count, 'MTFCC':['S1100']*count}, geometry=[LineString([(i,0),(i+1,1)]) for i in range(count)], crs='EPSG:4269')
        frame.to_file(directory / (stem+'.shp'), engine='pyogrio')
        for suffix in ('.shp.iso.xml', '.shp.ea.iso.xml'):
            (directory / (stem+suffix)).write_text('<metadata/>')
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, 'w') as archive:
            for path in sorted(directory.glob(stem+'.*')):
                archive.writestr(path.name, path.read_bytes())
        return sink.getvalue(), frame

    def test_current_archive_does_not_enforce_old_count_or_bounds(self):
        import geopandas as gpd
        from shapely.geometry import LineString
        from unittest.mock import patch
        from dagster_hifld.sources import census_primary_roads as roads
        frame = gpd.GeoDataFrame({'LINEARID':['00001'], 'FULLNAME':['Road'], 'RTTYP':['I'], 'MTFCC':['S1100']}, geometry=[LineString([(0,0),(1,1)])], crs='EPSG:4269')
        with tempfile.TemporaryDirectory() as tmp:
            body, _ = self.tiny_archive(Path(tmp))
            audit = roads._inspect_archive(body, edition='2027')
        self.assertEqual(audit['feature_count'], 1)
        self.assertEqual(audit['bounds'], [0,0,1,1])

    def test_current_archive_rejects_numeric_ids(self):
        import geopandas as gpd
        from shapely.geometry import LineString
        from unittest.mock import patch
        from dagster_hifld.sources import census_primary_roads as roads
        frame = gpd.GeoDataFrame({'LINEARID':[1], 'FULLNAME':['Road'], 'RTTYP':['I'], 'MTFCC':['S1100']}, geometry=[LineString([(0,0),(1,1)])], crs='EPSG:4269')
        with tempfile.TemporaryDirectory() as tmp:
            body, _ = self.tiny_archive(Path(tmp))
            with patch.object(roads.census_tiger_common, 'read_tiger_shapefile', return_value=frame):
                with self.assertRaisesRegex(ValueError, 'physical_type'):
                    roads._inspect_archive(body, edition='2027')

    def test_declared_native_population_rejects_decoder_row_loss(self):
        from dagster_hifld.sources import census_primary_roads as roads
        with tempfile.TemporaryDirectory() as tmp:
            body, frame = self.tiny_archive(Path(tmp), count=2)
            with patch.object(roads.census_tiger_common, 'read_tiger_shapefile', return_value=frame.iloc[:1]):
                with self.assertRaisesRegex(ValueError, 'native_population'):
                    roads._inspect_archive(body, edition='2027')

    def test_native_headers_reject_deleted_truncated_and_mismatched_records(self):
        from dagster_hifld.sources import census_primary_roads as roads
        with tempfile.TemporaryDirectory() as tmp:
            body, frame = self.tiny_archive(Path(tmp), count=2)
            for defect in ('deleted', 'truncated', 'shx_count'):
                sink = io.BytesIO()
                with zipfile.ZipFile(io.BytesIO(body)) as original, zipfile.ZipFile(sink, 'w') as changed:
                    for item in original.infolist():
                        value = original.read(item.filename)
                        if item.filename.endswith('.dbf'):
                            if defect == 'deleted':
                                header_length = struct.unpack_from('<H', value, 8)[0]
                                value = value[:header_length] + b'*' + value[header_length+1:]
                            elif defect == 'truncated': value = value[:-2]
                        if item.filename.endswith('.shx') and defect == 'shx_count':
                            value = bytearray(value[:-8]); struct.pack_into('>I', value, 24, len(value)//2)
                        changed.writestr(item, value)
                with self.subTest(defect=defect), patch.object(roads.census_tiger_common, 'read_tiger_shapefile', return_value=frame):
                    with self.assertRaisesRegex(ValueError, 'native_population'):
                        roads._inspect_archive(sink.getvalue(), edition='2027')

    def test_current_capture_reuses_unchanged_bytes_and_preserves_old_version(self):
        import io
        import json
        import zipfile
        import geopandas as gpd
        from shapely.geometry import LineString
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources import census_primary_roads as roads
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stem = 'tl_2027_us_primaryroads'
            frame = gpd.GeoDataFrame({'LINEARID':['0000000000000000000001'], 'FULLNAME':['Road'], 'RTTYP':['I'], 'MTFCC':['S1100']}, geometry=[LineString([(0,0),(1,1)])], crs='EPSG:4269')
            frame.to_file(root / (stem+'.shp'), engine='pyogrio')
            for suffix in ('.shp.iso.xml', '.shp.ea.iso.xml'):
                (root / (stem+suffix)).write_text('<metadata/>')
            def archive():
                sink = io.BytesIO()
                with zipfile.ZipFile(sink, 'w') as zipped:
                    for path in sorted(root.glob(stem+'.*')):
                        zipped.writestr(path.name, path.read_bytes())
                return sink.getvalue()
            payload = [archive()]
            def respond(request):
                url = str(request.url)
                body = (b'<a href="TIGER2027/">2027</a>' if url == ROOT else
                        f'<a href="{stem}.zip">zip</a>'.encode() if url.endswith('/') else payload[0])
                return httpx.Response(200, stream=httpx.ByteStream(body), request=request)
            storage = StagingStorageResource(use_local=True, local_dir=str(root/'staging'))
            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                first = roads.capture_latest_census_primary_roads(storage, client=client)
                second = roads.capture_latest_census_primary_roads(storage, client=client)
                self.assertEqual(first['version'], second['version'])
                self.assertTrue(second['source_cache_reused'])
                self.assertEqual(first['feature_count'], 1)
                prefix = f"{roads.DATASET_SLUG}/{roads.FILE_SLUG}/{first['version']}"
                manifest = json.loads(storage.read_key(prefix + '/metadata/source_manifest.json'))
                self.assertIsInstance(manifest['inputs'][0]['refresh']['unknown_reasons'], dict)
                self.assertTrue(manifest['inputs'][0]['refresh']['unknown_reasons']['update_cadence'])
                ready = json.loads(storage.read_key(prefix + '/metadata/candidate_ready.json'))
                self.assertFalse(ready['release_accepted'])
                self.assertEqual(ready['source_archive_sha256'], first['source_archive_sha256'])
                old_key = first['written_keys'][0]
                old_bytes = storage.read_key(old_key)
                # Same edition, changed actual input bytes creates a new version.
                (root/(stem+'.shp.iso.xml')).write_text('<metadata>revision</metadata>')
                payload[0] = archive()
                revised = roads.capture_latest_census_primary_roads(storage, client=client)
                self.assertNotEqual(first['version'], revised['version'])
                self.assertEqual(storage.read_key(old_key), old_bytes)
                self.assertFalse(revised['release_accepted'])

    def test_discovery_write_failure_exposes_no_source_archive_or_ready_marker(self):
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources import census_primary_roads as roads
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            body, _ = self.tiny_archive(root)
            def respond(request):
                value = b'<a href="TIGER2027/">2027</a>' if str(request.url) == ROOT else b'<a href="tl_2027_us_primaryroads.zip">zip</a>' if str(request.url).endswith('/') else body
                return httpx.Response(200, stream=httpx.ByteStream(value), request=request)
            storage = StagingStorageResource(use_local=True, local_dir=str(root/'stage'))
            original = roads.write_immutable
            def fail_discovery(storage, key, value):
                if '/metadata/discovery/' in key: raise OSError('injected evidence storage failure')
                return original(storage, key, value)
            with httpx.Client(transport=httpx.MockTransport(respond)) as client, patch.object(roads, 'write_immutable', side_effect=fail_discovery):
                with self.assertRaisesRegex(ValueError, 'quarantined_at'):
                    roads.capture_latest_census_primary_roads(storage, client=client)
            keys = storage.list_prefix(roads.DATASET_SLUG)
            self.assertFalse(any('/shapefile/' in key or key.endswith('/candidate_ready.json') for key in keys))

    def test_partial_archive_is_quarantined_not_unchanged(self):
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources import census_primary_roads as roads
        def respond(request):
            body = (b'<a href="TIGER2027/">2027</a>' if str(request.url) == ROOT else
                    b'<a href="tl_2027_us_primaryroads.zip">zip</a>' if str(request.url).endswith('/') else b'partial')
            return httpx.Response(200, stream=httpx.ByteStream(body), request=request)
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(respond)) as client:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with self.assertRaisesRegex(ValueError, 'quarantined_at'):
                roads.capture_latest_census_primary_roads(storage, client=client)
            self.assertTrue(storage.list_prefix('_quarantine/primary-roads-2'))
            self.assertFalse(storage.list_prefix('primary-roads-2'))
