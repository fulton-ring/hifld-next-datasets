"""Annual producer discovery must be explicit and fail closed."""
import tempfile
import unittest
import io
import json
import hashlib
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


class TigerGeodatabaseDiscoveryTests(unittest.TestCase):
    ROOT = 'https://www2.census.gov/geo/tiger/'

    def resolve(self, product, root, directories):
        def respond(request):
            url = str(request.url)
            if url == self.ROOT:
                return httpx.Response(200, stream=httpx.ByteStream(root.encode()), request=request)
            if url in directories:
                body = directories[url]
                if body is None:
                    return httpx.Response(404, stream=httpx.ByteStream(b''), request=request)
                return httpx.Response(200, stream=httpx.ByteStream(body.encode()), request=request)
            return httpx.Response(404, stream=httpx.ByteStream(b''), request=request)
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(respond)) as client:
            return census_latest.discover_geodatabase(client, Path(tmp), product)

    def test_newest_published_edition_and_exact_national_archive(self):
        release = self.resolve(
            'substategeo',
            '<a href="TGRGDB24/">24</a><a href="TGRGDB26/">26</a>',
            {'https://www2.census.gov/geo/tiger/TGRGDB26/':
             '<a href="tlgdb_2026_01_al.gdb.zip">al</a><a href="tlgdb_2026_us_substategeo.gdb.zip">us</a>'})
        self.assertEqual(release.vintage, 2026)
        self.assertEqual(release.source_file, 'tlgdb_2026_us_substategeo.gdb.zip')
        self.assertEqual(release.source_url, self.ROOT + 'TGRGDB26/tlgdb_2026_us_substategeo.gdb.zip')

    def test_legacy_a_national_name_is_accepted(self):
        release = self.resolve(
            'school', '<a href="TGRGDB24/">24</a>',
            {'https://www2.census.gov/geo/tiger/TGRGDB24/':
             '<a href="tlgdb_2024_a_us_school.gdb.zip">us</a>'})
        self.assertEqual(release.source_file, 'tlgdb_2024_a_us_school.gdb.zip')

    def test_newest_incomplete_edition_fails_closed(self):
        with self.assertRaisesRegex(ValueError, 'published_geodatabase_missing_or_ambiguous'):
            self.resolve(
                'substategeo',
                '<a href="TGRGDB26/">26</a><a href="TGRGDB27/">27</a>',
                {'https://www2.census.gov/geo/tiger/TGRGDB27/':
                 '<a href="tlgdb_2027_01_al.gdb.zip">al</a>'})

    def test_absent_newest_directory_probes_the_previous_edition(self):
        release = self.resolve(
            'substategeo',
            '<a href="TGRGDB26/">26</a><a href="TGRGDB27/">27</a>',
            {'https://www2.census.gov/geo/tiger/TGRGDB27/': None,
             'https://www2.census.gov/geo/tiger/TGRGDB26/':
             '<a href="tlgdb_2026_us_substategeo.gdb.zip">us</a>'})
        self.assertEqual(release.vintage, 2026)

    def test_lookalike_and_external_links_are_not_editions(self):
        with self.assertRaisesRegex(ValueError, 'geodatabase_editions_missing'):
            self.resolve('substategeo', '<a href="TGRGDB26preview/">x</a>'
                         '<a href="https://elsewhere.test/TGRGDB27/">x</a>', {})

    def test_duplicate_archive_is_ambiguous(self):
        with self.assertRaisesRegex(ValueError, 'published_geodatabase_missing_or_ambiguous'):
            self.resolve(
                'substategeo', '<a href="TGRGDB26/">26</a>',
                {'https://www2.census.gov/geo/tiger/TGRGDB26/':
                 '<a href="tlgdb_2026_us_substategeo.gdb.zip">a</a>'
                 '<a href="tlgdb_2026_us_substategeo.gdb.zip">b</a>'})


class RetainedGeodatabaseReleaseTests(unittest.TestCase):
    def test_url_and_receipt_rebuild_the_resolved_release(self):
        url = 'https://www2.census.gov/geo/tiger/TGRGDB26/tlgdb_2026_us_addr.gdb.zip'
        release = census_latest.geodatabase_release_from_url(url, 'addr')
        self.assertEqual(release.vintage, 2026)
        self.assertEqual(release.source_file, 'tlgdb_2026_us_addr.gdb.zip')
        self.assertEqual(release.directory_url, 'https://www2.census.gov/geo/tiger/TGRGDB26/')
        self.assertEqual(release.as_metadata()['tiger_vintage'], '2026')
        self.assertEqual(census_latest.geodatabase_release_from_receipt({'url': url}, 'addr'), release)

    def test_wrong_product_host_or_edition_is_rejected(self):
        for url, product in [
            ('https://www2.census.gov/geo/tiger/TGRGDB26/tlgdb_2026_us_addr.gdb.zip', 'substategeo'),
            ('https://elsewhere.test/geo/tiger/TGRGDB26/tlgdb_2026_us_addr.gdb.zip', 'addr'),
            ('https://www2.census.gov/geo/tiger/TGRGDB25/tlgdb_2026_us_addr.gdb.zip', 'addr'),
        ]:
            with self.subTest(url=url, product=product), self.assertRaisesRegex(ValueError, 'source_url_not_current_tgrgdb'):
                census_latest.geodatabase_release_from_url(url, product)
        with self.assertRaisesRegex(ValueError, 'source_url_not_current_tgrgdb'):
            census_latest.geodatabase_release_from_receipt({'url': None}, 'addr')


class TigerShapefileDiscoveryTests(unittest.TestCase):
    ROOT = 'https://www2.census.gov/geo/tiger/'

    def resolve(self, layer, product, root, directories, **kwargs):
        def respond(request):
            url = str(request.url)
            if url == self.ROOT:
                return httpx.Response(200, stream=httpx.ByteStream(root.encode()), request=request)
            if url in directories:
                body = directories[url]
                if body is None:
                    return httpx.Response(404, stream=httpx.ByteStream(b''), request=request)
                return httpx.Response(200, stream=httpx.ByteStream(body.encode()), request=request)
            return httpx.Response(404, stream=httpx.ByteStream(b''), request=request)
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(respond)) as client:
            return census_latest.discover_shapefile(client, Path(tmp), layer, product, **kwargs)

    def test_national_shapefile_uses_newest_complete_edition(self):
        release = self.resolve(
            'COUNTY', 'county', '<a href="TIGER2026/">26</a><a href="TIGER2027/">27</a>',
            {'https://www2.census.gov/geo/tiger/TIGER2027/COUNTY/':
             '<a href="tl_2027_us_county.zip">us</a>'})
        self.assertEqual(release.vintage, 2027)
        self.assertEqual(release.source_url, self.ROOT + 'TIGER2027/COUNTY/tl_2027_us_county.zip')

    def test_state_shapefile_uses_exact_state_geoid(self):
        release = self.resolve(
            'SUBBARRIO', 'subbarrio', '<a href="TIGER2026/">26</a>',
            {'https://www2.census.gov/geo/tiger/TIGER2026/SUBBARRIO/':
             '<a href="tl_2026_72_subbarrio.zip">pr</a>'}, geoid='72')
        self.assertEqual(release.source_file, 'tl_2026_72_subbarrio.zip')

    def test_newest_incomplete_shapefile_fails_closed(self):
        with self.assertRaisesRegex(ValueError, 'published_archive_missing_or_ambiguous'):
            self.resolve(
                'COUNTY', 'county', '<a href="TIGER2026/">26</a><a href="TIGER2027/">27</a>',
                {'https://www2.census.gov/geo/tiger/TIGER2027/COUNTY/':
                 '<a href="tl_2026_us_county.zip">old</a>'})

class CurrentRoadsTests(unittest.TestCase):
    @staticmethod
    def tiny_archive(directory, count=1, edition='2027'):
        stem = f'tl_{edition}_us_primaryroads'
        frame = gpd.GeoDataFrame({'LINEARID':[f'{i:022d}' for i in range(count)], 'FULLNAME':['Road']*count, 'RTTYP':['I']*count, 'MTFCC':['S1100']*count}, geometry=[LineString([(i,0),(i+1,1)]) for i in range(count)], crs='EPSG:4269')
        frame.to_file(directory / (stem+'.shp'), engine='pyogrio')
        (directory / (stem+'.shp.iso.xml')).write_text('<metadata>Primary roads fixture methodology.</metadata>')
        (directory / (stem+'.shp.ea.iso.xml')).write_text('''<gfc:FC_FeatureCatalogue xmlns:gfc="http://www.isotc211.org/2005/gfc" xmlns:gco="http://www.isotc211.org/2005/gco">
<gfc:FC_FeatureAttribute><gfc:memberName><gco:LocalName>LINEARID</gco:LocalName></gfc:memberName><gfc:definition><gco:CharacterString>Linear feature identifier</gco:CharacterString></gfc:definition></gfc:FC_FeatureAttribute>
<gfc:FC_FeatureAttribute><gfc:memberName><gco:LocalName>RTTYP</gco:LocalName></gfc:memberName><gfc:definition><gco:CharacterString>Route type code</gco:CharacterString></gfc:definition><gfc:listedValue><gfc:FC_ListedValue><gfc:label><gco:CharacterString>I</gco:CharacterString></gfc:label><gfc:definition><gco:CharacterString>Interstate</gco:CharacterString></gfc:definition></gfc:FC_ListedValue></gfc:listedValue></gfc:FC_FeatureAttribute>
<gfc:FC_FeatureAttribute><gfc:memberName><gco:LocalName>MTFCC</gco:LocalName></gfc:memberName><gfc:definition><gco:CharacterString>MAF/TIGER feature class code</gco:CharacterString></gfc:definition><gfc:listedValue><gfc:FC_ListedValue><gfc:label><gco:CharacterString>S1100</gco:CharacterString></gfc:label><gfc:definition><gco:CharacterString>Primary Road</gco:CharacterString></gfc:definition></gfc:FC_ListedValue></gfc:listedValue></gfc:FC_FeatureAttribute>
</gfc:FC_FeatureCatalogue>''')
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, 'w') as archive:
            for path in sorted(directory.glob(stem+'.*')):
                archive.writestr(path.name, path.read_bytes())
        return sink.getvalue(), frame

    @staticmethod
    def capture_fixture(storage, body):
        from dagster_hifld.sources import census_primary_roads as roads
        def respond(request):
            value = b'<a href="TIGER2027/">2027</a>' if str(request.url) == ROOT else b'<a href="tl_2027_us_primaryroads.zip">zip</a>' if str(request.url).endswith('/') else body
            return httpx.Response(200, stream=httpx.ByteStream(value), request=request)
        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            return roads.capture_latest_census_primary_roads(storage, client=client)

    def test_current_native_xml_documents_semantics_and_coverage_are_retained(self):
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources import census_primary_roads as roads
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); body, _ = self.tiny_archive(root)
            storage = StagingStorageResource(use_local=True, local_dir=str(root/'stage'))
            result = self.capture_fixture(storage, body)
            prefix = f"{roads.DATASET_SLUG}/{roads.FILE_SLUG}/{result['version']}"
            manifest = json.loads(storage.read_key(prefix+'/metadata/source_manifest.json'))
            ready = json.loads(storage.read_key(prefix+'/metadata/candidate_ready.json'))
            self.assertEqual(len(manifest['documentation']), 2)
            with zipfile.ZipFile(io.BytesIO(body)) as archive:
                for doc in manifest['documentation']:
                    expected = archive.read(Path(doc['path']).name)
                    self.assertEqual(storage.read_key(prefix+'/'+doc['path']), expected)
                    self.assertEqual(doc['bytes'], len(expected))
                    self.assertEqual(doc['sha256'], hashlib.sha256(expected).hexdigest())
                    self.assertEqual(doc['media_type'], 'application/xml')
                    self.assertEqual(doc['input_id'], manifest['inputs'][0]['input_id'])
                    self.assertIn(doc['purpose'], {'schema', 'methodology'})
                    self.assertEqual(ready['objects'][doc['path']], doc['sha256'])
            fields = manifest['field_definitions']
            self.assertEqual(set(fields), set(roads.EXPECTED_FIELDS))
            self.assertEqual(fields['LINEARID']['description'], 'Linear feature identifier')
            self.assertEqual(fields['LINEARID']['basis'], 'publisher')
            self.assertEqual(fields['FULLNAME']['basis'], 'unknown')
            self.assertTrue(fields['FULLNAME']['unknown_reasons']['definition'])
            self.assertEqual(fields['RTTYP']['codes'], {'I': 'Interstate'})
            self.assertEqual(fields['MTFCC']['codes'], {'S1100': 'Primary Road'})
            self.assertEqual(manifest['coverage']['observed_mtfcc_values'], ['S1100'])
            self.assertEqual(manifest['coverage']['served_feature_count'], 1)
            self.assertTrue(manifest['coverage']['intended_population'])
            self.assertEqual(manifest['source_relationship']['relationship'], 'unverified')
            self.assertTrue(manifest['source_relationship']['unknown_reason'])

    def test_current_xml_write_failure_cannot_expose_source_or_ready_marker(self):
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources import census_primary_roads as roads
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); body, _ = self.tiny_archive(root)
            storage = StagingStorageResource(use_local=True, local_dir=str(root/'stage'))
            original = roads.write_immutable
            def fail_xml(storage, key, value):
                if key.endswith('.ea.iso.xml'): raise OSError('injected native XML write failure')
                return original(storage, key, value)
            with patch.object(roads, 'write_immutable', side_effect=fail_xml):
                with self.assertRaisesRegex(ValueError, 'quarantined_at'):
                    self.capture_fixture(storage, body)
            self.assertFalse(any('/shapefile/' in key or key.endswith('/candidate_ready.json') for key in storage.list_prefix(roads.DATASET_SLUG)))

    def test_changed_staged_native_xml_is_rejected_on_reuse(self):
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources import census_primary_roads as roads
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); body, _ = self.tiny_archive(root)
            storage = StagingStorageResource(use_local=True, local_dir=str(root/'stage'))
            result = self.capture_fixture(storage, body)
            prefix = f"{roads.DATASET_SLUG}/{roads.FILE_SLUG}/{result['version']}"
            xml_key = prefix+'/metadata/source/tl_2027_us_primaryroads.shp.ea.iso.xml'
            storage.write_key(xml_key, b'changed native XML')
            with self.assertRaisesRegex(ValueError, 'quarantined_at'):
                self.capture_fixture(storage, body)

    def test_historical_reproduction_manifest_and_cache_do_not_gain_current_metadata(self):
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.sources import census_primary_roads as roads
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); body, _ = self.tiny_archive(root, edition='2025')
            storage = StagingStorageResource(use_local=True, local_dir=str(root/'stage'))
            receipt = {'url': roads.SOURCE_URL, 'final_url': roads.SOURCE_URL, 'observed_at': '2026-11-16T00:00:00+00:00', 'sha256': hashlib.sha256(body).hexdigest(), 'bytes': len(body)}
            with patch.object(roads, 'EXPECTED_SHA256', receipt['sha256']), patch.object(roads, 'EXPECTED_BYTES', len(body)), patch.object(roads, 'EXPECTED_FEATURE_COUNT', 1), patch.object(roads, 'EXPECTED_BOUNDS', [0,0,1,1]):
                first = roads.capture_census_primary_roads(storage, version='vfixture', captured=(body, receipt))
                encoded = storage.read_key(first['written_keys'][1])
                second = roads.capture_census_primary_roads(storage, version='vfixture', captured=(body, receipt))
            manifest = json.loads(encoded)
            self.assertFalse(first['latest_release_discovered'])
            self.assertTrue(second['source_cache_reused'])
            self.assertEqual(encoded, storage.read_key(first['written_keys'][1]))
            for key in ('documentation', 'coverage', 'field_definitions', 'source_relationship'):
                self.assertNotIn(key, manifest)
            self.assertFalse(any(key.endswith('.xml') for key in storage.list_prefix(roads.DATASET_SLUG)))

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
