"""Parked-record fixtures grant no source/contract acceptance."""
import os
import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlsplit

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import miscellaneous_industrial_mineral_operations as s


def atlas_xml():
    commodity_keys = ''.join('<themekey>%s</themekey>' % commodity for commodity in s.MISCELLANEOUS_COMMODITIES)
    return ('<?xml version="1.0" encoding="UTF-8"?><metadata><idinfo><citation><citeinfo>'
            '<origin>U.S. Geological Survey</origin><pubdate>200512</pubdate>'
            '<title>Miscellaneous Industrial Minerals Operations - Direct Download</title>'
            '<pubinfo><pubplace>Reston, VA</pubplace><publish>National Atlas of the United States</publish></pubinfo>'
            '<onlink>http://dds.cr.usgs.gov/pub/data/nationalatlas/minop5x020_nt00022.tar.gz</onlink>'
            '</citeinfo></citation><timeperd><timeinfo><sngdate><caldate>2003</caldate></sngdate></timeinfo></timeperd>'
            '<spdom><bounding><westbc>-123.0</westbc><eastbc>-73.0</eastbc><northbc>49.0</northbc><southbc>25.0</southbc></bounding></spdom>'
            '<keywords><theme><themekey>economy</themekey><themekey>Minerals</themekey><themekey>Industrial minerals</themekey>'
            + commodity_keys +
            '</theme></keywords><accconst>None</accconst></idinfo>'
            '<dataqual><lineage><procstep><procdesc>extraction</procdesc><procdate>200511</procdate></procstep></lineage></dataqual>'
            '<spdoinfo><direct>Vector</direct><ptvctinf><sdtsterm><sdtstype>Entity point</sdtstype><ptvctcnt>304</ptvctcnt></sdtsterm></ptvctinf></spdoinfo>'
            '<spref><horizsys><geograph><geogunit>Decimal degrees</geogunit></geograph><geodetic>'
            '<horizdn>North American Datum of 1983</horizdn></geodetic></horizsys></spref>'
            '<eainfo><detailed><enttyp><enttypl>Miscellaneous Industrial Minerals Operations</enttypl>'
            '<enttypd>Facilities that extract, process, and (or) distribute miscellaneous industrial minerals.</enttypd></enttyp>'
            '<attr><attrlabl>Minop5x020</attrlabl><attrdef>Internal feature number</attrdef><rdom><rdommin>1</rdommin><rdommax>305</rdommax></rdom></attr>'
            '<attr><attrlabl>Commodity</attrlabl><attrdef>The mineral(s) produced or processed by the facility.</attrdef></attr>'
            '<attr><attrlabl>Plant_min</attrlabl><attrdef>Whether the facility is a plant or a mine.</attrdef>'
            '<edom><edomv>M</edomv></edom><edom><edomv>M/P</edomv></edom><edom><edomv>P</edomv></edom></attr>'
            '<attr><attrlabl>Latitude</attrlabl><rdom><rdommin>25.925</rdommin><rdommax>48.565</rdommax></rdom></attr>'
            '<attr><attrlabl>Longitude</attrlabl><rdom><rdommin>-122.782</rdommin><rdommax>-73.112</rdommax></rdom></attr>'
            '</detailed></eainfo><distinfo><stdorder><digform><digtopt><onlinopt><computer><networka>'
            '<networkr>http://dds.cr.usgs.gov/pub/data/nationalatlas/minop5x020_nt00022.tar.gz</networkr>'
            '</networka></computer></onlinopt></digtopt></digform></stdorder></distinfo>'
            '<metainfo><metd>20060713</metd></metainfo></metadata>').encode('utf-8')


def mineplant_xml(progress='Complete', update='None planned'):
    attrs = ''.join('<attr><attrlabl>%s</attrlabl></attr>' % name for name in s.MINEPLANT_METADATA_ATTRS)
    body = ('<metadata><idinfo><citation><citeinfo>'
            '<originator>U.S. Geological Survey</originator>'
            '<title>Active Mines and Mineral Processing Plants in the United States in 2003</title></citeinfo></citation></idinfo>'
            '<status><progress>' + progress + '</progress><update>' + update + '</update></status>'
            '<eainfo><detailed>' + attrs + '</detailed></eainfo>'
            '<metainfo><metd>20201230</metd></metainfo></metadata>')
    return body.encode('utf-8')


class StructuralMiscellaneousIndustrialMineralsTests(unittest.TestCase):
    def test_atlas_scope_pins_declared_producer_scope(self):
        scope = s.parse_atlas_scope(atlas_xml())
        self.assertFalse(scope['approved'])
        self.assertEqual(scope['declared_entity_points'], 304)
        self.assertEqual(scope['declared_plant_mine_domain'], ['M', 'M/P', 'P'])
        self.assertEqual(scope['declared_commodity_classes'], list(s.MISCELLANEOUS_COMMODITIES))
        self.assertEqual(scope['declared_latitude_range'], ['25.925', '48.565'])
        self.assertEqual(scope['declared_longitude_range'], ['-122.782', '-73.112'])
        self.assertEqual(scope['atlas_pubdate'], '200512')
        self.assertEqual(scope['atlas_metadata_date'], '20060713')
        self.assertIn('minop5x020_nt00022.tar.gz', scope['atlas_original_download'])
        self.assertIn('Fullers earth', scope['declared_commodity_classes'])
        for label, mutation, replacement in (
                ('ptvctcnt', '<ptvctcnt>304</ptvctcnt>', '<ptvctcnt>305</ptvctcnt>'),
                ('domain', '<edom><edomv>P</edomv></edom>', ''),
                ('latitude', '<rdommin>25.925</rdommin>', '<rdommin>25.926</rdommin>'),
                ('pubdate', '<pubdate>200512</pubdate>', '<pubdate>200511</pubdate>'),
                ('enttypd', 'extract, process, and (or) distribute miscellaneous industrial minerals.',
                 'extract and process ferrous metals.'),
                ('onlink', 'minop5x020_nt00022.tar.gz', 'minop5x020_nt00023.tar.gz'),
                ('metd', '<metd>20060713</metd>', '<metd>20060714</metd>'),
                ('datum', '<horizdn>North American Datum of 1983</horizdn>', '<horizdn>WGS 1984</horizdn>')):
            mutated = atlas_xml().replace(mutation.encode(), replacement.encode())
            with self.subTest(label=label), self.assertRaises(ValueError):
                s.parse_atlas_scope(mutated)
        for label, extra in (('commodity', '<themekey>Iron</themekey>'),
                             ('commodity_gold', '<themekey>Gold</themekey>')):
            mutated = atlas_xml().replace(b'<themekey>Trona</themekey>',
                                          b'<themekey>Trona</themekey>' + extra.encode())
            with self.subTest(label=label), self.assertRaises(ValueError):
                s.parse_atlas_scope(mutated)

    def test_mineplant_release_status_records_one_time_no_refresh(self):
        status = s.parse_mineplant_release_status(mineplant_xml())
        self.assertFalse(status['approved'])
        self.assertTrue(status['one_time_release'])
        self.assertEqual(status['declared_progress'], 'Complete')
        self.assertEqual(status['declared_update_frequency'], 'None planned')
        self.assertEqual(status['metadata_date'], '20201230')
        self.assertIn('excluded from pipeline inputs', status['eligibility'])
        for label, progress, update in (
                ('progress', 'Ongoing', 'None planned'),
                ('update', 'Complete', 'Ongoing'),
                ('title', 'Retired Mines', 'None planned'),
                ('metd', '20201231', 'None planned'),
                ('roster', 'Complete', 'None planned')):
            body = mineplant_xml(progress, update)
            if label == 'title':
                body = body.replace(b'Active Mines', b'Retired Mines')
            elif label == 'metd':
                body = body.replace(b'<metd>20201230</metd>', b'<metd>20201231</metd>')
            elif label == 'roster':
                body = body.replace(b'<attrlabl>PLANT_MIN</attrlabl>', b'<attrlabl>PLANT_MINE</attrlabl>')
            with self.subTest(label=label), self.assertRaises(ValueError):
                s.parse_mineplant_release_status(body)

    def test_staging_fails_closed_without_reading_or_writing_anything(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            for kwargs in ({}, {'capture_dir': tmp, 'package_id': 'some-package-id'},
                           {'capture_dir': tmp, 'package_id': 'other-package'}):
                with self.subTest(kwargs=kwargs), self.assertRaisesRegex(
                        ValueError, 'no_maintained_producer_publication_park_or_retire'):
                    s.stage_miscellaneous_industrial_mineral_operations(storage, **kwargs)
            self.assertEqual(os.listdir(tmp), [])
        with tempfile.TemporaryDirectory() as tmp:
            for resource in (PublishedStorageResource(local_dir=tmp),
                             StagingStorageResource(bucket='forbidden', use_local=False), None):
                with self.subTest(resource=type(resource).__name__), self.assertRaisesRegex(
                        ValueError, 'no_maintained_producer_publication'):
                    s.stage_miscellaneous_industrial_mineral_operations(resource)
            self.assertEqual(os.listdir(tmp), [])

    def test_park_evidence_names_the_dated_blockers(self):
        evidence = s.PARK_EVIDENCE
        self.assertIn('park or retire', evidence['decision'])
        self.assertIn('404', evidence['original_release']['status'])
        self.assertIn('2026-11-16', evidence['original_release']['status'])
        self.assertIn('DataLumos', evidence['banned_archive'])
        mineplant = evidence['mineplant_survey_publication']
        self.assertEqual(mineplant['declared_progress'], 'Complete')
        self.assertEqual(mineplant['declared_update_frequency'], 'None planned')
        self.assertIn('2026-11-16', mineplant['verified'])
        self.assertIn('exactly 304', mineplant['scope_match'])
        self.assertIn('per-commodity counts identical', mineplant['scope_match'])
        self.assertIn('1..6841', mineplant['record_identity_difference'])
        self.assertIn('not a faithful reconstruction', mineplant['record_identity_difference'])
        alternatives = {item['product']: item for item in evidence['current_alternatives_checked']}
        nmic = alternatives['Mines, smelters, refineries, and recycling facilities in the United States (USGS NMIC)']
        self.assertEqual(nmic['doi'], '10.5066/P1BUPUAM')
        self.assertIn('no coordinate', nmic['finding'])
        self.assertEqual(alternatives['Mineral Resource Data System (MRDS)']['check_date'], '2026-11-12')
        self.assertEqual(alternatives['USMIN mineral deposit areas']['check_date'], '2026-11-12')

    def test_pins_are_producer_owned_research_records(self):
        self.assertEqual(s.producer_hosts(), {'mrdata.usgs.gov', 'thor-f5.er.usgs.gov'})
        for name, pin in s.PINS.items():
            self.assertTrue(pin['url'].startswith('https://'), name)
            self.assertGreater(pin['bytes'], 0, name)
            self.assertEqual(len(pin['sha256']), 64, name)
            self.assertEqual(pin['observed'], '2026-11-16', name)
            self.assertTrue(pin['role'].startswith('research_record'), name)
        self.assertEqual(s.PINS['legacy_metadata']['sha256'],
                         '1ff2b2c8910aca0fe86e3241c0c3c3e1c45a24c396c4401c074c110a98b7c9ee')
        self.assertEqual(s.PINS['usgs_mineplant_zip']['sha256'],
                         '6dabd3bbedcb1fbf0c5155fb349228951a9b2cbd065311484eb479c25997ee4e')
        self.assertEqual(s.PINS['usgs_mineplant_csv_zip']['sha256'],
                         'aac018066ca3bb9b0c4ac40438c86901f80e27787b3b9ecdd4de9234d2e6b0b1')
        self.assertEqual(s.PINS['usgs_mineplant_metadata']['sha256'],
                         '419629edaf825f2d55ed1dd03fe3a9921d0d35a8240c208b44b53d30c8b9ead0')

    def test_module_is_network_free_and_leaves_no_reconstruction(self):
        source = Path(s.__file__).read_text()
        for forbidden in ('httpx', 'requests', 'urllib.request', 'http.client', 'socket',
                          'import zipfile', 'import struct', 'import tarfile', 'import pyarrow',
                          'import shapely', 'acquire', 'manifest', 'parquet',
                          'storage.googleapis.com', 'hifld-next-datasets-prod'):
            self.assertNotIn(forbidden, source, forbidden)
        self.assertIn('PARK_REASON', source)
        self.assertEqual(sorted(name for name in dir(s) if name.startswith('stage_')),
                         ['stage_miscellaneous_industrial_mineral_operations'])


class RegistrationTests(unittest.TestCase):
    def test_asset_job_and_schedule_are_parked_registered_and_stopped(self):
        from dagster import DefaultScheduleStatus
        from dagster_hifld.assets import miscellaneous_industrial_mineral_operations as module
        from dagster_hifld.assets.discovery import discovered
        self.assertEqual(module.miscellaneous_industrial_mineral_operations_candidate.key.to_user_string(),
                         'ingest/miscellaneous-industrial-mineral-operations/'
                         'miscellaneous-industrial-mineral-operations')
        self.assertIn(module.miscellaneous_industrial_mineral_operations_candidate, discovered('INGEST_ASSETS'))
        self.assertIn(module.miscellaneous_industrial_mineral_operations_parked_job, discovered('JOBS'))
        self.assertIn(module.miscellaneous_industrial_mineral_operations_parked_schedule, discovered('SCHEDULES'))
        self.assertIs(module.miscellaneous_industrial_mineral_operations_parked_schedule.default_status,
                      DefaultScheduleStatus.STOPPED)
        self.assertEqual(module.miscellaneous_industrial_mineral_operations_parked_schedule.cron_schedule,
                         '0 7 1 * *')
        self.assertEqual(module.miscellaneous_industrial_mineral_operations_parked_schedule.job_name,
                         'miscellaneous_industrial_mineral_operations_parked_refusal')
        self.assertEqual(s.SLUG, 'miscellaneous-industrial-mineral-operations')
        self.assertEqual(s.PARK_REASON, 'no_maintained_producer_publication_park_or_retire')
