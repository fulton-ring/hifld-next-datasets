"""Parked-record fixtures grant no source/contract acceptance."""
import os
import tempfile
import unittest
from pathlib import Path
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import uranium_vanadium_deposits as s


def mrds_html(status=None, title=None):
    return ('<html><head><title>' + (title or s.MRDS_LANDING_TITLE) + '</title></head><body>'
            '<p>MRDS is a collection of reports describing metallic and nonmetallic mineral resources '
            'throughout the world. Included are deposit name, location, commodity, deposit description, '
            'geologic characteristics, production, reserves, resources, and references.</p>'
            '<p>' + (status or s.MRDS_STATUS_SENTENCE) + '</p></body></html>').encode('utf-8')


def usmin_html(status=None, title=None, extra_packages=''):
    packages = ''.join('<a>USGS_%s_US_CSV.zip</a>' % name for name in s.USMIN_PACKAGE_COMMODITIES)
    return ('<html><head><title>' + (title or s.USMIN_LANDING_TITLE) + '</title></head><body>'
            '<p>' + (status or s.USMIN_STATUS_SENTENCE) + '</p>' + packages + extra_packages
            + '</body></html>').encode('utf-8')


class StructuralUraniumVanadiumDepositsTests(unittest.TestCase):
    def test_mrds_status_pins_the_frozen_producer_release(self):
        status = s.parse_mrds_status(mrds_html())
        self.assertFalse(status['approved'])
        self.assertTrue(status['frozen_release'])
        self.assertEqual(status['updates_ceased'], '2011')
        self.assertEqual(status['best_available_claim_scope'], 'locations outside the United States')
        self.assertIn('Alaska Resource Data File', status['alaska_alternative'])
        self.assertIn('not a pipeline input', status['eligibility'])
        self.assertIn('MRDS_STATUS_SENTENCE', Path(s.__file__).read_text())
        for label, mutation in (
            ('status', 'Current status: As of 2026, USGS resumed systematic updates to MRDS.'),
            ('title', 'MRDS Archive'),
        ):
            body = mrds_html(status=mutation) if label == 'status' else mrds_html(title=mutation)
            with self.subTest(label=label), self.assertRaises(ValueError):
                s.parse_mrds_status(body)

    def test_usmin_status_records_developing_scope_and_uv_package_absence(self):
        status = s.parse_usmin_status(usmin_html())
        self.assertFalse(status['approved'])
        self.assertTrue(status['developing'])
        self.assertEqual(status['uranium_vanadium_packages'], [])
        self.assertEqual(status['published_commodity_packages'], list(s.USMIN_PACKAGE_COMMODITIES))
        self.assertIn('not a maintained successor', status['eligibility'])
        for label, extra in (
            ('uranium_package', '<a>USGS_Uranium_US_CSV.zip</a>'),
            ('vanadium_package', '<a>USGS_Vanadium_US_Shapefiles.zip</a>'),
        ):
            with self.subTest(label=label), self.assertRaisesRegex(
                ValueError, 'usmin_uranium_or_vanadium_package_published_re_evaluate_source_eligibility'):
                s.parse_usmin_status(usmin_html(extra_packages=extra))
        with self.assertRaisesRegex(ValueError, 'usmin_release_status_changed'):
            s.parse_usmin_status(usmin_html(
                status='An authoritative national-scale geospatial database of the mineral deposits of the United States.'))
        with self.assertRaisesRegex(ValueError, 'usmin_package_roster_drift'):
            s.parse_usmin_status(usmin_html().replace(b'USGS_Cobalt_US_CSV.zip', b'USGS_Cobalt_Dr_US_CSV.zip'))
        with self.assertRaisesRegex(ValueError, 'usmin_status_title_drift'):
            s.parse_usmin_status(usmin_html(title='USMIN Deposit Archive'))

    def test_staging_fails_closed_without_reading_or_writing_anything(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            for kwargs in ({}, {'capture_dir': tmp}, {'capture_dir': tmp, 'unrelated': 'value'}):
                with self.subTest(kwargs=kwargs), self.assertRaisesRegex(
                        ValueError, 'no_maintained_producer_publication_park_or_retire'):
                    s.stage_uranium_vanadium_deposits(storage, **kwargs)
            self.assertEqual(os.listdir(tmp), [])
        with tempfile.TemporaryDirectory() as tmp:
            for resource in (PublishedStorageResource(local_dir=tmp), StagingStorageResource(bucket='forbidden', use_local=False), None):
                with self.subTest(resource=type(resource).__name__), self.assertRaisesRegex(
                        ValueError, 'no_maintained_producer_publication'):
                    s.stage_uranium_vanadium_deposits(resource)
            self.assertEqual(os.listdir(tmp), [])

    def test_park_evidence_names_the_dated_blockers(self):
        evidence = s.PARK_EVIDENCE
        self.assertIn('park or retire', evidence['decision'])
        self.assertIn('2026-11-14', evidence['retired_hifld_item']['status'])
        self.assertIn('Invalid URL', evidence['retired_hifld_item']['status'])
        self.assertIn('DataLumos', evidence['banned_archive'])
        frozen = evidence['frozen_producer_publication']
        self.assertIn('ceased systematic updates to MRDS', frozen['producer_status_statement'])
        self.assertIn('2011', frozen['producer_status_statement'])
        self.assertIn('2026-11-14', frozen['verified'])
        self.assertIn('frozen', frozen['eligibility'])
        def alternative(prefix):
            return next(item for item in evidence['current_alternatives_checked']
                        if item['product'].startswith(prefix))
        self.assertIn('no uranium and no vanadium', alternative('USMIN Mineral Deposit Database')['finding'])
        self.assertEqual(alternative('USMIN Mineral Deposit Database')['check_date'], '2026-11-14')
        self.assertIn('Alaska locations only', alternative('Alaska Resource Data File')['finding'])
        self.assertIn('not deposit points', alternative('Earth MRI focus areas')['finding'])
        self.assertIn('681', alternative('Critical mineral deposits of the United States')['finding'])

    def test_pins_are_producer_owned_research_records(self):
        self.assertEqual(s.producer_hosts(), {'mrdata.usgs.gov'})
        for name, pin in s.PINS.items():
            self.assertTrue(pin['url'].startswith('https://'), name)
            self.assertGreater(pin['bytes'], 0, name)
            self.assertEqual(len(pin['sha256']), 64, name)
            self.assertEqual(pin['observed'], '2026-11-14', name)
            self.assertTrue(pin['role'].startswith('research_record'), name)
        self.assertEqual(s.PINS['usgs_mrds_trim_zip']['sha256'],
                         'f24f995faaa6f5c66415971b1bb0fa1906273adb37f82c1098266aece47f6a84')
        self.assertEqual(s.PINS['usgs_mrds_landing']['sha256'],
                         '793b6de964e124c11fe915630d59d63a056f4dce05ea577fe9d9ac2d6c4452ac')
        self.assertEqual(s.PINS['usgs_usmin_landing']['sha256'],
                         '49014311524b79db7af267a8e2f0b7cd6a201d7dd46ed3caf6fba5ab990ba502')
        self.assertEqual(s.PINS['usgs_ardf_landing']['sha256'],
                         '82b4b73adebe5cc8cfe49a8358fe838f0c0c799c7e440a220cb9c9499fadff44')

    def test_module_is_network_free_and_leaves_no_reconstruction(self):
        source = Path(s.__file__).read_text()
        for forbidden in ('httpx', 'requests', 'urllib.request', 'http.client', 'socket', 'import zipfile',
                          'import struct', 'import tarfile', 'import pyarrow', 'import shapely', 'acquire',
                          'manifest', 'parquet', 'storage.googleapis.com', 'hifld-next-datasets-prod'):
            self.assertNotIn(forbidden, source, forbidden)
        self.assertIn('PARK_REASON', source)
        self.assertEqual(sorted(name for name in dir(s) if name.startswith('stage_')),
                         ['stage_uranium_vanadium_deposits'])


class RegistrationTests(unittest.TestCase):
    def test_asset_job_and_schedule_are_parked_registered_and_stopped(self):
        from dagster import DefaultScheduleStatus
        from dagster_hifld.assets import uranium_vanadium_deposits as module
        from dagster_hifld.assets.discovery import discovered
        self.assertEqual(module.uranium_vanadium_deposits_candidate.key.to_user_string(),
                         'ingest/uranium-and-vanadium-deposits/uranium-and-vanadium-deposits')
        self.assertIn(module.uranium_vanadium_deposits_candidate, discovered('INGEST_ASSETS'))
        self.assertIn(module.uranium_vanadium_deposits_parked_job, discovered('JOBS'))
        self.assertIn(module.uranium_vanadium_deposits_parked_schedule, discovered('SCHEDULES'))
        self.assertIs(module.uranium_vanadium_deposits_parked_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(module.uranium_vanadium_deposits_parked_schedule.cron_schedule, '0 7 1 * *')
        self.assertEqual(module.uranium_vanadium_deposits_parked_schedule.job_name,
                         'uranium_vanadium_deposits_parked_refusal')
        self.assertEqual(s.SLUG, 'uranium-and-vanadium-deposits')
        self.assertEqual(s.PARK_REASON, 'no_maintained_producer_publication_park_or_retire')
