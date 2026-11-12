"""AIANNH-specific trust, drift and raw-preservation tests (no live requests)."""
import io
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch

import httpx
from dagster import DefaultScheduleStatus, build_schedule_context

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.assets.census_bureau import aiannh_source_integrity_refresh_schedule
from dagster_hifld.definitions import defs
from dagster_hifld.sources.aiannh import (
    AiannhQuarantineError, ARCHIVE_NAME, PIN, PROPOSAL, SCHEMA, SLUG,
    MEMBERS, _current_commit, _immutable_stage, _population_audit, inspect_archive, stage_aiannh_candidate,
)


def row():
    properties = {k: 0 if v.startswith('int') else '0' for k, v in SCHEMA.items()}
    properties.update(GEOID='0001R', GEOIDFQ='2500000US0001R', AIANNHCE='0001', AIANNHNS='00000001')
    return {'type': 'Feature', 'properties': properties,
            'geometry': {'type': 'Polygon', 'coordinates': [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}}


class AiannhSourceTests(unittest.TestCase):
    def audit(self, rows):
        return _population_audit(rows, {'properties': SCHEMA, 'geometry': 'Polygon'}, 'EPSG:4269')

    def test_missing_renamed_duplicate_and_integer_type_loss_detected(self):
        cases = []
        missing = row(); missing['properties'].pop('GEOID'); cases.append((missing, 'unexpected_or_missing_column'))
        renamed = row(); renamed['properties']['geoid'] = renamed['properties'].pop('GEOID'); cases.append((renamed, 'missing_or_duplicate_id:GEOID'))
        precision = row(); precision['properties']['ALAND'] = float(9007199254740993); cases.append((precision, 'type_or_null_loss:ALAND'))
        extra = row(); extra['properties']['surrogate_id'] = 'invented'; cases.append((extra, 'unexpected_or_missing_column'))
        null = row(); null['properties']['AIANNHNS'] = None; cases.append((null, 'type_or_null_loss:AIANNHNS'))
        for feature, reason in cases:
            with self.subTest(reason=reason):
                self.assertIn(reason, self.audit([feature])['reasons'])
        self.assertIn('missing_or_duplicate_id:GEOID', self.audit([row(), row()])['reasons'])

    def test_population_coverage_and_geometry_drift_are_quarantined(self):
        self.assertIn('incomplete_population', self.audit([row()])['reasons'])
        changed = row(); changed['properties']['COMPTYP'] = 'M'
        self.assertIn('attribute_id_or_coverage_drift', self.audit([changed])['reasons'])
        invalid = row(); invalid['geometry']['coordinates'] = [[[0, 0], [1, 1], [0, 1], [1, 0], [0, 0]]]
        result = self.audit([invalid]); self.assertEqual(result['invalid_geometry_count'], 1)
        self.assertIn('geometry_drift_or_invalid', result['reasons'])
        self.assertIn('native_crs_drift', _population_audit([row()], {'properties': SCHEMA, 'geometry': 'Polygon'}, 'EPSG:3857')['reasons'])

    def test_remote_or_published_destinations_are_rejected_before_network_or_write(self):
        for storage in (StagingStorageResource(bucket='must-never-contact', use_local=False),
                        PublishedStorageResource(local_dir='/tmp/must-never-write', use_local=True)):
            with patch('dagster_hifld.sources.aiannh.httpx.Client') as client:
                with self.assertRaisesRegex(AiannhQuarantineError, 'isolated_local_staging'):
                    stage_aiannh_candidate(storage)
                client.assert_not_called()

    def test_monthly_schedule_resolves_stopped_and_selects_only_fixed2023(self):
        schedule = aiannh_source_integrity_refresh_schedule
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.execution_timezone, 'America/New_York')
        self.assertEqual(schedule.cron_schedule, '0 3 1 * *')
        job = defs.get_job_def('aiannh_source_integrity_refresh')
        self.assertEqual(list(job.graph.node_dict), ['ingest__aiannh_areas_1__aiannh_areas_1'])
        with build_schedule_context(scheduled_execution_time=datetime(2026, 11, 1, 8, tzinfo=timezone.utc)) as context:
            tick = schedule.evaluate_tick(context)
        self.assertEqual(len(tick.run_requests), 1)
        config = tick.run_requests[0].run_config
        self.assertEqual(config['ops']['ingest__aiannh_areas_1__aiannh_areas_1']['config'], {'vintage': 2023})

    def test_changed_bytes_quarantine_preserves_exact_response_and_existing_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); body = b'{"error":"not a complete ZIP"}'
            archive = root / 'download'; archive.write_bytes(body)
            storage = StagingStorageResource(local_dir=str(root / 'stage'), use_local=True)
            _immutable_stage(storage, 'retained/accepted', b'last-good')
            with self.assertRaisesRegex(AiannhQuarantineError, 'changed_incomplete'):
                stage_aiannh_candidate(storage, archive_path=archive)
            self.assertEqual(storage.read_key('retained/accepted'), b'last-good')
            quarantine = list((root / 'stage/_quarantine').rglob('response.body'))
            self.assertEqual(len(quarantine), 1); self.assertEqual(quarantine[0].read_bytes(), body)
            self.assertFalse((root / 'stage' / SLUG).exists())

    def test_successor_explicitly_quarantined_without_silent_2023_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); source = root / 'later.zip'; source.write_bytes(b'2025-original')
            storage = StagingStorageResource(local_dir=str(root / 'stage'), use_local=True)
            with patch('dagster_hifld.sources.aiannh.httpx.Client') as client:
                with self.assertRaisesRegex(AiannhQuarantineError, 'successor_vintage'):
                    stage_aiannh_candidate(storage, vintage=2025, archive_path=source)
                client.assert_not_called()
            self.assertEqual(next((root / 'stage/_quarantine').rglob('response.body')).read_bytes(), b'2025-original')

    def test_truncated_http206_and_html200_never_stage_canonical_input(self):
        for status in (206, 200, 403):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); storage = StagingStorageResource(local_dir=str(root), use_local=True)
                transport = httpx.MockTransport(lambda request: httpx.Response(status, content=b'<html>incomplete</html>'))
                client = httpx.Client(transport=transport)
                with patch('dagster_hifld.sources.aiannh.httpx.Client', return_value=client):
                    with self.assertRaises(AiannhQuarantineError):
                        stage_aiannh_candidate(storage)
                self.assertFalse((root / SLUG).exists())
                self.assertEqual(next((root / '_quarantine').rglob('response.body')).read_bytes(), b'<html>incomplete</html>')

    def test_archive_duplicate_missing_members_and_crc_fail_before_geodata(self):
        with tempfile.TemporaryDirectory() as directory:
            for names in (['../evil.shp'], ['tl_2023_us_aiannh.shp']):
                body = io.BytesIO()
                with zipfile.ZipFile(body, 'w') as archive:
                    for name in names: archive.writestr(name, b'bad')
                with self.assertRaisesRegex(AiannhQuarantineError, 'zip_member'):
                    inspect_archive(body.getvalue(), Path(directory))

    def test_zip_crc_failure_is_detected_before_fiona(self):
        body = io.BytesIO()
        with zipfile.ZipFile(body, 'w', compression=zipfile.ZIP_STORED) as archive:
            for name in sorted(MEMBERS):
                archive.writestr(name, b'0123456789')
        corrupted = bytearray(body.getvalue())
        first_name = sorted(MEMBERS)[0]
        corrupted[30 + len(first_name)] ^= 0x01
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(AiannhQuarantineError, 'crc_failure'):
                inspect_archive(bytes(corrupted), Path(directory))

    def test_staging_conditional_write_is_idempotent_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            _immutable_stage(storage, 'raw/source.zip', b'original')
            before = storage.object_snapshot('raw/source.zip')
            _immutable_stage(storage, 'raw/source.zip', b'original')
            self.assertEqual(before, storage.object_snapshot('raw/source.zip'))
            with self.assertRaises(AiannhQuarantineError):
                _immutable_stage(storage, 'raw/source.zip', b'changed')
            self.assertEqual(storage.read_key('raw/source.zip'), b'original')

    def test_packaged_identity_falls_back_when_git_executable_is_unavailable(self):
        with patch('dagster_hifld.sources.aiannh.subprocess.run', side_effect=FileNotFoundError):
            self.assertEqual(_current_commit(),
                             'installed-package-without-git; exact-builder-and-implementation-sha256-recorded')

    @unittest.skipUnless(os.environ.get('AIANNH_TEST_ARCHIVE'), 'set AIANNH_TEST_ARCHIVE to preserved exact2023 ZIP')
    def test_complete_pinned862_record_source_and_rerun(self):
        archive = Path(os.environ['AIANNH_TEST_ARCHIVE'])
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            first = stage_aiannh_candidate(storage, archive_path=archive, code_commit='explicit-local-test')
            before = storage.list_object_snapshots(f'{SLUG}/{SLUG}/{first["version"]}')
            second = stage_aiannh_candidate(storage, archive_path=archive, code_commit='explicit-local-test')
            self.assertEqual(first, second)
            self.assertEqual(before, storage.list_object_snapshots(f'{SLUG}/{SLUG}/{first["version"]}'))
            self.assertEqual(first['feature_count'], 862)
            self.assertEqual(first['source_sha256'], PIN)
            prefix = f'{SLUG}/{SLUG}/{first["version"]}'
            manifest = json.loads(storage.read_key(f'{prefix}/metadata/source_manifest.json'))
            self.assertEqual(manifest['processor'], 'HIFLD Next')
            self.assertEqual(manifest['processor_description'], 'HIFLD Next draft AIANNH source adapter')
            validate_release_provenance(manifest)
            self.assertEqual(storage.read_key(f'{prefix}/raw/{ARCHIVE_NAME}'), archive.read_bytes())
            rows = [json.loads(line) for line in storage.read_key(f'{prefix}/metadata/candidate.records.jsonl').splitlines()]
            self.assertEqual(len(rows), 862)
            self.assertTrue(any(r['properties']['GEOID'].startswith('0') for r in rows))
            self.assertTrue(all(set(r['properties']) == set(SCHEMA) for r in rows))
            self.assertFalse(PROPOSAL['approved'])

    @unittest.skipUnless(os.environ.get('AIANNH_TEST_ARCHIVE'), 'set AIANNH_TEST_ARCHIVE to preserved exact2023 ZIP')
    def test_complete_provenance_failure_precedes_canonical_staging(self):
        archive = Path(os.environ['AIANNH_TEST_ARCHIVE'])
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            _immutable_stage(storage, 'retained/accepted', b'last-good')
            with patch('dagster_hifld.sources.aiannh.validate_release_provenance',
                       side_effect=ValueError('synthetic invalid release provenance')):
                with self.assertRaisesRegex(AiannhQuarantineError, 'synthetic invalid release provenance'):
                    stage_aiannh_candidate(storage, archive_path=archive)
            self.assertFalse((Path(directory) / SLUG).exists())
            self.assertEqual(storage.read_key('retained/accepted'), b'last-good')
            self.assertEqual(next((Path(directory) / '_quarantine').rglob('response.body')).read_bytes(), archive.read_bytes())


if __name__ == '__main__':
    unittest.main()
