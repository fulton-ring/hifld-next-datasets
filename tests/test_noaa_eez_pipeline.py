"""Real complete NOAA fixture replay; no source/baseline/contract acceptance."""
import gzip
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus, DagsterInstance, Definitions, define_asset_job, materialize

from dagster_hifld.assets.noaa import noaa_200nm_eez_and_maritime_boundaries as ingest
from dagster_hifld.assets.noaa import noaa_eez_draft_refresh_job, noaa_eez_draft_weekly_schedule
from dagster_hifld.assets.publish import publish_version, verify_release
from dagster_hifld.noaa_extract import noaa_reference_fingerprints
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

EVIDENCE = Path(__file__).parents[1] / 'docs/dataset-sources/200nm-eez-and-maritime-boundaries/evidence'


def replay_directory(root):
    root.mkdir()
    names = {'layer': 'layer.body', 'features': 'all.json.gz', 'geojson': 'geojson.json.gz', 'count_before': 'count.json', 'count_after': 'count.json', 'ids_before': 'id-page.json', 'ids_after': 'id-page.json'}
    receipts = []
    for name, filename in names.items():
        raw = (EVIDENCE / filename).read_bytes()
        if filename.endswith('.gz'):
            raw = gzip.decompress(raw)
        (root / (name + '.json')).write_bytes(raw)
        receipts.append({'capture': name, 'url': 'https://maritimeboundaries.noaa.gov/replay/' + name, 'http_status': 200, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(), 'fetched_at': '2026-10-19T04:19:08Z', 'fixture_note': 'Historical complete real96 rows; count/ID projection reused across before/after fixture slots, not independent live requests'})
    (root / 'receipts.json').write_text(json.dumps(receipts))


class NoaaEezPipelineTests(unittest.TestCase):
    def test_real96_native_output_provenance_idempotency_and_unapproved_graph_preservation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); replay_directory(root / 'capture')
            (root / 'dagster').mkdir(); (root / 'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
            storage = StagingStorageResource(local_dir=str(root / 'staging'), use_local=True)
            published = PublishedStorageResource(local_dir=str(root / 'published'), use_local=True)
            # Local previous-release behavioral sentinel grants no real acceptance.
            old = root / 'published/synthetic-previous-release.bin'; old.parent.mkdir(); old.write_bytes(b'previous release preserved')
            with patch.dict(os.environ, {'DAGSTER_HOME': str(root / 'dagster'), 'HIFLD_RELEASE_APPROVALS': ''}), DagsterInstance.get() as instance:
                config = {'ops': {ingest.op.name: {'config': {'allow_unapproved_local_draft': True, 'capture_directory': str(root / 'capture')}}}}
                first = materialize([ingest], resources={'staging_storage': storage}, instance=instance, run_config=config)
                output = first.output_for_node(ingest.op.name)
                self.assertEqual(output['feature_count'], 96)
                candidate = root / 'staging' / output['candidate_key']; before = candidate.read_bytes()
                table = pq.read_table(candidate)
                self.assertEqual(table.num_rows, 96)
                self.assertEqual(str(table.schema.field('UNILATERAL').type), 'double')
                self.assertEqual(str(table.schema.field('PUB_DATE').type), 'int64')
                self.assertNotIn('id', table.column_names)
                self.assertNotIn('bbox', table.column_names)
                original = json.loads((root / 'capture/features.json').read_text())
                expected = {f['attributes']['OBJECTID']: f['attributes'] for f in original['features']}
                for row in table.to_pylist():
                    self.assertEqual({k: v for k, v in row.items() if k != 'SHAPE'}, expected[row['OBJECTID']])
                version_root = candidate.parents[2]
                manifest = json.loads((version_root / 'metadata/source_manifest.json').read_text())
                self.assertEqual(len(manifest['inputs']), 7)
                self.assertFalse(manifest['baseline_accepted'])
                self.assertFalse(manifest['contract_approved'])
                self.assertIn('shapefile_zip', manifest['unsupported_output_formats'])
                for item in manifest['inputs']:
                    self.assertEqual(hashlib.sha256((root / 'staging' / item['raw_key']).read_bytes()).hexdigest(), item['sha256'])
                second = materialize([ingest], resources={'staging_storage': storage}, instance=instance, run_config=config)
                self.assertEqual(second.output_for_node(ingest.op.name), output)
                self.assertEqual(candidate.read_bytes(), before)
                partition = '/'.join([output['dataset_slug'], output['file_slug'], output['version']])
                instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
                defs = Definitions(assets=[verify_release, publish_version], resources={'staging_storage': storage, 'published_storage': published}, jobs=[define_asset_job('unapproved_noaa')])
                failure = defs.resolve_job_def('unapproved_noaa').execute_in_process(instance=instance, partition_key=partition, raise_on_error=False)
                self.assertFalse(failure.success)
                self.assertIn('missing_independent_release_approval_registry', failure.get_step_failure_events()[0].event_specific_data.error.to_string())
                self.assertEqual(old.read_bytes(), b'previous release preserved')
                self.assertEqual(candidate.read_bytes(), before)
                self.assertEqual(published.list_prefix(), ['synthetic-previous-release.bin'])

    def test_exported_refresh_is_stopped_weekly_source_only(self):
        schedule = noaa_eez_draft_weekly_schedule
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.cron_schedule, "0 6 * * 1")
        self.assertEqual(schedule.execution_timezone, "America/New_York")
        definitions = Definitions(assets=[ingest], resources={"staging_storage": StagingStorageResource(local_dir="/tmp/noaa-schedule-not-executed", use_local=True)}, jobs=[noaa_eez_draft_refresh_job], schedules=[schedule])
        job = definitions.resolve_job_def("noaa_eez_draft_refresh")
        self.assertEqual([node.name for node in job.graph.node_defs], [ingest.op.name])
        self.assertTrue(noaa_eez_draft_refresh_job.config["ops"][ingest.op.name]["config"]["allow_unapproved_local_draft"])

    def test_draft_requires_explicit_opt_in_before_staging(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(Exception, 'explicit allow_unapproved_local_draft'):
                materialize([ingest], resources={'staging_storage': StagingStorageResource(local_dir=directory, use_local=True)})
            self.assertEqual(list(Path(directory).rglob('*')), [])


if __name__ == '__main__':
    unittest.main()
