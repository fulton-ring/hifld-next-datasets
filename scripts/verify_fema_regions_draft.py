"""Replay complete preserved FEMA inputs through an isolated real Dagster graph.

Large original captures are explicit external inputs, never downloaded implicitly.
Synthetic receipt/quality faults test implementation behavior, not acceptance.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--capture-dir', type=Path, required=True)
parser.add_argument('--runtime-dir', type=Path, required=True)
parser.add_argument('--live-refresh', action='store_true', help='Additional anonymous official extraction; retain every observed response.')
args = parser.parse_args()
runtime, capture = args.runtime_dir.resolve(), args.capture_dir.resolve()
if runtime.exists() and any(runtime.iterdir()):
    raise ValueError('Use an empty isolated runtime directory.')
runtime.mkdir(parents=True, exist_ok=True)
if shutil.disk_usage(runtime).free < 900_000_000:
    raise OSError('Reserve at least 900MB free for complete raw copies, native payloads, rejection preservation and the 300MB floor.')
for name in ['dagster', 'staging', 'published']:
    (runtime/name).mkdir()
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for name in ['HIFLD_STAGING_BUCKET', 'HIFLD_DATASETS_BUCKET', 'HIFLD_RELEASE_APPROVALS', 'KUBERNETES_SERVICE_HOST']:
    os.environ.pop(name, None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'), HIFLD_STAGING_DIR=str(runtime/'staging'),
                  HIFLD_DATASETS_DIR=str(runtime/'published'), HIFLD_SHADOW_MANUAL_ONLY='1')

from dagster import AssetKey, AssetSelection, DagsterInstance, Definitions, define_asset_job
from dagster_hifld.assets.fema_regions import fema_regions_official as source
from dagster_hifld.checks import publish_catalog_quality_manifest
from dagster_hifld.definitions import defs as normal, _iter_staged_version_paths
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.sources import fema_regions as f

stage = f.StagingStorageResource(local_dir=str(runtime/'staging'))
publish = f.PublishedStorageResource(local_dir=str(runtime/'published'))
publish.write_key('synthetic-accepted-fixture.bin', b'Synthetic prior local release fixture. No FEMA release is approved.')
prior = {key: publish.read_key(key) for key in publish.list_prefix()}
verify_selection = AssetSelection.assets(AssetKey(['publish', 'verify']), AssetKey(['publish', 'promote'])).without_checks()
jobs = [normal.resolve_job_def('fema_regions_draft_refresh'),
        define_asset_job('fema_normal_catalog_formats', selection=AssetSelection.assets(*[
            key for key in normal.resolve_asset_graph().get_all_asset_keys() if key.path[0] == 'publish'])),
        define_asset_job('fema_approval_only', selection=verify_selection),
        define_asset_job('fema_synthetic_failed_quality', selection=verify_selection | AssetSelection.checks(publish_catalog_quality_manifest))]
defs = Definitions(assets=normal.assets, asset_checks=normal.asset_checks,
                   resources={'staging_storage': stage, 'published_storage': publish}, jobs=jobs)
source_config = {'ops': {source.op.name: {'config': {'capture_directory': str(capture)}}}}
report = {'classification': 'Official FEMA region boundary DRAFT; source release, baseline, rights and contract acceptance pending.',
          'code_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=Path(__file__).parents[1], text=True).strip(),
          'complete_capture_directory_required': True, 'runs': []}

def save():
    (runtime/'execution.json').write_text(json.dumps(report, indent=2, default=str)+'\n')

def run(label, job, instance, **kwargs):
    result = defs.resolve_job_def(job).execute_in_process(instance=instance, raise_on_error=False, **kwargs)
    errors = [event.event_specific_data.error.to_string() for event in result.get_step_failure_events()]
    item = {'label': label, 'job': job, 'run_id': result.run_id, 'success': result.success, 'errors': errors,
            'materialized_assets': [event.asset_key.to_user_string() for event in result.get_asset_materialization_events()],
            'checks': [{'name': check.check_name, 'passed': check.passed, 'metadata': str(check.metadata)}
                       for check in result.get_asset_check_evaluations()]}
    if result.success and job == 'fema_regions_draft_refresh':
        item['output'] = result.output_for_node(source.op.name)
    report['runs'].append(item)
    save()
    assert prior == {key: publish.read_key(key) for key in publish.list_prefix()}, 'Prior local release changed.'
    return result, errors

with patch('dagster_hifld.assets.publish.process_layer_partitioned_geoparquet',
           side_effect=AssertionError('Native prebuilt Parquet must not be regenerated.')) as converter, DagsterInstance.get() as instance:
    result, errors = run('complete_10_region_source_and_full_quality', 'fema_regions_draft_refresh', instance, run_config=source_config)
    assert result.success, errors
    output = result.output_for_node(source.op.name)
    assert output['feature_count'] == 10 and output['source_invalid_geometry_count'] == 0 and not output['reused']
    assert any(check.check_name == 'fema_regions_full_quality' and check.passed for check in result.get_asset_check_evaluations())
    version = output['version']
    prefix = f'{f.SLUG}/{f.SLUG}/{version}'
    root = runtime/'staging'/prefix
    report.update(version=version, output=output)
    assert (f.SLUG, f.SLUG, version) in set(_iter_staged_version_paths(stage))
    instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [prefix])
    original_manifest = (root/'metadata/source_manifest.json').read_bytes()
    owned = f.strict_json((root/f.READY).read_bytes())['objects']
    raw_before = {key: stage.sha256_key(key) for key in stage.list_prefix('_raw-captures')}
    result, errors = run('unchanged_source_reuse', 'fema_regions_draft_refresh', instance, run_config=source_config)
    assert result.success and result.output_for_node(source.op.name)['reused'], errors
    for cycle in [1, 2]:
        result, errors = run(f'normal_catalog_formats_cycle_{cycle}_approval_blocked', 'fema_normal_catalog_formats', instance, partition_key=prefix)
        assert not result.success and any('missing_independent_release_approval_registry' in error for error in errors), errors
        keys = {event.asset_key.to_user_string() for event in result.get_asset_materialization_events()}
        assert {'publish/catalog', 'publish/formats/geopackage', 'publish/formats/geoparquet'} <= keys
        assert any(check.check_name == 'quality_manifest' and check.passed for check in result.get_asset_check_evaluations())
        assert 'publish/promote' not in keys
        result, errors = run(f'post_catalog_cycle_{cycle}_source_reuse', 'fema_regions_draft_refresh', instance, run_config=source_config)
        assert result.success and result.output_for_node(source.op.name)['reused'], errors
    result, errors = run('missing_approval_blocks_promotion', 'fema_approval_only', instance, partition_key=prefix)
    assert not result.success and any('missing_independent_release_approval_registry' in error for error in errors), errors
    quality_path = root/'metadata/quality_manifest.json'
    quality_body = quality_path.read_bytes()
    synthetic = f.strict_json(quality_body)
    synthetic['quality_check_passed'] = False
    synthetic['synthetic_test_fault'] = 'Implementation gate behavior only; real complete native source has zero invalid geometries.'
    quality_path.write_bytes(f.encode(synthetic))
    try:
        result, errors = run('SYNTHETIC_failed_quality_blocks_graph_no_approval_claim', 'fema_synthetic_failed_quality', instance, partition_key=prefix)
        assert not result.success and any(check.check_name == 'quality_manifest' and not check.passed for check in result.get_asset_check_evaluations())
        assert not any(event.asset_key.to_user_string() == 'publish/promote' for event in result.get_asset_materialization_events())
    finally:
        quality_path.write_bytes(quality_body)
    outside = runtime/'staging/_construction-state'/f.SLUG
    trusted = runtime/'trusted-original-construction'
    shutil.copytree(outside, trusted)
    shutil.rmtree(outside)
    result, errors = run('observed_whole_outside_witness_loss_rejected', 'fema_regions_draft_refresh', instance, run_config=source_config)
    assert not result.success and not root.exists(), errors
    assert (f.SLUG, f.SLUG, version) not in set(_iter_staged_version_paths(stage))
    incident = runtime/'staging/_construction-incidents'/f.SLUG/version/'incident.json'
    incident_body = incident.read_bytes()
    original_captures = f.acquire(capture)
    later = {}
    for name, (path, receipt) in original_captures.items():
        newer = dict(receipt, started_at='2026-10-20T01:02:03+00:00', observed_at='2026-10-20T01:02:04+00:00')
        later[name] = (path, newer)
    # Only observation times are synthetic; all complete original body paths,
    # data decoding, actual Dagster execution and provenance gates remain real.
    with patch.object(f, 'acquire', return_value=later):
        for retry in [1, 2]:
            result, errors = run(f'SYNTHETIC_later_receipts_retry_{retry}_no_bootstrap', 'fema_regions_draft_refresh', instance, run_config=source_config)
            assert not result.success and any('construction_missing_original_state' in error for error in errors), errors
            assert not root.exists() and incident.read_bytes() == incident_body and not outside.exists()
        shutil.copytree(trusted, outside)
        result, errors = run('explicit_exact_original_state_restore_cold_rebuild', 'fema_regions_draft_refresh', instance, run_config=source_config)
        assert result.success and not result.output_for_node(source.op.name)['reused'], errors
    assert (root/'metadata/source_manifest.json').read_bytes() == original_manifest
    assert all(f.sha256_file(root/name) == pin['sha256'] for name, pin in owned.items())
    assert incident.read_bytes() == incident_body
    if args.live_refresh:
        live_config = {'ops': {source.op.name: {'config': {'capture_directory': str(capture), 'refresh_official': True}}}}
        result, errors = run('LIVE_official_complete_new_observation_preserves_original_context', 'fema_regions_draft_refresh', instance, run_config=live_config)
        assert result.success and result.output_for_node(source.op.name)['reused'], errors
        assert (root/'metadata/source_manifest.json').read_bytes() == original_manifest
    assert all(stage.sha256_key(key) == pin for key, pin in raw_before.items())
    assert converter.call_count == 0
    table, audit, metadata = f.decode_native(original_captures)
    assert f.pq.read_table(root/'geoparquet'/f'{f.SLUG}.parquet').equals(table, check_metadata=True)
    f.reconcile_native(root, table)
    assert sum(row['vertices'] for row in audit) == 2_640_166 and all(row['valid'] for row in audit)
    report.update(all_10_rows_native_types_nulls_ids_ordered_rings_and_WKB_exact=True, complete_native_vertices=2_640_166,
                  source_invalid_geometry_count=0, source_owned_pins=owned, all_original_raw_preserved=True,
                  original_manifest_preserved_after_later_observations=True, prior_synthetic_accepted_fixture_unchanged=True,
                  release_promoted=False, generic_converter_calls=converter.call_count)
    save()
print(json.dumps({'runs': [(item['label'], item['run_id'], item['success']) for item in report['runs']], 'version': version}))
