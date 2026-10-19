"""Exercise the registered AIANNH source job and complete normal publish graph.

All resources/state are isolated local paths. Source and format fidelity may pass;
independent release acceptance is deliberately absent, so the normal graph must
stop at its existing release boundary. No asset, check or approval is patched.
"""
from __future__ import annotations

import argparse
import json
import importlib
import os
import subprocess
from pathlib import Path

from dagster import AssetKey, DagsterInstance, DefaultScheduleStatus

from aiannh_draft_integration import encode, read_derivative, snapshot
from dagster_hifld import conversion
from dagster_hifld.assets import publish
from dagster_hifld.assets.census_bureau import (
    aiannh_source_integrity_refresh_schedule,
    census_bureau_aiannh_areas,
)
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import aiannh
from dagster_hifld.validation import sha256_file


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--workdir', type=Path, required=True)
    args = parser.parse_args()
    root = args.workdir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    home, staging, published = root / 'dagster', root / 'staging', root / 'published'
    home.mkdir(exist_ok=True)
    (home / 'dagster.yaml').write_text('telemetry:\n  enabled: false\n')
    for variable in ('HIFLD_STAGING_BUCKET', 'HIFLD_DATASETS_BUCKET', 'HIFLD_RELEASE_APPROVALS'):
        os.environ.pop(variable, None)
    os.environ.update(DAGSTER_HOME=str(home), HIFLD_STAGING_DIR=str(staging),
                      HIFLD_DATASETS_DIR=str(published), CRAWL4_AI_BASE_DIRECTORY=str(root / 'crawl'))
    # The registered Definitions bind resources when imported. Load them only
    # after local environment configuration, then execute their own resources
    # unchanged; replacing bound resources by reference breaks subset resolution.
    defs = importlib.import_module('dagster_hifld.definitions').defs
    source = defs.resources['staging_storage']
    destination = defs.resources['published_storage']
    assert source.use_local and source.local_dir == str(staging) and not source.bucket
    assert destination.use_local and destination.local_dir == str(published) and not destination.bucket
    code_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    report = {'approved': False, 'scope': 'registered normal graph; isolated local working draft',
              'code_commit': code_commit, 'dependency': 'c7343b802966c566e8f6088bf31f19081b2a0cde',
              'modules': {module.__name__: {'path': module.__file__, 'sha256': sha256_file(Path(module.__file__))}
                          for module in (aiannh, publish, conversion)}, 'runs': [], 'derivatives': {}}
    assert sha256_file(args.archive) == aiannh.PIN
    assert aiannh_source_integrity_refresh_schedule.default_status == DefaultScheduleStatus.STOPPED
    report['schedule'] = {'status': 'STOPPED', 'cron': aiannh_source_integrity_refresh_schedule.cron_schedule,
                          'timezone': aiannh_source_integrity_refresh_schedule.execution_timezone}
    path = root / 'normal-graph.json'

    def save():
        path.write_bytes(encode(report))

    with DagsterInstance.get() as instance:
        job = defs.resolve_job_def('aiannh_source_integrity_refresh')
        assert list(job.graph.node_dict) == [census_bureau_aiannh_areas.op.name]
        config = {'ops': {census_bureau_aiannh_areas.op.name: {'config': {
            'archive_path': str(args.archive.resolve()), 'code_commit': code_commit, 'vintage': 2023}}}}
        result = job.execute_in_process(instance=instance, run_config=config)
        assert result.success
        candidate = result.output_for_node(census_bureau_aiannh_areas.op.name)
        report.update(candidate=candidate)
        report['runs'].append({'step': 'registered_source_job', 'job': job.name, 'run_id': result.run_id, 'success': result.success})
        version = candidate['version']
        version_path = staging / aiannh.SLUG / aiannh.SLUG / version
        source_records = version_path / 'metadata/candidate.records.jsonl'
        assert len(source_records.read_bytes().splitlines()) == 862
        assert (version_path / f'raw/{aiannh.ARCHIVE_NAME}').read_bytes() == args.archive.read_bytes()
        before = snapshot(version_path)
        repeated = job.execute_in_process(instance=instance, run_config=config)
        assert repeated.success and snapshot(version_path) == before
        report['runs'].append({'step': 'registered_source_idempotent_rerun', 'job': job.name,
                               'run_id': repeated.run_id, 'success': repeated.success})
        partition = build_publish_partition_key(aiannh.SLUG, aiannh.SLUG, version)
        instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
        keys = {asset.key for asset in publish.publish_assets} | {AssetKey(['publish', 'catalog'])}
        graph_job = defs.resolve_implicit_job_def_def_for_assets(keys)
        assert graph_job is not None
        sentinel = published / 'retained/accepted/marker.bin'
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_bytes(b'PREEXISTING LOCAL FIXTURE; no Census acceptance')
        published_before = snapshot(published)
        result = graph_job.execute_in_process(instance=instance, partition_key=partition,
                                             asset_selection=list(keys), raise_on_error=False)
        failures = {event.step_key: event.event_specific_data.error.to_string() for event in result.get_step_failure_events()}
        report['runs'].append({'step': 'normal_combined_publish_graph', 'job': graph_job.name,
                               'run_id': result.run_id, 'success': result.success, 'failures': failures})
        succeeded = {event.step_key for event in result.all_events if event.is_step_success}
        report['normal_graph_successful_steps'] = sorted(succeeded)
        assert not result.success
        assert any('missing_independent_release_approval_registry' in message for message in failures.values())
        assert 'publish__formats__pmtiles' in succeeded and 'publish__formats__shapefile_zip' in succeeded
        for step in ('publish__formats__pmtiles', 'publish__formats__shapefile_zip'):
            output = result.output_for_node(step)
            output_events = [event for event in result.all_events
                             if event.step_key == step and event.event_type_value == 'STEP_OUTPUT']
            reasons = [getattr(event.event_specific_data.metadata.get('skip_reason'), 'text', '')
                       for event in output_events]
            report.setdefault('excluded_format_outputs', {})[step] = {'value': output, 'skip_reasons': reasons}
            # The regular format asset reports an explicit exclusion in its
            # output metadata, never a generated artifact or missing converter.
            assert not output['files'] and any('unsupported_output_format' in reason for reason in reasons)
        assert not list((version_path / 'pmtiles').glob('*'))
        assert not list((version_path / 'shapefile').glob('*'))
        assert snapshot(published) == published_before
        report['published_bytes_preserved'] = True
        report['catalog'] = json.loads((version_path / 'metadata/quality_manifest.json').read_text())
        assert report['catalog']['feature_count'] == 862
        assert report['catalog']['quality_check_passed'] is True
        for format_name, suffix in (('geopackage', '*.gpkg'), ('geoparquet', '*.parquet')):
            files = list((version_path / format_name).rglob(suffix))
            assert len(files) == 1
            decoded = root / f'{format_name}.records.jsonl'
            physical = read_derivative(files[0], decoded)
            comparison = aiannh.compare_derivative(source_records, decoded, root / f'{format_name}-fidelity')
            assert comparison['status'] == 'pass'
            assert physical['feature_count'] == 862
            assert physical['geometry_types'] == {'Polygon': 561, 'MultiPolygon': 301}
            report['derivatives'][format_name] = {'sha256': sha256_file(files[0]), 'physical': physical,
                                                 'full_comparison': comparison}
            save()
        report['source_archive_sha256'] = sha256_file(version_path / f'raw/{aiannh.ARCHIVE_NAME}')
        report['raw_members'] = json.loads((version_path / 'metadata/aiannh_source_audit.json').read_text())['members']
        report['result'] = 'normal_graph_source_and_lossless_formats_pass_exclusions_observed_release_denied'
        save()
        print(json.dumps({'result': report['result'], 'report': str(path), 'runs': report['runs']}, indent=2))


if __name__ == '__main__':
    main()
