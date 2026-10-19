"""Exercise the actual NOAA Territorial Sea draft graph in isolated local storage.

No acceptance registry is created. The expected final result is a failed approval
boundary with every generated candidate and the previous local fixture preserved.
Use --capture-directory for complete checksummed captures or omit it for bounded
live NOAA reads. This command never writes to remote staging or production.
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

import fiona
import pyarrow.parquet as pq
import dagster_hifld
from dagster import AssetSelection, DefaultScheduleStatus, DagsterInstance, Definitions, define_asset_job
from dagster_hifld.assets.noaa import noaa_12nm_territorial_sea as ingest
from dagster_hifld.definitions import defs as normal_definitions
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

SLUG = '12nm-territorial-sea'
BASELINE_SHA256 = '81235ea80c9febddf06871facd8afa5ca3670940da7a7fa05acc17c05f0e0152'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-root', required=True, type=Path)
    parser.add_argument('--capture-directory', default='')
    parser.add_argument('--previous-candidate', required=True, type=Path)
    args = parser.parse_args()
    root = args.runtime_root.resolve()
    if root.exists() and any(root.iterdir()):
        raise ValueError('Use an empty isolated runtime directory for reviewable evidence')
    for name in ('dagster', 'staging', 'published'):
        (root / name).mkdir(parents=True, exist_ok=True)
    (root / 'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
    for name in ('HIFLD_STAGING_BUCKET', 'HIFLD_DATASETS_BUCKET', 'KUBERNETES_SERVICE_HOST', 'HIFLD_RELEASE_APPROVALS'):
        os.environ.pop(name, None)
    os.environ['DAGSTER_HOME'] = str(root / 'dagster')
    os.environ['HIFLD_STAGING_DIR'] = str(root / 'staging')
    os.environ['HIFLD_DATASETS_DIR'] = str(root / 'published')
    # Pin actual source code instead of relying on the original editable install.
    module = Path(dagster_hifld.__file__).resolve()
    checkout = Path(__file__).resolve().parents[2]
    assert module.is_relative_to(checkout / 'src'), module
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip()
    assert sha(args.previous_candidate) == BASELINE_SHA256
    previous = root / 'published' / SLUG / SLUG / 'v1.0.0/geoparquet' / (SLUG + '.parquet')
    previous.parent.mkdir(parents=True)
    shutil.copyfile(args.previous_candidate, previous)
    report = {'classification': 'Real87-row draft graph; previous published candidate is a local preservation fixture, not an accepted baseline',
              'code_head': head, 'module_path': str(module), 'runtime_root': str(root),
              'tippecanoe_version': subprocess.check_output(['tippecanoe', '--version'], text=True, stderr=subprocess.STDOUT).strip(),
              'baseline_accepted': False, 'contract_approved': False}
    staging = StagingStorageResource(local_dir=str(root / 'staging'), use_local=True)
    published = PublishedStorageResource(local_dir=str(root / 'published'), use_local=True)
    with DagsterInstance.get() as instance:
        registered = {key for asset in normal_definitions.assets for key in asset.keys}
        assert ingest.key in registered, "Territorial Sea source asset missing from normal registry"
        publish_keys = [key for key in registered if key.path[0] == "publish"]
        definitions = Definitions(assets=normal_definitions.assets, asset_checks=normal_definitions.asset_checks,
                                  resources={"staging_storage": staging, "published_storage": published},
                                  jobs=[define_asset_job("noaa_territorial_normal_ingest", selection=AssetSelection.assets(ingest.key)),
                                        define_asset_job("noaa_territorial_normal_full_graph", selection=AssetSelection.assets(*publish_keys))])
        registered_repository = normal_definitions.get_repository_def()
        refresh = registered_repository.get_job("noaa_territorial_draft_refresh")
        schedule = registered_repository.get_schedule_def("noaa_territorial_draft_weekly")
        assert set(refresh.asset_layer.executable_asset_keys) == {ingest.key}
        assert schedule.default_status == DefaultScheduleStatus.STOPPED
        report["refresh_schedule"] = {"name": schedule.name, "default_status": schedule.default_status.value,
                                      "cron": schedule.cron_schedule, "timezone": schedule.execution_timezone,
                                      "selected_assets": [key.to_user_string() for key in refresh.asset_layer.executable_asset_keys],
                                      "activated": False}
        report["normal_definitions_registered_territorial"] = True
        report["normal_definitions_asset_count"] = len(registered)
        source = definitions.resolve_job_def("noaa_territorial_normal_ingest").execute_in_process(instance=instance,
                             run_config={"ops": {ingest.op.name: {"config": {"allow_unapproved_local_draft": True, "capture_directory": args.capture_directory}}}},
                             tags={"hifld/source_code_commit": head}, raise_on_error=False)
        report.update(ingest_run_id=source.run_id, ingest_success=source.success)
        if source.success:
            output = source.output_for_node(ingest.op.name)
            report['output'] = output
            table = pq.read_table(root / 'staging' / output['candidate_key'])
            assert table.num_rows == 87 and table['OBJECTID'].to_pylist() == output['ids']
            assert str(table.schema.field('UNILATERAL').type) == 'double'
            assert str(table.schema.field('PUB_DATE').type) == 'int64'
            with fiona.open(root / 'staging' / output['catalog_source_key']) as dataset:
                rows = list(dataset)
                assert len(rows) == 87 and sorted(f['properties']['OBJECTID'] for f in rows) == output['ids']
                assert all(type(f['properties']['UNILATERAL']) is float for f in rows)
                report['gpkg_schema'] = dict(dataset.schema)
            partition = '/'.join([SLUG, SLUG, output['version']])
            instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
            graph = definitions.resolve_job_def('noaa_territorial_normal_full_graph').execute_in_process(instance=instance, partition_key=partition, raise_on_error=False)
            report.update(partition=partition, graph_run_id=graph.run_id, graph_success=graph.success,
                          materialized_asset_keys=[event.asset_key.to_user_string() for event in graph.get_asset_materialization_events()],
                          step_failures=[{'step': event.step_key, 'message': event.event_specific_data.error.to_string()} for event in graph.get_step_failure_events()])
            assert not graph.success
            assert any(failure['step'] == 'publish__verify' and 'missing_independent_release_approval_registry' in failure['message'] for failure in report['step_failures'])
            assert not any(key.endswith('promote') for key in report['materialized_asset_keys'])
            assert (root / 'staging' / output['candidate_key']).exists()
        report['previous_public_candidate_preserved'] = sha(previous) == BASELINE_SHA256
        report['published_files'] = published.list_prefix()
        assert report['previous_public_candidate_preserved'] and len(report['published_files']) == 1
    (root / 'integration-result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({key: value for key, value in report.items() if key != 'output'}, indent=2))


if __name__ == '__main__':
    main()
