"""Replay complete Amtrak raw capture through the normal local Dagster graph.

This is implementation verification, never dataset/baseline acceptance. Set
PYTHONPATH=src and run with a separate --output for each code/capture identity.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory(root):
    return {str(p.relative_to(root)): digest(p) for p in root.rglob('*') if p.is_file()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--capture', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=True)
    if args.capture.resolve().is_relative_to(root):
        raise ValueError('Keep immutable source capture outside the run output')
    for key in ('HIFLD_STAGING_BUCKET', 'HIFLD_DATASETS_BUCKET', 'HIFLD_RELEASE_APPROVALS'):
        os.environ.pop(key, None)
    os.environ.update(DAGSTER_HOME=str(root/'dagster'), HIFLD_STAGING_DIR=str(root/'staging'),
                      HIFLD_DATASETS_DIR=str(root/'published'), HIFLD_STAGING_PREFIX='', HIFLD_DATASETS_PREFIX='')
    (root/'dagster').mkdir(exist_ok=True)
    (root/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')

    import pyarrow.parquet as pq
    from dagster import AssetKey, DagsterInstance
    from shapely import Point, from_wkb, to_wkb
    import dagster_hifld.amtrak_extract as adapter
    from dagster_hifld.definitions import defs
    from dagster_hifld.partitions import PUBLISH_PARTITIONS
    from dagster_hifld.release_approval import verifier_implementation_sha256

    audit = adapter.inspect_capture(args.capture)
    implementation = verifier_implementation_sha256()
    version = 'v0.0.0-research-' + implementation[:12]
    prefix = f'{adapter.SLUG}/{adapter.SLUG}/{version}'
    stage = root/'staging'/prefix
    published = root/'published'
    published.mkdir(exist_ok=True)
    fixture = published/'accepted-local-fixture/keep.bin'
    fixture.parent.mkdir(exist_ok=True)
    if not fixture.exists():
        fixture.write_bytes(b'Synthetic accepted-release sentinel; not a dataset acceptance.')
    before = inventory(published)
    repository = defs.get_repository_def()
    source_key = AssetKey(['ingest', adapter.SLUG, adapter.SLUG])
    keys = [AssetKey(['publish', 'catalog']), AssetKey(['publish', 'verify']), AssetKey(['publish', 'promote'])]
    keys += [AssetKey(['publish', 'formats', name]) for name in ('geopackage', 'geoparquet', 'pmtiles', 'shapefile_zip')]
    record = {'classification': 'Real complete-source local draft; no dataset or baseline acceptance',
              'module': adapter.__file__, 'implementation_sha256': implementation,
              'version': version, 'rows': audit['rows'], 'source_fields': len(audit['fields']),
              'published_before': before, 'schedule_activated': False, 'local_only': True}
    def save():
        (root/'execution.json').write_text(json.dumps(record, indent=2)+'\n')
    source_job = repository.get_implicit_job_def_for_assets([source_key])
    config = {'ops': {adapter.bts_amtrak_stations.op.name: {'config': {'capture_dir': str(args.capture.resolve()), 'version': version}}}}
    with DagsterInstance.get() as instance:
        result = source_job.execute_in_process(asset_selection=[source_key], instance=instance, run_config=config, raise_on_error=False)
        record['ingest'] = {'run_id': result.run_id, 'success': result.success}
        save()
        assert result.success
        candidate = stage/'draft_candidate/candidate.parquet'
        table = pq.read_table(candidate)
        expected = sorted([feature for path in sorted(args.capture.glob('page-*.body'))
                           for feature in json.loads(path.read_bytes())['features']], key=lambda f: f['attributes']['OBJECTID'])
        actual = table.to_pylist()
        assert len(actual) == len(expected) == audit['rows']
        for wanted, observed in zip(expected, actual, strict=True):
            assert {k:v for k,v in observed.items() if k != 'geometry'} == wanted['attributes']
            assert observed['geometry'] == to_wkb(Point(wanted['geometry']['x'], wanted['geometry']['y']), byte_order=1)
            assert from_wkb(observed['geometry']).is_valid
        record['candidate_audit'] = {'all_rows_fields_values_ids_native_wkb_equal': True,
            'invalid_geometry_count': 0, 'schema': [{'name':f.name,'type':str(f.type),'nullable':f.nullable} for f in table.schema],
            'bounds': audit['bounds'], 'candidate_sha256': digest(candidate)}
        record['geopackage_audit'] = adapter.verify_typed_geopackage(candidate, stage/'geopackage'/(adapter.SLUG+'.gpkg'))
        staged_before = inventory(stage)
        again = source_job.execute_in_process(asset_selection=[source_key], instance=instance, run_config=config, raise_on_error=False)
        record['idempotent_refresh'] = {'run_id': again.run_id, 'success': again.success, 'version_bytes_equal': inventory(stage) == staged_before}
        assert again.success and inventory(stage) == staged_before
        partition = prefix
        instance.add_dynamic_partitions(PUBLISH_PARTITIONS.name, [partition])
        graph = repository.get_implicit_job_def_for_assets(keys)
        result = graph.execute_in_process(asset_selection=keys, instance=instance, partition_key=partition, raise_on_error=False)
        failures = {e.step_key:e.event_specific_data.error.to_string() for e in result.get_step_failure_events()}
        record['normal_graph'] = {'run_id': result.run_id, 'success': result.success, 'failed_steps': failures,
            'successful_steps': [e.step_key for e in result.all_events if e.event_type_value == 'STEP_SUCCESS'],
            'normal_asset_count': len(repository.asset_graph.get_all_asset_keys())}
        save()
        assert set(failures) == {'publish__verify'} and 'missing_independent_release_approval_registry' in failures['publish__verify']
        assert 'publish__promote' not in record['normal_graph']['successful_steps']
        terminal = pq.read_table(stage/'geoparquet'/(adapter.SLUG+'.parquet'))
        rows_by_id = {r['OBJECTID']:r for r in terminal.to_pylist()}
        assert len(rows_by_id) == table.num_rows
        for row in actual:
            for name, value in row.items():
                assert rows_by_id[row['OBJECTID']][name] == value, name
                assert terminal.schema.field(name).type == table.schema.field(name).type
        record['terminal_parquet_audit'] = {'rows':terminal.num_rows, 'all_source_values_types_native_geometry_equal': True,
            'extra_columns':sorted(set(terminal.column_names)-set(table.column_names)),
            'nullable_metadata_changes':[{'name':f.name,'candidate':f.nullable,'terminal':terminal.schema.field(f.name).nullable}
                                         for f in table.schema if f.nullable != terminal.schema.field(f.name).nullable]}
        # Exercise an actual failed source-quality gate on an explicitly corrupted local copy.
        bad = root/'synthetic-corrupt-capture'
        if bad.exists(): shutil.rmtree(bad)
        shutil.copytree(args.capture, bad)
        page = json.loads((bad/'page-000.body').read_bytes())
        page['features'][0]['geometry'] = None
        (bad/'page-000.body').write_text(json.dumps(page))
        receipt = json.loads((bad/'page-000.receipt.json').read_bytes())
        receipt.update(bytes=(bad/'page-000.body').stat().st_size, sha256=digest(bad/'page-000.body'))
        (bad/'page-000.receipt.json').write_text(json.dumps(receipt))
        bad_config = {'ops': {adapter.bts_amtrak_stations.op.name: {'config': {'capture_dir': str(bad), 'version':version+'-synthetic-invalid'}}}}
        bad_result = source_job.execute_in_process(asset_selection=[source_key], instance=instance, run_config=bad_config, raise_on_error=False)
        record['synthetic_failed_quality'] = {'run_id':bad_result.run_id, 'success':bad_result.success,
            'meaning':'Only this local capture copy was deliberately corrupted; not a producer source defect',
            'failed_steps':{e.step_key:e.event_specific_data.error.to_string() for e in bad_result.get_step_failure_events()}}
        assert not bad_result.success
        assert any('Missing/invalid point geometry' in v for v in record['synthetic_failed_quality']['failed_steps'].values())
    record['published_after'] = inventory(published)
    record['accepted_fixture_preserved'] = record['published_after'] == before
    record['real_raw_candidate_preserved_after_failures'] = all((stage/'raw_arcgis'/r['name']).read_bytes() == (args.capture/r['name']).read_bytes() for r in audit['raw_files'])
    assert record['accepted_fixture_preserved'] and record['real_raw_candidate_preserved_after_failures']
    save()
    print(json.dumps({'ingest':record['ingest'],'refresh':record['idempotent_refresh'],
                      'graph_run':record['normal_graph']['run_id'],'quality_failure_run':record['synthetic_failed_quality']['run_id'],
                      'rows':record['rows'],'preserved':record['accepted_fixture_preserved']}))


if __name__ == '__main__':
    main()
