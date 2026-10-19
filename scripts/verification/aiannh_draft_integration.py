"""Execute the real862-feature AIANNH draft in isolated local Dagster storage.

Supply a preserved original ZIP and a scratch directory outside the repository.
No release approval is created. Successful checks remain source-fidelity results,
not an accepted HIFLD baseline, schema/grain migration or production release.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import subprocess
from pathlib import Path

import fiona
import pyarrow.parquet as pq
from dagster import DagsterInstance, materialize
from pyproj import CRS
from shapely import from_wkb
from shapely.geometry import mapping, shape

from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets.census_bureau import census_bureau_aiannh_areas
from dagster_hifld.assets.publish import (
    publish_geopackage, publish_geoparquet, publish_shapefile_zip, publish_version,
)
from dagster_hifld.checks.common import read_quality_check_result
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import aiannh
from dagster_hifld.validation import sha256_file


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def snapshot(root):
    return {str(p.relative_to(root)): sha256_file(p) for p in root.rglob('*') if p.is_file()}


def read_derivative(path, output):
    """Reject unknown physical columns before adapting a lossless source view.

    GeoParquet's declared covering bbox is retained/recomputed and reported as
    storage metadata; it never masquerades as an original source attribute.
    No identifier, attribute, type, coordinate or geometry type is normalized.
    """
    if path.suffix == '.gpkg':
        with fiona.open(path) as src:
            schema = dict(src.schema)
            if set(schema['properties']) != set(aiannh.SCHEMA):
                raise ValueError('GeoPackage source columns changed')
            if not CRS(src.crs).equals(CRS.from_epsg(4269)):
                raise ValueError('GeoPackage source CRS changed')
            features = [{'type': 'Feature', 'properties': dict(f['properties']),
                         'geometry': mapping(shape(f['geometry'])) if f['geometry'] else None} for f in src]
        physical = {'schema': schema, 'crs': 'EPSG:4269', 'generated_storage_primary_key': 'GeoPackage internal fid; absent from source properties'}
    else:
        table = pq.read_table(path)
        metadata = json.loads(table.schema.metadata[b'geo'])
        column = metadata['columns']['geometry']
        if not CRS(column['crs']).equals(CRS.from_epsg(4269)):
            raise ValueError('GeoParquet native CRS changed')
        if set(table.column_names) != set(aiannh.SCHEMA) | {'geometry', 'bbox'}:
            raise ValueError('GeoParquet has unknown/missing physical columns')
        expected_covering = {k: ['bbox', k] for k in ('xmin', 'ymin', 'xmax', 'ymax')}
        if column.get('covering', {}).get('bbox') != expected_covering:
            raise ValueError('Unrecognized GeoParquet bbox mapping')
        features = []
        for record in table.to_pylist():
            g = from_wkb(record['geometry'])
            if list(record['bbox'].values()) != list(g.bounds):
                raise ValueError('GeoParquet generated bbox differs from full native polygon bounds')
            features.append({'type': 'Feature', 'properties': {k: record[k] for k in aiannh.SCHEMA}, 'geometry': mapping(g)})
        physical = {'schema': {f.name: str(f.type) for f in table.schema}, 'crs': 'EPSG:4269',
                    'generated_storage_columns': {'bbox': 'GeoParquet1.1 covering bbox; every862 value checked against full native polygon bounds; no generated row IDs'},
                    'geo_metadata': metadata}
    output.write_bytes(b''.join(encode(f) for f in features))
    physical['geometry_types'] = dict(collections.Counter(f['geometry']['type'] for f in features))
    physical['feature_count'] = len(features)
    return physical


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
    source = StagingStorageResource(local_dir=str(staging), use_local=True)
    destination = PublishedStorageResource(local_dir=str(published), use_local=True)
    resources = {'staging_storage': source, 'published_storage': destination}
    report = {'working_draft': True, 'approved': False, 'module': aiannh.__file__,
              'module_sha256': sha256_file(Path(aiannh.__file__)), 'source_archive_sha256': sha256_file(args.archive),
              'runs': [], 'derivatives': {}}
    assert report['source_archive_sha256'] == aiannh.PIN
    current_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    report['code_commit'] = current_commit
    report_path = root / 'execution.json'

    def save():
        report_path.write_bytes(encode(report))

    with DagsterInstance.get() as instance:
        config = {'ops': {census_bureau_aiannh_areas.op.name: {'config': {
            'archive_path': str(args.archive.resolve()), 'vintage': 2023, 'code_commit': current_commit}}}}
        result = materialize([census_bureau_aiannh_areas], resources=resources, instance=instance, run_config=config)
        candidate = result.output_for_node(census_bureau_aiannh_areas.op.name)
        version = candidate['version']
        report.update(version=version, candidate=candidate)
        report['runs'].append({'step': 'ingest', 'run_id': result.run_id, 'success': result.success})
        partition = build_publish_partition_key(aiannh.SLUG, aiannh.SLUG, version)
        instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
        version_path = staging / aiannh.SLUG / aiannh.SLUG / version
        source_records = version_path / 'metadata/candidate.records.jsonl'
        assert len(source_records.read_bytes().splitlines()) == 862
        assert (version_path / f'raw/{aiannh.ARCHIVE_NAME}').read_bytes() == args.archive.read_bytes()
        before_rerun = snapshot(version_path)
        rerun = materialize([census_bureau_aiannh_areas], resources=resources, instance=instance, run_config=config)
        assert rerun.success and snapshot(version_path) == before_rerun
        report['runs'].append({'step': 'idempotent_ingest_rerun', 'run_id': rerun.run_id, 'success': rerun.success})
        for assets, label in (([publish_catalog], 'catalog'), ([publish_geopackage], 'geopackage'),
                              ([publish_geoparquet], 'geoparquet'), ([publish_shapefile_zip], 'unsupported_regenerated_shapefile')):
            result = materialize(assets, resources=resources, instance=instance, partition_key=partition, raise_on_error=False)
            report['runs'].append({'step': label, 'run_id': result.run_id, 'success': result.success})
            save()
            assert result.success, f'{label} failed; inspect Dagster state/logs'
        quality_path = version_path / 'metadata/quality_manifest.json'
        quality = json.loads(quality_path.read_text())
        assert quality['feature_count'] == 862 and quality['quality_check_passed'] is True
        report['catalog'] = quality
        for format_name, suffix in (('geopackage', '*.gpkg'), ('geoparquet', '*.parquet')):
            paths = list((version_path / format_name).rglob(suffix))
            assert len(paths) == 1, 'Expected complete single-file derivative for862 features'
            decoded = root / f'{format_name}.records.jsonl'
            physical = read_derivative(paths[0], decoded)
            comparison = aiannh.compare_derivative(source_records, decoded, root / f'{format_name}-fidelity')
            report['derivatives'][format_name] = {'sha256': sha256_file(paths[0]), 'physical': physical, 'full_comparison': comparison}
            save()
            assert comparison['status'] == 'pass'
            assert physical['feature_count'] == 862
            assert physical['geometry_types'] == {'Polygon': 561, 'MultiPolygon': 301}
        assert not list((version_path / 'shapefile').glob('*'))
        assert not list((version_path / 'pmtiles').glob('*'))
        report['raw_preserved'] = {'archive_sha256': sha256_file(version_path / f'raw/{aiannh.ARCHIVE_NAME}'),
                                   'all_members': json.loads((version_path / 'metadata/aiannh_source_audit.json').read_text())['members']}
        sentinel = published / 'retained/accepted/source.bin'
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_bytes(b'PREEXISTING LOCAL FIXTURE; not production approval')
        baseline = snapshot(published)
        stage_before = snapshot(version_path)
        denied = materialize([publish_version], resources=resources, instance=instance, partition_key=partition, raise_on_error=False)
        errors = [event.event_specific_data.error.to_string() for event in denied.get_step_failure_events()]
        assert not denied.success and any('missing_independent_release_approval_registry' in error for error in errors)
        assert snapshot(published) == baseline and snapshot(version_path) == stage_before
        report['runs'].append({'step': 'promotion_without_acceptance', 'run_id': denied.run_id, 'success': denied.success, 'expected_denial': True})
        report['missing_approval_preserved_all_published_and_staged_bytes'] = True
        original_quality = quality_path.read_bytes()
        try:
            quality_path.write_bytes(encode({**quality, 'quality_check_passed': False, 'injected_local_test_failure': True}))
            check = read_quality_check_result(source, aiannh.SLUG, aiannh.SLUG, version)
            assert check.passed is False
            denied = materialize([publish_version], resources=resources, instance=instance, partition_key=partition, raise_on_error=False)
            assert not denied.success and snapshot(published) == baseline
            report['runs'].append({'step': 'promotion_with_failed_quality_and_missing_acceptance', 'run_id': denied.run_id,
                                   'success': denied.success, 'expected_denial': True,
                                   'quality_check_passed': check.passed,
                                   'limitation': 'No Census approval forged; registry denial occurs before quality gate. Independent shared-verifier synthetic tests cover quality-specific denial with test-only acceptance.'})
        finally:
            quality_path.write_bytes(original_quality)
        assert snapshot(version_path) == stage_before
        report['failed_quality_preserved_all_published_bytes'] = True
        report['result'] = 'draft_source_and_lossless_format_fidelity_pass_release_blocked'
        save()
        print(json.dumps({'result': report['result'], 'version': version, 'report': str(report_path), 'runs': report['runs']}, indent=2))


if __name__ == '__main__':
    main()
