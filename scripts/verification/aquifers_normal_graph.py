"""Execute real USGS4,637-row source and failed quality/approval gates locally.

Run with a complete preserved official ZIP and original HTTP receipt. No quality
bypass, geometry repair, acceptance registry or production destination is used.
"""
import argparse
import importlib
import json
import os
import sqlite3
import subprocess
from pathlib import Path

import fiona
import pyarrow.parquet as pq
import shapely
from dagster import AssetKey, DagsterInstance, DefaultScheduleStatus

from dagster_hifld.assets import publish
from dagster_hifld.assets.usgs import usgs_aquifers, usgs_aquifers_draft_refresh_schedule
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.sources import aquifers as source
from dagster_hifld.validation import sha256_file


def snapshot(root):
    return {str(p.relative_to(root)):sha256_file(p) for p in root.rglob('*') if p.is_file()}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',type=Path,required=True)
    parser.add_argument('--receipt',type=Path,required=True)
    parser.add_argument('--workdir',type=Path,required=True)
    parser.add_argument('--cache-metadata-probes',action='store_true')
    args=parser.parse_args();root=args.workdir.resolve();root.mkdir(parents=True,exist_ok=True)
    home=root/'dagster';home.mkdir(exist_ok=True);(home/'dagster.yaml').write_text('telemetry:\n  enabled: false\n')
    stage=root/'staging';destination=root/'published'
    for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS']:
        os.environ.pop(key,None)
    os.environ.update(DAGSTER_HOME=str(home),HIFLD_STAGING_DIR=str(stage),HIFLD_DATASETS_DIR=str(destination))
    definitions=importlib.import_module('dagster_hifld.definitions')
    defs=definitions.defs
    staging=defs.resources['staging_storage'];published=defs.resources['published_storage']
    assert staging.use_local and not staging.bucket and staging.local_dir==str(stage)
    assert published.use_local and not published.bucket and published.local_dir==str(destination)
    assert Path(source.__file__).resolve().is_relative_to(Path(__file__).resolve().parents[2]/'src')
    assert usgs_aquifers_draft_refresh_schedule.default_status==DefaultScheduleStatus.STOPPED
    report={'approved':False,'scope':'real original static USGS2003 source; no geometryrepair; isolated actualnormalDagster graph',
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'source_module':source.__file__,'source_module_sha256':sha256_file(Path(source.__file__)),
        'runs':[],'schedule':{'status':'STOPPED','cron':usgs_aquifers_draft_refresh_schedule.cron_schedule}}
    def save():root.joinpath('execution.json').write_bytes(source.encode(report))
    with DagsterInstance.get() as instance:
        job=defs.resolve_job_def('usgs_aquifers_draft_refresh')
        report['registered_source_refresh_job']=True
        assert list(job.graph.node_dict)==[usgs_aquifers.op.name]
        config={'ops':{usgs_aquifers.op.name:{'config':{'archive_path':str(args.archive.resolve()),'receipt_path':str(args.receipt.resolve())}}}}
        result=job.execute_in_process(instance=instance,run_config=config)
        assert result.success
        candidate=result.output_for_node(usgs_aquifers.op.name)
        report['candidate']=candidate;report['runs'].append({'step':'registered_real_source','run_id':result.run_id,'success':True})
        prefix=stage/'aquifers'/'aquifers'/candidate['version'];records=prefix/'metadata/candidate.records.jsonl'
        assert len(records.read_bytes().splitlines())==4637
        assert prefix.joinpath(f'raw/{source.ARCHIVE_NAME}').read_bytes()==args.archive.read_bytes()
        with fiona.open(f'/vsizip/{{{args.archive.resolve()}}}/{source.STEM}.shp') as original:
            raw={r['properties']['OBJECTID_1']:{'type':'Feature','properties':dict(r['properties']),
                'geometry':shapely.geometry.mapping(shapely.geometry.shape(r['geometry']))} for r in original}
        expected=b''.join(source.encode(raw[k]) for k in sorted(raw))
        assert records.read_bytes()==expected
        table=pq.read_table(prefix/'candidate/typed-source.parquet')
        assert table.num_rows==4637 and all(str(table.schema.field(k).type)=='int32' for k in ['OBJECTID_1','ROCK_TYPE','AQ_CODE'])
        assert all(field.nullable==(field.name=='ROCK_NAME') for field in table.schema)
        decoded=b''.join(source.encode({'type':'Feature','properties':{k:r[k] for k in source.SCHEMA},
            'geometry':shapely.geometry.mapping(shapely.from_wkb(r['geometry']))}) for r in table.to_pylist())
        assert decoded==expected
        with fiona.open(prefix/'geopackage/aquifers.gpkg') as gpkg:
            decoded=b''.join(source.encode({'type':'Feature','properties':dict(r['properties']),
                'geometry':shapely.geometry.mapping(shapely.geometry.shape(r['geometry']))}) for r in gpkg)
            assert decoded==expected
        report['actual_produced_data']={'rows':table.num_rows,'source_IDs':4637,'retained_source_null_ROCK_NAME':1627,
            'native_invalid_polygons_retained':20,'all7attributes_and_geometry_canonical_byte_identical':True,
            'arrow_physical_schema':str(table.schema),'source_crs':'EPSG:4269'}
        with __import__('zipfile').ZipFile(args.archive) as archive:
            assert set(archive.namelist())==source.MEMBERS
            for name in archive.namelist():assert prefix.joinpath('raw/members',name).read_bytes()==archive.read(name)
        manifest=source.strict_json(prefix.joinpath('metadata/source_manifest.json').read_bytes());validate_release_provenance(manifest)
        before=snapshot(prefix)
        repeated=job.execute_in_process(instance=instance,run_config=config)
        assert repeated.success and before==snapshot(prefix)
        report['runs'].append({'step':'real_source_idempotent_rerun','run_id':repeated.run_id,'success':True})
        if args.cache_metadata_probes:
            # Mutate only disposable isolated test storage. Runtime quarantines the
            # entire rejected version; the harness restores original bytes only
            # after verifying failure and preservation, then exercises real reuse.
            gpkg=prefix/'geopackage/aquifers.gpkg';marker=prefix/'metadata/aquifers_candidate_ready.json'
            original_gpkg=gpkg.read_bytes();original_marker=marker.read_bytes()
            raw_before=snapshot(stage/'_raw-captures')
            prior=destination/'retained/metadata-probe-prior-local-fixture.bin'
            prior.parent.mkdir(parents=True,exist_ok=True);prior.write_bytes(b'prior fixture; no dataset approval')
            prior_before=snapshot(destination)
            report['cache_metadata_probes']=[]
            for name,statement in [
                ('contents-crs', 'UPDATE gpkg_contents SET srs_id=4326'),
                ('srs-definition', "UPDATE gpkg_spatial_ref_sys SET definition=(SELECT definition FROM gpkg_spatial_ref_sys WHERE srs_id=4326) WHERE srs_id=4269")]:
                with sqlite3.connect(gpkg) as database:database.execute(statement)
                changed_hash=sha256_file(gpkg)
                ready=json.loads(original_marker);ready['objects']['geopackage/aquifers.gpkg']=changed_hash
                marker.write_bytes(source.encode(ready))
                result=job.execute_in_process(instance=instance,run_config=config,raise_on_error=False)
                assert not result.success
                failures=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
                assert any('cached_geopackage_native_crs_bounds_or_header_drift' in error for error in failures)
                assert not prefix.exists()
                assert ('aquifers','aquifers',candidate['version']) not in list(definitions._iter_staged_version_paths(staging))
                assert raw_before==snapshot(stage/'_raw-captures') and prior_before==snapshot(destination)
                quarantined=[p for p in stage.glob('_quarantine/aquifers/*/*/candidate')
                             if p.joinpath('geopackage/aquifers.gpkg').is_file()
                             and sha256_file(p/'geopackage/aquifers.gpkg')==changed_hash]
                assert len(quarantined)==1
                retained=quarantined[0]
                assert all(sha256_file(retained/key)==digest for key,digest in before.items()
                           if key not in {'geopackage/aquifers.gpkg','metadata/aquifers_candidate_ready.json'})
                report['runs'].append({'step':'real_cached_metadata_'+name+'_denied','run_id':result.run_id,'success':False})
                report['cache_metadata_probes'].append({'case':name,'only_GPKG_and_ready_hash_mutated':True,
                    'changed_geopackage_sha256':changed_hash,'original_geopackage_sha256':source.sha(original_gpkg),
                    'canonical_version_absent_after_failure':True,'quarantine':str(retained.relative_to(stage)),
                    'other_candidate_raw_and_prior_fixture_preserved':True})
                retained.joinpath('geopackage/aquifers.gpkg').write_bytes(original_gpkg)
                retained.joinpath('metadata/aquifers_candidate_ready.json').write_bytes(original_marker)
                retained.rename(prefix)
                restored=job.execute_in_process(instance=instance,run_config=config)
                assert restored.success and snapshot(prefix)==before
                report['runs'].append({'step':'restored_original_fixture_'+name+'_reuse','run_id':restored.run_id,'success':True})
                save()
        partition=build_publish_partition_key('aquifers','aquifers',candidate['version'])
        instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[partition])
        sentinel=destination/'retained/accepted-local-fixture.bin';sentinel.parent.mkdir(parents=True,exist_ok=True)
        sentinel.write_bytes(b'previous local fixture; no approval of real aquifers')
        accepted_before=snapshot(destination)
        keys={a.key for a in publish.publish_assets}|{AssetKey(['publish','catalog'])}
        graph=defs.resolve_implicit_job_def_def_for_assets(keys)
        result=graph.execute_in_process(instance=instance,partition_key=partition,asset_selection=list(keys),raise_on_error=False)
        assert not result.success
        quality=json.loads(prefix.joinpath('metadata/quality_manifest.json').read_text())
        assert quality['feature_count']==4637 and quality['invalid_geometry_count']==20 and quality['quality_check_passed'] is False
        evaluations=result.get_asset_check_evaluations()
        assert any(e.check_name=='quality_manifest' and not e.passed for e in evaluations)
        assert snapshot(destination)==accepted_before
        assert all(prefix.joinpath(name).exists() and sha256_file(prefix/name)==digest for name,digest in before.items())
        assert not any(e.is_step_success and e.step_key=='publish__promote' for e in result.all_events)
        report['runs'].append({'step':'normal_full_graph_real_native_quality_denied','run_id':result.run_id,'success':False,
            'failed_checks':[{'name':e.check_name,'passed':e.passed} for e in evaluations]})
        report['catalog_quality']=quality
        dictionary=json.loads(prefix.joinpath('metadata/data_dictionary.json').read_text())
        assert dictionary['inputs']==manifest['inputs']
        report['catalog_provenance_inputs_unchanged']=True
        # Select the existing verify asset independently to test its real missing-
        # acceptance error; no quality result is rewritten to reach this boundary.
        verify={AssetKey(['publish','verify'])}
        verify_job=defs.resolve_implicit_job_def_def_for_assets(verify)
        result=verify_job.execute_in_process(instance=instance,partition_key=partition,asset_selection=list(verify),raise_on_error=False)
        assert not result.success
        failures=[e.event_specific_data.error.to_string() for e in result.get_step_failure_events()]
        assert any('missing_independent_release_approval_registry' in error for error in failures)
        assert snapshot(destination)==accepted_before
        assert all(sha256_file(prefix/name)==digest for name,digest in before.items())
        report['runs'].append({'step':'existing_verify_asset_missing_approval_denied','run_id':result.run_id,'success':False,'failures':failures})
        report['preservation']={'all_original_raw_candidate_objects_unchanged':True,'prior_accepted_local_fixture_unchanged':True}
        report['result']='faithful_source_implemented_quality_and_release_blockers_enforced'
        save();print(json.dumps({'report':str(root/'execution.json'),'runs':report['runs']},indent=2))


if __name__=='__main__':main()
