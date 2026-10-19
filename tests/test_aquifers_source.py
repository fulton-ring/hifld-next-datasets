"""Draft source fidelity/denial behavior; fixtures confer no release approval."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import fiona
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry import mapping, shape
from dagster import AssetKey, DefaultScheduleStatus, build_schedule_context
from dagster._core.errors import ScheduleExecutionError

from dagster_hifld.definitions import _iter_staged_version_paths
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usgs import usgs_aquifers, usgs_aquifers_draft_refresh_schedule
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import aquifers as source

CAPTURES = Path('/workspace/work/hifld-coordinator/19')


class AquifersTests(unittest.TestCase):
    def test_receipt_strict_json_and_official_identity(self):
        for body in (b'[]', b'{"error":{}}', b'{"a":1,"a":2}', b'{"x":{"a":1,"a":2}}'):
            with self.assertRaises(ValueError):
                source.strict_json(body)
        receipt = dict(requested_url=source.URL, final_url=source.URL, status=200, bytes=1,
                       sha256=source.sha(b'x'), fetched_at='2026-10-19T00:00:00+00:00')
        source.validate_receipt(b'x', receipt)
        for field, value in [('final_url','https://example.org/substitute.zip'), ('requested_url','https://example.org/source.zip'),
                             ('status',True), ('bytes',True), ('fetched_at','2026-10-19T00:00:00'), ('sha256','0'*64)]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                source.validate_receipt(b'x', {**receipt, field:value})

    def test_registry_and_stopped_local_schedule(self):
        self.assertEqual(sum(a is usgs_aquifers for a in hifld_ingest_assets), 1)
        self.assertEqual(usgs_aquifers.key, AssetKey(['ingest','aquifers','aquifers']))
        self.assertEqual(usgs_aquifers_draft_refresh_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(usgs_aquifers_draft_refresh_schedule.cron_schedule, '0 7 1 * *')
        for storage in [PublishedStorageResource(use_local=True, local_dir='/tmp/never'),
                        StagingStorageResource(use_local=False, bucket='never'),
                        StagingStorageResource(use_local=True, bucket='still-remote', local_dir='/tmp/never')]:
            with self.subTest(storage=type(storage).__name__), patch.object(source,'acquire') as fetch:
                with self.assertRaises(ValueError):
                    source.stage_aquifers_candidate(storage)
                fetch.assert_not_called()
                with self.assertRaises(ValueError):
                    source.expose_completed_candidate(storage,'v0.0.0-draft-test',{}, {})
                with self.assertRaises(ValueError):
                    source.immutable(storage,'never-write',b'never')
            with build_schedule_context(resources={'staging_storage': storage}) as context:
                with self.assertRaises(ScheduleExecutionError) as caught:
                    usgs_aquifers_draft_refresh_schedule.evaluate_tick(context)
                self.assertIsInstance(caught.exception.__cause__, source.AquifersQuarantineError)

    def test_bounded_partial_failures_are_all_durable(self):
        class Response:
            status_code = 200
            url = source.URL
            headers = {}
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def iter_bytes(self, **kwargs):
                yield b'partial-real-network-chunk'
                raise httpx.ReadError('connection reset')
        class Client:
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def stream(self,*args): return Response()
        with tempfile.TemporaryDirectory() as tmp, patch.object(source.httpx,'Client',return_value=Client()) as mocked:
            storage=StagingStorageResource(local_dir=tmp)
            with self.assertRaisesRegex(ValueError,'bounded_fetch_exhausted'):
                source.acquire(storage,None,None)
            self.assertEqual(mocked.call_count,3)
            bodies=list(Path(tmp).rglob('response.body'))
            self.assertEqual(len(bodies),3)
            self.assertTrue(all(p.read_bytes()==b'partial-real-network-chunk' for p in bodies))
            self.assertFalse(Path(tmp,'aquifers').exists())

    @unittest.skipUnless(CAPTURES.joinpath('zip.body').exists(), 'optional complete real capture not installed')
    def test_real_archive_native_fidelity_no_repair(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows,audit,members=source.inspect_archive(CAPTURES.joinpath('zip.body').read_bytes(),Path(tmp))
            self.assertEqual(len(rows),4637)
            self.assertEqual(audit['invalid_geometry_count'],20)
            self.assertEqual(audit['nulls']['ROCK_NAME'],1627)
            self.assertEqual(len(members),7)
            self.assertEqual(rows[0]['properties']['OBJECTID_1'],1)
            self.assertEqual(rows[-1]['properties']['OBJECTID_1'],4637)
            self.assertEqual(audit['schema']['properties'],source.SCHEMA)
            with self.assertRaisesRegex(ValueError,'changed_truncated'):
                source.inspect_archive(CAPTURES.joinpath('zip.body').read_bytes()[:-1],Path(tmp))

    @unittest.skipUnless(CAPTURES.joinpath('zip.body').exists(), 'optional complete real capture not installed')
    def test_immutable_reuse_and_changed_dependency_namespace(self):
        identity=source.processing_identity()
        with tempfile.TemporaryDirectory() as tmp, patch.object(source,'processing_identity',return_value=identity):
            storage=StagingStorageResource(local_dir=tmp)
            kwargs=dict(archive_path=CAPTURES/'zip.body',receipt_path=CAPTURES/'zip.json')
            first=source.stage_aquifers_candidate(storage,**kwargs)
            prefix=Path(tmp,'aquifers','aquifers',first['version'])
            before={str(p.relative_to(prefix)):source.sha(p.read_bytes()) for p in prefix.rglob('*') if p.is_file()}
            repeated=source.stage_aquifers_candidate(storage,**kwargs)
            self.assertEqual(first,repeated)
            self.assertEqual(before,{str(p.relative_to(prefix)):source.sha(p.read_bytes()) for p in prefix.rglob('*') if p.is_file()})
            changed=copy.deepcopy(identity);changed['gdal']='proposed-new-runtime'
            with patch.object(source,'processing_identity',return_value=changed):
                other=source.stage_aquifers_candidate(storage,**kwargs)
            self.assertNotEqual(first['version'],other['version'])
            self.assertEqual(before,{str(p.relative_to(prefix)):source.sha(p.read_bytes()) for p in prefix.rglob('*') if p.is_file()})
            for format_name in ['geopackage','parquet']:
                artifact=prefix/'metadata'/f'{format_name}_fidelity'
                comparison=json.loads(artifact.joinpath('baseline_comparison.json').read_text())
                self.assertEqual(comparison['status'],'fail')
                self.assertEqual(source.sha(artifact.joinpath(comparison['sidecar']['path']).read_bytes()),comparison['sidecar']['sha256'])
            manifest=json.loads(prefix.joinpath('metadata/source_manifest.json').read_text())
            source.validate_release_provenance(manifest)
            self.assert_cached_self_repin_rejected(storage, prefix, kwargs, Path(tmp))
            marker=prefix/'metadata/aquifers_candidate_ready.json';ready=json.loads(marker.read_text())
            del ready['objects']['raw/members/us_aquifers.dbf'];marker.write_text(json.dumps(ready))
            with self.assertRaisesRegex(ValueError,'inventory_conflict'):
                source.stage_aquifers_candidate(storage,**kwargs)
            self.assertFalse(prefix.exists())

    @unittest.skipUnless(CAPTURES.joinpath('zip.body').exists(), 'optional complete real capture not installed')
    def test_missing_duplicate_lossy_types_columns_and_geometry_reject(self):
        with fiona.open(CAPTURES/'zip/us_aquifers.shp') as original:
            rows=[{'properties':dict(r['properties']),'geometry':mapping(shape(r['geometry']))} for r in original]
        class Stream:
            schema={'properties':source.SCHEMA,'geometry':'Polygon'}
            crs='EPSG:4269'
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def __iter__(self):return iter(self.rows)
        cases=[('missing-ID',lambda r:r['properties'].pop('OBJECTID_1')),
               ('duplicate-ID',lambda r:r['properties'].__setitem__('OBJECTID_1',2)),
               ('integer-to-float',lambda r:r['properties'].__setitem__('AQ_CODE',999.0)),
               ('unexpected-column',lambda r:r['properties'].__setitem__('unapproved',1)),
               ('geometry-null',lambda r:r.__setitem__('geometry',None))]
        for name,change in cases:
            altered=copy.deepcopy(rows);change(altered[0]);stream=Stream();stream.rows=altered
            with self.subTest(name=name),tempfile.TemporaryDirectory() as tmp,patch.object(source.fiona,'open',return_value=stream):
                with self.assertRaises((ValueError,TypeError)):
                    source.inspect_archive(CAPTURES.joinpath('zip.body').read_bytes(),Path(tmp))


    def assert_cached_self_repin_rejected(self, storage, prefix, kwargs, root):
        marker=prefix/'metadata/aquifers_candidate_ready.json'
        original_marker=marker.read_bytes()
        parquet=prefix/'candidate/typed-source.parquet'
        geopackage=prefix/'geopackage/aquifers.gpkg'
        manifest=prefix/'metadata/source_manifest.json'
        originals={path.relative_to(prefix).as_posix():path.read_bytes()
                   for path in (parquet,geopackage,manifest)}
        prior={str(path.relative_to(root)):source.sha(path.read_bytes())
               for path in root.glob('aquifers/aquifers/*/metadata/aquifers_candidate_ready.json')
               if path.parent.parent!=prefix}
        sentinel=root/'published/prior-local-accepted-fixture'
        sentinel.parent.mkdir();sentinel.write_bytes(b'prior local fixture; no dataset acceptance')
        extra='candidate/unapproved-extra.parquet'
        cases=['parquet-value','parquet-integer-width','parquet-native-geometry',
               'parquet-nullability','parquet-malformed-geometry','geopackage-value','provenance',
               'extra-output','missing-output','nonobject-roster']
        observed=[]
        for case in cases:
            ready=json.loads(original_marker)
            if case.startswith('parquet-'):
                table=pq.read_table(parquet)
                if case=='parquet-value':
                    values=table['AQ_NAME'].to_pylist();values[0]='INDEPENDENT CACHE TAMPER'
                    table=table.set_column(table.schema.get_field_index('AQ_NAME'),table.schema.field('AQ_NAME'),pa.array(values))
                elif case=='parquet-integer-width':
                    table=table.set_column(table.schema.get_field_index('OBJECTID_1'),pa.field('OBJECTID_1',pa.int64(),nullable=False),table['OBJECTID_1'].cast(pa.int64()))
                elif case=='parquet-nullability':
                    table=table.set_column(table.schema.get_field_index('AQ_NAME'),pa.field('AQ_NAME',pa.string(),nullable=True),table['AQ_NAME'])
                else:
                    values=table['geometry'].to_pylist()
                    if case=='parquet-malformed-geometry':values[0]=b'invalid WKB'
                    else:values[2]=shapely.make_valid(shapely.from_wkb(values[2])).wkb
                    table=table.set_column(table.schema.get_field_index('geometry'),table.schema.field('geometry'),pa.array(values,pa.binary()))
                pq.write_table(table,parquet)
                ready['objects']['candidate/typed-source.parquet']=source.sha(parquet.read_bytes())
            elif case=='geopackage-value':
                with fiona.open(geopackage) as dataset:
                    physical,crs=dict(dataset.schema),dataset.crs
                    features=[{'properties':dict(row['properties']),'geometry':mapping(shape(row['geometry']))} for row in dataset]
                features[0]['properties']['AQ_NAME']='INDEPENDENT CACHE TAMPER'
                with fiona.open(geopackage,'w',driver='GPKG',layer=source.SLUG,schema=physical,crs=crs) as destination:
                    destination.writerecords(features)
                ready['objects']['geopackage/aquifers.gpkg']=source.sha(geopackage.read_bytes())
            elif case=='provenance':
                value=json.loads(manifest.read_text());value['inputs'][0]['attribution']='Invented attribution'
                manifest.write_bytes(source.encode(value))
                ready['objects']['metadata/source_manifest.json']=source.sha(manifest.read_bytes())
            elif case=='extra-output':
                (prefix/extra).write_bytes(b'unapproved extra output')
                ready['objects'][extra]=source.sha((prefix/extra).read_bytes())
            elif case=='missing-output':
                parquet.unlink()
            else:
                ready['objects']=list(ready['objects'])
            marker.write_bytes(source.encode(ready))
            with self.subTest(cache_case=case),self.assertRaises(source.AquifersQuarantineError) as caught:
                source.stage_aquifers_candidate(storage,**kwargs)
            retained=str(caught.exception).split('retained:',1)[1]
            preserved=root/retained/'candidate'
            self.assertTrue(preserved.exists())
            self.assertFalse(prefix.exists())
            self.assertNotIn(('aquifers','aquifers',prefix.name),list(_iter_staged_version_paths(storage)))
            self.assertEqual(sentinel.read_bytes(),b'prior local fixture; no dataset acceptance')
            self.assertEqual(source.sha((root/f'_raw-captures/aquifers/{source.PIN}/{source.ARCHIVE_NAME}').read_bytes()),source.PIN)
            for name,digest in prior.items():self.assertEqual(source.sha((root/name).read_bytes()),digest)
            observed.append({'case':case,'quarantined':True,'corrupt_version_discoverable':False,
                             'original_raw_and_prior_fixture_preserved':True})
            # Reset only this disposable test fixture after checking the quarantined
            # bytes. Runtime never repairs or silently restores a rejected cache.
            for name,body in originals.items():
                (preserved/name).write_bytes(body)
            (preserved/extra).unlink(missing_ok=True)
            (preserved/'metadata/aquifers_candidate_ready.json').write_bytes(original_marker)
            preserved.rename(prefix)
        print(json.dumps({'real_cache_negative_cases':observed},sort_keys=True))

    @unittest.skipUnless(CAPTURES.joinpath('zip.body').exists(), 'optional complete real capture not installed')
    def test_interrupted_source_never_exposes_discoverable_partial_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            storage=StagingStorageResource(local_dir=tmp,prefix='nested')
            prior=root/'nested/aquifers/aquifers/v1.0.0/geopackage/prior.gpkg'
            prior.parent.mkdir(parents=True);prior.write_bytes(b'prior local fixture; no source acceptance')
            normal_before=list(_iter_staged_version_paths(storage))
            original_write=source.immutable
            discovery_at_interruption=[]
            def interrupt(target,key,body):
                if key.endswith('candidate/typed-source.parquet'):
                    discovery_at_interruption.extend(_iter_staged_version_paths(storage))
                    raise OSError('injected typed candidate write interruption')
                return original_write(target,key,body)
            with patch.object(source,'immutable',side_effect=interrupt),self.assertRaisesRegex(ValueError,'injected typed candidate write interruption'):
                source.stage_aquifers_candidate(storage,archive_path=CAPTURES/'zip.body',receipt_path=CAPTURES/'zip.json')
            self.assertEqual(discovery_at_interruption,normal_before)
            self.assertEqual(list(_iter_staged_version_paths(storage)),normal_before)
            self.assertEqual(prior.read_bytes(),b'prior local fixture; no source acceptance')
            self.assertEqual(source.sha((root/f'nested/_raw-captures/aquifers/{source.PIN}/{source.ARCHIVE_NAME}').read_bytes()),source.PIN)
            self.assertTrue(list(root.glob('nested/_quarantine/aquifers/*/*/report.json')))
            self.assertFalse(list(prior.parent.parent.parent.glob('.aquifers-build-*')))
            print(json.dumps({'real_write_interruption':{'canonical_partial_discoverable':False,
                'prior_and_original_raw_preserved':True,'quarantined':True}},sort_keys=True))
