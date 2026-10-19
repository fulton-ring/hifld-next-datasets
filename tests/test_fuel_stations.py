"""Draft BTS adapter failures must quarantine before any promotion is possible."""
import copy
import hashlib
import json
import pathlib
import tempfile
import sqlite3
import unittest
import datetime
import urllib.parse
from unittest.mock import patch

from dagster import DefaultScheduleStatus, Definitions, DagsterInstance, build_schedule_context, materialize
from dagster_hifld.resources import StagingStorageResource

import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.assets.fuel_stations import build_candidate, inspect_capture, stage_fuel_capture, bts_alternative_fueling_stations, verify_typed_geopackage, SLUG
from dagster_hifld.assets.fuel_stations import fuel_stations_draft_refresh_job, fuel_stations_draft_refresh_schedule


def write(root, name, value):
    body = json.dumps(value, sort_keys=True).encode()
    (root / (name + '.body')).write_bytes(body)
    service = 'https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/Alternative_Fueling_Stations/FeatureServer/0'
    if name.startswith('page-'):
        query = {'f':'json', 'where':'OBJECTID >= 9999999999 AND OBJECTID <= 9999999999',
                 'outFields':'*', 'returnGeometry':'true', 'orderByFields':'OBJECTID ASC', 'resultRecordCount':'2000'}
        url = service + '/query?' + urllib.parse.urlencode(query)
    elif name.startswith('bts_count'):
        url = service + '/query?' + urllib.parse.urlencode({'f':'json','where':'1=1','returnCountOnly':'true'})
    elif name.startswith('bts_ids'):
        url = service + '/query?' + urllib.parse.urlencode({'f':'json','where':'1=1','returnIdsOnly':'true'})
    else:
        url = service + '?f=pjson'
    (root / (name + '.receipt.json')).write_text(json.dumps({'url':url,'final_url':url,
        'status': 200, 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}))


def fixture(root):
    fields = [{'name': n, 'type': t, 'nullable': nullable} for n, t, nullable in [
        ('OBJECTID', 'esriFieldTypeOID', False), ('id', 'esriFieldTypeInteger', True),
        ('zip', 'esriFieldTypeString', True), ('updated_at', 'esriFieldTypeDate', True),
        ('latitude', 'esriFieldTypeDouble', True)]]
    sr = {'wkid': 102100, 'latestWkid': 3857}
    layer = {'fields': fields, 'editingInfo': {'lastEditDate': 1, 'dataLastEditDate': 1}, 'geometryType': 'esriGeometryPoint', 'objectIdField': 'OBJECTID', 'hasZ': False, 'hasM': False, 'extent': {'spatialReference': sr}}
    page = {'fields': fields, 'spatialReference': sr, 'features': [{'attributes': {'OBJECTID': 9999999999, 'id': 7, 'zip': '00123', 'updated_at': 1792320841494, 'latitude': 38.5}, 'geometry': {'x': -100.123456789, 'y': 200.25}}]}
    for suffix in ('', '_after'):
        write(root, 'bts_layer' + suffix, layer)
        write(root, 'bts_count' + suffix, {'count': 1})
        write(root, 'bts_ids' + suffix, {'objectIds': [9999999999]})
    write(root, 'page-000', page)
    return layer, page


class FuelDraftTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.layer, self.page = fixture(self.root)

    def test_lossless_types_ids_geometry_and_idempotency(self):
        audit = build_candidate(self.root, self.root / 'candidate')
        table = pq.read_table(self.root / 'candidate/candidate.parquet')
        row = table.to_pylist()[0]
        self.assertEqual(row['OBJECTID'], 9999999999)
        self.assertEqual(row['id'], 7)
        self.assertEqual(row['zip'], '00123')
        self.assertEqual(row['updated_at'], 1792320841494)
        self.assertEqual(str(table.schema.field('id').type), 'int32')
        self.assertEqual(str(table.schema.field('OBJECTID').type), 'int64')
        self.assertEqual(tuple(from_wkb(row['geometry']).coords)[0], (-100.123456789, 200.25))
        self.assertFalse(audit['promotion_allowed'])
        self.assertEqual(build_candidate(self.root, self.root / 'candidate'), audit)

    def test_rejects_incomplete_and_changed_census(self):
        for doc, value in [('bts_count_after', {'count': 2}), ('bts_ids_after', {'objectIds': [2]})]:
            fixture(self.root); write(self.root, doc, value)
            with self.subTest(doc=doc), self.assertRaises(ValueError): inspect_capture(self.root)

    def test_rejects_missing_renamed_extra_and_mistyped_ids(self):
        for change in ('missing', 'renamed', 'extra', 'float', 'null', 'overflow'):
            _, page = fixture(self.root); attrs = page['features'][0]['attributes']
            if change == 'missing': del attrs['id']
            if change == 'renamed': attrs['station_id'] = attrs.pop('id')
            if change == 'extra': attrs['unknown_id'] = 8
            if change == 'float': attrs['id'] = 7.0
            if change == 'null': attrs['id'] = None
            if change == 'overflow': attrs['id'] = 2**32
            write(self.root, 'page-000', page)
            with self.subTest(change=change), self.assertRaises(ValueError): inspect_capture(self.root)

    def test_rejects_duplicate_ids_truncation_crs_and_geometry(self):
        for change in ('duplicate', 'truncated', 'crs', 'null_geometry', 'extra_dimension', 'error'):
            _, page = fixture(self.root)
            if change == 'duplicate': page['features'].append(copy.deepcopy(page['features'][0]))
            if change == 'truncated': page['exceededTransferLimit'] = True
            if change == 'crs': page['spatialReference'] = {'wkid': 4326}
            if change == 'null_geometry': page['features'][0]['geometry'] = None
            if change == 'extra_dimension': page['features'][0]['geometry']['z'] = 1
            if change == 'error': page = {'error': {'code': 400}}
            write(self.root, 'page-000', page)
            with self.subTest(change=change), self.assertRaises(ValueError): inspect_capture(self.root)

    def test_rejects_source_edit_or_schema_change(self):
        for kind in ('edit', 'schema'):
            layer, _ = fixture(self.root)
            if kind == 'edit': layer['editingInfo']['lastEditDate'] = 2
            else: layer['fields'][1]['type'] = 'esriFieldTypeString'
            write(self.root, 'bts_layer_after', layer)
            with self.subTest(kind=kind), self.assertRaises(ValueError): inspect_capture(self.root)

    def test_standard_asset_stages_raw_candidate_and_provenance(self):
        signature = [{k:f[k] for k in ('name','type','nullable')} for f in self.layer['fields']]
        digest = hashlib.sha256(json.dumps(signature,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        storage = StagingStorageResource(local_dir=str(self.root/'staging'), use_local=True)
        with patch('dagster_hifld.assets.fuel_stations.PROPOSED_SCHEMA_SHA256', digest), DagsterInstance.ephemeral() as instance:
            result=materialize([bts_alternative_fueling_stations],instance=instance,resources={'staging_storage':storage},run_config={'ops':{bts_alternative_fueling_stations.op.name:{'config':{'capture_dir':str(self.root),'version':'v0.0.0-synthetic'}}}})
        self.assertTrue(result.success)
        root=self.root/'staging'/SLUG/SLUG/'v0.0.0-synthetic'
        self.assertEqual((root/'raw_arcgis/page-000.body').read_bytes(),(self.root/'page-000.body').read_bytes())
        source=json.loads((root/'metadata/source_manifest.json').read_bytes())
        self.assertTrue(source['inputs'][0]['input_id'])
        self.assertFalse(source['promotion_allowed'])
        self.assertEqual(pq.read_table(root/'draft_candidate/candidate.parquet').num_rows,1)
        bridge = root / 'geopackage' / (SLUG + '.gpkg')
        self.assertEqual(verify_typed_geopackage(root/'draft_candidate/candidate.parquet', bridge)['rows'], 1)
        with sqlite3.connect(bridge) as connection:
            # Only this synthetic tamper fixture bypasses spatial maintenance.
            for (trigger,) in connection.execute('SELECT name FROM sqlite_master WHERE type="trigger"').fetchall():
                connection.execute(f'DROP TRIGGER "{trigger}"')
            connection.execute(f'UPDATE "{SLUG}" SET zip=?', ('BAD',))
        with self.assertRaisesRegex(ValueError, 'value/geometry changed'):
            verify_typed_geopackage(root/'draft_candidate/candidate.parquet', bridge)
        geojson=json.loads((root/'geojson'/(SLUG+'.geojson')).read_bytes())
        self.assertEqual(geojson['features'][0]['properties']['zip'],'00123')
        self.assertEqual(geojson['features'][0]['properties']['OBJECTID'],9999999999)

    def test_unreviewed_schema_drift_and_remote_staging_are_rejected(self):
        storage=StagingStorageResource(local_dir=str(self.root/'staging'),use_local=True)
        with self.assertRaisesRegex(ValueError,'schema drift'): stage_fuel_capture(self.root,storage,'v0.0.0-test')
        self.assertTrue((self.root/'staging/quarantine').exists())
        self.assertFalse((self.root/'staging'/SLUG).exists())
        remote=StagingStorageResource(bucket='not-used',use_local=False)
        with self.assertRaisesRegex(ValueError,'local staging'): stage_fuel_capture(self.root,remote,'v0.0.0-test')

    def test_refresh_schedule_is_stopped_and_selects_only_source_with_dated_version(self):
        definitions = Definitions(assets=[bts_alternative_fueling_stations],
            jobs=[fuel_stations_draft_refresh_job], schedules=[fuel_stations_draft_refresh_schedule],
            resources={'staging_storage': StagingStorageResource(local_dir=str(self.root/'staging'), use_local=True)})
        job = definitions.resolve_job_def('fuel_stations_draft_refresh_job')
        self.assertEqual(set(job.graph.node_names()), {bts_alternative_fueling_stations.op.name})
        self.assertEqual(fuel_stations_draft_refresh_schedule.default_status, DefaultScheduleStatus.STOPPED)
        instant = datetime.datetime(2026, 10, 19, 13, tzinfo=datetime.timezone.utc)
        with build_schedule_context(scheduled_execution_time=instant) as context:
            requests = fuel_stations_draft_refresh_schedule.evaluate_tick(context).run_requests
        self.assertEqual(len(requests), 1)
        config = requests[0].run_config['ops'][bts_alternative_fueling_stations.op.name]['config']
        self.assertEqual(config, {'capture_dir': '', 'version': 'v0.0.0-research-20261019T130000Z'})
        self.assertEqual(requests[0].tags['hifld_promotion_allowed'], 'false')

    def test_source_asset_rejects_cloud_before_any_live_capture(self):
        remote = StagingStorageResource(bucket='not-used', use_local=False)
        with patch('dagster_hifld.assets.fuel_stations.capture_live') as fetch, DagsterInstance.ephemeral() as instance:
            result = materialize([bts_alternative_fueling_stations], instance=instance,
                resources={'staging_storage': remote}, raise_on_error=False,
                run_config={'ops': {bts_alternative_fueling_stations.op.name: {'config': {'capture_dir': '', 'version': 'v0.0.0-test'}}}})
        self.assertFalse(result.success)
        fetch.assert_not_called()

    def test_rejects_byte_tampering_and_preserves_candidate(self):
        build_candidate(self.root, self.root / 'candidate')
        target = self.root / 'candidate/candidate.parquet'
        before = target.read_bytes()
        self.page['features'][0]['attributes']['zip'] = '00001'
        write(self.root, 'page-000', self.page)
        with self.assertRaises(FileExistsError): build_candidate(self.root, self.root / 'candidate')
        self.assertEqual(before, target.read_bytes())
        path = self.root / 'page-000.body'; path.write_bytes(path.read_bytes() + b' ')
        with self.assertRaisesRegex(ValueError, 'receipt/checksum'): inspect_capture(self.root)


if __name__ == '__main__': unittest.main()
