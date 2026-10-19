"""Small fabricated complete snapshot; fixtures never approve a real source."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow as pa
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import arc_regions as s


def fixture():
    fields = [{'name': name, 'type': kind, 'nullable': nullable, 'editable': False,
        **({'length': width} if width else {})} for name, kind, nullable, width in s.FIELDS]
    layer = {'id': 2, 'name': 'Master_RC_Geo_Region_2027', 'fields': fields, 'objectIdField': 'OBJECTID',
        'geometryType': 'esriGeometryPolygon', 'hasZ': False, 'hasM': False,
        'spatialReference': {'wkid': 102100, 'latestWkid': 3857}}
    rows = []
    for i in s.EXPECTED_IDS:
        attrs = {name: i if kind == 'esriFieldTypeOID' else (0.125 if i != 2 else None) if kind == 'esriFieldTypeDouble'
            else ('00123' if name == 'Region_HQ_Zip' else ' Démo ' if name == 'Region' else '') for name, kind, *_ in s.FIELDS}
        rows.append({'attributes': attrs, 'geometry': {'rings': [[[i, 0], [i, 1], [i + 0.5, 1], [i + 0.5, 0], [i, 0]]]}})
    item = {'id': s.ITEM, 'owner': 'NHQRedCrossGIS', 'url': s.SERVICE, 'access': 'public',
        'title': 'EXPLICIT SYNTHETIC FY27', 'description': 'EXPLICIT SYNTHETIC not source authority', 'licenseInfo': 'Fixture only, no rights grant'}
    values = {'service': {'serviceItemId': s.ITEM, 'description': 'EXPLICIT SYNTHETIC FY26 banner'}, 'item': item,
        'owner': {'username': 'NHQRedCrossGIS'}, 'layer': layer, 'layer_after': layer, 'item_after': item,
        'count_before': {'count': 47}, 'count_after': {'count': 47},
        'ids_before': {'objectIdFieldName': 'OBJECTID', 'objectIds': list(s.EXPECTED_IDS)},
        'ids_after': {'objectIdFieldName': 'OBJECTID', 'objectIds': list(reversed(s.EXPECTED_IDS))},
        'features': {'geometryType': 'esriGeometryPolygon', 'spatialReference': layer['spatialReference'],
            'fields': [{k: v for k, v in field.items() if k not in {'nullable', 'editable'}} for field in fields], 'features': rows},
        'tail': {'features': []}}
    captures = {}
    for name, value in values.items():
        body = s.encode(value)
        receipt = {'requested_url': s.URLS[name], 'final_url': s.URLS[name], 'status': 200, 'complete': True,
            'bytes': len(body), 'sha256': s.digest(body), 'observed_at': '2026-10-19T00:00:00+00:00', 'fixture': 'EXPLICIT_SYNTHETIC_NOT_LIVE'}
        captures[name] = (body, receipt, s.encode(receipt))
    signature = s.digest(s.encode([[x['attributes']['OBJECTID'], x['geometry']] for x in rows]))
    return captures, signature


def changed(captures, name, callback):
    result = copy.deepcopy(captures); value = s.strict_json(result[name][0]); callback(value)
    body = s.encode(value); receipt = {**result[name][1], 'bytes': len(body), 'sha256': s.digest(body)}
    result[name] = (body, receipt, s.encode(receipt)); return result


class ArcRegionsTests(unittest.TestCase):
    def test_full_native_values_types_leading_empty_unicode_nulls_and_geometry(self):
        captures, signature = fixture()
        with patch.object(s, 'GEOMETRY_SHA256', signature): table, audit = s.inspect_captures(captures)
        self.assertEqual(table.num_rows, 47); self.assertEqual(table.num_columns, 18)
        self.assertEqual(table.schema.field('OBJECTID').type, pa.int64())
        self.assertEqual(table.schema.field('Acres').type, pa.float64())
        self.assertEqual(table.column('Region_HQ_Zip')[0].as_py(), '00123')
        self.assertEqual(table.column('Region')[0].as_py(), ' Démo ')
        self.assertIsNone(table.column('Acres')[1].as_py()); self.assertEqual(table.column('FEMA_Region')[0].as_py(), '')
        self.assertEqual(audit['invalid_geometry_ids'], []); self.assertFalse(audit['authority_accepted'])
        self.assertIsNone(audit['edition_claims']['effective_release_date'])
        with tempfile.TemporaryDirectory() as d:
            first = s.native_outputs(table, Path(d))
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(s.native_outputs(table, Path(d)), first)

    def test_complete_census_tail_schema_type_and_geometry_drift_reject(self):
        captures, signature = fixture()
        cases = [('features', lambda v: v['features'].pop()), ('features', lambda v: v.update(exceededTransferLimit=True)),
            ('tail', lambda v: v['features'].append({'attributes': {'OBJECTID': 48}})),
            ('ids_after', lambda v: v['objectIds'].__setitem__(0, 99)),
            ('features', lambda v: v['features'][0]['attributes'].__setitem__('OBJECTID', True)),
            ('features', lambda v: v['features'][0]['attributes'].__setitem__('RCODE', 10)),
            ('features', lambda v: v['features'][0]['attributes'].__setitem__('new_column', 'unexpected')),
            ('features', lambda v: v['features'][0]['geometry']['rings'][0][1].__setitem__(1, 2)),
            ('layer_after', lambda v: v.update(name='unreviewed edition'))]
        with patch.object(s, 'GEOMETRY_SHA256', signature):
            for name, mutate in cases:
                with self.subTest(name=name), self.assertRaises((ValueError, TypeError)):
                    s.inspect_captures(changed(captures, name, mutate))

    def test_metadata_snapshot_delegation_schema_and_nullability_drift(self):
        captures, signature = fixture()
        cases = [('item', lambda v: v.update(owner='other')), ('service', lambda v: v.update(serviceItemId='wrong')),
            ('layer', lambda v: v['fields'][0].update(type='esriFieldTypeDouble')),
            ('features', lambda v: v['features'][0]['attributes'].update(OBJECTID=None)),
            ('features', lambda v: v['features'][0]['geometry'].update(z=1))]
        with patch.object(s, 'GEOMETRY_SHA256', signature):
            for name, mutate in cases:
                with self.subTest(name=name), self.assertRaises(ValueError): s.inspect_captures(changed(captures, name, mutate))

    def test_nonfinite_duplicate_json_and_incomplete_receipts_fail(self):
        for body in [b'{"a":1,"a":2}', b'{"value":NaN}', b'{"error":{}}', b'[]']:
            with self.assertRaises(ValueError): s.strict_json(body)
        captures, signature = fixture(); body, receipt, raw = captures['features']
        for bad in [{**receipt, 'complete': False}, {**receipt, 'status': 403}, {**receipt, 'sha256': '0'*64}]:
            with self.assertRaises(ValueError): s.validate_receipt('features', body, bad)

    def test_capture_malformed_receipt_retains_exact_consumed_bytes_before_parse(self):
        with tempfile.TemporaryDirectory() as d:
            source = Path(d)/'capture'; source.mkdir(); storage = StagingStorageResource(local_dir=str(Path(d)/'staging'))
            name = next(iter(s.URLS)); body = b'actual consumed untrusted body'; bad = b'{invalid receipt'
            (source/(name+'.body')).write_bytes(body); (source/(name+'.receipt.json')).write_bytes(bad)
            with self.assertRaises(ValueError): s.acquire(storage, source)
            retained = [p.read_bytes() for p in (Path(d)/'staging/_quarantine').rglob('original-receipt.body')]
            self.assertEqual(retained, [bad]); self.assertEqual([p.read_bytes() for p in (Path(d)/'staging/_quarantine').rglob('response.body')], [body])

    def test_published_root_and_ancestor_aliases_reject_before_reads_or_writes(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); outside = root/'outside'; outside.mkdir(); (outside/'sentinel').write_bytes(b'prior')
            (root/'alias').symlink_to(outside, target_is_directory=True)
            for directory in [root/'alias', root/'alias'/'new-storage']:
                with self.assertRaises((ValueError, OSError)): s.acquire(StagingStorageResource(local_dir=str(directory)), root/'missing')
            with self.assertRaises(ValueError): s.acquire(PublishedStorageResource(local_dir=str(outside)), root/'missing')
            self.assertEqual({p.name: p.read_bytes() for p in outside.iterdir()}, {'sentinel': b'prior'})

    def test_shared_http_error_body_is_preserved_and_not_accepted_as_source(self):
        factory = httpx.Client
        def response(request): return httpx.Response(403, stream=httpx.ByteStream(b'complete forbidden body'))
        with tempfile.TemporaryDirectory() as d, patch.object(s.httpx, 'Client', side_effect=lambda **kwargs: factory(transport=httpx.MockTransport(response), **kwargs)):
            with self.assertRaises(ValueError): s.acquire(StagingStorageResource(local_dir=d))
            self.assertEqual([p.read_bytes() for p in Path(d).rglob('response.body')], [b'complete forbidden body'])
            self.assertFalse((Path(d)/s.SLUG).exists())

    def test_complete_private_first_binding_reuse_and_permanent_loss(self):
        captures, signature = fixture()
        with tempfile.TemporaryDirectory() as d, patch.object(s, 'GEOMETRY_SHA256', signature):
            capture = Path(d)/'captures'; capture.mkdir()
            for name, (body, receipt, raw) in captures.items():
                (capture/(name+'.body')).write_bytes(body); (capture/(name+'.receipt.json')).write_bytes(raw)
            storage = StagingStorageResource(local_dir=str(Path(d)/'staging'))
            result = s.stage_arc_regions(storage, capture); root = Path(storage.local_dir)/result['staging_prefix']; before = s.inventory(root)
            self.assertFalse(result['release_approved']); self.assertFalse(result['source_accepted'])
            again = s.stage_arc_regions(storage, capture); self.assertTrue(again['reused']); self.assertEqual(s.inventory(root), before)
            key = s.state_key(result['version']); Path(storage.local_dir, key).unlink()
            with self.assertRaisesRegex(ValueError, 'observed_original_state_loss'): s.stage_arc_regions(storage, capture)
            retained = list(Path(storage.local_dir, '_quarantine', s.SLUG).glob('version-*'))
            self.assertEqual(len(retained), 1); self.assertEqual(s.inventory(retained[0]), before)
            with self.assertRaisesRegex(ValueError, 'observed_trust_loss'): s.stage_arc_regions(storage, capture)
            self.assertFalse(root.exists())


if __name__ == '__main__': unittest.main()
