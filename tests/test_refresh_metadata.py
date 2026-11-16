"""Maintained-source evidence stays distinct from release acceptance."""
import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pyarrow as pa

from dagster_hifld.catalog import generate_data_dictionary, write_catalog_metadata
from dagster_hifld.native_geoparquet_catalog import _dictionary
from dagster_hifld.assets.publish import _copy_version_files
from dagster_hifld.portolan.catalog import AssetRecord, CatalogRecord, build_catalog_sqlite, render_portolan_tree
from dagster_hifld.portolan.provenance import attach_published_provenance, prepared_asset_dependencies, verify_prepared_metadata_dependencies
from dagster_hifld.portolan.workflow import PortolanPublishRequest, _promote_source_metadata, columns_from_dictionary
from dagster_hifld.provenance import provenance_projection, validate_provenance
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from test_provenance import provenance_fixture


def refresh_fixture():
    value = provenance_fixture()
    value['inputs'][0].update(purpose='data', refresh={
        'maintenance_status': 'maintained', 'maintenance_evidence_input_id': 'producer',
        'source_release_id': 'publisher-edition-2026', 'data_period': '2025',
        'update_cadence': 'annual', 'latest_checked_at': '2026-11-16T00:00:00Z',
        'latest_evidence_input_id': 'producer', 'selection_rule': 'latest final annual edition',
        'source_state_fingerprint': 'sha256:' + 'a' * 64, 'unknown_reasons': {}})
    value['coverage'] = {'geographic': 'Synthetic jurisdiction', 'unknown_reasons': ['No temporal extent supplied']}
    value['source_relationship'] = {'relationship': 'changed_successor', 'evidence_input_ids': ['producer']}
    value['field_definitions'] = {
        'id': {'source_fields': ['SOURCE_ID'], 'input_ids': ['producer'], 'basis': 'publisher',
               'description': 'Publisher identifier, including leading zeroes', 'identifier_meaning': 'Stable source ID'},
        'code': {'source_fields': ['CODE'], 'input_ids': ['producer'], 'basis': 'publisher',
                 'description': 'Status', 'codes': {'A': 'Active'}, 'sentinel_values': {'-9': 'Not reported'},
                 'units': 'MW', 'null_meaning': 'Not supplied'},
        'count': {'source_fields': ['COUNT'], 'input_ids': [], 'basis': 'unknown'}}
    value['format_transformations'] = {'geoparquet': [{'name': 'faithful conversion', 'input_ids': ['producer']}]}
    return value


class RefreshMetadataTests(unittest.TestCase):
    def test_legacy_manifest_cannot_bypass_document_descriptor_validation(self):
        with tempfile.TemporaryDirectory() as d:
            storage = StagingStorageResource(use_local=True, local_dir=d)
            storage.write_key('synthetic/points/v20261116/metadata/source_manifest.json',
                              json.dumps({'documentation': [{'path': '../outside'}]}).encode())
            with self.assertRaises(ValueError):
                load_resolved_source_manifest(storage, 'synthetic', 'points', 'v20261116')

    def test_unknown_release_id_requires_its_own_explanation(self):
        value = refresh_fixture()
        refresh = value['inputs'][0]['refresh']
        refresh['source_release_id'] = None
        refresh['unknown_reasons'] = {'source_release_id': 'Publisher exposes an unversioned live service'}
        validate_provenance(value)
        for reasons in ({}, {'data_period': 'Not published'}, {'source_release_id': ''}):
            refresh['unknown_reasons'] = reasons
            with self.subTest(reasons=reasons), self.assertRaises(ValueError):
                validate_provenance(value)

    def test_standalone_promotion_refuses_document_descriptor_mismatch(self):
        with tempfile.TemporaryDirectory() as d:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(d) / 'stage'))
            published = PublishedStorageResource(use_local=True, local_dir=str(Path(d) / 'published'))
            prefix = 'synthetic/points/v20261116'
            metadata = refresh_fixture()
            metadata['documentation'] = [{'path': 'raw/schema.xml', 'input_id': 'producer', 'purpose': 'schema',
                                          'media_type': 'application/xml', 'bytes': 3, 'sha256': hashlib.sha256(b'old').hexdigest()}]
            storage.write_key(prefix + '/metadata/source_manifest.json', json.dumps(metadata).encode())
            storage.write_key(prefix + '/raw/schema.xml', b'new')
            with patch('dagster_hifld.assets.publish.load_release_approvals'), \
                 patch('dagster_hifld.assets.publish.read_staged_release', return_value=({}, prefix)), \
                 patch('dagster_hifld.assets.publish.promote_immutable_objects') as promote:
                with self.assertRaisesRegex(ValueError, 'documentation'):
                    _copy_version_files(storage, published, 'synthetic', 'points', 'v20261116')
                promote.assert_not_called()
            self.assertEqual(published.list_prefix(), [])

    def test_projection_and_typed_validation(self):
        value = refresh_fixture()
        self.assertEqual(provenance_projection(value), value)
        changes = [lambda v: v['inputs'][0].update(purpose='anything'),
                   lambda v: v['inputs'][0].update(refresh=None),
                   lambda v: v['inputs'][0]['refresh'].update(maintenance_status=[]),
                   lambda v: v['inputs'][0]['refresh'].update(latest_checked_at='2026-11-16'),
                   lambda v: v['inputs'][0]['refresh'].update(latest_evidence_input_id='missing'),
                   lambda v: v['inputs'][0]['refresh'].update(unknown_reasons=[]),
                   lambda v: v['field_definitions']['id'].update(input_ids=['missing']),
                   lambda v: v['field_definitions']['id'].update(basis='inferred'),
                   lambda v: v.update(documentation=[{'path': '../secret'}]),
                   lambda v: v.update(format_transformations={'geoparquet': [{'name': 'convert', 'input_ids': ['missing']}]})]
        for change in changes:
            wrong = copy.deepcopy(value); change(wrong)
            with self.subTest(wrong=wrong), self.assertRaises(ValueError):
                validate_provenance(wrong)

    def test_layer_conflicts_and_explicit_invalid_metadata_fail_closed(self):
        with tempfile.TemporaryDirectory() as d:
            storage = StagingStorageResource(use_local=True, local_dir=d)
            parent = refresh_fixture()
            storage.write_key('synthetic/metadata/source_manifest.json', json.dumps(parent).encode())
            version_key = 'synthetic/points/v20261116/metadata/source_manifest.json'
            conflicts = [
                {'field_definitions': {'id': {**parent['field_definitions']['id'], 'description': 'conflicting'}}},
                {'documentation': [{'path': 'raw/schema.xml', 'input_id': 'missing', 'purpose': 'schema',
                                    'media_type': 'application/xml', 'bytes': 1, 'sha256': 'a' * 64}]},
                {'coverage': None}, {'field_definitions': None}, {'documentation': None},
            ]
            for value in conflicts:
                storage.write_key(version_key, json.dumps(value).encode())
                with self.subTest(value=value), self.assertRaises(ValueError):
                    load_resolved_source_manifest(storage, 'synthetic', 'points', 'v20261116')

    def test_generic_and_native_semantics_preserve_identifiers_statistics_and_unknowns(self):
        metadata = refresh_fixture()
        frame = pd.DataFrame({'id': ['001'], 'code': ['A'], 'count': [2**63-1]})
        dictionary = generate_data_dictionary(frame, 'fixture', source_metadata=metadata)
        self.assertEqual(dictionary['field_definitions'], metadata['field_definitions'])
        columns = {c['name']: c for c in dictionary['columns']}
        self.assertEqual(columns['id']['semantic_definition'], metadata['field_definitions']['id'])
        self.assertEqual(columns['id']['exampleValues'], ['001'])
        self.assertEqual(columns['count']['max'], 2**63-1)
        self.assertNotIn('description', columns['count'])
        self.assertEqual(columns['id']['valuesBasis'], 'observed_values_not_publisher_domain')
        self.assertNotIn('codes', columns['id']['semantic_definition'])
        schema = pa.schema([pa.field('id', pa.string()), pa.field('code', pa.string()), pa.field('count', pa.int64())])
        native = _dictionary(pa.Table.from_pydict(frame.to_dict('list'), schema=schema), schema, None, 'fixture', metadata)
        self.assertEqual(native[0]['semantic_definition'], columns['id']['semantic_definition'])
        self.assertEqual(native[2]['max'], 2**63-1)
        self.assertEqual(native[2]['nativeArrowType'], 'int64')
        self.assertEqual(native[2]['statisticsScope'], 'bounded_arrow_sample')

    def test_descriptor_documents_validate_before_promotion_and_retain_approval_gate(self):
        with tempfile.TemporaryDirectory() as d:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(d) / 'stage'))
            published = PublishedStorageResource(use_local=True, local_dir=str(Path(d) / 'published'))
            prefix = 'synthetic/points/v20261116'
            metadata = refresh_fixture()
            body = b'publisher methodology'
            metadata['documentation'] = [{'path': 'raw/methodology.pdf', 'input_id': 'producer', 'purpose': 'methodology',
                                           'media_type': 'application/pdf', 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}]
            storage.write_key(prefix + '/metadata/source_manifest.json', json.dumps(metadata).encode())
            storage.write_key(prefix + '/metadata/data_dictionary.json', b'{}')
            storage.write_key(prefix + '/metadata/quality_manifest.json', b'{}')
            storage.write_key(prefix + '/raw/unreferenced.bin', b'not referenced')
            storage.write_key(prefix + '/raw/methodology.pdf', b'changed')
            request = PortolanPublishRequest('synthetic', 'synthetic', 'points', 'v20261116', 'Fixture', 'Synthetic', 'Agency')
            with self.assertRaisesRegex(ValueError, 'documentation'):
                _promote_source_metadata(storage, published, request)
            self.assertEqual(published.list_prefix(), [])
            storage.write_key(prefix + '/raw/methodology.pdf', body)
            with self.assertRaisesRegex(ReleaseApprovalError, 'missing_independent_release_approval_registry'):
                _promote_source_metadata(storage, published, request)
            self.assertEqual(published.list_prefix(), [])
            with patch('dagster_hifld.portolan.workflow.promote_immutable_objects') as promote:
                _promote_source_metadata(storage, published, request)
            pairs = [pair for call in promote.call_args_list for pair in call.args[2]]
            self.assertIn((prefix + '/raw/methodology.pdf', prefix + '/raw/methodology.pdf'), pairs)
            self.assertNotIn((prefix + '/raw/unreferenced.bin', prefix + '/raw/unreferenced.bin'), pairs)

    def test_layer_merge_references_and_document_roundtrip(self):
        self._assert_layer_roundtrip()

    def test_version_layer_inherits_omitted_inputs(self):
        self._assert_layer_roundtrip(('inputs',))

    def test_version_layer_inherits_omitted_inputs_and_processor(self):
        self._assert_layer_roundtrip(('inputs', 'processor'))

    def test_legacy_resolved_metadata_retains_legacy_behavior(self):
        with tempfile.TemporaryDirectory() as d:
            published = PublishedStorageResource(use_local=True, local_dir=d)
            prefix = 'synthetic/points/v20261116'
            legacy = json.dumps({'title': 'Legacy reference'}).encode()
            published.write_key(prefix + '/metadata/source_manifest.json', legacy)
            published.write_key(prefix + '/metadata/resolved_provenance.json', legacy)
            record = CatalogRecord('synthetic', 'synthetic', 'points', 'v20261116', 'Fixture', 'Synthetic', 'spatial', 1, ())
            self.assertIs(attach_published_provenance(record, published, prefix, 'https://example.invalid/data'), record)

    def _assert_layer_roundtrip(self, omitted=()):
        with tempfile.TemporaryDirectory() as d:
            storage = StagingStorageResource(use_local=True, local_dir=d)
            prefix = 'synthetic/points/v20261116'
            parent = provenance_fixture()
            version = refresh_fixture()
            parent['field_definitions'] = {'code': version['field_definitions'].pop('code')}
            version['inputs'] = []  # references deliberately live in the parent
            for field in omitted:
                version.pop(field)
            body = b'<metadata><field>ID</field></metadata>'
            version['documentation'] = [{'path': 'raw/schema.xml', 'input_id': 'producer', 'purpose': 'schema',
                                        'media_type': 'application/xml', 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}]
            parent['documentation'] = [{**version['documentation'][0], 'path': 'raw/parent.xml'}]
            storage.write_key('synthetic/metadata/source_manifest.json', json.dumps(parent).encode())
            storage.write_key(prefix + '/metadata/source_manifest.json', json.dumps(version).encode())
            storage.write_key(prefix + '/raw/schema.xml', body)
            storage.write_key(prefix + '/raw/parent.xml', body)
            storage.write_key(prefix + '/geoparquet/points.parquet', b'fixture')
            snapshot_source_metadata(storage, 'synthetic', 'points', 'v20261116')
            resolved = load_resolved_source_manifest(storage, 'synthetic', 'points', 'v20261116').metadata
            dictionary = generate_data_dictionary(pd.DataFrame({'id': ['001']}), 'fixture', source_metadata=resolved)
            write_catalog_metadata(storage, 'synthetic', 'points', 'v20261116', {}, dictionary)
            published = PublishedStorageResource(use_local=True, local_dir=d)
            data_key = prefix + '/geoparquet/points.parquet'
            data_asset = AssetRecord('geoparquet', 'geoparquet', 'Data', 'https://example.invalid/data/' + data_key,
                                     'application/vnd.apache.parquet', 7, hashlib.sha256(b'fixture').hexdigest(), object_key=data_key)
            record = CatalogRecord('synthetic', 'synthetic', 'points', 'v20261116', 'Fixture', 'Synthetic', 'spatial', 1, (data_asset,), columns=columns_from_dictionary(dictionary))
            record = attach_published_provenance(record, published, prefix, 'https://example.invalid/data')
            self.assertEqual(record.source_provenance, provenance_projection(resolved))
            document = next(a for a in record.metadata_assets if a.title == 'raw/schema.xml')
            self.assertEqual(document.media_type, 'application/xml')
            self.assertEqual(document.sha256, hashlib.sha256(body).hexdigest())
            tree = Path(d) / 'tree'
            render_portolan_tree(tree, (record,), public_root='https://example.invalid/data')
            dependencies = prepared_asset_dependencies(tree, published, 'https://example.invalid/data')
            verify_prepared_metadata_dependencies(tree, published, {a.object_key: dependencies[a.object_key] for a in record.metadata_assets})
            collection_path = tree / record.version_path / 'collection.json'
            incomplete = json.loads(collection_path.read_text())
            incomplete.pop('hifld:provenance')
            incomplete['assets'] = {key: value for key, value in incomplete['assets'].items() if 'metadata' not in value.get('roles', [])}
            collection_path.write_text(json.dumps(incomplete))
            with self.assertRaisesRegex(ValueError, 'cannot lose its input provenance'):
                verify_prepared_metadata_dependencies(tree, published, {})
            render_portolan_tree(tree, (record,), public_root='https://example.invalid/data')
            stac = json.loads((tree / record.version_path / 'collection.json').read_text())
            self.assertEqual(stac['hifld:provenance']['field_definitions'], {**parent['field_definitions'], **version['field_definitions']})
            build_catalog_sqlite(tree / '_catalog/catalog.sqlite', (record,))
            with sqlite3.connect(tree / '_catalog/catalog.sqlite') as connection:
                self.assertEqual(connection.execute("SELECT description FROM columns WHERE name='id'").fetchone()[0], version['field_definitions']['id']['description'])
                self.assertTrue(connection.execute("SELECT 1 FROM assets WHERE media_type='application/xml'").fetchone())
            damaged = copy.deepcopy(dictionary)
            damaged['columns'][0]['semantic_definition']['identifier_meaning'] = 'changed'
            storage.write_key(prefix + '/metadata/data_dictionary.json', json.dumps(damaged).encode())
            with self.assertRaisesRegex(ValueError, 'semantics'):
                attach_published_provenance(record, published, prefix, 'https://example.invalid/data')
            storage.write_key(prefix + '/metadata/data_dictionary.json', json.dumps(dictionary).encode())
            storage.write_key(prefix + '/raw/schema.xml', b'changed')
            with self.assertRaisesRegex(ValueError, 'documentation'):
                attach_published_provenance(record, published, prefix, 'https://example.invalid/data')
