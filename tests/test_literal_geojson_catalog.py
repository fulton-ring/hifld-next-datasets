"""Literal opt-in integrity/type semantics; entirely synthetic small fixtures."""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.literal_geojson_catalog import resolve_literal_metadata
from dagster_hifld.literal_geojson_policy import literal_geojson_spec, MAX_LITERAL_GEOJSON_BYTES
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld import literal_geojson_catalog as module

PREFIX = 'synthetic-literal/native/v0.0.0-test'


def feature(properties=None, *, geometry='default', **members):
    return {'type':'Feature', 'properties':{} if properties is None else properties,
            'geometry':{'type':'Point','coordinates':[1,2]} if geometry=='default' else geometry, **members}


def fixture(root, features=None, *, body=None, prefix=''):
    storage=StagingStorageResource(local_dir=str(root),prefix=prefix)
    if body is None:
        body=json.dumps({'type':'FeatureCollection','features':features if features is not None else [feature()]}).encode()
    spec={'schema_version':1,'path':'geojson/native.geojson','bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()}
    metadata={'title':'Synthetic literal JSON; no source/contract/release approval','literal_geojson_catalog':spec}
    storage.write_key(PREFIX+'/'+spec['path'],body)
    storage.write_key(PREFIX+'/metadata/source_manifest.json',json.dumps(metadata).encode())
    return storage,metadata


def summary(storage,metadata=None):
    return summarize_staged_catalog(storage,*PREFIX.split('/'),'Synthetic literal',source_metadata=metadata)


class Remote:
    """GCS-shaped deterministic transport, no cloud access or source acceptance."""
    use_local=False;bucket='synthetic-bucket'
    def __init__(self,local):self.local=local;self.prefix=local.prefix;self.downloads=[];self.ranges=[]
    def _ensure_prefixed(self,key):return self.local._ensure_prefixed(key)
    def list_prefix(self,key):return self.local.list_prefix(key)
    def object_snapshot(self,key):return self.local.object_snapshot(key)
    def read_key_range(self,key,offset,length,*,expected_snapshot):
        self.ranges.append((key,length));return self.local.read_key_range(key,offset,length,expected_snapshot=expected_snapshot)
    def download_key_to(self,key,path):
        self.downloads.append(key);shutil.copyfile(Path(self.local.local_dir)/self._ensure_prefixed(key),path)


class LiteralCatalogTests(unittest.TestCase):
    def test_mixed_literals_presence_nulls_large_IDs_and_geometry_named_property(self):
        values=[feature({'ID':'001','mixed':1,'geometry':'ordinary field','absent_later':None,'nested':[True,1,1.0,{'n':''}]},id=2**53+1),
                feature({'ID':2**53+7,'mixed':'','geometry':False}),feature({'mixed':False},id='001'),
                feature({'mixed':1.0}),feature({'mixed':None},id=None),feature({'mixed':[1,'',None]})]
        with tempfile.TemporaryDirectory()as tmp:
            storage,metadata=fixture(Path(tmp),values)
            with patch('dagster_hifld.catalog._summarize_geospatial_source',side_effect=AssertionError('Fiona must not run')):
                result=summary(storage,metadata)
            columns={c['name']:c for c in result.data_dictionary['columns']}
            self.assertEqual(columns['mixed']['nativeJSONTypeCounts'],{'array':1,'boolean':1,'float':1,'integer':1,'null':1,'string':1})
            self.assertEqual(columns['absent_later']['numMissingValues'],5)
            self.assertEqual(columns['absent_later']['numNullValues'],1)
            self.assertEqual(columns['geometry']['nativeJSONTypeCounts'],{'boolean':1,'string':1})
            ids=result.data_dictionary['featureIDMember'];self.assertEqual(ids['presentCount'],3)
            self.assertEqual(ids['numMissingValues'],3);self.assertEqual(ids['numNullValues'],1)
            self.assertEqual(ids['typedExamples'][0]['value'],2**53+1)
            self.assertEqual(columns['ID']['typedExamples'][1]['value'],2**53+7)
            self.assertTrue(result.quality_manifest['quality_check_passed'])

    def test_float_lexemes_retained_separately_from_decoded_binary64_examples(self):
        body=b'{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"number":1.0000000000000001,"nested":[1e-10,1.0]},"geometry":{"type":"Point","coordinates":[1.0,2]}}]}'
        with tempfile.TemporaryDirectory()as tmp:
            storage,metadata=fixture(Path(tmp),body=body);a=summary(storage,metadata)
            c={x['name']:x for x in a.data_dictionary['columns']}
            example=c['number']['typedExamples'][0]
            self.assertEqual(example['value'],1.0)
            self.assertEqual(example['nativeJSONValue']['numericLexeme'],'1.0000000000000001')
            self.assertEqual(c['nested']['typedExamples'][0]['nativeJSONValue']['value'][0]['numericLexeme'],'1e-10')
            other=body.replace(b'1.0000000000000001',b'1.0')
            storage,metadata=fixture(Path(tmp),body=other);b=summary(storage,metadata)
            self.assertNotEqual(a.data_dictionary['nativePropertyPopulationSHA256'],b.data_dictionary['nativePropertyPopulationSHA256'])

    def test_full_population_geometry_and_new_field_after_dictionary_sample(self):
        for tail in ['extent','null','invalid']:
            with self.subTest(tail=tail),tempfile.TemporaryDirectory()as tmp:
                values=[feature({'ID':i})for i in range(6001)]
                values[-1]['properties']['tail_only']=False
                values[-1]['geometry']=None if tail=='null' else {'type':'Polygon','coordinates':[[[0,0],[9,9],[0,9],[9,0],[0,0]]]}if tail=='invalid'else{'type':'Point','coordinates':[170,80]}
                storage,metadata=fixture(Path(tmp),values);result=summary(storage,metadata)
                q=result.quality_manifest;self.assertEqual(q['feature_count'],6001);self.assertEqual(q['sampled_feature_count'],5000)
                field=next(c for c in result.data_dictionary['columns']if c['name']=='tail_only')
                self.assertEqual(field['numMissingValues'],6000);self.assertEqual(field['nativeJSONTypeCounts'],{'boolean':1});self.assertEqual(field['typedExamples'],[])
                if tail=='extent':self.assertEqual(q['bounds'],[1,2,170,80]);self.assertTrue(q['quality_check_passed'])
                elif tail=='null':self.assertEqual(q['null_geometry_count'],1);self.assertFalse(q['quality_check_passed'])
                else:self.assertEqual(q['invalid_geometry_count'],1);self.assertFalse(q['quality_check_passed'])

    def test_strict_JSON_rejects_duplicates_overflow_nonfinite_truncation_and_shapes(self):
        bad=[b'{"type":"FeatureCollection","type":"FeatureCollection","features":[]}',
             b'{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"x":1,"x":2},"geometry":null}]}',
             b'{"type":"FeatureCollection","features":[],"bbox":[NaN,0,1,2]}',
             b'{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"x":1e9999},"geometry":null}]}',
             b'{"type":"FeatureCollection","features":[',b'{"type":"FeatureCollection","features":{}}']
        for body in bad:
            with self.subTest(body=body),tempfile.TemporaryDirectory()as tmp:
                storage,metadata=fixture(Path(tmp),body=body)
                with patch('dagster_hifld.catalog._summarize_geospatial_source',side_effect=AssertionError('fallback forbidden')),self.assertRaises(ValueError):summary(storage,metadata)

    def test_invalid_geometry_is_counted_without_coercion_or_polygon_repair(self):
        geometries=[{'type':'Point','coordinates':['1','2']},{'type':'Point','coordinates':[True,False]},
                    {'type':'Unknown','coordinates':[1,2]}, {'type':'Point','coordinates':[10**400,1]},
                    {'type':'Polygon','coordinates':[[[0,0],[1,0],[1,1],[0,1]]]}]
        for geometry in geometries:
            with self.subTest(geometry=geometry),tempfile.TemporaryDirectory()as tmp:
                storage,metadata=fixture(Path(tmp),[feature(geometry=geometry)]);q=summary(storage,metadata).quality_manifest
                self.assertEqual(q['invalid_geometry_count'],1);self.assertFalse(q['quality_check_passed'])

    def test_explicit_unknown_CRS_foreign_members_and_forged_bbox_fail(self):
        roots=[{'type':'FeatureCollection','features':[feature()],'crs':None},
               {'type':'FeatureCollection','features':[feature()],'bbox':[0,0,0,0]},
               {'type':'FeatureCollection','features':[feature(bbox=[0,0,0,0])]},
               {'type':'FeatureCollection','features':[feature()],'unhandled_root':True}]
        for data in roots:
            with self.subTest(data=data),tempfile.TemporaryDirectory()as tmp:
                storage,metadata=fixture(Path(tmp),body=json.dumps(data).encode())
                with self.assertRaises(ValueError):summary(storage,metadata)

    def test_profile_bad_values_null_empty_conflicts_and_unpinned_objects_fail(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);storage,metadata=fixture(root)
            spec=metadata['literal_geojson_catalog'];bad=[None,{},[],{**spec,'schema_version':True},
                {**spec,'bytes':True},{**spec,'bytes':float(spec['bytes'])},{**spec,'bytes':MAX_LITERAL_GEOJSON_BYTES+1},
                {**spec,'path':'geojson/../other.geojson'},{**spec,'sha256':'0'*64},{**spec,'unknown':1}]
            for value in bad:
                with self.subTest(value=value):
                    storage.write_key(PREFIX+'/metadata/source_manifest.json',json.dumps({'literal_geojson_catalog':value}).encode())
                    with self.assertRaises(ValueError):summary(storage,{'literal_geojson_catalog':value})
            storage,metadata=fixture(root)
            with self.assertRaisesRegex(ValueError,'not_authored'):
                summary(storage,{'literal_geojson_catalog':{**spec,'sha256':'0'*64}})
            storage.write_key(PREFIX+'/geojson/extra.json',b'{}')
            with self.assertRaisesRegex(ValueError,'roster'):summary(storage,metadata)

    def test_higher_precedence_corrupt_canonical_is_not_masked(self):
        with tempfile.TemporaryDirectory()as tmp:
            storage,metadata=fixture(Path(tmp));storage.write_key(PREFIX+'/geopackage/corrupt.gpkg',b'not a database')
            with self.assertRaisesRegex(ValueError,'higher_precedence'):summary(storage,metadata)

    def test_caller_omission_cannot_erase_actual_authored_profile_or_invalid_presence(self):
        for supplied in [{}, {'title':'Legacy subset without capability fields'}]:
            with self.subTest(supplied=supplied),tempfile.TemporaryDirectory()as tmp:
                storage,metadata=fixture(Path(tmp),[feature({'mixed':1}),feature({'mixed':''})])
                # Valid explicit input remains literal, even without a profile
                # in the caller's optional display-metadata subset.
                self.assertEqual(summary(storage,supplied).quality_manifest['catalog_mode'],'literal_geojson_full_population')
                storage.write_key(PREFIX+'/geojson/native.geojson',json.dumps({'type':'FeatureCollection','features':[feature({'mixed':1}),feature({'mixed':2})]}).encode())
                with patch('dagster_hifld.catalog._summarize_geospatial_source',side_effect=AssertionError('Fiona fallback forbidden')),self.assertRaises(ValueError):summary(storage,supplied)
                storage.write_key(PREFIX+'/metadata/source_manifest.json',b'{"literal_geojson_catalog":null}')
                with self.assertRaisesRegex(ValueError,'profile'):summary(storage,supplied)

    def test_GCS_shaped_transport_nested_prefix_downloads_only_the_pinned_object(self):
        with tempfile.TemporaryDirectory()as tmp:
            local,metadata=fixture(Path(tmp),[feature({'ID':2**53+1,'value':''})],prefix='sandbox/nested')
            local.write_key(PREFIX+'/raw/huge-original.bin',b'Untouched synthetic raw evidence')
            remote=Remote(local);result=summary(remote,metadata)
            self.assertEqual(remote.downloads,['sandbox/nested/'+PREFIX+'/geojson/native.geojson'])
            self.assertTrue(all('/metadata/source_manifest.json'in key for key,length in remote.ranges))
            self.assertTrue(result.quality_manifest['quality_check_passed'])

    def test_input_or_manifest_drift_during_scan_fails_before_catalog_write(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);storage,metadata=fixture(root);path=root/PREFIX/'geojson/native.geojson'
            real=module._summarize
            def race(*args):
                result=real(*args);path.write_bytes(path.read_bytes()+b' ');return result
            with patch.object(module,'_summarize',side_effect=race),self.assertRaisesRegex(ValueError,'pinned_bytes'):summary(storage,metadata)
            storage,metadata=fixture(root);manifest=root/PREFIX/'metadata/source_manifest.json'
            def manifest_race(*args):
                result=real(*args);manifest.write_bytes(manifest.read_bytes()+b' ');return result
            with patch.object(module,'_summarize',side_effect=manifest_race),self.assertRaises((ValueError,RuntimeError)):summary(storage,metadata)

    def test_aliases_and_oversized_layer_rejected_before_source_download(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);storage,metadata=fixture(root);path=root/PREFIX/'geojson/native.geojson';body=path.read_bytes();outside=root/'outside';outside.write_bytes(body);path.unlink();path.symlink_to(outside)
            with self.assertRaises(ValueError):summary(storage,metadata)
            self.assertEqual(outside.read_bytes(),body)
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);local,metadata=fixture(root);local.write_key(PREFIX+'/metadata/source_manifest.json',b' '* (2*1024*1024+1));remote=Remote(local)
            with self.assertRaisesRegex(ValueError,'layer limit'):summary(remote,metadata)
            self.assertEqual(remote.downloads,[]);self.assertEqual(remote.ranges,[])

    def test_profile_survives_resolved_layers_dictionary_and_downloadable_provenance(self):
        with tempfile.TemporaryDirectory()as tmp:
            storage,metadata=fixture(Path(tmp));metadata.update(schema_version=1,processor='HIFLD Next',code_commit='synthetic-only',transformations=[],inputs=[{
                'input_id':'synthetic','producer':'Synthetic Agency','host':'example.invalid','canonical_url':'https://example.invalid/input',
                'retrieval_url':'https://example.invalid/input','retrieved_at':'2026-10-19T00:00:00Z','sha256':'a'*64,'bytes':1,
                'license':'Synthetic','attribution':'Synthetic Agency','role':'producer_publication','release_date_unknown_reason':'Synthetic test, no real source'}])
            storage.write_key(PREFIX+'/metadata/source_manifest.json',json.dumps(metadata).encode());resolved=load_resolved_source_manifest(storage,*PREFIX.split('/')).metadata
            result=summary(storage,resolved);self.assertEqual(result.data_dictionary['literal_geojson_catalog'],metadata['literal_geojson_catalog'])
            paths=write_catalog_metadata(storage,*PREFIX.split('/'),result.quality_manifest,result.data_dictionary)
            projection=json.loads(storage.read_key(paths['resolved_provenance']))
            self.assertEqual(projection['literal_geojson_catalog'],metadata['literal_geojson_catalog'])
            self.assertEqual(provenance_projection(resolved)['literal_geojson_catalog'],metadata['literal_geojson_catalog'])
            storage.write_key('synthetic-literal/metadata/source_manifest.json',json.dumps({'literal_geojson_catalog':{**metadata['literal_geojson_catalog'],'sha256':'b'*64}}).encode())
            with self.assertRaisesRegex(ValueError,'Conflicting inherited'):summary(storage,metadata)
