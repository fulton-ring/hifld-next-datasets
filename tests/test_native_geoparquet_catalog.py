"""Pinned native fallback; all fixtures synthetic and no real data accepted."""
from __future__ import annotations
import base64
import copy
import json
import shutil
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
import shapely

from dagster_hifld.catalog import summarize_staged_catalog
from dagster_hifld.definitions import _iter_staged_version_paths,_iter_validated_gcs_staged_versions
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld import native_geoparquet_catalog as native
from dagster_hifld.validation import sha256_file
from dagster_hifld.source_manifest import load_resolved_source_manifest

PREFIX='synthetic-native/synthetic-native/v0.0.0-test'


def write_fixture(root,*,count=6,crs='explicit',tail=None,metadata_transform=None):
    geometry=[shapely.to_wkb(shapely.Point(i,0))for i in range(count)]
    if tail=='invalid':geometry[-1]=shapely.to_wkb(shapely.Polygon([(0,0),(9,9),(0,9),(9,0),(0,0)]))
    if tail=='null':geometry[-1]=None
    if tail=='extent':geometry[-1]=shapely.to_wkb(shapely.Point(90000,80000))
    fields=[pa.field('large_id',pa.int64()),pa.field('amount',pa.decimal128(38,15)),pa.field('large_decimal',pa.decimal256(60,20)),
        pa.field('small',pa.int16()),pa.field('single',pa.float32()),pa.field('when',pa.date32()),
        pa.field('geometry',pa.string()),pa.field('wkb_geometry',pa.binary()),pa.field('native_shape',pa.binary()),
        pa.field('bbox',pa.struct([pa.field('xmin',pa.float64()),pa.field('xmax',pa.float64())]))]
    arrays=[pa.array([None]+[2**53+1+i for i in range(count-1)],type=pa.int64()),
        pa.array([Decimal('2586408727577.089843750000000')]*count,type=fields[1].type),
        pa.array([Decimal('123456789012345678901234567890123456789.12345678901234567890')]*count,type=fields[2].type),
        pa.array([1]*count,type=pa.int16()),pa.array([1.25]*count,type=pa.float32()),pa.array([date(2026,1,1)]*count,type=pa.date32()),
        pa.array(['literal ordinary geometry attribute']*count),pa.array([b'ordinary opaque attribute']*count),pa.array(geometry,type=pa.binary()),
        pa.array([{'xmin':0.0,'xmax':1.0}]*count,type=fields[-1].type)]
    column={'encoding':'WKB','geometry_types':['Point']if tail!='invalid'else['Point','Polygon']}
    if crs=='explicit':column['crs']=CRS(3857).to_json_dict()
    elif crs=='null':column['crs']=None
    geo={'version':'1.1.0','primary_column':'native_shape','columns':{'native_shape':column}}
    if metadata_transform:metadata_transform(geo)
    table=pa.Table.from_arrays(arrays,schema=pa.schema(fields,metadata={b'geo':json.dumps(geo).encode()}))
    path=root/PREFIX/'geoparquet/native.parquet';path.parent.mkdir(parents=True,exist_ok=True);pq.write_table(table,path)
    metadata={'title':'Synthetic native fixture; no dataset/source acceptance','prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{
        'reason':'SYNTHETIC native fidelity mechanics only; no source/contract/baseline/release approval.',
        'objects':[{'path':'geoparquet/native.parquet','bytes':path.stat().st_size,'sha256':sha256_file(path)}]}}}}
    (root/PREFIX/'metadata').mkdir();(root/PREFIX/'metadata/source_manifest.json').write_text(json.dumps(metadata))
    return table,path,metadata


class RemoteFixture:
    """Deterministic local transport emulates GCS protocol; no cloud write/read."""
    use_local=False;bucket='synthetic-bucket';prefix=''
    def __init__(self,local):self.local=local;self.downloads=[]
    def _ensure_prefixed(self,key):return self.local._ensure_prefixed(key)
    def list_prefix(self,prefix):return self.local.list_prefix(prefix)
    def object_snapshot(self,key):return self.local.object_snapshot(key)
    def download_key_to(self,key,destination):self.downloads.append(key);shutil.copyfile(Path(self.local.local_dir)/key,destination)


class NativeCatalogTests(unittest.TestCase):
    def test_exact_native_attributes_names_widths_decimals_Date32_and_nulls(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);table,path,metadata=write_fixture(root);storage=StagingStorageResource(local_dir=tmp)
            summary=summarize_staged_catalog(storage,*PREFIX.split('/'),'synthetic',source_metadata=metadata)
            columns={c['name']:c for c in summary.data_dictionary['columns']}
            self.assertEqual(list(columns),table.column_names)
            self.assertEqual(columns['geometry']['type'],'string');self.assertEqual(columns['wkb_geometry']['type'],'binary')
            self.assertEqual(columns['native_shape']['type'],'geometry')
            self.assertEqual(columns['large_id']['nativeArrowType'],'int64');self.assertEqual(columns['large_id']['exampleValues'][0],str(2**53+1))
            self.assertEqual(columns['small']['nativeArrowType'],'int16');self.assertEqual(columns['single']['nativeArrowType'],'float')
            self.assertEqual(columns['when']['nativeArrowType'],'date32[day]')
            self.assertEqual(columns['amount']['type'],'decimal');self.assertEqual(columns['amount']['min'],'2586408727577.089843750000000')
            self.assertEqual(columns['amount']['precision'],38);self.assertEqual(columns['amount']['scale'],15)
            self.assertEqual(columns['large_decimal']['min'],'123456789012345678901234567890123456789.12345678901234567890')
            self.assertEqual(columns['bbox']['type'],'struct')
            self.assertTrue(summary.quality_manifest['quality_check_passed']);self.assertEqual(summary.quality_manifest['feature_count'],6)
            self.assertEqual(sha256_file(path),metadata['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]['sha256'])

    def test_full_population_after_5000_quality_bounds_and_nulls(self):
        for tail in ['invalid','null','extent']:
            with self.subTest(tail=tail),tempfile.TemporaryDirectory()as tmp:
                root=Path(tmp);table,path,metadata=write_fixture(root,count=6001,tail=tail)
                summary=summarize_staged_catalog(StagingStorageResource(local_dir=tmp),*PREFIX.split('/'),'synthetic',source_metadata=metadata)
                quality=summary.quality_manifest
                self.assertEqual(quality['feature_count'],6001);self.assertEqual(quality['sampled_feature_count'],5000)
                if tail=='invalid':self.assertEqual(quality['invalid_geometry_count'],1);self.assertFalse(quality['quality_check_passed'])
                if tail=='null':self.assertEqual(quality['null_geometry_count'],1);self.assertFalse(quality['quality_check_passed'])
                if tail=='extent':self.assertEqual(quality['bounds'],[0.0,0.0,90000.0,80000.0]);self.assertTrue(quality['quality_check_passed'])

    def test_omitted_CRS84_and_explicit_unknown_null_distinct(self):
        for crs in ['omitted','null']:
            with self.subTest(crs=crs),tempfile.TemporaryDirectory()as tmp:
                root=Path(tmp);table,path,metadata=write_fixture(root,crs=crs);storage=StagingStorageResource(local_dir=tmp)
                if crs=='null':
                    with self.assertRaisesRegex(ValueError,'explicit_null_crs'):summarize_staged_catalog(storage,*PREFIX.split('/'),'synthetic',source_metadata=metadata)
                else:
                    quality=summarize_staged_catalog(storage,*PREFIX.split('/'),'synthetic',source_metadata=metadata).quality_manifest
                    self.assertEqual(quality['crs_source'],'GeoParquet_normative_OGC_CRS84')
                    self.assertTrue(CRS(quality['native_crs']).equals(CRS('OGC:CRS84')))

    def test_stale_bbox_unknown_geometry_encoding_and_primary_metadata_block(self):
        changes=[lambda g:g['columns']['native_shape'].update(bbox=[0,0,1,1]),
            lambda g:g['columns']['native_shape'].update(encoding='point'),
            lambda g:g.update(primary_column='missing'),lambda g:g['columns']['native_shape'].update(geometry_types=[{}])]
        for change in changes:
            with self.subTest(change=change),tempfile.TemporaryDirectory()as tmp:
                root=Path(tmp);table,path,metadata=write_fixture(root,metadata_transform=change)
                with self.assertRaises(ValueError):summarize_staged_catalog(StagingStorageResource(local_dir=tmp),*PREFIX.split('/'),'synthetic',source_metadata=metadata)

    def test_missing_extra_changed_or_multi_object_policy_block(self):
        for mode in ['missing','extra','changed','multiple']:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory()as tmp:
                root=Path(tmp);table,path,metadata=write_fixture(root);storage=StagingStorageResource(local_dir=tmp)
                if mode=='missing':path.unlink()
                if mode=='extra':path.with_name('unlisted.parquet').write_bytes(path.read_bytes())
                if mode=='changed':path.write_bytes(path.read_bytes()+b'changed')
                if mode=='multiple':metadata['prebuilt_output_policy']['formats']['geoparquet']['objects'].append({**metadata['prebuilt_output_policy']['formats']['geoparquet']['objects'][0],'path':'geoparquet/other.parquet'})
                with self.assertRaises(ValueError):summarize_staged_catalog(storage,*PREFIX.split('/'),'synthetic',source_metadata=metadata)

    def test_discovery_opt_in_local_and_GCS_protocol_ignores_plain_derived(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);table,path,metadata=write_fixture(root);storage=StagingStorageResource(local_dir=tmp)
            self.assertEqual(list(_iter_staged_version_paths(storage)),[tuple(PREFIX.split('/'))])
            names=storage.list_prefix(PREFIX);remote=RemoteFixture(storage)
            with patch('dagster_hifld.native_geoparquet_catalog.load_resolved_source_manifest',return_value=SimpleNamespace(metadata=metadata)):
                self.assertEqual(list(_iter_validated_gcs_staged_versions(names,storage=remote)),[tuple(PREFIX.split('/'))])
            self.assertEqual(len(remote.downloads),1)
            self.assertEqual(list(_iter_validated_gcs_staged_versions(names)),[])
            (root/PREFIX/'metadata/source_manifest.json').unlink()
            self.assertEqual(list(_iter_staged_version_paths(storage)),[])
            with patch('dagster_hifld.native_geoparquet_catalog.load_resolved_source_manifest',return_value=SimpleNamespace(metadata={})):
                self.assertEqual(list(_iter_validated_gcs_staged_versions(names,storage=remote)),[])

    def test_remote_catalog_downloads_single_native_object_without_raw_duplicates(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);table,path,metadata=write_fixture(root);local=StagingStorageResource(local_dir=tmp)
            raw=root/PREFIX/'raw/large-diagnostic';raw.parent.mkdir();raw.write_bytes(b'raw remains untouched')
            remote=RemoteFixture(local);summary=summarize_staged_catalog(remote,*PREFIX.split('/'),'synthetic',source_metadata=metadata)
            self.assertEqual(summary.quality_manifest['feature_count'],6);self.assertEqual(remote.downloads,[PREFIX+'/geoparquet/native.parquet'])
            self.assertEqual(raw.read_bytes(),b'raw remains untouched')

    def test_existing_corrupt_canonical_and_unknown_CSV_are_not_masked(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);table,path,metadata=write_fixture(root);storage=StagingStorageResource(local_dir=tmp)
            gpkg=root/PREFIX/'geopackage/bad.gpkg';gpkg.parent.mkdir();gpkg.write_bytes(b'not a valid GPKG')
            with self.assertRaisesRegex(ValueError,'No loadable source'):summarize_staged_catalog(storage,*PREFIX.split('/'),'synthetic',source_metadata=metadata)
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);table,path,metadata=write_fixture(root);storage=StagingStorageResource(local_dir=tmp)
            csv=root/PREFIX/'unknown/ordinary.csv';csv.parent.mkdir();csv.write_text('id,value\n1,a\n2,b\n')
            summary=summarize_staged_catalog(storage,*PREFIX.split('/'),'synthetic',source_metadata=metadata)
            self.assertEqual(summary.quality_manifest['feature_count'],2)
            self.assertEqual(summary.quality_manifest['catalog_mode'],'tabular')

    def test_native_and_manifest_drift_during_read_preserves_original_files(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);table,path,metadata=write_fixture(root);storage=StagingStorageResource(local_dir=tmp)
            original=path.read_bytes()
            with self.assertRaises(ValueError):
                with native.pinned_native_path(storage,PREFIX,metadata):path.write_bytes(original+b'changed')
            path.write_bytes(original)
            resolved=load_resolved_source_manifest(storage,*PREFIX.split('/')).metadata
            manifest=root/PREFIX/'metadata/source_manifest.json';saved=manifest.read_bytes()
            with self.assertRaises((ValueError,RuntimeError)):
                with native.pinned_native_path(storage,PREFIX,resolved):manifest.write_bytes(saved+b' ')
            self.assertEqual(path.read_bytes(),original)

    def test_local_aliases_reject_before_any_snapshot_body_hash(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);table,path,metadata=write_fixture(root);original=path.read_bytes();storage=StagingStorageResource(local_dir=tmp)
            other=root/'outside.parquet';path.rename(other);path.symlink_to(other)
            with patch.object(StagingStorageResource,'object_snapshot',side_effect=AssertionError('Must not hash aliased bytes')):
                with self.assertRaises(ValueError):
                    with native.pinned_native_path(storage,PREFIX,metadata):pass
            self.assertEqual(other.read_bytes(),original)

    def test_native_schema_IPC_and_mixed_2D_3D_collection_preserved(self):
        geometry=shapely.GeometryCollection([shapely.Point(0,0),shapely.Point(1,1,2)])
        finite,bounds=native._finite_geometry_coordinates(geometry)
        self.assertTrue(finite);self.assertEqual(bounds,[2.0,2.0])
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);table,path,metadata=write_fixture(root)
            summary=summarize_staged_catalog(StagingStorageResource(local_dir=tmp),*PREFIX.split('/'),'synthetic',source_metadata=metadata)
            schema=pa.ipc.read_schema(pa.BufferReader(base64.b64decode(summary.data_dictionary['nativeArrowSchemaIPCBase64'])))
            self.assertEqual(schema,table.schema)

    def test_integer_and_float_ranges_keep_existing_convention_without_ID_rounding(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);table,path,metadata=write_fixture(root)
            index=table.schema.get_field_index('single')
            values=pa.array([0.1,0.3,1.0,2.0,3.0,4.0],type=pa.float32())
            table=table.set_column(index,table.schema.field(index),values);pq.write_table(table,path)
            metadata['prebuilt_output_policy']['formats']['geoparquet']['objects'][0].update(bytes=path.stat().st_size,sha256=sha256_file(path))
            summary=summarize_staged_catalog(StagingStorageResource(local_dir=tmp),*PREFIX.split('/'),'synthetic',source_metadata=metadata)
            fields={c['name']:c for c in summary.data_dictionary['columns']}
            self.assertEqual(fields['large_id']['min'],2**53+1);self.assertIs(type(fields['large_id']['min']),int)
            self.assertEqual(fields['large_id']['max'],2**53+5)
            self.assertEqual(fields['single']['min'],values[0].as_py());self.assertEqual(fields['single']['max'],4.0)
            self.assertEqual(fields['single']['nativeBitWidth'],32)
            self.assertEqual(fields['large_id']['statisticsScope'],'bounded_arrow_sample')
