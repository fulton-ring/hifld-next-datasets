"""Physical widths and nullable integer precision survive real terminal conversion."""
import asyncio
import tempfile
import unittest
from pathlib import Path

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from shapely import Point, to_wkb

from dagster_hifld.conversion import (
    GeoParquetWritePolicy, _StorageAdapter, _coerce_gdf_to_fiona_schema,
    process_layer_partitioned_geoparquet,
)
from dagster_hifld.resources import StagingStorageResource


class IntegerFidelityTest(unittest.TestCase):
    def test_actual_geopackage_to_terminal_parquet_preserves_width_nulls_and_big_ids(self):
        values32 = [-(2**31), 2**31-1, None, 0]
        values64 = [-(2**63), 2**53+1, 2**63-1, None]
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); source=root/'source.gpkg'
            schema=pa.schema([pa.field('row_key',pa.int32()),pa.field('id32',pa.int32()),pa.field('id64',pa.int64()),pa.field('geometry',pa.binary())])
            table=pa.Table.from_pylist([{'row_key':i,'id32':a,'id64':b,'geometry':to_wkb(Point(i,i))} for i,(a,b) in enumerate(zip(values32,values64))],schema=schema)
            pyogrio.write_arrow(table,source,layer='source',driver='GPKG',geometry_name='geometry',geometry_type='Point',crs='EPSG:4326')
            storage=StagingStorageResource(local_dir=str(root/'staging'),use_local=True)
            result=asyncio.run(process_layer_partitioned_geoparquet(file_path=source,format_type='geopackage',layer_name='source',layer_filename='source',dest_folder='dataset/file/v0.0.0-synthetic/',dest_storage=_StorageAdapter(storage),work_dir=root/'work',policy=GeoParquetWritePolicy()))
            self.assertNotIn('error',result)
            outputs=list((root/'staging').rglob('*.parquet'));self.assertTrue(outputs)
            rows=[]
            for path in outputs:
                terminal=pq.ParquetFile(path).read()
                self.assertEqual(terminal.schema.field('id32').type,pa.int32())
                self.assertEqual(terminal.schema.field('id64').type,pa.int64())
                rows.extend(terminal.select(['row_key','id32','id64']).to_pylist())
            rows.sort(key=lambda row:row['row_key'])
            self.assertEqual([row['id32'] for row in rows],values32)
            self.assertEqual([row['id64'] for row in rows],values64)

    def test_original_integer_records_restore_values_already_rounded_by_dataframe(self):
        features=[{'type':'Feature','properties':{'id':2**53+1},'geometry':{'type':'Point','coordinates':[0,0]}}, {'type':'Feature','properties':{'id':None},'geometry':{'type':'Point','coordinates':[1,1]}}]
        frame=gpd.GeoDataFrame.from_features(features,crs='EPSG:4326')
        self.assertNotEqual(int(frame.iloc[0]['id']),2**53+1)
        exact=_coerce_gdf_to_fiona_schema(frame,{'properties':{'id':'int64'}},features)
        self.assertEqual(exact['id'].iloc[0],2**53+1)
        self.assertTrue(pa.Table.from_pandas(exact.drop(columns='geometry'))['id'].null_count==1)

    def test_invalid_integer_values_fail_instead_of_coercing_or_truncating(self):
        for kind,value in [('int32',2**31),('int32',-(2**31)-1),('int64',2**63),('int64',2.5),('int64',True),('int64','7')]:
            features=[{'type':'Feature','properties':{'id':value},'geometry':{'type':'Point','coordinates':[0,0]}}]
            frame=gpd.GeoDataFrame.from_features(features,crs='EPSG:4326')
            with self.subTest(kind=kind,value=value),self.assertRaisesRegex(ValueError,'Noninteger or out-of-range'):
                _coerce_gdf_to_fiona_schema(frame,{'properties':{'id':kind}},features)


if __name__=='__main__':unittest.main()
