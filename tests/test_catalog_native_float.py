"""Native FLOAT GeoPackage catalog regression; bounded synthetic values only."""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase,mock
import fiona
import pyarrow as pa
import pyogrio
from shapely import Point,to_wkb
from dagster_hifld.catalog import _summarize_geospatial_source,generate_data_dictionary
from dagster_hifld.sources.tsunami_observations import NATIVE_FIELDS,KIND

class NativeFloatCatalogTests(TestCase):
    def test_all_fifty_native_attributes_float_values_and_nullable_widths_survive_sampling(self):
        types={'int16':pa.int16(),'int32':pa.int32(),'float32':pa.float32(),'float64':pa.float64(),'string':pa.string()}
        arrays=[];fields=[]
        for f in NATIVE_FIELDS:
            kind=KIND[f['type']];values=([17,18,None] if kind.startswith('int') else [0.1,3.0,None] if kind.startswith('float') else ['original','text',None])
            if not f['nullable']:values=[17,18,19]
            arrays.append(pa.array(values,type=types[kind]));fields.append(pa.field(f['name'],types[kind],nullable=f['nullable']))
        arrays.append(pa.array([to_wkb(Point(1,2)),to_wkb(Point(3,4)),None],type=pa.binary()));fields.append(pa.field('geometry',pa.binary()))
        table=pa.Table.from_arrays(arrays,schema=pa.schema(fields))
        with TemporaryDirectory() as d:
            path=Path(d)/'native.gpkg'
            pyogrio.raw.write_arrow(table,path,driver='GPKG',layer='native',geometry_name='geometry',geometry_type='Point',crs='EPSG:3857',layer_options={'GEOMETRY_NAME':'geometry','FID':'__storage_fid'})
            with fiona.open(path,layer='native') as source:
                fiona_columns=set(source.schema['properties'])
            self.assertLessEqual(len(fiona_columns),50)
            # Limit two proves the catalog does not read all rows and still reports the full population.
            with mock.patch('dagster_hifld.catalog.CATALOG_SAMPLE_FEATURE_LIMIT',2):sample,quality=_summarize_geospatial_source(path,'native')
            self.assertEqual(len(sample),2);self.assertEqual(quality['feature_count'],3);self.assertEqual(quality['sampled_feature_count'],2)
            self.assertEqual(set(sample.columns),set(table.column_names));self.assertEqual(str(sample['YEAR'].dtype),'int16[pyarrow]')
            self.assertEqual(str(sample['RUNUP_HT'].dtype),'float[pyarrow]')
            columns={c['name']:c for c in generate_data_dictionary(sample,'native')['columns']}
            for name in ['PERIOD','RUNUP_HT','RUNUP_HORIZ','PRED_TRAV_HOURS']:
                self.assertEqual(columns[name]['type'],'float');self.assertEqual(columns[name]['numNullValues'],0)
                self.assertEqual(columns[name]['min'],table.column(name)[0].as_py());self.assertEqual(columns[name]['max'],3.0)
            with mock.patch('dagster_hifld.catalog.CATALOG_SAMPLE_FEATURE_LIMIT',3):sample,quality=_summarize_geospatial_source(path,'native')
            self.assertFalse(quality['quality_check_passed']);self.assertEqual(quality['sampled_null_geometry_count'],1)
            columns={c['name']:c for c in generate_data_dictionary(sample,'native')['columns']};self.assertEqual(columns['RUNUP_HT']['numNullValues'],1)
            self.assertEqual(pyogrio.raw.read_arrow(path,layer='native')[1].column('RUNUP_HT').to_pylist(),table.column('RUNUP_HT').to_pylist())

    def test_attributes_only_float_geopackage_retains_values_without_inventing_geometry(self):
        table=pa.table({'ID':pa.array([1,2],type=pa.int32()),'MEASUREMENT':pa.array([0.1,None],type=pa.float32()),'geometry':pa.array(['original attribute','retained attribute'],type=pa.string())})
        with TemporaryDirectory() as d:
            path=Path(d)/'attributes.gpkg'
            pyogrio.raw.write_arrow(table,path,driver='GPKG',layer='attributes')
            sample,quality=_summarize_geospatial_source(path,'attributes')
            self.assertEqual(list(sample.columns),table.column_names)
            self.assertEqual(sample['geometry'].tolist(),table.column('geometry').to_pylist())
            self.assertEqual(str(sample['MEASUREMENT'].dtype),'float[pyarrow]')
            self.assertEqual(quality['feature_count'],2);self.assertTrue(quality['quality_check_passed'])
            self.assertEqual(quality['spatial_status'],'non_spatial_source');self.assertIsNone(quality['bounds'])
            columns={c['name']:c for c in generate_data_dictionary(sample,'attributes')['columns']}
            self.assertEqual(columns['MEASUREMENT']['type'],'float');self.assertEqual(columns['MEASUREMENT']['numNullValues'],1)
            self.assertEqual(columns['MEASUREMENT']['min'],table.column('MEASUREMENT')[0].as_py())
            self.assertEqual(columns['geometry']['type'],'string')
