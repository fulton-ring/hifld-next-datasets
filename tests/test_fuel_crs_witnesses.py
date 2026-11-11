"""Native CRS/geometry witnesses must reject drift before canonical staging."""
import copy
import json
import pathlib
import tempfile
import unittest

import pyarrow.parquet as pq
from pyproj import CRS
from shapely import from_wkb

from dagster_hifld.assets.fuel_stations import SLUG, build_candidate, inspect_capture, stage_fuel_capture
from dagster_hifld.resources import StagingStorageResource
from tests.test_fuel_stations import fixture, write


class FuelCrsWitnessTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=pathlib.Path(self.temp.name);self.capture=self.root/'capture';self.capture.mkdir()
        fixture(self.capture)
        self.staging=StagingStorageResource(local_dir=str(self.root/'staging'),use_local=True)

    def set_all_crs(self, crs):
        for name in ('bts_layer','bts_layer_after'):
            value=json.loads((self.capture/(name+'.body')).read_bytes())
            value['spatialReference']=copy.deepcopy(crs)
            value['extent']['spatialReference']=copy.deepcopy(crs)
            write(self.capture,name,value)
        value=json.loads((self.capture/'page-000.body').read_bytes())
        value['spatialReference']=copy.deepcopy(crs)
        write(self.capture,'page-000',value)

    def test_explicit_native_and_legacy_aliases_preserve_coordinates(self):
        cases=[{'wkid':102100,'latestWkid':3857},{'wkid':3857,'latestWkid':3857},
            {'wkid':102100},{'wkid':3857}]
        for index,crs in enumerate(cases):
            fixture(self.capture);self.set_all_crs(crs)
            target=self.root/f'candidate-{index}'
            with self.subTest(crs=crs):
                audit=build_candidate(self.capture,target)
                table=pq.read_table(target/'candidate.parquet')
                metadata=json.loads(table.schema.metadata[b'geo'])
                self.assertEqual(CRS.from_user_input(metadata['columns']['geometry']['crs']),CRS.from_epsg(3857))
                self.assertEqual(audit['crs'],crs)
                self.assertEqual(tuple(from_wkb(table['geometry'][0].as_py()).coords)[0],(-100.123456789,200.25))

    def test_four_coordinator_reproductions_quarantine_before_candidate_and_preserve_prior(self):
        previous=self.root/'valid-prior';build_candidate(self.capture,previous)
        saved=(previous/'candidate.parquet').read_bytes()
        for case in ('contradictory_aliases','unexpected_4326','after_crs_drift','after_geometry_drift'):
            fixture(self.capture)
            if case=='contradictory_aliases':self.set_all_crs({'wkid':4326,'latestWkid':3857})
            elif case=='unexpected_4326':self.set_all_crs({'wkid':4326,'latestWkid':4326})
            else:
                value=json.loads((self.capture/'bts_layer_after.body').read_bytes())
                if case=='after_crs_drift':
                    value['spatialReference']={'wkid':4326,'latestWkid':4326}
                    value['extent']['spatialReference']=value['spatialReference']
                else:value['geometryType']='esriGeometryPolygon'
                write(self.capture,'bts_layer_after',value)
            version='v0.0.0-'+case.replace('_','-')
            with self.subTest(case=case),self.assertRaises(ValueError):
                stage_fuel_capture(self.capture,self.staging,version)
            retained=list((self.root/'staging/quarantine'/SLUG/version).glob('*/raw'))
            self.assertEqual(len(retained),1)
            for path in self.capture.iterdir():
                self.assertEqual((retained[0]/path.name).read_bytes(),path.read_bytes())
            status=json.loads((retained[0].parent/'status.json').read_text())
            self.assertEqual(status['state'],'blocked');self.assertTrue(status['raw_preserved'])
            self.assertFalse((self.root/'staging'/SLUG).exists())
            self.assertEqual((previous/'candidate.parquet').read_bytes(),saved)

    def test_every_native_crs_location_rejects_unknown_wkt_vertical_and_noninteger_codes(self):
        invalid=[{'wkid':4326},{'wkid':102100,'latestWkid':4326},{'wkid':3857,'wkt':'EPSG:4326'},
            {'wkid':3857,'vcsWkid':5703},{'wkid':'3857'},{'wkid':3857,'latestWkid':3857.0},
            {'wkid':3857,'latestWkid':True},{'latestWkid':3857},None]
        for crs in invalid:
            for name,field in [('bts_layer','spatialReference'),('bts_layer','extent'),
                    ('bts_layer_after','spatialReference'),('bts_layer_after','extent'),('page-000','spatialReference')]:
                fixture(self.capture)
                value=json.loads((self.capture/(name+'.body')).read_bytes())
                if field=='extent':value['extent']['spatialReference']=crs
                else:value[field]=crs
                write(self.capture,name,value)
                with self.subTest(crs=crs,name=name,field=field),self.assertRaises(ValueError):inspect_capture(self.capture)

    def test_stable_before_after_service_geometry_id_dimensions_and_extent_metadata(self):
        changes={'id':1,'name':'Different feed','type':'Table','serviceItemId':'other-item',
            'objectIdField':'id','uniqueIdField':{'name':'id','isSystemMaintained':True},
            'globalIdField':'GlobalID','hasZ':True,'hasM':True,
            'sourceSpatialReference':{'wkid':3857},'extent':{'xmin':-101,'ymin':200.25,'xmax':-100.123456789,
                'ymax':200.25,'spatialReference':{'wkid':102100,'latestWkid':3857}}}
        for field,value in changes.items():
            fixture(self.capture)
            layer=json.loads((self.capture/'bts_layer_after.body').read_bytes());layer[field]=value
            write(self.capture,'bts_layer_after',layer)
            with self.subTest(field=field),self.assertRaises(ValueError):inspect_capture(self.capture)

    def test_pages_reject_geometry_identifier_service_and_dimension_contradictions(self):
        changes={'geometryType':'esriGeometryPolygon','objectIdFieldName':'id','globalIdFieldName':'GlobalID',
            'uniqueIdField':{'name':'id','isSystemMaintained':True},'serviceItemId':'other-item',
            'hasZ':True,'hasM':True,'sourceSpatialReference':{'wkid':4326}}
        for field,value in changes.items():
            fixture(self.capture)
            page=json.loads((self.capture/'page-000.body').read_bytes());page[field]=value
            write(self.capture,'page-000',page)
            with self.subTest(field=field),self.assertRaises(ValueError):inspect_capture(self.capture)


if __name__=='__main__':unittest.main()
