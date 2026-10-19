"""Small explicit synthetic EPA EEZ polygons; complete capture opt-in only."""
import copy
import os
import struct
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from dagster import AssetKey,DefaultScheduleStatus
from dagster_hifld.sources import epa_eez as f
from dagster_hifld.assets.epa_eez import epa_eez_draft_schedule,epa_eez_full_quality
from dagster_hifld.definitions import defs


def page():
    shell=[[0.,0.],[0.,10.],[10.,10.],[10.,0.],[0.,0.]];hole=[[2.,2.],[8.,2.],[8.,8.],[2.,8.],[2.,2.]];island=[[20.,0.],[20.,1.],[21.,1.],[21.,0.],[20.,0.]]
    return {'displayFieldName':'EPAREGION','geometryType':'esriGeometryPolygon','spatialReference':{'wkid':4269,'latestWkid':4269},'fields':copy.deepcopy(f.PAGE_FIELDS),'features':[{'attributes':{'FIRST_CONTACT':' 001 literal contact ','Shape_Length':1.25,'Shape_Area':2.5,'OBJECTID':1,'EPAREGION':'Region 1'},'geometry':{'rings':[shell,hole,island]}},{'attributes':{'FIRST_CONTACT':None,'Shape_Length':None,'Shape_Area':0.0,'OBJECTID':2,'EPAREGION':None},'geometry':None}]}


def synthetic_captures():
    p=page();prototype=p['features'][0];p['features']=[]
    for oid,name in f.REGION_IDENTITIES.items():
        feature=copy.deepcopy(prototype);feature['attributes'].update(OBJECTID=oid,EPAREGION=name);p['features'].append(feature)
    layer={'id':4,'name':'EPA Region Boundaries with Exclusive Economic Zone','geometryType':'esriGeometryPolygon','fields':f.FIELD_DEFINITIONS,'sourceSpatialReference':f.SOURCE_SPATIAL_REFERENCE,'extent':{'spatialReference':f.DISPLAY_SPATIAL_REFERENCE},'description':'2011 TIGER/Line and NOAA U.S. Maritime Boundaries, Office of Environmental Information'}
    values={'source_service':{'layers':[{'id':4,'name':layer['name']}]},'layer_before':layer,'layer_after':layer,'license_epa':{'explicit':'SYNTHETIC fixture, not real rights evidence'},'count_before':{'count':10},'count_after':{'count':10},'ids_before':{'objectIdFieldName':'OBJECTID','objectIds':list(range(1,11))},'ids_after':{'objectIdFieldName':'OBJECTID','objectIds':list(range(1,11))},'page_00000':p}
    captures={}
    for name,value in values.items():
        body=f.encode(value);captures[name]=(body,{'url':f.URLS[name],'final_url':f.URLS[name],'status':200,'bytes':len(body),'sha256':f.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00+00:00'})
    return captures,f.digest(f.encode(p['features']))


class EpaEezTests(unittest.TestCase):
    def test_all_original_attributes_shape_name_native_types_nulls_and_rings(self):
        table,a=f.decode_page(page());self.assertEqual(table.column_names,[d['name']for d in f.FIELD_DEFINITIONS]);self.assertEqual(table['OBJECTID'].type,pa.int64());self.assertEqual(table['Shape_Length'].type,pa.float64());self.assertEqual(table['FIRST_CONTACT'][0].as_py(),' 001 literal contact ');self.assertEqual(table['Shape'].null_count,1);self.assertEqual(a,{'rings':3,'vertices':15})
        geometry=shapely.from_wkb(table['Shape'][0].as_py());self.assertEqual(geometry.geom_type,'MultiPolygon');self.assertEqual(len(shapely.get_parts(geometry)),2)
        for decl in f.FIELD_DEFINITIONS:self.assertEqual(table.schema.field(decl['name']).metadata,{b'original_esri_field':f.encode(decl)})
    def test_exact_ordered_ring_XY_bytes_preserved_and_no_repair(self):
        p=page();table,_=f.decode_page(p);geometry=shapely.from_wkb(table['Shape'][0].as_py());self.assertEqual(f.verify_original_rings(p['features'][0]['geometry'],geometry),(3,15))
        raw=copy.deepcopy(p['features'][0]['geometry']);raw['rings'][0][1][0]=0.0000000000001
        with self.assertRaises(ValueError):f.verify_original_rings(raw,geometry)
    def test_deterministic_native_4269_parquet_and_shape_primary_column(self):
        table,_=f.decode_page(page())
        with tempfile.TemporaryDirectory()as d:
            root=Path(d);(root/'geoparquet').mkdir();path=root/'geoparquet'/f'{f.SLUG}.parquet';pq.write_table(f.parquet_table(table),path,compression='zstd');first=path.read_bytes();f.validate_output(root,table);pq.write_table(f.parquet_table(table),path,compression='zstd');self.assertEqual(first,path.read_bytes());geo=f.strict_json(pq.read_schema(path).metadata[b'geo']);self.assertEqual(geo['primary_column'],'Shape');self.assertEqual(geo['columns']['Shape']['crs']['id']['code'],4269)
    def test_missing_extra_duplicate_id_physical_types_and_text_width(self):
        for mutate in[lambda p:p['features'][0]['attributes'].pop('FIRST_CONTACT'),lambda p:p['features'][0]['attributes'].update(NEW=1),lambda p:p['features'][0]['attributes'].update(OBJECTID=True),lambda p:p['features'][0]['attributes'].update(OBJECTID=1.0),lambda p:p['features'][1]['attributes'].update(OBJECTID=1),lambda p:p['features'][0]['attributes'].update(Shape_Area=True),lambda p:p['features'][0]['attributes'].update(Shape_Area=2**53+1),lambda p:p['features'][0]['attributes'].update(FIRST_CONTACT='x'*501)]:
            p=page();mutate(p)
            with self.assertRaises((ValueError,RuntimeError)):f.decode_page(p)
    def test_transfer_metadata_CRSkinds_curves_and_dimension_changes_reject(self):
        for mutate in[lambda p:p.update(exceededTransferLimit=True),lambda p:p.update(exceededTransferLimit=0),lambda p:p['spatialReference'].update(wkid=4269.0),lambda p:p['fields'][0].update(length=500.0),lambda p:p['features'][0]['geometry'].update(curveRings=[]),lambda p:p['features'][0]['geometry']['rings'][0][0].append(0.)]:
            p=page();mutate(p)
            with self.assertRaises((ValueError,RuntimeError)):f.decode_page(p)
    def test_large_feature_before_header_keeps_driver_detection_and_every_vertex(self):
        p=page();n=3000
        ring=[[0.,i/1000]for i in range(n)]+[[i/1000,n/1000]for i in range(n)]+[[n/1000,(n-i)/1000]for i in range(n)]+[[(n-i)/1000,0.]for i in range(n)]+[[0.,0.]]
        p['features'][0]['geometry']={'rings':[ring]};p=f.strict_json(f.encode(p))
        table,a=f.decode_page(p);self.assertEqual(a,{'rings':1,'vertices':12001});self.assertEqual(f.verify_original_rings(p['features'][0]['geometry'],shapely.from_wkb(table['Shape'][0].as_py())),(1,12001))
    def test_complete_roster_metadata_numeric_kinds_and_geometry_drift(self):
        captures,sha=synthetic_captures()
        with mock.patch.object(f,'SOURCE_RECORD_SHA256',sha):
            table,a=f.inspect_captures(captures);self.assertEqual(table.num_rows,10);self.assertEqual(a['source_CRS'],4269)
            for name,change in [('layer_before',lambda x:x['fields'][3].update(nullable=0)),('count_after',lambda x:x.update(count=10.0)),('ids_after',lambda x:x['objectIds'].__setitem__(0,True)),('page_00000',lambda x:x['features'][0]['geometry']['rings'][0][1].__setitem__(0,0.1))]:
                broken=copy.deepcopy(captures);body=f.strict_json(broken[name][0]);change(body);encoded=f.encode(body);receipt={**broken[name][1],'bytes':len(encoded),'sha256':f.digest(encoded)};broken[name]=(encoded,receipt)
                with self.assertRaises(ValueError):f.inspect_captures(broken)
    def test_both_metadata_copies_cannot_hide_bool_numeric_schema_drift(self):
        for value in (0,0.0):
            captures,sha=synthetic_captures()
            for name in ('layer_before','layer_after'):
                body=f.strict_json(captures[name][0]);body['fields'][3]['nullable']=value;encoded=f.encode(body);captures[name]=(encoded,{**captures[name][1],'bytes':len(encoded),'sha256':f.digest(encoded)})
            with mock.patch.object(f,'SOURCE_RECORD_SHA256',sha),self.assertRaises(ValueError):f.inspect_captures(captures)
    def test_original_id_census_strict_and_complete(self):
        for ids in[[1]*10,[True,*range(2,11)],[1.0,*range(2,11)],list(range(1,10))]:
            with self.assertRaises(ValueError):f.id_roster(f.encode({'objectIdFieldName':'OBJECTID','objectIds':ids}))
    def test_normal_registration_blocking_quality_and_stopped_monthly(self):
        job=defs.resolve_job_def('epa_eez_draft_refresh');self.assertEqual(job.asset_layer.executable_asset_keys,{AssetKey(['ingest',f.SLUG,f.SLUG])});self.assertTrue(next(iter(epa_eez_full_quality.check_specs)).blocking);self.assertEqual(epa_eez_draft_schedule.default_status,DefaultScheduleStatus.STOPPED)
    @unittest.skipUnless(os.environ.get('HIFLD_EPA_EEZ_REAL_CAPTURE_DIR'),'complete real EEZ capture external opt-in')
    def test_complete_real_EPA_EEZ_capture(self):
        root=Path(os.environ['HIFLD_EPA_EEZ_REAL_CAPTURE_DIR']);captures={n:((root/(n+'.body')).read_bytes(),f.strict_json((root/(n+'.receipt.json')).read_bytes()))for n in f.URLS};table,a=f.inspect_captures(captures);self.assertEqual(table.num_rows,10);self.assertEqual(table.column_names,[d['name']for d in f.FIELD_DEFINITIONS]);self.assertGreater(a['vertices'],100000)
