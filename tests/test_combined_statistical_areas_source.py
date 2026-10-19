"""Draft guard tests. Authentic complete materializations are recorded separately."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import pyarrow as pa
from dagster_hifld.sources import combined_statistical_areas as c
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.assets.combined_statistical_areas import census_combined_statistical_areas,csa_source_integrity_schedule
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILE_PAIRS

class CSASourceTests(unittest.TestCase):
    def receipt(self,body=b'x'):
        return {'url':c.URL,'final_url':c.URL,'status':200,'bytes':len(body),'sha256':c.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00Z'}
    def test_registration_and_disabled_schedule(self):
        self.assertIn((c.SLUG,c.SLUG),SUPPORTED_DATASET_FILE_PAIRS)
        self.assertEqual(csa_source_integrity_schedule.default_status.value,'STOPPED')
    def test_storage_refused_before_acquire(self):
        for storage in [PublishedStorageResource(local_dir='/tmp/never-csa-published'),StagingStorageResource(bucket='never')]:
            with patch.object(c,'acquire') as acquire,self.assertRaises(c.CSAQuarantineError):
                c.stage_csa_candidate(storage)
            acquire.assert_not_called()
    def test_duplicate_json_and_error_payload(self):
        for body in [b'{"x":1,"x":2}',b'{"error":{}}',b'[]',b'null']:
            with self.assertRaises(ValueError):c.strict_json(body)
    def test_provenance_preflight_all_invalid_receipts_no_candidate(self):
        mutations=[{'observed_at':'not-a-date'},{'observed_at':'2026-10-19T00:00:00'}, {'observed_at':True},
            {'url':'https://example.com/a'},{'final_url':'https://example.com/b'}, {'status':True},{'status':403},{'bytes':True},
            {'sha256':'0'*64},{'complete_body':False}]
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp)
            prior=Path(tmp)/'prior-accepted-fixture';prior.write_bytes(b'keep')
            for mutation in mutations:
                receipt=self.receipt();receipt.update(mutation)
                with patch.object(c,'acquire',return_value=(b'x',receipt)),patch.object(c,'processing_identity',return_value={'test':True}),self.assertRaises(ValueError):
                    c.stage_csa_candidate(storage)
                self.assertFalse((Path(tmp)/c.SLUG).exists());self.assertEqual(prior.read_bytes(),b'keep')
            self.assertEqual(len(list((Path(tmp)/'_quarantine').rglob('reason.json'))),len(mutations))
    def test_changed_or_truncated_source_is_quarantined(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp)
            with patch.object(c,'acquire',return_value=(b'PKbad',self.receipt(b'PKbad'))),patch.object(c,'processing_identity',return_value={}),self.assertRaisesRegex(ValueError,'changed_truncated'):
                c.stage_csa_candidate(storage)
            self.assertFalse((Path(tmp)/c.SLUG).exists())
            self.assertEqual(next((Path(tmp)/'_quarantine').rglob('response.body')).read_bytes(),b'PKbad')
    def test_cached_native_rederivation_rejects_repinned_payload_tampering(self):
        props={'OBJECTID':[1],'GEOID':['001'],'GEOIDFQ':['x'],'NAMELSAD':['x'],'LSAD':['M0'],'ALAND':[1.0],'AWATER':[0.0],'INTPTLAT':['1'],'INTPTLON':['1'],'SHAPE':[c.shapely.to_wkb(c.shapely.MultiPolygon([c.shapely.Polygon([(0,0),(1,0),(0,1),(0,0)])]))]}
        original=pa.Table.from_pydict(props,schema=pa.schema([pa.field(n,t) for n,t in zip(c.FIELDS,c.TYPES)]))
        original=c.expected_parquet_table(original)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'geopackage').mkdir();(root/'candidate').mkdir()
            gpkg=root/'geopackage'/f'{c.SLUG}.gpkg';parquet=root/'candidate/typed-source.parquet'
            c.pyogrio.write_arrow(original,gpkg,layer=c.LAYER,driver='GPKG',geometry_name='SHAPE',geometry_type='MultiPolygon',crs='EPSG:4269',layer_options={'FID':'_storage_fid','GEOMETRY_NAME':'SHAPE'})
            c.pq.write_table(original,parquet)
            c.reconcile_cached_native_outputs(root,original)
            for name,values in [('ALAND',[2.0]),('OBJECTID',[99]),('GEOID',['999']),('SHAPE',[c.shapely.to_wkb(c.shapely.MultiPolygon([c.shapely.Polygon([(0,0),(2,0),(0,1),(0,0)])]))])]:
                changed=original.set_column(original.column_names.index(name),name,pa.array(values,type=c.TYPES[c.FIELDS.index(name)]))
                c.pq.write_table(changed,parquet)
                with self.assertRaisesRegex(ValueError,'fresh_original'):
                    c.reconcile_cached_native_outputs(root,original)
            c.pq.write_table(original,parquet)
            gpkg.unlink()
            changed=original.set_column(3,'NAMELSAD',pa.array(['tampered']))
            c.pyogrio.write_arrow(changed,gpkg,layer=c.LAYER,driver='GPKG',geometry_name='SHAPE',geometry_type='MultiPolygon',crs='EPSG:4269',layer_options={'FID':'_storage_fid','GEOMETRY_NAME':'SHAPE'})
            with self.assertRaisesRegex(ValueError,'fresh_original'):
                c.reconcile_cached_native_outputs(root,original)

    def test_geometry_declaration_metadata_mutations_are_not_fidelity(self):
        geometry=c.shapely.to_wkb(c.shapely.MultiPolygon([c.shapely.Polygon([(0,0),(1,0),(0,1),(0,0)])]))
        props={'OBJECTID':[1],'GEOID':['001'],'GEOIDFQ':['x'],'NAMELSAD':['x'],'LSAD':['M0'],'ALAND':[1.0],'AWATER':[0.0],'INTPTLAT':['1'],'INTPTLON':['1'],'SHAPE':[geometry]}
        original=pa.Table.from_pydict(props,schema=pa.schema([pa.field(n,t) for n,t in zip(c.FIELDS,c.TYPES)]))
        expected=c.expected_parquet_table(original)
        mutations=[('crs',c.pyproj.CRS(3857).to_json_dict()),('encoding','WKT'),('geometry_types',['Polygon']),('bbox',[0,0,999,999])]
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'geopackage').mkdir();(root/'candidate').mkdir()
            gpkg=root/'geopackage'/f'{c.SLUG}.gpkg';parquet=root/'candidate/typed-source.parquet'
            c.pyogrio.write_arrow(original,gpkg,layer=c.LAYER,driver='GPKG',geometry_name='SHAPE',geometry_type='MultiPolygon',crs='EPSG:4269',layer_options={'FID':'_storage_fid','GEOMETRY_NAME':'SHAPE'})
            for name,value in mutations:
                meta=dict(expected.schema.metadata);geo=json.loads(meta[b'geo']);geo['columns']['SHAPE'][name]=value;meta[b'geo']=c.encode(geo)
                c.pq.write_table(expected.replace_schema_metadata(meta),parquet)
                with self.subTest(name=name),self.assertRaisesRegex(ValueError,'metadata_disagrees_with_fresh_original'):
                    c.reconcile_cached_native_outputs(root,original)
            for geo in [{}, {'version':'1.1.0','primary_column':'invented','columns':{'SHAPE':{}}}]:
                c.pq.write_table(expected.replace_schema_metadata({b'geo':c.encode(geo)}),parquet)
                with self.assertRaises(ValueError):c.reconcile_cached_native_outputs(root,original)
            c.pq.write_table(expected,parquet)
            with c.sqlite3.connect(gpkg) as db:db.execute('UPDATE gpkg_contents SET min_x=999')
            with self.assertRaisesRegex(ValueError,'geopackage_geometry_declarations'):
                c.reconcile_cached_native_outputs(root,original)

    def test_source_schema_types_ids_completeness_and_geometry_guards(self):
        # Fixtures test guards only and cannot approve the real release.
        props={'OBJECTID':list(range(1,185)),'GEOID':[str(i).zfill(3) for i in range(184)],'GEOIDFQ':['x']*184,'NAMELSAD':['x']*184,'LSAD':['M0']*184,'ALAND':[1.0]*184,'AWATER':[0.0]*184,'INTPTLAT':['1']*184,'INTPTLON':['1']*184,'SHAPE':[c.shapely.to_wkb(c.shapely.MultiPolygon([c.shapely.Polygon([(0,0),(1,0),(0,1),(0,0)])]))]*184}
        table=pa.Table.from_pydict(props,schema=pa.schema([pa.field(n,t) for n,t in zip(c.FIELDS,c.TYPES)]))
        metadata={'geometry_type':'MultiPolygon','crs':'EPSG:4269','fid_column':'OBJECTID'}
        c.validate_table(table,metadata)
        for candidate in [table.slice(0,183),table.drop(['OBJECTID']),table.append_column('invented',pa.array([1]*184)),table.set_column(5,'ALAND',pa.array([1]*184)),table.set_column(0,'OBJECTID',pa.array([1]*184)),table.set_column(9,'SHAPE',pa.array([None]*184,type=pa.binary()))]:
            with self.assertRaises(ValueError):c.validate_table(candidate,metadata)
        with self.assertRaises(ValueError):c.validate_table(table,{**metadata,'crs':'EPSG:4326'})

if __name__=='__main__':unittest.main()
