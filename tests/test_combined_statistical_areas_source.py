"""Draft guard tests. Authentic complete materializations are recorded separately."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import pyarrow as pa
from dagster_hifld.sources import census_latest
from dagster_hifld.sources import combined_statistical_areas as c
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.assets.combined_statistical_areas import census_combined_statistical_areas,csa_source_integrity_schedule
from dagster_hifld.assets.supported_assets import SUPPORTED_DATASET_FILE_PAIRS


def _release(vintage=2026):
    source_file=f'tlgdb_{vintage}_a_us_nationgeo.gdb.zip'
    directory=f'https://www2.census.gov/geo/tiger/TGRGDB{vintage % 100:02d}/'
    return census_latest.GeodatabaseRelease(
        vintage=vintage,product='nationgeo',source_file=source_file,
        source_url=directory+source_file,directory_url=directory,
        listing_sha256='',listing_bytes=0,
        latest_checked_at='2026-11-15T00:00:00+00:00',annual_listing_sha256='',
    )


class CSASourceTests(unittest.TestCase):
    def receipt(self,body=b'x',release=None):
        release=release or _release()
        return {'url':release.source_url,'final_url':release.source_url,'status':200,'bytes':len(body),'sha256':c.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00Z'}
    def test_registration_and_disabled_schedule(self):
        self.assertIn((c.SLUG,c.SLUG),SUPPORTED_DATASET_FILE_PAIRS)
        self.assertEqual(csa_source_integrity_schedule.default_status.value,'STOPPED')
    def test_cached_inventory_accepts_only_exact_catalog_provenance_snapshots(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            expected={'candidate/typed-source.parquet','geopackage/csa.gpkg','raw/source_capture.json',
                'raw/retrieval.json','metadata/source_manifest.json','metadata/processing_identity.json',
                'metadata/csa_source_audit.json','metadata/baseline_comparison.json'}
            for relative in expected:
                path=root/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'pinned')
            manifest=c.encode({'approved':False,'schema_version':1})
            (root/'metadata/source_manifest.json').write_bytes(manifest)
            snapshot=f'metadata/source/version_{c.digest(manifest)}_source_manifest.json'
            downstream={
                'metadata/resolved_provenance.json':c.encode({'schema_version':1}),
                'metadata/data_dictionary.json':c.encode({'schema_version':1}),
                'metadata/quality_manifest.json':c.encode({'quality_check_passed':True}),
                'metadata/source/source_manifest.json':manifest,
                snapshot:manifest,
            }
            for relative,body in downstream.items():
                path=root/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(body)
            c.validate_cached_candidate_inventory(root,expected)
            (root/snapshot).write_bytes(b'changed authored snapshot')
            with self.assertRaisesRegex(ValueError,'cached_authored_source_manifest_snapshot_drift'):
                c.validate_cached_candidate_inventory(root,expected)
            (root/snapshot).write_bytes(manifest)
            (root/'metadata/unrecognized_catalog_output.json').write_bytes(b'no')
            with self.assertRaisesRegex(ValueError,'immutable_candidate_inventory_conflict'):
                c.validate_cached_candidate_inventory(root,expected)
    def test_identity_adapter_uses_shared_scheme_and_records_gdal(self):
        identity={'code_identity_scheme':'sha256-packaged-dagster-hifld-v1','working_tree_status':'unknown',
            'code_commit':'packaged-sha256:'+'0'*64,'files':{},'installed_distributions':[],
            'python':'3.12','pyarrow':'23','proj':'9','geos':'3'}
        with patch.object(c,'packaged_processing_identity',return_value=identity):
            actual=c.processing_identity()
        self.assertEqual(actual['code_identity_scheme'],'sha256-packaged-dagster-hifld-v1')
        self.assertEqual(actual['working_tree_status'],'unknown')
        self.assertEqual(actual['gdal'],c.pyogrio.__gdal_version_string__)
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
                with patch.object(c,'acquire',return_value=(b'x',receipt,_release())),patch.object(c,'processing_identity',return_value={'code_commit':'fixture','code_identity_scheme':'fixture','working_tree_status':'clean','files':{}}),patch.object(c,'validate_construction_code_identity'),self.assertRaises(ValueError):
                    c.stage_csa_candidate(storage)
                self.assertFalse((Path(tmp)/c.SLUG).exists());self.assertEqual(prior.read_bytes(),b'keep')
            self.assertEqual(len(list((Path(tmp)/'_quarantine').rglob('reason.json'))),len(mutations))
    def test_changed_or_truncated_source_is_quarantined(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=tmp)
            with patch.object(c,'acquire',return_value=(b'PKbad',self.receipt(b'PKbad'),_release())),patch.object(c,'processing_identity',return_value={'code_commit':'fixture','code_identity_scheme':'fixture','working_tree_status':'clean','files':{}}),patch.object(c,'validate_construction_code_identity'):
                with self.assertRaises(ValueError):
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
            c.write_native_gpkg(original,gpkg)
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
            c.write_native_gpkg(changed,gpkg)
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
            c.write_native_gpkg(original,gpkg)
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

    def test_full_canonical_container_rejects_srs_headers_and_spatial_index_changes(self):
        geometry=c.shapely.to_wkb(c.shapely.MultiPolygon([c.shapely.Polygon([(0,0),(1,0),(0,1),(0,0)])]))
        props={'OBJECTID':[1],'GEOID':['001'],'GEOIDFQ':['x'],'NAMELSAD':['x'],'LSAD':['M0'],'ALAND':[1.0],'AWATER':[0.0],'INTPTLAT':['1'],'INTPTLON':['1'],'SHAPE':[geometry]}
        original=pa.Table.from_pydict(props,schema=pa.schema([pa.field(n,t) for n,t in zip(c.FIELDS,c.TYPES)]))
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'geopackage').mkdir();(root/'candidate').mkdir()
            gpkg=root/'geopackage'/f'{c.SLUG}.gpkg';parquet=root/'candidate/typed-source.parquet'
            c.write_native_gpkg(original,gpkg);c.pq.write_table(c.expected_parquet_table(original),parquet)
            saved=gpkg.read_bytes();c.reconcile_cached_native_outputs(root,original)
            cases=['definition','organization_coordsys_id','header_srs','header_flags','header_envelope','rtree']
            for case in cases:
                gpkg.write_bytes(saved)
                with c.sqlite3.connect(gpkg) as db:
                    if case=='definition':db.execute('UPDATE gpkg_spatial_ref_sys SET definition=? WHERE srs_id=4269',(c.pyproj.CRS(4326).to_wkt(version='WKT1_GDAL'),))
                    elif case=='organization_coordsys_id':db.execute('UPDATE gpkg_spatial_ref_sys SET organization_coordsys_id=4326 WHERE srs_id=4269')
                    elif case=='rtree':db.execute(f'UPDATE "rtree_{c.LAYER}_SHAPE" SET minx=-999')
                    else:
                        # Incremental BLOB writes exercise physical header bytes
                        # without firing geometry/index-maintenance SQL triggers.
                        with db.blobopen(c.LAYER,'SHAPE',1) as blob:
                            if case=='header_srs':blob.seek(4);blob.write((4326).to_bytes(4,'little',signed=True))
                            elif case=='header_flags':blob.seek(3);flag=blob.read(1)[0];blob.seek(3);blob.write(bytes([flag^32]))
                            else:blob.seek(8);blob.write(bytes.fromhex('0000000000388f40'))
                with self.subTest(case=case),self.assertRaises(ValueError):
                    c.reconcile_cached_native_outputs(root,original)
            gpkg.write_bytes(saved);c.reconcile_cached_native_outputs(root,original)

    def test_source_schema_types_ids_completeness_and_geometry_guards(self):
        # Fixtures test guards only and cannot approve the real release.
        props={'OBJECTID':list(range(1,185)),'GEOID':[str(i).zfill(3) for i in range(184)],'GEOIDFQ':['x']*184,'NAMELSAD':['x']*184,'LSAD':['M0']*184,'ALAND':[1.0]*184,'AWATER':[0.0]*184,'INTPTLAT':['1']*184,'INTPTLON':['1']*184,'SHAPE':[c.shapely.to_wkb(c.shapely.MultiPolygon([c.shapely.Polygon([(0,0),(1,0),(0,1),(0,0)])]))]*184}
        table=pa.Table.from_pydict(props,schema=pa.schema([pa.field(n,t) for n,t in zip(c.FIELDS,c.TYPES)]))
        metadata={'geometry_type':'MultiPolygon','crs':'EPSG:4269','fid_column':'OBJECTID'}
        c.validate_table(table,metadata)
        out_of_range=table.slice(0,c.FEATURE_COUNT_RANGE[0]-1)
        for candidate in [out_of_range,table.drop(['OBJECTID']),table.append_column('invented',pa.array([1]*184)),table.set_column(5,'ALAND',pa.array([1]*184)),table.set_column(0,'OBJECTID',pa.array([1]*184)),table.set_column(9,'SHAPE',pa.array([None]*184,type=pa.binary()))]:
            with self.assertRaises(ValueError):c.validate_table(candidate,metadata)
        with self.assertRaises(ValueError):c.validate_table(table,{**metadata,'crs':'EPSG:4326'})


    def test_producer_oid_and_double_declarations_are_distinct_from_arrow(self):
        xml=(Path(__file__).parent/'fixtures/csa_native_layer_definition.xml').read_text()
        with patch.object(c.pyogrio,'read_arrow',return_value=({},pa.table({'FIELD_1':[xml]}))):
            declaration=c.read_native_definition(Path('/synthetic-unit-only.zip'),_release())
        self.assertEqual(declaration['fields']['OBJECTID']['Length'],'4')
        self.assertEqual(c.TYPES[0],pa.int64())
        for field,key,value in [('OBJECTID','Length','8'),('OBJECTID','IsNullable','true'),('OBJECTID','Required','false'),('OBJECTID','Editable','true'),('ALAND','FieldType','esriFieldTypeInteger')]:
            document=c.ET.fromstring(xml)
            item=next(x for x in document.findall('./GPFieldInfoExs/GPFieldInfoEx') if x.findtext('Name')==field)
            item.find(key).text=value
            changed=c.ET.tostring(document,encoding='unicode')
            with self.subTest(field=field,key=key),patch.object(c.pyogrio,'read_arrow',return_value=({},pa.table({'FIELD_1':[changed]}))),self.assertRaisesRegex(ValueError,'declaration_drift'):
                c.read_native_definition(Path('/synthetic-unit-only.zip'),_release())

class CSAReleaseDiscoveryTests(unittest.TestCase):
    def test_manifest_uses_resolved_vintage_and_discovery_evidence(self):
        release=_release(2027)
        body=b'current official archive'
        receipt={'url':release.source_url,'final_url':release.source_url,'status':200,
            'bytes':len(body),'sha256':c.digest(body),'complete_body':True,
            'observed_at':'2026-11-15T00:00:00Z'}
        identity={'code_commit':'fixture','code_identity_scheme':'fixture','working_tree_status':'clean'}
        manifest=c.manifest_for(body,receipt,identity,release)
        self.assertEqual(manifest['source_url'],release.source_url)
        self.assertEqual(manifest['source_release'],release.as_metadata())
        self.assertIn('2027',manifest['title'])
        self.assertIn('2027',manifest['inputs'][0]['attribution'])

    def test_manifest_rejects_old_or_mismatched_receipt_url(self):
        release=_release(2027)
        body=b'current official archive'
        receipt={'url':release.source_url,'final_url':release.source_url,'status':200,
            'bytes':len(body),'sha256':c.digest(body),'complete_body':True,
            'observed_at':'2026-11-15T00:00:00Z'}
        identity={'code_commit':'fixture','code_identity_scheme':'fixture','working_tree_status':'clean'}
        old={**receipt,'url':'https://www2.census.gov/geo/tiger/TIGER2024/tl_2024_us_nationgeo.zip'}
        with self.assertRaises(c.CSAQuarantineError):
            c.manifest_for(body,old,identity,release)
        with self.assertRaises(c.CSAQuarantineError):
            c.manifest_for(body,{**receipt,'final_url':release.source_url+'?changed=1'},identity,release)

    def test_acquire_replay_accepts_current_release_and_rejects_old_url(self):
        release=_release(2026)
        body=b'archive bytes'
        receipt={'url':release.source_url,'final_url':release.source_url,'status':200,
            'bytes':len(body),'sha256':c.digest(body),'complete_body':True,
            'observed_at':'2026-11-15T00:00:00Z'}
        with tempfile.TemporaryDirectory() as tmp:
            storage=StagingStorageResource(local_dir=str(Path(tmp)/'staging'))
            archive=Path(tmp)/'archive.zip';archive.write_bytes(body)
            receipt_path=Path(tmp)/'receipt.json';receipt_path.write_text(json.dumps(receipt))
            captured,captured_receipt,resolved=c.acquire(storage,archive,receipt_path)
            self.assertEqual(captured,body)
            self.assertEqual(captured_receipt,receipt)
            self.assertEqual(resolved.vintage,2026)
            old_receipt={**receipt,'url':'https://www2.census.gov/geo/tiger/TIGER2024/tl_2024_us_nationgeo.zip'}
            receipt_path.write_text(json.dumps(old_receipt))
            with self.assertRaisesRegex(c.CSAQuarantineError,'source_url_not_current_tgrgdb'):
                c.acquire(storage,archive,receipt_path)

    def test_release_from_receipt_rejects_wrong_product(self):
        with self.assertRaisesRegex(ValueError,'source_url_not_current_tgrgdb'):
            census_latest.geodatabase_release_from_receipt(
                {'url':'https://www2.census.gov/geo/tiger/TGRGDB26/tlgdb_2026_us_addr.gdb.zip'},
                'nationgeo')


if __name__=='__main__':unittest.main()
