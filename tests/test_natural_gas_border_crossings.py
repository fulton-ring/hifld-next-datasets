"""Small explicitly synthetic behavior fixtures; optional COMPLETE retained real input."""
import copy
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import natural_gas_border_crossings as source


def receipt(url,body):
    return {'url':url,'final_url':url,'status':200,'observed_at':'2026-10-19T00:00:00+00:00','bytes':len(body),'sha256':source.digest(body),'complete_body':True,'fixture':'EXPLICIT SYNTHETIC; no source or contract approval'}


def synthetic_captures():
    """Two structural records, not an approved or complete real dataset."""
    geometry={'name':'SHAPE','type':'esriFieldTypeGeometry','alias':'SHAPE','domain':None}
    meta={'id':2,'name':'Natural Gas Pipeline','geometryType':'esriGeometryPoint','sourceSpatialReference':{'wkid':4326,'latestWkid':4326},'copyrightText':'North American Cooperation on Energy Information (NACEI), August 2017','maxRecordCount':10000,'fields':source.FIELD_DEFINITIONS[:1]+[geometry]+source.FIELD_DEFINITIONS[1:]}
    features=[]
    for oid,country in [(1,'Canada'),(2,'United States')]:
        attrs={f['name']:(' 'if f['type']=='esriFieldTypeString'else 1)for f in source.FIELD_DEFINITIONS}
        attrs.update(OBJECTID=oid,Country=country,Pipeline=' Original literal ',ZipCode='00123',Period=2017,Vol_MMcfd='215.066',Owner=None)
        features.append({'attributes':attrs,'geometry':{'x':-100.00000000000001,'y':49.00000000000001}})
    values={'authority_catalog':source.BASE+' e313db89-4219-4a5e-a543-772a86068710 Natural Resources Canada','authority_program':'North American Energy Information',
        'metadata_before':meta,'metadata_after':copy.deepcopy(meta),'ids_before':{'objectIdFieldName':'OBJECTID','objectIds':[1,2]},'ids_after':{'objectIdFieldName':'OBJECTID','objectIds':[1,2]},'count_before':{'count':2},'count_after':{'count':2},
        'page_00000':{'geometryType':'esriGeometryPoint','spatialReference':{'wkid':4326,'latestWkid':4326},'fields':[{k:v for k,v in f.items()if k!='domain'}for f in source.FIELD_DEFINITIONS],'features':features}}
    urls=source.request_plan([1,2]);captures={}
    for name,value in values.items():
        body=value.encode()if isinstance(value,str)else source.encode(value);captures[name]=(body,receipt(urls[name],body))
    return captures


def mutate(captures,name,change):
    value=source.strict_json(captures[name][0]);change(value);body=source.encode(value);captures[name]=(body,receipt(captures[name][1]['url'],body))


class NaturalGasCrossingsBehavior(unittest.TestCase):
    def setUp(self):
        self.pins=patch.object(source,'PINS',{});self.pins.start();self.addCleanup(self.pins.stop)

    def test_structural_native_fields_ids_whitespace_no_US_filter(self):
        table,audit=source.inspect_captures(synthetic_captures())
        self.assertEqual(table.num_rows,2);self.assertEqual(table['OBJECTID'].to_pylist(),[1,2]);self.assertEqual(table['ZipCode'].to_pylist(),['00123']*2)
        self.assertEqual(table['Pipeline'][0].as_py(),' Original literal ');self.assertIsNone(table['Owner'][0].as_py());self.assertEqual(table['Vol_MMcfd'][0].as_py(),'215.066')
        self.assertEqual(table.schema.field('Period').type,pa.int32());self.assertEqual(table.schema.field('NumPipes').type,pa.float64());self.assertEqual(audit['country_counts'],{'Canada':1,'United States':1})

    def test_integer_signed32_bounds_reject_bool_float_overflow(self):
        field=next(f for f in source.FIELD_DEFINITIONS if f['name']=='Period')
        for value in (-2**31,2**31-1):self.assertEqual(source.physical_value(field,value),value)
        for value in (True,1.0,-2**31-1,2**31):
            with self.subTest(value=value),self.assertRaises(ValueError):source.physical_value(field,value)

    def test_native_gpkg_sql32_and_parquet_nullability_values(self):
        table,_=source.inspect_captures(synthetic_captures())
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp)
            for part in ('geopackage','geoparquet','candidate'):(root/part).mkdir()
            path=root/f'geoparquet/{source.SLUG}.parquet';pq.write_table(source.expected_parquet(table),path,compression='zstd');(root/'candidate/typed-source.parquet').write_bytes(path.read_bytes());source.write_gpkg(table,root/f'geopackage/{source.SLUG}.gpkg');source.verify_outputs(root,table)
            with sqlite3.connect(root/f'geopackage/{source.SLUG}.gpkg')as db:types={r[1]:r[2]for r in db.execute('PRAGMA table_info('+source.LAYER+')')}
            self.assertEqual(types['Period'],'MEDIUMINT');self.assertEqual(types['NumPipes'],'REAL');self.assertEqual(types['Vol_MMcfd'],'TEXT')
            self.assertEqual(pq.read_table(path).schema.field('OBJECTID').nullable,False)

    def test_missing_duplicate_reordered_ids_and_truncated_page(self):
        for change in (lambda p:p['features'].pop(),lambda p:p['features'].reverse(),lambda p:p['features'][1]['attributes'].update(OBJECTID=1),lambda p:p.update(exceededTransferLimit=True)):
            captures=synthetic_captures();mutate(captures,'page_00000',change)
            with self.assertRaises(ValueError):source.inspect_captures(captures)

    def test_type_sensitive_metadata_and_between_read_drift(self):
        for change in (lambda p:p.update(id=2.0),lambda p:p['fields'][0].update(nullable=0),lambda p:p.update(maxRecordCount=True),lambda p:p.update(description='changed')):
            captures=synthetic_captures();mutate(captures,'metadata_after',change)
            with self.assertRaises(ValueError):source.inspect_captures(captures)

    def test_string_width_native_coordinate_and_renamed_column_drift(self):
        for change in (lambda p:p['features'][0]['attributes'].update(ZipCode='012345678'),lambda p:p['features'][0]['attributes'].pop('Pipeline'),lambda p:p['features'][0]['geometry'].update(x=200),lambda p:p['features'][0]['attributes'].update(NumPipes=True)):
            captures=synthetic_captures();mutate(captures,'page_00000',change)
            with self.assertRaises(ValueError):source.inspect_captures(captures)

    def test_count_and_authority_must_match_full_layer(self):
        captures=synthetic_captures();mutate(captures,'count_after',lambda p:p.update(count=1))
        with self.assertRaises(ValueError):source.inspect_captures(captures)
        captures=synthetic_captures();body=b'Not delegated';captures['authority_catalog']=(body,receipt(source.AUTHORITY,body))
        with self.assertRaises(ValueError):source.inspect_captures(captures)

    def test_receipt_hash_length_bool_and_timezone_rejected(self):
        body=b'{}';r=receipt('https://example.test',body)
        for changes in ({'bytes':True},{'sha256':'0'*64},{'observed_at':'2026-10-19T00:00:00'},{'status':200.0}):
            with self.assertRaises(ValueError):source.validate_capture('probe',body,dict(r,**changes),'https://example.test')

    def test_malformed_receipt_preserves_body_and_original_receipt_before_parse(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);inputs=root/'inputs';inputs.mkdir();storage=StagingStorageResource(use_local=True,local_dir=str(root/'stage'))
            (inputs/'authority_catalog.body').write_bytes(b'complete attempted body');(inputs/'authority_catalog.receipt.json').write_bytes(b'{bad-json')
            with self.assertRaises(ValueError):source.acquire(storage,inputs)
            data=[p.read_bytes()for p in (root/'stage').rglob('*')if p.is_file()]
            self.assertIn(b'complete attempted body',data);self.assertIn(b'{bad-json',data)

    def test_unreadable_receipt_preserves_body_before_open(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);inputs=root/'inputs';inputs.mkdir();stage=root/'stage';storage=StagingStorageResource(use_local=True,local_dir=str(stage))
            (inputs/'authority_catalog.body').write_bytes(b'complete body before missing receipt')
            with self.assertRaises(ValueError):source.acquire(storage,inputs)
            self.assertIn(b'complete body before missing receipt',[p.read_bytes()for p in stage.rglob('*')if p.is_file()])

    def test_FIFO_and_root_alias_fail_before_network(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);fifo=root/'fifo';os.mkfifo(fifo)
            with self.assertRaises(ValueError):source.bounded_input(fifo,1024)
            (root/'alias').symlink_to(root,target_is_directory=True)
            with patch.object(source,'fetch_one')as fetch:
                with self.assertRaises(ValueError):source.acquire(StagingStorageResource(use_local=True,local_dir=str(root/'alias')))
                fetch.assert_not_called()

    def test_partial_wire_retained_with_truthful_failure(self):
        class Partial(httpx.SyncByteStream):
            def __iter__(self):yield b'partial';raise httpx.ReadError('synthetic interruption')
        real=httpx.Client;transport=httpx.MockTransport(lambda _:httpx.Response(200,stream=Partial()))
        with tempfile.TemporaryDirectory()as tmp,patch.object(source.httpx,'Client',side_effect=lambda **kw:real(transport=transport,**kw)):
            storage=StagingStorageResource(use_local=True,local_dir=tmp)
            with self.assertRaises(source.NaturalGasCrossingsFetchError)as failure:source.fetch_one(storage,'https://example.test')
            wire=failure.exception.receipt['wire_evidence'];self.assertEqual((Path(tmp)/wire['key']).read_bytes(),b'partial');self.assertEqual(wire['bytes'],7);self.assertFalse(failure.exception.receipt['complete_body'])

    def test_permanent_observed_loss_survives_other_witness_absence(self):
        with tempfile.TemporaryDirectory()as tmp:
            storage=StagingStorageResource(use_local=True,local_dir=tmp);source.record_observed_loss(storage,'v0-test','0'*64,'synthetic missing witness')
            with self.assertRaisesRegex(ValueError,'permanent_observed'):source.refuse_observed_loss(storage,'v0-test')

    def test_empty_partial_incident_directory_alone_blocks_reconstruction(self):
        with tempfile.TemporaryDirectory()as tmp:
            storage=StagingStorageResource(use_local=True,local_dir=tmp)
            directory=Path(tmp)/source.incident_directory('v0-test');directory.mkdir(parents=True)
            self.assertEqual(list(directory.iterdir()),[])
            with self.assertRaisesRegex(ValueError,'permanent_observed'):source.refuse_observed_loss(storage,'v0-test')

    def test_final_ready_truthful_and_lying_short_writes_refused(self):
        original=Path.write_bytes
        for lie in (False,True):
            with self.subTest(lying_count=lie),tempfile.TemporaryDirectory()as tmp:
                path=Path(tmp)/'ready.json';body=b'{"approved":false,"complete":true}\n'
                def short_write(target,value):
                    actual=original(target,value[:17]);return len(value)if lie else actual
                with patch.object(Path,'write_bytes',short_write):
                    with self.assertRaisesRegex(ValueError,'private_ready_incomplete_write'):source.checked_ready_write(path,body)
                self.assertEqual(path.read_bytes(),body[:17])

    def test_complete_private_inventory_and_ready_are_bound(self):
        captures=synthetic_captures();table,audit=source.inspect_captures(captures);identity={'fixture':'explicit synthetic'}
        with tempfile.TemporaryDirectory()as tmp,patch.object(source,'source_payloads',return_value={'metadata/source.json':b'original'}):
            root=Path(tmp)
            for part in ('metadata','candidate','geopackage','geoparquet'):(root/part).mkdir()
            (root/'metadata/source.json').write_bytes(b'original')
            path=root/f'geoparquet/{source.SLUG}.parquet';pq.write_table(source.expected_parquet(table),path,compression='zstd');(root/'candidate/typed-source.parquet').write_bytes(path.read_bytes());source.write_gpkg(table,root/f'geopackage/{source.SLUG}.gpkg')
            ready={'version':'v0-test','approved':False,'processing_fingerprint':source.digest(source.encode(identity)),'capture_fingerprint':'0'*64,'objects':{str(p.relative_to(root)):source.sha256_file(p)for p in root.rglob('*')if p.is_file()}}
            ready_path=root/source.READY;source.checked_ready_write(ready_path,source.encode(ready));source.verify_private(root,'v0-test','0'*64,captures,identity,audit,{},table)
            (root/'metadata/source.json').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError,'private_source_object_drift'):source.verify_private(root,'v0-test','0'*64,captures,identity,audit,{},table)
            (root/'metadata/source.json').write_bytes(b'original');ready['extra']=True;ready_path.write_bytes(source.encode(ready))
            with self.assertRaisesRegex(ValueError,'private_ready_or_inventory_drift'):source.verify_private(root,'v0-test','0'*64,captures,identity,audit,{},table)


class CompleteRetainedNaturalGasInput(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('HIFLD_NATURAL_GAS_CROSSINGS_CAPTURE_DIR'),'explicit complete real captured input opt-in')
    def test_all99_original24_attributes_and_native_points(self):
        root=Path(os.environ['HIFLD_NATURAL_GAS_CROSSINGS_CAPTURE_DIR']);captures={p.stem:(p.read_bytes(),json.loads(p.with_name(p.stem+'.receipt.json').read_bytes()))for p in root.glob('*.body')}
        table,audit=source.inspect_captures(captures);self.assertEqual(table.num_rows,99);self.assertEqual(len(table.column_names),25);self.assertEqual(audit['country_counts'],{'Canada':28,'Mexico':20,'United States':51})
        original=source.strict_json(captures['page_00000'][0])['features']
        for row,feature in zip(table.to_pylist(),original):
            for field,value in feature['attributes'].items():self.assertEqual(row[field],value)
            self.assertEqual(row['geometry'],__import__('struct').pack('<BIdd',1,1,feature['geometry']['x'],feature['geometry']['y']))
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp)
            for part in ('geopackage','geoparquet','candidate'):(root/part).mkdir()
            path=root/f'geoparquet/{source.SLUG}.parquet';pq.write_table(source.expected_parquet(table),path,compression='zstd');(root/'candidate/typed-source.parquet').write_bytes(path.read_bytes());source.write_gpkg(table,root/f'geopackage/{source.SLUG}.gpkg');source.verify_outputs(root,table)



class RegistrationTests(unittest.TestCase):
    def test_explicit_registration_and_stopped_draft_refresh(self):
        from dagster import DefaultScheduleStatus
        from dagster_hifld.definitions import defs
        from dagster_hifld.assets.natural_gas_border_crossings import natural_gas_crossings_draft_schedule
        from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
        keys={tuple(key.path)for key in defs.resolve_asset_graph().get_all_asset_keys()}
        for key in [('ingest',source.SLUG,source.SLUG),('publish','catalog'),('publish','verify'),('publish','promote')]:self.assertIn(key,keys)
        self.assertEqual(sum(any(tuple(key.path)==('ingest',source.SLUG,source.SLUG)for key in asset.keys)for asset in hifld_ingest_assets),1)
        self.assertIsNotNone(defs.resolve_job_def('nacei_natural_gas_border_crossings_draft_refresh'))
        self.assertEqual(natural_gas_crossings_draft_schedule.default_status,DefaultScheduleStatus.STOPPED)

if __name__=='__main__':unittest.main()
