"""Complete preserved source is opt-in; bounded original DBF is always tested."""
from __future__ import annotations

import copy
import os
import tempfile
import unittest
from pathlib import Path
from decimal import Decimal
from datetime import date
import shapely
import pyarrow as pa
from unittest.mock import patch

from dagster import DefaultScheduleStatus, build_schedule_context
from dagster_hifld.assets.ferc_regions import ferc_historical_integrity_schedule, ferc_historical_preservation
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import ferc_regions as source
from dagster_hifld.sources.ferc_regions_spec import SPEC

FIXTURE=Path(__file__).parent/'fixtures/ferc_regions'
CAPTURES=os.environ.get('HIFLD_FERC_TEST_CAPTURE_DIR')


class FercBoundedTests(unittest.TestCase):
    def test_original_dbf_all_values_and_Date32_semantics(self):
        rows=source.dbf_rows((FIXTURE/'FERC_Regions.dbf').read_bytes())
        self.assertEqual(source.encode(rows),source.encode([{k:v for k,v in r.items()if k!='OGC_FID'}for r in SPEC['rows']]))
        self.assertEqual([r['ID']for r in rows],['1','2','3','4','5'])
        self.assertEqual([r['SOURCEDATE']for r in rows],['2010-06-28']*5)

    def test_dbf_declaration_deleted_partial_population_rejected(self):
        original=(FIXTURE/'FERC_Regions.dbf').read_bytes()
        mutations=[original[:-20],original[:4]+(4).to_bytes(4,'little')+original[8:]]
        changed=bytearray(original);changed[48]=2;mutations.append(bytes(changed))
        changed=bytearray(original);changed[int.from_bytes(original[8:10],'little')]=42;mutations.append(bytes(changed))
        for body in mutations:
            with self.subTest(sha=source.digest(body)),self.assertRaises(ValueError):source.dbf_rows(body)

    def test_receipt_types_and_duplicate_JSON_are_strict(self):
        body=b'context';pin=SPEC['inputs']['dhs_raw'];receipt={'requested_url':pin['url'],'observed_final_url':pin['url'],
            'status':200,'bytes':len(body),'sha256':source.digest(body),'complete_body':True,'observed_at':'2026-10-19T00:00:00Z'}
        self.assertEqual(source.validate_receipt('dhs_raw',body,receipt)['role'],'producer_publication')
        for name,value in [('status',200.0),('bytes',True),('complete_body',1),('sha256','0'*64),('observed_final_url','https://invalid.example/')]:
            bad={**receipt,name:value}
            with self.subTest(name=name),self.assertRaises(source.FercPreservationError):source.validate_receipt('dhs_raw',body,bad)
        with self.assertRaises(source.FercPreservationError):source.strict_json(b'{"status":200,"status":200}')
        self.assertNotEqual(source.encode({'nullable':False}),source.encode({'nullable':0}))

    def test_local_root_aliases_and_published_fail_before_write(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);outside=root/'outside';outside.mkdir();(outside/'prior').write_bytes(b'unchanged')
            (root/'link').symlink_to(outside,target_is_directory=True)
            for directory in [root/'link',root/'link'/'nested']:
                with self.subTest(directory=directory),self.assertRaises(ValueError):source.local_only(StagingStorageResource(local_dir=str(directory)))
            target=root/'published'
            with self.assertRaises(ValueError):source.local_only(PublishedStorageResource(local_dir=str(target)))
            self.assertFalse(target.exists());self.assertEqual(list(outside.iterdir()),[outside/'prior'])
            self.assertEqual((outside/'prior').read_bytes(),b'unchanged')

    def test_bounded_read_no_FIFO_symlink_hardlink_or_oversize(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);body=root/'body';body.write_bytes(b'original')
            link=root/'link';link.symlink_to(body)
            with self.assertRaises(OSError):source.bounded_read(link,8)
            with self.assertRaises(ValueError):source.bounded_read(body,7)
            os.link(body,root/'hard')
            with self.assertRaises(ValueError):source.bounded_read(body,8)
            self.assertEqual(body.read_bytes(),b'original')
            fifo=root/'fifo';os.mkfifo(fifo)
            with self.assertRaises(ValueError):source.bounded_read(fifo,8)

    def test_exact_decimal_literal_policy_preserves_all_values_and_rejects_rounding(self):
        body=(FIXTURE/'FERC_Regions.dbf').read_bytes()
        table=pa.table({'SHAPE__Are':[0.0]*5,'SHAPE__Len':[0.0]*5})
        exact=source.exact_numeric_table(table,body);literals=source.numeric_literals(body)
        for name in ['SHAPE__Are','SHAPE__Len']:
            self.assertEqual(exact[name].to_pylist(),[Decimal(row[name].strip())for row in literals])
            self.assertEqual(str(exact.schema.field(name).type),'decimal128(38, 15)')
        header=int.from_bytes(body[8:10],'little');offset=header+1+sum(field[2]for field in SPEC['dbf_fields'][:-2])
        for text in ['NaN','123456789012345678901234','0.12345678901234567890']:
            changed=bytearray(body);changed[offset:offset+24]=text.rjust(24).encode()
            with self.subTest(text=text),self.assertRaisesRegex(ValueError,'not_representable'):
                source.exact_numeric_table(table,bytes(changed))

    def test_small_synthetic_container_preserves_Date32_int32_exact_decimals_and_schema(self):
        # Only geometry is synthetic/tiny. This tests native format mechanics,
        # never substitutes for complete source/candidate materialization.
        rows=copy.deepcopy(SPEC['rows'])
        for i,row in enumerate(rows):
            row['SOURCEDATE']=date.fromisoformat(row['SOURCEDATE']);row['VAL_DATE']=date.fromisoformat(row['VAL_DATE'])
            row['wkb_geometry']=shapely.to_wkb(shapely.MultiPolygon([shapely.Polygon([(i,0),(i+0.1,0),(i,0.1),(i,0)])]))
        aliases={'int64':pa.int64(),'int32':pa.int32(),'string':pa.string(),'date32[day]':pa.date32(),'double':pa.float64(),'binary':pa.binary()}
        schema=pa.schema([pa.field(f['name'],aliases[f['type']],nullable=f['nullable'],metadata={b'GDAL:OGR:width':f['width'].encode()}if f['width']else None)for f in SPEC['arrow_fields']])
        table=source.exact_numeric_table(pa.Table.from_pylist(rows,schema=schema),(FIXTURE/'FERC_Regions.dbf').read_bytes())
        body=source.parquet_bytes(table)
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);(root/'geoparquet').mkdir();path=root/'geoparquet/ferc-regions.parquet';path.write_bytes(body)
            source.reconcile_outputs(root,table)
            actual=source.pq.read_table(path)
            self.assertEqual(str(actual.schema.field('SOURCEDATE').type),'date32[day]')
            self.assertEqual(str(actual.schema.field('OBJECTID').type),'int32')
            self.assertEqual(actual['SHAPE__Len'][0].as_py(),Decimal('13763641.201288700103760'))
            self.assertEqual(actual.schema.metadata[b'original_DBF_physical_schema'],source.encode(SPEC['dbf_fields']))
            changed=actual.set_column(actual.schema.get_field_index('SHAPE__Len'),'SHAPE__Len',actual['SHAPE__Len'].cast(pa.float64()))
            source.pq.write_table(changed,path)
            with self.assertRaisesRegex(ValueError,'native_parquet_full_schema'):source.reconcile_outputs(root,table)

    def test_stopped_integrity_has_real_op_config_and_no_autonomous_feed(self):
        schedule=ferc_historical_integrity_schedule
        self.assertEqual(schedule.default_status,DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.evaluate_tick(build_schedule_context()).run_requests[0].run_config['ops'][ferc_historical_preservation.op.name]['config']['capture_directory'],{'env':'HIFLD_FERC_CAPTURE_DIR'})
        with tempfile.TemporaryDirectory()as tmp,self.assertRaisesRegex(ValueError,'explicit_pinned_capture'):
            source.acquire(StagingStorageResource(local_dir=tmp),None)

    def test_permanent_incident_partial_write_and_typed_body(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);candidate=root/'candidate';(candidate/'metadata').mkdir(parents=True)
            manifest=b'{"original":true}\n';(candidate/'metadata/source_manifest.json').write_bytes(manifest)
            storage=StagingStorageResource(local_dir=tmp);fingerprint='a'*64
            with patch.object(source,'immutable',side_effect=OSError('injected body write')):
                with self.assertRaises(OSError):source.observed_trust_loss(storage,'v1',fingerprint,candidate=candidate)
            for _ in range(2):
                with self.assertRaisesRegex(ValueError,'incomplete_observed'):source.observed_trust_loss(storage,'v1',fingerprint)
            self.assertEqual((candidate/'metadata/source_manifest.json').read_bytes(),manifest)
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);candidate=root/'candidate';(candidate/'metadata').mkdir(parents=True)
            (candidate/'metadata/source_manifest.json').write_bytes(source.encode({'original':True}))
            storage=StagingStorageResource(local_dir=tmp)
            self.assertTrue(source.observed_trust_loss(storage,'v1','a'*64,candidate=candidate))
            journal=root/'_construction-incidents/ferc-regions/v1/incident.json';body=source.strict_json(journal.read_bytes());body['approved']=0;journal.write_bytes(source.encode(body))
            with self.assertRaisesRegex(ValueError,'invalid_observed'):source.observed_trust_loss(storage,'v1','a'*64)


@unittest.skipUnless(CAPTURES,'Complete43MB historical original is explicit opt-in HIFLD_FERC_TEST_CAPTURE_DIR; never fetched by tests.')
class FercCompleteOriginalTests(unittest.TestCase):
    def test_all_five_regions_every_native_attribute_ID_and_ring(self):
        table,audit=source.inspect_source(Path(CAPTURES)/'pasda_original.body')
        self.assertEqual(table.num_rows,5);self.assertEqual(audit['coordinates'],3727592);self.assertEqual(audit['rings'],17526)
        self.assertEqual(audit['invalid_objectids'],[2,3]);self.assertEqual(table['OGC_FID'].to_pylist(),list(range(5)))
        self.assertEqual(str(table.schema.field('OBJECTID').type),'int32')
        self.assertEqual(str(table.schema.field('SOURCEDATE').type),'date32[day]')
        self.assertEqual(str(table.schema.field('SHAPE__Are').type),'decimal128(38, 15)')
        self.assertEqual(str(table['SHAPE__Are'][0].as_py()),'2586408727577.089843750000000')
        self.assertEqual(audit['exact_numeric_literals'][0]['SHAPE__Len'],'13763641.201288700103760')

    def test_actual_three_input_receipts_pass_strict_provenance_structure_only(self):
        inputs=[]
        for name in SPEC['inputs']:
            body=source.bounded_read(Path(CAPTURES)/(name+'.body'),50_000_000)
            receipt=source.strict_json(source.bounded_read(Path(CAPTURES)/(name+'.receipt.json'),65_536))
            inputs.append(source.validate_receipt(name,body,receipt))
        manifest={'schema_version':1,'processor':'HIFLD Next','inputs':inputs,
            'code_commit':'a'*40,'transformations':[{'operation':'structural_receipt_test_only','parameters':{}}]}
        source.validate_release_provenance(manifest)
        self.assertEqual([v['role']for v in inputs],['mirror','producer_publication','producer_publication'])
        self.assertEqual(len(inputs),3)
        bad=copy.deepcopy(manifest);bad['inputs'][0]['retrieved_at']='2026-10-19T00:00:00+00:99'
        with self.assertRaises(ValueError):source.validate_release_provenance(bad)

    def test_source_body_retained_before_missing_or_malformed_receipt(self):
        # Small complete contextual input first, then a missing receipt. No large
        # copies/native outputs needed to establish preservation ordering.
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);capture=root/'capture';capture.mkdir();body=(Path(CAPTURES)/'dhs_raw.body').read_bytes()
            (capture/'dhs_raw.body').write_bytes(body)
            storage=StagingStorageResource(local_dir=str(root/'staging'))
            with patch.object(source,'INPUTS',{'dhs_raw':SPEC['inputs']['dhs_raw']}):
                with self.assertRaises(ValueError):source.acquire(storage,capture)
            raw=root/'staging'/f'_raw-captures/ferc-regions/{source.digest(body)}/dhs_raw.body'
            self.assertEqual(raw.read_bytes(),body)
            self.assertTrue(list((root/'staging/_quarantine').rglob('reason.json')))
