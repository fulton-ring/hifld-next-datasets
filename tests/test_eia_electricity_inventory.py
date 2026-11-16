"""Maintained EIA source selection and source-native identity regressions."""
import unittest
import io
import json
import tempfile
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import patch

import httpx

from dagster_hifld.sources import eia_electricity_inventory as source

class EIAInventoryTests(unittest.TestCase):
    def test_active_links_only(self):
        html = '<!-- <a href="xls/december_generator2026.xlsx">December</a> --><a href="xls/september_generator2026.xlsx">September</a><a href="archive/xls/august_generator2026.xlsx">August</a>'
        self.assertEqual(source.latest_release(html), ('2026-09', source.PAGE_URL + 'xls/september_generator2026.xlsx'))

    def test_future_active_period_is_not_a_released_inventory(self):
        html = '<a href="xls/december_generator2026.xlsx">Preview</a><a href="xls/september_generator2026.xlsx">Released</a>'
        self.assertEqual(source.latest_release(html, as_of=date(2026, 11, 16))[0], '2026-09')
        with self.assertRaisesRegex(ValueError, 'no_active'):
            source.latest_release('<a href="xls/january_generator2099.xlsx">Future</a>', as_of=date(2026, 11, 16))

    def test_rows_keep_lexical_ids_status_and_pr(self):
        row = {'Plant ID':'1','Generator ID':'001','Status':'(OP) Operating','Plant State':'PR','Latitude':'18','Longitude':'-66','Unit Code':'01'}
        records, plants, audit = source.normalize_existing({'Operating':[], 'Operating_PR':[row]})
        self.assertEqual(records[0]['eia_generator_key'], '["1","001"]')
        self.assertEqual(records[0]['Unit Code'], '01')
        self.assertEqual(audit['puerto_rico_generators'],1)
        self.assertEqual(plants[0]['coordinate_status'],'consistent')

    def test_missing_duplicate_identity_status_and_pr_fail(self):
        row = {'Plant ID':'1','Generator ID':'01','Status':'(OP) Operating','Plant State':'GA','Latitude':'34','Longitude':'-84'}
        for field in ('Plant ID','Generator ID','Status'):
            with self.subTest(field=field), self.assertRaises(ValueError):
                source.normalize_existing({'Operating':[{**row,field:''}],'Operating_PR':[]})
        with self.assertRaises(ValueError): source.normalize_existing({'Operating':[row,row],'Operating_PR':[]})
        with self.assertRaises(ValueError): source.normalize_existing({'Operating':[row]})
        with self.assertRaises(ValueError): source.normalize_existing({'Operating':[],'Operating_PR':[row]})

    def test_conflicting_coords_are_never_averaged(self):
        row = {'Plant ID':'1','Generator ID':'01','Status':'(OP) Operating','Plant State':'GA','Latitude':'34','Longitude':'-84'}
        _,plants,audit=source.normalize_existing({'Operating':[row,{**row,'Generator ID':'02','Latitude':'35'}],'Operating_PR':[]})
        self.assertIsNone(plants[0]['Latitude'])
        self.assertEqual(plants[0]['coordinate_status'],'conflicting')
        self.assertFalse(audit['geospatial_release_ready'])
        with self.assertRaises(ValueError): source.normalize_existing({'Operating':[{**row,'Latitude':'NaN'}],'Operating_PR':[]})
        _,plants,_=source.normalize_existing({'Operating':[{**row,'Latitude':''}],'Operating_PR':[]})
        self.assertEqual(plants[0]['coordinate_status'],'missing')

if __name__=='__main__': unittest.main()

class WorkbookTests(unittest.TestCase):
    @staticmethod
    def workbook(period='September 2026', capacity='1'):
        import io
        import zipfile
        from xml.sax.saxutils import escape
        fields=['Plant ID','Generator ID','Unit Code','Status','Plant State','Plant Name','Latitude','Longitude','Nameplate Capacity (MW)']
        output=io.BytesIO()
        with zipfile.ZipFile(output,'w') as z:
            z.writestr('xl/sharedStrings.xml','<sst xmlns="'+source.NS['s']+'"/>')
            z.writestr('xl/workbook.xml','<workbook xmlns="'+source.NS['s']+'" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Operating" r:id="rId1"/><sheet name="Operating_PR" r:id="rId2"/></sheets></workbook>')
            z.writestr('xl/_rels/workbook.xml.rels','<Relationships><Relationship Id="rId1" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Target="worksheets/sheet2.xml"/></Relationships>')
            for index,state in ((1,'GA'),(2,'PR')):
                def row(number,values):
                    return '<row r="'+str(number)+'">'+''.join('<c r="'+chr(65+i)+str(number)+'" t="inlineStr"><is><t>'+escape(v)+'</t></is></c>' for i,v in enumerate(values))+'</row>'
                data=row(1,['Inventory of Operating Generators as of '+period+(' - Puerto Rico' if index==2 else '')])+row(3,fields)+row(4,[str(index),'001','01','(OP) Operating',state,'Plant','18','-66',capacity])+row(5,['NOTES: source note'])
                z.writestr(f'xl/worksheets/sheet{index}.xml','<worksheet xmlns="'+source.NS['s']+'"><sheetData>'+data+'</sheetData></worksheet>')
        return output.getvalue()

    def test_layout_period_and_all_columns(self):
        sheets,layout=source.read_existing_workbook(self.workbook(),'2026-09')
        self.assertEqual(sheets['Operating'][0]['Generator ID'],'001')
        self.assertEqual(layout['operating_sheets']['Operating']['rows'],1)
        with self.assertRaisesRegex(ValueError,'period_mismatch'):
            source.read_existing_workbook(self.workbook(),'2026-10')

    def test_uncached_formula_is_not_an_absent_native_cell(self):
        output = io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(self.workbook())) as original, zipfile.ZipFile(output, 'w') as changed:
            for item in original.infolist():
                body = original.read(item.filename)
                if item.filename == 'xl/worksheets/sheet1.xml':
                    body = body.replace(b'<c r="I4" t="inlineStr"><is><t>1</t></is></c>', b'<c r="I4"><f>1+1</f></c>')
                changed.writestr(item, body)
        with self.assertRaisesRegex(ValueError, 'uncached_formula'):
            source.read_existing_workbook(output.getvalue(), '2026-09')

    def test_supporting_evidence_revision_and_observation_retention(self):
        from dagster_hifld.resources import StagingStorageResource
        policy = [b'<main>Reuse is permitted.</main><span>Page views: 1</span>']
        workbook = self.workbook()
        def respond(request):
            body = workbook if request.url.path.endswith('.xlsx') else policy[0] if request.url.path.endswith('copyrights_reuse.php') else b'<a href="xls/september_generator2026.xlsx">September</a>'
            return httpx.Response(200, stream=httpx.ByteStream(body))
        with tempfile.TemporaryDirectory() as temp:
            storage = StagingStorageResource(local_dir=temp+'/stage')
            with httpx.Client(transport=httpx.MockTransport(respond)) as client, patch('dagster_hifld.sources.epa_frs_common.validate_construction_code_identity'):
                first = source.capture_inventory(storage, capture_directory=temp+'/one', client=client)
                same_content = source.capture_inventory(storage, capture_directory=temp+'/same', client=client)
                self.assertEqual(first['candidates'][0]['version'], same_content['candidates'][0]['version'])
                self.assertTrue(all(item['reused'] for item in same_content['candidates']))
                policy[0] = policy[0].replace(b'views: 1', b'views: 999')
                unchanged = source.capture_inventory(storage, capture_directory=temp+'/two', client=client)
                self.assertEqual(first['candidates'][0]['version'], unchanged['candidates'][0]['version'])
                policy[0] = b'<main>Reuse requires permission.</main><span>Page views: 999</span>'
                revised = source.capture_inventory(storage, capture_directory=temp+'/three', client=client)
                self.assertNotEqual(first['candidates'][0]['version'], revised['candidates'][0]['version'])
            observations = storage.list_prefix('_capture-observations/' + source.DATASET_SLUG)
            self.assertEqual(len([key for key in observations if key.endswith('/workbook.receipt.json')]), 4)

    def test_manifest_refresh_contract_and_unknown_relationship(self):
        from dagster_hifld.resources import StagingStorageResource
        def respond(request):
            return httpx.Response(200, stream=httpx.ByteStream(self.workbook() if request.url.path.endswith('.xlsx') else b'<a href="xls/september_generator2026.xlsx">September</a>'))
        with tempfile.TemporaryDirectory() as temp:
            storage = StagingStorageResource(local_dir=temp+'/stage')
            with httpx.Client(transport=httpx.MockTransport(respond)) as client, patch('dagster_hifld.sources.epa_frs_common.validate_construction_code_identity'):
                result = source.capture_inventory(storage, capture_directory=temp+'/capture', client=client)
            item = result['candidates'][0]
            manifest = json.loads(storage.read_key(f"{item['dataset_slug']}/{item['file_slug']}/{item['version']}/metadata/source_manifest.json"))
            self.assertTrue(manifest['source_relationship']['unknown_reason'])
            for entry in manifest['inputs']:
                refresh = entry['refresh']
                self.assertIn(refresh['maintenance_status'], {'maintained', 'unverified', 'retired'})
                self.assertIn(refresh['latest_evidence_input_id'], {i['input_id'] for i in manifest['inputs']})
                self.assertTrue(refresh['latest_checked_at'])
            self.assertIn('identifier_meaning', manifest['field_definitions']['Unit Code'])
            self.assertIn('parquet', manifest['format_transformations'])

    def test_registered_stopped_assets(self):
        from dagster_hifld.assets import eia_electricity_inventory as assets
        from dagster import DefaultScheduleStatus
        self.assertEqual(len(assets.INGEST_ASSETS),2)
        self.assertEqual(len(assets.ASSET_CHECKS),2)
        self.assertEqual(assets.SCHEDULES[0].default_status,DefaultScheduleStatus.STOPPED)

    def test_replay_noop_same_period_correction_and_tamper(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        import httpx
        from dagster_hifld.resources import StagingStorageResource
        workbook=[self.workbook()]
        def respond(request):
            return httpx.Response(200,stream=httpx.ByteStream(workbook[0] if request.url.path.endswith('.xlsx') else b'<a href="xls/september_generator2026.xlsx">September</a>'))
        with tempfile.TemporaryDirectory() as temp:
            storage=StagingStorageResource(local_dir=temp+'/stage')
            client=httpx.Client(transport=httpx.MockTransport(respond))
            with patch('dagster_hifld.sources.epa_frs_common.validate_construction_code_identity'):
                first=source.capture_inventory(storage,capture_directory=temp+'/capture',client=client)
                replay=source.capture_inventory(storage,capture_directory=temp+'/capture',replay=True,client=client)
                self.assertTrue(all(c['reused'] for c in replay['candidates']))
                self.assertEqual(first['candidates'][0]['version'],replay['candidates'][0]['version'])
                workbook[0]=self.workbook(capacity='2')
                corrected=source.capture_inventory(storage,capture_directory=temp+'/corrected',client=client)
                self.assertNotEqual(first['candidates'][0]['version'],corrected['candidates'][0]['version'])
                path=Path(temp+'/capture/workbook.body');path.write_bytes(b'tampered')
                with self.assertRaisesRegex(ValueError,'invalid_retained_capture'):
                    source.capture_inventory(storage,capture_directory=temp+'/capture',replay=True,client=client)
