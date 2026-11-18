import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from xml.sax.saxutils import escape

import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import eia_860m_common as common
from dagster_hifld.sources import ornl_power_plants as source
from dagster_hifld.sources.eia_860m_common import normalize_existing, read_existing_workbook

FIELDS = [
    'Plant ID', 'Plant Name', 'Plant State', 'County', 'Generator ID', 'Unit Code',
    'Nameplate Capacity (MW)', 'Net Summer Capacity (MW)', 'Technology',
    'Energy Source Code', 'Status', 'Latitude', 'Longitude',
]


def _row(number, values):
    return '<row r="' + str(number) + '">' + ''.join(
        '<c r="' + chr(65 + index) + str(number) + '" t="inlineStr"><is><t>' + escape(value) + '</t></is></c>'
        for index, value in enumerate(values)) + '</row>'


def workbook(period='September 2026', operating=None, operating_pr=None):
    operating = operating if operating is not None else [
        ['1', 'Alpha', 'GA', 'Fulton', '001', '01', '1.5', '1.2', 'Solar', 'SUN', '(OP) Operating', '33.7', '-84.4'],
        ['1', 'Alpha', 'GA', 'Fulton', '002', '02', '2.5', '2.2', 'Petroleum Liquids', 'DFO', '(OP) Operating', '33.7', '-84.4'],
        ['2', 'Beta', 'TX', 'Harris', '001', '01', ' ', ' ', 'Wind', 'WND', '(OP) Operating', ' ', ' '],
    ]
    operating_pr = operating_pr if operating_pr is not None else [
        ['3', 'Gamma', 'PR', 'Bayamon', '001', '01', '4.0', '3.5', 'Solar', 'SUN', '(SB) Standby/Backup', '18.4', '-66.1'],
    ]
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w') as archive:
        archive.writestr('xl/sharedStrings.xml', '<sst xmlns="' + common.NS['s'] + '"/>')
        archive.writestr('xl/workbook.xml', '<workbook xmlns="' + common.NS['s'] + '" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Operating" r:id="rId1"/><sheet name="Operating_PR" r:id="rId2"/></sheets></workbook>')
        archive.writestr('xl/_rels/workbook.xml.rels', '<Relationships><Relationship Id="rId1" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Target="worksheets/sheet2.xml"/></Relationships>')
        for index, (name, rows) in enumerate((('Operating', operating), ('Operating_PR', operating_pr)), start=1):
            data = _row(1, [f'Inventory of Operating Generators as of {period}' + (' - Puerto Rico' if index == 2 else '')])
            data += _row(3, FIELDS)
            data += ''.join(_row(4 + offset, row) for offset, row in enumerate(rows))
            archive.writestr(f'xl/worksheets/sheet{index}.xml', '<worksheet xmlns="' + common.NS['s'] + '"><sheetData>' + data + '</sheetData></worksheet>')
    return output.getvalue()


class AggregationTests(unittest.TestCase):
    def test_aggregates_by_plant_with_capacity_and_primary_fuel(self):
        sheets, _ = read_existing_workbook(workbook(), '2026-09')
        generators, _, _ = normalize_existing(sheets)
        plants = source.aggregate_plants(generators)
        by_id = {plant['Plant ID']: plant for plant in plants}
        self.assertEqual(by_id['1']['generator_count'], 2)
        self.assertAlmostEqual(by_id['1']['total_nameplate_capacity_mw'], 4.0)
        self.assertAlmostEqual(by_id['1']['total_net_summer_capacity_mw'], 3.4)
        self.assertEqual(by_id['1']['primary_technology'], 'Petroleum Liquids')
        self.assertEqual(by_id['1']['primary_energy_source_code'], 'DFO')
        self.assertEqual(by_id['1']['coordinate_status'], 'consistent')
        self.assertIsNone(by_id['2']['total_nameplate_capacity_mw'])
        self.assertEqual(by_id['2']['primary_technology'], 'Wind')
        self.assertEqual(by_id['2']['coordinate_status'], 'missing')

    def test_conflicting_coordinates_are_not_averaged(self):
        rows = [
            ['1', 'Alpha', 'GA', 'Fulton', '001', '01', '1', '1', 'Solar', 'SUN', '(OP) Operating', '33', '-84'],
            ['1', 'Alpha', 'GA', 'Fulton', '002', '02', '1', '1', 'Solar', 'SUN', '(OP) Operating', '34', '-84'],
        ]
        sheets, _ = read_existing_workbook(workbook(operating=rows), '2026-09')
        generators, _, _ = normalize_existing(sheets)
        plant = source.aggregate_plants(generators)[0]
        self.assertEqual(plant['coordinate_status'], 'conflicting')
        self.assertIsNone(plant['Latitude'])
        self.assertIsNone(plant['Longitude'])

    def test_capacity_ties_break_lexicographically(self):
        rows = [
            ['1', 'Alpha', 'GA', 'Fulton', '001', '01', '1', '1', 'Wind', 'WND', '(OP) Operating', '33', '-84'],
            ['1', 'Alpha', 'GA', 'Fulton', '002', '02', '1', '1', 'Solar', 'SUN', '(OP) Operating', '33', '-84'],
        ]
        sheets, _ = read_existing_workbook(workbook(operating=rows), '2026-09')
        generators, _, _ = normalize_existing(sheets)
        self.assertEqual(source.aggregate_plants(generators)[0]['primary_technology'], 'Solar')

    def test_invalid_capacity_fails_closed(self):
        rows = [['1', 'Alpha', 'GA', 'Fulton', '001', '01', 'not-a-number', '1', 'Solar', 'SUN', '(OP) Operating', '33', '-84']]
        sheets, _ = read_existing_workbook(workbook(operating=rows), '2026-09')
        generators, _, _ = normalize_existing(sheets)
        with self.assertRaisesRegex(ValueError, 'invalid_source_capacity'):
            source.aggregate_plants(generators)


class CaptureTests(unittest.TestCase):
    def _client(self, body):
        def respond(request):
            if request.url.path.endswith('.xlsx'):
                payload = body
            elif request.url.path.endswith('copyrights_reuse.php'):
                payload = b'<main>Reuse is permitted.</main>'
            else:
                payload = b'<a href="xls/september_generator2026.xlsx">September</a>'
            return httpx.Response(200, stream=httpx.ByteStream(payload))
        return httpx.Client(transport=httpx.MockTransport(respond))

    def test_capture_writes_native_nonspatial_candidate(self):
        body = workbook()
        with tempfile.TemporaryDirectory() as temp:
            storage = StagingStorageResource(local_dir=temp + '/stage')
            with self._client(body) as client, patch('dagster_hifld.sources.capture_common.validate_construction_code_identity'):
                result = source.capture_power_plants(storage, capture_directory=temp + '/capture', client=client)
            self.assertEqual(result['source_period'], '2026-09')
            self.assertEqual(result['plant_count'], 3)
            self.assertEqual(result['generator_count'], 4)
            self.assertEqual(result['row_count'], 3)
            self.assertTrue(result['source_population_complete'])
            self.assertFalse(result['release_accepted'])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}"
            import pyarrow.parquet as pq
            table = pq.read_table(io.BytesIO(storage.read_key(prefix + '/unknown/power-plants-1.parquet')))
            self.assertNotIn('geometry', table.column_names)
            rows = {row['Plant ID']: row for row in table.to_pylist()}
            self.assertAlmostEqual(rows['1']['total_nameplate_capacity_mw'], 4.0)
            manifest = json.loads(storage.read_key(prefix + '/metadata/source_manifest.json'))
            self.assertEqual(manifest['source_period'], '2026-09')
            self.assertEqual(manifest['coverage']['plant_count'], 3)
            self.assertEqual(manifest['coverage']['generator_count'], 4)
            self.assertIsNone(manifest['coordinate_datum'])
            self.assertFalse(manifest['approved'])
            self.assertFalse(manifest['release_accepted'])
            audit = json.loads(storage.read_key(prefix + '/metadata/source_audit.json'))
            self.assertEqual(audit['source_period'], '2026-09')
            with self._client(body) as client, patch('dagster_hifld.sources.capture_common.validate_construction_code_identity'):
                replay = source.capture_power_plants(storage, capture_directory=temp + '/capture', replay=True, client=client)
            self.assertTrue(replay['reused'])
            self.assertEqual(replay['version'], result['version'])

    def test_no_frozen_map_zip_fetch_remains(self):
        text = Path(source.__file__).read_text()
        self.assertNotIn('maps/map_data', text)
        self.assertNotIn('EXPECTED_COUNT', text)
        self.assertNotIn('PowerPlants_US_202108', text)

    def test_schedule_is_stopped(self):
        from dagster_hifld.assets.ornl_power_plants import eia_power_plants_draft_schedule
        self.assertEqual(eia_power_plants_draft_schedule.default_status.value, 'STOPPED')


if __name__ == '__main__':
    unittest.main()
