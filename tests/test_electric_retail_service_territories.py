import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from xml.sax.saxutils import escape

import geopandas as gpd
import httpx
import pyarrow.parquet as pq
from shapely.geometry import box

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.electric_retail_service_territories import (
    electric_retail_service_territories_draft_schedule,
    electric_retail_service_territories_geometry_valid,
    electric_retail_service_territories_population_complete,
    electric_retail_service_territories_release_pending,
)
from dagster_hifld.sources import electric_retail_service_territories_common as source

SERVICE_HEADERS = ["Data Year", "Utility Number", "Utility Name", "Short Form", "State", "County"]
FRAME_HEADERS = [
    "Data Year", "Utility Number", "Utility Name", "Short Form", "Ownership Code",
    "Ownership", "Monthly", "Advanced Metering", "Delivery Company", "Demand Response",
    "Distribution Systems", "Dynamic Pricing", "Energy Efficiency", "Mergers",
    "Net Metering", "Non Net Metering Distributed", "Operational Data", "Reliablity",
    "Sales to Ultimate Customers", "Service Territory", "Utility Data",
]


def _xlsx(sheets: dict[str, list[list[str]]]) -> bytes:
    content_types = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="xml" ContentType="application/xml"/></Types>'
    )
    workbook_sheets = []
    relationships = []
    sheet_xml = {}
    for index, (name, rows) in enumerate(sheets.items(), start=1):
        workbook_sheets.append(
            f'<sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>'
        )
        relationships.append(
            f'<Relationship Id="rId{index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{index}.xml"/>'
        )
        body = []
        for row_index, row in enumerate(rows, start=1):
            cells = []
            for column_index, value in enumerate(row):
                column = chr(ord("A") + column_index)
                cells.append(
                    f'<c r="{column}{row_index}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'
                )
            body.append(f'<row r="{row_index}">{"".join(cells)}</row>')
        sheet_xml[f"xl/worksheets/sheet{index}.xml"] = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f'<sheetData>{"".join(body)}</sheetData></worksheet>'
        )
    workbook = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheets>{"".join(workbook_sheets)}</sheets></workbook>'
    )
    rels = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        f'{"".join(relationships)}</Relationships>'
    )
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w") as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("xl/workbook.xml", workbook)
        archive.writestr("xl/_rels/workbook.xml.rels", rels)
        for path, body in sheet_xml.items():
            archive.writestr(path, body)
    return sink.getvalue()


def _service_rows() -> list[list[str]]:
    return [
        SERVICE_HEADERS,
        ["2025", "1", "Utility One", "", "AL", "Autauga"],
        ["2025", "1", "Utility One", "", "AL", "Baldwin"],
        ["2025", "2", "Utility Two", "Y", "CT", "Middlesex"],
        ["2025", "2", "Utility Two", "Y", "AK", "Valdez Cordova"],
        ["2025", "2", "Utility Two", "Y", "VA", "Bedford City"],
        ["2025", "2", "Utility Two", "Y", "AK", "Not Applicable"],
    ]


def _frame_rows() -> list[list[str]]:
    blank = [""] * len(FRAME_HEADERS)
    one = list(blank)
    one[0:6] = ["2025", "1", "Utility One", "", "IOU", "Investor Owned"]
    one[19] = "X"
    two = list(blank)
    two[0:6] = ["2025", "2", "Utility Two", "Y", "C", "Cooperative"]
    two[19] = "X"
    return [FRAME_HEADERS, one, two]


def _eia_zip() -> bytes:
    service = _xlsx({
        "Counties_States": _service_rows(),
        "Counties_Territories": [SERVICE_HEADERS],
    })
    frame = _xlsx({"Frame": _frame_rows()})
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w") as archive:
        archive.writestr("Service_Territory_2025.xlsx", service)
        archive.writestr("Frame_2025.xlsx", frame)
    return sink.getvalue()


def _counties() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "GEOID": ["01001", "01003", "09130", "02063", "02066", "51019"],
            "STATEFP": ["01", "01", "09", "02", "02", "51"],
            "NAME": ["Autauga", "Baldwin", "Middlesex", "Chugach", "Copper River", "Bedford"],
            "NAMELSAD": [
                "Autauga County", "Baldwin County", "Middlesex Planning Region",
                "Chugach Census Area", "Copper River Census Area", "Bedford County",
            ],
        },
        geometry=[
            box(0, 0, 1, 1), box(1, 0, 2, 1), box(10, 10, 11, 11),
            box(20, 20, 21, 21), box(21, 20, 22, 21), box(30, 30, 31, 31),
        ],
        crs="EPSG:4269",
    )


def _tiger_zip() -> bytes:
    sink = io.BytesIO()
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        stem = "tl_2026_us_county"
        _counties().to_file(directory / f"{stem}.shp", engine="pyogrio")
        (directory / f"{stem}.cpg").write_text("UTF-8")
        (directory / f"{stem}.shp.iso.xml").write_text("<metadata/>")
        (directory / f"{stem}.shp.ea.iso.xml").write_text("<metadata/>")
        with zipfile.ZipFile(sink, "w") as archive:
            for path in sorted(directory.glob(f"{stem}.*")):
                archive.writestr(path.name, path.read_bytes())
    return sink.getvalue()


class ElectricRetailTerritoryTests(unittest.TestCase):
    def test_latest_release_selects_newest_active_annual_zip(self):
        html = (
            '<a href="zip/f8612025.zip">2025</a>'
            '<a href="archive/zip/f8612024.zip">2024</a>'
            '<a href="zip/f8612025er.zip">errata</a>'
        )
        self.assertEqual(
            source.latest_release(html),
            (2025, "https://www.eia.gov/electricity/data/eia861/zip/f8612025.zip"),
        )

    def test_latest_release_refuses_ambiguous_year(self):
        with self.assertRaisesRegex(ValueError, "ambiguous_latest_eia861_release"):
            source.latest_release('<a href="zip/f8612025.zip">a</a><a href="zip/f8612025.zip">b</a>')

    def test_workbook_readers_preserve_native_utility_numbers_and_ownership(self):
        rows = source.read_service_territory_workbook(_xlsx({
            "Counties_States": _service_rows(), "Counties_Territories": [SERVICE_HEADERS],
        }), 2025)
        self.assertEqual(rows[0]["utility_number"], "1")
        self.assertEqual(rows[2]["state"], "CT")
        frame = source.read_frame_workbook(_xlsx({"Frame": _frame_rows()}), 2025)
        self.assertEqual(frame["2"]["ownership_type"], "Cooperative")
        self.assertEqual(frame["2"]["service_territory"], "X")

    def test_join_uses_current_names_aliases_and_skips_not_applicable(self):
        rows = source.read_service_territory_workbook(_xlsx({
            "Counties_States": _service_rows(), "Counties_Territories": [SERVICE_HEADERS],
        }), 2025)
        frame = source.read_frame_workbook(_xlsx({"Frame": _frame_rows()}), 2025)
        records, audit = source.join_territories(rows, frame, _counties())
        self.assertEqual(audit, {"skipped_not_applicable": 1, "matched_county_rows": 5})
        self.assertEqual(
            [record["geoids"] for record in records if record["county"] == "Valdez Cordova"],
            [("02063", "02066")],
        )

    def test_unmatched_county_name_fails_closed(self):
        rows = source.read_service_territory_workbook(_xlsx({
            "Counties_States": _service_rows() + [["2025", "1", "Utility One", "", "AL", "Missing"]],
            "Counties_Territories": [SERVICE_HEADERS],
        }), 2025)
        frame = source.read_frame_workbook(_xlsx({"Frame": _frame_rows()}), 2025)
        with self.assertRaisesRegex(ValueError, "unmatched_eia861_county_names"):
            source.join_territories(rows, frame, _counties())

    def test_join_resolves_county_and_independent_city_name_collisions(self):
        counties = gpd.GeoDataFrame(
            {
                "GEOID": ["51059", "51600", "51036", "51095"],
                "STATEFP": ["51", "51", "51", "51"],
                "NAME": ["Fairfax", "Fairfax", "Charles City", "James City"],
                "NAMELSAD": [
                    "Fairfax County", "Fairfax city",
                    "Charles City County", "James City County",
                ],
            },
            geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1), box(2, 0, 3, 1), box(3, 0, 4, 1)],
            crs="EPSG:4269",
        )
        rows = [
            {"utility_number": "1", "utility_name": "Utility One", "state": "VA", "county": "Fairfax"},
            {"utility_number": "1", "utility_name": "Utility One", "state": "VA", "county": "Fairfax City"},
            {"utility_number": "1", "utility_name": "Utility One", "state": "VA", "county": "Charles City"},
            {"utility_number": "1", "utility_name": "Utility One", "state": "VA", "county": "James City"},
        ]
        frame = {"1": {
            "utility_name": "Utility One", "ownership_type": "Cooperative",
            "service_territory": "X",
        }}
        records, _ = source.join_territories(rows, frame, counties)
        self.assertEqual(
            [record["geoids"] for record in records],
            [("51059",), ("51600",), ("51036",), ("51095",)],
        )

    def test_join_fails_closed_on_unlisted_county_and_city_name_collision(self):
        counties = gpd.GeoDataFrame(
            {
                "GEOID": ["51998", "51999"],
                "STATEFP": ["51", "51"],
                "NAME": ["Example", "Example"],
                "NAMELSAD": ["Example County", "Example city"],
            },
            geometry=[box(0, 0, 1, 1), box(1, 0, 2, 1)],
            crs="EPSG:4269",
        )
        rows = [
            {"utility_number": "1", "utility_name": "Utility One", "state": "VA", "county": "Example"},
        ]
        frame = {"1": {
            "utility_name": "Utility One", "ownership_type": "Cooperative",
            "service_territory": "X",
        }}
        with self.assertRaisesRegex(ValueError, "ambiguous_eia861_county_name"):
            source.join_territories(rows, frame, counties)

    def test_aggregate_dissolves_one_feature_per_utility_state(self):
        rows = source.read_service_territory_workbook(_xlsx({
            "Counties_States": _service_rows(), "Counties_Territories": [SERVICE_HEADERS],
        }), 2025)
        frame = source.read_frame_workbook(_xlsx({"Frame": _frame_rows()}), 2025)
        records, _ = source.join_territories(rows, frame, _counties())
        features = source.aggregate_territories(records, _counties(), 2025)
        self.assertEqual(len(features), 4)
        self.assertEqual([row["utility_number"] for row in features], ["1", "2", "2", "2"])
        self.assertEqual([row["state"] for row in features], ["AL", "AK", "CT", "VA"])
        self.assertEqual(features[0]["county_count"], 2)
        self.assertEqual(features[1]["county_count"], 1)
        self.assertAlmostEqual(features[1]["geometry"].area, 2.0)

    def test_candidate_capture_writes_geoparquet_and_closed_approvals(self):
        from dagster_hifld.resources import StagingStorageResource

        eia_page = (
            '<a href="zip/f8612025.zip">2025</a>'
            '<a href="archive/zip/f8612024.zip">2024</a>'
        )
        tiger_zip = _tiger_zip()

        def respond(request):
            url = str(request.url)
            if url == source.PAGE_URL:
                return httpx.Response(200, stream=httpx.ByteStream(eia_page.encode()), request=request)
            if url.endswith("/f8612025.zip"):
                return httpx.Response(200, stream=httpx.ByteStream(_eia_zip()), request=request)
            if url == source.REUSE_URL:
                return httpx.Response(200, stream=httpx.ByteStream(b"<html>reuse</html>"), request=request)
            if url == source.TIGER_ROOT:
                return httpx.Response(200, stream=httpx.ByteStream(b'<a href="TIGER2026/">2026</a>'), request=request)
            if url == source.TIGER_ROOT + "TIGER2026/COUNTY/":
                return httpx.Response(200, stream=httpx.ByteStream(b'<a href="tl_2026_us_county.zip">zip</a>'), request=request)
            if url.endswith("/tl_2026_us_county.zip"):
                return httpx.Response(200, stream=httpx.ByteStream(tiger_zip), request=request)
            return httpx.Response(404, stream=httpx.ByteStream(b"missing"), request=request)

        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=httpx.MockTransport(respond)) as client:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "stage"))
            with patch.object(source, "MIN_COUNTY_COUNT", 1):
                result = source.capture_electric_retail_service_territories(storage, client=client)
            self.assertEqual(result["source_year"], 2025)
            self.assertEqual(result["tiger_edition"], "2026")
            self.assertEqual(result["feature_count"], 4)
            self.assertEqual(result["utility_count"], 2)
            self.assertEqual(result["state_count"], 4)
            self.assertEqual(result["county_row_count"], 6)
            self.assertEqual(result["matched_county_row_count"], 5)
            self.assertEqual(result["skipped_not_applicable_count"], 1)
            self.assertEqual(result["distinct_county_count"], 6)
            self.assertTrue(result["source_population_complete"])
            self.assertFalse(result["release_accepted"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}"
            table = pq.read_table(io.BytesIO(storage.read_key(prefix + "/geoparquet/" + source.FILE_SLUG + ".parquet")))
            self.assertEqual(table.num_rows, 4)
            self.assertEqual(table.column_names, [
                "utility_number", "utility_name", "ownership_type", "state",
                "county_count", "source_year", "geometry",
            ])
            manifest = json.loads(storage.read_key(prefix + "/metadata/source_manifest.json"))
            self.assertEqual(len(manifest["inputs"]), 6)
            self.assertTrue(all(manifest[key] is False for key in source.APPROVALS))
            ready = json.loads(storage.read_key(prefix + "/metadata/candidate_ready.json"))
            self.assertFalse(ready["approved"])

    def test_release_checks_and_schedule_remain_stopped(self):
        candidate = {
            "source_population_complete": True, "feature_count": 4, "utility_count": 2,
            "state_count": 4, "county_row_count": 6, "matched_county_row_count": 5,
            "distinct_county_count": 6, "skipped_not_applicable_count": 1,
            "null_geometry_count": 0, "invalid_geometry_count": 0, "release_accepted": False,
        }
        self.assertTrue(electric_retail_service_territories_population_complete(candidate).passed)
        self.assertTrue(electric_retail_service_territories_geometry_valid(candidate).passed)
        candidate["invalid_geometry_count"] = 1
        self.assertFalse(electric_retail_service_territories_geometry_valid(candidate).passed)
        self.assertFalse(electric_retail_service_territories_release_pending(candidate).passed)
        self.assertEqual(
            electric_retail_service_territories_draft_schedule.default_status,
            DefaultScheduleStatus.STOPPED,
        )


if __name__ == "__main__":
    unittest.main()
