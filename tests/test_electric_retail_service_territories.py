import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from xml.sax.saxutils import escape

import geopandas as gpd
import httpx
import pyarrow.parquet as pq
from shapely.geometry import box

from dagster import AssetCheckSeverity, DagsterInstance, DefaultScheduleStatus, materialize
from dagster_hifld.assets import electric_retail_service_territories as assets
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.assets.electric_retail_service_territories import (
    electric_retail_service_territories_draft_schedule,
    electric_retail_service_territories_geometry_valid,
    electric_retail_service_territories_population_complete,
    electric_retail_service_territories_release_pending,
)
from dagster_hifld.resources import (
    PublishedStorageResource, StagingStorageResource, StorageObjectSnapshot,
)
from dagster_hifld.sources import capture_common
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


def _eia_zip(*, added_utility: bool = False) -> bytes:
    service_rows = _service_rows()
    frame_rows = _frame_rows()
    if added_utility:
        service_rows.append(["2025", "3", "Utility Three", "", "AL", "Autauga"])
        frame_row = list(frame_rows[1])
        frame_row[1:3] = ["3", "Utility Three"]
        frame_rows.append(frame_row)
    service = _xlsx({
        "Counties_States": service_rows,
        "Counties_Territories": [SERVICE_HEADERS],
    })
    frame = _xlsx({"Frame": frame_rows})
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


def _client(eia_body=None, tiger_body=None):
    payloads = {
        source.PAGE_URL: b'<a href="zip/f8612025.zip">2025</a>',
        source.PAGE_URL + 'zip/f8612025.zip': eia_body if eia_body is not None else _eia_zip(),
        source.REUSE_URL: b'<html>reuse</html>',
        source.TIGER_ROOT: b'<a href="TIGER2026/">2026</a>',
        source.TIGER_ROOT + 'TIGER2026/COUNTY/': b'<a href="tl_2026_us_county.zip">zip</a>',
        source.TIGER_ROOT + 'TIGER2026/COUNTY/tl_2026_us_county.zip': tiger_body if tiger_body is not None else _tiger_zip(),
    }
    return httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(
        200, stream=httpx.ByteStream(payloads[str(request.url)])) ))


class ElectricRetailTerritoryTests(unittest.TestCase):
    def test_identical_refresh_preserves_first_capture_and_candidate(self):
        from dagster_hifld.resources import StagingStorageResource

        with tempfile.TemporaryDirectory() as tmp, _client() as client, \
                patch.object(source, "MIN_COUNTY_COUNT", 1), patch.object(capture_common, "datetime") as clock:
            root = Path(tmp) / "capture"
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "stage"))
            clock.now.return_value = datetime(2026, 11, 20, tzinfo=timezone.utc)
            first = source.capture_electric_retail_service_territories(storage, client=client,
                capture_directory=str(root))
            staged = {key: storage.read_key(key) for key in storage.list_prefix()}
            retained = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
            clock.now.return_value = datetime(2026, 11, 21, tzinfo=timezone.utc)
            with patch.object(StagingStorageResource, "write_key_if_unchanged",
                    side_effect=AssertionError("refresh attempted a staged write")):
                second = source.capture_electric_retail_service_territories(storage, client=client,
                    capture_directory=str(root))
            self.assertEqual(first, second)
            self.assertEqual({key: storage.read_key(key) for key in storage.list_prefix()}, staged)
            self.assertEqual({path: path.read_bytes() for path in retained}, retained)

    def test_refresh_rejects_conflicting_or_partial_existing_candidate(self):
        from dagster_hifld.resources import StagingStorageResource

        with tempfile.TemporaryDirectory() as tmp, _client() as client, patch.object(source, "MIN_COUNTY_COUNT", 1):
            stage = Path(tmp) / "stage"
            storage = StagingStorageResource(use_local=True, local_dir=str(stage))
            first = source.capture_electric_retail_service_territories(storage, client=client,
                capture_directory=str(Path(tmp) / "capture"))
            ready = stage / first["candidate_ready"]
            receipt = ready.parent.parent / "raw/eia861_page.receipt.json"
            original = receipt.read_bytes()
            for file, replacement, error in (
                (receipt, original.replace(b"https://", b"http://", 1), "electric_territory_capture_changed"),
                (ready.parent.parent / "raw/eia861_page.wire", b"altered wire", "candidate_readback_mismatch"),
                (ready, None, "electric_territory_candidate_incomplete"),
            ):
                with self.subTest(path=file.name):
                    before = file.read_bytes()
                    if replacement is None:
                        file.unlink()
                    else:
                        file.write_bytes(replacement)
                    with patch.object(StagingStorageResource, "write_key_if_unchanged",
                            side_effect=AssertionError("invalid candidate attempted a staged write")):
                        with self.assertRaisesRegex(ValueError, error):
                            source.capture_electric_retail_service_territories(storage, client=client,
                                capture_directory=str(Path(tmp) / "capture"))
                    file.write_bytes(before)

    def test_oversized_dbf_is_refused_before_payload_read_or_decode(self):
        with zipfile.ZipFile(io.BytesIO(_tiger_zip())) as archive:
            members = {name: archive.read(name) for name in archive.namelist()}
        output = io.BytesIO()
        with tempfile.TemporaryDirectory() as tmp:
            oversized = Path(tmp) / 'oversized.dbf'
            with oversized.open('wb') as stream:
                stream.truncate(65535 + source.MAX_COUNTY_COUNT * 65535 + 2)
            with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
                for name, body in members.items():
                    if name.endswith('.dbf'):
                        archive.write(oversized, name)
                    else:
                        archive.writestr(name, body)
        read_bytes = Path.read_bytes
        def refuse_dbf_read(path):
            if path.suffix == '.dbf':
                raise AssertionError('oversized DBF payload was materialized')
            return read_bytes(path)
        with patch.object(Path, 'read_bytes', refuse_dbf_read), \
                patch.object(zipfile.ZipFile, 'read', side_effect=AssertionError('unexpected member allocation')), \
                patch.object(source.gpd, 'read_file', side_effect=AssertionError('unexpected decode')):
            with self.assertRaisesRegex(ValueError, 'tiger_county_dbf_size_limit'):
                source._read_counties(output.getvalue(), '2026')

    def test_decoded_county_truncation_is_refused_against_publisher_count(self):
        archive = _tiger_zip()
        with patch.object(source, 'MIN_COUNTY_COUNT', 1), \
                patch.object(source.gpd, 'read_file', return_value=_counties().iloc[:-1].copy()):
            with self.assertRaisesRegex(ValueError, 'tiger_county_source_record_count_disagreement'):
                source._read_counties(archive, '2026')

    def test_physically_truncated_county_shapefile_is_refused(self):
        with zipfile.ZipFile(io.BytesIO(_tiger_zip())) as archive:
            members = {name: archive.read(name) for name in archive.namelist()}
        members['tl_2026_us_county.shp'] = members['tl_2026_us_county.shp'][:-20]
        output = io.BytesIO()
        with zipfile.ZipFile(output, 'w') as archive:
            for name, body in members.items():
                archive.writestr(name, body)
        with patch.object(source, 'MIN_COUNTY_COUNT', 1):
            with self.assertRaises(ValueError):
                source._read_counties(output.getvalue(), '2026')

    def test_added_utility_materializes_with_completeness_and_baseline_warn(self):
        from dagster_hifld.resources import StagingStorageResource
        self.assertIn(assets.electric_retail_service_territories_approved_baseline_drift, assets.ASSET_CHECKS)
        with tempfile.TemporaryDirectory() as tmp, DagsterInstance.ephemeral() as instance, \
                patch.object(source, 'MIN_COUNTY_COUNT', 1):
            storage = StagingStorageResource(local_dir=str(Path(tmp) / 'stage'))
            with _client() as client:
                before = source.capture_electric_retail_service_territories(storage, client=client,
                    capture_directory=str(Path(tmp) / 'before'))
            client = _client(_eia_zip(added_utility=True))
            with patch.object(source.httpx, 'Client', return_value=client):
                result = materialize([*assets.INGEST_ASSETS, *assets.ASSET_CHECKS], instance=instance,
                    resources={'staging_storage': storage}, run_config={'ops': {
                        assets.electric_retail_service_territories_eia861_candidate.op.name: {
                            'config': {'capture_directory': str(Path(tmp) / 'after')}}}})
            self.assertTrue(result.success)
            after = result.output_for_node(assets.electric_retail_service_territories_eia861_candidate.op.name)
            self.assertEqual((before['feature_count'], after['feature_count']), (4, 5))
            self.assertEqual((before['county_row_count'], after['county_row_count']), (6, 7))
            self.assertTrue(after['source_population_complete'])
            self.assertNotEqual(before['source_id_sha256'], after['source_id_sha256'])
            self.assertFalse(after['release_accepted'])
            checks = {check.check_name: check for check in result.get_asset_check_evaluations()}
            self.assertTrue(checks['electric_retail_service_territories_population_complete'].passed)
            self.assertTrue(checks['electric_retail_service_territories_geometry_valid'].passed)
            warning = checks['electric_retail_service_territories_approved_baseline_drift']
            self.assertFalse(warning.passed)
            self.assertEqual(warning.severity, AssetCheckSeverity.WARN)
            self.assertEqual(warning.metadata['comparison_status'].value, 'skipped_no_approved_baseline')
            drift = approved_baseline_drift(after, {**before, 'release_accepted': True})
            self.assertFalse(drift.passed)
            self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
            self.assertEqual(drift.metadata['feature_count_delta'].value, 1)

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


class _RemoteStorage(StagingStorageResource):
    """In-memory remote staging double serving the remote storage contract."""

    def __init__(self, objects, prefix=""):
        super().__init__(bucket="remote-test-bucket", use_local=False, prefix=prefix)
        object.__setattr__(self, "objects",
                           {self._ensure_prefixed(key): body for key, body in objects.items()})
        object.__setattr__(self, "writes", 0)

    def list_prefix(self, key_prefix=""):
        physical = self._ensure_prefixed(key_prefix).rstrip("/")
        return sorted(key for key in self.objects
                      if not physical or key.startswith(physical + "/"))

    def object_snapshot(self, key):
        key = self._ensure_prefixed(key)
        if key not in self.objects:
            return None
        body = self.objects[key]
        return StorageObjectSnapshot(key=key, size=len(body), generation="1",
                                     md5=hashlib.md5(body, usedforsecurity=False).hexdigest(),
                                     crc32c=None, sha256=hashlib.sha256(body).hexdigest())

    def read_key(self, key):
        return self.objects[self._ensure_prefixed(key)]

    def sha256_key(self, key):
        return hashlib.sha256(self.objects[self._ensure_prefixed(key)]).hexdigest()

    def write_key_if_unchanged(self, key, data, expected_snapshot):
        object.__setattr__(self, "writes", self.writes + 1)
        key = self._ensure_prefixed(key)
        current = self.object_snapshot(key)
        if current != expected_snapshot:
            raise RuntimeError(
                f"destination {'created' if expected_snapshot is None else 'changed'} before write: {key}")
        self.objects[key] = data
        return self.object_snapshot(key)


def _counting_client(requests):
    def handle(request):
        requests.append(str(request.url))
        return httpx.Response(200, stream=httpx.ByteStream(b"unused-by-replay"))
    return httpx.Client(transport=httpx.MockTransport(handle))


class ElectricRetailTerritoryReplayTests(unittest.TestCase):
    def test_replay_retained_candidate_uses_zero_network_and_zero_writes(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(source, "MIN_COUNTY_COUNT", 1):
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "stage"))
            with _client() as client:
                first = source.capture_electric_retail_service_territories(
                    storage, client=client, capture_directory=str(Path(tmp) / "capture"))
            staged = {key: storage.read_key(key) for key in storage.list_prefix()}
            requests = []
            with _counting_client(requests) as counting, patch.object(
                    StagingStorageResource, "write_key_if_unchanged",
                    side_effect=AssertionError("replay attempted a staged write")):
                replayed = source.capture_electric_retail_service_territories(
                    storage, client=counting, replay_version=first["version"])
            self.assertEqual(requests, [])
            self.assertEqual(replayed, first)
            self.assertEqual({key: storage.read_key(key) for key in storage.list_prefix()}, staged)

    def test_replay_fails_closed_without_rewriting_retained_evidence(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(source, "MIN_COUNTY_COUNT", 1):
            stage_root = Path(tmp) / "stage"
            storage = StagingStorageResource(use_local=True, local_dir=str(stage_root))
            with _client() as client:
                first = source.capture_electric_retail_service_territories(
                    storage, client=client, capture_directory=str(Path(tmp) / "capture"))
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            staged = {key: storage.read_key(key) for key in storage.list_prefix()}
            zip_body = _eia_zip(added_utility=True)
            tampered = json.loads(staged[prefix + "/raw/eia861_zip.receipt.json"])
            tampered.update(bytes=len(zip_body), sha256=hashlib.sha256(zip_body).hexdigest(),
                            wire_bytes=len(zip_body), wire_sha256=hashlib.sha256(zip_body).hexdigest())
            cases = [
                # Forged: receipt names a different endpoint than the producer page.
                (((prefix + "/raw/eia861_page.receipt.json",
                   staged[prefix + "/raw/eia861_page.receipt.json"].replace(b"https://", b"http://", 1)),),
                 "retained_capture_request_identity_mismatch"),
                # Forged: receipt bytes no longer self-canonicalize.
                (((prefix + "/raw/eia_reuse_policy.receipt.json",
                   staged[prefix + "/raw/eia_reuse_policy.receipt.json"] + b" "),),
                 "retained_capture_receipt_not_canonical"),
                # Truncated body.
                (((prefix + "/raw/tiger_county_zip.body",
                   staged[prefix + "/raw/tiger_county_zip.body"][:-100]),),
                 "retained_capture_body_mismatch"),
                # Truncated wire.
                (((prefix + "/raw/eia861_page.wire",
                   staged[prefix + "/raw/eia861_page.wire"][:-1]),),
                 "retained_capture_wire_mismatch"),
                # Partial: one raw object removed.
                (((prefix + "/raw/eia861_zip.body", None),),
                 "electric_territory_replay_roster_changed"),
                # Partial: the ready seal removed.
                (((prefix + "/metadata/candidate_ready.json", None),),
                 "electric_territory_replay_candidate_incomplete"),
                # Foreign object inside the candidate.
                (((prefix + "/raw/eia861_page.extra", b"foreign"),),
                 "electric_territory_replay_roster_changed"),
                # Conflicting: staged manifest no longer matches the retained evidence.
                (((prefix + "/metadata/source_manifest.json",
                   staged[prefix + "/metadata/source_manifest.json"].replace(
                       b"County-level approximation only", b"Altered coverage claim")),),
                 "electric_territory_replay_candidate_conflict"),
                # Conflicting: self-consistent retained evidence that re-derives another version.
                (((prefix + "/raw/eia861_zip.body", zip_body),
                  (prefix + "/raw/eia861_zip.wire", zip_body),
                  (prefix + "/raw/eia861_zip.receipt.json", source._canonical(tampered) + b"\n")),
                 "electric_territory_replay_version_conflict"),
            ]
            for mutations, error in cases:
                with self.subTest(error=error):
                    for key, data in mutations:
                        path = stage_root / key
                        if data is None:
                            path.unlink()
                        else:
                            path.write_bytes(data)
                    try:
                        with patch.object(StagingStorageResource, "write_key_if_unchanged",
                                side_effect=AssertionError("failed replay attempted a staged write")):
                            with self.assertRaisesRegex(ValueError, error):
                                source.capture_electric_retail_service_territories(
                                    storage, replay_version=first["version"])
                    finally:
                        for key, data in mutations:
                            path = stage_root / key
                            if key in staged:
                                path.write_bytes(staged[key])
                            elif data is not None:
                                path.unlink(missing_ok=True)
            self.assertEqual({key: storage.read_key(key) for key in staged}, staged)

    def test_replay_refuses_unsafe_missing_or_conflicting_versions(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(source, "MIN_COUNTY_COUNT", 1):
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "stage"))
            for bad in ("", ".", "..", "a/b", "v" * 129, "../evil"):
                with self.subTest(version=repr(bad)):
                    with self.assertRaisesRegex(ValueError, "unsafe_electric_retail_replay_version"):
                        source.capture_electric_retail_service_territories(
                            storage, replay_version=bad)
            with self.assertRaisesRegex(ValueError, "electric_territory_replay_candidate_incomplete"):
                source.capture_electric_retail_service_territories(
                    storage, replay_version="v" + "1" * 32)

    def test_replay_refuses_published_storage(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "explicit_staging_resource_required"):
                source.capture_electric_retail_service_territories(
                    PublishedStorageResource(use_local=True, local_dir=str(Path(tmp) / "pub")),
                    replay_version="v" + "0" * 32)

    def test_replay_honours_remote_staging_contract(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(source, "MIN_COUNTY_COUNT", 1):
            local = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "stage"))
            with _client() as client:
                first = source.capture_electric_retail_service_territories(
                    local, client=client, capture_directory=str(Path(tmp) / "capture"))
            objects = {key: local.read_key(key) for key in local.list_prefix()}
            for prefix in ("", "hifld"):
                with self.subTest(prefix=prefix):
                    remote = _RemoteStorage(objects, prefix=prefix)
                    requests = []
                    with _counting_client(requests) as counting:
                        replayed = source.capture_electric_retail_service_territories(
                            remote, client=counting, replay_version=first["version"])
                    self.assertEqual(requests, [])
                    self.assertEqual(replayed, first)
                    self.assertEqual(remote.writes, 0)
                    self.assertEqual(remote.objects,
                                     {remote._ensure_prefixed(key): body
                                      for key, body in objects.items()})

    def test_asset_replay_config_replays_retained_candidate_offline(self):
        with tempfile.TemporaryDirectory() as tmp, DagsterInstance.ephemeral() as instance, \
                patch.object(source, "MIN_COUNTY_COUNT", 1):
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with _client() as client:
                first = source.capture_electric_retail_service_territories(
                    storage, client=client, capture_directory=str(Path(tmp) / "capture"))
            requests = []
            with patch.object(source.httpx, "Client",
                    side_effect=lambda *a, **k: _counting_client(requests)):
                result = materialize(
                    [assets.electric_retail_service_territories_eia861_candidate],
                    instance=instance, resources={"staging_storage": storage},
                    run_config={"ops": {
                        assets.electric_retail_service_territories_eia861_candidate.op.name: {
                            "config": {"replay_version": first["version"]}}}})
            self.assertTrue(result.success)
            self.assertEqual(requests, [])
            self.assertEqual(result.output_for_node(
                assets.electric_retail_service_territories_eia861_candidate.op.name), first)

    def test_tiger_listing_derivation_matches_live_discovery(self):
        from dagster_hifld.sources import census_latest

        annual = b'<a href="TIGER2025/">2025</a><a href="TIGER2026/">2026</a>'
        product = b'<a href="tl_2026_us_county.zip">zip</a>'
        transport = httpx.MockTransport(lambda request: httpx.Response(
            200, stream=httpx.ByteStream(
                annual if str(request.url) == source.TIGER_ROOT else product)))
        with tempfile.TemporaryDirectory() as tmp, httpx.Client(transport=transport) as client:
            discovered = census_latest.discover_county_boundaries(client, Path(tmp))
        derived = census_latest.annual_zip_release_from_listings(
            annual, product, product="COUNTY")
        for field in ("edition", "source_url", "listing_url"):
            self.assertEqual(derived[field], discovered[field])


if __name__ == "__main__":
    unittest.main()
