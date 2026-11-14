"""Focused GSA IOLP Buildings source-contract, decode and validation checks."""
from __future__ import annotations

import tempfile
import unittest
from unittest.mock import patch
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio

from dagster_hifld.assets.gsa_iolp_buildings import (
    gsa_iolp_buildings_complete, gsa_iolp_buildings_draft_schedule,
)
from dagster_hifld.sources.gsa_iolp_buildings import (
    CATALOG_IDENTIFIER, CATALOG_LICENSE, CATALOG_URL, CKAN_DATASET_ID, DATASET_SLUG,
    EXPECTED_FIELDS, FIELD_NAMES, FILE_SLUG, LANDING_URL, GsaIolpBuildingsError,
    _decode_workbook, _ogr_types, _parquet, _point_wkb,
    parse_release_date, select_buildings_distribution,
)

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _catalog_record(title="2026 - 11 - 6 - IOLP Buildings.xlsx",
                    modified="2026-11-06T15:49:19.926Z", distributions=None):
    return {"results": [{"_score": 0.0, "dcat": {
        "identifier": CATALOG_IDENTIFIER,
        "publisher": {"name": "General Services Administration"},
        "accrualPeriodicity": "R/P1W",
        "license": CATALOG_LICENSE,
        "modified": modified,
        "distribution": distributions or [
            {"mediaType": "application/xml",
             "title": "IOLP19139_07092026.xml",
             "downloadURL": f"https://inventory.data.gov/dataset/{CKAN_DATASET_ID}/"
                            "resource/76d0c902-54de-4483-a5ad-5869ae474e0f/download/"
                            "iolp19139_07092026.xml"},
            {"mediaType": XLSX_MEDIA_TYPE,
             "title": title,
             "downloadURL": f"https://inventory.data.gov/dataset/{CKAN_DATASET_ID}/"
                            "resource/c9db0ffa-d55b-4dc4-b62a-597c72737deb/download/"
                            "2026-11-6-iolp-buildings.xlsx"},
            {"mediaType": XLSX_MEDIA_TYPE,
             "title": "2026 - 11 - 6 - IOLP Leases.xlsx",
             "downloadURL": f"https://inventory.data.gov/dataset/{CKAN_DATASET_ID}/"
                            "resource/d05232d8-dc36-46e0-905b-275413399499/download/"
                            "2026-11-6-iolp-leases.xlsx"},
        ]}}]}


def _rows():
    return [
        {"Location Code": "AZ0715", "Real Property Asset Name": "SAN LUIS 1 VAULT",
         "Installation Name": "SAN LUIS I LPOE", "Owned or Leased": "OWNED", "GSA Region": 9,
         "Street Address": "431 MAIN STREET", "City": "SAN LUIS", "State": "AZ",
         "Zip Code": "85349-8534", "Latitude": 31.766351, "Longitude": -96.330486,
         "Building Rentable Square Feet": 0.25, "Available Square Feet": None,
         "Construction Date": 2026, "Congressional District": "4806",
         "Congressional District Representative Name": "JAKE ELLZEY",
         "Building Status": "ACTIVE", "Real Property Asset Type": "BUILDING"},
        {"Location Code": "MA0113", "Real Property Asset Name": "SUFFOLK COUNTY COURTHOUSE",
         "Installation Name": None, "Owned or Leased": "LEASED", "GSA Region": 1,
         "Street Address": "3 PEMBERTON SQUARE", "City": "BOSTON", "State": "MA",
         "Zip Code": "02114", "Latitude": 42.35913, "Longitude": -71.05712,
         "Building Rentable Square Feet": 551200.5, "Available Square Feet": 12345.75,
         "Construction Date": None, "Congressional District": None,
         "Congressional District Representative Name": None,
         "Building Status": "ACTIVE", "Real Property Asset Type": "BUILDING"},
    ]


def _workbook(rows=None, fields=None):
    rows = _rows() if rows is None else rows
    fields = fields or FIELD_NAMES
    return pd.DataFrame(rows, columns=list(fields))


class GsaIolpBuildingsContractTest(unittest.TestCase):
    def test_source_chain_pins_official_gsa_endpoints(self):
        self.assertEqual(DATASET_SLUG, "gsa-iolp-buildings")
        self.assertEqual(FILE_SLUG, "gsa-iolp-buildings")
        self.assertTrue(LANDING_URL.startswith("https://www.gsa.gov/"))
        self.assertEqual(CATALOG_URL,
                         "https://catalog.data.gov/api/dataset/"
                         "inventory-of-owned-and-leased-properties-iolp")
        self.assertEqual(CKAN_DATASET_ID, "9a9e946e-124e-46f5-a934-e458a6c1c2b2")
        self.assertEqual(CATALOG_IDENTIFIER, "GSA-4495")
        self.assertEqual([name for name, _ in EXPECTED_FIELDS], [
            "Location Code", "Real Property Asset Name", "Installation Name",
            "Owned or Leased", "GSA Region", "Street Address", "City", "State",
            "Zip Code", "Latitude", "Longitude", "Building Rentable Square Feet",
            "Available Square Feet", "Construction Date", "Congressional District",
            "Congressional District Representative Name", "Building Status",
            "Real Property Asset Type"])
        self.assertEqual(_ogr_types(), [
            ("Location Code", "OFTString"), ("Real Property Asset Name", "OFTString"),
            ("Installation Name", "OFTString"), ("Owned or Leased", "OFTString"),
            ("GSA Region", "OFTInteger"), ("Street Address", "OFTString"),
            ("City", "OFTString"), ("State", "OFTString"), ("Zip Code", "OFTString"),
            ("Latitude", "OFTReal"), ("Longitude", "OFTReal"),
            ("Building Rentable Square Feet", "OFTReal"),
            ("Available Square Feet", "OFTReal"), ("Construction Date", "OFTInteger"),
            ("Congressional District", "OFTString"),
            ("Congressional District Representative Name", "OFTString"),
            ("Building Status", "OFTString"), ("Real Property Asset Type", "OFTString")])

    def test_release_date_parsed_from_distribution_title(self):
        self.assertEqual(parse_release_date("2026 - 11 - 6 - IOLP Buildings.xlsx"), "2026-11-06")
        self.assertEqual(parse_release_date("2025 - 9 - 25 - IOLP Buildings.xlsx"), "2025-09-25")
        with self.assertRaises(GsaIolpBuildingsError):
            parse_release_date("IOLP Buildings.xlsx")
        with self.assertRaises(GsaIolpBuildingsError):
            parse_release_date("2026 - 13 - 6 - IOLP Buildings.xlsx")

    def test_distribution_selection_requires_all_identity_markers(self):
        selected = select_buildings_distribution(_catalog_record())
        self.assertEqual(selected["title"], "2026 - 11 - 6 - IOLP Buildings.xlsx")
        self.assertEqual(selected["release_date"], "2026-11-06")
        self.assertEqual(selected["catalog_modified"], "2026-11-06T15:49:19.926Z")
        self.assertEqual(selected["cadence"], "R/P1W")
        self.assertEqual(selected["license"], CATALOG_LICENSE)
        self.assertIn("resource/c9db0ffa", selected["download_url"])

    def test_distribution_selection_rejects_drift(self):
        wrong_title = _catalog_record(title="2026 - 11 - 6 - IOLP Leases.xlsx")
        with self.assertRaises(GsaIolpBuildingsError):
            select_buildings_distribution(wrong_title)
        with self.assertRaises(GsaIolpBuildingsError):
            select_buildings_distribution(_catalog_record(modified="not-a-timestamp"))
        leases_only = _catalog_record(distributions=[
            {"mediaType": XLSX_MEDIA_TYPE, "title": "2026 - 11 - 6 - IOLP Leases.xlsx",
             "downloadURL": "https://inventory.data.gov/x.xlsx"}])
        with self.assertRaises(GsaIolpBuildingsError):
            select_buildings_distribution(leases_only)
        record = _catalog_record()
        record["results"][0]["dcat"]["identifier"] = "OTHER-1"
        with self.assertRaises(GsaIolpBuildingsError):
            select_buildings_distribution(record)
        record = _catalog_record()
        record["results"][0]["dcat"]["publisher"]["name"] = "Some Mirror LLC"
        with self.assertRaises(GsaIolpBuildingsError):
            select_buildings_distribution(record)
        record = _catalog_record()
        record["results"][0]["dcat"]["distribution"][1]["downloadURL"] = \
            "https://mirror.example.com/iolp-buildings.xlsx"
        with self.assertRaises(GsaIolpBuildingsError):
            select_buildings_distribution(record)
        record = _catalog_record()
        record["results"][0]["dcat"]["accrualPeriodicity"] = "R/P1Y"
        with self.assertRaises(GsaIolpBuildingsError):
            select_buildings_distribution(record)
        record = _catalog_record()
        record["results"][0]["dcat"]["license"] = "https://example.com/not-cc0"
        with self.assertRaises(GsaIolpBuildingsError):
            select_buildings_distribution(record)
        record = _catalog_record(modified="2026-11-01T00:00:00Z")
        with self.assertRaises(GsaIolpBuildingsError):
            select_buildings_distribution(record)

    def test_geometry_encoding_preserves_native_xy(self):
        self.assertEqual(_point_wkb(1.5, -2.25).hex(),
                         "0101000000000000000000f83f00000000000002c0")


class GsaIolpBuildingsDecodeTest(unittest.TestCase):
    def _write_workbook(self, root: Path, rows=None, fields=None, sheet="Sheet1") -> str:
        path = root / "iolp_buildings.xlsx"
        pyogrio.write_dataframe(_workbook(rows, fields), str(path), driver="XLSX", layer=sheet)
        return path

    def test_decode_roundtrip_and_parquet_contract(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = self._write_workbook(root)
            with patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MAX", 100):
                decoded = _decode_workbook(path.read_bytes(), root)
            audit = decoded["audit"]
            self.assertEqual(decoded["count"], 2)
            self.assertEqual(audit["source_feature_count"], 2)
            self.assertEqual(audit["source_population_complete"], True)
            self.assertEqual(audit["source_id_field"], "Location Code")
            self.assertEqual(audit["source_id_unique"], True)
            self.assertEqual(audit["source_crs"], "EPSG:4326")
            self.assertEqual(audit["source_geometry_type"], "Point")
            self.assertEqual(audit["source_null_counts"]["Zip Code"], 0)
            self.assertEqual(audit["source_null_counts"]["Available Square Feet"], 1)
            self.assertEqual(audit["source_null_counts"]["Construction Date"], 1)
            self.assertEqual(audit["source_value_distributions"]["Owned or Leased"],
                             {"LEASED": 1, "OWNED": 1})
            parquet = _parquet(decoded["columns"], decoded["count"])
            table = pq.read_table(pa.BufferReader(parquet))
            self.assertEqual(table.num_rows, 2)
            zips = table.column("Zip Code").to_pylist()
            self.assertEqual(zips, ["85349-8534", "02114"])
            self.assertTrue(zips[1].startswith("0"))
            geometry_meta = table.schema.metadata[b"geo"]
            self.assertIn(b'"encoding":"WKB"', geometry_meta)
            self.assertIn(b"WGS 84", geometry_meta)
            first = table.slice(0, 1).to_pylist()[0]
            self.assertEqual(first["Location Code"], "AZ0715")
            self.assertEqual(first["Construction Date"], 2026)
            self.assertIsNone(first["Available Square Feet"])
            self.assertEqual(str(table.schema.field("GSA Region").type), "int32")
            self.assertEqual(str(table.schema.field("Construction Date").type), "int32")

    def test_decode_rejects_id_and_value_drift(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = _rows()
            rows[1]["Location Code"] = rows[1]["Location Code"].lower()
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpBuildingsError):
                _decode_workbook(path.read_bytes(), root)
            rows = _rows()
            rows[1]["Location Code"] = rows[0]["Location Code"]
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpBuildingsError):
                _decode_workbook(path.read_bytes(), root)
            rows = _rows()
            rows[0]["Latitude"] = 95.0
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpBuildingsError):
                _decode_workbook(path.read_bytes(), root)
            rows = _rows()
            rows[0]["Congressional District"] = "48A6"
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpBuildingsError):
                _decode_workbook(path.read_bytes(), root)
            rows = _rows()
            rows[0]["Construction Date"] = 950
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpBuildingsError):
                _decode_workbook(path.read_bytes(), root)

    def test_decode_rejects_schema_drift(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            renamed = [name if name != "Location Code" else "Building ID"
                       for name in FIELD_NAMES]
            rows = [dict(row, **{"Building ID": row.pop("Location Code")}) for row in _rows()]
            path = self._write_workbook(root, rows, renamed)
            with self.assertRaises(GsaIolpBuildingsError):
                _decode_workbook(path.read_bytes(), root)

    def test_decode_rejects_wrong_sheet_magic_or_type(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = self._write_workbook(root, sheet="Data")
            with self.assertRaises(GsaIolpBuildingsError):
                _decode_workbook(path.read_bytes(), root)
            with self.assertRaises(GsaIolpBuildingsError):
                _decode_workbook(b"NOTZIP", root)
            archive = root / "not_xlsx.zip"
            with zipfile.ZipFile(archive, "w") as sink:
                sink.writestr("sheet.xml", "<x/>")
            with self.assertRaises(GsaIolpBuildingsError):
                _decode_workbook(archive.read_bytes(), root)
            rows = _rows()
            rows[0]["GSA Region"] = "Nine"
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_buildings.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpBuildingsError):
                _decode_workbook(path.read_bytes(), root)


class GsaIolpBuildingsAssetTest(unittest.TestCase):
    def test_refresh_schedule_is_stopped(self):
        self.assertEqual(gsa_iolp_buildings_draft_schedule.default_status.value, "STOPPED")

    def test_blocking_check_refuses_incomplete_candidate(self):
        result = gsa_iolp_buildings_complete({"source_population_complete": False,
                                              "feature_count": 0})
        self.assertFalse(result.passed)
        result = gsa_iolp_buildings_complete({"source_population_complete": True,
                                              "feature_count": 7733,
                                              "source_release_date": "2026-11-06"})
        self.assertTrue(result.passed)
        metadata = result.metadata
        for flag in ("source_release_accepted", "source_freshness_accepted",
                     "schema_contract_accepted", "id_contract_accepted",
                     "coverage_accepted", "rights_accepted",
                     "baseline_accepted", "release_accepted"):
            self.assertFalse(metadata[flag].value)


if __name__ == "__main__":
    unittest.main()
