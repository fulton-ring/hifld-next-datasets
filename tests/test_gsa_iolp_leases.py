"""Focused GSA IOLP Leases source-contract, decode and validation checks."""
from __future__ import annotations

import tempfile
import unittest
from unittest.mock import patch
import zipfile
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio

from dagster_hifld.assets.gsa_iolp_leases import (
    gsa_iolp_leases_complete, gsa_iolp_leases_draft_schedule,
)
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.gsa_iolp_leases import (
    CATALOG_IDENTIFIER, CATALOG_LICENSE, CATALOG_URL, CKAN_DATASET_ID, DATASET_SLUG,
    EXPECTED_FIELDS, FIELD_NAMES, FILE_SLUG, LANDING_URL, GsaIolpLeasesError,
    _capture, _decode_workbook, _ogr_types, _parquet, _point_wkb,
    parse_release_date, select_leases_distribution,
)

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


class _BodyStream(httpx.SyncByteStream):
    def __init__(self, body: bytes):
        self.body = body

    def __iter__(self):
        yield self.body

    def close(self):
        return None


def _catalog_record(title="2026 - 11 - 6 - IOLP Leases.xlsx",
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
             "title": "2026 - 11 - 6 - IOLP Buildings.xlsx",
             "downloadURL": f"https://inventory.data.gov/dataset/{CKAN_DATASET_ID}/"
                            "resource/c9db0ffa-d55b-4dc4-b62a-597c72737deb/download/"
                            "2026-11-6-iolp-buildings.xlsx"},
            {"mediaType": XLSX_MEDIA_TYPE,
             "title": title,
             "downloadURL": f"https://inventory.data.gov/dataset/{CKAN_DATASET_ID}/"
                            "resource/d05232d8-dc36-46e0-905b-275413399499/download/"
                            "2026-11-6-iolp-leases.xlsx"},
        ]}}]}


def _rows():
    # Two leases in the same GSA building intentionally share the Location Code
    # and coordinates while each keeps its own unique Lease Number.
    return [
        {"Location Code": "CT3360", "Real Property Asset Name": "141 WEST MAIN STREET",
         "Installation Name": None, "Federal Leased Code": "L", "GSA Region": 1,
         "Street Address": "141 WEST MAIN STREET", "City": "NORWICH", "State": "CT",
         "Zip Code": "06360-5602", "Latitude": 41.52426, "Longitude": -72.08019,
         "Building Rentable Square Feet": 12450.0, "Available Square Feet": None,
         "Congressional District": "0202",
         "Congressional District Representative": "JOE COURTNEY",
         "Lease Number": "LCT00005", "Lease Effective Date": "7/27/2019",
         "Lease Expiration Date": "7/26/2034", "Real Property Asset type": "BUILDING"},
        {"Location Code": "CT3360", "Real Property Asset Name": "141 WEST MAIN STREET",
         "Installation Name": None, "Federal Leased Code": "L", "GSA Region": 1,
         "Street Address": "141 WEST MAIN STREET", "City": "NORWICH", "State": "CT",
         "Zip Code": "06360-5602", "Latitude": 41.52426, "Longitude": -72.08019,
         "Building Rentable Square Feet": 8155.25, "Available Square Feet": 1200.5,
         "Congressional District": None,
         "Congressional District Representative": None,
         "Lease Number": "LCT00130", "Lease Effective Date": "2/15/2022",
         "Lease Expiration Date": "2/14/2032", "Real Property Asset type": "BUILDING"},
    ]


def _workbook(rows=None, fields=None):
    rows = _rows() if rows is None else rows
    fields = fields or FIELD_NAMES
    return pd.DataFrame(rows, columns=list(fields))


class GsaIolpLeasesContractTest(unittest.TestCase):
    def test_source_chain_pins_official_gsa_endpoints(self):
        self.assertEqual(DATASET_SLUG, "gsa-iolp-leases")
        self.assertEqual(FILE_SLUG, "gsa-iolp-leases")
        self.assertTrue(LANDING_URL.startswith("https://www.gsa.gov/"))
        self.assertEqual(CATALOG_URL,
                         "https://catalog.data.gov/api/dataset/"
                         "inventory-of-owned-and-leased-properties-iolp")
        self.assertEqual(CKAN_DATASET_ID, "9a9e946e-124e-46f5-a934-e458a6c1c2b2")
        self.assertEqual(CATALOG_IDENTIFIER, "GSA-4495")
        self.assertEqual([name for name, _ in EXPECTED_FIELDS], [
            "Location Code", "Real Property Asset Name", "Installation Name",
            "Federal Leased Code", "GSA Region", "Street Address", "City", "State",
            "Zip Code", "Latitude", "Longitude", "Building Rentable Square Feet",
            "Available Square Feet", "Congressional District",
            "Congressional District Representative", "Lease Number",
            "Lease Effective Date", "Lease Expiration Date", "Real Property Asset type"])
        self.assertEqual(_ogr_types(), [
            ("Location Code", "OFTString"), ("Real Property Asset Name", "OFTString"),
            ("Installation Name", "OFTString"), ("Federal Leased Code", "OFTString"),
            ("GSA Region", "OFTInteger"), ("Street Address", "OFTString"),
            ("City", "OFTString"), ("State", "OFTString"), ("Zip Code", "OFTString"),
            ("Latitude", "OFTReal"), ("Longitude", "OFTReal"),
            ("Building Rentable Square Feet", "OFTReal"),
            ("Available Square Feet", "OFTReal"), ("Congressional District", "OFTString"),
            ("Congressional District Representative", "OFTString"),
            ("Lease Number", "OFTString"), ("Lease Effective Date", "OFTString"),
            ("Lease Expiration Date", "OFTString"), ("Real Property Asset type", "OFTString")])

    def test_release_date_parsed_from_distribution_title(self):
        self.assertEqual(parse_release_date("2026 - 11 - 6 - IOLP Leases.xlsx"), "2026-11-06")
        self.assertEqual(parse_release_date("2025 - 9 - 25 - IOLP Leases.xlsx"), "2025-09-25")
        with self.assertRaises(GsaIolpLeasesError):
            parse_release_date("IOLP Leases.xlsx")
        with self.assertRaises(GsaIolpLeasesError):
            parse_release_date("2026 - 13 - 6 - IOLP Leases.xlsx")

    def test_distribution_selection_requires_all_identity_markers(self):
        selected = select_leases_distribution(_catalog_record())
        self.assertEqual(selected["title"], "2026 - 11 - 6 - IOLP Leases.xlsx")
        self.assertEqual(selected["release_date"], "2026-11-06")
        self.assertEqual(selected["catalog_modified"], "2026-11-06T15:49:19.926Z")
        self.assertEqual(selected["cadence"], "R/P1W")
        self.assertEqual(selected["license"], CATALOG_LICENSE)
        self.assertIn("resource/d05232d8", selected["download_url"])

    def test_distribution_selection_rejects_drift(self):
        wrong_title = _catalog_record(title="2026 - 11 - 6 - IOLP Buildings.xlsx")
        with self.assertRaises(GsaIolpLeasesError):
            select_leases_distribution(wrong_title)
        with self.assertRaises(GsaIolpLeasesError):
            select_leases_distribution(_catalog_record(modified="not-a-timestamp"))
        buildings_only = _catalog_record(distributions=[
            {"mediaType": XLSX_MEDIA_TYPE, "title": "2026 - 11 - 6 - IOLP Buildings.xlsx",
             "downloadURL": "https://inventory.data.gov/x.xlsx"}])
        with self.assertRaises(GsaIolpLeasesError):
            select_leases_distribution(buildings_only)
        record = _catalog_record()
        record["results"][0]["dcat"]["identifier"] = "OTHER-1"
        with self.assertRaises(GsaIolpLeasesError):
            select_leases_distribution(record)
        record = _catalog_record()
        record["results"][0]["dcat"]["publisher"]["name"] = "Some Mirror LLC"
        with self.assertRaises(GsaIolpLeasesError):
            select_leases_distribution(record)
        record = _catalog_record()
        record["results"][0]["dcat"]["distribution"][2]["downloadURL"] = \
            "https://mirror.example.com/iolp-leases.xlsx"
        with self.assertRaises(GsaIolpLeasesError):
            select_leases_distribution(record)
        record = _catalog_record()
        record["results"][0]["dcat"]["accrualPeriodicity"] = "R/P1Y"
        with self.assertRaises(GsaIolpLeasesError):
            select_leases_distribution(record)
        record = _catalog_record()
        record["results"][0]["dcat"]["license"] = "https://example.com/not-cc0"
        with self.assertRaises(GsaIolpLeasesError):
            select_leases_distribution(record)
        record = _catalog_record(modified="2026-11-01T00:00:00Z")
        with self.assertRaises(GsaIolpLeasesError):
            select_leases_distribution(record)

    def test_geometry_encoding_preserves_native_xy(self):
        self.assertEqual(_point_wkb(1.5, -2.25).hex(),
                         "0101000000000000000000f83f00000000000002c0")


class GsaIolpLeasesDecodeTest(unittest.TestCase):
    def _write_workbook(self, root: Path, rows=None, fields=None, sheet="Sheet1") -> str:
        path = root / "iolp_leases.xlsx"
        pyogrio.write_dataframe(_workbook(rows, fields), str(path), driver="XLSX", layer=sheet)
        return path

    def test_decode_roundtrip_and_parquet_contract(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = self._write_workbook(root)
            with patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MAX", 100):
                decoded = _decode_workbook(path.read_bytes(), root)
            audit = decoded["audit"]
            self.assertEqual(decoded["count"], 2)
            self.assertEqual(audit["source_feature_count"], 2)
            self.assertEqual(audit["source_population_complete"], True)
            self.assertEqual(audit["source_id_field"], "Lease Number")
            self.assertEqual(audit["source_id_unique"], True)
            self.assertEqual(audit["source_crs"], "EPSG:4326")
            self.assertEqual(audit["source_geometry_type"], "Point")
            self.assertEqual(audit["source_location_code_unique"], False)
            self.assertEqual(audit["source_duplicate_location_codes"], 1)
            self.assertEqual(audit["source_null_counts"]["Zip Code"], 0)
            self.assertEqual(audit["source_null_counts"]["Available Square Feet"], 1)
            self.assertEqual(audit["source_null_counts"]["Congressional District"], 1)
            self.assertEqual(audit["source_null_counts"]["Lease Effective Date"], 0)
            self.assertEqual(audit["source_value_distributions"]["Federal Leased Code"],
                             {"L": 2})
            self.assertEqual(audit["source_value_distributions"]["Real Property Asset type"],
                             {"BUILDING": 2})
            bounds = audit["source_bounds"]
            parquet = _parquet(decoded["columns"], decoded["count"],
                               [bounds["lon_min"], bounds["lat_min"],
                                bounds["lon_max"], bounds["lat_max"]])
            table = pq.read_table(pa.BufferReader(parquet))
            self.assertEqual(table.num_rows, 2)
            zips = table.column("Zip Code").to_pylist()
            self.assertEqual(zips, ["06360-5602", "06360-5602"])
            self.assertTrue(zips[0].startswith("0"))
            geometry_meta = table.schema.metadata[b"geo"]
            self.assertIn(b'"encoding":"WKB"', geometry_meta)
            self.assertIn(b"WGS 84", geometry_meta)
            first = table.slice(0, 1).to_pylist()[0]
            self.assertEqual(first["Lease Number"], "LCT00005")
            self.assertEqual(first["Lease Effective Date"], "7/27/2019")
            self.assertEqual(first["Lease Expiration Date"], "7/26/2034")
            self.assertIsNone(first["Available Square Feet"])
            self.assertEqual(table.column("Congressional District").to_pylist(),
                             ["0202", None])
            self.assertEqual(table.column("Congressional District Representative").to_pylist(),
                             ["JOE COURTNEY", None])
            self.assertEqual(str(table.schema.field("GSA Region").type), "int32")
            self.assertEqual(str(table.schema.field("Location Code").type), "string")

    def test_decode_accepts_repeated_location_codes_only_with_unique_ids(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = _rows()
            rows[1]["Lease Number"] = rows[0]["Lease Number"]
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(path.read_bytes(), root)

    def test_decode_rejects_id_and_value_drift(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rows = _rows()
            rows[0]["Lease Number"] = "CT00005"
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(path.read_bytes(), root)
            rows = _rows()
            rows[0]["Location Code"] = "ct3360"
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(path.read_bytes(), root)
            rows = _rows()
            rows[0]["Latitude"] = 95.0
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(path.read_bytes(), root)
            rows = _rows()
            rows[0]["Congressional District"] = "02A2"
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(path.read_bytes(), root)
            rows = _rows()
            rows[0]["Lease Effective Date"] = "2019-07-27"
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(path.read_bytes(), root)
            rows = _rows()
            rows[0]["Lease Expiration Date"] = None
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(path.read_bytes(), root)

    def test_decode_rejects_schema_drift(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            renamed = [name if name != "Lease Number" else "Lease ID"
                       for name in FIELD_NAMES]
            rows = [dict(row, **{"Lease ID": row.pop("Lease Number")}) for row in _rows()]
            path = self._write_workbook(root, rows, renamed)
            with self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(path.read_bytes(), root)

    def test_decode_rejects_wrong_sheet_magic_or_type(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = self._write_workbook(root, sheet="Data")
            with self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(path.read_bytes(), root)
            with self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(b"NOTZIP", root)
            archive = root / "not_xlsx.zip"
            with zipfile.ZipFile(archive, "w") as sink:
                sink.writestr("sheet.xml", "<x/>")
            with self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(archive.read_bytes(), root)
            rows = _rows()
            rows[0]["GSA Region"] = "One"
            path = self._write_workbook(root, rows)
            with patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MIN", 0), \
                    patch("dagster_hifld.sources.gsa_iolp_leases.COUNT_MAX", 100), \
                    self.assertRaises(GsaIolpLeasesError):
                _decode_workbook(path.read_bytes(), root)


class GsaIolpLeasesCaptureReplayTest(unittest.TestCase):
    """Sealed captures replay byte-identically without refetch and quarantine failures.

    Replay must re-derive the distribution identity from the captured catalog
    record itself; a manifest-only identity edit must be refused even though the
    manifest is outside the artifact digest roster.
    """

    def _seal_capture(self, root: Path) -> dict:
        root.mkdir(parents=True)
        identity = select_leases_distribution(_catalog_record())
        catalog_body = json.dumps(_catalog_record()).encode("utf-8")
        artifacts = {
            "catalog_discovery.body": catalog_body,
            "catalog_discovery.wire": catalog_body,
            "leases_xlsx.body": b"PK\x03\x04leases",
            "leases_xlsx.wire": b"PK\x03\x04leases",
            "leases_xlsx.redirect.wire": b"redirect",
        }
        for name, body in artifacts.items():
            (root / name).write_bytes(body)
        catalog_receipt = {"method": "GET", "url": CATALOG_URL, "final_url": CATALOG_URL,
                           "observed_at": "2026-11-14T00:00:00+00:00", "status": 200,
                           "complete_body": True,
                           "bytes": len(artifacts["catalog_discovery.body"]),
                           "sha256": hashlib.sha256(artifacts["catalog_discovery.body"]).hexdigest(),
                           "wire_bytes": len(artifacts["catalog_discovery.wire"]),
                           "wire_sha256": hashlib.sha256(artifacts["catalog_discovery.wire"]).hexdigest()}
        download_url = identity["download_url"]
        redirect_target = "https://s3-us-gov-west-1.example/redacted/2026-11-6-iolp-leases.xlsx"
        body_receipt = {"method": "GET", "url": redirect_target, "final_url": redirect_target,
                        "observed_at": "2026-11-14T00:00:01+00:00", "status": 200,
                        "complete_body": True,
                        "bytes": len(artifacts["leases_xlsx.body"]),
                        "sha256": hashlib.sha256(artifacts["leases_xlsx.body"]).hexdigest(),
                        "wire_bytes": len(artifacts["leases_xlsx.wire"]),
                        "wire_sha256": hashlib.sha256(artifacts["leases_xlsx.wire"]).hexdigest(),
                        "url_query_omitted": True, "error_code": None}
        redirect_receipt = {"method": "GET", "url": download_url, "final_url": redirect_target,
                            "observed_at": "2026-11-14T00:00:00+00:00", "status": 302,
                            "complete_body": True,
                            "wire_bytes": len(artifacts["leases_xlsx.redirect.wire"]),
                            "wire_sha256": hashlib.sha256(
                                artifacts["leases_xlsx.redirect.wire"]).hexdigest(),
                            "location_query_omitted": True}
        (root / "catalog_discovery.receipt.json").write_bytes(json.dumps(catalog_receipt).encode())
        (root / "leases_xlsx.receipt.json").write_bytes(json.dumps(body_receipt).encode())
        (root / "leases_xlsx.redirect.receipt.json").write_bytes(
            json.dumps(redirect_receipt).encode())
        roster = capture_roster_digests(root, exclude={"capture_manifest.json"})
        (root / "capture_manifest.json").write_bytes(json.dumps({
            "complete": True, "captured_at": "2026-11-14T00:00:02+00:00",
            "identity": identity, "artifact_roster": roster}).encode())
        return identity

    def test_sealed_capture_manifest_reloads_without_refetch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "sealed"
            identity = self._seal_capture(root)
            loaded = _capture(root, storage=None, version="v0.0.0-research-test")
            self.assertEqual(loaded["identity"], identity)
            self.assertEqual(loaded["body"], b"PK\x03\x04leases")

    def test_sealed_capture_rejects_tampered_artifact(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "sealed"
            self._seal_capture(root)
            (root / "leases_xlsx.body").write_bytes(b"PK\x03\x04tampered")
            with self.assertRaises(GsaIolpLeasesError):
                _capture(root, storage=None, version="v0.0.0-research-test")

    def test_reuse_rejects_manifest_identity_tamper(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "sealed"
            identity = self._seal_capture(root)
            loaded = _capture(root, storage=None, version="v0.0.0-research-test")
            self.assertEqual(loaded["identity"], identity)
            manifest_path = root / "capture_manifest.json"
            manifest = json.loads(manifest_path.read_bytes())
            manifest["identity"]["release_date"] = "2099-01-01"
            manifest_path.write_bytes(json.dumps(manifest).encode("utf-8"))
            with self.assertRaises(GsaIolpLeasesError) as ctx:
                _capture(root, storage=None, version="v0.0.0-research-test")
            self.assertEqual(str(ctx.exception), "capture_manifest_identity_drift")

    def test_capture_failure_quarantines_evidence_and_propagates(self):
        with tempfile.TemporaryDirectory() as temp:
            root_dir = Path(temp)
            storage = StagingStorageResource(use_local=True,
                                             local_dir=str(root_dir / "staging"),
                                             prefix="", backend="local")
            root = root_dir / "capture"
            catalog_body = json.dumps(_catalog_record()).encode("utf-8")
            signed = ("https://inventory-filestore.s3.us-gov-west-1.amazonaws.com/"
                      f"{CKAN_DATASET_ID}/d05232d8-dc36-46e0-905b-275413399499/"
                      "2026-11-6-iolp-leases.xlsx?X-Amz-Signature=sig")
            download_path = (f"/dataset/{CKAN_DATASET_ID}/"
                             "resource/d05232d8-dc36-46e0-905b-275413399499/"
                             "download/2026-11-6-iolp-leases.xlsx")

            def respond(request: httpx.Request) -> httpx.Response:
                url = str(request.url)
                if url == CATALOG_URL:
                    return httpx.Response(200, stream=_BodyStream(catalog_body))
                if urlsplit(url).path == download_path:
                    return httpx.Response(302, stream=_BodyStream(b""),
                                          headers={"location": signed})
                if urlsplit(url).path == urlsplit(signed).path:
                    return httpx.Response(404, stream=_BodyStream(b"forbidden"))
                raise AssertionError("unrouted request " + url)

            real_client = httpx.Client

            def run() -> dict:
                with patch("dagster_hifld.sources.gsa_iolp_leases.httpx.Client",
                           side_effect=lambda **kwargs: real_client(
                               transport=httpx.MockTransport(respond), **kwargs)):
                    return _capture(root, storage, "vtest")

            with self.assertRaises(GsaIolpLeasesError) as ctx:
                run()
            self.assertEqual(str(ctx.exception), "ckan_download_file_status_not_ok")
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])
            self.assertEqual(manifest["error_code"], "GsaIolpLeasesError")
            tokens = sorted((root_dir / "staging" / "_quarantine" / DATASET_SLUG).glob("*"))
            self.assertEqual(len(tokens), 1)
            failure = json.loads((tokens[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"], "GsaIolpLeasesError")
            self.assertEqual(failure["captured_names"], ["catalog_discovery"])
            retained = (tokens[0] / "raw" / "catalog_discovery.body").read_bytes()
            self.assertEqual(json.loads(retained), _catalog_record())
            retained_receipt = json.loads(
                (tokens[0] / "raw" / "catalog_discovery.receipt.json").read_bytes())
            self.assertEqual(retained_receipt["url"], CATALOG_URL)
            self.assertTrue(retained_receipt["complete_body"])

            def deny(request: httpx.Request) -> httpx.Response:
                raise AssertionError("second network access attempted")

            with patch("dagster_hifld.sources.gsa_iolp_leases.httpx.Client",
                       side_effect=lambda **kwargs: real_client(
                           transport=httpx.MockTransport(deny), **kwargs)):
                with self.assertRaises(GsaIolpLeasesError) as replay:
                    run()
            self.assertEqual(str(replay.exception), "capture_manifest_incomplete")


class GsaIolpLeasesAssetTest(unittest.TestCase):
    def test_refresh_schedule_is_stopped(self):
        self.assertEqual(gsa_iolp_leases_draft_schedule.default_status.value, "STOPPED")

    def test_blocking_check_refuses_incomplete_candidate(self):
        result = gsa_iolp_leases_complete({"source_population_complete": False,
                                           "feature_count": 0})
        self.assertFalse(result.passed)
        result = gsa_iolp_leases_complete({"source_population_complete": True,
                                           "feature_count": 7075,
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
