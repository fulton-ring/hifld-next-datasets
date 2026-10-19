import hashlib
import json
import csv
import tempfile
import unittest
import zipfile
from io import BytesIO, StringIO
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.sources.ncua_insured_credit_unions import (
    BRANCH_FIELDS,
    LOCATOR_FIELDS,
    LOCATOR_TYPES,
    BRANCH_FIELDS,
    _analyze,
    _candidate_result,
    _deterministic_capture_archive,
    _canonical,
    _load_retained_capture,
    _verify_and_reuse_cached_candidate,
    _xlsx_charter_numbers,
    ACTIVE_LANDING,
    QUARTERLY_LANDING,
    LOCATOR_CYCLE_URL,
    LOCATOR_VERSION_URL,
    LOCATOR_SEARCH_URL,
)


def _feature(charter: int, site: int) -> dict:
    values = {
        "addressLatitude": 40.0, "addressLongitude": -73.0,
        "creditUnionName": "Example CU", "creditUnionSiteName": "Main Office",
        "creditUnionNumber": charter, "city": "New York", "cityPhysical": None,
        "country": "United States", "isMainOffice": True, "isMdi": False,
        "bilingual_Services": False, "credit_Builder": False,
        "financial_Counseling": False, "first_Time_Homebuyer_Program": False,
        "inSchoolBranch": False, "low_cost_wire_transfers": False,
        "no_Cost_Tax_Preparation": False, "no_Cost_Share_Drafts": False,
        "palS_I": False, "palS_II": False, "phone": "5550100",
        "siteFunctions": "Member Services;;;;", "siteName": None,
        "siteId": site, "state": "NY", "url": "https://example.test",
        "zipcode": "10001", "distance": -1, "formattedDistance": None,
        "index": 0, "street": "1 Main St", "fieldOfMembership": False,
        "siteIdAndCUNumber": f"{site}_{charter}", "numericFields": None,
    }
    assert tuple(values) == LOCATOR_FIELDS
    assert {k: ("null" if v is None else "bool" if type(v) is bool else "int" if type(v) is int else "float" if type(v) is float else "str") for k, v in values.items()} == LOCATOR_TYPES
    return values


def _page(charter: str, rows: list[dict], skip: int = 0) -> tuple[str, dict]:
    body = json.dumps({"latitude": 0, "longitude": 0, "valid": True, "list": rows,
                       "totalResults": len(rows), "statusCode": 0}, separators=(",", ":"))
    body_bytes = body.encode()
    return f"locator_{charter}_{skip:06d}", {
        "request": {"searchText": charter, "rdSearchType": "cunumber", "skip": skip,
                    "take": 100, "sort_item": "cuName", "sort_direction": "asc"},
        "status": 200, "complete_body": True, "error_code": None,
        "bytes": len(body_bytes), "sha256": hashlib.sha256(body_bytes).hexdigest(),
        "response_body": body,
    }


def _branch(charter: str, site: str) -> dict[str, str]:
    row = {field: "" for field in BRANCH_FIELDS}
    row.update({"CU_NUMBER": charter, "CYCLE_DATE": "6/30/2026 0:00:00",
                "SiteId": site, "CU_NAME": "Example CU", "SiteName": "Main Office",
                "PhysicalAddressCity": "New York", "PhysicalAddressStateCode": "NY",
                "PhysicalAddressPostalCode": "10001", "PhysicalAddressLine1": "1 Main St",
                "PhoneNumber": "5550100"})
    return row


def _workload():
    return _analyze(["100", "200"], [_branch("100", "7")],
                    [_page("100", [_feature(100, 7)]), _page("200", [])])


def _retained_fixture(root: Path) -> Path:
    root.mkdir()
    roster_xlsx = BytesIO()
    with zipfile.ZipFile(roster_xlsx, "w") as archive:
        archive.writestr("xl/workbook.xml", b'<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="FICUs" r:id="rId1"/></sheets></workbook>')
        archive.writestr("xl/_rels/workbook.xml.rels", b'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>')
        archive.writestr("xl/worksheets/sheet1.xml", b'<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>Charter number</t></is></c></row><row r="2"><c r="A2"><v>100</v></c></row><row r="3"><c r="A3"><v>200</v></c></row></sheetData></worksheet>')
    roster_zip = BytesIO()
    with zipfile.ZipFile(roster_zip, "w") as archive:
        archive.writestr("FederallyInsuredCreditUnions_test.xlsx", roster_xlsx.getvalue())
    branch_csv = StringIO()
    writer = csv.DictWriter(branch_csv, fieldnames=BRANCH_FIELDS, lineterminator="\n")
    writer.writeheader(); writer.writerow(_branch("100", "7"))
    branch_zip = BytesIO()
    with zipfile.ZipFile(branch_zip, "w") as archive:
        archive.writestr("Credit Union Branch Information.txt", branch_csv.getvalue().encode())
    active_page = b'<a href="https://ncua.gov/files/publications/analysis/federally-insured-credit-union-list-june-2026.zip">List of Active Federally Insured Credit Unions</a>'
    quarter_page = b'<a href="https://ncua.gov/files/publications/analysis/call-report-data-2026-06.zip">Quarterly June file</a>'
    payloads = {
        "official_active_roster_landing": (active_page, ACTIVE_LANDING, "GET", None),
        "official_quarterly_data_landing": (quarter_page, QUARTERLY_LANDING, "GET", None),
        "active_roster_xlsx_zip": (roster_zip.getvalue(), "https://ncua.gov/files/publications/analysis/federally-insured-credit-union-list-june-2026.zip", "GET", None),
        "quarterly_call_report_zip": (branch_zip.getvalue(), "https://ncua.gov/files/publications/analysis/call-report-data-2026-06.zip", "GET", None),
        "locator_cycle_before": (b'"2026-06-30T00:00:00"', LOCATOR_CYCLE_URL, "GET", None),
        "locator_app_version_before": (b"2.3.1", LOCATOR_VERSION_URL, "GET", None),
        "locator_cycle_after": (b'"2026-06-30T00:00:00"', LOCATOR_CYCLE_URL, "GET", None),
        "locator_app_version_after": (b"2.3.1", LOCATOR_VERSION_URL, "GET", None),
    }
    page_inputs = [("locator_100_000000", "100", 0, [_feature(100, 7)]),
                   ("locator_200_000000", "200", 0, [])]
    for name, charter, skip, features in page_inputs:
        _, receipt = _page(charter, features, skip)
        body = receipt["response_body"].encode()
        payloads[name] = (body, LOCATOR_SEARCH_URL, "POST", receipt["request"])
    receipt_files = {}
    for name, (body, url, method, request) in payloads.items():
        receipt = {"method": method, "url": url, "final_url": url, "observed_at": "2026-10-19T00:00:00+00:00",
                   "status": 200, "complete_body": True, "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
                   "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(), "content_encoding": "identity",
                   "content_length": len(body), "error_code": None}
        if request is not None: receipt["request"] = request
        (root / f"{name}.body").write_bytes(body)
        (root / f"{name}.wire").write_bytes(body)
        receipt_bytes = _canonical(receipt) + b"\n"
        (root / f"{name}.receipt.json").write_bytes(receipt_bytes)
        receipt_files[name] = hashlib.sha256(receipt_bytes).hexdigest()
    capture_manifest = {"schema_version": 1, "complete": True,
        "captured_at": "2026-10-19T00:00:00+00:00", "capture_finished_at": "2026-10-19T00:01:00+00:00",
        "receipt_files": receipt_files, "active_roster_sha256": hashlib.sha256(_canonical(["100", "200"])).hexdigest(),
        "quarterly_branch_zip_sha256": hashlib.sha256(branch_zip.getvalue()).hexdigest(),
        "locator_cycle_before": "2026-06-30T00:00:00", "locator_cycle_after": "2026-06-30T00:00:00",
        "locator_app_version_before": "2.3.1", "locator_app_version_after": "2.3.1",
        "active_charter_count": 2, "locator_page_count": 2}
    (root / "capture_manifest.json").write_bytes(_canonical(capture_manifest) + b"\n")
    return root


class NcuaCandidateTests(unittest.TestCase):
 def test_xlsx_charter_reader_preserves_native_ids(self):
    files = {
        "xl/workbook.xml": b'<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="FICUs" r:id="rId1"/></sheets></workbook>',
        "xl/_rels/workbook.xml.rels": b'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>',
        "xl/worksheets/sheet1.xml": b'<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>Charter number</t></is></c></row><row r="2"><c r="A2"><v>0007</v></c></row><row r="3"><c r="A3"><v>0011</v></c></row></sheetData></worksheet>',
    }
    body = BytesIO()
    with zipfile.ZipFile(body, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    self.assertEqual(_xlsx_charter_numbers(body.getvalue()), ["0007", "0011"])


 def test_candidate_audit_keeps_companion_gaps_and_native_coordinates(self):
    audit, rows, geojson = _workload()
    self.assertEqual(audit["active_charter_count"], 2)
    self.assertEqual(audit["active_charters_without_locator_results"], ["200"])
    self.assertEqual(audit["active_quarterly_branch_ids_missing_from_locator"], 0)
    self.assertEqual(audit["locator_ids_not_in_active_quarterly_branch_table"], 0)
    self.assertEqual(geojson["features"][0]["geometry"]["coordinates"], [-73.0, 40.0])
    self.assertEqual(geojson["features"][0]["properties"], rows[0])
    self.assertFalse(audit["release_accepted"])


 def test_candidate_rejects_tampered_raw_receipt_and_schema_drift(self):
    name, receipt = _page("100", [_feature(100, 7)])
    receipt["sha256"] = "0" * 64
    with self.assertRaisesRegex(ValueError, "receipt_body_mismatch"):
        _analyze(["100"], [], [(name, receipt)])

    row = _feature(100, 7)
    row["unexpectedNewField"] = "producer drift"
    with self.assertRaisesRegex(ValueError, "field_roster_drift"):
        _analyze(["100"], [], [_page("100", [row])])


 def test_candidate_rejects_missing_roster_query_and_pagination_gap(self):
    with self.assertRaisesRegex(ValueError, "query_incomplete"):
        _analyze(["100"], [], [])
    first = _page("100", [_feature(100, 7)])
    response = json.loads(first[1]["response_body"])
    response["totalResults"] = 2
    first[1]["response_body"] = json.dumps(response)
    first[1]["bytes"] = len(first[1]["response_body"].encode())
    first[1]["sha256"] = hashlib.sha256(first[1]["response_body"].encode()).hexdigest()
    with self.assertRaisesRegex(ValueError, "pagination_incomplete"):
        _analyze(["100"], [], [first])

 def test_retained_capture_replays_complete_wire_and_body_receipts(self):
    with tempfile.TemporaryDirectory() as temp:
        root = _retained_fixture(Path(temp) / "capture")
        captures, roster, _member, branches, pages, cycle, app_version, _roster_url, _branch_url, manifest = _load_retained_capture(root)
        audit, rows, _geojson = _analyze(roster, branches, pages)
        audit["candidate_input_record_sha256"] = audit["source_record_sha256"]
        self.assertEqual(len(captures), 10)
        self.assertEqual(manifest["active_charter_count"], 2)
        self.assertEqual(json.loads(cycle), "2026-06-30T00:00:00")
        self.assertEqual(app_version.decode(), "2.3.1")
        self.assertEqual(audit["locator_feature_count"], 1)
        self.assertEqual(len(rows), 1)
        candidate = _candidate_result("vtest", audit, rows, roster, root, reused=True)
        self.assertTrue(candidate["capture_complete"])
        self.assertTrue(candidate["reused"])
        self.assertEqual(_deterministic_capture_archive(root), _deterministic_capture_archive(root))

 def test_retained_capture_rejects_tamper_incomplete_and_rogue_files(self):
    with tempfile.TemporaryDirectory() as temp:
        root = _retained_fixture(Path(temp) / "tampered")
        (root / "locator_100_000000.body").write_bytes(b"changed body")
        with self.assertRaisesRegex(ValueError, "body_or_wire_mismatch"):
            _load_retained_capture(root)
    with tempfile.TemporaryDirectory() as temp:
        root = _retained_fixture(Path(temp) / "rogue")
        (root / "untracked.body").write_bytes(b"rogue")
        with self.assertRaisesRegex(ValueError, "unexpected_files"):
            _load_retained_capture(root)

 def test_cached_candidate_returns_full_asset_result_after_verification(self):
    audit, rows, _ = _workload()
    audit["candidate_input_record_sha256"] = audit["source_record_sha256"]
    payload = {"processing_identity": {"code_commit": "deadbeef"},
               "source_record_sha256": "1" * 64, "source_schema_sha256": "2" * 64,
               "capture_receipts": {}, "objects": {}}
    sealed = json.dumps({"payload": payload})
    class Storage:
        def object_snapshot(self, _key): return object()
    with patch("dagster_hifld.sources.ncua_insured_credit_unions.load_first_construction", return_value=sealed), \
         patch("dagster_hifld.sources.ncua_insured_credit_unions.verify_cached_candidate") as verify:
        result = _verify_and_reuse_cached_candidate(Storage(), "state", "prefix", "vtest", "3" * 64,
            payload["processing_identity"], payload["source_record_sha256"], payload["source_schema_sha256"],
            audit, rows, ["100", "200"], Path("/tmp/retained"))
    verify.assert_called_once()
    self.assertTrue(result["capture_complete"])
    self.assertTrue(result["reused"])
