"""First-party NCUA Locator capture and unapproved spatial candidate builder.

The Locator response is preserved verbatim alongside a GeoJSON view of its
native rows. The separate NCUA quarterly branch ZIP is retained unchanged and
compared by its native charter/site identifiers; the comparison is a release
gate, never a destructive join or coordinate substitute.
"""
from __future__ import annotations

import concurrent.futures
import csv
import hashlib
import io
import json
import math
import os
import platform
import posixpath
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from threading import Lock
from typing import Any, Mapping
from urllib.parse import urljoin, urlencode
from uuid import uuid4

import httpx

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "ncua-insured-credit-unions"
FILE_SLUG = DATASET_SLUG
AGENCY = "National Credit Union Administration"
QUARTERLY_LANDING = "https://ncua.gov/analysis/credit-union-corporate-call-report-data/quarterly-data"
ACTIVE_LANDING = "https://ncua.gov/analysis/credit-union-corporate-call-report-data"
LOCATOR_ROOT = "https://mapping.ncua.gov"
LOCATOR_VERSION_URL = LOCATOR_ROOT + "/api/Search/version"
LOCATOR_CYCLE_URL = LOCATOR_ROOT + "/api/DataQuery/GetCurrentCycle"
LOCATOR_SEARCH_URL = LOCATOR_ROOT + "/api/Search/GetSearchLocations"
ACTIVE_ROSTER_TEXT = "List of Active Federally Insured Credit Unions"
BRANCH_MEMBER = "Credit Union Branch Information.txt"
REQUEST_PAGE_SIZE = 100
MAX_SOURCE_BYTES = 250_000_000

LOCATOR_FIELDS = (
    "addressLatitude", "addressLongitude", "creditUnionName", "creditUnionSiteName",
    "creditUnionNumber", "city", "cityPhysical", "country", "isMainOffice", "isMdi",
    "bilingual_Services", "credit_Builder", "financial_Counseling",
    "first_Time_Homebuyer_Program", "inSchoolBranch", "low_cost_wire_transfers",
    "no_Cost_Tax_Preparation", "no_Cost_Share_Drafts", "palS_I", "palS_II", "phone",
    "siteFunctions", "siteName", "siteId", "state", "url", "zipcode", "distance",
    "formattedDistance", "index", "street", "fieldOfMembership", "siteIdAndCUNumber",
    "numericFields",
)
LOCATOR_TYPES = {
    "addressLatitude": "float", "addressLongitude": "float", "creditUnionName": "str",
    "creditUnionSiteName": "str", "creditUnionNumber": "int", "city": "str",
    "cityPhysical": "null", "country": "str", "isMainOffice": "bool", "isMdi": "bool",
    "bilingual_Services": "bool", "credit_Builder": "bool", "financial_Counseling": "bool",
    "first_Time_Homebuyer_Program": "bool", "inSchoolBranch": "bool",
    "low_cost_wire_transfers": "bool", "no_Cost_Tax_Preparation": "bool",
    "no_Cost_Share_Drafts": "bool", "palS_I": "bool", "palS_II": "bool", "phone": "str",
    "siteFunctions": "str", "siteName": "null", "siteId": "int", "state": "str",
    "url": "str", "zipcode": "str", "distance": "int", "formattedDistance": "null",
    "index": "int", "street": "str", "fieldOfMembership": "bool",
    "siteIdAndCUNumber": "str", "numericFields": "null",
}
LOCATOR_RESPONSE_FIELDS = ("latitude", "longitude", "valid", "list", "totalResults", "statusCode")
BRANCH_FIELDS = (
    "CU_NUMBER", "CYCLE_DATE", "JOIN_NUMBER", "SiteId", "CU_NAME", "SiteName",
    "SiteTypeName", "MainOffice", "PhysicalAddressLine1", "PhysicalAddressLine2",
    "PhysicalAddressCity", "PhysicalAddressStateCode", "PhysicalAddressPostalCode",
    "PhysicalAddressCountyName2", "PhysicalAddressCountry", "MailingAddressLine1",
    "MailingAddressLine2", "MailingAddressCity", "MailingAddressStateCode",
    "MailingAddressStateName", "MailingAddressPostalCode", "PhoneNumber",
    "HoursOfOperation", "MemberServices", "ATM", "DriveThru", "Shrd_Serv_Cntr_Net",
)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class _CaptureByteBudget:
    """Reserve retained wire+decoded bytes across concurrent HTTP responses."""
    def __init__(self, maximum: int):
        self.maximum=maximum; self.lock=Lock(); self.used=0
        self._progress:dict[str,tuple[int,int,int]]={}; self._reserved:dict[str,int]={}
    def reserve(self, name: str):
        def callback(progress):
            with self.lock:
                wire,decoded,previous=self._progress.get(name,(0,0,0))
                actual=progress.wire_bytes_retained+progress.decoded_bytes_retained
                self.used+=actual-previous
                self._progress[name]=(progress.wire_bytes_retained,progress.decoded_bytes_retained,actual)
                outstanding=sum(self._reserved.values())-self._reserved.get(name,0)
                if self.used+outstanding+progress.reserve_bytes>self.maximum:
                    raise ValueError("source_capture_byte_budget_exceeded")
                self._reserved[name]=progress.reserve_bytes
        return callback
    def finish(self, name: str) -> None:
        with self.lock:
            self._reserved.pop(name,None)
    def within_budget(self) -> bool:
        with self.lock:
            return self.used+sum(self._reserved.values())<=self.maximum


def _json_type(value: object) -> str:
    if value is None: return "null"
    if type(value) is bool: return "bool"
    if type(value) is int: return "int"
    if type(value) is float: return "float"
    if type(value) is str: return "str"
    raise ValueError("locator_attribute_type_unsupported")


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__(); self.links: list[tuple[str, str]] = []; self._href: str | None = None; self._text: list[str] = []
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            self._href = dict(attrs).get("href"); self._text = []
    def handle_data(self, data: str) -> None:
        if self._href is not None: self._text.append(data)
    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._href is not None:
            self.links.append((" ".join("".join(self._text).split()), self._href)); self._href = None


def _active_url(page: bytes) -> str:
    parser = _Links(); parser.feed(page.decode("utf-8", "replace"))
    found = [urljoin(ACTIVE_LANDING, href) for text, href in parser.links
             if ACTIVE_ROSTER_TEXT.casefold() in text.casefold() and href]
    if len(found) != 1 or not found[0].lower().endswith(".zip"):
        raise ValueError("active_roster_link_missing_or_ambiguous")
    return found[0]


def _quarterly_url(page: bytes, active_url: str) -> str:
    import re
    basename = Path(active_url.split("?", 1)[0]).name.lower()
    months = {"january":"01","february":"02","march":"03","april":"04",
              "may":"05","june":"06","july":"07","august":"08",
              "september":"09","october":"10","november":"11","december":"12"}
    match = re.search(r"-([a-z]+)-(20\d\d)\.zip$", basename)
    if not match or match.group(1) not in months:
        raise ValueError("active_roster_release_name_unrecognized")
    year, month = match.group(2), months[match.group(1)]
    expected = f"call-report-data-{year}-{month}.zip"
    parser = _Links(); parser.feed(page.decode("utf-8", "replace"))
    found = [urljoin(QUARTERLY_LANDING, href) for _text, href in parser.links
             if Path(href.split("?", 1)[0]).name.lower() == expected]
    if len(found) != 1:
        raise ValueError("matching_quarterly_download_missing_or_ambiguous")
    return found[0]


def _capture_post(client: httpx.Client, url: str, payload: Mapping[str, object],
                  capture_root: Path, name: str, budget: _CaptureByteBudget | None = None) -> tuple[bytes, dict[str, object]]:
    capture_root.mkdir(parents=True, exist_ok=True)
    wire_path, body_path = capture_root / f"{name}.wire", capture_root / f"{name}.body"
    wire_path.unlink(missing_ok=True); body_path.unlink(missing_ok=True)
    observed = datetime.now(timezone.utc).isoformat(); hashes = {"wire": hashlib.sha256(), "body": hashlib.sha256()}; sizes = {"wire": 0, "body": 0}
    def retain(path: Path, kind: str):
        def write(data: bytes) -> None:
            with path.open("ab") as stream: stream.write(data)
            hashes[kind].update(data); sizes[kind] += len(data)
        return write
    try:
        with client.stream("POST", url, json=dict(payload), headers={"Accept-Encoding":"identity", "Accept":"application/json"}) as response:
            result = capture_http_response(response, max_wire_bytes=2_000_000,
                max_decoded_bytes=2_000_000, reserve=budget.reserve(name) if budget else (lambda _progress: None),
                retain_wire=retain(wire_path, "wire"), retain_decoded=retain(body_path, "body"))
            final_url = str(response.url); response_headers = dict(response.headers)
    finally:
        if budget: budget.finish(name)
    receipt = {"method":"POST", "url":url, "request":dict(payload), "final_url":final_url,
        "observed_at":observed,"status":result.status_code,"complete_body":result.complete,
        "wire_bytes":sizes["wire"],"wire_sha256":hashes["wire"].hexdigest(),
        "bytes":sizes["body"],"sha256":hashes["body"].hexdigest(),
        "content_encoding":result.content_encoding,"content_length":result.content_length,
        "error_code":result.error_code,"http_date":response_headers.get("Date")}
    (capture_root/f"{name}.receipt.json").write_bytes(_canonical(receipt)+b"\n")
    body = body_path.read_bytes(); wire = wire_path.read_bytes()
    if (not result.complete or result.status_code != 200 or final_url != url
        or len(body) != receipt["bytes"] or _sha(body) != receipt["sha256"]
        or len(wire) != receipt["wire_bytes"] or _sha(wire) != receipt["wire_sha256"]):
        raise ValueError("incomplete_locator_response:"+name)
    return body, receipt


def _xlsx_charter_numbers(body: bytes) -> list[str]:
    """Read the native Charter number column from the official XLSX using stdlib only."""
    from xml.etree.ElementTree import fromstring
    ns = {"m":"http://schemas.openxmlformats.org/spreadsheetml/2006/main",
          "r":"http://schemas.openxmlformats.org/officeDocument/2006/relationships",
          "rel":"http://schemas.openxmlformats.org/package/2006/relationships"}
    with zipfile.ZipFile(__import__("io").BytesIO(body)) as archive:
        workbook = fromstring(archive.read("xl/workbook.xml"))
        relationships = fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        rel_map = {x.attrib["Id"]:x.attrib["Target"] for x in relationships.findall("rel:Relationship", ns)}
        sheet = next((x for x in workbook.findall("m:sheets/m:sheet", ns) if x.attrib.get("name")=="FICUs"), None)
        if sheet is None: raise ValueError("active_roster_sheet_missing")
        target = rel_map.get(sheet.attrib.get(f"{{{ns['r']}}}id"))
        if not target: raise ValueError("active_roster_sheet_relationship_missing")
        sheet_path = posixpath.normpath(posixpath.join("xl", target))
        shared: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            shared_root = fromstring(archive.read("xl/sharedStrings.xml"))
            shared = ["".join(t.text or "" for t in item.findall(".//m:t", ns)) for item in shared_root.findall("m:si", ns)]
        root = fromstring(archive.read(sheet_path))
        rows = root.findall("m:sheetData/m:row", ns)
        if not rows: raise ValueError("active_roster_rows_missing")
        def cell_value(cell):
            value = cell.find("m:v", ns); raw = value.text if value is not None else ""
            if cell.attrib.get("t") == "s": return shared[int(raw)]
            if cell.attrib.get("t") == "inlineStr":
                inline=cell.find("m:is", ns)
                return "".join(x.text or "" for x in inline.findall(".//m:t", ns)) if inline is not None else ""
            return raw
        header = {cell_value(cell):cell.attrib["r"].rstrip("0123456789") for cell in rows[0].findall("m:c", ns)}
        col = header.get("Charter number")
        if not col: raise ValueError("active_roster_charter_column_missing")
        result=[]
        for row in rows[1:]:
            cell=next((x for x in row.findall("m:c", ns) if x.attrib.get("r", "").startswith(col)), None)
            if cell is None: continue
            raw=cell_value(cell).strip()
            if raw:
                if not raw.isdigit(): raise ValueError("active_roster_charter_value_invalid")
                result.append(raw)
    if not result or len(result)!=len(set(result)): raise ValueError("active_roster_charter_roster_invalid")
    return result


def _branch_records(zip_body: bytes) -> tuple[bytes, list[dict[str,str]]]:
    with zipfile.ZipFile(__import__("io").BytesIO(zip_body)) as archive:
        names=[x for x in archive.namelist() if Path(x).name==BRANCH_MEMBER]
        if len(names)!=1: raise ValueError("quarterly_branch_member_missing_or_ambiguous")
        member=archive.read(names[0])
    reader=csv.DictReader(io.StringIO(member.decode("utf-8-sig"), newline=""))
    if tuple(reader.fieldnames or ())!=BRANCH_FIELDS: raise ValueError("quarterly_branch_schema_drift")
    rows=list(reader)
    if not rows or any(None in x or any(v is None for v in x.values()) for x in rows): raise ValueError("quarterly_branch_record_width_drift")
    if any(not x.get("CU_NUMBER") or not x.get("SiteId") for x in rows): raise ValueError("quarterly_branch_identifier_missing")
    return member,rows


def _analyze(charters: list[str], branch_rows: list[dict[str,str]],
             locator_pages: list[tuple[str, Mapping[str,object]]]) -> tuple[dict[str,object], list[dict[str,object]], dict[str,object]]:
    if len(charters)!=len(set(charters)) or not charters: raise ValueError("active_roster_incomplete_or_duplicate")
    active=set(charters); seen_queries=set(); rows=[]; per_charter={};
    for capture_name, receipt in locator_pages:
        request=receipt.get("request"); body=receipt.get("response_body")
        if not isinstance(request,dict) or not isinstance(body,str): raise ValueError("locator_capture_receipt_invalid")
        body_bytes=body.encode("utf-8")
        if (receipt.get("status") != 200 or receipt.get("complete_body") is not True
                or receipt.get("error_code") is not None or receipt.get("bytes") != len(body_bytes)
                or receipt.get("sha256") != _sha(body_bytes)):
            raise ValueError("locator_capture_receipt_body_mismatch")
        charter=request.get("searchText"); skip=request.get("skip")
        if (request.get("rdSearchType")!="cunumber" or charter not in active
                or request.get("take") != REQUEST_PAGE_SIZE or request.get("sort_item") != "cuName"
                or request.get("sort_direction") != "asc"):
            raise ValueError("locator_query_outside_active_roster")
        j=json.loads(body)
        if not isinstance(j,dict) or tuple(j.keys())!=LOCATOR_RESPONSE_FIELDS: raise ValueError("locator_response_schema_drift")
        if j.get("valid") is not True: raise ValueError("locator_query_invalid")
        if not isinstance(skip,int) or not isinstance(j.get("list"),list) or type(j.get("totalResults")) is not int: raise ValueError("locator_page_shape_invalid")
        page_rows=j["list"]
        if skip==0:
            if charter in seen_queries: raise ValueError("duplicate_locator_charter_seed")
            seen_queries.add(charter); per_charter[charter]={"total":j["totalResults"],"items":0,"page_skips":[]}
        if charter not in per_charter or j["totalResults"]!=per_charter[charter]["total"]: raise ValueError("locator_pagination_total_changed")
        state=per_charter[charter]
        if skip!=state["items"] or skip in state["page_skips"] or len(page_rows)>REQUEST_PAGE_SIZE: raise ValueError("locator_page_gap_or_overlap")
        state["page_skips"].append(skip); state["items"]+=len(page_rows)
        for feature in page_rows:
            if not isinstance(feature,dict) or tuple(feature.keys())!=LOCATOR_FIELDS: raise ValueError("locator_field_roster_drift")
            if {k:_json_type(v) for k,v in feature.items()}!=LOCATOR_TYPES: raise ValueError("locator_field_type_drift")
            cu=feature["creditUnionNumber"]; site=feature["siteId"]
            if type(cu) is not int or type(site) is not int or str(cu)!=charter: raise ValueError("locator_identifier_invalid")
            if feature["siteIdAndCUNumber"]!=f"{site}_{cu}": raise ValueError("locator_native_composite_identifier_mismatch")
            lat,lon=feature["addressLatitude"],feature["addressLongitude"]
            if type(lat) is not float or type(lon) is not float or not math.isfinite(lat) or not math.isfinite(lon) or not -90<=lat<=90 or not -180<=lon<=180: raise ValueError("locator_source_coordinate_invalid")
            rows.append(feature)
    if seen_queries!=active: raise ValueError("locator_active_charter_query_incomplete")
    for charter,state in per_charter.items():
        if state["items"]!=state["total"]: raise ValueError("locator_pagination_incomplete")
    keys=[(str(x["creditUnionNumber"]),str(x["siteId"])) for x in rows]
    if len(keys)!=len(set(keys)): raise ValueError("locator_duplicate_native_identifier")
    rows.sort(key=lambda x:(x["creditUnionNumber"],x["siteId"]))
    branch_active=[r for r in branch_rows if r["CU_NUMBER"] in active]
    branch_keys=[(r["CU_NUMBER"],r["SiteId"]) for r in branch_active]
    if len(branch_keys)!=len(set(branch_keys)): raise ValueError("quarterly_branch_duplicate_native_identifier")
    branch_map={(r["CU_NUMBER"],r["SiteId"]):r for r in branch_active}
    locator_map={(str(r["creditUnionNumber"]),str(r["siteId"])):r for r in rows}
    overlap=set(branch_map)&set(locator_map); missing=sorted(set(branch_map)-set(locator_map)); extra=sorted(set(locator_map)-set(branch_map))
    diffs=[]
    pairs=(("CU_NAME","creditUnionName"),("SiteName","creditUnionSiteName"),("PhysicalAddressCity","city"),
           ("PhysicalAddressStateCode","state"),("PhysicalAddressPostalCode","zipcode"),
           ("PhysicalAddressLine1","street"),("PhoneNumber","phone"))
    for key in sorted(overlap,key=lambda x:(int(x[0]),int(x[1]))):
        changed=[a for a,b in pairs if str(branch_map[key].get(a,"" )).strip().casefold()!=str(locator_map[key].get(b) or "").strip().casefold()]
        if changed: diffs.append({"cu_number":key[0],"site_id":key[1],"fields":changed})
    geojson={"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[r["addressLongitude"],r["addressLatitude"]]},"properties":r} for r in rows]}
    record_sha=_sha(_canonical(rows)); schema_sha=_sha(_canonical([{"name":k,"type":LOCATOR_TYPES[k]} for k in LOCATOR_FIELDS]))
    cycle_dates=sorted({r["CYCLE_DATE"] for r in branch_rows})
    audit={"capture_complete":True,"candidate_unapproved":True,"publisher":AGENCY,
        "locator_query_scope":"all charter numbers in the NCUA active federally insured CU XLSX roster; one query and full API-reported pagination per charter",
        "active_charter_count":len(active),"charters_with_locator_results":sum(bool(x["items"]) for x in per_charter.values()),
        "active_charters_without_locator_results":sorted(k for k,v in per_charter.items() if v["total"]==0),
        "locator_feature_count":len(rows),"locator_unique_composite_ids":len(set(keys)),
        "locator_source_fields":[{"name":k,"json_type":LOCATOR_TYPES[k]} for k in LOCATOR_FIELDS],
        "source_record_sha256":record_sha,"source_schema_sha256":schema_sha,
        "quarterly_branch_row_count":len(branch_rows),"active_charter_branch_row_count":len(branch_active),
        "branch_cycle_dates":cycle_dates,"branch_unique_native_ids":len(branch_map),
        "locator_branch_id_overlap":len(overlap),"active_quarterly_branch_ids_missing_from_locator":len(missing),
        "locator_ids_not_in_active_quarterly_branch_table":len(extra),"overlapping_ids_with_checked_attribute_differences":len(diffs),
        "missing_branch_examples":[{"cu_number":k[0],"site_id":k[1],"name":branch_map[k]["CU_NAME"],"site_name":branch_map[k]["SiteName"],"state":branch_map[k]["PhysicalAddressStateCode"]} for k in missing[:50]],
        "extra_locator_examples":[{"cu_number":k[0],"site_id":k[1],"name":locator_map[k]["creditUnionName"],"site_name":locator_map[k]["creditUnionSiteName"],"state":locator_map[k]["state"]} for k in extra[:50]],
        "attribute_difference_examples":diffs[:50],"source_release_accepted":False,
        "quarterly_companion_alignment_accepted":not (missing or extra or diffs),
        "schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,
        "mapping_approved":False,"freshness_accepted":False,"release_accepted":False}
    return audit, rows, geojson


def _code_identity() -> dict[str,object]: return processing_identity(Path(__file__).parents[3])


def _source_urls(active_page: bytes, quarterly_page: bytes) -> tuple[str,str]:
    roster=_active_url(active_page); report=_quarterly_url(quarterly_page,roster)
    return roster,report


def _load_retained_capture(capture_root: Path):
    """Load only a complete capture emitted with original HTTP wire custody."""
    manifest_path=capture_root/"capture_manifest.json"
    try:
        capture_manifest=json.loads(manifest_path.read_bytes())
    except (OSError,ValueError) as exc:
        raise ValueError("retained_capture_manifest_missing_or_invalid") from exc
    required={"schema_version","complete","captured_at","capture_finished_at","receipt_files",
              "active_roster_sha256","quarterly_branch_zip_sha256","locator_cycle_before",
              "locator_cycle_after","locator_app_version_before","locator_app_version_after",
              "active_charter_count","locator_page_count"}
    if (not isinstance(capture_manifest,dict) or set(capture_manifest)!=required
            or capture_manifest.get("schema_version")!=1 or capture_manifest.get("complete") is not True
            or not isinstance(capture_manifest.get("receipt_files"),dict)):
        raise ValueError("retained_capture_incomplete_or_unsupported")
    try:
        started=datetime.fromisoformat(capture_manifest["captured_at"].replace("Z","+00:00"))
        finished=datetime.fromisoformat(capture_manifest["capture_finished_at"].replace("Z","+00:00"))
        if started.tzinfo is None or finished.tzinfo is None or finished<started: raise ValueError("bad acquisition interval")
    except (AttributeError,TypeError,ValueError) as exc:
        raise ValueError("retained_capture_acquisition_interval_invalid") from exc
    if any(path.is_symlink() for path in capture_root.iterdir()):
        raise ValueError("retained_capture_symlink_rejected")
    if capture_manifest["locator_cycle_before"]!=capture_manifest["locator_cycle_after"] or capture_manifest["locator_app_version_before"]!=capture_manifest["locator_app_version_after"]:
        raise ValueError("retained_capture_source_brackets_changed")
    receipts_by_name=capture_manifest["receipt_files"]
    names=set(receipts_by_name)
    required_gets={"official_active_roster_landing","official_quarterly_data_landing",
        "active_roster_xlsx_zip","quarterly_call_report_zip","locator_cycle_before",
        "locator_app_version_before","locator_cycle_after","locator_app_version_after"}
    if not required_gets<=names: raise ValueError("retained_capture_required_get_missing")
    captures={}; query_pages=[]
    for name,digest in receipts_by_name.items():
        if (not isinstance(name,str) or not isinstance(digest,str) or len(digest)!=64
                or any(c not in "0123456789abcdef" for c in digest)):
            raise ValueError("retained_capture_receipt_roster_invalid")
        receipt_path=capture_root/f"{name}.receipt.json"
        body_path=capture_root/f"{name}.body"; wire_path=capture_root/f"{name}.wire"
        receipt_bytes=receipt_path.read_bytes(); body=body_path.read_bytes(); wire=wire_path.read_bytes()
        if _sha(receipt_bytes)!=digest: raise ValueError("retained_capture_receipt_hash_mismatch:"+name)
        receipt=json.loads(receipt_bytes)
        try:
            observed=datetime.fromisoformat(receipt["observed_at"].replace("Z","+00:00"))
        except (AttributeError,KeyError,TypeError,ValueError) as exc:
            raise ValueError("retained_capture_receipt_time_invalid:"+name) from exc
        if (not isinstance(receipt,dict) or receipt.get("status")!=200
                or receipt.get("complete_body") is not True or receipt.get("error_code") is not None
                or observed.tzinfo is None or observed<started or observed>finished
                or receipt.get("final_url")!=receipt.get("url")
                or receipt.get("bytes")!=len(body) or receipt.get("sha256")!=_sha(body)
                or receipt.get("wire_bytes")!=len(wire) or receipt.get("wire_sha256")!=_sha(wire)):
            raise ValueError("retained_capture_body_or_wire_mismatch:"+name)
        if name in required_gets:
            if receipt.get("method")!="GET": raise ValueError("retained_capture_method_mismatch:"+name)
        elif name.startswith("locator_"):
            if receipt.get("method")!="POST" or receipt.get("url")!=LOCATOR_SEARCH_URL:
                raise ValueError("retained_locator_request_endpoint_mismatch:"+name)
            request=receipt.get("request")
            if not isinstance(request,dict): raise ValueError("retained_locator_request_missing:"+name)
            try: charter,skip=name.removeprefix("locator_").rsplit("_",1); skip=int(skip)
            except (ValueError,TypeError) as exc: raise ValueError("retained_locator_page_name_invalid") from exc
            expected={"searchText":charter,"rdSearchType":"cunumber","skip":skip,
                "take":REQUEST_PAGE_SIZE,"sort_item":"cuName","sort_direction":"asc"}
            if request!=expected: raise ValueError("retained_locator_payload_mismatch:"+name)
            query_pages.append((name,receipt|{"response_body":body.decode("utf-8")}))
        else:
            raise ValueError("retained_capture_unexpected_receipt:"+name)
        captures[name]=(body,receipt)
    if not set(capture_root.iterdir())=={manifest_path,*[capture_root/f"{n}.{suffix}" for n in names for suffix in ("receipt.json","body","wire")]}:
        raise ValueError("retained_capture_unexpected_files")
    if sum(int(captures[n][1]["wire_bytes"])+int(captures[n][1]["bytes"]) for n in captures)>MAX_SOURCE_BYTES:
        raise ValueError("retained_capture_source_byte_budget_exceeded")
    active_page=captures["official_active_roster_landing"][0]
    quarterly_page=captures["official_quarterly_data_landing"][0]
    roster_url,branch_url=_source_urls(active_page,quarterly_page)
    expected_urls={"official_active_roster_landing":ACTIVE_LANDING,
        "official_quarterly_data_landing":QUARTERLY_LANDING,
        "active_roster_xlsx_zip":roster_url,"quarterly_call_report_zip":branch_url,
        "locator_cycle_before":LOCATOR_CYCLE_URL,"locator_cycle_after":LOCATOR_CYCLE_URL,
        "locator_app_version_before":LOCATOR_VERSION_URL,"locator_app_version_after":LOCATOR_VERSION_URL}
    for name,url in expected_urls.items():
        if captures[name][1].get("url")!=url: raise ValueError("retained_capture_source_url_mismatch:"+name)
    roster_zip=captures["active_roster_xlsx_zip"][0]
    with zipfile.ZipFile(io.BytesIO(roster_zip)) as archive:
        xlsx=[n for n in archive.namelist() if n.lower().endswith(".xlsx")]
        if len(xlsx)!=1: raise ValueError("active_roster_xlsx_member_missing_or_ambiguous")
        roster=_xlsx_charter_numbers(archive.read(xlsx[0]))
    if (_sha(_canonical(roster))!=capture_manifest["active_roster_sha256"]
            or len(roster)!=capture_manifest["active_charter_count"]):
        raise ValueError("retained_capture_active_roster_pin_mismatch")
    branch_zip=captures["quarterly_call_report_zip"][0]
    if _sha(branch_zip)!=capture_manifest["quarterly_branch_zip_sha256"]:
        raise ValueError("retained_capture_quarterly_zip_pin_mismatch")
    branch_member,branch_rows=_branch_records(branch_zip)
    cycle_before=captures["locator_cycle_before"][0]
    cycle_after=captures["locator_cycle_after"][0]
    version_before=captures["locator_app_version_before"][0]
    version_after=captures["locator_app_version_after"][0]
    if (json.loads(cycle_before)!=capture_manifest["locator_cycle_before"]
            or json.loads(cycle_after)!=capture_manifest["locator_cycle_after"]
            or version_before.decode().strip()!=capture_manifest["locator_app_version_before"]
            or version_after.decode().strip()!=capture_manifest["locator_app_version_after"]
            or len(query_pages)!=capture_manifest["locator_page_count"]):
        raise ValueError("retained_capture_source_bracket_or_page_pin_mismatch")
    return (captures,roster,branch_member,branch_rows,query_pages,cycle_before,version_before,
            roster_url,branch_url,capture_manifest)


def _capture_from_network(storage: StagingStorageResource, capture_root: Path, version: str):
    captures:dict[str,tuple[bytes,dict[str,object]]]={}; lock=Lock()
    budget=_CaptureByteBudget(MAX_SOURCE_BYTES)
    captured_at=datetime.now(timezone.utc).isoformat()
    def get(client,url,name,max_bytes=MAX_SOURCE_BYTES):
        try:
            body,receipt=capture_get(client,url,capture_root,name,max_bytes=max_bytes,reserve=budget.reserve(name))
        finally:
            budget.finish(name)
        with lock: captures[name]=(body,receipt)
        return body,receipt
    try:
        with httpx.Client(follow_redirects=False,timeout=90,headers={"User-Agent":"hifld-next-datasets/ncua-draft"}) as client:
            active_page,_=get(client,ACTIVE_LANDING,"official_active_roster_landing",5_000_000)
            quarterly_page,_=get(client,QUARTERLY_LANDING,"official_quarterly_data_landing",5_000_000)
            roster_url,branch_url=_source_urls(active_page,quarterly_page)
            roster_zip,_=get(client,roster_url,"active_roster_xlsx_zip",30_000_000)
            branch_zip,_=get(client,branch_url,"quarterly_call_report_zip",100_000_000)
            with zipfile.ZipFile(io.BytesIO(roster_zip)) as archive:
                xlsx=[n for n in archive.namelist() if n.lower().endswith(".xlsx")]
                if len(xlsx)!=1: raise ValueError("active_roster_xlsx_member_missing_or_ambiguous")
                roster=_xlsx_charter_numbers(archive.read(xlsx[0]))
            branch_member,branch_rows=_branch_records(branch_zip)
            cycle_before,_=get(client,LOCATOR_CYCLE_URL,"locator_cycle_before",100_000)
            version_before,_=get(client,LOCATOR_VERSION_URL,"locator_app_version_before",100_000)
            def fetch_charter(cu:str):
                pages=[]; total=None; skip=0
                while total is None or skip<total:
                    payload={"searchText":cu,"rdSearchType":"cunumber","skip":skip,"take":REQUEST_PAGE_SIZE,
                             "sort_item":"cuName","sort_direction":"asc"}
                    name=f"locator_{cu}_{skip:06d}"
                    body,receipt=_capture_post(client,LOCATOR_SEARCH_URL,payload,capture_root,name,budget)
                    with lock: captures[name]=(body,receipt)
                    j=json.loads(body)
                    if not isinstance(j,dict) or tuple(j.keys())!=LOCATOR_RESPONSE_FIELDS or j.get("valid") is not True or type(j.get("totalResults")) is not int or not isinstance(j.get("list"),list): raise ValueError("locator_search_response_invalid:"+cu)
                    if total is None: total=j["totalResults"]
                    elif total!=j["totalResults"]: raise ValueError("locator_total_changed_during_pagination:"+cu)
                    if len(j["list"])>REQUEST_PAGE_SIZE or (skip<total and not j["list"]): raise ValueError("locator_pagination_truncated:"+cu)
                    pages.append((name,receipt|{"response_body":body.decode("utf-8")}))
                    skip+=len(j["list"])
                    if total==0: break
                return pages
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                results=list(pool.map(fetch_charter,roster))
            query_pages=[page for per_cu in results for page in per_cu]
            cycle_after,_=get(client,LOCATOR_CYCLE_URL,"locator_cycle_after",100_000)
            version_after,_=get(client,LOCATOR_VERSION_URL,"locator_app_version_after",100_000)
        if cycle_before!=cycle_after or version_before!=version_after: raise ValueError("locator_source_changed_during_capture")
        if not budget.within_budget():
            raise ValueError("source_capture_byte_budget_exceeded")
        audit,_,_=_analyze(roster,branch_rows,query_pages)
        if not audit["branch_cycle_dates"] or len(audit["branch_cycle_dates"])!=1: raise ValueError("quarterly_branch_release_date_ambiguous")
        receipt_files={name:_sha((capture_root/f"{name}.receipt.json").read_bytes()) for name in sorted(captures)}
        capture_manifest={"schema_version":1,"complete":True,"captured_at":captured_at,
            "capture_finished_at":datetime.now(timezone.utc).isoformat(),"receipt_files":receipt_files,
            "active_roster_sha256":_sha(_canonical(roster)),"quarterly_branch_zip_sha256":_sha(branch_zip),
            "locator_cycle_before":json.loads(cycle_before),"locator_cycle_after":json.loads(cycle_after),
            "locator_app_version_before":version_before.decode().strip(),"locator_app_version_after":version_after.decode().strip(),
            "active_charter_count":len(roster),"locator_page_count":len(query_pages)}
        (capture_root/"capture_manifest.json").write_bytes(_canonical(capture_manifest)+b"\n")
        return (captures,roster,branch_member,branch_rows,query_pages,cycle_before,version_before,
                roster_url,branch_url,capture_manifest)
    except Exception as exc:
        receipts={}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try: receipts[path.name.removesuffix(".receipt.json")]=json.loads(path.read_bytes())
            except (ValueError,OSError): receipts[path.name]={"receipt_readback_failed":True}
        (capture_root/"capture_manifest.json").write_bytes(_canonical({"schema_version":1,"captured_at":captured_at,
            "complete":False,"error_code":type(exc).__name__,"message":str(exc),"receipts":receipts})+b"\n")
        if captures: quarantine_incomplete_capture(storage,capture_root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise


def _candidate_result(version: str, audit: Mapping[str,object], rows: list[dict[str,object]],
                      roster: list[str], capture_root: Path, *, reused: bool = False) -> dict[str,object]:
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,
        "feature_count":len(rows),"active_charter_count":len(roster),"capture_complete":True,
        "candidate_ready":prefix+"/metadata/candidate_ready.json","source_record_sha256":audit["candidate_input_record_sha256"],
        "active_quarterly_branch_id_gap":audit["active_quarterly_branch_ids_missing_from_locator"],
        "locator_ids_not_in_active_quarterly_branch_table":audit["locator_ids_not_in_active_quarterly_branch_table"],
        "source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,
        "quarterly_companion_alignment_accepted":False,"historical_successor_equivalence_accepted":False,
        "release_accepted":False,"capture_directory":str(capture_root),"reused":reused}


def _verify_and_reuse_cached_candidate(storage: StagingStorageResource, state_key: str,
        prefix: str, version: str, fingerprint: str, identity: Mapping[str,object],
        source_record_sha: str, source_schema_sha: str, audit: Mapping[str,object],
        rows: list[dict[str,object]], roster: list[str], capture_root: Path) -> dict[str,object]:
    sealed=json.loads(load_first_construction(storage,state_key,version,fingerprint))
    payload=sealed.get("payload")
    if (not isinstance(payload,dict) or payload.get("processing_identity")!=identity
            or payload.get("source_record_sha256")!=source_record_sha
            or payload.get("source_schema_sha256")!=source_schema_sha
            or not isinstance(payload.get("objects"),dict)
            or not isinstance(payload.get("capture_receipts"),dict)):
        raise ValueError("cached_first_candidate_identity_or_source_changed")
    verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,
        identity,source_record_sha,source_schema_sha,payload["objects"],payload["capture_receipts"])
    return _candidate_result(version,audit,rows,roster,capture_root,reused=True)


def _deterministic_capture_archive(capture_root: Path) -> bytes:
    output=io.BytesIO()
    with zipfile.ZipFile(output,"w",compression=zipfile.ZIP_DEFLATED,compresslevel=6) as archive:
        for path in sorted(capture_root.iterdir(),key=lambda p:p.name):
            if not path.is_file() or path.is_symlink(): raise ValueError("capture_bundle_unexpected_member")
            member=zipfile.ZipInfo(path.name,date_time=(1980,1,1,0,0,0))
            member.compress_type=zipfile.ZIP_DEFLATED; member.external_attr=0o600<<16
            archive.writestr(member,path.read_bytes(),compress_type=zipfile.ZIP_DEFLATED,compresslevel=6)
    bundle=output.getvalue()
    if len(bundle)>MAX_SOURCE_BYTES: raise ValueError("source_capture_bundle_byte_budget_exceeded")
    return bundle


def capture_ncua_candidate(storage: StagingStorageResource, *, version: str,
                           capture_directory: str | None = None) -> dict[str,object]:
    """Capture current official roster, quarterly branch companion and Locator points."""
    if (not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource)
        or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version): raise ValueError("invalid_version")
    root=Path(storage.local_dir).absolute()
    if ".." in root.parts or any(p.is_symlink() for p in (root,*root.parents)): raise ValueError("unsafe_local_staging_root")
    root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<256_000_000: raise ValueError("disk_floor_before_acquisition")
    if capture_directory:
        capture_root=Path(capture_directory).absolute()
        if ".." in capture_root.parts or any(p.is_symlink() for p in (capture_root,*capture_root.parents)) or not capture_root.is_dir():
            raise ValueError("unsafe_or_missing_retained_capture_directory")
        (captures,roster,branch_member,branch_rows,query_pages,cycle_before,version_before,
         roster_url,branch_url,capture_manifest)=_load_retained_capture(capture_root)
    else:
        capture_base=Path("/tmp/hifld-native-reconstruction/182/captures")
        capture_root=capture_base.absolute()/f"{version}-{uuid4().hex}"
        if ".." in capture_root.parts or any(p.is_symlink() for p in (capture_root,*capture_root.parents)): raise ValueError("unsafe_capture_directory")
        capture_root.mkdir(parents=True,exist_ok=False)
        (captures,roster,branch_member,branch_rows,query_pages,cycle_before,version_before,
         roster_url,branch_url,capture_manifest)=_capture_from_network(storage,capture_root,version)
    audit,rows,geojson=_analyze(roster,branch_rows,query_pages)
    if not audit["branch_cycle_dates"] or len(audit["branch_cycle_dates"])!=1: raise ValueError("quarterly_branch_release_date_ambiguous")
    audit["locator_cycle_date"]=json.loads(cycle_before)
    audit["locator_app_version"]=version_before.decode().strip()
    audit["quarterly_branch_release_date"]=audit["branch_cycle_dates"][0]
    audit["active_roster_url"]=roster_url; audit["quarterly_branch_url"]=branch_url

    identity=_code_identity()
    locator_record_sha=audit["source_record_sha256"]; source_schema_sha=audit["source_schema_sha256"]
    evidence_sha=_sha(_canonical({name:{"method":receipt["method"],"url":receipt["url"],
        "request":receipt.get("request"),"body_sha256":receipt["sha256"],"wire_sha256":receipt["wire_sha256"]}
        for name,(_,receipt) in sorted(captures.items())}))
    # Cache identity includes every original response body/wire digest so a
    # companion, endpoint, envelope or roster change creates a new version.
    source_record_sha=_sha(_canonical({
        "locator_records_sha256":locator_record_sha,
        "source_evidence_sha256":evidence_sha,
    }))
    audit["locator_record_sha256"]=locator_record_sha
    audit["candidate_input_record_sha256"]=source_record_sha
    audit["source_evidence_sha256"]=evidence_sha
    geojson_body=_canonical(geojson)+b"\n"
    source_name="ncua-insured-credit-unions.geojson"
    source_objects={f"geojson/{source_name}":{"bytes":len(geojson_body),"sha256":_sha(geojson_body)}}
    manifest={"schema_version":1,"approved":False,"title":"NCUA Insured Credit Unions",
        "description":(f"Draft first-party NCUA Locator point capture queried for every charter in the official active federally insured credit union roster. The separate current quarterly branch file is preserved in full; {audit['active_quarterly_branch_ids_missing_from_locator']} active-roster branch IDs are absent from Locator, {audit['locator_ids_not_in_active_quarterly_branch_table']} Locator IDs are absent from that active branch table, and {audit['overlapping_ids_with_checked_attribute_differences']} overlapping IDs have checked value differences. This candidate does not claim complete quarterly-branch coverage or approved equivalence to the historical HIFLD item."),
        "publisher":AGENCY,"agency":AGENCY,"source_url":LOCATOR_ROOT,"license":None,
        "source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
        "inputs":[{"input_id":name,"producer":AGENCY,"host":"mapping.ncua.gov" if name.startswith("locator_") else "ncua.gov",
            "role":"producer_publication","canonical_url":LOCATOR_ROOT if name.startswith("locator_") else (ACTIVE_LANDING if name.startswith("active_") or name=="official_active_roster_landing" else QUARTERLY_LANDING),
            "retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],
            "http_status":receipt["status"],"bytes":receipt["bytes"],"sha256":receipt["sha256"],
            "wire_bytes":receipt["wire_bytes"],"wire_sha256":receipt["wire_sha256"],
            "release_date":None,
            "release_date_unknown_reason":"The producer response does not provide an independently verified publication timestamp; the quarterly cycle date is recorded separately as an effective date."}
            | {"license":"NCUA public source; license terms not verified in this research.",
               "attribution":"National Credit Union Administration first-party public data; attribution and reuse terms remain under review."}
            for name,(_,receipt) in sorted(captures.items())],
        "lineage":"The live search endpoint is called by the NCUA mapping.ncua.gov client; its frontend JavaScript declares the POST route /api/Search/GetSearchLocations and sends charter, skip, take, and sort parameters. Coordinates come directly from the returned addressLatitude/addressLongitude values. The official NCUA active-credit-union roster seeds one full query per charter. The official NCUA quarterly branch ZIP is retained as a separate full-coverage companion and compared using CU_NUMBER + SiteId without replacing either source's values.",
        "transformations":[{"operation":"native_NCUA_Locator_JSON_to_GeoJSON_candidate","parameters":{"fields":"all 34 response keys preserved in producer order and native JSON value types; geometry is Point coordinates copied from source addressLongitude/addressLatitude","ids":"preserve creditUnionNumber, siteId, and siteIdAndCUNumber exactly; no generated identifier","quarterly_companion":"full ZIP preserved as a source capture; not joined into point properties","candidate_status":"unapproved; complete quarterly-branch companion parity and historical successor equivalence unresolved"}}],
        "literal_geojson_catalog":{"schema_version":1,"path":f"geojson/{source_name}","bytes":len(geojson_body),"sha256":_sha(geojson_body)},
        "unsupported_output_formats":{"geopackage":"No approved published column contract; native Locator JSON/GeoJSON candidate only.","geoparquet":"No approved published column contract; release gate remains closed.","pmtiles":"No approved map display or source-to-output mapping.","shapefile_zip":"No approved field/type/identifier mapping."}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"source_url":LOCATOR_SEARCH_URL,"official_producer":AGENCY,
        "coordinate_mapping":"Point coordinates copy source longitude/latitude; no geocoding, reprojection or coordinate repair",
        "full_active_roster_query_completed":True,"source_release_accepted":False,
        "freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
        "rights_accepted":False,"mapping_approved":False,"quarterly_companion_alignment_accepted":False,
        "historical_successor_equivalence_accepted":False,"release_accepted":False})
    geometry_sha=_sha(_canonical([[row["creditUnionNumber"],row["siteId"],row["addressLongitude"],row["addressLatitude"]] for row in rows]))
    version=content_derived_version(str(source_record_sha),geometry_sha,str(source_schema_sha),identity)
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version); state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint=_sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        return _verify_and_reuse_cached_candidate(storage,state_key,prefix,version,fingerprint,
            identity,source_record_sha,source_schema_sha,audit,rows,roster,capture_root)
    # The immutable archive uses stable member order and timestamps; original
    # response bodies, wire bytes, receipts and acquisition interval are replayable.
    raw_zip=_deterministic_capture_archive(capture_root)
    # Every original wire/body/receipt is in the immutable source-capture ZIP,
    # which is itself pinned as a candidate object and replayed on cache hits.
    bundle_receipt={"complete_body":True,"status":200,"bytes":len(raw_zip),"sha256":_sha(raw_zip),
        "wire_bytes":len(raw_zip),"wire_sha256":_sha(raw_zip),"bundle_type":"lossless_source_capture_archive"}
    receipts={}
    receipt_summary={name:{"receipt_sha256":digest} for name,digest in capture_manifest["receipt_files"].items()}
    summary={"captured_at":capture_manifest["captured_at"],"capture_finished_at":capture_manifest["capture_finished_at"],"complete":True,
        "active_roster_charters":len(roster),"locator_query_pages":len(query_pages),
        "locator_feature_count":len(rows),"locator_record_sha256":locator_record_sha,
        "source_record_sha256":source_record_sha,"source_evidence_sha256":evidence_sha,
        "source_schema_sha256":source_schema_sha,"receipts":receipt_summary}
    outputs={f"geojson/{source_name}":geojson_body,
        "raw/ncua-complete-source-captures.zip":raw_zip,
        "metadata/source_manifest.json":_canonical(manifest)+b"\n",
        "metadata/source_audit.json":_canonical(audit)+b"\n",
        "metadata/processing_identity.json":_canonical(identity)+b"\n",
        "metadata/capture_manifest.json":_canonical(summary)+b"\n",
        "metadata/candidate_ready.json":_canonical({"approved":False,"version":version,
            "objects":source_objects,"source_record_sha256":source_record_sha,
            "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
            "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,
            "quarterly_companion_alignment_accepted":False,"historical_successor_equivalence_accepted":False,
            "release_accepted":False})+b"\n"}
    for name,body in outputs.items(): write_immutable(storage,f"{prefix}/{name}",body)
    descriptors={name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    for name,descriptor in descriptors.items():
        key=f"{prefix}/{name}"; snapshot=storage.object_snapshot(key)
        if snapshot is None or snapshot.size!=descriptor["bytes"] or storage.sha256_key(key)!=descriptor["sha256"] or storage.object_snapshot(key)!=snapshot:
            raise ValueError("candidate_output_physical_readback_failed:"+name)
    payload={"processing_identity":identity,"source_record_sha256":source_record_sha,
        "source_schema_sha256":source_schema_sha,"capture_receipts":receipts,"objects":descriptors}
    seal_ready_candidate(storage,state_key,version,fingerprint,payload)
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return _candidate_result(version,audit,rows,roster,capture_root)
