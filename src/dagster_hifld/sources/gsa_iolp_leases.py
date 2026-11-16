"""GSA Inventory of Owned and Leased Properties (IOLP) Leases draft candidate.

Source chain (both hops GSA-operated): the GSA IOLP landing page delegates the
full-dataset download to Data.gov; the GSA catalog record (identifier GSA-4495,
weekly cadence, CC0) exposes the current dated Leases workbook on GSA's
inventory.data.gov CKAN file store. The DHS HIFLD crosswalk contains no GSA row
(explicitly unmatched), and the frozen DataLumos HIFLD archive is never a
pipeline input. The current native workbook schema is preserved column for
column; Lease Number is the source primary identifier and leases sharing a
building intentionally repeat the same Location Code and coordinates.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import shutil
import struct
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    capture_get,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = FILE_SLUG = "gsa-iolp-leases"
LANDING_URL = ("https://www.gsa.gov/tools-overview/buildings-and-real-estate-tools/"
               "inventory-of-gsa-owned-and-leased-properties")
CATALOG_URL = "https://catalog.data.gov/api/dataset/inventory-of-owned-and-leased-properties-iolp"
CKAN_DATASET_ID = "9a9e946e-124e-46f5-a934-e458a6c1c2b2"
CATALOG_IDENTIFIER = "GSA-4495"
CATALOG_PUBLISHER = "General Services Administration"
CATALOG_CADENCE = "R/P1W"
CATALOG_LICENSE = "https://creativecommons.org/publicdomain/zero/1.0/"
XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
BUILDINGS_TITLE_FRAGMENT = "IOLP Buildings"
LEASES_TITLE_RE = re.compile(
    r"^(?P<year>\d{4})\s*-\s*(?P<month>\d{1,2})\s*-\s*(?P<day>\d{1,2})\s*-\s*IOLP Leases\.xlsx$")
DOWNLOAD_URL_RE = re.compile(
    r"^https://inventory\.data\.gov/dataset/" + re.escape(CKAN_DATASET_ID) +
    r"/resource/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/"
    r"download/[0-9.\-]+-iolp-leases\.xlsx$")
ZIP_MAGIC = b"PK\x03\x04"
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
SHEET_NAME = "Sheet1"
COUNT_MIN, COUNT_MAX = 5_000, 50_000
EXPECTED_FIELDS: list[tuple[str, str]] = [
    ("Location Code", "string"),
    ("Real Property Asset Name", "string"),
    ("Installation Name", "string"),
    ("Federal Leased Code", "string"),
    ("GSA Region", "int32"),
    ("Street Address", "string"),
    ("City", "string"),
    ("State", "string"),
    ("Zip Code", "string"),
    ("Latitude", "double"),
    ("Longitude", "double"),
    ("Building Rentable Square Feet", "double"),
    ("Available Square Feet", "double"),
    ("Congressional District", "string"),
    ("Congressional District Representative", "string"),
    ("Lease Number", "string"),
    ("Lease Effective Date", "string"),
    ("Lease Expiration Date", "string"),
    ("Real Property Asset type", "string"),
]
FIELD_NAMES = tuple(name for name, _ in EXPECTED_FIELDS)
ID_FIELD = "Lease Number"
LAT_FIELD, LON_FIELD = "Latitude", "Longitude"
ID_VALUE_RE = re.compile(r"^L[A-Z]{2}\d{5}$")
LOCATION_CODE_RE = re.compile(r"^[A-Z]{2}\d{4}$")
LEASE_DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
CONGRESSIONAL_DISTRICT_RE = re.compile(r"^\d{4}$")
GEOMETRY_TYPES = ["Point"]
WGS84 = CRS.from_epsg(4326)
VALUE_DOMAIN_FIELDS = ("Federal Leased Code", "Real Property Asset type",
                       "GSA Region", "State")


class GsaIolpLeasesError(ValueError):
    """Untrusted, incomplete or contract-drifting source input; never release."""


def _redact_url(url: object) -> str | None:
    """Strip any signed query string before a URL may be stored durably."""
    if not isinstance(url, str) or not url:
        return None
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}{parts.path}"


def parse_release_date(title: str) -> str:
    match = LEASES_TITLE_RE.match(title.strip())
    if not match:
        raise GsaIolpLeasesError("catalog_leases_title_unrecognized")
    try:
        stamp = datetime(int(match["year"]), int(match["month"]), int(match["day"]),
                         tzinfo=timezone.utc)
    except ValueError:
        raise GsaIolpLeasesError("catalog_leases_title_date_invalid") from None
    return stamp.strftime("%Y-%m-%d")


def select_leases_distribution(catalog: dict) -> dict:
    """Strictly select the current IOLP Leases workbook from the GSA catalog record."""
    results = catalog.get("results")
    if not isinstance(results, list) or not results or not isinstance(results[0], dict):
        raise GsaIolpLeasesError("catalog_results_missing")
    dcat = results[0].get("dcat")
    if not isinstance(dcat, dict):
        raise GsaIolpLeasesError("catalog_dcat_missing")
    if dcat.get("identifier") != CATALOG_IDENTIFIER:
        raise GsaIolpLeasesError("catalog_identifier_drift")
    publisher = dcat.get("publisher")
    if not isinstance(publisher, dict) or publisher.get("name") != CATALOG_PUBLISHER:
        raise GsaIolpLeasesError("catalog_publisher_drift")
    if dcat.get("accrualPeriodicity") != CATALOG_CADENCE:
        raise GsaIolpLeasesError("catalog_cadence_drift")
    if dcat.get("license") != CATALOG_LICENSE:
        raise GsaIolpLeasesError("catalog_license_drift")
    modified = dcat.get("modified")
    if not isinstance(modified, str) or not re.match(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$", modified):
        raise GsaIolpLeasesError("catalog_modified_missing")
    distributions = dcat.get("distribution")
    if not isinstance(distributions, list) or not distributions:
        raise GsaIolpLeasesError("catalog_distributions_missing")
    found = [item for item in distributions if isinstance(item, dict)
             and item.get("mediaType") == XLSX_MEDIA_TYPE
             and isinstance(item.get("title"), str)
             and LEASES_TITLE_RE.match(item["title"].strip())]
    if len(found) != 1:
        raise GsaIolpLeasesError("catalog_leases_distribution_not_unique")
    if not any(isinstance(item, dict) and item.get("mediaType") == XLSX_MEDIA_TYPE
               and isinstance(item.get("title"), str)
               and BUILDINGS_TITLE_FRAGMENT in item["title"] for item in distributions):
        raise GsaIolpLeasesError("catalog_buildings_distribution_missing")
    selected = found[0]
    url = selected.get("downloadURL")
    if not isinstance(url, str) or not DOWNLOAD_URL_RE.match(url):
        raise GsaIolpLeasesError("catalog_download_url_not_inventory_datagov")
    release_date = parse_release_date(selected["title"])
    if release_date > modified[:10]:
        raise GsaIolpLeasesError("catalog_release_after_modified")
    return {"title": selected["title"], "download_url": url,
            "release_date": release_date, "catalog_modified": modified,
            "identifier": CATALOG_IDENTIFIER, "publisher": CATALOG_PUBLISHER,
            "cadence": CATALOG_CADENCE, "license": CATALOG_LICENSE}


def _strict_json(body: bytes) -> dict:
    def members(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise GsaIolpLeasesError("source_duplicate_json_member")
            out[key] = value
        return out

    parsed = json.loads(body, object_pairs_hook=members,
                        parse_constant=lambda _: (_ for _ in ()).throw(
                            GsaIolpLeasesError("source_nonfinite_json")))
    if not isinstance(parsed, dict):
        raise GsaIolpLeasesError("source_json_object_required")
    return parsed


def _capture_ckan_download(client: httpx.Client, url: str, capture_root: Path, name: str,
                           *, max_bytes: int = MAX_CAPTURE_BYTES) -> tuple[bytes, dict, dict]:
    """Capture the CKAN resource download through its single signed redirect.

    The canonical CKAN GET must answer 302; the Location target must answer 200
    with a complete XLSX body. Receipts record URLs with any signed query string
    redacted, so pre-signed parameters are never stored.
    """
    observed = datetime.now(timezone.utc).isoformat()
    redirect_wire = capture_root / f"{name}.redirect.wire"
    redirect_receipt_path = capture_root / f"{name}.redirect.receipt.json"
    body_path, wire_path, receipt_path = (capture_root / f"{name}.body",
                                          capture_root / f"{name}.wire",
                                          capture_root / f"{name}.receipt.json")
    for path in (redirect_wire, redirect_receipt_path, body_path, wire_path, receipt_path):
        path.unlink(missing_ok=True)
    body_hash, wire_hash = hashlib.sha256(), hashlib.sha256()
    body_size = wire_size = 0
    redirect_target = None
    try:
        with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
            if response.status_code != 302:
                raise GsaIolpLeasesError("ckan_download_redirect_missing")
            location = response.headers.get("Location")
            if not isinstance(location, str) or not location:
                raise GsaIolpLeasesError("ckan_download_redirect_location_missing")
            redirect_target = _redact_url(location)
            chunks = []
            for chunk in response.iter_raw():
                chunks.append(chunk)
            redirect_wire.write_bytes(b"".join(chunks))
    except httpx.HTTPError:
        raise GsaIolpLeasesError("ckan_download_redirect_request_error") from None
    redirect_receipt = {"method": "GET", "url": url, "final_url": redirect_target,
                        "observed_at": observed, "status": 302, "complete_body": True,
                        "wire_bytes": redirect_wire.stat().st_size,
                        "wire_sha256": _sha(redirect_wire.read_bytes()),
                        "location_query_omitted": True}
    redirect_receipt_path.write_bytes(_canonical(redirect_receipt) + b"\n")
    observed_final = datetime.now(timezone.utc).isoformat()
    error_code = None
    final_url = None
    try:
        with client.stream("GET", location, headers={"Accept-Encoding": "identity"}) as response:
            if response.status_code != 200:
                error_code = "ckan_download_file_status_not_ok"
            final_url = str(response.url)
            body_chunks, wire_chunks = [], []
            for chunk in response.iter_raw():
                if body_size + len(chunk) > max_bytes:
                    error_code = error_code or "ckan_download_file_oversized"
                    break
                body_chunks.append(chunk); wire_chunks.append(chunk)
                body_size += len(chunk); wire_size += len(chunk)
                body_hash.update(chunk); wire_hash.update(chunk)
            body_path.write_bytes(b"".join(body_chunks))
            wire_path.write_bytes(b"".join(wire_chunks))
    except httpx.HTTPError:
        error_code = error_code or "ckan_download_file_request_error"
        final_url = None
    body, wire = body_path.read_bytes(), wire_path.read_bytes()
    receipt = {"method": "GET", "url": _redact_url(location), "final_url": _redact_url(final_url),
               "observed_at": observed_final, "status": None, "complete_body": False,
               "wire_bytes": wire_size, "wire_sha256": wire_hash.hexdigest(),
               "bytes": body_size, "sha256": body_hash.hexdigest(),
               "url_query_omitted": True, "error_code": error_code}
    if error_code is None:
        receipt["status"] = 200
        receipt["complete_body"] = True
    receipt_path.write_bytes(_canonical(receipt) + b"\n")
    if error_code is not None or not body.startswith(ZIP_MAGIC):
        raise GsaIolpLeasesError(error_code or "ckan_download_file_not_xlsx")
    return body, receipt, redirect_receipt


def _ogr_types() -> list[tuple[str, str]]:
    mapping = {"string": "OFTString", "int32": "OFTInteger", "double": "OFTReal"}
    return [(name, mapping[kind]) for name, kind in EXPECTED_FIELDS]


def _decode_workbook(body: bytes, tmp: Path) -> dict:
    if not body.startswith(ZIP_MAGIC):
        raise GsaIolpLeasesError("source_xlsx_magic_invalid")
    path = Path(tmp) / "iolp_leases.xlsx"
    path.write_bytes(body)
    try:
        info = pyogrio.read_info(str(path))
    except Exception:
        raise GsaIolpLeasesError("source_xlsx_unreadable") from None
    if info.get("layer_name") != SHEET_NAME or info.get("geometry_type") is not None \
            or not COUNT_MIN <= int(info["features"]) <= COUNT_MAX:
        raise GsaIolpLeasesError("source_layer_metadata_drift")
    observed_types = list(zip(info["fields"], info["ogr_types"]))
    if observed_types != _ogr_types():
        raise GsaIolpLeasesError(
            "source_schema_drift:" + _sha(_canonical(observed_types)))
    try:
        _meta, table = pyogrio.read_arrow(str(path))
    except Exception:
        raise GsaIolpLeasesError("source_xlsx_unreadable") from None
    schema = {field.name: str(field.type) for field in table.schema}
    if list(schema) != list(dict(EXPECTED_FIELDS)) or schema != dict(EXPECTED_FIELDS):
        raise GsaIolpLeasesError("source_arrow_schema_drift")
    return _validate_population(table)


def _validate_population(table) -> dict:
    columns = {name: table.column(name).to_pylist() for name in FIELD_NAMES}
    count = table.num_rows
    ids = columns[ID_FIELD]
    if any(not isinstance(value, str) or not ID_VALUE_RE.match(value) for value in ids):
        raise GsaIolpLeasesError("source_id_value_drift")
    if len(set(ids)) != count:
        raise GsaIolpLeasesError("source_id_not_unique")
    location_codes = columns["Location Code"]
    if any(not isinstance(value, str) or not LOCATION_CODE_RE.match(value)
           for value in location_codes):
        raise GsaIolpLeasesError("source_location_code_value_drift")
    duplicate_location_codes = sorted(
        code for code, occurrences in _counts(location_codes).items()
        if code != "null" and occurrences > 1)
    for name in ("Lease Effective Date", "Lease Expiration Date"):
        if any(not isinstance(value, str) or not LEASE_DATE_RE.match(value)
               for value in columns[name]):
            raise GsaIolpLeasesError("source_lease_date_format_drift:" + name)
    for name, low, high in ((LAT_FIELD, -90.0, 90.0), (LON_FIELD, -180.0, 180.0)):
        if any(value is None or not math.isfinite(value) or not low <= value <= high
               for value in columns[name]):
            raise GsaIolpLeasesError("source_coordinate_range_drift:" + name)
    if any(value is not None and (not isinstance(value, str)
                                  or not CONGRESSIONAL_DISTRICT_RE.match(value))
           for value in columns["Congressional District"]):
        raise GsaIolpLeasesError("source_congressional_district_format_drift")
    for name in FIELD_NAMES:
        if any(value is not None and isinstance(value, float) and not math.isfinite(value)
               for value in columns[name]):
            raise GsaIolpLeasesError("source_nonfinite_attribute:" + name)
    rows = [[columns[name][index] for name in FIELD_NAMES] for index in range(count)]
    id_sha = _sha(_canonical(sorted(ids)))
    record_sha = _sha(_canonical(rows))
    geometry_sha = _sha(_canonical(
        [[ids[index], columns[LON_FIELD][index], columns[LAT_FIELD][index]]
         for index in range(count)]))
    audit = {
        "source_feature_count": count, "source_population_complete": True,
        "source_id_field": ID_FIELD, "source_id_unique": True,
        "source_id_sha256": id_sha, "source_record_sha256": record_sha,
        "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": _sha(_canonical(EXPECTED_FIELDS)),
        "source_schema": [list(field) for field in EXPECTED_FIELDS],
        "source_crs": "EPSG:4326", "source_geometry_type": "Point",
        "source_bounds": {"lat_min": min(columns[LAT_FIELD]), "lat_max": max(columns[LAT_FIELD]),
                          "lon_min": min(columns[LON_FIELD]), "lon_max": max(columns[LON_FIELD])},
        "source_null_counts": {name: sum(1 for value in columns[name] if value is None)
                               for name in FIELD_NAMES},
        "source_value_distributions": {
            name: dict(sorted(_counts(columns[name]).items())[:64])
            for name in VALUE_DOMAIN_FIELDS},
        "source_location_code_unique": len(duplicate_location_codes) == 0,
        "source_duplicate_location_codes": len(duplicate_location_codes),
        "source_coverage_scope": "Complete current GSA IOLP Leases workbook population as "
                                 "published by GSA on its data.gov record (weekly cadence); "
                                 "multiple leases sharing a building intentionally repeat the "
                                 "same Location Code and coordinates while Lease Number stays "
                                 "unique.",
        "drift_guard": "Exact sheet name, column names/order and GDAL Arrow types, bounded row "
                       "count band, unique ^L[A-Z]{2}\\d{5}$ Lease Numbers, ^[A-Z]{2}\\d{4}$ "
                       "non-null Location Codes, M/D/YYYY non-null lease dates, finite WGS84 "
                       "coordinates, ^\\d{4}$ non-null congressional districts, finite "
                       "attribute values.",
    }
    return {"columns": columns, "count": count, "audit": audit,
            "id_sha": id_sha, "record_sha": record_sha, "geometry_sha": geometry_sha}


def _counts(values: list) -> dict:
    result: dict = {}
    for value in values:
        key = "null" if value is None else str(value)
        result[key] = result.get(key, 0) + 1
    return result


def _point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, float(x), float(y))


def _parquet(columns: dict, count: int, bbox: list[float]) -> bytes:
    arrays, fields = [], []
    for name, kind in EXPECTED_FIELDS:
        arrays.append(pa.array(columns[name], type=pa.type_for_alias(kind), safe=True))
        fields.append(pa.field(name, pa.type_for_alias(kind), nullable=True))
    geometry = [_point_wkb(columns[LON_FIELD][index], columns[LAT_FIELD][index])
                for index in range(count)]
    arrays.append(pa.array(geometry, type=pa.binary()))
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB", "geometry_types": GEOMETRY_TYPES,
                                    "crs": WGS84.to_json_dict(),
                                    "bbox": bbox, "epoch": None}}}
    schema = pa.schema(fields, metadata={b"geo": _canonical(geo)})
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), sink,
                   compression="zstd", use_dictionary=False)
    body = sink.getvalue().to_pybytes()
    table = pq.read_table(pa.BufferReader(body))
    if table.num_rows != count:
        raise GsaIolpLeasesError("candidate_row_count_readback_drift")
    for name, kind in EXPECTED_FIELDS:
        if str(table[name].type) != kind:
            raise GsaIolpLeasesError("candidate_attribute_type_readback_drift:" + name)
        if table[name].to_pylist() != columns[name]:
            raise GsaIolpLeasesError("candidate_attribute_readback_drift:" + name)
    if any(table["geometry"][index].as_py() != geometry[index] for index in range(count)):
        raise GsaIolpLeasesError("candidate_geometry_readback_drift")
    return body


def _load_capture(root: Path, identity: dict) -> dict:
    catalog_body = (root / "catalog_discovery.body").read_bytes()
    catalog_receipt = json.loads((root / "catalog_discovery.receipt.json").read_bytes())
    body = (root / "leases_xlsx.body").read_bytes()
    receipt = json.loads((root / "leases_xlsx.receipt.json").read_bytes())
    redirect_receipt = json.loads((root / "leases_xlsx.redirect.receipt.json").read_bytes())
    if catalog_receipt.get("method") != "GET" or catalog_receipt.get("url") != CATALOG_URL \
            or catalog_receipt.get("final_url") != CATALOG_URL \
            or catalog_receipt.get("complete_body") is not True \
            or catalog_receipt.get("status") != 200 \
            or catalog_receipt.get("bytes") != len(catalog_body) \
            or catalog_receipt.get("sha256") != _sha(catalog_body):
        raise GsaIolpLeasesError("capture_catalog_receipt_invalid")
    derived = select_leases_distribution(_strict_json(catalog_body))
    if derived != identity:
        raise GsaIolpLeasesError("capture_manifest_identity_drift")
    if receipt.get("method") != "GET" \
            or receipt.get("url") != receipt.get("final_url") \
            or receipt.get("final_url") != redirect_receipt.get("final_url") \
            or receipt.get("complete_body") is not True or receipt.get("status") != 200 \
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body) \
            or receipt.get("final_url") != _redact_url(receipt.get("final_url")):
        raise GsaIolpLeasesError("capture_workbook_receipt_invalid")
    if redirect_receipt.get("status") != 302 or redirect_receipt.get("complete_body") is not True \
            or redirect_receipt.get("final_url") is None \
            or redirect_receipt.get("url") != identity["download_url"] \
            or redirect_receipt.get("final_url") != _redact_url(redirect_receipt.get("final_url")):
        raise GsaIolpLeasesError("capture_redirect_receipt_invalid")
    if not body.startswith(ZIP_MAGIC):
        raise GsaIolpLeasesError("capture_workbook_not_xlsx")
    return {"identity": identity, "catalog_body": catalog_body, "body": body,
            "catalog_receipt": catalog_receipt, "receipt": receipt,
            "redirect_receipt": redirect_receipt, "capture_root": root}


def _capture(root: Path, storage: StagingStorageResource, version: str) -> dict:
    if root.is_symlink() or any(parent.is_symlink() for parent in root.parents):
        raise GsaIolpLeasesError("unsafe_capture_directory")
    manifest_path = root / "capture_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_bytes())
        if manifest.get("complete") is not True:
            raise GsaIolpLeasesError("capture_manifest_incomplete")
        identity = manifest.get("identity")
        if not isinstance(identity, dict) or not isinstance(identity.get("download_url"), str):
            raise GsaIolpLeasesError("capture_manifest_identity_invalid")
        roster = manifest.get("artifact_roster")
        if not isinstance(roster, dict):
            raise GsaIolpLeasesError("capture_manifest_roster_invalid")
        validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
        if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
            raise GsaIolpLeasesError("capture_roster_digest_drift")
        return _load_capture(root, identity)
    if any(root.glob("*")):
        raise GsaIolpLeasesError("unsealed_capture_attempt")
    root.mkdir(parents=True, exist_ok=True)
    captured: dict[str, tuple[bytes, dict]] = {}
    try:
        with httpx.Client(follow_redirects=False,
                          timeout=httpx.Timeout(60, connect=15),
                          headers={"User-Agent": "hifld-next-datasets/gsa-iolp-leases-draft"}) as client:
            catalog_body, catalog_receipt = capture_get(
                client, CATALOG_URL, root, "catalog_discovery", max_bytes=16 * 1024 * 1024)
            captured["catalog_discovery"] = (catalog_body, catalog_receipt)
            identity = select_leases_distribution(_strict_json(catalog_body))
            body, receipt, redirect_receipt = _capture_ckan_download(
                client, identity["download_url"], root, "leases_xlsx")
            captured["leases_xlsx"] = (body, receipt)
        manifest_path.write_bytes(_canonical({
            "complete": True,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "identity": identity,
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"}),
        }) + b"\n")
        return {"identity": identity, "catalog_body": catalog_body, "body": body,
                "catalog_receipt": catalog_receipt, "receipt": receipt,
                "redirect_receipt": redirect_receipt, "capture_root": root}
    except Exception as exc:
        receipts = {}
        for path in sorted(root.glob("*.receipt.json")):
            try:
                receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except Exception:
                pass
        manifest_path.write_bytes(_canonical({
            "complete": False, "captured_at": datetime.now(timezone.utc).isoformat(),
            "error_code": type(exc).__name__, "receipts": receipts}) + b"\n")
        quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captured,
                                      type(exc).__name__)
        raise


def capture_gsa_iolp_leases(storage: StagingStorageResource, *, version: str,
                            capture_directory: str | None = None) -> dict:
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise GsaIolpLeasesError("explicit_local_staging_resource_required")
    if not isinstance(version, str) or not version or Path(version).name != version \
            or "/" in version or "\\" in version:
        raise GsaIolpLeasesError("unsafe_capture_version")
    stage = Path(storage.local_dir).absolute()
    if ".." in stage.parts or any(parent.is_symlink() for parent in (stage, *stage.parents)):
        raise GsaIolpLeasesError("unsafe_local_staging_root")
    stage.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(stage).free < 300_000_000:
        raise GsaIolpLeasesError("disk_floor_before_acquisition")
    root = (Path(capture_directory).absolute() if capture_directory
            else Path("/tmp/hifld-native-reconstruction/395/captures")
            / (version + "-" + uuid4().hex))
    captured = _capture(root, storage, version)
    identity = captured["identity"]
    catalog_receipt, receipt = captured["catalog_receipt"], captured["receipt"]
    root = captured["capture_root"]
    with tempfile.TemporaryDirectory() as tmp:
        decoded = _decode_workbook(captured["body"], Path(tmp))
        audit_bounds = decoded["audit"]["source_bounds"]
        parquet = _parquet(decoded["columns"], decoded["count"],
                           [audit_bounds["lon_min"], audit_bounds["lat_min"],
                            audit_bounds["lon_max"], audit_bounds["lat_max"]])
    audit = decoded["audit"]
    identity_pkg = processing_identity(Path(__file__).parents[3])
    evidence_sha = _sha(_canonical({
        "catalog_discovery": {"body_sha256": _sha(captured["catalog_body"]),
                              "wire_sha256": catalog_receipt["wire_sha256"]},
        "leases_xlsx": {"body_sha256": _sha(captured["body"]),
                        "wire_sha256": receipt["wire_sha256"]}}))
    audit.update({
        "source_capture_evidence_sha256": evidence_sha,
        "source_release_date": identity["release_date"],
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False, "baseline_accepted": False,
    })
    cv = "v" + _sha(_canonical({
        "workbook_sha256": _sha(captured["body"]),
        "record_sha256": audit["source_record_sha256"],
        "geometry_sha256": audit["source_geometry_sha256"],
        "schema_sha256": audit["source_schema_sha256"],
        "processing_identity": identity_pkg}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, cv)
    op = f"geoparquet/{FILE_SLUG}.parquet"
    inputs = [
        {"input_id": "catalog_discovery", "producer": CATALOG_PUBLISHER,
         "host": urlsplit(CATALOG_URL).hostname, "role": "producer_publication",
         "canonical_url": CATALOG_URL, "retrieval_url": CATALOG_URL,
         "retrieved_at": catalog_receipt.get("observed_at"),
         "http_status": catalog_receipt["status"],
         "bytes": len(captured["catalog_body"]),
         "sha256": _sha(captured["catalog_body"]),
         "release_date": identity["release_date"],
         "license": CATALOG_LICENSE,
         "attribution": "GSA catalog record GSA-4495 'Inventory of Owned and Leased Properties "
                        "(IOLP)', weekly cadence (R/P1W), CC0 1.0; modified "
                        + identity["catalog_modified"] + "."},
        {"input_id": "leases_xlsx", "producer": CATALOG_PUBLISHER,
         "host": urlsplit(identity["download_url"]).hostname,
         "role": "producer_publication",
         "canonical_url": identity["download_url"],
         "retrieval_url": identity["download_url"],
         "observed_final_url": receipt["final_url"],
         "retrieved_at": receipt["observed_at"],
         "http_status": receipt["status"], "bytes": len(captured["body"]),
         "sha256": _sha(captured["body"]),
         "wire_bytes": receipt["wire_bytes"],
         "wire_sha256": receipt["wire_sha256"],
         "release_date": identity["release_date"],
         "license": CATALOG_LICENSE,
         "attribution": "GSA IOLP Leases workbook '" + identity["title"] + "' on the GSA-operated "
                        "inventory.data.gov CKAN file store; single signed redirect to the "
                        "GovCloud S3 file store, query string redacted in receipts."},
    ]
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "GSA Inventory of Owned and Leased Properties (IOLP) Leases",
        "description": "Current GSA-published IOLP Leases workbook (release "
                       + identity["release_date"] + ") staged from the GSA data.gov catalog record "
                       "and the GSA inventory.data.gov file store; native 19-column schema, "
                       "Lease Number identifiers and WGS84 point coordinates preserved. Draft "
                       "candidate: release, baseline and rights acceptance remain unresolved.",
        "publisher": CATALOG_PUBLISHER, "agency": "U.S. General Services Administration",
        "source_url": CATALOG_URL, "license": CATALOG_LICENSE,
        "source_modified": identity["catalog_modified"],
        "date_issued": None, "date_modified": identity["release_date"],
        "processor": "HIFLD Next", "code_commit": identity_pkg["code_commit"],
        "inputs": inputs,
        "transformations": [{"operation": "Native XLSX sheet to WGS84 point GeoParquet",
                             "parameters": {"source_fields": "all 19 native columns preserved in "
                                                            "order, source types and values",
                                            "source_ids": "Lease Number retained unchanged as the "
                                                          "unique lease identifier; no synthetic "
                                                          "identifiers",
                                            "geometry": "Longitude/Latitude encoded as WKB Point "
                                                        "without reprojection",
                                            "zip_codes": "Zip Code kept as text with leading zeros",
                                            "lease_dates": "Lease Effective/Expiration Date kept "
                                                           "as source M/D/YYYY strings",
                                            "candidate_status": "draft, unapproved"}}],
        "lineage": "GSA publishes IOLP on its data.gov record (identifier GSA-4495, weekly "
                   "cadence); the gsa.gov IOLP landing page explicitly delegates the full-dataset "
                   "download to Data.gov, and the Leases workbook itself is served from GSA's "
                   "inventory.data.gov CKAN file store via a single signed GovCloud S3 redirect. "
                   "GSA's ISO 19139 metadata (IOLP19139, 2026-07-09) identifies the layer as "
                   "National Geospatial Data Asset NGDAID133 in the Real Property Theme. The DHS "
                   "HIFLD Open crosswalk contains no GSA row (explicitly unmatched), and the "
                   "frozen DataLumos HIFLD archive copy is identification context only, never a "
                   "pipeline input. The Leases edition carries lease-specific fields (Federal "
                   "Leased Code, Lease Number, Lease Effective/Expiration Date) and omits the "
                   "Buildings-only Construction Date and Building Status columns; Lease Number "
                   "is unique while Location Code repeats for multiple leases in one building.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "All 19 source columns, Lease Number values, leading-zero zip codes and "
                      "native WGS84 coordinates are retained.",
            "objects": [{"path": op, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No published contract/mapping approved",
                                       "pmtiles": "No display transformation approved"},
        "promotion_allowed": False, "source_accepted": False, "baseline": None,
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs = {op: parquet,
               "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity_pkg) + b"\n",
               "metadata/candidate_ready.json": _canonical(
                   {"approved": False, "version": cv,
                    "objects": {op: {"bytes": len(parquet), "sha256": _sha(parquet)}},
                    "source_population_complete": True, "source_release_accepted": False,
                    "freshness_accepted": False, "schema_contract_accepted": False,
                    "id_contract_accepted": False, "rights_accepted": False,
                    "coverage_accepted": False, "release_accepted": False}) + b"\n",
               "raw/catalog_discovery.body": captured["catalog_body"],
               "raw/catalog_discovery.wire": (root / "catalog_discovery.wire").read_bytes(),
               "raw/catalog_discovery.receipt.json": (root / "catalog_discovery.receipt.json").read_bytes(),
               "raw/leases_xlsx.body": captured["body"],
               "raw/leases_xlsx.wire": (root / "leases_xlsx.wire").read_bytes(),
               "raw/leases_xlsx.receipt.json": (root / "leases_xlsx.receipt.json").read_bytes(),
               "raw/leases_xlsx.redirect.wire": (root / "leases_xlsx.redirect.wire").read_bytes(),
               "raw/leases_xlsx.redirect.receipt.json": (root / "leases_xlsx.redirect.receipt.json").read_bytes()}
    state = f"_construction-state/{DATASET_SLUG}/{cv}.json"
    fingerprint = _sha(_canonical({
        "identity": identity_pkg, "workbook_sha256": _sha(captured["body"]),
        "record_sha256": audit["source_record_sha256"],
        "schema_sha256": audit["source_schema_sha256"]}))
    if storage.object_snapshot(state) is not None:
        original = json.loads(load_first_construction(storage, state, cv, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("objects"), dict):
            raise GsaIolpLeasesError("cached_candidate_state_invalid")
        verify_cached_candidate(storage, Path(__file__).parents[3], state, prefix, cv,
                                fingerprint, identity_pkg, audit["source_record_sha256"],
                                audit["source_schema_sha256"], original["objects"],
                                original.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": cv,
                "feature_count": decoded["count"], "source_population_complete": True,
                "source_id_sha256": audit["source_id_sha256"],
                "source_release_date": identity["release_date"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "release_accepted": False}
    descriptors = {}
    for name, body in outputs.items():
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        if snapshot is None or snapshot.size != len(body) or storage.sha256_key(key) != _sha(body):
            raise GsaIolpLeasesError("candidate_object_readback_failed:" + name)
        descriptors[name] = {"bytes": len(body), "sha256": _sha(body)}
    seal_ready_candidate(storage, state, cv, fingerprint, {
        "processing_identity": identity_pkg,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": {"catalog_discovery": dict(catalog_receipt),
                             "leases_xlsx": dict(receipt)},
        "objects": descriptors})
    validate_construction_code_identity(
        Path(__file__).parents[3], identity_pkg["code_commit"], identity_pkg["files"],
        identity=identity_pkg)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": cv,
            "feature_count": decoded["count"], "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"],
            "source_record_sha256": audit["source_record_sha256"],
            "source_geometry_sha256": audit["source_geometry_sha256"],
            "source_release_date": identity["release_date"],
            "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "release_accepted": False}
