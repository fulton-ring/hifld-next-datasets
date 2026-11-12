"""Lossless, approval-gated capture of the FEMA NFHL "Flood Hazard Zones" polygons
from FEMA's own National Flood Hazard Layer (NFHL) county/community databases.

FEMA publishes the NFHL — the producer's own database of effective flood hazard
data that its MSC "GIS Web Services" page documents behind the public REST
service — as state- and county/community-level NFHL Database extracts through
the FEMA Map Service Center. This module captures every NFHL Database product
that FEMA's own NFHL Database search page
(https://hazards.fema.gov/femaportal/NFHL/searchResult.action, one complete
HTML listing of all county/community products with their update dates, sizes
and download links) lists, downloading each product zip from that page's own
download link and extracting its ``S_FLD_HAZ_AR`` polygon feature class
("Flood Hazard Area"), the producer's publication of this HIFLD inventory
dataset (``nfhl/flood-hazard-zones-1``; the DataLumos 239971 historical
archive copy is identification context only, never a pipeline input).

Why the bulk NFHL Databases and not the public REST layer 28 (dated evidence,
2026-11-12): the REST service's query endpoint permanently refuses to serve
one roster feature's full-resolution geometry — roster offset 1328 (OBJECTID
1886) returns a complete HTTP 200 Esri 500 error document after 8-20 s of
server work for every faithful request shape (resultOffset paging,
``where=OBJECTID=1886``, geometry-only fields, envelope intersects), while
attributes-only and reduced-precision requests are served, the immediately
adjacent roster features serve at full fidelity, and the same feature is
present complete in FEMA's bulk distribution (Winston County NFHL Database
01133C_20110916.zip ``S_FLD_HAZ_AR``: one polygon, 1,541,156 vertices, bounds
-87.6292..-87.1106 x 33.9849..34.3028, matching the REST feature's diagnostic
envelope). A REST capture of layer 28 therefore can never tile the complete
roster, while FEMA's own bulk distribution serves the complete population.
Both are the same maintained FEMA NFHL product; this module captures the
distribution FEMA itself serves completely.

Scale economics measured by dated anonymous probes (2026-11-12): the listing
rosters 2,672 county/community products (~89 GiB of zips by FEMA's own size
labels); the ``S_FLD_HAZ_AR`` feature class carries the NFHL schema verified
identical across 2003-2025 product vintages (19 typed DBF fields plus polygon
geometry; unset numeric BFE/depth/velocity values are the producer's -9999
sentinel, retained verbatim). The module therefore streams per product: each
product zip is captured once with a hash-pinned receipt and verified against
the zip's own CRCs, its feature class is read and folded into
order-dependent record/geometry/roster digests and an incrementally written
GeoParquet candidate, and the extracted sidecars are released immediately so
disk stays bounded.

- The capture attempt directory is stable per version and persists accepted
  product zips with hash-pinned receipts; a resumed run re-verifies each
  cached zip against its receipt digest, re-fetches the listing fresh and
  re-derives the product roster, and downloads only the missing products.
  Listing stability is enforced by capturing the listing again after the
  download phase and requiring the parsed product roster to be unchanged.
- Geometry is the producer's own shapefile geometry as GDAL serializes it to
  WKB: verbatim vertices, the shapefile's explicit part structure (first ring
  of each part exterior, following rings holes — shapefile semantics, not an
  orientation heuristic), and the shapefile's implicit ring closure made
  explicit as WKB requires. GDAL's version is pinned in the processing
  identity, and the geometry digest folds each feature's WKB bytes verbatim.
- Until a dated complete capture establishes the population pins below, the
  module runs in live mode: acquisition and streaming validation complete,
  the computed population contract is written to ``population_pins.json`` in
  the attempt directory, and the run fails closed with
  ``source_population_pins_not_established`` without staging anything. With
  pins set, the module reproduces every pin and stages the unapproved
  candidate exactly like its NFHL siblings.
"""
from __future__ import annotations

import hashlib
import html as html_module
import json
import re
import shutil
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import quote, unquote, urlencode, urlsplit
from uuid import uuid4

import httpx
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)

DATASET_SLUG = "nfhl-flood-hazard-zones"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
# FEMA's own NFHL Database search page: one complete listing of every
# county/community NFHL Database product (Item ID, county, state, update
# date, size, download link).
LISTING_URL = "https://hazards.fema.gov/femaportal/NFHL/searchResult.action"
# Each listing row's download link points at this portal servlet, which
# redirects to the product zip on the same FEMA host.
SERVLET_URL = "https://hazards.fema.gov/femaportal/NFHL/Download/ProductsDownLoadServlet"
FEATURE_CLASS = "S_FLD_HAZ_AR"
FEATURE_CLASS_SUFFIXES = (".shp", ".shx", ".dbf", ".prj")
FEATURE_CLASS_OPTIONAL_SUFFIXES = (".cpg",)
ALLOWED_DOWNLOAD_HOSTS = {"hazards.fema.gov"}
LISTING_MARKER = "National Flood Hazard Layer (NFHL) Database Search Results"
LISTING_MAX_BYTES = 64 * 1024 * 1024
PRODUCT_ZIP_MAX_BYTES = 2 * 1024 * 1024 * 1024
LISTING_ATTEMPTS = 4
LISTING_BASE_DELAY_SECONDS = 10.0
ZIP_ATTEMPTS = 4
ZIP_BASE_DELAY_SECONDS = 10.0
MIN_FREE_BYTES = 24_000_000_000
# Population pins below are None until a dated complete capture of every
# listed product establishes them; see the module docstring. In live mode the
# capture completes, writes the computed contract to ``population_pins.json``,
# and fails closed without staging.
EXPECTED_PRODUCT_COUNT = None
EXPECTED_PRODUCT_ROSTER_SHA256 = None
EXPECTED_COUNT = None
EXPECTED_ID_SHA256 = None
EXPECTED_SCHEMA_SHA256 = None
EXPECTED_RECORD_SHA256 = None
EXPECTED_GEOMETRY_SHA256 = None
EXPECTED_OBSERVED_BOUNDS = None
EXPECTED_NULL_GEOMETRY_COUNT = None
EXPECTED_NAN_GEOMETRY_COUNT = None

_ARROW_TYPES = {"string": pa.string(), "double": pa.float64(),
                "int64": pa.int64(), "boolean": pa.bool_()}


class _PinsPendingError(Exception):
    """Raised when a live-mode capture completes but population pins are unset."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _plain_text(value: object) -> str:
    return html_module.unescape(value).strip() if isinstance(value, str) else value


_ITEM_ID = re.compile(r"^[0-9A-Z]{5,6}-NFHL$")
_CENTER_CELL = re.compile(r'^\s*<center class="txt12">(.*?)</center>\s*$', re.S)
_HIDDEN_SPAN = re.compile(r'<span style="display:none;">([^<>]*)</span>')
_HREF_CELL = re.compile(r'<a href="Download/ProductsDownLoadServlet\?([^"]+)"')
_JAVA_MONTHS = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
_SIZE_CELL = re.compile(r"^([0-9.]+)\s*(KB|MB|GB)$")
_JAVA_DATE = re.compile(r"^([A-Z][a-z]{2}) ([A-Z][a-z]{2}) +(\d{1,2}) \d{2}:\d{2}:\d{2} ([A-Z]{2,5}) (\d{4})$")


def _servlet_url(product: Mapping[str, str]) -> str:
    """The listing row's own download link, percent-encoded for provenance."""
    return SERVLET_URL + "?" + urlencode({
        "DFIRMID": product["dfirm_id"], "state": product["state"],
        "county": product["county"], "fileName": product["file_name"]},
        quote_via=quote)


def _parse_listing(body: bytes) -> list[dict[str, str]]:
    """Parse FEMA's NFHL Database listing into its canonical product roster.

    Every product row must carry a complete, self-consistent six-cell record
    (Item ID, county, state, update date, size, download link): an
    ``<id>-NFHL`` Item ID whose identifier matches the row's download link, a
    java-format update date whose calendar date matches the zip file name's
    publication date, and a FEMA size label. Any structural surprise fails
    closed; the roster is returned in canonical Item ID order.
    """
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("source_listing_structure_drift") from exc
    if LISTING_MARKER not in text:
        raise ValueError("official_fema_nfhl_database_listing_drift")
    products: list[dict[str, str]] = []
    seen_items: set[str] = set()
    seen_files: set[str] = set()
    for chunk in text.split("<tr>")[1:]:
        if "ProductsDownLoadServlet" not in chunk:
            continue
        row = chunk.split("</tr>")[0]
        pieces = re.split(r"(<td[^>]*>)", row)
        contents = pieces[2::2]
        if len(pieces) != 13 or len(contents) != 6:
            raise ValueError("source_listing_structure_drift")
        contents = [content.replace("</td>", "") for content in contents]
        item_center = _CENTER_CELL.fullmatch(contents[0])
        county_center = _CENTER_CELL.fullmatch(contents[1])
        state_center = _CENTER_CELL.fullmatch(contents[2])
        date_center = _CENTER_CELL.fullmatch(contents[3])
        size_center = _CENTER_CELL.fullmatch(contents[4])
        href_match = _HREF_CELL.search(contents[5])
        if None in (item_center, county_center, state_center, date_center, size_center) \
                or href_match is None:
            raise ValueError("source_listing_structure_drift")
        item_match = _ITEM_ID.fullmatch(item_center.group(1).strip())
        county_span = _HIDDEN_SPAN.search(county_center.group(1))
        date_span = _HIDDEN_SPAN.search(date_center.group(1))
        if item_match is None or county_span is None or date_span is None:
            raise ValueError("source_listing_structure_drift")
        item_id = _plain_text(item_match.group(0))
        # The download link carries the display county name, not the hidden
        # sort name (they differ, e.g. "TERRITORY OF THE VIRGIN ISLANDS").
        county_name = county_center.group(1)[county_span.end():]
        county = _plain_text(county_name)
        if not county:
            raise ValueError("source_listing_structure_drift")
        state = _plain_text(state_center.group(1))
        if not state:
            raise ValueError("source_listing_structure_drift")
        size_match = _SIZE_CELL.fullmatch(_plain_text(size_center.group(1)))
        if size_match is None:
            raise ValueError("source_listing_structure_drift")
        size_label = size_match.group(0)
        java_date = _plain_text(date_span.group(1))
        date_match = _JAVA_DATE.fullmatch(java_date)
        if date_match is None or date_match.group(2) not in _JAVA_MONTHS:
            raise ValueError("source_listing_structure_drift")
        params = dict(re.findall(r"([^&=]+)=([^&]*)", href_match.group(1)))
        dfirm_id = _plain_text(unquote(params.get("DFIRMID", "")))
        link_state = _plain_text(unquote(params.get("state", "")))
        link_county = _plain_text(unquote(params.get("county", "")))
        file_name = _plain_text(unquote(params.get("fileName", "")))
        if dfirm_id + "-NFHL" != item_id:
            raise ValueError("source_listing_structure_drift")
        if link_state != state or link_county != county:
            raise ValueError("source_listing_structure_drift")
        if not re.fullmatch(rf"{re.escape(dfirm_id)}_[0-9]{{8}}\.zip", file_name):
            raise ValueError("source_listing_structure_drift")
        date_match = _JAVA_DATE.fullmatch(java_date)
        if date_match is None or date_match.group(2) not in _JAVA_MONTHS:
            raise ValueError("source_listing_structure_drift")
        # The listing's update date and the zip file name's publication date
        # are distinct producer fields (observed 2026-11-12, 47167C: listing
        # update 2011-10-21, product publication 2019-08-28); both are
        # recorded, and either changing is roster drift.
        file_date = file_name.split("_", 1)[1].split(".", 1)[0]
        if item_id in seen_items or file_name in seen_files:
            raise ValueError("source_product_roster_drift")
        seen_items.add(item_id)
        seen_files.add(file_name)
        products.append({"item_id": item_id, "dfirm_id": dfirm_id, "state": state,
                         "county": county, "file_name": file_name,
                         "update_date_java": java_date, "file_date": file_date,
                         "size_label": size_label})
    if not products:
        raise ValueError("source_listing_structure_drift")
    return sorted(products, key=lambda product: product["item_id"])


def product_roster_sha256(products: list[dict[str, str]]) -> str:
    return _sha(_canonical(products))


def _update_date_iso(product: Mapping[str, str]) -> str:
    """The product's own publication date (baked into its zip file name), ISO form."""
    if not re.fullmatch(r"[0-9]{8}", product["file_date"]):
        raise ValueError("source_listing_structure_drift")
    return f"{product['file_date'][0:4]}-{product['file_date'][4:6]}-{product['file_date'][6:8]}"


def _capture_listing(client: httpx.Client, capture_root: Path, name: str) -> bytes:
    """Capture one listing bracket, retrying bounded times on transient failures."""
    for attempt in range(1, LISTING_ATTEMPTS + 1):
        try:
            body, _receipt = capture_get(client, LISTING_URL, capture_root, name,
                                         max_bytes=LISTING_MAX_BYTES)
            return body
        except ValueError as exc:
            if attempt >= LISTING_ATTEMPTS or not str(exc).startswith("incomplete_source_capture:"):
                raise
        time.sleep(LISTING_BASE_DELAY_SECONDS * (3 ** (attempt - 1)))
    raise ValueError(f"incomplete_source_capture:{name}")


def _capture_download_location(client: httpx.Client, product: Mapping[str, str],
                               capture_root: Path, *, attempts: int, base_delay: float) -> str:
    """Resolve the listing row's download link, capturing the redirect as evidence.

    The servlet answers with a redirect to the product zip on the same FEMA
    host; the redirect receipt records exactly where FEMA's own link pointed
    at retrieval time. Redirects off the producer's host fail closed.
    """
    url = _servlet_url(product)
    name = f"{product['item_id']}.redirect"
    for attempt in range(1, attempts + 1):
        observed = datetime.now(timezone.utc).isoformat()
        try:
            with client.stream("GET", url) as response:
                status = response.status_code
                location = response.headers.get("location") if status in (301, 302, 303, 307, 308) else None
                receipt = {"method": "GET", "url": url, "observed_at": observed,
                           "status": status, "location": location}
                (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
                if location is None:
                    raise ValueError("source_download_redirect_missing")
                parsed = urlsplit(location)
                if parsed.scheme != "https" or (parsed.hostname or "") not in ALLOWED_DOWNLOAD_HOSTS:
                    raise ValueError("source_download_redirect_host_unrecognized")
                return location
        except httpx.HTTPError:
            if attempt >= attempts:
                raise ValueError(f"incomplete_source_capture:{name}") from None
        time.sleep(base_delay * (3 ** (attempt - 1)))
    raise ValueError(f"incomplete_source_capture:{name}")


def _product_zip_receipt(capture_root: Path, name: str) -> dict[str, object] | None:
    path = capture_root / f"{name}.receipt.json"
    if not path.exists():
        return None
    try:
        receipt = json.loads(path.read_bytes())
    except (OSError, ValueError):
        return None
    return receipt if isinstance(receipt, dict) else None


def _feature_class_members(capture_root: Path, name: str) -> list[str] | None:
    zip_path = capture_root / f"{name}.zip"
    try:
        with zipfile.ZipFile(zip_path) as archive:
            members = set(archive.namelist())
    except (OSError, zipfile.BadZipFile):
        return None
    required = [f"{FEATURE_CLASS}{suffix}" for suffix in FEATURE_CLASS_SUFFIXES]
    optional = [f"{FEATURE_CLASS}{suffix}" for suffix in FEATURE_CLASS_OPTIONAL_SUFFIXES]
    present = [member for member in required if member in members]
    unexpected = [member for member in members
                  if member.startswith(FEATURE_CLASS + ".")
                  and member not in required and member not in optional]
    if (present and len(present) != len(required)) or (not present and unexpected):
        return None
    return present


def _verify_cached_product_zip(capture_root: Path, product: Mapping[str, str]) -> bool:
    name = product["item_id"]
    zip_path = capture_root / f"{name}.zip"
    receipt = _product_zip_receipt(capture_root, name)
    if receipt is None or receipt.get("complete_body") is not True or receipt.get("status") != 200:
        return False
    try:
        size = zip_path.stat().st_size
    except OSError:
        return False
    if receipt.get("bytes") != size or receipt.get("sha256") != _file_sha256(zip_path):
        return False
    return _feature_class_members(capture_root, name) is not None


def _capture_product_zip(client: httpx.Client, product: Mapping[str, str],
                         capture_root: Path, *, attempts: int, base_delay: float) -> Path:
    """Capture one product zip with a hash-pinned receipt, resumably.

    The zip body is streamed straight to disk under the identity encoding
    this request pins (its wire bytes are then identical to the body, so the
    receipt's sha256 plus the zip's own member CRCs verify it), which keeps
    this layer's ~89 GiB raw capture and staging within one disk. Failed
    attempts are preserved as ``<item>.attempt-NN.zip`` evidence with their
    own receipts and never enter the capture roster; a product the producer
    keeps failing to serve fails the capture explicitly.
    """
    name = product["item_id"]
    zip_path = capture_root / f"{name}.zip"
    if _verify_cached_product_zip(capture_root, product):
        return zip_path
    for attempt in range(1, attempts + 1):
        location = _capture_download_location(client, product, capture_root,
                                              attempts=attempts, base_delay=base_delay)
        observed = datetime.now(timezone.utc).isoformat()
        digest = hashlib.sha256()
        size = 0
        status = None
        final_url = None
        content_length = None
        partial = capture_root / f"{name}.zip.partial"
        # Touch first so a connect-phase failure still leaves bounded attempt
        # evidence (the same discipline as the shared capture helper).
        partial.touch(exist_ok=True)
        transport_failed = False
        crc_failed = False
        cap_exceeded = False
        try:
            with client.stream("GET", location, headers={"Accept-Encoding": "identity"}) as response:
                status = response.status_code
                final_url = str(response.url)
                content_length = response.headers.get("Content-Length")
                if status != 200:
                    raise ValueError(f"incomplete_source_capture:{name}")
                with partial.open("wb") as stream:
                    for block in response.iter_bytes(1 << 20):
                        size += len(block)
                        if size > PRODUCT_ZIP_MAX_BYTES:
                            raise ValueError("source_product_zip_exceeds_capture_cap")
                        digest.update(block)
                        stream.write(block)
        except ValueError as exc:
            if str(exc) == "source_product_zip_exceeds_capture_cap":
                cap_exceeded = True
            else:
                transport_failed = True
        except httpx.HTTPError:
            transport_failed = True
        complete = (not transport_failed and not cap_exceeded and status == 200
                    and size > 0 and (content_length is None or content_length == str(size)))
        if complete:
            try:
                with zipfile.ZipFile(partial) as archive:
                    if archive.testzip() is not None:
                        crc_failed = True
            except (OSError, zipfile.BadZipFile):
                crc_failed = True
        if complete and not crc_failed:
            receipt = {"method": "GET", "url": location, "final_url": final_url,
                       "observed_at": observed, "status": status, "complete_body": True,
                       "bytes": size, "sha256": digest.hexdigest(),
                       "content_encoding": "identity",
                       "content_length": int(content_length) if content_length else None}
            partial.replace(zip_path)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            if _feature_class_members(capture_root, name) is None:
                raise ValueError("source_product_zip_integrity_failed")
            return zip_path
        if cap_exceeded:
            partial.unlink(missing_ok=True)
            raise ValueError("source_product_zip_exceeds_capture_cap")
        evidence = capture_root / f"{name}.attempt-{attempt:02d}.zip"
        if partial.exists():
            partial.rename(evidence)
        attempt_receipt = {"method": "GET", "url": location, "final_url": final_url,
                           "observed_at": observed, "status": status, "complete_body": False,
                           "bytes": size, "sha256": digest.hexdigest(),
                           "error_code": "source_product_zip_crc_failed" if crc_failed
                                         else "incomplete_source_capture"}
        (capture_root / f"{name}.attempt-{attempt:02d}.receipt.json").write_bytes(
            _canonical(attempt_receipt) + b"\n")
        if attempt < attempts:
            time.sleep(base_delay * (3 ** (attempt - 1)))
    raise ValueError(f"incomplete_source_capture:{name}")


def _extract_feature_class(zip_path: Path, work_dir: Path) -> dict[str, Path] | None:
    """Extract the S_FLD_HAZ_AR sidecars for one product, or None if absent.

    A product zip without the polygon feature class contributes zero features
    (FEMA publishes county/community databases whose NFHL extract has no
    flood hazard area polygons); a zip with only some of the feature class
    members is corrupt and fails closed.
    """
    with zipfile.ZipFile(zip_path) as archive:
        members = set(archive.namelist())
        required = [f"{FEATURE_CLASS}{suffix}" for suffix in FEATURE_CLASS_SUFFIXES]
        if not all(member in members for member in required):
            if any(member.startswith(FEATURE_CLASS + ".") for member in members):
                raise ValueError("source_product_zip_integrity_failed")
            return None
        work_dir.mkdir(parents=True, exist_ok=True)
        paths: dict[str, Path] = {}
        wanted = list(required) + [f"{FEATURE_CLASS}{suffix}"
                                   for suffix in FEATURE_CLASS_OPTIONAL_SUFFIXES
                                   if f"{FEATURE_CLASS}{suffix}" in members]
        for member in wanted:
            target = work_dir / member
            with archive.open(member) as source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination)
            paths[member] = target
        return paths


def _schema_signature(meta: Mapping[str, object]) -> list[list[str]]:
    fields = meta.get("fields")
    dtypes = meta.get("dtypes")
    if fields is None or dtypes is None:
        raise ValueError("source_schema_contract_drift")
    fields, dtypes = list(fields), list(dtypes)
    if len(fields) != len(dtypes):
        raise ValueError("source_schema_contract_drift")
    signature = []
    for name, dtype in zip(fields, dtypes):
        type_name = {"object": "string", "float64": "double",
                     "int64": "int64", "bool": "boolean"}.get(str(dtype))
        if type_name is None:
            raise ValueError("source_field_type_unsupported")
        signature.append([str(name), type_name])
    return signature


def _feature_schema(signature: list[list[str]]) -> list[pa.Field]:
    fields = [pa.field(name, _ARROW_TYPES[type_name]) for name, type_name in signature]
    fields.append(pa.field("SHAPE", pa.binary()))
    return fields


def _validate_feature_geometry(wkb: bytes | None, bounds: list,
                               counters: dict[str, int]) -> bytes | None:
    """Validate one feature's GDAL WKB geometry and return the candidate bytes.

    The raw WKB is returned for the candidate unless the source geometry is
    null or carries unusable NaN ordinates (which, like the sibling REST
    captures' string-'NaN' rows, are retained in the record/geometry digests
    as verbatim evidence but yield null candidate geometry). Bounds and the
    null/NaN counters accumulate in place.
    """
    if wkb is None:
        counters["null_geometry"] += 1
        return None
    try:
        geometry = shapely.wkb.loads(wkb)
    except Exception as exc:  # noqa: BLE001 - producer data driven
        raise ValueError("source_geometry_unreadable") from exc
    if geometry.geom_type not in ("Polygon", "MultiPolygon"):
        raise ValueError("source_geometry_type_drift")
    if bool(shapely.has_z(geometry)):
        raise ValueError("source_z_or_m_geometry_unsupported")
    coordinates = np.asarray(shapely.get_coordinates(geometry), dtype="float64")
    if np.isnan(coordinates).any():
        counters["nan_geometry"] += 1
        return None
    if (coordinates[:, 0].min() < -180.0 or coordinates[:, 0].max() > 180.0
            or coordinates[:, 1].min() < -90.0 or coordinates[:, 1].max() > 90.0):
        raise ValueError("source_coordinate_range_drift")
    x_min = float(coordinates[:, 0].min())
    y_min = float(coordinates[:, 1].min())
    x_max = float(coordinates[:, 0].max())
    y_max = float(coordinates[:, 1].max())
    if bounds[0] is None or x_min < bounds[0]:
        bounds[0] = x_min
    if bounds[1] is None or y_min < bounds[1]:
        bounds[1] = y_min
    if bounds[2] is None or x_max > bounds[2]:
        bounds[2] = x_max
    if bounds[3] is None or y_max > bounds[3]:
        bounds[3] = y_max
    return wkb


def _stream_validate(products: list[dict[str, str]], capture_root: Path,
                     parquet_path: Path) -> dict[str, object]:
    """Stream every product's feature class into the population contract.

    Products are consumed in canonical Item ID order; each zip is re-verified
    against its receipt, its feature class is extracted and read, and every
    feature folds into the record/geometry digests, the ID roster, the
    observed bounds and the null/NaN counters while the GeoParquet candidate
    is written incrementally. Feature-class schemas must be identical across
    every product (verified identical across 2003-2025 vintages) or the
    capture fails closed.
    """
    capture_root = Path(capture_root)
    record_digest = hashlib.sha256()
    record_digest.update(b"[")
    geometry_digest = hashlib.sha256()
    geometry_digest.update(b"[")
    ids: list[list[str | None]] = []
    counters = {"null_geometry": 0, "nan_geometry": 0, "features": 0,
                "products": 0, "products_without_polygon_feature_class": 0}
    bounds: list = [None, None, None, None]
    running_signature: list[list[str]] | None = None
    first_feature = True
    writer = None
    arrow_schema = None
    work_root = capture_root / "_work"
    try:
        for product in products:
            name = product["item_id"]
            zip_path = capture_root / f"{name}.zip"
            if not _verify_cached_product_zip(capture_root, product):
                raise ValueError("source_raw_evidence_missing_for_staging:" + name)
            extracted = _extract_feature_class(zip_path, work_root / name)
            if extracted is None:
                counters["products_without_polygon_feature_class"] += 1
                continue
            counters["products"] += 1
            shp_path = extracted[f"{FEATURE_CLASS}.shp"]
            try:
                meta = pyogrio.read_info(str(shp_path))
                signature = _schema_signature(meta)
                if meta.get("crs") != f"EPSG:{SOURCE_CRS_WKID}":
                    raise ValueError("source_crs_drift")
                if meta.get("geometry_type") not in ("Polygon", "MultiPolygon"):
                    raise ValueError("source_geometry_type_drift")
                if running_signature is None:
                    running_signature = signature
                    arrow_schema = pa.schema(_feature_schema(signature))
                    geo = {"version": "1.1.0", "primary_column": "SHAPE",
                           "columns": {"SHAPE": {"encoding": "WKB",
                                                 "geometry_types": ["Polygon", "MultiPolygon"],
                                                 "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict(),
                                                 "edges": "planar"}}}
                    writer = pq.ParquetWriter(parquet_path,
                                              arrow_schema.with_metadata({b"geo": _canonical(geo)}),
                                              compression="zstd")
                elif signature != running_signature:
                    raise ValueError("source_schema_contract_drift")
                _meta, table = pyogrio.read_arrow(str(shp_path))
            except ValueError:
                raise
            except Exception as exc:  # noqa: BLE001 - producer data driven
                raise ValueError(f"source_feature_class_unreadable:{name}") from exc
            field_names = [field[0] for field in running_signature]
            try:
                for batch in table.to_batches(max_chunksize=4096):
                    columns = {field: batch.column(field).to_pylist() for field in field_names}
                    wkbs = batch.column("wkb_geometry").to_pylist()
                    shapes: list[bytes | None] = []
                    for position in range(batch.num_rows):
                        attributes = {field: columns[field][position] for field in field_names}
                        try:
                            record_canonical = _canonical(attributes)
                        except (TypeError, ValueError) as exc:
                            raise ValueError("source_record_value_drift") from exc
                        if not first_feature:
                            record_digest.update(b",")
                            geometry_digest.update(b",")
                        record_digest.update(record_canonical)
                        first_feature = False
                        ids.append([product["item_id"], attributes.get("DFIRM_ID"),
                                    attributes.get("FLD_AR_ID")])
                        wkb = wkbs[position]
                        geometry_digest.update(len(wkb).to_bytes(8, "big") if wkb is not None
                                               else (0).to_bytes(8, "big"))
                        if wkb is not None:
                            geometry_digest.update(wkb)
                        shapes.append(_validate_feature_geometry(wkb, bounds, counters))
                        counters["features"] += 1
                    parquet_columns = {field: pa.array(columns[field], type=arrow_schema.field(field).type)
                                       for field in field_names}
                    parquet_columns["SHAPE"] = pa.array(shapes, type=pa.binary())
                    writer.write_table(pa.Table.from_pydict(parquet_columns, schema=arrow_schema))
            finally:
                shutil.rmtree(work_root / name, ignore_errors=True)
    finally:
        if writer is not None:
            writer.close()
        shutil.rmtree(work_root, ignore_errors=True)
    record_digest.update(b"]")
    geometry_digest.update(b"]")
    return {"record_sha256": record_digest.hexdigest(),
            "geometry_sha256": geometry_digest.hexdigest(),
            "schema_sha256": _sha(_canonical(running_signature)),
            "schema_signature": running_signature,
            "id_sha256": _sha(_canonical(sorted(ids))),
            "id_count": len(ids),
            "bounds": bounds,
            "counters": counters}


def _resolve_capture_root(attempt_root: Path, version: str) -> Path:
    """Reuse the one resumable attempt for this version, or start a fresh one.

    A prior incomplete attempt (``capture_manifest.json`` present and not
    marked sealed) is reused so its accepted product zips carry forward;
    multiple incomplete attempts are ambiguous and fail closed rather than
    guessing which evidence to trust. A prior sealed attempt is never reused
    (its candidate must be consumed through the construction-state cache).
    """
    prefix = f"{version}-"
    candidates = []
    for child in sorted(child for child in attempt_root.iterdir()
                        if child.is_dir() and child.name.startswith(prefix)):
        manifest_path = child / "capture_manifest.json"
        manifest = {}
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_bytes())
            except ValueError:
                manifest = {}
        if manifest.get("complete") is True and manifest.get("staged") is True:
            continue
        candidates.append(child)
    if len(candidates) > 1:
        raise ValueError("ambiguous_resume_capture_attempts")
    if candidates:
        return candidates[0]
    return attempt_root / f"{version}-{uuid4().hex}"


def _manifest_body(manifest: Mapping[str, object]) -> bytes:
    return _canonical(manifest) + b"\n"


def _upload_immutable_file(storage: StagingStorageResource, key: str, source: Path,
                           descriptor: Mapping[str, object], *, consume_source: bool = False) -> None:
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        if not source.exists():
            raise ValueError("source_raw_evidence_missing_for_staging:" + source.name)
        storage.upload_local_file(source, key)
        snapshot = storage.object_snapshot(key)
    if snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"]:
        raise ValueError("candidate_output_physical_readback_failed:" + key)
    if consume_source and storage.use_local:
        # The staged object is now the verified durable copy of this raw
        # evidence (the local backend stages by copying), and the raw
        # descriptors come from the receipts, so a staging run interrupted
        # after some uploads resumes from the already-staged objects without
        # re-downloading their products; releasing each capture-directory
        # source as it is staged keeps this layer's ~89 GiB raw staging
        # within one disk.
        source.unlink()


def _reusable_cached_candidate(storage: StagingStorageResource) -> dict[str, object] | None:
    """Return the sealed candidate for the current code identity, if still valid.

    Reached only after the live listing's parsed product roster matched the
    pinned roster (the caller enforces that gate), so a repeated ingest of
    unchanged source content reuses the already-staged candidate — re-verifying
    every staged object and original listing receipt — instead of re-acquiring
    this layer's ~89 GiB of product zips. A roster drift or a changed
    processing identity never reaches this path; multiple identity-matching
    cached candidates are ambiguous and fail closed rather than guessing.
    """
    if EXPECTED_COUNT is None or EXPECTED_PRODUCT_ROSTER_SHA256 is None:
        return None
    identity = _code_identity()
    fingerprint = _sha(_canonical(identity))
    state_prefix = f"_construction-state/{DATASET_SLUG}/"
    matching: list[str] = []
    for key in storage.list_prefix(state_prefix):
        if not re.fullmatch(r"v[0-9a-f]{32}\.json", key.rsplit("/", 1)[-1]):
            continue
        try:
            bundle = json.loads(storage.read_key(key))
        except (OSError, RuntimeError, ValueError):
            continue
        if isinstance(bundle, dict) and bundle.get("processing_fingerprint") == fingerprint:
            matching.append(key)
    if not matching:
        return None
    if len(matching) > 1:
        raise ValueError("ambiguous_cached_candidates")
    state_key = matching[0]
    bundle = json.loads(storage.read_key(state_key))
    payload = bundle.get("payload")
    version = bundle.get("version")
    if not isinstance(payload, dict) or not isinstance(version, str):
        raise ValueError("cached_first_candidate_invalid")
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    objects = payload.get("objects", {})
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint, identity,
                            payload.get("source_record_sha256"), payload.get("source_schema_sha256"), objects,
                            payload.get("capture_receipts", {}))
    audit_object = objects.get("metadata/source_audit.json")
    if not isinstance(audit_object, dict):
        raise ValueError("cached_first_candidate_invalid")
    audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit.get("source_feature_count"),
            "source_null_geometry_count": audit.get("null_geometry_count"),
            "source_nan_string_geometry_count": audit.get("nan_string_geometry_count"),
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}


def _capture_input_provenance(capture_root: Path, products: list[dict[str, str]]) -> list[dict[str, object]]:
    """Provenance for both listing brackets and every captured product zip."""
    inputs: list[dict[str, object]] = []
    for bracket in ("listing_before", "listing_after"):
        receipt = _product_zip_receipt(capture_root, bracket)
        if receipt is None or receipt.get("complete_body") is not True:
            raise ValueError("source_raw_evidence_missing_for_staging:" + bracket)
        inputs.append({
            "input_id": f"nfhl_db_{bracket}", "producer": PUBLISHER,
            "host": urlsplit(receipt["url"]).hostname, "role": "producer_publication",
            "canonical_url": LISTING_URL, "retrieval_url": receipt["url"],
            "observed_final_url": receipt.get("final_url"),
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "release_date": None,
            "release_date_unknown_reason": "FEMA's NFHL Database search page publishes no "
                                           "listing release date; product update dates are "
                                           "recorded per product below.",
            "license": "Not stated by the captured endpoint; rights review pending.",
            "attribution": "FEMA National Flood Hazard Layer; formal attribution terms "
                           "require maintainer review."})
    for product in products:
        receipt = _product_zip_receipt(capture_root, product["item_id"])
        if receipt is None or receipt.get("complete_body") is not True:
            raise ValueError("source_raw_evidence_missing_for_staging:" + product["item_id"])
        inputs.append({
            "input_id": product["item_id"], "producer": PUBLISHER,
            "host": urlsplit(receipt["url"]).hostname, "role": "producer_publication",
            "canonical_url": LISTING_URL, "retrieval_url": _servlet_url(product),
            "observed_final_url": receipt.get("final_url"),
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "product_state": product["state"], "product_county": product["county"],
            "product_file_name": product["file_name"],
            "product_size_label": product["size_label"],
            "product_update_date": _update_date_iso(product),
            "product_listing_update_date": product["update_date_java"],
            "release_date": _update_date_iso(product),
            "license": "Not stated by the captured endpoints; rights review pending.",
            "attribution": "FEMA National Flood Hazard Layer; formal attribution terms "
                           "require maintainer review."})
    return inputs


def capture_nfhl_flood_hazard_zones(storage: StagingStorageResource, *, version: str,
                                    capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and (with pins set) stage the NFHL Flood Hazard Zones candidate.

    With population pins unset, the resumable capture completes in live mode:
    the computed population contract is written to ``population_pins.json`` in
    the attempt directory and the run fails closed without staging anything.
    """
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else Path("/tmp/hifld-native-reconstruction/194/captures")
    attempt_root.mkdir(parents=True, exist_ok=True)
    capture_root = _resolve_capture_root(attempt_root, version).absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    try:
        with httpx.Client(follow_redirects=False, timeout=300,
                          headers={"User-Agent": "hifld-next-datasets/nfhl-flood-hazard-zones-draft"}) as client:
            listing_before_body = _capture_listing(client, capture_root, "listing_before")
            products = _parse_listing(listing_before_body)
            if EXPECTED_PRODUCT_ROSTER_SHA256 is not None:
                if product_roster_sha256(products) != EXPECTED_PRODUCT_ROSTER_SHA256 \
                        or (EXPECTED_PRODUCT_COUNT is not None and len(products) != EXPECTED_PRODUCT_COUNT):
                    raise ValueError("source_product_roster_drift")
            (capture_root / "products.json").write_bytes(_manifest_body({
                "product_count": len(products), "products": products}))
            reused = _reusable_cached_candidate(storage)
            if reused is not None:
                return reused
            for product in products:
                if shutil.disk_usage(capture_root).free < MIN_FREE_BYTES:
                    raise ValueError("disk_floor_before_acquisition")
                _capture_product_zip(client, product, capture_root,
                                     attempts=ZIP_ATTEMPTS, base_delay=ZIP_BASE_DELAY_SECONDS)
            listing_after_body = _capture_listing(client, capture_root, "listing_after")
        if _parse_listing(listing_after_body) != products:
            raise ValueError("source_listing_changed_during_capture")
        parquet_path = capture_root / f"{DATASET_SLUG}.parquet"
        computed = _stream_validate(products, capture_root, parquet_path)
    except ValueError as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try:
                receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError):
                receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
        (capture_root / "capture_manifest.json").write_bytes(_manifest_body(
            {"captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
             "staged": False, "error_code": type(exc).__name__,
             "error_text": str(exc), "resumable": True, "receipt_names": sorted(receipts)}))
        raise
    counters = computed["counters"]
    bounds = computed["bounds"]
    computed_bounds = None
    if bounds[0] is not None:
        computed_bounds = {"xmin": bounds[0], "ymin": bounds[1], "xmax": bounds[2], "ymax": bounds[3]}
    audit = {"source_feature_count": counters["features"],
             "source_product_count": len(products),
             "source_products_with_feature_class": counters["products"],
             "source_products_without_polygon_feature_class":
                 counters["products_without_polygon_feature_class"],
             "source_product_roster_sha256": product_roster_sha256(products),
             "source_id_sha256": computed["id_sha256"],
             "source_record_sha256": computed["record_sha256"],
             "source_geometry_sha256": computed["geometry_sha256"],
             "source_schema_sha256": computed["schema_sha256"],
             "source_crs_epsg": SOURCE_CRS_WKID,
             "geometry_type": "Polygon", "null_geometry_count": counters["null_geometry"],
             "nan_string_geometry_count": counters["nan_geometry"],
             "observed_bounds": computed_bounds,
             "unique_source_id_count": computed["id_count"],
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "FEMA updates the NFHL continuously and publishes no single release date; "
                 "each product's own update date is recorded in the source manifest inputs."}
    if not audit["source_feature_count"] or audit["source_feature_count"] != computed["id_count"]:
        raise ValueError("source_feature_count_roster_mismatch")
    if EXPECTED_COUNT is None:
        (capture_root / "population_pins.json").write_bytes(_manifest_body({
            "established_by": "live-mode complete capture (pins unset at capture time)",
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "source": "FEMA NFHL county/community databases (bulk distribution)",
            "expected_product_count": len(products),
            "expected_product_roster_sha256": audit["source_product_roster_sha256"],
            "expected_count": counters["features"],
            "expected_id_sha256": audit["source_id_sha256"],
            "expected_schema_sha256": audit["source_schema_sha256"],
            "expected_record_sha256": audit["source_record_sha256"],
            "expected_geometry_sha256": audit["source_geometry_sha256"],
            "expected_observed_bounds": computed_bounds,
            "expected_null_geometry_count": counters["null_geometry"],
            "expected_nan_geometry_count": counters["nan_geometry"]}))
        (capture_root / "capture_manifest.json").write_bytes(_manifest_body(
            {"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True, "staged": False,
             "reason_code": "source_population_pins_not_established",
             "source_feature_count": audit["source_feature_count"]}))
        raise _PinsPendingError("source_population_pins_not_established")
    if counters["features"] != EXPECTED_COUNT:
        raise ValueError("source_feature_count_drift")
    if audit["source_product_roster_sha256"] != EXPECTED_PRODUCT_ROSTER_SHA256 \
            or len(products) != EXPECTED_PRODUCT_COUNT:
        raise ValueError("source_product_roster_drift")
    if computed["id_sha256"] != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if computed["geometry_sha256"] != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    if computed["record_sha256"] != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if computed["schema_sha256"] != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_contract_drift")
    if (counters["null_geometry"] != EXPECTED_NULL_GEOMETRY_COUNT
            or counters["nan_geometry"] != EXPECTED_NAN_GEOMETRY_COUNT):
        raise ValueError("source_geometry_null_or_nan_count_drift")
    if EXPECTED_OBSERVED_BOUNDS is not None and computed_bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    audit = dict(audit)
    identity = _code_identity()
    parquet_rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_rel: {"bytes": parquet_path.stat().st_size,
                                    "sha256": _file_sha256(parquet_path)}}
    manifest = {
        "schema_version": 1, "approved": False, "title": "Flood Hazard Zones (FEMA NFHL)",
        "description": "FEMA National Flood Hazard Layer county/community NFHL Databases, captured "
                       "completely from FEMA's own bulk distribution: every product FEMA's NFHL "
                       "Database search page lists, each product's S_FLD_HAZ_AR 'Flood Hazard Area' "
                       "polygon feature class extracted and retained. Complete source attributes and "
                       "NAD83 polygon geometry are represented in the candidate. The NFHL is "
                       "continuously updated and publishes no per-snapshot release date; freshness, "
                       "rights, public schema/ID contract, and release acceptance remain unresolved "
                       "and unapproved.",
        "publisher": PUBLISHER, "agency": "Federal Emergency Management Agency",
        "source_url": LISTING_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": _capture_input_provenance(capture_root, products),
        "transformations": [{"operation": "native_nfhl_database_to_geoparquet_candidate",
                             "parameters": {"feature_class": FEATURE_CLASS,
                                            "source_fields": "all S_FLD_HAZ_AR producer fields "
                                                             "retained in producer order and "
                                                             "physical types (unset numeric values "
                                                             "are the producer's -9999 sentinel, "
                                                             "retained verbatim); SHAPE is the "
                                                             "original shapefile geometry as WKB",
                                            "source_ids": "Preserve producer DFIRM_ID, FLD_AR_ID "
                                                          "and all source attribute values "
                                                          "unchanged; no substitute IDs, casts, "
                                                          "trimming, or joins",
                                            "geometry": "Source NAD83 (EPSG:4269) shapefile polygon "
                                                        "geometry retained as WKB without projection: "
                                                        "verbatim vertices, the shapefile's explicit "
                                                        "part structure (first ring of each part "
                                                        "exterior, following rings holes), and the "
                                                        "shapefile's implicit ring closure made "
                                                        "explicit as WKB requires; null source "
                                                        "geometry retained as null; features with "
                                                        "NaN ordinates retained as records with null "
                                                        "candidate geometry, raw evidence intact",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending freshness, schema/ID "
                                                                "contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FEMA publishes the National Flood Hazard Layer (NFHL) as its own geospatial "
                   "database (fema.gov/flood-maps/national-flood-hazard-layer) and distributes "
                   "state- and county/community-level NFHL Database extracts through the FEMA Map "
                   "Service Center: FEMA's NFHL Database search page "
                   "(https://hazards.fema.gov/femaportal/NFHL/searchResult.action) lists every "
                   "county/community product with its update date and download link, and each link "
                   "serves the product zip from FEMA's own host. This candidate is the S_FLD_HAZ_AR "
                   "'Flood Hazard Area' polygon feature class of every listed product — the "
                   "producer's bulk publication of the same NFHL flood hazard zone data the MSC GIS "
                   "Web Services page documents behind its public REST service (layer 28 'Flood "
                   "Hazard Zones'). The REST layer was not used as the capture surface because its "
                   "query endpoint permanently refuses to serve one roster feature's "
                   "full-resolution geometry (roster offset 1328, OBJECTID 1886, complete HTTP 200 "
                   "Esri 500 error documents on every faithful request shape, 2026-11-12) while the "
                   "same feature is present complete in this bulk distribution (Winston County NFHL "
                   "Database 01133C_20110916.zip). The HIFLD Open inventory lists this dataset as "
                   "nfhl/flood-hazard-zones-1 with DataLumos project 239971; that ICPSR archive "
                   "copy and the 2024-10-31 archive modification date are identification and "
                   "comparison context only, never pipeline inputs.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls, and geometry retained in the "
                      "producer CRS; all release and compatibility approvals remain pending.",
            "objects": [{"path": parquet_rel, **source_objects[parquet_rel]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit.update({"source_fields": [list(field) for field in computed["schema_signature"]],
                  "official_dataset_name": "Flood Hazard Zones", "source_url": LISTING_URL,
                  "release_date_unknown": True, "freshness_accepted": False, "schema_contract_accepted": False,
                  "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
                  "release_accepted": False})
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    metadata_objects = {
        "metadata/source_manifest.json": _manifest_body(manifest),
        "metadata/source_audit.json": _manifest_body(audit),
        "metadata/processing_identity.json": _manifest_body(identity),
        "metadata/candidate_ready.json": _manifest_body(
            {"approved": False, "version": version, "objects": source_objects,
             "source_record_sha256": audit["source_record_sha256"], "freshness_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
             "mapping_approved": False, "release_accepted": False}),
    }
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("cached_first_candidate_invalid")
        objects = payload.get("objects", {})
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"], objects,
                                payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": audit["source_feature_count"],
                "source_null_geometry_count": audit["null_geometry_count"],
                "source_nan_string_geometry_count": audit["nan_string_geometry_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}
    for name, body in metadata_objects.items():
        write_immutable(storage, f"{prefix}/{name}", body)
        snapshot = storage.object_snapshot(f"{prefix}/{name}")
        if snapshot is None or snapshot.size != len(body) or storage.sha256_key(f"{prefix}/{name}") != _sha(body):
            raise ValueError("candidate_output_physical_readback_failed:" + name)
    _upload_immutable_file(storage, f"{prefix}/{parquet_rel}", parquet_path, source_objects[parquet_rel])
    # Only the two listing brackets carry body+wire capture receipts in the
    # construction payload (the shared candidate verifier reads those two
    # evidence shapes); every product zip's integrity is enforced through its
    # raw object descriptor readback in the same verifier.
    capture_receipts = {name: _product_zip_receipt(capture_root, name)
                        for name in ("listing_before", "listing_after")}
    if any(receipt is None or receipt.get("complete_body") is not True for receipt in capture_receipts.values()):
        raise ValueError("source_raw_evidence_missing_for_staging:listing")
    raw_descriptors = {}
    staged_names = [name for name in ("listing_before", "listing_after")
                    + tuple(product["item_id"] for product in products)]
    for name in staged_names:
        receipt = _product_zip_receipt(capture_root, name)
        if receipt is None or receipt.get("complete_body") is not True:
            raise ValueError("source_raw_evidence_missing_for_staging:" + name)
        is_zip = name not in ("listing_before", "listing_after")
        body_suffix = ".zip" if is_zip else ".body"
        raw_descriptors[f"raw/{name}{body_suffix}"] = {"bytes": receipt["bytes"],
                                                       "sha256": receipt["sha256"]}
        if not is_zip:
            raw_descriptors[f"raw/{name}.wire"] = {"bytes": receipt["wire_bytes"],
                                                   "sha256": receipt["wire_sha256"]}
        raw_descriptors[f"raw/{name}.receipt.json"] = {"bytes": len(_manifest_body(receipt)),
                                                       "sha256": _sha(_manifest_body(receipt))}
    for name in staged_names:
        is_zip = name not in ("listing_before", "listing_after")
        receipt = _product_zip_receipt(capture_root, name)
        if receipt is None or receipt.get("complete_body") is not True:
            raise ValueError("source_raw_evidence_missing_for_staging:" + name)
        if is_zip:
            _upload_immutable_file(storage, f"raw/{name}.zip", capture_root / f"{name}.zip",
                                   {"bytes": receipt["bytes"], "sha256": receipt["sha256"]},
                                   consume_source=True)
        else:
            _upload_immutable_file(storage, f"raw/{name}.body", capture_root / f"{name}.body",
                                   {"bytes": receipt["bytes"], "sha256": receipt["sha256"]},
                                   consume_source=True)
            _upload_immutable_file(storage, f"raw/{name}.wire", capture_root / f"{name}.wire",
                                   {"bytes": receipt["wire_bytes"], "sha256": receipt["wire_sha256"]},
                                   consume_source=True)
        write_immutable(storage, f"raw/{name}.receipt.json", _manifest_body(receipt))
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in metadata_objects.items()}
    descriptors[parquet_rel] = source_objects[parquet_rel]
    descriptors.update(raw_descriptors)
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": capture_receipts,
               "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    (capture_root / "capture_manifest.json").write_bytes(_manifest_body(
        {"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True, "staged": True,
         "candidate_version": version, "candidate_prefix": prefix}))
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["source_feature_count"],
            "source_null_geometry_count": audit["null_geometry_count"],
            "source_nan_string_geometry_count": audit["nan_string_geometry_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity
