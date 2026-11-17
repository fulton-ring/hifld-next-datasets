"""Shared capture of FEMA's bulk NFHL county/community database distribution.

FEMA publishes the National Flood Hazard Layer (NFHL) as its own database and
distributes county/community NFHL Database extracts through the Map Service
Center. FEMA's NFHL Database search page
(``https://hazards.fema.gov/femaportal/NFHL/searchResult.action``) is one
complete HTML listing of every product with its update date, size label and
download link; each link redirects to the product zip on FEMA's own host.

This module captures that distribution once for the whole NFHL family. The
shared capture directory holds every product zip with a hash-pinned receipt
and a roster of the producer's update dates and sizes. A refresh re-downloads
only products whose listing identity changed, and every NFHL layer reads its
own feature class from the same copy instead of acquiring the distribution
again.

The state FileGDB route was measured and rejected as the family source: it has
no listing endpoint, its directory listing is forbidden, and the MSC search
exposes a state product for only part of the country. The county/community
route is the complete maintained distribution and its listing carries the
per-product update date and size that resumable refresh needs.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import tempfile
from contextlib import ExitStack, contextmanager
import re
import shutil
import time
import zipfile
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping
from urllib.parse import quote, unquote, urlencode, urlsplit

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
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity, seal_ready_candidate,
    verify_cached_candidate)
from dagster_hifld.sources.nfhl.candidate import check_local_staging, stage_object
from dagster_hifld.sources.nfhl.capture import SERVICE_URL
from dagster_hifld.sources.nfhl.layers import NfhlLayer
from dagster_hifld.validation import sha256_file

LISTING_URL = "https://hazards.fema.gov/femaportal/NFHL/searchResult.action"
SERVLET_URL = "https://hazards.fema.gov/femaportal/NFHL/Download/ProductsDownLoadServlet"
ALLOWED_DOWNLOAD_HOSTS = {"hazards.fema.gov"}
LISTING_MARKER = "National Flood Hazard Layer (NFHL) Database Search Results"
LISTING_MAX_BYTES = 64 * 1024 * 1024
PRODUCT_ZIP_MAX_BYTES = 2 * 1024 * 1024 * 1024
LISTING_ATTEMPTS = 4
LISTING_BASE_DELAY_SECONDS = 10.0
ZIP_ATTEMPTS = 4
ZIP_BASE_DELAY_SECONDS = 10.0
SOURCE_CRS_WKID = 4269
# The county/community distribution is ~89 GiB of zips by FEMA's own size
# labels; the shared capture keeps every product for the whole family, so a
# node that cannot hold the roster plus headroom must fail before acquiring.
BULK_CAPTURE_FLOOR_BYTES = 110 * 1024 * 1024 * 1024
FEATURE_CLASS_SUFFIXES = (".shp", ".shx", ".dbf", ".prj")
FEATURE_CLASS_OPTIONAL_SUFFIXES = (".cpg",)

_ITEM_ID = re.compile(r"^[0-9A-Z]{5,6}-NFHL$")
_CENTER_CELL = re.compile(r'^\s*<center class="txt12">(.*?)</center>\s*$', re.S)
_HIDDEN_SPAN = re.compile(r'<span style="display:none;">([^<>]*)</span>')
_HREF_CELL = re.compile(r'<a href="Download/ProductsDownLoadServlet\?([^"]+)"')
_JAVA_MONTHS = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
_SIZE_CELL = re.compile(r"^([0-9.]+)\s*(KB|MB|GB)$")
_JAVA_DATE = re.compile(r"^([A-Z][a-z]{2}) ([A-Z][a-z]{2}) +(\d{1,2}) \d{2}:\d{2}:\d{2} ([A-Z]{2,5}) (\d{4})$")
_ARROW_TYPES = {"string": pa.string(), "double": pa.float64(),
                "int64": pa.int64(), "boolean": pa.bool_()}
_GEOMETRY_TYPES = {"point": ("Point", "MultiPoint"),
                   "line": ("LineString", "MultiLineString"),
                   "polygon": ("Polygon", "MultiPolygon")}


@dataclass(frozen=True)
class BulkProduct:
    item_id: str
    dfirm_id: str
    state: str
    county: str
    file_name: str
    update_date_java: str
    file_date: str
    size_label: str

    def as_dict(self) -> dict[str, str]:
        return {"item_id": self.item_id, "dfirm_id": self.dfirm_id, "state": self.state,
                "county": self.county, "file_name": self.file_name,
                "update_date_java": self.update_date_java, "file_date": self.file_date,
                "size_label": self.size_label}


@dataclass(frozen=True)
class BulkSettings:
    listing_attempts: int = LISTING_ATTEMPTS
    listing_delay: float = LISTING_BASE_DELAY_SECONDS
    zip_attempts: int = ZIP_ATTEMPTS
    zip_delay: float = ZIP_BASE_DELAY_SECONDS
    timeout: float = 300.0
    transport: httpx.BaseTransport | None = None

    def client(self) -> httpx.Client:
        return httpx.Client(follow_redirects=False, timeout=self.timeout, transport=self.transport,
                            headers={"User-Agent": "hifld-next-datasets/nfhl-bulk"})


@dataclass
class StreamCounters:
    features: int = 0
    products_with_feature_class: int = 0
    products_without_feature_class: int = 0
    null_geometry: int = 0
    nan_geometry: int = 0


@dataclass
class StreamResult:
    feature_count: int
    record_sha256: str
    geometry_sha256: str
    schema_sha256: str
    schema_signature: list[list[str]]
    id_sha256: str
    id_count: int
    bounds: list[float | None]
    counters: StreamCounters = field(default_factory=StreamCounters)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _plain_text(value: object) -> str:
    return " ".join(str(value).replace("\xa0", " ").split())


def _servlet_url(product: Mapping[str, str]) -> str:
    return SERVLET_URL + "?" + urlencode({
        "DFIRMID": product["dfirm_id"], "state": product["state"],
        "county": product["county"], "fileName": product["file_name"]}, quote_via=quote)


def parse_listing(body: bytes) -> tuple[BulkProduct, ...]:
    """Parse FEMA's NFHL Database listing into its canonical product roster.

    Every product row must carry a complete, self-consistent six-cell record
    (Item ID, county, state, update date, size, download link): an
    ``<id>-NFHL`` Item ID whose identifier matches the row's download link, a
    java-format update date, a zip file name whose publication date is
    recorded, and a FEMA size label. Any structural surprise fails closed; the
    roster is returned in canonical Item ID order.
    """
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("source_listing_structure_drift") from exc
    if LISTING_MARKER not in text:
        raise ValueError("official_fema_nfhl_database_listing_drift")
    products: list[BulkProduct] = []
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
        file_date = file_name.split("_", 1)[1].split(".", 1)[0]
        if item_id in seen_items or file_name in seen_files:
            raise ValueError("source_product_roster_drift")
        seen_items.add(item_id)
        seen_files.add(file_name)
        products.append(BulkProduct(item_id=item_id, dfirm_id=dfirm_id, state=state, county=county,
                                    file_name=file_name, update_date_java=java_date,
                                    file_date=file_date, size_label=size_label))
    if not products:
        raise ValueError("source_listing_structure_drift")
    return tuple(sorted(products, key=lambda product: product.item_id))


def product_roster_sha256(products: Iterable[BulkProduct]) -> str:
    return _sha(_canonical([product.as_dict() for product in products]))


def update_date_iso(product: Mapping[str, str]) -> str:
    if not re.fullmatch(r"[0-9]{8}", product["file_date"]):
        raise ValueError("source_listing_structure_drift")
    return f"{product['file_date'][0:4]}-{product['file_date'][4:6]}-{product['file_date'][6:8]}"


def capture_listing(client: httpx.Client, capture_root: Path, name: str, *,
                    attempts: int, base_delay: float) -> bytes:
    for attempt in range(1, attempts + 1):
        try:
            body, _receipt = capture_get(client, LISTING_URL, capture_root, name,
                                         max_bytes=LISTING_MAX_BYTES)
            return body
        except ValueError as exc:
            if attempt >= attempts or not str(exc).startswith("incomplete_source_capture:"):
                raise
        time.sleep(base_delay * (3 ** (attempt - 1)))
    raise ValueError(f"incomplete_source_capture:{name}")


def _capture_download_location(client: httpx.Client, product: BulkProduct, capture_root: Path, *,
                               attempts: int, base_delay: float) -> str:
    """Resolve the listing row's download link, capturing the redirect as evidence."""
    url = _servlet_url(product.as_dict())
    name = f"{product.item_id}.redirect"
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


def product_receipt(capture_root: Path, name: str) -> dict[str, object] | None:
    path = capture_root / f"{name}.receipt.json"
    if not path.exists():
        return None
    try:
        receipt = json.loads(path.read_bytes())
    except (OSError, ValueError):
        return None
    return receipt if isinstance(receipt, dict) else None


def _archive_has_products(capture_root: Path, name: str) -> bool:
    """A product zip must be a valid archive with at least one shapefile layer."""
    try:
        with zipfile.ZipFile(capture_root / f"{name}.zip") as archive:
            return any(member.lower().endswith(".shp") for member in archive.namelist())
    except (OSError, zipfile.BadZipFile):
        return False


def verify_product_zip(capture_root: Path, product: BulkProduct) -> bool:
    name = product.item_id
    zip_path = capture_root / f"{name}.zip"
    receipt = product_receipt(capture_root, name)
    if (receipt is None or receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("item_id") != product.item_id
            or receipt.get("product_file_name") != product.file_name):
        return False
    try:
        size = zip_path.stat().st_size
    except OSError:
        return False
    if receipt.get("bytes") != size or receipt.get("sha256") != _file_sha256(zip_path):
        return False
    return _archive_has_products(capture_root, name)


def capture_product_zip(client: httpx.Client, product: BulkProduct, capture_root: Path, *,
                        attempts: int, base_delay: float) -> Path:
    """Capture one product zip with a hash-pinned receipt, resumably.

    The zip body is streamed to disk under the identity encoding this request
    pins (its wire bytes are then identical to the body, so the receipt's
    sha256 plus the zip's own member CRCs verify it). Failed attempts are
    preserved as ``<item>.attempt-NN.zip`` evidence and never enter the
    roster; a product the producer keeps failing to serve fails explicitly.
    """
    name = product.item_id
    zip_path = capture_root / f"{name}.zip"
    if verify_product_zip(capture_root, product):
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
            except (OSError, zipfile.BadZipFile, zlib.error):
                crc_failed = True
        if complete and not crc_failed:
            receipt = {"method": "GET", "url": location, "final_url": final_url,
                       "observed_at": observed, "status": status, "complete_body": True,
                       "bytes": size, "sha256": digest.hexdigest(),
                       "content_encoding": "identity",
                       "content_length": int(content_length) if content_length else None,
                       "item_id": product.item_id, "product_file_name": product.file_name,
                       "product_update_date": update_date_iso(product.as_dict())}
            partial.replace(zip_path)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            if not _archive_has_products(capture_root, name):
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


def _roster_path(capture_root: Path) -> Path:
    return capture_root / "products.json"


def _read_roster(capture_root: Path) -> tuple[BulkProduct, ...]:
    path = _roster_path(capture_root)
    if not path.exists():
        return ()
    try:
        document = json.loads(path.read_bytes())
        rows = document["products"]
        return tuple(BulkProduct(**row) for row in rows)
    except (OSError, ValueError, KeyError, TypeError):
        raise ValueError("source_product_roster_cache_invalid") from None


def refresh_products(client: httpx.Client, capture_root: Path, *,
                     settings: BulkSettings) -> tuple[tuple[BulkProduct, ...], dict[str, int]]:
    """Capture the listing and download every product whose identity changed.

    The roster and every product receipt are the shared capture state. A
    refresh reuses a product only when its Item ID, file name, update date and
    size label are unchanged; a changed product is downloaded again. Products
    that left the listing are dropped from the roster but their zips stay for
    concurrent readers; a separate maintenance job releases them.
    """
    capture_root.mkdir(parents=True, exist_ok=True)
    listing_before = capture_listing(client, capture_root, "listing_before",
                                     attempts=settings.listing_attempts,
                                     base_delay=settings.listing_delay)
    products = parse_listing(listing_before)
    previous = {product.item_id: product for product in _read_roster(capture_root)}
    current = {product.item_id: product for product in products}
    downloaded = reused = removed = 0
    for product in products:
        if previous.get(product.item_id) == product and verify_product_zip(capture_root, product):
            reused += 1
            continue
        capture_product_zip(client, product, capture_root, attempts=settings.zip_attempts,
                            base_delay=settings.zip_delay)
        downloaded += 1
    # A product that left the listing is dropped from the roster but its zip
    # stays: another layer may be reading it, and an unlink during that read
    # would break a concurrent capture. Cleanup is a separate maintenance job.
    removed = len(previous.keys() - current.keys())
    _roster_path(capture_root).write_bytes(_canonical({
        "product_count": len(products), "roster_sha256": product_roster_sha256(products),
        "products": [product.as_dict() for product in products]}) + b"\n")
    listing_after = capture_listing(client, capture_root, "listing_after",
                                    attempts=settings.listing_attempts,
                                    base_delay=settings.listing_delay)
    if parse_listing(listing_after) != products:
        raise ValueError("source_listing_changed_during_capture")
    return products, {"products": len(products), "downloaded": downloaded,
                      "reused": reused, "removed": removed}


@contextmanager
def capture_lock(capture_root: Path):
    """Exclusive lock so concurrent layer captures refresh the shared copy once."""
    capture_root.mkdir(parents=True, exist_ok=True)
    with (capture_root / ".lock").open("w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield


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


def _string_column_values(column: pa.Array, item_id: str, field: str) -> list[object]:
    try:
        values = column.to_pylist()
    except UnicodeError as exc:
        raise ValueError(f"source_dbf_text_unencodable:{item_id}:{field}") from exc
    return [None if value is None else str(value) for value in values]


def _record_canonical(attributes: Mapping[str, object]) -> bytes:
    return _canonical({key: (value.hex() if isinstance(value, bytes) else value)
                       for key, value in attributes.items()})


def _id_sort_key(entry: list) -> str:
    return _canonical([None if value is None else value.hex() if isinstance(value, bytes)
                       else value for value in entry]).decode()


def _validate_feature_geometry(wkb: bytes | None, layer: NfhlLayer,
                               counters: StreamCounters, bounds: list) -> bytes | None:
    if wkb is None:
        counters.null_geometry += 1
        return None
    try:
        geometry = shapely.wkb.loads(wkb)
    except Exception as exc:  # noqa: BLE001 - producer data driven
        raise ValueError("source_geometry_unreadable") from exc
    if geometry.geom_type not in _GEOMETRY_TYPES[layer.geometry]:
        raise ValueError("source_geometry_type_drift")
    if bool(shapely.has_z(geometry)) or bool(shapely.has_m(geometry)):
        raise ValueError("source_z_or_m_geometry_unsupported")
    coordinates = np.asarray(shapely.get_coordinates(geometry), dtype="float64")
    if np.isnan(coordinates).any():
        counters.nan_geometry += 1
        return None
    if (coordinates[:, 0].min() < -180.0 or coordinates[:, 0].max() > 180.0
            or coordinates[:, 1].min() < -90.0 or coordinates[:, 1].max() > 90.0):
        raise ValueError("source_coordinate_range_drift")
    x_min, y_min = float(coordinates[:, 0].min()), float(coordinates[:, 1].min())
    x_max, y_max = float(coordinates[:, 0].max()), float(coordinates[:, 1].max())
    if bounds[0] is None or x_min < bounds[0]:
        bounds[0] = x_min
    if bounds[1] is None or y_min < bounds[1]:
        bounds[1] = y_min
    if bounds[2] is None or x_max > bounds[2]:
        bounds[2] = x_max
    if bounds[3] is None or y_max > bounds[3]:
        bounds[3] = y_max
    return wkb


def _extract_feature_class(zip_path: Path, work_dir: Path, feature_class: str) -> dict[str, Path] | None:
    with zipfile.ZipFile(zip_path) as archive:
        members = set(archive.namelist())
        required = [f"{feature_class}{suffix}" for suffix in FEATURE_CLASS_SUFFIXES]
        if not all(member in members for member in required):
            if any(member.startswith(feature_class + ".") for member in members):
                raise ValueError("source_product_zip_integrity_failed")
            return None
        work_dir.mkdir(parents=True, exist_ok=True)
        paths: dict[str, Path] = {}
        wanted = list(required) + [f"{feature_class}{suffix}"
                                   for suffix in FEATURE_CLASS_OPTIONAL_SUFFIXES
                                   if f"{feature_class}{suffix}" in members]
        for member in wanted:
            target = work_dir / member
            with archive.open(member) as source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination)
            paths[member] = target
        return paths


def stream_feature_class(products: tuple[BulkProduct, ...], capture_root: Path, layer: NfhlLayer,
                         parquet_path: Path) -> StreamResult:
    """Stream one feature class from every product into a GeoParquet candidate."""
    if not layer.feature_class:
        raise ValueError(f"nfhl_layer_missing_feature_class:{layer.slug}")
    capture_root = Path(capture_root)
    record_digest = hashlib.sha256()
    record_digest.update(b"[")
    geometry_digest = hashlib.sha256()
    geometry_digest.update(b"[")
    ids: list[list[str | None]] = []
    counters = StreamCounters()
    bounds: list = [None, None, None, None]
    running_signature: list[list[str]] | None = None
    first_feature = True
    writer = None
    arrow_schema = None
    work_root = capture_root / "_work"
    try:
        for product in products:
            name = product.item_id
            zip_path = capture_root / f"{name}.zip"
            if not verify_product_zip(capture_root, product):
                raise ValueError("source_raw_evidence_missing_for_staging:" + name)
            extracted = _extract_feature_class(zip_path, work_root / name, layer.feature_class)
            if extracted is None:
                counters.products_without_feature_class += 1
                continue
            counters.products_with_feature_class += 1
            shp_path = extracted[f"{layer.feature_class}.shp"]
            try:
                meta = pyogrio.read_info(str(shp_path))
                signature = _schema_signature(meta)
                if meta.get("crs") != f"EPSG:{SOURCE_CRS_WKID}":
                    raise ValueError("source_crs_drift")
                if meta.get("geometry_type") not in _GEOMETRY_TYPES[layer.geometry]:
                    raise ValueError("source_geometry_type_drift")
                if running_signature is None:
                    running_signature = signature
                    arrow_schema = pa.schema(_feature_schema(signature))
                    geo = {"version": "1.1.0", "primary_column": "SHAPE",
                           "columns": {"SHAPE": {"encoding": "WKB",
                                                 "geometry_types": list(_GEOMETRY_TYPES[layer.geometry]),
                                                 "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict(),
                                                 "edges": "planar"}}}
                    writer = pq.ParquetWriter(parquet_path,
                                              arrow_schema.with_metadata({b"geo": _canonical(geo)}),
                                              compression="zstd", compression_level=12)
                elif signature != running_signature:
                    raise ValueError("source_schema_contract_drift")
                _meta, table = pyogrio.read_arrow(str(shp_path))
            except ValueError:
                raise
            except Exception as exc:  # noqa: BLE001 - producer data driven
                raise ValueError(f"source_feature_class_unreadable:{name}") from exc
            field_names = [field[0] for field in running_signature]
            missing_ids = [field for field in layer.id_fields if field not in field_names]
            if missing_ids:
                raise ValueError("source_id_fields_missing:" + ",".join(missing_ids))
            try:
                for batch in table.to_batches(max_chunksize=4096):
                    columns = {field: (_string_column_values(batch.column(field), product.item_id, field)
                                       if arrow_schema.field(field).type == pa.string()
                                       else batch.column(field).to_pylist())
                               for field in field_names}
                    wkbs = batch.column("wkb_geometry").to_pylist()
                    shapes: list[bytes | None] = []
                    for position in range(batch.num_rows):
                        attributes = {field: columns[field][position] for field in field_names}
                        try:
                            record_canonical = _record_canonical(attributes)
                        except (TypeError, ValueError) as exc:
                            raise ValueError("source_record_value_drift") from exc
                        if not first_feature:
                            record_digest.update(b",")
                            geometry_digest.update(b",")
                        record_digest.update(record_canonical)
                        first_feature = False
                        ids.append([product.item_id]
                                   + [attributes.get(field) for field in layer.id_fields])
                        wkb = wkbs[position]
                        geometry_digest.update(len(wkb).to_bytes(8, "big") if wkb is not None
                                               else (0).to_bytes(8, "big"))
                        if wkb is not None:
                            geometry_digest.update(wkb)
                        shapes.append(_validate_feature_geometry(wkb, layer, counters, bounds))
                        counters.features += 1
                    parquet_columns = {}
                    for field in field_names:
                        field_type = arrow_schema.field(field).type
                        parquet_columns[field] = pa.array(columns[field], type=field_type)
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
    if running_signature is None:
        raise ValueError("source_feature_class_absent_from_every_product")
    return StreamResult(feature_count=counters.features,
                        record_sha256=record_digest.hexdigest(),
                        geometry_sha256=geometry_digest.hexdigest(),
                        schema_sha256=_sha(_canonical(running_signature)),
                        schema_signature=running_signature,
                        id_sha256=_sha(_canonical(sorted(ids, key=_id_sort_key))),
                        id_count=len(ids), bounds=bounds, counters=counters)


def rest_feature_count(client: httpx.Client, layer: NfhlLayer) -> int | None:
    """The producer's live REST count for this layer, when it serves one."""
    url = f"{SERVICE_URL}/{layer.layer_id}/query"
    try:
        response = client.get(url, params={"where": "1=1", "returnCountOnly": "true", "f": "json"})
        response.raise_for_status()
        document = response.json()
    except (httpx.HTTPError, ValueError):
        return None
    count = document.get("count") if isinstance(document, dict) else None
    return count if type(count) is int else None


REPO_ROOT = Path(__file__).parents[4]
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
APPROVAL_FLAGS = {"source_release_accepted": False, "source_freshness_accepted": False,
                  "schema_contract_accepted": False, "id_contract_accepted": False,
                  "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
_READY_FLAGS = {"freshness_accepted": False, "schema_contract_accepted": False,
                "id_contract_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}


def _code_identity() -> dict[str, object]:
    identity = processing_identity(REPO_ROOT)
    identity["gdal"] = pyogrio.__gdal_version_string__
    return identity


def _inputs(capture_root: Path, products: tuple[BulkProduct, ...]) -> list[dict[str, object]]:
    inputs: list[dict[str, object]] = []
    for bracket in ("listing_before", "listing_after"):
        receipt = product_receipt(capture_root, bracket)
        if receipt is None or receipt.get("complete_body") is not True:
            raise ValueError("source_raw_evidence_missing_for_staging:" + bracket)
        inputs.append({
            "input_id": f"nfhl_db_{bracket}", "producer": PUBLISHER,
            "host": urlsplit(receipt["url"]).hostname, "role": "producer_publication",
            "canonical_url": LISTING_URL, "retrieval_url": receipt["url"],
            "observed_final_url": receipt.get("final_url"),
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"], "release_date": None,
            "release_date_unknown_reason": "FEMA's NFHL Database search page publishes no listing "
                                           "release date; product update dates are recorded per product.",
            "license": "Not stated by the captured endpoint; rights review pending.",
            "attribution": "FEMA National Flood Hazard Layer; formal attribution terms require "
                           "maintainer review."})
    for product in products:
        receipt = product_receipt(capture_root, product.item_id)
        if receipt is None or receipt.get("complete_body") is not True:
            raise ValueError("source_raw_evidence_missing_for_staging:" + product.item_id)
        inputs.append({
            "input_id": product.item_id, "producer": PUBLISHER,
            "host": urlsplit(receipt["url"]).hostname, "role": "producer_publication",
            "canonical_url": LISTING_URL, "retrieval_url": _servlet_url(product.as_dict()),
            "observed_final_url": receipt.get("final_url"),
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "product_state": product.state, "product_county": product.county,
            "product_file_name": product.file_name, "product_size_label": product.size_label,
            "product_update_date": update_date_iso(product.as_dict()),
            "product_listing_update_date": product.update_date_java,
            "release_date": update_date_iso(product.as_dict()),
            "license": "Not stated by the captured endpoints; rights review pending.",
            "attribution": "FEMA National Flood Hazard Layer; formal attribution terms require "
                           "maintainer review."})
    return inputs


def _manifest(layer: NfhlLayer, identity: Mapping[str, object], inputs: list[dict[str, object]],
              rel: str, parquet: Mapping[str, object]) -> dict[str, object]:
    return {
        "schema_version": 1, "approved": False, "title": layer.title,
        "description": f"FEMA National Flood Hazard Layer (NFHL) county/community NFHL Databases, "
                       f"captured from FEMA's own bulk distribution: every product FEMA's NFHL Database "
                       f"search page lists, with each product's {layer.feature_class} feature class "
                       "retained. Complete source attributes and NAD83 geometry are represented in the "
                       "candidate. The NFHL is continuously updated and publishes no per-snapshot release "
                       "date; freshness, rights, public schema/ID contract, and release acceptance remain "
                       "unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "Federal Emergency Management Agency", "source_url": LISTING_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"], "inputs": inputs,
        "transformations": [{"operation": "native_nfhl_database_to_geoparquet_candidate",
                             "parameters": {
                                 "feature_class": layer.feature_class,
                                 "source_fields": f"all {layer.feature_class} producer fields retained in "
                                                  "producer order and physical types (unset numeric values "
                                                  "are the producer's -9999 sentinel, retained verbatim); "
                                                  "SHAPE is the original shapefile geometry as WKB",
                                 "attribute_text": "Producer attribute text is published as UTF-8: DBF "
                                                   "character values are decoded as UTF-8 and retained "
                                                   "verbatim; a product whose DBF text is not valid UTF-8 "
                                                   "fails closed with source_dbf_text_unencodable naming "
                                                   "the product and field, and the raw evidence zips "
                                                   "remain the lossless source of every byte",
                                 "source_ids": f"Preserve producer {', '.join(layer.id_fields)} values "
                                               "unchanged; no substitute IDs, casts, trimming, or joins",
                                 "geometry": "Source NAD83 (EPSG:4269) shapefile geometry retained as WKB "
                                             "without projection: verbatim vertices, the shapefile's "
                                             "explicit part structure, and the shapefile's implicit ring "
                                             "closure made explicit as WKB requires; null source geometry "
                                             "retained as null; features with NaN ordinates retained as "
                                             "records with null candidate geometry, raw evidence intact",
                                 "completeness": "Every product in the captured roster is re-verified "
                                                 "against its receipt and read; feature-class schemas must be "
                                                 "identical across every product; the producer's live REST "
                                                 "count is recorded as an independent cross-check",
                                 "candidate_status": "unapproved; publication blocked pending freshness, "
                                                     "schema/ID contract, attribution and release acceptance"}}],
        "lineage": "FEMA publishes the National Flood Hazard Layer (NFHL) as its own geospatial database "
                   "(fema.gov/flood-maps/national-flood-hazard-layer) and distributes county/community NFHL "
                   "Database extracts through the FEMA Map Service Center. FEMA's NFHL Database search page "
                   f"({LISTING_URL}) lists every product with its update date and download link, and each "
                   "link serves the product zip from FEMA's own host. This candidate is the "
                   f"{layer.feature_class} feature class of every listed product, the producer's bulk "
                   f"publication of the same NFHL content the MSC GIS Web Services page documents behind "
                   f"its public REST service (layer {layer.layer_id} '{layer.name}'). The HIFLD Open "
                   f"inventory lists this dataset as {layer.inventory_id} with DataLumos project "
                   f"{layer.datalumos_project}; that ICPSR archive copy is identification and comparison "
                   "context only, never a pipeline input.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls, and geometry retained in "
                      "the producer CRS; all release and compatibility approvals remain pending.",
            "objects": [{"path": rel, **parquet}]}}},
        "unsupported_output_formats": {
            "shapefile_zip": "No approved source-to-public Shapefile field mapping.",
            "geopackage": "No approved source-to-public schema mapping.",
            "pmtiles": "No display transformation or generalized geometry approved."}}


def capture_nfhl_bulk_layer(storage, layer: NfhlLayer, *, capture_directory: str | None = None,
                            settings: BulkSettings | None = None) -> dict[str, object]:
    """Capture one layer's feature class from the shared bulk NFHL copy and stage it."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if layer.source != "bulk" or not layer.feature_class:
        raise ValueError(f"nfhl_layer_not_bulk_backed:{layer.slug}")
    settings = settings or BulkSettings()
    base = check_local_staging(storage)
    if shutil.disk_usage(base).free < layer.min_free_bytes:
        raise ValueError("disk_floor_before_acquisition")
    with ExitStack() as stack:
        parent = Path(capture_directory) if capture_directory else Path(
            stack.enter_context(tempfile.TemporaryDirectory(prefix="nfhl-bulk-"))).resolve()
        root = (parent / "bulk").absolute()
        if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.parent.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(root.parent).free < BULK_CAPTURE_FLOOR_BYTES:
            raise ValueError("disk_floor_before_bulk_acquisition")
        with settings.client() as client:
            with capture_lock(root):
                products, refresh = refresh_products(client, root, settings=settings)
            rest_count = rest_feature_count(client, layer)
        parquet_path = root / "work" / f"{layer.slug}.parquet"
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        parquet_path.unlink(missing_ok=True)
        result = stream_feature_class(products, root, layer, parquet_path)
        if result.feature_count != result.id_count:
            raise ValueError("source_feature_count_roster_mismatch")
        if (result.counters.products_with_feature_class
                + result.counters.products_without_feature_class != len(products)):
            raise ValueError("source_product_roster_mismatch")
        if not result.feature_count:
            raise ValueError("source_feature_count_empty")
        bounds = result.bounds
        observed_bounds = None if bounds[0] is None else {
            "xmin": bounds[0], "ymin": bounds[1], "xmax": bounds[2], "ymax": bounds[3]}
        identity = _code_identity()
        rel = f"geoparquet/{layer.slug}.parquet"
        parquet = {"bytes": parquet_path.stat().st_size, "sha256": sha256_file(parquet_path)}
        inputs = _inputs(root, products)
        audit = {
            "source_feature_count": result.feature_count,
            "source_product_count": len(products),
            "source_products_with_feature_class": result.counters.products_with_feature_class,
            "source_products_without_feature_class": result.counters.products_without_feature_class,
            "source_product_roster_sha256": product_roster_sha256(products),
            "source_id_sha256": result.id_sha256, "source_record_sha256": result.record_sha256,
            "source_geometry_sha256": result.geometry_sha256, "source_schema_sha256": result.schema_sha256,
            "source_crs_epsg": SOURCE_CRS_WKID, "geometry_kind": layer.geometry,
            "null_geometry_count": result.counters.null_geometry,
            "nan_string_geometry_count": result.counters.nan_geometry,
            "observed_bounds": observed_bounds, "unique_source_id_count": result.id_count,
            "source_fields": result.schema_signature, "feature_class": layer.feature_class,
            "official_layer_name": layer.name, "layer_id": layer.layer_id,
            "rest_count_cross_check": rest_count,
            "rest_count_delta": None if rest_count is None else result.feature_count - rest_count,
            "source_release_date": None,
            "source_release_date_unknown_reason": "FEMA updates the NFHL continuously and publishes no "
                                                  "single release date; each product's own update date is "
                                                  "recorded in the source manifest inputs.",
            "release_date_unknown": True, **_READY_FLAGS}
        version = content_derived_version(result.record_sha256, result.geometry_sha256,
                                          result.schema_sha256, identity)
        prefix = candidate_prefix(layer.slug, layer.slug, version)
        state_key = f"_construction-state/{layer.slug}/{version}.json"
        fingerprint = _sha(_canonical(identity))
        if storage.object_snapshot(state_key) is not None:
            bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
            payload = bundle.get("payload")
            if not isinstance(payload, dict):
                raise ValueError("cached_first_candidate_invalid")
            verify_cached_candidate(storage, REPO_ROOT, state_key, prefix, version, fingerprint, identity,
                                    result.record_sha256, result.schema_sha256, payload.get("objects", {}),
                                    payload.get("capture_receipts", {}))
            return {**audit, "dataset_slug": layer.slug, "file_slug": layer.slug, "version": version,
                    "feature_count": result.feature_count, "source_count": result.feature_count,
                    "source_null_geometry_count": result.counters.null_geometry,
                    "source_nan_string_geometry_count": result.counters.nan_geometry,
                    "candidate_ready": prefix + "/metadata/candidate_ready.json",
                    "reused": True, **APPROVAL_FLAGS}
        manifest = _manifest(layer, identity, inputs, rel, parquet)
        validate_release_provenance(manifest)
        validate_output_policy(manifest)
        receipts = {name: product_receipt(root, name) for name in ("listing_before", "listing_after")}
        if any(receipt is None or receipt.get("complete_body") is not True for receipt in receipts.values()):
            raise ValueError("source_raw_evidence_missing_for_staging:listing")
        product_receipts = {product.item_id: product_receipt(root, product.item_id) for product in products}
        if any(receipt is None or receipt.get("complete_body") is not True
               for receipt in product_receipts.values()):
            raise ValueError("source_raw_evidence_missing_for_staging:product")
        documents = {
            "metadata/source_manifest.json": _canonical(manifest) + b"\n",
            "metadata/source_audit.json": _canonical(audit) + b"\n",
            "metadata/processing_identity.json": _canonical(identity) + b"\n",
            "raw/products.json": (root / "products.json").read_bytes(),
            "raw/product_receipts.json": _canonical(product_receipts) + b"\n",
        }
        objects = {rel: stage_object(storage, f"{prefix}/{rel}", path=parquet_path)}
        for name, body in documents.items():
            objects[name] = stage_object(storage, f"{prefix}/{name}", body=body)
        for name in ("listing_before", "listing_after"):
            for suffix in (".body", ".wire", ".receipt.json"):
                objects[f"raw/{name}{suffix}"] = stage_object(
                    storage, f"{prefix}/raw/{name}{suffix}", path=root / f"{name}{suffix}")
        ready = {"approved": False, "version": version, "objects": {rel: objects[rel]},
                 "source_record_sha256": result.record_sha256, **_READY_FLAGS}
        objects["metadata/candidate_ready.json"] = stage_object(
            storage, f"{prefix}/metadata/candidate_ready.json", body=_canonical(ready) + b"\n")
        seal_ready_candidate(storage, state_key, version, fingerprint, {
            "processing_identity": identity, "source_record_sha256": result.record_sha256,
            "source_schema_sha256": result.schema_sha256,
            "capture_receipts": {name: receipts[name] for name in receipts},
            "objects": objects})
        validate_construction_code_identity(REPO_ROOT, identity["code_commit"], identity["files"])
        parquet_path.unlink(missing_ok=True)
        return {**audit, "dataset_slug": layer.slug, "file_slug": layer.slug, "version": version,
                "feature_count": result.feature_count, "source_count": result.feature_count,
                "source_null_geometry_count": result.counters.null_geometry,
                "source_nan_string_geometry_count": result.counters.nan_geometry,
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
                "capture_refresh": refresh, "capture_directory": str(root), **APPROVAL_FLAGS}
