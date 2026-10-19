"""Bounded-memory capture of the EPA FRS interest-contact table."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_hifld.http_capture import capture_http_response


SERVICE_URL = "https://gispub.epa.gov/arcgis/rest/services/OEI/FRS_INTERESTS/MapServer"
TABLE_ID = 27
TABLE_NAME = "FRS_INTEREST_CONTACT"
TABLE_URL = f"{SERVICE_URL}/{TABLE_ID}"
PAGE_SIZE = 2_000
ID_WINDOW = 100_000
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
EXPECTED_ROWS = 5_454_002
FIELDS = (
    ("OBJECTID", "esriFieldTypeOID", False, None),
    ("PGM_SYS_ACRNM", "esriFieldTypeString", False, 20),
    ("PGM_SYS_ID", "esriFieldTypeString", False, 30),
    ("FULL_NAME", "esriFieldTypeString", False, 70),
    ("PHONE_NUMBER", "esriFieldTypeString", True, 30),
    ("MAILING_ADDRESS", "esriFieldTypeString", True, 100),
    ("SUPPLEMENTAL_ADDRESS", "esriFieldTypeString", True, 100),
    ("CITY_NAME", "esriFieldTypeString", True, 30),
    ("STATE_CODE", "esriFieldTypeString", True, 5),
    ("POSTAL_CODE", "esriFieldTypeString", True, 14),
    ("KEY_FIELD", "esriFieldTypeString", True, 50),
)
FIELD_NAMES = tuple(field[0] for field in FIELDS)


class EPAFRSContactCaptureError(ValueError):
    """The source response is incomplete or has drifted from its contract."""


class IncompleteSourceResponse(EPAFRSContactCaptureError):
    """A failed HTTP response with the exact partial wire bytes attached."""

    def __init__(self, reason: str, wire: bytes, url: str,
                 headers: Mapping[str, str], status: int) -> None:
        super().__init__(reason)
        self.wire = wire
        self.url = url
        self.headers = dict(headers)
        self.status = status


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def record_digest_update(digest, attributes: Mapping[str, Any]) -> None:
    digest.update(canonical(dict(attributes)))
    digest.update(b"\n")


@dataclass(frozen=True)
class PageResponse:
    """A response body after bounded decompression, with its original wire bytes."""
    wire: bytes
    decoded: bytes
    headers: Mapping[str, str]
    status: int
    final_url: str


def request_page(client: httpx.Client, url: str, params: Mapping[str, Any],
                 *, max_bytes: int = MAX_RESPONSE_BYTES) -> PageResponse:
    """Incrementally retain one bounded response and its original wire bytes."""
    request = client.build_request("GET", url, params=params)
    response = client.send(request, stream=True)
    try:
        wire = bytearray()
        decoded = bytearray()
        capture = capture_http_response(
            response, max_wire_bytes=max_bytes, max_decoded_bytes=max_bytes,
            reserve=lambda _progress: None,
            retain_wire=wire.extend,
            retain_decoded=decoded.extend,
        )
        final_url = str(response.url)
        status = response.status_code
        headers = dict(response.headers)
        if not capture.complete:
            code = ("source_wire_response_exceeds_bound" if capture.error_code == "wire_limit_exceeded"
                    else "source_decoded_response_exceeds_bound" if capture.error_code == "decoded_limit_exceeded"
                    else "source_compressed_response_incomplete")
            raise IncompleteSourceResponse(code, bytes(wire), str(request.url), headers, status)
    except IncompleteSourceResponse:
        raise
    except Exception as exc:
        partial_wire = bytes(wire) if "wire" in locals() else b""
        raise IncompleteSourceResponse("source_response_stream_failed", partial_wire,
                                       str(request.url), response.headers, response.status_code) from exc
    finally:
        response.close()
    if status != 200 or final_url != str(request.url):
        raise IncompleteSourceResponse("source_http_status_or_redirect_changed", bytes(wire),
                                        str(request.url), headers, status)
    return PageResponse(bytes(wire), bytes(decoded), headers, status, final_url)


def _payload(response: PageResponse) -> dict[str, Any]:
    try:
        payload = json.loads(response.decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EPAFRSContactCaptureError("source_response_is_not_json") from exc
    if not isinstance(payload, dict) or "error" in payload:
        raise EPAFRSContactCaptureError("source_response_has_arcgis_error")
    return payload


def check_schema(table: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    if table.get("name") != TABLE_NAME or table.get("geometryType") is not None:
        raise EPAFRSContactCaptureError("source_table_identity_or_geometry_changed")
    raw = table.get("fields")
    if not isinstance(raw, list):
        raise EPAFRSContactCaptureError("source_table_fields_missing")
    actual = tuple((f.get("name"), f.get("type"), f.get("nullable"), f.get("length"))
                   for f in raw if isinstance(f, dict))
    if actual != FIELDS:
        raise EPAFRSContactCaptureError("source_table_schema_changed")
    return tuple(raw)


def _validate_attributes(attributes: Any) -> dict[str, Any]:
    if not isinstance(attributes, dict) or tuple(attributes) != FIELD_NAMES:
        raise EPAFRSContactCaptureError("source_record_fields_changed")
    if type(attributes["OBJECTID"]) is not int:
        raise EPAFRSContactCaptureError("source_object_id_type_changed")
    for name, _esri_type, nullable, _length in FIELDS:
        value = attributes[name]
        if value is None and not nullable:
            raise EPAFRSContactCaptureError("source_nonnullable_value_is_null")
        if name == "OBJECTID" and value is not None:
            continue
        if value is not None and not isinstance(value, str):
            raise EPAFRSContactCaptureError("source_native_string_type_changed")
    return attributes


def query_ids(client: httpx.Client, *, max_object_id: int,
              request: Callable[..., PageResponse] = request_page) -> Iterable[int]:
    """Yield a complete ordered ID roster from bounded numeric windows.

    Windows select by the native object ID value, so gaps are preserved and no
    assumption is made that IDs are contiguous or start at one.
    """
    previous = 0
    for low in range(0, max_object_id, ID_WINDOW):
        high = min(low + ID_WINDOW, max_object_id)
        payload = _payload(request(client, TABLE_URL + "/query", {
            "where": f"OBJECTID > {low} AND OBJECTID <= {high}",
            "returnIdsOnly": "true", "f": "json",
        }))
        ids = payload.get("objectIds")
        if not isinstance(ids, list) or any(type(value) is not int for value in ids):
            raise EPAFRSContactCaptureError("source_id_window_invalid")
        if ids != sorted(ids) or any(not low < value <= high for value in ids):
            raise EPAFRSContactCaptureError("source_id_window_order_or_bounds_changed")
        for value in ids:
            if value <= previous:
                raise EPAFRSContactCaptureError("source_id_roster_duplicate_or_unordered")
            previous = value
            yield value


def _arrow_schema(fields: tuple[dict[str, Any], ...]) -> pa.Schema:
    return pa.schema([pa.field(field["name"], pa.int64() if field["name"] == "OBJECTID" else pa.string(),
                               nullable=field["nullable"],
                               metadata={b"original_esri_field": canonical(field)})
                      for field in fields], metadata={
        b"hifld:source_url": TABLE_URL.encode(),
        b"hifld:source_table": TABLE_NAME.encode(), b"hifld:geometry": b"absent",
    })


def _receipt(name: str, url: str, params: Mapping[str, Any], page: PageResponse) -> dict[str, Any]:
    return {"name": name, "method": "GET", "url": url, "final_url": page.final_url,
            "query": dict(params), "status": page.status,
            "content_encoding": page.headers.get("content-encoding", "identity"),
            "wire_bytes": len(page.wire), "wire_sha256": hashlib.sha256(page.wire).hexdigest(),
            "decoded_bytes": len(page.decoded),
            "decoded_sha256": hashlib.sha256(page.decoded).hexdigest(),
            "complete_body": True}


def capture_to_parquet(client: httpx.Client, capture_root: Path, parquet_path: Path,
                       *, expected_rows: int = EXPECTED_ROWS,
                       page_size: int = PAGE_SIZE,
                       request: Callable[..., PageResponse] = request_page) -> dict[str, Any]:
    """Capture the current source with one decoded query page in memory at a time."""
    if not 0 < page_size <= 2_000:
        raise ValueError("page_size_must_be_between_1_and_2000")
    capture_root.mkdir(parents=True, exist_ok=True)
    nonpage_receipts: list[dict[str, Any]] = []
    metadata_counter = 0
    request_fn = request

    def recorded_request(client, url, params):
        nonlocal metadata_counter
        try:
            page = request_fn(client, url, params)
        except IncompleteSourceResponse as exc:
            name = f"incomplete_{metadata_counter:04d}"
            metadata_counter += 1
            body_path = capture_root / f"{name}.wire"
            body_path.write_bytes(exc.wire)
            receipt = {"name": name, "method": "GET", "url": exc.url, "query": dict(params),
                       "status": exc.status, "complete_body": False, "error_code": str(exc),
                       "content_encoding": exc.headers.get("content-encoding", "identity"),
                       "wire_bytes": len(exc.wire),
                       "wire_sha256": hashlib.sha256(exc.wire).hexdigest()}
            (capture_root / f"{name}.receipt.json").write_bytes(canonical(receipt) + b"\n")
            raise
        if params.get("outFields") != ",".join(FIELD_NAMES):
            name = f"metadata_{metadata_counter:04d}"
            metadata_counter += 1
            receipt = _receipt(name, url, params, page)
            (capture_root / f"{name}.wire.gz").write_bytes(page.wire)
            (capture_root / f"{name}.receipt.json").write_bytes(canonical(receipt) + b"\n")
            nonpage_receipts.append(receipt)
        return page

    request = recorded_request
    metadata = _payload(request(client, SERVICE_URL, {"f": "json"}))
    tables = metadata.get("tables")
    if not isinstance(tables, list) or not any(
        isinstance(item, dict) and item.get("id") == TABLE_ID and item.get("name") == TABLE_NAME
        for item in tables
    ):
        raise EPAFRSContactCaptureError("source_service_table_identity_changed")
    table_before = _payload(request(client, TABLE_URL, {"f": "json"}))
    fields = check_schema(table_before)
    count_before = _payload(request(client, TABLE_URL + "/query", {
        "where": "1=1", "returnCountOnly": "true", "f": "json"})).get("count")
    if type(count_before) is not int or count_before != expected_rows:
        raise EPAFRSContactCaptureError("source_count_changed")

    # The maximum native OID bounds the ID windows; it does not imply a dense roster.
    bounds = _payload(request(client, TABLE_URL + "/query", {
        "where": "1=1", "outFields": "OBJECTID", "returnGeometry": "false",
        "orderByFields": "OBJECTID DESC", "resultRecordCount": 1, "f": "json"}))
    features = bounds.get("features")
    if not isinstance(features, list) or len(features) != 1:
        raise EPAFRSContactCaptureError("source_maximum_id_missing")
    maximum_attrs = features[0].get("attributes")
    maximum_id = maximum_attrs.get("OBJECTID") if isinstance(maximum_attrs, dict) else None
    if type(maximum_id) is not int or maximum_id < 1:
        raise EPAFRSContactCaptureError("source_maximum_id_invalid")

    roster_db = sqlite3.connect(capture_root / "id-roster.sqlite")
    try:
        roster_db.execute("CREATE TABLE before_ids (object_id INTEGER PRIMARY KEY)")
        roster_db.execute("CREATE TABLE captured_ids (object_id INTEGER PRIMARY KEY)")
        before_id_hash = hashlib.sha256()
        before_count = 0
        for object_id in query_ids(client, max_object_id=maximum_id, request=request):
            roster_db.execute("INSERT INTO before_ids VALUES (?)", (object_id,))
            before_id_hash.update(f"{object_id}\n".encode())
            before_count += 1
        roster_db.commit()
        if before_count != count_before:
            raise EPAFRSContactCaptureError("source_id_roster_count_changed_before_capture")

        writer = None
        record_hash = hashlib.sha256()
        page_receipts: list[dict[str, Any]] = []
        seen_rows = 0
        previous_object_id = 0
        try:
            for page_number, offset in enumerate(range(0, count_before, page_size)):
                params = {"where": "1=1", "outFields": ",".join(FIELD_NAMES),
                          "returnGeometry": "false", "orderByFields": "OBJECTID ASC",
                          "resultOffset": offset, "resultRecordCount": page_size, "f": "json"}
                page = request(client, TABLE_URL + "/query", params)
                receipt = _receipt(f"page_{page_number:05d}", TABLE_URL + "/query", params, page)
                (capture_root / f"{receipt['name']}.wire.gz").write_bytes(page.wire)
                (capture_root / f"{receipt['name']}.receipt.json").write_bytes(canonical(receipt) + b"\n")
                payload = _payload(page)
                rows = payload.get("features")
                expected_page = min(page_size, count_before - offset)
                if not isinstance(rows, list) or len(rows) != expected_page:
                    raise EPAFRSContactCaptureError("source_page_row_count_incomplete")
                if payload.get("exceededTransferLimit") is True and expected_page < page_size:
                    raise EPAFRSContactCaptureError("source_final_page_exceeded_transfer_limit")
                attrs_batch = []
                for feature in rows:
                    if not isinstance(feature, dict) or feature.get("geometry") is not None:
                        raise EPAFRSContactCaptureError("source_table_returned_unexpected_geometry")
                    attrs = _validate_attributes(feature.get("attributes"))
                    if attrs["OBJECTID"] <= previous_object_id:
                        raise EPAFRSContactCaptureError("source_object_ids_not_strictly_ordered")
                    previous_object_id = attrs["OBJECTID"]
                    try:
                        roster_db.execute("INSERT INTO captured_ids VALUES (?)", (attrs["OBJECTID"],))
                    except sqlite3.IntegrityError as exc:
                        raise EPAFRSContactCaptureError("source_duplicate_object_id") from exc
                    record_digest_update(record_hash, attrs)
                    attrs_batch.append(attrs)
                if writer is None:
                    writer = pq.ParquetWriter(parquet_path, _arrow_schema(fields), compression="zstd",
                                              version="2.6")
                writer.write_table(pa.Table.from_pylist(attrs_batch, schema=_arrow_schema(fields)))
                page_receipts.append(receipt)
                seen_rows += len(attrs_batch)
            roster_db.commit()
        finally:
            if writer is not None:
                writer.close()
        if seen_rows != count_before:
            raise EPAFRSContactCaptureError("source_captured_row_count_changed")

        # Compare the entire captured OID set against the bracket roster with SQLite,
        # avoiding millions of Python integers or a population-sized feature list.
        missing = roster_db.execute(
            "SELECT 1 FROM before_ids b LEFT JOIN captured_ids c USING(object_id) "
            "WHERE c.object_id IS NULL LIMIT 1").fetchone()
        extra = roster_db.execute(
            "SELECT 1 FROM captured_ids c LEFT JOIN before_ids b USING(object_id) "
            "WHERE b.object_id IS NULL LIMIT 1").fetchone()
        captured_count = roster_db.execute("SELECT COUNT(*) FROM captured_ids").fetchone()[0]
        if missing or extra or captured_count != count_before:
            raise EPAFRSContactCaptureError("source_page_ids_do_not_match_bracket_roster")

        after_count = _payload(request(client, TABLE_URL + "/query", {
            "where": "1=1", "returnCountOnly": "true", "f": "json"})).get("count")
        table_after = _payload(request(client, TABLE_URL, {"f": "json"}))
        fields_after = check_schema(table_after)
        service_after = _payload(request(client, SERVICE_URL, {"f": "json"}))
        tables_after = service_after.get("tables")
        after_hash = hashlib.sha256()
        after_count_ids = 0
        for object_id in query_ids(client, max_object_id=maximum_id, request=request):
            after_hash.update(f"{object_id}\n".encode())
            after_count_ids += 1
        if (after_count != count_before or fields_after != fields
                or table_after.get("relationships") != table_before.get("relationships")
                or tables_after != tables
                or service_after.get("description") != metadata.get("description")
                or service_after.get("serviceDescription") != metadata.get("serviceDescription")
                or after_count_ids != before_count or after_hash.digest() != before_id_hash.digest()):
            raise EPAFRSContactCaptureError("source_changed_during_capture")
        return {"row_count": seen_rows, "maximum_object_id": maximum_id,
                "source_record_sha256": record_hash.hexdigest(),
                "source_ids_sha256": before_id_hash.hexdigest(),
                "source_schema_sha256": hashlib.sha256(canonical(list(fields))).hexdigest(),
                "source_count_before": count_before, "source_count_after": after_count,
                "id_count_before": before_count, "id_count_after": after_count_ids,
                "id_roster_equal_before_after": True, "captured_ids_match_roster": True,
                "page_size": page_size, "page_count": len(page_receipts),
                "page_receipts": page_receipts, "fields": list(fields),
                "table_before": table_before, "table_after": table_after,
                "service_tables_before": tables, "service_tables_after": tables_after,
                "service_description": metadata.get("description"),
                "service_description_after": service_after.get("description"),
                "metadata_receipts": nonpage_receipts}
    finally:
        roster_db.close()
