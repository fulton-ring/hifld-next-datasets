"""Profile-driven capture of nonspatial EPA FRS ArcGIS tables."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlencode

import httpx

from dagster_hifld.sources.epa_frs_common import capture_get


@dataclass(frozen=True)
class ArcGISTableProfile:
    service_url: str
    table_id: int
    table_name: str
    fields: tuple[tuple[str, str, bool, int | None], ...]
    relationship_name: str
    related_table_id: int
    relationship_key: str
    relationship_cardinality: str
    source_vintage: str
    expected_rows: int
    page_size: int = 500
    max_response_bytes: int = 8 * 1024 * 1024

    @property
    def table_url(self) -> str:
        return f"{self.service_url.rstrip('/')}/{self.table_id}"

    @property
    def field_names(self) -> tuple[str, ...]:
        return tuple(field[0] for field in self.fields)


class ArcGISTableCaptureError(ValueError):
    """The source table is incomplete, malformed, or has drifted."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _table_contract(payload: Mapping[str, Any], profile: ArcGISTableProfile) -> tuple[dict[str, Any], ...]:
    fields = payload.get("fields")
    if payload.get("name") != profile.table_name or payload.get("geometryType") is not None:
        raise ArcGISTableCaptureError("source_table_identity_or_geometry_changed")
    if not isinstance(fields, list):
        raise ArcGISTableCaptureError("source_table_fields_missing")
    observed = tuple((f.get("name"), f.get("type"), f.get("nullable"), f.get("length"))
                    for f in fields if isinstance(f, dict))
    if observed != profile.fields:
        raise ArcGISTableCaptureError("source_table_schema_changed")
    return tuple(fields)


def _metadata_contract(payload: Mapping[str, Any], profile: ArcGISTableProfile) -> list[dict[str, Any]]:
    fields = _table_contract(payload, profile)
    relationships = payload.get("relationships")
    if not isinstance(relationships, list) or not any(
        isinstance(item, dict) and item.get("name") == profile.relationship_name
        and item.get("relatedTableId") == profile.related_table_id
        and item.get("keyField") == profile.relationship_key
        and item.get("cardinality") == profile.relationship_cardinality
        for item in relationships
    ):
        raise ArcGISTableCaptureError("source_table_relationship_changed")
    return fields


def capture_table(
    client: httpx.Client, profile: ArcGISTableProfile, capture_root: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, tuple[bytes, dict[str, Any]]]]:
    """Fetch and retain exact GET responses, bracketing all rows with count/ID reads."""
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, Any]]] = {}

    def request(name: str, url: str, params: Mapping[str, Any]) -> dict[str, Any]:
        request_url = url + "?" + urlencode(params)
        body, receipt = capture_get(client, request_url, capture_root, name,
                                    max_bytes=profile.max_response_bytes)
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ArcGISTableCaptureError("source_response_is_not_valid_json") from exc
        if not isinstance(payload, dict) or "error" in payload:
            raise ArcGISTableCaptureError("source_response_has_arcgis_error")
        captures[name] = (body, receipt)
        return payload

    service = request("service", profile.service_url, {"f": "json"})
    tables = service.get("tables")
    month_vintage = datetime.strptime(profile.source_vintage, "%Y-%m").strftime("%B %Y")
    if (not isinstance(tables, list) or not any(isinstance(item, dict)
            and item.get("id") == profile.table_id and item.get("name") == profile.table_name
            for item in tables) or month_vintage not in str(service.get("description", ""))):
        raise ArcGISTableCaptureError("source_service_table_or_vintage_changed")
    table_before = request("table_before", profile.table_url, {"f": "json"})
    fields = _metadata_contract(table_before, profile)
    count_query = {"where": "1=1", "returnCountOnly": "true", "f": "json"}
    ids_query = {"where": "1=1", "returnIdsOnly": "true", "f": "json"}
    count_before = request("count_before", profile.table_url + "/query", count_query).get("count")
    ids_before = request("ids_before", profile.table_url + "/query", ids_query).get("objectIds")
    if count_before != profile.expected_rows or not isinstance(ids_before, list) or len(ids_before) != count_before:
        raise ArcGISTableCaptureError("source_count_and_id_roster_disagree_before_capture")
    if (any(type(value) is not int for value in ids_before)
            or len(set(ids_before)) != count_before):
        raise ArcGISTableCaptureError("source_object_id_roster_is_invalid")

    rows: list[dict[str, Any]] = []
    for page_number, offset in enumerate(range(0, count_before, profile.page_size)):
        page = request(
            f"page_{page_number:03d}", profile.table_url + "/query",
            {"where": "1=1", "outFields": ",".join(profile.field_names),
             "returnGeometry": "false", "orderByFields": "OBJECTID ASC",
             "resultOffset": offset, "resultRecordCount": profile.page_size, "f": "json"},
        )
        features = page.get("features")
        wanted = min(profile.page_size, count_before - offset)
        if not isinstance(features, list) or len(features) != wanted:
            raise ArcGISTableCaptureError("source_page_row_count_incomplete")
        if page.get("exceededTransferLimit") is True and wanted < profile.page_size:
            raise ArcGISTableCaptureError("source_final_page_exceeded_transfer_limit")
        for feature in features:
            if not isinstance(feature, dict) or feature.get("geometry") is not None:
                raise ArcGISTableCaptureError("source_table_returned_unexpected_geometry")
            attrs = feature.get("attributes")
            if not isinstance(attrs, dict) or tuple(attrs.keys()) != profile.field_names:
                raise ArcGISTableCaptureError("source_record_fields_changed")
            if type(attrs["OBJECTID"]) is not int:
                raise ArcGISTableCaptureError("source_object_id_type_changed")
            for name, esri_type, nullable, _length in profile.fields:
                value = attrs[name]
                if value is None and not nullable:
                    raise ArcGISTableCaptureError("source_nonnullable_value_is_null")
                if name != "OBJECTID" and value is not None and not isinstance(value, str):
                    raise ArcGISTableCaptureError("source_native_string_type_changed")
            rows.append(attrs)

    table_after = request("table_after", profile.table_url, {"f": "json"})
    fields_after = _metadata_contract(table_after, profile)
    service_after = request("service_after", profile.service_url, {"f": "json"})
    after_count = request("count_after", profile.table_url + "/query", count_query).get("count")
    ids_after = request("ids_after", profile.table_url + "/query", ids_query).get("objectIds")
    captured_ids = [row["OBJECTID"] for row in rows]
    before_ids = sorted(ids_before)
    if (fields_after != fields or table_after.get("relationships") != table_before.get("relationships")
            or service_after.get("description") != service.get("description")
            or service_after.get("tables") != service.get("tables")
            or after_count != count_before or not isinstance(ids_after, list)
            or sorted(ids_after) != before_ids or len(ids_after) != count_before
            or sorted(captured_ids) != before_ids or len(set(captured_ids)) != count_before):
        raise ArcGISTableCaptureError("source_capture_incomplete_or_changed_during_read")
    manifest = {
        "source_url": profile.table_url, "service_url": profile.service_url,
        "source_table": profile.table_name, "relationship": table_before["relationships"],
        "service_description": service.get("description"), "source_vintage": profile.source_vintage,
        "count_before": count_before, "count_after": after_count,
        "id_count_before": len(ids_before), "id_count_after": len(ids_after),
        "page_size": profile.page_size, "page_count": len(range(0, count_before, profile.page_size)),
        "rows_captured": len(rows), "all_ids_captured_once": True,
        "source_ids_equal_before_after": True, "fields": list(fields),
        "response_receipts": {name: receipt for name, (_body, receipt) in captures.items()},
    }
    return rows, manifest, captures
