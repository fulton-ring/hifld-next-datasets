"""Profile-driven capture of nonspatial EPA FRS ArcGIS tables."""
from __future__ import annotations

import hashlib
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
    relationship_name: str | None
    related_table_id: int | None
    relationship_key: str | None
    relationship_cardinality: str | None
    source_vintage: str | None
    expected_rows: int
    portal_item_id: str | None = None
    portal_item_owner: str | None = None
    portal_item_title: str | None = None
    page_size: int = 500
    max_response_bytes: int = 8 * 1024 * 1024

    def __post_init__(self) -> None:
        relationship = (self.relationship_name, self.related_table_id,
                        self.relationship_key, self.relationship_cardinality)
        if any(value is None for value in relationship) and any(value is not None for value in relationship):
            raise ValueError("relationship_contract_must_be_complete_or_undeclared")
        portal = (self.portal_item_id, self.portal_item_owner, self.portal_item_title)
        if any(value is None for value in portal) and any(value is not None for value in portal):
            raise ValueError("portal_item_contract_must_be_complete")
        if self.source_vintage is None and self.portal_item_id is None:
            raise ValueError("source_identity_contract_required")

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
    if not isinstance(relationships, list):
        raise ArcGISTableCaptureError("source_table_relationship_metadata_missing")
    if profile.relationship_name is None:
        return fields
    if not any(
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
    *, replay_captures: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, tuple[bytes, dict[str, Any]]]]:
    """Fetch and retain exact GET responses, bracketing all rows with count/ID reads."""
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, Any]]] = {}

    def request(name: str, url: str, params: Mapping[str, Any]) -> dict[str, Any]:
        request_url = url + "?" + urlencode(params)
        if replay_captures:
            body_path = capture_root / f"{name}.body"
            wire_path = capture_root / f"{name}.wire"
            receipt_path = capture_root / f"{name}.receipt.json"
            body, wire = body_path.read_bytes(), wire_path.read_bytes()
            receipt = json.loads(receipt_path.read_bytes())
            if (receipt.get("method") != "GET" or receipt.get("url") != request_url
                    or receipt.get("final_url") != request_url or receipt.get("status") != 200
                    or receipt.get("complete_body") is not True
                    or receipt.get("bytes") != len(body)
                    or receipt.get("sha256") != hashlib.sha256(body).hexdigest()
                    or receipt.get("wire_bytes") != len(wire)
                    or receipt.get("wire_sha256") != hashlib.sha256(wire).hexdigest()):
                raise ArcGISTableCaptureError("retained_source_capture_receipt_invalid:" + name)
        else:
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
    portal_item = None
    if profile.portal_item_id is not None:
        portal_item = request(
            "portal_item",
            f"https://www.arcgis.com/sharing/rest/content/items/{profile.portal_item_id}",
            {"f": "json"},
        )
        if (portal_item.get("id") != profile.portal_item_id
                or portal_item.get("owner") != profile.portal_item_owner
                or portal_item.get("title") != profile.portal_item_title
                or portal_item.get("type") != "Feature Service"
                or portal_item.get("url", "").rstrip("/") != profile.service_url.rstrip("/")):
            raise ArcGISTableCaptureError("source_portal_item_identity_changed")
    tables = service.get("tables")
    vintage_matches = True
    if profile.source_vintage is not None:
        month_vintage = datetime.strptime(profile.source_vintage, "%Y-%m").strftime("%B %Y")
        vintage_matches = month_vintage in str(service.get("description", ""))
    if (not isinstance(tables, list) or not any(isinstance(item, dict)
            and item.get("id") == profile.table_id and item.get("name") == profile.table_name
            for item in tables) or not vintage_matches):
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
        "declared_relationship_contract": ({"name": profile.relationship_name,
            "related_table_id": profile.related_table_id, "key": profile.relationship_key,
            "cardinality": profile.relationship_cardinality}
            if profile.relationship_name is not None else None),
        "portal_item": ({key: portal_item.get(key) for key in
            ("id", "owner", "title", "type", "url", "modified")}
            if portal_item is not None else None),
        "service_description": service.get("description"), "source_vintage": profile.source_vintage,
        "count_before": count_before, "count_after": after_count,
        "id_count_before": len(ids_before), "id_count_after": len(ids_after),
        "page_size": profile.page_size, "page_count": len(range(0, count_before, profile.page_size)),
        "rows_captured": len(rows), "all_ids_captured_once": True,
        "source_ids_equal_before_after": True, "fields": list(fields),
        "response_receipts": {name: receipt for name, (_body, receipt) in captures.items()},
    }
    return rows, manifest, captures
