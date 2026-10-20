"""Verified extraction of USACE IENC pipeline-area polygons."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import httpx

SERVICE_URL = (
    "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/"
    "USACE_IENC_Master_Service/FeatureServer"
)
LAYER_ID = 82
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
SOURCE_ITEM_ID = "6ee858a220a143a48c8d40a6cf0d3680"
EXPECTED_COUNT = 112
EXPECTED_OBJECTIDS_SHA256 = "4cabeaf4e238179ac9acfc1448ad6a906779a99fbec1e6da743c44cb4604b004"
EXPECTED_FIELDS = (
    "OBJECTID",
    "Object_Name",
    "Product",
    "Restriction",
    "Status",
    "Category_of_Pipeline",
    "Information",
    "Scale_Minimum",
    "Source_Date",
    "Source_Indication",
    "Source_Dataset",
    "Feature_ID",
    "Shape__Area",
    "Shape__Length",
)


class SourceDriftError(ValueError):
    """Raised when the live source no longer matches its reviewed baseline."""


def _get_json(client: httpx.Client, url: str, params: dict[str, object]) -> dict:
    response = client.get(url, params=params)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise SourceDriftError(f"Expected a JSON object from {url}.")
    if payload.get("error"):
        raise SourceDriftError(f"ArcGIS returned an error: {payload['error']}")
    return payload


def _objectid_digest(ids: list[int]) -> str:
    encoded = ",".join(str(value) for value in sorted(ids)).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def extract_pipeline_areas(
    client: httpx.Client | None = None,
) -> tuple[bytes, dict[str, object]]:
    """Fetch and validate the complete reviewed 112-feature baseline.

    All IDs, records, attributes and polygon coordinates are preserved in the
    native ArcGIS GeoJSON response. Any count, roster, schema, or geometry drift
    fails before the caller writes a new staged version.
    """
    own_client = client is None
    client = client or httpx.Client(follow_redirects=True, timeout=120.0)
    try:
        service = _get_json(client, SERVICE_URL, {"f": "json"})
        layer = _get_json(client, LAYER_URL, {"f": "json"})
        if layer.get("id") != LAYER_ID or layer.get("name") != "PIPE_AREA":
            raise SourceDriftError("Expected USACE IENC layer 82 (PIPE_AREA).")
        if layer.get("geometryType") != "esriGeometryPolygon":
            raise SourceDriftError("PIPE_AREA geometry type changed from polygon.")
        if layer.get("copyrightText") != "USACE IENC":
            raise SourceDriftError("PIPE_AREA source attribution changed.")

        fields = layer.get("fields")
        if not isinstance(fields, list) or not fields:
            raise SourceDriftError("PIPE_AREA layer has no field schema.")
        field_names = [field.get("name") for field in fields]
        if any(not isinstance(name, str) for name in field_names):
            raise SourceDriftError("PIPE_AREA field schema contains unnamed fields.")
        if tuple(field_names) != EXPECTED_FIELDS:
            raise SourceDriftError("PIPE_AREA field schema changed from the reviewed baseline.")

        count_payload = _get_json(
            client,
            f"{LAYER_URL}/query",
            {"where": "1=1", "returnCountOnly": "true", "f": "json"},
        )
        count = count_payload.get("count")
        if count != EXPECTED_COUNT:
            raise SourceDriftError(
                f"PIPE_AREA count drift: expected {EXPECTED_COUNT}, received {count}."
            )
        ids_payload = _get_json(
            client,
            f"{LAYER_URL}/query",
            {"where": "1=1", "returnIdsOnly": "true", "f": "json"},
        )
        ids = ids_payload.get("objectIds")
        if not isinstance(ids, list) or len(ids) != count or any(type(i) is not int for i in ids):
            raise SourceDriftError("PIPE_AREA object-ID roster is missing or inconsistent.")
        ids_digest = _objectid_digest(ids)
        if ids_digest != EXPECTED_OBJECTIDS_SHA256:
            raise SourceDriftError("PIPE_AREA object-ID roster drifted from the reviewed baseline.")

        response = client.get(
            f"{LAYER_URL}/query",
            params={
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4326",
                "orderByFields": "OBJECTID",
                "f": "geojson",
            },
        )
        response.raise_for_status()
        body = response.content
        try:
            collection = response.json()
        except ValueError as exc:
            raise SourceDriftError("PIPE_AREA returned invalid GeoJSON.") from exc
        if collection.get("type") != "FeatureCollection":
            raise SourceDriftError("PIPE_AREA response is not a GeoJSON FeatureCollection.")
        features = collection.get("features")
        if not isinstance(features, list) or len(features) != count:
            raise SourceDriftError("PIPE_AREA feature response is incomplete.")
        feature_ids: list[int] = []
        business_ids: list[str] = []
        expected_properties = set(field_names)
        for feature in features:
            props = feature.get("properties")
            geom = feature.get("geometry")
            if not isinstance(props, dict) or set(props) != expected_properties:
                raise SourceDriftError("PIPE_AREA feature attributes do not match the layer schema.")
            oid = props.get("OBJECTID")
            if type(oid) is not int:
                raise SourceDriftError("PIPE_AREA feature has a missing or non-integer OBJECTID.")
            business_id = props.get("Feature_ID")
            if not isinstance(business_id, str) or not business_id:
                raise SourceDriftError(f"PIPE_AREA feature {oid} has no native Feature_ID.")
            if not isinstance(geom, dict) or geom.get("type") != "Polygon" or not geom.get("coordinates"):
                raise SourceDriftError(f"PIPE_AREA feature {oid} has missing/non-polygon geometry.")
            feature_ids.append(oid)
            business_ids.append(business_id)
        if sorted(feature_ids) != sorted(ids):
            raise SourceDriftError("PIPE_AREA GeoJSON IDs do not match the complete ID roster.")
        if len(set(business_ids)) != len(business_ids):
            raise SourceDriftError("PIPE_AREA contains duplicate native Feature_ID values.")
        if collection.get("crs", {}).get("properties", {}).get("name") not in {
            "EPSG:4326",
            "urn:ogc:def:crs:OGC:1.3:CRS84",
        }:
            raise SourceDriftError("PIPE_AREA GeoJSON CRS changed from WGS84.")

        receipt: dict[str, object] = {
            "source_item_id": SOURCE_ITEM_ID,
            "service_url": SERVICE_URL,
            "layer_url": LAYER_URL,
            "layer_id": LAYER_ID,
            "layer_name": layer["name"],
            "copyright": layer["copyrightText"],
            "service_description": service.get("serviceDescription", ""),
            "service_item_url": f"https://www.arcgis.com/home/item.html?id={SOURCE_ITEM_ID}",
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
            "feature_count": len(features),
            "objectids_sha256": ids_digest,
            "schema": fields,
            "geojson_sha256": hashlib.sha256(body).hexdigest(),
            "geojson_bytes": len(body),
            "crs": "EPSG:4326",
            "preservation": "Original ArcGIS GeoJSON response bytes; no field coercion or geometry conversion.",
            "lineage_limit": (
                "USACE IENC master service identifies its vector data as all IENC data in its entirety. "
                "Equivalence to the archived HIFLD Pipeline Areas snapshot and currency of underlying "
                "pipeline surveys are not established by this service response."
            ),
        }
        return body, receipt
    finally:
        if own_client:
            client.close()
