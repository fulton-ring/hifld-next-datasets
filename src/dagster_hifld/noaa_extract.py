"""Bounded, lossless NOAA research extraction and deterministic draft candidates.

This module never approves or publishes data. Raw bytes remain unchanged; explicit
Esri field declarations drive candidate physical types. Candidate outputs live
outside canonical source directories until a versioned mapping is accepted.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import sqlite3
import tempfile
from pathlib import Path
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import fiona
import pyarrow as pa
from pyproj import CRS
import pyarrow.parquet as pq
from shapely import from_wkb
from shapely.geometry import LineString, MultiLineString, mapping

from dagster_hifld.promotion import write_immutable_object
from dagster_hifld.resources import StagingStorageResource

_CAPTURE_NAMES = ("layer", "count_before", "features", "ids_before", "ids_after", "count_after", "geojson")


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _digest(value: object) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _body(captures: dict[str, bytes], name: str) -> dict:
    result = json.loads(captures[name])
    if not isinstance(result, dict) or "error" in result:
        raise ValueError(f"NOAA {name}: invalid JSON object or ArcGIS error response")
    return result


def fetch_noaa_layer(layer_url: str, *, timeout: float = 45, attempts: int = 2) -> tuple[dict[str, bytes], list[dict]]:
    """Fetch one full bounded layer plus independent before/after ID/count checks.

    Default proxy/TLS and runtime identity are retained. No pagination guess: a
    future oversized response fails closed and requires a reviewed adapter change.
    """
    if attempts < 1 or attempts > 3:
        raise ValueError("NOAA retry attempts must be between 1 and 3")
    query = layer_url.rstrip("/") + "/query?where=1%3D1&"
    urls = {
        "layer": layer_url + "?f=pjson",
        "count_before": query + "returnCountOnly=true&f=json",
        "ids_before": query + "outFields=OBJECTID&returnGeometry=false&orderByFields=OBJECTID&resultOffset=0&resultRecordCount=1000&f=pjson",
        "features": query + "outFields=*&returnGeometry=true&outSR=4326&f=pjson",
        "geojson": query + "outFields=*&f=geojson",
        "ids_after": query + "outFields=OBJECTID&returnGeometry=false&orderByFields=OBJECTID&resultOffset=0&resultRecordCount=1000&f=pjson",
        "count_after": query + "returnCountOnly=true&f=json",
    }
    captures = {}
    receipts = []
    for name, url in urls.items():
        started = datetime.now(timezone.utc).isoformat()
        for attempt in range(attempts):
            try:
                with urlopen(Request(url, headers={"User-Agent": "HIFLD NOAA draft source adapter"}), timeout=timeout) as response:
                    content = response.read(32 * 1024 * 1024 + 1)
                    status = response.status
                    final_url = response.url
                break
            except (HTTPError, URLError, TimeoutError) as exc:
                if attempt + 1 == attempts:
                    if isinstance(exc, HTTPError):
                        content = exc.read(32 * 1024 * 1024 + 1)
                        status, final_url = exc.code, exc.url
                        break
                    receipts.append({"capture": name, "url": url, "fetched_at": started, "transport_error": type(exc).__name__, "attempts": attempts})
                    return captures, receipts  # Partial raw evidence is quarantined by staging.
                time.sleep(0.25)
        captures[name] = content
        if len(content) > 32 * 1024 * 1024:
            receipts.append({"capture": name, "url": url, "fetched_at": started, "error": "response exceeds 32MiB bound; retained bounded prefix", "truncated": True})
            return captures, receipts
        receipts.append({"capture": name, "url": url, "final_url": final_url, "http_status": status,
                         "fetched_at": started, "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()})
    return captures, receipts


def noaa_reference_fingerprints(layer: dict, features: dict) -> dict:
    """Fingerprint a proposed source snapshot; these hashes never imply acceptance."""
    ordered = sorted(features["features"], key=lambda f: f["attributes"]["OBJECTID"])
    return {
        "schema": _digest(layer["fields"]),
        "identifiers": _digest([{k: f["attributes"][k] for k in ("OBJECTID", "BOUND_ID")} for f in ordered]),
        "geometry": _digest([{"OBJECTID": f["attributes"]["OBJECTID"], "geometry": f["geometry"]} for f in ordered]),
        "coverage": _digest({k: dict(sorted(Counter(f["attributes"][k] for f in ordered).items())) for k in ("REGION", "FEAT_TYPE")}),
    }


def validate_noaa_capture(captures: dict[str, bytes], *, expected_fields: dict[str, str], expected_fingerprints: dict[str, str], expected_count: int | None = None) -> tuple[pa.Table, dict]:
    """Validate full extraction and return an explicitly unapproved native candidate."""
    if set(captures) != set(_CAPTURE_NAMES):
        raise ValueError("NOAA requires exact complete raw capture set")
    bodies = {name: _body(captures, name) for name in _CAPTURE_NAMES}
    layer, full, geo = bodies["layer"], bodies["features"], bodies["geojson"]
    actual_fields = {f["name"]: f["type"] for f in layer["fields"]}
    if actual_fields != expected_fields or len(actual_fields) != len(layer["fields"]):
        raise ValueError("NOAA schema drift: fields missing, renamed, duplicated, unexpected or changed type")
    if layer.get("geometryType") != "esriGeometryPolyline" or full.get("geometryType") != "esriGeometryPolyline":
        raise ValueError("NOAA geometry type drift")
    sr = full.get("spatialReference", {})
    if sr.get("latestWkid", sr.get("wkid")) != 4326:
        raise ValueError("NOAA coordinate reference drift; EPSG4326 required without transform")
    if full.get("hasZ") or full.get("hasM") or layer.get("hasZ") or layer.get("hasM"):
        raise ValueError("NOAA unexpected Z/M dimensions")
    count = bodies["count_before"].get("count")
    if type(count) is not int or count < 1 or count != bodies["count_after"].get("count"):
        raise ValueError("NOAA incomplete extraction: before/after count changed or invalid")
    if expected_count is not None and count != expected_count:
        raise ValueError("NOAA count/coverage drift from proposed research snapshot")
    if count > min(layer.get("maxRecordCount", 0), 1000):
        raise ValueError("NOAA completeness requires reviewed pagination; count exceeds bounded page")
    for name in ("features", "geojson"):
        body = bodies[name]
        if body.get("exceededTransferLimit") or not isinstance(body.get("features"), list) or len(body["features"]) != count:
            raise ValueError(f"NOAA incomplete extraction or transfer limit: {name}")
    ordered = sorted(full["features"], key=lambda f: f["attributes"]["OBJECTID"])
    ids = [f["attributes"].get("OBJECTID") for f in ordered]
    if any(type(v) is not int for v in ids) or len(set(ids)) != count:
        raise ValueError("NOAA missing, duplicate or invalid OBJECTID")
    for name in ("ids_before", "ids_after"):
        body = bodies[name]
        if body.get("exceededTransferLimit"):
            raise ValueError(f"NOAA incomplete ID response: {name}")
        other = body.get("objectIds") if "objectIds" in body else [f["attributes"].get("OBJECTID") for f in body.get("features", [])]
        if any(type(v) is not int for v in other) or len(set(other)) != count or set(other) != set(ids):
            raise ValueError(f"NOAA ID reconciliation failed: {name}")
    for f in ordered:
        if set(f["attributes"]) != set(actual_fields) - {"SHAPE"}:
            raise ValueError("NOAA unmapped or missing attribute fields")
    boundary_ids = [f["attributes"]["BOUND_ID"] for f in ordered]
    if any(type(v) is not str or not v for v in boundary_ids) or len(set(boundary_ids)) != count:
        raise ValueError("NOAA BOUND_ID missing, duplicate or changed physical type")
    fingerprints = noaa_reference_fingerprints(layer, full)
    if set(expected_fingerprints) != set(fingerprints):
        raise ValueError("NOAA proposed schema/ID/geometry/coverage fingerprints required")
    for category, fingerprint in fingerprints.items():
        if expected_fingerprints[category] != fingerprint:
            raise ValueError(f"NOAA {category} drift from proposed snapshot; quarantine for review")
    geos = {f["properties"]["OBJECTID"]: f for f in geo["features"]}
    if len(geos) != count or set(geos) != set(ids):
        raise ValueError("NOAA GeoJSON IDs mismatch or duplicate")
    geometries = []
    for f in ordered:
        object_id = f["attributes"]["OBJECTID"]
        g = f.get("geometry")
        if not isinstance(g, dict) or set(g) != {"paths"} or not g["paths"]:
            raise ValueError("NOAA missing geometry or unexpected geometry members")
        for part in g["paths"]:
            if len(part) < 2 or any(len(v) != 2 or any(type(n) not in (int, float) or not math.isfinite(n) for n in v) or not (-180 <= v[0] <= 180 and -90 <= v[1] <= 90) for v in part):
                raise ValueError("NOAA invalid or out of range geometry coordinates")
        geometry = LineString(g["paths"][0]) if len(g["paths"]) == 1 else MultiLineString(g["paths"])
        if geometry.is_empty or not geometry.is_valid:
            raise ValueError("NOAA invalid or empty geometry")
        gf = geos[object_id]
        coordinates = gf["geometry"]["coordinates"]
        parts = [coordinates] if gf["geometry"]["type"] == "LineString" else coordinates
        if parts != g["paths"] or gf["properties"] != f["attributes"] or gf.get("id") != object_id:
            raise ValueError("NOAA independent GeoJSON geometry/attribute/ID mismatch")
        geometries.append(geometry)
    arrays, arrow_fields = [], []
    for field in layer["fields"]:
        name, kind = field["name"], field["type"]
        if kind == "esriFieldTypeGeometry":
            if name != "SHAPE":
                raise ValueError("NOAA unknown geometry field")
            data, dtype = [g.wkb for g in geometries], pa.binary()
        else:
            data = [f["attributes"][name] for f in ordered]
            if kind in ("esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeDate"):
                dtype = pa.int64()
                if any(v is not None and type(v) is not int for v in data):
                    raise ValueError(f"NOAA integer/date physical type loss: {name}")
            elif kind == "esriFieldTypeDouble":
                dtype = pa.float64()
                if any(v is not None and (type(v) not in (float, int) or not math.isfinite(v) or (type(v) is int and int(float(v)) != v)) for v in data):
                    raise ValueError(f"NOAA double physical type/value loss: {name}")
            elif kind == "esriFieldTypeString":
                dtype = pa.string()
                if any(v is not None and type(v) is not str for v in data):
                    raise ValueError(f"NOAA string physical type loss: {name}")
            else:
                raise ValueError(f"NOAA unsupported declared source type: {kind}")
        if field.get("nullable") is False and any(v is None for v in data):
            raise ValueError(f"NOAA nonnullable source field has NULL: {name}")
        arrow_fields.append(pa.field(name, dtype, nullable=field.get("nullable", True)))
        arrays.append(pa.array(data, type=dtype))
    metadata = {b"geo": _json_bytes({"version": "1.1.0", "primary_column": "SHAPE", "columns": {"SHAPE": {"encoding": "WKB", "geometry_types": sorted({g.geom_type for g in geometries}), "crs": CRS.from_epsg(4326).to_json_dict()}}}),
                b"hifld:noaa_field_declarations": _json_bytes(layer["fields"]),
                b"hifld:acceptance": b"unapproved_draft"}
    table = pa.Table.from_arrays(arrays, schema=pa.schema(arrow_fields, metadata=metadata))
    quality = {"feature_count": count, "ids": ids, "identifier_columns": ["OBJECTID", "BOUND_ID"], "fingerprints": fingerprints,
               "source_geometry_types": dict(sorted(Counter(g.geom_type for g in geometries).items())),
               "bounds": [min(g.bounds[0] for g in geometries), min(g.bounds[1] for g in geometries), max(g.bounds[2] for g in geometries), max(g.bounds[3] for g in geometries)],
               "coverage": {name: dict(sorted(Counter(f["attributes"][name] for f in ordered).items())) for name in ("REGION", "FEAT_TYPE")},
               "schema": [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in table.schema],
               "invalid_geometry_count": 0, "completeness_passed": True, "promotion_approved": False,
               "mapping": "Esri SHAPE paths -> same-named WKB; one path LineString, multiple paths MultiLineString; declared source Double float64, dates epoch-ms int64; all source fields retained; no generated id/bbox or coordinate changes"}
    return table, quality


def stage_noaa_candidate(*, staging: StagingStorageResource, dataset_slug: str, file_slug: str, captures: dict[str, bytes], receipts: list[dict], expected_fields: dict[str, str], expected_fingerprints: dict[str, str], expected_count: int | None = None) -> dict:
    """Persist immutable raw evidence, quarantine drift, and stage a local-only draft.

    A typed GeoPackage processing source is staged for the existing catalog graph.
    Shared #417 approval gates must be installed before wiring this into an asset.
    Candidate staging remains local-only and never grants promotion approval.
    """
    if not staging.use_local or staging.bucket:
        raise ValueError("Unapproved NOAA draft extraction requires isolated local storage")
    identity = _digest({name: hashlib.sha256(raw).hexdigest() for name, raw in sorted(captures.items())})
    version = "v0.0.0-draft.noaa." + identity[:20]
    prefix = f"{dataset_slug}/{file_slug}/{version}"
    if not set(captures).issubset(_CAPTURE_NAMES):
        raise ValueError("NOAA unknown raw capture names")
    for name, raw in sorted(captures.items()):
        write_immutable_object(staging, f"{prefix}/raw/{name}.json", raw)
    receipt_key = f"{prefix}/raw/receipts/{_digest(receipts)}.json"
    write_immutable_object(staging, receipt_key, _json_bytes(receipts))
    try:
        table, quality = validate_noaa_capture(captures, expected_fields=expected_fields, expected_fingerprints=expected_fingerprints, expected_count=expected_count)
    except (ValueError, KeyError, TypeError, OverflowError, pa.ArrowException) as exc:
        write_immutable_object(staging, f"{prefix}/metadata/quarantine.json", _json_bytes({"status": "quarantined", "promotion_approved": False, "reason": str(exc), "raw_identity": identity}))
        raise ValueError(f"NOAA extraction quarantined at {prefix}: {exc}") from exc
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd", use_dictionary=False, write_statistics=True, version="2.6")
    candidate = buffer.getvalue()
    key = write_immutable_object(staging, f"{prefix}/candidate/geoparquet/{file_slug}.parquet", candidate)
    # Typed local bridge for the existing catalog/format graph. Its internal SQLite
    # feature key is a storage implementation detail, never a substitute source ID.
    # All declared source identifier properties remain explicit and unchanged.
    with tempfile.TemporaryDirectory(prefix="hifld_noaa_candidate_") as directory:
        path = Path(directory) / "source.gpkg"
        properties = {f.name: ("int64" if pa.types.is_integer(f.type) else "float" if pa.types.is_floating(f.type) else "str") for f in table.schema if f.name != "SHAPE"}
        with fiona.open(path, "w", driver="GPKG", layer=file_slug, crs="EPSG:4326", schema={"geometry": "Unknown", "properties": properties}, GEOMETRY_NAME="SHAPE") as destination:
            for row in table.to_pylist():
                from_geometry = from_wkb(row.pop("SHAPE"))
                destination.write({"geometry": mapping(from_geometry), "properties": row})
        with sqlite3.connect(path) as connection:
            connection.execute("UPDATE gpkg_contents SET last_change = '1970-01-01T00:00:00.000Z'")
            connection.commit()
            connection.execute("VACUUM")
        source_key = write_immutable_object(staging, f"{prefix}/geopackage/{file_slug}.gpkg", path.read_bytes())
    report = {**quality, "catalog_source_key": source_key, "dataset_slug": dataset_slug, "file_slug": file_slug, "version": version, "candidate_key": key,
              "candidate_sha256": hashlib.sha256(candidate).hexdigest(), "raw_identity": identity,
              "status": "draft_candidate_unapproved", "source_receipts": [{k: v for k, v in r.items() if k != "fetched_at"} for r in receipts],
              "refresh_policy": "Bounded retry on transport failure; content-addressed no-op for identical extraction; any proposed schema/IDs/geometry/coverage drift quarantines; not a release-date assertion"}
    write_immutable_object(staging, f"{prefix}/metadata/noaa_extraction.json", _json_bytes(report))
    return report
