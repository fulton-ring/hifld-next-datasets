"""Bounded, lossless NOAA research extraction and deterministic draft candidates.

This module never approves or publishes data. Raw bytes remain unchanged; explicit
Esri field declarations drive candidate physical types. Candidate outputs live
outside canonical source directories until a versioned mapping is accepted.
"""
from __future__ import annotations

import base64
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
from http.client import HTTPException
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import parse_qsl, urlsplit

import fiona
import pyarrow as pa
from pyproj import CRS
import pyarrow.parquet as pq
from shapely import from_wkb
from shapely.geometry import LineString, MultiLineString, mapping

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.release_approval import verifier_implementation_sha256

_CAPTURE_NAMES = ("layer", "count_before", "features", "ids_before", "ids_after", "count_after", "geojson")


def _write_draft_staging(storage: StagingStorageResource, key: str, contents: bytes) -> str:
    """Immutable local staging creation; never a published-storage gate bypass."""
    if isinstance(storage, PublishedStorageResource) or not storage.use_local or storage.bucket:
        raise ValueError("Unapproved NOAA drafts require local staging, not published storage")
    before = storage.object_snapshot(key)
    if before is None:
        storage.write_key_if_unchanged(key, contents, None)
    elif storage.read_key(key) != contents or storage.object_snapshot(key) != before:
        raise ValueError(f"NOAA staged content is immutable: {key}")
    return storage._ensure_prefixed(key)


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _digest(value: object) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _body(captures: dict[str, bytes], name: str) -> dict:
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"NOAA {name}: duplicate JSON member {key}")
            result[key] = value
        return result
    result = json.loads(captures[name], object_pairs_hook=unique_object)
    if not isinstance(result, dict) or "error" in result:
        raise ValueError(f"NOAA {name}: invalid JSON object or ArcGIS error response")
    return result


def noaa_capture_urls(layer_url: str) -> dict[str, str]:
    """Exact public NOAA extraction requests; not proof that a replay is authentic."""
    parsed = urlsplit(layer_url)
    prefix = "/arcgis/rest/services/MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer/"
    if (parsed.scheme != "https" or parsed.netloc != "maritimeboundaries.noaa.gov"
            or parsed.query or parsed.fragment or not parsed.path.startswith(prefix)
            or parsed.path[len(prefix):] not in {"1", "2", "3"}):
        raise ValueError("NOAA expected layer must be an exact official HTTPS layer1/2/3 URL")
    query = layer_url.rstrip("/") + "/query?where=1%3D1&"
    return {
        "layer": layer_url + "?f=pjson",
        "count_before": query + "returnCountOnly=true&f=json",
        "ids_before": query + "outFields=OBJECTID&returnGeometry=false&orderByFields=OBJECTID&resultOffset=0&resultRecordCount=1000&f=pjson",
        "features": query + "outFields=*&returnGeometry=true&outSR=4326&f=pjson",
        "geojson": query + "outFields=*&f=geojson",
        "ids_after": query + "outFields=OBJECTID&returnGeometry=false&orderByFields=OBJECTID&resultOffset=0&resultRecordCount=1000&f=pjson",
        "count_after": query + "returnCountOnly=true&f=json",
    }


def _verify_receipt_origin(receipt: dict, expected_layer_url: str) -> None:
    expected = urlsplit(noaa_capture_urls(expected_layer_url)[receipt["capture"]])
    for field in ("url", "final_url"):
        observed_url = receipt.get(field)
        if not isinstance(observed_url, str) or not observed_url:
            raise ValueError(f"NOAA retrieval requires observed {field}")
        observed = urlsplit(observed_url)
        # A URL string is attribution evidence only. Matching it never grants
        # source/contract/baseline approval or authenticates caller-supplied bytes.
        if (observed.scheme != "https" or observed.netloc != expected.netloc
                or observed.path != expected.path or observed.fragment):
            raise ValueError(f"NOAA retrieval {field} is not the exact official layer request")
        query = parse_qsl(observed.query, keep_blank_values=True, strict_parsing=True)
        if len({key for key, value in query}) != len(query) or sorted(query) != sorted(parse_qsl(expected.query)):
            raise ValueError(f"NOAA retrieval {field} has changed or ambiguous request parameters")


def fetch_noaa_layer(layer_url: str, *, timeout: float = 45, attempts: int = 3) -> tuple[dict[str, bytes], list[dict]]:
    """Fetch one full bounded layer plus independent before/after ID/count checks.

    Default proxy/TLS and runtime identity are retained. No pagination guess: a
    future oversized response fails closed and requires a reviewed adapter change.
    """
    if attempts < 1 or attempts > 3:
        raise ValueError("NOAA retry attempts must be between 1 and 3")
    urls = noaa_capture_urls(layer_url)
    captures = {}
    receipts = []
    for name, url in urls.items():
        started = datetime.now(timezone.utc).isoformat()
        failures = []
        for attempt in range(attempts):
            try:
                try:
                    response = urlopen(Request(url, headers={"User-Agent": "HIFLD NOAA draft source adapter"}), timeout=timeout)
                except HTTPError as exc:
                    response = exc
                with response:
                    status, final_url = response.status, response.url
                    content = response.read(32 * 1024 * 1024 + 1)
            except (OSError, HTTPException) as exc:
                failure = {"attempt": attempt + 1, "transport_error": type(exc).__name__}
                partial = getattr(exc, "partial", None)
                if isinstance(partial, bytes):
                    failure.update(body_base64=base64.b64encode(partial).decode(),
                                   bytes=len(partial), sha256=hashlib.sha256(partial).hexdigest(), truncated=True)
                failures.append(failure)
                if attempt + 1 == attempts:
                    if isinstance(partial, bytes):
                        captures[name] = partial
                    receipts.append({"capture": name, "url": url, "fetched_at": started,
                                     "transport_error": type(exc).__name__, "failed_attempts": failures})
                    return captures, receipts
                time.sleep(0.25)
                continue
            try:
                parsed = json.loads(content)
                valid = status == 200 and isinstance(parsed, dict) and "error" not in parsed
            except (ValueError, UnicodeError):
                valid = False
            if valid or attempt + 1 == attempts:
                break
            failures.append({"attempt": attempt + 1, "http_status": status, "bytes": len(content),
                             "sha256": hashlib.sha256(content).hexdigest(),
                             "body_base64": base64.b64encode(content).decode()})
            time.sleep(0.25)
        captures[name] = content
        receipt = {"capture": name, "url": url, "final_url": final_url, "http_status": status,
                   "fetched_at": started, "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
                   "failed_attempts": failures}
        receipts.append(receipt)
        if len(content) > 32 * 1024 * 1024:
            receipt.update(error="response exceeds32MiB; retained bounded prefix", truncated=True)
            return captures, receipts
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
    if "fields" in full and (len(full["fields"]) != len(actual_fields) - 1 or {f["name"]: f["type"] for f in full["fields"]} != {name: kind for name, kind in actual_fields.items() if kind != "esriFieldTypeGeometry"}):
        raise ValueError("NOAA full response field declarations differ from layer")
    if layer.get("geometryType") != "esriGeometryPolyline" or full.get("geometryType") != "esriGeometryPolyline":
        raise ValueError("NOAA geometry type drift")
    sr = full.get("spatialReference", {})
    if sr.get("latestWkid", sr.get("wkid")) != 4326:
        raise ValueError("NOAA coordinate reference drift; EPSG4326 required without transform")
    if full.get("hasZ") or full.get("hasM") or layer.get("hasZ") or layer.get("hasM"):
        raise ValueError("NOAA unexpected Z/M dimensions")
    count = bodies["count_before"].get("count")
    if type(count) is not int or count < 1 or type(bodies["count_after"].get("count")) is not int or count != bodies["count_after"].get("count"):
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
        if not isinstance(other, list) or len(other) != count or any(type(v) is not int for v in other) or len(set(other)) != count or set(other) != set(ids):
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
    if any(type(f["properties"].get("OBJECTID")) is not int or type(f.get("id")) is not int for f in geo["features"]):
        raise ValueError("NOAA independent GeoJSON identifier physical type mismatch")
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
        if gf["geometry"]["type"] != geometry.geom_type:
            raise ValueError("NOAA independent GeoJSON physical geometry type mismatch")
        coordinates = gf["geometry"]["coordinates"]
        parts = [coordinates] if gf["geometry"]["type"] == "LineString" else coordinates
        if any(type(n) not in (int, float) or not math.isfinite(n) for part in parts for vertex in part for n in vertex):
            raise ValueError("NOAA independent GeoJSON coordinate physical type mismatch")
        for field_name, field_type in actual_fields.items():
            if field_type == "esriFieldTypeGeometry":
                continue
            value = gf["properties"].get(field_name)
            if value is None:
                continue
            if ((field_type in ("esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeDate") and type(value) is not int)
                    or (field_type == "esriFieldTypeString" and type(value) is not str)
                    or (field_type == "esriFieldTypeDouble" and type(value) not in (int, float))):
                raise ValueError(f"NOAA independent GeoJSON attribute physical type mismatch: {field_name}")
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


def stage_noaa_candidate(*, staging: StagingStorageResource, dataset_slug: str, file_slug: str, captures: dict[str, bytes], receipts: list[dict], expected_fields: dict[str, str], expected_fingerprints: dict[str, str], expected_count: int | None = None, expected_layer_url: str | None = None, code_commit: str | None = None) -> dict:
    """Persist immutable raw evidence, quarantine drift, and stage a local-only draft.

    A typed GeoPackage processing source is staged for the existing catalog graph.
    Shared #417 approval gates must be installed before wiring this into an asset.
    Candidate staging remains local-only and never grants promotion approval.
    """
    if isinstance(staging, PublishedStorageResource) or not staging.use_local or staging.bucket:
        raise ValueError("Unapproved NOAA draft extraction requires isolated local storage")
    raw_identity = _digest({name: hashlib.sha256(raw).hexdigest() for name, raw in sorted(captures.items())})
    processing_fingerprint = verifier_implementation_sha256()
    identity = _digest({"raw_identity": raw_identity, "processing_fingerprint": processing_fingerprint,
                        "expected_fields": expected_fields, "expected_fingerprints": expected_fingerprints, "expected_count": expected_count, "expected_layer_url": expected_layer_url,
                        "retrieval_urls": [{"capture": r.get("capture"), "url": r.get("url"), "final_url": r.get("final_url")} for r in receipts]})
    version = "v0.0.0-draft.noaa." + identity[:20]
    prefix = f"{dataset_slug}/{file_slug}/{version}"
    if not set(captures).issubset(_CAPTURE_NAMES):
        raise ValueError("NOAA unknown raw capture names")
    for name, raw in sorted(captures.items()):
        _write_draft_staging(staging, f"{prefix}/raw/{name}.json", raw)
    receipt_key = f"{prefix}/raw/receipts/{_digest(receipts)}.json"
    _write_draft_staging(staging, receipt_key, _json_bytes(receipts))
    try:
        if expected_layer_url is None:
            raise ValueError("NOAA expected_layer_url is required for producer attribution")
        noaa_capture_urls(expected_layer_url)
        if len(receipts) != len(_CAPTURE_NAMES) or {r.get("capture") for r in receipts} != set(_CAPTURE_NAMES):
            raise ValueError("NOAA retrieval receipts missing, duplicated or unexpected")
        for receipt in receipts:
            name = receipt["capture"]
            raw = captures.get(name)
            if (type(receipt.get("http_status")) is not int or receipt["http_status"] != 200
                    or receipt.get("transport_error") or receipt.get("truncated")):
                raise ValueError(f"NOAA terminal HTTP/transport status failed: {name}")
            if (raw is None or type(receipt.get("bytes")) is not int or receipt["bytes"] != len(raw)
                    or receipt.get("sha256") != hashlib.sha256(raw).hexdigest()):
                raise ValueError(f"NOAA retrieval receipt does not match raw body: {name}")
            _verify_receipt_origin(receipt, expected_layer_url)
        table, quality = validate_noaa_capture(captures, expected_fields=expected_fields, expected_fingerprints=expected_fingerprints, expected_count=expected_count)
    except (ValueError, KeyError, TypeError, OverflowError, pa.ArrowException) as exc:
        _write_draft_staging(staging, f"{prefix}/metadata/quarantine.json", _json_bytes({"status": "quarantined", "promotion_approved": False, "reason": str(exc), "raw_identity": raw_identity, "processing_fingerprint": processing_fingerprint}))
        raise ValueError(f"NOAA extraction quarantined at {prefix}: {exc}") from exc
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd", use_dictionary=False, write_statistics=True, version="2.6")
    candidate = buffer.getvalue()
    key = _write_draft_staging(staging, f"{prefix}/candidate/geoparquet/{file_slug}.parquet", candidate)
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
        source_key = _write_draft_staging(staging, f"{prefix}/geopackage/{file_slug}.gpkg", path.read_bytes())
    receipt_by_name = {r["capture"]: r for r in receipts}
    maximum_lengths = {field.name: max((len(value.encode("utf-8")) for value in table[field.name].to_pylist() if value is not None), default=0) for field in table.schema if pa.types.is_string(field.type)}
    trailing_whitespace = {field.name: sum(value is not None and value != value.rstrip() for value in table[field.name].to_pylist()) for field in table.schema if pa.types.is_string(field.type)}
    unsupported = {"shapefile_zip": "Exact NOAA source preservation exceeds DBF capabilities: string max UTF8 bytes=" + json.dumps(maximum_lengths, sort_keys=True) + "; trailing whitespace counts=" + json.dumps(trailing_whitespace, sort_keys=True) + "; nullable epoch-ms int64 and float64 values require exact physical type/precision. DBF trimming/rounding is incompatible with exact preservation; no lossy mapping approved."}
    source_manifest = {
        "schema_version": 1, "processor": "HIFLD Next", "title": dataset_slug,
        "producer": "NOAA / NOS / Office of Coast Survey", "publisher": "NOAA / NOS / Office of Coast Survey",
        "source_url": "https://nauticalcharts.noaa.gov/data/us-maritime-limits-and-boundaries.html",
        "code_commit": code_commit, "processing_fingerprint": processing_fingerprint, "baseline_accepted": False, "contract_approved": False,
        "unsupported_output_formats": unsupported,
        "inputs": [{"input_id": name, "producer": "NOAA / NOS / Office of Coast Survey",
                    "host": urlsplit(receipt_by_name.get(name, {}).get("url", "")).hostname,
                    "canonical_url": "https://nauticalcharts.noaa.gov/data/us-maritime-limits-and-boundaries.html",
                    "retrieval_url": receipt_by_name.get(name, {}).get("url"),
                    "final_url": receipt_by_name[name]["final_url"],
                    "retrieved_at": receipt_by_name.get(name, {}).get("fetched_at"),
                    "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw), "raw_key": f"{prefix}/raw/{name}.json",
                    "role": "producer_publication", "license": "Producer use/distribution terms apply; NOT FOR LEGAL USE; final license acceptance pending",
                    "attribution": "NOAA / NOS / Office of Coast Survey; US Baseline Committee",
                    "release_date": None, "release_date_unknown_reason": "Producer publication/revision year2013 and service4.1 revision2013-09-13 do not independently pin a current downloadable release;2026 metadata edit is not source release"}
                   for name, raw in sorted(captures.items())],
        "transformations": [{"operation": "source-native draft candidate", "description": quality["mapping"]},
                            {"operation": "typed GeoPackage catalog bridge", "description": "All source attributes and SHAPE vertices retained; SQLite internal feature key is not a replacement identifier"}],
    }
    manifest_key = f"{prefix}/metadata/source_manifest.json"
    if not staging.object_exists(manifest_key):
        _write_draft_staging(staging, manifest_key, _json_bytes(source_manifest))
    else:
        previous = json.loads(staging.read_key(manifest_key))
        # Keep original extraction attribution immutable; refresh receipts append.
        def semantic_manifest(value):
            clean = json.loads(json.dumps(value))
            clean.pop("code_commit", None)  # First code commit remains valid for identical installed code bytes.
            for item in clean["inputs"]:
                item.pop("retrieved_at", None)  # Keep first extraction attribution; later receipts are appended.
            return clean
        if semantic_manifest(previous) != semantic_manifest(source_manifest):
            raise ValueError("NOAA existing provenance semantics changed; use new processing namespace")
    report = {**quality, "catalog_source_key": source_key, "dataset_slug": dataset_slug, "file_slug": file_slug, "version": version, "candidate_key": key,
              "candidate_sha256": hashlib.sha256(candidate).hexdigest(), "raw_identity": raw_identity,
              "processing_fingerprint": processing_fingerprint, "namespace_identity": identity,
              "status": "draft_candidate_unapproved", "receipt_directory": "raw/receipts",
              "refresh_policy": "Bounded retry on transport failure; content-addressed no-op for identical extraction; any proposed schema/IDs/geometry/coverage drift quarantines; not a release-date assertion"}
    _write_draft_staging(staging, f"{prefix}/metadata/noaa_extraction.json", _json_bytes(report))
    return report
