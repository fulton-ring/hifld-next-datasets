"""Guarded NOAA OCS Overview layer ingestion for an unapproved draft."""

from __future__ import annotations

import collections
import datetime as dt
import hashlib
import importlib.metadata
import io
import json
import math
import pathlib
import shutil
import sqlite3
import sys
import tempfile
import urllib.error
import urllib.request
import zipfile

import pyarrow as pa
import pyogrio
import pyarrow.parquet as pq
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.provenance import validate_provenance
from pyproj import CRS
from shapely.geometry import LineString, MultiLineString

SLUG = "overview-20"
SERVICE = "https://maritimeboundaries.noaa.gov/arcgis/rest/services/MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer"
ZIP_URL = "https://maritimeboundaries.noaa.gov/downloads/USMaritimeLimitsAndBoundariesSHP.zip"
MAX_BYTES = 100 * 1024 * 1024
MAX_RECORD_COUNT = 1000
EXPECTED_COUNT = 260
EXPECTED_SERVICE_TITLE = "U.S. Maritime Limits and Boundaries (version 4.1)"
EXPECTED_SERVICE_DESCRIPTION_PREFIX = "(Version 4.1, 9/13/2013)"
EXPECTED_IDS_SHA256 = "ff1a9250b4f0b618e83644fc3f7057fccb141fc0e95725d6cec2ee149fad8006"
EXPECTED_FIELDS = (
    ("OBJECTID", "esriFieldTypeOID", None),
    ("BOUND_ID", "esriFieldTypeString", 5),
    ("REGION", "esriFieldTypeString", 100),
    ("FEAT_TYPE", "esriFieldTypeString", 100),
    ("PUB_DATE", "esriFieldTypeDate", 8),
    ("APPRV_DATE", "esriFieldTypeDate", 8),
    ("LEGAL_AUTH", "esriFieldTypeString", 100),
    ("AOR", "esriFieldTypeString", 20),
    ("NOTE", "esriFieldTypeString", 1000),
    ("SUPP_INFO", "esriFieldTypeString", 500),
    ("UNILATERAL", "esriFieldTypeDouble", None),
    ("RULEID", "esriFieldTypeInteger", None),
)
EXPECTED_FIELDS_SHA256 = "b1ba8a90183c60d0895e176f6af4f1946ce73f27fe656573ac41e6b71a5e0ae2"


def _url(name: str, *, count: int | None = None) -> str:
    base = SERVICE + "/0/query?where=1%3D1"
    if name in {"count", "count_after"}:
        return base + "&returnCountOnly=true&f=pjson"
    if name in {"ids", "ids_after"}:
        return base + "&returnIdsOnly=true&f=pjson"
    if name == "features":
        return (base + "&outFields=%2A&returnGeometry=true&orderByFields=OBJECTID"
                f"&resultOffset=0&resultRecordCount={MAX_RECORD_COUNT}&f=pjson")
    if name == "page_end":
        return (base + f"&outFields=OBJECTID&orderByFields=OBJECTID&resultOffset={count}"
                f"&resultRecordCount={MAX_RECORD_COUNT}&f=pjson")
    if name == "service":
        return SERVICE + "?f=pjson"
    if name == "layer":
        return SERVICE + "/0?f=pjson"
    if name == "official_zip":
        return ZIP_URL
    raise ValueError(f"Unknown NOAA Overview capture: {name}")


def _strict_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"NOAA Overview duplicate JSON member: {key}")
        result[key] = value
    return result


def _loads(body: bytes):
    return json.loads(body, object_pairs_hook=_strict_pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"Nonfinite JSON: {value}")))


def _fetch(directory: pathlib.Path, name: str, url: str) -> tuple[dict, bytes]:
    checked_at = dt.datetime.now(dt.timezone.utc).isoformat()
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "HIFLD Next NOAA Overview audit"})
        try:
            response = urllib.request.urlopen(request, timeout=90)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            body = response.read(MAX_BYTES + 1)
            status = response.status
            final_url = response.geturl()
            content_type = response.headers.get("Content-Type")
    except Exception as exc:
        receipt = {"url": url, "checked_at": checked_at, "status": None,
                   "error_type": type(exc).__name__, "reason": str(exc)}
        (directory / f"{name}.json").write_text(json.dumps(receipt, sort_keys=True))
        (directory / f"{name}.body").write_bytes(b"")
        raise
    truncated = len(body) > MAX_BYTES
    body = body[:MAX_BYTES]
    receipt = {"url": url, "final_url": final_url, "checked_at": checked_at,
               "status": status, "content_type": content_type, "bytes": len(body),
               "sha256": hashlib.sha256(body).hexdigest(), "truncated": truncated}
    (directory / f"{name}.body").write_bytes(body)
    (directory / f"{name}.json").write_text(json.dumps(receipt, sort_keys=True))
    if truncated:
        raise ValueError(f"NOAA Overview bounded response truncated: {name}")
    try:
        value = _loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        value = None
    return receipt, body


def _fetch_json_with_retries(directory: pathlib.Path, name: str, url: str,
                             validator, attempts: int = 5) -> tuple[dict, bytes]:
    """Retry transient ArcGIS error JSON while retaining every exact attempt."""
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            receipt, body = _fetch(directory, f"{name}_attempt{attempt}", url)
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            continue
        try:
            parsed = _loads(body)
        except (ValueError, json.JSONDecodeError) as exc:
            last_error = f"invalid JSON: {type(exc).__name__}: {exc}"
            continue
        if receipt.get("status") == 200 and isinstance(parsed, dict) and "error" not in parsed and validator(parsed):
            shutil.copyfile(directory / f"{name}_attempt{attempt}.body", directory / f"{name}.body")
            shutil.copyfile(directory / f"{name}_attempt{attempt}.json", directory / f"{name}.json")
            return receipt, body
        last_error = json.dumps(parsed.get("error") or parsed, sort_keys=True)[:500]
    raise ValueError(f"NOAA Overview {name} witness unavailable after {attempts} attempts: {last_error}")


def capture(directory: pathlib.Path) -> None:
    """Capture independent completeness witnesses and the full bounded layer."""
    directory.mkdir(parents=True, exist_ok=True)
    _fetch(directory, "service", _url("service"))
    # The producer's layer-info route is currently erroring while query routes
    # work. Retain the response as evidence; the full query includes its fields.
    _fetch(directory, "layer", _url("layer"))
    _fetch_json_with_retries(directory, "count", _url("count"), lambda value: type(value.get("count")) is int)
    _fetch_json_with_retries(directory, "ids", _url("ids"), lambda value: isinstance(value.get("objectIds"), list))
    _fetch_json_with_retries(directory, "features", _url("features"), lambda value: isinstance(value.get("features"), list))
    count = _loads((directory / "count.body").read_bytes())["count"]
    _fetch_json_with_retries(directory, "page_end", _url("page_end", count=count), lambda value: isinstance(value.get("features"), list))
    _fetch_json_with_retries(directory, "count_after", _url("count"), lambda value: type(value.get("count")) is int)
    _fetch_json_with_retries(directory, "ids_after", _url("ids"), lambda value: isinstance(value.get("objectIds"), list))


def _local_only(storage: StagingStorageResource) -> None:
    if isinstance(storage, PublishedStorageResource) or not storage.use_local or storage.bucket:
        raise ValueError("NOAA Overview draft adapter requires isolated local candidate storage")


def _write_immutable(storage: StagingStorageResource, outputs: dict[str, bytes]) -> None:
    _local_only(storage)
    for key, body in outputs.items():
        if storage.object_exists(key) and storage.read_key(key) != body:
            raise ValueError("NOAA Overview immutable candidate collision")
    for key, body in outputs.items():
        if not storage.object_exists(key):
            storage.write_key_if_unchanged(key, body, None)


def retain_capture(storage: StagingStorageResource, directory: pathlib.Path,
                   error: Exception | None = None) -> str:
    """Persist bounded raw bodies and receipts, including blocked attempts."""
    _local_only(storage)
    files = {}
    inventory = {}
    for path in sorted(directory.glob("*")):
        if not path.is_file() or path.suffix not in {".body", ".json"}:
            continue
        body = path.read_bytes()[:MAX_BYTES]
        key = f"raw/{path.name}"
        files[key] = body
        inventory[key] = {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                          "original_bytes": path.stat().st_size,
                          "retention_truncated": path.stat().st_size > len(body)}
    digest = hashlib.sha256(json.dumps(inventory, sort_keys=True).encode()).hexdigest()
    prefix = f"{SLUG}/{SLUG}/capture-{digest[:24]}/quarantine"
    files["metadata/capture.json"] = json.dumps({"schema_version": 1, "status": "unvalidated_capture",
        "promotion_allowed": False, "inventory": inventory}, sort_keys=True, indent=2).encode()
    if error is not None:
        failure = {"status": "blocked", "error_type": type(error).__name__, "reason": str(error),
                   "promotion_allowed": False, "complete": False, "capture_prefix": prefix}
        raw = json.dumps(failure, sort_keys=True, indent=2).encode()
        files[f"metadata/failures/{hashlib.sha256(raw).hexdigest()}.json"] = raw
    _write_immutable(storage, {f"{prefix}/{name}": body for name, body in files.items()})
    return prefix


def _checked_body(directory: pathlib.Path, name: str) -> tuple[dict, bytes]:
    body = (directory / f"{name}.body").read_bytes()
    receipt = _loads((directory / f"{name}.json").read_bytes())
    if (receipt.get("status") != 200 or receipt.get("truncated")
            or receipt.get("bytes") != len(body)
            or receipt.get("sha256") != hashlib.sha256(body).hexdigest()):
        raise ValueError(f"NOAA Overview receipt/status/checksum failure: {name}")
    return receipt, body


def _ids(parsed: dict) -> list[int]:
    values = parsed.get("objectIds")
    if not isinstance(values, list) or any(type(value) is not int for value in values):
        raise ValueError("NOAA Overview independent OBJECTID response invalid")
    if len(values) != len(set(values)):
        raise ValueError("NOAA Overview duplicate OBJECTID in ID witness")
    return values


def _field_signature(fields: list[dict]) -> list[tuple]:
    return [(field.get("name"), field.get("type"), field.get("length")) for field in fields]


def _validate_service_edition(service: dict) -> None:
    """Pin the published NOAA edition marker, not merely the layer name."""
    title = service.get("documentInfo", {}).get("Title")
    description = service.get("serviceDescription")
    if title != EXPECTED_SERVICE_TITLE or not isinstance(description, str) or not description.startswith(
        EXPECTED_SERVICE_DESCRIPTION_PREFIX
    ):
        raise ValueError("NOAA Overview published service edition changed; review required")


def _validate_native_crs(value: object, label: str) -> None:
    """Require every declared ArcGIS CRS representation to mean EPSG:4326."""
    if not isinstance(value, dict):
        raise ValueError(f"NOAA Overview {label} CRS missing or invalid")
    codes = []
    for name in ("wkid", "latestWkid"):
        code = value.get(name)
        if code is not None:
            if type(code) is not int or code != 4326:
                raise ValueError(f"NOAA Overview {label} CRS code drift: {name}")
            codes.append(code)
    if len(set(codes)) > 1:
        raise ValueError(f"NOAA Overview {label} CRS representations conflict")
    wkt = value.get("wkt")
    if wkt is not None:
        try:
            matches = CRS.from_wkt(wkt).equals(CRS.from_epsg(4326))
        except Exception as exc:
            raise ValueError(f"NOAA Overview {label} CRS WKT invalid") from exc
        if not matches:
            raise ValueError(f"NOAA Overview {label} CRS WKT conflicts with EPSG:4326")
    if not codes and wkt is None:
        raise ValueError(f"NOAA Overview {label} CRS representation missing")


def validate_snapshot(count: int, ids: list[int], features: dict, page_end: dict,
                      count_after: int, ids_after: list[int], *,
                      expected_count: int = EXPECTED_COUNT,
                      expected_ids_sha256: str = EXPECTED_IDS_SHA256,
                      expected_fields_sha256: str = EXPECTED_FIELDS_SHA256,
                      expected_fields: tuple = EXPECTED_FIELDS) -> list[dict]:
    """Reject incomplete extracts and quarantine any source/schema/coverage drift."""
    rows = features.get("features")
    if type(count) is not int or count <= 0 or count > MAX_RECORD_COUNT:
        raise ValueError("NOAA Overview count outside verified single-page limit")
    if count != expected_count or count_after != count:
        raise ValueError("NOAA Overview feature count changed; review required")
    if len(ids) != count or len(rows) != count or set(ids) != set(ids_after):
        raise ValueError("NOAA Overview count/ID/feature incompleteness")
    if features.get("exceededTransferLimit"):
        raise ValueError("NOAA Overview transfer limit exceeded")
    if page_end.get("features") or page_end.get("exceededTransferLimit"):
        raise ValueError("NOAA Overview terminal page is not empty")
    if hashlib.sha256(json.dumps(sorted(ids), separators=(",", ":")).encode()).hexdigest() != expected_ids_sha256:
        raise ValueError("NOAA Overview source ID roster changed; review required")
    fields = features.get("fields")
    if not isinstance(fields, list):
        raise ValueError("NOAA Overview source fields missing")
    signature = _field_signature(fields)
    if signature != list(expected_fields):
        raise ValueError("NOAA Overview all-column schema or field-order drift")
    if hashlib.sha256(json.dumps(signature, separators=(",", ":")).encode()).hexdigest() != expected_fields_sha256:
        raise ValueError("NOAA Overview physical schema pin mismatch")
    observed = [feature.get("attributes", {}).get("OBJECTID") for feature in rows]
    if any(type(value) is not int for value in observed) or len(set(observed)) != count or set(observed) != set(ids):
        raise ValueError("NOAA Overview feature IDs are missing, duplicated or mismatched")
    if features.get("geometryType") != "esriGeometryPolyline":
        raise ValueError("NOAA Overview geometry type drift")
    _validate_native_crs(features.get("spatialReference"), "feature response")
    for feature in rows:
        attrs, geometry = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or set(attrs) != {field[0] for field in expected_fields}:
            raise ValueError("NOAA Overview source row column drift")
        if not isinstance(geometry, dict) or not isinstance(geometry.get("paths"), list) or not geometry["paths"]:
            raise ValueError("NOAA Overview null/empty source geometry")
        for flag in ("hasZ", "hasM"):
            if flag in geometry and type(geometry[flag]) is not bool:
                raise ValueError(f"NOAA Overview geometry {flag} flag invalid")
            if geometry.get(flag, False):
                raise ValueError(f"NOAA Overview {flag} geometry requires review")
        if "spatialReference" in geometry:
            _validate_native_crs(geometry["spatialReference"], "feature geometry")
        for path in geometry["paths"]:
            if not isinstance(path, list) or len(path) < 2:
                raise ValueError("NOAA Overview malformed path")
            for point in path:
                if (not isinstance(point, list) or len(point) != 2
                        or any(isinstance(v, bool) or not isinstance(v, (int, float))
                               or not math.isfinite(float(v)) for v in point)):
                    raise ValueError("NOAA Overview coordinate is not finite native XY")
    return rows


def _table_and_geometries(rows: list[dict], fields: list[dict]) -> tuple[pa.Table, list]:
    arrays = []
    schema_fields = []
    geometries = []
    types = {"esriFieldTypeOID": pa.int64(), "esriFieldTypeString": pa.string(),
             "esriFieldTypeDate": pa.timestamp("ms", tz="UTC"),
             "esriFieldTypeDouble": pa.float64(), "esriFieldTypeInteger": pa.int32()}
    for field in fields:
        name, kind = field["name"], field["type"]
        if kind not in types:
            raise ValueError(f"NOAA Overview unsupported source field type: {kind}")
        values = [row["attributes"].get(name) for row in rows]
        if kind in ("esriFieldTypeOID", "esriFieldTypeInteger"):
            if any(value is not None and type(value) is not int for value in values):
                raise ValueError(f"NOAA Overview integer source value drift: {name}")
        elif kind == "esriFieldTypeDouble":
            if any(value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)) for value in values):
                raise ValueError(f"NOAA Overview double source value drift: {name}")
            values = [None if value is None else float(value) for value in values]
        elif kind == "esriFieldTypeString" and any(value is not None and not isinstance(value, str) for value in values):
            raise ValueError(f"NOAA Overview string source value drift: {name}")
        elif kind == "esriFieldTypeDate":
            if any(value is not None and type(value) is not int for value in values):
                raise ValueError(f"NOAA Overview date source value drift: {name}")
        converted = pa.array(values, type=types[kind], safe=True)
        if kind != "esriFieldTypeDate" and converted.to_pylist() != values:
            raise ValueError(f"NOAA Overview Arrow conversion changed source values: {name}")
        arrays.append(converted)
        schema_fields.append(pa.field(name, types[kind], nullable=True,
                                      metadata={"esri_field": json.dumps(field, sort_keys=True).encode()}))
    for feature in rows:
        paths = feature["geometry"]["paths"]
        geometries.append(LineString(paths[0]) if len(paths) == 1 else MultiLineString(paths))
    if any(geometry.is_empty or not geometry.is_valid for geometry in geometries):
        raise ValueError("NOAA Overview invalid native line geometry requires review")
    wkb = pa.array([geometry.wkb for geometry in geometries], type=pa.binary())
    arrays.append(wkb)
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo_metadata = {"version": "1.1.0", "primary_column": "geometry", "columns": {
        "geometry": {"encoding": "WKB", "crs": CRS.from_epsg(4326).to_json_dict(),
                     "geometry_types": sorted({geometry.geom_type for geometry in geometries})}}}
    schema = pa.schema(schema_fields, metadata={b"geo": json.dumps(geo_metadata, sort_keys=True).encode()})
    table = pa.Table.from_arrays(arrays, schema=schema)
    return table, geometries


def _processing_identity() -> dict:
    module = pathlib.Path(__file__).resolve()
    packages = sorted((dist.metadata["Name"].lower(), dist.version)
                      for dist in importlib.metadata.distributions() if dist.metadata.get("Name"))
    root = module.parents[2]
    return {"module_sha256": hashlib.sha256(module.read_bytes()).hexdigest(),
            "dependency_inventory": packages, "python": sys.version,
            "project_files": {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
                              for name in ("uv.lock", "pyproject.toml")}}


def stage_candidate(storage: StagingStorageResource, directory: pathlib.Path) -> dict:
    _local_only(storage)
    capture_prefix = retain_capture(storage, directory)
    try:
        return _stage_candidate(storage, directory, capture_prefix)
    except Exception as exc:
        retain_capture(storage, directory, exc)
        raise


def _stage_candidate(storage: StagingStorageResource, directory: pathlib.Path,
                     capture_prefix: str) -> dict:
    _local_only(storage)
    receipts = {}
    bodies = {}
    names = ("service", "count", "ids", "features", "page_end", "count_after", "ids_after")
    for name in names:
        receipts[name], bodies[name] = _checked_body(directory, name)
        expected_url = _url(name, count=EXPECTED_COUNT)
        if receipts[name].get("url") != expected_url or receipts[name].get("final_url") != expected_url:
            raise ValueError(f"NOAA Overview requested/final source URL drift: {name}")
    service, count_data, id_data, feature_data, page_end, after_count, after_ids = (
        _loads(bodies[name]) for name in names)
    _validate_service_edition(service)
    _validate_native_crs(service.get("spatialReference"), "service")
    if not any(layer.get("id") == 0 and layer.get("name") == "Overview" for layer in service.get("layers", [])):
        raise ValueError("NOAA Overview MapServer no longer advertises layer 0 as Overview")
    if "error" in count_data or "error" in id_data or "error" in feature_data or "error" in page_end:
        raise ValueError("NOAA Overview ArcGIS error response")
    if (id_data.get("objectIdFieldName") != "OBJECTID"
            or after_ids.get("objectIdFieldName") != "OBJECTID"):
        raise ValueError("NOAA Overview source identifier name drift")
    rows = validate_snapshot(count_data["count"], _ids(id_data), feature_data, page_end,
                             after_count.get("count"), _ids(after_ids))
    table, geometries = _table_and_geometries(rows, feature_data["fields"])
    input_digest = hashlib.sha256(b"".join(hashlib.sha256(bodies[name]).digest() for name in names)).hexdigest()
    recipe = _processing_identity()
    version_digest = hashlib.sha256((input_digest + json.dumps(recipe, sort_keys=True)).encode()).hexdigest()
    version = "v0.0.0-draft." + version_digest
    prefix = f"{SLUG}/{SLUG}/{version}"
    with tempfile.TemporaryDirectory(prefix="noaa-overview-gpkg-") as tmp:
        gpkg_path = pathlib.Path(tmp) / "overview.gpkg"
        pyogrio.write_arrow(table, gpkg_path, driver="GPKG", layer="overview",
                            geometry_name="geometry", geometry_type="Unknown", crs="EPSG:4326",
                            layer_options={"GEOMETRY_NAME": "geometry", "GEOMETRY_NULLABLE": "NO"})
        # GDAL stamps a wall-clock value into gpkg_contents.last_change. A
        # stable metadata value keeps retries of the same immutable candidate
        # byte-identical, while the true retrieval time remains in provenance.
        with sqlite3.connect(gpkg_path) as connection:
            connection.execute(
                "UPDATE gpkg_contents SET last_change = ? WHERE table_name = ?",
                ("2000-01-01T00:00:00.000Z", "overview"),
            )
        metadata, restored = pyogrio.read_arrow(gpkg_path, layer="overview")
        if metadata.get("crs") != "EPSG:4326" or restored.num_rows != EXPECTED_COUNT:
            raise ValueError("NOAA Overview GeoPackage roundtrip CRS/count mismatch")
        for name in table.column_names:
            if restored.schema.field(name).type != table.schema.field(name).type:
                raise ValueError(f"NOAA Overview GeoPackage physical type drift: {name}")
            if name != "geometry" and restored[name].to_pylist() != table[name].to_pylist():
                raise ValueError(f"NOAA Overview GeoPackage value drift: {name}")
            if name == "geometry" and any(a != b for a, b in zip(table[name].to_pylist(), restored[name].to_pylist())):
                raise ValueError("NOAA Overview GeoPackage geometry bytes changed")
        gpkg = gpkg_path.read_bytes()
    candidate_buffer = io.BytesIO()
    pq.write_table(table, candidate_buffer, compression="zstd")
    parquet = candidate_buffer.getvalue()
    date_values = [row["attributes"]["PUB_DATE"] for row in rows if row["attributes"]["PUB_DATE"] is not None]
    manifest = {"schema_version": 2, "status": "needs_decision", "promotion_allowed": False,
        "complete": True, "feature_count": len(rows), "expected_id_roster_sha256": EXPECTED_IDS_SHA256,
        "source_fields": feature_data["fields"], "candidate_columns": table.column_names,
        "source_native_crs": feature_data["spatialReference"], "candidate_geometry_crs": "EPSG:4326",
        "source_release_date": "2013-09-13", "source_release_date_basis": "Pinned NOAA service marker: documentInfo.Title version 4.1 and serviceDescription '(Version 4.1, 9/13/2013)'; capture time is separate",
        "source_feature_publication_dates_ms": {"minimum": min(date_values), "maximum": max(date_values)},
        "captured_at": receipts["features"]["checked_at"], "candidate_sha256": hashlib.sha256(gpkg).hexdigest(),
        "raw_source_sha256": {name: receipts[name]["sha256"] for name in names},
        "raw_capture_prefix": capture_prefix,
        "comparative_static_snapshot": "Official NOAA shapefile ZIP captured separately; 260 IDs and all line coordinates match exactly. It exposes a different 16-field representation, so it is retained as evidence and not substituted for the 12-field Overview REST layer.",
        "compatibility_status": "unapproved: exact DHS crosswalk row and archived HIFLD baseline bodies unavailable in this runtime",
        "quarantine_reason": "Source/published schema, identifier, field mapping and historical compatibility decisions remain open",
        "derivation_recipe": recipe,
        "unsupported_output_formats": {
            "pmtiles": "Not generated pending projection/wrap and rendering review; source has global 180-degree crossings.",
            "shapefile_zip": "Not generated pending review of source fields with DBF width limits, especially NOTE (1000) and SUPP_INFO (500).",
        },
    }
    inputs = []
    for name in names:
        receipt = receipts[name]
        inputs.append({"input_id": name, "producer": "NOAA National Ocean Service Office of Coast Survey",
            "host": "maritimeboundaries.noaa.gov", "canonical_url": _url(name, count=EXPECTED_COUNT),
            "retrieval_url": receipt["final_url"], "requested_url": receipt["url"],
            "retrieved_at": receipt["checked_at"], "sha256": receipt["sha256"], "bytes": receipt["bytes"],
            "http_status": receipt["status"], "role": "producer_publication",
            "release_date": "2013-09-13" if name == "service" else None,
            "release_date_unknown_reason": None if name == "service" else "Retrieval time is not a producer release date; no per-response release field is published.",
            "license": "Unresolved; InPort lists no access constraints but this does not establish a reuse license.",
            "attribution": "NOAA/NOS/Office of Coast Survey", "source_accepted": False,
            "raw_key": f"{prefix}/raw/{name}.body", "receipt_key": f"{prefix}/raw/{name}.json"})
    provenance = {"schema_version": 1, "processor": "HIFLD Next", "inputs": inputs,
        "code_commit": "installed-sha256:" + recipe["module_sha256"],
        "transformations": ["ArcGIS JSON path coordinates mapped to GeoPackage LineString/MultiLineString with coordinate sequences unchanged",
                            "ArcGIS Date millisecond values mapped to nullable UTC timestamp[ms] fields",
                            "ArcGIS OBJECTID retained as int64; all returned attributes retained without trimming or alias renaming"],
        "lineage": {"service": SERVICE, "layer": 0, "name": "Overview", "capture_prefix": capture_prefix},
        "promotion_allowed": False, "source_accepted": False, "approved_contract": None, "baseline": None,
        "unsupported_output_formats": manifest["unsupported_output_formats"]}
    validate_provenance(provenance)
    outputs = {"geopackage/overview.gpkg": gpkg,
        "candidate/overview.parquet": parquet,
        "metadata/provenance.json": json.dumps(manifest, sort_keys=True, indent=2).encode(),
        "metadata/source_manifest.json": json.dumps(provenance, sort_keys=True, indent=2).encode()}
    for name in names:
        outputs[f"raw/{name}.body"] = bodies[name]
        outputs[f"raw/{name}.json"] = (directory / f"{name}.json").read_bytes()
    _write_immutable(storage, {f"{prefix}/{name}": body for name, body in outputs.items()})
    return {"dataset_slug": SLUG, "file_slug": SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "staging_prefix": prefix,
            "candidate_sha256": hashlib.sha256(gpkg).hexdigest(), "promotion_allowed": False}


def ingest_candidate(storage: StagingStorageResource,
                     capture_dir: pathlib.Path | None = None) -> dict:
    _local_only(storage)
    if capture_dir is not None:
        return stage_candidate(storage, capture_dir)
    with tempfile.TemporaryDirectory(prefix="noaa-overview-capture-") as tmp:
        directory = pathlib.Path(tmp)
        try:
            capture(directory)
            return stage_candidate(storage, directory)
        except Exception as exc:
            retain_capture(storage, directory, exc)
            raise
