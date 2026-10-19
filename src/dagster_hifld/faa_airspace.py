"""FAA full-extraction draft; exact inputs and typed candidates stay quarantined.

The native Esri response is the geometry source. The server's separate GeoJSON
export is retained as diagnostic evidence, never used to repair native geometry.
No source, baseline, mapping, release or production decision is made here.
"""
from __future__ import annotations

import collections
import datetime
import hashlib
import http.client
import io
import json
import math
import pathlib
import sqlite3
import tempfile
import urllib.error
import urllib.request

import fiona
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from pyproj import CRS
from shapely import from_wkb
from fiona.io import MemoryFile
from shapely.geometry import mapping, shape

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

SLUG = "airspace-boundaries-1"
ITEM_URL = "https://www.arcgis.com/sharing/rest/content/items/67885972e4e940b2aa6d74024901c561?f=pjson"
LAYER_URL = "https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/Boundary_Airspace/FeatureServer/0"
MAX_BYTES = 100 * 1024 * 1024
NAMES = ("item", "layer", "count", "ids", "source-native", "source-geojson", "page-end")
UNSUPPORTED_OUTPUT_FORMATS = {
    "pmtiles": (
        "Native EPSG:4269 rings are intentionally unwrapped, with observed "
        "longitude bounds -200.992180556 to 191.02666667 and latitude reaching90. "
        "The actual PMTiles converter rejects out-of-range longitude/latitude "
        "after CRS conversion. No antimeridian split/wrap or polar clipping "
        "mapping has been accepted, so a rendered substitute is not generated."
    ),
    "shapefile_zip": (
        "Native Shape__Area (11 characters), Shape__Length (13), and retained "
        "__source_geojson_feature_id exceed Shapefile's 10-character DBF field "
        "limit. The historical candidate already truncates these shape fields "
        "to Shape__Are/Shape__Len. Exact names and explicit numeric physical "
        "types have no approved destructive Shapefile mapping."
    )
}


def _json_members(pairs):
    parsed = {}
    for key, value in pairs:
        if key in parsed:
            raise ValueError(f"FAA duplicate JSON member: {key}")
        parsed[key] = value
    return parsed


def _reject_json_constant(value):
    raise ValueError(f"FAA nonfinite JSON constant: {value}")


def _load_json(body):
    """Reject ambiguous source/receipt objects before any identifier can collapse."""
    return json.loads(body, object_pairs_hook=_json_members, parse_constant=_reject_json_constant)


def _local_only(storage: StagingStorageResource) -> None:
    if isinstance(storage, PublishedStorageResource) or not storage.use_local or storage.bucket:
        raise ValueError("Draft FAA adapter requires isolated local candidate storage")


def _write_immutable(storage: StagingStorageResource, outputs: dict[str, bytes]) -> None:
    """Preflight the complete set; identical reruns do not replace evidence."""
    _local_only(storage)
    for key, body in outputs.items():
        if storage.object_exists(key) and storage.read_key(key) != body:
            raise ValueError("FAA immutable candidate collision")
    for key, body in outputs.items():
        if not storage.object_exists(key):
            storage.write_key_if_unchanged(key, body, None)


def retain_capture(storage: StagingStorageResource, directory: pathlib.Path,
                   error: Exception | None = None) -> str:
    """Retain exact bounded full/partial captures before validation can reject.

    Receipts survive even when a response is HTTP/ArcGIS error JSON, malformed,
    truncated, missing or transport-interrupted. Evidence is never canonical data.
    """
    _local_only(storage)
    files = {}
    inventory = {}
    for name in NAMES:
        for suffix in ("body", "json"):
            path = directory / f"{name}.{suffix}"
            if not path.is_file():
                continue
            with path.open("rb") as stream:
                body = stream.read(MAX_BYTES)
            key = f"raw/{path.name}"
            files[key] = body
            inventory[key] = {
                "captured_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                "input_bytes": path.stat().st_size,
                "retention_truncated": path.stat().st_size > len(body),
            }
    digest = hashlib.sha256(json.dumps(inventory, sort_keys=True).encode()).hexdigest()
    prefix = f"{SLUG}/{SLUG}/capture-{digest[:24]}/quarantine"
    files["metadata/capture.json"] = json.dumps({
        "schema_version": 1, "status": "unvalidated_capture", "inventory": inventory,
        "promotion_allowed": False,
    }, sort_keys=True, indent=2).encode()
    if error is not None:
        failure = json.dumps({
            "status": "blocked", "error_type": type(error).__name__,
            "reason": str(error), "promotion_allowed": False,
            "complete": False, "capture_prefix": prefix,
        }, sort_keys=True, indent=2).encode()
        files[f"metadata/failures/{hashlib.sha256(failure).hexdigest()}.json"] = failure
    _write_immutable(storage, {f"{prefix}/{name}": body for name, body in files.items()})
    return prefix


def _fetch(directory: pathlib.Path, name: str, url: str) -> dict:
    """Persist a receipt and bounded body before interpreting an upstream reply."""
    checked_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    try:
        try:
            response = urllib.request.urlopen(url, timeout=60)
        except urllib.error.HTTPError as exc:
            response = exc
    except Exception as exc:
        (directory / f"{name}.json").write_text(json.dumps({
            "url": url, "checked_at": checked_at, "status": None,
            "error_type": type(exc).__name__, "reason": str(exc),
        }, sort_keys=True))
        raise
    read_error = None
    chunks = []
    received = 0
    with response:
        while received <= MAX_BYTES:
            try:
                chunk = response.read(min(1024 * 1024, MAX_BYTES + 1 - received))
            except http.client.IncompleteRead as exc:
                chunks.append(exc.partial[:MAX_BYTES + 1 - received])
                read_error = str(exc)
                break
            except Exception as exc:
                read_error = f"{type(exc).__name__}: {exc}"
                break
            if not chunk:
                break
            chunks.append(chunk)
            received += len(chunk)
        body = b"".join(chunks)
        truncated = len(body) > MAX_BYTES
        body = body[:MAX_BYTES]
        metadata = {
            "url": url, "status": response.status, "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(), "checked_at": checked_at,
            "truncated": truncated, "read_error": read_error,
        }
    (directory / f"{name}.body").write_bytes(body)
    (directory / f"{name}.json").write_text(json.dumps(metadata, sort_keys=True))
    if truncated or read_error:
        raise ValueError(f"FAA incomplete bounded response: {name}")
    parsed = _load_json(body)
    if metadata["status"] != 200 or "error" in parsed:
        raise ValueError(f"FAA upstream failure: {name}")
    return parsed


def capture(directory: pathlib.Path) -> None:
    """Anonymous complete snapshot; larger future scopes fail closed for review."""
    directory.mkdir(parents=True, exist_ok=True)
    urls = {
        "item": ITEM_URL, "layer": LAYER_URL + "?f=pjson",
        "count": LAYER_URL + "/query?where=1%3D1&returnCountOnly=true&f=pjson",
        "ids": LAYER_URL + "/query?where=1%3D1&returnIdsOnly=true&f=pjson",
    }
    for name, url in urls.items():
        _fetch(directory, name, url)
    count = _load_json((directory / "count.body").read_bytes())["count"]
    layer = _load_json((directory / "layer.body").read_bytes())
    if type(count) is not int or not 0 < count <= min(layer["maxRecordCount"], 2000):
        raise ValueError("FAA count exceeds verified single-page scope; pagination adapter review required")
    base = LAYER_URL + "/query?where=1%3D1&outFields=%2A&returnGeometry=true&orderByFields=OBJECTID&resultOffset=0&resultRecordCount=2000"
    _fetch(directory, "source-native", base + "&f=pjson")
    _fetch(directory, "source-geojson", base + "&f=geojson&outSR=4326")
    _fetch(directory, "page-end", LAYER_URL + f"/query?where=1%3D1&outFields=OBJECTID&orderByFields=OBJECTID&resultOffset={count}&resultRecordCount=2000&f=pjson")


def ingest_candidate(storage: StagingStorageResource,
                     capture_dir: pathlib.Path | None = None) -> dict:
    """Default live wrapper guarantees durable evidence before temp cleanup."""
    _local_only(storage)
    if capture_dir is not None:
        return stage_candidate(storage, capture_dir)
    with tempfile.TemporaryDirectory(prefix="faa-airspace-capture-") as tmp:
        directory = pathlib.Path(tmp)
        try:
            capture(directory)
            return stage_candidate(storage, directory)
        except Exception as exc:
            retain_capture(storage, directory, exc)
            raise


def _strict_int64(values: list, label: str) -> pa.Array:
    if any(type(v) is not int or not -(2**63) <= v < 2**63 for v in values):
        raise ValueError(f"FAA {label} requires nonnull exact int64 values")
    if len(set(values)) != len(values):
        raise ValueError(f"FAA duplicate {label}")
    converted = pa.array(values, type=pa.int64(), safe=True)
    if converted.to_pylist() != values:
        raise ValueError(f"FAA {label} int64 roundtrip loss")
    return converted


def _ring_counter(rings) -> collections.Counter:
    return collections.Counter(tuple(tuple(point) for point in ring) for ring in rings)


def native_geometries(body: bytes, rows: list[dict], count: int) -> dict:
    """Interpret Esri shell/hole semantics without changing a source coordinate.

    Fiona is used only for native geometry assembly and OID correspondence.
    Original JSON attributes feed Arrow independently, avoiding GDAL field casts.
    """
    parsed = _load_json(body)
    if parsed["spatialReference"].get("latestWkid", parsed["spatialReference"].get("wkid")) != 4269:
        raise ValueError("FAA native CRS changed from verified EPSG:4269")
    originals = {f["attributes"]["OBJECTID"]: f["geometry"] for f in parsed["features"]}
    geometries = {}
    with MemoryFile(body, ext=".json") as memory:
        with memory.open(driver="ESRIJSON") as collection:
            if collection.crs.to_epsg() != 4269:
                raise ValueError("FAA native GDAL CRS mismatch")
            for feature in collection:
                oid = feature.properties["OBJECTID"]
                if oid in geometries or feature.geometry is None:
                    raise ValueError("FAA native GDAL null/duplicate geometry identifier")
                geometry = shape(feature.geometry)
                if geometry.geom_type not in ("Polygon", "MultiPolygon"):
                    raise ValueError("FAA native geometry type changed")
                if geometry.is_empty or not geometry.is_valid:
                    raise ValueError("FAA invalid native geometry requires source review")
                assembled = mapping(geometry)
                rings = assembled["coordinates"] if assembled["type"] == "Polygon" else [r for p in assembled["coordinates"] for r in p]
                if _ring_counter(rings) != _ring_counter(originals[oid]["rings"]):
                    raise ValueError("FAA native geometry assembly changed ring coordinates")
                geometries[oid] = geometry
    if len(geometries) != count or set(geometries) != {r["OBJECTID"] for r in rows}:
        raise ValueError("FAA native GDAL geometry completeness mismatch")
    return geometries


def stage_candidate(storage: StagingStorageResource, directory: pathlib.Path) -> dict:
    """Capture first; checksum/type/ID/geometry failures remain durable quarantine."""
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
    bodies, metadata = {}, {}
    for name in NAMES:
        body = (directory / f"{name}.body").read_bytes()
        receipt = _load_json((directory / f"{name}.json").read_text())
        if (len(body) > MAX_BYTES or len(body) != receipt["bytes"]
                or hashlib.sha256(body).hexdigest() != receipt["sha256"]
                or receipt["status"] != 200 or receipt.get("truncated") or receipt.get("read_error")):
            raise ValueError(f"FAA checksum/status mismatch: {name}")
        parsed = _load_json(body)
        if "error" in parsed:
            raise ValueError(f"FAA error response: {name}")
        bodies[name], metadata[name] = body, receipt
    layer, native, geo = (_load_json(bodies[n]) for n in ("layer", "source-native", "source-geojson"))
    fields = layer["fields"]
    count = _load_json(bodies["count"])["count"]
    ids = _load_json(bodies["ids"])["objectIds"]
    if type(count) is not int or not 0 < count <= 2000:
        raise ValueError("FAA count outside verified bounded extraction scope")
    rows = [feature["attributes"] for feature in native["features"]]
    features = geo["features"]
    if not rows or len(rows) != count or len(features) != count or len(ids) != count:
        raise ValueError("FAA incomplete ID response")
    _strict_int64(ids, "independent OBJECTID")
    _strict_int64([r.get("OBJECTID") for r in rows], "integer source type OBJECTID")
    _strict_int64([f.get("properties", {}).get("OBJECTID") for f in features], "GeoJSON OBJECTID")
    _strict_int64([f.get("id") for f in features], "GeoJSON Feature.id")
    if native.get("exceededTransferLimit") or geo.get("exceededTransferLimit") or _load_json(bodies["page-end"])["features"]:
        raise ValueError("FAA incomplete pagination")
    if {r["OBJECTID"] for r in rows} != set(ids) or {f["properties"]["OBJECTID"] for f in features} != set(ids):
        raise ValueError("FAA independent ID set mismatch")
    if len({f["name"] for f in fields}) != len(fields) or any(set(r) != {f["name"] for f in fields} for r in rows):
        raise ValueError("FAA all-column schema mismatch")
    by_id = {feature["properties"]["OBJECTID"]: feature for feature in features}
    if any(r != by_id[r["OBJECTID"]]["properties"] for r in rows):
        raise ValueError("FAA native/GeoJSON attribute mismatch")
    required = {"OBJECTID": "esriFieldTypeOID", "GLOBAL_ID": "esriFieldTypeString", "GlobalID": "esriFieldTypeGlobalID"}
    declared = {field["name"]: field["type"] for field in fields}
    if {field["name"]: field["type"] for field in native["fields"]} != declared:
        raise ValueError("FAA native/layer declared physical schema drift")
    if any(declared.get(key) != value for key, value in required.items()):
        raise ValueError("FAA identifier name/type drift")
    for key in required:
        values = [r[key] for r in rows]
        if any(v is None for v in values) or len(set(values)) != count:
            raise ValueError(f"FAA required identifier null/duplicate: {key}")
    types = {"esriFieldTypeOID": pa.int64(), "esriFieldTypeString": pa.string(),
             "esriFieldTypeGlobalID": pa.string(), "esriFieldTypeDouble": pa.float64(),
             "esriFieldTypeSmallInteger": pa.int16()}
    arrays = {}
    for field in fields:
        values = [r[field["name"]] for r in rows]
        kind = field["type"]
        if kind not in types:
            raise ValueError("FAA unsupported source physical type")
        if not field["nullable"] and any(v is None for v in values):
            raise ValueError("FAA required column null")
        expected = str if kind in ("esriFieldTypeString", "esriFieldTypeGlobalID") else (int, float)
        if any(v is not None and (isinstance(v, bool) or not isinstance(v, expected)) for v in values):
            raise ValueError("FAA source value/type drift")
        if kind in ("esriFieldTypeOID", "esriFieldTypeSmallInteger") and any(v is not None and type(v) is not int for v in values):
            raise ValueError("FAA integer source type drift")
        if any(isinstance(v, float) and not math.isfinite(v) for v in values):
            raise ValueError("FAA nonfinite numeric value")
        converted = pa.array(values, type=types[kind], safe=True)
        if any(before != after for before, after in zip(values, converted.to_pylist())):
            raise ValueError("FAA Arrow conversion would lose source values")
        arrays[field["name"]] = converted
    geometries_by_id = native_geometries(bodies["source-native"], rows, count)
    geometries = [geometries_by_id[r["OBJECTID"]] for r in rows]
    diagnostic_geometries = [shape(by_id[r["OBJECTID"]]["geometry"]) for r in rows]
    if "geometry" in arrays or "__source_geojson_feature_id" in arrays:
        raise ValueError("FAA candidate reserved column collision")
    arrays["geometry"] = pa.array([g.wkb for g in geometries], type=pa.binary())
    # Reorder by actual OBJECTID correspondence; retain the original Feature.id.
    arrays["__source_geojson_feature_id"] = _strict_int64(
        [by_id[r["OBJECTID"]]["id"] for r in rows], "GeoJSON Feature.id")
    schema = pa.schema([
        pa.field(field["name"], types[field["type"]], nullable=field["nullable"],
                 metadata={"esri_field": json.dumps(field, sort_keys=True)}) for field in fields
    ] + [pa.field("geometry", pa.binary(), nullable=False),
         pa.field("__source_geojson_feature_id", pa.int64(), nullable=False)])
    geo_metadata = {"version": "1.1.0", "primary_column": "geometry", "columns": {
        "geometry": {"encoding": "WKB", "crs": CRS.from_epsg(4269).to_json_dict(),
                     "geometry_types": sorted({g.geom_type for g in geometries})}}}
    schema = schema.with_metadata({b"geo": json.dumps(geo_metadata, sort_keys=True).encode()})
    table = pa.Table.from_arrays(list(arrays.values()), schema=schema)
    stream = io.BytesIO()
    pq.write_table(table, stream, compression="zstd")
    candidate = stream.getvalue()
    derived = {"type": "FeatureCollection", "crs": {"type": "name", "properties": {"name": "EPSG:4269"}},
               "features": [{"type": "Feature", "id": by_id[r["OBJECTID"]]["id"],
                             "properties": {name: arrays[name][i].as_py() for name in arrays if name != "geometry"},
                             "geometry": mapping(geometries_by_id[r["OBJECTID"]])} for i, r in enumerate(rows)]}
    candidate_geojson = json.dumps(derived, separators=(",", ":"), allow_nan=False).encode()
    recipe = {"adapter_source_sha256": hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),
              "fiona": fiona.__version__, "gdal": fiona.__gdal_version__, "pyarrow": pa.__version__, "pyogrio": pyogrio.__version__, "pyogrio_gdal": pyogrio.__gdal_version__}
    digest = hashlib.sha256(b"".join(bodies[n] for n in NAMES)
                            + json.dumps(metadata, sort_keys=True).encode()
                            + json.dumps(recipe, sort_keys=True).encode()).hexdigest()
    # Adapter version separates revised derivations while historical candidates survive.
    version = "candidate-native-v2-" + digest[:24]
    prefix = f"{SLUG}/{SLUG}/{version}/quarantine"
    transformations = [
        "GDAL ESRIJSON shell/hole assembly only; every native ring/coordinate sequence reconciled unchanged",
        "Original native EPSG:4269 retained; no reprojection or geometry repair",
        "Original JSON attributes to explicit Esri physical Arrow types/nullability; all columns retained",
        "Original validated GeoJSON Feature.id retained as __source_geojson_feature_id (unapproved mapping)",
    ]
    # Canonical source container uses the explicit Arrow widths, not GeoJSON
    # width inference. Its full roundtrip is checked before any candidate write.
    with tempfile.TemporaryDirectory(prefix="faa-native-gpkg-") as tmp:
        gpkg_path = pathlib.Path(tmp) / "airspace.gpkg"
        pyogrio.write_arrow(table, gpkg_path, driver="GPKG", layer="airspace",
                            geometry_name="geometry", geometry_type="Unknown", crs="EPSG:4269",
                            layer_options={"GEOMETRY_NAME": "geometry", "GEOMETRY_NULLABLE": "NO"})
        # Container change time is the pinned fetch time, never source release time.
        observed = metadata["source-native"].get("checked_at", "2000-01-01T00:00:00+00:00")
        changed = datetime.datetime.fromisoformat(observed).astimezone(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        with sqlite3.connect(gpkg_path) as database:
            database.execute("UPDATE gpkg_contents SET last_change = ?", (changed,))
        gpkg_metadata, restored = pyogrio.read_arrow(gpkg_path)
        if gpkg_metadata["crs"] != "EPSG:4269" or restored.num_rows != count:
            raise ValueError("FAA native GeoPackage CRS/count roundtrip loss")
        for name in arrays:
            if name == "geometry":
                if any(not before.equals_exact(from_wkb(after), 0, normalize=True)
                       for before, after in zip(geometries, restored[name].to_pylist())):
                    raise ValueError("FAA native GeoPackage geometry roundtrip loss")
            elif (name not in restored.column_names or restored.schema.field(name).type != table.schema.field(name).type
                  or restored.schema.field(name).nullable != table.schema.field(name).nullable
                  or restored[name].to_pylist() != table[name].to_pylist()):
                raise ValueError(f"FAA native GeoPackage column/type/value roundtrip loss: {name}")
        gpkg = gpkg_path.read_bytes()
    manifest = {
        "schema_version": 2, "status": "needs_decision", "promotion_allowed": False,
        "feature_count": count, "source_fields": fields, "candidate_columns": table.column_names,
        "source_native_crs": native["spatialReference"], "candidate_geometry_crs": "EPSG:4269",
        "transformations": transformations, "candidate_sha256": hashlib.sha256(candidate).hexdigest(),
        "candidate_geojson_sha256": hashlib.sha256(candidate_geojson).hexdigest(),
        "candidate_geopackage_sha256": hashlib.sha256(gpkg).hexdigest(),
        "geopackage_roundtrip": "Every source/candidate property name, Arrow physical type and value; all native geometries/CRS/count verified",
        "captures": metadata, "durable_capture_prefix": capture_prefix,
        "publisher": _load_json(bodies["item"])["owner"],
        "release_description": _load_json(bodies["item"])["description"],
        "cadence": "56 days per producer item; refresh deployment not enabled",
        "baseline": None, "approved_contract": None, "complete": True,
        "geometry_types": dict(collections.Counter(g.geom_type for g in geometries)),
        "coverage_TYPE_CODE": dict(collections.Counter(str(r.get("TYPE_CODE")) for r in rows)),
        "bounds": [min(g.bounds[0] for g in geometries), min(g.bounds[1] for g in geometries),
                   max(g.bounds[2] for g in geometries), max(g.bounds[3] for g in geometries)],
        "invalid_geometry_count": 0, "empty_geometry_count": 0,
        "diagnostic_server_geojson_crs": "EPSG:4326",
        "diagnostic_server_geojson_invalid_geometry_count": sum(not g.is_valid for g in diagnostic_geometries),
        "unsupported_output_formats": UNSUPPORTED_OUTPUT_FORMATS,
        "quarantine_reason": "Unapproved baseline, all-column ID contract and changed scope; shared #417 exact acceptance required",
        "derivation_recipe": recipe,
    }
    outputs = {**{f"raw/{name}.json": body for name, body in bodies.items()},
               "candidate/airspace.parquet": candidate,
               "candidate/airspace-native.geojson": candidate_geojson,
               "candidate/airspace-native.gpkg": gpkg,
               "metadata/provenance.json": json.dumps(manifest, sort_keys=True, indent=2).encode()}
    _write_immutable(storage, {f"{prefix}/{name}": body for name, body in outputs.items()})
    return {"version": version, "feature_count": count, "quarantine_prefix": prefix,
            "candidate_sha256": manifest["candidate_sha256"], "promotion_allowed": False}
