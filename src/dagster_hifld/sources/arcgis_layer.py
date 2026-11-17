"""Shared ArcGIS FeatureServer/MapServer capture, replay and encoding primitives.

One canonical copy of the checkpoint load/save, retained-capture read, refusal
witness, failed-attempt evidence and bounded-retry mechanics shared by the
segmented NHD/3DHP adapters, plus the paged capture, replay, candidate-identity
digest, budget-guarded fetch with bounded retry, Esri polyline/polygon WKB
encoding and GeoParquet serialization the ArcGIS dataset adapters share. Every
body is byte-identical to the copy it replaces; per-dataset byte budgets and
error types are passed explicitly by each caller. Canonical JSON, sha256 and
the ArcGIS CRS drift guard delegate to the single implementations in
``sources.esri_common`` and ``sources.esri_geometry``. Fetch planning,
roster/offset algorithms, checkpoint state layouts, schema, ID and geometry
contracts stay local to each adapter.

The private aliases at the bottom keep the historical import names working so
each dataset module only changes its import line; new modules should use the
public names.
"""
from __future__ import annotations

import hashlib
import json
import struct
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Mapping
from urllib.parse import urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import LinearRing, Polygon

from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.sources.capture_common import capture_get, write_immutable
from dagster_hifld.sources.esri_common import arrow_type, canonical as _canonical, sha as _sha
from dagster_hifld.sources.esri_geometry import _validate_spatial_reference

PAGE_MAX_BYTES = 8_000_000


def capture_page(client: httpx.Client, url: str, root: Path, name: str, *,
                 page_max_bytes: int = PAGE_MAX_BYTES,
                 capture_max_bytes: int = 192_000_000) -> tuple[bytes, dict]:
    retained=sum(path.stat().st_size for suffix in ("*.body","*.wire") for path in root.glob(suffix) if path.is_file())
    if retained + 2 * page_max_bytes > capture_max_bytes:
        raise ValueError("source_capture_budget_exceeded_before_request")
    body, receipt = capture_get(client, url, root, name, max_bytes=page_max_bytes)
    return body, receipt


def load_capture(root: Path, name: str) -> tuple[bytes, bytes, dict]:
    body_path, wire_path, receipt_path = (root / f"{name}.body", root / f"{name}.wire",
                                          root / f"{name}.receipt.json")
    if any(path.is_symlink() for path in (body_path, wire_path, receipt_path)):
        raise ValueError("retained_capture_symlink_rejected")
    body, wire, receipt = body_path.read_bytes(), wire_path.read_bytes(), json.loads(receipt_path.read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("retained_capture_receipt_invalid:" + name)
    return body, wire, receipt


def capture_identity(captures: dict[str, tuple[bytes, dict]], root: Path) -> dict:
    # Candidate identity binds every decoded and wire artifact. Receipt JSON is
    # retained as provenance but its observation time does not change content identity.
    digests={}
    for name in sorted(captures):
        _body, _receipt = captures[name]
        _body2, wire, _receipt2 = load_capture(root, name)
        digests[name]={"decoded_sha256":_sha(_body),"wire_sha256":_sha(wire)}
    return digests


def esri_polyline_wkb(geometry: dict) -> bytes:
    paths = geometry["paths"]
    out = bytearray(struct.pack("<BI", 1, 5))  # little-endian MultiLineString
    out.extend(struct.pack("<I", len(paths)))
    for path in paths:
        out.extend(struct.pack("<BI", 1, 2))  # nested LineString
        out.extend(struct.pack("<I", len(path)))
        for point in path:
            out.extend(struct.pack("<dd", float(point[0]), float(point[1])))
    return bytes(out)


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID":pa.int64(), "esriFieldTypeInteger":pa.int32(),
            "esriFieldTypeString":pa.string()}[field["type"]]


def polyline_parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    properties = [field for field in fields if field["name"] != "SHAPE"]
    arrays = [pa.array([row["attributes"][field["name"]] for row in features], type=_arrow_type(field))
              for field in properties]
    arrays.append(pa.array([esri_polyline_wkb(row["geometry"]) for row in features], type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=[field["name"] for field in properties] + ["geometry"])
    crs = {"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json", "type":"GeographicCRS", "name":"WGS 84",
           "datum":{"type":"GeodeticReferenceFrame","name":"World Geodetic System 1984",
                    "ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563},
                    "id":{"authority":"EPSG","code":6326}},
           "coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"},
                                                                        {"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"}]},
           "id":{"authority":"EPSG","code":4326}}
    metadata={b"geo":_canonical({"version":"1.1.0","primary_column":"geometry",
                                  "columns":{"geometry":{"encoding":"WKB","geometry_types":["MultiLineString"],"crs":crs}}})}
    import io
    sink=io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata),sink,compression="zstd",use_dictionary=False)
    return sink.getvalue()


def esri_polygon(geometry: dict) -> bytes:
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    has_z, has_m = geometry.get("hasZ", False), geometry.get("hasM", False)
    if type(has_z) is not bool or type(has_m) is not bool:
        raise ValueError("source_ordinate_flags_invalid")
    shells, holes = [], []
    dimension = None
    for coords in rings:
        if not isinstance(coords, list) or len(coords) < 4:
            raise ValueError("source_polygon_ring_invalid")
        if coords[0] != coords[-1]:
            raise ValueError("source_polygon_ring_not_closed")
        if any(not isinstance(point, list) for point in coords):
            raise ValueError("source_polygon_coordinate_invalid")
        dims = {len(point) for point in coords}
        if len(dims) != 1:
            raise ValueError("source_polygon_ordinate_count_drift")
        point_dims = next(iter(dims))
        if dimension is None:
            dimension = point_dims
        if dimension != point_dims:
            raise ValueError("source_polygon_ordinate_count_drift")
        expected_dims = 2 + int(has_z) + int(has_m)
        if point_dims != expected_dims:
            raise ValueError("source_polygon_zm_flags_do_not_match_coordinates")
        xy = [point[:2] for point in coords]
        ring = LinearRing(xy)
        (holes if ring.is_ccw else shells).append([list(point) for point in coords])
    # ArcGIS encodes exterior rings clockwise and interior rings counter-clockwise.
    # Reject non-standard winding rather than repairing or guessing.
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    shell_polys = [Polygon([point[:2] for point in ring]) for ring in shells]
    assignments = [[] for _ in shell_polys]
    for ring in holes:
        hole = Polygon([point[:2] for point in ring])
        owners = [i for i, shell in enumerate(shell_polys) if shell.contains(hole.representative_point())]
        if len(owners) != 1:
            raise ValueError("source_hole_ownership_ambiguous")
        assignments[owners[0]].append(ring)
    dim_code = {2: 0, 3: (1000 if has_z else 2000), 4: 3000}[dimension]

    def polygon_wkb(shell, hole_rings):
        out = bytearray(struct.pack("<BI", 1, dim_code + 3))
        out.extend(struct.pack("<I", 1 + len(hole_rings)))
        for ring in [shell, *hole_rings]:
            out.extend(struct.pack("<I", len(ring)))
            for point in ring:
                out.extend(struct.pack("<" + "d" * dimension, *point))
        return bytes(out)

    polygons = [polygon_wkb(shell, assignments[i]) for i, shell in enumerate(shells)]
    if len(polygons) == 1:
        return polygons[0]
    multi = bytearray(struct.pack("<BI", 1, dim_code + 6))
    multi.extend(struct.pack("<I", len(polygons)))
    for polygon in polygons:
        multi.extend(polygon)
    return bytes(multi)


def esri_polygon_wkb(geometry: dict) -> bytes:
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    has_z, has_m = geometry.get("hasZ", False), geometry.get("hasM", False)
    if type(has_z) is not bool or type(has_m) is not bool:
        raise ValueError("source_ordinate_flags_invalid")
    if has_z or has_m:
        # The pinned layer contract is XY-only; correctly flagged dimensional
        # geometry is source drift and must fail closed, never be sealed.
        raise ValueError("source_polygon_dimensional_flags_rejected")
    shells, holes = [], []
    for coords in rings:
        if not isinstance(coords, list) or len(coords) < 4:
            raise ValueError("source_polygon_ring_invalid")
        if coords[0] != coords[-1]:
            raise ValueError("source_polygon_ring_not_closed")
        if any(not isinstance(point, list) for point in coords):
            raise ValueError("source_polygon_coordinate_invalid")
        dims = {len(point) for point in coords}
        if len(dims) != 1:
            raise ValueError("source_polygon_ordinate_count_drift")
        if next(iter(dims)) != 2:
            raise ValueError("source_polygon_zm_flags_do_not_match_coordinates")
        ring = LinearRing(coords)
        (holes if ring.is_ccw else shells).append([list(point) for point in coords])
    # ArcGIS encodes exterior rings clockwise and interior rings counter-clockwise.
    # Reject non-standard winding rather than repairing or guessing.
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    shell_polys = [Polygon([point[:2] for point in ring]) for ring in shells]
    assignments = [[] for _ in shell_polys]
    for ring in holes:
        hole = Polygon([point[:2] for point in ring])
        owners = [i for i, shell in enumerate(shell_polys) if shell.contains(hole.representative_point())]
        if len(owners) != 1:
            raise ValueError("source_hole_ownership_ambiguous")
        assignments[owners[0]].append(ring)

    def polygon_wkb(shell, hole_rings):
        out = bytearray(struct.pack("<BI", 1, 3))
        out.extend(struct.pack("<I", 1 + len(hole_rings)))
        for ring in [shell, *hole_rings]:
            out.extend(struct.pack("<I", len(ring)))
            for point in ring:
                out.extend(struct.pack("<dd", *point))
        return bytes(out)

    polygons = [polygon_wkb(shells[i], assignments[i]) for i in range(len(shells))]
    if len(polygons) == 1:
        return polygons[0]
    multi = bytearray(struct.pack("<BI", 1, 6))
    multi.extend(struct.pack("<I", len(polygons)))
    for polygon in polygons:
        multi.extend(polygon)
    return bytes(multi)


def layer_parquet_bytes(fields: list[dict], features: list[dict], wkid: int) -> bytes:
    properties = [f for f in fields if f["name"] != "SHAPE"]
    arrays = []
    for field in properties:
        name, dtype = field["name"], arrow_type(field)
        values = [feature["attributes"][name] for feature in features]
        arrays.append(pa.array(values, type=dtype))
    geometries = [esri_polygon(x["geometry"]) for x in features]
    arrays.append(pa.array(geometries, type=pa.binary()))
    names = [x["name"] for x in properties] + ["geometry"]
    table = pa.Table.from_arrays(arrays, names=names)
    crs = {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS",
           "name": "WGS 84", "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
           "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
           "id": {"authority": "EPSG", "code": 6326}},
           "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"}, {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
           "id": {"authority": "EPSG", "code": wkid}}
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["MultiPolygon", "Polygon"], "crs": crs}}})}
    import io

    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def load_replay(root: Path, name: str) -> tuple[bytes, dict]:
    paths = [root / f"{name}.body", root / f"{name}.wire", root / f"{name}.receipt.json"]
    if any(path.is_symlink() for path in paths):
        raise ValueError("retained_capture_symlink_rejected")
    body = paths[0].read_bytes()
    wire = paths[1].read_bytes()
    receipt = json.loads(paths[2].read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("retained_capture_receipt_invalid:" + name)
    return body, receipt


def write_outputs(storage, prefix: str, outputs: dict[str, bytes]) -> dict[str, dict[str, object]]:
    descriptors = {}
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
        snapshot = storage.object_snapshot(f"{prefix}/{name}")
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if (snapshot is None or snapshot.size != descriptor["bytes"]
                or storage.sha256_key(f"{prefix}/{name}") != descriptor["sha256"]
                or storage.object_snapshot(f"{prefix}/{name}") != snapshot):
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    return descriptors


def validate_crs(ref: object) -> None:
    if not isinstance(ref, dict):
        raise ValueError("source_crs_drift")
    codes = [ref[key] for key in ("wkid", "latestWkid") if key in ref]
    if not codes or any(type(code) is not int or code != 4326 for code in codes):
        raise ValueError("source_crs_drift")
    for key in ("wkt", "wkt2"):
        if key in ref:
            try:
                same_crs = pyproj.CRS.from_wkt(ref[key]).equals(pyproj.CRS.from_epsg(4326), ignore_axis_order=True)
            except Exception as exc:
                raise ValueError("source_crs_drift") from exc
            if not same_crs:
                raise ValueError("source_crs_drift")


def validate_dimensions(declaration: dict) -> None:
    for name in ("hasZ", "hasM"):
        if name in declaration and (type(declaration[name]) is not bool or declaration[name]):
            raise ValueError("source_dimension_drift")


def fetch_capture(client: httpx.Client, url: str, root: Path, name: str, budget: list[int], *,
                  max_capture_bytes: int, max_body_bytes: int,
                  prefill_empty_artifacts: bool = False,
                  allow_arcgis_item_redirect: bool = False) -> tuple[bytes, dict]:
    """Capture one IENC GET to disk under a running ``budget`` list.

    The body is byte-identical to the per-dataset ``_fetch`` copies it replaces.
    ``prefill_empty_artifacts`` pre-creates the body/wire artifacts so a valid
    zero-byte response still verifies against its receipt (the streaming
    callbacks are not invoked for it). ``allow_arcgis_item_redirect`` admits
    only https://www.arcgis.com/itemdata/ redirect targets.
    """
    root.mkdir(parents=True, exist_ok=True)
    body_path, wire_path = root / f"{name}.body", root / f"{name}.wire"
    for path in (body_path, wire_path):
        path.unlink(missing_ok=True)
        if prefill_empty_artifacts:
            path.write_bytes(b"")
    hashes = {"body": hashlib.sha256(), "wire": hashlib.sha256()}
    sizes = {"body": 0, "wire": 0}
    budget_start = budget[0]
    observed = datetime.now(timezone.utc).isoformat()

    def retain(path, kind):
        def write(chunk):
            with path.open("ab") as stream:
                stream.write(chunk)
            hashes[kind].update(chunk); sizes[kind] += len(chunk)
            budget[0] = budget_start + sizes["wire"] + sizes["body"]
        return write

    def reserve(progress):
        if budget_start + progress.wire_bytes_retained + progress.decoded_bytes_retained + progress.reserve_bytes > max_capture_bytes:
            raise ValueError("source_capture_budget_exceeded")

    response = None
    result = None
    final_url = url
    transport_error = None
    try:
        with client.stream("GET", url, follow_redirects=allow_arcgis_item_redirect,
                           headers={"Accept-Encoding": "identity"}) as response:
            final_url = str(response.url)
            if allow_arcgis_item_redirect and final_url != url:
                parsed = urlsplit(final_url)
                if parsed.scheme != "https" or parsed.netloc != "www.arcgis.com" or not parsed.path.startswith("/itemdata/"):
                    raise ValueError("unexpected_arcgis_pdf_redirect_target")
                final_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            result = capture_http_response(response, max_wire_bytes=max_body_bytes,
                max_decoded_bytes=max_body_bytes, reserve=reserve,
                retain_wire=retain(wire_path, "wire"), retain_decoded=retain(body_path, "body"))
    except Exception as exc:
        transport_error = type(exc).__name__
    receipt = {"method": "GET", "url": url, "final_url": final_url, "observed_at": observed,
        "status": result.status_code if result is not None else (response.status_code if response is not None else None),
        "complete_body": bool(result is not None and result.complete and transport_error is None),
        "wire_bytes": sizes["wire"], "wire_sha256": hashes["wire"].hexdigest(),
        "bytes": sizes["body"], "sha256": hashes["body"].hexdigest(),
        "content_encoding": result.content_encoding if result is not None else None,
        "content_length": result.content_length if result is not None else None,
        "error_code": result.error_code if result is not None else ("transport_error" if transport_error else "capture_result_missing"),
        "error_type": result.error_type if result is not None else transport_error}
    (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    if transport_error:
        raise httpx.TransportError("capture_transport_error:" + transport_error)
    if result is None:
        raise ValueError("capture_result_missing")
    body, wire = body_path.read_bytes(), wire_path.read_bytes()
    if (not result.complete or result.status_code != 200
        or (not allow_arcgis_item_redirect and final_url != url)
        or len(body) != receipt["bytes"] or _sha(body) != receipt["sha256"]
        or len(wire) != receipt["wire_bytes"] or _sha(wire) != receipt["wire_sha256"]):
        raise ValueError("incomplete_source_capture:" + name)
    return body, receipt


def fetch_capture_retry(client: httpx.Client, url: str, root: Path, name: str, budget: list[int], *,
                        max_capture_bytes: int, max_body_bytes: int,
                        attempts: int = 2, schedule: tuple[float, ...] = (),
                        prefill_empty_artifacts: bool = False,
                        allow_arcgis_item_redirect: bool = False) -> tuple[bytes, dict]:
    """Retry transient transport/incomplete reads, retaining every attempt.

    The loop is byte-identical to the per-dataset ``_fetch_retry`` copies it
    replaces: ``schedule[i]`` sleeps before attempt ``i`` when present, each
    attempt is retained under ``<name>.attempt<N>`` evidence names, a successful
    attempt's artifacts are promoted to the final names, and non-transient
    ValueError failures re-raise immediately.
    """
    last = None
    for attempt in range(attempts):
        wait = schedule[attempt] if attempt < len(schedule) else 0.0
        if wait:
            time.sleep(wait)
        attempt_name = f"{name}.attempt{attempt + 1}"
        try:
            body, receipt = fetch_capture(client, url, root, attempt_name, budget,
                max_capture_bytes=max_capture_bytes, max_body_bytes=max_body_bytes,
                prefill_empty_artifacts=prefill_empty_artifacts,
                allow_arcgis_item_redirect=allow_arcgis_item_redirect)
            for suffix in ("body", "wire", "receipt.json"):
                (root / f"{attempt_name}.{suffix}").replace(root / f"{name}.{suffix}")
            return body, receipt
        except (httpx.TransportError, ValueError) as exc:
            last = exc
            if isinstance(exc, ValueError) and not str(exc).startswith("incomplete_source_capture:"):
                raise
    raise last


def load_receipt(capture_root: Path, name: str, error: type[ValueError]) -> dict[str, object]:
    """Parse and structurally validate one retained receipt without page bytes."""
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise error("retained_capture_receipt_invalid:" + name) from exc
    if (not isinstance(receipt, dict) or receipt.get("method") != "GET"
            or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200):
        raise error("retained_capture_receipt_invalid:" + name)
    return receipt


def read_capture(capture_root: Path, name: str,
                 error: type[ValueError]) -> tuple[bytes, dict[str, object]]:
    body, wire = ((capture_root / f"{name}.body").read_bytes(),
                  (capture_root / f"{name}.wire").read_bytes())
    receipt = load_receipt(capture_root, name, error)
    if (receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise error("retained_capture_receipt_invalid:" + name)
    return body, receipt


def load_state(capture_root: Path, error: type[ValueError]) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        return {"segments": [], "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise error("capture_state_invalid") from exc
    if not isinstance(state, dict) or not isinstance(state.get("segments"), list):
        raise error("capture_state_invalid")
    return state


def save_state(capture_root: Path, state: Mapping[str, object]) -> None:
    tmp = capture_root / "capture_state.json.tmp"
    tmp.write_bytes(_canonical(dict(state)) + b"\n")
    tmp.replace(capture_root / "capture_state.json")


def witness_aside(capture_root: Path, name: str) -> None:
    """Move a retained refused/torn response aside so the slot can be refetched.

    A complete HTTP 200 error body carries a self-consistent receipt, so
    without this witness step a resumed capture would replay the same refusal
    forever; the witnessed evidence stays on disk next to the capture.
    """
    from time import time_ns
    stamp = time_ns()
    for suffix in ("body", "wire", "receipt.json"):
        source = capture_root / f"{name}.{suffix}"
        if source.exists():
            source.replace(capture_root / f"{name}.error-{stamp}.{suffix}")


def producer_error_doc(body: bytes) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        document = json.loads(body)
    except ValueError:
        return False
    return isinstance(document, dict) and "error" in document


def preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    for suffix in (".body", ".wire", ".receipt.json"):
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                       attempts: int, base_delay: float,
                       max_bytes: int) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures.

    Retries cover the producer's transient complete-200 Esri error documents
    and incomplete transport captures (connection resets, truncation); failed
    attempts are preserved on disk under ``<name>.attempt-NN`` evidence names
    and never enter the capture roster. The final attempt's body is always
    returned as captured, so a persistent producer failure surfaces to the
    caller unchanged.
    """
    for attempt in range(1, attempts + 1):
        try:
            body, receipt = capture_get(client, url, capture_root, name, max_bytes=max_bytes)
        except ValueError as exc:
            if attempt >= attempts or not str(exc).startswith("incomplete_source_capture:"):
                raise
            preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        if producer_error_doc(body) and attempt < attempts:
            preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        return body, receipt
    raise AssertionError("unreachable_retry_state")


class RetainedCaptureViews(Mapping):
    """Lazily materialize one capture view per access so quarantine stays bounded."""

    def __init__(self, capture_root: Path, names: Iterable[str], error: type[ValueError]):
        self._capture_root = Path(capture_root)
        self._names = tuple(names)
        self._error = error

    def __getitem__(self, name: str) -> tuple[bytes, dict[str, object]]:
        return read_capture(self._capture_root, name, self._error)

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)


# Historical private import names: the dataset modules under sources/ still
# import these (formerly from sources.usace_obstruction_lines), so their import
# blocks and bodies stay unchanged. New modules should use the public names.
_capture = capture_page
_load_capture = load_capture
_capture_identity = capture_identity
_esri_polyline_wkb = esri_polyline_wkb
_parquet_bytes = polyline_parquet_bytes
