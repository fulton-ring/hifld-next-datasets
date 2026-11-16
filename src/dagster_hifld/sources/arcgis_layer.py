"""Shared ArcGIS FeatureServer/MapServer capture and replay primitives.

The shared module the root-cause issue #993 defines for ArcGIS layer capture;
it owns two families of primitives:

- The capture/replay primitives the ArcGIS dataset adapters share:
  budget-guarded paged capture (``capture_page``), retained capture replay
  (``load_capture``), candidate identity digests (``capture_identity``), Esri
  polyline WKB encoding (``esri_polyline_wkb``) and GeoParquet serialization
  (``polyline_parquet_bytes``). Canonical JSON, sha256 and the ArcGIS CRS
  drift guard delegate to the single implementations in ``sources.esri_common``
  and ``sources.esri_geometry``.
- The checkpoint load/save, retained-capture read, refusal witness,
  failed-attempt evidence and bounded-retry mechanics previously duplicated
  across the segmented NHD/3DHP adapters (``nhd_catchment.py``,
  ``nhd_hydrolocation_htdcco.py``,
  ``nhd_hydrolocation_sink_spring_waterbody_outlet.py``) and the Flowline
  refusal witness (issue #927). Each body is identical to the copies it
  replaces; only each adapter's own error type and fetch budgets are injected
  explicitly. Fetch planning, roster/offset algorithms, checkpoint state
  layouts, schema, ID and geometry contracts stay local to each adapter. The
  full ``capture_layer(url, *, capture_dir, byte_budget, page_size)`` paging
  API remains #993's remaining scope.

The private aliases at the bottom keep the historical import names working so
each dataset module only changes its import line; new modules should use the
public names.
"""
from __future__ import annotations

import json
import struct
import time
from pathlib import Path
from typing import Iterable, Iterator, Mapping

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.sources.capture_common import capture_get
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
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
