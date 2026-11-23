"""Shared USACE IENC layer capture, replay and encoding primitives.

The budgeted page capture, retained-capture load with receipt verification,
candidate-identity digest, Esri polyline-to-WKB encoder and IENC GeoParquet
serializer the USACE IENC dataset adapters share. Every body is byte-identical
to the copy it replaces in ``sources.arcgis_layer``; per-dataset byte budgets
are passed explicitly by each caller, so a declared capture budget is enforced.
Canonical JSON and sha256 delegate to the single implementations in
``sources.esri_common``; the bounded fetch delegates to
``sources.capture_common``. ``sources.arcgis_layer`` re-exports these for the
historical import paths used by non-IENC ArcGIS adapters.
"""
from __future__ import annotations

import json
import struct
import tempfile
from pathlib import Path
from typing import Iterator

import httpx
import pyarrow as pa

from dagster_hifld.sources.capture_common import capture_get
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.sources.geoparquet_candidate import stream_geoparquet_candidate

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
            "esriFieldTypeSmallInteger":pa.int16(),
            "esriFieldTypeString":pa.string()}[field["type"]]


GEOMETRY_COLUMN = "geometry"
POLYLINE_BATCH_SIZE = 250


def polyline_candidate_parquet(fields: list[dict], features: list[dict], source_crs: int | str,
                               capture_root: Path) -> tuple[Path, str, int]:
    """Stream one captured Esri polyline layer to a local GeoParquet candidate.

    Attribute columns keep declared physical types and source order; paths
    encode to WKB with source order preserved. The GeoParquet 1.1 ``geo``
    metadata comes from the shared candidate writer, which builds the source
    CRS PROJJSON from the actual source CRS. Returns the candidate path under
    ``capture_root``, its sha256 digest and its row count.
    """
    properties = [field for field in fields if field["name"] != "SHAPE"]
    names = [field["name"] for field in properties] + [GEOMETRY_COLUMN]
    schema = pa.schema(
        [pa.field(field["name"], _arrow_type(field), nullable=True) for field in properties]
        + [pa.field(GEOMETRY_COLUMN, pa.binary(), nullable=True)])

    def batches() -> Iterator[pa.RecordBatch]:
        for start in range(0, len(features), POLYLINE_BATCH_SIZE):
            chunk = features[start:start + POLYLINE_BATCH_SIZE]
            arrays = [pa.array([row["attributes"][field["name"]] for row in chunk],
                               type=_arrow_type(field)) for field in properties]
            arrays.append(pa.array([esri_polyline_wkb(row["geometry"]) for row in chunk],
                                   type=pa.binary()))
            yield pa.record_batch(arrays, names=names)

    return stream_geoparquet_candidate(
        schema, source_crs, batches(), Path(capture_root) / "candidate.parquet",
        geometry_column=GEOMETRY_COLUMN, geometry_types=["MultiLineString"])


def polyline_parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    """Render polyline features to GeoParquet bytes through the shared writer.

    Transitional interface for the adapters not yet migrated to
    ``polyline_candidate_parquet``: the candidate streams to a private
    temporary directory and is read back whole, so the GeoParquet 1.1
    metadata and compression/dictionary policy stay with the shared writer.
    The source CRS is pinned to EPSG:4326, which every current caller
    validates before calling.
    """
    with tempfile.TemporaryDirectory(prefix="hifld-polyline-candidate-") as candidate_dir:
        path, _digest, _rows = polyline_candidate_parquet(fields, features, 4326,
                                                          Path(candidate_dir))
        return path.read_bytes()
