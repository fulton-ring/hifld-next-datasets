"""Shared ArcGIS layer capture and replay primitives.

This module owns the retained-capture primitives that the ArcGIS
FeatureServer/MapServer dataset adapters share: budget-guarded paged capture
(``capture_page``), retained capture replay (``load_capture``), candidate
identity digests (``capture_identity``), Esri polyline WKB encoding
(``esri_polyline_wkb``) and GeoParquet serialization (``polyline_parquet_bytes``).
Canonical JSON, sha256 and the ArcGIS CRS drift guard delegate to the single
implementations in ``sources.esri_common`` and ``sources.esri_geometry``.

The private aliases at the bottom keep the historical import names working so
each dataset module only changes its import line; new modules should use the
public names.
"""
from __future__ import annotations

import json
import struct
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.sources.epa_frs_common import capture_get
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


# Historical private import names: the dataset modules under sources/ still
# import these (formerly from sources.usace_obstruction_lines), so their import
# blocks and bodies stay unchanged. New modules should use the public names.
_capture = capture_page
_load_capture = load_capture
_capture_identity = capture_identity
_esri_polyline_wkb = esri_polyline_wkb
_parquet_bytes = polyline_parquet_bytes
