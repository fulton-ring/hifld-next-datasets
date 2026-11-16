"""Validate NFHL ESRI JSON pages and stream them into one (Geo)Parquet file.

Attributes keep the producer's fields, order and physical types (Esri dates
stay epoch-millisecond integers). Geometry is decoded by GDAL's ESRIJSON
driver through pyogrio and kept in the source CRS as WKB, so ring assembly and
hole assignment are GDAL's. Geometry GDAL cannot decode (for example
string-"NaN" ordinates), empty geometry, out-of-domain coordinates and invalid
geometry are counted and sampled in the audit, never dropped or repaired.
Pages are written in ascending OBJECTID order with bounded row groups, and
record/geometry digests are computed while streaming.
"""
from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Mapping
from itertools import pairwise
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from pyogrio.raw import read_arrow

from dagster_hifld.sources.esri_common import arrow_type, canonical
from dagster_hifld.sources.nfhl.capture import Page, check_spatial_reference
from dagster_hifld.sources.nfhl.layers import NfhlLayer

ROW_GROUP_ROWS = 50_000
SAMPLE_IDS = 25
GEOMETRY_TYPES = {"point": ["Point"], "line": ["LineString", "MultiLineString"],
                  "polygon": ["Polygon", "MultiPolygon"]}
_GEOGRAPHIC_DOMAIN = (-180.0, -90.0, 180.0, 90.0)


def plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


class LayerWriter:
    """Stream validated pages of one layer into a Parquet file."""

    def __init__(self, path: Path, layer: NfhlLayer, layer_doc: Mapping[str, object], oid_field: str,
                 crs_wkid: int):
        fields = list(layer_doc["fields"])
        self.layer, self.oid_field, self.crs_wkid = layer, oid_field, crs_wkid
        self.attr_fields = [f for f in fields if f["type"] != "esriFieldTypeGeometry"]
        self.attr_names = [f["name"] for f in self.attr_fields]
        self.geometry_columns = [f["name"] for f in fields if f["type"] == "esriFieldTypeGeometry"]
        if layer.is_table != (not self.geometry_columns):
            raise ValueError("source_geometry_field_roster_drift")
        self.types = {f["name"]: arrow_type(f) for f in self.attr_fields}
        schema = pa.schema([pa.field(f["name"], pa.binary() if f["type"] == "esriFieldTypeGeometry"
                                     else self.types[f["name"]]) for f in fields])
        if not layer.is_table:
            primary = (layer_doc.get("geometryField") or {}).get("name")
            primary = primary if primary in self.geometry_columns else self.geometry_columns[0]
            geo = {"version": "1.1.0", "primary_column": primary, "columns": {name: {
                "encoding": "WKB", "geometry_types": GEOMETRY_TYPES[layer.geometry],
                "crs": pyproj.CRS.from_epsg(crs_wkid).to_json_dict(), "edges": "planar"}
                for name in self.geometry_columns}}
            schema = schema.with_metadata({b"geo": canonical(geo)})
        self.schema = schema
        self._writer = pq.ParquetWriter(path, schema, compression="zstd")
        self._buffer: list[pa.Table] = []
        self._buffered = 0
        self._record_hash = hashlib.sha256()
        self._geometry_hash = hashlib.sha256()
        self.feature_count = 0
        self.last_oid: int | None = None
        self.report = GeometryReport()

    def add_page(self, page: Page) -> int:
        doc = page.doc
        features = doc.get("features")
        if "error" in doc or not isinstance(features, list):
            raise ValueError(f"source_page_missing_features:{page.name}")
        served = [(f.get("name"), f.get("type")) for f in doc.get("fields") or [] if isinstance(f, dict)]
        if served != [(f["name"], f["type"]) for f in self.attr_fields]:
            raise ValueError(f"source_page_field_schema_drift:{page.name}")
        if not self.layer.is_table:
            check_spatial_reference(doc.get("spatialReference"), self.crs_wkid, f"source_{page.name}")
            if doc.get("geometryType") != self.layer.esri_geometry_type:
                raise ValueError(f"source_page_geometry_type_drift:{page.name}")
        oids = []
        expected_keys = set(self.attr_names)
        for feature in features:
            attrs = feature.get("attributes") if isinstance(feature, dict) else None
            if not isinstance(attrs, dict) or set(attrs) != expected_keys:
                raise ValueError(f"source_record_attribute_schema_drift:{page.name}")
            oid = attrs[self.oid_field]
            if type(oid) is not int or not page.lo <= oid <= page.hi:
                raise ValueError(f"source_page_objectid_outside_window:{page.name}")
            oids.append(oid)
        order = sorted(range(len(features)), key=oids.__getitem__)
        ordered = [oids[i] for i in order]
        if any(a >= b for a, b in pairwise(ordered)):
            raise ValueError(f"source_page_duplicate_objectid:{page.name}")
        if ordered and self.last_oid is not None and ordered[0] <= self.last_oid:
            raise ValueError(f"source_page_overlaps_previous_window:{page.name}")
        if not features:
            return 0
        self.last_oid = ordered[-1]
        rows = [features[i] for i in order]
        columns: dict[str, pa.Array] = {}
        for name in self.attr_names:
            try:
                columns[name] = pa.array([row["attributes"][name] for row in rows], type=self.types[name])
            except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
                raise ValueError(f"source_field_value_type_drift:{name}:{page.name}") from None
        wkb = None
        if not self.layer.is_table:
            wkb = self._decode_geometry(page, ordered)
            self.report.add(ordered, [row.get("geometry") for row in rows], wkb, self.layer)
            for name in self.geometry_columns:
                columns[name] = pa.array(wkb, type=pa.binary())
        for oid, row in zip(ordered, rows):
            self._record_hash.update(canonical(row) + b"\n")
            self._geometry_hash.update(canonical([oid, row.get("geometry")]) + b"\n")
        self._buffer.append(pa.Table.from_pydict({f.name: columns[f.name] for f in self.schema},
                                                 schema=self.schema))
        self._buffered += len(rows)
        self.feature_count += len(rows)
        if self._buffered >= ROW_GROUP_ROWS:
            self._flush()
        return len(rows)

    def _decode_geometry(self, page: Page, ordered: list[int]) -> list[bytes | None]:
        meta, table = read_arrow(f"ESRIJSON:{page.body_path}", columns=[], return_fids=True)
        fids = table.column(0).to_pylist()
        geometry_name = meta.get("geometry_name") or "wkb_geometry"
        by_fid = dict(zip(fids, table.column(geometry_name).to_pylist()))
        if len(by_fid) != len(fids) or sorted(by_fid) != ordered:
            raise ValueError(f"source_page_geometry_decode_mismatch:{page.name}")
        return [by_fid[oid] for oid in ordered]

    def _flush(self) -> None:
        if self._buffer:
            self._writer.write_table(pa.concat_tables(self._buffer), row_group_size=ROW_GROUP_ROWS)
        self._buffer, self._buffered = [], 0

    def close(self) -> dict[str, object]:
        self._flush()
        self._writer.close()
        return {"feature_count": self.feature_count,
                "source_record_sha256": self._record_hash.hexdigest(),
                "source_geometry_sha256": self._geometry_hash.hexdigest(),
                "geometry": None if self.layer.is_table else self.report.summary()}

    def abort(self) -> None:
        self._buffer = []
        self._writer.close()


class GeometryReport:
    """Counts and sampled OBJECTIDs for source geometry that is not clean."""

    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.types: Counter[str] = Counter()
        self.reasons: Counter[str] = Counter()
        self.samples: dict[str, list[int]] = {}
        self.bounds: list[float] | None = None

    def _note(self, category: str, oids) -> None:
        oids = list(oids)
        if oids:
            self.counts[category] += len(oids)
            sample = self.samples.setdefault(category, [])
            sample.extend(int(oid) for oid in oids[:SAMPLE_IDS - len(sample)])

    def add(self, oids: list[int], source: list[object], wkb: list[bytes | None], layer: NfhlLayer) -> None:
        ids = np.asarray(oids)
        absent = np.array([g is None for g in source], dtype=bool)
        geoms = shapely.from_wkb(np.array(wkb, dtype=object))
        missing = shapely.is_missing(geoms)
        self._note("null_geometry", ids[absent])
        self._note("unconvertible_geometry", ids[missing & ~absent])
        present = ~missing
        # GDAL derives the coordinate dimension from the served vertices, so a
        # decoded XYZ or XYZM geometry means the page carried Z/M ordinates.
        dimensions = [d for d in set(shapely.get_coordinate_dimension(geoms[present]))
                      if d not in (2, None)]
        if dimensions:
            raise ValueError(f"source_z_or_m_geometry_unsupported:{dimensions[0]}")
        empty = present & shapely.is_empty(geoms)
        self._note("empty_geometry", ids[empty])
        usable = present & ~empty
        if layer.geometry != "point":
            reasons = shapely.is_valid_reason(geoms[usable])
            invalid = reasons != "Valid Geometry"
            self._note("invalid_geometry", ids[usable][invalid])
            self.reasons.update(str(reason).split("[", 1)[0] for reason in reasons[invalid])
        self.types.update(int(t) for t in shapely.get_type_id(geoms[present]))
        if usable.any():
            box = shapely.bounds(geoms[usable])
            xmin, ymin = box[:, 0], box[:, 1]
            xmax, ymax = box[:, 2], box[:, 3]
            lo_x, lo_y, hi_x, hi_y = _GEOGRAPHIC_DOMAIN
            outside = (xmin < lo_x) | (ymin < lo_y) | (xmax > hi_x) | (ymax > hi_y)
            self._note("out_of_domain_geometry", ids[usable][outside])
            page_bounds = [float(xmin.min()), float(ymin.min()), float(xmax.max()), float(ymax.max())]
            self.bounds = page_bounds if self.bounds is None else [
                min(self.bounds[0], page_bounds[0]), min(self.bounds[1], page_bounds[1]),
                max(self.bounds[2], page_bounds[2]), max(self.bounds[3], page_bounds[3])]
        if set(self.types) - {shapely.GeometryType[name.upper()].value for name in GEOMETRY_TYPES[layer.geometry]}:
            raise ValueError("source_geometry_type_outside_layer_kind")

    def summary(self) -> dict[str, object]:
        names = {shapely.GeometryType[name.upper()].value: name for kind in GEOMETRY_TYPES.values() for name in kind}
        categories = ("null_geometry", "unconvertible_geometry", "empty_geometry", "invalid_geometry",
                      "out_of_domain_geometry")
        return {**{f"{c}_count": self.counts[c] for c in categories},
                "sample_object_ids": {c: self.samples[c] for c in categories if c in self.samples},
                "invalid_geometry_reasons": dict(sorted(self.reasons.items())),
                "geometry_type_counts": {names[k]: v for k, v in sorted(self.types.items())},
                "observed_bounds": None if self.bounds is None else dict(zip(("xmin", "ymin", "xmax", "ymax"),
                                                                             self.bounds))}
