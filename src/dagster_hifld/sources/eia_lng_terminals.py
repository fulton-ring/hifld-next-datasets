"""Faithful draft ingest for EIA's frozen January 2020 LNG terminal ZIP."""
from __future__ import annotations

import hashlib
import json
import math
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
import fiona
from shapely.geometry import shape

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import capture_get, processing_identity, write_immutable

DATASET_SLUG = "liquified-natural-gas-lng-import-and-export-terminals-1"
FILE_SLUG = DATASET_SLUG
SOURCE_URL = "https://www.eia.gov/maps/map_data/Lng_ImportExportTerminals_US_EIA.zip"
SOURCE_TITLE = "Liquefied Natural Gas Import/Export Terminals in the United States"
SOURCE_VINTAGE = "January 2020"
MAX_ARCHIVE_BYTES = 2_000_000
EXPECTED_ARCHIVE_SHA256 = "e91c0e2b803d08a6f59f8da3e62e8410ceaac08c84b7bb92cf71d2bd75bcadde"
EXPECTED_COUNT = 8
EXPECTED_FIELDS = ["Facility", "Owner", "Operator", "City", "County", "State", "Functions", "Regas_Bcfd", "Stora_Bcf", "Liq_Bcfd", "Period", "Source", "Latitude", "Longitude"]


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _safe_zip(archive: bytes, root: Path) -> Path:
    """Extract only the one known shapefile bundle, rejecting traversal/links."""
    with zipfile.ZipFile(__import__("io").BytesIO(archive)) as zf:
        names = zf.namelist()
        if len(names) != 8 or any(PurePosixPath(n).name != n or ".." in PurePosixPath(n).parts or "\\" in n or "\x00" in n for n in names):
            raise ValueError("source_zip_member_roster_or_path_drift")
        if {n.lower().rsplit(".", 1)[-1] for n in names if not n.lower().endswith(".shp.xml")} != {"cpg", "dbf", "prj", "sbn", "sbx", "shp", "shx"} or sum(n.lower().endswith(".shp.xml") for n in names) != 1:
            raise ValueError("source_shapefile_sidecar_roster_drift")
        shp_names = [n for n in names if n.lower().endswith(".shp")]
        if len(shp_names) != 1:
            raise ValueError("source_shapefile_missing_or_ambiguous")
        stem = shp_names[0][:-4].lower()
        if any(not n.lower().startswith(stem + ".") for n in names):
            raise ValueError("source_shapefile_sidecar_stem_drift")
        for info in zf.infolist():
            if info.file_size > MAX_ARCHIVE_BYTES or info.is_dir() or (info.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError("source_zip_member_size_or_type_invalid")
            target = root / info.filename
            target.write_bytes(zf.read(info))
        return root / shp_names[0]


def build_candidate(archive: bytes) -> tuple[bytes, dict[str, Any]]:
    """Build exact Arrow columns and WKB from DBF fields and original points."""
    digest = _sha(archive)
    if digest != EXPECTED_ARCHIVE_SHA256:
        raise ValueError("source_archive_digest_drift")
    with tempfile.TemporaryDirectory(prefix="eia-lng-") as temp:
        shp = _safe_zip(archive, Path(temp))
        with fiona.open(shp) as src:
            schema = src.schema
            names = list(schema["properties"])
            if names != EXPECTED_FIELDS or len(src) != EXPECTED_COUNT or src.crs.to_epsg() != 4326:
                raise ValueError("source_schema_count_or_crs_drift")
            types = dict(schema["properties"])
            rows = list(src)
            features = []
            null_counts = {name: 0 for name in names}
            for rec in rows:
                props = dict(rec["properties"])
                geom = rec["geometry"]
                if list(props) != EXPECTED_FIELDS or geom is None or geom["type"] != "Point":
                    raise ValueError("source_record_or_geometry_drift")
                for field, value in props.items():
                    if value is None:
                        null_counts[field] += 1
                point = shape(geom)
                if not (math.isclose(point.x, float(props["Longitude"]), rel_tol=0, abs_tol=1e-10)
                        and math.isclose(point.y, float(props["Latitude"]), rel_tol=0, abs_tol=1e-10)):
                    raise ValueError("source_geometry_attribute_coordinate_mismatch")
                features.append((str(rec.id), props, point))
            if len({fid for fid, _, _ in features}) != EXPECTED_COUNT:
                raise ValueError("source_fid_record_index_drift")
            arrays = {}
            fields = []
            for name in names:
                type_name = types[name]
                atype = pa.float64() if type_name.startswith("float") else pa.string()
                fields.append(pa.field(name, atype, nullable=True))
                arrays[name] = pa.array([props[name] for _, props, _ in features], type=atype)
            fields.extend([pa.field("geometry", pa.binary(), nullable=True), pa.field("source_fid_record_index", pa.string(), nullable=False)])
            arrays["geometry"] = pa.array([geom.wkb for _, _, geom in features], type=pa.binary())
            arrays["source_fid_record_index"] = pa.array([fid for fid, _, _ in features], type=pa.string())
            crs = pyproj.CRS.from_epsg(4326).to_json_dict()
            geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{"encoding":"WKB", "geometry_types":["Point"], "crs":crs, "edges":"planar"}}}
            table = pa.Table.from_pydict(arrays, schema=pa.schema(fields, metadata={b"geo":json.dumps(geo, sort_keys=True, separators=(",", ":")).encode()}))
            out = pa.BufferOutputStream(); pq.write_table(table, out, compression="zstd")
            record_hash = hashlib.sha256(json.dumps([{ "fid_record_index":fid, "properties":props, "geometry_wkb":geom.wkb.hex()} for fid,props,geom in features], sort_keys=True, separators=(",",":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()
            bounds = [min(g.bounds[0] for _, _, g in features), min(g.bounds[1] for _, _, g in features),
                      max(g.bounds[2] for _, _, g in features), max(g.bounds[3] for _, _, g in features)]
            audit = {"source_url":SOURCE_URL, "source_title":SOURCE_TITLE, "source_vintage":SOURCE_VINTAGE, "archive_sha256":digest,
                     "feature_count":len(features), "source_fid_semantics":"Shapefile positional feature record index only; not a stable public facility identifier",
                     "source_fields":names, "source_dbf_field_types":types, "crs":"EPSG:4326", "geometry_type":"Point",
                     "null_geometry_count":0, "source_field_null_counts":null_counts, "native_geometry_bounds_epsg4326":bounds,
                     "source_record_geometry_sha256":record_hash,
                     "currentness_accepted":False, "public_id_mapping_accepted":False, "historical_candidate_adopted":False}
            return out.getvalue().to_pybytes(), audit


def capture_eia_lng_terminals(storage: StagingStorageResource, *, version: str, capture_directory: str | None = None) -> dict[str, Any]:
    capture_root = Path(capture_directory) if capture_directory else Path(tempfile.mkdtemp(prefix="eia-lng-receipts-"))
    capture_root.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        archive, receipt = capture_get(client, SOURCE_URL, capture_root, "eia_lng_zip", max_bytes=MAX_ARCHIVE_BYTES)
    parquet, audit = build_candidate(archive)
    base = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    identity = processing_identity(Path(__file__).parents[3])
    stable_receipt = {key: value for key, value in receipt.items() if key != "observed_at"}
    evidence = {**audit, "archive_bytes":len(archive), "capture_receipt":stable_receipt,
                "candidate_geoparquet_sha256":_sha(parquet), "processing_identity":identity,
                "source_retrieved_from":"EIA official map-data URL"}
    for key, body in ((f"{base}/shapefile/source.zip", archive),
                      (f"{base}/geoparquet/{FILE_SLUG}.parquet", parquet),
                      (f"{base}/metadata/eia_lng_source_audit.json", json.dumps(evidence, sort_keys=True, indent=2).encode())):
        write_immutable(storage, key, body)
    (capture_root/"eia_lng_source_audit.json").write_text(json.dumps(evidence, sort_keys=True, indent=2))
    return {"version":version, **evidence, "candidate_only":True, "release_accepted":False}
