"""Pinned historical USGS 2023 NSHM uniform-hazard ground motion candidate for issue #348.

The HIFLD layer ``uniform-hazard-ground-motion/us-sa1p0-5pct50yrs-bc-poly`` is one
contour-band polygon shapefile (5% in 50 years, 1.0 s spectral acceleration, NEHRP
site class B/C) out of the producer package
``US_SA1P0_2Pct_5Pct_10Pct_50Yrs_BC.zip`` of the USGS ScienceBase item
"04. Uniform-hazard ground motion maps for the conterminous U.S., Alaska, and
Hawaii" (data release for the 2023 U.S. 50-State National Seismic Hazard Model -
Overview, DOI 10.5066/P9GNPCOD). The revised 2023 NSHM releases (CONUS 2023.R1/R2,
published 2026-08-10) supersede the model's hazard *values* with point/curve CSVs
on a finer grid and publish no successor polygon-map shapefiles, so this pinned
producer package is the only official publication of the HIFLD layer's product.
"""
from __future__ import annotations
import hashlib, io, json, math, shutil, struct, tempfile, zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
import geopandas as gpd
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import shapely
from pyproj import CRS
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import candidate_prefix, capture_get, processing_identity, quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable

DATASET_SLUG = "uniform-hazard-ground-motion-1"
FILE_SLUG = "us-sa1p0-5pct50yrs-bc-poly-1"
SOURCE_ITEM_ID = "64ff886dd34ed30c2057b4d9"
SOURCE_ITEM_URL = f"https://www.sciencebase.gov/catalog/item/{SOURCE_ITEM_ID}"
SOURCE_URL = ("https://www.sciencebase.gov/catalog/file/get/" + SOURCE_ITEM_ID
              + "?f=__disk__0b%2F37%2F83%2F0b3783ed23dabb77b3d78a534237395f69e6ab99")
SOURCE_PACKAGE = "US_SA1P0_2Pct_5Pct_10Pct_50Yrs_BC.zip"
SOURCE_RELEASE = "2023 NSHM (original release)"
SOURCE_PUBLICATION = "2023-12-28"
SOURCE_ITEM_PUBLICATION = "2023-12-21"
SOURCE_DOI = "https://doi.org/10.5066/P9GNPCOD"
IDENTIFIER_COLUMN = "source_record_number"
EXPECTED_LAYER = "US_SA1P0_5Pct50Yrs_BC_poly"
EXPECTED_ZIP_BYTES = 41_289_138
EXPECTED_ZIP_SHA256 = "1ecc41448f089df15cd0a2802934e0f2b7dffbbcd5d418618da97a8c094bcd29"
EXPECTED_COUNT = 4014
EXPECTED_FIELDS = [
    ("low_cont", "C", 254, 0), ("high_cont", "C", 254, 0), ("range_cont", "C", 254, 0),
    ("valley", "C", 254, 0), ("Shape_Leng", "F", 19, 11), ("Shape_Area", "F", 19, 11),
]
EXPECTED_PRJ = ('GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,'
                '298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]')
EXPECTED_POLYGON_COUNT = 3927
EXPECTED_MULTIPOLYGON_COUNT = 87
EXPECTED_RING_COUNT = 6980
EXPECTED_VERTEX_COUNT = 678249
EXPECTED_INVALID_GEOMETRY_COUNT = 7
ID_SHA = "973df8bec6a54dcdfb0899a8a05ecc9d7e23210bbdcd65f3177ee49ab6daea0d"
RECORD_SHA = "6d0afb1cce5548b2b62f66887f44112c3136a4677483041b7e69e6fe6a56e685"
GEOMETRY_SHA = "63abcc64e0279c60b4f57aee4a5f587fad7bddeeb60f0b069b5058cdfe4b3fc0"
BOUNDS = [-179.99999999999997, 18.700000000000045, 180.0000000000001, 71.60012207000005]
WGS84 = CRS.from_epsg(4326)
PREFIX = "US_SA1P0_5Pct50Yrs_BC_poly."
POLY_MEMBERS = {PREFIX + s for s in ("dbf", "prj", "shp", "shx")}
MAX_DECOMPRESSED_BYTES = 200_000_000


def _member_roster():
    others = {"SA1P0_2Pct.lyrx", "SA1P0_5Pct_10Pct.lyrx",
              "US_2023_1sec_2PctIn50Yrs_Vs30_760.pdf",
              "US_2023_1sec_5PctIn50Yrs_Vs30_760.pdf",
              "US_2023_1sec_10PctIn50Yrs_Vs30_760.pdf"}
    members = set(POLY_MEMBERS)
    for pct in ("2Pct", "5Pct", "10Pct"):
        for kind in ("poly", "arc"):
            members |= {f"US_SA1P0_{pct}50Yrs_BC_{kind}.{s}" for s in ("dbf", "prj", "shp", "shx")}
    return members | others


MEMBERS = _member_roster()


def _canonical(x): return json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
def _sha(b): return hashlib.sha256(b).hexdigest()
def _native(v):
    if hasattr(v, "item"): v = v.item()
    if v is None or (isinstance(v, float) and math.isnan(v)): return None
    if isinstance(v, float) and not math.isfinite(v): raise ValueError("source_nonfinite_attribute")
    return v


def _schema_dbf(path):
    b = path.read_bytes(); h = int.from_bytes(b[8:10], "little")
    if h < 33 or h > len(b): raise ValueError("source_dbf_header_invalid")
    out = []; o = 32
    while o < h and b[o] != 0x0d:
        d = b[o:o + 32]
        if len(d) != 32: raise ValueError("source_dbf_descriptor_incomplete")
        out.append((d[:11].split(b"\0", 1)[0].decode("ascii"), chr(d[11]), d[16], d[17])); o += 32
    if o >= h or b[o] != 0x0d: raise ValueError("source_dbf_terminator_missing")
    return out


def _fid_roster(shp, shx, count):
    """Read the native record numbers straight from the shapefile index.

    The ESRI Shapefile driver exposes no FID column, so the source row identity
    is validated from the .shx offsets and the .shp record headers instead of
    any reader-derived index.
    """
    index = shx.read_bytes(); body = shp.read_bytes()
    if len(index) != 100 + 8 * count: raise ValueError("source_shx_length_drift")
    fids = []
    for i in range(count):
        offset, words = struct.unpack(">2I", index[100 + 8 * i:100 + 8 * i + 8])
        head = body[offset * 2:offset * 2 + 4]
        if len(head) != 4: raise ValueError("source_shp_record_missing")
        fids.append(int(struct.unpack(">I", head)[0]))
    if fids != list(range(1, count + 1)): raise ValueError("source_fid_roster_drift")
    return fids


def _ring_vertex_stats(geoms):
    rings = vertices = 0
    for g in geoms:
        parts = [g] if g.geom_type == "Polygon" else list(g.geoms)
        rings += len(parts) + sum(len(p.interiors) for p in parts)
        vertices += sum(len(p.exterior.coords) + sum(len(r.coords) for r in p.interiors) for p in parts)
    return rings, vertices


def _content_digests(g, names, fids):
    """Digest the complete native population exactly as read from the source.

    Row identity is the native shapefile record number (FID), so the geometry
    roster is [FID, per-feature native WKB SHA-256] per row in source order and
    the record roster is the full attribute vector in source field order.
    """
    attrs = [[_native(v) for v in row] for row in g[names].itertuples(index=False, name=None)]
    ids = sorted(int(f) for f in fids)
    wkbs = shapely.to_wkb(g.geometry.array, byte_order=1, output_dimension=2)
    geom = [[int(f), _sha(w)] for f, w in zip(fids, wkbs)]
    idsha, recsha, geomsha = (_sha(_canonical(x)) for x in (ids, attrs, geom))
    bounds = list(map(float, g.total_bounds))
    return ids, attrs, idsha, recsha, geomsha, bounds


def _decode(body, tmp):
    if len(body) != EXPECTED_ZIP_BYTES or _sha(body) != EXPECTED_ZIP_SHA256: raise ValueError("pinned_usgs_snapshot_drift")
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        names = []
        for i in z.infolist():
            mode = i.external_attr >> 16; parts = Path(i.filename).parts
            if not i.filename or i.filename.startswith(("/", "\\")) or "\\" in i.filename or ".." in parts or len(parts) != 1 or i.is_dir() or (mode & 0o170000) == 0o120000: raise ValueError("source_zip_member_path_rejected")
            names.append(i.filename)
        if len(set(names)) != len(names) or set(names) != MEMBERS or sum(i.file_size for i in z.infolist()) > MAX_DECOMPRESSED_BYTES: raise ValueError("source_zip_member_roster_drift")
        root = tmp / "shape"; root.mkdir()
        for name in POLY_MEMBERS:
            with z.open(name) as src, (root / name).open("xb") as dst: shutil.copyfileobj(src, dst)
    shp = root / (PREFIX + "shp")
    if (root / (PREFIX + "prj")).read_text().strip() != EXPECTED_PRJ: raise ValueError("source_prj_drift")
    if _schema_dbf(root / (PREFIX + "dbf")) != EXPECTED_FIELDS: raise ValueError("source_dbf_schema_drift")
    info = pyogrio.read_info(shp)
    if info["layer_name"] != EXPECTED_LAYER or info["geometry_type"] != "Polygon" or info["crs"] != "EPSG:4326" or int(info["features"]) != EXPECTED_COUNT: raise ValueError("source_layer_metadata_drift")
    fids = _fid_roster(shp, root / (PREFIX + "shx"), EXPECTED_COUNT)
    g = pyogrio.read_dataframe(shp)
    names = [x[0] for x in EXPECTED_FIELDS]
    if list(g.columns) != names + ["geometry"] or len(g) != EXPECTED_COUNT: raise ValueError("source_population_or_schema_drift")
    if any(str(t) != ("float64" if k == "F" else "object") for t, (_, k, _, _) in zip(g.dtypes, EXPECTED_FIELDS)): raise ValueError("source_attribute_physical_type_drift")
    if g[names].isna().any().any() or g.geometry.isna().any() or g.geometry.is_empty.any(): raise ValueError("source_population_or_null_drift")
    if not CRS.from_user_input(g.crs).equals(WGS84, ignore_axis_order=True): raise ValueError("source_crs_drift")
    counts = g.geometry.geom_type.value_counts()
    if int(counts.get("Polygon", 0)) != EXPECTED_POLYGON_COUNT or int(counts.get("MultiPolygon", 0)) != EXPECTED_MULTIPOLYGON_COUNT: raise ValueError("source_geometry_type_drift")
    if any(shapely.get_coordinate_dimension(gg) != 2 for gg in g.geometry.array): raise ValueError("source_non_2d_geometry")
    invalid = int((~g.geometry.is_valid).sum())
    if invalid != EXPECTED_INVALID_GEOMETRY_COUNT: raise ValueError("source_geometry_validity_drift")
    rings, vertices = _ring_vertex_stats(g.geometry.array)
    if rings != EXPECTED_RING_COUNT or vertices != EXPECTED_VERTEX_COUNT: raise ValueError("source_geometry_vertex_drift")
    ids, attrs, idsha, recsha, geomsha, bounds = _content_digests(g, names, fids)
    if idsha != ID_SHA or recsha != RECORD_SHA or geomsha != GEOMETRY_SHA: raise ValueError("source_content_snapshot_drift")
    if bounds != BOUNDS: raise ValueError("source_bounds_drift")
    audit = {
        "source_feature_count": len(g), "source_population_complete": True,
        "source_id_field": "FID (shapefile record number)", "source_id_unique": True,
        "source_id_min": min(ids), "source_id_max": max(ids), "source_id_sha256": idsha,
        "source_record_sha256": recsha, "source_geometry_sha256": geomsha,
        "source_schema_sha256": _sha(_canonical(EXPECTED_FIELDS)), "source_dbf_schema": EXPECTED_FIELDS,
        "source_crs": "EPSG:4326", "source_geometry_type": "Polygon/MultiPolygon",
        "source_polygon_count": EXPECTED_POLYGON_COUNT, "source_multipolygon_count": EXPECTED_MULTIPOLYGON_COUNT,
        "source_ring_count": EXPECTED_RING_COUNT, "source_vertex_count": EXPECTED_VERTEX_COUNT,
        "source_bounds": bounds, "source_release": SOURCE_RELEASE,
        "source_release_date": SOURCE_PUBLICATION, "source_release_date_basis": "producer-declared FGDC citation pubdate 20231228 in the item metadata HazardMapsAndGISdata_2023.xml",
        "source_item_publication_date": SOURCE_ITEM_PUBLICATION,
        "source_null_counts": {n: int(g[n].isna().sum()) for n in names},
        "source_geometry_null_count": int(g.geometry.isna().sum()), "source_geometry_empty_count": int(g.geometry.is_empty.sum()),
        "source_invalid_geometry_count": invalid,
        "source_coverage_scope": "Complete 4014-record contour-band polygon population of the pinned producer shapefile US_SA1P0_5Pct50Yrs_BC_poly (1.0 s spectral acceleration, 5% in 50 years, NEHRP site class B/C, VS30=760 m/s) over the conterminous U.S., Alaska and Hawaii, as published in the USGS ScienceBase item 04 package US_SA1P0_2Pct_5Pct_10Pct_50Yrs_BC.zip of the 2023 U.S. 50-State NSHM Overview data release (DOI 10.5066/P9GNPCOD). 157 distinct contour bands per field (low/high/range_cont) with the producer's -1000000 no-data band, 87 antimeridian MultiPolygons, 7 natively invalid (self-intersecting) ring geometries preserved verbatim without repair. Historical model snapshot; the 2026-08-10 Revised 2023 NSHM releases (CONUS 2023.R1/R2) supersede the model's hazard values with CSV point/curve data on a finer 0.05-degree grid and publish no successor polygon-map shapefiles.",
        "drift_guard": "Pinned official ScienceBase archive checksum, complete 29-member zip roster, exact PRJ text and DBF widths/types, layer name, CRS, contiguous FID roster 1..4014, polygon/multipolygon/ring/vertex totals, native validity census, all attributes and values, per-feature native WKB digests and bounds.",
    }
    return g, fids, audit


def _parquet(g, fids):
    # IDENTIFIER_COLUMN carries the native shapefile record number (ESRI FID,
    # the source row identity validated from the pinned SHP/SHX record headers)
    # exactly as extracted, in source row order; no reader-derived index or
    # other synthetic identifier is written.
    arrays = [pa.array([int(f) for f in fids], type=pa.int64())]
    fields = [pa.field(IDENTIFIER_COLUMN, pa.int64(), nullable=False)]
    for name, kind, *_ in EXPECTED_FIELDS:
        vals = [_native(v) for v in g[name].tolist()]
        dtype = pa.float64() if kind == "F" else pa.string()
        arrays.append(pa.array(vals, type=dtype, safe=True)); fields.append(pa.field(name, dtype, nullable=True))
    wkbs = shapely.to_wkb(g.geometry.array, byte_order=1, output_dimension=2)
    arrays.append(pa.array([bytes(w) for w in wkbs], type=pa.binary())); fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"], "crs": WGS84.to_json_dict()}}}
    sink = io.BytesIO(); pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata={b"geo": _canonical(geo)})), sink, compression="zstd", use_dictionary=False)
    t = pq.read_table(pa.BufferReader(sink.getvalue()))
    if t.num_rows != EXPECTED_COUNT: raise ValueError("candidate_row_count_readback_drift")
    if t[IDENTIFIER_COLUMN].to_pylist() != [int(f) for f in fids]: raise ValueError("candidate_identifier_readback_drift")
    for name, kind, *_ in EXPECTED_FIELDS:
        if t[name].to_pylist() != [_native(v) for v in g[name].tolist()]: raise ValueError("candidate_attribute_readback_drift:" + name)
    if any(t["geometry"][i].as_py() != bytes(w) for i, w in enumerate(wkbs)): raise ValueError("candidate_geometry_readback_drift")
    return sink.getvalue()


def _load_capture(root):
    paths = [root / "usgs_zip.body", root / "usgs_zip.wire", root / "usgs_zip.receipt.json"]
    if any(p.is_symlink() for p in paths): raise ValueError("capture_symlink_rejected")
    body, wire, receipt = paths[0].read_bytes(), paths[1].read_bytes(), json.loads(paths[2].read_bytes())
    if receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL or receipt.get("final_url") != SOURCE_URL or receipt.get("complete_body") is not True or receipt.get("status") != 200 or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body) or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire): raise ValueError("capture_receipt_invalid")
    return body, wire, receipt


def _capture(root, storage, version):
    mp = root / "capture_manifest.json"
    if root.is_symlink() or any(p.is_symlink() for p in root.parents): raise ValueError("unsafe_capture_directory")
    if mp.exists():
        m = json.loads(mp.read_bytes())
        if m.get("complete") is not True or set(m.get("receipts", {})) != {"usgs_zip"}: raise ValueError("capture_manifest_invalid")
        roster = m.get("artifact_roster"); validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
        if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}): raise ValueError("capture_roster_digest_drift")
        body, wire, receipt = _load_capture(root)
        if receipt != m["receipts"]["usgs_zip"]: raise ValueError("capture_manifest_receipt_drift")
        return body, wire, receipt
    if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")): raise ValueError("unsealed_capture_attempt")
    root.mkdir(parents=True, exist_ok=True); caps = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(300, connect=30), headers={"User-Agent": "hifld-next-datasets/usgs-uniform-hazard-ground-motion-draft"}) as client:
            body, receipt = capture_get(client, SOURCE_URL, root, "usgs_zip", max_bytes=128_000_000)
        if receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL or receipt.get("final_url") != SOURCE_URL: raise ValueError("source_request_identity_mismatch")
        caps["usgs_zip"] = (body, receipt)
        with tempfile.TemporaryDirectory(dir=root) as tmp: _decode(body, Path(tmp))
        mp.write_bytes(_canonical({"complete": True, "captured_at": datetime.now(timezone.utc).isoformat(), "receipts": {"usgs_zip": receipt}, "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        return body, (root / "usgs_zip.wire").read_bytes(), receipt
    except Exception as exc:
        rec = {}
        for p in root.glob("*.receipt.json"):
            try: rec[p.name.removesuffix(".receipt.json")] = json.loads(p.read_bytes()); caps.setdefault("usgs_zip", ((root / "usgs_zip.body").read_bytes(), rec[p.name.removesuffix(".receipt.json")]))
            except Exception: pass
        mp.write_bytes(_canonical({"complete": False, "captured_at": datetime.now(timezone.utc).isoformat(), "error_code": type(exc).__name__, "receipts": rec}) + b"\n")
        if caps: quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, caps, type(exc).__name__)
        raise


def capture_usgs_uniform_hazard_ground_motion(storage: StagingStorageResource, *, version: str, capture_directory: str | None = None) -> dict:
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix: raise ValueError("explicit_local_staging_resource_required")
    if not isinstance(version, str) or not version or Path(version).name != version or "/" in version or "\\" in version: raise ValueError("unsafe_capture_version")
    stage = Path(storage.local_dir).absolute()
    if ".." in stage.parts or any(p.is_symlink() for p in (stage, *stage.parents)): raise ValueError("unsafe_local_staging_root")
    stage.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(stage).free < 500_000_000: raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    root = requested or Path("/tmp/hifld-native-reconstruction/348/captures") / (version + "-" + uuid4().hex)
    body, wire, receipt = _capture(root, storage, version)
    with tempfile.TemporaryDirectory() as tmp: frame, fids, audit = _decode(body, Path(tmp)); parquet = _parquet(frame, fids)
    identity = processing_identity(Path(__file__).parents[3]); evidence_sha = _sha(_canonical({"body": receipt["sha256"], "wire": receipt["wire_sha256"], "receipt": receipt}))
    audit.update({"source_capture_evidence_sha256": evidence_sha, "source_receipt": receipt, "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    cv = "v" + _sha(_canonical({"archive_sha256": EXPECTED_ZIP_SHA256, "record_sha256": audit["source_record_sha256"], "geometry_sha256": audit["source_geometry_sha256"], "schema_sha256": audit["source_schema_sha256"], "identifier_sha256": audit["source_id_sha256"], "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, cv); op = f"geoparquet/{FILE_SLUG}.parquet"
    inp = {"input_id": "usgs_2023_nshm_uniform_hazard_maps", "producer": "U.S. Geological Survey - Geologic Hazards Science Center", "host": urlsplit(SOURCE_URL).hostname, "role": "producer_publication", "canonical_url": SOURCE_ITEM_URL, "retrieval_url": SOURCE_URL, "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"], "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"], "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"], "release_date": SOURCE_PUBLICATION, "release_date_basis": "Producer-declared FGDC citation pubdate 20231228 (HazardMapsAndGISdata_2023.xml, item metadata); ScienceBase item publication date 2023-12-21.", "package": SOURCE_PACKAGE, "package_doi": SOURCE_DOI, "license": "Public domain (work of the U.S. Federal Government); rights acceptance pending", "attribution": "Petersen, M.D., and others, 2023, Data Release for the 2023 U.S. 50-State National Seismic Hazard Model - Overview: U.S. Geological Survey data release, https://doi.org/10.5066/P9GNPCOD"}
    manifest = {"schema_version": 1, "approved": False, "title": "Uniform-Hazard Ground Motion - US SA1P0 5Pct50Yrs BC poly (2023 USGS NSHM contour polygons)", "description": "Pinned historical USGS-hosted US_SA1P0_5Pct50Yrs_BC_poly contour-band polygon shapefile (1.0 s spectral acceleration, 5% in 50 years, NEHRP site class B/C, VS30 760 m/s) from the 2023 U.S. 50-State National Seismic Hazard Model uniform-hazard map package. Complete 4014-record population with all native attributes, native WGS84 geometry preserved verbatim (including 87 antimeridian MultiPolygons and 7 natively self-intersecting rings left unrepaired). Not represented as a current release or accepted HIFLD successor.", "publisher": "U.S. Geological Survey - Geologic Hazards Science Center", "agency": "U.S. Department of the Interior", "source_url": SOURCE_URL, "license": None, "source_modified": None, "date_issued": SOURCE_PUBLICATION, "date_modified": SOURCE_ITEM_PUBLICATION, "processor": "HIFLD Next", "code_commit": identity["code_commit"], "inputs": [inp], "transformations": [{"operation": "Native DBF attributes and EPSG:4326 polygon shapefile to GeoParquet", "parameters": {"source_fields": "all native DBF fields preserved in order, source types and values", "source_ids": "source shapefile record numbers (FID 1..4014) carried as the explicit source_record_number identifier column in source row order; no synthetic identifiers", "geometry": "native WGS84 polygons retained without reprojection or repair; per-feature native WKB digests pinned", "model_lineage": "2023 U.S. 50-State National Seismic Hazard Model (NSHM); uniform-hazard ground motion maps on a 0.2-degree grid for CONUS/Alaska and 0.02-degree for Hawaii", "supersession_note": "Revised 2023 NSHM releases (CONUS 2023.R1/R2, ScienceBase, published 2026-08-10, Powers and others 2026) supersede the model's hazard values with hazard curves and uniform-hazard values (map.csv) on a 0.05-degree grid and publish no successor polygon-map shapefiles; this candidate captures the only official publication of the polygon map product.", "candidate_status": "historical, unapproved; equivalence/currentness unresolved"}}], "lineage": "USGS Geologic Hazards Science Center 2023 U.S. 50-State National Seismic Hazard Model (Petersen and others, 2024, Earthquake Spectra 40(1):5-88; data release DOI 10.5066/P9GNPCOD, ScienceBase item 64ff886dd34ed30c2057b4d9, published 2023-12-21, FGDC metadata citation pubdate 2023-12-28). The pinned package US_SA1P0_2Pct_5Pct_10Pct_50Yrs_BC.zip carries the complete contour-band map shapefiles for 1.0 s spectral acceleration at 2/5/10% in 50 years for site class B/C; this candidate captures its 5%-in-50-years polygon shapefile (4014 records). The 2026-08-10 Revised 2023 NSHM releases (CONUS 2023.R1/R2) supersede the model values with CSV hazard curves and map.csv point values on a finer grid and include no polygon-map shapefiles, so the pinned package remains the only official publication of this product. HIFLD inventory row uniform-hazard-ground-motion/us-sa1p0-5pct50yrs-bc-poly (DataLumos 240441 copy is research context only, never a pipeline input).","prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"All source fields, native contour-band polygon geometry with holes and antimeridian MultiPolygons retained verbatim, and validated native record numbers as the explicit identifier column.","objects":[{"path":op,"bytes":len(parquet),"sha256":_sha(parquet)}]}}},"unsupported_output_formats":{"geopackage":"No published contract/mapping approved","pmtiles":"No display transformation approved"},"promotion_allowed":False,"source_accepted":False,"baseline":None}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    outputs = {op: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n", "metadata/source_audit.json": _canonical(audit) + b"\n", "metadata/processing_identity.json": _canonical(identity) + b"\n", "metadata/candidate_ready.json": _canonical({"approved": False, "version": cv, "objects": {op: {"bytes": len(parquet), "sha256": _sha(parquet)}}, "source_population_complete": True, "source_release_accepted": False, "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False, "coverage_accepted": False, "release_accepted": False}) + b"\n", "raw/usgs_zip.body": body, "raw/usgs_zip.wire": wire, "raw/usgs_zip.receipt.json": _canonical(receipt) + b"\n"}
    state = f"_construction-state/{DATASET_SLUG}/{cv}.json"; fingerprint = _sha(_canonical({"identity": identity, "archive_sha256": EXPECTED_ZIP_SHA256, "record_sha256": audit["source_record_sha256"], "schema_sha256": audit["source_schema_sha256"]}))
    if storage.object_snapshot(state) is not None:
        original = json.loads(load_first_construction(storage, state, cv, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("objects"), dict): raise ValueError("cached_candidate_state_invalid")
        verify_cached_candidate(storage, Path(__file__).parents[3], state, prefix, cv, fingerprint, identity, audit["source_record_sha256"], audit["source_schema_sha256"], original["objects"], original.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": cv, "feature_count": EXPECTED_COUNT, "source_population_complete": True, "source_id_sha256": ID_SHA, "source_schema_sha256": audit["source_schema_sha256"], "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True, "source_capture_evidence_sha256": evidence_sha, "capture_directory": str(root), "release_accepted": False}
    descriptors = {}
    for n, b in outputs.items():
        key = f"{prefix}/{n}"; write_immutable(storage, key, b); snap = storage.object_snapshot(key)
        if snap is None or snap.size != len(b) or storage.sha256_key(key) != _sha(b): raise ValueError("candidate_object_readback_failed:" + n)
        descriptors[n] = {"bytes": len(b), "sha256": _sha(b)}
    seal_ready_candidate(storage, state, cv, fingerprint, {"processing_identity": identity, "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"], "capture_receipts": {"usgs_zip": receipt}, "objects": descriptors})
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
    )
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": cv, "feature_count": EXPECTED_COUNT, "source_population_complete": True, "source_id_sha256": ID_SHA, "source_schema_sha256": audit["source_schema_sha256"], "source_record_sha256": audit["source_record_sha256"], "source_geometry_sha256": audit["source_geometry_sha256"], "source_coverage_scope": audit["source_coverage_scope"], "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False, "source_capture_evidence_sha256": evidence_sha, "capture_directory": str(root), "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False, "release_accepted": False}
