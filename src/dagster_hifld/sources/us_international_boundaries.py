"""Current Census TIGER/Line international boundary candidate for issue #367.

The producing agency is the U.S. Census Bureau, Geography Division. The DHS HIFLD
Open crosswalk deprecates the old GeoPlatform hosting and names the Census
TIGER/Line international boundary shapefile as the maintained producer
publication. The adapter resolves the newest published annual vintage from the
producer's own index at capture time; retained replay derives the vintage from
its receipt URL. No mirror or archive host is ever fetched.
"""
from __future__ import annotations


import io, json, math, shutil, struct, tempfile, zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
import geopandas as gpd
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources import census_latest, census_tiger_common
from dagster_hifld.sources.capture_common import (
    candidate_prefix,
    capture_get,
    processing_identity,
    retain_failed_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
    seal_candidate_objects,
    write_candidate_outputs,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = FILE_SLUG = "us-international-boundaries-1"
LAYER = "INTERNATIONALBOUNDARY"
PRODUCT = "internationalboundary"
EXPECTED_FIELDS = [("IBTYPE", "C", 1, 0), ("IBSTATUS", "C", 1, 0), ("MTFCC", "C", 5, 0)]
FEATURE_COUNT_RANGE = (1, 100_000)
EXPECTED_VALUE_SETS = {"IBTYPE": ("A", "C", "M"), "IBSTATUS": ("U", "V"),
                       "MTFCC": ("H1100", "H3010", "H3020", "P0001", "S1400", "S1740")}
SOURCE_CRS = CRS.from_epsg(4269)


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
        d = b[o:o+32]
        if len(d) != 32: raise ValueError("source_dbf_descriptor_incomplete")
        out.append((d[:11].split(b"\0", 1)[0].decode("ascii"), chr(d[11]), d[16], d[17])); o += 32
    if o >= h or b[o] != 0x0d: raise ValueError("source_dbf_terminator_missing")
    return out

def _paths(geom):
    if geom.geom_type == "LineString": return [list(geom.coords)]
    if geom.geom_type == "MultiLineString": return [list(part.coords) for part in geom.geoms]
    raise ValueError("source_geometry_type_drift")

def _linestring_wkb(points):
    out = bytearray(struct.pack("<BI", 1, 2)); out.extend(struct.pack("<I", len(points)))
    for x, y in points: out.extend(struct.pack("<dd", float(x), float(y)))
    return bytes(out)

def _multilinestring_wkb(paths):
    out = bytearray(struct.pack("<BI", 1, 5)); out.extend(struct.pack("<I", len(paths)))
    for path in paths:
        out.extend(struct.pack("<BI", 1, 2)); out.extend(struct.pack("<I", len(path)))
        for x, y in path: out.extend(struct.pack("<dd", float(x), float(y)))
    return bytes(out)

def _decode(body, tmp, release):
    layer = Path(release.source_file).stem
    members = census_tiger_common.tiger_shapefile_members(release.source_file)
    try:
        zf = zipfile.ZipFile(io.BytesIO(body))
    except zipfile.BadZipFile as exc:
        raise ValueError("source_zip_invalid") from exc
    with zf as z:
        names = []
        for i in z.infolist():
            mode = i.external_attr >> 16; parts = Path(i.filename).parts
            if not i.filename or i.filename.startswith(("/", "\\")) or "\\" in i.filename or ".." in parts or len(parts) != 1 or i.is_dir() or (mode & 0o170000) == 0o120000: raise ValueError("source_zip_member_path_rejected")
            names.append(i.filename)
        if len(set(names)) != len(names) or set(names) != members or sum(i.file_size for i in z.infolist()) > 50_000_000: raise ValueError("source_zip_member_roster_drift")
        root = tmp / "shape"; root.mkdir()
        for name in members:
            with z.open(name) as src, (root / name).open("xb") as dst: shutil.copyfileobj(src, dst)
    shp = root / f"{layer}.shp"
    if (root / f"{layer}.cpg").read_bytes() != b"UTF-8": raise ValueError("source_cpg_encoding_drift")
    if _schema_dbf(root / f"{layer}.dbf") != EXPECTED_FIELDS: raise ValueError("source_dbf_schema_drift")
    import pyogrio
    info = pyogrio.read_info(shp)
    if info["layer_name"] != layer or info["crs"] != "EPSG:4269": raise ValueError("source_layer_metadata_drift")
    g = pyogrio.read_dataframe(shp, fid_as_index=True)
    names_ = [x[0] for x in EXPECTED_FIELDS]
    if list(g.columns) != names_ + ["geometry"]: raise ValueError("source_population_or_schema_drift")
    if not (FEATURE_COUNT_RANGE[0] <= len(g) <= FEATURE_COUNT_RANGE[1]): raise ValueError("source_population_or_schema_drift")
    if not CRS.from_user_input(g.crs).equals(SOURCE_CRS, ignore_axis_order=True): raise ValueError("source_crs_drift")
    if list(g.index) != list(range(len(g))): raise ValueError("source_record_order_drift")
    if g.geometry.isna().any() or g.geometry.is_empty.any(): raise ValueError("source_missing_or_empty_geometry_drift")
    types = {geom_type: 0 for geom_type in ("LineString", "MultiLineString")}; vertices = 0
    for geom in g.geometry:
        types[geom.geom_type] = types.get(geom.geom_type, 0) + 1
        paths = _paths(geom)
        for path in paths:
            if len(path) < 2 or any(len(point) != 2 or not math.isfinite(point[0]) or not math.isfinite(point[1]) for point in path): raise ValueError("source_path_invalid")
            vertices += len(path)
    if types.get("MultiLineString", 0) and not types.get("LineString", 0):
        raise ValueError("source_geometry_type_drift")
    if vertices <= 0: raise ValueError("source_vertex_count_drift")
    for name in names_:
        if g[name].isna().any(): raise ValueError("source_attribute_null_drift:" + name)
        if not all(isinstance(v, str) and v != "" for v in g[name].tolist()): raise ValueError("source_attribute_text_drift:" + name)
        observed = tuple(sorted(set(g[name].tolist())))
        if observed != EXPECTED_VALUE_SETS[name]: raise ValueError("source_attribute_value_set_drift:" + name)
    ids = [int(index) + 1 for index in g.index]
    attrs = [[fid, *[_native(v) for v in g.iloc[i][names_].tolist()]] for i, fid in enumerate(ids)]
    geoms = [[fid, {"paths": _paths(geom)}] for fid, geom in zip(ids, g.geometry)]
    idsha, recsha, geomsha = (_sha(_canonical(x)) for x in (sorted(ids), attrs, geoms))
    bounds = list(map(float, g.total_bounds))
    if not (-180 <= bounds[0] <= bounds[2] <= 180 and -90 <= bounds[1] <= bounds[3] <= 90):
        raise ValueError("source_bounds_drift")
    audit = {"source_feature_count": len(g), "source_population_complete": True, "source_id_field": "__source_fid", "source_id_kind": "1-based physical shapefile record number; explicit proposed identifier, unapproved; the TIGER/Line international boundary extract publishes no native unique identifier column", "source_id_unique": True, "source_id_min": min(ids), "source_id_max": max(ids), "source_id_sha256": idsha, "source_record_sha256": recsha, "source_geometry_sha256": geomsha, "source_schema_sha256": _sha(_canonical(EXPECTED_FIELDS)), "source_dbf_schema": EXPECTED_FIELDS, "source_crs": "EPSG:4269", "source_geometry_type": "native per-record NAD83 polyline", "source_linestring_count": types["LineString"], "source_multilinestring_count": types["MultiLineString"], "source_vertex_count": vertices, "source_bounds": bounds, "source_vintage": str(release.vintage), "source_release": release.as_metadata(), "source_value_sets": {name: list(EXPECTED_VALUE_SETS[name]) for name in names_}, "source_null_counts": {n: int(g[n].isna().sum()) for n in names_}, "source_geometry_null_count": int(g.geometry.isna().sum()), "source_geometry_empty_count": int(g.geometry.is_empty.sum()), "source_coverage_scope": f"Complete current Census TIGER/Line {release.vintage} national international boundary extract: {len(g):,} NAD83 polyline segments of the United States boundaries with Canada and Mexico per the producer ISO 19115 metadata. Census re-segments this product annually, so historical releases are preserved per vintage and the resolved vintage is replaced by the next annual release.", "drift_guard": "Official producer listing selection, complete zip member roster, CPG encoding, exact DBF widths/types, layer name, CRS, attribute values, nullability and value sets, contiguous record order, per-record polyline structure, vertex presence and native vertex bounds."}
    return g, audit

def _geometry_wkb(geom):
    paths = _paths(geom)
    if geom.geom_type == "LineString": return _linestring_wkb(paths[0])
    return _multilinestring_wkb(paths)

def _parquet(g):
    arrays = []; fields = []
    for name, kind, *_ in EXPECTED_FIELDS:
        vals = [_native(v) for v in g[name].tolist()]
        arrays.append(pa.array(vals, type=pa.string(), safe=True)); fields.append(pa.field(name, pa.string(), nullable=False))
    arrays.append(pa.array([fid for fid in range(1, len(g) + 1)], type=pa.int64())); fields.append(pa.field("__source_fid", pa.int64(), nullable=False))
    arrays.append(pa.array([_geometry_wkb(geom) for geom in g.geometry], type=pa.binary())); fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["LineString", "MultiLineString"], "crs": SOURCE_CRS.to_json_dict()}}}
    sink = io.BytesIO(); pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata={b"geo": _canonical(geo)})), sink, compression="zstd", use_dictionary=False)
    t = pq.read_table(pa.BufferReader(sink.getvalue()))
    if t.num_rows != len(g): raise ValueError("candidate_row_count_readback_drift")
    for name, *_ in EXPECTED_FIELDS:
        if t[name].to_pylist() != [_native(v) for v in g[name].tolist()]: raise ValueError("candidate_attribute_readback_drift:" + name)
    if t["__source_fid"].to_pylist() != list(range(1, len(g) + 1)): raise ValueError("candidate_source_fid_readback_drift")
    if any(t["geometry"][i].as_py() != _geometry_wkb(geom) for i, geom in enumerate(g.geometry)): raise ValueError("candidate_geometry_readback_drift")
    return sink.getvalue()

def _load_capture(root, release):
    paths = [root / "census_internationalboundary_zip.body", root / "census_internationalboundary_zip.wire", root / "census_internationalboundary_zip.receipt.json"]
    if any(p.is_symlink() for p in paths): raise ValueError("capture_symlink_rejected")
    body, wire, receipt = paths[0].read_bytes(), paths[1].read_bytes(), json.loads(paths[2].read_bytes())
    if receipt.get("method") != "GET" or receipt.get("url") != release.source_url or receipt.get("final_url") != release.source_url or receipt.get("complete_body") is not True or receipt.get("status") != 200 or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body) or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire): raise ValueError("capture_receipt_invalid")
    return body, wire, receipt

def _capture(root, storage, version):
    mp = root / "capture_manifest.json"
    if root.is_symlink() or any(p.is_symlink() for p in root.parents): raise ValueError("unsafe_capture_directory")
    if mp.exists():
        m = json.loads(mp.read_bytes())
        if m.get("complete") is not True or set(m.get("receipts", {})) != {"census_internationalboundary_zip"}: raise ValueError("capture_manifest_invalid")
        roster = m.get("artifact_roster"); validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
        if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}): raise ValueError("capture_roster_digest_drift")
        receipt = m["receipts"]["census_internationalboundary_zip"]
        release = census_tiger_common.shapefile_release_from_receipt(receipt, layer=LAYER, product=PRODUCT)
        body, wire, receipt = _load_capture(root, release)
        if receipt != m["receipts"]["census_internationalboundary_zip"]: raise ValueError("capture_manifest_receipt_drift")
        return body, wire, receipt, release
    if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")): raise ValueError("unsealed_capture_attempt")
    root.mkdir(parents=True, exist_ok=True); caps = {}; release = None
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(60, connect=15), headers={"User-Agent": "hifld-next-datasets/us-international-boundaries-draft"}) as client:
            release = census_latest.discover_shapefile(client, root, LAYER, PRODUCT)
            body, receipt = capture_get(client, release.source_url, root, "census_internationalboundary_zip", max_bytes=10_000_000)
        if receipt.get("method") != "GET" or receipt.get("url") != release.source_url or receipt.get("final_url") != release.source_url: raise ValueError("source_request_identity_mismatch")
        caps["census_internationalboundary_zip"] = (body, receipt)
        with tempfile.TemporaryDirectory(dir=root) as tmp: _decode(body, Path(tmp), release)
        mp.write_bytes(_canonical({"complete": True, "captured_at": datetime.now(timezone.utc).isoformat(), "receipts": {"census_internationalboundary_zip": receipt}, "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        return body, (root / "census_internationalboundary_zip.wire").read_bytes(), receipt, release
    except Exception as exc:
        retain_failed_capture(storage, root, DATASET_SLUG, version, exc, error=ValueError)
        mp.write_bytes(_canonical({"complete": False, "captured_at": datetime.now(timezone.utc).isoformat(), "error_code": type(exc).__name__}) + b"\n")
        raise

def capture_us_international_boundaries(storage: StagingStorageResource, *, version: str | None = None, capture_directory: str | None = None) -> dict:
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix: raise ValueError("explicit_local_staging_resource_required")
    if version is not None and (not isinstance(version, str) or not version or Path(version).name != version or "/" in version or "\\" in version): raise ValueError("unsafe_capture_version")
    stage = Path(storage.local_dir).absolute()
    if ".." in stage.parts or any(p.is_symlink() for p in (stage, *stage.parents)): raise ValueError("unsafe_local_staging_root")
    stage.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(stage).free < 300_000_000: raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    root = requested or default_capture_dir("hifld-native-reconstruction", "367", "captures") / ((version or "pending") + "-" + uuid4().hex)
    body, wire, receipt, release = _capture(root, storage, version or "pending")
    with tempfile.TemporaryDirectory() as tmp: frame, audit = _decode(body, Path(tmp), release); parquet = _parquet(frame)
    identity = processing_identity(Path(__file__).parents[3]); evidence_sha = _sha(_canonical({"body": receipt["sha256"], "wire": receipt["wire_sha256"], "receipt": receipt}))
    audit.update({"source_capture_evidence_sha256": evidence_sha, "source_receipt": receipt, "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False, "release_accepted": False})
    cv = version or ("v" + _sha(_canonical({"archive_sha256": receipt["sha256"], "record_sha256": audit["source_record_sha256"], "geometry_sha256": audit["source_geometry_sha256"], "schema_sha256": audit["source_schema_sha256"], "processing_identity": identity}))[:32])
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, cv); op = f"geoparquet/{FILE_SLUG}.parquet"
    vintage = str(release.vintage)
    inp = {"input_id": f"census_official_tiger{vintage}_internationalboundary_zip", "producer": "U.S. Census Bureau, Geography Division", "host": urlsplit(release.source_url).hostname, "role": "producer_publication", "canonical_url": release.source_url, "retrieval_url": release.source_url, "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"], "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"], "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"], "release_date_unknown_reason": f"The producer identifies this as the {vintage} TIGER/Line vintage; a precise publication date was not established. The HTTP Last-Modified value is retained in the receipt and is not treated as the producer data release date.", "license": "TIGER/Line Shapefile products are not copyrighted per producer metadata; TIGER/Line and Census TIGER are registered trademarks. Rights acceptance pending.", "attribution": "U.S. Census Bureau, Geography Division; rights acceptance pending"}
    manifest = {"schema_version": 1, "approved": False, "title": f"US International Boundaries (Census TIGER/Line {vintage} international boundary, current annual vintage)", "description": f"Current Census TIGER/Line Shapefile, {vintage}, Nation, U.S., International Boundaries ({Path(release.source_file).stem}): {len(frame):,} NAD83 polyline segments of the United States boundaries with Canada and Mexico. Unapproved faithful candidate from the producing agency's own maintained publication; the annual vintage is replaced by the next TIGER/Line release and historical releases are preserved per version.", "publisher": "U.S. Department of Commerce, U.S. Census Bureau, Geography Division", "agency": "U.S. Department of Commerce, U.S. Census Bureau", "source_url": release.source_url, "license": "TIGER/Line Shapefile products are not copyrighted per producer metadata; TIGER/Line and Census TIGER are registered trademarks. Rights acceptance pending.", "source_modified": None, "date_issued": None, "date_modified": vintage, "processor": "HIFLD Next", "code_commit": identity["code_commit"], "code_identity_scheme": identity["code_identity_scheme"], "inputs": [inp], "transformations": [{"operation": "Native DBF attributes and NAD83 polyline shapefile to GeoParquet", "parameters": {"source_fields": "all native DBF fields preserved in order, source types and values (IBTYPE/IBSTATUS/MTFCC as captured text codes)", "source_ids": "the extract publishes no native unique identifier column; __source_fid (1-based physical shapefile record number) retained as an explicit proposed identifier, unapproved", "geometry": "native NAD83 (EPSG:4269) polyline vertices and record order retained without reprojection, repair or coordinate edit; WKB candidate at EPSG:4269", "source_vintage": vintage, "candidate_status": "unapproved; current-vintage succession, baseline, schema/ID/coverage, rights and release acceptance pending"}}], "lineage": "DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1) marks this layer Deprecate ('No longer supported as part of the HIFLD mission') and names the Census Bureau TIGER/Line international boundary product as the external landing page. The current maintained annual vintage is resolved from the producer index at capture time. Producer lineage per the ISO 19115 metadata retained in the archive: Census MAF/TIGER data realigned with International Boundary Commission (Canada) and IBWC (Mexico) source files, with the NOAA Coast Survey representation filling the Cameron County-Gulf of Mexico gap. The retired GeoPlatform/HIFLD Open hosting (item 12a56832b66c43699902c2af367ba2e6, services1 FeatureServer) is dead (CONT_0001 / Invalid URL) and is never fetched; DataLumos/ICPSR, PASDA, Esri and other mirrors are excluded by source policy and not fetched.", "source_release": release.as_metadata(), "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "All source fields, native NAD83 polyline geometry and the explicit proposed __source_fid record identifier are retained.", "objects": [{"path": op, "bytes": len(parquet), "sha256": _sha(parquet)}]}}}, "unsupported_output_formats": {"geopackage": "No published contract/mapping approved.", "pmtiles": "No display transformation approved.", "shapefile_zip": "Native source shapefile retained only inside the raw archive body."}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    outputs = {op: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n", "metadata/source_audit.json": _canonical(audit) + b"\n", "metadata/processing_identity.json": _canonical(identity) + b"\n", "metadata/candidate_ready.json": _canonical({"approved": False, "version": cv, "objects": {op: {"bytes": len(parquet), "sha256": _sha(parquet)}}, "source_population_complete": True, "source_release_accepted": False, "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False, "coverage_accepted": False, "release_accepted": False}) + b"\n", "raw/census_internationalboundary_zip.body": body, "raw/census_internationalboundary_zip.wire": wire, "raw/census_internationalboundary_zip.receipt.json": _canonical(receipt) + b"\n"}
    state = f"_construction-state/{DATASET_SLUG}/{cv}.json"; fingerprint = _sha(_canonical({"identity": identity, "archive_sha256": receipt["sha256"], "record_sha256": audit["source_record_sha256"], "schema_sha256": audit["source_schema_sha256"]}))
    if storage.object_snapshot(state) is not None:
        original = json.loads(load_first_construction(storage, state, cv, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("objects"), dict): raise ValueError("cached_candidate_state_invalid")
        verify_cached_candidate(storage, Path(__file__).parents[3], state, prefix, cv, fingerprint, identity, audit["source_record_sha256"], audit["source_schema_sha256"], original["objects"], original.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": cv, "feature_count": len(frame), "source_population_complete": True, "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"], "source_vintage": vintage, "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True, "source_capture_evidence_sha256": evidence_sha, "capture_directory": str(root), "release_accepted": False}
    descriptors = write_candidate_outputs(storage, prefix, outputs, snapshot_check="content")
    seal_candidate_objects(
        storage, state, cv,
        identity, {'census_internationalboundary_zip': receipt}, descriptors,
        source_record_sha256=audit['source_record_sha256'],
        source_schema_sha256=audit['source_schema_sha256'],
        fingerprint=fingerprint)
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
    )
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": cv, "feature_count": len(frame), "source_population_complete": True, "source_id_sha256": audit["source_id_sha256"], "source_record_sha256": audit["source_record_sha256"], "source_geometry_sha256": audit["source_geometry_sha256"], "source_vintage": vintage, "source_release": release.as_metadata(), "source_coverage_scope": audit["source_coverage_scope"], "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False, "source_capture_evidence_sha256": evidence_sha, "capture_directory": str(root), "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False, "release_accepted": False}
