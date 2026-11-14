"""Pinned historical EIA Strategic Petroleum Reserves GIS candidate for issue #327.

The HIFLD Open inventory entry ``strategic-petroleum-reserves`` ("Strategic
Petroleum Reserves", agency U.S. Department of Energy, description "These data
were obtained from the U.S. Dept. of Energy and publicly available
information.") is served here from EIA's own static producer distribution,
``https://www.eia.gov/maps/map_data/SPR_US_EIA.zip`` (HTTP 200 anonymous,
last-modified 2022-02-03, 5,106 bytes). The DHS HIFLD Open crosswalk (DOI
10.3886/E241367V1; byte-identical public copy sha256 7f3394fed2712fdcc176b7ce
658293fdded656b9b719f0d90154c07dc38772bc) row for this layer (Status Deprecate,
DUA/GII not required, Agency "US Energy Atlas", Open ID
``6bb5f32811f64ca4aed9c50bf3d444e5``) directs the layer to EIA's own ArcGIS
Online organization's ``SPR_US_EIA`` FeatureServer layer 42, which answers
anonymous requests with ``HTTP 499 GWM_0003 Token Required`` (probed
2026-11-14); the same product's EIA-hosted ``map_data`` zip is the remaining
official anonymous publication of the layer, consistent with the merged EIA
sibling modules (power plants, biodiesel plants, hydrocarbon gas liquid
pipelines, petroleum product terminals). The DataLumos project 240636 archive
copy named by the issue is a banned mirror and is never fetched.

The zip contains the producer's ``SPR_202101`` shapefile: 4 EPSG:4326 point
features (the four Gulf Coast Strategic Petroleum Reserve storage sites: Big
Hill TX, Bryan Mound TX, West Hackberry LA, Bayou Choctaw LA), every native
DBF field retained in producer order with source types and values, per-row
``Source`` "DOE, Office of Fossil Energy" and ``Data_Perio`` "January 2021".
Embedded EIA metadata: "This is a point dataset representing strategic
petroleum reserves as of January 1, 2021. These data were obtained from the
U.S. Dept. of Energy: http://www.fossil.energy.gov/programs/reserves/spr/
spr-sites.html"; metadata date 2022-02-02/03. Producer labeling quirk retained
verbatim: the layer's DBF aliases name ``POINT_X`` "LATITUDE" and ``POINT_Y``
"LONGITUDE", but the attribute values and shapefile geometry agree that
``POINT_X`` carries longitude and ``POINT_Y`` carries latitude; attribute
names, aliases, values and geometry are preserved unchanged (no relabeling,
reprojection or repair). The Northeast Home Heating Oil Reserve is a separate
HIFLD inventory row and is not represented here. This is an unapproved
historical draft candidate: currentness, freshness, schema/ID contracts,
coverage, rights and release acceptance remain pending; all runtime approval
flags are false and the draft refresh schedule is stopped.
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
from pyproj import CRS
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import candidate_prefix, capture_get, processing_identity, quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable

DATASET_SLUG = FILE_SLUG = "strategic-petroleum-reserves"
SOURCE_URL = "https://www.eia.gov/maps/map_data/SPR_US_EIA.zip"
# The zip's DBF Data_Perio is "January 2021" on every row and the embedded EIA
# metadata states "as of January 1, 2021"; the file's Last-Modified is
# 2022-02-03 (observed 2026-11-14) and the metadata date is 2022-02-02/03.
SOURCE_RELEASE = "2021-01"
SOURCE_LAST_MODIFIED = "2022-02-03"
CROSSWALK_OPEN_ID = "6bb5f32811f64ca4aed9c50bf3d444e5"
CROSSWALK_COPY_SHA256 = ("7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38"
                         "772bc")
CROSSWALK_DIRECTED_SERVICE_URL = ("https://services7.arcgis.com/FGr1D95XCGALKXqM/arcgis"
                                  "/rest/services/SPR_US_EIA/FeatureServer/42")
CROSSWALK_GATE_NOTE = (
    "The crosswalk row's Open REST Service (EIA's own Energy Atlas FeatureServer "
    "SPR_US_EIA/42) answers anonymous requests with HTTP 499 GWM_0003 Token Required "
    "(probed 2026-11-14); this capture uses the producer's remaining official "
    "anonymous distribution, the EIA-hosted map_data zip of the same layer.")
EXPECTED_LAYER = "SPR_202101"
EXPECTED_ZIP_BYTES = 5106
EXPECTED_ZIP_SHA256 = "40ef9584d910603980eb1782672136125eb57f749c158e38b5f9174242da4481"
EXPECTED_COUNT = 4
EXPECTED_FIELDS = [
    ("OBJECTID", "N", 10, 0), ("Name", "C", 100, 0), ("POINT_X", "F", 19, 11),
    ("POINT_Y", "F", 19, 11), ("Source", "C", 50, 0), ("Capacity", "F", 19, 11),
    ("Caverns", "N", 5, 0), ("PADD", "N", 5, 0), ("Data_Perio", "C", 25, 0),
    ("State", "C", 20, 0), ("PADD_temp", "N", 10, 0), ("Caverns_te", "N", 10, 0),
]
MEMBERS = {f"{EXPECTED_LAYER}.{suffix}" for suffix in
           ("cpg", "dbf", "prj", "sbn", "sbx", "shp", "shp.xml", "shx")}
ID_SHA = "f6bd10506e9a4daed7c03eda2f2fde54be3bd58eee49dab471c18a888ffbdb6f"
RECORD_SHA = "f4cd84de71e9b8aa82c594f9ecf04846b48bf9f262f7b898524a3ea21036db09"
GEOMETRY_SHA = "f5c1a5e6875b554a98bcf06ec17f3751bd0643755dd96899c34f68b0a069a3be"
BOUNDS = [-95.37706908682291, 28.916479555697496, -91.30824488297226, 30.31664175636736]
# SHP doubles differ from the 11-decimal-place DBF attribute coordinates by a
# few floating-point ulps (observed maxima pinned below from the capture).
EXPECTED_XY_MAX_DELTA = 8.69732730279793e-10
WGS84 = CRS.from_epsg(4326)


def _canonical(x):
    return json.dumps(x, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _sha(b):
    return hashlib.sha256(b).hexdigest()


def _native(v):
    if hasattr(v, "item"):
        v = v.item()
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, float) and not math.isfinite(v):
        raise ValueError("source_nonfinite_attribute")
    return v


def _schema_dbf(path):
    b = path.read_bytes()
    h = int.from_bytes(b[8:10], "little")
    if h < 33 or h > len(b):
        raise ValueError("source_dbf_header_invalid")
    out = []
    o = 32
    while o < h and b[o] != 0x0d:
        d = b[o:o + 32]
        if len(d) != 32:
            raise ValueError("source_dbf_descriptor_incomplete")
        out.append((d[:11].split(b"\0", 1)[0].decode("ascii"), chr(d[11]), d[16], d[17]))
        o += 32
    if o >= h or b[o] != 0x0d:
        raise ValueError("source_dbf_terminator_missing")
    return out


def _decode(body, tmp):
    if len(body) != EXPECTED_ZIP_BYTES or _sha(body) != EXPECTED_ZIP_SHA256:
        raise ValueError("pinned_eia_snapshot_drift")
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        names = []
        for i in z.infolist():
            mode = i.external_attr >> 16
            parts = Path(i.filename).parts
            if (not i.filename or i.filename.startswith(("/", "\\")) or "\\" in i.filename
                    or ".." in parts or len(parts) != 1 or i.is_dir()
                    or (mode & 0o170000) == 0o120000):
                raise ValueError("source_zip_member_path_rejected")
            names.append(i.filename)
        if len(set(names)) != len(names) or set(names) != MEMBERS \
                or sum(i.file_size for i in z.infolist()) > 50_000_000:
            raise ValueError("source_zip_member_roster_drift")
        root = tmp / "shape"
        root.mkdir()
        for name in MEMBERS:
            with z.open(name) as src, (root / name).open("xb") as dst:
                shutil.copyfileobj(src, dst)
    shp = root / f"{EXPECTED_LAYER}.shp"
    if _schema_dbf(root / f"{EXPECTED_LAYER}.dbf") != EXPECTED_FIELDS:
        raise ValueError("source_dbf_schema_drift")
    import pyogrio
    info = pyogrio.read_info(shp)
    if (info["layer_name"] != EXPECTED_LAYER or info["geometry_type"] != "Point"
            or info["crs"] != "EPSG:4326" or int(info["features"]) != EXPECTED_COUNT):
        raise ValueError("source_layer_metadata_drift")
    g = gpd.read_file(shp, engine="pyogrio")
    names = [x[0] for x in EXPECTED_FIELDS]
    if (list(g.columns) != names + ["geometry"] or len(g) != EXPECTED_COUNT
            or g.geometry.isna().any() or g.geometry.is_empty.any()
            or not (g.geometry.geom_type == "Point").all()):
        raise ValueError("source_population_or_schema_drift")
    if not CRS.from_user_input(g.crs).equals(WGS84, ignore_axis_order=True):
        raise ValueError("source_crs_drift")
    if g["OBJECTID"].isna().any() or g["OBJECTID"].duplicated().any() \
            or sorted(int(v) for v in g["OBJECTID"]) != list(range(1, EXPECTED_COUNT + 1)):
        raise ValueError("source_id_drift")
    # The producer labels POINT_X "LATITUDE" and POINT_Y "LONGITUDE", but the
    # attribute values and the shapefile geometry agree that POINT_X carries
    # longitude and POINT_Y carries latitude; both are retained verbatim.
    if (abs(g.geometry.x.to_numpy() - g.POINT_X.to_numpy()) > EXPECTED_XY_MAX_DELTA).any() \
            or (abs(g.geometry.y.to_numpy() - g.POINT_Y.to_numpy()) > EXPECTED_XY_MAX_DELTA).any():
        raise ValueError("source_xy_attribute_drift")
    if (g["Data_Perio"] != "January 2021").any():
        raise ValueError("source_period_drift")
    if (g["Source"] != "DOE, Office of Fossil Energy").any():
        raise ValueError("source_provenance_attribute_drift")
    if (g["PADD"] != 3).any():
        raise ValueError("source_padd_drift")
    attrs = [[_native(v) for v in row] for row in g[names].itertuples(index=False, name=None)]
    ids = sorted(int(r[0]) for r in attrs)
    geom = [[int(r[0]), float(g.geometry.iloc[i].x), float(g.geometry.iloc[i].y)]
            for i, r in enumerate(attrs)]
    idsha, recsha, geomsha = (_sha(_canonical(x)) for x in (ids, attrs, geom))
    if idsha != ID_SHA or recsha != RECORD_SHA or geomsha != GEOMETRY_SHA:
        raise ValueError("source_content_snapshot_drift")
    bounds = list(map(float, g.total_bounds))
    if bounds != BOUNDS:
        raise ValueError("source_bounds_drift")
    audit = {"source_feature_count": len(g), "source_population_complete": True,
             "source_id_field": "OBJECTID", "source_id_unique": True,
             "source_id_min": min(ids), "source_id_max": max(ids),
             "source_id_sha256": idsha, "source_record_sha256": recsha,
             "source_geometry_sha256": geomsha,
             "source_schema_sha256": _sha(_canonical(EXPECTED_FIELDS)),
             "source_dbf_schema": EXPECTED_FIELDS, "source_crs": "EPSG:4326",
             "source_geometry_type": "Point", "source_bounds": bounds,
             "source_period": SOURCE_RELEASE,
             "source_last_modified": SOURCE_LAST_MODIFIED,
             "source_null_counts": {n: int(g[n].isna().sum()) for n in names},
             "source_geometry_null_count": int(g.geometry.isna().sum()),
             "source_geometry_empty_count": int(g.geometry.is_empty.sum()),
             "source_coverage_scope":
                 "Complete EIA January 2021 point layer of the four Gulf Coast Strategic "
                 "Petroleum Reserve storage sites (Big Hill TX, Bryan Mound TX, West "
                 "Hackberry LA, Bayou Choctaw LA) as published on EIA's own map_data "
                 "host. The Northeast Home Heating Oil Reserve is a separate HIFLD "
                 "inventory row and is not represented. Not a current feed and not an "
                 "accepted HIFLD equivalent.",
             "drift_guard":
                 "Pinned official archive checksum, complete zip member roster, exact "
                 "DBF widths/types/decimals, layer name, CRS, all attributes and values, "
                 "unique OBJECTID roster 1..4, Data_Perio/Source/PADD gates, native "
                 "shapefile XY within the pinned DBF-vs-SHP ulp delta and bounds.",
             "point_x_y_alias_note":
                 "The producer labels POINT_X 'LATITUDE' and POINT_Y 'LONGITUDE'; the "
                 "values and geometry agree POINT_X is longitude and POINT_Y is "
                 "latitude. Names, aliases, values and geometry are retained verbatim."}
    return g, audit


def _point_wkb(x, y):
    return struct.pack("<BIdd", 1, 1, float(x), float(y))


def _parquet(g):
    arrays = []
    fields = []
    for name, kind, *_ in EXPECTED_FIELDS:
        vals = [_native(v) for v in g[name].tolist()]
        dtype = pa.int64() if kind == "N" else pa.float64() if kind == "F" else pa.string()
        arrays.append(pa.array(vals, type=dtype, safe=True))
        fields.append(pa.field(name, dtype, nullable=True))
    arrays.append(pa.array([_point_wkb(x, y) for x, y in zip(g.geometry.x, g.geometry.y)],
                           type=pa.binary()))
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"],
                                    "crs": WGS84.to_json_dict()}}}
    sink = io.BytesIO()
    pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata={b"geo": _canonical(geo)})),
                   sink, compression="zstd", use_dictionary=False)
    table = pq.read_table(pa.BufferReader(sink.getvalue()))
    if table.num_rows != EXPECTED_COUNT:
        raise ValueError("candidate_row_count_readback_drift")
    for name, kind, *_ in EXPECTED_FIELDS:
        if table[name].to_pylist() != [_native(v) for v in g[name].tolist()]:
            raise ValueError("candidate_attribute_readback_drift:" + name)
    if any(table["geometry"][i].as_py() != _point_wkb(x, y)
           for i, (x, y) in enumerate(zip(g.geometry.x, g.geometry.y))):
        raise ValueError("candidate_geometry_readback_drift")
    return sink.getvalue()


def _load_capture(root):
    paths = [root / "eia_zip.body", root / "eia_zip.wire", root / "eia_zip.receipt.json"]
    if any(p.is_symlink() for p in paths):
        raise ValueError("capture_symlink_rejected")
    body, wire, receipt = (paths[0].read_bytes(), paths[1].read_bytes(),
                           json.loads(paths[2].read_bytes()))
    if (receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL
            or receipt.get("final_url") != SOURCE_URL or receipt.get("complete_body") is not True
            or receipt.get("status") != 200 or receipt.get("bytes") != len(body)
            or receipt.get("sha256") != _sha(body) or receipt.get("wire_bytes") != len(wire)
            or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("capture_receipt_invalid")
    return body, wire, receipt


def _capture(root, storage, version):
    manifest_path = root / "capture_manifest.json"
    if root.is_symlink() or any(p.is_symlink() for p in root.parents):
        raise ValueError("unsafe_capture_directory")
    if manifest_path.exists():
        summary = json.loads(manifest_path.read_bytes())
        if summary.get("complete") is not True or set(summary.get("receipts", {})) != {"eia_zip"}:
            raise ValueError("capture_manifest_invalid")
        roster = summary.get("artifact_roster")
        validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
        if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
            raise ValueError("capture_roster_digest_drift")
        body, wire, receipt = _load_capture(root)
        if receipt != summary["receipts"]["eia_zip"]:
            raise ValueError("capture_manifest_receipt_drift")
        return body, wire, receipt
    if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")):
        raise ValueError("unsealed_capture_attempt")
    root.mkdir(parents=True, exist_ok=True)
    captures = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(60, connect=15),
                          headers={"User-Agent": "hifld-next-datasets/spr-draft"}) as client:
            body, receipt = capture_get(client, SOURCE_URL, root, "eia_zip", max_bytes=10_000_000)
        if (receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL
                or receipt.get("final_url") != SOURCE_URL):
            raise ValueError("source_request_identity_mismatch")
        captures["eia_zip"] = (body, receipt)
        with tempfile.TemporaryDirectory(dir=root) as tmp:
            _decode(body, Path(tmp))
        manifest_path.write_bytes(_canonical({
            "complete": True, "captured_at": datetime.now(timezone.utc).isoformat(),
            "receipts": {"eia_zip": receipt},
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        return body, (root / "eia_zip.wire").read_bytes(), receipt
    except Exception as exc:
        receipts = {}
        for path in root.glob("*.receipt.json"):
            try:
                name = path.name.removesuffix(".receipt.json")
                receipts[name] = json.loads(path.read_bytes())
                captures.setdefault(name, ((root / f"{name}.body").read_bytes(), receipts[name]))
            except Exception:
                pass
        manifest_path.write_bytes(_canonical({
            "complete": False, "captured_at": datetime.now(timezone.utc).isoformat(),
            "error_code": type(exc).__name__, "receipts": receipts}) + b"\n")
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures,
                                          type(exc).__name__)
        raise


def capture_strategic_petroleum_reserves(storage: StagingStorageResource, *, version: str,
                                         capture_directory: str | None = None) -> dict:
    """Capture an unapproved pinned EIA SPR point candidate; resumable by checksummed manifest."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if (not isinstance(version, str) or not version or Path(version).name != version
            or "/" in version or "\\" in version):
        raise ValueError("unsafe_capture_version")
    stage = Path(storage.local_dir).absolute()
    if ".." in stage.parts or any(p.is_symlink() for p in (stage, *stage.parents)):
        raise ValueError("unsafe_local_staging_root")
    stage.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(stage).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    root = (Path(capture_directory).absolute() if capture_directory
            else Path("/tmp/hifld-native-reconstruction/327/captures") / (version + "-" + uuid4().hex))
    body, wire, receipt = _capture(root, storage, version)
    with tempfile.TemporaryDirectory() as tmp:
        frame, audit = _decode(body, Path(tmp))
        parquet = _parquet(frame)
    identity = processing_identity(Path(__file__).parents[3])
    evidence_sha = _sha(_canonical({"body": receipt["sha256"], "wire": receipt["wire_sha256"],
                                    "receipt": receipt}))
    audit.update({"source_capture_evidence_sha256": evidence_sha, "source_receipt": receipt,
                  "crosswalk_open_id": CROSSWALK_OPEN_ID,
                  "crosswalk_public_copy_sha256": CROSSWALK_COPY_SHA256,
                  "crosswalk_directed_service_url": CROSSWALK_DIRECTED_SERVICE_URL,
                  "crosswalk_gate_note": CROSSWALK_GATE_NOTE,
                  "source_release_accepted": False, "source_freshness_accepted": False,
                  "schema_contract_accepted": False, "id_contract_accepted": False,
                  "coverage_accepted": False, "rights_accepted": False,
                  "mapping_approved": False, "release_accepted": False,
                  "baseline_accepted": False})
    candidate_version = "v" + _sha(_canonical({
        "archive_sha256": EXPECTED_ZIP_SHA256, "record_sha256": audit["source_record_sha256"],
        "geometry_sha256": audit["source_geometry_sha256"],
        "schema_sha256": audit["source_schema_sha256"],
        "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, candidate_version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    source_input = {"input_id": "eia_official_spr_archive", "producer": "U.S. Energy Information Administration",
                    "host": urlsplit(SOURCE_URL).hostname, "role": "producer_publication",
                    "canonical_url": SOURCE_URL, "retrieval_url": SOURCE_URL,
                    "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
                    "http_status": receipt["status"], "bytes": receipt["bytes"],
                    "sha256": receipt["sha256"], "wire_bytes": receipt["wire_bytes"],
                    "wire_sha256": receipt["wire_sha256"], "release_date": None,
                    "release_date_unknown_reason":
                        "The zip's Last-Modified (2022-02-03), its embedded metadata date "
                        "(2022-02-02/03) and the per-row Data_Perio 'January 2021' are "
                        "dated evidence but no producer release event is published for "
                        "this static map_data file.",
                    "license": "Attribution/reuse review pending",
                    "attribution": "U.S. Energy Information Administration; underlying site "
                                   "data credited to DOE, Office of Fossil Energy per the "
                                   "embedded metadata and the per-row Source attribute; "
                                   "rights review pending"}
    manifest = {"schema_version": 1, "approved": False,
                "title": "Strategic Petroleum Reserves (EIA GIS, January 2021 snapshot)",
                "description": "Pinned historical EIA-hosted SPR_202101 shapefile of the four "
                               "Gulf Coast Strategic Petroleum Reserve storage sites; not "
                               "represented as a current release or accepted HIFLD successor.",
                "publisher": "U.S. Energy Information Administration",
                "agency": "U.S. Department of Energy", "source_url": SOURCE_URL,
                "license": None, "source_modified": SOURCE_LAST_MODIFIED,
                "date_issued": None, "date_modified": SOURCE_RELEASE,
                "processor": "HIFLD Next", "code_commit": identity["code_commit"],
                "code_identity_scheme": identity["code_identity_scheme"],
                "inputs": [source_input],
                "transformations": [{"operation": "Native DBF attributes and EPSG:4326 point "
                                                  "shapefile to GeoParquet",
                                     "parameters": {
                                         "source_fields": "all 12 native DBF fields preserved "
                                                          "in producer order with source types "
                                                          "and values; the producer's POINT_X/"
                                                          "POINT_Y latitude/longitude alias swap "
                                                          "is documented and retained verbatim",
                                         "source_ids": "OBJECTID retained unchanged; no "
                                                       "synthetic identifiers",
                                         "geometry": "native WGS84 shapefile XY retained "
                                                     "without reprojection",
                                         "source_period": SOURCE_RELEASE,
                                         "candidate_status": "historical, unapproved; "
                                                             "currentness/equivalence unresolved"}}],
                "lineage": "Embedded EIA shapefile metadata: 'This is a point dataset "
                           "representing strategic petroleum reserves as of January 1, 2021. "
                           "These data were obtained from the U.S. Dept. of Energy: "
                           "http://www.fossil.energy.gov/programs/reserves/spr/spr-sites.html' "
                           "(metadata date 2022-02-02/03); every row's Source attribute is "
                           "'DOE, Office of Fossil Energy'. The DHS HIFLD Open crosswalk row "
                           "for this layer (Open ID " + CROSSWALK_OPEN_ID + ", byte-identical "
                           "public copy sha256 " + CROSSWALK_COPY_SHA256 + ") directs the layer "
                           "to EIA's own Energy Atlas FeatureServer layer 42, which is token "
                           "gated for anonymous requests (HTTP 499 GWM_0003, probed 2026-11-14); "
                           "this capture is the same producer's remaining official anonymous "
                           "distribution of that product (the EIA-hosted map_data zip, "
                           "last-modified 2022-02-03), not a mirror or archive copy. The "
                           "DataLumos project 240636 archive named by the issue is never "
                           "fetched. The Northeast Home Heating Oil Reserve is a separate "
                           "HIFLD inventory row and is not represented.",
                "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
                    "reason": "All source fields, OBJECTID IDs and native point coordinates "
                              "are retained.",
                    "objects": [{"path": output_path, "bytes": len(parquet),
                                 "sha256": _sha(parquet)}]}}},
                "unsupported_output_formats": {
                    "geopackage": "No approved published schema mapping; release remains blocked.",
                    "pmtiles": "No approved display transformation or generalized geometry."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit.update({"source_fields": EXPECTED_FIELDS, "official_layer_name": EXPECTED_LAYER,
                  "source_population_complete": True})
    outputs = {output_path: parquet,
               "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n",
               "metadata/candidate_ready.json": _canonical({
                   "approved": False, "version": candidate_version,
                   "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
                   "source_population_complete": True, "source_release_accepted": False,
                   "freshness_accepted": False, "schema_contract_accepted": False,
                   "id_contract_accepted": False, "rights_accepted": False,
                   "coverage_accepted": False, "release_accepted": False}) + b"\n",
               "raw/eia_zip.body": body, "raw/eia_zip.wire": wire,
               "raw/eia_zip.receipt.json": _canonical(receipt) + b"\n"}
    state_key = f"_construction-state/{DATASET_SLUG}/{candidate_version}.json"
    fingerprint = _sha(_canonical({"identity": identity, "archive_sha256": EXPECTED_ZIP_SHA256,
                                   "record_sha256": audit["source_record_sha256"],
                                   "schema_sha256": audit["source_schema_sha256"]}))
    if storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(storage, state_key, candidate_version,
                                                      fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("objects"), dict):
            raise ValueError("cached_candidate_state_invalid")
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix,
                                candidate_version, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"],
                                original["objects"], original.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG,
                "version": candidate_version, "feature_count": EXPECTED_COUNT,
                "source_population_complete": True, "source_id_sha256": ID_SHA,
                "source_schema_sha256": audit["source_schema_sha256"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_capture_evidence_sha256": evidence_sha, "capture_directory": str(root),
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "coverage_accepted": False, "rights_accepted": False, "release_accepted": False}
    descriptors = {}
    for name, blob in outputs.items():
        key = f"{prefix}/{name}"
        write_immutable(storage, key, blob)
        snapshot = storage.object_snapshot(key)
        if snapshot is None or snapshot.size != len(blob) or storage.sha256_key(key) != _sha(blob):
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = {"bytes": len(blob), "sha256": _sha(blob)}
    seal_ready_candidate(storage, state_key, candidate_version, fingerprint, {
        "processing_identity": identity, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": {"eia_zip": receipt}, "objects": descriptors})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": ID_SHA, "source_schema_sha256": audit["source_schema_sha256"],
            "source_record_sha256": audit["source_record_sha256"],
            "source_geometry_sha256": audit["source_geometry_sha256"],
            "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_capture_evidence_sha256": evidence_sha, "capture_directory": str(root),
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "release_accepted": False}
