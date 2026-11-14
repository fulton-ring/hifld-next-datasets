"""Pinned historical EIA Petroleum Product Terminals shapefile candidate for issue #267."""
from __future__ import annotations

import hashlib
import io
import json
import math
import shutil
import struct
import tempfile
import zipfile
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
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)

DATASET_SLUG = FILE_SLUG = "petroleum-product-terminals"
SOURCE_URL = "https://www.eia.gov/maps/map_data/PetroleumProduct_Terminals_US_EIA.zip"
SURVEY_PAGE_URL = "https://www.eia.gov/survey/index.php?fuseName=detail&surveyNum=815"
SOURCE_RELEASE = "2021-09"
EXPECTED_LAYER = "PetroleumProduct_Terminals_US_202109"
EXPECTED_ZIP_BYTES = 129104
EXPECTED_ZIP_SHA256 = "c1b2c5c8c5d379b24e2b9ea8e1b2ed6544e47d006a6e705d0fb7178d23dbab40"
EXPECTED_COUNT = 1474
EXPECTED_FIELDS = [
    ("OBJECTID", "N", 10, 0), ("Company", "C", 254, 0), ("Site", "C", 254, 0),
    ("State_Name", "C", 254, 0), ("PADD", "N", 10, 0), ("LONGITUDE", "F", 19, 11),
    ("LATITUDE", "F", 19, 11), ("Source", "C", 254, 0),
]
EXPECTED_DATA_PERIOD = "As of September 2021"
EXPECTED_PADD_VALUES = [1, 2, 3, 4, 5, 6, 7]
EXPECTED_SOURCE_VALUE = "EIA-815"
EXPECTED_STATE_NAME_COUNT = 53
MEMBERS = {f"{EXPECTED_LAYER}.{suffix}" for suffix in ("cpg", "dbf", "prj", "sbn", "sbx", "shp", "shp.xml", "shx")}
# The SHP point doubles carry small ArcGIS de-duplication jitter relative to
# the DBF researched LONGITUDE/LATITUDE attributes (every vertex differs by
# roughly 1e-9..5e-05 degrees); both are preserved verbatim and the observed
# maximum divergence is pinned as the drift gate.
XY_ATTRIBUTE_MAX_DELTA = 5.000194516924239e-05
ID_SHA = "6f946fbf25653dec118db53b0754c47836e4715ec23524c2bc8eb7ca33a40827"
RECORD_SHA = "a0651216dc98cf8c7ef221ed4061a8f12549231a8655d62a494c9709bd57454a"
GEOMETRY_SHA = "a2de1ddfc7fea6cd980d0d76813690818a5d33351c5a70372965ab2204f3ef38"
BOUNDS = [-176.6576000003431, 13.416400000502051, 144.6865999999097, 66.9023000002397]
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
    if int.from_bytes(b[4:8], "little") != EXPECTED_COUNT:
        raise ValueError("source_dbf_record_count_drift")
    return out


def _decode(body, tmp):
    if len(body) != EXPECTED_ZIP_BYTES or _sha(body) != EXPECTED_ZIP_SHA256:
        raise ValueError("pinned_eia_snapshot_drift")
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        names = []
        for i in z.infolist():
            mode = i.external_attr >> 16
            parts = Path(i.filename).parts
            if not i.filename or i.filename.startswith(("/", "\\")) or "\\" in i.filename or ".." in parts \
                    or len(parts) != 1 or i.is_dir() or (mode & 0o170000) == 0o120000:
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
    xml = (root / f"{EXPECTED_LAYER}.shp.xml").read_bytes()
    for marker in ("All operable bulk petroleum product terminals",
                   "EIA-815, Monthly Bulk Terminal and Blender Report",
                   "As of September 2021",
                   "Survey locations adjusted using public data",
                   "50,000 barrels",
                   "U.S. Energy Information Administration", EXPECTED_LAYER):
        if marker.encode("utf-8") not in xml:
            raise ValueError("source_embedded_metadata_contract_drift")
    import pyogrio
    info = pyogrio.read_info(shp)
    if info["layer_name"] != EXPECTED_LAYER or info["geometry_type"] != "Point" \
            or info["crs"] != "EPSG:4326" or int(info["features"]) != EXPECTED_COUNT:
        raise ValueError("source_layer_metadata_drift")
    g = gpd.read_file(shp, engine="pyogrio")
    names = [x[0] for x in EXPECTED_FIELDS]
    if list(g.columns) != names + ["geometry"] or len(g) != EXPECTED_COUNT or g.geometry.isna().any() \
            or g.geometry.is_empty.any() or not (g.geometry.geom_type == "Point").all():
        raise ValueError("source_population_or_schema_drift")
    if not CRS.from_user_input(g.crs).equals(WGS84, ignore_axis_order=True):
        raise ValueError("source_crs_drift")
    if g["OBJECTID"].isna().any() or g["OBJECTID"].duplicated().any():
        raise ValueError("source_id_drift")
    if set(int(v) for v in g["PADD"].tolist()) - set(EXPECTED_PADD_VALUES) \
            or g["PADD"].isna().any():
        raise ValueError("source_padd_value_drift")
    if set(g["Source"].tolist()) != {EXPECTED_SOURCE_VALUE}:
        raise ValueError("source_survey_form_drift")
    if int(g["State_Name"].nunique()) != EXPECTED_STATE_NAME_COUNT:
        raise ValueError("source_state_name_roster_drift")
    dx = abs(g.geometry.x.to_numpy() - g["LONGITUDE"].to_numpy())
    dy = abs(g.geometry.y.to_numpy() - g["LATITUDE"].to_numpy())
    observed_delta = float(max(dx.max(), dy.max()))
    if observed_delta > XY_ATTRIBUTE_MAX_DELTA:
        raise ValueError("source_xy_attribute_drift")
    attrs = [[_native(v) for v in row] for row in g[names].itertuples(index=False, name=None)]
    ids = sorted(int(r[0]) for r in attrs)
    geom = [[int(r[0]), float(g.geometry.iloc[i].x), float(g.geometry.iloc[i].y)] for i, r in enumerate(attrs)]
    idsha, recsha, geomsha = (_sha(_canonical(x)) for x in (ids, attrs, geom))
    if idsha != ID_SHA or recsha != RECORD_SHA or geomsha != GEOMETRY_SHA:
        raise ValueError("source_content_snapshot_drift")
    bounds = list(map(float, g.total_bounds))
    if bounds != BOUNDS:
        raise ValueError("source_bounds_drift")
    audit = {
        "source_feature_count": len(g), "source_population_complete": True,
        "source_id_field": "OBJECTID", "source_id_unique": True,
        "source_id_min": min(ids), "source_id_max": max(ids), "source_id_sha256": idsha,
        "source_record_sha256": recsha, "source_geometry_sha256": geomsha,
        "source_schema_sha256": _sha(_canonical(EXPECTED_FIELDS)), "source_dbf_schema": EXPECTED_FIELDS,
        "source_crs": "EPSG:4326", "source_geometry_type": "Point", "source_bounds": bounds,
        "source_data_period": EXPECTED_DATA_PERIOD, "source_release": SOURCE_RELEASE,
        "source_null_counts": {n: int(g[n].isna().sum()) for n in names},
        "source_geometry_null_count": int(g.geometry.isna().sum()),
        "source_geometry_empty_count": int(g.geometry.is_empty.sum()),
        "source_xy_attribute_max_delta": observed_delta,
        "source_padd_values": sorted(set(int(v) for v in g["PADD"].tolist())),
        "source_survey_form": EXPECTED_SOURCE_VALUE,
        "source_state_name_count": int(g["State_Name"].nunique()),
        "source_coverage_scope": "Complete published EIA petroleum product terminal population for the "
                                 "September 2021 data period (1,474 operable bulk terminals with the "
                                 "State_Name roster of 50 States plus Guam, Puerto Rico and the U.S. "
                                 "Virgin Islands; the embedded abstract's '50 States and the District "
                                 "of Columbia' wording does not match the published State_Name roster, "
                                 "both preserved verbatim). Not a current feed or accepted HIFLD "
                                 "inventory equivalent.",
        "drift_guard": "Pinned official archive checksum, complete zip members, exact DBF widths/types, "
                       "embedded Esri metadata markers, layer name, CRS, all attributes and values, unique "
                       "OBJECTID roster, PADD/Source/State_Name rosters, native XY jitter bound and bounds.",
    }
    return g, audit


def _point_wkb(x, y):
    return struct.pack("<BIdd", 1, 1, float(x), float(y))


def _parquet(g):
    arrays = []
    fields = []
    for name, kind, _width, scale in EXPECTED_FIELDS:
        vals = [_native(v) for v in g[name].tolist()]
        dtype = pa.string() if kind == "C" else pa.int64() if kind == "N" and scale == 0 else pa.float64()
        arrays.append(pa.array(vals, type=dtype, safe=True))
        fields.append(pa.field(name, dtype, nullable=True))
    arrays.append(pa.array([_point_wkb(x, y) for x, y in zip(g.geometry.x, g.geometry.y)], type=pa.binary()))
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": WGS84.to_json_dict()}}}
    sink = io.BytesIO()
    pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata={b"geo": _canonical(geo)})),
                   sink, compression="zstd", use_dictionary=False)
    t = pq.read_table(pa.BufferReader(sink.getvalue()))
    if t.num_rows != EXPECTED_COUNT:
        raise ValueError("candidate_row_count_readback_drift")
    for name, kind, _width, scale in EXPECTED_FIELDS:
        dtype = pa.string() if kind == "C" else pa.int64() if kind == "N" and scale == 0 else pa.float64()
        if t[name].type != dtype:
            raise ValueError("candidate_attribute_type_readback_drift:" + name)
        if t[name].to_pylist() != [_native(v) for v in g[name].tolist()]:
            raise ValueError("candidate_attribute_readback_drift:" + name)
    if any(t["geometry"][i].as_py() != _point_wkb(x, y) for i, (x, y) in enumerate(zip(g.geometry.x, g.geometry.y))):
        raise ValueError("candidate_geometry_readback_drift")
    return sink.getvalue()


_EXPECTED_URLS = {"survey_page": SURVEY_PAGE_URL, "eia_zip": SOURCE_URL}


def _load_capture(name, root):
    if name not in _EXPECTED_URLS:
        raise ValueError("unexpected_source_capture_name")
    paths = [root / f"{name}.body", root / f"{name}.wire", root / f"{name}.receipt.json"]
    if any(p.is_symlink() for p in paths):
        raise ValueError("capture_symlink_rejected")
    body, wire, receipt = paths[0].read_bytes(), paths[1].read_bytes(), json.loads(paths[2].read_bytes())
    if receipt.get("method") != "GET" or receipt.get("url") != _EXPECTED_URLS[name] \
            or receipt.get("final_url") != _EXPECTED_URLS[name] or receipt.get("complete_body") is not True \
            or receipt.get("status") != 200 or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body) \
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire):
        raise ValueError("capture_receipt_invalid")
    return body, wire, receipt


def _capture(root, storage, version):
    mp = root / "capture_manifest.json"
    if root.is_symlink() or any(p.is_symlink() for p in root.parents):
        raise ValueError("unsafe_capture_directory")
    if mp.exists():
        m = json.loads(mp.read_bytes())
        if m.get("complete") is not True or set(m.get("receipts", {})) != set(_EXPECTED_URLS):
            raise ValueError("capture_manifest_invalid")
        roster = m.get("artifact_roster")
        validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
        if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
            raise ValueError("capture_roster_digest_drift")
        captures = {name: _load_capture(name, root) for name in _EXPECTED_URLS}
        if any(captures[name][2] != m["receipts"][name] for name in _EXPECTED_URLS):
            raise ValueError("capture_manifest_receipt_drift")
        return captures
    if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")):
        raise ValueError("unsealed_capture_attempt")
    root.mkdir(parents=True, exist_ok=True)
    caps = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(60, connect=15),
                          headers={"User-Agent": "hifld-next-datasets/petroleum-product-terminals-draft"}) as client:
            page_body, page_receipt = capture_get(client, SURVEY_PAGE_URL, root, "survey_page", max_bytes=2_000_000)
            if b"EIA-815" not in page_body or b"Monthly Bulk Terminal Report" not in page_body:
                raise ValueError("official_eia_survey_relationship_drift")
            caps["survey_page"] = (page_body, page_receipt)
            body, receipt = capture_get(client, SOURCE_URL, root, "eia_zip", max_bytes=8_000_000)
            if receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL \
                    or receipt.get("final_url") != SOURCE_URL:
                raise ValueError("source_request_identity_mismatch")
            caps["eia_zip"] = (body, receipt)
            with tempfile.TemporaryDirectory(dir=root) as tmp:
                _decode(body, Path(tmp))
        mp.write_bytes(_canonical({"complete": True, "captured_at": datetime.now(timezone.utc).isoformat(),
                                   "receipts": {name: caps[name][1] for name in _EXPECTED_URLS},
                                   "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        return {name: (caps[name][0], (root / f"{name}.wire").read_bytes(), caps[name][1])
                for name in _EXPECTED_URLS}
    except Exception as exc:
        rec = {}
        for p in root.glob("*.receipt.json"):
            name = p.name.removesuffix(".receipt.json")
            try:
                rec[name] = json.loads(p.read_bytes())
                body_path = root / f"{name}.body"
                if body_path.exists():
                    caps.setdefault(name, (body_path.read_bytes(), rec[name]))
            except Exception:
                pass
        mp.write_bytes(_canonical({"complete": False, "captured_at": datetime.now(timezone.utc).isoformat(),
                                   "error_code": type(exc).__name__, "receipts": rec}) + b"\n")
        if caps:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, caps, type(exc).__name__)
        raise


def capture_eia_petroleum_product_terminals(storage: StagingStorageResource, *, version: str,
                                            capture_directory: str | None = None) -> dict:
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not isinstance(version, str) or not version or Path(version).name != version or "/" in version or "\\" in version:
        raise ValueError("unsafe_capture_version")
    stage = Path(storage.local_dir).absolute()
    if ".." in stage.parts or any(p.is_symlink() for p in (stage, *stage.parents)):
        raise ValueError("unsafe_local_staging_root")
    stage.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(stage).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    root = requested or Path("/tmp/hifld-native-reconstruction/267/captures") / (version + "-" + uuid4().hex)
    caps = _capture(root, storage, version)
    with tempfile.TemporaryDirectory() as tmp:
        frame, audit = _decode(caps["eia_zip"][0], Path(tmp))
        parquet = _parquet(frame)
    identity = processing_identity(Path(__file__).parents[3])
    evidence_sha = _sha(_canonical({name: {"body_sha256": caps[name][2]["sha256"],
                                           "wire_sha256": caps[name][2]["wire_sha256"]}
                                     for name in _EXPECTED_URLS}))
    audit.update({"source_capture_evidence_sha256": evidence_sha,
                  "source_receipts": {name: caps[name][2] for name in _EXPECTED_URLS},
                  "source_release_accepted": False, "source_freshness_accepted": False,
                  "schema_contract_accepted": False, "id_contract_accepted": False,
                  "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
                  "release_accepted": False, "baseline_accepted": False})
    cv = "v" + _sha(_canonical({"archive_sha256": EXPECTED_ZIP_SHA256,
                                "record_sha256": audit["source_record_sha256"],
                                "geometry_sha256": audit["source_geometry_sha256"],
                                "schema_sha256": audit["source_schema_sha256"],
                                "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, cv)
    op = f"geoparquet/{FILE_SLUG}.parquet"
    inputs = []
    for name, canonical in (("survey_page", SURVEY_PAGE_URL), ("eia_zip", SOURCE_URL)):
        receipt = caps[name][2]
        inputs.append({
            "input_id": name, "producer": "U.S. Energy Information Administration",
            "host": urlsplit(_EXPECTED_URLS[name]).hostname, "role": "producer_publication",
            "canonical_url": canonical, "retrieval_url": _EXPECTED_URLS[name],
            "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
            "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
            "release_date": None,
            "release_date_unknown_reason": "The embedded Esri metadata dates the terminal population 'As of "
                                           "September 2021' (layer PetroleumProduct_Terminals_US_202109) and "
                                           "cites EIA-815, Monthly Bulk Terminal and Blender Report, but EIA "
                                           "publishes no release date for this static file; fetch or server "
                                           "file dates are not release dates.",
            "license": "Attribution/reuse review pending",
            "attribution": "U.S. Energy Information Administration; survey locations adjusted using public data "
                           "(approximate locations); review pending.",
        })
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Petroleum Product Terminals (EIA GIS, September 2021 snapshot)",
        "description": "Pinned historical EIA-hosted PetroleumProduct_Terminals_US_202109 shapefile: 1,474 operable "
                       "bulk petroleum product terminals as of September 2021, with attributes from EIA-815 "
                       "(Monthly Bulk Terminal and Blender Report) and survey locations adjusted using public "
                       "data. Not represented as a current release, the token-gated EIA Energy Atlas layer, or "
                       "the accepted HIFLD inventory equivalent.",
        "publisher": "U.S. Energy Information Administration", "agency": "U.S. Department of Energy",
        "source_url": SOURCE_URL, "license": None, "source_modified": "2022-02-03",
        "date_issued": None, "date_modified": SOURCE_RELEASE, "processor": "HIFLD Next",
        "code_commit": identity["code_commit"], "inputs": inputs,
        "transformations": [{"operation": "Native DBF attributes and EPSG:4326 point shapefile to GeoParquet",
                            "parameters": {"source_fields": "all native DBF fields preserved in order, source types "
                                                            "and values",
                                           "source_ids": "OBJECTID retained unchanged; no synthetic identifiers",
                                           "geometry": "native WGS84 XY retained without reprojection; the SHP point "
                                                       "doubles carry small ArcGIS de-duplication jitter relative "
                                                       "to the DBF researched LONGITUDE/LATITUDE attributes and "
                                                       "both are preserved verbatim",
                                           "source_period": SOURCE_RELEASE,
                                           "candidate_status": "historical, unapproved; inventory equivalence, "
                                                               "currentness and rights unresolved"}}],
        "lineage": "Embedded Esri shapefile metadata credits U.S. Energy Information Administration, quotes "
                   "'EIA-815, Monthly Bulk Terminal and Blender Report' as the source, dates the population 'As "
                   "of September 2021' (50,000-barrel bulk terminals; survey locations adjusted using public "
                   "data), and is captured here with EIA's own survey form page for EIA-815 (Monthly Bulk "
                   "Terminal Report). The DHS HIFLD Open crosswalk row names US Energy Atlas as agency and its "
                   "Open REST Service (services7.arcgis.com FGr1D95XCGALKXqM "
                   "PetroleumProduct_Terminals_US_EIA/FeatureServer/36) returned ArcGIS 499 Token Required "
                   "(GWM_0003) on 2026-11-13, as on 2026-11-12 for the sibling EIA layers; the Atlas "
                   "atlas.eia.gov dataset page is not anonymously retrievable. A services8.arcgis.com copy "
                   "owned by AirAllianceHouston reports the same 1,474-row 2021-09 vintage but is a third-party "
                   "copy, not the producer. The retired HIFLD inventory archive (DataLumos 240512, 2,302 rows "
                   "of 2024 vintage) is identification context only, never a pipeline input.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "All source fields, OBJECTID values, native strings and native point coordinates "
                      "are retained.",
            "objects": [{"path": op, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No published contract/mapping approved",
                                       "pmtiles": "No display transformation approved"},
        "promotion_allowed": False, "source_accepted": False, "baseline": None,
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs = {op: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n",
               "metadata/candidate_ready.json": _canonical(
                   {"approved": False, "version": cv, "objects": {op: {"bytes": len(parquet), "sha256": _sha(parquet)}},
                    "source_population_complete": True, "source_release_accepted": False, "freshness_accepted": False,
                    "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
                    "coverage_accepted": False, "release_accepted": False}) + b"\n"}
    for name in _EXPECTED_URLS:
        outputs[f"raw/{name}.body"] = caps[name][0]
        outputs[f"raw/{name}.wire"] = caps[name][1]
        outputs[f"raw/{name}.receipt.json"] = _canonical(caps[name][2]) + b"\n"
    state = f"_construction-state/{DATASET_SLUG}/{cv}.json"
    fingerprint = _sha(_canonical({"identity": identity, "archive_sha256": EXPECTED_ZIP_SHA256,
                                   "record_sha256": audit["source_record_sha256"],
                                   "schema_sha256": audit["source_schema_sha256"]}))
    if storage.object_snapshot(state) is not None:
        original = json.loads(load_first_construction(storage, state, cv, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("objects"), dict):
            raise ValueError("cached_candidate_state_invalid")
        verify_cached_candidate(storage, Path(__file__).parents[3], state, prefix, cv, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"],
                                original["objects"], original.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": cv,
                "feature_count": EXPECTED_COUNT, "source_population_complete": True,
                "source_id_sha256": ID_SHA, "source_schema_sha256": audit["source_schema_sha256"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_capture_evidence_sha256": evidence_sha, "capture_directory": str(root),
                "release_accepted": False}
    descriptors = {}
    for n, b in outputs.items():
        key = f"{prefix}/{n}"
        write_immutable(storage, key, b)
        snap = storage.object_snapshot(key)
        if snap is None or snap.size != len(b) or storage.sha256_key(key) != _sha(b):
            raise ValueError("candidate_object_readback_failed:" + n)
        descriptors[n] = {"bytes": len(b), "sha256": _sha(b)}
    seal_ready_candidate(storage, state, cv, fingerprint,
                         {"processing_identity": identity,
                          "source_record_sha256": audit["source_record_sha256"],
                          "source_schema_sha256": audit["source_schema_sha256"],
                          "capture_receipts": {name: caps[name][2] for name in _EXPECTED_URLS},
                          "objects": descriptors})
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
    )
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": cv,
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
