"""Unapproved draft reconstruction of the Agricultural Minerals Operations
dataset from the producer's own USGS MRData mineplant 2003-survey package.

The candidate input is the USGS "Active Mines and Mineral Processing Plants in
the United States in 2003" distribution (MRDATA mineplant, FGDC publication
2005): the Minerals Information Team survey that the retired National Atlas
agricultural-minerals layer documents through its producer FGDC metadata
(``minop1x020.xml``, December 2005 publication of operations active in 2003,
surviving on the USGS National Geospatial Technical Operations Center legacy
WAF). Staging replays an explicitly sealed capture package or live-captures
the three hash-pinned USGS documents, failing closed on any drift. Only
USGS-owned objects feed candidate values; the agricultural scope is the six
commodity classes the pinned producer metadata documents (magnesium compounds,
peat, phosphate, potash, sulfur, vermiculite). No default fetch, archive
polling, datum transformation, uppercase conversion, identifier mapping, or
release acceptance is performed.
"""
from __future__ import annotations

import collections
import io
import json
import math
import re
import shutil
import struct
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, processing_identity, retain_failed_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = FILE_SLUG = "agricultural-minerals-operations"
SOURCE_URL = "https://mrdata.usgs.gov/mineplant/"
MINEPLANT_FGDC_URL = "https://mrdata.usgs.gov/metadata/mineplant.xml"
ATLAS_LEGACY_FGDC_URL = "https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop1x020.xml"
AGENCY = "U.S. Geological Survey"
PUBLISHER = "U.S. Geological Survey Mineral Resources Program (MRDATA historical 2003 survey distribution)"
LICENSE = ("Both FGDC documents state access and use constraints 'None'; the National Atlas metadata asks that "
           "acknowledgment of the National Atlas of the United States and (or) the U.S. Geological Survey, and the "
           "mineplant metadata that acknowledgment of the National Minerals Information Center, be appreciated. "
           "Final rights review pending.")

_EXPECTED_URLS = {"mineplant_zip": SOURCE_URL + "mineplant.zip",
                  "mineplant_fgdc": MINEPLANT_FGDC_URL,
                  "atlas_legacy_fgdc": ATLAS_LEGACY_FGDC_URL}
PINS = {
    "mineplant_zip": {"bytes": 513191,
                      "sha256": "6dabd3bbedcb1fbf0c5155fb349228951a9b2cbd065311484eb479c25997ee4e"},
    "mineplant_fgdc": {"bytes": 26366,
                       "sha256": "419629edaf825f2d55ed1dd03fe3a9921d0d35a8240c208b44b53d30c8b9ead0"},
    "atlas_legacy_fgdc": {"bytes": 18676,
                          "sha256": "222a8ac2a2dba3e111f2dff61a37d4a5a67371aed9a7e420cf1a4efe8cbca2ef"},
}
MINEPLANT_ZIP_BYTES = PINS["mineplant_zip"]["bytes"]
MINEPLANT_ZIP_SHA256 = PINS["mineplant_zip"]["sha256"]
MINEPLANT_FGDC_BYTES = PINS["mineplant_fgdc"]["bytes"]
MINEPLANT_FGDC_SHA256 = PINS["mineplant_fgdc"]["sha256"]
ATLAS_LEGACY_FGDC_BYTES = PINS["atlas_legacy_fgdc"]["bytes"]
ATLAS_LEGACY_FGDC_SHA256 = PINS["atlas_legacy_fgdc"]["sha256"]

MEMBERS = {f"mineplant.{suffix}" for suffix in ("dbf", "prj", "shp", "shx", "txt")}
EXPECTED_COUNT = 6785
EXPECTED_FIELDS = [
    ("id", "N", 8, 0), ("COMMODITY", "C", 30, 0), ("SITE_NAME", "C", 50, 0),
    ("COMPANY_NA", "C", 50, 0), ("STATE_LOCA", "C", 40, 0), ("COUNTY", "C", 40, 0),
    ("LATITUDE", "N", 10, 5), ("LONGITUDE", "N", 10, 5), ("PLANT_MIN", "C", 5, 0),
]
FIELD_NAMES = [field[0] for field in EXPECTED_FIELDS]
TXT_HEADER = ["ID", "COMMODITY", "SITE_NAME", "COMPANY_NA", "STATE_LOCA",
              "COUNTY", "LATITUDE", "LONGITUDE", "PLANT_MIN"]
# The six agricultural commodity classes the pinned USGS National Atlas legacy
# metadata documents for the assigned retired layer (minop1x020); the DBF
# spells "Magnesium Compounds" while the FGDC theme keyword reads "Magnesium
# compounds" (case only).
AGRICULTURAL_COMMODITIES = ("Magnesium Compounds", "Peat", "Phosphate", "Potash",
                            "Sulfur", "Vermiculite")
AGRICULTURAL_COMMODITY_COUNTS = {"Magnesium Compounds": 11, "Peat": 52, "Phosphate": 31,
                                 "Potash": 7, "Sulfur": 113, "Vermiculite": 22}
AGRICULTURAL_ROWS = 236
AGRICULTURAL_STATES = 37
MINEPLANT_FGDC_TITLE_MARKER = "Active Mines and Mineral Processing Plants"
ATLAS_LEGACY_FGDC_TITLE_MARKER = "Agricultural Minerals Operations"

# Content pins over the agricultural subset of the pinned producer package:
# exact DBF literals (record), zero-based source record index plus native WKB
# point bytes (geometry), the mineplant identifier roster (ids), and the exact
# subset point bounds.
ID_SHA = "cd88665adeef2a48dc7a6f514b3f7f52fa79cec5560cdc9e3c4d8487b7afb644"
RECORD_SHA = "09bbd54dfd55ffe54fc87de2d8ec2d7cebfecfc508e705e8d709fdc5f6460c34"
GEOMETRY_SHA = "8569acb27f34c408eeb214a4be2282eeef638f0181ed3079e6e277afdcf56ac5"
BOUNDS = [-122.69, 26.243, -67.659, 48.858]


def _json_safe(value):
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "item"):
        return _json_safe(value.item())
    return value


def _decode_dbf(body: bytes) -> list[dict[str, object]]:
    """Decode every mineplant DBF record, preserving exact fixed-point literals."""
    if len(body) < 321:
        raise ValueError("source_dbf_truncated")
    count, header, width = struct.unpack_from("<IHH", body, 4)
    if count != EXPECTED_COUNT or header != 321 or width != 244:
        raise ValueError("source_dbf_schema_or_record_count_drift")
    fields = []
    offset = 32
    while offset + 32 <= header and body[offset] != 0x0D:
        descriptor = body[offset:offset + 32]
        fields.append((descriptor[:11].split(b"\0", 1)[0].decode("ascii"),
                       chr(descriptor[11]), descriptor[16], descriptor[17]))
        offset += 32
    if fields != EXPECTED_FIELDS or body[offset] != 0x0D:
        raise ValueError("source_dbf_schema_drift")
    if len(body) not in (header + count * width, header + count * width + 1):
        raise ValueError("source_dbf_record_population_incomplete")
    if len(body) == header + count * width + 1 and body[-1] != 0x1A:
        raise ValueError("source_dbf_unexpected_tail")
    rows = []
    seen_ids = set()
    for source_fid in range(count):
        record = body[header + source_fid * width:header + (source_fid + 1) * width]
        if record[:1] != b" ":
            raise ValueError("source_dbf_deleted_or_unknown_record")
        offset = 1
        row: dict[str, object] = {"source_fid": source_fid, "source_shp_record_number": source_fid + 1}
        for name, kind, size, scale in fields:
            raw = record[offset:offset + size]
            offset += size
            if kind == "C":
                if any(char > 127 or char < 32 for char in raw):
                    raise ValueError("source_undeclared_nonASCII_encoding:" + name)
                row[name] = raw.rstrip(b" ").decode("ascii")
            else:
                try:
                    text = raw.decode("ascii").strip()
                except UnicodeDecodeError as exc:
                    raise ValueError("source_undeclared_nonASCII_encoding:" + name) from exc
                if not re.fullmatch(r"-?\d+(?:\.\d+)?", text):
                    raise ValueError("source_null_or_unparseable_dbf_number:" + name)
                value = Decimal(text)
                if value.as_tuple().exponent < -scale:
                    raise ValueError("source_declared_dbf_scale_drift:" + name)
                row[name] = value
        identifier = row["id"]
        if identifier.as_tuple().exponent < 0 or int(identifier) < 1 or int(identifier) in seen_ids:
            raise ValueError("source_missing_duplicate_or_nonpositive_id")
        seen_ids.add(int(identifier))
        rows.append(row)
    return rows


def _crosscheck_txt(body: bytes, rows: list[dict[str, object]]) -> None:
    """Cross-check every DBF record against the producer's own TXT dump."""
    try:
        lines = body.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise ValueError("source_txt_nonASCII") from exc
    if len(lines) != len(rows) + 1 or lines[0].split("\t") != TXT_HEADER:
        raise ValueError("source_txt_roster_or_header_drift")

    def norm(value: str) -> str:
        return "" if value.strip().upper() == "NULL" else value.rstrip()

    for line, row in zip(lines[1:], rows):
        cells = line.split("\t")
        if len(cells) != len(TXT_HEADER) or int(cells[0]) != int(row["id"]):
            raise ValueError("source_txt_dbf_value_drift")
        for index, name in enumerate(FIELD_NAMES[1:], 1):
            cell, literal = norm(cells[index]), norm(_json_safe(row[name]))
            if isinstance(row[name], Decimal):
                if not re.fullmatch(r"-?\d+(?:\.\d+)?", cell) or Decimal(cell) != row[name]:
                    raise ValueError("source_txt_dbf_value_drift")
            elif cell != literal:
                raise ValueError("source_txt_dbf_value_drift")


def _decode_points(shape: bytes, index: bytes) -> tuple[list[tuple[float, float]], list[float]]:
    """Decode the native point SHP/SHX pairs without any reprojection."""
    if len(shape) < 100 or len(index) != 100 + 8 * EXPECTED_COUNT:
        raise ValueError("source_shp_shx_incomplete")
    for data in (shape, index):
        if (struct.unpack_from(">i", data)[0] != 9994
                or struct.unpack_from(">i", data, 24)[0] * 2 != len(data)
                or struct.unpack_from("<ii", data, 28) != (1000, 1)):
            raise ValueError("source_shp_shx_header_drift")
    points = []
    offset = 100
    for source_fid in range(EXPECTED_COUNT):
        if offset + 28 > len(shape):
            raise ValueError("source_shp_truncated_record")
        record, words = struct.unpack_from(">ii", shape, offset)
        shape_type, x, y = struct.unpack_from("<idd", shape, offset + 8)
        if (record != source_fid + 1 or words != 10 or shape_type != 1
                or struct.unpack_from(">ii", index, 100 + 8 * source_fid) != (offset // 2, 10)
                or not all(math.isfinite(value) for value in (x, y))):
            raise ValueError("source_shp_shx_record_offset_type_or_coordinate_drift")
        points.append((x, y))
        offset += 28
    if offset != len(shape):
        raise ValueError("source_shp_extra_records")
    bounds = list(struct.unpack_from("<4d", shape, 36))
    if index[36:68] != shape[36:68]:
        raise ValueError("source_shp_shx_bounds_drift")
    return points, bounds


def _decode(body: bytes) -> tuple[list[dict[str, object]], list[tuple[float, float]], pyproj.CRS,
                                 list[float], dict[str, object]]:
    """Decode the pinned producer package into exact subset rows and audit facts."""
    if len(body) != MINEPLANT_ZIP_BYTES or _sha(body) != MINEPLANT_ZIP_SHA256:
        raise ValueError("pinned_mineplant_package_drift")
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        names = []
        for info in archive.infolist():
            mode = info.external_attr >> 16
            parts = Path(info.filename).parts
            if (not info.filename or info.filename.startswith(("/", "\\")) or "\\" in info.filename
                    or ".." in parts or len(parts) != 1 or info.is_dir()
                    or (mode & 0o170000) == 0o120000):
                raise ValueError("source_zip_member_path_rejected")
            names.append(info.filename)
        if len(set(names)) != len(names) or set(names) != MEMBERS:
            raise ValueError("source_zip_member_roster_drift")
        members = {name: archive.read(name) for name in sorted(MEMBERS)}
    rows = _decode_dbf(members["mineplant.dbf"])
    _crosscheck_txt(members["mineplant.txt"], rows)
    points, header_bounds = _decode_points(members["mineplant.shp"], members["mineplant.shx"])
    try:
        crs = pyproj.CRS(members["mineplant.prj"].decode("ascii"))
    except (UnicodeDecodeError, pyproj.exceptions.CRSError) as exc:
        raise ValueError("source_native_prj_unparseable") from exc
    if not crs.is_geographic:
        raise ValueError("source_native_prj_not_geographic")
    for row, (x, y) in zip(rows, points):
        if (x, y) != (float(row["LONGITUDE"]), float(row["LATITUDE"])):
            raise ValueError("source_dbf_coordinate_geometry_value_drift")
    subset = [row for row in rows if row["COMMODITY"] in AGRICULTURAL_COMMODITIES]
    subset_points = [point for row, point in zip(rows, points) if row["COMMODITY"] in AGRICULTURAL_COMMODITIES]
    if len(subset) != AGRICULTURAL_ROWS:
        raise ValueError("agricultural_subset_completeness_drift")
    counts = collections.Counter(row["COMMODITY"] for row in subset)
    if dict(counts) != AGRICULTURAL_COMMODITY_COUNTS:
        raise ValueError("agricultural_subset_commodity_count_drift")
    if len({row["STATE_LOCA"] for row in subset}) != AGRICULTURAL_STATES:
        raise ValueError("agricultural_subset_state_coverage_drift")
    ids = sorted(int(row["id"]) for row in subset)
    attrs = [[_json_safe(row[name]) for name in FIELD_NAMES] for row in subset]
    geometry = [[int(row["id"]), shapely.Point(x, y).wkb.hex()]
                for row, (x, y) in zip(subset, subset_points)]
    bounds = [float(value) for value in shapely.total_bounds(shapely.points(subset_points)).tolist()]
    audit = {
        "source_feature_count": len(subset), "source_population_complete": True,
        "source_id_field": "id", "source_id_unique": True,
        "source_id_min": min(ids), "source_id_max": max(ids),
        "source_id_sha256": _sha(_canonical(ids)),
        "source_record_sha256": _sha(_canonical(attrs)),
        "source_geometry_sha256": _sha(_canonical(geometry)),
        "source_schema_sha256": _sha(_canonical(EXPECTED_FIELDS)),
        "source_dbf_schema": EXPECTED_FIELDS,
        "source_native_prj_wkt": members["mineplant.prj"].decode("ascii").rstrip("\n"),
        "source_native_crs_is_geographic": True,
        "source_bounds": bounds,
        "source_native_shp_header_bounds": [float(value) for value in header_bounds],
        "agricultural_commodities": dict(counts),
        "agricultural_states": len({row["STATE_LOCA"] for row in subset}),
        "source_full_package_records": len(rows),
        "source_xy_attribute_relationship": ("native SHP point doubles equal the DBF decimal "
                                             "coordinates exactly; no transformation applied"),
        "drift_guard": ("Pinned package and metadata checksums, complete zip member roster, exact DBF "
                        "widths/types/decimals, producer TXT cross-check, native point SHP/SHX decoding, "
                        "id roster uniqueness, commodity subset counts and state coverage."),
    }
    return subset, subset_points, crs, bounds, audit


def _fgdc_facts(body: bytes, label: str) -> dict[str, object]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise ValueError(f"{label}_unparseable") from exc

    def first(tag: str) -> object:
        for element in root.iter(tag):
            value = "".join(element.itertext()).strip()
            if value:
                return value
        return None

    return {"title": first("title"), "pubdate": first("pubdate"),
            "horizontal_datum": first("horizdn"), "grid_unit": first("geogunit"),
            "access_constraints": first("accconst"), "use_constraints": first("useconst")}


def _point_wkb(x, y):
    return struct.pack("<BIdd", 1, 1, float(x), float(y))


def _parquet(subset: list[dict[str, object]], points: list[tuple[float, float]],
             crs: pyproj.CRS, bounds: list[float]) -> bytes:
    arrays = []
    fields = []
    for name, kind, width, scale in EXPECTED_FIELDS:
        values = [_json_safe(row[name]) for row in subset]
        if kind == "C":
            arrays.append(pa.array(values, type=pa.string(), safe=True))
            fields.append(pa.field(name, pa.string(), nullable=False))
        else:
            arrays.append(pa.array([Decimal(value) for value in values],
                                   type=pa.decimal128(width, scale), safe=True))
            fields.append(pa.field(name, pa.decimal128(width, scale), nullable=False))
    arrays.append(pa.array([_point_wkb(x, y) for x, y in points], type=pa.binary()))
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"],
                                    "crs": crs.to_json_dict(), "bbox": bounds}}}
    schema = pa.schema(fields, metadata={b"geo": _canonical(geo)})
    sink = io.BytesIO()
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), sink, compression="zstd",
                   use_dictionary=False)
    table = pq.read_table(pa.BufferReader(sink.getvalue()))
    if table.num_rows != AGRICULTURAL_ROWS:
        raise ValueError("candidate_row_count_readback_drift")
    for name, kind, width, scale in EXPECTED_FIELDS:
        expected_type = pa.string() if kind == "C" else pa.decimal128(width, scale)
        if table[name].type != expected_type:
            raise ValueError("candidate_attribute_type_readback_drift:" + name)
        if table[name].to_pylist() != ([_json_safe(row[name]) for row in subset] if kind == "C"
                                       else [Decimal(_json_safe(row[name])) for row in subset]):
            raise ValueError("candidate_attribute_readback_drift:" + name)
    if any(table["geometry"][position].as_py() != _point_wkb(x, y)
           for position, (x, y) in enumerate(points)):
        raise ValueError("candidate_geometry_readback_drift")
    return sink.getvalue()


def _load_capture(root: Path, name: str) -> tuple[bytes, bytes, dict[str, object]]:
    url = _EXPECTED_URLS[name]
    paths = [root / f"{name}.body", root / f"{name}.wire", root / f"{name}.receipt.json"]
    if any(path.is_symlink() for path in paths):
        raise ValueError("capture_symlink_rejected")
    body, wire, receipt = paths[0].read_bytes(), paths[1].read_bytes(), json.loads(paths[2].read_bytes())
    if (receipt.get("method") != "GET" or receipt.get("url") != url
            or receipt.get("final_url") != url or receipt.get("complete_body") is not True
            or receipt.get("status") != 200 or receipt.get("bytes") != len(body)
            or receipt.get("sha256") != _sha(body) or receipt.get("wire_bytes") != len(wire)
            or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("capture_receipt_invalid:" + name)
    pin = PINS[name]
    if len(body) != pin["bytes"] or receipt["sha256"] != pin["sha256"]:
        raise ValueError("unreviewed_capture_bytes_require_explicit_new_package:" + name)
    return body, wire, receipt


def _capture(root: Path, storage: StagingStorageResource) -> dict[str, tuple[bytes, bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    if root.is_symlink() or any(path.is_symlink() for path in root.parents):
        raise ValueError("unsafe_capture_directory")
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_bytes())
        if manifest.get("complete") is not True or set(manifest.get("receipts", {})) != set(_EXPECTED_URLS):
            raise ValueError("capture_manifest_invalid")
        roster = manifest.get("artifact_roster")
        validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
        if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
            raise ValueError("capture_roster_digest_drift")
        captures = {name: _load_capture(root, name) for name in _EXPECTED_URLS}
        if any(captures[name][2] != manifest["receipts"][name] for name in _EXPECTED_URLS):
            raise ValueError("capture_manifest_receipt_drift")
        return captures
    if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")):
        raise ValueError("unsealed_capture_attempt")
    root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, bytes, dict]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(60, connect=15),
                          headers={"User-Agent": "hifld-next-datasets/agricultural-minerals-draft"}) as client:
            for name, url in _EXPECTED_URLS.items():
                body, receipt = capture_get(client, url, root, name, max_bytes=2_000_000)
                wire = (root / f"{name}.wire").read_bytes()
                pin = PINS[name]
                if (receipt.get("url") != url or receipt.get("final_url") != url
                        or len(body) != pin["bytes"] or receipt.get("sha256") != pin["sha256"]):
                    raise ValueError("pinned_live_capture_drift:" + name)
                captures[name] = (body, wire, receipt)
        _decode(captures["mineplant_zip"][0])
        _fgdc_facts(captures["mineplant_fgdc"][0], "mineplant_fgdc")
        _fgdc_facts(captures["atlas_legacy_fgdc"][0], "atlas_legacy_fgdc")
        manifest_path.write_bytes(_canonical({
            "complete": True, "captured_at": datetime.now(timezone.utc).isoformat(),
            "receipts": {name: captures[name][2] for name in _EXPECTED_URLS},
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"}),
        }) + b"\n")
        return captures
    except Exception as exc:
        retain_failed_capture(storage, root, DATASET_SLUG, "acquisition", exc, error=ValueError)
        manifest_path.write_bytes(_canonical({
            "complete": False, "captured_at": datetime.now(timezone.utc).isoformat(),
            "error_code": type(exc).__name__}) + b"\n")
        raise


def capture_agricultural_minerals(storage: StagingStorageResource, *,
                                  capture_directory: str | None = None) -> dict[str, object]:
    """Stage the unapproved draft candidate from explicit or live USGS captures."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    stage = Path(storage.local_dir).absolute()
    if ".." in stage.parts or any(path.is_symlink() for path in (stage, *stage.parents)):
        raise ValueError("unsafe_local_staging_root")
    stage.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(stage).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    root = requested or Path("/tmp/hifld-native-reconstruction/7/captures") / f"{uuid4().hex}"
    captures = _capture(root, storage)
    subset, points, crs, bounds, audit = _decode(captures["mineplant_zip"][0])
    parquet = _parquet(subset, points, crs, bounds)
    identity = processing_identity(Path(__file__).parents[3])
    evidence_sha = _sha(_canonical({name: {"body_sha256": captures[name][2]["sha256"],
                                           "wire_sha256": captures[name][2]["wire_sha256"]}
                                     for name in _EXPECTED_URLS}))
    audit.update({
        "mineplant_fgdc": _fgdc_facts(captures["mineplant_fgdc"][0], "mineplant_fgdc"),
        "atlas_legacy_fgdc": _fgdc_facts(captures["atlas_legacy_fgdc"][0], "atlas_legacy_fgdc"),
        "datum_contradiction": ("The mineplant FGDC and native PRJ both declare the North American Datum of "
                                "1927 while the National Atlas legacy FGDC declares the North American Datum "
                                "of 1983 for the same Minerals Information Team survey; coordinates are "
                                "retained as provided and no transformation or correction is applied."),
        "source_capture_evidence_sha256": evidence_sha,
        "source_receipts": {name: captures[name][2] for name in _EXPECTED_URLS},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False, "baseline_accepted": False,
    })
    if audit["source_id_sha256"] != ID_SHA or audit["source_record_sha256"] != RECORD_SHA \
            or audit["source_geometry_sha256"] != GEOMETRY_SHA or bounds != BOUNDS:
        raise ValueError("source_content_snapshot_drift")
    version = "v" + _sha(_canonical({"zip_sha256": MINEPLANT_ZIP_SHA256,
                                     "record_sha256": RECORD_SHA,
                                     "geometry_sha256": GEOMETRY_SHA,
                                     "schema_sha256": audit["source_schema_sha256"],
                                     "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    op = f"geoparquet/{FILE_SLUG}.parquet"
    inputs = []
    producers = {
        "mineplant_zip": AGENCY + " Mineral Resources Program (MRDATA historical 2003 survey distribution)",
        "mineplant_fgdc": AGENCY + " (FGDC metadata for the mineplant historical 2003 survey publication)",
        "atlas_legacy_fgdc": AGENCY + " (National Atlas legacy FGDC metadata for the retired "
                                      "agricultural-minerals layer)",
    }
    roles = {"mineplant_zip": "candidate values", "mineplant_fgdc": "producer metadata",
             "atlas_legacy_fgdc": "producer metadata for the assigned scope"}
    for name, url in _EXPECTED_URLS.items():
        receipt = captures[name][2]
        inputs.append({
            "input_id": name, "producer": producers[name], "host": urlsplit(url).hostname,
            "role": "producer_publication", "canonical_url": url, "retrieval_url": url,
            "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
            "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
            "pipeline_use": roles[name],
            "release_date": None,
            "release_date_unknown_reason": ("The mineplant FGDC publication year is 2005 and the National Atlas "
                                            "legacy FGDC pubdate is 200512; neither document gives an exact "
                                            "release day, and fetch or portal-edit times are not release dates."),
            "license": LICENSE,
            "attribution": producers[name] + "; FGDC documents retained unchanged.",
        })
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Agricultural Minerals Operations — USGS mineplant 2003-survey reconstruction (unapproved draft)",
        "description": "The six agricultural commodity classes (magnesium compounds, peat, phosphate, potash, "
                       "sulfur, vermiculite) of the producer's own USGS mineplant package for the Minerals "
                       "Information Team survey of operations active in 2003, the survey that the retired "
                       "National Atlas agricultural-minerals layer documents. Not the original National Atlas "
                       "layer bytes and not a current census.",
        "publisher": PUBLISHER, "agency": AGENCY, "source_url": SOURCE_URL,
        "license": LICENSE, "date_issued": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"], "inputs": inputs,
        "transformations": [{"operation": "Agricultural subset of the native USGS mineplant package to GeoParquet",
                             "parameters": {
                                 "assigned_scope": ("the six commodity classes documented by the pinned USGS "
                                                    "National Atlas legacy metadata; the FGDC theme keyword "
                                                    "reads 'Magnesium compounds' and the DBF value 'Magnesium "
                                                    "Compounds' (case only)"),
                                 "subset_selection": ("rows of the mineplant package whose COMMODITY equals one "
                                                      "of the six classes; pinned completeness 236 rows in 37 "
                                                      "states with per-commodity counts"),
                                 "source_fields": "all native DBF fields preserved in order, types and values",
                                 "source_ids": ("mineplant id plus zero-based source_fid and SHP record number "
                                                "retained; no substitute identifiers and no approved mapping "
                                                "to the published MINOP1X020 roster"),
                                 "geometry": ("native mineplant SHP point coordinates and own NAD27 PRJ as "
                                              "provided; no reprojection or datum correction"),
                                 "candidate_status": ("unapproved producer-successor reconstruction; source, "
                                                      "schema, ID, coverage, rights and release decisions "
                                                      "pending")}}],
        "lineage": ("USGS Minerals Information Team survey of operations active in 2003 (Automated Minerals "
                    "Information System and Minerals Yearbook lineage), published by the National Atlas of the "
                    "United States as the agricultural-minerals layer (FGDC pubdate 200512) and by the USGS "
                    "Mineral Resources Program as the MRDATA mineplant package (FGDC publication 2005). The "
                    "inventory marks the dataset legacy and not expected to be updated. No maintained successor "
                    "feed is asserted."),
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": ("All six agricultural commodity classes, source id and record indices, native DBF "
                       "literals, and native NAD27 point coordinates are retained."),
            "objects": [{"path": op, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {
            "shapefile_zip": ("Rebuilt archives are not byte-equivalent producer artifacts; no shapefile output "
                              "mapping is approved."),
            "geopackage": ("DBF decimal width/scale cannot be silently coerced to GPKG REAL/INTEGER; no "
                           "migration approved."),
            "pmtiles": ("Visualization requires reprojection and quantization the source does not declare; not "
                        "approved."),
        },
        "promotion_allowed": False, "source_accepted": False, "baseline": None,
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit_marker = "metadata/agricultural_minerals_source_audit.json"
    outputs = {op: parquet,
               "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               audit_marker: _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n",
               "metadata/candidate_ready.json": _canonical({
                   "approved": False, "version": version,
                   "objects": {op: {"bytes": len(parquet), "sha256": _sha(parquet)}},
                   "source_population_complete": True, "source_release_accepted": False,
                   "freshness_accepted": False, "schema_contract_accepted": False,
                   "id_contract_accepted": False, "rights_accepted": False,
                   "coverage_accepted": False, "release_accepted": False}) + b"\n"}
    for name in _EXPECTED_URLS:
        outputs[f"raw/{name}.body"] = captures[name][0]
        outputs[f"raw/{name}.wire"] = captures[name][1]
        outputs[f"raw/{name}.receipt.json"] = _canonical(captures[name][2]) + b"\n"
    state = f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint = _sha(_canonical({"identity": identity, "zip_sha256": MINEPLANT_ZIP_SHA256,
                                   "record_sha256": RECORD_SHA, "schema_sha256": audit["source_schema_sha256"]}))
    if storage.object_snapshot(state) is not None:
        original = json.loads(load_first_construction(storage, state, version, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("objects"), dict):
            raise ValueError("cached_candidate_state_invalid")
        verify_cached_candidate(storage, Path(__file__).parents[3], state, prefix, version, fingerprint,
                                identity, audit["source_record_sha256"], audit["source_schema_sha256"],
                                original["objects"], original.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": AGRICULTURAL_ROWS, "source_population_complete": True,
                "source_id_sha256": ID_SHA, "source_schema_sha256": audit["source_schema_sha256"],
                "source_capture_evidence_sha256": evidence_sha, "reused": True,
                "release_accepted": False}
    descriptors = {}
    for name, body in outputs.items():
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        if snapshot is None or snapshot.size != len(body) or storage.sha256_key(key) != _sha(body):
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = {"bytes": len(body), "sha256": _sha(body)}
    seal_ready_candidate(storage, state, version, fingerprint,
                         {"processing_identity": identity,
                          "source_record_sha256": audit["source_record_sha256"],
                          "source_schema_sha256": audit["source_schema_sha256"],
                          "capture_receipts": {name: captures[name][2] for name in _EXPECTED_URLS},
                          "objects": descriptors})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": AGRICULTURAL_ROWS, "source_population_complete": True,
            "source_id_sha256": ID_SHA, "source_schema_sha256": audit["source_schema_sha256"],
            "source_capture_evidence_sha256": evidence_sha, "reused": False,
            "release_accepted": False}
