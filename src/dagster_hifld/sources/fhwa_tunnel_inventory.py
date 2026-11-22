"""Draft capture for FHWA's National Tunnel Inventory (NTI) annual XML release.

The FHWA NTI download page is the producer's own annual publication index. This
adapter discovers the newest XML edition, preserves the producer's native
element values and portal coordinates, and stages an unapproved candidate.
Completeness is the complete XML population returned by the producer, not a
code-pinned row count or snapshot digest.
"""
from __future__ import annotations

import json
import re
import shutil
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from uuid import uuid4
from xml.etree import ElementTree as ET

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

LANDING_URL = "https://www.fhwa.dot.gov/bridge/inspection/tunnel/inventory/download.cfm"
DATASET_SLUG = "inventorynational-tunnel-inventory"
FILE_SLUG = DATASET_SLUG
LAYER_NAME = "National Tunnel Inventory"
PUBLISHER = "Federal Highway Administration (FHWA)"
LICENSE_URL = "https://www.usa.gov/publicdomain/label/1.0/"
MAX_BODY_BYTES = 100_000_000
SOURCE_CRS_EPSG = 4326
NESTED_ELEMENT = "FHWAElements"

# The SNTI XML schema declares these element types. I1 keeps its text form so
# leading zeros survive; I13/I14 are portal latitude/longitude in decimal
# degrees. SubmittedBy is present in the published XML but not in the 2017 XSD.
ELEMENT_FIELDS = (
    ("I1", "string"), ("I2", "string"), ("I3", "int"), ("I4", "int"),
    ("I5", "string"), ("I6", "string"), ("I7", "string"), ("I8", "string"),
    ("I9", "string"), ("I10", "string"), ("I11", "string"), ("I12", "string"),
    ("I13", "float"), ("I14", "float"), ("I15", "string"), ("I16", "string"),
    ("I17", "string"), ("I18", "string"), ("A1", "string"), ("A2", "string"),
    ("A3", "string"), ("A4", "string"), ("A5", "string"), ("A6", "string"),
    ("A7", "string"), ("A8", "string"), ("C1", "string"), ("C2", "string"),
    ("C3", "string"), ("C4", "string"), ("C5", "string"), ("C6", "string"),
    ("C7", "int"), ("C8", "int"), ("G1", "string"), ("G2", "float"),
    ("G3", "float"), ("G4", "float"), ("G5", "float"), ("D1", "string"),
    ("D2", "string"), ("D3", "int"), ("D4", "string"), ("D5", "string"),
    ("D6", "string"), ("L1", "string"), ("L2", "float"), ("L3", "float"),
    ("L4", "string"), ("L5", "string"), ("L6", "string"), ("L7", "string"),
    ("L8", "string"), ("L9", "string"), ("L10", "string"), ("L11", "string"),
    ("L12", "string"), ("N1", "string"), ("N2", "string"), ("N3", "string"),
    ("S1", "string"), ("S2", "string"), ("S3", "string"), ("S4", "string"),
    ("S5", "string"), ("SubmittedBy", "string"),
)
ELEMENT_NAMES = tuple(name for name, _kind in ELEMENT_FIELDS)
_EXPECTED_TAGS = frozenset(ELEMENT_NAMES) | {NESTED_ELEMENT}
_INTEGER_TEXT = re.compile(r"[+-]?\d+")
_DECIMAL_TEXT = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")
_RELEASE_FILE = re.compile(r"/(\d{4})NTI\.xml$", re.IGNORECASE)


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "a":
            return
        for name, value in attrs:
            if name.casefold() == "href" and value:
                self.hrefs.append(value)


def newest_release(index_body: bytes) -> tuple[int, str]:
    """Return the newest NTI XML edition linked by the producer's download page."""
    try:
        text = index_body.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("source_release_index_encoding_drift") from None
    parser = _AnchorParser()
    parser.feed(text)
    releases: dict[int, str] = {}
    for href in parser.hrefs:
        url = urljoin(LANDING_URL, href)
        match = _RELEASE_FILE.search(urlsplit(url).path)
        if match:
            year = int(match.group(1))
            if 2000 <= year <= 2100:
                releases[year] = url
    if not releases:
        raise ValueError("source_release_index_missing")
    year = max(releases)
    url = releases[year]
    if urlsplit(url).hostname != urlsplit(LANDING_URL).hostname:
        raise ValueError("source_release_host_drift")
    return year, url


def _parse_value(text: str | None, kind: str, name: str) -> str | int | float | None:
    if kind == "string":
        return "" if text is None else text
    if text is None or not text.strip():
        return None
    raw = text.strip()
    if kind == "int":
        if not _INTEGER_TEXT.fullmatch(raw):
            raise ValueError("source_integer_value_drift:" + name)
        return int(raw)
    if not _DECIMAL_TEXT.fullmatch(raw):
        raise ValueError("source_decimal_value_drift:" + name)
    return float(raw)


def _validate(index_body: bytes, data_body: bytes, year: int,
              data_url: str) -> tuple[list[dict], dict]:
    try:
        index_text = index_body.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("source_release_index_encoding_drift") from None
    if "Download NTI Data" not in index_text or "National Tunnel Inventory" not in index_text:
        raise ValueError("source_release_index_identity_drift")
    if urlsplit(data_url).hostname != urlsplit(LANDING_URL).hostname:
        raise ValueError("source_release_host_drift")
    try:
        root = ET.fromstring(data_body)
    except ET.ParseError:
        raise ValueError("source_xml_unreadable") from None
    if root.tag != "TunnelRoot":
        raise ValueError("source_xml_root_drift")
    instances = list(root)
    if not instances or any(instance.tag != "TunnelInstance" for instance in instances):
        raise ValueError("source_xml_record_roster_drift")

    features: list[dict] = []
    keys: list[list[object]] = []
    for instance in instances:
        tags = [child.tag for child in instance]
        if len(tags) != len(_EXPECTED_TAGS) or set(tags) != _EXPECTED_TAGS:
            raise ValueError("source_xml_element_roster_drift")
        attributes = {
            name: _parse_value(instance.findtext(name), kind, name)
            for name, kind in ELEMENT_FIELDS
        }
        state_code, tunnel_number = attributes["I3"], attributes["I1"]
        if type(state_code) is not int or not 0 <= state_code <= 99:
            raise ValueError("source_state_code_drift")
        if not isinstance(tunnel_number, str) or not tunnel_number:
            raise ValueError("source_tunnel_number_drift")
        latitude, longitude = attributes["I13"], attributes["I14"]
        if (type(latitude) is not float or type(longitude) is not float
                or not -90.0 <= latitude <= 90.0 or not -180.0 <= longitude <= 180.0):
            raise ValueError("source_coordinate_missing_or_out_of_range")
        keys.append([state_code, tunnel_number])
        features.append({"attributes": attributes,
                         "geometry": {"x": longitude, "y": latitude}})
    if len(keys) != len({(state, tunnel) for state, tunnel in keys}):
        raise ValueError("source_native_key_duplicate")

    features.sort(key=lambda row: (row["attributes"]["I3"], row["attributes"]["I1"]))
    ordered_keys = [[row["attributes"]["I3"], row["attributes"]["I1"]] for row in features]
    geometry_rows = [[row["attributes"]["I3"], row["attributes"]["I1"],
                      row["geometry"]["x"], row["geometry"]["y"]] for row in features]
    longitudes = [row["geometry"]["x"] for row in features]
    latitudes = [row["geometry"]["y"] for row in features]
    schema = [[name, kind] for name, kind in ELEMENT_FIELDS]
    audit = {
        "source_feature_count": len(features),
        "source_population_complete": True,
        "source_population_witness": "complete FHWA NTI XML release returned by the producer",
        "source_id_fields": ["I3", "I1"],
        "source_id_sha256": _sha(_canonical(ordered_keys)),
        "source_record_sha256": _sha(_canonical([row["attributes"] for row in features])),
        "source_geometry_sha256": _sha(_canonical(geometry_rows)),
        "source_schema_sha256": _sha(_canonical(schema)),
        "source_schema": schema,
        "source_crs_epsg": SOURCE_CRS_EPSG,
        "source_geometry_type": "Point",
        "source_null_geometry_count": 0,
        "source_unique_native_key_count": len(features),
        "source_bounds": {"lon_min": min(longitudes), "lat_min": min(latitudes),
                          "lon_max": max(longitudes), "lat_max": max(latitudes)},
        "source_release_year": year,
        "source_release_index_url": LANDING_URL,
        "source_data_url": data_url,
        "source_release_date": None,
        "source_release_date_unknown_reason": (
            "FHWA publishes annual NTI data by June 15; the download page does not "
            "state an exact publication date for this edition."
        ),
        "source_coverage_scope": (
            "Complete public highway tunnel point population in the FHWA NTI XML "
            "edition linked as newest by the producer's download page."
        ),
        "source_element_data_note": (
            "Nested FHWAElements is the separate National Tunnel Inventory Element "
            "Data product and is not expanded into this point candidate; the raw "
            "XML retains it."
        ),
    }
    return features, audit


def _arrow_type(kind: str) -> pa.DataType:
    return {"string": pa.string(), "int": pa.int64(), "float": pa.float64()}[kind]


def _parquet_bytes(features: list[dict]) -> bytes:
    schema_fields = [pa.field(name, _arrow_type(kind), nullable=True)
                     for name, kind in ELEMENT_FIELDS]
    arrays = [pa.array([row["attributes"][name] for row in features], type=_arrow_type(kind))
              for name, kind in ELEMENT_FIELDS]
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=False))
    arrays.append(pa.array([Point(row["geometry"]["x"], row["geometry"]["y"]).wkb
                            for row in features], type=pa.binary()))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Point"],
        "crs": pyproj.CRS.from_epsg(SOURCE_CRS_EPSG).to_json_dict(), "edges": "planar",
    }}}
    table = pa.Table.from_arrays(arrays, schema=pa.schema(schema_fields, metadata={b"geo": _canonical(geo)}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def _input(name: str, receipt: dict[str, object], canonical_url: str) -> dict[str, object]:
    return {
        "input_id": name,
        "producer": PUBLISHER,
        "host": urlsplit(str(receipt["url"])).hostname,
        "role": "producer_publication",
        "canonical_url": canonical_url,
        "retrieval_url": receipt["url"],
        "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"],
        "bytes": receipt["bytes"],
        "sha256": receipt["sha256"],
        "license": LICENSE_URL,
        "attribution": "Federal Highway Administration, National Tunnel Inventory",
        "release_date": None,
        "release_date_unknown_reason": (
            "FHWA publishes annual NTI data by June 15; the download page does not "
            "state an exact publication date for this edition."
        ),
    }


def capture_fhwa_tunnel_inventory(storage: StagingStorageResource, *, version: str,
                                  capture_directory: str | None = None) -> dict[str, object]:
    """Capture the newest complete NTI XML release and stage an unapproved candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000:
        raise ValueError("disk_floor_before_acquisition")
    capture_root = (Path(capture_directory) if capture_directory else default_capture_dir("hifld-native-reconstruction", "170", "captures")) / f"{version}-{uuid4().hex}"
    capture_root = capture_root.absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=90,
                          headers={"User-Agent": "hifld-next-datasets/fhwa-nti-draft",
                                   "Accept-Encoding": "identity"}) as client:
            index_body, index_receipt = capture_get(
                client, LANDING_URL, capture_root, "release_index", max_bytes=MAX_BODY_BYTES)
            captures["release_index"] = (index_body, index_receipt)
            year, data_url = newest_release(index_body)
            data_body, data_receipt = capture_get(
                client, data_url, capture_root, "data", max_bytes=MAX_BODY_BYTES)
            captures["data"] = (data_body, data_receipt)
        features, audit = _validate(index_body, data_body, year, data_url)
    except Exception as exc:
        failed: dict[str, tuple[bytes, dict[str, object]]] = dict(captures)
        for receipt_path in sorted(capture_root.glob("*.receipt.json")):
            name = receipt_path.name.removesuffix(".receipt.json")
            try:
                receipt = json.loads(receipt_path.read_bytes())
                body = (capture_root / f"{name}.body").read_bytes()
                (capture_root / f"{name}.wire").read_bytes()
                failed.setdefault(name, (body, receipt))
            except (OSError, ValueError):
                continue
        if failed:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, failed, type(exc).__name__)
        raise
    identity = _code_identity()
    parquet_body = _parquet_bytes(features)
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    manifest = {
        "schema_version": 1, "approved": False,
        "title": LAYER_NAME,
        "description": ("FHWA National Tunnel Inventory annual XML release. Native producer "
                        "element values and portal coordinates are preserved; candidate is "
                        "not release-approved."),
        "publisher": PUBLISHER, "agency": "Federal Highway Administration", "source_url": data_url,
        "license": LICENSE_URL, "source_modified": None,
        "date_issued": None, "date_modified": None, "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "inputs": [
            _input("release_index", captures["release_index"][1], LANDING_URL),
            _input("data", captures["data"][1], data_url),
        ],
        "transformations": [{
            "operation": "native_nti_xml_to_geoparquet_candidate",
            "parameters": {
                "release_year": year,
                "source_fields": ("all 66 producer elements in fixed NTI order; native text "
                                  "values preserved, numeric elements typed per the SNTI XML schema"),
                "source_ids": ("I1 tunnel number preserved as text including leading zeros; "
                               "I3 state code preserved as integer; the (I3, I1) pair is the "
                               "documented native key, no substitute ID added"),
                "geometry": ("Point from I14 longitude and I13 latitude in EPSG:4326; no "
                             "reprojection or repair"),
                "element_data": ("Nested FHWAElements is the separate National Tunnel "
                                 "Inventory Element Data product and is not expanded into "
                                 "this point candidate; the raw XML retains it"),
                "candidate_status": ("unapproved; source, schema, ID, rights, mapping and "
                                     "release acceptance remain pending"),
            },
        }],
        "lineage": (
            "FHWA's own NTI download page is the producer publication index. The newest "
            "linked XML edition is captured in full; the BTS NTAD ArcGIS view still served "
            "the 2025 edition when this candidate was built. No mirror or archive is used."
        ),
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": ("Native FHWA element values, native (I3, I1) key components, and "
                       "portal coordinates retained; approval gates remain closed."),
            "objects": [{"path": parquet_name, **source_objects[parquet_name]}],
        }}},
        "unsupported_output_formats": {
            "geopackage": "No approved source-to-public schema mapping.",
            "pmtiles": "No display transformation or generalized geometry approved.",
        },
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {**audit, "official_layer_name": LAYER_NAME,
             "freshness_accepted": False, "source_release_accepted": False,
             "schema_contract_accepted": False, "id_contract_accepted": False,
             "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    (capture_root / "capture_manifest.json").write_bytes(_canonical({
        "captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
        "source_feature_count": audit["source_feature_count"],
        "source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"],
        "receipts": {name: receipt for name, (_, receipt) in ((name, captures[name]) for name in sorted(captures))},
    }) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs = {
        parquet_name: parquet_body,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
    }
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    outputs["metadata/candidate_ready.json"] = _canonical({
        "approved": False, "version": version, "objects": source_objects,
        "source_record_sha256": audit["source_record_sha256"],
        "source_population_complete": True,
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    }) + b"\n"
    fingerprint = _sha(_canonical(identity))
    result = {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["source_feature_count"], "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_release_year": audit["source_release_year"],
        "source_data_url": audit["source_data_url"],
        "source_null_geometry_count": 0, "source_invalid_geometry_count": 0,
        "candidate_ready": prefix + "/metadata/candidate_ready.json",
        "capture_directory": str(capture_root),
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False, "quality_check_passed": False,
    }
    if storage.object_snapshot(state_key) is not None:
        # A fresh acquisition of unchanged source content carries newly
        # timestamped receipts; the sealed candidate keeps its original
        # immutable provenance. Reuse verifies the re-acquired bytes against
        # the staged originals instead of the new receipts.
        original = json.loads(load_first_construction(storage, state_key, version, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("capture_receipts"), dict):
            raise ValueError("cached_first_candidate_receipts_missing")
        original_receipts = original["capture_receipts"]
        if set(original_receipts) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name in captures:
            if (storage.read_key(f"{prefix}/raw/{name}.body") != captures[name][0]
                    or storage.read_key(f"{prefix}/raw/{name}.wire")
                    != (capture_root / f"{name}.wire").read_bytes()):
                raise ValueError("cached_source_evidence_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                fingerprint, identity, audit["source_record_sha256"],
                                audit["source_schema_sha256"], original.get("objects", {}),
                                original_receipts)
        return {**result, "reused": True}
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
    for name, descriptor in descriptors.items():
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if (snapshot is None or snapshot.size != descriptor["bytes"]
                or storage.sha256_key(key) != descriptor["sha256"]
                or storage.object_snapshot(key) != snapshot):
            raise ValueError("candidate_output_physical_readback_failed:" + name)
    seal_ready_candidate(storage, state_key, version, fingerprint, {
        "processing_identity": identity,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": {name: receipt for name, (_, receipt) in captures.items()},
        "objects": descriptors,
    })
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {**result, "reused": False}
