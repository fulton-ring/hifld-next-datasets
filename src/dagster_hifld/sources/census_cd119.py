"""Strict reader for the retained, complete TIGER/Line 2025 CD119 capture.

The Census Bureau publishes one ZIP for each of 56 state/equivalent GEOIDs.
This adapter consumes a protected read-only capture directory, validates its
bounded receipt and every archive/member/feature, then writes the exact native
attributes and WKB values as one GeoParquet object. It makes no source or
release acceptance claim.
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import tempfile
from uuid import uuid4
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import pyarrow as pa
import pyarrow.parquet as pq
import httpx
import pyogrio
import pyproj
import shapely

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import (
    ConstructionStateError,
    canonical_construction_bundle,
    load_first_construction,
    seal_first_construction,
)
from dagster_hifld.http_capture import CaptureProgress, capture_http_response
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import StagingStorageResource

SLUG = "119th-congressional-districts"
CAPTURE_RECEIPTS_SHA256 = "ced533ee909842e07ae40c915d5fdb8d3357361a56bbeec7c90772445addc6eb"
SOURCE_PINS = {
    '01': ('9748310098d7dbec7370445dad6a7baa80ae4ee93cbccc7725a94784f553e679', 954811),
    '02': ('c63ff2d68afad2fc58ae7276bc31b1a11c9d60857620f9d714a798ce0dd177d0', 192550),
    '04': ('6eac2bd5c22110c21f0c22720f99317a72fe68cd943800ee266c970e54b2d674', 762892),
    '05': ('54e828d1d2b971cf7f2e82a9007be9a27835d347e0a1916f25d8ed454ba728b9', 794827),
    '06': ('107a5178d53c1c741b5e0fac48295a5f7a9424d516e246a44a34667c111c5481', 3888488),
    '08': ('cfe56e4190ab954484117e3a65bce57a53667e1442a438de8cf7c109c033071b', 640369),
    '09': ('5e92903e98ed2536ba8f5c8dc21556ac4cdc394885a03111ee14b7a966066392', 266604),
    '10': ('2cde9e20b8c9000994933ccac510fa4212809991b2e083cc1246222116d5d86a', 67790),
    '11': ('f2131e9ea9f8567fe611436227fc1359e943d44d0b726438ab58caf64e2b1c6c', 85022),
    '12': ('037f8b6e99d178eed234675391dfea75ac245860c0b323faa75e79c47ba4813f', 1051175),
    '13': ('faedb31a05744ce4c56e71d062af47a87a2b8906cf486361100aa478043b6f45', 1730811),
    '15': ('0b199a067aed88b76a50a6e1bc10d4a648ecb5356fcfcc281c0d359682f823de', 93866),
    '16': ('bdeb3de387294efc33ac5d788094108f15ece518612e28e6c3a0493c0de37866', 599264),
    '17': ('de57b7f113d93f687b5d9c488bf6751a5d1165b43e55e2d4b67e2f9ad998ca02', 1579099),
    '18': ('80aef61d31209a4dca2d4640f4da41a0cfd12012f8ad77aa51221c6ab1b1fe4f', 489154),
    '19': ('ea25830813790276df5d5b96257e2148cb1cf9fd42a0dedbfda4d97035a2fb04', 458211),
    '20': ('3ef2c2217a0243dc66cf20fb38982041987ad73a3c0e8ad44c362c1ede1f6e72', 403415),
    '21': ('7e7df9d5d6c4dd64f2184a29b7437b18ade947abce5ed4cfe36ae1a87cb0e932', 1043330),
    '22': ('423f460e7ac18c342ae1c6e0ed8da2100b911fca774b36a36dde7ae7c6184730', 1408922),
    '23': ('c92f890be463967aea2c0d09de468037e3b168cc9cd82a57d421c3d9c5e77c58', 358233),
    '24': ('f6cdfd5687a8b2edb86382177bea8901b731cbe6c4ded11d77aec7b67adce103', 726847),
    '25': ('d7d129c0b38114b3f555d84c1ae74cc7bc405562964e0a649e7b45d9c4325e9b', 458933),
    '26': ('2d5e68577dcc7638acfcad8727705a3e97ebca92f59cd16876b2e0efc8183066', 382593),
    '27': ('4b7a969ad818b03cda1dc8294b6d945280d6e5ac4771aed03d8548aa1dae69f7', 862757),
    '28': ('2bb1a723077a2ad00dc80763773cf69b2da88daa093ce138fddc0be159d44bc6', 643591),
    '29': ('1e289687c4dd7ec9be62823d63124036605ea93ffe27286aaa9d2818b1ba4090', 735859),
    '30': ('c07a831d7d44f6f9cfcda71d2c00e4834de0ae447ec90a7b60dc6c0365adb151', 592532),
    '31': ('01a4a2fd957ba05e45cf784c4c0219914bad80999426d5d6dce8db12a707635c', 319910),
    '32': ('7933a2088afcc31c9039c2bbdc3c13d0621ea2898da4dbf40671ff207aa91bf3', 312135),
    '33': ('5eb6dde5a4ad1201e0c59ae50ccf227216d6de77d49a9a4d8cfd16e303d17d30', 294779),
    '34': ('0bda6693ad95eaeccf6cf30e0d86842ae0928f36d8fc78b6cb9c4ac814fe2155', 954635),
    '35': ('7c02b3b426aedbce09448dce5805d930a2e2f0680f3717a3ac06936ae7da7cb2', 354119),
    '36': ('0955e0f7060dd43af98d939cbabb10df578901184472cbba57c91518c52991c7', 774013),
    '37': ('c4d64496850af2830296540e97c64cf5a9d2c2b87b58788f2d424662b9017b41', 2183701),
    '38': ('69f739250e5391b98cb74e9112b427eb8c5b93553cb661b31427a5bdd0d495bd', 347216),
    '39': ('d1a3c0cae01f10954063c6e70b23a37680c9451d4318bdd790d1f814dbce57de', 853533),
    '40': ('f77ecd86b7947555c4993fe9cf7f623c178ab552d930b7629c5132224c699260', 621886),
    '41': ('264688a2c95768dcde1361db0dec03252df8f52c5a918ea1892a8bc320e6f33a', 853453),
    '42': ('63945666b675bc4ddc6b99e06e36b6fdb389c132937b880491c370f77f532ae8', 1219049),
    '44': ('630690725a49dd22d6d87e9b4b73ad29eda3d4195287211261128351c93edaf0', 65631),
    '45': ('d7460187ccc9e15f7053a32909b18fec20c8667be7114ea71d1212285f579ffe', 1384149),
    '46': ('8f2d00d3d768d6c2645e11d0d188d5cc380cc8cfcfa2a64aaac9dbc71f329f04', 212185),
    '47': ('2e8f72d018cdd25151272b68604ed3fbefb7cbeecaf980237459ea8ee3dbe9c5', 1307353),
    '48': ('769b4ca318a4b06f9ced717d3a55b6dc94fd98205248c1496d3ce9f2927cda5c', 4212042),
    '49': ('a7a9561053f047b9e42d7875cee7f31236b559774839f0df495cd924b74f58a8', 630308),
    '50': ('c43cbd550708a94011e0795ba36d15e0745464bcf4cfda91afaf2778766ba9bc', 139575),
    '51': ('e9c7eabefcf65e957fb4d505c759dfed08e1a994dbeff34ba015d4a9b612d731', 1832978),
    '53': ('25e5d538f8a177d18fbafa83aa2e826bfeaadb73efbeca94d4072139679d7260', 686493),
    '54': ('a0bf87ff0e2258050f722aed4045138216570897c5e051a1f047ecb43a241a7e', 542523),
    '55': ('036c4d1c72b0c2857b09a987bf93c8e495b571760f09e07d3bb1a6435dedf660', 570696),
    '56': ('3f57657aac6f3c4d70dea359210494d309aecf1052dd75733b38fce9660d6a96', 93350),
    '60': ('04ebef3c60bcffd4b7bcffed1821b9d0106e7fcbe9c93a26a5c308466996a01d', 17690),
    '66': ('42dc5fd4bedea230c1ac4bf613e8c2768ad9f912d8ee33a6dfd51e00ea854b81', 19103),
    '69': ('c0e1663ce4eeba035b752037097e3183f0348f3438bf08e0cd5dc18e35d487c0', 21839),
    '72': ('4455f6aeb5bd62293d6bb494144a5b7a9875e0521b66c336310712e176b34c1e', 30299),
    '78': ('faafe11a85e8ac6faed7ce1234c700d18030d9f5eb70e8fbe6a87cc97314026a', 17611),
}
CAPTURE_BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2025/CD"
GEOIDS = (
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17",
    "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
    "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47",
    "48", "49", "50", "51", "53", "54", "55", "56", "60", "66", "69", "72", "78",
)
COUNTS = {
    "01": 7, "02": 1, "04": 9, "05": 4, "06": 52, "08": 8, "09": 6, "10": 1, "11": 1,
    "12": 28, "13": 14, "15": 2, "16": 2, "17": 18, "18": 9, "19": 4, "20": 4, "21": 6,
    "22": 6, "23": 2, "24": 8, "25": 9, "26": 13, "27": 8, "28": 4, "29": 8, "30": 2,
    "31": 3, "32": 4, "33": 3, "34": 12, "35": 3, "36": 26, "37": 14, "38": 1, "39": 15,
    "40": 5, "41": 6, "42": 17, "44": 2, "45": 7, "46": 1, "47": 9, "48": 38, "49": 4,
    "50": 1, "51": 11, "53": 10, "54": 2, "55": 8, "56": 1, "60": 1, "66": 1, "69": 1,
    "72": 1, "78": 1,
}
FIELDS = (
    "STATEFP", "CD119FP", "GEOID", "GEOIDFQ", "NAMELSAD", "LSAD", "CDSESSN", "MTFCC",
    "FUNCSTAT", "ALAND", "AWATER", "INTPTLAT", "INTPTLON", "wkb_geometry",
)
SOURCE_SCHEMA = (
    pa.string(), pa.string(), pa.string(), pa.string(), pa.string(), pa.string(), pa.string(),
    pa.string(), pa.string(), pa.int64(), pa.int64(), pa.string(), pa.string(), pa.binary(),
)
EXPECTED_MEMBERS = (".cpg", ".dbf", ".prj", ".shp", ".shp.ea.iso.xml", ".shp.iso.xml", ".shx")


class CD119QuarantineError(ValueError):
    """Capture, edition, schema, identifier, or full-population drift."""


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _write_immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        storage.write_key_if_unchanged(key, body, None)
    else:
        digest = storage.sha256_key(key)
        if storage.object_snapshot(key) != snapshot or snapshot.size != len(body) or digest != _sha(body):
            raise CD119QuarantineError("existing_staged_object_changed:" + key)


def _processing_identity() -> tuple[str, dict, str]:
    repo = Path(__file__).parents[3]
    paths = [
        "src/dagster_hifld/assets/census_cd119.py",
        "src/dagster_hifld/assets/ingest_registry.py",
        "src/dagster_hifld/assets/supported_assets.py",
        "src/dagster_hifld/sources/census_cd119.py",
        "src/dagster_hifld/construction_provenance.py",
        "src/dagster_hifld/construction_state.py",
        "src/dagster_hifld/resources.py",
        "src/dagster_hifld/output_policy.py",
        "src/dagster_hifld/source_manifest.py",
        "src/dagster_hifld/native_geoparquet_catalog.py",
    "src/dagster_hifld/http_capture.py",
        "src/dagster_hifld/validation.py",
        "pyproject.toml",
        "uv.lock",
    ]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    files = {name: _sha((repo / name).read_bytes()) for name in paths}
    identity = {"python": platform.python_version(), "pyarrow": pa.__version__,
        "pyogrio": pyogrio.__version__, "gdal": pyogrio.__gdal_version_string__,
        "proj": pyproj.proj_version_str, "geos": shapely.geos_version_string,
        "files": files}
    validate_construction_code_identity(repo, commit, files)
    fingerprint = _sha(_canonical(identity))
    return commit, identity, fingerprint


def _canonical(value) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _capture_root() -> Path:
    configured = os.environ.get("HIFLD_CD119_CAPTURE_DIR", "/tmp/hifld-native-reconstruction/387/capture")
    root = Path(configured).absolute()
    if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)) or not root.is_dir():
        raise CD119QuarantineError("capture_root_missing_or_aliased")
    return root


def _verify_receipts(root: Path, *, replay: bool) -> tuple[list[dict], dict[str, dict]]:
    path = root / "receipts.json"
    if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1:
        raise CD119QuarantineError("capture_receipt_missing_or_linked")
    raw = path.read_bytes()
    if replay and _sha(raw) != CAPTURE_RECEIPTS_SHA256:
        raise CD119QuarantineError("capture_receipt_roster_digest_changed")
    try:
        rows = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise CD119QuarantineError("capture_receipt_invalid_json") from exc
    expected = {f"tl_2025_{g}_cd119.zip" for g in GEOIDS}
    if type(rows) is not list or len(rows) != len(expected):
        raise CD119QuarantineError("capture_receipt_count_changed")
    by_name = {}
    for receipt in rows:
        if type(receipt) is not dict:
            raise CD119QuarantineError("capture_receipt_shape_changed")
        url = receipt.get("url")
        name = url.rsplit("/", 1)[-1] if isinstance(url, str) else ""
        if name not in expected or name in by_name:
            raise CD119QuarantineError("capture_receipt_roster_changed")
        if (url != f"{CAPTURE_BASE_URL}/{name}" or receipt.get("final_url") != url
            or receipt.get("status") != 200 or receipt.get("complete_body") is not True
            or type(receipt.get("bytes")) is not int or receipt["bytes"] <= 0
            or not re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("sha256", "")))
            or receipt.get("content_encoding") not in (None, "identity")):
            raise CD119QuarantineError("capture_receipt_claim_changed")
        geoid = name[8:10]
        expected_sha, expected_bytes = SOURCE_PINS[geoid]
        if receipt["sha256"] != expected_sha or receipt["bytes"] != expected_bytes:
            raise CD119QuarantineError("pinned_2025_source_content_drift:" + geoid)
        by_name[name] = receipt
    if set(by_name) != expected:
        raise CD119QuarantineError("capture_receipt_incomplete_roster")
    actual = {p.name for p in root.iterdir() if p.name.endswith(".zip")}
    if actual != expected:
        raise CD119QuarantineError("capture_archive_roster_changed")
    return rows, by_name


def _mode() -> str:
    value = os.environ.get("HIFLD_CD119_MODE")
    if value not in {"live", "replay"}:
        raise CD119QuarantineError("HIFLD_CD119_MODE_must_explicitly_be_live_or_replay")
    return value


def _acquire_live() -> Path:
    """Capture each pinned official ZIP to a unique immutable acquisition dir."""
    parent = Path(os.environ.get("HIFLD_CD119_ACQUISITIONS_DIR", "/tmp/hifld-native-reconstruction/387/acquisitions")).absolute()
    if ".." in parent.parts or any(p.is_symlink() for p in (parent, *parent.parents)):
        raise CD119QuarantineError("acquisition_root_missing_or_aliased")
    parent.mkdir(parents=True, exist_ok=True)
    acquisition = parent / ("cd119-" + uuid4().hex)
    acquisition.mkdir(mode=0o700, exist_ok=False)
    receipts = []
    for geoid in GEOIDS:
        name = f"tl_2025_{geoid}_cd119.zip"
        url = f"{CAPTURE_BASE_URL}/{name}"
        wire_path = acquisition / name
        decoded_path = acquisition / (name + ".decoded")
        capture_receipt = {"url": url, "final_url": None, "retrieved_at": datetime.now(timezone.utc).isoformat(),
                           "complete_body": False, "status": None, "bytes": 0, "sha256": None,
                           "content_encoding": None, "capture_error": None}
        try:
            with wire_path.open("xb") as wire, decoded_path.open("xb") as decoded:
                with httpx.Client(follow_redirects=True, timeout=180.0, headers={"Accept-Encoding": "identity"}) as client:
                    with client.stream("GET", url) as response:
                        capture_receipt["final_url"] = str(response.url)
                        outcome = capture_http_response(
                            response, max_wire_bytes=8 * 1024 * 1024, max_decoded_bytes=8 * 1024 * 1024,
                            reserve=lambda _: (_ for _ in ()).throw(OSError("cd119_capture_space_floor"))
                                if shutil.disk_usage(parent).free < 300 * 1024 * 1024 else None,
                            retain_wire=wire.write, retain_decoded=decoded.write,
                        )
                        if outcome.pending_wire:
                            (acquisition / (name + ".pending-wire")).write_bytes(outcome.pending_wire)
                        if outcome.pending_decoded:
                            (acquisition / (name + ".pending-decoded")).write_bytes(outcome.pending_decoded)
                        capture_receipt.update({
                            "status": outcome.status_code, "complete_body": outcome.complete,
                            "content_encoding": outcome.content_encoding,
                            "content_length": outcome.content_length,
                            "wire_bytes_received": outcome.wire_bytes_received,
                            "wire_bytes_retained": outcome.wire_bytes_retained,
                            "wire_sha256": outcome.wire_sha256,
                            "decoded_bytes_produced": outcome.decoded_bytes_produced,
                            "decoded_bytes_retained": outcome.decoded_bytes_retained,
                            "decoded_sha256": outcome.decoded_sha256,
                            "httpx_bytes_downloaded": outcome.httpx_bytes_downloaded,
                            "stream_exhausted": outcome.stream_exhausted,
                            "decoder_eof": outcome.decoder_eof,
                            "capture_error": outcome.error_code,
                        })
                        if (not outcome.complete or outcome.status_code != 200 or str(response.url) != url
                            or outcome.content_encoding != "identity"
                            or outcome.wire_sha256 != outcome.decoded_sha256
                            or outcome.wire_bytes_retained != outcome.decoded_bytes_retained):
                            raise CD119QuarantineError("official_source_capture_incomplete_or_changed:" + geoid)
                        wire.flush(); decoded.flush()
            capture_receipt.update({"bytes": wire_path.stat().st_size, "sha256": _sha(wire_path.read_bytes())})
            expected_sha, expected_bytes = SOURCE_PINS[geoid]
            if capture_receipt["sha256"] != expected_sha or capture_receipt["bytes"] != expected_bytes:
                raise CD119QuarantineError("pinned_2025_source_content_drift:" + geoid)
            decoded_path.unlink()
        except Exception as exc:
            capture_receipt["capture_error"] = capture_receipt.get("capture_error") or type(exc).__name__
            capture_receipt["bytes"] = wire_path.stat().st_size if wire_path.exists() else 0
            capture_receipt["sha256"] = _sha(wire_path.read_bytes()) if wire_path.exists() else None
            receipts.append(capture_receipt)
            (acquisition / "receipts.json").write_bytes(_canonical(receipts))
            (acquisition / (name + ".receipt.json")).write_bytes(_canonical(capture_receipt))
            raise CD119QuarantineError("live_acquisition_preserved_but_quarantined:" + geoid) from exc
        receipts.append(capture_receipt)
        (acquisition / (name + ".receipt.json")).write_bytes(_canonical(capture_receipt))
    (acquisition / "receipts.json").write_bytes(_canonical(receipts))
    return acquisition


def _read_archive(root: Path, name: str, receipt: dict, expected_geoid: str) -> pa.Table:
    path = root / name
    if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1:
        raise CD119QuarantineError("capture_archive_missing_or_linked:" + name)
    if path.stat().st_size != receipt["bytes"] or _sha(path.read_bytes()) != receipt["sha256"]:
        raise CD119QuarantineError("capture_archive_receipt_mismatch:" + name)
    with zipfile.ZipFile(path) as archive:
        members = archive.infolist()
        names = [item.filename for item in members]
        stem = name[:-4]
        expected_names = {stem + suffix for suffix in EXPECTED_MEMBERS}
        if (len(names) != len(set(names)) or set(names) != expected_names
            or any(PurePosixPath(n).is_absolute() or ".." in PurePosixPath(n).parts or len(PurePosixPath(n).parts) != 1 for n in names)
            or any(item.flag_bits & 1 or stat.S_ISLNK(item.external_attr >> 16) for item in members)):
            raise CD119QuarantineError("archive_member_roster_or_path_changed:" + name)
        if sum(len(archive.read(item)) for item in members) != sum(item.file_size for item in members):
            raise CD119QuarantineError("archive_member_crc_or_length_changed:" + name)
    meta, table = pyogrio.read_arrow("/vsizip/" + str(path))
    if (tuple(table.schema.names) != FIELDS or tuple(f.type for f in table.schema) != SOURCE_SCHEMA
        or meta.get("crs") != "EPSG:4269" or meta.get("geometry_type") != "Polygon"
        or table.num_rows != COUNTS[expected_geoid]):
        raise CD119QuarantineError("source_schema_crs_geometry_or_count_changed:" + name)
    for row in table.select(["STATEFP", "CD119FP", "GEOID", "GEOIDFQ", "CDSESSN"]).to_pylist():
        if (row["STATEFP"] != expected_geoid or row["GEOID"] != row["STATEFP"] + row["CD119FP"]
            or row["GEOIDFQ"] != "5001900US" + row["GEOID"] or row["CDSESSN"] != "119"):
            raise CD119QuarantineError("source_literal_identifier_or_congress_changed:" + name)
    if any(table[field].null_count for field in FIELDS):
        raise CD119QuarantineError("source_null_attribute:" + name)
    geometries = shapely.from_wkb(table["wkb_geometry"].to_pylist())
    if table["wkb_geometry"].null_count or bool(shapely.is_empty(geometries).any()) or bool((~shapely.is_valid(geometries)).any()):
        raise CD119QuarantineError("source_geometry_null_empty_or_invalid:" + name)
    return table


def _geo_metadata(geometry_types: list[str]) -> dict[bytes, bytes]:
    crs = pyproj.CRS.from_epsg(4269).to_json_dict()
    body = {"version": "1.1.0", "primary_column": "wkb_geometry", "columns": {
        "wkb_geometry": {"encoding": "WKB", "geometry_types": geometry_types, "crs": crs}}}
    return {b"geo": _canonical(body)}


def stage_cd119_candidate(storage: StagingStorageResource, dataset: str, file_slug: str) -> dict:
    """Stage source-pinned complete population and exact typed/WKB GeoParquet."""
    mode = _mode()
    root = _acquire_live() if mode == "live" else _capture_root()
    receipts, by_name = _verify_receipts(root, replay=mode == "replay")
    expected = {f"tl_2025_{g}_cd119.zip" for g in GEOIDS}
    source_tables = []
    source_ids = set()
    source_wkb_hash = hashlib.sha256()
    for geoid in GEOIDS:
        name = f"tl_2025_{geoid}_cd119.zip"
        table = _read_archive(root, name, by_name[name], geoid)
        for record in table.to_pylist():
            identifier = record["GEOID"]
            if identifier in source_ids:
                raise CD119QuarantineError("duplicate_source_GEOID:" + identifier)
            source_ids.add(identifier)
            source_wkb_hash.update(identifier.encode() + b"\0" + record["wkb_geometry"])
        source_tables.append(table)
    table = pa.concat_tables(source_tables, promote_options="none")
    if len(expected) != 56 or table.num_rows != 444 or len(source_ids) != 444:
        raise CD119QuarantineError("national_source_population_or_ID_count_changed")
    code_commit, processing_identity, processing_fingerprint = _processing_identity()
    geometry_types = sorted({g.geom_type for g in shapely.from_wkb(table["wkb_geometry"].to_pylist())})
    if not geometry_types or not set(geometry_types).issubset({"Polygon", "MultiPolygon"}):
        raise CD119QuarantineError("source_geometry_type_drift")
    candidate_identity = _sha(_canonical({
        "source_file_sha256": {f"tl_2025_{g}_cd119.zip": by_name[f"tl_2025_{g}_cd119.zip"]["sha256"] for g in GEOIDS},
        "source_rows": table.num_rows, "unique_geoids": len(source_ids),
        "source_wkb_identity_sha256": source_wkb_hash.hexdigest(),
        "source_schema": [{"name": f.name, "type": str(f.type)} for f in table.schema],
        "crs": "EPSG:4269", "geometry_types": geometry_types,
        "processing_fingerprint": processing_fingerprint,
    }))
    version = f"v0.0.0-cd119-{candidate_identity[:16]}-{processing_fingerprint[:16]}"
    # Keep source attributes and WKB unchanged. The output column metadata is
    # added for GeoParquet interoperability; no feature values are transformed.
    table = table.replace_schema_metadata({**(table.schema.metadata or {}), **_geo_metadata(geometry_types)})
    with tempfile.TemporaryDirectory(prefix="cd119-native-") as tmp:
        parquet_path = Path(tmp) / f"{file_slug}.parquet"
        pq.write_table(table, parquet_path, compression="zstd", use_dictionary=False, write_statistics=True)
        decoded = pq.read_table(parquet_path)
        if (decoded.num_rows != table.num_rows or decoded.schema.names != table.schema.names
            or tuple(field.type for field in decoded.schema) != tuple(field.type for field in table.schema)
            or not decoded.equals(table)):
            raise CD119QuarantineError("native_parquet_full_value_or_type_roundtrip_failed")
        parquet = parquet_path.read_bytes()
    target = storage.build_target_location(dataset, file_slug, version, "").rstrip("/")
    output_key = f"{target}/geoparquet/{file_slug}.parquet"
    _write_immutable(storage, output_key, parquet)
    output_sha = _sha(parquet)
    input_receipts = []
    for name in sorted(expected):
        receipt = by_name[name]
        input_receipts.append({"input_id": "census-tiger2025-cd119:" + receipt["sha256"],
            "producer": "U.S. Census Bureau", "host": "www2.census.gov", "role": "producer_publication",
            "canonical_url": receipt["url"], "retrieval_url": receipt["url"],
            "observed_final_url": receipt["final_url"], "retrieved_at": receipt["retrieved_at"],
            "http_status": 200, "sha256": receipt["sha256"], "bytes": receipt["bytes"],
            "license": "https://www.census.gov/about/policies/quality/guidelines/copyright.html",
            "attribution": "U.S. Census Bureau, TIGER/Line 2025 Congressional Districts (CD119).",
            "release_date_unknown_reason": "TIGER/Line 2025 edition is identified by the official archive names; a producer publication date was not established."})
    manifest = {"schema_version": 1, "processor": "HIFLD Next", "approved": False,
        "title": "119th Congressional Districts — TIGER/Line 2025 working draft",
        "description": "Complete Census Bureau 2025 CD119 candidate, with original Census attributes and WKB geometry.",
        "publisher": "U.S. Census Bureau", "agency": "U.S. Census Bureau",
        "code_commit": code_commit, "processing_fingerprint": processing_fingerprint,
        "source_url": CAPTURE_BASE_URL + "/", "inputs": input_receipts,
        "transformations": [{"operation": "read_complete_state_equivalent_sources_and_write_GeoParquet",
            "parameters": {"source_files": 56, "source_rows": 444, "source_GEOID_count": 444,
                "attributes_preserved": list(FIELDS[:-1]), "geometry": "native EPSG:4269 Polygon WKB, no repair or reprojection"}}],
        "unsupported_output_formats": {"shapefile_zip": "No accepted reconstructed national shapefile package contract.",
            "geopackage": "No accepted reconstructed GeoPackage contract.", "pmtiles": "No approved reprojection or quantization contract."},
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Pinned source-native attributes and WKB; candidate only, with release approval unset.",
            "objects": [{"path": f"geoparquet/{file_slug}.parquet", "sha256": output_sha, "bytes": len(parquet)}]}}},
        "lineage": {"archive_receipts_sha256": _sha((root / "receipts.json").read_bytes()),
            "source_capture_directory": str(root), "source_wkb_identity_sha256": source_wkb_hash.hexdigest(),
            "source_ID_count": len(source_ids)}}
    provenance_projection(manifest)
    manifest_key = f"{target}/metadata/source_manifest.json"
    receipt_body = (root / "receipts.json").read_bytes()
    construction_key = f"_construction-state/{dataset}/{version}.json"
    payload = {
        "capture": {"kind": "external_preserved_read_only_capture", "path": str(root),
            "receipts_sha256": _sha(receipt_body), "archive_count": len(receipts)},
        "receipt_bytes_sha256": _sha(receipt_body), "receipt_bytes": len(receipt_body),
        "processing_identity": processing_identity, "manifest": manifest,
        "native_output": {"key": output_key, "sha256": output_sha, "bytes": len(parquet)},
        "source_wkb_identity_sha256": source_wkb_hash.hexdigest(), "source_ID_count": len(source_ids),
        "candidate_identity": candidate_identity,
    }
    bundle = canonical_construction_bundle(version, processing_fingerprint, payload)
    try:
        original = load_first_construction(storage, construction_key, version, processing_fingerprint)
        first_run = False
    except ConstructionStateError as exc:
        if exc.code != "construction_missing_original_state":
            raise CD119QuarantineError("construction_state_unavailable:" + exc.code) from exc
        original = seal_first_construction(storage, construction_key, version, processing_fingerprint, bundle)
        first_run = True
    try:
        original_payload = json.loads(original)["payload"]
    except (ValueError, KeyError, TypeError) as exc:
        raise CD119QuarantineError("construction_original_payload_invalid") from exc
    if (original_payload.get("candidate_identity") != candidate_identity
        or original_payload.get("source_wkb_identity_sha256") != source_wkb_hash.hexdigest()
        or original_payload.get("source_ID_count") != len(source_ids)
        or original_payload.get("native_output") != payload["native_output"]
        or original_payload.get("processing_identity") != processing_identity):
        raise CD119QuarantineError("construction_original_candidate_or_code_drift")
    if first_run:
        _write_immutable(storage, f"{target}/metadata/source_capture_receipts.json", receipt_body)
        _write_immutable(storage, manifest_key, _canonical(manifest))
        if _sha(receipt_body) != original_payload.get("receipt_bytes_sha256"):
            raise CD119QuarantineError("construction_first_receipt_changed_during_seal")
    else:
        original_receipt = storage.read_key(f"{target}/metadata/source_capture_receipts.json")
        if _sha(original_receipt) != original_payload.get("receipt_bytes_sha256"):
            raise CD119QuarantineError("construction_original_receipt_missing_or_changed")
        manifest = original_payload["manifest"]
        if _canonical(manifest) != storage.read_key(manifest_key):
            raise CD119QuarantineError("construction_original_manifest_missing_or_changed")
        _write_immutable(storage, manifest_key, _canonical(manifest))
        observation_key = f"{target}/metadata/acquisition_observations/{uuid4().hex}/receipts.json"
        _write_immutable(storage, observation_key, receipt_body)
    return {"dataset_slug": dataset, "file_slug": file_slug, "version": version,
        "source_archive_count": len(receipts), "source_feature_count": table.num_rows,
        "source_unique_geoid_count": len(source_ids), "source_wkb_identity_sha256": source_wkb_hash.hexdigest(),
        "capture_receipts_sha256": _sha(receipt_body), "prebuilt_geoparquet_sha256": output_sha,
        "prebuilt_geoparquet_bytes": len(parquet), "source_manifest_key": manifest_key,
        "construction_key": construction_key, "code_commit": code_commit,
        "processing_fingerprint": processing_fingerprint, "candidate_identity": candidate_identity,
        "acquisition_mode": mode,
        "source_accepted": False, "release_approved": False}
