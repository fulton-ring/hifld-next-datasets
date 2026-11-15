"""Candidate-only capture of the official Census 2020 VTD state publications.

The HIFLD "Voting Districts" inventory row points to the producing agency's own
maintained publication: the Census 2020 Redistricting Data (P.L. 94-171) TIGER/Line
Voting District (VTD) state shapefiles at www2.census.gov TIGER2020PL. All 49
producer state publications (the states that participated in the 2020 VTD
project), their complete official directory roster, original DBF-declared
attributes, file-local OGC FIDs and native EPSG:4269 Polygon/MultiPolygon
coordinates are retained. No source, baseline, schema mapping, territorial
interpretation or release is accepted by this code.
"""
from __future__ import annotations

import collections
import hashlib
import json
import platform
import re
import shutil
import stat
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Iterable
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.provenance import validate_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "voting-districts"
FILE_SLUG = DATASET_SLUG
DIRECTORY = "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/VTD/2020/"
PUBLISHER = "U.S. Census Bureau, Geography Division"
LICENSE = "TIGER/Line products are not copyrighted; U.S. Census Bureau acknowledgement required (captured producer ISO resourceConstraints)."
RELEASE_EVIDENCE = (
    "The producer landing page dates the 2020 Census Redistricting Data (P.L. 94-171) "
    "Shapefiles 'Released January 19, 2021 - February 11, 2021'; the producer ISO series "
    "series_tl_2020_vtd20 records publication 2020-05 with metadata revision 2022-10-26. "
    "Fetch/edit/HTTP Last-Modified are not treated as release days."
)
OUTPUT_NAME = f"{DATASET_SLUG}.parquet"
SOURCE_ARCHIVE_FIELD = "source_archive"
LISTING_NAME = "census2020pl_vtd_directory"
LISTING_URL = DIRECTORY
MAX_ARCHIVE_BYTES = 60_000_000
MAX_LISTING_BYTES = 2_000_000
CAPTURE_ATTEMPTS = 3
MIN_FREE_BYTES = 1_500_000_000
MEMBER_SUFFIXES = (".cpg", ".dbf", ".prj", ".shp", ".shp.ea.iso.xml", ".shp.iso.xml", ".shx")
SOURCE_FIELDS = ("OGC_FID", "STATEFP20", "COUNTYFP20", "VTDST20", "GEOID20", "VTDI20",
                 "NAME20", "NAMELSAD20", "LSAD20", "MTFCC20", "FUNCSTAT20", "ALAND20",
                 "AWATER20", "INTPTLAT20", "INTPTLON20", "wkb_geometry")
FIELDS = (SOURCE_ARCHIVE_FIELD, *SOURCE_FIELDS)
SOURCE_TYPES = (pa.int64(), *[pa.string()] * 10, pa.int64(), pa.int64(), pa.string(),
                pa.string(), pa.binary())
TYPES = (pa.string(), *SOURCE_TYPES)
DBF_FIELDS = [
    ("STATEFP20", "C", 2, 0), ("COUNTYFP20", "C", 3, 0), ("VTDST20", "C", 6, 0),
    ("GEOID20", "C", 11, 0), ("VTDI20", "C", 1, 0), ("NAME20", "C", 100, 0),
    ("NAMELSAD20", "C", 100, 0), ("LSAD20", "C", 2, 0), ("MTFCC20", "C", 5, 0),
    ("FUNCSTAT20", "C", 1, 0), ("ALAND20", "N", 14, 0), ("AWATER20", "N", 14, 0),
    ("INTPTLAT20", "C", 11, 0), ("INTPTLON20", "C", 12, 0),
]
CANDIDATE_FILES = (
    "geoparquet/" + OUTPUT_NAME,
    "metadata/source_manifest.json",
    "metadata/processing_identity.json",
    "metadata/source_capture.json",
    "metadata/source_audit.json",
    "metadata/baseline_comparison.json",
)
# Exact producer archives and complete-population pins from the dated
# capture run (2026-11-14, www2.census.gov TIGER2020PL VTD directory).
PINS = {
    "tl_2020_01_vtd20": {"bytes": 16271191, "sha256": "d85d4959c729ec6a52f388ea128a58ef38138c73eb3b63502970a243c577e327"},
    "tl_2020_02_vtd20": {"bytes": 10679869, "sha256": "cc8321fb4fdba7d0cbe5b723e78efccd30341cd77d807eb299b4cb32b4e07c0f"},
    "tl_2020_04_vtd20": {"bytes": 9281742, "sha256": "d2b6ea0742204b134cfd2ba28607d5edf314d084ff2d4b4c31fe11ce2ebe775c"},
    "tl_2020_05_vtd20": {"bytes": 12959118, "sha256": "4bcc5c2e9a2b647dd56e2f4516e85b1f5265098503a8cd09aa18c81a35fc4bca"},
    "tl_2020_08_vtd20": {"bytes": 12445608, "sha256": "5461fd15f66d19b626214728b2552a5db408aa291c6ae2e0be88874f821112f5"},
    "tl_2020_09_vtd20": {"bytes": 2588867, "sha256": "3d43f9663224d07e8f68f4d51809fede9ca5867f67de07ab1a7b005768edaf84"},
    "tl_2020_10_vtd20": {"bytes": 1383235, "sha256": "8787797ec666f5bea3b74c331d54b1606250d1226ccc8b0993c389cb5abce4b0"},
    "tl_2020_11_vtd20": {"bytes": 313986, "sha256": "2feda4cb7cf25f691cc4b89756ba01b15698c0c15c0438c0de1f6b1dc656c571"},
    "tl_2020_12_vtd20": {"bytes": 17643267, "sha256": "fd0c0da3c0104d920cb0055132a6f22da5dd2f872daeec1db4f01a77765064c2"},
    "tl_2020_13_vtd20": {"bytes": 17139192, "sha256": "9ff0a518d962f9b571c450925ecc342ef38eb935354266e334ae35c94e8dc648"},
    "tl_2020_16_vtd20": {"bytes": 7872555, "sha256": "be5750373f67e129d515763360ceefbc58ad4c185008c9d1fed7deb7a339ef2a"},
    "tl_2020_17_vtd20": {"bytes": 15266165, "sha256": "adbdd52c7e295139027ae87ed6465675dc73875e657f256dedc4d0f6e48419fe"},
    "tl_2020_18_vtd20": {"bytes": 10003977, "sha256": "b1114d91d6f5ddf0ec75247e0787092e7e77fc110c6aa8e2b62989b8d12a46ed"},
    "tl_2020_19_vtd20": {"bytes": 5615711, "sha256": "0fa3b544bfe29e38640394a65dec955461f4f165b888605298c86c9f3ecd55bc"},
    "tl_2020_20_vtd20": {"bytes": 6691609, "sha256": "ad04340dc6253825c765559590700e5f39a3cad3ff1af079e9dc6933a5845cfc"},
    "tl_2020_21_vtd20": {"bytes": 20013703, "sha256": "4850f1b4d3b67495dc0ba3dcc1eef87b079f2f9c44a8b1db5864484977b4c87c"},
    "tl_2020_22_vtd20": {"bytes": 14307529, "sha256": "305c1b5fe61332f9af55f44b1370b615b96a46057adbe674793df521ad279c02"},
    "tl_2020_23_vtd20": {"bytes": 1020195, "sha256": "5eaa62acc3271cbee8f60d30e7f792637c4dab5ab1d5bc8a85f6da9d07d232b1"},
    "tl_2020_24_vtd20": {"bytes": 11468028, "sha256": "007ff27ed0f5d0106e1be95221909bc8572e60ffd09845d981a40e2a944c2a79"},
    "tl_2020_25_vtd20": {"bytes": 5413985, "sha256": "3397fbef02d76de9ea0141c4987e882330f247a1fd16439982576eb7d9dd15a8"},
    "tl_2020_26_vtd20": {"bytes": 7345254, "sha256": "9fb7fd8fd9d9494326d6752f165a02500ddb6d5a13aebc1db06bea6381f7610b"},
    "tl_2020_27_vtd20": {"bytes": 10167427, "sha256": "180ee944a3fc35a298223d879f1ac7ea8924fb4ffe923329f10fe2d8e213701d"},
    "tl_2020_28_vtd20": {"bytes": 13580578, "sha256": "b2739ca24da7ae9f4c68347e36c78870a0abdde62ccc18ce4a2bf6809a010eba"},
    "tl_2020_29_vtd20": {"bytes": 11305575, "sha256": "24a01706a624eb90d249215387a176ad3d63343a1348d0fac0789828b3dd84f2"},
    "tl_2020_30_vtd20": {"bytes": 6577398, "sha256": "ae93c1569227cab23d65c9dee6c3c35aa1d71a83a60ec20af2d9d5b2d0c956df"},
    "tl_2020_31_vtd20": {"bytes": 3294509, "sha256": "2d22bbc8fcd115105fec245ba212dcfc134625a31287f87a5eb91c73d7a9f564"},
    "tl_2020_32_vtd20": {"bytes": 5128916, "sha256": "dffd431b0f91b0bc690e79b74923f875f14ac124a15dd85e22b6c8bc25c5c5fa"},
    "tl_2020_33_vtd20": {"bytes": 1253812, "sha256": "d87eac7cd77e23ee22619c2970740a5e87d154efba72f8c3cd20998c81843ce5"},
    "tl_2020_34_vtd20": {"bytes": 11624880, "sha256": "d5d978f19e84af3313dd433a463b2b7d2f475c1e077ff06ca216ad6e1d83d044"},
    "tl_2020_35_vtd20": {"bytes": 11770473, "sha256": "560bc5f4b7066c2dbf73092d6cd5afd15ef12e9e2560e65431d223264008944c"},
    "tl_2020_36_vtd20": {"bytes": 16642299, "sha256": "7f8c1e456d69e670944168354a6a4ccce5e1f202c4457f69e919640b40f56532"},
    "tl_2020_37_vtd20": {"bytes": 19297243, "sha256": "6196ebf40ac1c93839e67f88d351eb00f081bc56ca8f73fa900be57bb11c42d2"},
    "tl_2020_38_vtd20": {"bytes": 2268486, "sha256": "de61e3183ceb06987a6928a053a11a3a71d3e33517c0bbb5d13b6e9f218fc4b4"},
    "tl_2020_39_vtd20": {"bytes": 16218634, "sha256": "a0c2125fc318abf4f2897c7dea1980363e3a4183c4fb522f00603237700d36e5"},
    "tl_2020_40_vtd20": {"bytes": 10074855, "sha256": "d1c906a48074acc3f95fa0d6a6c4ad90ca707c50d6c0de78c0db415e494d53f8"},
    "tl_2020_42_vtd20": {"bytes": 19472350, "sha256": "a2568ba2a1143341031d11482fd179be838f91dbdf0425ed024a378f8cc5ed79"},
    "tl_2020_44_vtd20": {"bytes": 1380912, "sha256": "0cff311d27b6f7c995ff7c65057c228e5293c7868efc3184a894f765550d1e9b"},
    "tl_2020_45_vtd20": {"bytes": 12567306, "sha256": "5e6ddc3aebba1000cd68db4d2d14cf1778384fdc48162712999e94955a5dc274"},
    "tl_2020_46_vtd20": {"bytes": 4315722, "sha256": "b48632cb589f4b6c45b5a353fd04e3e9426bbb08503440687960a0498ac06edb"},
    "tl_2020_47_vtd20": {"bytes": 20326151, "sha256": "9597f39b0f8ad72986181158148fea82155890d97048ed5729a16a9b197ebf7a"},
    "tl_2020_48_vtd20": {"bytes": 42957967, "sha256": "51c7c66bf282b85b3a6ca1ba96f896df69b324fc6a766dbe09023fb60d86f540"},
    "tl_2020_49_vtd20": {"bytes": 11414697, "sha256": "4cd172a8012bc27b8b11baa874d9e50417c3df9e44d26f10fb7f1bb557f24c42"},
    "tl_2020_50_vtd20": {"bytes": 682521, "sha256": "1d6d8bc3b720ca5c61f5da6ff99d56073f845474c1683ebf1c04109a19930ece"},
    "tl_2020_51_vtd20": {"bytes": 19997192, "sha256": "9ce75ec39b36e4286a15de7a29d935e18abc3e75e1b86af989a57a57f9fe14c4"},
    "tl_2020_53_vtd20": {"bytes": 13111906, "sha256": "0b57eae199918a47b9667a616a33a6d0713b6769ffe93453daf75e5246dd3866"},
    "tl_2020_54_vtd20": {"bytes": 9421435, "sha256": "a6f336f25474ff3880dcd7086830f511d314d09029c6c054b2e011b35876408e"},
    "tl_2020_55_vtd20": {"bytes": 14637168, "sha256": "a7d6bddc5896dbc332c0fd7cc911d89aae079e0562ec9a79b459226c73c190f1"},
    "tl_2020_56_vtd20": {"bytes": 3920047, "sha256": "b8ab8bdb4d24a522ce05a84200ab8a26984dfa5e71d1362a7761597cc3f903f1"},
    "tl_2020_72_vtd20": {"bytes": 4712816, "sha256": "858fb7c7ee66c73662f3dcb77a42dcdd36ca135a64dd412cb96eb8a5ffeb13d4"},
}

STATE_COUNTS = {
    "01": 1837,
    "02": 456,
    "04": 1538,
    "05": 2747,
    "08": 3108,
    "09": 766,
    "10": 412,
    "11": 143,
    "12": 7211,
    "13": 2698,
    "16": 931,
    "17": 10084,
    "18": 5159,
    "19": 2536,
    "20": 4240,
    "21": 3693,
    "22": 3540,
    "23": 199,
    "24": 2042,
    "25": 2157,
    "26": 4805,
    "27": 4110,
    "28": 1834,
    "29": 4604,
    "30": 666,
    "31": 1402,
    "32": 2102,
    "33": 326,
    "34": 6361,
    "35": 1977,
    "36": 14191,
    "37": 2666,
    "38": 421,
    "39": 8941,
    "40": 1947,
    "42": 9178,
    "44": 423,
    "45": 2268,
    "46": 785,
    "47": 1965,
    "48": 9007,
    "49": 2745,
    "50": 285,
    "51": 2465,
    "53": 7434,
    "54": 1051,
    "55": 7059,
    "56": 512,
    "72": 1417,
}

NULL_COUNTS = {
    "ALAND20": 0,
    "AWATER20": 0,
    "COUNTYFP20": 0,
    "FUNCSTAT20": 0,
    "GEOID20": 0,
    "INTPTLAT20": 0,
    "INTPTLON20": 0,
    "LSAD20": 0,
    "MTFCC20": 0,
    "NAME20": 0,
    "NAMELSAD20": 0,
    "OGC_FID": 0,
    "STATEFP20": 0,
    "VTDI20": 0,
    "VTDST20": 0,
    "wkb_geometry": 0,
}

VTDI_COUNTS = {
    "A": 137078,
    "P": 21366,
}

LSAD_COUNTS = {
    "00": 120535,
    "V1": 12679,
    "V2": 25230,
}

FUNCSTAT_COUNTS = {
    "N": 137055,
    "S": 21389,
}

MTFCC_COUNTS = {
    "G5240": 158444,
}

GEOM_TYPE_COUNTS = {
    "Polygon": 153732,
    "MultiPolygon": 4712,
}

DRIVER_GEOMETRY_TYPE = "Polygon"
COUNT = 158444
COORDINATES = 55060259
BOUNDS = [-179.231086, 17.831509, 179.859681, 71.439786]
GEOIDS_SHA256 = "42d51b2463f039da9a064ec0818769bfd38412e440fc36fbca7283dcfc3af7c4"
SOURCE_RECORDS_SHA256 = "0f8673910f9662cceddc6fb70322ed6eb2604f7bf072b4701778b5086e4c82f0"

class VotingDistrictsError(ValueError):
    """A source, capture, or candidate invariant changed; nothing is accepted."""


def encode(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                       allow_nan=False) + "\n").encode("utf-8")


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def pins_complete() -> bool:
    required = (PINS, STATE_COUNTS, NULL_COUNTS, VTDI_COUNTS, LSAD_COUNTS,
                FUNCSTAT_COUNTS, MTFCC_COUNTS, GEOM_TYPE_COUNTS)
    if not all(required) or COUNT <= 0 or not BOUNDS or not GEOIDS_SHA256 or not SOURCE_RECORDS_SHA256:
        return False
    if sum(int(pin["bytes"]) for pin in PINS.values()) <= 0 or len(STATE_COUNTS) != len(PINS):
        return False
    return True


def urls() -> dict[str, str]:
    return {LISTING_NAME: LISTING_URL,
            **{name: DIRECTORY + name + ".zip" for name in PINS}}


def capture_limits() -> dict[str, int]:
    return {LISTING_NAME: MAX_LISTING_BYTES,
            **{name: MAX_ARCHIVE_BYTES for name in PINS}}


def _safe_local_staging(storage: StagingStorageResource) -> Path:
    if (isinstance(storage, PublishedStorageResource) or storage.bucket
            or not storage.use_local or not storage.local_dir or storage.prefix):
        raise VotingDistrictsError("explicit_unprefixed_local_staging_required")
    root = Path(storage.local_dir).absolute()
    if any(path.is_symlink() for path in [root, *root.parents]):
        raise VotingDistrictsError("staging_path_symlink_refused")
    root.mkdir(parents=True, exist_ok=True)
    if any(path.is_symlink() or (path.is_file() and path.stat().st_nlink != 1)
           for path in root.rglob("*")):
        raise VotingDistrictsError("aliased_local_staging_object")
    return root


def _quarantine(storage: StagingStorageResource, captures: dict[str, tuple[bytes, dict]],
                reason: str) -> str:
    """Retain every attempted byte plus its receipt before refusing a bad capture."""
    root = _safe_local_staging(storage)
    key = f"_quarantine/{DATASET_SLUG}/{uuid4().hex}"
    target = root / key
    target.mkdir(parents=True, exist_ok=True)
    for name, (body, receipt) in captures.items():
        if re.fullmatch(r"[A-Za-z0-9_.-]+", name) is None:
            raise VotingDistrictsError("unsafe_capture_name")
        (target / (name + ".body")).write_bytes(body)
        (target / (name + ".receipt.json")).write_bytes(encode(receipt))
    (target / "reason.json").write_bytes(encode({"approved": False, "reason": reason}))
    return key


def _stream_capture(url: str, limit: int) -> tuple[bytes, dict[str, object]]:
    body = bytearray()
    receipt: dict[str, object] = {
        "url": url, "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(300.0, connect=30.0),
                      headers={"Accept-Encoding": "identity"}) as client:
        with client.stream("GET", url) as response:
            receipt.update(
                status=response.status_code, final_url=str(response.url),
                requested_accept_encoding="identity",
                content_encoding=response.headers.get("content-encoding"),
                content_length=response.headers.get("content-length"),
            )
            for block in response.iter_bytes(None):
                body.extend(block)
                if len(body) > limit:
                    raise VotingDistrictsError("oversized_or_changed_source")
            receipt["wire_bytes_downloaded"] = response.num_bytes_downloaded
    receipt.update(bytes=len(body), sha256=digest(bytes(body)), complete_body=True)
    if (
        receipt["status"] != 200 or receipt["final_url"] != url
        or receipt["content_encoding"] not in (None, "identity")
        or (receipt["content_length"] is not None
            and int(receipt["content_length"]) != receipt["wire_bytes_downloaded"])
        or receipt["wire_bytes_downloaded"] != len(body)
    ):
        raise VotingDistrictsError("non200_or_incomplete_http_response")
    return bytes(body), receipt


def acquire(storage: StagingStorageResource, capture_dir: str | Path | None = None
            ) -> dict[str, tuple[bytes, dict[str, object]]]:
    """Capture the official directory roster and every pinned producer archive."""
    _safe_local_staging(storage)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        for name, url in urls().items():
            limit = capture_limits()[name]
            if capture_dir is not None:
                path = Path(capture_dir) / (name + ".body")
                receipt_path = Path(capture_dir) / (name + ".receipt.json")
                if (path.is_symlink() or receipt_path.is_symlink()
                        or not path.is_file() or not receipt_path.is_file()):
                    captures[name] = (b"", {
                        "url": url, "observed_at": datetime.now(timezone.utc).isoformat(),
                        "error": "capture_dir_file_missing", "complete_body": False,
                    })
                    raise VotingDistrictsError("capture_dir_file_missing:" + name)
                body = path.read_bytes()
                receipt = json.loads(receipt_path.read_text("utf-8"))
                if not isinstance(receipt, dict) or len(body) > limit:
                    raise VotingDistrictsError("capture_dir_identity_invalid:" + name)
                captures[name] = (body, receipt)
                continue
            failure: dict[str, object] = {}
            for attempt in range(CAPTURE_ATTEMPTS):
                try:
                    captures[name] = _stream_capture(url, limit)
                    break
                except (httpx.HTTPError, OSError, VotingDistrictsError) as exc:
                    failure = {
                        "url": url, "observed_at": datetime.now(timezone.utc).isoformat(),
                        "attempt": attempt + 1, "error": type(exc).__name__,
                        "error_message": str(exc), "complete_body": False,
                    }
            if name not in captures:
                captures[name] = (b"", failure)
                raise VotingDistrictsError(
                    "official_census_capture_failed:"
                    + name + ":"
                    + str(failure.get("error_message", failure.get("error", "unknown"))))
        return captures
    except (OSError, ValueError, TypeError) as exc:
        where = _quarantine(storage, captures, str(exc))
        raise VotingDistrictsError(f"{exc}; retained:{where}") from exc


def validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]) -> None:
    if set(captures) != set(urls()):
        raise VotingDistrictsError("missing_extra_or_incomplete_publication_roster")
    listing = captures[LISTING_NAME][0].decode("utf-8")
    listed = re.findall(r'href="(tl_2020_\d{2}_vtd20\.zip)"', listing)
    if sorted(listed) != sorted(name + ".zip" for name in PINS):
        raise VotingDistrictsError("official_directory_completeness_roster_drift")
    for name in PINS:
        body, receipt = captures[name]
        if len(body) != int(PINS[name]["bytes"]) or digest(body) != PINS[name]["sha256"]:
            raise VotingDistrictsError("changed_truncated_or_unknown_census_publication:" + name)
        if (
            receipt.get("url") != urls()[name] or receipt.get("final_url") != urls()[name]
            or type(receipt.get("status")) is not int or receipt["status"] != 200
            or type(receipt.get("bytes")) is not int or receipt["bytes"] != len(body)
            or receipt.get("sha256") != digest(body) or receipt.get("complete_body") is not True
        ):
            raise VotingDistrictsError(
                "original_receipt_url_status_hash_length_or_completeness_mismatch:" + name)


def decode_dbf_rows(body: bytes) -> list[dict[str, object]]:
    """Independent physical DBF decode checks every source attribute and precision."""
    if len(body) < 33:
        raise VotingDistrictsError("short_dbf_header")
    rows = int.from_bytes(body[4:8], "little")
    header = int.from_bytes(body[8:10], "little")
    record = int.from_bytes(body[10:12], "little")
    if (header < 33 or (header - 33) % 32 or body[header - 1] != 13
            or len(body) not in (header + rows * record, header + rows * record + 1)):
        raise VotingDistrictsError("incomplete_or_changed_dbf_records")
    fields = []
    for index in range(32, header - 1, 32):
        field = body[index:index + 32]
        fields.append((field[:11].split(b"\x00")[0].decode("ascii"), chr(field[11]),
                       field[16], field[17]))
    if (fields != DBF_FIELDS
            or record != 1 + sum(field[2] for field in fields)
            or any(body[header + i * record] != 32 for i in range(rows))):
        raise VotingDistrictsError("native_dbf_physical_declarations_or_deleted_rows_drift")
    decoded: list[dict[str, object]] = []
    for index in range(rows):
        cursor = header + index * record + 1
        row: dict[str, object] = {}
        for name, kind, width, _decimals in DBF_FIELDS:
            raw = body[cursor:cursor + width]
            cursor += width
            if kind == "C":
                value = raw.rstrip(b" \x00").decode("utf-8")
                row[name] = value if value else None
            else:
                text = raw.strip().decode("ascii")
                if not re.fullmatch(r"-?\d+", text):
                    raise VotingDistrictsError("native_dbf_numeric_null_or_nonintegral_drift")
                row[name] = int(text)
        decoded.append(row)
    return decoded


def read_archive_table(path: Path, name: str) -> tuple[dict[str, object], pa.Table]:
    """Validate one producer archive and return its audit facts plus its Arrow table."""
    with zipfile.ZipFile(path) as archive:
        infos = archive.infolist()
        names = [i.filename for i in infos]
        if (set(names) != {name + suffix for suffix in MEMBER_SUFFIXES}
                or len(names) != len(MEMBER_SUFFIXES) or archive.testzip() is not None):
            raise VotingDistrictsError("missing_duplicate_or_corrupt_shapefile_member:" + name)
        if (any(PurePosixPath(i.filename).name != i.filename or i.flag_bits & 1
                or stat.S_ISLNK(i.external_attr >> 16) for i in infos)
                or sum(i.file_size for i in infos) > 120_000_000):
            raise VotingDistrictsError("unsafe_archive_member_or_expansion:" + name)
        if archive.read(name + ".cpg") != b"UTF-8":
            raise VotingDistrictsError("encoding_drift:" + name)
        dbf_rows = decode_dbf_rows(archive.read(name + ".dbf"))
    uri = f"/vsizip/{{{path}}}/" + name + ".shp"
    meta, table = pyogrio.read_arrow(uri, return_fids=True)
    if (
        tuple(table.column_names) != SOURCE_FIELDS
        or tuple(field.type for field in table.schema) != SOURCE_TYPES
        or meta["crs"] != "EPSG:4269" or meta["fid_column"] != "OGC_FID"
        or meta["geometry_type"] != DRIVER_GEOMETRY_TYPE
        or table.num_rows != len(dbf_rows)
    ):
        raise VotingDistrictsError(
            "source_schema_types_crs_geometry_or_row_completeness_drift:" + name)
    if table["OGC_FID"].to_pylist() != list(range(table.num_rows)):
        raise VotingDistrictsError("file_local_fid_roster_drift:" + name)
    if [{k: row[k] for k, *_ in DBF_FIELDS} for row in table.to_pylist()] != dbf_rows:
        raise VotingDistrictsError(
            "driver_attribute_values_disagree_with_independent_physical_dbf_decode:" + name)
    if any(value != name[8:10] for value in table["STATEFP20"].to_pylist()):
        raise VotingDistrictsError("file_state_coverage_drift:" + name)
    audit = {
        "row_count": table.num_rows, "fid_column": meta["fid_column"],
        "driver_geometry_type": meta["geometry_type"], "driver_arrow_schema": str(table.schema),
        "dbf_field_declarations": [list(item) for item in DBF_FIELDS],
    }
    table = table.add_column(0, SOURCE_ARCHIVE_FIELD,
                             pa.array([name] * table.num_rows, pa.string()))
    return audit, table


def build_combined_table(paths: dict[str, Path]) -> tuple[pa.Table, dict[str, object]]:
    """Concatenate every state publication in pinned roster order with provenance."""
    audits: dict[str, object] = {}
    tables = []
    for name in PINS:
        audit, table = read_archive_table(paths[name], name)
        audits[name] = audit
        tables.append(table)
    combined = pa.concat_tables(tables)
    validate_table(combined)
    return combined, audits


def _counter(column: pa.ChunkedArray) -> dict[str, int]:
    return dict(sorted(collections.Counter(column.to_pylist()).items()))


def validate_table(table: pa.Table, chunk: int = 4096) -> None:
    if tuple(table.column_names) != FIELDS or tuple(f.type for f in table.schema) != TYPES:
        raise VotingDistrictsError("native_schema_physical_types_or_completeness_drift")
    if table.num_rows != COUNT:
        raise VotingDistrictsError("complete_producer_population_drift")
    for name in FIELDS:
        if table[name].null_count != NULL_COUNTS.get(name, 0):
            raise VotingDistrictsError("source_nullability_drift:" + name)
    if _counter(table.column("STATEFP20")) != STATE_COUNTS:
        raise VotingDistrictsError("complete_producer_state_publication_coverage_drift")
    for column, pinned in (("VTDI20", VTDI_COUNTS), ("LSAD20", LSAD_COUNTS),
                           ("FUNCSTAT20", FUNCSTAT_COUNTS), ("MTFCC20", MTFCC_COUNTS)):
        if _counter(table.column(column)) != pinned:
            raise VotingDistrictsError("source_attribute_value_roster_drift:" + column)
    ids = table.column("GEOID20").to_pylist()
    if len(set(ids)) != COUNT or digest(encode(sorted(ids))) != GEOIDS_SHA256:
        raise VotingDistrictsError("missing_duplicate_changed_source_GEOID20")
    expected_geoids = pc.binary_join_element_wise(
        table.column("STATEFP20"), table.column("COUNTYFP20"), table.column("VTDST20"), "")
    if (pc.any(pc.not_equal(pc.fill_null(table.column("GEOID20"), ""),
                            pc.fill_null(expected_geoids, ""))).as_py() is True
            or table.column("GEOID20").null_count != expected_geoids.null_count):
        raise VotingDistrictsError("literal_voting_district_identifier_relation_drift")
    if record_hash_stream([table], chunk=chunk) != SOURCE_RECORDS_SHA256:
        raise VotingDistrictsError("complete_original_attribute_values_and_native_WKB_drift")
    polygons = multipolygons = z_values = invalid = empty = coordinates = 0
    lower = [float("inf"), float("inf")]
    upper = [float("-inf"), float("-inf")]
    for start in range(0, table.num_rows, chunk):
        wkbs = table.column("wkb_geometry")[start:start + chunk].to_pylist()
        geoms = shapely.from_wkb(wkbs)
        invalid += int((~shapely.is_valid(geoms)).sum())
        empty += int(shapely.is_empty(geoms).sum())
        z_values += int(shapely.has_z(geoms).sum())
        type_counts = collections.Counter(map(int, shapely.get_type_id(geoms)))
        polygons += type_counts.get(3, 0)
        multipolygons += type_counts.get(6, 0)
        if set(type_counts) - {3, 6}:
            raise VotingDistrictsError("native_geometry_type_drift")
        coordinates += int(shapely.get_num_coordinates(geoms).sum())
        bounds = shapely.bounds(geoms)
        lower[0] = min(lower[0], float(bounds[:, 0].min()))
        lower[1] = min(lower[1], float(bounds[:, 1].min()))
        upper[0] = max(upper[0], float(bounds[:, 2].max()))
        upper[1] = max(upper[1], float(bounds[:, 3].max()))
    if invalid or empty or z_values:
        raise VotingDistrictsError("missing_invalid_empty_or_changed_native_geometry")
    if (polygons != GEOM_TYPE_COUNTS.get("Polygon", 0)
            or multipolygons != GEOM_TYPE_COUNTS.get("MultiPolygon", 0)):
        raise VotingDistrictsError("native_geometry_type_drift")
    if coordinates != COORDINATES:
        raise VotingDistrictsError("native_coordinate_population_drift")
    if [lower[0], lower[1], upper[0], upper[1]] != list(BOUNDS):
        raise VotingDistrictsError("native_coverage_extent_drift")


def record_hash_stream(tables: Iterable[pa.Table], chunk: int = 4096) -> str:
    value = hashlib.sha256()
    for table in tables:
        for start in range(0, table.num_rows, chunk):
            for row in table.slice(start, chunk).to_pylist():
                row["wkb_geometry"] = row["wkb_geometry"].hex()
                value.update(encode(row))
    return value.hexdigest()


def record_hash(table: pa.Table) -> str:
    return record_hash_stream([table])


def geo_metadata() -> dict[str, object]:
    return {
        "version": "1.1.0", "primary_column": "wkb_geometry",
        "columns": {"wkb_geometry": {
            "encoding": "WKB",
            "geometry_types": sorted(GEOM_TYPE_COUNTS),
            "crs": pyproj.CRS.from_epsg(4269).to_json_dict(),
            "bbox": list(BOUNDS),
        }},
    }


def expected_parquet_table(table: pa.Table) -> pa.Table:
    return table.replace_schema_metadata({b"geo": encode(geo_metadata())})


def processing_identity() -> dict[str, object]:
    """Bind the candidate to exact installed Dagster source files without Git."""
    root = Path(__file__).resolve().parents[3]
    files = {str(path.relative_to(root)): sha256_file(path)
             for path in sorted(root.joinpath("src/dagster_hifld").rglob("*.py"))}
    return {
        "code_identity_scheme": "sha256-packaged-dagster_hifld-py-v1",
        "working_tree_status": "unknown",
        "files": files,
        "python": platform.python_version(),
        "pyarrow": pa.__version__, "gdal": pyogrio.__gdal_version_string__,
        "geos": shapely.geos_version_string, "proj": pyproj.proj_version_str,
    }


def candidate_version() -> str:
    fingerprint = digest(encode(processing_identity()))
    return "v0.0.0-draft-census2020pl-" + fingerprint[:16]


def input_records(captures: dict[str, tuple[bytes, dict[str, object]]]) -> list[dict]:
    records = []
    for name, (body, receipt) in captures.items():
        url = urls()[name]
        if name == LISTING_NAME:
            purpose = "official directory roster page"
            input_id = "census2020pl-vtd-directory"
        else:
            purpose = "2020 Census P.L. 94-171 VTD state shapefile archive"
            input_id = name
        records.append({
            "input_id": input_id + ":" + digest(body),
            "producer": PUBLISHER, "host": urlsplit(url).hostname,
            "role": "producer_publication", "canonical_url": url,
            "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "sha256": digest(body), "bytes": len(body), "license": LICENSE,
            "attribution": f"U.S. Census Bureau TIGER/Line 2020PL {purpose}.",
            "release_date_unknown_reason": RELEASE_EVIDENCE,
        })
    validate_provenance({"schema_version": 1, "processor": "HIFLD Next",
                         "inputs": records, "code_commit": "candidate",
                         "transformations": [{"operation": "capture"}]})
    return records


def source_manifest(captures: dict[str, tuple[bytes, dict[str, object]]], parquet_path: Path
                    ) -> dict[str, object]:
    identity = processing_identity()
    fingerprint = digest(encode(identity))
    manifest = {
        "schema_version": 1, "processor": "HIFLD Next", "approved": False,
        "code_identity_scheme": identity["code_identity_scheme"],
        "code_commit": identity["code_identity_scheme"] + ":" + fingerprint,
        "title": "Voting Districts — complete Census 2020 P.L. 94-171 VTD publications, unapproved candidate",
        "publisher": PUBLISHER, "agency": "U.S. Census Bureau",
        "source_url": DIRECTORY, "license": LICENSE,
        "processing_fingerprint": fingerprint,
        "inputs": input_records(captures),
        "transformations": [{
            "operation": "decode_all_state_publications_and_stream_native_geoparquet",
            "parameters": {
                "source_identifiers": ("STATEFP20, COUNTYFP20, VTDST20, GEOID20, VTDI20 and "
                                       "file-local OGC_FID retained exactly; no published-ID substitution"),
                "raw_dbf": ("all original DBF names/types/widths/null conventions retained in raw "
                            "archives; blank C fields decode to source NULL"),
                "geometry": ("native EPSG:4269 Polygon/MultiPolygon WKB, no repair, wrapping, "
                             "simplification or reprojection"),
                "coverage": ("all participating state publications in the exact official "
                             "TIGER2020PL VTD directory; states without 2020 VTD submissions "
                             "(CA, HI, OR) are absent from the producer roster"),
            },
        }],
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": ("Exact original physical values and native WKB are retained; output "
                       "capability grants no source, baseline, rights, coverage, schema or "
                       "release acceptance."),
            "objects": [{"path": "geoparquet/" + OUTPUT_NAME,
                         "sha256": sha256_file(parquet_path),
                         "bytes": parquet_path.stat().st_size}],
        }}},
        "unsupported_output_formats": {
            "shapefile_zip": ("A source_archive provenance column and multi-state coverage cannot "
                              "be silently merged into single producer archives; all original "
                              "shapefiles remain raw."),
            "geopackage": ("No separately approved conversion policy; the original archives "
                           "remain authoritative."),
            "pmtiles": ("Reprojection and quantization would change native source coordinates; "
                        "no visualization mapping is approved."),
        },
    }
    validate_provenance(manifest)
    return manifest


def _metadata_values(table: pa.Table, audits: dict[str, object],
                     captures: dict[str, tuple[bytes, dict[str, object]]]) -> dict[str, bytes]:
    return {
        "metadata/processing_identity.json": encode(
            {"approved": False, **processing_identity()}),
        "metadata/source_capture.json": encode({
            "approved": False, "directory": DIRECTORY, "archive_count": len(PINS),
            "pins": {name: dict(PINS[name]) for name in PINS},
        }),
        "metadata/source_audit.json": encode({
            "approved": False, "feature_count": COUNT, "crs": "EPSG:4269",
            "geometry_types": dict(sorted(GEOM_TYPE_COUNTS.items())),
            "driver_geometry_type": DRIVER_GEOMETRY_TYPE,
            "native_coordinates": COORDINATES, "bounds": list(BOUNDS),
            "invalid_geometries": 0, "empty_geometries": 0, "z_geometries": 0,
            "state_counts": dict(sorted(STATE_COUNTS.items())),
            "null_counts": dict(sorted(NULL_COUNTS.items())),
            "vtdi_counts": dict(sorted(VTDI_COUNTS.items())),
            "lsad_counts": dict(sorted(LSAD_COUNTS.items())),
            "funcstat_counts": dict(sorted(FUNCSTAT_COUNTS.items())),
            "mtfcc_counts": dict(sorted(MTFCC_COUNTS.items())),
            "geoids_sha256": GEOIDS_SHA256,
            "source_records_sha256": SOURCE_RECORDS_SHA256,
            "per_archive_audit": audits,
            "release_evidence": RELEASE_EVIDENCE,
        }),
        "metadata/baseline_comparison.json": encode({
            "schema_version": 1, "approved": False, "status": "blocked",
            "reason_codes": ["source_baseline_ID_schema_type_geometry_coverage_mapping_not_approved"],
        }),
    }


def _build_candidate(building: Path, table: pa.Table, audits: dict[str, object],
                     captures: dict[str, tuple[bytes, dict[str, object]]]
                     ) -> tuple[dict[str, bytes], dict[str, object]]:
    """Write the complete candidate object set into an empty building directory."""
    parquet_dir = building / "geoparquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = parquet_dir / OUTPUT_NAME
    pq.write_table(expected_parquet_table(table), parquet_path,
                   compression="zstd", version="2.6")
    parquet_bytes = parquet_path.read_bytes()
    manifest = {
        **source_manifest(captures, parquet_path),
        "approved": False,
    }
    manifest_bytes = encode(manifest)
    values = _metadata_values(table, audits, captures)
    values["metadata/source_manifest.json"] = manifest_bytes
    metadata_dir = building / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    for relative, body in values.items():
        path = building / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    result = {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": "",
        "feature_count": COUNT, "source_population_complete": True,
        "source_archive_bytes": sum(int(PINS[name]["bytes"]) for name in PINS),
        "source_archive_count": len(PINS),
        "source_id_sha256": GEOIDS_SHA256,
        "source_schema_sha256": digest(encode(list(FIELDS))),
        "native_parquet_sha256": digest(parquet_bytes),
        "native_parquet_bytes": len(parquet_bytes),
        "native_value_parity": True,
        "statefp_counts": dict(sorted(STATE_COUNTS.items())),
        "approved": False,
    }
    values["geoparquet/" + OUTPUT_NAME] = parquet_bytes
    return values, result


def _reuse_existing_candidate(target: Path, fresh: dict[str, bytes],
                              fresh_result: dict[str, object]) -> dict[str, object]:
    """Revalidate an existing content-addressed candidate against a fresh rebuild.

    The candidate is content-addressed by processing identity, so a rebuild from
    the same captures must reproduce every stored source-owned byte. Retrieval
    timestamps are the only nondeterministic field in the manifest and are taken
    from the stored candidate before comparison. Catalog-owned metadata written
    after staging (quality/data-dictionary/resolved-provenance documents and
    metadata/source/ snapshots) is allowed as an extra without becoming
    source-owned.
    """
    files = {path.relative_to(target).as_posix() for path in target.rglob("*") if path.is_file()}
    catalog_owned = {
        "metadata/quality_manifest.json", "metadata/data_dictionary.json",
        "metadata/resolved_provenance.json",
    }
    extras = files - set(CANDIDATE_FILES)
    unexpected = {name for name in extras
                  if name not in catalog_owned and not name.startswith("metadata/source/")}
    if unexpected or not set(CANDIDATE_FILES) <= files:
        raise VotingDistrictsError("existing_candidate_roster_drift")
    if any((target / name).is_symlink() for name in files):
        raise VotingDistrictsError("existing_candidate_roster_drift")
    stored_manifest = json.loads((target / "metadata/source_manifest.json").read_text("utf-8"))
    stored_times = {record["input_id"]: record["retrieved_at"]
                    for record in stored_manifest.get("inputs", []) if "input_id" in record}
    fresh_manifest = json.loads(fresh["metadata/source_manifest.json"])
    for record in fresh_manifest.get("inputs", []):
        if "input_id" in record and record["input_id"] in stored_times:
            record["retrieved_at"] = stored_times[record["input_id"]]
    if encode(fresh_manifest) != (target / "metadata/source_manifest.json").read_bytes():
        raise VotingDistrictsError("existing_candidate_manifest_drift")
    for name in CANDIDATE_FILES:
        if name == "metadata/source_manifest.json":
            continue
        if (target / name).read_bytes() != fresh[name]:
            raise VotingDistrictsError("existing_candidate_bytes_drift:" + name)
    result = dict(fresh_result)
    result["reused"] = True
    result["version"] = target.name
    return result


def stage_voting_districts(storage: StagingStorageResource, *,
                           capture_dir: str | Path | None = None) -> dict[str, object]:
    """Capture every official publication, validate, and stage the candidate."""
    if not pins_complete():
        raise VotingDistrictsError("source_pins_or_population_audit_not_populated")
    root = _safe_local_staging(storage)
    if shutil.disk_usage(root).free < MIN_FREE_BYTES:
        raise VotingDistrictsError("insufficient_space_before_capture")
    captures = acquire(storage, capture_dir)
    try:
        validate_captures(captures)
        capture_root = root / f"_raw-captures/{DATASET_SLUG}"
        paths: dict[str, Path] = {}
        for name in PINS:
            body, receipt = captures[name]
            target = capture_root / f"{digest(body)}/{name}.body"
            if not target.exists():
                if shutil.disk_usage(root).free < 2 * len(body) + MIN_FREE_BYTES:
                    raise VotingDistrictsError("insufficient_space_before_raw_copy")
                target.parent.mkdir(parents=True, exist_ok=True)
                partial = target.with_name(name + ".body.partial")
                partial.write_bytes(body)
                if sha256_file(partial) != PINS[name]["sha256"]:
                    raise VotingDistrictsError("canonical_raw_copy_hash_mismatch:" + name)
                partial.replace(target)
                receipt_path = target.with_name(name + ".receipt.json")
                if not receipt_path.exists():
                    receipt_path.write_bytes(encode(receipt))
            paths[name] = target
        table, audits = build_combined_table(paths)
        identity = processing_identity()
        fingerprint = digest(encode(identity))
        version = "v0.0.0-draft-census2020pl-" + fingerprint[:16]
        prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
        target = root / prefix
        building = root / f".{DATASET_SLUG}-building-{fingerprint[:16]}"
        try:
            values, result = _build_candidate(building, table, audits, captures)
            if target.exists():
                try:
                    return _reuse_existing_candidate(target, values, result)
                finally:
                    shutil.rmtree(building, ignore_errors=True)
            target.parent.mkdir(parents=True, exist_ok=True)
            building.rename(target)
            result["version"] = version
            result["reused"] = False
            return result
        except BaseException:
            shutil.rmtree(building, ignore_errors=True)
            raise
    except (OSError, ValueError, TypeError) as exc:
        raise VotingDistrictsError(f"{exc}") from exc
