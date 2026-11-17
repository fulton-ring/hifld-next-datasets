"""Candidate-only capture of the current Census TIGER/Line Congressional Districts.

The Census Bureau publishes one ZIP per state or equivalent area under
``https://www2.census.gov/geo/tiger/TIGER<year>/CD/``, named
``tl_<year>_<geoid>_cd<nnn>.zip``. This adapter discovers the newest published
edition from those official directory listings, derives the Congress number from
the archive names, and captures every listed file. Completeness is checked
against the source's own listing and the capture receipts, not against digests
or row counts from an earlier capture.
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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4
import zipfile

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
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
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import StagingStorageResource, default_capture_dir
from dagster_hifld.sources.esri_common import sha as _sha

DATASET_SLUG = "congressional-districts"
FILE_SLUG = DATASET_SLUG
SOURCE_HOST = "www2.census.gov"
TIGER_ROOT_URL = f"https://{SOURCE_HOST}/geo/tiger/"
MIN_TIGER_VINTAGE = 2020
MAX_VINTAGE_PROBES = 3
# Floor that catches a truncated directory listing. It is not an exact roster
# pin: every archive the producer lists is still captured and validated.
MIN_SOURCE_GEOIDS = 50
MAX_DIRECTORY_BYTES = 4 * 1024 * 1024
MAX_ARCHIVE_BYTES = 8 * 1024 * 1024

_VINTAGE_LINK = re.compile(r'href="(TIGER(20\d{2})/)"')
_CD_ARCHIVE_NAME = re.compile(r"tl_(\d{4})_(\d{2})_cd(\d{3})\.zip")
_GEOID = re.compile(r"\d{2}")

SOURCE_SCHEMA = (
    pa.string(), pa.string(), pa.string(), pa.string(), pa.string(), pa.string(), pa.string(),
    pa.string(), pa.string(), pa.int64(), pa.int64(), pa.string(), pa.string(), pa.binary(),
)
EXPECTED_MEMBERS = (".cpg", ".dbf", ".prj", ".shp", ".shp.ea.iso.xml", ".shp.iso.xml", ".shx")


class CongressionalDistrictsQuarantineError(ValueError):
    """Capture, edition, schema, identifier, or population drift."""


class _NoCdEdition(LookupError):
    """The probed TIGER vintage has no Congressional District directory yet."""


def congress_ordinal(number: int) -> str:
    """Return the English ordinal used in the dataset title (120 -> ``120th``)."""
    if type(number) is not int or number < 1:
        raise CongressionalDistrictsQuarantineError("congress_number_invalid")
    if 10 <= number % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(number % 10, "th")
    return f"{number}{suffix}"


def source_fields(congress: int) -> tuple[str, ...]:
    """Return the native attribute order, whose district field tracks the Congress."""
    return (
        "STATEFP", f"CD{congress:03d}FP", "GEOID", "GEOIDFQ", "NAMELSAD", "LSAD", "CDSESSN",
        "MTFCC", "FUNCSTAT", "ALAND", "AWATER", "INTPTLAT", "INTPTLON", "wkb_geometry",
    )


@dataclass(frozen=True)
class CongressionalEdition:
    vintage: int
    congress: int
    geoids: tuple[str, ...]
    directory_url: str
    directory_last_modified: str | None = None
    listing_sha256: str | None = None
    listing_bytes: int | None = None

    @property
    def layer(self) -> str:
        return f"CD{self.congress:03d}"

    @property
    def ordinal(self) -> str:
        return congress_ordinal(self.congress)

    @property
    def fields(self) -> tuple[str, ...]:
        return source_fields(self.congress)

    def archive_name(self, geoid: str) -> str:
        return f"tl_{self.vintage}_{geoid}_cd{self.congress:03d}.zip"

    @property
    def archives(self) -> tuple[str, ...]:
        return tuple(self.archive_name(geoid) for geoid in self.geoids)

    def archive_url(self, name: str) -> str:
        return f"{self.directory_url}{name}"

    def as_metadata(self) -> dict:
        return {
            "tiger_vintage": self.vintage,
            "congress": self.congress,
            "congress_ordinal": self.ordinal,
            "cd_layer": self.layer,
            "source_directory": self.directory_url,
            "source_directory_last_modified": self.directory_last_modified,
        }


def _canonical(value) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _candidate_vintages(index_html: str) -> list[int]:
    """Return recent TIGER vintage years named by the official index, newest first."""
    vintages = sorted({int(year) for _, year in _VINTAGE_LINK.findall(index_html)}, reverse=True)
    return [vintage for vintage in vintages if vintage >= MIN_TIGER_VINTAGE][:MAX_VINTAGE_PROBES]


def parse_cd_directory(
    body: str,
    *,
    vintage: int,
    directory_url: str,
    last_modified: str | None = None,
    listing_sha256: str | None = None,
    listing_bytes: int | None = None,
) -> CongressionalEdition:
    """Derive one complete CD edition from an official directory listing."""
    by_name: dict[str, tuple[str, int]] = {}
    for year, geoid, congress in _CD_ARCHIVE_NAME.findall(body):
        if int(year) != vintage or not _GEOID.fullmatch(geoid):
            continue
        by_name[f"tl_{year}_{geoid}_cd{congress}.zip"] = (geoid, int(congress))
    if not by_name:
        raise _NoCdEdition("no_cd_archives_for_vintage")
    congresses = {congress for _, congress in by_name.values()}
    if len(congresses) != 1:
        raise CongressionalDistrictsQuarantineError("mixed_congress_editions_in_directory")
    congress = congresses.pop()
    geoids = tuple(sorted(geoid for geoid, _ in by_name.values()))
    if len(geoids) != len(set(geoids)):
        raise CongressionalDistrictsQuarantineError("duplicate_cd_archive_geoid_in_directory")
    if len(geoids) < MIN_SOURCE_GEOIDS:
        raise CongressionalDistrictsQuarantineError("cd_directory_roster_incomplete")
    return CongressionalEdition(
        vintage=vintage,
        congress=congress,
        geoids=geoids,
        directory_url=directory_url,
        directory_last_modified=last_modified,
        listing_sha256=listing_sha256,
        listing_bytes=listing_bytes,
    )


def _get_directory(client: httpx.Client, url: str) -> tuple[str, str | None] | None:
    """Read one bounded official directory listing, or None when it is absent."""
    with client.stream("GET", url) as response:
        if response.status_code == 404:
            return None
        response.raise_for_status()
        if response.status_code != 200:
            raise CongressionalDistrictsQuarantineError("directory_listing_status_changed")
        chunks = []
        total = 0
        for chunk in response.iter_bytes():
            total += len(chunk)
            if total > MAX_DIRECTORY_BYTES:
                raise CongressionalDistrictsQuarantineError("directory_listing_too_large")
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8"), response.headers.get("last-modified")


def discover_edition(client: httpx.Client) -> CongressionalEdition:
    """Find the newest published TIGER edition that carries a complete CD roster."""
    index = _get_directory(client, TIGER_ROOT_URL)
    if index is None:
        raise CongressionalDistrictsQuarantineError("tiger_index_unavailable")
    body, _ = index
    for vintage in _candidate_vintages(body):
        directory_url = f"{TIGER_ROOT_URL}TIGER{vintage}/CD/"
        listing = _get_directory(client, directory_url)
        if listing is None:
            continue
        listing_body, last_modified = listing
        raw = listing_body.encode("utf-8")
        try:
            return parse_cd_directory(
                listing_body,
                vintage=vintage,
                directory_url=directory_url,
                last_modified=last_modified,
                listing_sha256=_sha(raw),
                listing_bytes=len(raw),
            )
        except _NoCdEdition:
            continue
    raise CongressionalDistrictsQuarantineError("no_published_cd_edition")


def _write_immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        storage.write_key_if_unchanged(key, body, None)
    else:
        digest = storage.sha256_key(key)
        if storage.object_snapshot(key) != snapshot or snapshot.size != len(body) or digest != _sha(body):
            raise CongressionalDistrictsQuarantineError("existing_staged_object_changed:" + key)


def _processing_identity() -> tuple[str, dict, str]:
    repo = Path(__file__).parents[3]
    paths = [
        "src/dagster_hifld/assets/congressional_districts.py",
        "src/dagster_hifld/assets/ingest_registry.py",
        "src/dagster_hifld/assets/supported_assets.py",
        "src/dagster_hifld/sources/congressional_districts.py",
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


def _capture_root() -> Path:
    root = default_capture_dir("hifld-native-reconstruction", "387", "capture")
    if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)) or not root.is_dir():
        raise CongressionalDistrictsQuarantineError("capture_root_missing_or_aliased")
    return root


def _verify_receipts(root: Path) -> tuple[list[dict], dict[str, dict], CongressionalEdition]:
    """Validate capture receipts against their archives and derive the edition."""
    path = root / "receipts.json"
    if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1:
        raise CongressionalDistrictsQuarantineError("capture_receipt_missing_or_linked")
    try:
        rows = json.loads(path.read_bytes())
    except (ValueError, UnicodeError) as exc:
        raise CongressionalDistrictsQuarantineError("capture_receipt_invalid_json") from exc
    if type(rows) is not list or not rows:
        raise CongressionalDistrictsQuarantineError("capture_receipt_roster_empty")
    by_name: dict[str, dict] = {}
    vintage = congress = None
    geoids = set()
    for receipt in rows:
        if type(receipt) is not dict:
            raise CongressionalDistrictsQuarantineError("capture_receipt_shape_changed")
        url = receipt.get("url")
        if not isinstance(url, str):
            raise CongressionalDistrictsQuarantineError("capture_receipt_url_changed")
        name = url.rsplit("/", 1)[-1]
        match = _CD_ARCHIVE_NAME.fullmatch(name)
        if match is None or name in by_name:
            raise CongressionalDistrictsQuarantineError("capture_receipt_roster_changed")
        year, geoid, cd = int(match.group(1)), match.group(2), int(match.group(3))
        if url != f"{TIGER_ROOT_URL}TIGER{year}/CD/{name}" or receipt.get("final_url") != url:
            raise CongressionalDistrictsQuarantineError("capture_receipt_url_changed")
        if (receipt.get("status") != 200 or receipt.get("complete_body") is not True
            or type(receipt.get("bytes")) is not int or receipt["bytes"] <= 0
            or receipt["bytes"] > MAX_ARCHIVE_BYTES
            or not re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("sha256", "")))
            or receipt.get("content_encoding") not in (None, "identity")):
            raise CongressionalDistrictsQuarantineError("capture_receipt_claim_changed")
        if vintage is None:
            vintage, congress = year, cd
        elif (year, cd) != (vintage, congress):
            raise CongressionalDistrictsQuarantineError("mixed_congress_editions_in_receipts")
        by_name[name] = receipt
        geoids.add(geoid)
    if len(geoids) < MIN_SOURCE_GEOIDS:
        raise CongressionalDistrictsQuarantineError("capture_receipt_roster_incomplete")
    actual = {p.name for p in root.iterdir() if p.name.endswith(".zip")}
    if actual != set(by_name):
        raise CongressionalDistrictsQuarantineError("capture_archive_roster_changed")
    edition = CongressionalEdition(
        vintage=vintage,
        congress=congress,
        geoids=tuple(sorted(geoids)),
        directory_url=f"{TIGER_ROOT_URL}TIGER{vintage}/CD/",
    )
    return rows, by_name, edition


def _read_directory_receipt(root: Path, edition: CongressionalEdition) -> dict | None:
    path = root / "directory.json"
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1:
        raise CongressionalDistrictsQuarantineError("directory_receipt_missing_or_linked")
    try:
        receipt = json.loads(path.read_bytes())
    except (ValueError, UnicodeError) as exc:
        raise CongressionalDistrictsQuarantineError("directory_receipt_invalid_json") from exc
    if (type(receipt) is not dict or receipt.get("vintage") != edition.vintage
        or receipt.get("congress") != edition.congress
        or receipt.get("directory_url") != edition.directory_url
        or not isinstance(receipt.get("archives"), list)
        or set(receipt["archives"]) != set(edition.archives)
        or not re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("listing_sha256", "")))):
        raise CongressionalDistrictsQuarantineError("directory_receipt_changed")
    return receipt


def _mode() -> str:
    value = os.environ.get("HIFLD_CD_MODE")
    if value not in {"live", "replay"}:
        raise CongressionalDistrictsQuarantineError("HIFLD_CD_MODE_must_explicitly_be_live_or_replay")
    return value


def _acquire_live(acquisitions_dir: str | None = None) -> Path:
    """Capture every archive the current official CD directory lists."""
    parent = Path(acquisitions_dir).absolute() if acquisitions_dir \
        else default_capture_dir("hifld-native-reconstruction", "387", "acquisitions")
    if ".." in parent.parts or any(p.is_symlink() for p in (parent, *parent.parents)):
        raise CongressionalDistrictsQuarantineError("acquisition_root_missing_or_aliased")
    parent.mkdir(parents=True, exist_ok=True)
    acquisition = parent / ("cd-" + uuid4().hex)
    acquisition.mkdir(mode=0o700, exist_ok=False)
    receipts = []
    with httpx.Client(follow_redirects=True, timeout=180.0,
                      headers={"Accept-Encoding": "identity"}) as client:
        edition = discover_edition(client)
        directory_receipt = {
            "index_url": TIGER_ROOT_URL,
            "directory_url": edition.directory_url,
            "directory_last_modified": edition.directory_last_modified,
            "listing_sha256": edition.listing_sha256,
            "listing_bytes": edition.listing_bytes,
            "vintage": edition.vintage,
            "congress": edition.congress,
            "archives": list(edition.archives),
        }
        (acquisition / "directory.json").write_bytes(_canonical(directory_receipt))
        for name in edition.archives:
            url = edition.archive_url(name)
            geoid = _CD_ARCHIVE_NAME.fullmatch(name).group(2)
            wire_path = acquisition / name
            decoded_path = acquisition / (name + ".decoded")
            capture_receipt = {"url": url, "final_url": None,
                               "retrieved_at": datetime.now(timezone.utc).isoformat(),
                               "complete_body": False, "status": None, "bytes": 0,
                               "sha256": None, "content_encoding": None, "capture_error": None}
            try:
                with wire_path.open("xb") as wire, decoded_path.open("xb") as decoded:
                    with client.stream("GET", url) as response:
                        capture_receipt["final_url"] = str(response.url)
                        outcome = capture_http_response(
                            response, max_wire_bytes=MAX_ARCHIVE_BYTES,
                            max_decoded_bytes=MAX_ARCHIVE_BYTES,
                            reserve=lambda _: (_ for _ in ()).throw(OSError("cd_capture_space_floor"))
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
                            raise CongressionalDistrictsQuarantineError(
                                "official_source_capture_incomplete_or_changed:" + geoid)
                        wire.flush(); decoded.flush()
                capture_receipt.update({"bytes": wire_path.stat().st_size,
                                        "sha256": _sha(wire_path.read_bytes())})
                decoded_path.unlink()
            except Exception as exc:
                capture_receipt["capture_error"] = capture_receipt.get("capture_error") or type(exc).__name__
                capture_receipt["bytes"] = wire_path.stat().st_size if wire_path.exists() else 0
                capture_receipt["sha256"] = _sha(wire_path.read_bytes()) if wire_path.exists() else None
                receipts.append(capture_receipt)
                (acquisition / "receipts.json").write_bytes(_canonical(receipts))
                (acquisition / (name + ".receipt.json")).write_bytes(_canonical(capture_receipt))
                raise CongressionalDistrictsQuarantineError(
                    "live_acquisition_preserved_but_quarantined:" + geoid) from exc
            receipts.append(capture_receipt)
            (acquisition / (name + ".receipt.json")).write_bytes(_canonical(capture_receipt))
    (acquisition / "receipts.json").write_bytes(_canonical(receipts))
    return acquisition


def _read_archive(root: Path, name: str, receipt: dict, edition: CongressionalEdition) -> pa.Table:
    path = root / name
    if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1:
        raise CongressionalDistrictsQuarantineError("capture_archive_missing_or_linked:" + name)
    if path.stat().st_size != receipt["bytes"] or _sha(path.read_bytes()) != receipt["sha256"]:
        raise CongressionalDistrictsQuarantineError("capture_archive_receipt_mismatch:" + name)
    with zipfile.ZipFile(path) as archive:
        members = archive.infolist()
        names = [item.filename for item in members]
        stem = name[:-4]
        expected_names = {stem + suffix for suffix in EXPECTED_MEMBERS}
        if (len(names) != len(set(names)) or set(names) != expected_names
            or any(PurePosixPath(n).is_absolute() or ".." in PurePosixPath(n).parts
                   or len(PurePosixPath(n).parts) != 1 for n in names)
            or any(item.flag_bits & 1 or stat.S_ISLNK(item.external_attr >> 16) for item in members)):
            raise CongressionalDistrictsQuarantineError("archive_member_roster_or_path_changed:" + name)
        if sum(len(archive.read(item)) for item in members) != sum(item.file_size for item in members):
            raise CongressionalDistrictsQuarantineError("archive_member_crc_or_length_changed:" + name)
    geoid = _CD_ARCHIVE_NAME.fullmatch(name).group(2)
    fields = edition.fields
    meta, table = pyogrio.read_arrow("/vsizip/" + str(path))
    if (tuple(table.schema.names) != fields or tuple(f.type for f in table.schema) != SOURCE_SCHEMA
        or meta.get("crs") != "EPSG:4269" or meta.get("geometry_type") != "Polygon"
        or table.num_rows < 1):
        raise CongressionalDistrictsQuarantineError("source_schema_crs_geometry_or_count_changed:" + name)
    fq_prefixes = set()
    for row in table.select(["STATEFP", fields[1], "GEOID", "GEOIDFQ", "CDSESSN"]).to_pylist():
        if (row["STATEFP"] != geoid or row["GEOID"] != row["STATEFP"] + row[fields[1]]
            or row["CDSESSN"] != str(edition.congress)
            or not row["GEOIDFQ"].endswith("US" + row["GEOID"])):
            raise CongressionalDistrictsQuarantineError("source_literal_identifier_or_congress_changed:" + name)
        fq_prefixes.add(row["GEOIDFQ"][: -len("US" + row["GEOID"])])
    if len(fq_prefixes) != 1:
        raise CongressionalDistrictsQuarantineError("source_GEOIDFQ_prefix_drift:" + name)
    if any(table[field].null_count for field in fields):
        raise CongressionalDistrictsQuarantineError("source_null_attribute:" + name)
    geometries = shapely.from_wkb(table["wkb_geometry"].to_pylist())
    if table["wkb_geometry"].null_count or bool(shapely.is_empty(geometries).any()) \
            or bool((~shapely.is_valid(geometries)).any()):
        raise CongressionalDistrictsQuarantineError("source_geometry_null_empty_or_invalid:" + name)
    return table


def _geo_metadata(geometry_types: list[str]) -> dict[bytes, bytes]:
    crs = pyproj.CRS.from_epsg(4269).to_json_dict()
    body = {"version": "1.1.0", "primary_column": "wkb_geometry", "columns": {
        "wkb_geometry": {"encoding": "WKB", "geometry_types": geometry_types, "crs": crs}}}
    return {b"geo": _canonical(body)}


def stage_congressional_districts_candidate(
    storage: StagingStorageResource,
    dataset: str,
    file_slug: str,
    acquisitions_dir: str | None = None,
) -> dict:
    """Stage the complete current CD population and exact typed/WKB GeoParquet."""
    mode = _mode()
    root = _acquire_live(acquisitions_dir) if mode == "live" else _capture_root()
    receipts, by_name, edition = _verify_receipts(root)
    directory_receipt = _read_directory_receipt(root, edition)
    source_tables = []
    source_ids = set()
    source_wkb_hash = hashlib.sha256()
    for geoid in edition.geoids:
        name = edition.archive_name(geoid)
        table = _read_archive(root, name, by_name[name], edition)
        for record in table.to_pylist():
            identifier = record["GEOID"]
            if identifier in source_ids:
                raise CongressionalDistrictsQuarantineError("duplicate_source_GEOID:" + identifier)
            source_ids.add(identifier)
            source_wkb_hash.update(identifier.encode() + b"\0" + record["wkb_geometry"])
        source_tables.append(table)
    table = pa.concat_tables(source_tables, promote_options="none")
    if table.num_rows != len(source_ids) or len(source_ids) < MIN_SOURCE_GEOIDS:
        raise CongressionalDistrictsQuarantineError("national_source_population_or_ID_count_changed")
    code_commit, processing_identity, processing_fingerprint = _processing_identity()
    geometry_types = sorted({g.geom_type for g in shapely.from_wkb(table["wkb_geometry"].to_pylist())})
    if not geometry_types or not set(geometry_types).issubset({"Polygon", "MultiPolygon"}):
        raise CongressionalDistrictsQuarantineError("source_geometry_type_drift")
    source_file_sha256 = {name: by_name[name]["sha256"] for name in edition.archives}
    candidate_identity = _sha(_canonical({
        "edition": edition.as_metadata(),
        "source_file_sha256": source_file_sha256,
        "source_rows": table.num_rows, "unique_geoids": len(source_ids),
        "source_wkb_identity_sha256": source_wkb_hash.hexdigest(),
        "source_schema": [{"name": f.name, "type": str(f.type)} for f in table.schema],
        "crs": "EPSG:4269", "geometry_types": geometry_types,
        "processing_fingerprint": processing_fingerprint,
    }))
    version = f"v0.0.0-{edition.layer.lower()}-{candidate_identity[:16]}-{processing_fingerprint[:16]}"
    # Keep source attributes and WKB unchanged. The output column metadata is
    # added for GeoParquet interoperability; no feature values are transformed.
    table = table.replace_schema_metadata({**(table.schema.metadata or {}), **_geo_metadata(geometry_types)})
    with tempfile.TemporaryDirectory(prefix="cd-native-") as tmp:
        parquet_path = Path(tmp) / f"{file_slug}.parquet"
        pq.write_table(table, parquet_path, compression="zstd", use_dictionary=False, write_statistics=True)
        decoded = pq.read_table(parquet_path)
        if (decoded.num_rows != table.num_rows or decoded.schema.names != table.schema.names
            or tuple(field.type for field in decoded.schema) != tuple(field.type for field in table.schema)
            or not decoded.equals(table)):
            raise CongressionalDistrictsQuarantineError("native_parquet_full_value_or_type_roundtrip_failed")
        parquet = parquet_path.read_bytes()
    target = storage.build_target_location(dataset, file_slug, version, "").rstrip("/")
    output_key = f"{target}/geoparquet/{file_slug}.parquet"
    _write_immutable(storage, output_key, parquet)
    output_sha = _sha(parquet)
    input_receipts = []
    for name in sorted(edition.archives):
        receipt = by_name[name]
        input_receipts.append({"input_id": f"census-tiger{edition.vintage}-{edition.layer.lower()}:" + receipt["sha256"],
            "producer": "U.S. Census Bureau", "host": SOURCE_HOST, "role": "producer_publication",
            "canonical_url": receipt["url"], "retrieval_url": receipt["url"],
            "observed_final_url": receipt["final_url"], "retrieved_at": receipt["retrieved_at"],
            "http_status": 200, "sha256": receipt["sha256"], "bytes": receipt["bytes"],
            "license": "https://www.census.gov/about/policies/quality/guidelines/copyright.html",
            "attribution": f"U.S. Census Bureau, TIGER/Line {edition.vintage} Congressional Districts ({edition.layer}).",
            "release_date_unknown_reason": f"TIGER/Line {edition.vintage} edition is identified by the official archive names; a producer publication date was not established."})
    lineage = {"archive_receipts_sha256": _sha((root / "receipts.json").read_bytes()),
        "source_capture_directory": str(root), "source_wkb_identity_sha256": source_wkb_hash.hexdigest(),
        "source_ID_count": len(source_ids), "edition": edition.as_metadata()}
    if directory_receipt is not None:
        lineage["source_directory_receipt"] = directory_receipt
    manifest = {"schema_version": 1, "processor": "HIFLD Next", "approved": False,
        "title": f"{edition.ordinal} Congressional Districts — TIGER/Line {edition.vintage} working draft",
        "description": f"Complete Census Bureau TIGER/Line {edition.vintage} {edition.layer} candidate for the {edition.ordinal} Congress, with original Census attributes and WKB geometry.",
        "publisher": "U.S. Census Bureau", "agency": "U.S. Census Bureau",
        "edition": edition.as_metadata(),
        "code_commit": code_commit, "processing_fingerprint": processing_fingerprint,
        "source_url": edition.directory_url, "inputs": input_receipts,
        "transformations": [{"operation": "read_complete_state_equivalent_sources_and_write_GeoParquet",
            "parameters": {"source_files": len(edition.geoids), "source_rows": table.num_rows,
                "source_GEOID_count": len(source_ids),
                "attributes_preserved": list(edition.fields[:-1]),
                "geometry": "native EPSG:4269 Polygon WKB, no repair or reprojection"}}],
        "unsupported_output_formats": {"shapefile_zip": "No accepted reconstructed national shapefile package contract.",
            "geopackage": "No accepted reconstructed GeoPackage contract.", "pmtiles": "No approved reprojection or quantization contract."},
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Source-native attributes and WKB; candidate only, with release approval unset.",
            "objects": [{"path": f"geoparquet/{file_slug}.parquet", "sha256": output_sha, "bytes": len(parquet)}]}}},
        "lineage": lineage}
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
        "candidate_identity": candidate_identity, "edition": edition.as_metadata(),
    }
    bundle = canonical_construction_bundle(version, processing_fingerprint, payload)
    try:
        original = load_first_construction(storage, construction_key, version, processing_fingerprint)
        first_run = False
    except ConstructionStateError as exc:
        if exc.code != "construction_missing_original_state":
            raise CongressionalDistrictsQuarantineError("construction_state_unavailable:" + exc.code) from exc
        original = seal_first_construction(storage, construction_key, version, processing_fingerprint, bundle)
        first_run = True
    try:
        original_payload = json.loads(original)["payload"]
    except (ValueError, KeyError, TypeError) as exc:
        raise CongressionalDistrictsQuarantineError("construction_original_payload_invalid") from exc
    if (original_payload.get("candidate_identity") != candidate_identity
        or original_payload.get("source_wkb_identity_sha256") != source_wkb_hash.hexdigest()
        or original_payload.get("source_ID_count") != len(source_ids)
        or original_payload.get("native_output") != payload["native_output"]
        or original_payload.get("processing_identity") != processing_identity
        or original_payload.get("edition") != edition.as_metadata()):
        raise CongressionalDistrictsQuarantineError("construction_original_candidate_or_code_drift")
    if first_run:
        _write_immutable(storage, f"{target}/metadata/source_capture_receipts.json", receipt_body)
        _write_immutable(storage, manifest_key, _canonical(manifest))
        if _sha(receipt_body) != original_payload.get("receipt_bytes_sha256"):
            raise CongressionalDistrictsQuarantineError("construction_first_receipt_changed_during_seal")
    else:
        original_receipt = storage.read_key(f"{target}/metadata/source_capture_receipts.json")
        if _sha(original_receipt) != original_payload.get("receipt_bytes_sha256"):
            raise CongressionalDistrictsQuarantineError("construction_original_receipt_missing_or_changed")
        manifest = original_payload["manifest"]
        if _canonical(manifest) != storage.read_key(manifest_key):
            raise CongressionalDistrictsQuarantineError("construction_original_manifest_missing_or_changed")
        _write_immutable(storage, manifest_key, _canonical(manifest))
        observation_key = f"{target}/metadata/acquisition_observations/{uuid4().hex}/receipts.json"
        _write_immutable(storage, observation_key, receipt_body)
    return {"dataset_slug": dataset, "file_slug": file_slug, "version": version,
        "tiger_vintage": edition.vintage, "congress": edition.congress,
        "congress_ordinal": edition.ordinal, "cd_layer": edition.layer,
        "source_archive_count": len(receipts), "source_feature_count": table.num_rows,
        "source_unique_geoid_count": len(source_ids), "source_wkb_identity_sha256": source_wkb_hash.hexdigest(),
        "capture_receipts_sha256": _sha(receipt_body), "prebuilt_geoparquet_sha256": output_sha,
        "prebuilt_geoparquet_bytes": len(parquet), "source_manifest_key": manifest_key,
        "construction_key": construction_key, "code_commit": code_commit,
        "processing_fingerprint": processing_fingerprint, "candidate_identity": candidate_identity,
        "acquisition_mode": mode,
        "source_accepted": False, "release_approved": False}
