"""USGS NLDI ``ref_gage`` feature listing — the agency's maintained distribution.

The HIFLD inventory entry ``nhd/usgs-hydro-nldi-streamgage-monitoring-locations``
(agency US Geological Survey) names the NLDI streamgage monitoring locations.
The pipeline input is the U.S. Geological Survey's own NLDI publication:
``https://api.water.usgs.gov/nldi/linked-data/ref_gage`` on the official
``api.water.usgs.gov`` API host, whose registered feature sources and paginated
complete feature listing are documented by USGS at
``https://api.water.usgs.gov/docs/nldi/feature-sources/`` ("Requesting a
feature source with no filters returns all features in that collection...
supports ``limit`` and ``offset`` query parameters for pagination"). The bulk
endpoint without pagination was observed failing with a gateway timeout, so the
capture walks the documented pagination: pages of 250 features from offset 0
until a page returns fewer features than the limit, each page retained as raw
evidence in a resumable capture directory before any candidate is sealed. The
250-feature page size is chosen because larger pages were observed exceeding
the NLDI gateway's server-side budget under load (1000-feature pages answered
with gateway-504 bodies or hung until read timeout, while smaller documented
limit/offset pages completed), and any ``limit`` is a documented pagination
value.

The collection is captured as served. Where the ``ref_gage`` listing itself
repeats a feature — the same feature served twice consecutively inside one
page (2026-11-14 capture: 9 such source-served duplicate rows) — the walk
retains those duplicate rows verbatim, audits and pins their count and
identifier roster, and audits the unique identifier count separately from the
row count. Every other non-ascending identifier order, a repeated identifier
with different content, and any duplicate across a page boundary is refused as
pagination drift.

The upstream ``internetofwater/ref_gages`` GitHub compilation is the community
project NLDI indexes; it is named as lineage context only and is never fetched.
The DataLumos 240397 archive is historical provenance and is not the refresh
source. The staged candidate recomposes the walked pages into one GeoJSON
FeatureCollection by slicing each page's raw ``features`` array text, so every
serialized number lexeme and member order is retained verbatim; the composed
bytes are re-parsed and every feature re-validated before a candidate is
sealed. Publication stays blocked pending USGS authority, freshness, schema/ID
contract, compatibility and release decisions; all runtime approval flags are
false and the draft refresh schedule is stopped.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import platform
import re
import subprocess
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx
from dagster import (AssetCheckResult, AssetKey, AssetSelection, DefaultScheduleStatus, Field,
                     ScheduleDefinition, asset, asset_check, define_asset_job)

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.http_capture import CaptureProgress, capture_http_response
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, SEMVER_VERSION_RE, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "nhd"
FILE_SLUG = "usgs-hydro-nldi-streamgage-monitoring-locations"
NLDI_ROOT = "https://api.water.usgs.gov/nldi"
SOURCES_URL = NLDI_ROOT + "/linked-data"
COLLECTION_PATH = "/linked-data/ref_gage"
COLLECTION_URL = NLDI_ROOT + COLLECTION_PATH
FEATURE_SOURCES_DOC_URL = "https://api.water.usgs.gov/docs/nldi/feature-sources/"
EXPECTED_SOURCE_ENTRY = {"source": "ref_gage", "sourceName": "geoconnex.us reference gages",
                         "features": COLLECTION_URL}
PAGE_LIMIT = 250
PAGE_CAPTURE_BYTES = 8 * 1024 * 1024
SOURCES_CAPTURE_BYTES = 1024 * 1024
PROPERTY_NAMES = ("identifier", "navigation", "name", "source", "sourceName", "comid",
                  "type", "uri", "reachcode", "measure", "mainstem")
GAGE_URI_PREFIX = "https://geoconnex.us/ref/gages/"
MAINSTEM_URI_PREFIX = "https://geoconnex.us/ref/mainstems/"
ASSET_PATH = "geojson/usgs_nldi_gages.geojson"
DEFAULT_CAPTURE_ROOT = ("/data/workspace/scratch/nhd/"
                        "usgs-hydro-nldi-streamgage-monitoring-locations/captures/default")
# Walk-derived content pins from the dated complete capture; unpinned values
# ("" / None) are checked only once pinned. See BASELINE_NOTE.
EXPECTED_COUNT = None
EXPECTED_UNIQUE_ID_COUNT = None
EXPECTED_IDS_SHA256 = ""
EXPECTED_SCHEMA_SHA256 = ""
EXPECTED_ASSET_SHA256 = ""
EXPECTED_FIRST_ID = None
EXPECTED_LAST_ID = None
EXPECTED_NULL_GEOMETRY_COUNT = None
EXPECTED_OBSERVED_BOUNDS = None
EXPECTED_DUPLICATE_FEATURE_COUNT = None
EXPECTED_DUPLICATE_IDS = None
BASELINE_NOTE = (
    "The complete-population count, unique-identifier roster digest, property "
    "schema digest, composed-asset digest and source-served duplicate-row "
    "roster are pinned from the dated complete resumable pagination walk; "
    "unpinned values are checked only once pinned, and drift against a pinned "
    "value is refused and quarantined.")
PAGE_NAME_PATTERN = re.compile(r"page_([0-9]{9})$")
SOURCE_NAMES = ("sources-before", "sources-after")
REPO_ROOT = Path(__file__).resolve().parents[3]


def _reserve(_progress: CaptureProgress) -> None:
    """The caller bounds each response before capturing it."""


def _immutable(staging: StagingStorageResource, key: str, body: bytes) -> None:
    digest = hashlib.sha256(body).hexdigest()
    old = staging.object_snapshot(key)
    if old is not None:
        if old.size != len(body) or staging.sha256_key(key) != digest:
            raise ValueError("pinned capture key already has different bytes")
        return
    staging.write_key_if_unchanged(key, body, None)
    current = staging.object_snapshot(key)
    if current is None or current.size != len(body) or staging.sha256_key(key) != digest:
        raise ValueError("capture readback did not match response bytes")


def _redacted_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _fetch(client: httpx.Client, url: str, *, accept: str, limit: int) -> tuple[bytes, bytes, dict]:
    wire, decoded = bytearray(), bytearray()
    with client.stream("GET", url, headers={"Accept": accept}) as response:
        result = capture_http_response(
            response, max_wire_bytes=limit, max_decoded_bytes=limit,
            reserve=_reserve, retain_wire=wire.extend, retain_decoded=decoded.extend,
        )
        receipt = {
            "request": {"method": "GET", "url": url, "headers": {"Accept": accept}, "body": None},
            "final_url": _redacted_url(str(response.url)), "status_code": response.status_code,
            "content_type": response.headers.get("content-type"),
            "content_encoding": result.content_encoding,
            "content_length_header": result.content_length_header,
            "last_modified": response.headers.get("last-modified"), "etag": response.headers.get("etag"),
            "http_date": response.headers.get("date"), "complete": result.complete,
            "wire_bytes": result.wire_bytes_retained, "decoded_bytes": result.decoded_bytes_retained,
            "wire_sha256": result.wire_sha256, "decoded_sha256": result.decoded_sha256,
            "error_code": result.error_code,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    return bytes(wire), bytes(decoded), receipt


def _load_json(body: bytes, label: str) -> dict:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} was not valid JSON") from exc
    if type(value) is not dict:
        raise ValueError(f"{label} was not a JSON object")
    return value


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _sources_list(document: object) -> list:
    """USGS documents the feature-source listing as a JSON array of objects."""
    if isinstance(document, dict) and type(document.get("sources")) is list:
        return document["sources"]
    if type(document) is list:
        return document
    raise ValueError("NLDI feature-source listing is not a sources array")


def _sources_entry(document: object) -> dict:
    sources = _sources_list(document)
    entries = [entry for entry in sources
               if type(entry) is dict and entry.get("source") == EXPECTED_SOURCE_ENTRY["source"]]
    if len(entries) != 1:
        raise ValueError("NLDI ref_gage feature source is missing or ambiguous")
    entry = {key: entries[0].get(key) for key in ("source", "sourceName", "features")}
    if entry != EXPECTED_SOURCE_ENTRY:
        raise ValueError("NLDI ref_gage feature-source registration drifted")
    return entry


def _feature_identifier(feature: object) -> str:
    if (type(feature) is not dict or feature.get("type") != "Feature"
            or not {"type", "properties", "geometry"} <= set(feature)
            or set(feature) - {"type", "properties", "geometry", "id"}):
        raise ValueError("NLDI feature is not a plain GeoJSON Feature")
    identifier = feature.get("id")
    if type(identifier) is not str or not identifier:
        raise ValueError("NLDI feature id is not a non-empty string")
    return identifier


def _validate_feature(feature: dict, identifier: str) -> None:
    props = feature.get("properties")
    if type(props) is not dict or tuple(props) != PROPERTY_NAMES:
        raise ValueError("NLDI feature property roster or order drifted")
    if feature.get("id") != identifier or props["identifier"] != identifier:
        raise ValueError("NLDI feature id/identifier do not match the walk position")
    if props["source"] != EXPECTED_SOURCE_ENTRY["source"] or props["sourceName"] != EXPECTED_SOURCE_ENTRY["sourceName"]:
        raise ValueError("NLDI feature source fields drifted")
    navigation = props["navigation"]
    if (not isinstance(navigation, str)
            or not navigation.startswith(NLDI_ROOT + COLLECTION_PATH + "/" + identifier + "/")):
        raise ValueError("NLDI navigation link drifted")
    uri = props["uri"]
    if not isinstance(uri, str) or uri != GAGE_URI_PREFIX + identifier:
        raise ValueError("NLDI feature uri drifted")
    if props["type"] != "hydrolocation":
        raise ValueError("NLDI feature type drifted")
    name = props["name"]
    if name is not None and not isinstance(name, str):
        raise ValueError("NLDI feature name is neither a string nor null")
    comid = props["comid"]
    if comid is not None and type(comid) is not int:
        raise ValueError("NLDI comid is neither an integer nor null")
    reachcode = props["reachcode"]
    if reachcode is not None and not isinstance(reachcode, str):
        raise ValueError("NLDI reachcode is neither a string nor null")
    measure = props["measure"]
    if measure is not None and (type(measure) not in (int, float)
                                or not (measure == measure and -1e308 < measure < 1e308)):
        raise ValueError("NLDI measure is not a finite number or null")
    mainstem = props["mainstem"]
    if mainstem is not None and (not isinstance(mainstem, str)
                                 or not mainstem.startswith(MAINSTEM_URI_PREFIX)):
        raise ValueError("NLDI mainstem uri drifted")
    geometry = feature.get("geometry")
    if type(geometry) is not dict or geometry.get("type") != "Point" \
            or set(geometry) != {"type", "coordinates"}:
        raise ValueError("NLDI feature geometry is not a plain Point")
    coordinates = geometry.get("coordinates")
    if (type(coordinates) is not list or len(coordinates) != 2
            or any(type(value) not in (int, float) for value in coordinates)
            or any(not (value == value and -1e308 < value < 1e308) for value in coordinates)):
        raise ValueError("NLDI feature coordinates are not finite numbers")
    longitude, latitude = coordinates
    if not (-180.0 <= longitude <= 180.0 and -90.0 <= latitude <= 90.0):
        raise ValueError("NLDI feature coordinates fall outside the geographic degree ranges")


def _page_collection(document: object) -> list:
    if (type(document) is not dict or document.get("type") != "FeatureCollection"
            or set(document) != {"type", "features"}
            or type(document.get("features")) is not list):
        raise ValueError("NLDI page is not a plain GeoJSON FeatureCollection")
    return document["features"]


def _page_slice(decoded: bytes) -> bytes:
    """Slice one page's raw ``features`` array interior, proving byte fidelity."""
    marker = b'"features":['
    index = decoded.find(marker)
    if index < 0:
        raise ValueError("NLDI page does not carry a plain features array envelope")
    interior = decoded[index + len(marker):decoded.rindex(b"]")]
    if decoded[:index + len(marker)] + interior + b"]}" != decoded:
        raise ValueError("NLDI page features slice is not byte-exact")
    return interior


def _page_body(offset: int) -> str:
    return f"{COLLECTION_URL}?limit={PAGE_LIMIT}&offset={offset}&f=json"


def _evidence_path(capture_root: Path, name: str, suffix: str) -> Path:
    if suffix == "receipt":
        return capture_root / f"{name}.receipt.json"
    return capture_root / f"{name}.{suffix}"


def _retain(capture_root: Path, name: str, wire: bytes, decoded: bytes, receipt: dict) -> Path:
    _evidence_path(capture_root, name, "wire").write_bytes(wire)
    decoded_path = _evidence_path(capture_root, name, "decoded")
    decoded_path.write_bytes(decoded)
    _evidence_path(capture_root, name, "receipt").write_bytes(_canonical(receipt) + b"\n")
    return decoded_path


class _CaptureState:
    """Resumable per-page evidence roster with atomic state persistence."""

    def __init__(self, root: Path):
        self.root = root
        self.path = root / "capture_state.json"
        self.data = {"pages": [], "complete": False}
        if self.path.exists():
            loaded = _load_json(self.path.read_bytes(), "capture state")
            if (type(loaded.get("pages")) is not list
                    or any(type(entry) is not dict for entry in loaded["pages"])
                    or loaded.get("complete") not in (True, False)):
                raise ValueError("capture state is invalid")
            self.data = loaded

    def accepted(self, name: str) -> dict | None:
        for entry in self.data["pages"]:
            if entry.get("name") == name:
                return entry
        return None

    def save(self) -> None:
        tmp = self.root / "capture_state.json.tmp"
        tmp.write_bytes(_canonical(self.data) + b"\n")
        tmp.replace(self.path)

    def accept(self, entry: dict) -> None:
        self.data["pages"].append(entry)
        self.save()

    def entries(self) -> list[dict]:
        return sorted(self.data["pages"], key=lambda entry: entry["offset"])

    def page_entries(self) -> list[dict]:
        return [entry for entry in self.entries() if PAGE_NAME_PATTERN.fullmatch(str(entry.get("name")))]


def _validate_walk(pages: list[dict], ids: list[str], duplicate_rows: list[dict]) -> None:
    """The accepted pages must tile one ascending identifier walk.

    The collection is captured as served: a repeated identifier is accepted
    only where the streaming audit recorded the same feature served twice
    consecutively inside one page (a source-served duplicate row, retained
    verbatim). Any other non-ascending order, a repeated identifier with
    different content, and any duplicate across a page boundary is refused as
    pagination drift.
    """
    offset = 0
    for entry in pages:
        if (entry.get("offset") != offset or not isinstance(entry.get("count"), int)
                or not isinstance(entry.get("sha256"), str) or not entry.get("fetched_at")):
            raise ValueError("accepted page roster is not a contiguous ascending walk")
        if entry["count"] > PAGE_LIMIT:
            raise ValueError("NLDI page exceeded the requested page limit")
        offset += entry["count"]
    if offset != len(ids):
        raise ValueError("the walked page counts do not reproduce the identifier roster")
    if not ids:
        raise ValueError("the pagination walk produced no features")
    audited = set()
    for row in duplicate_rows:
        index = row.get("index")
        if (type(index) is not int or index < 1 or index >= len(ids)
                or row.get("id") != ids[index] or ids[index] != ids[index - 1]):
            raise ValueError("audited duplicate rows do not match the walked identifiers")
        audited.add(index)
    cursor = 0
    for entry in pages:
        if cursor and entry["count"] and not ids[cursor] > ids[cursor - 1]:
            raise ValueError("the walked identifiers repeat or descend across a page boundary")
        cursor += entry["count"]
    for index, identifier in enumerate(ids):
        if index and not identifier > ids[index - 1] and index not in audited:
            raise ValueError("the walked identifier order is not strictly ascending")
    if len(set(ids)) != len(ids) - len(duplicate_rows):
        raise ValueError("the walked identifiers contain unaudited duplicates")


class _WalkAudit:
    """Streaming per-feature audit so a 200k-feature walk never holds its rows.

    The digests stream the exact canonical serializations a whole-collection
    parse would produce, and features are discarded after each page except the
    immediately previous one, which is retained to identify source-served
    duplicate rows (a repeated identifier whose feature is canonically
    identical to the row before it, served consecutively inside one page).
    """

    def __init__(self):
        self.ids: list[str] = []
        self.duplicate_rows: list[dict] = []
        self._id_hash = hashlib.sha256()
        self._geometry_hash = hashlib.sha256()
        self._bounds: dict[str, float] | None = None
        self._last_feature: dict | None = None
        self._last_feature_page_end = False
        schema = {"feature_type": "Feature", "property_names_in_source_order": list(PROPERTY_NAMES),
                  "geometry_type": "Point", "coordinate_dimension": 2, "root_metadata": {}}
        self.schema_sha256 = hashlib.sha256(_canonical(schema)).hexdigest()

    def update(self, features: list) -> None:
        for feature in features:
            identifier = _feature_identifier(feature)
            if self.ids and identifier == self.ids[-1]:
                if self._last_feature_page_end:
                    raise ValueError("NLDI walk repeated an identifier across a page boundary")
                if _canonical(feature) != _canonical(self._last_feature):
                    raise ValueError("NLDI walk repeated an identifier with different feature content")
                self.duplicate_rows.append({"id": identifier, "index": len(self.ids)})
            elif self.ids and identifier < self.ids[-1]:
                raise ValueError("the walked identifier order is not strictly ascending")
            _validate_feature(feature, identifier)
            self._id_hash.update(b"[" if not self.ids else b",")
            self._id_hash.update(_canonical(identifier))
            self.ids.append(identifier)
            coordinates = feature["geometry"]["coordinates"]
            self._geometry_hash.update(b"[" if len(self.ids) == 1 else b",")
            self._geometry_hash.update(_canonical([identifier, coordinates]))
            longitude, latitude = coordinates
            if self._bounds is None:
                self._bounds = {"xmin": longitude, "ymin": latitude, "xmax": longitude, "ymax": latitude}
            else:
                self._bounds = {"xmin": min(self._bounds["xmin"], longitude),
                                "ymin": min(self._bounds["ymin"], latitude),
                                "xmax": max(self._bounds["xmax"], longitude),
                                "ymax": max(self._bounds["ymax"], latitude)}
            self._last_feature = feature
            self._last_feature_page_end = False
        if features:
            self._last_feature_page_end = True

    def finish(self) -> tuple[int, list[str], dict]:
        ids = self.ids
        id_digest = self._id_hash.copy()
        id_digest.update(b"]")
        geometry_digest = self._geometry_hash.copy()
        geometry_digest.update(b"]")
        audit = {"schema_sha256": self.schema_sha256,
                 "id_roster_sha256": id_digest.hexdigest(),
                 "geometry_rows_sha256": geometry_digest.hexdigest(),
                 "source_property_names": list(PROPERTY_NAMES),
                 "unique_id_count": len(set(ids)),
                 "duplicate_feature_count": len(self.duplicate_rows),
                 "duplicate_rows": [dict(row) for row in self.duplicate_rows],
                 "null_geometry_count": 0,
                 "observed_bounds": self._bounds, "first_id": ids[0] if ids else None,
                 "last_id": ids[-1] if ids else None, "feature_count": len(ids)}
        return len(ids), ids, audit


def _compose_asset(pages: list[dict], capture_root: Path) -> bytes:
    """Recompose the walked pages, retaining every serialized lexeme verbatim."""
    interiors = []
    total_features = 0
    for entry in pages:
        decoded_path = _evidence_path(capture_root, entry["name"], "decoded")
        if not decoded_path.exists():
            raise ValueError("retained page evidence is missing at composition time")
        interiors.append(_page_slice(decoded_path.read_bytes()))
        total_features += entry["count"]
    body = (b'{"type":"FeatureCollection","features":[' + b",".join(interiors) + b"]}")
    expected_length = (len(b'{"type":"FeatureCollection","features":[') + len(b"]}")) \
        + sum(len(interior) for interior in interiors) + max(0, len(interiors) - 1)
    if len(body) != expected_length or total_features == 0:
        raise ValueError("composed candidate asset does not reproduce the walked features")
    return body


def _quarantine(staging: StagingStorageResource, version: str,
                captures: list[tuple[str, bytes, bytes, dict]], reason: str) -> str:
    identity = hashlib.sha256(_canonical({"reason": reason, "receipts": [c[3] for c in captures]})).hexdigest()[:20]
    base = f"_quarantine/{DATASET_SLUG}/{FILE_SLUG}/{version}-{identity}"
    names = []
    for name, wire, decoded, receipt in captures:
        _immutable(staging, f"{base}/raw/{name}.wire", wire)
        _immutable(staging, f"{base}/raw/{name}.decoded", decoded)
        _immutable(staging, f"{base}/raw/{name}.receipt.json", _canonical(receipt) + b"\n")
        names.append(name)
    _immutable(staging, f"{base}/metadata/failure.json", _canonical({"reason_code": reason, "captures": names}) + b"\n")
    return base


def _processing_identity() -> dict:
    files = {str(path.relative_to(REPO_ROOT)): sha256_file(path)
             for path in sorted(REPO_ROOT.joinpath("src/dagster_hifld").rglob("*.py"))}
    files.update({name: sha256_file(REPO_ROOT / name) for name in ("pyproject.toml", "uv.lock")})
    return {"code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True).strip(),
            "files": files, "python": platform.python_version()}


@contextmanager
def _version_lock(staging: StagingStorageResource, version: str):
    if staging.bucket or not staging.use_local or staging.prefix:
        raise ValueError("construction_state_requires_explicit_unprefixed_local_staging")
    root = Path(staging.local_dir).absolute()
    if ".." in root.parts or any(part.is_symlink() for part in (root, *root.parents)):
        raise ValueError("unsafe_construction_staging_root")
    lockdir = root / "_construction-state" / "locks"
    lockdir.mkdir(parents=True, exist_ok=True)
    lock_path = lockdir / f"{version}.lock"
    with lock_path.open("a+b") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _candidate_version(audit: dict, asset_sha256: str, identity: dict) -> str:
    stable_identity = {"distribution": "usgs-nldi-ref_gage-paginated-walk",
                       "feature_count": audit["feature_count"],
                       "source_schema_sha256": audit["schema_sha256"],
                       "source_id_roster_sha256": audit["id_roster_sha256"],
                       "source_geometry_sha256": audit["geometry_rows_sha256"],
                       "asset_sha256": asset_sha256, "processing_identity": identity}
    version = "v0.0.0-candidate." + hashlib.sha256(_canonical(stable_identity)).hexdigest()[:32]
    if not SEMVER_VERSION_RE.fullmatch(version):
        raise ValueError("capture cannot form a supported content-derived version")
    return version


def _verify_cached(staging: StagingStorageResource, prefix: str, state_key: str, version: str,
                   identity: dict, cache_inputs: list[dict], descriptors: dict[str, dict], audit: dict) -> dict:
    fingerprint = hashlib.sha256(_canonical(identity)).hexdigest()
    bundle = json.loads(load_first_construction(staging, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity", "inputs", "objects", "source_audit"}
            or payload["processing_identity"] != identity or payload["inputs"] != cache_inputs
            or payload["source_audit"] != audit
            or not isinstance(payload["objects"], dict) or set(payload["objects"]) != set(descriptors)):
        raise ValueError("cached_first_candidate_code_source_or_output_identity_changed")
    original_descriptors = payload["objects"]
    if set(staging.list_prefix(prefix)) != {f"{prefix}/{name}" for name in original_descriptors}:
        raise ValueError("cached_candidate_storage_roster_changed")
    for name, descriptor in original_descriptors.items():
        key = f"{prefix}/{name}"
        snapshot = staging.object_snapshot(key)
        if snapshot is None or descriptor != {"bytes": snapshot.size, "sha256": staging.sha256_key(key)} or staging.object_snapshot(key) != snapshot:
            raise ValueError("cached_candidate_object_integrity_failed:" + name)
    validate_construction_code_identity(REPO_ROOT, identity["code_commit"], identity["files"])
    return _result(audit, version, reused=True)


def _result(audit: dict, version: str, *, reused: bool) -> dict:
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["feature_count"], "unique_id_count": audit["unique_id_count"],
            "source_audit": audit, "reused": reused, "publication_status": "STOPPED",
            "source_release_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "compatibility_accepted": False, "release_accepted": False}


def _default_client() -> httpx.Client:
    return httpx.Client(follow_redirects=True,
                        timeout=httpx.Timeout(60.0, read=240.0, write=60.0, pool=60.0))


def fetch_and_stage_nldi(staging: StagingStorageResource, *, client: httpx.Client | None = None,
                         capture_directory: str | None = None) -> dict:
    """Walk the USGS NLDI ref_gage pagination, verify it completely, and stage one candidate."""
    capture_root = Path(capture_directory or DEFAULT_CAPTURE_ROOT).absolute()
    if ".." in capture_root.parts or any(part.is_symlink() for part in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    state = _CaptureState(capture_root)
    own_client = client is None
    session = client or _default_client()
    captures: list[tuple[str, bytes, bytes, dict]] = []
    capture_version = "unresolved-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    try:
        for label in SOURCE_NAMES:
            entry = state.accepted(label)
            if entry is not None:
                document = json.loads(_evidence_path(capture_root, label, "decoded").read_bytes())
                _sources_entry(document)
                continue
            wire, decoded, receipt = _fetch(session, SOURCES_URL + "?f=json",
                                            accept="application/json", limit=SOURCES_CAPTURE_BYTES)
            captures.append((label, wire, decoded, receipt))
            if receipt["status_code"] != 200 or not receipt["complete"]:
                _quarantine(staging, capture_version, captures, "nldi_source_listing_http_capture_incomplete")
                raise ValueError(f"NLDI {label} HTTP capture incomplete")
            try:
                document = json.loads(decoded)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                _quarantine(staging, capture_version, captures, "nldi_source_listing_malformed_json")
                raise ValueError(f"NLDI {label} listing was not valid JSON") from exc
            try:
                _sources_entry(document)
            except ValueError:
                _quarantine(staging, capture_version, captures, "nldi_feature_source_registration_drift")
                raise
            decoded_path = _retain(capture_root, label, wire, decoded, receipt)
            state.accept({"name": label, "offset": 0, "count": 0, "sha256": receipt["decoded_sha256"],
                          "fetched_at": receipt["fetched_at"], "decoded_path": str(decoded_path)})
            captures.clear()
        pages: list[dict] = []
        audit = _WalkAudit()
        offset = 0
        while True:
            name = f"page_{offset:09d}"
            retained = state.accepted(name)
            if retained is not None:
                decoded = _evidence_path(capture_root, name, "decoded").read_bytes()
                document = _load_json(decoded, f"retained NLDI page {name}")
                features = _page_collection(document)
                if len(features) != retained["count"]:
                    raise ValueError("retained NLDI page evidence does not match its accepted roster entry")
                audit.update(features)
                del features, document
                pages.append(retained)
                if retained["count"] < PAGE_LIMIT:
                    break
                offset += retained["count"]
                continue
            wire, decoded, receipt = _fetch(session, _page_body(offset),
                                            accept="application/geo+json", limit=PAGE_CAPTURE_BYTES)
            captures.append((name, wire, decoded, receipt))
            if receipt["status_code"] != 200 or not receipt["complete"]:
                _quarantine(staging, capture_version, captures, "nldi_page_http_capture_incomplete")
                raise ValueError(f"NLDI page at offset {offset} HTTP capture incomplete")
            try:
                document = _load_json(decoded, f"NLDI page {name}")
                features = _page_collection(document)
                if len(features) > PAGE_LIMIT:
                    raise ValueError("NLDI served more features than the requested page limit")
            except ValueError:
                _quarantine(staging, capture_version, captures, "nldi_page_limit_not_honored_or_malformed_page")
                raise
            # Retain the raw page before per-feature validation so a refused
            # feature still leaves its page evidence on disk unaccepted.
            decoded_path = _retain(capture_root, name, wire, decoded, receipt)
            count = len(features)
            audit.update(features)
            del features, document
            entry = {"name": name, "offset": offset, "count": count,
                     "sha256": receipt["decoded_sha256"], "fetched_at": receipt["fetched_at"],
                     "decoded_path": str(decoded_path)}
            state.accept(entry)
            pages.append(entry)
            captures.clear()
            if count < PAGE_LIMIT:
                break
            offset += count
        for entry in state.page_entries():
            if entry not in pages:
                raise ValueError("retained page evidence is outside the accepted walk")
        ids = audit.ids
        _validate_walk(pages, ids, audit.duplicate_rows)
        body = _compose_asset(pages, capture_root)
        feature_count, ids, audit = audit.finish()
        asset_sha256 = hashlib.sha256(body).hexdigest()
        audit["asset_sha256"] = asset_sha256
        if EXPECTED_COUNT is not None and feature_count != EXPECTED_COUNT:
            raise ValueError("NLDI ref_gage population count drifted from the pinned complete walk")
        if EXPECTED_UNIQUE_ID_COUNT is not None and audit["unique_id_count"] != EXPECTED_UNIQUE_ID_COUNT:
            raise ValueError("NLDI ref_gage unique identifier count drifted from the pinned walk")
        if EXPECTED_IDS_SHA256 and audit["id_roster_sha256"] != EXPECTED_IDS_SHA256:
            raise ValueError("NLDI ref_gage identifier roster drifted from the pinned walk")
        if EXPECTED_SCHEMA_SHA256 and audit["schema_sha256"] != EXPECTED_SCHEMA_SHA256:
            raise ValueError("NLDI ref_gage property schema drifted from the pinned walk")
        if EXPECTED_ASSET_SHA256 and asset_sha256 != EXPECTED_ASSET_SHA256:
            raise ValueError("composed NLDI candidate asset bytes drifted from the pinned walk")
        if EXPECTED_FIRST_ID is not None and audit["first_id"] != EXPECTED_FIRST_ID:
            raise ValueError("NLDI ref_gage first identifier drifted from the pinned walk")
        if EXPECTED_LAST_ID is not None and audit["last_id"] != EXPECTED_LAST_ID:
            raise ValueError("NLDI ref_gage last identifier drifted from the pinned walk")
        if EXPECTED_NULL_GEOMETRY_COUNT is not None and audit["null_geometry_count"] != EXPECTED_NULL_GEOMETRY_COUNT:
            raise ValueError("NLDI ref_gage null-geometry count drifted from the pinned walk")
        if EXPECTED_OBSERVED_BOUNDS is not None and audit["observed_bounds"] != EXPECTED_OBSERVED_BOUNDS:
            raise ValueError("NLDI ref_gage observed bounds drifted from the pinned walk")
        if (EXPECTED_DUPLICATE_FEATURE_COUNT is not None
                and audit["duplicate_feature_count"] != EXPECTED_DUPLICATE_FEATURE_COUNT):
            raise ValueError("NLDI ref_gage duplicate-row count drifted from the pinned walk")
        if (EXPECTED_DUPLICATE_IDS is not None
                and [row["id"] for row in audit["duplicate_rows"]] != EXPECTED_DUPLICATE_IDS):
            raise ValueError("NLDI ref_gage duplicate-row roster drifted from the pinned walk")
        state.data["complete"] = True
        state.save()
    finally:
        if own_client:
            session.close()
    identity = _processing_identity()
    version = _candidate_version(audit, asset_sha256, identity)
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    receipts = []
    inputs = []
    for entry in state.entries():
        name = entry["name"]
        receipt = _load_json(_evidence_path(capture_root, name, "receipt").read_bytes(), f"retained receipt {name}")
        receipts.append(receipt)
        if PAGE_NAME_PATTERN.fullmatch(name):
            canonical_url, retrieval_url = COLLECTION_URL, _page_body(entry["offset"])
        else:
            canonical_url = retrieval_url = SOURCES_URL + "?f=json"
        inputs.append({"input_id": f"usgs-nldi-{name}", "producer": "U.S. Geological Survey",
                       "host": "api.water.usgs.gov", "role": "producer_publication",
                       "canonical_url": canonical_url, "retrieval_url": retrieval_url,
                       "observed_final_url": receipt.get("final_url", retrieval_url),
                       "retrieved_at": receipt["fetched_at"], "http_status": receipt["status_code"],
                       "bytes": receipt["decoded_bytes"], "sha256": receipt["decoded_sha256"],
                       "release_date": None,
                       "release_date_unknown_reason": "The NLDI feature listing publishes no per-snapshot "
                                                      "release date; USGS documents the registered feature "
                                                      "sources and their pagination instead.",
                       "license": "Public domain (U.S. Government work); NLDI publishes no access constraints",
                       "attribution": "U.S. Geological Survey Network Linked Data Index (NLDI), ref_gage feature source."})
    manifest = {
        "title": "USGS Hydro Network Linked Data Index (NLDI) Streamgage Monitoring Locations",
        "description": "Candidate composed from the complete pagination walk of the USGS NLDI ref_gage feature "
                       "source on the agency's api.water.usgs.gov host. The upstream internetofwater/ref_gages "
                       "GitHub compilation is community lineage context and is never fetched. Publication "
                       "remains STOPPED pending source, compatibility and release review.",
        "publisher": "U.S. Geological Survey (NLDI)",
        "agency": "U.S. Geological Survey",
        "source_url": COLLECTION_URL,
        "license": "Public domain (U.S. Government work); NLDI publishes no access constraints",
        "schema_version": 1, "processor": "HIFLD Next",
        "lineage": "The inventory entry nhd/usgs-hydro-nldi-streamgage-monitoring-locations (agency US Geological "
                   "Survey) describes the NLDI streamgage monitoring locations. The pipeline input is USGS's own "
                   "NLDI publication: the ref_gage registered feature source documented by USGS at "
                   "api.water.usgs.gov/docs/nldi/feature-sources/ with limit/offset pagination, walked completely "
                   "from offset 0 to the end of the collection. The upstream internetofwater/ref_gages GitHub "
                   "compilation is the community project NLDI indexes; it is lineage context only and is never "
                   "fetched. The DataLumos 240397 archive is historical provenance and is not the refresh source. "
                   "NLDI pages declare no CRS member; the composed candidate retains the source-serialized x/y "
                   "order without axis swap or reprojection, and its semantic compatibility with legacy "
                   "declared-CRS contracts remains unaccepted.",
        "inputs": inputs,
        "code_commit": identity["code_commit"],
        "transformations": ["Pagination walk recomposition: each page's raw features array text is sliced and "
                            "concatenated into one FeatureCollection, retaining every serialized number lexeme, "
                            "member order and feature value verbatim; no feature, property, ID, geometry or "
                            "number-lexeme transformations are applied."],
        "literal_geojson_catalog": {"schema_version": 1, "path": ASSET_PATH, "bytes": len(body),
                                    "sha256": asset_sha256},
        "source_observation": {
            "fetched_at": receipts[0]["fetched_at"], "receipts": receipts,
            "publisher": "U.S. Geological Survey (NLDI)", "service_root": NLDI_ROOT,
            "feature_sources_documentation": FEATURE_SOURCES_DOC_URL,
            "registered_source_entry": EXPECTED_SOURCE_ENTRY,
            "pagination": {"mode": "limit_offset_walk", "page_limit": PAGE_LIMIT,
                           "pages": len(state.page_entries()), "resumable": True,
                           "documented_at": FEATURE_SOURCES_DOC_URL,
                           "note": "The bulk endpoint without pagination was observed returning HTTP 504 after a "
                                   "server-side timeout; the documented limit/offset pagination is used instead, "
                                   "with 250-feature pages because 1000-feature pages were observed exceeding the "
                                   "NLDI gateway's server-side budget (gateway-504 bodies and read timeouts) while "
                                   "smaller documented pages completed."},
            "feature_count": feature_count, "unique_id_count": audit["unique_id_count"],
            "duplicate_rows": {"feature_count": audit["duplicate_feature_count"],
                               "ids": [row["id"] for row in audit["duplicate_rows"]],
                               "treatment": "retained_verbatim_as_served",
                               "note": "The served collection repeated these identifiers as canonically "
                                       "identical consecutive rows inside one page; the walk retains them "
                                       "verbatim, refuses any other repeated, descending or cross-page "
                                       "identifier order as pagination drift, and pins the duplicate "
                                       "roster against drift."},
            "id_field": "Feature.id == properties.identifier",
            "id_type": "string", "id_examples": ids[:5], "first_id": audit["first_id"],
            "last_id": audit["last_id"],
            "geometry_type": "Point", "coordinate_dimension": 2,
            "source_declared_crs": None,
            "coordinate_order_interpretation": "source_serialized_x_y_retained_without_axis_order_reinterpretation_or_swap",
            "complete_capture": True,
            "source_audit": audit,
            "upstream_compilation_context": "https://github.com/internetofwater/ref_gages (community project NLDI "
                                            "indexes; named context only and never fetched)",
            "baseline_continuity": "unverified-for-nldi-capture",
            "agency_authority_accepted": False, "source_contract_accepted": False,
            "compatibility_accepted": False, "publication_status": "STOPPED",
            "baseline_note": BASELINE_NOTE,
        },
    }
    manifest["unsupported_output_formats"] = {"geoparquet": "No compatibility contract for the NLDI-served schema "
                                                            "and identifiers.",
                                              "geopackage": "No approved mapping for the NLDI-served fields/IDs.",
                                              "pmtiles": "No approved publication styling or tiling contract."}
    validate_provenance(manifest)
    outputs = {ASSET_PATH: body,
               "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for entry in state.entries():
        name = entry["name"]
        for suffix in ("wire", "decoded", "receipt.json"):
            outputs[f"evidence/{name}.{suffix}"] = _evidence_path(capture_root, name, suffix).read_bytes()
    descriptors = {name: {"bytes": len(body_), "sha256": hashlib.sha256(body_).hexdigest()}
                   for name, body_ in outputs.items()}
    cache_inputs = [{"distribution": "usgs-nldi-ref_gage-paginated-walk", "page_limit": PAGE_LIMIT,
                     "feature_count": feature_count, "asset_sha256": asset_sha256,
                     "id_roster_sha256": audit["id_roster_sha256"],
                     "schema_sha256": audit["schema_sha256"], "capture_root": str(capture_root)}]
    state_key = f"_construction-state/{DATASET_SLUG}/{FILE_SLUG}/{version}.json"
    fingerprint = hashlib.sha256(_canonical(identity)).hexdigest()
    with _version_lock(staging, version):
        if staging.object_snapshot(state_key) is not None:
            result = _verify_cached(staging, prefix, state_key, version, identity, cache_inputs, descriptors, audit)
            result["written_keys"] = [f"{prefix}/{ASSET_PATH}", f"{prefix}/evidence/",
                                      f"{prefix}/metadata/source_manifest.json"]
            return result
        if staging.list_prefix(prefix):
            raise ValueError("candidate prefix is nonempty without a valid first-construction witness")
        for relative, body_ in outputs.items():
            _immutable(staging, f"{prefix}/{relative}", body_)
        for relative, descriptor in descriptors.items():
            key = f"{prefix}/{relative}"
            snapshot = staging.object_snapshot(key)
            if snapshot is None or descriptor != {"bytes": snapshot.size, "sha256": staging.sha256_key(key)} \
                    or staging.object_snapshot(key) != snapshot:
                raise ValueError("candidate_output_physical_readback_failed:" + relative)
        payload = {"processing_identity": identity, "inputs": cache_inputs, "objects": descriptors,
                   "source_audit": audit}
        seal_first_construction(staging, state_key, version, fingerprint,
                                canonical_construction_bundle(version, fingerprint, payload))
        validate_construction_code_identity(REPO_ROOT, identity["code_commit"], identity["files"])
    result = _result(audit, version, reused=False)
    result["written_keys"] = [f"{prefix}/{ASSET_PATH}", f"{prefix}/evidence/",
                              f"{prefix}/metadata/source_manifest.json"]
    return result


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs_nldi",
    compute_kind="http",
    description="Complete USGS NLDI ref_gage pagination walk candidate from the agency's api.water.usgs.gov "
                "publication. Registration drift and pinned-walk drift stop for review; authority, "
                "compatibility, and release approvals remain blocked.",
    config_schema={"capture_directory": Field(str, default_value="")},
)
def usgs_nldi_streamgage_monitoring_locations(context, staging_storage: StagingStorageResource) -> dict:
    config = context.op_config
    result = fetch_and_stage_nldi(staging_storage,
                                  capture_directory=config["capture_directory"] or None)
    version = result["version"]
    partition_key = build_publish_partition_key(DATASET_SLUG, FILE_SLUG, version)
    if partition_key not in set(context.instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME)):
        context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition_key])
    context.add_output_metadata(result)
    return result


@asset_check(asset=usgs_nldi_streamgage_monitoring_locations, blocking=True)
def usgs_nldi_complete_source(candidate: dict) -> AssetCheckResult:
    count_ok = EXPECTED_COUNT is None or candidate.get("feature_count") == EXPECTED_COUNT
    audit = candidate.get("source_audit") or {}
    asset_ok = not EXPECTED_ASSET_SHA256 or audit.get("asset_sha256") == EXPECTED_ASSET_SHA256
    return AssetCheckResult(passed=count_ok and asset_ok, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "publication_status": "STOPPED", "source_release_accepted": False,
        "agency_authority_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "compatibility_accepted": False, "release_accepted": False,
    })


usgs_nldi_draft_refresh_job = define_asset_job(
    "usgs_nldi_draft_refresh",
    selection=AssetSelection.assets(usgs_nldi_streamgage_monitoring_locations),
)
usgs_nldi_draft_refresh_schedule = ScheduleDefinition(
    name="usgs_nldi_draft_refresh_schedule", job=usgs_nldi_draft_refresh_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

INGEST_ASSETS = [usgs_nldi_streamgage_monitoring_locations]
ASSET_CHECKS = [usgs_nldi_complete_source]
JOBS = [usgs_nldi_draft_refresh_job]
SCHEDULES = [usgs_nldi_draft_refresh_schedule]
