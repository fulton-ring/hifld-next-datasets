"""Internet of Water's published NLDI reference-gage compilation.

The source is pinned to the complete v1.7 release. ``releases/latest`` is
checked on every run so a new upstream release stops for review rather than
silently changing the inventory's source. Bytes and GeoJSON number lexemes are
retained exactly; release/authority/compatibility approval remains blocked.
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import subprocess
import fcntl
from datetime import datetime, timezone
from contextlib import contextmanager
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
REPOSITORY = "internetofwater/ref_gages"
LATEST_RELEASE_URL = f"https://api.github.com/repos/{REPOSITORY}/releases/latest"
PINNED_TAG = "v1.7"
PINNED_ASSET = "usgs_nldi_gages.geojson"
PINNED_PUBLISHED_AT = "2026-07-16T20:14:57Z"
PINNED_ASSET_BYTES = 132_757_553
PINNED_ASSET_SHA256 = "b8f14064fa77607921c32492f8beaa83e16870daee57bdde104b31ca2fc52cce"
PINNED_FEATURE_COUNT = 201_344
MAX_CAPTURE_BYTES = 128 * 1024 * 1024
ASSET_PATH = "geojson/usgs_nldi_gages.geojson"
REPO_ROOT = Path(__file__).resolve().parents[3]


def _reserve(_progress: CaptureProgress) -> None:
    """The caller bounds each response before capturing it."""


def _immutable(staging: StagingStorageResource, key: str, body: bytes) -> None:
    digest = hashlib.sha256(body).hexdigest()
    old = staging.object_snapshot(key)
    if old is not None:
        if old.size != len(body) or staging.sha256_key(key) != digest:
            raise ValueError("pinned Internet of Water release key already has different bytes")
        return
    staging.write_key_if_unchanged(key, body, None)
    current = staging.object_snapshot(key)
    if current is None or current.size != len(body) or staging.sha256_key(key) != digest:
        raise ValueError("Internet of Water capture readback did not match response bytes")


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


def _select_release(document: dict) -> tuple[dict, dict]:
    if document.get("draft") is not False or document.get("prerelease") is not False:
        raise ValueError("latest upstream release is draft/prerelease; manual source review required")
    tag = document.get("tag_name")
    if tag != PINNED_TAG or document.get("published_at") != PINNED_PUBLISHED_AT:
        raise ValueError("latest upstream release differs from pinned v1.7; manual drift review required")
    assets = document.get("assets")
    if type(assets) is not list:
        raise ValueError("latest upstream release has no asset listing")
    matches = [a for a in assets if type(a) is dict and a.get("name") == PINNED_ASSET]
    if len(matches) != 1:
        raise ValueError("pinned upstream release asset is missing or ambiguous")
    asset = matches[0]
    if asset.get("size") != PINNED_ASSET_BYTES or asset.get("digest") != f"sha256:{PINNED_ASSET_SHA256}":
        raise ValueError("pinned upstream asset size/digest metadata drifted; manual review required")
    url = asset.get("browser_download_url")
    if not isinstance(url, str) or not url.startswith(f"https://github.com/{REPOSITORY}/releases/download/{PINNED_TAG}/"):
        raise ValueError("pinned upstream release has an unexpected download URL")
    return document, asset


def _check_geojson(body: bytes) -> tuple[int, list[str], dict]:
    collection = _load_json(body, "pinned release asset")
    features = collection.get("features")
    if collection.get("type") != "FeatureCollection" or type(features) is not list:
        raise ValueError("pinned upstream asset is not a GeoJSON FeatureCollection")
    if len(features) != PINNED_FEATURE_COUNT:
        raise ValueError("pinned v1.7 feature count drifted; manual review required")
    ids: list[str] = []
    geometry_rows: list[list] = []
    property_keys: list[str] | None = None
    for feature in features:
        if type(feature) is not dict or feature.get("type") != "Feature":
            raise ValueError("pinned source contains a non-Feature member")
        props, geom = feature.get("properties"), feature.get("geometry")
        if type(props) is not dict or not isinstance(props.get("id"), str) or not props["id"]:
            raise ValueError("pinned source feature is missing its original string id")
        if not isinstance(props.get("provider_id"), str) or not props["provider_id"]:
            raise ValueError("pinned source feature is missing its original provider_id")
        if type(geom) is not dict or geom.get("type") != "Point":
            raise ValueError("pinned source geometry is not a Point")
        coordinates = geom.get("coordinates")
        if (type(coordinates) is not list or len(coordinates) != 2
                or any(type(value) not in (int, float) for value in coordinates)):
            raise ValueError("pinned source has unsupported Point coordinates")
        if property_keys is None:
            property_keys = list(props)
        elif list(props) != property_keys:
            raise ValueError("pinned source property roster/order drifted")
        ids.append(props["id"])
        geometry_rows.append([props["id"], coordinates])
    if len(set(ids)) != len(ids):
        raise ValueError("pinned source contains duplicate original ids")
    schema = {"root_members": list(collection), "feature_type": "Feature", "property_names_in_source_order": property_keys,
              "geometry_type": "Point", "coordinate_dimension": 2}
    audit = {"schema_sha256": hashlib.sha256(_canonical(schema)).hexdigest(),
             "id_roster_sha256": hashlib.sha256(_canonical(ids)).hexdigest(),
             "geometry_rows_sha256": hashlib.sha256(_canonical(geometry_rows)).hexdigest(),
             "source_root_members": list(collection), "source_property_names": property_keys,
             "unique_id_count": len(set(ids)), "null_geometry_count": 0}
    return len(features), ids, audit


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


def _record_observation(staging: StagingStorageResource, version: str,
                        release_wire: bytes, release_bytes: bytes,
                        release_receipt: dict, asset_receipt: dict) -> None:
    """Preserve each fresh metadata/receipt observation without mutating a candidate."""
    digest = hashlib.sha256(_canonical([release_receipt, asset_receipt])).hexdigest()
    base = f"_observations/{FILE_SLUG}/{version}/{digest}"
    for name, body in (("release-metadata.wire", release_wire), ("release-metadata.decoded", release_bytes),
                        ("release-metadata.receipt.json", _canonical(release_receipt) + b"\n"),
                        ("release-asset.receipt.json", _canonical(asset_receipt) + b"\n")):
        _immutable(staging, f"{base}/{name}", body)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _candidate_version(release: dict, asset: dict, source_sha256: str, audit: dict, identity: dict) -> str:
    stable_identity = {
        "release_tag": release["tag_name"], "release_published_at": release["published_at"],
        "asset_name": asset["name"], "asset_bytes": asset["size"], "asset_digest": asset["digest"],
        "source_sha256": source_sha256, "source_schema_sha256": audit["schema_sha256"],
        "source_id_roster_sha256": audit["id_roster_sha256"],
        "source_geometry_sha256": audit["geometry_rows_sha256"], "processing_identity": identity,
    }
    version = "v" + release["tag_name"].removeprefix("v") + ".0-candidate." + hashlib.sha256(_canonical(stable_identity)).hexdigest()[:32]
    if not SEMVER_VERSION_RE.fullmatch(version):
        raise ValueError("upstream release tag cannot form a supported content-derived version")
    return version


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
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "release_tag": PINNED_TAG, "feature_count": PINNED_FEATURE_COUNT, "sha256": PINNED_ASSET_SHA256,
            "byte_count": PINNED_ASSET_BYTES, "source_audit": audit, "reused": True, "publication_status": "STOPPED",
            "source_release_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "compatibility_accepted": False, "release_accepted": False}


def _replay_capture(root: Path) -> tuple[dict, dict, bytes, bytes, bytes, bytes, list[dict]]:
    """Load a prior complete, hash-pinned capture while retaining its timestamps."""
    evidence = root / "evidence"
    manifest_path = root / "metadata" / "source_manifest.json"
    old = _load_json(manifest_path.read_bytes(), "verified replay manifest")
    receipts = old.get("source_observation", {}).get("receipts")
    if not isinstance(receipts, list) or len(receipts) != 2:
        raise ValueError("verified replay requires original release metadata and asset receipts")
    release_receipt, asset_receipt = receipts
    metadata_candidates = list(evidence.glob("release-metadata*.decoded"))
    if len(metadata_candidates) != 1:
        raise ValueError("verified replay metadata capture is missing or ambiguous")
    release_bytes = metadata_candidates[0].read_bytes()
    release_wire = metadata_candidates[0].with_suffix(".wire").read_bytes()
    asset_bytes = (evidence / f"{PINNED_ASSET}.decoded").read_bytes()
    asset_wire = (evidence / f"{PINNED_ASSET}.wire").read_bytes()
    for body, wire, receipt in ((release_bytes, release_wire, release_receipt), (asset_bytes, asset_wire, asset_receipt)):
        if (receipt.get("complete") is not True or receipt.get("status_code") != 200
                or receipt.get("decoded_bytes") != len(body) or receipt.get("decoded_sha256") != hashlib.sha256(body).hexdigest()
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != hashlib.sha256(wire).hexdigest()):
            raise ValueError("verified replay original receipt does not match captured raw bytes")
    release, asset = _select_release(_load_json(release_bytes, "replayed release metadata"))
    if asset_receipt.get("request", {}).get("url") != asset.get("browser_download_url"):
        raise ValueError("verified replay asset receipt URL differs from published release metadata")
    return release, asset, release_wire, release_bytes, asset_wire, asset_bytes, receipts


def fetch_and_stage_nldi(staging: StagingStorageResource, *, client: httpx.Client | None = None,
                         replay_capture_directory: str | None = None) -> dict:
    """Capture pinned release or explicitly replay a hash-verified prior capture."""
    capture_version = "unresolved-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    captures: list[tuple[str, bytes, bytes, dict]] = []
    if replay_capture_directory:
        (release, asset, release_wire, release_bytes, asset_wire, asset_bytes,
         receipts) = _replay_capture(Path(replay_capture_directory))
    else:
        own_client = client is None
        session = client or httpx.Client(follow_redirects=True, timeout=httpx.Timeout(120.0))
        try:
            release_wire, release_bytes, release_receipt = _fetch(
                session, LATEST_RELEASE_URL, accept="application/vnd.github+json", limit=1024 * 1024,
            )
            captures.append(("release-metadata", release_wire, release_bytes, release_receipt))
            if release_receipt["status_code"] != 200 or not release_receipt["complete"]:
                _quarantine(staging, capture_version, captures, "release_metadata_http_capture_incomplete")
                raise ValueError("release metadata HTTP capture incomplete")
            try:
                release, asset = _select_release(_load_json(release_bytes, "GitHub latest release metadata"))
            except ValueError:
                _quarantine(staging, capture_version, captures, "latest_release_manual_review_required")
                raise
            asset_wire, asset_bytes, asset_receipt = _fetch(
                session, asset["browser_download_url"], accept="application/geo+json", limit=MAX_CAPTURE_BYTES,
            )
            captures.append(("release-asset", asset_wire, asset_bytes, asset_receipt))
            if asset_receipt["status_code"] != 200 or not asset_receipt["complete"]:
                _quarantine(staging, capture_version, captures, "release_asset_http_capture_incomplete")
                raise ValueError("pinned release asset HTTP capture incomplete")
        finally:
            if own_client:
                session.close()
        receipts = [release_receipt, asset_receipt]
    release_receipt, asset_receipt = receipts
    digest = hashlib.sha256(asset_bytes).hexdigest()
    if len(asset_bytes) != PINNED_ASSET_BYTES or digest != PINNED_ASSET_SHA256:
        if captures: _quarantine(staging, capture_version, captures, "complete_release_asset_hash_or_size_drift")
        raise ValueError("complete release asset bytes differ from pinned v1.7 size/SHA-256")
    try:
        feature_count, ids, audit = _check_geojson(asset_bytes)
    except ValueError:
        if captures: _quarantine(staging, capture_version, captures, "release_geojson_schema_or_coverage_drift")
        raise
    metadata_digest = hashlib.sha256(release_bytes).hexdigest()
    receipt_bytes = [_canonical(receipt) + b"\n" for receipt in receipts]
    identity = _processing_identity()
    derived_version = _candidate_version(release, asset, digest, audit, identity)
    version = derived_version
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    _record_observation(staging, version, release_wire, release_bytes, release_receipt, asset_receipt)
    manifest = {
        "title": "USGS Hydro Network Linked Data Index (NLDI) Streamgage Monitoring Locations",
        "description": "Candidate native GeoJSON from Internet of Water's maintained ref_gages v1.7 release. Upstream release is a changed successor to the historical HIFLD v1.0.0 baseline with material identity, schema, and population drift; refresh and publication remain STOPPED pending source delegation and compatibility review.",
        "publisher": "Internet of Water (maintainer/publisher of the ref_gages compilation)",
        "agency": "USGS sites are represented in the compilation; USGS agency authority/delegation for this release is unaccepted",
        "source_url": asset["browser_download_url"], "license": "Unverified; upstream release licensing requires review",
        "schema_version": 1, "processor": "HIFLD Next",
        "lineage": "Inventory and historical published provenance identify internetofwater/ref_gages. The complete pinned v1.7 release was compared with the complete HIFLD v1.0.0 candidate: it is a changed successor with material population, ID, and schema drift, not a clean continuation. Official NLDI ref_gage distribution is corroborating evidence only. DataLumos 240397 V1 is a historical archive and is not the refresh source.",
        "inputs": [
            {"input_id": "internetofwater-ref-gages-release-metadata", "producer": "Internet of Water", "host": "api.github.com", "role": "producer_publication", "canonical_url": f"https://github.com/{REPOSITORY}/releases/tag/{PINNED_TAG}", "retrieval_url": LATEST_RELEASE_URL, "observed_final_url": release_receipt.get("final_url", LATEST_RELEASE_URL), "retrieved_at": release_receipt["fetched_at"], "http_status": 200, "bytes": len(release_bytes), "sha256": metadata_digest, "license": "Upstream release licensing not verified", "attribution": "Internet of Water ref_gages release metadata; agency delegation unaccepted", "release_date": release.get("published_at"), "distributor": "GitHub Releases", "release_date_unknown_reason": None},
            {"input_id": "internetofwater-ref-gages-v1.7", "producer": "Internet of Water", "host": "github.com", "role": "producer_publication", "canonical_url": f"https://github.com/{REPOSITORY}/releases/tag/{PINNED_TAG}", "retrieval_url": asset["browser_download_url"], "observed_final_url": asset_receipt.get("final_url", asset["browser_download_url"]), "retrieved_at": asset_receipt["fetched_at"], "http_status": 200, "bytes": len(asset_bytes), "sha256": digest, "license": "Upstream release licensing not verified", "attribution": "Internet of Water ref_gages v1.7; USGS agency/delegation acceptance unresolved", "release_date": release.get("published_at"), "distributor": "GitHub Releases"},
        ],
        "code_commit": identity["code_commit"],
        "transformations": ["Identity preservation: exact published source GeoJSON asset bytes retained as the staged catalog asset; no feature, property, ID, geometry, or number-lexeme transformations."],
        "literal_geojson_catalog": {"schema_version": 1, "path": ASSET_PATH, "bytes": len(asset_bytes), "sha256": digest},
        "source_observation": {
            "fetched_at": receipts[0]["fetched_at"], "receipts": receipts,
            "publisher": "Internet of Water", "repository": f"https://github.com/{REPOSITORY}",
            "release_tag": PINNED_TAG, "release_published_at": release.get("published_at"),
            "release_date_is_extraction_date": False, "release_author_login": (release.get("author") or {}).get("login"),
            "asset_name": PINNED_ASSET, "asset_size": PINNED_ASSET_BYTES,
            "asset_github_digest": f"sha256:{PINNED_ASSET_SHA256}", "asset_sha256_independent": digest,
            "feature_count": feature_count, "unique_id_count": len(ids), "id_field": "properties.id",
            "id_type": "string", "id_examples": ids[:5], "provider_id_field": "properties.provider_id",
            "geometry_type": "Point", "complete_capture": True,
            "source_audit": audit,
            "official_nldi_distribution_evidence": "https://api.water.usgs.gov/nldi/linked-data/ref_gage",
            "inventory_crosswalk_status": "unverified", "baseline_continuity": "changed-successor-material-drift",
            "agency_delegation_acceptance": False, "source_contract_accepted": False,
            "compatibility_accepted": False, "publication_status": "STOPPED",
        },
    }
    manifest["transformations"] = ["Identity preservation: exact published source GeoJSON asset bytes retained as the staged catalog asset; no feature, property, ID, geometry, or number-lexeme transformations."]
    manifest["unsupported_output_formats"] = {"geoparquet": "No compatibility contract for changed v1.7 schema and IDs.", "geopackage": "No approved mapping for changed source fields/IDs.", "pmtiles": "No approved publication styling or tiling contract."}
    validate_provenance(manifest)
    outputs = {
        ASSET_PATH: asset_bytes,
        "evidence/release-metadata.wire": release_wire,
        "evidence/release-metadata.decoded": release_bytes,
        f"evidence/{PINNED_ASSET}.wire": asset_wire,
        f"evidence/{PINNED_ASSET}.decoded": asset_bytes,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/release-metadata.receipt.json": receipt_bytes[0],
        f"evidence/{PINNED_ASSET}.receipt.json": receipt_bytes[1],
    }
    descriptors = {name: {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()} for name, body in outputs.items()}
    cache_inputs = [{"release_tag": PINNED_TAG, "release_published_at": release.get("published_at"),
                     "asset_name": PINNED_ASSET, "asset_size": PINNED_ASSET_BYTES,
                     "asset_digest": f"sha256:{PINNED_ASSET_SHA256}", "asset_sha256": digest,
                     "asset_bytes": len(asset_bytes), "asset_retrieval_url": asset["browser_download_url"],
                     "latest_metadata_endpoint": LATEST_RELEASE_URL}]
    state_key = f"_construction-state/{DATASET_SLUG}/{FILE_SLUG}/{version}.json"
    fingerprint = hashlib.sha256(_canonical(identity)).hexdigest()
    with _version_lock(staging, version):
        if staging.object_snapshot(state_key) is not None:
            result = _verify_cached(staging, prefix, state_key, version, identity, cache_inputs, descriptors, audit)
            result["written_keys"] = [f"{prefix}/{ASSET_PATH}", f"{prefix}/evidence/", f"{prefix}/metadata/source_manifest.json"]
            return result
        if staging.list_prefix(prefix):
            raise ValueError("candidate prefix is nonempty without a valid first-construction witness")
        for relative, body in outputs.items():
            _immutable(staging, f"{prefix}/{relative}", body)
        for relative, descriptor in descriptors.items():
            key = f"{prefix}/{relative}"
            snapshot = staging.object_snapshot(key)
            if snapshot is None or descriptor != {"bytes": snapshot.size, "sha256": staging.sha256_key(key)} or staging.object_snapshot(key) != snapshot:
                raise ValueError("candidate_output_physical_readback_failed:" + relative)
        payload = {"processing_identity": identity, "inputs": cache_inputs, "objects": descriptors, "source_audit": audit}
        seal_first_construction(staging, state_key, version, fingerprint,
                                canonical_construction_bundle(version, fingerprint, payload))
        validate_construction_code_identity(REPO_ROOT, identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "release_tag": PINNED_TAG, "feature_count": feature_count, "sha256": digest,
            "byte_count": len(asset_bytes), "written_keys": [f"{prefix}/{ASSET_PATH}", f"{prefix}/evidence/", f"{prefix}/metadata/source_manifest.json"],
            "source_audit": audit, "reused": False, "publication_status": "STOPPED", "source_release_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "compatibility_accepted": False, "release_accepted": False}


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs_nldi",
    compute_kind="http",
    description="Complete Internet of Water ref_gages v1.7 release candidate. Latest-release drift stops for review; source, agency, and compatibility approvals remain blocked.",
    config_schema={"replay_capture_directory": Field(str, default_value="")},
)
def usgs_nldi_streamgage_monitoring_locations(context, staging_storage: StagingStorageResource) -> dict:
    config = context.op_config
    result = fetch_and_stage_nldi(staging_storage,
                                  replay_capture_directory=config["replay_capture_directory"] or None)
    version = result["version"]
    partition_key = build_publish_partition_key(DATASET_SLUG, FILE_SLUG, version)
    if partition_key not in set(context.instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME)):
        context.instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition_key])
    context.add_output_metadata(result)
    return result


@asset_check(asset=usgs_nldi_streamgage_monitoring_locations, blocking=True)
def usgs_nldi_complete_source(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == PINNED_FEATURE_COUNT and candidate.get("sha256") == PINNED_ASSET_SHA256
    return AssetCheckResult(passed=passed, metadata={
        "complete_source_population": candidate.get("feature_count", 0),
        "publication_status": "STOPPED", "source_release_accepted": False,
        "agency_delegation_accepted": False, "schema_contract_accepted": False,
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
