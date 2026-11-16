"""Capture one NFHL layer and stage it as an unapproved, content-versioned candidate.

Completeness is proven against the producer at capture time: the before and
after witnesses (count, OBJECTID count/min/max, service and layer documents)
must agree, and the captured unique OBJECTIDs must equal the producer's count.
The candidate layout, metadata keys, construction-state seal, cached-candidate
verification and quarantine are the shared ``epa_frs_common`` contract, so the
shared catalog and publish stages need no NFHL-specific code. The candidate
version is derived from record, geometry and schema digests plus code
identity, never from the clock.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import ExitStack
from pathlib import Path
from urllib.parse import urlsplit

import pyogrio

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.gdal import with_large_geojson_support
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    content_derived_version,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical, sha
from dagster_hifld.sources.nfhl.capture import (
    EXPECTED_PORTAL_ITEM,
    EXPECTED_PORTAL_ORG,
    PORTAL_ITEM_URL,
    PORTAL_ORG_URL,
    SERVICE_DOC_URL,
    SERVICE_URL,
    CaptureSettings,
    EvidenceDirectory,
    LayerCapture,
    check_spatial_reference,
    strip_rotating_palette,
    window_width,
)
from dagster_hifld.sources.nfhl.convert import LayerWriter, plain_field
from dagster_hifld.sources.nfhl.layers import NfhlLayer
from dagster_hifld.validation import sha256_file

REPO_ROOT = Path(__file__).parents[4]
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
DEFAULT_SETTINGS = CaptureSettings()
APPROVAL_FLAGS = {"source_release_accepted": False, "source_freshness_accepted": False,
                  "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
                  "mapping_approved": False, "release_accepted": False}
_READY_FLAGS = {"freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
                "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
_RELEASE_DATE_REASON = ("The NFHL is continuously updated; the service and its documentation page publish no "
                        "per-snapshot release date.")
_GEOMETRY_WORDS = {"point": "point", "line": "polyline", "polygon": "polygon"}


def layer_url(layer: NfhlLayer) -> str:
    return f"{SERVICE_URL}/{layer.layer_id}"


def _check_identity(layer: NfhlLayer, service: dict, layer_doc: dict) -> str:
    """Check the live service and layer documents name this layer; return its OBJECTID field."""
    roster_key, kind = ("tables", "Table") if layer.is_table else ("layers", "Feature Layer")
    if service.get("mapName") != "NFHL":
        raise ValueError("official_fema_nfhl_service_identity_drift")
    roster = service.get(roster_key)
    if not isinstance(roster, list) or not any(
            isinstance(x, dict) and (x.get("id"), x.get("name"), x.get("type")) == (layer.layer_id, layer.name, kind)
            for x in roster):
        raise ValueError(f"nfhl_service_roster_missing_layer:{layer.layer_id}:{layer.name}")
    if (layer_doc.get("id"), layer_doc.get("name"), layer_doc.get("type")) != (layer.layer_id, layer.name, kind) \
            or layer_doc.get("geometryType") != layer.esri_geometry_type:
        raise ValueError(f"nfhl_layer_identity_drift:{layer.layer_id}")
    declared_zm = (layer_doc.get("hasZ") is True, layer_doc.get("hasM") is True)
    if any(declared_zm):
        if layer.source_zm is None:
            raise ValueError("source_z_or_m_geometry_unsupported")
        if declared_zm != layer.source_zm:
            raise ValueError(f"nfhl_layer_zm_declaration_drift:{declared_zm}")
    elif layer.source_zm is not None:
        raise ValueError(f"nfhl_layer_zm_declaration_drift:{declared_zm}")
    if type(layer_doc.get("maxRecordCount")) is not int or layer_doc["maxRecordCount"] < 1:
        raise ValueError("source_record_limit_missing")
    fields = layer_doc.get("fields")
    if not isinstance(fields, list) or any(not isinstance(f, dict) or not isinstance(f.get("name"), str)
                                           or not isinstance(f.get("type"), str) for f in fields):
        raise ValueError("source_field_roster_invalid")
    names = [f["name"] for f in fields]
    oid_fields = [f["name"] for f in fields if f["type"] == "esriFieldTypeOID"]
    if len(set(names)) != len(names) or len(oid_fields) != 1:
        raise ValueError("source_field_roster_invalid")
    missing = [name for name in layer.id_fields if name not in names]
    if missing:
        raise ValueError("source_id_fields_missing:" + ",".join(missing))
    if not layer.is_table:
        check_spatial_reference(layer_doc.get("sourceSpatialReference"), SOURCE_CRS_WKID, "source_layer")
    return oid_fields[0]


def _capture(cap: LayerCapture, layer: NfhlLayer, evidence: EvidenceDirectory, parquet_path: Path) -> dict:
    url = layer_url(layer)
    if SERVICE_URL.encode() not in cap.raw("service_doc", SERVICE_DOC_URL):
        raise ValueError("fema_service_documentation_link_drift")
    if layer.portal_identity:
        item = cap.doc("portal_item", PORTAL_ITEM_URL + "?f=json")
        org = cap.doc("portal_org", PORTAL_ORG_URL + "?f=json")
        if {key: item.get(key) for key in EXPECTED_PORTAL_ITEM} != EXPECTED_PORTAL_ITEM:
            raise ValueError("fema_portal_service_item_identity_drift")
        if {key: org.get(key) for key in EXPECTED_PORTAL_ORG} != EXPECTED_PORTAL_ORG:
            raise ValueError("fema_portal_org_identity_drift")
    service = cap.doc("service_before", SERVICE_URL + "?f=pjson")
    layer_doc = cap.doc("layer_before", url + "?f=pjson")
    oid_field = _check_identity(layer, service, layer_doc)
    before = cap.witness("before", oid_field)
    width = 0
    if before["count"]:
        width = window_width(before["count"], before["min"], before["max"],
                             layer.page_records or layer_doc["maxRecordCount"])
    evidence.accept_witness({"layer_url": url, "witness": before, "window_width": width,
                             "service_sha256": sha(canonical(service)),
                             "layer_sha256": sha(canonical(strip_rotating_palette(layer_doc)))})
    writer = LayerWriter(parquet_path, layer, layer_doc, oid_field, SOURCE_CRS_WKID)
    try:
        if before["count"]:
            for page in cap.windows(oid_field, before["min"], before["max"], width, spatial=not layer.is_table):
                writer.add_page(page)
        summary = writer.close()
    except BaseException:
        writer.abort()
        raise
    layer_after = cap.doc("layer_after", url + "?f=pjson")
    service_after = cap.doc("service_after", SERVICE_URL + "?f=pjson")
    after = cap.witness("after", oid_field)
    if strip_rotating_palette(layer_after) != strip_rotating_palette(layer_doc):
        raise ValueError("source_layer_metadata_changed_during_capture")
    if service_after != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if after != before:
        raise ValueError(f"source_count_changed_during_capture:before={canonical(before).decode()},"
                         f"after={canonical(after).decode()}")
    if summary["feature_count"] != before["count"]:
        raise ValueError(f"source_capture_incomplete:captured={summary['feature_count']},"
                         f"source_count={before['count']}")
    fields = [plain_field(f) for f in layer_doc["fields"]]
    return {**summary, "layer_doc": layer_doc, "oid_field": oid_field, "witness": before,
            "source_fields": fields, "source_schema_sha256": sha(canonical(fields)),
            "page_count": sum(name.startswith("page_") for name in cap.log.accepted)}


class _EvidenceBodies(Mapping):
    """Read retained evidence one capture at a time for quarantine."""

    def __init__(self, root: Path):
        self.root = root
        self.names = sorted(path.name.removesuffix(".receipt.json") for path in root.glob("*.receipt.json")
                            if path.with_name(path.name.replace(".receipt.json", ".body")).exists()
                            and path.with_name(path.name.replace(".receipt.json", ".wire")).exists())

    def __getitem__(self, name: str):
        return (self.root / f"{name}.body").read_bytes(), json.loads((self.root / f"{name}.receipt.json").read_bytes())

    def __iter__(self) -> Iterator[str]:
        return iter(self.names)

    def __len__(self) -> int:
        return len(self.names)


def _check_storage(storage: StagingStorageResource) -> Path:
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    return base


def capture_nfhl_layer(storage: StagingStorageResource, layer: NfhlLayer, *, capture_directory: str | None = None,
                       settings: CaptureSettings | None = None) -> dict[str, object]:
    """Capture, verify and stage a complete unapproved candidate for one NFHL layer.

    With ``capture_directory`` the capture is resumable under
    ``<capture_directory>/<slug>``; without it, a temporary directory is used.
    """
    settings = settings or DEFAULT_SETTINGS
    base = _check_storage(storage)
    if shutil.disk_usage(base).free < layer.min_free_bytes:
        raise ValueError("disk_floor_before_acquisition")
    with ExitStack() as stack:
        parent = Path(capture_directory) if capture_directory else Path(
            stack.enter_context(tempfile.TemporaryDirectory(prefix="nfhl-capture-"))).resolve()
        root = (parent / layer.slug).absolute()
        if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        evidence = EvidenceDirectory(root)
        stack.callback(evidence.close)
        parquet_path = root / "work" / "candidate.parquet"
        parquet_path.parent.mkdir(exist_ok=True)
        parquet_path.unlink(missing_ok=True)
        cap = LayerCapture(None, evidence, settings, layer_url(layer), max_page_bytes=layer.max_page_bytes)
        try:
            with settings.client() as client, with_large_geojson_support():
                cap.client = client
                result = _capture(cap, layer, evidence, parquet_path)
        except Exception as exc:
            parquet_path.unlink(missing_ok=True)
            reason = str(exc).split(":", 1)[0] or type(exc).__name__
            (root / "capture_manifest.json").write_bytes(canonical({
                "complete": False, "error": str(exc)[:2000], "error_type": type(exc).__name__,
                "accepted": cap.log.accepted, "set_aside": cap.log.set_aside,
                "retired_to": str(evidence.previous), "retired": evidence.retired}) + b"\n")
            bodies = _EvidenceBodies(root)
            if bodies:
                quarantine_incomplete_capture(storage, root, layer.slug, "capture", bodies, reason)
            raise
        (root / "capture_manifest.json").write_bytes(canonical({
            "complete": True, "accepted": cap.log.accepted, "set_aside": cap.log.set_aside,
            "retired": evidence.retired, "source_feature_count": result["feature_count"],
            "source_record_sha256": result["source_record_sha256"]}) + b"\n")
        try:
            candidate = _stage_candidate(storage, layer, root, cap.log.accepted, result, parquet_path)
        finally:
            parquet_path.unlink(missing_ok=True)
        return {**candidate, "page_count": result["page_count"], "reused_page_count": cap.log.reused_pages,
                "set_aside_window_count": len(cap.log.set_aside),
                "capture_directory": str(root) if capture_directory else None}


def _stage(storage: StagingStorageResource, key: str, *, body: bytes | None = None,
           path: Path | None = None) -> dict[str, object]:
    if path is not None:
        size, digest = path.stat().st_size, sha256_file(path)
        snapshot = storage.object_snapshot(key)
        if snapshot is None:
            storage.upload_local_file(path, key)
        elif snapshot.size != size or storage.sha256_key(key) != digest:
            raise ValueError("immutable_staged_object_changed:" + key)
    else:
        write_immutable(storage, key, body)
        size, digest = len(body), sha(body)
    snapshot = storage.object_snapshot(key)
    if snapshot is None or snapshot.size != size or storage.sha256_key(key) != digest \
            or storage.object_snapshot(key) != snapshot:
        raise ValueError("candidate_output_physical_readback_failed:" + key)
    return {"bytes": size, "sha256": digest}


def _alias_text(aliases: tuple[tuple[str, int], ...]) -> str:
    names = [f"{inventory_id} with DataLumos project {project}" for inventory_id, project in aliases]
    return "".join(f", and the HIFLD Open inventory alias {name}" for name in names)


def _manifest(layer: NfhlLayer, identity: dict, receipts: dict[str, dict], rel: str,
              parquet: dict[str, object]) -> dict[str, object]:
    url, kind = layer_url(layer), ("table" if layer.is_table else "layer")
    subject = f" ({layer.subject})" if layer.subject else ""
    geometry = ("The producer publishes no geometry for this table; the candidate is attributes-only and "
                "fabricates no geometry column" if layer.is_table else
                f"Source NAD83 (EPSG:{SOURCE_CRS_WKID}) {_GEOMETRY_WORDS[layer.geometry]} geometry decoded by "
                "GDAL's ESRIJSON driver and retained as WKB without projection or repair; null source geometry "
                "stays null; geometry GDAL cannot decode (such as string-'NaN' ordinates) is null in the "
                "candidate with raw evidence intact; empty, invalid and out-of-domain geometry is retained and "
                "counted in source_audit.json")
    manifest = {
        "schema_version": 1, "approved": False, "title": layer.title,
        "description": f"FEMA National Flood Hazard Layer (NFHL) MapServer {kind} {layer.layer_id} "
                       f"'{layer.name}'{subject}, captured completely from the producer's own REST publication "
                       "and checked against the producer's own count and OBJECTID statistics at capture time. "
                       "The NFHL is continuously updated and publishes no per-snapshot release date; freshness, "
                       "rights, public schema/ID contract, and release acceptance remain unresolved and "
                       "unapproved.",
        "publisher": PUBLISHER, "agency": "Federal Emergency Management Agency", "source_url": url,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication",
                    "canonical_url": {"service_doc": SERVICE_DOC_URL, "portal_item": PORTAL_ITEM_URL,
                                      "portal_org": PORTAL_ORG_URL}.get(name, url),
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"], "release_date": None,
                    "release_date_unknown_reason": _RELEASE_DATE_REASON,
                    "license": "Not stated by the captured endpoints; rights review pending.",
                    "attribution": "FEMA National Flood Hazard Layer; formal attribution terms require maintainer "
                                   "review."}
                   for name, receipt in sorted(receipts.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate" if not layer.is_table
                             else "native_esri_json_table_to_parquet_candidate",
                             "parameters": {
                                 f"{kind}_id": layer.layer_id,
                                 "source_fields": "all producer fields retained in producer order and physical "
                                                  "types; Esri dates retained as epoch-millisecond integers",
                                 "source_ids": f"Preserve producer {', '.join(layer.id_fields)} values unchanged; "
                                               f"no substitute IDs, casts, trimming, or joins. {layer.id_note}"
                                               .strip(),
                                 "geometry": geometry,
                                 "completeness": "OBJECTID windows bisected on producer refusal or truncation; "
                                                 "captured unique OBJECTIDs equal the producer count, and the "
                                                 "before/after count, statistics, service and layer witnesses "
                                                 "agree",
                                 "candidate_status": "unapproved; publication blocked pending freshness, "
                                                     "schema/ID contract, attribution and release acceptance"}}],
        "lineage": "FEMA publishes the NFHL as its own geospatial database (fema.gov/flood-maps/national-flood-"
                   "hazard-layer). FEMA's MSC page 'GIS Web Services for the FEMA National Flood Hazard Layer "
                   f"(NFHL)' documents this exact REST URL ({SERVICE_URL}) as the ArcGIS REST service for effective "
                   "NFHL data; its HTML is captured as 'service_doc' evidence. This candidate is exactly "
                   f"{kind} {layer.layer_id} '{layer.name}' of that producer service. The HIFLD Open inventory lists "
                   f"this dataset as {layer.inventory_id} with DataLumos project {layer.datalumos_project}"
                   f"{_alias_text(layer.inventory_aliases)}; those "
                   "ICPSR archive copies are identification and comparison context only, never a pipeline input.",
    }
    if layer.is_table:
        manifest["unsupported_output_formats"] = {
            "geoparquet": "The producer publishes a nonspatial table with no geometry; the geometry-free "
                          "candidate is staged under unknown/ and no geometry-bearing published mapping is approved.",
            "shapefile_zip": "No approved source-to-public Shapefile field mapping.",
            "geopackage": "No approved source-to-public schema mapping.",
            "pmtiles": "The source has no geometry; no display transformation or generalized geometry is approved."}
    else:
        manifest["prebuilt_output_policy"] = {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls, and geometry retained in the "
                      "producer CRS; all release and compatibility approvals remain pending.",
            "objects": [{"path": rel, **parquet}]}}}
        manifest["unsupported_output_formats"] = {
            "shapefile_zip": "No approved source-to-public Shapefile field mapping.",
            "geopackage": "No approved source-to-public schema mapping.",
            "pmtiles": "No display transformation or generalized geometry approved."}
    return manifest


def _stage_candidate(storage: StagingStorageResource, layer: NfhlLayer, root: Path, accepted: list[str],
                     result: dict, parquet_path: Path) -> dict[str, object]:
    identity = processing_identity(REPO_ROOT)
    identity["gdal"] = pyogrio.__gdal_version_string__
    record_sha, schema_sha = result["source_record_sha256"], result["source_schema_sha256"]
    version = content_derived_version(record_sha, result["source_geometry_sha256"], schema_sha, identity)
    prefix = candidate_prefix(layer.slug, layer.slug, version)
    state_key = f"_construction-state/{layer.slug}/{version}.json"
    fingerprint = sha(canonical(identity))
    geometry = result["geometry"] or {}
    summary = {"dataset_slug": layer.slug, "file_slug": layer.slug, "version": version,
               "feature_count": result["feature_count"], "source_count": result["witness"]["count"],
               **{key: value for key, value in geometry.items() if key.endswith("_count")},
               "candidate_ready": prefix + "/metadata/candidate_ready.json", **APPROVAL_FLAGS}
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("cached_first_candidate_invalid")
        verify_cached_candidate(storage, REPO_ROOT, state_key, prefix, version, fingerprint, identity, record_sha,
                                schema_sha, payload.get("objects", {}), payload.get("capture_receipts", {}))
        return {**summary, "reused": True}
    receipts = {name: json.loads((root / f"{name}.receipt.json").read_bytes()) for name in accepted}
    rel = f"{'unknown' if layer.is_table else 'geoparquet'}/{layer.slug}.parquet"
    parquet = {"bytes": parquet_path.stat().st_size, "sha256": sha256_file(parquet_path)}
    manifest = _manifest(layer, identity, receipts, rel, parquet)
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {"source_feature_count": result["feature_count"], "source_count_witness": result["witness"],
             "source_record_sha256": record_sha, "source_geometry_sha256": result["source_geometry_sha256"],
             "source_schema_sha256": schema_sha, "source_crs_epsg": None if layer.is_table else SOURCE_CRS_WKID,
             "geometry_kind": layer.geometry, "geometry": result["geometry"], "object_id_field": result["oid_field"],
             "declared_layer_extent": result["layer_doc"].get("extent"),
             "source_declared_zm": {"hasZ": result["layer_doc"].get("hasZ") is True,
                                    "hasM": result["layer_doc"].get("hasM") is True},
             "unique_source_id_count": result["feature_count"], "page_count": result["page_count"],
             "source_fields": result["source_fields"], "official_layer_name": layer.name,
             "layer_id": layer.layer_id, "service_url": SERVICE_URL, "source_release_date": None,
             "source_release_date_unknown_reason": _RELEASE_DATE_REASON, "release_date_unknown": True,
             **_READY_FLAGS}
    documents = {name: canonical(document) + b"\n" for name, document in (
        ("metadata/source_manifest.json", manifest), ("metadata/source_audit.json", audit),
        ("metadata/processing_identity.json", identity))}
    objects = {rel: _stage(storage, f"{prefix}/{rel}", path=parquet_path)}
    for name, body in documents.items():
        objects[name] = _stage(storage, f"{prefix}/{name}", body=body)
    for name in accepted:
        for suffix in (".body", ".wire", ".receipt.json"):
            objects[f"raw/{name}{suffix}"] = _stage(storage, f"{prefix}/raw/{name}{suffix}",
                                                    path=root / f"{name}{suffix}")
    ready = {"approved": False, "version": version, "objects": {rel: objects[rel]},
             "source_record_sha256": record_sha, **_READY_FLAGS}
    objects["metadata/candidate_ready.json"] = _stage(storage, f"{prefix}/metadata/candidate_ready.json",
                                                      body=canonical(ready) + b"\n")
    seal_ready_candidate(storage, state_key, version, fingerprint, {
        "processing_identity": identity, "source_record_sha256": record_sha, "source_schema_sha256": schema_sha,
        "capture_receipts": {name: receipts[name] for name in accepted if not name.startswith("page_")},
        "objects": objects})
    validate_construction_code_identity(REPO_ROOT, identity["code_commit"], identity["files"])
    return {**summary, "reused": False}
