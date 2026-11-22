"""Shared ArcGIS FeatureServer/MapServer capture, replay and encoding primitives.

One canonical copy of the checkpoint load/save, retained-capture read, refusal
witness, failed-attempt evidence and bounded-retry mechanics shared by the
segmented NHD/3DHP adapters, plus the budget-guarded fetch with bounded retry,
Esri polygon encoding and GeoParquet serialization the ArcGIS dataset adapters
share. The USACE IENC paged capture, retained-capture load, candidate-identity
digest, Esri polyline encoding and IENC GeoParquet serializer are owned by
``sources.usace_ienc_common`` and re-exported here. Every body is
byte-identical to the copy it replaces; per-dataset byte budgets and error
types are passed explicitly by each caller. Canonical JSON, sha256, the ArcGIS
CRS drift guard and polygon ring assembly delegate to the single
implementations in ``sources.esri_common`` and ``sources.esri_geometry``.
The USACE IENC layer capture roster is shared too. Roster capture can validate
the producer's own before/after count and ID witnesses so completeness comes
from the source at capture time. Fetch planning, other roster/offset
algorithms, checkpoint state layouts, schema, ID and geometry contracts stay
local to each adapter.

The private aliases at the bottom keep the historical import names working so
each dataset module only changes its import line; new modules should use the
public names.
"""
from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator, Mapping, Sequence
from urllib.parse import urlencode, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj

from dagster_hifld.capture_completeness import validate_capture_witness
from dagster_hifld.http_capture import capture_http_response, validate_capture_roster, validate_retained_capture_replay
from dagster_hifld.source_release import ReleaseSignal
from dagster_hifld.sources.capture_common import capture_get, write_immutable
from dagster_hifld.sources.esri_common import arrow_type, canonical as _canonical, sha as _sha
from dagster_hifld.sources.esri_geometry import (
    _validate_spatial_reference, esri_polygon_wkb as _shared_esri_polygon_wkb,
)
from dagster_hifld.sources.usace_ienc_common import (
    PAGE_MAX_BYTES, _arrow_type, capture_identity, capture_page, esri_polyline_wkb,
    load_capture, polyline_parquet_bytes,
)


def layer_capture_roster(
    object_ids: Sequence[int], *,
    layer_url: str, service_url: str, page_size: int,
    extra_documents: Sequence[tuple[str, str]] = (),
    trailing_documents: Sequence[tuple[str, str]] = (),
    service_format: str = "pjson", layer_format: str = "pjson",
    page_params: Mapping[str, object] | None = None,
    page_namer: Callable[[int, Sequence[int]], str] | None = None,
    oid_field: str = "OBJECTID", spatial: bool = True,
    include_ids_before: bool = True,
    after_order: Sequence[str] = ("layer_after", "count_after", "ids_after", "service_after"),
) -> list[tuple[str, str]]:
    """Ordered discovery, page and witness requests for one layer capture.

    Emits the URL strings the per-adapter ``_capture_roster`` copies build:
    optional leading documents, the service and layer documents, the
    ``returnCountOnly`` and ``returnIdsOnly`` discovery queries, one exact-ID
    slice page per ``page_size`` IDs, the after witnesses, and optional trailing
    documents. Callers pass their own URL identity, page options and byte
    budgets. Parameter order is fixed so retained receipts stay byte-identical.
    ``after_order`` sets the witness order and must contain the four witness
    names once each.
    """
    if page_size < 1:
        raise ValueError("page_size_invalid")
    witness_names = ("layer_after", "count_after", "ids_after", "service_after")
    if sorted(after_order) != sorted(witness_names):
        raise ValueError("after_order_invalid")

    def query(params: Mapping[str, object]) -> str:
        return layer_url + "/query?" + urlencode(params)

    urls = list(extra_documents)
    urls.append(("service_before", service_url + f"?f={service_format}"))
    urls.append(("layer_before", layer_url + f"?f={layer_format}"))
    urls.append(("count_before", query({"where": "1=1", "returnCountOnly": "true", "f": "json"})))
    if include_ids_before:
        urls.append(("ids_before", query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})))
    for index, start in enumerate(range(0, len(object_ids), page_size)):
        page_ids = list(object_ids[start:start + page_size])
        name = page_namer(index, page_ids) if page_namer is not None else f"page_{index:03d}"
        params: dict[str, object] = {
            "objectIds": ",".join(map(str, page_ids)), "outFields": "*",
        }
        if spatial:
            params["returnGeometry"] = "true"
        if page_params:
            params.update(page_params)
        params.update({"orderByFields": f"{oid_field} ASC", "f": "json"})
        urls.append((name, query(params)))
    witnesses = {
        "layer_after": ("layer_after", layer_url + f"?f={layer_format}"),
        "count_after": ("count_after", query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        "ids_after": ("ids_after", query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
        "service_after": ("service_after", service_url + f"?f={service_format}"),
    }
    urls.extend(witnesses[name] for name in after_order)
    urls.extend(trailing_documents)
    return urls


def offset_layer_capture_roster(
    count: int, *,
    layer_url: str, service_url: str, page_size: int,
    extra_documents: Sequence[tuple[str, str]] = (),
    trailing_documents: Sequence[tuple[str, str]] = (),
    service_format: str = "pjson", layer_format: str = "pjson",
    page_params: Mapping[str, object] | None = None,
    page_namer: Callable[[int, int], str] | None = None,
    oid_field: str = "OBJECTID", spatial: bool = True,
    include_ids_before: bool = True, short_last_page: bool = True,
    after_order: Sequence[str] = ("layer_after", "count_after", "ids_after", "service_after"),
) -> list[tuple[str, str]]:
    """Ordered discovery, offset-page and witness requests for one layer capture.

    Same shape as :func:`layer_capture_roster`, but pages with
    ``resultOffset``/``resultRecordCount`` instead of exact-ID slices, for the
    adapters that must keep offset paging. ``short_last_page`` trims the final
    request to the remaining count; ``after_order`` sets the witness order and
    must contain the four witness names once each.
    """
    if count < 0 or page_size < 1:
        raise ValueError("page_count_or_size_invalid")
    witness_names = ("layer_after", "count_after", "ids_after", "service_after")
    if sorted(after_order) != sorted(witness_names):
        raise ValueError("after_order_invalid")

    def query(params: Mapping[str, object]) -> str:
        return layer_url + "/query?" + urlencode(params)

    urls = list(extra_documents)
    urls.append(("service_before", service_url + f"?f={service_format}"))
    urls.append(("layer_before", layer_url + f"?f={layer_format}"))
    urls.append(("count_before", query({"where": "1=1", "returnCountOnly": "true", "f": "json"})))
    if include_ids_before:
        urls.append(("ids_before", query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})))
    for index, offset in enumerate(range(0, count, page_size)):
        name = page_namer(index, offset) if page_namer is not None else f"page_{index:03d}"
        params: dict[str, object] = {"where": "1=1", "outFields": "*"}
        if spatial:
            params["returnGeometry"] = "true"
        if page_params:
            params.update(page_params)
        params.update({
            "orderByFields": f"{oid_field} ASC", "resultOffset": offset,
            "resultRecordCount": min(page_size, count - offset) if short_last_page else page_size,
            "f": "json",
        })
        urls.append((name, query(params)))
    witnesses = {
        "layer_after": ("layer_after", layer_url + f"?f={layer_format}"),
        "count_after": ("count_after", query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        "ids_after": ("ids_after", query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
        "service_after": ("service_after", service_url + f"?f={service_format}"),
    }
    urls.extend(witnesses[name] for name in after_order)
    urls.extend(trailing_documents)
    return urls


def ienc_layer_capture_roster(ids: list[int], *, bts_item_url: str, service_directory_url: str,
                              service_url: str, layer_url: str, page_size: int,
                              layer_format: str = "pjson") -> list[tuple[str, str]]:
    """Return the ordered USACE IENC layer capture roster for a fixed ID list."""
    return layer_capture_roster(
        ids, layer_url=layer_url, service_url=service_url, page_size=page_size,
        extra_documents=(("bts_item", bts_item_url),
                         ("service_directory", service_directory_url)),
        layer_format=layer_format,
        page_params={"returnZ": "true", "returnM": "true", "outSR": "4326"},
    )


def capture_witness(captures: Mapping[str, tuple[bytes, dict]], *,
                    captured_ids: Sequence[int] | None = None) -> list[int]:
    """Validate the source count/ID witnesses retained around one layer capture.

    :func:`layer_capture_roster` and :func:`offset_layer_capture_roster` emit the
    producer's own ``count_before``, ``ids_before``, ``count_after`` and
    ``ids_after`` requests. This decodes those retained bodies and calls
    :func:`validate_capture_witness`, so completeness comes from the source's
    count at capture time rather than a count pinned in code. When
    ``captured_ids`` is supplied the captured pages must cover that roster
    exactly. A complete upstream addition or removal passes; a torn,
    duplicated or inconsistent capture is refused.
    """
    witness_names = ("count_before", "ids_before", "count_after", "ids_after")
    missing = [name for name in witness_names if name not in captures]
    if missing:
        raise ValueError("source_capture_witness_missing:" + ",".join(missing))
    docs = {}
    for name in witness_names:
        value = captures[name]
        body = value[0] if isinstance(value, tuple) else value
        docs[name] = json.loads(body)
    return validate_capture_witness(
        docs["count_before"].get("count"), docs["ids_before"].get("objectIds"),
        docs["count_after"].get("count"), docs["ids_after"].get("objectIds"),
        None if captured_ids is None else list(captured_ids))


def capture_layer_roster(client: httpx.Client, root: Path,
                        roster: Iterable[tuple[str, str]],
                        fetch: Callable[[httpx.Client, str, Path, str], tuple[bytes, dict]],
                        *, captured_ids: Sequence[int] | None = None,
                        ) -> dict[str, tuple[bytes, dict]]:
    """Capture one ordered layer roster and validate its source witnesses.

    ``roster`` is the ordered output of :func:`layer_capture_roster` or
    :func:`offset_layer_capture_roster`; ``fetch`` is the adapter's bounded
    capture callable. Completeness is enforced from the source's own before and
    after count and ID roster, not a count pinned in code.
    """
    roster = list(roster)
    names = [name for name, _url in roster]
    if len(set(names)) != len(names):
        raise ValueError("capture_roster_name_duplicate")
    captures = {name: fetch(client, url, root, name) for name, url in roster}
    capture_witness(captures, captured_ids=captured_ids)
    return captures


def retained_capture_ids(root: Path) -> list[int]:
    """Read the native ID roster retained in a capture's ``ids_before`` body."""
    path = root / "ids_before.body"
    if path.is_symlink():
        raise ValueError("retained_capture_ids_symlink_rejected")
    try:
        ids = json.loads(path.read_bytes()).get("objectIds")
    except (OSError, ValueError) as exc:
        raise ValueError("retained_capture_ids_invalid") from exc
    if (not isinstance(ids, list) or not ids
            or any(type(oid) is not int or oid <= 0 for oid in ids)
            or len(set(ids)) != len(ids)):
        raise ValueError("retained_capture_ids_invalid")
    return sorted(ids)


def read_ienc_layer_replay(root: Path, *, capture_roster: Callable[[list[int]], Iterable[tuple[str, str]]],
                           load_capture: Callable[[Path, str], tuple[bytes, bytes, dict]],
                           validate_request_identity: Callable[[str, Mapping[str, object], str], None],
                           ) -> dict[str, tuple[bytes, dict]]:
    """Replay a retained IENC capture with page names derived from its source IDs."""
    ids = retained_capture_ids(root)
    urls = dict(capture_roster(ids))
    return validate_retained_capture_replay(
        root, capture_names=set(urls), expected_urls=urls,
        load_capture=load_capture, validate_request_identity=validate_request_identity)


def esri_polygon(geometry: dict) -> bytes:
    """Encode an ArcGIS polygon, preserving XY/Z/M and rejecting ambiguous rings."""
    return _shared_esri_polygon_wkb(geometry)


def esri_polygon_wkb(geometry: dict) -> bytes:
    """Encode an XY-only ArcGIS polygon, rejecting flagged Z/M geometry."""
    return _shared_esri_polygon_wkb(geometry, allow_zm=False)


def layer_parquet_bytes(fields: list[dict], features: list[dict], wkid: int) -> bytes:
    properties = [f for f in fields if f["name"] != "SHAPE"]
    arrays = []
    for field in properties:
        name, dtype = field["name"], arrow_type(field)
        values = [feature["attributes"][name] for feature in features]
        arrays.append(pa.array(values, type=dtype))
    geometries = [esri_polygon(x["geometry"]) for x in features]
    arrays.append(pa.array(geometries, type=pa.binary()))
    names = [x["name"] for x in properties] + ["geometry"]
    table = pa.Table.from_arrays(arrays, names=names)
    crs = {"$schema": "https://proj.org/schemas/v0.7/projjson.schema.json", "type": "GeographicCRS",
           "name": "WGS 84", "datum": {"type": "GeodeticReferenceFrame", "name": "World Geodetic System 1984",
           "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
           "id": {"authority": "EPSG", "code": 6326}},
           "coordinate_system": {"subtype": "ellipsoidal", "axis": [{"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"}, {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"}]},
           "id": {"authority": "EPSG", "code": wkid}}
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["MultiPolygon", "Polygon"], "crs": crs}}})}
    import io

    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def load_replay(root: Path, name: str) -> tuple[bytes, dict]:
    paths = [root / f"{name}.body", root / f"{name}.wire", root / f"{name}.receipt.json"]
    if any(path.is_symlink() for path in paths):
        raise ValueError("retained_capture_symlink_rejected")
    body = paths[0].read_bytes()
    wire = paths[1].read_bytes()
    receipt = json.loads(paths[2].read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("retained_capture_receipt_invalid:" + name)
    headers = {"Content-Encoding": receipt.get("content_encoding") or "identity"}
    if receipt.get("content_length") is not None:
        headers["Content-Length"] = str(receipt["content_length"])
    response = httpx.Response(200, headers=headers, stream=httpx.ByteStream(wire))
    result = capture_http_response(response, max_wire_bytes=len(wire), max_decoded_bytes=len(body),
        apply_retention_budget=False, reserve=lambda _progress: None, retain_wire=lambda _chunk: None,
        retain_decoded=lambda _chunk: None)
    response.close()
    if (not result.complete or result.decoded_bytes_retained != len(body)
            or result.decoded_sha256 != _sha(body)):
        raise ValueError("retained_capture_wire_body_mismatch:" + name)
    return body, receipt


def read_adaptive_offset_replay(root: Path, *, document_urls: Mapping[str, str],
                                page_size: int, page_name: Callable[[int, int], str],
                                parse_page_name: Callable[[str], tuple[int, int]],
                                page_query: Callable[[int, int], str], max_capture_bytes: int,
                                max_capture_total_bytes: int, doc_capture_attempts: int,
                                page_probe_attempts: int) -> dict[str, tuple[bytes, dict[str, object]]]:
    """Replay adaptive offset captures with bounded, request-bound refusal custody."""
    if any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError('retained_capture_symlink_rejected')
    manifest_path = root / 'capture_manifest.json'
    if manifest_path.is_symlink() or manifest_path.stat().st_size > max_capture_bytes:
        raise ValueError('retained_capture_manifest_invalid')
    entries = list(root.iterdir())
    validate_capture_roster(root, {path.name for path in entries})
    sizes = [path.stat().st_size for path in entries]
    if any(size > max_capture_bytes for size in sizes) or sum(sizes) > max_capture_total_bytes:
        raise ValueError('source_capture_exceeds_scratch_budget')
    manifest = json.loads(manifest_path.read_bytes())
    if (not isinstance(manifest, dict) or manifest.get('complete') is not True
            or not isinstance(manifest.get('receipts'), dict)
            or not isinstance(manifest.get('artifact_roster'), dict)):
        raise ValueError('retained_capture_manifest_invalid')
    urls = dict(document_urls)
    names = set(manifest['receipts'])
    if not set(urls) <= names:
        raise ValueError('source_capture_roster_incomplete_or_unexpected')
    pages = {}
    for name in names - set(urls):
        offset, count = parse_page_name(name)
        if not 0 < count <= page_size:
            raise ValueError('source_capture_roster_incomplete_or_unexpected')
        pages[name] = (offset, count)
        urls[name] = page_query(offset, count)
    total = sum(count for offset, count in pages.values())
    evidence_names = set(urls)
    for offset, count in pages.values():
        parent_offset = offset // page_size * page_size
        parent_count = min(page_size, total - parent_offset)
        while True:
            if parent_count <= 0 or offset < parent_offset or offset + count > parent_offset + parent_count:
                raise ValueError('source_capture_roster_incomplete_or_unexpected')
            evidence_names.add(page_name(parent_offset, parent_count))
            if (offset, count) == (parent_offset, parent_count):
                break
            middle = parent_count // 2
            if middle == 0:
                raise ValueError('source_capture_roster_incomplete_or_unexpected')
            if offset < parent_offset + middle:
                parent_count = middle
            else:
                parent_offset += middle
                parent_count -= middle
    allowed_artifacts = set()
    for name in evidence_names:
        attempts = page_probe_attempts if name.startswith('page_') and parse_page_name(name)[1] > 1 else doc_capture_attempts
        stems = [name, *(f'{name}.attempt-{attempt:02d}' for attempt in range(1, attempts))]
        allowed_artifacts.update(stem + suffix for stem in stems for suffix in ('.body', '.wire', '.receipt.json'))
    if set(manifest['artifact_roster']) - allowed_artifacts:
        raise ValueError('retained_capture_artifact_name_invalid')
    validate_capture_roster(root, set(manifest['artifact_roster']) | {'capture_manifest.json'})

    def load(directory, name):
        body, receipt = load_replay(directory, name)
        return body, (directory / f'{name}.wire').read_bytes(), receipt

    def validate_request(name, receipt, url):
        if receipt.get('method') != 'GET' or receipt.get('url') != url or receipt.get('final_url') != url:
            raise ValueError('retained_capture_request_mismatch:' + name)

    captures = validate_retained_capture_replay(root, capture_names=names, expected_urls=urls,
        load_capture=load, validate_request_identity=validate_request)
    for name in evidence_names:
        attempts = page_probe_attempts if name.startswith('page_') and parse_page_name(name)[1] > 1 else doc_capture_attempts
        retained_attempts = [attempt for attempt in range(1, attempts)
            if any(f'{name}.attempt-{attempt:02d}{suffix}' in manifest['artifact_roster']
                   for suffix in ('.body', '.wire', '.receipt.json'))]
        last_attempt = attempts - 1 if name not in names else max(retained_attempts, default=0)
        refusals = [f'{name}.attempt-{attempt:02d}' for attempt in range(1, last_attempt + 1)]
        if name not in names:
            refusals.append(name)
        for refusal in refusals:
            if any(refusal + suffix not in manifest['artifact_roster'] for suffix in ('.body', '.wire', '.receipt.json')):
                raise ValueError('retained_capture_refusal_missing:' + refusal)
            url = urls[name] if name in urls else page_query(*parse_page_name(name))
            _validate_offset_replay_refusal(root, refusal, url, max_capture_bytes=max_capture_bytes)
    capture_witness(captures)
    return captures


def _validate_offset_replay_refusal(root: Path, name: str, url: str, *, max_capture_bytes: int) -> None:
    receipt = json.loads((root / f'{name}.receipt.json').read_bytes())
    if (not isinstance(receipt, dict) or receipt.get('method') != 'GET' or receipt.get('url') != url
            or type(receipt.get('complete_body')) is not bool):
        raise ValueError('retained_capture_refusal_invalid:' + name)
    if receipt['complete_body'] and receipt.get('status') == 200:
        body, receipt = load_replay(root, name)
        if (receipt.get('final_url') != url or receipt.get('error_code') is not None
                or receipt.get('error_type') is not None or not producer_error_doc(body)):
            raise ValueError('retained_capture_refusal_invalid:' + name)
        return
    body, wire = (root / f'{name}.body').read_bytes(), (root / f'{name}.wire').read_bytes()
    if (receipt.get('bytes') != len(body) or receipt.get('sha256') != _sha(body)
            or receipt.get('wire_bytes') != len(wire) or receipt.get('wire_sha256') != _sha(wire)):
        raise ValueError('retained_capture_refusal_invalid:' + name)
    if receipt.get('error_code') == 'request_error':
        if (receipt['complete_body'] or receipt.get('status') is not None or receipt.get('final_url') is not None
                or body or wire or receipt.get('content_encoding') is not None
                or receipt.get('content_length') is not None or not isinstance(receipt.get('error_type'), str)):
            raise ValueError('retained_capture_refusal_invalid:' + name)
        return
    if (receipt.get('final_url') != url or type(receipt.get('status')) is not int
            or not 100 <= receipt['status'] <= 599
            or not isinstance(receipt.get('content_encoding'), str)
            or (receipt.get('content_length') is not None
                and (type(receipt['content_length']) is not int or receipt['content_length'] < 0))
            or (not receipt['complete_body'] and not isinstance(receipt.get('error_code'), str))):
        raise ValueError('retained_capture_refusal_invalid:' + name)
    if receipt.get('error_code') == 'invalid_content_length':
        if (receipt['complete_body'] or body or wire or receipt.get('content_length') is not None
                or receipt.get('error_type') is not None or receipt['content_encoding'] not in {'identity', 'gzip', 'deflate'}):
            raise ValueError('retained_capture_refusal_invalid:' + name)
        return
    headers = {'Content-Encoding': receipt.get('content_encoding') or 'identity'}
    if receipt.get('content_length') is not None:
        headers['Content-Length'] = str(receipt['content_length'])
    response = httpx.Response(receipt['status'], headers=headers, stream=httpx.ByteStream(wire))
    result = capture_http_response(response, max_wire_bytes=max_capture_bytes, max_decoded_bytes=max_capture_bytes,
        apply_retention_budget=False, reserve=lambda _progress: None, retain_wire=lambda _chunk: None, retain_decoded=lambda _chunk: None)
    response.close()
    if (result.wire_bytes_retained != len(wire) or result.wire_sha256 != _sha(wire)
            or result.decoded_bytes_retained != len(body) or result.decoded_sha256 != _sha(body)
            or (receipt['complete_body'] and (not result.complete or receipt.get('error_code') is not None
                or receipt.get('error_type') is not None))
            or (not receipt['complete_body'] and receipt.get('error_code') != 'transport_error'
                and (result.complete or result.error_code != receipt['error_code']))
            or (receipt.get('error_code') == 'transport_error'
                and (not isinstance(receipt.get('error_type'), str)
                     or result.error_code not in {None, 'compressed_stream_incomplete', 'content_length_mismatch'}))):
        raise ValueError('retained_capture_refusal_invalid:' + name)


def write_outputs(storage, prefix: str, outputs: dict[str, bytes]) -> dict[str, dict[str, object]]:
    descriptors = {}
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
        snapshot = storage.object_snapshot(f"{prefix}/{name}")
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if (snapshot is None or snapshot.size != descriptor["bytes"]
                or storage.sha256_key(f"{prefix}/{name}") != descriptor["sha256"]
                or storage.object_snapshot(f"{prefix}/{name}") != snapshot):
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    return descriptors


def validate_crs(ref: object) -> None:
    if not isinstance(ref, dict):
        raise ValueError("source_crs_drift")
    codes = [ref[key] for key in ("wkid", "latestWkid") if key in ref]
    if not codes or any(type(code) is not int or code != 4326 for code in codes):
        raise ValueError("source_crs_drift")
    for key in ("wkt", "wkt2"):
        if key in ref:
            try:
                same_crs = pyproj.CRS.from_wkt(ref[key]).equals(pyproj.CRS.from_epsg(4326), ignore_axis_order=True)
            except Exception as exc:
                raise ValueError("source_crs_drift") from exc
            if not same_crs:
                raise ValueError("source_crs_drift")


def validate_dimensions(declaration: dict) -> None:
    for name in ("hasZ", "hasM"):
        if name in declaration and (type(declaration[name]) is not bool or declaration[name]):
            raise ValueError("source_dimension_drift")


def fetch_capture(client: httpx.Client, url: str, root: Path, name: str, budget: list[int], *,
                  max_capture_bytes: int, max_body_bytes: int,
                  prefill_empty_artifacts: bool = False,
                  allow_arcgis_item_redirect: bool = False) -> tuple[bytes, dict]:
    """Capture one IENC GET to disk under a running ``budget`` list.

    The body is byte-identical to the per-dataset ``_fetch`` copies it replaces.
    ``prefill_empty_artifacts`` pre-creates the body/wire artifacts so a valid
    zero-byte response still verifies against its receipt (the streaming
    callbacks are not invoked for it). ``allow_arcgis_item_redirect`` admits
    only https://www.arcgis.com/itemdata/ redirect targets.
    """
    root.mkdir(parents=True, exist_ok=True)
    body_path, wire_path = root / f"{name}.body", root / f"{name}.wire"
    for path in (body_path, wire_path):
        path.unlink(missing_ok=True)
        if prefill_empty_artifacts:
            path.write_bytes(b"")
    hashes = {"body": hashlib.sha256(), "wire": hashlib.sha256()}
    sizes = {"body": 0, "wire": 0}
    budget_start = budget[0]
    observed = datetime.now(timezone.utc).isoformat()

    def retain(path, kind):
        def write(chunk):
            with path.open("ab") as stream:
                stream.write(chunk)
            hashes[kind].update(chunk); sizes[kind] += len(chunk)
            budget[0] = budget_start + sizes["wire"] + sizes["body"]
        return write

    def reserve(progress):
        if budget_start + progress.wire_bytes_retained + progress.decoded_bytes_retained + progress.reserve_bytes > max_capture_bytes:
            raise ValueError("source_capture_budget_exceeded")

    response = None
    result = None
    final_url = url
    transport_error = None
    try:
        with client.stream("GET", url, follow_redirects=allow_arcgis_item_redirect,
                           headers={"Accept-Encoding": "identity"}) as response:
            final_url = str(response.url)
            if allow_arcgis_item_redirect and final_url != url:
                parsed = urlsplit(final_url)
                if parsed.scheme != "https" or parsed.netloc != "www.arcgis.com" or not parsed.path.startswith("/itemdata/"):
                    raise ValueError("unexpected_arcgis_pdf_redirect_target")
                final_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            result = capture_http_response(response, max_wire_bytes=max_body_bytes,
                max_decoded_bytes=max_body_bytes, reserve=reserve,
                retain_wire=retain(wire_path, "wire"), retain_decoded=retain(body_path, "body"))
    except Exception as exc:
        transport_error = type(exc).__name__
    receipt = {"method": "GET", "url": url, "final_url": final_url, "observed_at": observed,
        "status": result.status_code if result is not None else (response.status_code if response is not None else None),
        "complete_body": bool(result is not None and result.complete and transport_error is None),
        "wire_bytes": sizes["wire"], "wire_sha256": hashes["wire"].hexdigest(),
        "bytes": sizes["body"], "sha256": hashes["body"].hexdigest(),
        "content_encoding": result.content_encoding if result is not None else None,
        "content_length": result.content_length if result is not None else None,
        "error_code": result.error_code if result is not None else ("transport_error" if transport_error else "capture_result_missing"),
        "error_type": result.error_type if result is not None else transport_error}
    (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    if transport_error:
        raise httpx.TransportError("capture_transport_error:" + transport_error)
    if result is None:
        raise ValueError("capture_result_missing")
    body, wire = body_path.read_bytes(), wire_path.read_bytes()
    if (not result.complete or result.status_code != 200
        or (not allow_arcgis_item_redirect and final_url != url)
        or len(body) != receipt["bytes"] or _sha(body) != receipt["sha256"]
        or len(wire) != receipt["wire_bytes"] or _sha(wire) != receipt["wire_sha256"]):
        raise ValueError("incomplete_source_capture:" + name)
    return body, receipt


def fetch_capture_retry(client: httpx.Client, url: str, root: Path, name: str, budget: list[int], *,
                        max_capture_bytes: int, max_body_bytes: int,
                        attempts: int = 2, schedule: tuple[float, ...] = (),
                        prefill_empty_artifacts: bool = False,
                        allow_arcgis_item_redirect: bool = False) -> tuple[bytes, dict]:
    """Retry transient transport/incomplete reads, retaining every attempt.

    The loop is byte-identical to the per-dataset ``_fetch_retry`` copies it
    replaces: ``schedule[i]`` sleeps before attempt ``i`` when present, each
    attempt is retained under ``<name>.attempt<N>`` evidence names, a successful
    attempt's artifacts are promoted to the final names, and non-transient
    ValueError failures re-raise immediately.
    """
    last = None
    for attempt in range(attempts):
        wait = schedule[attempt] if attempt < len(schedule) else 0.0
        if wait:
            time.sleep(wait)
        attempt_name = f"{name}.attempt{attempt + 1}"
        try:
            body, receipt = fetch_capture(client, url, root, attempt_name, budget,
                max_capture_bytes=max_capture_bytes, max_body_bytes=max_body_bytes,
                prefill_empty_artifacts=prefill_empty_artifacts,
                allow_arcgis_item_redirect=allow_arcgis_item_redirect)
            for suffix in ("body", "wire", "receipt.json"):
                (root / f"{attempt_name}.{suffix}").replace(root / f"{name}.{suffix}")
            return body, receipt
        except (httpx.TransportError, ValueError) as exc:
            last = exc
            if isinstance(exc, ValueError) and not str(exc).startswith("incomplete_source_capture:"):
                raise
    raise last


def load_receipt(capture_root: Path, name: str, error: type[ValueError]) -> dict[str, object]:
    """Parse and structurally validate one retained receipt without page bytes."""
    try:
        receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise error("retained_capture_receipt_invalid:" + name) from exc
    if (not isinstance(receipt, dict) or receipt.get("method") != "GET"
            or receipt.get("url") != receipt.get("final_url")
            or receipt.get("complete_body") is not True or receipt.get("status") != 200):
        raise error("retained_capture_receipt_invalid:" + name)
    return receipt


def read_capture(capture_root: Path, name: str,
                 error: type[ValueError]) -> tuple[bytes, dict[str, object]]:
    body, wire = ((capture_root / f"{name}.body").read_bytes(),
                  (capture_root / f"{name}.wire").read_bytes())
    receipt = load_receipt(capture_root, name, error)
    if (receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise error("retained_capture_receipt_invalid:" + name)
    return body, receipt


def load_state(capture_root: Path, error: type[ValueError]) -> dict[str, object]:
    path = capture_root / "capture_state.json"
    if not path.exists():
        return {"segments": [], "complete": False}
    try:
        state = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise error("capture_state_invalid") from exc
    if not isinstance(state, dict) or not isinstance(state.get("segments"), list):
        raise error("capture_state_invalid")
    return state


def save_state(capture_root: Path, state: Mapping[str, object]) -> None:
    tmp = capture_root / "capture_state.json.tmp"
    tmp.write_bytes(_canonical(dict(state)) + b"\n")
    tmp.replace(capture_root / "capture_state.json")


def witness_aside(capture_root: Path, name: str) -> None:
    """Move a retained refused/torn response aside so the slot can be refetched.

    A complete HTTP 200 error body carries a self-consistent receipt, so
    without this witness step a resumed capture would replay the same refusal
    forever; the witnessed evidence stays on disk next to the capture.
    """
    from time import time_ns
    stamp = time_ns()
    for suffix in ("body", "wire", "receipt.json"):
        source = capture_root / f"{name}.{suffix}"
        if source.exists():
            source.replace(capture_root / f"{name}.error-{stamp}.{suffix}")


def producer_error_doc(body: bytes, *, max_bytes: int = 65536,
                       require_int_code: bool = False) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > max_bytes:
        return False
    try:
        document = json.loads(body)
    except ValueError:
        return False
    if not isinstance(document, dict) or "error" not in document:
        return False
    if require_int_code:
        envelope = document["error"]
        return isinstance(envelope, dict) and type(envelope.get("code")) is int
    return True


def preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    suffixes = (".body", ".wire", ".receipt.json")
    while any((capture_root / f"{name}.attempt-{attempt:02d}{suffix}").exists()
              for suffix in suffixes):
        attempt += 1
    for suffix in suffixes:
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.replace(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                       attempts: int, base_delay: float, max_bytes: int,
                       max_total_bytes: int | None = None,
                       budget_error: type[ValueError] = ValueError,
                       reserve_all_attempts: bool = False,
                       preserve_final_failure: bool = False,
                       retryable_statuses: tuple[int, ...] | None = None,
                       require_json: bool = False,
                       return_final_error: bool = True,
                       schedule: tuple[float, ...] = (),
                       capture: Callable[..., tuple[bytes, dict]] | None = None) -> tuple[bytes, dict[str, object]]:
    """Own bounded producer retries and retain all failed response evidence.

    Family policy controls reservation, status refusal and final error return.
    Returning the final complete error body lets windowed families bisect it.
    Attempt names never overwrite evidence retained by a previous session.
    """
    if reserve_all_attempts and max_total_bytes is not None:
        retained = sum(p.stat().st_size for p in capture_root.iterdir()
                       if p.is_file() and p.name.endswith((".body", ".wire", ".receipt.json")))
        remaining = max_total_bytes - retained - attempts * 65536
        if remaining < 2 * attempts:
            raise budget_error("source_capture_exceeds_scratch_budget")
        max_bytes = min(max_bytes, remaining // (2 * attempts))
    for attempt in range(1, attempts + 1):
        if max_total_bytes is not None and not reserve_all_attempts:
            retained = sum(p.stat().st_size for p in capture_root.iterdir()
                           if p.is_file() and p.name.endswith((".body", ".wire")))
            if retained + 2 * max_bytes > max_total_bytes:
                raise budget_error("source_capture_budget_exceeded_before_request")
        try:
            body, receipt = (capture or capture_get)(client, url, capture_root, name, max_bytes=max_bytes)
        except ValueError as exc:
            if not str(exc).startswith("incomplete_source_capture:"):
                raise
            if retryable_statuses is not None:
                try:
                    status = json.loads((capture_root / f"{name}.receipt.json").read_bytes()).get("status")
                except (OSError, ValueError):
                    status = None
                if status is not None and status not in retryable_statuses:
                    raise
            if attempt >= attempts:
                if preserve_final_failure:
                    preserve_failed_attempt(capture_root, name, attempt)
                if return_final_error:
                    raise
                raise ValueError("source_endpoint_retries_exhausted:" + name) from exc
        else:
            if require_json:
                document = json.loads(body)
                is_error = isinstance(document, dict) and "error" in document
            else:
                is_error = producer_error_doc(body)
                document = json.loads(body) if is_error else None
            if not is_error:
                return body, receipt
            if retryable_statuses is not None:
                envelope = document.get("error")
                code = envelope.get("code") if isinstance(envelope, dict) else None
                if code is not None and code not in retryable_statuses:
                    raise ValueError(f"source_endpoint_error:{name}:{code}")
            if attempt >= attempts and return_final_error:
                return body, receipt
        preserve_failed_attempt(capture_root, name, attempt)
        if attempt >= attempts:
            raise ValueError("source_endpoint_retries_exhausted:" + name)
        wait = schedule[attempt - 1] if attempt <= len(schedule) else base_delay * 3 ** (attempt - 1)
        if wait:
            time.sleep(wait)
    raise AssertionError("unreachable_retry_state")


class RetainedCaptureViews(Mapping):
    """Lazily materialize one capture view per access so quarantine stays bounded."""

    def __init__(self, capture_root: Path, names: Iterable[str], error: type[ValueError]):
        self._capture_root = Path(capture_root)
        self._names = tuple(names)
        self._error = error

    def __getitem__(self, name: str) -> tuple[bytes, dict[str, object]]:
        return read_capture(self._capture_root, name, self._error)

    def __iter__(self) -> Iterator[str]:
        return iter(self._names)

    def __len__(self) -> int:
        return len(self._names)


def poll_layer_release(client: httpx.Client, *, service_url: str, layer_url: str,
                       capture_root: Path, extra_documents: Sequence[tuple[str, str]] = (),
                       service_format: str = "pjson", layer_format: str = "pjson",
                       max_bytes: int = PAGE_MAX_BYTES,
                       capture: Callable[..., tuple[bytes, dict]] = capture_get) -> dict[str, tuple[bytes, dict]]:
    """Capture only the cheap release state for one layer, never its pages.

    This is the same service/layer/count/ID discovery the full capture performs,
    so an unchanged poll can stop before paging or downloading any payload.
    """
    urls = list(extra_documents)
    urls.append(("service_before", service_url + f"?f={service_format}"))
    urls.append(("layer_before", layer_url + f"?f={layer_format}"))
    urls.append(("count_before", layer_url + "/query?" + urlencode(
        {"where": "1=1", "returnCountOnly": "true", "f": "json"})))
    urls.append(("ids_before", layer_url + "/query?" + urlencode(
        {"where": "1=1", "returnIdsOnly": "true", "f": "json"})))
    captures = {}
    for name, url in urls:
        captures[name] = capture(client, url, capture_root, name, max_bytes=max_bytes)
    return captures


def layer_release_signal(captures: Mapping[str, tuple[bytes, dict]], *,
                         selection_rule: str) -> ReleaseSignal:
    """Build the publisher release signal from discovery documents.

    ArcGIS exposes no edition label, so ``release_id``/``release_date`` stay
    unknown and a changed layer is treated as a same-release correction. The
    publisher's ``editingInfo.lastEditDate`` is the primary edit state; count,
    ID roster and field schema catch changes when it is absent.
    """
    try:
        service = json.loads(captures["service_before"][0])
        layer = json.loads(captures["layer_before"][0])
        count_doc = json.loads(captures["count_before"][0])
        ids_doc = json.loads(captures["ids_before"][0])
    except (KeyError, ValueError, TypeError) as exc:
        raise ValueError("source_release_discovery_incomplete") from exc
    if not isinstance(service, dict) or not isinstance(layer, dict):
        raise ValueError("source_release_discovery_incomplete")
    count = count_doc.get("count") if isinstance(count_doc, dict) else None
    ids = ids_doc.get("objectIds") if isinstance(ids_doc, dict) else None
    if (type(count) is not int or count < 0 or not isinstance(ids, list)
            or any(type(value) is not int for value in ids) or len(ids) != count
            or len(set(ids)) != len(ids)):
        raise ValueError("source_release_discovery_incomplete")
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(field, dict) for field in fields):
        raise ValueError("source_release_discovery_incomplete")
    plain_fields = [{key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
                     if key in field} for field in fields]
    editing = layer.get("editingInfo") if isinstance(layer.get("editingInfo"), dict) else {}
    last_edit = editing.get("lastEditDate")
    if last_edit is None and isinstance(service.get("editingInfo"), dict):
        last_edit = service["editingInfo"].get("lastEditDate")
    last_edit = None if last_edit is None else str(last_edit)
    schema_sha = _sha(_canonical(plain_fields))
    roster = sorted(ids)
    poll_fingerprint = _sha(_canonical({
        "count": count, "ids": roster, "schema_sha256": schema_sha,
        "last_edit": last_edit,
        "service_item_id": service.get("serviceItemId"),
        "layer": {"name": layer.get("name"), "geometryType": layer.get("geometryType"),
                  "objectIdField": layer.get("objectIdField")},
    }))
    observed = captures["layer_before"][1].get("observed_at")
    return ReleaseSignal(
        release_id=None, release_date=None, last_edit=last_edit,
        poll_fingerprint=poll_fingerprint, content_sha256=None,
        selection_rule=selection_rule,
        evidence={"count": count, "id_count": len(ids), "schema_sha256": schema_sha,
                  "last_edit": last_edit, "observed_at": observed},
    )


# Historical private import names: the dataset modules under sources/ still
# import these (formerly from sources.usace_obstruction_lines), so their import
# blocks and bodies stay unchanged. New modules should use the public names.
_capture = capture_page
_load_capture = load_capture
_capture_identity = capture_identity
_esri_polyline_wkb = esri_polyline_wkb
_parquet_bytes = polyline_parquet_bytes

# Public disk-backed acquisition boundary.
from dagster_hifld.sources.arcgis_capture import capture_layer, RetainedCaptureView
