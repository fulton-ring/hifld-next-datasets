"""Focused tests for the USGS US Landslide Regions draft pipeline."""
import gzip
import json
import os
import struct
import tempfile
import unittest
import zlib
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse, urlsplit

import httpx
import pyarrow.parquet as pq
import shapely

from dagster import AssetCheckSeverity
from dagster_hifld.sources import arcgis_adaptive
from dagster_hifld.assets.us_landslide_regions import (
    us_landslide_regions,
    us_landslide_regions_approved_baseline_drift,
    us_landslide_regions_complete,
)
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import capture_common
from dagster_hifld.sources import us_landslide_regions as src
from dagster_hifld.sources.esri_geometry import to_wkb

FIXTURE_COUNT = 8
FIXTURE_OBJECTIDS = (2, 3, 5, 8, 13, 21, 34, 55)
# Fixture stand-in for the layer document's declared extent; the source only
# requires a structurally valid extent in the native SR, not a pinned one.
FIXTURE_LAYER_EXTENT = {"xmin": -17_337_802.24197813, "ymin": 2_031_850.1690669723,
                        "xmax": -7_339_708.080696905, "ymax": 9_269_118.005589234}


def _shell(x: float, y: float) -> list[list[float]]:
    """Clockwise closed ring (ArcGIS exterior convention) with three ordinates."""
    return [[x, y, 0.0], [x, y + 10_000.0, 0.0], [x + 10_000.0, y + 10_000.0, 0.0],
            [x + 10_000.0, y, 0.0], [x, y, 0.0]]


def _hole(x: float, y: float) -> list[list[float]]:
    """Counter-clockwise closed ring inside the shell (ArcGIS interior convention)."""
    return [[x, y, 0.0], [x + 8_000.0, y, 0.0], [x + 8_000.0, y + 6_000.0, 0.0],
            [x, y + 6_000.0, 0.0], [x, y, 0.0]]


def _fixture_attrs(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    return {
        "OBJECTID": oid,
        "fid_1": 101.25 + index,
        "USGS_ID": f"ply{oid:06d}",
        "Date_Min": "2004-02-17" if index != 1 else None,
        "Date_Max": "2004-02-19",
        "Fatalities": 1.0 if index == 4 else None,
        "Confidence": 2,
        "LS_Type": "Debris Flow" if index % 2 == 0 else "Landslide",
        "Inventory": "Inventory A" if index != 3 else None,
        "Inv_URL": "https://ssl.wrds.usgs.gov/",
        "Info_Sourc": "USGS",
        "Notes": "synthetic fixture" if index != 5 else None,
        "Lat_N": 39.5 + index / 10,
        "Lon_W": -110.5 - index / 10,
        "Shape__Area": 10000.0 * (index + 1),
        "Shape__Length": 500.0 * (index + 1),
    }


def _fixture_geometry(oid: object) -> object:
    index = FIXTURE_OBJECTIDS.index(oid)
    if index == 3:
        return None
    if index == 0:
        rings = [_shell(-10_000_000.0, 4_000_000.0)]
    elif index == 1:
        rings = [_shell(-10_000_000.0, 4_000_000.0), _hole(-9_999_000.0, 4_002_000.0)]
    elif index == 2:
        rings = [_shell(-10_000_000.0, 4_000_000.0), _shell(-9_000_000.0, 3_000_000.0)]
    else:
        rings = [_shell(-10_000_000.0 - 1000.0 * index, 4_000_000.0)]
    return {"rings": rings, "hasZ": True}


def _fixture_rows() -> list[dict]:
    return [{"attributes": _fixture_attrs(oid), "geometry": _fixture_geometry(oid)}
            for oid in FIXTURE_OBJECTIDS]


def _fixture_layer_fields() -> list[dict]:
    fields = []
    for name, esri_type, length in src.NATIVE_FIELDS[:8]:
        field = {"name": name, "alias": name, "type": esri_type, "domain": None}
        if length is not None:
            field["length"] = length
        fields.append(field)
    fields.append({"name": "Shape", "alias": "Shape",
                   "type": "esriFieldTypeGeometry", "domain": None})
    for name, esri_type, length in src.NATIVE_FIELDS[8:]:
        field = {"name": name, "alias": name, "type": esri_type, "domain": None}
        if length is not None:
            field["length"] = length
        fields.append(field)
    return fields


def _fixture_layer_doc() -> dict:
    return {"id": src.LAYER_ID, "name": src.LAYER_NAME, "type": "Feature Layer",
            "geometryType": src.GEOMETRY_TYPE, "hasZ": True, "hasM": True,
            "maxRecordCount": src.MAX_RECORD_COUNT,
            "advancedQueryCapabilities": {"supportsPagination": True},
            "extent": {**FIXTURE_LAYER_EXTENT,
                       "spatialReference": {**src.NATIVE_LAYER_SR}},
            "fields": _fixture_layer_fields()}


def _fixture_service_doc() -> dict:
    return {"copyrightText": src.EXPECTED_COPYRIGHT_TEXT,
            "serviceDescription": src.EXPECTED_SERVICE_DESCRIPTION,
            "spatialReference": {**src.NATIVE_LAYER_SR},
            "maxRecordCount": src.MAX_RECORD_COUNT,
            "layers": [{"id": layer_id, "name": name, "geometryType": geometry_type}
                       for layer_id, name, geometry_type in src.EXPECTED_SERVICE_LAYERS]}


def _fixture_item_doc() -> dict:
    return {"id": src.AGOL_ITEM_ID, "owner": src.AGOL_ITEM_OWNER,
            "orgId": src.AGOL_ITEM_ORGID, "title": src.AGOL_ITEM_TITLE,
            "type": src.AGOL_ITEM_TYPE, "url": src.SERVICE_URL}


class JSONStream(httpx.SyncByteStream):
    def __init__(self, payload):
        if isinstance(payload, bytes):
            self.body = payload
        else:
            self.body = json.dumps(payload, separators=(",", ":"),
                                   ensure_ascii=False).encode()

    def __iter__(self):
        yield self.body

    def close(self):
        return None


def _json_response(payload) -> httpx.Response:
    return httpx.Response(200, stream=JSONStream(payload),
                          headers={"content-type": "application/json"})


def _error_response() -> httpx.Response:
    return _json_response({"error": {"code": 500,
                                     "message": "Error performing query operation",
                                     "details": []}})


def make_responder(rows: list[dict], *, count=None, ids=None, service=None, layer=None,
                   item=None, refuse_counts=()) -> httpx.MockTransport:
    count = count if count is not None else len(rows)
    objectids = ids if ids is not None else list(FIXTURE_OBJECTIDS)
    service_doc = service if service is not None else _fixture_service_doc()
    layer_doc = layer if layer is not None else _fixture_layer_doc()
    item_doc = item if item is not None else _fixture_item_doc()
    service_path = urlsplit(src.SERVICE_URL).path
    layer_path = urlsplit(src.LAYER_URL).path
    item_path = f"/sharing/rest/content/items/{src.AGOL_ITEM_ID}"

    def respond(request: httpx.Request) -> httpx.Response:
        parsed = urlparse(str(request.url))
        query = parse_qs(parsed.query)
        if parsed.path == service_path:
            return _json_response(service_doc)
        if parsed.path == layer_path:
            return _json_response(layer_doc)
        if parsed.path == item_path:
            return _json_response(item_doc)
        if query.get("returnCountOnly") == ["true"]:
            return _json_response({"count": count})
        if query.get("returnIdsOnly") == ["true"]:
            return _json_response({"objectIdFieldName": "OBJECTID",
                                   "objectIds": objectids})
        offset = int(query["resultOffset"][0])
        record_count = int(query["resultRecordCount"][0])
        if record_count in refuse_counts:
            return _error_response()
        page = {"objectIdFieldName": "OBJECTID",
                "spatialReference": {**src.NATIVE_LAYER_SR},
                "geometryType": src.GEOMETRY_TYPE,
                "features": rows[offset:offset + record_count],
                "exceededTransferLimit": offset + record_count < count}
        return _json_response(page)

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(schema_sha: str, transport: httpx.MockTransport, *, segment_size=4,
                  mock_construction_validators=True):
    contexts = [
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "PAGE_START_RECORDS", segment_size),
        patch.object(src, "PAGE_PROBE_BASE_DELAY_SECONDS", 0.0),
        patch.object(src, "DOC_CAPTURE_BASE_DELAY_SECONDS", 0.0),
        patch.object(src, "_default_client",
                      side_effect=lambda: httpx.Client(transport=transport)),
    ]
    if mock_construction_validators:
        contexts.extend([
            patch.object(src, "_code_identity",
                         return_value={"code_identity_scheme": "test",
                                       "code_commit": "synthetic", "files": [],
                                       "working_tree_status": "unknown"}),
            patch.object(src, "validate_construction_code_identity"),
            patch.object(capture_common, "validate_construction_code_identity"),
        ])
    for context in contexts:
        context.start()
    try:
        yield
    finally:
        for context in reversed(contexts):
            context.stop()


def _staging(root: Path) -> StagingStorageResource:
    return StagingStorageResource(use_local=True, local_dir=str(root / "staging"),
                                  prefix="", backend="local")


class UsLandslideRegionsSourceTests(unittest.TestCase):
    def test_retained_page_reads_and_decodes_have_no_duplicate_corpus_passes(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        for initially_sealed in (True, False):
            with self.subTest(initially_sealed=initially_sealed), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                capture_root = root / "capture"
                decodes, reads = Counter(), Counter()
                load, read_bytes = src.load_replay, Path.read_bytes

                def load_page(directory, name):
                    if src._PAGE_NAME_PATTERN.fullmatch(name): decodes[name] += 1
                    return load(directory, name)

                def read_page(path):
                    if path.parent == capture_root and path.suffix in {".body", ".wire"}:
                        name = path.name.removesuffix(path.suffix)
                        if src._PAGE_NAME_PATTERN.fullmatch(name): reads[name, path.suffix] += 1
                    return read_bytes(path)

                with fixture_mode(schema_sha, make_responder(rows)), \
                        patch.object(src, "load_replay", side_effect=load_page), \
                        patch.object(Path, "read_bytes", autospec=True, side_effect=read_page):
                    storage = _staging(root)
                    if initially_sealed:
                        src.capture_us_landslide_regions(storage, version="first", capture_directory=str(capture_root))
                        self.assertEqual(set(decodes.values()), {3})
                        self.assertEqual({count for (name, suffix), count in reads.items() if suffix == ".body"}, {4})
                        self.assertEqual({count for (name, suffix), count in reads.items() if suffix == ".wire"}, {5})
                    else:
                        src.fetch_us_landslide_regions_pages(capture_root)
                    decodes.clear(); reads.clear()
                    with patch.object(src, "_PageStream", wraps=src._PageStream) as streams:
                        result = src.capture_us_landslide_regions(storage, version="resume", capture_directory=str(capture_root))
                    self.assertEqual(streams.call_count, 2)
                    self.assertEqual(result["reused"], initially_sealed)
                    self.assertEqual(len(decodes), 2)
                    self.assertEqual(set(decodes.values()), {4 if initially_sealed else 5})
                    self.assertEqual({count for (name, suffix), count in reads.items() if suffix == ".body"},
                                     {4 if initially_sealed else 5})
                    self.assertEqual({count for (name, suffix), count in reads.items() if suffix == ".wire"},
                                     {4 if initially_sealed else 6})

    def test_accepted_corruption_fails_before_network_write_or_quarantine(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, make_responder(_fixture_rows())):
                src.fetch_us_landslide_regions_pages(capture_root)
                page = json.loads((capture_root / "capture_state.json").read_bytes())["pages"][0]["name"]
                body = (capture_root / f"{page}.body").read_bytes().replace(b"Inventory A", b"Inventory X")
                (capture_root / f"{page}.body").write_bytes(body)
                receipt_path = capture_root / f"{page}.receipt.json"
                receipt = json.loads(receipt_path.read_bytes())
                receipt.update(bytes=len(body), sha256=src._sha(body))
                receipt_path.write_bytes(json.dumps(receipt).encode())
                before = {p.name: p.read_bytes() for p in capture_root.iterdir()}
                with patch.object(src, "_default_client", side_effect=AssertionError("HTTP forbidden")), \
                        patch.object(Path, "write_bytes", side_effect=AssertionError("write forbidden")), \
                        patch.object(src, "_witness_aside", side_effect=AssertionError("rename forbidden")), \
                        patch.object(src, "quarantine_incomplete_capture", side_effect=AssertionError("quarantine forbidden")):
                    with self.assertRaisesRegex(src.UsLandslideRegionsError, "retained_capture_wire_body_mismatch"):
                        src.capture_us_landslide_regions(_staging(root), version="forged", capture_directory=str(capture_root))
                self.assertEqual({p.name: p.read_bytes() for p in capture_root.iterdir()}, before)

    def test_refused_custody_is_verified_once_at_admission_not_by_repeated_corpus_scans(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        for failure in ("producer_error", "truncated_gzip"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                capture_root = root / "capture"
                inner = make_responder(_fixture_rows(), refuse_counts={4})

                def respond(request):
                    response = inner.handler(request)
                    if failure == "producer_error": return response
                    body = response.read(); response.close()
                    wire = gzip.compress(body, mtime=0)
                    if parse_qs(urlsplit(str(request.url)).query).get("resultRecordCount") == ["4"]:
                        wire = wire[:-4]
                    return httpx.Response(200, headers={"Content-Encoding": "gzip", "Content-Length": str(len(wire))},
                                          stream=httpx.ByteStream(wire))

                with fixture_mode(schema_sha, httpx.MockTransport(respond)), \
                        patch.object(src, "_validate_retained_transport", wraps=src._validate_retained_transport) as verification:
                    storage = _staging(root)
                    src.capture_us_landslide_regions(storage, version="first", capture_directory=str(capture_root))
                    admissions = [tuple(call.kwargs["names"]) for call in verification.call_args_list if call.kwargs.get("names") is not None]
                    self.assertEqual(len(admissions), 6)
                    self.assertEqual(len(set(admissions)), 6)
                    self.assertEqual(len(verification.call_args_list), 6)
                    originals = {key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}
                    evidence = [key for key in originals if "/evidence/refused/" in key]
                    self.assertEqual(len(evidence), 18)
                    for key in evidence:
                        self.assertEqual(storage.read_key(key), (capture_root / key.split("/evidence/refused/")[1]).read_bytes())
                    verification.reset_mock()
                    with patch.object(src, "_default_client", side_effect=lambda: httpx.Client(
                            transport=httpx.MockTransport(lambda request: (_ for _ in ()).throw(AssertionError("HTTP forbidden"))))):
                        result = src.capture_us_landslide_regions(storage, version="replay", capture_directory=str(capture_root))
                    self.assertTrue(result["reused"])
                    self.assertEqual(verification.call_count, 1)
                    self.assertIsNone(verification.call_args.kwargs.get("names"))
                    self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}, originals)

    def test_refused_bytes_cannot_change_between_verification_and_admission(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            verify = src._validate_retained_transport

            def change_after_verification(directory, *, names=None):
                verify(directory, names=names)
                if names:
                    path = directory / f"{next(iter(names))}.wire"
                    path.write_bytes(path.read_bytes() + b"changed")

            with fixture_mode(schema_sha, make_responder(_fixture_rows(), refuse_counts={4})), \
                    patch.object(src, "_validate_retained_transport", side_effect=change_after_verification):
                storage = _staging(root)
                with self.assertRaisesRegex(src.UsLandslideRegionsError, "retained_capture_changed_during_admission"):
                    src.capture_us_landslide_regions(storage, version="changed", capture_directory=str(capture_root))
                self.assertFalse(storage.list_prefix("_construction-state/"))
                self.assertFalse(any("/evidence/refused/" in key for key in storage.list_prefix("")))

    def test_retained_capture_binds_identity_and_gzip_wire_to_body(self):
        for encoding in ("identity", "gzip"):
            with self.subTest(encoding=encoding), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                body = b'{"id":12}'
                wire = gzip.compress(body, mtime=0) if encoding == "gzip" else body
                receipt = {"method": "GET", "url": src.LAYER_URL + "?f=pjson",
                           "final_url": src.LAYER_URL + "?f=pjson", "status": 200,
                           "complete_body": True, "content_encoding": encoding,
                           "bytes": len(body), "sha256": src._sha(body),
                           "wire_bytes": len(wire), "wire_sha256": src._sha(wire)}
                (root / "layer_before.body").write_bytes(body)
                (root / "layer_before.wire").write_bytes(wire)
                (root / "layer_before.receipt.json").write_bytes(json.dumps(receipt).encode())
                self.assertEqual(src._read_capture(root, "layer_before")[0], body)
                forged = b'{"id":13}'
                (root / "layer_before.body").write_bytes(forged)
                receipt.update(bytes=len(forged), sha256=src._sha(forged))
                (root / "layer_before.receipt.json").write_bytes(json.dumps(receipt).encode())
                before = {p.name: p.read_bytes() for p in root.iterdir()}
                with self.assertRaisesRegex(src.UsLandslideRegionsError, "retained_capture_wire_body_mismatch"):
                    src._read_capture(root, "layer_before")
                self.assertEqual({p.name: p.read_bytes() for p in root.iterdir()}, before)

    def test_retained_files_are_preflighted_before_any_payload_read(self):
        names = ("capture_state.json", "layer_before.receipt.json", "ids_seed.body",
                 "page_000000_00004.wire", "page_000000_00004.attempt-01.body",
                 "page_000000_00004.error-123.wire")
        for name in names:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / name).write_bytes(b"x" * 21)
                with patch.object(src, "MAX_BODY_BYTES", 20), \
                        patch.object(src, "_default_client", side_effect=AssertionError("HTTP forbidden")), \
                        patch.object(Path, "read_bytes", side_effect=AssertionError("payload read before preflight")):
                    with self.assertRaisesRegex(src.UsLandslideRegionsError, "retained_capture_response_budget_exceeded"):
                        src.fetch_us_landslide_regions_pages(root)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "capture_state.json").write_bytes(b"x" * 20)
            (root / "layer_before.body").write_bytes(b"x" * 20)
            with patch.object(src, "MAX_CAPTURE_BYTES", 30), \
                    patch.object(src, "_default_client", side_effect=AssertionError("HTTP forbidden")), \
                    patch.object(Path, "read_bytes", side_effect=AssertionError("payload read before preflight")):
                with self.assertRaisesRegex(src.UsLandslideRegionsError, "retained_capture_total_budget_exceeded"):
                    src.fetch_us_landslide_regions_pages(root)
        for kind in ("directory", "symlink", "fifo"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                target = root / "layer_before.body"
                if kind == "directory": target.mkdir()
                elif kind == "symlink": target.symlink_to(root / "missing")
                else: os.mkfifo(target)
                with patch.object(Path, "read_bytes", side_effect=AssertionError("payload read before preflight")):
                    with self.assertRaisesRegex(src.UsLandslideRegionsError, "retained_capture_nonregular_path"):
                        src.fetch_us_landslide_regions_pages(root)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / f"{src.FILE_SLUG}.parquet").write_bytes(b"x" * 31)
            with patch.object(src, "MAX_CAPTURE_BYTES", 30), \
                    patch.object(Path, "read_bytes", side_effect=AssertionError("payload read before preflight")):
                with self.assertRaisesRegex(src.UsLandslideRegionsError, "retained_derived_output_budget_exceeded"):
                    src.fetch_us_landslide_regions_pages(root)

    def test_refused_transfers_are_request_bound_and_wire_verified(self):
        cases = (("identity", 503, False), ("gzip", 503, False),
                 ("gzip", 200, True), ("deflate", 200, True),
                 ("gzip", 200, False), ("request_error", None, False))
        for encoding, status, truncated in cases:
            with self.subTest(encoding=encoding, status=status, truncated=truncated), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                url = src.LAYER_URL + "?f=pjson"
                body = b'{"error":{"code":503,"message":"unavailable"}}'
                wire = (gzip.compress(body, mtime=0) if encoding == "gzip"
                        else zlib.compress(body) if encoding == "deflate" else body)
                if truncated: wire = wire[:-4]

                def respond(request):
                    if encoding == "request_error":
                        raise httpx.ConnectError("offline fixture", request=request)
                    return httpx.Response(status, headers={"Content-Encoding": encoding,
                        "Content-Length": str(len(wire))}, stream=httpx.ByteStream(wire))

                with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                    if status == 200 and not truncated:
                        capture_common.capture_get(client, url, root, "layer_before", max_bytes=4096)
                    else:
                        with self.assertRaises(ValueError):
                            capture_common.capture_get(client, url, root, "layer_before", max_bytes=4096)
                name = "layer_before.attempt-01"
                for suffix in ("body", "wire", "receipt.json"):
                    (root / f"layer_before.{suffix}").rename(root / f"{name}.{suffix}")
                src._validate_retained_transport(root)
                receipt_path = root / f"{name}.receipt.json"
                receipt = json.loads(receipt_path.read_bytes())
                forged = b"x" * max(1, receipt["bytes"])
                (root / f"{name}.body").write_bytes(forged)
                receipt.update(bytes=len(forged), sha256=src._sha(forged))
                receipt_path.write_bytes(json.dumps(receipt).encode())
                before = {p.name: p.read_bytes() for p in root.iterdir()}
                with self.assertRaises(src.UsLandslideRegionsError):
                    src._validate_retained_transport(root)
                self.assertEqual({p.name: p.read_bytes() for p in root.iterdir()}, before)

    def test_refused_evidence_rejects_wrong_request_and_missing_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            name = "layer_before.attempt-01"
            for suffix in ("body", "wire"):
                (root / f"{name}.{suffix}").write_bytes(b"unavailable")
            receipt = {"method": "GET", "url": "https://example.org/other", "final_url": "https://example.org/other",
                       "complete_body": True, "status": 503, "content_encoding": "identity",
                       "bytes": 11, "sha256": src._sha(b"unavailable"),
                       "wire_bytes": 11, "wire_sha256": src._sha(b"unavailable")}
            (root / f"{name}.receipt.json").write_bytes(json.dumps(receipt).encode())
            with self.assertRaisesRegex(src.UsLandslideRegionsError, "source_capture_endpoint_drift"):
                src._validate_retained_transport(root)
            (root / f"{name}.wire").unlink()
            with self.assertRaisesRegex(src.UsLandslideRegionsError, "retained_capture_artifact_missing"):
                src._validate_retained_transport(root)

    def test_real_sealed_replay_and_fresh_capture_keep_immutable_originals(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        for encoding in ("identity", "gzip"):
            with self.subTest(encoding=encoding), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                inner = make_responder(rows, refuse_counts={4})

                def respond(request):
                    response = inner.handler(request)
                    body = response.read()
                    response.close()
                    wire = gzip.compress(body, mtime=0) if encoding == "gzip" else body
                    return httpx.Response(response.status_code,
                        headers={"Content-Encoding": encoding, "Content-Length": str(len(wire))},
                        stream=httpx.ByteStream(wire))

                transport = httpx.MockTransport(respond)
                with fixture_mode(schema_sha, transport, mock_construction_validators=False):
                    storage = _staging(root)
                    capture_root = root / "capture"
                    first = src.capture_us_landslide_regions(storage, version="first", capture_directory=str(capture_root))
                    self.assertFalse(first["reused"])
                    originals = {key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}
                    evidence_keys = [key for key in originals if "/evidence/refused/" in key]
                    self.assertTrue(evidence_keys)
                    for key in evidence_keys:
                        self.assertEqual(storage.read_key(key), (capture_root / key.split("/evidence/refused/")[1]).read_bytes())
                    with patch.object(src, "_default_client", side_effect=lambda: httpx.Client(
                            transport=httpx.MockTransport(lambda request: (_ for _ in ()).throw(AssertionError("HTTP forbidden"))))):
                        second = src.capture_us_landslide_regions(storage, version="replay", capture_directory=str(capture_root))
                    self.assertTrue(second["reused"])
                    fresh = src.capture_us_landslide_regions(storage, version="fresh", capture_directory=str(root / "fresh"))
                    self.assertTrue(fresh["reused"])
                    self.assertEqual(fresh["version"], first["version"])
                    self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}, originals)
                    page = next(entry["name"] for entry in json.loads((capture_root / "capture_state.json").read_bytes())["pages"])
                    body = (capture_root / f"{page}.body").read_bytes().replace(b"Inventory A", b"Inventory X")
                    (capture_root / f"{page}.body").write_bytes(body)
                    receipt_path = capture_root / f"{page}.receipt.json"
                    receipt = json.loads(receipt_path.read_bytes())
                    receipt.update(bytes=len(body), sha256=src._sha(body))
                    receipt_path.write_bytes(json.dumps(receipt).encode())
                    retained = {p.name: p.read_bytes() for p in capture_root.iterdir()}
                    with patch.object(src, "_default_client", side_effect=AssertionError("HTTP forbidden")):
                        with self.assertRaisesRegex(src.UsLandslideRegionsError, "retained_capture_wire_body_mismatch"):
                            src.capture_us_landslide_regions(storage, version="forged", capture_directory=str(capture_root))
                    self.assertEqual({p.name: p.read_bytes() for p in capture_root.iterdir()}, retained)
                    self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}, originals)
                    parquet_key = next(key for key in originals if "/geoparquet/" in key)
                    storage.write_key(parquet_key, b"corrupt parquet")
                    tampered = {key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}
                    with patch.object(src, "_default_client", side_effect=lambda: httpx.Client(
                            transport=httpx.MockTransport(lambda request: (_ for _ in ()).throw(AssertionError("HTTP forbidden"))))):
                        with self.assertRaisesRegex(ValueError, "cached_candidate_object_integrity_failed"):
                            src.capture_us_landslide_regions(storage, version="corrupt", capture_directory=str(root / "fresh"))
                    for key, original in tampered.items():
                        self.assertEqual((storage.read_key(key), storage.object_snapshot(key)), original)
                    diagnostics = set(storage.list_prefix("")) - set(tampered)
                    self.assertTrue(diagnostics)
                    self.assertTrue(all(key.startswith("_quarantine/") for key in diagnostics))

    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "us-landslide-regions")
        self.assertEqual(src.SERVICE_URL,
                         "https://services.arcgis.com/v01gqwM5QqNysAAi/arcgis/rest/"
                         "services/US_Landslide_poly_v2/FeatureServer")
        self.assertEqual(src.AGOL_ITEM_ID, "f332e370f4dd4952a77a9d454f2362eb")
        self.assertEqual(src.AGOL_ITEM_ORGID, "v01gqwM5QqNysAAi")
        self.assertEqual(src.AGOL_ITEM_OWNER, "esjones@usgs.gov_USGS")
        self.assertEqual(src.SCIENCEBASE_DOI, "10.5066/P14AJF8I")
        self.assertEqual(src.LAYER_ID, 12)
        self.assertEqual(src.LAYER_NAME, "US_Landslide_v3poly")
        self.assertEqual(src.GEOMETRY_TYPE, "esriGeometryPolygon")
        self.assertEqual(src.MAX_RECORD_COUNT, 2000)
        self.assertEqual(src.PAGE_START_RECORDS, 1000)
        self.assertEqual(src.EXPECTED_SERVICE_DESCRIPTION,
                          "USGS US Landslide version 3 polygons")
        self.assertTrue(src.EXPECTED_SCHEMA_SHA256)
        for removed in ("EXPECTED_COUNT", "EXPECTED_LAYER_EXTENT", "EXPECTED_IDS_SHA256",
                        "EXPECTED_RECORD_SHA256", "EXPECTED_GEOMETRY_SHA256",
                        "EXPECTED_NULL_GEOMETRY_COUNT", "EXPECTED_EMPTY_GEOMETRY_COUNT",
                        "EXPECTED_INVALID_GEOMETRY_COUNT", "EXPECTED_TOTAL_VERTEX_COUNT",
                        "EXPECTED_RINGS_PER_FEATURE_DISTRIBUTION",
                        "EXPECTED_OBSERVED_BOUNDS"):
            self.assertFalse(hasattr(src, removed), removed)
        self.assertEqual(src.EXPECTED_ATTRIBUTE_NAMES,
                         ("OBJECTID", "fid_1", "USGS_ID", "Date_Min", "Date_Max",
                          "Fatalities", "Confidence", "LS_Type", "Inventory",
                          "Inv_URL", "Info_Sourc", "Notes", "Lat_N", "Lon_W",
                          "Shape__Area", "Shape__Length"))
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.DATASET_SLUG, src.FILE_SLUG]
                            for key in keys))
        import dagster_hifld.assets.us_landslide_regions as asset_module
        self.assertEqual(asset_module.ASSET_CHECKS,
                         [us_landslide_regions_complete,
                          us_landslide_regions_approved_baseline_drift])
        schedule = defs.get_schedule_def(
            "us_landslide_regions_draft_refresh_schedule")
        from dagster import DefaultScheduleStatus
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)

    def test_segment_query_contract_is_roster_bounded(self):
        url = src._segment_query(2000, 1000)
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["where"], ["1=1"])
        self.assertEqual(query["outFields"], ["*"])
        self.assertEqual(query["returnGeometry"], ["true"])
        self.assertEqual(query["returnZ"], ["true"])
        self.assertEqual(query["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(query["resultOffset"], ["2000"])
        self.assertEqual(query["resultRecordCount"], ["1000"])
        self.assertEqual(parse_qs(urlparse(src._count_url()).query)["returnCountOnly"],
                         ["true"])
        self.assertEqual(parse_qs(urlparse(src._ids_url()).query)["returnIdsOnly"],
                         ["true"])
        self.assertTrue(urlparse(src._count_url()).path.endswith("/12/query"))
        self.assertTrue(urlparse(src.AGOL_ITEM_URL).path.endswith(
            f"/content/items/{src.AGOL_ITEM_ID}"))

    def test_polygon_wkb_keeps_rings_holes_and_z(self):
        rings = [_shell(-10_000_000.0, 4_000_000.0)]
        wkb = src._geometry_wkb({"rings": rings, "hasZ": True})
        self.assertEqual(wkb[0:5], b"\x01" + struct.pack("<I", 1000 + 3))
        from shapely import from_wkb
        geometry = from_wkb(wkb)
        self.assertEqual(geometry.geom_type, "Polygon")
        self.assertTrue(geometry.has_z)
        self.assertEqual(len(geometry.exterior.coords), 5)
        self.assertEqual(len(geometry.interiors), 0)
        with_hole = src._geometry_wkb(
            {"rings": [_shell(-10_000_000.0, 4_000_000.0), _hole(-9_999_000.0, 4_002_000.0)],
             "hasZ": True})
        holed = from_wkb(with_hole)
        self.assertEqual(holed.geom_type, "Polygon")
        self.assertEqual(len(holed.interiors), 1)
        multi = src._geometry_wkb(
            {"rings": [_shell(-10_000_000.0, 4_000_000.0), _shell(-9_000_000.0, 3_000_000.0)],
             "hasZ": True})
        multipolygon = from_wkb(multi)
        self.assertEqual(multipolygon.geom_type, "MultiPolygon")
        self.assertEqual(len(multipolygon.geoms), 2)

    def test_polygon_geometry_drift_is_refused(self):
        base = {"rings": [_shell(-10_000_000.0, 4_000_000.0)], "hasZ": True}
        closed = _shell(-10_000_000.0, 4_000_000.0)
        cases = {
            "unexpected_source_geometry_members": {**base, "curves": []},
            "source_polygon_z_flag_drift": {"rings": base["rings"], "hasZ": False},
            "source_polygon_rings_missing": {"hasZ": True, "rings": []},
            "source_polygon_ring_invalid": {"rings": [closed[:3]], "hasZ": True},
            "source_polygon_ordinate_count_drift":
                {"rings": [[point[:2] for point in closed]], "hasZ": True},
            "source_polygon_ordinate_not_numeric":
                {"rings": [[["NaN", 2.0, 0.0]] * 5], "hasZ": True},
            "source_polygon_ordinate_not_finite":
                {"rings": [[[float("inf"), 2.0, 0.0]] * 5], "hasZ": True},
            "source_coordinate_range_drift":
                {"rings": [[[25_000_000.0, 4_000_000.0, 0.0],
                            [25_000_000.0, 4_010_000.0, 0.0],
                            [25_010_000.0, 4_010_000.0, 0.0],
                            [25_010_000.0, 4_000_000.0, 0.0],
                            [25_000_000.0, 4_000_000.0, 0.0]]], "hasZ": True},
            "source_polygon_ring_not_closed":
                {"rings": [closed[:-1] + [[-10_000_000.5, 4_000_000.0, 0.0]]],
                 "hasZ": True},
        }
        for expected_error, geometry in cases.items():
            with self.subTest(expected_error):
                with self.assertRaisesRegex(src.UsLandslideRegionsError,
                                            expected_error):
                    src._validate_feature_geometry(geometry)
        # Ring orientation and hole assignment are GDAL's (issue #997 shared
        # converter): a lone CCW ring and a hole lying inside no exterior of
        # its feature are retained as valid geometry instead of refusing the
        # capture, and every served ordinate is verified unchanged.
        from shapely import from_wkb
        lone = from_wkb(src._geometry_wkb({"rings": [_hole(-9_999_000.0, 4_002_000.0)],
                                           "hasZ": True}))
        self.assertTrue(lone.is_valid)
        orphan = from_wkb(src._geometry_wkb(
            {"rings": [_shell(-10_000_000.0, 4_000_000.0), _shell(-9_000_000.0, 3_000_000.0),
                       _hole(-9_500_000.0, 3_500_000.0)], "hasZ": True}))
        self.assertTrue(orphan.is_valid)
        self.assertEqual(orphan.geom_type, "MultiPolygon")
        self.assertEqual(len(orphan.geoms), 3)

    def test_page_matches_slice_and_gap_planning(self):
        roster = list(FIXTURE_OBJECTIDS)
        rows = _fixture_rows()
        page = {"features": rows[:4]}
        self.assertTrue(arcgis_adaptive.page_matches_id_slice(
            json.dumps(page).encode(), 0, 4, roster))
        self.assertFalse(arcgis_adaptive.page_matches_id_slice(
            json.dumps({"error": {}}).encode(), 0, 4, roster))
        self.assertFalse(arcgis_adaptive.page_matches_id_slice(
            json.dumps({"features": rows[:2]}).encode(), 0, 4, roster))
        self.assertFalse(arcgis_adaptive.page_matches_id_slice(
            json.dumps({"features": list(reversed(rows[:4]))}).encode(), 0, 4,
            roster))
        self.assertEqual(src._page_name(0, 1000), "page_000000_01000")
        self.assertEqual(src._parse_page_name("page_012000_01000"), (12000, 1000))
        with tempfile.TemporaryDirectory() as tmp:
            capture_root = Path(tmp) / "capture"
            capture_root.mkdir()
            state = {"pages": [{"name": "page_000000_00004", "offset": 0, "count": 4,
                                "features": 4, "captured_at": "t",
                                "body_sha256": "x"},
                               {"name": "page_000006_00002", "offset": 6, "count": 2,
                                "features": 2, "captured_at": "t",
                                "body_sha256": "x"}]}
            (capture_root / "capture_state.json").write_bytes(
                json.dumps(state).encode())
            for name in ("page_000000_00004", "page_000006_00002"):
                body = json.dumps({"features": rows}).encode()
                (capture_root / f"{name}.body").write_bytes(body)
                (capture_root / f"{name}.wire").write_bytes(body)
                receipt = {"method": "GET", "url": "https://x/y", "final_url": "https://x/y",
                           "complete_body": True, "status": 200, "bytes": len(body),
                           "sha256": src._sha(body), "wire_bytes": len(body),
                           "wire_sha256": src._sha(body), "observed_at": "t"}
                (capture_root / f"{name}.receipt.json").write_bytes(
                    json.dumps(receipt).encode())
            gaps = arcgis_adaptive.plan_page_gaps(src._capture_spec(), capture_root, roster, src._load_state(capture_root))
            self.assertEqual(gaps, [(4, 2)])

    def test_complete_fixture_capture_materializes_and_repeats(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = _staging(root)
                first = src.capture_us_landslide_regions(
                    storage, version="fixture-run", capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                self.assertFalse(first["source_release_accepted"])
                second = src.capture_us_landslide_regions(
                    storage, version="fixture-run-2", capture_directory=str(capture_root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertTrue((capture_root / "computed_baseline.json").exists())
            parquet_path = capture_root / f"{src.FILE_SLUG}.parquet"
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, FIXTURE_COUNT)
            self.assertEqual(table.schema.names[-1], "geometry")
            self.assertEqual(table.column("OBJECTID").to_pylist(),
                             list(FIXTURE_OBJECTIDS))
            self.assertEqual(table.column("USGS_ID").to_pylist(),
                             [row["attributes"]["USGS_ID"] for row in rows])
            self.assertEqual(table.column("Fatalities").to_pylist(),
                             [row["attributes"]["Fatalities"] for row in rows])
            geometries = table.column("geometry").to_pylist()
            self.assertIsNone(geometries[3])
            self.assertIsNotNone(geometries[0])
            from shapely import from_wkb
            first_geometry = from_wkb(geometries[0])
            self.assertEqual(first_geometry.geom_type, "Polygon")
            self.assertEqual(len(first_geometry.interiors), 0)
            self.assertTrue(first_geometry.has_z)
            holed = from_wkb(geometries[1])
            self.assertEqual(len(holed.interiors), 1)
            self.assertEqual(from_wkb(geometries[2]).geom_type, "MultiPolygon")
            geo_metadata = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                             ["Polygon Z", "MultiPolygon Z"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["crs"]
                             .get("name", ""), "WGS 84 / Pseudo-Mercator")

            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            from dagster import DagsterInstance, materialize
            with fixture_mode(schema_sha, transport):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    result = materialize(
                        [us_landslide_regions, us_landslide_regions_complete,
                         us_landslide_regions_approved_baseline_drift],
                        resources={"staging_storage": _staging(root)},
                        instance=instance,
                        run_config={"ops": {
                            "ingest__us_landslide_regions__us_landslide_regions":
                                {"config": {"capture_directory": str(capture_root)}}}},
                    )
                    self.assertTrue(result.success)
                    checks = {check.check_name: check
                              for check in result.get_asset_check_evaluations()}
                    self.assertTrue(checks["us_landslide_regions_complete"].passed)
                    warning = checks["us_landslide_regions_approved_baseline_drift"]
                    self.assertFalse(warning.passed)
                    self.assertEqual(warning.severity, AssetCheckSeverity.WARN)
                    self.assertEqual(warning.metadata["comparison_status"].value,
                                     "skipped_no_approved_baseline")
                    candidate = result.output_for_node(
                        "ingest__us_landslide_regions__us_landslide_regions")
                    self.assertEqual(candidate["feature_count"], FIXTURE_COUNT)
                    self.assertEqual(candidate["source_reported_count"], FIXTURE_COUNT)
                    self.assertTrue(candidate["source_population_complete"])
                    self.assertTrue(candidate["reused"])

    def test_completeness_check_uses_capture_facts_not_an_old_population(self):
        candidate = {"feature_count": FIXTURE_COUNT,
                     "source_reported_count": FIXTURE_COUNT,
                     "source_population_complete": True,
                     "source_id_sha256": "a" * 64,
                     "source_schema_sha256": src.EXPECTED_SCHEMA_SHA256}
        self.assertTrue(us_landslide_regions_complete(candidate).passed)
        for changes in ({"source_reported_count": FIXTURE_COUNT + 1},
                        {"source_reported_count": True},
                        {"feature_count": True},
                        {"feature_count": 0, "source_reported_count": 0},
                        {"source_population_complete": False},
                        {"source_id_sha256": ""},
                        {"source_schema_sha256": ""}):
            with self.subTest(changes=changes):
                self.assertFalse(
                    us_landslide_regions_complete({**candidate, **changes}).passed)

    def test_sealed_candidate_manifest_and_audit_are_unapproved(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_us_landslide_regions(
                    _staging(root), version="seal-run", capture_directory=str(capture_root))
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            staged = root / "staging" / prefix
            audit = json.loads((staged / "metadata" / "source_audit.json").read_bytes())
            self.assertEqual(audit["source_feature_count"], FIXTURE_COUNT)
            self.assertEqual(audit["source_reported_count"], FIXTURE_COUNT)
            self.assertTrue(audit["source_population_complete"])
            self.assertEqual(audit["unique_source_id_count"], FIXTURE_COUNT)
            self.assertEqual(audit["null_geometry_count"], 1)
            self.assertEqual(audit["rings_per_feature_distribution"],
                             {str(ring_count): count
                              for ring_count, count in {1: 5, 2: 2}.items()})
            self.assertEqual(audit["declared_layer_extent_3857"], FIXTURE_LAYER_EXTENT)
            self.assertIsNone(audit["crosswalk_layer_row"])
            self.assertEqual(audit["pagination"]["mode"],
                             "enumerated_roster_bisected_resultoffset_segments")
            self.assertEqual(audit["pagination"]["page_start_records"], 4)
            self.assertTrue(audit["pagination"]["resumable"])
            self.assertFalse(audit["release_accepted"])
            self.assertFalse(audit["rights_accepted"])
            manifest = json.loads(
                (staged / "metadata" / "source_manifest.json").read_bytes())
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["agency"], "U.S. Geological Survey")
            self.assertEqual(manifest["publisher"], "U.S. Geological Survey")
            hosts = {entry["host"] for entry in manifest["inputs"]}
            self.assertEqual(hosts, {"services.arcgis.com", "www.arcgis.com"})
            self.assertTrue(all(entry["role"] == "producer_publication"
                                for entry in manifest["inputs"]))
            self.assertTrue(
                all(entry["canonical_url"].startswith("https://services.arcgis.com/")
                    or entry["canonical_url"].startswith(
                        "https://www.arcgis.com/sharing/")
                    for entry in manifest["inputs"]))
            parquet_bytes = (staged / "geoparquet" / f"{src.DATASET_SLUG}.parquet") \
                .read_bytes()
            self.assertEqual(pq.read_table(
                __import__("io").BytesIO(parquet_bytes)).num_rows, FIXTURE_COUNT)
            ready = json.loads((staged / "metadata" / "candidate_ready.json").read_bytes())
            self.assertFalse(ready["approved"])
            self.assertFalse(ready["release_accepted"])

    def test_refused_pages_bisect_down_to_singles(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_counts={4})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, segment_size=4):
                result = src.capture_us_landslide_regions(
                    _staging(root), version="bisect-run", capture_directory=str(capture_root))
                self.assertEqual(result["feature_count"], FIXTURE_COUNT)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertEqual(
                    sorted((entry["offset"], entry["count"])
                           for entry in state["pages"]),
                    [(0, 2), (2, 2), (4, 2), (6, 2)])
                resumed = src.capture_us_landslide_regions(
                    _staging(root), version="bisect-run-2",
                    capture_directory=str(capture_root))
                self.assertTrue(resumed["reused"])

    def test_item_usage_counter_drift_between_brackets_is_tolerated(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        before_doc = {**_fixture_item_doc(), "lastViewed": 1794672000000,
                      "numViews": 236313}
        counter_drift = {**before_doc, "lastViewed": 1794690000000,
                         "numViews": 236351}
        real_drift = {**counter_drift, "title": "Drifted title"}
        item_path = f"/sharing/rest/content/items/{src.AGOL_ITEM_ID}"
        for after_doc, expected_error in ((counter_drift, None),
                                          (real_drift,
                                           "source_metadata_or_count_changed_during_capture")):
            with self.subTest(drift=expected_error):
                inner = make_responder(rows)
                docs = [before_doc, after_doc]

                def respond(request, docs=docs, inner=inner):
                    if urlsplit(str(request.url)).path == item_path:
                        return _json_response(
                            docs.pop(0) if len(docs) > 1 else docs[0])
                    return inner.handler(request)

                transport = httpx.MockTransport(respond)
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    capture_root = root / "capture"
                    with fixture_mode(schema_sha, transport):
                        if expected_error is None:
                            result = src.capture_us_landslide_regions(
                                _staging(root), version="item-drift-run",
                                capture_directory=str(capture_root))
                            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
                            self.assertTrue((capture_root
                                             / "computed_baseline.json").exists())
                        else:
                            with self.assertRaises(src.UsLandslideRegionsError) as raised:
                                src.capture_us_landslide_regions(
                                    _staging(root), version="item-drift-run",
                                    capture_directory=str(capture_root))
                            self.assertIn(expected_error, str(raised.exception))

    def test_unservable_single_feature_refuses_explicitly(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_counts={4, 2, 1})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport, segment_size=4):
                with self.assertRaisesRegex(src.UsLandslideRegionsError,
                                            "source_single_feature_unservable"):
                    src.fetch_us_landslide_regions_pages(root / "capture")
                self.assertTrue(sorted((root / "capture").glob("page_*.error-*.body")))

    def test_drift_and_provenance_failures_are_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        cases = {
            "source_count_drift": dict(count=FIXTURE_COUNT + 1),
            "source_schema_drift": dict(layer=_fixture_layer_doc()
                                        | {"fields": _fixture_layer_fields()
                                           + [{"name": "EXTRA",
                                               "type": "esriFieldTypeString",
                                               "alias": "EXTRA", "length": 4,
                                               "domain": None}]}),
            "us_landslide_regions_service_identity_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "serviceDescription": "legacy HIFLD service."}),
            "us_landslide_regions_service_layer_roster_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "layers": _fixture_service_doc()["layers"][:-1]}),
            "us_landslide_regions_item_identity_drift:item_before": dict(
                item={**_fixture_item_doc(), "title": "Renamed"}),
            "us_landslide_regions_layer_metadata_drift": dict(
                layer={**_fixture_layer_doc(), "geometryType": "esriGeometryPoint"}),
            "source_layer_z_or_m_contract_drift": dict(
                layer={**_fixture_layer_doc(), "hasM": False}),
            "source_record_limit_drift": dict(layer={**_fixture_layer_doc(),
                                                     "maxRecordCount": 1000}),
            "source_pagination_capability_changed": dict(
                layer={**_fixture_layer_doc(),
                       "advancedQueryCapabilities": {"supportsPagination": False}}),
            "source_layer_extent_drift": dict(
                layer={**_fixture_layer_doc(),
                       "extent": {**FIXTURE_LAYER_EXTENT, "xmax": -20_000_000.0,
                                  "spatialReference": {**src.NATIVE_LAYER_SR}}}),
            "source_id_population_drift": dict(
                ids=[*FIXTURE_OBJECTIDS[:-1], FIXTURE_OBJECTIDS[0]]),
            "unexpected_source_geometry_members": dict(rows=[
                {**rows[0], "geometry": {**rows[0]["geometry"], "curves": []}},
                *rows[1:]]),
            "source_coordinate_range_drift": dict(rows=[
                {**rows[0], "geometry": {"rings": [
                    [[25_000_000.0, *point[1:]] for point in
                     _shell(-10_000_000.0, 4_000_000.0)]], "hasZ": True}},
                *rows[1:]]),
            "source_polygon_ring_not_closed": dict(rows=[
                {**rows[0], "geometry": {"rings": [
                    _shell(-10_000_000.0, 4_000_000.0)[:-1]
                    + [[-10_000_000.5, 4_000_000.0, 0.0]]], "hasZ": True}},
                *rows[1:]]),
            "source_page_id_slice_incomplete_or_reordered": dict(rows=[
                rows[0],
                {**rows[1], "attributes": {**rows[1]["attributes"], "OBJECTID": 999}},
                *rows[2:]]),
            "source_record_schema_or_geometry_drift": dict(rows=[
                {**rows[0], "attributes": {"OBJECTID": 2, "USGS_ID": "ply000002"}},
                *rows[1:]]),
            "source_native_type_changed:fid_1": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "fid_1": "101.25"}},
                *rows[1:]]),
            "source_spatial_reference_drift:feature": dict(rows=[
                {**rows[0], "geometry": {**rows[0]["geometry"],
                                         "spatialReference": {"wkid": 4326,
                                                              "latestWkid": 4326}}},
                *rows[1:]]),
        }
        for expected_error, kwargs in cases.items():
            with self.subTest(expected_error):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    capture_root = root / "capture"
                    case_rows = kwargs.pop("rows", rows)
                    transport = make_responder(case_rows, **kwargs)
                    with fixture_mode(schema_sha, transport):
                        storage = _staging(root)
                        with self.assertRaisesRegex(
                                src.UsLandslideRegionsError, expected_error):
                            src.capture_us_landslide_regions(
                                storage, version="drift-run",
                                capture_directory=str(capture_root))
                    quarantined = sorted(
                        (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
                    self.assertTrue(quarantined, f"no quarantine for {expected_error}")
                    failure = json.loads((quarantined[0] / "metadata" / "failure.json")
                                         .read_bytes())
                    self.assertTrue(failure["reason_code"])

    def test_geometry_gates_are_observed_without_population_pins(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, make_responder(rows)):
                first = src.capture_us_landslide_regions(
                    _staging(root), version="gate-run", capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                self.assertEqual(baseline["source_null_geometry_count"], 1)
                self.assertEqual(baseline["source_empty_geometry_count"], 0)
                self.assertEqual(baseline["source_invalid_geometry_count"], 0)
                self.assertEqual(baseline["source_total_vertex_count"], 45)
                self.assertEqual(
                    {int(key): value
                     for key, value in baseline["source_rings_per_feature_distribution"]
                     .items()},
                    {1: 5, 2: 2})
                again = src.capture_us_landslide_regions(
                    _staging(root), version="gate-run-verify",
                    capture_directory=str(capture_root))
                self.assertTrue(again["reused"])
                self.assertEqual({k: v for k, v in first.items() if k != "reused"},
                                 {k: v for k, v in again.items() if k != "reused"})

    def test_capture_witness_drift_is_refused_and_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, make_responder(rows)):
                storage = _staging(root)
                src.capture_us_landslide_regions(
                    storage, version="witness-run", capture_directory=str(capture_root))
                _replace_retained_body(
                    capture_root, "ids_before",
                    src._canonical({"objectIdFieldName": "OBJECTID",
                                    "objectIds": [*FIXTURE_OBJECTIDS[:-1], 99]}))
                with self.assertRaisesRegex(
                        src.UsLandslideRegionsError,
                        "source_id_population_drift:ids_seed_vs_before"):
                    src.capture_us_landslide_regions(
                        storage, version="witness-drift",
                        capture_directory=str(capture_root))
                quarantined = sorted(
                    (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
                self.assertTrue(quarantined)
                failure = json.loads((quarantined[0] / "metadata" / "failure.json")
                                     .read_bytes())
                self.assertEqual(failure["reason_code"],
                                 "source_id_population_drift:ids_seed_vs_before")


def _replace_retained_body(root: Path, name: str, body: bytes, *, retain_wire=False):
    receipt_path = root / f"{name}.receipt.json"
    receipt = json.loads(receipt_path.read_bytes())
    (root / f"{name}.body").write_bytes(body)
    receipt.update(bytes=len(body), sha256=src._sha(body))
    if not retain_wire:
        (root / f"{name}.wire").write_bytes(body)
        receipt.update(wire_bytes=len(body), wire_sha256=src._sha(body),
                       content_encoding="identity")
        if receipt.get("content_length") is not None:
            receipt["content_length"] = len(body)
    receipt_path.write_bytes(src._canonical(receipt) + b"\n")


class UsLandslideRegionsWitnessTests(unittest.TestCase):
    def _capture(self, root, capture_root, transport, version="witness"):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with fixture_mode(schema_sha, transport):
            return src.capture_us_landslide_regions(
                _staging(root), version=version, capture_directory=str(capture_root))

    def _inventory(self, root):
        return {str(path.relative_to(root)): src._sha(path.read_bytes())
                for path in root.rglob("*") if path.is_file()}

    def _offline(self, requests):
        def reject(request):
            requests.append(str(request.url))
            raise AssertionError("HTTP forbidden during replay")
        return httpx.MockTransport(reject)

    def _after_urls(self):
        return {"layer_after": src.LAYER_URL + "?f=pjson",
                "service_after": src.SERVICE_URL + "?f=pjson",
                "count_after": src._count_url(), "ids_after": src._ids_url(),
                "item_after": src.AGOL_ITEM_URL}

    def test_invalid_or_unstable_witnesses_and_incomplete_pages_cannot_seal(self):
        for attack in ("duplicate_ids", "nonpositive_ids", "boolean_ids", "boolean_count",
                       "changed_closing_ids", "changed_closing_count", "missing_row",
                       "reordered_rows"):
            with self.subTest(attack=attack), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                rows = _fixture_rows()
                ids = list(FIXTURE_OBJECTIDS)
                count = len(ids)
                if attack == "duplicate_ids":
                    ids[-1] = ids[0]
                elif attack == "nonpositive_ids":
                    ids[-1] = 0
                elif attack == "boolean_ids":
                    ids[0] = True
                elif attack == "boolean_count":
                    count = True
                elif attack == "missing_row":
                    rows.pop()
                elif attack == "reordered_rows":
                    rows[0], rows[1] = rows[1], rows[0]
                inner = make_responder(rows, ids=ids, count=count)
                ids_calls = 0
                count_calls = 0

                def respond(request):
                    nonlocal ids_calls, count_calls
                    query = parse_qs(urlparse(str(request.url)).query)
                    if query.get("returnIdsOnly") == ["true"]:
                        ids_calls += 1
                        if attack == "changed_closing_ids" and ids_calls == 3:
                            return _json_response({"objectIdFieldName": "OBJECTID",
                                                   "objectIds": [*FIXTURE_OBJECTIDS[:-1],
                                                                 99]})
                    if query.get("returnCountOnly") == ["true"]:
                        count_calls += 1
                        if attack == "changed_closing_count" and count_calls == 2:
                            return _json_response({"count": count + 1})
                    return inner.handler(request)

                with self.assertRaises(src.UsLandslideRegionsError):
                    self._capture(root, root / "capture", httpx.MockTransport(respond))
                self.assertFalse(list((root / "staging").rglob("candidate_ready.json")))

    def test_gapped_added_removed_populations_preserve_native_values_and_cache(self):
        for count in (8, 7, 6):
            with self.subTest(count=count), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                rows = _fixture_rows()
                for _ in range(FIXTURE_COUNT - count):
                    rows.pop()
                ids = [11 + index * 19 for index in range(count)]
                ids[-1] = 900001
                for row, oid in zip(rows, ids):
                    row["attributes"]["OBJECTID"] = oid
                capture_root = root / "capture"
                first = self._capture(root, capture_root, make_responder(rows, ids=ids))
                inventory = self._inventory(root / "staging")
                requests = []
                cached = self._capture(root, capture_root, self._offline(requests),
                                       "offline")
                self.assertEqual(requests, [])
                self.assertFalse(first["reused"])
                self.assertTrue(cached["reused"])
                self.assertEqual({k: v for k, v in first.items() if k != "reused"},
                                 {k: v for k, v in cached.items() if k != "reused"})
                self.assertEqual(inventory, self._inventory(root / "staging"))
                self.assertEqual(first["feature_count"], count)
                self.assertEqual(first["source_reported_count"], count)
                self.assertIs(first["source_population_complete"], True)
                self.assertEqual(first["source_id_sha256"],
                                 src._sha(src._canonical(ids)))
                self.assertEqual(first["source_null_geometry_count"], 1)
                self.assertEqual(first["source_empty_geometry_count"], 0)
                self.assertEqual(first["source_invalid_geometry_count"], 0)
                for key, value in first.items():
                    if key.endswith(("_accepted", "_approved")):
                        self.assertIs(value, False, key)
                table = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
                for name, _type, _length in src.NATIVE_FIELDS:
                    self.assertEqual(table.column(name).to_pylist(),
                                     [row["attributes"][name] for row in rows], name)
                self.assertEqual(table.column("OBJECTID").to_pylist(), ids)
                for row, wkb in zip(rows, table.column("geometry").to_pylist()):
                    geometry = row["geometry"]
                    if geometry is None:
                        self.assertIsNone(wkb)
                        continue
                    shape = shapely.wkb.loads(wkb)
                    parts = [shape] if shape.geom_type == "Polygon" else list(shape.geoms)
                    xy = lambda ring: sorted((round(x, 9), round(y, 9))
                                             for x, y, *_ in ring[:-1])
                    decoded_rings = [xy(part.exterior.coords) for part in parts]
                    for part in parts:
                        for interior in part.interiors:
                            decoded_rings.append(xy(interior.coords))
                    expected_rings = [xy(ring) for ring in geometry["rings"]]
                    self.assertEqual(sorted(decoded_rings), sorted(expected_rings))
                prefix = root / "staging" / first["candidate_ready"]
                audit = json.loads((prefix.parent / "source_audit.json").read_bytes())
                self.assertEqual(audit["source_feature_count"], count)
                self.assertEqual(audit["source_id_sha256"], first["source_id_sha256"])
                self.assertEqual(audit["source_reported_count"], count)
                self.assertIs(audit["source_population_complete"], True)
                self.assertEqual(audit["observed_bounds"], first["observed_bounds"])
                identity = json.loads(
                    (prefix.parent / "processing_identity.json").read_bytes())
                self.assertEqual(identity["code_identity_scheme"], "test")

    def test_upstream_added_feature_passes_completeness_and_warns_on_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows = _fixture_rows()
            first = self._capture(root, root / "capture-1", make_responder(rows))
            self.assertEqual(first["feature_count"], FIXTURE_COUNT)
            added = {"attributes": {**rows[0]["attributes"], "OBJECTID": 900001,
                                    "USGS_ID": "ply900001", "Shape__Area": 90000.0,
                                    "Shape__Length": 4500.0},
                     "geometry": {"rings": [_shell(-7_000_000.0, 5_000_000.0)],
                                  "hasZ": True}}
            new_rows = [*rows, added]
            new_ids = [*FIXTURE_OBJECTIDS, 900001]
            second = self._capture(root, root / "capture-2",
                                   make_responder(new_rows, ids=new_ids, count=9))
            self.assertFalse(second["reused"])
            self.assertEqual(second["feature_count"], 9)
            self.assertEqual(second["source_reported_count"], 9)
            self.assertIs(second["source_population_complete"], True)
            self.assertEqual(second["source_id_sha256"],
                             src._sha(src._canonical(new_ids)))
            table = pq.read_table(root / "capture-2" / f"{src.FILE_SLUG}.parquet")
            self.assertEqual(table.column("OBJECTID").to_pylist(), new_ids)
            self.assertTrue(us_landslide_regions_complete(second).passed)
            baseline = {**first, "release_accepted": True}
            warning = approved_baseline_drift(second, baseline)
            self.assertFalse(warning.passed)
            self.assertEqual(warning.severity, AssetCheckSeverity.WARN)
            self.assertEqual(warning.metadata["comparison_status"].value,
                             "compared_approved_release")
            self.assertEqual(warning.metadata["feature_count_delta"].value, 1)
            self.assertIs(warning.metadata["native_id_roster_changed"].value, True)
            skip = us_landslide_regions_approved_baseline_drift(second)
            self.assertFalse(skip.passed)
            self.assertEqual(skip.severity, AssetCheckSeverity.WARN)
            self.assertEqual(skip.metadata["comparison_status"].value,
                             "skipped_no_approved_baseline")

    def test_reacquired_closing_group_detects_id_roster_drift(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            capture_root = Path(tmp) / "capture"
            inner = make_responder(_fixture_rows())
            with fixture_mode(schema_sha, inner):
                src.fetch_us_landslide_regions_pages(capture_root)
            # Simulate a crash between the closing witnesses being witnessed aside
            # and the completed state being written: the retained corpus is still
            # valid, so the next admission re-acquires the whole closing group.
            state = src._load_state(capture_root)
            src._save_state(capture_root, {**state, "complete": False})
            originals = {name + suffix: (capture_root / (name + suffix)).read_bytes()
                         for name in src.AFTER_NAMES
                         for suffix in (".body", ".wire", ".receipt.json")}
            requests = []

            def changed(request):
                requests.append(str(request.url))
                query = parse_qs(urlparse(str(request.url)).query)
                self.assertNotIn("resultOffset", query)
                if query.get("returnIdsOnly") == ["true"]:
                    return _json_response({"objectIdFieldName": "OBJECTID",
                                           "objectIds": [*FIXTURE_OBJECTIDS[:-1], 99]})
                return inner.handler(request)

            with fixture_mode(schema_sha, httpx.MockTransport(changed)):
                with self.assertRaisesRegex(
                        src.UsLandslideRegionsError,
                        "source_id_population_drift:ids_after"):
                    src.fetch_us_landslide_regions_pages(capture_root)
            self.assertEqual(requests,
                             [self._after_urls()[name] for name in src.AFTER_NAMES])
            self.assertIs(src._load_state(capture_root)["complete"], False)
            for name in src.AFTER_NAMES:
                for suffix in (".body", ".wire", ".receipt.json"):
                    self.assertIn(
                        originals[name + suffix],
                        [path.read_bytes()
                         for path in capture_root.glob(name + ".error-*" + suffix)])

    def test_interrupted_closing_group_repair_restarts_all_five_on_second_attempt(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            capture_root = Path(tmp) / "capture"
            inner = make_responder(_fixture_rows())
            with fixture_mode(schema_sha, inner):
                src.fetch_us_landslide_regions_pages(capture_root)
            # Simulate a crash between the closing witnesses being witnessed aside
            # and the completed state being written: the retained corpus is still
            # valid, so the next admission restarts the whole closing group.
            state = src._load_state(capture_root)
            src._save_state(capture_root, {**state, "complete": False})
            originals = {path.name: path.read_bytes() for name in src.AFTER_NAMES
                         for suffix in (".body", ".wire", ".receipt.json")
                         if (path := capture_root / (name + suffix)).exists()}
            first_requests, pending_states = [], []

            def interrupted(request):
                first_requests.append(str(request.url))
                pending_states.append(src._load_state(capture_root)["complete"])
                if str(request.url) == src.SERVICE_URL + "?f=pjson":
                    raise httpx.ConnectError("closing service interrupted")
                response = inner.handler(request)
                document = json.loads(response.stream.body)
                document["repair_epoch"] = "first attempt"
                return _json_response(document)

            with fixture_mode(schema_sha, httpx.MockTransport(interrupted)):
                with self.assertRaisesRegex(
                        src.UsLandslideRegionsError,
                        "source_capture_failed:ValueError"):
                    src.fetch_us_landslide_regions_pages(capture_root)
            self.assertEqual(first_requests,
                             [src.LAYER_URL + "?f=pjson", src.SERVICE_URL + "?f=pjson"])
            self.assertEqual(pending_states, [False, False])
            self.assertIs(src._load_state(capture_root)["complete"], False)
            for name in src.AFTER_NAMES:
                for suffix in (".body", ".wire", ".receipt.json"):
                    if name + suffix in originals:
                        self.assertIn(
                            originals[name + suffix],
                            [path.read_bytes()
                             for path in capture_root.glob(name + ".error-*" + suffix)])
            self.assertIn(
                "repair_epoch",
                json.loads((capture_root / "layer_after.body").read_bytes()))
            second_requests = []

            def resumed(request):
                second_requests.append(str(request.url))
                self.assertIs(src._load_state(capture_root)["complete"], False)
                return inner.handler(request)

            with fixture_mode(schema_sha, httpx.MockTransport(resumed)):
                result = src.fetch_us_landslide_regions_pages(capture_root)
            self.assertEqual(second_requests,
                             [self._after_urls()[name] for name in src.AFTER_NAMES])
            self.assertTrue(result["complete"])
            self.assertIs(src._load_state(capture_root)["complete"], True)
            self.assertNotIn(
                "repair_epoch",
                json.loads((capture_root / "layer_after.body").read_bytes()))

    def test_interrupted_closing_resume_reacquires_all_five_and_retains_failed_evidence(self):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            inner = make_responder(_fixture_rows())
            with fixture_mode(schema_sha, inner):
                src.fetch_us_landslide_regions_pages(capture_root)
            # Simulate a crash between the closing witnesses being witnessed aside
            # and the completed state being written: the retained corpus is still
            # valid, so the next admission restarts the whole closing group.
            state = src._load_state(capture_root)
            src._save_state(capture_root, {**state, "complete": False})
            first_requests = []

            def interrupted(request):
                first_requests.append(str(request.url))
                query = parse_qs(urlparse(str(request.url)).query)
                self.assertNotIn("resultOffset", query)
                if query.get("returnCountOnly") == ["true"]:
                    raise httpx.ConnectError("closing witness interrupted")
                return inner.handler(request)

            with fixture_mode(schema_sha, httpx.MockTransport(interrupted)):
                with self.assertRaisesRegex(
                        src.UsLandslideRegionsError,
                        "source_capture_failed:ValueError"):
                    src.fetch_us_landslide_regions_pages(capture_root)
            self.assertEqual(first_requests[:3],
                             [self._after_urls()[name]
                              for name in src.AFTER_NAMES[:3]])
            self.assertIs(src._load_state(capture_root)["complete"], False)
            failed = json.loads(
                (capture_root / "count_after.receipt.json").read_bytes())
            self.assertIs(failed["complete_body"], False)
            self.assertEqual(failed["error_code"], "request_error")
            closing_requests = []

            def resume(request):
                query = parse_qs(urlparse(str(request.url)).query)
                self.assertNotIn("resultOffset", query)
                closing_requests.append(str(request.url))
                return inner.handler(request)

            self._capture(root, capture_root, httpx.MockTransport(resume), "recovered")
            self.assertEqual(closing_requests,
                             [self._after_urls()[name] for name in src.AFTER_NAMES])
            self.assertIs(src._load_state(capture_root)["complete"], True)
            for witness in src.AFTER_NAMES:
                self.assertTrue(
                    list(capture_root.glob(witness + ".error-*.receipt.json")))
            requests = []
            self.assertTrue(self._capture(root, capture_root,
                                          self._offline(requests))["reused"])
            self.assertEqual(requests, [])


def _page_failures_transport(inner: httpx.MockTransport, *, fail_first: int | None):
    """Fail the first ``fail_first`` (or every) page request with a reset.

    Returns the wrapped transport and the count of page requests seen, so a
    test can assert the shared retry helper's bounded attempt budget.
    """
    seen = {"page_requests": 0}

    def respond(request: httpx.Request) -> httpx.Response:
        query = parse_qs(urlparse(str(request.url)).query)
        if query.get("resultOffset") is not None:
            seen["page_requests"] += 1
            if fail_first is None or seen["page_requests"] <= fail_first:
                raise httpx.ConnectError("connection reset by peer")
        return inner.handler(request)

    return httpx.MockTransport(respond), seen


class UsLandslideRegionsRetainedStateFailureTests(unittest.TestCase):
    """Failure paths of the retained-capture primitives this adapter binds.

    The checkpoint/receipt/retry helpers live in
    ``dagster_hifld.sources.arcgis_layer`` and ``capture_common``; every case
    here drives them through this adapter's own error type over retained
    fixtures: corrupt checkpoint JSON, missing and changed page receipts,
    incomplete transport captures retried by the shared helper, that
    helper's bounded retry budget, and a foreign checkpoint whose coverage
    claims are never trusted.
    """

    def _first_capture(self, root: Path, capture_root: Path, schema_sha: str,
                       transport: httpx.MockTransport, version: str) -> None:
        with fixture_mode(schema_sha, transport):
            result = src.capture_us_landslide_regions(
                _staging(root), version=version, capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)

    def _row_ids(self, capture_root: Path) -> list[int]:
        table = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
        return table.column("OBJECTID").to_pylist()

    def test_corrupt_checkpoint_json_refuses_and_retains_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "state-run")
            corrupted = b'{"pages": [ truncated'
            (capture_root / "capture_state.json").write_bytes(corrupted)
            before = sorted(path.name for path in capture_root.iterdir())
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(src.UsLandslideRegionsError,
                                            "capture_state_invalid"):
                    src.fetch_us_landslide_regions_pages(capture_root)
            self.assertEqual((capture_root / "capture_state.json").read_bytes(),
                             corrupted)
            self.assertEqual(sorted(path.name for path in capture_root.iterdir()),
                             before)

    def test_missing_page_receipt_refuses_and_retains_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "receipt-run")
            name = src._page_name(0, 4)
            (capture_root / f"{name}.receipt.json").unlink()
            with self.assertRaisesRegex(
                    src.UsLandslideRegionsError,
                    "retained_capture_receipt_invalid:" + name):
                src._read_capture(capture_root, name)
            before = sorted(path.name for path in capture_root.iterdir())
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(
                        src.UsLandslideRegionsError,
                        "retained_capture_artifact_missing:" + name):
                    src.capture_us_landslide_regions(
                        _staging(root), version="receipt-run-2",
                        capture_directory=str(capture_root))
            self.assertEqual(sorted(path.name for path in capture_root.iterdir()),
                             before)

    def test_changed_page_receipt_refuses_and_retains_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "tamper-run")
            name = src._page_name(4, 4)
            receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
            receipt["sha256"] = "0" * 64
            (capture_root / f"{name}.receipt.json").write_bytes(
                json.dumps(receipt).encode())
            with self.assertRaisesRegex(
                    src.UsLandslideRegionsError,
                    "retained_capture_receipt_invalid:" + name):
                src._read_capture(capture_root, name)
            before = sorted(path.name for path in capture_root.iterdir())
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(
                        src.UsLandslideRegionsError,
                        "retained_capture_receipt_invalid:" + name):
                    src.capture_us_landslide_regions(
                        _staging(root), version="tamper-run-2",
                        capture_directory=str(capture_root))
            self.assertEqual(sorted(path.name for path in capture_root.iterdir()),
                             before)

    def test_corrupt_closing_count_is_refused_at_admission(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "closing-run")
            (capture_root / "count_after.body").write_bytes(b"corrupt closing count")
            before = sorted(path.name for path in capture_root.iterdir())
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(
                        src.UsLandslideRegionsError,
                        "retained_capture_receipt_invalid:count_after"):
                    src.fetch_us_landslide_regions_pages(capture_root)
            self.assertEqual((capture_root / "count_after.body").read_bytes(),
                             b"corrupt closing count")
            self.assertEqual(sorted(path.name for path in capture_root.iterdir()),
                             before)

    def test_incomplete_transport_capture_is_retried_with_retained_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport, _seen = _page_failures_transport(make_responder(rows), fail_first=1)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_us_landslide_regions(
                    _staging(root), version="retry-run",
                    capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            name = src._page_name(0, 4)
            attempt_receipt = json.loads(
                (capture_root / f"{name}.attempt-01.receipt.json").read_bytes())
            self.assertFalse(attempt_receipt["complete_body"])
            self.assertIsNone(attempt_receipt["status"])
            self.assertEqual(attempt_receipt["error_code"], "request_error")
            for suffix in ("body", "wire", "receipt.json"):
                self.assertTrue((capture_root / f"{name}.attempt-01.{suffix}").exists())
            self.assertFalse(list(capture_root.glob(f"{name}.attempt-02.*")))
            self.assertEqual(self._row_ids(capture_root), list(FIXTURE_OBJECTIDS))

    def test_retry_budget_exhaustion_is_bounded_and_retains_failure_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport, seen = _page_failures_transport(make_responder(rows), fail_first=None)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(
                        src.UsLandslideRegionsError,
                        "source_capture_failed:ValueError"):
                    src.fetch_us_landslide_regions_pages(capture_root)
            self.assertEqual(seen["page_requests"],
                             2 * src.PAGE_PROBE_ATTEMPTS + src.DOC_CAPTURE_ATTEMPTS)
            probe = src._page_name(0, 4)
            half = src._page_name(0, 2)
            single = src._page_name(0, 1)
            self.assertTrue((capture_root / f"{probe}.attempt-01.receipt.json").exists())
            self.assertTrue((capture_root / f"{probe}.attempt-02.receipt.json").exists())
            self.assertFalse(list(capture_root.glob(f"{probe}.attempt-03.*")))
            self.assertTrue((capture_root / f"{half}.attempt-01.receipt.json").exists())
            self.assertTrue((capture_root / f"{half}.attempt-02.receipt.json").exists())
            self.assertFalse(list(capture_root.glob(f"{half}.attempt-03.*")))
            for attempt in range(1, src.DOC_CAPTURE_ATTEMPTS):
                for suffix in ("body", "wire", "receipt.json"):
                    self.assertTrue(
                        (capture_root / f"{single}.attempt-{attempt:02d}.{suffix}").exists(),
                        f"missing attempt evidence {single}.attempt-{attempt:02d}.{suffix}")
            self.assertFalse(
                list(capture_root.glob(f"{single}.attempt-{src.DOC_CAPTURE_ATTEMPTS:02d}.*")))
            final = json.loads((capture_root / f"{single}.receipt.json").read_bytes())
            self.assertFalse(final["complete_body"])
            self.assertEqual(final["error_code"], "request_error")
            self.assertFalse((capture_root / "capture_state.json").exists())
            self.assertFalse((capture_root / "computed_baseline.json").exists())

    def test_foreign_checkpoint_coverage_is_never_trusted(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "foreign-run")
            foreign = {"pages": [{"name": "foreign_product_page", "offset": 0,
                                  "count": FIXTURE_COUNT, "features": FIXTURE_COUNT,
                                  "captured_at": "1999-01-01T00:00:00+00:00",
                                  "body_sha256": "0" * 64}], "complete": True}
            (capture_root / "capture_state.json").write_bytes(
                json.dumps(foreign).encode())
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(
                        src.UsLandslideRegionsError,
                        "source_capture_roster_incomplete_or_unexpected"):
                    src.capture_us_landslide_regions(
                        _staging(root), version="foreign-run-2",
                        capture_directory=str(capture_root))
            self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
            quarantined = sorted(
                (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
            self.assertTrue(quarantined)
            failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"],
                             "source_capture_roster_incomplete_or_unexpected")


if __name__ == "__main__":
    unittest.main()
