from __future__ import annotations

import contextlib
import hashlib
import gzip
import json
import struct
import tempfile
import tracemalloc
import unittest
import httpx
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from shapely import from_wkb
from shapely.geometry import LineString

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.wbd_wbdline import (
    wbd_wbdline, wbd_wbdline_complete, wbd_wbdline_draft_schedule,
    wbd_wbdline_drift,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import wbd_wbdline as source
from dagster_hifld.sources.esri_common import canonical as _canonical

_TEST_COUNT = 9
_TEST_PAGE_SIZE = 4
_TEST_PAGE_COUNT = 3
_TEST_ROSTER = [11, 12, 501, 100_003, 200_004, 300_005, 400_006, 500_007, 600_008]
_SR = {"wkid": 102100, "latestWkid": 3857}
_SERVICE_DOC = ("<html>USGS National Hydrography products: WBD data downloads and services "
                "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer "
                "https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/"
                "Hydrography/WBD/National/</html>")

_GATE_FEATURES = {501: "source_polyline_path_degenerate",
                  100_003: "source_polyline_paths_missing"}


@contextlib.contextmanager
def patches(module, **values):
    with contextlib.ExitStack() as stack:
        for name, value in values.items():
            stack.enter_context(patch.object(module, name, value))
        yield


@contextlib.contextmanager
def relaxed_pins(**extra):
    scale = {
        "PAGE_SIZE": _TEST_PAGE_SIZE, "EXPECTED_SCHEMA_SHA256": "",
    }
    scale.update(extra)
    with patches(source, **scale):
        yield


def _line(oid: int) -> list[list[float]]:
    x0 = -13_131_705.0 + (oid % 7) * 9000.0
    y0 = 3_984_685.0 + ((oid // 7) % 900) * 7000.0
    return [[x0, y0], [x0 + 5000.0, y0 + 4000.0], [x0 + 11_000.0, y0 + 4000.0],
            [x0 + 16_000.0, y0]]


def _second_path(oid: int) -> list[list[float]]:
    x0 = -13_131_705.0 + (oid % 7) * 9000.0
    y0 = 3_984_685.0 + ((oid // 7) % 900) * 7000.0
    return [[x0 + 20_000.0, y0], [x0 + 26_000.0, y0 + 3000.0], [x0 + 31_000.0, y0 + 8000.0]]


def _feature_paths(oid: int) -> list[list[list[float]]]:
    if _GATE_FEATURES.get(oid) == "source_polyline_path_degenerate":
        return [[_line(oid)[0]]]
    if _GATE_FEATURES.get(oid) == "source_polyline_paths_missing":
        return []
    if oid == 12:
        return [_line(oid), _second_path(oid)]
    return [_line(oid)]


def _feature_ids(oid: int) -> list[list[int]] | None:
    # The producer serves a per-vertex integer ids member on some features
    # (observed 2026-11-15); it is cross-checked and counted, never published.
    if oid == 12:
        return [[position + 1 for position, _ in enumerate(path)]
                for path in _feature_paths(oid)]
    return None


def _attributes(oid: int) -> dict:
    attrs = {}
    for field in source.ATTRIBUTE_FIELDS:
        name, kind = field["name"], field["type"]
        if name == "objectid":
            attrs[name] = oid
        elif name == "hudigit":
            attrs[name] = 2 if oid % 2 else 4
        elif kind == "esriFieldTypeDate":
            attrs[name] = 1_700_000_000_000 + oid
        elif kind == "esriFieldTypeDouble":
            attrs[name] = float(oid) * 10.0
        elif name == "tnmid":
            attrs[name] = "{%08X-4B8E-9E52-388A49AC0A23}" % (oid & 0xFFFFFFFF)
        elif name == "globalid":
            attrs[name] = "{%08X-4C1E-4A8D-9E52-388A49AC0A23}" % (oid & 0xFFFFFFFF)
        else:
            attrs[name] = f"{name}-{oid}"
    if oid not in _GATE_FEATURES:
        decoded = from_wkb(source._assemble_geometry(_feature_paths(oid)))
        attrs["shape_Length"] = decoded.length
    else:
        attrs["shape_Length"] = 340.0
    return attrs


def _documents(roster=None) -> dict:
    roster = _TEST_ROSTER if roster is None else roster
    page_count = (len(roster) + _TEST_PAGE_SIZE - 1) // _TEST_PAGE_SIZE
    service = {"currentVersion": 11.3, "mapName": source.MAP_NAME,
               "copyrightText": source.EXPECTED_COPYRIGHT_TEXT,
               "capabilities": "Map,Query,Data", "maxRecordCount": 2000,
               "units": "esriMeters", "tables": [], "spatialReference": dict(_SR),
               "fullExtent": {**source.EXPECTED_EXTENT, "spatialReference": dict(_SR)},
               "layers": [{"id": layer_id, "name": name, "parentLayerId": -1,
                           "subLayerIds": None, "type": "Feature Layer"}
                          for layer_id, name in source.EXPECTED_SERVICE_LAYERS]}
    layer = {"id": source.LAYER_ID, "name": source.LAYER_NAME, "type": "Feature Layer",
             "geometryType": "esriGeometryPolyline", "maxRecordCount": 2000,
             "copyrightText": source.EXPECTED_LAYER_COPYRIGHT_TEXT, "displayField": "TNMID",
             "geometryField": {"name": "shape"},
             "geometryProperties": {"shapeLengthFieldName": "shape_Length",
                                    "units": "esriMeters", "mapUnits": {"uwkid": 9001}},
             "typeIdField": None, "isDataVersioned": False, "isDataArchived": False,
             "extent": {**source.EXPECTED_LAYER_EXTENT, "spatialReference": dict(_SR)},
             "advancedQueryCapabilities": {"supportsPagination": True},
             "drawingInfo": {"renderer": {"authoringInfo": {
                 "colorRamp": {"type": "multipart",
                               "colorRamps": [{"type": "algorithmic",
                                               "algorithm": "esriCIELabAlgorithm",
                                               "fromColor": [236, 252, 204, 255],
                                               "toColor": [236, 252, 204, 255]}]}}}},
             "sourceSpatialReference": dict(_SR), "spatialReference": None,
             "fields": json.loads(json.dumps(source.EXPECTED_LAYER_FIELDS))}
    features = {}
    for oid in roster:
        geometry = {"paths": _feature_paths(oid)}
        ids = _feature_ids(oid)
        if ids is not None:
            geometry["ids"] = ids
        features[oid] = {"attributes": _attributes(oid), "geometry": geometry}
    pages = {}
    for index in range(page_count):
        page_ids = roster[index * _TEST_PAGE_SIZE:(index + 1) * _TEST_PAGE_SIZE]
        pages[f"page_{index:04d}"] = {
            "displayFieldName": "TNMID",
            "exceededTransferLimit": index < page_count - 1,
            "features": [features[oid] for oid in page_ids],
            "fieldAliases": {field["name"]: field["alias"]
                             for field in source.EXPECTED_PAGE_FIELD_DECLARATIONS},
            "fields": json.loads(json.dumps(source.EXPECTED_PAGE_FIELD_DECLARATIONS)),
            "geometryType": "esriGeometryPolyline",
            "spatialReference": dict(_SR),
        }
    return {
        "service_doc": _SERVICE_DOC,
        "service_before": service, "layer_before": layer,
        "count_before": {"count": len(roster)},
        "ids_before": {"objectIdFieldName": source.OID_FIELD_NAME, "objectIds": list(roster)},
        "service_after": json.loads(json.dumps(service)),
        "layer_after": json.loads(json.dumps(layer)),
        "count_after": {"count": len(roster)},
        "ids_after": {"objectIdFieldName": source.OID_FIELD_NAME, "objectIds": list(roster)},
        **pages,
    }


def _digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _encode(document) -> bytes:
    if isinstance(document, bytes):
        return document
    return json.dumps(document, ensure_ascii=False, allow_nan=False).encode()


def _receipt_url(name: str, roster: list[int], expected_urls: dict[str, str]) -> str:
    if name in expected_urls:
        return expected_urls[name]
    window = name[:-len("_refusal")] if name.endswith("_refusal") else name
    return source._window_url(roster, int(window[5:9]), window[9:])


def _write_capture_root(root: Path, documents: dict) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    roster = documents["ids_before"]["objectIds"]
    expected_urls = dict(source._capture_roster(roster))
    for name, document in documents.items():
        body = _encode(document)
        wire = body
        url = _receipt_url(name, roster, expected_urls)
        receipt = {"method": "GET", "url": url, "final_url": url,
                   "observed_at": "2026-11-14T12:00:00.000000Z", "status": 200,
                   "complete_body": True, "bytes": len(body), "sha256": _digest(body),
                   "wire_bytes": len(wire), "wire_sha256": _digest(wire),
                   "content_encoding": None, "content_length": None,
                   "error_code": None, "error_type": None}
        (root / f"{name}.body").write_bytes(body)
        (root / f"{name}.wire").write_bytes(wire)
        (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    return root


def _seal_manifest(root: Path, documents: dict, *, page_parts: dict | None = None,
                   page_refusals: list[str] | None = None) -> None:
    manifest = {"complete": True, "captured_at": "2026-11-14T12:00:00.000000Z",
                "receipts": {}, "artifact_roster": {},
                "page_parts": page_parts or {},
                "page_refusals": page_refusals or []}
    for name in sorted(documents):
        body = (root / f"{name}.body").read_bytes()
        wire = (root / f"{name}.wire").read_bytes()
        receipt = json.loads((root / f"{name}.receipt.json").read_bytes())
        receipt_body = (root / f"{name}.receipt.json").read_bytes()
        manifest["receipts"][name] = receipt
        manifest["artifact_roster"][f"{name}.body"] = {
            "bytes": len(body), "sha256": _digest(body)}
        manifest["artifact_roster"][f"{name}.wire"] = {
            "bytes": len(wire), "sha256": _digest(wire)}
        manifest["artifact_roster"][f"{name}.receipt.json"] = {
            "bytes": len(receipt_body), "sha256": _digest(receipt_body)}
    (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")


class WbdWbdlineTests(unittest.TestCase):
    def _network_capture(self, storage, root, documents, extra=None, encoding="identity"):
        roster = documents["ids_before"]["objectIds"]
        responses = {}
        metadata_urls = {
            "service_doc": source.SERVICE_DOC_URL,
            "service_before": source.SERVICE_URL + "?f=pjson",
            "service_after": source.SERVICE_URL + "?f=pjson",
            "layer_before": source.LAYER_URL + "?f=pjson",
            "layer_after": source.LAYER_URL + "?f=pjson",
            "count_before": source._count_url(), "count_after": source._count_url(),
            "ids_before": source._ids_url(), "ids_after": source._ids_url()}
        for name, url in metadata_urls.items():
            if name in documents:
                responses.setdefault(str(httpx.URL(url)), []).append(documents[name])
        for index in range((len(roster) + _TEST_PAGE_SIZE - 1) // _TEST_PAGE_SIZE):
            name = f"page_{index:04d}"
            url = source._window_url(roster, index, "")
            if str(httpx.URL(url)) not in responses:
                responses[str(httpx.URL(url))] = [documents[name]]
        for url, document in (extra or {}).items():
            responses[str(httpx.URL(url))] = [document]
        client = httpx.Client
        def respond(request):
            body = _encode(responses[str(request.url)].pop(0))
            wire = gzip.compress(body, mtime=0) if encoding == "gzip" else body
            return httpx.Response(200, headers={"Content-Encoding": encoding,
                "Content-Length": str(len(wire))}, stream=httpx.ByteStream(wire))
        with patch.object(source.httpx, "Client", side_effect=lambda **kwargs: client(
                **kwargs, transport=httpx.MockTransport(respond))):
            return source.capture_wbd_wbdline(storage, version="observed", capture_directory=str(root))

    def test_real_maintained_population_constructs_and_replays(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"))
        with patches(source, PAGE_SIZE=_TEST_PAGE_SIZE, DISK_FLOOR_BYTES=1):
            result = self._network_capture(storage, root / "captures", _documents())
            self.assertEqual(result["feature_count"], _TEST_COUNT)
            self.assertEqual(result["source_reported_count"], _TEST_COUNT)
            with patch.object(source.httpx, "Client", side_effect=AssertionError("network on replay")):
                replay = source.capture_wbd_wbdline(storage, version="replay",
                    capture_directory=result["capture_directory"])
            self.assertTrue(replay["reused"])
            self.assertEqual(replay["version"], result["version"])

    def test_real_added_removed_populations_preserve_native_values_and_original_candidates(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"))
        versions = set()
        with patches(source, PAGE_SIZE=_TEST_PAGE_SIZE, DISK_FLOOR_BYTES=1):
            for index, roster in enumerate((_TEST_ROSTER[:3], _TEST_ROSTER,
                    sorted([*_TEST_ROSTER, 700_009, 800_010, 900_011, 1_000_012]))):
                with self.subTest(roster=roster):
                    result = self._network_capture(storage, root / f"capture-{index}", _documents(roster))
                    self.assertEqual(result["feature_count"], len(roster))
                    self.assertEqual(result["source_reported_count"], len(roster))
                    self.assertEqual(result["source_id_sha256"], _digest(_canonical(roster)))
                    self.assertEqual((result["source_id_min"], result["source_id_max"]),
                                     (min(roster), max(roster)))
                    self.assertTrue(wbd_wbdline_complete(result).passed)
                    prefix = Path(storage.local_dir) / source.DATASET_SLUG / source.FILE_SLUG / result["version"]
                    parquet = pq.read_table(prefix / "geoparquet" / (source.FILE_SLUG + ".parquet"))
                    self.assertEqual(parquet["objectid"].to_pylist(), list(roster))
                    self.assertEqual(parquet.column_names, [*source.FIELD_NAMES, "geometry"])
                    for name, value in _attributes(roster[0]).items():
                        self.assertEqual(parquet[name][0].as_py(), value)
                    self.assertEqual(parquet["geometry"][0].as_py(), source._assemble_geometry(_feature_paths(roster[0])))
                    self.assertEqual(json.loads(parquet.schema.metadata[b"geo"])["columns"]["geometry"]["crs"]["id"]["code"], 3857)
                    self.assertFalse(result["release_accepted"])
                    original = {p.relative_to(prefix): p.read_bytes() for p in prefix.rglob("*") if p.is_file()}
                    refresh = self._network_capture(storage, root / f"refresh-{index}", _documents(roster))
                    self.assertTrue(refresh["reused"])
                    self.assertEqual(refresh["version"], result["version"])
                    self.assertEqual(original, {p.relative_to(prefix): p.read_bytes()
                        for p in prefix.rglob("*") if p.is_file()})
                    versions.add(result["version"])
            self.assertEqual(len(versions), 3)
            drift = wbd_wbdline_drift(result)
            self.assertFalse(drift.passed)
            self.assertEqual(drift.severity.value, "WARN")
            self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
            changed = _documents(roster)
            changed["page_0000"]["features"][0]["attributes"]["linesource"] = "Maintained publisher edit"
            edited = self._network_capture(storage, root / "changed", changed)
            self.assertFalse(edited["reused"])
            self.assertNotEqual(edited["version"], result["version"])
            self.assertEqual(original, {p.relative_to(prefix): p.read_bytes()
                for p in prefix.rglob("*") if p.is_file()})

    def test_real_identity_gzip_replay_rejects_forged_decoded_pages_without_changing_originals(self):
        for encoding in ("identity", "gzip"):
            with self.subTest(encoding=encoding):
                temporary = tempfile.TemporaryDirectory()
                self.addCleanup(temporary.cleanup)
                root = Path(temporary.name)
                storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"))
                with patches(source, PAGE_SIZE=_TEST_PAGE_SIZE, DISK_FLOOR_BYTES=1):
                    result = self._network_capture(storage, root / "captures", _documents(), encoding=encoding)
                    capture = Path(result["capture_directory"])
                    def inventory():
                        stage = Path(storage.local_dir)
                        return {p.relative_to(stage): p.read_bytes()
                            for directory in (stage / source.DATASET_SLUG, stage / "_construction-state")
                            for p in directory.rglob("*") if p.is_file()}
                    with patch.object(source.httpx, "Client", side_effect=AssertionError("network on replay")):
                        replay = source.capture_wbd_wbdline(storage, version="replay", capture_directory=str(capture))
                    self.assertTrue(replay["reused"])
                    original = inventory()
                    name = "page_0000"
                    body_path = capture / f"{name}.body"
                    document = json.loads(body_path.read_bytes())
                    document["features"][0]["attributes"]["linesource"] = "Forged decoded string"
                    body = _encode(document)
                    body_path.write_bytes(body)
                    receipt_path = capture / f"{name}.receipt.json"
                    receipt = json.loads(receipt_path.read_bytes())
                    receipt.update(bytes=len(body), sha256=_digest(body))
                    receipt_path.write_bytes(_canonical(receipt))
                    manifest_path = capture / "capture_manifest.json"
                    manifest = json.loads(manifest_path.read_bytes())
                    manifest["receipts"][name] = receipt
                    manifest["artifact_roster"] = source.capture_roster_digests(capture, exclude={"capture_manifest.json"})
                    manifest_path.write_bytes(_canonical(manifest))
                    with patch.object(source.httpx, "Client", side_effect=AssertionError("network on replay")):
                        with self.assertRaisesRegex(ValueError, "retained_capture_wire_body_mismatch"):
                            source.capture_wbd_wbdline(storage, version="forged", capture_directory=str(capture))
                    self.assertEqual(inventory(), original)

    def test_replay_retains_receipts_not_population_bodies(self):
        def peak(page_count):
            roster = list(range(11, 11 + page_count * _TEST_PAGE_SIZE))
            documents = _documents(roster)
            for name in documents:
                if name.startswith("page_"):
                    documents[name] = _encode(documents[name]) + b" " * 1_000_000
            root = self._capture_root(documents)
            _seal_manifest(root, documents)
            with patches(source, PAGE_SIZE=_TEST_PAGE_SIZE):
                tracemalloc.start()
                try:
                    captures, _parts = source._read_replay(root)
                    _current, maximum = tracemalloc.get_traced_memory()
                finally:
                    tracemalloc.stop()
            self.assertTrue(all(isinstance(receipt, dict) for receipt in captures.values()))
            return maximum
        small, large = peak(4), peak(8)
        self.assertLess(large, small * 1.4, (small, large))

    def test_replay_requires_exact_actual_refusal_ancestors_for_split_parts(self):
        for case in ("valid", "missing_root", "missing_parent", "extra", "duplicate", "forged", "wrong_url"):
            with self.subTest(case=case):
                documents = _documents()
                page = documents.pop("page_0000")
                for suffix, features in (("aa", page["features"][:1]),
                                         ("ab", page["features"][1:2]), ("b", page["features"][2:])):
                    documents["page_0000" + suffix] = dict(page, features=features)
                refusal = {"error": {"code": 500, "message": "Error performing query operation"}}
                refusals = ["page_0000_refusal", "page_0000a_refusal"]
                if case == "missing_root":
                    refusals.remove("page_0000_refusal")
                elif case == "missing_parent":
                    refusals.remove("page_0000a_refusal")
                elif case == "extra":
                    refusals.append("page_0000b_refusal")
                elif case == "duplicate":
                    refusals.append("page_0000a_refusal")
                documents.update((name, refusal) for name in refusals)
                if case == "forged":
                    documents["page_0000a_refusal"] = page
                root = self._capture_root(documents)
                if case == "wrong_url":
                    path = root / "page_0000a_refusal.receipt.json"
                    receipt = json.loads(path.read_bytes())
                    with relaxed_pins():
                        receipt["url"] = receipt["final_url"] = source._window_url(_TEST_ROSTER, 0, "b")
                    path.write_bytes(_canonical(receipt))
                parts = {"page_0000": ["page_0000aa", "page_0000ab", "page_0000b"]}
                _seal_manifest(root, documents, page_parts=parts, page_refusals=refusals)
                with relaxed_pins():
                    if case == "valid":
                        _receipts, replay_parts = source._read_replay(root)
                        audit, _derived = source._validate_stream(root, parquet_sink=None, page_parts=replay_parts)
                        self.assertEqual(audit["source_feature_count"], len(_TEST_ROSTER))
                    else:
                        with self.assertRaisesRegex(ValueError, "refusal|request_identity"):
                            source._read_replay(root)

    def test_replay_preflights_flat_paths_and_all_budgets_before_reading(self):
        for case in ("manifest", "body", "wire", "receipt", "service_doc", "aggregate", "directory", "symlink"):
            with self.subTest(case=case):
                documents = _documents()
                root = self._capture_root(documents)
                _seal_manifest(root, documents)
                if case in ("manifest", "body", "wire", "receipt"):
                    filename = {"manifest": "capture_manifest.json", "body": "page_0000.body",
                                "wire": "page_0000.wire", "receipt": "page_0000.receipt.json"}[case]
                    with (root / filename).open("r+b") as artifact:
                        artifact.truncate(65537 if case == "receipt" else 1_000_001)
                elif case == "directory":
                    (root / "nested").mkdir()
                elif case == "symlink":
                    (root / "linked.body").symlink_to(root / "page_0000.body")
                budget = sum(path.stat().st_size for path in root.iterdir() if path.is_file())
                with relaxed_pins(PAGE_MAX_BYTES=1_000_000,
                        DOC_MAX_BYTES=8 if case == "service_doc" else 4_000_000,
                        CAPTURE_MAX_BYTES=budget - 1 if case == "aggregate" else 10_000_000), \
                        patch.object(Path, "read_bytes", side_effect=AssertionError("read before replay preflight")):
                    with self.assertRaisesRegex(ValueError, "size_limit|budget|nonregular"):
                        source._read_replay(root)

    def test_real_recursive_bisection_preserves_refusals_and_single_feature_floor(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"))
        documents = _documents()
        page = documents["page_0000"]
        refusal = {"error": {"code": 500, "message": "Error performing query operation"}}
        documents["page_0000"] = refusal
        with patches(source, PAGE_SIZE=_TEST_PAGE_SIZE, DISK_FLOOR_BYTES=1):
            extra = {source._window_url(_TEST_ROSTER, 0, "a"): refusal}
            for suffix, features in (("aa", page["features"][:1]),
                                     ("ab", page["features"][1:2]), ("b", page["features"][2:])):
                extra[source._window_url(_TEST_ROSTER, 0, suffix)] = dict(page, features=features)
            result = self._network_capture(storage, root / "captures", documents, extra)
            manifest = json.loads((Path(result["capture_directory"]) / "capture_manifest.json").read_bytes())
            self.assertEqual(manifest["page_parts"]["page_0000"],
                ["page_0000aa", "page_0000ab", "page_0000b"])
            self.assertEqual(manifest["page_refusals"], ["page_0000_refusal", "page_0000a_refusal"])
            with patch.object(source.httpx, "Client", side_effect=AssertionError("network on replay")):
                replay = source.capture_wbd_wbdline(storage, version="replay",
                    capture_directory=result["capture_directory"])
            self.assertTrue(replay["reused"])
            extra[source._window_url(_TEST_ROSTER, 0, "aa")] = refusal
            with self.assertRaisesRegex(ValueError, "source_page_window_refused_single_feature"):
                self._network_capture(storage, root / "refused", documents, extra)
            self.assertTrue(any((Path(storage.local_dir) / "_quarantine").rglob("failure.json")))

    def _capture_root(self, documents: dict) -> Path:
        temporary = tempfile.TemporaryDirectory(prefix="wbd-wbdline-capture-")
        self.addCleanup(temporary.cleanup)
        with relaxed_pins():
            return _write_capture_root(Path(temporary.name) / "captures", documents)

    def test_pinned_source_constants_match_verified_evidence(self):
        self.assertEqual(source.DATASET_SLUG, "wbd-wbdline")
        self.assertEqual(source.SERVICE_URL,
                         "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer")
        self.assertEqual(source.LAYER_URL, source.SERVICE_URL + "/0")
        self.assertEqual(source.LAYER_NAME, "WBDLine")
        self.assertEqual(source.MAP_NAME, "wbd")
        self.assertNotIn("EXPECTED_COUNT", vars(source))
        self.assertEqual(source.PAGE_SIZE, 1000)
        self.assertEqual(source.DECLARED_MAX_RECORD_COUNT, 2000)
        self.assertNotIn("PAGE_COUNT", vars(source))
        self.assertEqual(source.SERVICE_DOC_URL,
                         "https://www.usgs.gov/national-hydrography/access-national-hydrography-products")
        self.assertEqual(source.EXPECTED_COPYRIGHT_TEXT,
                         "USGS WBD - Watershed Boundary Dataset. Data refreshed October, 2026.")
        self.assertIn("Watershed Boundary Dataset (WBD) was provided by the USDA-NRCS",
                      source.EXPECTED_LAYER_COPYRIGHT_TEXT)
        self.assertEqual(source.EXPECTED_EXTENT["xmin"], -19951755.682)
        self.assertEqual(source.EXPECTED_EXTENT["xmax"], 20021551.7474)
        self.assertEqual(source.EXPECTED_EXTENT, source.EXPECTED_LAYER_EXTENT)
        self.assertEqual([entry for entry in source.EXPECTED_SERVICE_LAYERS if entry[0] == 0],
                         [(0, "WBDLine")])
        self.assertEqual(source.OID_FIELD_NAME, "objectid")
        self.assertEqual(source.DISPLAY_FIELD_NAME, "TNMID")
        self.assertEqual(source.NATIVE_WKID, 102100)
        self.assertEqual(source.SOURCE_CRS_WKID, 3857)
        self.assertEqual([field["name"] for field in source.EXPECTED_PAGE_FIELD_DECLARATIONS],
                         ["objectid", "tnmid", "hudigit", "humod", "linesource", "loaddate",
                          "metasourceid", "shape_Length", "globalid"])
        self.assertEqual(len(source.EXPECTED_LAYER_FIELDS), 10)
        self.assertEqual(source.EXPECTED_LAYER_FIELDS[1]["name"], "shape")
        self.assertEqual(source.EXPECTED_LAYER_FIELDS[1]["type"], "esriFieldTypeGeometry")
        self.assertEqual(source.EXPECTED_LAYER_FIELDS[3]["domain"]["name"], "HUDigit Domain")
        self.assertEqual(source.PUBLISHER,
                         "U.S. Geological Survey, National Geospatial Technical Operations Center")

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(wbd_wbdline, hifld_ingest_assets)
        self.assertEqual(wbd_wbdline_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = wbd_wbdline_complete({"feature_count": 9, "source_reported_count": 9,
                                      "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = wbd_wbdline_complete({"feature_count": 8, "source_reported_count": 9,
                                       "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_roster_exactly_once(self):
        with relaxed_pins():
            urls = dict(source._capture_roster(list(_TEST_ROSTER)))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         ["page_0000", "page_0001", "page_0002"])
        self.assertIn("objectid%3E10", urls["page_0000"])
        self.assertIn("objectid%3E200003", urls["page_0001"])
        self.assertIn("objectid%3E600007", urls["page_0002"])
        self.assertIn("returnZ=true", urls["page_0000"])
        self.assertIn("returnM=true", urls["page_0000"])
        self.assertIn("orderByFields=objectid+ASC", urls["page_0000"])
        self.assertIn("resultRecordCount=1000", source._page_url(0))
        self.assertEqual(urls["ids_before"], source._ids_url())
        self.assertEqual(urls["service_doc"], source.SERVICE_DOC_URL)

    def test_complete_streaming_population_and_baselines_are_validated(self):
        root = self._capture_root(_documents())
        with relaxed_pins():
            audit, derived = source._validate_stream(root, parquet_sink=None)
        self.assertEqual(audit["source_feature_count"], len(_TEST_ROSTER))
        self.assertEqual(audit["source_id_min"], min(_TEST_ROSTER))
        self.assertEqual(audit["source_id_max"], max(_TEST_ROSTER))
        self.assertEqual(audit["source_id_sha256"], _digest(_canonical(list(_TEST_ROSTER))))
        self.assertEqual(audit["null_geometry_count"], len(_GATE_FEATURES))
        self.assertEqual(audit["geometry_valid_count"], len(_TEST_ROSTER) - len(_GATE_FEATURES))
        self.assertEqual(audit["unusable_geometry_counts"], dict(sorted(
            (mode, sum(1 for value in _GATE_FEATURES.values() if value == mode))
            for mode in set(_GATE_FEATURES.values()))))
        self.assertEqual(audit["source_crs"], "EPSG:3857")
        self.assertLess(audit["geometry_bounds"][0], -13_100_000)
        self.assertEqual(sorted(derived["geometry_types"]), ["LineString", "MultiLineString"])
        self.assertEqual(audit["source_ids_member_feature_count"], 1)
        self.assertEqual(audit["source_path_count"],
                         sum(len(_feature_paths(oid)) for oid in _TEST_ROSTER))

    def test_streaming_digests_match_the_pinned_scheme(self):
        root = self._capture_root(_documents())
        with relaxed_pins():
            audit, _derived = source._validate_stream(root, parquet_sink=None)
        expected_record = hashlib.sha256()
        expected_geometry = hashlib.sha256()
        for oid in _TEST_ROSTER:
            expected_record.update(_canonical(_attributes(oid)) + source.DIGEST_SEPARATOR)
            expected_geometry.update(
                _canonical([oid, _feature_paths(oid)]) + source.DIGEST_SEPARATOR)
        self.assertEqual(audit["source_record_sha256"], expected_record.hexdigest())
        self.assertEqual(audit["source_geometry_sha256"], expected_geometry.hexdigest())

    def test_pinned_baselines_and_gate_counts_are_enforced_once_pinned(self):
        documents = _documents()
        root = self._capture_root(documents)
        with relaxed_pins():
            audit, _derived = source._validate_stream(root, parquet_sink=None)
        with relaxed_pins(EXPECTED_SCHEMA_SHA256=audit["source_schema_sha256"]):
            source._validate_stream(root, parquet_sink=None)
        with relaxed_pins(EXPECTED_SCHEMA_SHA256="f" * 64):
            with self.assertRaisesRegex(ValueError, "source_schema_baseline_drift"):
                source._validate_stream(root, parquet_sink=None)
        mutated = json.loads(json.dumps(documents))
        mutated["page_0000"]["features"][0]["attributes"]["tnmid"] = "{RENAMED-GUID}"
        with relaxed_pins():
            edited, _derived = source._validate_stream(self._capture_root(mutated), parquet_sink=None)
        self.assertNotEqual(edited["source_record_sha256"], audit["source_record_sha256"])
        self.assertEqual(edited["source_geometry_sha256"], audit["source_geometry_sha256"])
        path = mutated["page_0000"]["features"][0]["geometry"]["paths"][0]
        path.insert(2, [path[1][0] + (path[2][0] - path[1][0]) / 2,
                        path[1][1] + (path[2][1] - path[1][1]) / 2])
        with relaxed_pins():
            edited, _derived = source._validate_stream(self._capture_root(mutated), parquet_sink=None)
        self.assertNotEqual(edited["source_geometry_sha256"], audit["source_geometry_sha256"])
        self.assertEqual(edited["null_geometry_count"], audit["null_geometry_count"])
        self.assertEqual(edited["unusable_geometry_counts"], audit["unusable_geometry_counts"])
        for name in ("count_before", "count_after"):
            torn = json.loads(json.dumps(documents))
            torn[name]["count"] -= 1
            with relaxed_pins(), self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
                source._validate_stream(self._capture_root(torn), parquet_sink=None)

    def test_gated_polyline_coordinate_drift_is_refused_by_the_geometry_pin(self):
        documents = _documents()
        root = self._capture_root(documents)
        with relaxed_pins():
            audit, _derived = source._validate_stream(root, parquet_sink=None)
        degenerate = next(oid for oid, mode in _GATE_FEATURES.items()
                          if mode == "source_polyline_path_degenerate")
        mutated = json.loads(json.dumps(documents))
        row = next(feature for feature in mutated["page_0000"]["features"]
                   if feature["attributes"]["objectid"] == degenerate)
        row["geometry"]["paths"][0][0][0] += 500.0
        with relaxed_pins():
            drifted, _derived = source._validate_stream(
                self._capture_root(mutated), parquet_sink=None)
        self.assertEqual(drifted["source_record_sha256"], audit["source_record_sha256"])
        self.assertEqual(drifted["unusable_geometry_counts"],
                         audit["unusable_geometry_counts"])
        self.assertEqual(drifted["null_geometry_count"], audit["null_geometry_count"])
        self.assertNotEqual(drifted["source_geometry_sha256"], audit["source_geometry_sha256"])
        self.assertEqual(drifted["source_reported_count"], len(_TEST_ROSTER))

    def test_refused_window_recursive_bisection_streams_and_replays_to_the_same_population(self):
        documents = _documents()
        roster = list(_TEST_ROSTER)
        refusal = b'{"error": {"code": 500, "message": "Error performing query operation", ' \
                  b'"details": []}}'
        # page_0000 (ids 11, 12, 501, 100_003) refuses nominally; its first half
        # refuses again; the two single-feature leaves and the second half serve.
        first, second, third, fourth = documents["page_0000"]["features"]
        documents["page_0000_refusal"] = refusal
        documents["page_0000a_refusal"] = refusal
        documents["page_0000aa"] = {**documents["page_0000"], "features": [first],
                                    "exceededTransferLimit": True}
        documents["page_0000ab"] = {**documents["page_0000"], "features": [second],
                                    "exceededTransferLimit": None}
        documents["page_0000b"] = {**documents["page_0000"], "features": [third, fourth],
                                   "exceededTransferLimit": False}
        del documents["page_0000"]
        temporary = tempfile.TemporaryDirectory(prefix="wbd-wbdline-bisect-")
        self.addCleanup(temporary.cleanup)
        page_parts = {"page_0000": ["page_0000aa", "page_0000ab", "page_0000b"]}
        with relaxed_pins():
            root = _write_capture_root(Path(temporary.name) / "captures", documents)
            audit, _derived = source._validate_stream(root, parquet_sink=None,
                                                      page_parts=page_parts)
        self.assertEqual(audit["source_feature_count"], len(roster))
        self.assertEqual(audit["source_id_sha256"], _digest(_canonical(roster)))
        expected_record, expected_geometry = hashlib.sha256(), hashlib.sha256()
        for oid in roster:
            expected_record.update(_canonical(_attributes(oid)) + source.DIGEST_SEPARATOR)
            expected_geometry.update(
                _canonical([oid, _feature_paths(oid)]) + source.DIGEST_SEPARATOR)
        self.assertEqual(audit["source_record_sha256"], expected_record.hexdigest())
        self.assertEqual(audit["source_geometry_sha256"], expected_geometry.hexdigest())
        self.assertEqual(audit["source_ids_member_feature_count"], 1)
        self.assertEqual(audit["page_parts"], dict(sorted(page_parts.items())))
        _seal_manifest(root, documents, page_parts=page_parts,
                       page_refusals=["page_0000_refusal", "page_0000a_refusal"])
        with relaxed_pins(DISK_FLOOR_BYTES=1):
            captures, replay_parts = source._read_replay(root)
            self.assertEqual(replay_parts, page_parts)
            self.assertEqual(set(captures), set(documents))
            audit, _derived = source._validate_stream(root, parquet_sink=None,
                                                      page_parts=replay_parts)
            self.assertEqual(audit["source_feature_count"], len(roster))
        mutated = json.loads(json.dumps({name: value for name, value in documents.items()
                                         if not isinstance(value, bytes)}))
        mutated["page_0000aa"]["features"][0]["attributes"]["tnmid"] = "{RENAMED-GUID}"
        with relaxed_pins():
            edited, _derived = source._validate_stream(self._capture_root(mutated),
                parquet_sink=None, page_parts=page_parts)
        self.assertNotEqual(edited["source_record_sha256"], expected_record.hexdigest())
        self.assertEqual(edited["source_geometry_sha256"], expected_geometry.hexdigest())

    def test_recursive_window_derivation_and_fail_closed_single_refusal(self):
        roster = list(_TEST_ROSTER)
        with relaxed_pins():
            self.assertEqual(source._slice_for_suffix(roster, 0, ""), roster[:4])
            self.assertEqual(source._slice_for_suffix(roster, 0, "a"), roster[:2])
            self.assertEqual(source._slice_for_suffix(roster, 0, "aa"), [roster[0]])
            self.assertEqual(source._slice_for_suffix(roster, 2, ""), [roster[8]])
            url = source._window_url(roster, 0, "ab")
        self.assertIn("objectid%3E11", url)
        self.assertIn("resultRecordCount=1", url)

        refusal = b'{"error": {"code": 500, "message": "Error performing query operation", ' \
                  b'"details": []}}'

        def refusing_capture_get(client, url, root, name, max_bytes=0):
            (root / f"{name}.body").write_bytes(refusal)
            (root / f"{name}.wire").write_bytes(refusal)
            receipt = {"method": "GET", "url": url, "final_url": url,
                       "observed_at": "2026-11-15T12:00:00.000000Z", "status": 200,
                       "complete_body": True, "bytes": len(refusal),
                       "sha256": _digest(refusal), "wire_bytes": 21,
                       "wire_sha256": _digest(refusal),
                       "content_encoding": None, "content_length": None,
                       "error_code": None, "error_type": None}
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return refusal, receipt

        temporary = tempfile.TemporaryDirectory(prefix="wbd-wbdline-floor-")
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "captures"
        root.mkdir(parents=True)
        with relaxed_pins(), patches(source, capture_get=refusing_capture_get):
            with self.assertRaisesRegex(ValueError, "source_page_window_refused_single_feature"
                                                     ":page_0000aa:objectid=11"):
                source._capture_page_window(None, roster, 0, root)
        for name in ("page_0000_refusal", "page_0000a_refusal", "page_0000aa_refusal"):
            self.assertTrue((root / f"{name}.body").is_file(), name)
            self.assertTrue(source._is_refusal((root / f"{name}.body").read_bytes()), name)

    def test_wkb_preserves_path_structure_and_declared_types(self):
        single = source._assemble_geometry(_feature_paths(11))
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 2))
        self.assertIsInstance(from_wkb(single), LineString)
        multi = source._assemble_geometry(_feature_paths(12))
        self.assertEqual(struct.unpack("<BI", multi[:5]), (1, 5))
        self.assertEqual(len(from_wkb(multi).geoms), 2)
        for paths, mode in (
                ([], "source_polyline_paths_missing"),
                ([[_line(1)[0]]], "source_polyline_path_degenerate")):
            with self.assertRaisesRegex(Exception, mode):
                source._assemble_geometry(paths)
        self.assertIn("source_polyline_assembly_invalid", source.GATE_MODES)

    def test_replay_rejects_roster_drift_and_honors_retained_receipts(self):
        documents = _documents()
        roster = list(_TEST_ROSTER)
        temporary = tempfile.TemporaryDirectory(prefix="wbd-wbdline-replay-")
        self.addCleanup(temporary.cleanup)
        with relaxed_pins():
            root = _write_capture_root(Path(temporary.name) / "captures", documents)
        _seal_manifest(root, documents)
        with relaxed_pins(DISK_FLOOR_BYTES=1):
            captures, replay_parts = source._read_replay(root)
            self.assertEqual(set(captures), set(documents))
            self.assertEqual(replay_parts, {})
            audit, _derived = source._validate_stream(root, parquet_sink=None)
            self.assertEqual(audit["source_id_sha256"], _digest(_canonical(roster)))
        mutated = json.loads(json.dumps(documents))
        mutated["ids_after"]["objectIds"] = [11, 12, 999, 100_003, 200_004, 300_005, 400_006, 500_007, 600_008]
        with relaxed_pins():
            with self.assertRaisesRegex(ValueError, "source_id_roster_changed_during_capture"):
                source._validate_stream(self._capture_root(mutated), parquet_sink=None)

    def test_population_schema_transfer_crs_alias_and_parity_drift_fail_closed(self):
        documents = _documents()
        cases = [
            ("page error", lambda d: d["page_0000"].update(error={"code": 500}),
             "source_page_error"),
            ("page extra member", lambda d: d["page_0000"].update(extraKey=1),
             "source_page_unexpected_member"),
            ("page display field", lambda d: d["page_0000"].update(displayFieldName="TNMID2"),
             "source_page_identity_drift"),
            ("transfer flag", lambda d: d["page_0000"].update(exceededTransferLimit="yes"),
             "source_page_transfer_flag_drift"),
            ("page population", lambda d: d["page_0000"]["features"].pop(),
             "source_page_population_incomplete"),
            ("transfer limit on final page", lambda d: d["page_0002"].update(exceededTransferLimit=True),
             "source_page_transfer_limit_on_final_page"),
            ("page crs", lambda d: d["page_0000"].update(spatialReference={"wkid": 4326}),
             "source_page_0000_page_crs_drift"),
            ("page alias", lambda d: d["page_0000"].update(
                fieldAliases={**d["page_0000"]["fieldAliases"], "humod": "HUM"}),
             "source_page_alias_drift"),
            ("page field drift", lambda d: d["page_0000"]["fields"][1].update(name="renamed"),
             "source_page_physical_schema_changed"),
            ("page geometry type", lambda d: d["page_0000"].update(geometryType="esriGeometryPoint"),
             "source_page_identity_drift"),
            ("attribute order", lambda d: d["page_0000"]["features"][0]["attributes"].pop("tnmid"),
             "source_attribute_roster_or_order_drift"),
            ("extra attribute", lambda d: d["page_0000"]["features"][0]["attributes"].update(EXTRA="x"),
             "source_attribute_roster_or_order_drift"),
            ("oid value", lambda d: d["page_0000"]["features"][0]["attributes"].update(objectid=0),
             "source_oid_value_type_drift"),
            ("oid roster", lambda d: d["page_0000"]["features"][0]["attributes"].update(objectid=999),
             "source_feature_id_roster_drift"),
            ("id order", lambda d: d["page_0000"]["features"].reverse(),
             "source_feature_ids_not_strictly_increasing"),
            ("string value", lambda d: d["page_0000"]["features"][0]["attributes"].update(humod=5),
             "source_string_value_type_drift:humod"),
            ("date value", lambda d: d["page_0000"]["features"][0]["attributes"].update(loaddate="now"),
             "source_date_value_type_drift:loaddate"),
            ("double value", lambda d: d["page_0000"]["features"][0]["attributes"].update(shape_Length="x"),
             "source_double_value_type_drift:shape_Length"),
            ("length parity", lambda d: d["page_0000"]["features"][0]["attributes"].update(shape_Length=1.0),
             "source_shape_Length_parity_drift"),
            ("outside coverage", lambda d: d["page_0000"]["features"][1]["geometry"]["paths"][0].__setitem__(
                1, [22_000_000.0, 3_984_685.0]), "source_geometry_outside_declared_coverage"),
            ("geometry extra key", lambda d: d["page_0000"]["features"][0]["geometry"].update(z=1),
             "source_polyline_geometry_structure_drift"),
            ("geometry ids member drift", lambda d: d["page_0000"]["features"][0]["geometry"].update(
                ids=[[1, 2, 3, 4, 5]]), "source_geometry_ids_member_drift"),
            ("geometry ids wrong arity", lambda d: d["page_0000"]["features"][0]["geometry"].update(
                ids=[[1, 2], [3]]), "source_geometry_ids_member_drift"),
            ("geometry z flag", lambda d: d["page_0000"]["features"][0]["geometry"].update(hasZ=True),
             "source_geometry_dimensional_drift"),
            ("paths key missing", lambda d: d["page_0000"]["features"][0]["geometry"].update(
                {"paths": None}), "source_polyline_paths_missing"),
            ("degenerate path is gated not fatal", lambda d: d["page_0000"]["features"][0]["geometry"].update(
                {"paths": [[[-13_000_000.0, 3_984_685.0]]]}), None),
            ("schema", lambda d: d["layer_before"]["fields"][1].update(name="renamed"),
             "source_field_schema_drift"),
            ("layer identity", lambda d: d["layer_before"].update(name="Other"),
             "source_layer_identity_or_extent_drift"),
            ("layer extent", lambda d: d["layer_before"]["extent"].update(xmax=0.0),
             "source_layer_identity_or_extent_drift"),
            ("layer type field", lambda d: d["layer_before"].update(typeIdField="regiontype"),
             "source_layer_identity_or_extent_drift"),
            ("layer geometry props", lambda d: d["layer_before"]["geometryProperties"].update(units="esriFeet"),
             "source_layer_identity_or_extent_drift"),
            ("layer crs present", lambda d: d["layer_before"].update(spatialReference={"wkid": 4326}),
             "source_layer_crs_drift"),
            ("layer versioned", lambda d: d["layer_before"].update(isDataVersioned=True),
             "source_layer_identity_or_extent_drift"),
            ("count before", lambda d: d["count_before"].update(count=_TEST_COUNT - 1),
             "source_count_or_id_roster_drift"),
            ("ids before", lambda d: d["ids_before"].update(objectIds=_TEST_ROSTER[:-1]),
             "source_id_roster_changed_during_capture"),
            ("ids changed mid capture", lambda d: d["ids_after"].update(objectIds=[11, 12, 999, 100_003, 200_004, 300_005, 400_006, 500_007, 600_008]),
             "source_id_roster_changed_during_capture"),
            ("service identity", lambda d: d["service_before"].update(copyrightText="changed"),
             "service_before_service_identity_drift"),
            ("service extent", lambda d: d["service_before"]["fullExtent"].update(xmin=0.0),
             "service_before_service_extent_drift"),
            ("service roster", lambda d: d["service_before"]["layers"].pop(),
             "service_before_service_layer_roster_drift"),
            ("service changed during capture", lambda d: d["service_after"].update(mapName="other"),
             "source_service_or_layer_changed_during_capture"),
            ("layer colorramp nondeterminism is not a data change", lambda d: d["layer_after"]
             ["drawingInfo"]["renderer"]["authoringInfo"].update(
                 colorRamp={"type": "multipart",
                            "colorRamps": [{"type": "algorithmic",
                                            "algorithm": "esriCIELabAlgorithm",
                                            "fromColor": [1, 2, 3, 255],
                                            "toColor": [4, 5, 6, 255]}]}), None),
            ("service doc drift", lambda d: d.update(service_doc="<html>unrelated</html>"),
             "usgs_service_documentation_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(documents))
                mutate(docs)
                with relaxed_pins():
                    if expected is None:
                        audit, _derived = source._validate_stream(
                            self._capture_root(docs), parquet_sink=None)
                        self.assertEqual(audit["source_feature_count"], _TEST_COUNT)
                    else:
                        with self.assertRaisesRegex(ValueError, expected):
                            source._validate_stream(self._capture_root(docs), parquet_sink=None)

    def test_native_parquet_round_trips_native_schema_and_wkb(self):
        documents = _documents()
        root = self._capture_root(documents)
        temporary = tempfile.TemporaryDirectory(prefix="wbd-wbdline-parquet-")
        self.addCleanup(temporary.cleanup)
        sink = Path(temporary.name) / "wbd-wbdline.parquet"
        with relaxed_pins():
            audit, derived = source._validate_stream(root, parquet_sink=sink)
        self.assertTrue(sink.exists())
        table = pq.read_table(sink)
        self.assertEqual(table.num_rows, len(_TEST_ROSTER))
        self.assertEqual(table.schema.metadata[b"geo"], _canonical({
            "version": "1.1.0", "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB",
                                     "geometry_types": ["LineString", "MultiLineString"],
                                     "crs": source._EPSG3857_PROJJSON, "edges": "planar"}}}))
        self.assertEqual(table.column("objectid").to_pylist(), _TEST_ROSTER)
        for position, oid in enumerate(_TEST_ROSTER):
            expected = _attributes(oid)
            for field in source.ATTRIBUTE_FIELDS:
                name = field["name"]
                self.assertEqual(table.column(name)[position].as_py(), expected[name])
            wkb = table.column("geometry")[position].as_py()
            if oid in _GATE_FEATURES:
                self.assertIsNone(wkb)
            else:
                decoded = from_wkb(wkb)
                reference = from_wkb(source._assemble_geometry(_feature_paths(oid)))
                self.assertEqual(decoded.length, reference.length)
                self.assertEqual(decoded.wkt, reference.wkt)
        self.assertEqual(derived["parquet_path"], sink)

    def test_unsafe_capture_version_is_refused(self):
        temporary = tempfile.TemporaryDirectory(prefix="wbd-wbdline-version-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True,
                                         local_dir=str(Path(temporary.name) / "stage"))
        with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
            source.capture_wbd_wbdline(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()
