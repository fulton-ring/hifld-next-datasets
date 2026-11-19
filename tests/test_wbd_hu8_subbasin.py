"""Focused tests for the USGS WBD 8-digit HU (Subbasin) native capture draft."""

import json
import gzip
import os
import shutil
import tempfile
import tracemalloc
import unittest
from urllib.parse import parse_qs
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import httpx
from dagster import DefaultScheduleStatus
from shapely import from_wkb

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.wbd_hu8_subbasin import (
    wbd_hu8_subbasin, wbd_hu8_subbasin_complete, wbd_hu8_subbasin_draft_schedule,
    wbd_hu8_subbasin_drift,
)
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import wbd_hu8_subbasin as source
from dagster_hifld.sources import capture_common
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.validation import sha256_file

FIXTURE_COUNT = 8
FIXTURE_PAGE_SIZE = 3
FIXTURE_ATTRS_PAGE_SIZE = 5
FIXTURE_ATTRS_OFFSETS = [0, 5]
FAKE_IDENTITY = {"code_identity_scheme": "test-scheme", "working_tree_status": "test",
                 "files": {}, "installed_distributions": [], "python": "3.12",
                 "pyarrow": "0", "proj": "0", "geos": "0", "code_commit": "test-commit"}


def _shell(oid):
    x0 = 0.1 + (oid % 4) * 0.5
    y0 = 40.0 + ((oid // 4) % 80) * 0.5
    return [[x0, y0], [x0, y0 + 0.1], [x0 + 0.1, y0 + 0.1], [x0 + 0.1, y0], [x0, y0]]


def _hole(shell):
    inset = 0.02
    return [[shell[0][0] + inset, shell[0][1] + inset],
            [shell[2][0] - inset, shell[0][1] + inset],
            [shell[2][0] - inset, shell[2][1] - inset],
            [shell[0][0] + inset, shell[2][1] - inset],
            [shell[0][0] + inset, shell[0][1] + inset]]


def _rings_for(oid):
    if oid == 5:
        return [_shell(oid), _shell(oid + 20)]
    if oid == 6:
        return [_shell(oid), _hole(_shell(oid))]
    return [_shell(oid)]


def _attrs(oid):
    return {"objectid": oid, "tnmid": f"TMD{oid:08d}", "metasourceid": f"MS{oid:08d}",
            "sourcedatadesc": "Source desc", "sourceoriginator": "USGS",
            "sourcefeatureid": None,
            "loaddate": None if oid == 7 else 1700000000000 + oid,
            "referencegnis_ids": "1,2", "areaacres": float(oid) * 1000.0,
            "areasqkm": float(oid) * 4.047, "states": "AB",
            "huc8": f"{oid:08d}", "name": f"Subbasin {oid}",
            "globalid": "{" + f"{oid:032x}" + "}",
            "shape_Length": 1.0 + oid / 100.0, "shape_Area": 2.0 + oid / 100.0}


def _service_doc():
    return {"copyrightText": "test-service-refresh-statement",
            "capabilities": source.EXPECTED_SERVICE_CAPABILITIES,
            "supportedQueryFormats": source.EXPECTED_SERVICE_QUERY_FORMATS,
            "maxRecordCount": source.EXPECTED_MAX_RECORD_COUNT,
            "currentVersion": 11.3,
            "spatialReference": {"wkid": 102100, "latestWkid": 3857},
            "layers": [{"id": layer_id, "name": layer_name, "type": "Feature Layer",
                        "parentLayerId": -1}
                       for layer_id, layer_name in source.EXPECTED_SERVICE_LAYERS]}


def _layer_doc():
    return {"id": source.LAYER_ID, "name": source.LAYER_NAME, "type": "Feature Layer",
            "geometryType": "esriGeometryPolygon",
            "maxRecordCount": source.EXPECTED_MAX_RECORD_COUNT,
            "displayField": source.EXPECTED_DISPLAY_FIELD,
            "capabilities": source.EXPECTED_SERVICE_CAPABILITIES,
            "supportedQueryFormats": source.EXPECTED_SERVICE_QUERY_FORMATS,
            "copyrightText": source.LAYER_COPYRIGHT_TEXT,
            "geometryProperties": dict(source.EXPECTED_GEOMETRY_PROPERTIES),
            "extent": {"xmin": -1.0, "ymin": 39.0, "xmax": 3.0, "ymax": 43.0,
                       "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
            "advancedQueryCapabilities": {"supportsPagination": True},
            "fields": [dict(field) for field in source.EXPECTED_FIELD_DECLARATIONS]}


def _page_features(offset, count, with_geometry):
    features = []
    for oid in range(offset + 1, offset + count + 1):
        feature = {"attributes": _attrs(oid)}
        if with_geometry:
            feature["geometry"] = {"rings": _rings_for(oid)}
        features.append(feature)
    return features


def _page_doc(offset, count, total, with_geometry, page_size):
    exceeded = offset + count < total
    doc = {"displayFieldName": "NAME",
           "fieldAliases": {field["name"]: field["alias"]
                            for field in source.EXPECTED_FIELD_DECLARATIONS},
           "fields": [field for field in source.EXPECTED_FIELD_DECLARATIONS
                      if field["type"] != "esriFieldTypeGeometry"],
           "geometryType": "esriGeometryPolygon",
           "spatialReference": {"wkid": 4326, "latestWkid": 4326},
           "features": _page_features(offset, count, with_geometry)}
    if with_geometry:
        doc["objectIdFieldName"] = "objectid"
    if exceeded:
        doc["exceededTransferLimit"] = True
    return doc


def _retained_receipt(url, body, wire):
    return {"method": "GET", "url": url, "final_url": url,
            "observed_at": "2026-11-14T00:00:00+00:00", "status": 200,
            "complete_body": True, "bytes": len(body), "sha256": _sha(body),
            "wire_bytes": len(wire), "wire_sha256": _sha(wire),
            "content_encoding": None, "content_length": None}


def _write_capture(root, name, url, document):
    # The producer serializes JSON with attribute members in declared field
    # order; the fixture body preserves that natural order (no key sorting).
    body = json.dumps(document, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")
    wire = body[::-1]
    (root / f"{name}.body").write_bytes(body)
    (root / f"{name}.wire").write_bytes(wire)
    (root / f"{name}.receipt.json").write_bytes(
        _canonical(_retained_receipt(url, body, wire)) + b"\n")


def _fixture_urls():
    urls = dict(source._doc_urls(FIXTURE_COUNT))
    for offset in range(0, FIXTURE_COUNT, FIXTURE_PAGE_SIZE):
        count = min(FIXTURE_PAGE_SIZE, FIXTURE_COUNT - offset)
        urls[source._segment_name(offset, count)] = source._segment_url(offset, count)
    return urls


def _build_fixture_capture(root):
    root.mkdir(parents=True, exist_ok=True)
    urls = _fixture_urls()
    docs = {"service_before": _service_doc(), "service_after": _service_doc(),
            "layer_before": _layer_doc(), "layer_after": _layer_doc(),
            "count_before": {"count": FIXTURE_COUNT},
            "count_after": {"count": FIXTURE_COUNT},
            "ids_before": {"objectIdFieldName": "objectid",
                           "objectIds": list(range(1, FIXTURE_COUNT + 1))},
            "ids_after": {"objectIdFieldName": "objectid",
                          "objectIds": list(range(1, FIXTURE_COUNT + 1))}}
    for index, offset in enumerate(FIXTURE_ATTRS_OFFSETS):
        count = min(FIXTURE_ATTRS_PAGE_SIZE, FIXTURE_COUNT - offset)
        docs[f"attrs_{offset:05d}"] = _page_doc(offset, count, FIXTURE_COUNT,
                                                with_geometry=False,
                                                page_size=FIXTURE_ATTRS_PAGE_SIZE)
    segments = []
    for offset in range(0, FIXTURE_COUNT, FIXTURE_PAGE_SIZE):
        count = min(FIXTURE_PAGE_SIZE, FIXTURE_COUNT - offset)
        name = source._segment_name(offset, count)
        docs[name] = _page_doc(offset, count, FIXTURE_COUNT, with_geometry=True,
                               page_size=FIXTURE_PAGE_SIZE)
        segments.append({"offset": offset, "count": count, "name": name,
                         "features": count, "captured_at": "2026-11-14T00:00:00+00:00"})
    for name, url in urls.items():
        _write_capture(root, name, url, docs[name])
    (root / "capture_state.json").write_bytes(_canonical(
        {"segments": segments, "complete": True}) + b"\n")
    return urls, docs


_PATCHES = (
    patch.object(source, "PAGE_SIZE", FIXTURE_PAGE_SIZE),
    patch.object(source, "ATTRS_PAGE_SIZE", FIXTURE_ATTRS_PAGE_SIZE),
    patch.object(source, "EXPECTED_SCHEMA_SHA256", ""),
    patch.object(source, "EXPECTED_INVALID_OBJECTIDS", []),
    patch.object(source, "EXPECTED_INVALID_VALIDITY_EXPLANATIONS", {}),
    patch.object(source, "EXPECTED_COPYRIGHT_TEXT", "test-service-refresh-statement"),
    patch.object(source, "MIN_FREE_BYTES", 1_000_000),
)


class _OfflineClient:
    def __init__(self, *_args, **_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def get(self, *_args, **_kwargs):
        raise AssertionError("offline fixture replay must not fetch")

    def stream(self, *_args, **_kwargs):
        raise AssertionError("offline fixture replay must not fetch")


class _ContextClient:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _fixture_doc_bodies():
    docs = {"service_before": _service_doc(), "layer_before": _layer_doc(),
            "count_before": {"count": FIXTURE_COUNT},
            "ids_before": {"objectIdFieldName": "objectid",
                           "objectIds": list(range(1, FIXTURE_COUNT + 1))}}
    docs["service_after"] = _service_doc()
    docs["layer_after"] = _layer_doc()
    docs["count_after"] = {"count": FIXTURE_COUNT}
    docs["ids_after"] = dict(docs["ids_before"])
    for offset in FIXTURE_ATTRS_OFFSETS:
        count = min(FIXTURE_ATTRS_PAGE_SIZE, FIXTURE_COUNT - offset)
        docs[f"attrs_{offset:05d}"] = _page_doc(offset, count, FIXTURE_COUNT,
                                                with_geometry=False,
                                                page_size=FIXTURE_ATTRS_PAGE_SIZE)
    return docs


def _fixture_segment_bodies():
    bodies = {}
    for offset in range(0, FIXTURE_COUNT, FIXTURE_PAGE_SIZE):
        count = min(FIXTURE_PAGE_SIZE, FIXTURE_COUNT - offset)
        bodies[source._segment_name(offset, count)] = _page_doc(
            offset, count, FIXTURE_COUNT, with_geometry=True,
            page_size=FIXTURE_PAGE_SIZE)
    return bodies


def _segment_names():
    return [source._segment_name(offset, min(FIXTURE_PAGE_SIZE, FIXTURE_COUNT - offset))
            for offset in range(0, FIXTURE_COUNT, FIXTURE_PAGE_SIZE)]


class WbdHu8SubbasinTests(unittest.TestCase):
    def _transport(self, roster, mutate=None, encoding="identity"):
        calls = []
        visits = {}

        def respond(request):
            calls.append(str(request.url))
            query = parse_qs(request.url.query.decode())
            if request.url.path == httpx.URL(source.SERVICE_URL).path:
                name, document = "service", _service_doc()
                document["copyrightText"] = source.EXPECTED_COPYRIGHT_TEXT
            elif query.get("f") == ["pjson"]:
                name, document = "layer", _layer_doc()
            elif query.get("returnCountOnly") == ["true"]:
                name, document = "count", {"count": len(roster)}
            elif query.get("returnIdsOnly") == ["true"]:
                name, document = "ids", {"objectIdFieldName": "objectid", "objectIds": roster}
            else:
                offset = int(query["resultOffset"][0])
                count = min(int(query["resultRecordCount"][0]), len(roster) - offset)
                geometry = query["returnGeometry"] == ["true"]
                name = source._segment_name(offset, count) if geometry else f"attrs_{offset:05d}"
                document = _page_doc(offset, count, len(roster), geometry, count)
                for feature, oid in zip(document["features"], roster[offset:offset + count]):
                    feature["attributes"] = _attrs(oid)
                    if geometry:
                        feature["geometry"] = {"rings": _rings_for(oid)}
            visit = visits.get(name, 0)
            visits[name] = visit + 1
            if mutate:
                mutate(name, visit, document)
            body = json.dumps(document, separators=(",", ":"), allow_nan=False).encode()
            wire = gzip.compress(body, mtime=0) if encoding == "gzip" else body
            return httpx.Response(200, headers={"Content-Encoding": encoding,
                "Content-Length": str(len(wire))}, stream=httpx.ByteStream(wire))

        return httpx.MockTransport(respond), calls

    def test_real_retained_segment_request_identity_is_bound(self):
        root = Path(self._tmpdir().name) / "captures"
        transport, calls = self._transport(list(range(1, FIXTURE_COUNT + 1)))
        with self._patched(), patch.object(source, "_default_client",
                side_effect=lambda: httpx.Client(transport=transport)):
            source.fetch_wbd_hu8_subbasin_pages(root)
            self.assertTrue(calls)
            receipt_path = root / (source._segment_name(0, FIXTURE_PAGE_SIZE) + ".receipt.json")
            receipt = json.loads(receipt_path.read_bytes())
            receipt["url"] = receipt["final_url"] = "https://example.invalid/substituted"
            receipt_path.write_bytes(_canonical(receipt))
            with self.assertRaisesRegex(ValueError, "request_identity"):
                source._stream_validate(root, parquet_sink=None)

    def test_interrupted_resumed_attrs_keep_closing_witnesses_pending(self):
        root = Path(self._tmpdir().name) / "captures"
        roster = list(range(1, FIXTURE_COUNT + 1))
        transport, _calls = self._transport(roster)
        with self._patched(), patch.object(source, "_default_client",
                side_effect=lambda: httpx.Client(transport=transport)):
            source.fetch_wbd_hu8_subbasin_pages(root)
        state = json.loads((root / "capture_state.json").read_bytes())
        removed = state["segments"].pop(0)
        (root / "capture_state.json").write_bytes(_canonical(state))
        for name in (removed["name"], *(f"attrs_{offset:05d}" for offset in FIXTURE_ATTRS_OFFSETS)):
            for suffix in ("body", "wire", "receipt.json"):
                (root / f"{name}.{suffix}").unlink()

        def interrupt(name, _visit, _document):
            if name == "attrs_00000":
                raise httpx.ConnectError("fixture interruption")

        transport, _calls = self._transport(roster, interrupt)
        with self._patched(patch.object(source, "CAPTURE_ATTEMPTS", 1)), patch.object(
                source, "_default_client", side_effect=lambda: httpx.Client(transport=transport)):
            with self.assertRaises(ValueError):
                source.fetch_wbd_hu8_subbasin_pages(root)
        self.assertFalse(json.loads((root / "capture_state.json").read_bytes())["complete"])
        self.assertTrue((root / "attrs_00000.attempt-01.receipt.json").exists())
        transport, calls = self._transport(roster)
        with self._patched(), patch.object(source, "_default_client",
                side_effect=lambda: httpx.Client(transport=transport)):
            source.fetch_wbd_hu8_subbasin_pages(root)
            self.assertEqual(calls[-4:], [str(httpx.URL(url)) for _name, url in source._after_doc_urls()])
            source._stream_validate(root, parquet_sink=None)
        for suffix in ("body", "wire", "receipt.json"):
            (root / f"attrs_00000.{suffix}").unlink()
        transport, calls = self._transport(roster)
        with self._patched(), patch.object(source, "_default_client",
                side_effect=lambda: httpx.Client(transport=transport)):
            source.fetch_wbd_hu8_subbasin_pages(root)
            self.assertEqual(calls[-4:], [str(httpx.URL(url)) for _name, url in source._after_doc_urls()])

    def _real_capture(self, storage, root, roster, mutate=None, encoding="identity"):
        transport, calls = self._transport(roster, mutate, encoding)
        with patch.object(source, "PAGE_SIZE", FIXTURE_PAGE_SIZE), \
                patch.object(source, "ATTRS_PAGE_SIZE", FIXTURE_ATTRS_PAGE_SIZE), \
                patch.object(source, "MIN_FREE_BYTES", 1), \
                patch.object(source, "_default_client", side_effect=lambda: httpx.Client(transport=transport)):
            result = source.capture_wbd_hu8_subbasin(storage, version="observed",
                                                    capture_directory=str(root))
        return result, calls

    def _real_replay(self, storage, root):
        def refuse(_request):
            raise AssertionError("network request during replay")
        with patch.object(source, "PAGE_SIZE", FIXTURE_PAGE_SIZE), \
                patch.object(source, "ATTRS_PAGE_SIZE", FIXTURE_ATTRS_PAGE_SIZE), \
                patch.object(source, "MIN_FREE_BYTES", 1), \
                patch.object(source, "_default_client", side_effect=lambda: httpx.Client(
                    transport=httpx.MockTransport(refuse))):
            return source.capture_wbd_hu8_subbasin(storage, version="replay",
                                                   capture_directory=str(root))

    def test_real_population_refresh_replay_and_immutable_originals(self):
        tmp = Path(self._tmpdir().name)
        storage = self._storage(tmp)
        versions = set()
        for index, roster in enumerate(([1, 2, 5, 6, 7, 220, 897, 2001],
                                        [2, 7, 220, 2001],
                                        [1, 2, 5, 6, 7, 220, 897, 2001, 3001, 4001, 5001])):
            with self.subTest(roster=roster):
                root = tmp / f"capture-{index}"
                result, calls = self._real_capture(storage, root, roster)
                self.assertFalse(result["reused"])
                self.assertEqual(result["feature_count"], len(roster))
                self.assertEqual(result["source_reported_count"], len(roster))
                self.assertEqual(result["source_id_sha256"], _sha(_canonical(roster)))
                self.assertEqual((result["source_id_min"], result["source_id_max"]),
                                 (min(roster), max(roster)))
                self.assertTrue(wbd_hu8_subbasin_complete(result).passed)
                prefix = Path(storage.local_dir) / source.DATASET_SLUG / source.FILE_SLUG / result["version"]
                parquet = pq.read_table(prefix / "geoparquet" / (source.FILE_SLUG + ".parquet"))
                self.assertEqual(parquet["objectid"].to_pylist(), list(roster))
                self.assertEqual(parquet.column_names, [*source.ATTRIBUTE_NAMES, "geometry"])
                for column, expected in _attrs(roster[0]).items():
                    self.assertEqual(parquet[column][0].as_py(), expected)
                self.assertEqual(parquet["geometry"][0].as_py(), source._esri_polygon_wkb(
                    {"rings": _rings_for(roster[0])}))
                self.assertEqual(json.loads(parquet.schema.metadata[b"geo"])["columns"]["geometry"]["crs"]["id"]["code"], 4326)
                query_calls = [parse_qs(httpx.URL(url).query.decode()) for url in calls]
                attrs_offsets = [int(q["resultOffset"][0]) for q in query_calls
                                 if q.get("returnGeometry") == ["false"]]
                self.assertEqual(attrs_offsets, list(range(0, len(roster), FIXTURE_ATTRS_PAGE_SIZE)))
                original = {path.relative_to(prefix): path.read_bytes()
                            for path in prefix.rglob("*") if path.is_file()}
                replay = self._real_replay(storage, root)
                self.assertTrue(replay["reused"])
                self.assertEqual(replay["source_reported_count"], len(roster))
                self.assertEqual(replay["version"], result["version"])
                fresh_root = tmp / f"refresh-{index}"
                refresh, _calls = self._real_capture(storage, fresh_root, roster)
                self.assertTrue(refresh["reused"])
                self.assertEqual(refresh["version"], result["version"])
                self.assertNotEqual((root / "ids_before.receipt.json").read_bytes(),
                                    (fresh_root / "ids_before.receipt.json").read_bytes())
                self.assertEqual(original, {path.relative_to(prefix): path.read_bytes()
                                           for path in prefix.rglob("*") if path.is_file()})
                self.assertFalse(result["geometry_repair_accepted"])
                self.assertFalse(result["release_accepted"])
                versions.add(result["version"])
        self.assertEqual(len(versions), 3)
        drift = wbd_hu8_subbasin_drift(result)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.severity.value, "WARN")
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        def edit(_name, _visit, document):
            for feature in document.get("features", []):
                if feature["attributes"]["objectid"] == roster[0]:
                    feature["attributes"]["name"] = "Maintained publisher edit"
        changed, _calls = self._real_capture(storage, tmp / "changed", roster, edit)
        self.assertFalse(changed["reused"])
        self.assertNotEqual(changed["version"], result["version"])
        self.assertEqual(original, {path.relative_to(prefix): path.read_bytes()
                                   for path in prefix.rglob("*") if path.is_file()})
        cached_raw = prefix / "raw" / (source._segment_name(0, FIXTURE_PAGE_SIZE) + ".wire")
        cached_raw.write_bytes(b"corrupt cached original")
        with self.assertRaises(ValueError):
            self._real_replay(storage, root)
        self.assertEqual(cached_raw.read_bytes(), b"corrupt cached original")

    def test_real_identity_and_gzip_replay_reject_cross_path_decoded_body_forgery(self):
        for encoding in ("identity", "gzip"):
            with self.subTest(encoding=encoding):
                tmp = Path(self._tmpdir().name)
                storage = self._storage(tmp)
                root = tmp / "captures"
                result, _calls = self._real_capture(storage, root, list(range(1, 9)), encoding=encoding)
                replay = self._real_replay(storage, root)
                self.assertTrue(replay["reused"])
                self.assertEqual(replay["version"], result["version"])
                def staged_inventory():
                    stage = Path(storage.local_dir)
                    return {path.relative_to(stage): path.read_bytes()
                            for directory in (stage / source.DATASET_SLUG, stage / "_construction-state")
                            for path in directory.rglob("*") if path.is_file()}
                original = staged_inventory()
                for name in (source._segment_name(0, FIXTURE_PAGE_SIZE), "attrs_00000"):
                    body_path = root / f"{name}.body"
                    wire_path = root / f"{name}.wire"
                    wire = wire_path.read_bytes()
                    document = json.loads(body_path.read_bytes())
                    document["features"][0]["attributes"]["name"] = "Consistent forged native string"
                    body = json.dumps(document, separators=(",", ":")).encode()
                    body_path.write_bytes(body)
                    receipt_path = root / f"{name}.receipt.json"
                    receipt = json.loads(receipt_path.read_bytes())
                    self.assertEqual(receipt["content_encoding"], encoding)
                    receipt.update(bytes=len(body), sha256=_sha(body))
                    receipt_path.write_bytes(_canonical(receipt))
                    self.assertEqual(wire_path.read_bytes(), wire)
                with self.assertRaisesRegex(source.WbdHu8SubbasinError, "wire_body_mismatch"):
                    self._real_replay(storage, root)
                self.assertEqual(staged_inventory(), original)

    def test_real_capture_transport_and_torn_population_regressions(self):
        tmp = Path(self._tmpdir().name)
        root = tmp / "captures"
        roster = list(range(1, FIXTURE_COUNT + 1))
        transport, _calls = self._transport(roster)
        with self._patched(), patch.object(source, "_default_client",
                side_effect=lambda: httpx.Client(transport=transport)):
            source.fetch_wbd_hu8_subbasin_pages(root)
        for case in ("wire", "body", "segment_url", "attrs_url", "metadata_url", "receipt_symlink",
                     "state_symlink", "count", "ids", "duplicate_ids", "attrs_torn", "tile_gap"):
            with self.subTest(case=case):
                altered = tmp / case
                shutil.copytree(root, altered)
                first = source._segment_name(0, FIXTURE_PAGE_SIZE)
                if case in ("wire", "body"):
                    path = altered / f"{first}.{case}"
                    path.write_bytes(path.read_bytes() + b"corrupt")
                elif case.endswith("_url"):
                    name = {"segment_url": first, "attrs_url": "attrs_00000",
                            "metadata_url": "ids_before"}[case]
                    path = altered / f"{name}.receipt.json"
                    receipt = json.loads(path.read_bytes())
                    receipt["url"] = receipt["final_url"] = "https://example.invalid/substituted"
                    path.write_bytes(_canonical(receipt))
                elif case.endswith("_symlink"):
                    path = altered / ("capture_state.json" if case == "state_symlink"
                                      else first + ".receipt.json")
                    target = tmp / (case + "-target")
                    path.rename(target)
                    path.symlink_to(target)
                elif case == "count":
                    self._mutate_page(None, "count_after", lambda d: d.update(count=7))(altered)
                elif case == "ids":
                    self._mutate_page(None, "ids_after", lambda d: d["objectIds"].__setitem__(-1, 99))(altered)
                elif case == "duplicate_ids":
                    self._mutate_page(None, "ids_before", lambda d: d["objectIds"].__setitem__(1, 1))(altered)
                elif case == "attrs_torn":
                    self._mutate_page(None, "attrs_00000", lambda d: d["features"][0]["attributes"].update(
                        name="Torn extraction"))(altered)
                else:
                    path = altered / "capture_state.json"
                    state = json.loads(path.read_bytes())
                    state["segments"].pop(0)
                    path.write_bytes(_canonical(state))
                with self._patched():
                    with self.assertRaises(ValueError):
                        source._stream_validate(altered, parquet_sink=None)

    def test_attrs_validation_memory_is_bounded_by_one_retained_page(self):
        def peak(count):
            root = Path(self._tmpdir().name) / "captures"
            def pad(name, _visit, document):
                if name.startswith("attrs_"):
                    document["padding"] = "p" * 1_000_000
            transport, _calls = self._transport(list(range(1, count + 1)), pad)
            with self._patched(), patch.object(source, "_default_client",
                    side_effect=lambda: httpx.Client(transport=transport)):
                source.fetch_wbd_hu8_subbasin_pages(root)
                tracemalloc.start()
                try:
                    extracted = source._stream_validate(root, parquet_sink=None)
                    _current, maximum = tracemalloc.get_traced_memory()
                finally:
                    tracemalloc.stop()
                self.assertEqual(extracted["feature_count"], count)
                return maximum
        small = peak(40)
        large = peak(80)
        self.assertLess(large, small * 1.4, (small, large))

    def _patched(self, *extra):
        return _patch_all(*_PATCHES, *extra)

    def _tmpdir(self):
        tmp = tempfile.TemporaryDirectory(prefix="wbd-hu8-tests-")
        self.addCleanup(tmp.cleanup)
        return tmp

    def _storage(self, root):
        return StagingStorageResource(local_dir=str(Path(root) / "stage"))

    def _capture(self, storage, root, mutate=None):
        if not root.exists():
            _build_fixture_capture(root)
        if mutate is not None:
            mutate(root)
        return source.capture_wbd_hu8_subbasin(
            storage, version="vtest0000000000000000000000000000",
            capture_directory=str(root))

    def _scripted_capture_get(self, documents, calls):
        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = json.dumps(documents[name], separators=(",", ":"), ensure_ascii=False,
                              allow_nan=False).encode("utf-8")
            wire = body[::-1]
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(wire)
            (capture_root / f"{name}.receipt.json").write_bytes(
                _canonical(_retained_receipt(url, body, wire)) + b"\n")
            return body, _retained_receipt(url, body, wire)
        return fake_capture_get

    def _mocked_acquisition(self, documents, calls):
        return (patch.object(source, "capture_get",
                             side_effect=self._scripted_capture_get(documents, calls)),
                patch.object(source.httpx, "Client",
                             side_effect=lambda *_args, **_kwargs: _ContextClient()))

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(wbd_hu8_subbasin, hifld_ingest_assets)
        self.assertEqual(wbd_hu8_subbasin_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        passed = wbd_hu8_subbasin_complete({
            "feature_count": FIXTURE_COUNT,
            "source_reported_count": FIXTURE_COUNT, "source_population_complete": True,
            "source_invalid_geometry_count": len(source.EXPECTED_INVALID_OBJECTIDS),
            "source_invalid_geometry_ids": source.EXPECTED_INVALID_OBJECTIDS,
            "candidate_invalid_geometry_count": 0,
            "source_empty_geometry_count": 0,
            "source_distinct_huc8_count": FIXTURE_COUNT})
        self.assertTrue(passed.passed)
        self.assertFalse(passed.metadata["release_accepted"].value)
        self.assertFalse(passed.metadata["geometry_repair_accepted"].value)
        candidate_invalid = wbd_hu8_subbasin_complete({
            "feature_count": FIXTURE_COUNT,
            "source_reported_count": FIXTURE_COUNT, "source_population_complete": True,
            "source_invalid_geometry_count": len(source.EXPECTED_INVALID_OBJECTIDS),
            "source_invalid_geometry_ids": source.EXPECTED_INVALID_OBJECTIDS,
            "candidate_invalid_geometry_count": 1,
            "source_empty_geometry_count": 0,
            "source_distinct_huc8_count": FIXTURE_COUNT})
        self.assertFalse(candidate_invalid.passed)
        roster_drift = wbd_hu8_subbasin_complete({
            "feature_count": FIXTURE_COUNT,
            "source_reported_count": FIXTURE_COUNT, "source_population_complete": True,
            "source_invalid_geometry_count": len(source.EXPECTED_INVALID_OBJECTIDS),
            "source_invalid_geometry_ids": [source.EXPECTED_INVALID_OBJECTIDS[0]],
            "candidate_invalid_geometry_count": 0,
            "source_empty_geometry_count": 0,
            "source_distinct_huc8_count": FIXTURE_COUNT})
        self.assertFalse(roster_drift.passed)
        source_drift = wbd_hu8_subbasin_complete({
            "feature_count": FIXTURE_COUNT,
            "source_reported_count": FIXTURE_COUNT, "source_population_complete": True,
            "source_invalid_geometry_count": len(source.EXPECTED_INVALID_OBJECTIDS) + 1,
            "source_invalid_geometry_ids": source.EXPECTED_INVALID_OBJECTIDS,
            "candidate_invalid_geometry_count": 0,
            "source_empty_geometry_count": 0,
            "source_distinct_huc8_count": FIXTURE_COUNT})
        self.assertFalse(source_drift.passed)

    def test_capture_roster_pages_tile_source_ids(self):
        from urllib.parse import parse_qs, urlsplit
        urls = source._capture_urls(FIXTURE_COUNT)
        self.assertEqual(len(urls), len(source._doc_urls(FIXTURE_COUNT))
                         + (FIXTURE_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE)
        covered = []
        for offset in range(0, FIXTURE_COUNT, source.PAGE_SIZE):
            count = min(source.PAGE_SIZE, FIXTURE_COUNT - offset)
            params = parse_qs(urlsplit(urls[source._segment_name(offset, count)]).query)
            self.assertEqual(params["resultOffset"], [str(offset)])
            self.assertEqual(params["resultRecordCount"], [str(count)])
            self.assertEqual(params["outSR"], ["4326"])
            self.assertEqual(params["outFields"], ["*"])
            self.assertEqual(params["returnGeometry"], ["true"])
            self.assertNotIn("objectIds", params)
            covered.extend(range(offset + 1, offset + count + 1))
        self.assertEqual(covered, list(range(1, FIXTURE_COUNT + 1)))
        self.assertEqual(source.LAYER_URL,
                         "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer/4")
        self.assertEqual(source.DATASET_SLUG, "wbd")
        self.assertEqual(source.FILE_SLUG, "8-digit-hu-subbasin")

    def test_source_pins_match_researched_producer_publication(self):
        for name in ("EXPECTED_COUNT", "EXPECTED_IDS", "PAGE_COUNT", "ATTRS_PAGE_OFFSETS",
                     "EXPECTED_RECORD_SHA256", "EXPECTED_GEOMETRY_SHA256", "EXPECTED_HUC8_SHA256"):
            self.assertNotIn(name, vars(source))
        self.assertIn("Data refreshed October, 2026.", source.EXPECTED_COPYRIGHT_TEXT)
        self.assertEqual(source.LAYER_NAME, "8-digit HU  (Subbasin)")
        self.assertEqual([name for _id, name in source.EXPECTED_SERVICE_LAYERS][4],
                         source.LAYER_NAME)
        self.assertEqual(len(source.EXPECTED_FIELD_DECLARATIONS), 17)
        self.assertEqual(
            [field["name"] for field in source.EXPECTED_FIELD_DECLARATIONS],
            ["objectid", "tnmid", "metasourceid", "sourcedatadesc", "sourceoriginator",
             "sourcefeatureid", "loaddate", "referencegnis_ids", "areaacres", "areasqkm",
             "states", "huc8", "name", "shape", "globalid", "shape_Length",
             "shape_Area"])
        for pin in (source.EXPECTED_SCHEMA_SHA256,):
            self.assertTrue(pin == "" or (len(pin) == 64 and
                                          all(c in "0123456789abcdef" for c in pin)))
        self.assertIn("hydro.nationalmap.gov", source.CROSSWALK_OPEN_REST_SERVICE)
        self.assertEqual(source.CROSSWALK_STATUS, "Deprecate")
        self.assertIn("StagedProducts/Hydrography/WBD/National/", source.SNAPSHOT_URL)

    def test_stream_validates_complete_population_and_streams_parquet(self):
        with self._patched():
            self._run_stream_validate()

    def _run_stream_validate(self):
        root = Path(self._tmpdir().name) / "captures"
        _build_fixture_capture(root)
        sink = root / "8-digit-hu-subbasin.parquet"
        extracted = source._stream_validate(root, parquet_sink=sink)
        self.assertEqual(extracted["attrs_path_sha"], extracted["record_sha"])
        self.assertEqual(extracted["segments"], 3)
        self.assertEqual(extracted["vertex_count"], 50)
        self.assertEqual(extracted["invalid_geometry_count"], 0)
        self.assertEqual(extracted["candidate_invalid_geometry_count"], 0)
        self.assertEqual(extracted["empty_geometry_count"], 0)
        self.assertEqual(extracted["distinct_huc8_count"], FIXTURE_COUNT)
        self.assertEqual(extracted["observed_bounds"]["ymin"], 40.0)
        self.assertEqual(extracted["max_objectid"], FIXTURE_COUNT)
        self.assertEqual(extracted["null_attr_counts"]["sourcefeatureid"], FIXTURE_COUNT)
        self.assertEqual(extracted["null_attr_counts"]["loaddate"], 1)
        self.assertEqual(extracted["geometry_types"], ["MultiPolygon", "Polygon"])
        table = pq.read_table(sink)
        self.assertEqual(table.num_rows, FIXTURE_COUNT)
        self.assertEqual(table.column_names[-1], "geometry")
        types = {from_wkb(wkb).geom_type for wkb in table.column("geometry").to_pylist()}
        self.assertEqual(types, {"Polygon", "MultiPolygon"})
        with_hole = from_wkb(table.column("geometry").to_pylist()[5])
        self.assertEqual(len(with_hole.interiors), 1)
        self.assertIn(b"geo", table.schema.metadata or {})
        record = table.to_pylist()[0]
        self.assertEqual(record["huc8"], "00000001")
        self.assertEqual(record["objectid"], 1)
        self.assertIsNone(record["sourcefeatureid"])

    def test_pins_enforce_and_skip(self):
        with self._patched():
            self._run_pins_enforce_and_skip()

    def _run_pins_enforce_and_skip(self):
        root = Path(self._tmpdir().name) / "captures"
        _build_fixture_capture(root)
        summary = source._stream_validate(root, parquet_sink=None)
        with patch.object(source, "EXPECTED_SCHEMA_SHA256", summary["schema_sha"]):
            source._stream_validate(root, parquet_sink=None)
        with patch.object(source, "EXPECTED_SCHEMA_SHA256", "f" * 64):
            with self.assertRaisesRegex(source.WbdHu8SubbasinError,
                                        "source_schema_baseline_drift"):
                source._stream_validate(root, parquet_sink=None)
        for name in (source._segment_name(0, FIXTURE_PAGE_SIZE), "attrs_00000"):
            self._mutate_page(None, name, lambda d: d["features"][0]["attributes"].update(
                name="Publisher edit", sourcefeatureid="new native value"))(root)
        edited = source._stream_validate(root, parquet_sink=None)
        self.assertNotEqual(edited["record_sha"], summary["record_sha"])
        self.assertEqual(edited["attrs_path_sha"], edited["record_sha"])
        self.assertNotEqual(edited["null_attr_counts"], summary["null_attr_counts"])
        def densify(document):
            ring = document["features"][0]["geometry"]["rings"][0]
            ring.insert(1, [(ring[0][0] + ring[1][0]) / 2, (ring[0][1] + ring[1][1]) / 2])
        self._mutate_page(None, source._segment_name(0, FIXTURE_PAGE_SIZE), densify)(root)
        edited = source._stream_validate(root, parquet_sink=None)
        self.assertNotEqual(edited["geometry_sha"], summary["geometry_sha"])
        self.assertEqual(edited["vertex_count"], summary["vertex_count"] + 1)
        self._mutate_page(None, "attrs_00000", lambda d: d["features"][0]["attributes"].update(
            name="Torn publisher edit"))(root)
        with self.assertRaisesRegex(source.WbdHu8SubbasinError, "source_attribute_cross_path_drift"):
            source._stream_validate(root, parquet_sink=None)

    def _mutate_page(self, _root, name, mutator):
        def apply(root):
            path = root / f"{name}.body"
            document = json.loads(path.read_bytes())
            mutator(document)
            body = json.dumps(document, separators=(",", ":"), ensure_ascii=False,
                              allow_nan=False).encode("utf-8")
            wire = body[::-1]
            path.write_bytes(body)
            (root / f"{name}.wire").write_bytes(wire)
            receipt_path = root / f"{name}.receipt.json"
            receipt = json.loads(receipt_path.read_bytes())
            receipt.update({"bytes": len(body), "sha256": _sha(body),
                            "wire_bytes": len(wire), "wire_sha256": _sha(wire)})
            receipt_path.write_bytes(_canonical(receipt) + b"\n")
        return apply

    def _drift_cases(self):
        return [
            ("count drift", self._mutate_page(None, "count_before",
                                              lambda d: d.update(count=7)),
             "source_count_drift:before"),
            ("id roster drift", self._mutate_page(None, "ids_before",
                                                  lambda d: d.update(objectIds=list(range(1, FIXTURE_COUNT)))),
             "source_count_drift:before"),
            ("id changed during capture", self._mutate_page(
                None, "ids_after", lambda d: d.update(objectIds=list(range(2, FIXTURE_COUNT + 2)))),
             "source_metadata_or_count_changed_during_capture:ids_after"),
            ("service drift", self._mutate_page(None, "service_before",
                                                lambda d: d.update(maxRecordCount=100)),
             "service_before_service_identity_drift"),
            ("service layer roster", self._mutate_page(
                None, "service_before", lambda d: d["layers"].pop()),
             "service_before_service_layer_roster_drift"),
            ("service changed during capture", self._mutate_page(
                None, "service_after", lambda d: d.update(currentVersion="12.0")),
             "source_metadata_or_count_changed_during_capture:service_after"),
            ("field schema drift", self._mutate_page(
                None, "layer_before", lambda d: d["fields"][11].update(name="huc9")),
             "source_field_schema_drift"),
            ("layer identity drift", self._mutate_page(
                None, "layer_before", lambda d: d.update(maxRecordCount=100)),
             "source_layer_identity_drift"),
            ("layer pagination drift", self._mutate_page(
                None, "layer_before",
                lambda d: d["advancedQueryCapabilities"].update(supportsPagination=False)),
             "source_layer_identity_drift"),
            ("layer native crs drift", self._mutate_page(
                None, "layer_before",
                lambda d: d["extent"]["spatialReference"].update(wkid=4326)),
             "source_layer_native_crs_drift"),
            ("page crs drift", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d.update(spatialReference={"wkid": 3857, "latestWkid": 3857})),
             "source_page_crs_drift"),
            ("page transfer flag drift", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d.update(exceededTransferLimit=False)),
             "source_page_transfer_flag_drift"),
            ("page row count drift", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["features"].pop()),
             "source_page_row_count_incomplete"),
            ("page physical schema drift", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["fields"][2].update(name="renamed")),
             "source_page_physical_schema_changed"),
            ("page id slice reorder", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["features"].reverse()),
             "source_page_id_slice_incomplete_or_reordered"),
            ("attribute roster drift", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["features"][0]["attributes"].pop("huc8")),
             "source_attribute_roster_or_order_drift"),
            ("attribute cross-path drift", self._mutate_page(
                None, "attrs_00000",
                lambda d: d["features"][0]["attributes"].update(sourceoriginator="x")),
             "source_attribute_cross_path_drift"),
            ("huc8 identifier drift", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["features"][0]["attributes"].update(huc8="0403")),
             "source_huc8_identifier_drift"),
            ("string value type drift", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["features"][0]["attributes"].update(tnmid=5)),
             "source_string_value_type_drift:tnmid"),
            ("date value type drift", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["features"][0]["attributes"].update(loaddate="x")),
             "source_date_value_type_drift:loaddate"),
            ("double value type drift", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["features"][0]["attributes"].update(areaacres="x")),
             "source_double_value_type_drift:areaacres"),
            ("dimensional geometry drift", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["features"][0]["geometry"].update(hasZ=True)),
             "source_polygon_dimensional_flags_rejected"),
            ("open ring", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["features"][0]["geometry"]["rings"][0].pop()),
             "source_polygon_ring_invalid_or_unclosed"),
            ("nonfinite ordinate", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["features"][0]["geometry"]["rings"][0][1].__setitem__(0, None)),
             "source_polygon_ordinate_invalid"),
            ("coordinate range drift", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d["features"][0]["geometry"]["rings"][0][1].__setitem__(1, 95.0)),
             "source_coordinate_range_drift"),
            ("state roster gap", lambda root: self._drop_segment(root), None),
            ("error envelope segment", self._mutate_page(
                None, source._segment_name(0, FIXTURE_PAGE_SIZE),
                lambda d: d.update(error={"code": 500, "message": "Error"})),
             "source_page_error"),
            ("attrs page incomplete", self._mutate_page(
                None, "attrs_00000", lambda d: d["features"].pop()),
             "source_attrs_page_incomplete"),
        ]

    def _drop_segment(self, root):
        state_path = root / "capture_state.json"
        state = json.loads(state_path.read_bytes())
        state["segments"] = state["segments"][:-1]
        state_path.write_bytes(_canonical(state) + b"\n")

    def test_fail_closed_drifts(self):
        with self._patched():
            self._run_fail_closed_drifts()

    def _run_fail_closed_drifts(self):
        for label, mutate, expected in self._drift_cases():
            with self.subTest(case=label):
                with tempfile.TemporaryDirectory(prefix="wbd-drift-") as tmp:
                    root = Path(tmp) / "captures"
                    _build_fixture_capture(root)
                    if expected is None:
                        # State roster gaps surface at the plan stage.
                        mutate(root)
                        with self.assertRaisesRegex(source.WbdHu8SubbasinError,
                                                    "capture_roster_incomplete"):
                            source._stream_validate(root, parquet_sink=None)
                        continue
                    mutate(root)
                    with self.assertRaisesRegex(source.WbdHu8SubbasinError, expected):
                        source._stream_validate(root, parquet_sink=None)

    def test_source_invalid_geometry_is_minimally_repaired(self):
        spike = [[0.0, 0.0], [0.0, 4.0], [2.0, 4.0], [2.0, 2.0],
                 [2.0, 4.0], [4.0, 4.0], [4.0, 0.0], [0.0, 0.0]]
        from shapely.validation import explain_validity
        with self._patched(
                patch.object(source, "EXPECTED_INVALID_OBJECTIDS", [3]),
                patch.object(source, "EXPECTED_INVALID_VALIDITY_EXPLANATIONS",
                             {3: explain_validity(from_wkb(source._esri_polygon_wkb(
                                 {"rings": [spike]})))})):
            root = Path(self._tmpdir().name) / "captures"
            _build_fixture_capture(root)

            def break_oid_3(fixture_root):
                name = source._segment_name(0, FIXTURE_PAGE_SIZE)
                path = fixture_root / f"{name}.body"
                document = json.loads(path.read_bytes())
                document["features"][2]["geometry"]["rings"] = [spike]
                body = json.dumps(document, separators=(",", ":"),
                                  ensure_ascii=False, allow_nan=False).encode("utf-8")
                path.write_bytes(body)
                (fixture_root / f"{name}.wire").write_bytes(body[::-1])
                receipt_path = fixture_root / f"{name}.receipt.json"
                receipt = json.loads(receipt_path.read_bytes())
                receipt.update({"bytes": len(body), "sha256": _sha(body),
                                "wire_bytes": len(body), "wire_sha256": _sha(body[::-1])})
                receipt_path.write_bytes(_canonical(receipt) + b"\n")
            break_oid_3(root)
            sink = root / "8-digit-hu-subbasin.parquet"
            extracted = source._stream_validate(root, parquet_sink=sink)
            self.assertEqual(extracted["invalid_geometry_count"], 1)
            self.assertEqual(extracted["invalid_geometry_ids"], [3])
            self.assertEqual(extracted["candidate_invalid_geometry_count"], 0)
            self.assertEqual(len(extracted["repair_records"]), 1)
            record = extracted["repair_records"][0]
            self.assertEqual(record["objectid"], 3)
            self.assertEqual(record["operation"], "make_valid(method=structure)")
            table = pq.read_table(sink)
            repaired = from_wkb(table.column("geometry").to_pylist()[2])
            self.assertTrue(repaired.is_valid)
            self.assertGreater(repaired.area, 0)

    def test_fresh_acquisition_request_order_brackets_the_segments(self):
        calls: list[str] = []
        documents = {**_fixture_doc_bodies(), **_fixture_segment_bodies()}
        with self._patched(*self._mocked_acquisition(documents, calls)):
            root = Path(self._tmpdir().name) / "captures"
            source.fetch_wbd_hu8_subbasin_pages(root)
        self.assertEqual(calls[:4], ["service_before", "layer_before",
                                     "count_before", "ids_before"])
        segments = _segment_names()
        self.assertEqual([name for name in calls if name.startswith("seg_")], segments)
        self.assertLess(calls.index("ids_before"), calls.index(segments[0]))
        self.assertLess(calls.index(segments[-1]), calls.index("attrs_00000"))
        self.assertLess(calls.index("attrs_00005"), calls.index("service_after"))
        self.assertEqual(calls[-4:], ["service_after", "layer_after",
                                      "count_after", "ids_after"])
        self.assertEqual(len(calls), 4 + len(segments) + len(FIXTURE_ATTRS_OFFSETS) + 4)

    def test_resumed_acquisition_refreshes_closing_brackets(self):
        documents = {**_fixture_doc_bodies(), **_fixture_segment_bodies()}
        with self._patched(*self._mocked_acquisition(documents, [])):
            root = Path(self._tmpdir().name) / "captures"
            source.fetch_wbd_hu8_subbasin_pages(root)
        state = json.loads((root / "capture_state.json").read_bytes())
        dropped = state["segments"].pop()
        state["complete"] = False
        (root / "capture_state.json").write_bytes(_canonical(state) + b"\n")
        for suffix in (".body", ".wire", ".receipt.json"):
            (root / f"{dropped['name']}{suffix}").unlink()
        resume_calls: list[str] = []
        with self._patched(*self._mocked_acquisition(documents, resume_calls)):
            source.fetch_wbd_hu8_subbasin_pages(root)
        self.assertEqual(resume_calls, [dropped["name"], "service_after", "layer_after",
                                        "count_after", "ids_after"])
        refreshed = json.loads((root / "ids_after.receipt.json").read_bytes())
        self.assertEqual(refreshed["url"], source._query(
            {"where": "1=1", "returnIdsOnly": "true", "f": "json"}))

    def test_mapped_previous_root_resumes_the_default_capture_state(self):
        documents = {**_fixture_doc_bodies(), **_fixture_segment_bodies()}
        tmp = self._tmpdir().name
        mapped_root = Path(tmp) / "previous-root"
        env = patch.dict(os.environ, {"HIFLD_CAPTURE_ROOT": str(mapped_root)})
        identity_patches = (
            patch.object(source, "processing_identity", lambda _root: dict(FAKE_IDENTITY)),
            patch.object(source, "validate_construction_code_identity"),
            patch.object(capture_common, "validate_construction_code_identity"),
        )
        storage = self._storage(tmp)
        # The first acquisition runs to completion at the mapped default and
        # seals its candidate from it.
        with self._patched(*self._mocked_acquisition(documents, []), env, *identity_patches):
            source.capture_wbd_hu8_subbasin(
                storage, version="vtest0000000000000000000000000000")
        # The default keeps the historical wbd-hu8-subbasin layout under the
        # mapped root, so the interrupted state is the same directory.
        default_root = mapped_root / "wbd-hu8-subbasin" / "captures"
        state = json.loads((default_root / "capture_state.json").read_bytes())
        dropped = state["segments"].pop()
        state["complete"] = False
        (default_root / "capture_state.json").write_bytes(_canonical(state) + b"\n")
        for suffix in (".body", ".wire", ".receipt.json"):
            (default_root / f"{dropped['name']}{suffix}").unlink()
        resume_calls: list[str] = []
        with self._patched(*self._mocked_acquisition(documents, resume_calls),
                           env, *identity_patches):
            result = source.capture_wbd_hu8_subbasin(
                storage, version="vtest0000000000000000000000000000")
        self.assertEqual(resume_calls, [dropped["name"], "service_after", "layer_after",
                                        "count_after", "ids_after"])
        self.assertEqual(result["feature_count"], FIXTURE_COUNT)

    def test_producer_change_during_extraction_fails_closed(self):
        calls: list[str] = []
        documents = {**_fixture_doc_bodies(), **_fixture_segment_bodies()}
        documents["ids_after"] = {"objectIdFieldName": "objectid",
                                  "objectIds": list(range(2, FIXTURE_COUNT + 2))}
        with self._patched(*self._mocked_acquisition(documents, calls)):
            root = Path(self._tmpdir().name) / "captures"
            source.fetch_wbd_hu8_subbasin_pages(root)
            self.assertEqual(calls[-1], "ids_after")
            with self.assertRaisesRegex(
                    source.WbdHu8SubbasinError,
                    "source_metadata_or_count_changed_during_capture:ids_after"):
                source._stream_validate(root, parquet_sink=None)

    def test_sealed_manifest_declares_and_gates_the_pinned_repair(self):
        spike = [[0.0, 0.0], [0.0, 4.0], [2.0, 4.0], [2.0, 2.0],
                 [2.0, 4.0], [4.0, 4.0], [4.0, 0.0], [0.0, 0.0]]
        from shapely.validation import explain_validity
        with self._patched(
                patch.object(source, "EXPECTED_INVALID_OBJECTIDS", [3]),
                patch.object(source, "EXPECTED_INVALID_VALIDITY_EXPLANATIONS",
                             {3: explain_validity(from_wkb(source._esri_polygon_wkb(
                                 {"rings": [spike]})))}),
                patch.object(source, "processing_identity", lambda _root: dict(FAKE_IDENTITY)),
                patch.object(source, "validate_construction_code_identity"),
                patch.object(capture_common, "validate_construction_code_identity"),
                patch.object(source, "_default_client", _OfflineClient)):
            tmp = self._tmpdir().name
            storage = self._storage(tmp)
            root = Path(tmp) / "captures"
            _build_fixture_capture(root)
            name = source._segment_name(0, FIXTURE_PAGE_SIZE)
            path = root / f"{name}.body"
            document = json.loads(path.read_bytes())
            document["features"][2]["geometry"]["rings"] = [spike]
            body = json.dumps(document, separators=(",", ":"), ensure_ascii=False,
                              allow_nan=False).encode("utf-8")
            path.write_bytes(body)
            (root / f"{name}.wire").write_bytes(body[::-1])
            receipt_path = root / f"{name}.receipt.json"
            receipt = json.loads(receipt_path.read_bytes())
            receipt.update({"bytes": len(body), "sha256": _sha(body),
                            "wire_bytes": len(body), "wire_sha256": _sha(body[::-1])})
            receipt_path.write_bytes(_canonical(receipt) + b"\n")
            result = source.capture_wbd_hu8_subbasin(
                storage, version="vtest0000000000000000000000000000",
                capture_directory=str(root))
            self.assertEqual(result["source_invalid_geometry_count"], 1)
            self.assertEqual(result["source_invalid_geometry_ids"], [3])
            self.assertEqual(result["candidate_invalid_geometry_count"], 0)
            self.assertEqual(result["source_repair_record_count"], 1)
            self.assertFalse(result["geometry_repair_accepted"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}"
            audit = json.loads(storage.read_key(prefix + "/metadata/source_audit.json"))
            self.assertEqual(audit["invalid_geometry_objectids"], [3])
            self.assertEqual([entry["objectid"]
                              for entry in audit["geometry_repair_records"]], [3])
            self.assertEqual(audit["geometry_repair_records"][0]["operation"],
                             "make_valid(method=structure)")
            self.assertFalse(audit["geometry_repair_accepted"])
            self.assertEqual(audit["candidate_invalid_geometry_count"], 0)
            manifest = json.loads(storage.read_key(prefix + "/metadata/source_manifest.json"))
            declaration = manifest["transformations"][0]["parameters"]["geometry"]
            self.assertIn("make_valid(method=structure)", declaration)
            self.assertIn("OBJECTIDs 3", declaration)
            self.assertNotIn("densification or repair", declaration)
            ready = json.loads(storage.read_key(prefix + "/metadata/candidate_ready.json"))
            self.assertFalse(ready["geometry_repair_accepted"])

    def test_candidate_seal_and_cached_reuse(self):
        with self._patched(), \
                patch.object(source, "processing_identity", lambda _root: dict(FAKE_IDENTITY)), \
                patch.object(source, "validate_construction_code_identity"), \
                patch.object(capture_common, "validate_construction_code_identity"), \
                patch.object(source, "_default_client", _OfflineClient):
            tmp = self._tmpdir().name
            storage = self._storage(tmp)
            root = Path(tmp) / "captures"
            result = self._capture(storage, root)
            self.assertFalse(result["reused"])
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}"
            ready = json.loads(storage.read_key(
                prefix + "/metadata/candidate_ready.json"))
            self.assertFalse(ready["approved"])
            parquet_name = f"geoparquet/{source.FILE_SLUG}.parquet"
            self.assertIn(parquet_name, ready["objects"])
            staged = (Path(storage.local_dir) / prefix / parquet_name)
            self.assertEqual(ready["objects"][parquet_name],
                             {"bytes": staged.stat().st_size,
                              "sha256": sha256_file(staged)})
            manifest = json.loads(storage.read_key(
                prefix + "/metadata/source_manifest.json"))
            self.assertFalse(manifest["approved"])
            self.assertEqual(len(manifest["inputs"]), 13)
            self.assertTrue(all(entry["role"] == "producer_publication"
                                for entry in manifest["inputs"]))
            self.assertTrue(manifest["transformations"][0]["parameters"]
                            ["candidate_status"].startswith("unapproved"))
            audit = json.loads(storage.read_key(prefix + "/metadata/source_audit.json"))
            self.assertEqual(audit["invalid_geometry_objectids"], [])
            self.assertEqual(audit["geometry_repair_records"], [])
            self.assertEqual(audit["candidate_invalid_geometry_count"], 0)
            self.assertFalse(audit["geometry_repair_accepted"])
            self.assertEqual(result["candidate_invalid_geometry_count"], 0)
            self.assertEqual(result["source_repair_record_count"], 0)
            self.assertFalse(result["geometry_repair_accepted"])
            parquet = pq.read_table(staged)
            self.assertEqual(parquet.num_rows, FIXTURE_COUNT)
            self.assertFalse(result["source_release_accepted"])
            again = self._capture(storage, root)
            self.assertTrue(again["reused"])
            self.assertEqual(again["version"], result["version"])

    def test_capture_requires_local_staging_resource(self):
        with self.assertRaisesRegex(source.WbdHu8SubbasinError,
                                    "explicit_local_staging_resource_required"):
            source.capture_wbd_hu8_subbasin(
                PublishedStorageResource(local_dir=str(Path(self._tmpdir().name) / "pub")),
                version="vtest")
        with self.assertRaisesRegex(source.WbdHu8SubbasinError, "invalid_version"):
            source.capture_wbd_hu8_subbasin(
                self._storage(self._tmpdir().name), version="bad version")

    def test_incomplete_capture_is_quarantined(self):
        with self._patched(), \
                patch.object(source, "processing_identity", lambda _root: dict(FAKE_IDENTITY)), \
                patch.object(source, "validate_construction_code_identity"), \
                patch.object(capture_common, "validate_construction_code_identity"), \
                patch.object(source, "_default_client", _OfflineClient):
            tmp = self._tmpdir().name
            storage = self._storage(tmp)
            root = Path(tmp) / "captures"
            _build_fixture_capture(root)
            first = source._segment_name(0, FIXTURE_PAGE_SIZE)
            for suffix in (".body", ".wire", ".receipt.json"):
                (root / f"{first}{suffix}").unlink()
            state_path = root / "capture_state.json"
            state = json.loads(state_path.read_bytes())
            state["segments"] = [entry for entry in state["segments"]
                                 if entry.get("name") != first]
            state_path.write_bytes(_canonical(state) + b"\n")
            with self.assertRaises(AssertionError):
                source.capture_wbd_hu8_subbasin(
                    storage, version="vtest0000000000000000000000000000",
                    capture_directory=str(root))
            quarantined = [key for key in storage.list_prefix("_quarantine/wbd/")]
            self.assertTrue(quarantined)
            self.assertTrue(any(key.endswith("metadata/failure.json")
                                for key in quarantined))


@contextmanager
def _patch_all(*patches):
    entered = []
    for item in patches:
        entered.append(item.__enter__())
    try:
        yield
    finally:
        for item in reversed(patches):
            item.__exit__(None, None, None)


if __name__ == "__main__":
    unittest.main()
