"""Shared contract tests for the NFHL capture and replay machinery.

These tests exercise ``dagster_hifld.sources.nfhl_common`` directly, over one
synthetic FEMA NFHL layer fixture, for the behaviors every NFHL layer adapter
must inherit: complete roster coverage, duplicate/missing id rejection,
transient and complete-HTTP-200 Esri error handling, recursive segment
splitting, replay evidence reuse and corruption healing, interrupted-resume
completion, pending-pin fail-closure, and the publication refusal gate.
"""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import nfhl_common as nfhl

SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")

FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
    {"name": "SUBBAS_ID", "type": "esriFieldTypeString", "alias": "SUBBAS_ID", "length": 32, "domain": None},
    {"name": "SUB_AREA", "type": "esriFieldTypeDouble", "alias": "SUB_AREA", "domain": None},
    {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
    {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
]

IDS = [10, 11, 12, 13]


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _raw(value):
    """JSON body that preserves dict insertion order (producer page bodies)."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _attrs(oid):
    return {"OBJECTID": oid, "DFIRM_ID": "020516", "SUBBAS_ID": f"020516-{oid}",
            "SUB_AREA": 0.001, "GFID": f"g-{oid}", "GlobalID": f"{{G-{oid}}}"}


def _feature(oid, geometry):
    return {"attributes": _attrs(oid), "geometry": geometry}


def _features():
    return [
        _feature(10, {"rings": [[[-87.8, 30.49], [-87.79, 30.49], [-87.75, 30.45], [-87.8, 30.49]]]}),
        _feature(11, None),
        _feature(12, {"rings": [[[-87.0, 33.0], ["NaN", "NaN"], [-87.2, 33.2], [-87.0, 33.0]]]}),
        _feature(13, {"rings": [[[-88.0, 32.0], [-88.1, 32.1], [-88.05, 32.2], [-88.0, 32.0]]]}),
    ]


def _layer_doc():
    return {"id": 31, "name": "Test Layer", "type": "Feature Layer", "geometryType": "esriGeometryPolygon",
            "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
            "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
            "extent": {"xmin": -159.78, "ymin": 21.07, "xmax": -66.98, "ymax": 64.84,
                       "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
            "fields": FIELDS}


def _service_doc():
    return {"mapName": "NFHL", "layers": [{"id": 31, "name": "Test Layer", "type": "Feature Layer"}]}


def _query_params(url):
    return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}


def _segment_url(offset, count):
    return nfhl.segment_query_url("https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/31",
                                  offset, count)


def _pins(audited=None, ids=IDS, count=None, **overrides):
    values = {
        "count": count if count is not None else len(ids),
        "id_sha256": hashlib.sha256(_canonical(sorted(ids))).hexdigest(),
        "schema_sha256": hashlib.sha256(_canonical([nfhl.plain_field(f) for f in FIELDS])).hexdigest(),
        "record_sha256": None if audited is None else audited["source_record_sha256"],
        "geometry_sha256": None if audited is None else audited["source_geometry_sha256"],
        "observed_bounds": None if audited is None else audited["observed_bounds"],
        "null_geometry_count": None if audited is None else audited["null_geometry_count"],
        "nan_geometry_count": None if audited is None else audited["nan_string_geometry_count"],
        "unusable_geometry_feature_counts": (None if audited is None
                                             else audited["unusable_geometry_feature_counts"]),
        "layer_extent": {"xmin": -159.78, "ymin": 21.07, "xmax": -66.98, "ymax": 64.84},
        "service_layer_roster": [(31, "Test Layer", "Feature Layer")],
    }
    values.update(overrides)
    return nfhl.PopulationPins(**values)


_RESUMABLE_CONTRACT = nfhl.LayerContract(
    dataset_slug="nfhl-testlayer", file_slug="nfhl-testlayer", layer_id=31, layer_name="Test Layer",
    geometry_family="polygon", esri_geometry_type="esriGeometryPolygon", mode="bisect",
    max_capture_bytes=1 << 20, min_free_bytes=0,
    reconstruction_dir="/tmp/hifld-native-reconstruction/999/captures",
    segment_start_records=2, doc_attempts=4, doc_base_delay=0.0,
    segment_attempts=2, segment_base_delay=0.0, client_timeout=1.0,
    gated_rings=True, resumable=True,
    pending_pin_names=nfhl.PENDING_PIN_NAMES)


class EmptyClient:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class FakeCaptureGet:
    """Serves bodies per request name; refused names answer with the error doc."""

    def __init__(self, bodies, refused=frozenset(), refused_once=frozenset(), calls=None):
        self.bodies = bodies
        self.refused = set(refused)
        self.refused_once = set(refused_once)
        self.calls = calls if calls is not None else []

    def __call__(self, _client, url, capture_root, name, *, max_bytes):
        self.calls.append((name, url))
        body = self.bodies[name]
        if name in self.refused or (name in self.refused_once and self.calls.count((name, url)) == 1):
            body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        digest = hashlib.sha256(body).hexdigest()
        receipt = {"method": "GET", "url": url, "final_url": url, "status": 200, "complete_body": True,
                   "observed_at": "2026-11-15T00:00:00+00:00",
                   "wire_bytes": len(body), "wire_sha256": digest,
                   "bytes": len(body), "sha256": digest}
        (capture_root / f"{name}.body").write_bytes(body)
        (capture_root / f"{name}.wire").write_bytes(body)
        (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        return body, receipt


def _document_bodies():
    layer = _layer_doc()
    service = _service_doc()
    ids_doc = {"objectIdFieldName": "OBJECTID", "objectIds": IDS}
    return {
        "service_doc": SERVICE_DOC_BODY,
        "service_before": _canonical(service),
        "service_after": _canonical(service),
        "layer_before": _canonical(layer),
        "layer_after": _canonical(layer),
        "count_before": _canonical({"count": len(IDS)}),
        "count_after": _canonical({"count": len(IDS)}),
        "ids_seed": _canonical(ids_doc),
        "ids_before": _canonical(ids_doc),
        "ids_after": _canonical(ids_doc),
    }


def _segment_bodies(features, width, ids=IDS):
    bodies = {}
    for offset in range(0, len(ids), width):
        count = min(width, len(ids) - offset)
        page = {"features": features[offset:offset + count],
                "spatialReference": {"wkid": 4269, "latestWkid": 4269}}
        bodies[nfhl.segment_name(offset, count)] = _raw(page)
    return bodies


def _all_bodies(width=2):
    bodies = _document_bodies()
    bodies.update(_segment_bodies(_features(), width))
    return bodies


def _audited(width=2):
    with tempfile.TemporaryDirectory(prefix="nfhl-common-audit-") as temporary:
        stream = nfhl.SegmentStream(FIELDS, Path(temporary) / "audit.parquet", "polygon")
        features = sorted(_features(), key=lambda row: row["attributes"]["OBJECTID"])
        stream.process({"features": features, "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
                       IDS, 0, len(features))
        return stream.finish()


class NfhlCommonSegmentTests(unittest.TestCase):
    def test_recursive_splitting_serves_refused_ranges(self):
        bodies = _all_bodies(width=2)
        # Every request is served except any width-4 segment, which the
        # producer always refuses; the capture must bisect it recursively.
        bodies[nfhl.segment_name(0, 4)] = b'{"error": {"code": 500, "message": "refused"}}'
        calls = []
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            capture_get = FakeCaptureGet(bodies, calls=calls)
            contract = nfhl.LayerContract(
                dataset_slug="nfhl-testlayer", file_slug="nfhl-testlayer", layer_id=31,
                layer_name="Test Layer", geometry_family="polygon",
                esri_geometry_type="esriGeometryPolygon", mode="bisect",
                max_capture_bytes=1 << 20, min_free_bytes=0,
                reconstruction_dir="/tmp/x", segment_start_records=4,
                doc_attempts=2, doc_base_delay=0.0, segment_attempts=1, segment_base_delay=0.0)
            with patch.object(nfhl.time, "sleep"):
                captures = nfhl.capture_bisect_segments(None, contract, root, 4, capture_get)
            self.assertEqual(sorted(captures), ["page_000000_00002", "page_000002_00002"])
            self.assertIn(nfhl.segment_name(0, 4), [name for name, _url in calls])
            self.assertEqual(calls.count((nfhl.segment_name(0, 4), _segment_url(0, 4))), 1)
            self.assertEqual((root / f"{nfhl.segment_name(0, 4)}.body").read_bytes(),
                             b'{"error": {"code": 500, "message": "refused"}}')

    def test_unservable_single_feature_fails_explicitly(self):
        bodies = _all_bodies(width=2)
        bodies[nfhl.segment_name(2, 1)] = b'{"error": {"code": 500, "message": "refused"}}'
        bodies[nfhl.segment_name(3, 1)] = b'{"error": {"code": 500, "message": "refused"}}'
        bodies[nfhl.segment_name(2, 2)] = b'{"error": {"code": 500, "message": "refused"}}'
        contract = nfhl.LayerContract(
            dataset_slug="nfhl-testlayer", file_slug="nfhl-testlayer", layer_id=31, layer_name="Test Layer",
            geometry_family="polygon", esri_geometry_type="esriGeometryPolygon", mode="bisect",
            max_capture_bytes=1 << 20, min_free_bytes=0, reconstruction_dir="/tmp/x",
            segment_start_records=2, doc_attempts=2, doc_base_delay=0.0,
            segment_attempts=2, segment_base_delay=0.0)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            with patch.object(nfhl.time, "sleep"):
                with self.assertRaisesRegex(ValueError, "source_single_feature_unservable"):
                    nfhl.capture_bisect_segments(None, contract, root, 4, FakeCaptureGet(bodies))

    def test_transient_http200_esri_error_doc_is_retried_with_evidence(self):
        ok_body = b'{"objectIds": [10, 11, 12, 13]}'
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def capture_get(_client, _url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = error_body if len(calls) == 1 else ok_body
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": _url, "final_url": _url, "status": 200,
                       "complete_body": True, "bytes": len(body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(nfhl.time, "sleep") as sleep_mock:
                body, _receipt = nfhl.capture_with_retry(None, "https://producer.invalid/page", root,
                                                         "probe", max_bytes=1024, attempts=4,
                                                         base_delay=0.0, capture_get=capture_get)
            self.assertEqual(body, ok_body)
            self.assertEqual(calls, ["probe", "probe"])
            self.assertEqual((root / "probe.body").read_bytes(), ok_body)
            self.assertEqual((root / "probe.attempt-01.body").read_bytes(), error_body)
            self.assertTrue(sleep_mock.called)

    def test_persistent_http200_esri_error_doc_exhausts_retries_unchanged(self):
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def capture_get(_client, _url, capture_root, name, *, max_bytes):
            calls.append(name)
            digest = hashlib.sha256(error_body).hexdigest()
            receipt = {"method": "GET", "url": _url, "final_url": _url, "status": 200,
                       "complete_body": True, "bytes": len(error_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(error_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return error_body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(nfhl.time, "sleep"):
                body, _receipt = nfhl.capture_with_retry(None, "https://producer.invalid/page", root,
                                                         "probe", max_bytes=1024, attempts=4,
                                                         base_delay=0.0, capture_get=capture_get)
            self.assertEqual(body, error_body)
            self.assertEqual(len(calls), 4)
            for attempt in range(1, 4):
                self.assertEqual((root / f"probe.attempt-{attempt:02d}.body").read_bytes(), error_body)

    def test_incomplete_transport_capture_is_retried_bounded(self):
        ok_body = b'{"objectIds": [10, 11, 12, 13]}'
        calls = []

        def capture_get(_client, _url, capture_root, name, *, max_bytes):
            calls.append(name)
            if name == "probe2":
                raise ValueError("unrelated_source_capture_failure")
            if len(calls) == 1:
                (capture_root / f"{name}.body").write_bytes(b"")
                (capture_root / f"{name}.receipt.json").write_bytes(b'{"status": null}\n')
                raise ValueError(f"incomplete_source_capture:{name}")
            digest = hashlib.sha256(ok_body).hexdigest()
            receipt = {"method": "GET", "url": _url, "final_url": _url, "status": 200,
                       "complete_body": True, "bytes": len(ok_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(ok_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return ok_body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(nfhl.time, "sleep"):
                body, _receipt = nfhl.capture_with_retry(None, "https://producer.invalid/page", root,
                                                         "probe", max_bytes=1024, attempts=4,
                                                         base_delay=0.0, capture_get=capture_get)
                with self.assertRaisesRegex(ValueError, "unrelated_source_capture_failure"):
                    nfhl.capture_with_retry(None, "https://producer.invalid/other", root,
                                            "probe2", max_bytes=1024, attempts=4,
                                            base_delay=0.0, capture_get=capture_get)
            self.assertEqual(body, ok_body)
            self.assertEqual(calls[-1], "probe2")

    def test_missing_segment_breaks_exact_roster_tiling(self):
        pins = _pins(_audited())
        bodies = _all_bodies(width=2)
        captures = {name: (body, {"url": "u", "status": 200}) for name, body in bodies.items()}
        del captures["page_000002_00002"]
        with self.assertRaisesRegex(ValueError, "source_page_tiling_incomplete_or_overlapped"):
            nfhl.validate_resumable_capture(_RESUMABLE_CONTRACT, pins, captures, _audited())

    def test_duplicate_or_missing_page_ids_are_rejected(self):
        pins = _pins(_audited())
        features = sorted(_features(), key=lambda row: row["attributes"]["OBJECTID"])
        duplicated = {"features": [features[0], features[0]],
                      "spatialReference": {"wkid": 4269, "latestWkid": 4269}}
        with tempfile.TemporaryDirectory() as temporary:
            stream = nfhl.SegmentStream(FIELDS, Path(temporary) / "x.parquet", "polygon")
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                stream.process(duplicated, IDS, 0, 2)
        missing = {"features": features[:1],
                   "spatialReference": {"wkid": 4269, "latestWkid": 4269}}
        with tempfile.TemporaryDirectory() as temporary:
            stream = nfhl.SegmentStream(FIELDS, Path(temporary) / "x.parquet", "polygon")
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                stream.process(missing, IDS, 0, 2)

    def test_duplicate_roster_ids_are_rejected(self):
        pins = _pins(_audited(), ids=[10, 11, 12, 13])
        roster = {"objectIdFieldName": "OBJECTID", "objectIds": [10, 11, 12, 13, 13]}
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            nfhl.validate_rosters_resumable({"ids_seed": (_canonical(roster), {}),
                                             "ids_before": (_canonical(roster), {}),
                                             "ids_after": (_canonical(roster), {})}, pins)

    def test_roster_short_of_pinned_count_is_rejected(self):
        pins = _pins(_audited())
        roster = {"objectIdFieldName": "OBJECTID", "objectIds": [10, 11, 12]}
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            nfhl.validate_rosters_resumable({"ids_seed": (_canonical(roster), {}),
                                             "ids_before": (_canonical(roster), {}),
                                             "ids_after": (_canonical(roster), {})}, pins)

    def test_fixed_pages_reject_exceeded_transfer_limit_on_the_final_page(self):
        layer = _layer_doc()
        service = _service_doc()
        ids_doc = {"objectIdFieldName": "OBJECTID", "objectIds": IDS}
        page_final = {"features": _features()[2:4],
                      "spatialReference": {"wkid": 4269, "latestWkid": 4269},
                      "exceededTransferLimit": True}
        page_full = {"features": _features()[0:2],
                     "spatialReference": {"wkid": 4269, "latestWkid": 4269},
                     "exceededTransferLimit": True}
        captures = {"service_doc": (SERVICE_DOC_BODY, {}),
                    "service_before": (_canonical(service), {}), "service_after": (_canonical(service), {}),
                    "layer_before": (_canonical(layer), {}), "layer_after": (_canonical(layer), {}),
                    "count_before": (_canonical({"count": 4}), {}),
                    "count_after": (_canonical({"count": 4}), {}),
                    "ids_before": (_canonical(ids_doc), {}), "ids_seed": (_canonical(ids_doc), {}),
                    "ids_after": (_canonical(ids_doc), {}),
                    "page_000": (_raw(page_full), {}), "page_001": (_raw(page_final), {})}
        contract = nfhl.LayerContract(
            dataset_slug="nfhl-testlayer", file_slug="nfhl-testlayer", layer_id=31, layer_name="Test Layer",
            geometry_family="polygon", esri_geometry_type="esriGeometryPolygon", mode="fixed",
            max_capture_bytes=1 << 20, min_free_bytes=0, reconstruction_dir="/tmp/x", page_size=2)
        with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
            nfhl.validate_in_memory_capture(contract, _pins(_audited()), captures)


class NfhlCommonReplayTests(unittest.TestCase):
    def setUp(self):
        self._temporary = tempfile.TemporaryDirectory(prefix="nfhl-common-replay-")
        self.root = Path(self._temporary.name) / "captures"
        self.root.mkdir(parents=True)
        self.bodies = _all_bodies(width=2)
        self.contract = _RESUMABLE_CONTRACT
        self.stream = nfhl.SegmentStream(FIELDS, self.root / "candidate.parquet", "polygon")
        self.ids = IDS

    def tearDown(self):
        self._temporary.cleanup()

    def _write_segment_evidence(self, name, body=None, receipt_overrides=None, url=None):
        body = self.bodies[name] if body is None else body
        url = url or _segment_url(*nfhl.parse_segment_name(name))
        digest = hashlib.sha256(body).hexdigest()
        receipt = {"method": "GET", "url": url, "final_url": url, "status": 200, "complete_body": True,
                   "bytes": len(body), "sha256": digest}
        receipt.update(receipt_overrides or {})
        (self.root / f"{name}.body").write_bytes(body)
        (self.root / f"{name}.wire").write_bytes(body)
        (self.root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")

    def test_complete_segment_evidence_is_reused_without_refetching(self):
        for name in ("page_000000_00002", "page_000002_00002"):
            self._write_segment_evidence(name)
        calls = []
        with patch.object(nfhl.time, "sleep"):
            captures = nfhl.capture_bisect_segments(None, self.contract, self.root, len(self.ids),
                                                    FakeCaptureGet(self.bodies, calls=calls),
                                                    stream=self.stream, ids=self.ids)
        self.assertEqual(sorted(captures), ["page_000000_00002", "page_000002_00002"])
        self.assertEqual(calls, [])
        self.assertEqual(self.stream.feature_count, len(self.ids))

    def test_corrupted_segment_evidence_is_renamed_stale_and_never_reused(self):
        self._write_segment_evidence("page_000000_00002",
                                     receipt_overrides={"sha256": "0" * 64})
        self._write_segment_evidence("page_000002_00002",
                                     receipt_overrides={"url": "https://producer.invalid/other"})
        calls = []
        with patch.object(nfhl.time, "sleep"):
            captures = nfhl.capture_bisect_segments(None, self.contract, self.root, len(self.ids),
                                                    FakeCaptureGet(self.bodies, calls=calls),
                                                    stream=self.stream, ids=self.ids)
        self.assertEqual(sorted(captures), ["page_000000_00002", "page_000002_00002"])
        self.assertEqual([name for name, _url in calls], ["page_000000_00002", "page_000002_00002"])
        self.assertTrue((self.root / "page_000000_00002.stale.body").exists())
        self.assertTrue((self.root / "page_000002_00002.stale.receipt.json").exists())

    def test_error_document_evidence_is_not_reused_as_served(self):
        self._write_segment_evidence("page_000000_00002",
                                     body=b'{"error": {"code": 500, "message": "refused"}}')
        self._write_segment_evidence("page_000002_00002")
        calls = []
        with patch.object(nfhl.time, "sleep"):
            captures = nfhl.capture_bisect_segments(None, self.contract, self.root, len(self.ids),
                                                    FakeCaptureGet(self.bodies, calls=calls),
                                                    stream=self.stream, ids=self.ids)
        self.assertEqual(sorted(captures), ["page_000000_00002", "page_000002_00002"])
        self.assertEqual([name for name, _url in calls], ["page_000000_00002"])
        self.assertTrue((self.root / "page_000000_00002.stale.body").exists())

    def test_interrupted_resume_completes_from_existing_evidence(self):
        features = _features()
        self._write_segment_evidence("page_000000_00002")
        calls = []
        with patch.object(nfhl.time, "sleep"):
            captures = nfhl.capture_bisect_segments(None, self.contract, self.root, len(self.ids),
                                                    FakeCaptureGet(self.bodies, calls=calls),
                                                    stream=self.stream, ids=self.ids)
        self.assertEqual([name for name, _url in calls], ["page_000002_00002"])
        self.assertEqual(sorted(captures), ["page_000000_00002", "page_000002_00002"])
        self.assertEqual(self.stream.feature_count, len(features))
        self.assertEqual(self.stream.finish()["source_feature_count"], len(features))


class NfhlCommonCandidateTests(unittest.TestCase):
    """End-to-end candidate tests over the shared capture_layer driver."""

    def _fragments(self):
        return nfhl.ManifestFragments(title="Test Layer (FEMA NFHL)", description="d",
                                      source_fields_note="f", source_ids_note="i",
                                      geometry_note="g",
                                      lineage_sentences="This candidate is exactly layer 31 'Test Layer' "
                                                        "of that producer service. Inventory context only.")

    def _capture(self, storage, temporary, capture_directory=None, calls=None, pins=None, contract=None):
        bodies = _all_bodies(width=2)
        return nfhl.capture_layer(
            contract or _RESUMABLE_CONTRACT, pins or _pins(_audited()), storage, version="vfixture",
            capture_directory=capture_directory, capture_get=FakeCaptureGet(bodies, calls=calls),
            fragments=self._fragments())

    def test_pending_digest_pins_fail_closed_and_stage_nothing(self):
        with tempfile.TemporaryDirectory() as temporary, \
                tempfile.TemporaryDirectory() as capture_temporary:
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            capture_dir = Path(capture_temporary) / "captures"
            capture_dir.mkdir()
            calls = []
            with patch.object(nfhl.httpx, "Client", side_effect=lambda *a, **k: EmptyClient()):
                with self.assertRaisesRegex(ValueError, "population_digest_pins_not_established"):
                    self._capture(storage, temporary, capture_directory=str(capture_dir), calls=calls,
                                  pins=_pins())
            manifest = json.loads((capture_dir / "capture_manifest.json").read_bytes())
            self.assertTrue(manifest["complete"])
            self.assertFalse(manifest["staged"])
            self.assertEqual(manifest["error_code"], "population_digest_pins_not_established")
            self.assertIn("EXPECTED_RECORD_SHA256", manifest["pending_pins"])
            self.assertIn("capture_directory", manifest)
            self.assertTrue((capture_dir / "page_000000_00002.body").exists())
            keys = list(storage.list_prefix())
            self.assertTrue(keys)
            self.assertTrue(all(key.startswith("_quarantine/") for key in keys))
            self.assertFalse(any("candidate_ready" in key for key in keys))

    def test_first_complete_capture_establishes_pins_then_stages_nothing(self):
        with tempfile.TemporaryDirectory() as temporary, \
                tempfile.TemporaryDirectory() as capture_temporary:
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            capture_dir = Path(capture_temporary) / "captures"
            capture_dir.mkdir()
            calls = []
            with patch.object(nfhl.httpx, "Client", side_effect=lambda *a, **k: EmptyClient()):
                with self.assertRaisesRegex(ValueError, "population_digest_pins_not_established"):
                    self._capture(storage, temporary, capture_directory=str(capture_dir), calls=calls,
                                  pins=_pins())
            manifest = json.loads((capture_dir / "capture_manifest.json").read_bytes())
            pins = _pins(count=manifest["source_feature_count"],
                         record_sha256=manifest["source_record_sha256"],
                         geometry_sha256=manifest["source_geometry_sha256"],
                         observed_bounds=manifest["source_observed_bounds"],
                         null_geometry_count=manifest["source_null_geometry_count"],
                         nan_geometry_count=manifest["source_nan_string_geometry_count"],
                         unusable_geometry_feature_counts=manifest["source_unusable_geometry_feature_counts"])
            resumed_calls = []
            with patch.object(nfhl.httpx, "Client", side_effect=lambda *a, **k: EmptyClient()):
                candidate = self._capture(storage, temporary, capture_directory=str(capture_dir),
                                          calls=resumed_calls, pins=pins)
        self.assertNotIn("reused", candidate)
        self.assertEqual(candidate["feature_count"], len(IDS))
        self.assertFalse(candidate["release_accepted"])
        self.assertFalse(candidate["mapping_approved"])
        self.assertFalse(candidate["schema_contract_accepted"])
        self.assertFalse(candidate["id_contract_accepted"])
        self.assertFalse(candidate["rights_accepted"])
        # Resumed capture refetched only the ids bracket and document requests,
        # never a segment already on disk.
        self.assertEqual([name for name, _url in resumed_calls if name.startswith("page_")], [])
        self.assertLess(len(resumed_calls), len(calls))

    def test_candidate_ready_refuses_publication_and_reuse_verifies(self):
        with tempfile.TemporaryDirectory() as temporary, \
                tempfile.TemporaryDirectory() as capture_temporary:
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            capture_dir = Path(capture_temporary) / "captures"
            capture_dir.mkdir()
            calls = []
            with patch.object(nfhl.httpx, "Client", side_effect=lambda *a, **k: EmptyClient()):
                with self.assertRaisesRegex(ValueError, "population_digest_pins_not_established"):
                    self._capture(storage, temporary, capture_directory=str(capture_dir), calls=calls,
                                  pins=_pins())
            manifest = json.loads((capture_dir / "capture_manifest.json").read_bytes())
            pins = _pins(count=manifest["source_feature_count"],
                         record_sha256=manifest["source_record_sha256"],
                         geometry_sha256=manifest["source_geometry_sha256"],
                         observed_bounds=manifest["source_observed_bounds"],
                         null_geometry_count=manifest["source_null_geometry_count"],
                         nan_geometry_count=manifest["source_nan_string_geometry_count"],
                         unusable_geometry_feature_counts=manifest["source_unusable_geometry_feature_counts"])
            with patch.object(nfhl.httpx, "Client", side_effect=lambda *a, **k: EmptyClient()):
                candidate = self._capture(storage, temporary, capture_directory=str(capture_dir), pins=pins)
            ready = json.loads(storage.read_key(
                f"{candidate['dataset_slug']}/{candidate['file_slug']}/{candidate['version']}"
                f"/metadata/candidate_ready.json"))
            self.assertFalse(ready["approved"])
            self.assertFalse(ready["freshness_accepted"])
            self.assertFalse(ready["schema_contract_accepted"])
            self.assertFalse(ready["id_contract_accepted"])
            self.assertFalse(ready["rights_accepted"])
            self.assertFalse(ready["mapping_approved"])
            self.assertFalse(ready["release_accepted"])
            reused_calls = []
            with patch.object(nfhl.httpx, "Client", side_effect=lambda *a, **k: EmptyClient()):
                reused = self._capture(storage, temporary, capture_directory=str(capture_dir), pins=pins,
                                       calls=reused_calls)
            self.assertTrue(reused["reused"])
            self.assertEqual(reused["version"], candidate["version"])
            # Reuse replays segment evidence from the capture directory; only
            # the bounded bracket documents are refetched.
            self.assertEqual([name for name, _url in reused_calls if name.startswith("page_")], [])

if __name__ == "__main__":
    unittest.main()
