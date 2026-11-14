"""Focused contract checks for the FEMA NFHL Flood Hazard Boundaries source adapter."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from shapely import wkb
from shapely.geometry import LineString, MultiLineString

from dagster_hifld.sources import nfhl_flood_hazard_boundaries as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")

PAGE_NAMES = ("page_000000_00002", "page_000002_00002")


def _write_page_files(capture_root: Path, index: int, page: dict) -> dict:
    name = f"page_{index * 2:06d}_00002"
    body = json.dumps(page, separators=(",", ":"), ensure_ascii=False).encode()
    (capture_root / f"{name}.body").write_bytes(body)
    (capture_root / f"{name}.wire").write_bytes(body)
    receipt = {"method": "GET", "url": source._segment_query(index * 2, 2), "final_url": None, "status": 200,
               "complete_body": True, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
    (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    return {"name": name, "body": body, "receipt": receipt}


def _write_roster_windows(capture_root: Path, stage: str, ids: list[int]) -> list[dict]:
    """Persist one stage's roster windows at the fixture's ROSTER_WINDOW of 2."""
    windows = []
    for index, low in enumerate(range(min(ids), max(ids) + 1, 2)):
        name = f"ids_{stage}_r{index:03d}"
        chunk = [value for value in ids if low <= value < low + 2]
        body = json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": chunk},
                          separators=(",", ":")).encode()
        (capture_root / f"{name}.body").write_bytes(body)
        (capture_root / f"{name}.wire").write_bytes(body)
        receipt = {"method": "GET", "url": source._roster_range_url(low, min(low + 2, max(ids) + 1)),
                   "final_url": None, "status": 200, "complete_body": True,
                   "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
        (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        windows.append({"name": name, "body": body, "receipt": receipt})
    return windows


def _audited(features):
    """Digests/bounds for the fixture population, via the module's own accumulator."""
    attr_names = [f["name"] for f in FIXTURE_FIELDS if f["name"] != "SHAPE"]
    accumulator = source._SegmentAccumulator(attr_names)
    accumulator.add_page(sorted(features, key=lambda row: row["attributes"]["OBJECTID"]))
    return accumulator.finish()


def _pins(schema, audited, ids):
    return dict(EXPECTED_COUNT=len(ids), SEGMENT_START_RECORDS=2, ROSTER_WINDOW=2,
                EXPECTED_NULL_GEOMETRY_COUNT=audited["null_geometry_count"],
                EXPECTED_NAN_GEOMETRY_COUNT=audited["nan_string_geometry_count"],
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical(ids)).hexdigest(),
                EXPECTED_RECORD_SHA256=audited["record_sha256"],
                EXPECTED_GEOMETRY_SHA256=audited["geometry_sha256"],
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(27, "Flood Hazard Boundaries", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -170.8, "ymin": -14.3, "xmax": 145.8, "ymax": 66.6},
                EXPECTED_OBSERVED_BOUNDS=audited["observed_bounds"],
                MIN_FREE_BYTES=0)


FIXTURE_FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
    {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
    {"name": "FLD_LN_ID", "type": "esriFieldTypeString", "alias": "FLD_LN_ID", "length": 32, "domain": None},
    {"name": "LN_TYP", "type": "esriFieldTypeString", "alias": "LN_TYP", "length": 26, "domain": None},
    {"name": "SOURCE_CIT", "type": "esriFieldTypeString", "alias": "SOURCE_CIT", "length": 21, "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    {"name": "SHAPE.STLength()", "type": "esriFieldTypeDouble", "alias": "SHAPE.STLength()", "domain": None},
    {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
    {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
]


class NfhlFloodHazardBoundariesTests(unittest.TestCase):
    def setUp(self):
        self._temporary = tempfile.TemporaryDirectory(prefix="nfhl-fhb-tests-")
        self.root = Path(self._temporary.name) / "captures"
        self.root.mkdir(parents=True)
        self._build_fixture()

    def tearDown(self):
        self._temporary.cleanup()

    def _build_fixture(self):
        fields = FIXTURE_FIELDS
        layer = {"id": 27, "name": "Flood Hazard Boundaries", "type": "Feature Layer",
                 "geometryType": "esriGeometryPolyline",
                 "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
                 "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
                 "extent": {"xmin": -170.8, "ymin": -14.3, "xmax": 145.8, "ymax": 66.6,
                            "spatialReference": {"wkid": 4269, "latestWkid": 4269}},
                 "fields": fields}
        service = {"mapName": "NFHL",
                   "layers": [{"id": 27, "name": "Flood Hazard Boundaries", "type": "Feature Layer"}]}
        attrs = lambda oid, fld_ln_id: {"OBJECTID": oid, "DFIRM_ID": "01003C", "VERSION_ID": "1.1.1.0",
                                        "FLD_LN_ID": fld_ln_id, "LN_TYP": "SFHA / Flood Zone Boundary",
                                        "SOURCE_CIT": None, "SHAPE.STLength()": 0.1851277091846197,
                                        "GFID": "2daa8f96-8523-4ed3-bdef-653fac6517df",
                                        "GlobalID": "{0E002140-B6B3-4CD9-880E-C501AAEB6188}"}
        self.features = [
            {"attributes": attrs(10, "01003C_1"),
             "geometry": {"paths": [[[-87.79666449851726, 30.490215793390348], [-87.79, 30.49]]]}},
            {"attributes": attrs(11, "01003C_2"), "geometry": None},
            {"attributes": attrs(12, "01003C_3"),
             "geometry": {"paths": [[[-87.0, 33.0], ["NaN", "NaN"], [-87.2, 33.2]]]}},
            {"attributes": attrs(13, "01003C_4"),
             "geometry": {"paths": [[[-88.0, 32.0], [-88.1, 32.1]],
                                    [[-89.0, 31.0], [-89.2, 31.2]]]}},
        ]
        self.ids = [10, 11, 12, 13]
        self.schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain")
                        if k in f} for f in fields]
        self.audited = _audited(self.features)
        crs = {"wkid": 4269, "latestWkid": 4269}
        self.pages = [
            {"features": self.features[:2], "spatialReference": crs, "exceededTransferLimit": True},
            {"features": self.features[2:], "spatialReference": crs},
        ]
        self.page_files = [_write_page_files(self.root, index, page)
                           for index, page in enumerate(self.pages)]
        self.stats_doc = {"features": [{"attributes": {"MIN_OBJECTID": 10, "MAX_OBJECTID": 13}}]}
        self.window_files = {stage: _write_roster_windows(self.root, stage, self.ids)
                             for stage in ("seed", "before", "after")}
        docs = {"service_before": service, "service_after": service, "layer_before": layer,
                "layer_after": layer,
                "count_before": {"count": 4}, "count_after": {"count": 4},
                "ids_stats": self.stats_doc}
        self.doc_bodies = {name: json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode()
                           for name, doc in docs.items()}
        self.captures = {name: (body, {"status": 200}) for name, body in self.doc_bodies.items()}
        for page_file in self.page_files:
            self.captures[page_file["name"]] = (None, page_file["receipt"])
        for windows in self.window_files.values():
            for window in windows:
                self.captures[window["name"]] = (None, window["receipt"])
        self.captures["service_doc"] = (SERVICE_DOC_BODY, {"status": 200})
        self.roster_windows = {f"ids_{stage}": [w["name"] for w in windows]
                               for stage, windows in self.window_files.items()}
        self.pins = _pins(self.schema, self.audited, self.ids)

    def _bodies_for_capture(self):
        bodies = dict(self.doc_bodies)
        bodies["service_doc"] = SERVICE_DOC_BODY
        for page_file in self.page_files:
            bodies[page_file["name"]] = page_file["body"]
        for windows in self.window_files.values():
            for window in windows:
                bodies[window["name"]] = window["body"]
        return bodies

    def _fake_capture_get(self, bodies, calls):
        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at":
                       "2026-11-12T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": digest, "bytes": len(body),
                       "sha256": digest, "content_encoding": None, "content_length": str(len(body)),
                       "error_code": None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt
        return fake_capture_get

    def _validate(self, **pin_overrides):
        pins = dict(self.pins)
        pins.update(pin_overrides)
        with patch.multiple(source, **pins):
            return source._validate_captures(self.root, self.captures, self.roster_windows)

    def test_streamed_list_sha_matches_canonical_digest(self):
        self.assertEqual(source._list_sha(self.features), source._sha(_canonical(self.features)))
        self.assertEqual(source._list_sha([]), source._sha(_canonical([])))

    def test_eight_digit_segment_offsets_parse_and_tile(self):
        # The live layer spans more than fifteen million OBJECTIDs, so segment
        # names carry eight-digit offsets; the roster parser must accept them.
        self.assertEqual(source._parse_segment_name("page_15270000_02000"), (15270000, 2000))
        self.assertEqual(source._parse_segment_name("page_000000_02000"), (0, 2000))
        with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
            source._parse_segment_name("page_abc_00002")

    def test_mixed_width_segment_names_sort_numerically_not_lexicographically(self):
        # Regression: "page_15270000_02000" sorts lexicographically BEFORE
        # "page_998000_02000", which would falsely fail the tiling proof on
        # the live population; segment order must be numeric.
        names = [source._segment_name(offset, count) for offset, count in
                 ((998000, 2000), (15270000, 2000), (0, 2000), (15272000, 2000))]
        segments = sorted(source._parse_segment_name(name) for name in names)
        self.assertEqual(segments, [(0, 2000), (998000, 2000), (15270000, 2000), (15272000, 2000)])

    def test_eight_digit_roster_passes_tiling_then_fails_roster_completeness(self):
        # A captures roster that tiles a 15,000,005-feature population across
        # the seven/eight-digit offset boundary must get past the tiling proof
        # (failing later, on the missing documentation evidence).
        segments = [(offset, 2000) for offset in range(0, 15000000, 2000)] + [(15000000, 5)]
        captures = {source._segment_name(offset, count): (b"{}", {"status": 200})
                    for offset, count in segments}
        with patch.multiple(source, EXPECTED_COUNT=15000005):
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
                source._validate_captures(self.root, captures, {"ids_seed": [], "ids_before": [], "ids_after": []})

    def test_valid_fixture_stream_validates(self):
        _, _, extracted = self._validate()
        self.assertEqual(extracted["ids"], [10, 11, 12, 13])
        self.assertEqual(extracted["audit"]["null_geometry_count"], 1)
        self.assertEqual(extracted["audit"]["nan_string_geometry_count"], 1)

    def test_id_roster_response_order_is_not_population_drift(self):
        window = self.root / "ids_before_r000.body"
        doc = json.loads(window.read_bytes())
        doc["objectIds"] = list(reversed(doc["objectIds"]))
        window.write_bytes(json.dumps(doc, separators=(",", ":")).encode())
        _, _, extracted = self._validate()
        self.assertEqual(extracted["ids"], [10, 11, 12, 13])

    def test_missing_roster_window_is_rejected(self):
        del self.captures["ids_before_r001"]
        self.roster_windows["ids_before"].remove("ids_before_r001")
        (self.root / "ids_before_r001.body").unlink()
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            self._validate()

    def test_malformed_oid_stats_are_rejected(self):
        self.captures["ids_stats"] = (json.dumps(
            {"features": [{"attributes": {"MIN_OBJECTID": "10", "MAX_OBJECTID": 13}}]},
            separators=(",", ":")).encode(), self.captures["ids_stats"][1])
        with self.assertRaisesRegex(ValueError, "source_oid_stats_drift"):
            self._validate()

    def test_missing_service_documentation_link_is_rejected(self):
        self.captures["service_doc"] = (b"<html>unrelated</html>", {"status": 200})
        with self.assertRaisesRegex(ValueError, "fema_service_documentation_link_drift"):
            self._validate()

    def test_crs_drift_is_rejected(self):
        page = json.loads((self.root / "page_000000_00002.body").read_bytes())
        page["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        (self.root / "page_000000_00002.body").write_bytes(json.dumps(page).encode())
        with self.assertRaisesRegex(ValueError, "source_page_000000_crs_drift"):
            self._validate()

    def test_native_nad83_fields_and_null_geometry_preserved_in_parquet(self):
        body_path = self.root / "candidate.parquet"
        stat = source._write_parquet(FIXTURE_FIELDS, self.root, list(PAGE_NAMES), self.ids, body_path)
        table = pq.read_table(body_path)
        self.assertEqual(table.num_rows, 4)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13])
        self.assertEqual(table.column("FLD_LN_ID").to_pylist(),
                         ["01003C_1", "01003C_2", "01003C_3", "01003C_4"])
        self.assertEqual(table.column("LN_TYP").to_pylist(), ["SFHA / Flood Zone Boundary"] * 4)
        self.assertEqual(table.column("SOURCE_CIT").to_pylist(), [None] * 4)
        self.assertEqual(table.column("SHAPE.STLength()").type.__str__(), "double")
        self.assertEqual(table.column("GlobalID").type.__str__(), "string")
        self.assertEqual(table.column("SHAPE").null_count, 2)
        shapes = table.column("SHAPE").to_pylist()
        self.assertIsInstance(wkb.loads(shapes[0]), LineString)
        self.assertIsNone(shapes[1])
        self.assertIsNone(shapes[2])
        multi = wkb.loads(shapes[3])
        self.assertIsInstance(multi, MultiLineString)
        self.assertEqual(len(multi.geoms), 2)
        self.assertEqual(wkb.loads(shapes[0]).coords[0], (-87.79666449851726, 30.490215793390348))
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geo["columns"]["SHAPE"]["geometry_types"], ["LineString", "MultiLineString"])
        self.assertEqual(stat["bytes"], body_path.stat().st_size)
        self.assertEqual(stat["sha256"], hashlib.sha256(body_path.read_bytes()).hexdigest())

    def test_curve_paths_geometry_is_rejected(self):
        page = json.loads((self.root / "page_000000_00002.body").read_bytes())
        page["features"][0]["geometry"] = {"curvePaths": [[-86.4, 34.1], [-86.5, 34.2]]}
        (self.root / "page_000000_00002.body").write_bytes(json.dumps(page).encode())
        with self.assertRaisesRegex(ValueError, "source_line_geometry_invalid"):
            self._validate()

    def test_degenerate_line_geometry_is_rejected(self):
        page = json.loads((self.root / "page_000002_00002.body").read_bytes())
        page["features"][1]["geometry"] = {"paths": [[-88.0, 32.0]]}
        (self.root / "page_000002_00002.body").write_bytes(json.dumps(page).encode())
        with self.assertRaisesRegex(ValueError, "source_line_geometry_invalid"):
            self._validate()

    def test_out_of_order_page_records_are_rejected(self):
        # The whole-population digest is accumulated by streaming segments in
        # OBJECTID order, so any non-ascending record stream must fail.
        page = json.loads((self.root / "page_000000_00002.body").read_bytes())
        page["features"] = [page["features"][1], page["features"][0]]
        (self.root / "page_000000_00002.body").write_bytes(json.dumps(page).encode())
        with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
            self._validate()

    def test_final_page_cannot_claim_transfer_truncation(self):
        final = json.loads((self.root / "page_000002_00002.body").read_bytes())
        final["exceededTransferLimit"] = True
        (self.root / "page_000002_00002.body").write_bytes(json.dumps(final).encode())
        with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
            self._validate()

    def test_unexpected_field_is_quarantined_by_contract(self):
        layer = json.loads(self.captures["layer_before"][0])
        layer["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        self.captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(),
                                         self.captures["layer_before"][1])
        with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
            self._validate()

    def test_missing_page_id_is_not_accepted(self):
        page = json.loads((self.root / "page_000000_00002.body").read_bytes())
        page["features"] = page["features"][:1]
        (self.root / "page_000000_00002.body").write_bytes(json.dumps(page).encode())
        with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
            self._validate()

    def test_record_value_change_is_detected(self):
        page = json.loads((self.root / "page_000000_00002.body").read_bytes())
        page["features"][0]["attributes"]["LN_TYP"] = "Unexpected Type"
        (self.root / "page_000000_00002.body").write_bytes(json.dumps(page).encode())
        with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
            self._validate()

    def test_observed_coverage_drift_is_detected(self):
        with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
            self._validate(EXPECTED_OBSERVED_BOUNDS={"xmin": -89.2, "ymin": 31.0, "xmax": -86.4, "ymax": 34.2})

    def test_null_geometry_count_drift_is_detected(self):
        with self.assertRaisesRegex(ValueError, "source_null_geometry_count_drift"):
            self._validate(EXPECTED_NULL_GEOMETRY_COUNT=0)

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        import os

        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          "geoparquet/nfhl-flood-hazard-boundaries.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/nfhl-flood-hazard-boundaries.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG,
                                        source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)

    def test_resumable_capture_reuses_verified_segments_without_refetching(self):
        bodies = self._bodies_for_capture()
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            if name.startswith("page_"):
                raise AssertionError("resumed segment was refetched: " + name)
            return self._fake_capture_get(bodies, calls)(_client, url, capture_root, name, max_bytes=max_bytes)

        with patch.multiple(source, SEGMENT_START_RECORDS=2), \
             patch.object(source, "capture_get", side_effect=fake_capture_get):
            segments = source._capture_segments(None, self.root, 4)
        self.assertEqual(sorted(segments), list(PAGE_NAMES))
        self.assertEqual((self.root / "page_000000_00002.body").read_bytes(),
                         self.page_files[0]["body"])

    def test_stale_segment_evidence_is_renamed_and_refetched(self):
        bodies = self._bodies_for_capture()
        stale_body = b'{"features": []}'
        (self.root / "page_000000_00002.body").write_bytes(stale_body)
        (self.root / "page_000000_00002.wire").write_bytes(stale_body)
        receipt = {"url": source._segment_query(0, 2), "status": 200, "complete_body": True,
                   "sha256": hashlib.sha256(b"mismatched-evidence").hexdigest()}
        (self.root / "page_000000_00002.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        calls = []
        with patch.multiple(source, SEGMENT_START_RECORDS=2), \
             patch.object(source, "capture_get", side_effect=self._fake_capture_get(bodies, calls)), \
             patch.object(source.time, "sleep"):
            segments = source._capture_segments(None, self.root, 4)
        self.assertEqual(sorted(segments), list(PAGE_NAMES))
        self.assertEqual((self.root / "page_000000_00002.body").read_bytes(), self.page_files[0]["body"])
        self.assertEqual((self.root / "page_000000_00002.stale.body").read_bytes(), stale_body)
        self.assertIn("page_000000_00002", calls)

    def test_pending_digest_pins_fail_closed_with_computed_evidence(self):
        pending = {name: None for name in ("EXPECTED_RECORD_SHA256", "EXPECTED_GEOMETRY_SHA256",
                                           "EXPECTED_OBSERVED_BOUNDS", "EXPECTED_NULL_GEOMETRY_COUNT",
                                           "EXPECTED_NAN_GEOMETRY_COUNT")}
        with self.assertRaises(source.PopulationDigestPinsPending) as caught:
            self._validate(**pending)
        audit = caught.exception.audit
        self.assertEqual(audit["source_record_sha256"], self.audited["record_sha256"])
        self.assertEqual(audit["source_geometry_sha256"], self.audited["geometry_sha256"])
        self.assertEqual(audit["observed_bounds"], self.audited["observed_bounds"])
        self.assertEqual(audit["null_geometry_count"], self.audited["null_geometry_count"])
        self.assertEqual(audit["nan_string_geometry_count"], self.audited["nan_string_geometry_count"])
        self.assertEqual(audit["source_feature_count"], 4)
        self.assertIn("EXPECTED_RECORD_SHA256", audit["pending_pins"])

    def test_pending_pins_capture_writes_manifest_and_stages_nothing(self):
        from dagster_hifld.resources import StagingStorageResource

        pending = {**self.pins}
        for name in ("EXPECTED_RECORD_SHA256", "EXPECTED_GEOMETRY_SHA256", "EXPECTED_OBSERVED_BOUNDS",
                     "EXPECTED_NULL_GEOMETRY_COUNT", "EXPECTED_NAN_GEOMETRY_COUNT"):
            pending[name] = None
        bodies = self._bodies_for_capture()
        calls = []

        class EmptyClient:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with tempfile.TemporaryDirectory(prefix="nfhl-fhb-pending-") as temporary, \
             tempfile.TemporaryDirectory(prefix="nfhl-fhb-pending-cap-") as capture_temporary:
            storage = StagingStorageResource(use_local=True, local_dir=str(temporary), prefix="")
            capture_dir = Path(capture_temporary) / "captures"
            capture_dir.mkdir()
            with patch.multiple(source, **pending), \
                 patch.object(source, "capture_get", side_effect=self._fake_capture_get(bodies, calls)), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                with self.assertRaisesRegex(ValueError, "population_digest_pins_not_established"):
                    source.capture_nfhl_flood_hazard_boundaries(storage, version="vfixture",
                                                                capture_directory=str(capture_dir))
            manifest = json.loads((capture_dir / "capture_manifest.json").read_bytes())
            self.assertTrue(manifest["complete"])
            self.assertFalse(manifest["staged"])
            self.assertEqual(manifest["error_code"], "population_digest_pins_not_established")
            self.assertEqual(manifest["source_record_sha256"], self.audited["record_sha256"])
            self.assertEqual(manifest["source_feature_count"], 4)
            self.assertEqual(manifest["source_segment_count"], 2)
            self.assertIn("EXPECTED_RECORD_SHA256", manifest["pending_pins"])
            self.assertIn("capture_directory", manifest)
            # Resumable segment evidence is preserved on disk, never discarded.
            self.assertTrue((capture_dir / "page_000000_00002.body").exists())
            keys = list(storage.list_prefix())
            self.assertTrue(keys)
            self.assertTrue(all(key.startswith("_quarantine/") for key in keys))
            self.assertFalse(any("candidate_ready" in key for key in keys))

    def test_whole_layer_roster_request_is_never_issued(self):
        # The producer refuses the whole-layer returnIdsOnly request with its
        # 500 error document, so the capture must only ever issue
        # OBJECTID-range roster windows plus the other documented inputs.
        from dagster_hifld.resources import StagingStorageResource

        bodies = self._bodies_for_capture()
        urls = []

        class EmptyClient:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            urls.append(url)
            return self._fake_capture_get(bodies, [])(_client, url, capture_root, name, max_bytes=max_bytes)

        with tempfile.TemporaryDirectory(prefix="nfhl-fhb-roster-") as temporary:
            storage = StagingStorageResource(use_local=True, local_dir=str(temporary), prefix="")
            pending = {**self.pins}
            for name in ("EXPECTED_RECORD_SHA256", "EXPECTED_GEOMETRY_SHA256", "EXPECTED_OBSERVED_BOUNDS",
                         "EXPECTED_NULL_GEOMETRY_COUNT", "EXPECTED_NAN_GEOMETRY_COUNT"):
                pending[name] = None
            with patch.multiple(source, **pending), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                with self.assertRaisesRegex(ValueError, "population_digest_pins_not_established"):
                    source.capture_nfhl_flood_hazard_boundaries(storage, version="vfixture",
                                                                capture_directory=str(Path(temporary) / "captures"))
        forbidden = source._query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})
        self.assertNotIn(forbidden, urls)
        roster_urls = [url for url in urls if "returnIdsOnly" in url]
        self.assertTrue(roster_urls)
        self.assertTrue(all("OBJECTID%3E%3D" in url for url in roster_urls),
                        "every roster request must be OBJECTID-range constrained")

    def test_transient_producer_error_doc_is_retried_with_evidence(self):
        ok_body = b'{"objectIds": []}'
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = error_body if len(calls) == 1 else ok_body
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                       "bytes": len(body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with patch.object(source, "capture_get", side_effect=fake_capture_get), \
             patch.object(source.time, "sleep") as sleep_mock:
            body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                        self.root, "probe", max_bytes=1024,
                                                        attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                        base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
        self.assertEqual(body, ok_body)
        self.assertEqual(calls, ["probe", "probe"])
        self.assertEqual((self.root / "probe.body").read_bytes(), ok_body)
        self.assertEqual((self.root / "probe.attempt-01.body").read_bytes(), error_body)
        self.assertTrue(sleep_mock.called)

    def test_persistent_producer_error_doc_exhausts_retries(self):
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            digest = hashlib.sha256(error_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(error_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(error_body)
            (capture_root / f"{name}.wire").write_bytes(error_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return error_body, receipt

        with patch.object(source, "capture_get", side_effect=fake_capture_get), \
             patch.object(source.time, "sleep"):
            body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                        self.root, "probe", max_bytes=1024,
                                                        attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                        base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
        self.assertEqual(body, error_body)
        self.assertEqual(len(calls), source.DOC_CAPTURE_ATTEMPTS)
        for attempt in range(1, source.DOC_CAPTURE_ATTEMPTS):
            self.assertEqual((self.root / f"probe.attempt-{attempt:02d}.body").read_bytes(), error_body)

    def test_incomplete_transport_capture_is_retried_once_bounded(self):
        ok_body = b'{"objectIds": []}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            if name == "probe2":
                raise ValueError("unrelated_source_capture_failure")
            if len(calls) == 1:
                (capture_root / f"{name}.body").write_bytes(b"")
                (capture_root / f"{name}.wire").write_bytes(b"")
                (capture_root / f"{name}.receipt.json").write_bytes(b'{"status": null}\n')
                raise ValueError(f"incomplete_source_capture:{name}")
            digest = hashlib.sha256(ok_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(ok_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(ok_body)
            (capture_root / f"{name}.wire").write_bytes(ok_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return ok_body, receipt

        with patch.object(source, "capture_get", side_effect=fake_capture_get), \
             patch.object(source.time, "sleep"):
            body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                        self.root, "probe", max_bytes=1024,
                                                        attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                        base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
            with self.assertRaisesRegex(ValueError, "unrelated_source_capture_failure"):
                source._capture_with_retry(None, "https://producer.invalid/other",
                                           self.root, "probe2", max_bytes=1024,
                                           attempts=source.DOC_CAPTURE_ATTEMPTS,
                                           base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS)
        self.assertEqual(body, ok_body)
        self.assertEqual(calls.count("probe"), 2)
        self.assertEqual(len(calls), 3)
        self.assertEqual((self.root / "probe.attempt-01.receipt.json").read_bytes(), b'{"status": null}\n')

    def test_page_tiling_gap_is_rejected(self):
        del self.captures["page_000002_00002"]
        with self.assertRaisesRegex(ValueError, "source_page_tiling_incomplete_or_overlapped"):
            self._validate()

    def test_malformed_segment_name_is_rejected(self):
        self.captures["page_xxxxx_00002"] = self.captures.pop("page_000002_00002")
        with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
            self._validate()

    def test_adaptive_bisection_splits_producer_cap_failures(self):
        bodies = self._bodies_for_capture()
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        bodies["page_000000_00004"] = error_body
        calls = []
        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = error_body if name == "page_000000_00004" else bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at":
                       "2026-11-12T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": digest, "bytes": len(body),
                       "sha256": digest, "content_encoding": None, "content_length": str(len(body)),
                       "error_code": None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as fresh:
            capture_root = Path(fresh) / "captures"
            capture_root.mkdir()
            with patch.multiple(source, SEGMENT_START_RECORDS=4,
                                SEGMENT_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=1), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                segments = source._capture_segments(None, capture_root, 4)
            self.assertEqual(sorted(segments), list(PAGE_NAMES))
            self.assertEqual((capture_root / "page_000000_00004.body").read_bytes(), error_body)
            self.assertEqual(calls.count("page_000000_00004"), 1)

    def test_unservable_single_feature_fails_explicitly(self):
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            digest = hashlib.sha256(error_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(error_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(error_body)
            (capture_root / f"{name}.wire").write_bytes(error_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return error_body, receipt

        with tempfile.TemporaryDirectory() as fresh:
            capture_root = Path(fresh) / "captures"
            capture_root.mkdir()
            with patch.multiple(source, SEGMENT_START_RECORDS=4,
                                SEGMENT_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=2), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                with self.assertRaisesRegex(ValueError, "source_single_feature_unservable"):
                    source._capture_segments(None, capture_root, 4)

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        bodies = self._bodies_for_capture()
        calls = []

        class EmptyClient:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with tempfile.TemporaryDirectory(prefix="nfhl-fhb-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **self.pins), \
                 patch.object(source, "capture_get", side_effect=self._fake_capture_get(bodies, calls)), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_flood_hazard_boundaries(storage, version="vfixture",
                                                                        capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 4)
                self.assertEqual(candidate["source_null_geometry_count"], self.audited["null_geometry_count"])
                self.assertEqual(candidate["source_nan_string_geometry_count"],
                                 self.audited["nan_string_geometry_count"])
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                for name, body in bodies.items():
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.body"), body)
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, candidate["version"])
                manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                         candidate["version"]).metadata
                summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                   candidate["version"], source.DATASET_SLUG,
                                                   source_metadata=manifest)
                self.assertEqual(summary.quality_manifest["null_geometry_count"],
                                 self.audited["null_geometry_count"]
                                 + self.audited["nan_string_geometry_count"])
                self.assertFalse(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG, file_slug=source.FILE_SLUG,
                                       version=candidate["version"], quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_nfhl_flood_hazard_boundaries(storage, version="vfixture",
                                                                     capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])


if __name__ == "__main__":
    unittest.main()
