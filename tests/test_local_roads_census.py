import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets import local_roads_census as asset_module
from dagster_hifld.assets.local_roads_census import (
    local_roads_census, local_roads_census_complete, local_roads_census_draft_schedule,
)
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.sources import local_roads_census as source
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.esri_common import canonical as _canonical

# The live layer's dense OBJECTID roster is exercised against a small patched
# roster for speed; windows, digest gating and page logic are identical.
PATCH_IDS = [1, 2, 3, 4, 5, 6]
PATCH = dict(EXPECTED_COUNT=6, EXPECTED_ID_MIN=1, EXPECTED_ID_MAX=6, PAGE_SIZE=2,
             ROSTER_WINDOW_SIZE=2,
             EXPECTED_ID_SHA256=hashlib.sha256(_canonical(PATCH_IDS)).hexdigest(),
             EXPECTED_RECORD_SHA256="", EXPECTED_GEOMETRY_SHA256="",
             EXPECTED_MTFCC_VALUES={"S1400"},
             EXPECTED_MTFCC_RTTYPE_COUNTS={"S1400|null": 5, "S1400|M": 1})


def _page_geometry(oid: int) -> dict:
    return {"paths": [[[-1_500_000.0 + oid / 1000, 500_000.0 + oid / 1000],
                       [-1_499_000.0 + oid / 1000, 500_500.0 + oid / 1000]]]}


def _patched_feature(oid: int) -> dict:
    attrs = {}
    for field in source.ATTRIBUTE_FIELDS:
        name = field["name"]
        if name == "OBJECTID":
            attrs[name] = oid
        elif name == "OID":
            attrs[name] = f"110{oid:05d}"
        elif name == "MTFCC":
            attrs[name] = "S1400"
        elif name == "RTTYP":
            attrs[name] = "M" if oid == 6 else None
        elif name in ("PREDIR", "PREQUAL", "PRETYP", "SUFDIR", "SUFQUAL", "SUFTYP"):
            attrs[name] = None
        else:
            attrs[name] = f"{name}-{oid}"
    return {"attributes": attrs, "geometry": _page_geometry(oid)}


def _documents() -> dict:
    service = {"mapName": source.EXPECTED_SERVICE_MAP_NAME,
               "maxRecordCount": source.EXPECTED_SERVICE_MAX_RECORD_COUNT,
               "spatialReference": {"wkid": 102100, "latestWkid": 3857},
               "layers": [{"id": layer_id, "name": name, "geometryType": "esriGeometryPolyline"}
                          for layer_id, name in source.EXPECTED_SERVICE_LAYERS]}
    layer = {"id": source.LAYER_ID, "name": source.EXPECTED_LAYER_NAME,
             "geometryType": "esriGeometryPolyline",
             "description": source.EXPECTED_LAYER_DESCRIPTION,
             "minScale": source.EXPECTED_SCALE_RANGE["minScale"],
             "maxScale": source.EXPECTED_SCALE_RANGE["maxScale"],
             "spatialReference": {"wkid": 102100, "latestWkid": 3857},
             "extent": {**source.EXPECTED_EXTENT,
                        "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
             "fields": source.EXPECTED_FIELD_DECLARATIONS}
    docs = {
        "folder": {"services": [{"name": "TIGERweb/Transportation", "type": "MapServer"}]},
        "service_before": service, "layer_before": layer,
        "count_before": {"count": source.EXPECTED_COUNT},
        "service_after": json.loads(json.dumps(service)), "layer_after": json.loads(json.dumps(layer)),
        "count_after": {"count": source.EXPECTED_COUNT},
    }
    for phase in ("before", "after"):
        for index in range(source.roster_window_count()):
            lo, hi = source.roster_window(index)
            docs[f"roster_{phase}_{index:03d}"] = {"objectIds": list(range(lo, hi + 1))}
    for page_index in range(source.page_count()):
        lo, hi = source.page_window(page_index)
        docs[f"page_{page_index:04d}"] = {
            "spatialReference": {"wkid": 102100, "latestWkid": 3857},
            "features": [_patched_feature(oid) for oid in range(lo, hi + 1)]}
    return docs


def _receipt_for(name: str, url: str, body: bytes) -> dict:
    return {"method": "GET", "url": url, "final_url": url,
            "observed_at": "2026-11-15T00:00:00+00:00", "status": 200, "complete_body": True,
            "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
            "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
            "content_encoding": "identity", "content_length": len(body),
            "error_code": None, "error_type": None}


def _validate_documents(docs: dict):
    """Materialize documents as a retained capture, then run the streaming pass."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "capture"
        root.mkdir()
        urls = source._capture_urls()
        receipts = {}
        for name in sorted(source._capture_names()):
            body = _canonical(docs[name])
            receipt = _receipt_for(name, urls[name], body)
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            receipts[name] = receipt
        fields, parquet_path, audit = source._validate_stream(root, receipts)
        return fields, audit, parquet_path.read_bytes()


class LocalRoadsCensusTests(unittest.TestCase):
    def setUp(self):
        self._patches = [patch.object(source, name, value) for name, value in PATCH.items()]
        for item in self._patches:
            item.start()
            self.addCleanup(item.stop)

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(local_roads_census, hifld_ingest_assets)
        self.assertEqual(local_roads_census_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = local_roads_census_complete({"feature_count": asset_module.EXPECTED_COUNT,
                                             "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = local_roads_census_complete({"feature_count": asset_module.EXPECTED_COUNT - 1,
                                              "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_page_windows_cover_the_dense_roster_without_overlap(self):
        windows = [source.page_window(index) for index in range(source.page_count())]
        self.assertEqual(windows[0][0], source.EXPECTED_ID_MIN)
        self.assertEqual(windows[-1][1], source.EXPECTED_ID_MAX)
        for first, second in zip(windows, windows[1:]):
            self.assertEqual(first[1] + 1, second[0])
        with self.assertRaisesRegex(ValueError, "source_page_index_out_of_range"):
            source.page_window(source.page_count())

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, audit, _parquet = _validate_documents(_documents())
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(audit["source_feature_count"], 6)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 12)
        self.assertEqual(audit["source_wkid"], 102100)
        self.assertEqual(audit["source_latest_wkid"], 3857)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["source_mtfcc_rttype_counts"], {"S1400|null": 5, "S1400|M": 1})
        self.assertEqual(audit["source_null_counts"]["PREDIR"], 6)
        self.assertEqual(audit["source_null_counts"]["RTTYP"], 5)
        self.assertIn("not the TIGER/Line All Roads geodatabase product", audit["source_coverage_scope"])

    def test_parquet_preserves_native_attributes_and_web_mercator_crs(self):
        import io
        import struct

        import pyarrow.parquet as pq
        fields, audit, parquet_bytes = _validate_documents(_documents())
        table = pq.read_table(io.BytesIO(parquet_bytes))
        self.assertEqual(table.num_rows, 6)
        self.assertEqual(table.schema.field("OID").type.__str__(), "string")
        self.assertEqual(table.schema.field("PREDIR").type.__str__(), "int16")
        self.assertEqual(table.schema.field("OBJECTID").type.__str__(), "int64")
        self.assertEqual(table.column("OBJECTID").to_pylist(), PATCH_IDS)
        first = struct.unpack("<BI", table.column("geometry")[0].as_py()[:5])
        self.assertEqual(first[1], 5)  # MultiLineString WKB header
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 3857})
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["MultiLineString"])

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population", lambda d: d["roster_after_000"].update(objectIds=PATCH_IDS[:1]),
             "source_count_or_id_population_drift:roster_after_000"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("scale range", lambda d: d["layer_before"].update(minScale=999),
             "source_layer_identity_extent_or_scale_drift"),
            ("layer vintage changed", lambda d: d["layer_before"].update(
                description="Local roads; July 1, 2026"),
             "source_layer_identity_extent_or_scale_drift"),
            ("layer vintage missing", lambda d: d["layer_before"].pop("description"),
             "source_layer_identity_extent_or_scale_drift"),
            ("transfer", lambda d: d["page_0001"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_0001"].update(spatialReference={"wkid": 4326}), "crs_drift"),
            ("page ids", lambda d: d["page_0000"]["features"].reverse(),
             "source_page_id_population_drift"),
            ("ordinate", lambda d: d["page_0000"]["features"][0]["geometry"]["paths"][0][0].append(20),
             "source_xy_ordinate_contract_drift"),
            ("mtfcc contract", lambda d: d["page_0000"]["features"][0]["attributes"].update(MTFCC="S1100"),
             "source_mtfcc_population_contract_drift"),
            ("rttype contract", lambda d: d["page_0000"]["features"][0]["attributes"].update(RTTYP="X"),
             "source_rttype_population_contract_drift"),
            ("pair contract", lambda d: d["page_0000"]["features"][0]["attributes"].update(RTTYP="U"),
             "source_feature_id_or_population_contract_drift"),
            ("smallinteger type", lambda d: d["page_0000"]["features"][0]["attributes"].update(PREDIR="1"),
             "source_integer_value_type_drift"),
            ("string length", lambda d: d["page_0000"]["features"][0]["attributes"].update(RTTYP="UU"),
             "source_string_length_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(_documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    _validate_documents(docs)

    def test_record_digest_gate_fails_closed_when_pinned(self):
        fields, audit, _parquet = _validate_documents(_documents())
        with patch.object(source, "EXPECTED_RECORD_SHA256", audit["source_record_sha256"]):
            _validate_documents(_documents())
        with patch.object(source, "EXPECTED_RECORD_SHA256", "0" * 64), \
                self.assertRaisesRegex(ValueError, "source_record_sha256_drift"):
            _validate_documents(_documents())

    def test_vertex_domain_and_coverage_drift_fail_closed(self):
        def replace_vertex(docs, vertex, index):
            docs["page_0000"]["features"][0]["geometry"]["paths"][0][index] = vertex
        cases = [
            ("out-of-domain x", lambda d: replace_vertex(d, [-20_100_000.0, 500_000.0], 0),
             "source_xy_web_mercator_domain_violation"),
            ("in-range outside coverage", lambda d: replace_vertex(d, [-1_000_000.0, -2_000_000.0], 1),
             "source_vertex_outside_declared_extent"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(_documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    _validate_documents(docs)
        boundary = json.loads(json.dumps(_documents()))
        boundary["page_0000"]["features"][0]["geometry"]["paths"] = [[
            [source.EXPECTED_EXTENT["xmin"], source.EXPECTED_EXTENT["ymin"]],
            [source.EXPECTED_EXTENT["xmax"], source.EXPECTED_EXTENT["ymax"]]]]
        _fields, audit, _parquet = _validate_documents(boundary)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertIn("pinned-extent containment of every vertex", audit["drift_guard"])

    def test_service_roster_drift_fails_closed(self):
        cases = [
            ("folder", lambda d: d["folder"].update(services=[]), "official_tigerweb_service_directory_drift"),
            ("service map", lambda d: d["service_before"].update(mapName="Roads"),
             "official_tigerweb_transportation_service_drift"),
            ("service layers", lambda d: d["service_before"].update(layers=[]),
             "official_tigerweb_transportation_service_drift"),
            ("changed during capture", lambda d: d["layer_after"].update(name="other"),
             "source_service_or_layer_changed_during_capture"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(_documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    _validate_documents(docs)


class LocalRoadsCaptureFlowTests(unittest.TestCase):
    def setUp(self):
        self._patches = [patch.object(source, name, value) for name, value in PATCH.items()]
        for item in self._patches:
            item.start()
            self.addCleanup(item.stop)
        retry_patch = patch.object(source, "_TRANSIENT_RETRY_DELAYS", (0.0, 0.0))
        retry_patch.start()
        self.addCleanup(retry_patch.stop)

    def _fake_capture_get(self, fail_page: str | None, mutate_page=None):
        urls = source._capture_urls()

        def capture_get(client, url, root, name, max_bytes):
            if name == fail_page:
                raise ValueError(f"incomplete_source_capture:{name}")
            body = _canonical(_documents()[name])
            if mutate_page is not None and name == mutate_page[0]:
                doc = json.loads(body)
                mutate_page[1](doc)
                body = _canonical(doc)
            receipt = _receipt_for(name, urls[name], body)
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        return capture_get

    def _storage(self, tmp: str) -> StagingStorageResource:
        return StagingStorageResource(local_dir=str(Path(tmp) / "stage"))

    def test_interrupted_capture_resumes_and_seals(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = self._storage(tmp)
            capture_dir = Path(tmp) / "capture"
            capture_dir.mkdir()
            with patch.object(source, "capture_get", self._fake_capture_get("page_0002")):
                with self.assertRaisesRegex(ValueError, "incomplete_source_capture:page_0002"):
                    source.capture_local_roads_census(storage, version="regression",
                                                      capture_directory=str(capture_dir))
            progress = json.loads((capture_dir / "capture_manifest.json").read_bytes())
            self.assertFalse(progress["complete"])
            self.assertEqual(progress["error_code"], "ValueError")
            self.assertEqual(set(progress["receipts"]),
                             set(source._before_names()) | {"page_0000", "page_0001"})
            self.assertEqual(storage.list_prefix(""), [])
            with patch.object(source, "capture_get", self._fake_capture_get(None)):
                result = source.capture_local_roads_census(storage, version="regression",
                                                           capture_directory=str(capture_dir))
            self.assertFalse(result["reused"])
            self.assertEqual(result["feature_count"], 6)
            self.assertTrue(result["source_population_complete"])
            self.assertFalse(result["release_accepted"])
            ready = json.loads(storage.read_key(result["candidate_ready"]))
            self.assertEqual(ready["version"], result["version"])
            self.assertFalse(ready["approved"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}"
            staged = set(storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet", staged)
            self.assertIn(f"{prefix}/raw/page_0002.body", staged)
            self.assertIn(f"{prefix}/metadata/candidate_ready.json", staged)
            audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
            self.assertEqual(audit["geometry_valid_count"], 6)
            self.assertEqual(audit["source_vintage"], "2026-01-01")
            self.assertEqual(audit["source_vintage_evidence"], source.EXPECTED_LAYER_DESCRIPTION)
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            self.assertEqual(manifest["date_issued"], "2026-01-01")
            self.assertIsNone(manifest["source_modified"])
            self.assertEqual({item["release_date"] for item in manifest["inputs"]}, {"2026-01-01"})
            projected = provenance_projection(manifest)
            self.assertIsNotNone(projected)
            self.assertEqual({item["release_date"] for item in projected["inputs"]}, {"2026-01-01"})
            replay = source.capture_local_roads_census(storage, version="regression",
                                                       capture_directory=str(capture_dir))
            self.assertTrue(replay["reused"])
            self.assertEqual(replay["version"], result["version"])

    def test_completed_capture_manifest_replay_seals_the_same_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = self._storage(tmp)
            capture_dir = Path(tmp) / "capture"
            capture_dir.mkdir()
            with patch.object(source, "capture_get", self._fake_capture_get(None)):
                first = source.capture_local_roads_census(storage, version="regression",
                                                          capture_directory=str(capture_dir))
            manifest = json.loads((capture_dir / "capture_manifest.json").read_bytes())
            self.assertTrue(manifest["complete"])
            self.assertEqual(set(manifest["receipts"]), source._capture_names())
            self.assertEqual(manifest["artifact_roster"],
                             capture_roster_digests(capture_dir, exclude={"capture_manifest.json"}))
            second = source.capture_local_roads_census(storage, version="regression",
                                                       capture_directory=str(capture_dir))
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])

    def test_drifted_completed_capture_fails_closed_and_quarantines(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = self._storage(tmp)
            capture_dir = Path(tmp) / "capture"
            capture_dir.mkdir()
            drifted = ("page_0000",
                       lambda doc: doc["features"][0]["geometry"]["paths"][0][0].__setitem__(0, -20_100_000.0))
            with patch.object(source, "capture_get", self._fake_capture_get(None, drifted)):
                with self.assertRaisesRegex(ValueError, "local_roads_capture_failed; quarantined_at:"):
                    source.capture_local_roads_census(storage, version="regression",
                                                      capture_directory=str(capture_dir))
            quarantined = storage.list_prefix("_quarantine")
            self.assertTrue(any(key.endswith("page_0000.body") for key in quarantined))
            self.assertEqual(storage.list_prefix("local-roads-2"), [])

    def test_nonlocal_or_published_storage_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
            source.capture_local_roads_census(
                StagingStorageResource(bucket="remote", use_local=False, local_dir="/tmp/x"),
                version="v1")

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = self._storage(tmp)
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_local_roads_census(storage, version="../outside")


class LocalRoadsLivePinsTests(unittest.TestCase):
    def test_live_layer_pins_carry_the_publisher_vintage(self):
        self.assertEqual(source.EXPECTED_LAYER_DESCRIPTION, "Local roads; January 1, 2026")
        self.assertEqual(source.LAYER_ID, 8)
        self.assertEqual(source.EXPECTED_LAYER_NAME, "Local Roads")
        self.assertEqual(source._publisher_stated_vintage(source.EXPECTED_LAYER_DESCRIPTION), "2026-01-01")
        with self.assertRaisesRegex(ValueError, "source_layer_description_or_vintage_drift"):
            source._publisher_stated_vintage("Local roads")
        with self.assertRaisesRegex(ValueError, "source_layer_description_or_vintage_drift"):
            source._publisher_stated_vintage(None)

    def test_record_and_geometry_baseline_gates_are_pinned_active(self):
        self.assertRegex(source.EXPECTED_RECORD_SHA256, r"^[0-9a-f]{64}$")
        self.assertRegex(source.EXPECTED_GEOMETRY_SHA256, r"^[0-9a-f]{64}$")
        self.assertIn("pinned from the completed", source.BASELINE_PENDING_NOTE)

    def test_live_roster_pins_are_dense_and_self_consistent(self):
        self.assertEqual(source.EXPECTED_COUNT, 16_098_190)
        self.assertEqual(source.EXPECTED_ID_MIN, 1)
        self.assertEqual(source.EXPECTED_ID_MAX, 16_098_190)
        self.assertEqual(source.EXPECTED_COUNT, source.EXPECTED_ID_MAX - source.EXPECTED_ID_MIN + 1)
        self.assertEqual(source._sha(_canonical(list(range(
            source.EXPECTED_ID_MIN, source.EXPECTED_ID_MAX + 1)))), source.EXPECTED_ID_SHA256)
        self.assertEqual(source.page_count(), 805)
        self.assertEqual(source.page_window(0), (1, 20000))
        self.assertEqual(source.page_window(804), (16080001, 16098190))
        # The producer refuses a single full-layer returnIdsOnly roster request
        # (Esri 500, verified 2026-11-15), so the roster evidence is captured as
        # dense windowed id lists whose assembled bytes hash to the same pin.
        self.assertEqual(source.roster_window_count(), 65)
        self.assertEqual(source.roster_window(0), (1, 250000))
        self.assertEqual(source.roster_window(64), (16000001, 16098190))
        windows = [source.roster_window(index) for index in range(source.roster_window_count())]
        self.assertEqual(windows[0][0], source.EXPECTED_ID_MIN)
        self.assertEqual(windows[-1][1], source.EXPECTED_ID_MAX)
        for first, second in zip(windows, windows[1:]):
            self.assertEqual(first[1] + 1, second[0])
        assembled = _canonical(list(range(source.EXPECTED_ID_MIN, source.EXPECTED_ID_MAX + 1)))
        parts = []
        for index, (lo, hi) in enumerate(windows):
            if index:
                parts.append(b",")
            parts.append(_canonical(list(range(lo, hi + 1)))[1:-1])
        self.assertEqual(b"[" + b"".join(parts) + b"]", assembled)
        self.assertEqual(len(source._capture_names()), 4 + 65 + 805 + 3 + 65)

    def test_no_bare_full_layer_roster_request_is_captured(self):
        urls = source._capture_urls()
        bare = [name for name, url in urls.items()
                if "returnIdsOnly=true" in url and "objectid" not in url.lower()]
        self.assertEqual(bare, [])
        self.assertEqual(sum(1 for name in urls if "roster_before_" in name), 65)
        self.assertEqual(sum(1 for name in urls if "roster_after_" in name), 65)

    def test_live_population_pair_contract_sums_to_the_pinned_count(self):
        self.assertEqual(len(source.EXPECTED_MTFCC_VALUES), 13)
        self.assertEqual(len(source.EXPECTED_MTFCC_RTTYPE_COUNTS), 59)
        self.assertEqual(sum(source.EXPECTED_MTFCC_RTTYPE_COUNTS.values()), source.EXPECTED_COUNT)
        allowed = {f"{mtfcc}|{rttype}" for mtfcc in source.EXPECTED_MTFCC_VALUES
                   for rttype in ("null", *source.EXPECTED_RTTYPE_VALUES)}
        self.assertEqual(set(source.EXPECTED_MTFCC_RTTYPE_COUNTS) - allowed, set())

    def test_page_urls_stay_short(self):
        urls = source._capture_urls()
        longest = max(len(url) for name, url in urls.items())
        self.assertLess(longest, 500)


if __name__ == "__main__":
    unittest.main()
