import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.secondary_roads_census import (
    secondary_roads_census, secondary_roads_census_complete,
    secondary_roads_census_draft_schedule, secondary_roads_census_drift,
)
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.sources import secondary_roads_census as source
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.esri_common import canonical as _canonical

# The live layer's OBJECTIDs are not contiguous; tests exercise the identical
# logic against a small fixture roster for speed. Completeness is driven by the
# fixture's own count/ID witnesses, not by code pins.
PATCH_IDS = [2, 3, 5, 8, 13, 21]
FIXTURE_COUNT = len(PATCH_IDS)
FIXTURE_ID_SHA256 = hashlib.sha256(_canonical(PATCH_IDS)).hexdigest()
FIXTURE_EXTENT = {"xmin": -1_600_000.0, "ymin": 400_000.0,
                  "xmax": -1_400_000.0, "ymax": 600_000.0}
FIXTURE_DESCRIPTION = "Secondary roads - Interstates and US Highways; January 1, 2026"
PATCH = dict(PAGE_SIZE=2)


def _page_geometry(oid: int) -> dict:
    return {"paths": [[[-1_500_000.0 + oid / 1000, 500_000.0 + oid / 1000],
                       [-1_499_000.0 + oid / 1000, 500_500.0 + oid / 1000]]]}


def _attrs_for(oid: int, rttyp: str = None) -> dict:
    rttyp = ("I" if oid == 21 else "U") if rttyp is None else rttyp
    return {field["name"]: oid if field["name"] == "OBJECTID" else
            (f"110{oid:05d}" if field["name"] == "OID" else
             (rttyp if field["name"] == "RTTYP" else
              ("S1200" if field["name"] == "MTFCC" else
               (None if field["name"] in ("PREDIR", "PREQUAL", "PRETYP", "SUFDIR", "SUFQUAL", "SUFTYP")
                else f"{field['name']}-{oid}"))))
            for field in source.ATTRIBUTE_FIELDS}


class SecondaryRoadsCensusTests(unittest.TestCase):
    def setUp(self):
        self._patches = [patch.object(source, name, value) for name, value in PATCH.items()]
        for item in self._patches:
            item.start()
            self.addCleanup(item.stop)

    def _documents(self, ids=None):
        ids = list(PATCH_IDS) if ids is None else list(ids)
        service = {"mapName": source.EXPECTED_SERVICE_MAP_NAME,
                   "maxRecordCount": source.EXPECTED_SERVICE_MAX_RECORD_COUNT,
                   "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                   "layers": [{"id": layer_id, "name": name, "geometryType": "esriGeometryPolyline"}
                              for layer_id, name in source.EXPECTED_SERVICE_LAYERS]}
        layer = {"id": source.LAYER_ID, "name": source.EXPECTED_LAYER_NAME,
                 "geometryType": "esriGeometryPolyline",
                 "description": FIXTURE_DESCRIPTION,
                 "minScale": source.EXPECTED_SCALE_RANGE["minScale"],
                 "maxScale": source.EXPECTED_SCALE_RANGE["maxScale"],
                 "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                 "extent": {**FIXTURE_EXTENT,
                            "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "folder": {"services": [{"name": "TIGERweb/Transportation", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": len(ids)},
            "ids_before": {"objectIds": sorted(ids)},
            "service_after": json.loads(json.dumps(service)), "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": len(ids)},
            "ids_after": {"objectIds": sorted(ids)},
        }
        ordered = sorted(ids)
        for page_index in range((len(ordered) + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
            rows = [{"attributes": _attrs_for(oid), "geometry": _page_geometry(oid)}
                    for oid in ordered[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]]
            docs[f"page_{page_index:03d}"] = {"spatialReference": {"wkid": 102100, "latestWkid": 3857},
                                              "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(secondary_roads_census, hifld_ingest_assets)
        self.assertEqual(secondary_roads_census_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = secondary_roads_census_complete({"feature_count": 6, "source_reported_count": 6,
                                                 "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = secondary_roads_census_complete({"feature_count": 6, "source_reported_count": 5,
                                                  "source_population_complete": True})
        self.assertFalse(failed.passed)
        missing = secondary_roads_census_complete({"feature_count": 6,
                                                   "source_population_complete": True})
        self.assertFalse(missing.passed)
        self.assertEqual(secondary_roads_census_drift({"feature_count": 6,
                                                       "source_id_sha256": "abc"}).severity.value, "WARN")

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        with patch.object(source, "PAGE_SIZE", 3):
            roster = [2, 3, 5, 8, 13, 21, 34]
            urls = dict(source._capture_roster(roster))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         [f"page_{index:03d}" for index in range(3)])
        self.assertIn("objectIds=2%2C3%2C5", urls["page_000"])
        self.assertIn("objectIds=8%2C13%2C21", urls["page_001"])
        self.assertIn("objectIds=34", urls["page_002"])
        self.assertIn("returnZ=true", urls["page_000"])
        self.assertIn("returnM=true", urls["page_000"])
        self.assertIn(f"outFields=%2A", urls["page_000"])
        self.assertNotIn("objectIds=35", urls["page_002"])

    def test_capture_names_derive_page_count_from_observed_roster(self):
        with patch.object(source, "PAGE_SIZE", 3):
            names = source._capture_names([2, 3, 5, 8, 13, 21, 34])
        self.assertEqual([name for name in sorted(names) if name.startswith("page_")],
                         [f"page_{index:03d}" for index in range(3)])
        self.assertEqual(len(names), len(source._BASE_CAPTURE_NAMES) + 3)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), FIXTURE_COUNT)
        self.assertEqual(audit["source_id_sha256"], FIXTURE_ID_SHA256)
        self.assertEqual(audit["source_id_min"], min(PATCH_IDS))
        self.assertEqual(audit["source_id_max"], max(PATCH_IDS))
        self.assertEqual(audit["source_vertex_count"], 2 * FIXTURE_COUNT)
        self.assertEqual(audit["source_wkid"], 102100)
        self.assertEqual(audit["source_latest_wkid"], 3857)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["source_mtfcc_counts"], {"S1200": FIXTURE_COUNT})
        self.assertEqual(audit["source_rttype_counts"], {"U": 5, "I": 1})
        self.assertEqual(audit["source_null_counts"]["PREDIR"], FIXTURE_COUNT)
        self.assertIn("not the TIGER/Line All Roads geodatabase product", audit["source_coverage_scope"])
        self.assertIn("observed roster is sparse", audit["source_id_note"])

    def test_parquet_preserves_native_attributes_and_web_mercator_crs(self):
        import io
        import struct

        import pyarrow.parquet as pq
        fields, features, audit = source._validate(self._captures(self._documents()))
        data = source._parquet_bytes(fields, features, "EPSG:3857")
        table = pq.read_table(io.BytesIO(data))
        self.assertEqual(table.num_rows, FIXTURE_COUNT)
        self.assertEqual(table.schema.field("OID").type.__str__(), "string")
        self.assertEqual(table.schema.field("PREDIR").type.__str__(), "int16")
        self.assertEqual(table.schema.field("OBJECTID").type.__str__(), "int64")
        self.assertEqual(table.column("OBJECTID").to_pylist(), sorted(PATCH_IDS))
        first = struct.unpack("<BI", table.column("geometry")[0].as_py()[:5])
        self.assertEqual(first[1], 5)  # MultiLineString WKB header
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 3857})
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["MultiLineString"])

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population", lambda d: d["ids_after"].update(objectIds=PATCH_IDS[:-1]),
             "source_count_or_id_roster_drift"),
            ("count mismatch", lambda d: d["count_after"].update(count=FIXTURE_COUNT + 1),
             "source_count_or_id_roster_drift"),
            ("torn after roster", lambda d: d["ids_after"].update(objectIds=PATCH_IDS + [99]),
             "source_count_or_id_roster_drift"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("scale range", lambda d: d["layer_before"].update(minScale=999),
             "source_layer_identity_extent_or_scale_drift"),
            ("layer extent invalid", lambda d: d["layer_before"]["extent"].update(xmax=-1_600_000.0),
             "source_layer_extent_invalid"),
            ("layer vintage unparseable", lambda d: d["layer_before"].update(
                description="Secondary roads - Interstates and US Highways"),
             "source_layer_description_or_vintage_drift"),
            ("layer vintage missing", lambda d: d["layer_before"].pop("description"),
             "source_layer_description_or_vintage_drift"),
            ("transfer", lambda d: d["page_001"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_001"].update(spatialReference={"wkid": 4326}), "crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("ordinate", lambda d: d["page_000"]["features"][0]["geometry"]["paths"][0][0].append(20),
             "source_xy_ordinate_contract_drift"),
            ("mtfcc contract", lambda d: d["page_000"]["features"][0]["attributes"].update(MTFCC="S1100"),
             "source_mtfcc_population_contract_drift"),
            ("rttype contract", lambda d: d["page_000"]["features"][0]["attributes"].update(RTTYP="S"),
             "source_rttype_population_contract_drift"),
            ("smallinteger type", lambda d: d["page_000"]["features"][0]["attributes"].update(PREDIR="1"),
             "source_integer_value_type_drift"),
            ("string length", lambda d: d["page_000"]["features"][0]["attributes"].update(RTTYP="UU"),
             "source_string_length_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def test_vertex_domain_and_coverage_drift_fail_closed(self):
        def replace_vertex(docs, vertex, index):
            docs["page_000"]["features"][0]["geometry"]["paths"][0][index] = vertex
        cases = [
            ("out-of-domain x", lambda d: replace_vertex(d, [-20_100_000.0, 500_000.0], 0),
             "source_xy_web_mercator_domain_violation"),
            ("in-range outside coverage", lambda d: replace_vertex(d, [-19_500_000.0, 0.0], 1),
             "source_vertex_outside_declared_extent"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))
        boundary = json.loads(json.dumps(self._documents()))
        boundary["page_000"]["features"][0]["geometry"]["paths"] = [[
            [FIXTURE_EXTENT["xmin"], FIXTURE_EXTENT["ymin"]],
            [FIXTURE_EXTENT["xmax"], FIXTURE_EXTENT["ymax"]]]]
        _fields, _features, audit = source._validate(self._captures(boundary))
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertIn("observed-extent containment of every vertex", audit["drift_guard"])

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
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def _replay_directory(self, root, docs, mutate=None):
        documents = json.loads(json.dumps(docs))
        if mutate is not None:
            mutate(documents)
        root.mkdir(parents=True)
        ids = documents["ids_before"]["objectIds"]
        roster_urls = dict(source._capture_roster(ids))
        receipts = {}
        for name in sorted(source._capture_names(ids)):
            body = _canonical(documents[name])
            receipt = {"method": "GET", "url": roster_urls[name], "final_url": roster_urls[name],
                       "observed_at": "2026-11-14T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
                       "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                       "content_encoding": "identity", "content_length": len(body),
                       "error_code": None, "error_type": None}
            receipts[name] = receipt
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        manifest = {"complete": True, "captured_at": "2026-11-14T00:00:00+00:00",
                    "source_feature_count": len(ids), "receipts": receipts,
                    "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}
        (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        return root

    def test_drifted_replay_seals_no_ready_candidate(self):
        cases = [
            ("out-of-domain x", lambda d: d["page_000"]["features"][0]["geometry"]["paths"][0][0].__setitem__(
                0, -20_100_000.0), "source_xy_web_mercator_domain_violation"),
            ("roster drift", lambda d: d["ids_after"].update(objectIds=PATCH_IDS[:-1]),
             "source_count_or_id_roster_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                root = self._replay_directory(Path(tmp) / "capture", self._documents(), mutate)
                with self.assertRaisesRegex(ValueError, expected):
                    source.capture_secondary_roads_census(storage, version="regression",
                                                          capture_directory=str(root))
                self.assertEqual(storage.list_prefix(""), [])

    def test_valid_replay_seals_ready_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            root = self._replay_directory(Path(tmp) / "capture", self._documents())
            first = source.capture_secondary_roads_census(storage, version="regression",
                                                          capture_directory=str(root))
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], FIXTURE_COUNT)
            self.assertEqual(first["source_reported_count"], FIXTURE_COUNT)
            self.assertTrue(first["source_population_complete"])
            self.assertFalse(first["release_accepted"])
            ready = json.loads(storage.read_key(first["candidate_ready"]))
            self.assertEqual(ready["version"], first["version"])
            self.assertFalse(ready["approved"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            staged = set(storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet", staged)
            self.assertIn(f"{prefix}/raw/page_000.body", staged)
            self.assertIn(f"{prefix}/metadata/candidate_ready.json", staged)
            audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
            self.assertEqual(audit["geometry_valid_count"], FIXTURE_COUNT)
            self.assertEqual(audit["geometry_invalid_count"], 0)
            self.assertEqual(audit["source_vintage"], "2026-01-01")
            self.assertEqual(audit["source_vintage_evidence"], FIXTURE_DESCRIPTION)
            self.assertIn("not a per-feature last-edit timestamp", audit["source_vintage_note"])
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            self.assertEqual(manifest["date_issued"], "2026-01-01")
            self.assertIsNone(manifest["source_modified"])
            self.assertIsNone(manifest["date_modified"])
            self.assertEqual({item["release_date"] for item in manifest["inputs"]}, {"2026-01-01"})
            self.assertTrue(all("release_date_unknown_reason" not in item for item in manifest["inputs"]))
            # The publisher-stated vintage must survive into the projected
            # provenance that publication consumes.
            projected = provenance_projection(manifest)
            self.assertIsNotNone(projected)
            self.assertEqual({item["release_date"] for item in projected["inputs"]}, {"2026-01-01"})
            self.assertTrue(all("release_date_unknown_reason" not in item for item in projected["inputs"]))
            second = source.capture_secondary_roads_census(storage, version="regression",
                                                           capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])

    def test_upstream_added_feature_passes_completeness_and_warns_drift(self):
        # An upstream edit that adds one S1200 U/I feature must pass the
        # capture-time witness completeness and surface as a drift warning, not
        # a failed capture.
        added = 34
        docs = self._documents(PATCH_IDS + [added])
        fields, features, audit = source._validate(self._captures(docs))
        self.assertEqual(len(features), FIXTURE_COUNT + 1)
        self.assertEqual(audit["source_feature_count"], FIXTURE_COUNT + 1)
        self.assertEqual(audit["source_id_max"], added)
        # The approved-baseline drift check warns (does not block) on the
        # changed native ID roster.
        drift = secondary_roads_census_drift({"feature_count": FIXTURE_COUNT + 1,
                                              "source_id_sha256": source._sha(_canonical(PATCH_IDS + [added]))})
        self.assertEqual(drift.severity.value, "WARN")

    def test_nonlocal_or_published_storage_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
            source.capture_secondary_roads_census(
                StagingStorageResource(bucket="remote", use_local=False, local_dir="/tmp/x"),
                version="v1")

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_secondary_roads_census(storage, version="../outside")


class SecondaryRoadsTransientRetryTests(unittest.TestCase):
    def test_transient_incomplete_capture_is_retried_with_partial_files_cleared(self):
        calls = []
        sentinel = (b"body", {"status": 200})

        def flaky_capture_get(client, url, root, name, max_bytes):
            calls.append(name)
            if len(calls) == 1:
                (root / f"{name}.body").write_bytes(b"partial")
                (root / f"{name}.wire").write_bytes(b"partial")
                (root / f"{name}.receipt.json").write_bytes(b"{}")
                raise ValueError(f"incomplete_source_capture:{name}")
            self.assertFalse((root / f"{name}.body").exists())
            self.assertFalse((root / f"{name}.receipt.json").exists())
            return sentinel

        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(source, "_TRANSIENT_RETRY_DELAYS", (0.0, 0.0)), \
                patch.object(source, "capture_get", flaky_capture_get):
            root = Path(tmp)
            result = source._capture(None, "https://example.invalid/query", root, "page_000")
        self.assertEqual(result, sentinel)
        self.assertEqual(calls, ["page_000", "page_000"])

    def test_non_transient_capture_failure_is_not_retried(self):
        calls = []

        def failing_capture_get(client, url, root, name, max_bytes):
            calls.append(name)
            raise ValueError("source_capture_budget_exceeded_before_request")

        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(source, "capture_get", failing_capture_get):
            with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded"):
                source._capture(None, "https://example.invalid/query", Path(tmp), "page_000")
        self.assertEqual(len(calls), 1)

    def test_persistently_incomplete_capture_fails_after_bounded_retries(self):
        calls = []

        def failing_capture_get(client, url, root, name, max_bytes):
            calls.append(name)
            raise ValueError(f"incomplete_source_capture:{name}")

        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(source, "_TRANSIENT_RETRY_DELAYS", (0.0, 0.0)), \
                patch.object(source, "capture_get", failing_capture_get):
            with self.assertRaisesRegex(ValueError, "incomplete_source_capture:page_000"):
                source._capture(None, "https://example.invalid/query", Path(tmp), "page_000")
        self.assertEqual(len(calls), 3)


class SecondaryRoadsWitnessTests(unittest.TestCase):
    def test_publisher_vintage_is_parsed_not_pinned(self):
        self.assertEqual(source._publisher_stated_vintage(FIXTURE_DESCRIPTION), "2026-01-01")
        self.assertEqual(
            source._publisher_stated_vintage("Secondary roads - Interstates and US Highways; July 1, 2026"),
            "2026-07-01")
        with self.assertRaisesRegex(ValueError, "source_layer_description_or_vintage_drift"):
            source._publisher_stated_vintage("Secondary roads - Interstates and US Highways")
        with self.assertRaisesRegex(ValueError, "source_layer_description_or_vintage_drift"):
            source._publisher_stated_vintage(None)

    def test_capture_names_derive_from_observed_count(self):
        self.assertEqual(source.PAGE_SIZE, 250)
        ids = list(range(1, 31213))  # representative 31212-feature roster
        names = source._capture_names(ids)
        self.assertEqual(len([name for name in names if name.startswith("page_")]),
                         (31212 + 250 - 1) // 250)
        self.assertEqual(len(names), len(source._BASE_CAPTURE_NAMES) + (31212 + 250 - 1) // 250)

    def test_page_urls_stay_under_the_gateway_request_line_limit(self):
        ids = list(range(1, 31213))
        urls = dict(source._capture_roster(ids))
        longest = max(len(url) for name, url in urls.items() if name.startswith("page_"))
        self.assertLess(longest, 8000)


if __name__ == "__main__":
    unittest.main()
